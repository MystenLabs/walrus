// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// Allowing `unwrap`s in examples.
#![allow(clippy::unwrap_used)]

//! Profiling binary for blob encoding.
//!
//! Runs `encode_with_metadata()` on configurable blob sizes with wall-clock timing.
//! Designed to be run under `samply record` or `cargo flamegraph` without criterion overhead.
//!
//! ```bash
//! cargo build --release --example profile_encoding
//! samply record ./target/release/examples/profile_encoding --size 32m
//! ```

use std::{alloc::System, num::NonZeroU16, time::Instant};

use clap::Parser;
use peakmem_alloc::{PeakMemAlloc, PeakMemAllocTrait};
use walrus_core::encoding::{BlobEncoder, ReedSolomonEncodingConfig};
use walrus_test_utils::random_data;
use walrus_utils::size::{format_size, parse_size};

#[global_allocator]
static PEAK_ALLOC: PeakMemAlloc<System> = PeakMemAlloc::new(System);

fn get_peak_rss_bytes() -> usize {
    unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut usage);
        let max_rss = usize::try_from(usage.ru_maxrss).unwrap();
        // macOS reports bytes, Linux reports KB
        if cfg!(target_os = "macos") {
            max_rss
        } else {
            max_rss * 1024
        }
    }
}

#[derive(Parser)]
#[command(about = "Profile blob encoding pipeline")]
struct Args {
    /// Blob size (e.g. 1k, 1m, 32m, 256m, 1g)
    #[arg(long, default_value = "32m", value_parser = parse_size)]
    size: usize,

    /// Number of shards
    #[arg(long, default_value_t = 1000)]
    shards: u16,

    /// Number of iterations
    #[arg(long, default_value_t = 1)]
    iterations: u32,

    /// Number of rayon threads (0 = use rayon default, which is num CPUs)
    #[arg(long, default_value_t = 0)]
    threads: usize,

    /// Number of blobs to encode concurrently (simulates multi-blob uploads)
    #[arg(long, default_value_t = 1)]
    concurrent_blobs: u32,
}

fn main() {
    let args = Args::parse();

    if args.threads > 0 {
        rayon::ThreadPoolBuilder::new()
            .num_threads(args.threads)
            .build_global()
            .unwrap();
    }
    let thread_count = rayon::current_num_threads();

    let config = ReedSolomonEncodingConfig::new(NonZeroU16::new(args.shards).unwrap());

    if args.concurrent_blobs > 1 {
        print!(
            "blob_size={} shards={} iterations={} threads={} concurrent_blobs={}",
            format_size(args.size),
            args.shards,
            args.iterations,
            thread_count,
            args.concurrent_blobs
        );
    } else {
        print!(
            "blob_size={} shards={} iterations={} threads={}",
            format_size(args.size),
            args.shards,
            args.iterations,
            thread_count
        );
    }

    let blob = random_data(args.size);
    let symbol_size = {
        let encoder = config.get_blob_encoder(&blob).unwrap();
        encoder.symbol_usize()
    };
    println!("\nsymbol_size={symbol_size}");

    if args.concurrent_blobs <= 1 {
        run_single_blob(&args, &config, &blob);
    } else {
        run_concurrent_blobs(&args, &config, &blob);
    }
}

fn run_single_blob(args: &Args, config: &ReedSolomonEncodingConfig, blob: &[u8]) {
    let mut durations = Vec::with_capacity(args.iterations.try_into().unwrap());
    let mut max_peak_heap: usize = 0;

    for i in 0..args.iterations {
        let blob_copy = blob.to_vec();
        let encoder = config.get_blob_encoder(&blob_copy).unwrap();

        PEAK_ALLOC.reset_peak_memory();
        let start = Instant::now();
        let (_sliver_pairs, _metadata) = encoder.encode_with_metadata();
        let elapsed = start.elapsed();
        let peak_heap = PEAK_ALLOC.get_peak_memory();
        let peak_rss = get_peak_rss_bytes();

        durations.push(elapsed);
        max_peak_heap = max_peak_heap.max(peak_heap);
        let throughput_mbs = args.size as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0);
        let expansion = peak_heap as f64 / args.size as f64;
        println!(
            "  iteration {}: {:.3}s ({:.1} MiB/s) peak_heap={} peak_rss={} expansion={:.1}x",
            i + 1,
            elapsed.as_secs_f64(),
            throughput_mbs,
            format_size(peak_heap),
            format_size(peak_rss),
            expansion
        );
    }

    if args.iterations > 1 {
        let total: f64 = durations.iter().map(|d| d.as_secs_f64()).sum();
        let avg = total / f64::from(args.iterations);
        let throughput_mbs = args.size as f64 / avg / (1024.0 * 1024.0);
        println!(
            "average: {avg:.3}s ({throughput_mbs:.1} MiB/s) max_peak_heap={}",
            format_size(max_peak_heap)
        );
    }
}

fn run_concurrent_blobs(args: &Args, config: &ReedSolomonEncodingConfig, blob: &[u8]) {
    let n: usize = args.concurrent_blobs.try_into().unwrap();
    let mut durations = Vec::with_capacity(args.iterations.try_into().unwrap());
    let mut max_peak_heap: usize = 0;

    for i in 0..args.iterations {
        // Pre-generate N blob copies and N encoders.
        let blob_copies: Vec<Vec<u8>> = (0..n).map(|_| blob.to_vec()).collect();
        let encoders: Vec<BlobEncoder<'_>> = blob_copies
            .iter()
            .map(|b| config.get_blob_encoder(b).unwrap())
            .collect();

        PEAK_ALLOC.reset_peak_memory();
        let start = Instant::now();

        // Use std::thread::scope to spawn N threads, each encoding one blob.
        let per_blob_elapsed: Vec<_> = std::thread::scope(|s| {
            let handles: Vec<_> = encoders
                .into_iter()
                .map(|encoder| {
                    s.spawn(move || {
                        let blob_start = Instant::now();
                        let (_sliver_pairs, _metadata) = encoder.encode_with_metadata();
                        blob_start.elapsed()
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

        let wall_time = start.elapsed();
        let peak_heap = PEAK_ALLOC.get_peak_memory();
        let peak_rss = get_peak_rss_bytes();

        durations.push(wall_time);
        max_peak_heap = max_peak_heap.max(peak_heap);

        println!(
            "  iteration {}: {:.3}s total wall time",
            i + 1,
            wall_time.as_secs_f64()
        );
        for (j, elapsed) in per_blob_elapsed.iter().enumerate() {
            let throughput_mbs = args.size as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0);
            println!(
                "    blob {}: {:.3}s ({:.1} MiB/s)",
                j + 1,
                elapsed.as_secs_f64(),
                throughput_mbs
            );
        }
        let total_data = args.size * n;
        let expansion = peak_heap as f64 / total_data as f64;
        println!(
            "  peak_heap={} peak_rss={} expansion={:.1}x (per blob: {})",
            format_size(peak_heap),
            format_size(peak_rss),
            expansion,
            format_size(peak_heap / n)
        );
    }

    if args.iterations > 1 {
        let total: f64 = durations.iter().map(|d| d.as_secs_f64()).sum();
        let avg = total / f64::from(args.iterations);
        let total_data = args.size * n;
        let throughput_mbs = total_data as f64 / avg / (1024.0 * 1024.0);
        println!(
            "average: {avg:.3}s ({throughput_mbs:.1} MiB/s) max_peak_heap={}",
            format_size(max_peak_heap)
        );
    }
}
