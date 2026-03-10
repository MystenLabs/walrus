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
use walrus_core::encoding::ReedSolomonEncodingConfig;
use walrus_test_utils::random_data;

#[global_allocator]
static PEAK_ALLOC: PeakMemAlloc<System> = PeakMemAlloc::new(System);

fn get_peak_rss_bytes() -> usize {
    unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut usage);
        let max_rss = usage.ru_maxrss as usize;
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
}

fn parse_size(s: &str) -> Result<usize, String> {
    let s = s.to_lowercase();
    let (num, mult) = if let Some(n) = s.strip_suffix('g') {
        (n, 1 << 30)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 1 << 20)
    } else if let Some(n) = s.strip_suffix('k') {
        (n, 1 << 10)
    } else {
        (s.as_str(), 1)
    };
    let n: usize = num.parse().map_err(|e| format!("invalid size: {e}"))?;
    Ok(n * mult)
}

fn main() {
    let args = Args::parse();
    let config = ReedSolomonEncodingConfig::new(NonZeroU16::new(args.shards).unwrap());

    println!(
        "blob_size={} shards={} iterations={}",
        format_size(args.size),
        args.shards,
        args.iterations
    );

    let blob = random_data(args.size);
    let symbol_size = {
        let encoder = config.get_blob_encoder(&blob).unwrap();
        encoder.symbol_usize()
    };
    println!("symbol_size={symbol_size}");

    let mut durations = Vec::with_capacity(args.iterations.try_into().unwrap());
    let mut max_peak_heap: usize = 0;

    for i in 0..args.iterations {
        let blob_copy = blob.clone();
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

fn format_size(bytes: usize) -> String {
    if bytes >= 1 << 30 {
        format!("{}GiB", bytes >> 30)
    } else if bytes >= 1 << 20 {
        format!("{}MiB", bytes >> 20)
    } else if bytes >= 1 << 10 {
        format!("{}KiB", bytes >> 10)
    } else {
        format!("{bytes}B")
    }
}
