// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Compare the previous RustCrypto hasher with the production backend on identical inputs.
//! Run with `cargo bench -p walrus-core --bench merkle_hashing` on an x86 machine for SIMD results.

#![allow(clippy::unwrap_used)]

use core::{hint::black_box, num::NonZeroU16, time::Duration};

use criterion::{BenchmarkId, Criterion, Throughput};
use fastcrypto::hash::{Blake2b256 as Reference, HashFunction};
use walrus_core::{
    encoding::{
        EncodingAxis,
        EncodingConfigEnum,
        EncodingFactory as _,
        ReedSolomonEncodingConfig,
        SliverData,
    },
    hash::Blake2b256,
    merkle::MerkleTree,
};

fn leaf_hash<H: HashFunction<32>>(bytes: &[u8]) -> [u8; 32] {
    let mut hash = H::default();
    hash.update([0]);
    hash.update(bytes);
    hash.finalize().digest
}

fn inner_hash<H: HashFunction<32>>(bytes: &[u8; 64]) -> [u8; 32] {
    let mut hash = H::default();
    hash.update([1]);
    hash.update(&bytes[..32]);
    hash.update(&bytes[32..]);
    hash.finalize().digest
}

fn hash_inputs(c: &mut Criterion) {
    let config = ReedSolomonEncodingConfig::new(NonZeroU16::new(100).unwrap());
    let symbol_bytes = usize::from(config.symbol_size_for_blob(5 * 1024 * 1024).unwrap().get());
    eprintln!("5 MiB / 100 shards: {symbol_bytes} bytes per recovery symbol");
    let mut sizes = vec![0, 64, 127, 128, 129, 512, symbol_bytes, 65535];
    sizes.sort_unstable();
    sizes.dedup();
    let mut group = c.benchmark_group("merkle_leaf_hash");
    for length in sizes {
        let data: Vec<u8> = (0..=255).cycle().take(length).collect();
        assert_eq!(
            leaf_hash::<Reference>(&data),
            leaf_hash::<Blake2b256>(&data)
        );
        group.throughput(Throughput::Bytes(u64::try_from(length + 1).unwrap()));
        group.bench_with_input(BenchmarkId::new("rustcrypto", length), &data, |b, data| {
            b.iter(|| black_box(leaf_hash::<Reference>(black_box(data))));
        });
        group.bench_with_input(BenchmarkId::new("production", length), &data, |b, data| {
            b.iter(|| black_box(leaf_hash::<Blake2b256>(black_box(data))));
        });
    }
    group.finish();

    let mut group = c.benchmark_group("merkle_inner_hash");
    let data = [42; 64];
    assert_eq!(
        inner_hash::<Reference>(&data),
        inner_hash::<Blake2b256>(&data)
    );
    group.throughput(Throughput::Bytes(65));
    group.bench_function("rustcrypto", |b| {
        b.iter(|| black_box(inner_hash::<Reference>(black_box(&data))))
    });
    group.bench_function("production", |b| {
        b.iter(|| black_box(inner_hash::<Blake2b256>(black_box(&data))))
    });
    group.finish();
}

fn merkle_and_sliver_roots(c: &mut Criterion) {
    // 100 shards matches the Oregon experiment; 1000 covers a larger committee. Data and
    // encoding are prepared outside the timed Merkle loop. Both backends build the same tree.
    for shards in [100, 1000] {
        let config = ReedSolomonEncodingConfig::new(NonZeroU16::new(shards).unwrap());
        let blob: Vec<u8> = (0..=255).cycle().take(5 * 1024 * 1024).collect();
        let (pairs, _) = config.encode_with_metadata(blob).unwrap();
        let config = config.into();
        let symbols = pairs[0].primary.recovery_symbols(&config).unwrap();
        assert_eq!(
            MerkleTree::<Reference>::build(symbols.to_symbols()).root(),
            MerkleTree::<Blake2b256>::build(symbols.to_symbols()).root(),
        );
        let mut group = c.benchmark_group(format!("merkle_root_5MiB_{shards}_shards"));
        group.bench_function("rustcrypto", |b| {
            b.iter(|| {
                black_box(MerkleTree::<Reference>::build(black_box(&symbols).to_symbols()).root())
            })
        });
        group.bench_function("production", |b| {
            b.iter(|| {
                black_box(MerkleTree::<Blake2b256>::build(black_box(&symbols).to_symbols()).root())
            })
        });
        group.finish();

        // Include recovery-symbol expansion to measure how much the hasher changes the work
        // performed by sliver verification, rather than only timing the hash compression loop.
        sliver_root(
            c,
            &format!("sliver_root_primary_5MiB_{shards}_shards"),
            &pairs[0].primary,
            &config,
        );
        sliver_root(
            c,
            &format!("sliver_root_secondary_5MiB_{shards}_shards"),
            &pairs[0].secondary,
            &config,
        );
    }
}

fn sliver_root<E: EncodingAxis>(
    c: &mut Criterion,
    name: &str,
    sliver: &SliverData<E>,
    config: &EncodingConfigEnum,
) {
    assert_eq!(
        sliver.get_merkle_root::<Reference>(config).unwrap(),
        sliver.get_merkle_root::<Blake2b256>(config).unwrap()
    );
    let mut group = c.benchmark_group(name);
    group.bench_function("rustcrypto", |b| {
        b.iter(|| {
            black_box(
                black_box(sliver)
                    .get_merkle_root::<Reference>(black_box(config))
                    .unwrap(),
            )
        })
    });
    group.bench_function("production", |b| {
        b.iter(|| {
            black_box(
                black_box(sliver)
                    .get_merkle_root::<Blake2b256>(black_box(config))
                    .unwrap(),
            )
        })
    });
    group.finish();
}

fn main() {
    eprintln!("architecture: {}", std::env::consts::ARCH);
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    eprintln!(
        "AVX2: {}, SSE4.1: {}",
        std::is_x86_feature_detected!("avx2"),
        std::is_x86_feature_detected!("sse4.1")
    );
    let mut criterion = Criterion::default()
        .sample_size(50)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
        .configure_from_args();
    hash_inputs(&mut criterion);
    merkle_and_sliver_roots(&mut criterion);
    criterion.final_summary();
}
