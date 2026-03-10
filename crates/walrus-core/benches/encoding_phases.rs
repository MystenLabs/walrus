// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// Allowing `unwrap`s in benchmarks.
#![allow(clippy::unwrap_used)]

//! Phase-level benchmarks for the blob encoding pipeline at production parameters (n_shards=1000).
//!
//! Complements `blob_encoding.rs` (end-to-end) and `basic_encoding.rs` (individual RS/merkle ops)
//! by measuring each phase of `encode_with_metadata()` independently.

use core::{num::NonZeroU16, time::Duration};

use criterion::{AxisScale, BenchmarkId, Criterion, PlotConfiguration};
use fastcrypto::hash::Blake2b256;
use walrus_core::{
    encoding::{EncodingFactory as _, ReedSolomonEncoder, ReedSolomonEncodingConfig},
    merkle::{MerkleTree, Node, leaf_hash},
};
use walrus_test_utils::random_data;

const N_SHARDS: u16 = 1000;

const BLOB_SIZES: &[(u64, &str)] = &[(1 << 20, "1MiB"), (1 << 25, "32MiB"), (1 << 28, "256MiB")];

fn encoding_config() -> ReedSolomonEncodingConfig {
    ReedSolomonEncodingConfig::new(NonZeroU16::new(N_SHARDS).unwrap())
}

/// Benchmark secondary encoding: RS-encode each row to produce repair secondary slivers.
fn secondary_encoding(c: &mut Criterion) {
    let config = encoding_config();
    let mut group = c.benchmark_group("secondary_encoding");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));

    for (blob_size, size_str) in BLOB_SIZES {
        let blob = random_data((*blob_size).try_into().unwrap());
        group.throughput(criterion::Throughput::Bytes(*blob_size));

        let encoder = config.get_blob_encoder(&blob).unwrap();
        let symbol_size = encoder.symbol_usize();
        let n_rows: usize = config.n_primary_source_symbols().get().into();
        let n_cols: usize = config.n_secondary_source_symbols().get().into();
        let row_len = n_cols * symbol_size;

        // Build row data (same layout as primary slivers' data).
        let mut rows: Vec<Vec<u8>> = Vec::with_capacity(n_rows);
        for r in 0..n_rows {
            let start = r * row_len;
            let end = (start + row_len).min(blob.len());
            let mut row = vec![0u8; row_len];
            if start < blob.len() {
                row[..end - start].copy_from_slice(&blob[start..end]);
            }
            rows.push(row);
        }

        group.bench_with_input(
            BenchmarkId::new("encode_rows", size_str),
            &rows,
            |b, rows| {
                b.iter(|| {
                    let mut enc = ReedSolomonEncoder::new(
                        NonZeroU16::new(symbol_size.try_into().unwrap()).unwrap(),
                        config.n_secondary_source_symbols(),
                        NonZeroU16::new(N_SHARDS).unwrap(),
                    )
                    .unwrap();
                    for row in rows {
                        let _ = enc.encode_all_repair_symbols(row);
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark primary encoding: RS-encode each column to produce all primary symbols.
fn primary_encoding(c: &mut Criterion) {
    let config = encoding_config();
    let mut group = c.benchmark_group("primary_encoding");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));

    for (blob_size, size_str) in BLOB_SIZES {
        let blob = random_data((*blob_size).try_into().unwrap());
        group.throughput(criterion::Throughput::Bytes(*blob_size));

        let encoder = config.get_blob_encoder(&blob).unwrap();
        let symbol_size = encoder.symbol_usize();
        let n_shards: usize = N_SHARDS.into();
        let n_rows: usize = config.n_primary_source_symbols().get().into();

        // Build column data (each column = one secondary sliver's symbols data).
        let col_len = n_rows * symbol_size;
        let columns: Vec<Vec<u8>> = (0..n_shards).map(|_| random_data(col_len)).collect();

        group.bench_with_input(
            BenchmarkId::new("encode_columns", size_str),
            &columns,
            |b, columns| {
                b.iter(|| {
                    let mut enc = ReedSolomonEncoder::new(
                        NonZeroU16::new(symbol_size.try_into().unwrap()).unwrap(),
                        config.n_primary_source_symbols(),
                        NonZeroU16::new(N_SHARDS).unwrap(),
                    )
                    .unwrap();
                    for col in columns {
                        let _ = enc.encode_all_ref(col);
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark primary encoding + leaf hashing of each symbol.
fn primary_encoding_with_hashing(c: &mut Criterion) {
    let config = encoding_config();
    let mut group = c.benchmark_group("primary_encoding_with_hashing");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));

    for (blob_size, size_str) in BLOB_SIZES {
        let blob = random_data((*blob_size).try_into().unwrap());
        group.throughput(criterion::Throughput::Bytes(*blob_size));

        let encoder = config.get_blob_encoder(&blob).unwrap();
        let symbol_size = encoder.symbol_usize();
        let n_shards: usize = N_SHARDS.into();
        let n_rows: usize = config.n_primary_source_symbols().get().into();

        let col_len = n_rows * symbol_size;
        let columns: Vec<Vec<u8>> = (0..n_shards).map(|_| random_data(col_len)).collect();

        group.bench_with_input(
            BenchmarkId::new("encode_columns_and_hash", size_str),
            &columns,
            |b, columns| {
                b.iter(|| {
                    let mut enc = ReedSolomonEncoder::new(
                        NonZeroU16::new(symbol_size.try_into().unwrap()).unwrap(),
                        config.n_primary_source_symbols(),
                        NonZeroU16::new(N_SHARDS).unwrap(),
                    )
                    .unwrap();
                    let mut hashes = vec![Node::Empty; n_shards * n_shards];
                    for (col_index, col) in columns.iter().enumerate() {
                        let symbols = enc.encode_all_ref(col).unwrap();
                        for (row_index, symbol) in symbols.to_symbols().enumerate() {
                            hashes[col_index * n_shards + row_index] =
                                leaf_hash::<Blake2b256>(symbol);
                        }
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark metadata computation from pre-computed symbol hashes (Merkle tree construction).
fn metadata_from_hashes(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata_from_hashes");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));

    for (blob_size, size_str) in BLOB_SIZES {
        group.throughput(criterion::Throughput::Bytes(*blob_size));

        let n_shards: usize = N_SHARDS.into();
        // Pre-compute random hashes to isolate metadata construction time.
        let hashes: Vec<Node> = (0..n_shards * n_shards)
            .map(|i| {
                let data = i.to_le_bytes();
                leaf_hash::<Blake2b256>(&data)
            })
            .collect();

        group.bench_with_input(
            BenchmarkId::new("build_merkle_trees", size_str),
            &hashes,
            |b, hashes| {
                b.iter(|| {
                    // Build 2 * n_shards Merkle trees (primary + secondary per sliver pair).
                    for sliver_index in 0..n_shards {
                        let _primary = MerkleTree::<Blake2b256>::build_from_leaf_hashes(
                            (0..n_shards).map(|col| hashes[col * n_shards + sliver_index].clone()),
                        );
                        let sec_col = n_shards - 1 - sliver_index;
                        let _secondary = MerkleTree::<Blake2b256>::build_from_leaf_hashes(
                            hashes[sec_col * n_shards..(sec_col + 1) * n_shards]
                                .iter()
                                .cloned(),
                        );
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark the full `encode_with_metadata()` pipeline for comparison.
fn full_pipeline(c: &mut Criterion) {
    let config = encoding_config();
    let mut group = c.benchmark_group("full_pipeline");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));

    for (blob_size, size_str) in BLOB_SIZES {
        let blob = random_data((*blob_size).try_into().unwrap());
        group.throughput(criterion::Throughput::Bytes(*blob_size));

        group.bench_with_input(
            BenchmarkId::new("encode_with_metadata", size_str),
            &blob,
            |b, blob| {
                b.iter(|| {
                    let encoder = config.get_blob_encoder(blob).unwrap();
                    let _result = encoder.encode_with_metadata();
                });
            },
        );
    }

    group.finish();
}

fn main() {
    let mut criterion = Criterion::default()
        .configure_from_args()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(10));

    secondary_encoding(&mut criterion);
    primary_encoding(&mut criterion);
    primary_encoding_with_hashing(&mut criterion);
    metadata_from_hashes(&mut criterion);
    full_pipeline(&mut criterion);

    criterion.final_summary();
}
