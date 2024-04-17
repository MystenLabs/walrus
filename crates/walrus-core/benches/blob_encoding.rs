// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Benchmarks for the blob encoding and decoding with and without authentication.

use std::{num::NonZeroU16, time::Duration};

use criterion::{AxisScale, BatchSize, BenchmarkId, Criterion, PlotConfiguration};
use walrus_core::encoding::{EncodingConfig, Primary};
use walrus_test_utils::{random_data, random_subset};

const N_SHARDS: u16 = 1000;

// The maximum symbol size is `u16::MAX`, which means a maximum blob size of ~13 GiB.
// The blob size does not have to be a multiple of the number of symbols as we pad with 0s.
const BLOB_SIZES: [(usize, &str); 6] = [
    (1, "1B"),
    (1 << 10, "1KiB"),
    (1 << 20, "1MiB"),
    (1 << 24, "16MiB"),
    (1 << 28, "256MiB"),
    (1 << 30, "1GiB"),
];

fn encoding_config() -> EncodingConfig {
    EncodingConfig::new(NonZeroU16::new(N_SHARDS).unwrap())
}

fn blob_encoding(c: &mut Criterion) {
    let config = encoding_config();
    let mut group = c.benchmark_group("blob_encoding");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));

    for (blob_size, size_str) in BLOB_SIZES {
        let blob = random_data(blob_size);
        group.throughput(criterion::Throughput::Bytes(blob_size as u64));

        group.bench_with_input(BenchmarkId::new("encode", size_str), &(blob), |b, blob| {
            b.iter(|| {
                let encoder = config.get_blob_encoder(blob).unwrap();
                let _sliver_pairs = encoder.encode();
            });
        });

        group.bench_with_input(
            BenchmarkId::new("encode_with_metadata", size_str),
            &(blob),
            |b, blob| {
                b.iter(|| {
                    let encoder = config.get_blob_encoder(blob).unwrap();
                    let (_sliver_pairs, _metadata) = encoder.encode_with_metadata();
                });
            },
        );
    }

    group.finish();
}

fn blob_decoding(c: &mut Criterion) {
    let config = encoding_config();
    let mut group = c.benchmark_group("blob_decoding");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));

    for (blob_size, size_str) in BLOB_SIZES {
        let blob = random_data(blob_size);
        group.throughput(criterion::Throughput::Bytes(blob_size as u64));
        let encoder = config.get_blob_encoder(&blob).unwrap();
        let (sliver_pairs, metadata) = encoder.encode_with_metadata();
        let primary_slivers_for_decoding: Vec<_> = random_subset(
            sliver_pairs.into_iter().map(|p| p.primary),
            config.n_primary_source_symbols().get().into(),
        )
        .collect();
        let blob_id = metadata.blob_id();

        group.bench_with_input(
            BenchmarkId::new("decode", size_str),
            &(blob_size, primary_slivers_for_decoding.clone()),
            |b, (blob_size, slivers)| {
                b.iter_batched(
                    || slivers.clone(),
                    |slivers| {
                        let mut decoder = config.get_blob_decoder::<Primary>(*blob_size).unwrap();
                        let _blob = decoder.decode(slivers).unwrap();
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("decode_and_verify", size_str),
            &(blob_size, *blob_id, primary_slivers_for_decoding),
            |b, (blob_size, blob_id, slivers)| {
                b.iter_batched(
                    || slivers.clone(),
                    |slivers| {
                        let mut decoder = config.get_blob_decoder::<Primary>(*blob_size).unwrap();
                        let _blob = decoder
                            .decode_and_verify(blob_id, slivers)
                            .unwrap()
                            .unwrap();
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn main() {
    let mut criterion = Criterion::default()
        .configure_from_args()
        .sample_size(10) // set sample size to the minimum to limit execution time
        .warm_up_time(Duration::from_millis(10)); // warm up doesn't make much sense in this case

    blob_encoding(&mut criterion);
    blob_decoding(&mut criterion);

    criterion.final_summary();
}
