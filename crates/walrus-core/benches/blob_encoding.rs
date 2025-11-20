// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// Allowing `unwrap`s in benchmarks.
#![allow(clippy::unwrap_used)]

//! Benchmarks for the blob encoding and decoding with and without authentication.

use core::{num::NonZeroU16, time::Duration};

use criterion::{AxisScale, BatchSize, BenchmarkId, Criterion, PlotConfiguration};
use walrus_core::encoding::{
    ConsistencyCheckType, EncodingFactory as _, ReedSolomonEncodingConfig,
};
use walrus_test_utils::{random_data, random_subset};

const N_SHARDS: u16 = 1000;

// The maximum symbol size is `u16::MAX`, which means a maximum blob size of ~13 GiB.
// The blob size does not have to be a multiple of the number of symbols as we pad with 0s.
const BLOB_SIZES: &[(u64, &str)] = &[
    (1, "1B"),
    (1 << 10, "1KiB"),
    (1 << 20, "1MiB"),
    (1 << 25, "32MiB"),
    (1 << 30, "1GiB"),
];

fn encoding_config() -> ReedSolomonEncodingConfig {
    ReedSolomonEncodingConfig::new(NonZeroU16::new(N_SHARDS).unwrap())
}

fn blob_encoding(c: &mut Criterion) {
    let config = encoding_config();
    let mut group = c.benchmark_group("blob_encoding");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));

    for (blob_size, size_str) in BLOB_SIZES {
        let blob = random_data((*blob_size).try_into().unwrap());
        group.throughput(criterion::Throughput::Bytes(*blob_size));

        group.bench_with_input(
            BenchmarkId::new("compute_metadata", size_str),
            &(blob),
            |b, blob| {
                b.iter(|| {
                    let _metadata = config.compute_metadata(blob).unwrap();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("encode_with_metadata", size_str),
            &(blob),
            |b, blob| {
                b.iter(|| {
                    let encoder = config.get_blob_encoder(blob).unwrap();
                    let _sliver_pairs = encoder.encode_with_metadata();
                });
            },
        );

        #[allow(deprecated)]
        group.bench_with_input(
            BenchmarkId::new("encode_with_metadata_legacy", size_str),
            &(blob),
            |b, blob| {
                b.iter(|| {
                    let encoder = config.get_blob_encoder(blob).unwrap();
                    let (_sliver_pairs, _metadata) = encoder.encode_with_metadata_legacy();
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
        let blob = random_data((*blob_size).try_into().unwrap());
        group.throughput(criterion::Throughput::Bytes(*blob_size));
        let (sliver_pairs, metadata) = config.encode_with_metadata(blob.clone()).unwrap();
        let primary_slivers_for_decoding: Vec<_> = random_subset(
            sliver_pairs.into_iter().map(|p| p.primary),
            config.n_primary_source_symbols().get().into(),
        )
        .collect();

        for consistency_check in [
            ConsistencyCheckType::Skip,
            ConsistencyCheckType::Default,
            ConsistencyCheckType::Strict,
        ] {
            group.bench_with_input(
                BenchmarkId::new(format!("decode_and_verify_{consistency_check}"), size_str),
                &(&metadata, primary_slivers_for_decoding.clone()),
                |b, (metadata, slivers)| {
                    b.iter_batched(
                        || slivers.clone(),
                        |slivers| {
                            let decoded_blob = config
                                .decode_and_verify(metadata, slivers, consistency_check)
                                .unwrap();
                            assert_eq!(blob.len(), decoded_blob.len());
                            assert_eq!(blob, decoded_blob);
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
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
