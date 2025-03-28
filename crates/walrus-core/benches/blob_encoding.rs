// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Benchmarks for the blob encoding and decoding with and without authentication.

use core::{num::NonZeroU16, time::Duration};

use criterion::{AxisScale, BatchSize, BenchmarkId, Criterion, PlotConfiguration};
use walrus_core::encoding::{EncodingConfigTrait as _, Primary, RaptorQEncodingConfig};
use walrus_test_utils::{random_data, random_subset};

// TODO (WAL-610): Support both encoding types.

const N_SHARDS: u16 = 1000;

// The maximum symbol size is `u16::MAX`, which means a maximum blob size of ~13 GiB.
// The blob size does not have to be a multiple of the number of symbols as we pad with 0s.
const BLOB_SIZES: [(u64, &str); 6] = [
    (1, "1B"),
    (1 << 10, "1KiB"),
    (1 << 20, "1MiB"),
    (1 << 24, "16MiB"),
    (1 << 28, "256MiB"),
    (1 << 30, "1GiB"),
];

/// Creates a new encoding config with `N_SHARDS` primary shards.
///
/// # Panics
///
/// Panics if creating a `NonZeroU16` from `N_SHARDS` fails,
/// which is guaranteed not to happen.
fn encoding_config() -> RaptorQEncodingConfig {
    RaptorQEncodingConfig::new(NonZeroU16::new(N_SHARDS).expect("N_SHARDS must be non-zero"))
}

fn blob_encoding(c: &mut Criterion) {
    let config = encoding_config();
    let mut group = c.benchmark_group("blob_encoding");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));

    for (blob_size, size_str) in BLOB_SIZES {
        let blob = random_data(blob_size.try_into().expect("blob size fits in usize"));
        group.throughput(criterion::Throughput::Bytes(blob_size));

        group.bench_with_input(BenchmarkId::new("encode", size_str), &(blob), |b, blob| {
            b.iter(|| {
                let encoder = config
                    .get_blob_encoder(blob)
                    .expect("Failed to create encoder");
                let _sliver_pairs = encoder.encode();
            });
        });

        group.bench_with_input(
            BenchmarkId::new("encode_with_metadata", size_str),
            &(blob),
            |b, blob| {
                b.iter(|| {
                    let encoder = config
                        .get_blob_encoder(blob)
                        .expect("Failed to create encoder with metadata");
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
        let blob = random_data(blob_size.try_into().expect("blob size fits in usize"));
        group.throughput(criterion::Throughput::Bytes(blob_size));
        let encoder = config
            .get_blob_encoder(&blob)
            .expect("Failed to create encoder");
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
                        let mut decoder = config
                            .get_blob_decoder::<Primary>(*blob_size)
                            .expect("Failed to create decoder");
                        let decoded_blob = decoder
                            .decode(slivers)
                            .expect("Failed to decode blob");
                        assert_eq!(blob.len(), decoded_blob.len());
                        assert_eq!(blob, decoded_blob);
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
                        let mut decoder = config
                            .get_blob_decoder::<Primary>(*blob_size)
                            .expect("Failed to create decoder");
                        let (decoded_blob, _metadata) = decoder
                            .decode_and_verify(blob_id, slivers)
                            .expect("Failed to decode and verify blob")
                            .expect("No metadata returned during decode and verify");
                        assert_eq!(blob.len(), decoded_blob.len());
                        assert_eq!(blob, decoded_blob);
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
