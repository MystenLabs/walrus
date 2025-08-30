// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// Allowing `unwrap`s in benchmarks.
#![allow(clippy::unwrap_used)]

//! Benchmarks for the basic encoding and decoding.

use core::time::Duration;

use criterion::{AxisScale, BatchSize, BenchmarkId, Criterion, PlotConfiguration};
use fastcrypto::hash::Blake2b256;
use walrus_core::{
    encoding::{Decoder as _, DecodingSymbol, Primary, ReedSolomonDecoder, ReedSolomonEncoder},
    merkle::MerkleTree,
};
use walrus_test_utils::{random_data, random_subset};

const N_SHARDS: u16 = 1000;
// Likely values for the number of source symbols for the primary and secondary encoding, which are
// consistent with BFT.
const SOURCE_SYMBOLS_PRIMARY: u16 = 329;
const SOURCE_SYMBOLS_SECONDARY: u16 = 662;

const SYMBOL_COUNTS: [u16; 2] = [SOURCE_SYMBOLS_PRIMARY, SOURCE_SYMBOLS_SECONDARY];
// Can be at most `u16::MAX`. Using multiples of 2 to be compatible with Reed-Solomon encoding.
const SYMBOL_SIZES: [u16; 5] = [2, 16, 256, 4096, u16::MAX - 1];

fn basic_encoding(c: &mut Criterion) {
    let mut group = c.benchmark_group("basic_encoding");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));

    for symbol_count in SYMBOL_COUNTS {
        for symbol_size in SYMBOL_SIZES {
            let data_length = usize::from(symbol_size) * usize::from(symbol_count);
            let data = random_data(data_length);
            group.throughput(criterion::Throughput::Bytes(
                u64::try_from(data_length).unwrap(),
            ));

            group.bench_with_input(
                BenchmarkId::from_parameter(format!(
                    "symbol_count={symbol_count},symbol_size={symbol_size}"
                )),
                &(symbol_count, data),
                |b, (symbol_count, data)| {
                    b.iter(|| {
                        let encoder = ReedSolomonEncoder::new(
                            data,
                            (*symbol_count).try_into().unwrap(),
                            N_SHARDS.try_into().unwrap(),
                        )
                        .unwrap();
                        let _encoded_symbols = encoder.encode_all();
                    });
                },
            );
        }
    }

    group.finish();
}

fn basic_decoding(c: &mut Criterion) {
    let mut group = c.benchmark_group("basic_decoding");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));

    for symbol_count in SYMBOL_COUNTS {
        for symbol_size in SYMBOL_SIZES {
            let data_length = usize::from(symbol_size) * usize::from(symbol_count);
            let data = random_data(data_length);
            group.throughput(criterion::Throughput::Bytes(
                u64::try_from(data_length).unwrap(),
            ));
            let encoder = ReedSolomonEncoder::new(
                &data,
                symbol_count.try_into().unwrap(),
                N_SHARDS.try_into().unwrap(),
            )
            .unwrap();
            let symbols: Vec<_> = random_subset(
                encoder
                    .encode_all()
                    .into_iter()
                    .enumerate()
                    .map(|(i, s)| DecodingSymbol::<Primary>::new(i.try_into().unwrap(), s)),
                usize::from(symbol_count) + 1,
            )
            .collect();
            group.bench_with_input(
                BenchmarkId::from_parameter(format!(
                    "symbol_count={symbol_count},symbol_size={symbol_size}"
                )),
                &(symbol_count, symbol_size, symbols),
                |b, (symbol_count, symbol_size, symbols)| {
                    b.iter_batched(
                        || symbols.clone(),
                        |symbols| {
                            let mut decoder = ReedSolomonDecoder::new(
                                (*symbol_count).try_into().unwrap(),
                                N_SHARDS.try_into().unwrap(),
                                (*symbol_size).try_into().unwrap(),
                            )
                            .unwrap();
                            let decoded_data = &decoder.decode(symbols).unwrap();
                            assert_eq!(data.len(), decoded_data.len());
                            assert_eq!(&data, decoded_data);
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }

    group.finish();
}

fn flatten_faster(input: Vec<Vec<u8>>) -> Vec<u8> {
    assert!(!input.is_empty(), "input must not be empty");
    assert!(!input[0].is_empty(), "data must not be empty");
    let symbol_size = input[0].len();

    let mut output = vec![0; input.len() * symbol_size];
    for (src, dst) in input.iter().zip(output.chunks_exact_mut(symbol_size)) {
        dst.copy_from_slice(src);
    }
    output
}

fn flatten_symbols(c: &mut Criterion) {
    let mut group = c.benchmark_group("flatten");

    for symbol_count in SYMBOL_COUNTS {
        for symbol_size in SYMBOL_SIZES {
            let data_length = usize::from(symbol_size) * usize::from(symbol_count);
            let input: Vec<_> = (0..symbol_count)
                .map(|_| random_data(symbol_size.into()))
                .collect();

            group.throughput(criterion::Throughput::Bytes(
                u64::try_from(data_length).unwrap(),
            ));

            group.bench_with_input(
                BenchmarkId::new(
                    "original",
                    format!("symbol_count={symbol_count},symbol_size={symbol_size}"),
                ),
                &input,
                |b, input| {
                    b.iter_batched(
                        || input.clone(),
                        |cloned_input| {
                            let _flattened: Vec<_> = cloned_input.into_iter().flatten().collect();
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
            group.bench_with_input(
                BenchmarkId::new(
                    "memcpy",
                    format!("symbol_count={symbol_count},symbol_size={symbol_size}"),
                ),
                &input,
                |b, input| {
                    b.iter_batched(
                        || input.clone(),
                        |cloned_input| {
                            let _flattened = flatten_faster(cloned_input);
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }

    group.finish();
}

fn merkle_tree(c: &mut Criterion) {
    let mut group = c.benchmark_group("merkle_tree");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));

    for symbol_count in SYMBOL_COUNTS {
        for symbol_size in SYMBOL_SIZES {
            let data_length = usize::from(symbol_size) * usize::from(symbol_count);
            let data = random_data(data_length);
            let encoded_symbols = ReedSolomonEncoder::new(
                &data,
                symbol_count.try_into().unwrap(),
                N_SHARDS.try_into().unwrap(),
            )
            .unwrap()
            .encode_all();

            group.throughput(criterion::Throughput::Bytes(
                u64::try_from(data_length).unwrap(),
            ));

            group.bench_with_input(
                BenchmarkId::from_parameter(format!(
                    "symbol_count={symbol_count},symbol_size={symbol_size}"
                )),
                &encoded_symbols,
                |b, encoded_symbols| {
                    b.iter(|| {
                        let _tree = MerkleTree::<Blake2b256>::build(encoded_symbols);
                    });
                },
            );
        }
    }

    group.finish();
}

fn main() {
    let mut criterion = Criterion::default()
        .configure_from_args()
        .measurement_time(Duration::from_secs(10))
        .sample_size(50) // reduce sample size to limit execution time
        .warm_up_time(Duration::from_millis(500)); // reduce warm up

    basic_encoding(&mut criterion);
    basic_decoding(&mut criterion);
    flatten_symbols(&mut criterion);
    merkle_tree(&mut criterion);

    criterion.final_summary();
}
