// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Benchmarks for the basic encoding and decoding.

use core::time::Duration;

use criterion::{AxisScale, BatchSize, BenchmarkId, Criterion, PlotConfiguration};
use raptorq::SourceBlockEncodingPlan;
use walrus_core::encoding::{
    Decoder as _,
    DecodingSymbol,
    Primary,
    RaptorQDecoder,
    RaptorQEncoder,
};
use walrus_test_utils::{random_data, random_subset};

// TODO (WAL-610): Support both encoding types.

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
        let encoding_plan = SourceBlockEncodingPlan::generate(symbol_count);

        for symbol_size in SYMBOL_SIZES {
            let data_length = usize::from(symbol_size) * usize::from(symbol_count);
            let data = random_data(data_length);
            group.throughput(criterion::Throughput::Bytes(
                u64::try_from(data_length).unwrap(),
            ));

            group.bench_with_input(
                BenchmarkId::from_parameter(format!(
                    "symbol_count={},symbol_size={}",
                    symbol_count, symbol_size
                )),
                &(symbol_count, data),
                |b, (symbol_count, data)| {
                    b.iter(|| {
                        let encoder = RaptorQEncoder::new(
                            data,
                            (*symbol_count).try_into().unwrap(),
                            N_SHARDS.try_into().unwrap(),
                            &encoding_plan,
                        )
                        .unwrap();
                        let _encoded_symbols = encoder.encode_all().collect::<Vec<_>>();
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
        let encoding_plan = SourceBlockEncodingPlan::generate(symbol_count);
        for symbol_size in SYMBOL_SIZES {
            let data_length = usize::from(symbol_size) * usize::from(symbol_count);
            let data = random_data(data_length);
            group.throughput(criterion::Throughput::Bytes(
                u64::try_from(data_length).unwrap(),
            ));
            let encoder = RaptorQEncoder::new(
                &data,
                symbol_count.try_into().unwrap(),
                N_SHARDS.try_into().unwrap(),
                &encoding_plan,
            )
            .unwrap();
            let symbols: Vec<_> = random_subset(
                encoder
                    .encode_all()
                    .enumerate()
                    .map(|(i, s)| DecodingSymbol::<Primary>::new(i as u16, s)),
                usize::from(symbol_count) + 1,
            )
            .collect();
            group.bench_with_input(
                BenchmarkId::from_parameter(format!(
                    "symbol_count={},symbol_size={}",
                    symbol_count, symbol_size
                )),
                &(symbol_count, symbol_size, symbols),
                |b, (symbol_count, symbol_size, symbols)| {
                    b.iter_batched(
                        || symbols.clone(),
                        |symbols| {
                            let mut decoder = RaptorQDecoder::new(
                                (*symbol_count).try_into().unwrap(),
                                N_SHARDS.try_into().unwrap(),
                                (*symbol_size).try_into().unwrap(),
                            );
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

fn main() {
    let mut criterion = Criterion::default()
        .configure_from_args()
        .sample_size(50) // reduce sample size to limit execution time
        .warm_up_time(Duration::from_millis(500)); // reduce warm up

    basic_encoding(&mut criterion);
    basic_decoding(&mut criterion);

    criterion.final_summary();
}
