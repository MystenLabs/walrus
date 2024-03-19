// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Benchmarks for the basic encoding and decoding.

use std::time::Duration;

use criterion::{AxisScale, BatchSize, BenchmarkId, Criterion, PlotConfiguration};
use raptorq::SourceBlockEncodingPlan;
use walrus_core::encoding::{Decoder, DecodingSymbol, Encoder, Primary};
use walrus_test_utils::{random_data, random_subset};

mod constants;

const SYMBOL_COUNTS: [u16; 2] = [
    constants::SOURCE_SYMBOLS_PRIMARY,
    constants::SOURCE_SYMBOLS_SECONDARY,
];
// Can be at most `u16::MAX`.
const SYMBOL_SIZES: [u16; 5] = [1, 16, 256, 4096, u16::MAX];

fn basic_encoding(c: &mut Criterion) {
    let mut group = c.benchmark_group("basic_encoding");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));

    for symbol_count in SYMBOL_COUNTS {
        let encoding_plan = SourceBlockEncodingPlan::generate(symbol_count);

        for symbol_size in SYMBOL_SIZES {
            let data_length = symbol_size as usize * symbol_count as usize;
            let data = random_data(data_length);
            group.throughput(criterion::Throughput::Bytes(data_length as u64));

            group.bench_with_input(
                BenchmarkId::from_parameter(format!(
                    "symbol_count={},symbol_size={}",
                    symbol_count, symbol_size
                )),
                &(symbol_count, data),
                |b, (symbol_count, data)| {
                    b.iter(|| {
                        let encoder =
                            Encoder::new(&data, *symbol_count, constants::N_SHARDS, &encoding_plan)
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
            let data_length = symbol_size as usize * symbol_count as usize;
            let data = random_data(data_length);
            group.throughput(criterion::Throughput::Bytes(data_length as u64));
            let encoder =
                Encoder::new(&data, symbol_count, constants::N_SHARDS, &encoding_plan).unwrap();
            let symbols: Vec<_> = random_subset(
                encoder
                    .encode_all()
                    .enumerate()
                    .map(|(i, s)| DecodingSymbol::<Primary>::new(i as u32, s.into())),
                symbol_count as usize + 1,
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
                            let mut decoder = Decoder::new(*symbol_count, *symbol_size);
                            let _decoded_data = &decoder.decode(symbols).unwrap();
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
