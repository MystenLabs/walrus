// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(missing_docs)]

//! Benchmarks for the basic encoding and decoding.

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use raptorq::SourceBlockEncodingPlan;
use walrus_core::encoding::{Decoder, DecodingSymbol, Encoder, Primary};
use walrus_test_utils::{random_data, random_subset};

const N_SHARDS: u32 = 1000;
// Likely values for the number of source symbols for the primary and secondary encoding.
// These values are consistent with BFT and are supported source-block sizes that do not require
// padding, see https://datatracker.ietf.org/doc/html/rfc6330#section-5.6.
const SYMBOL_COUNTS: [u16; 2] = [324, 648];
// Can be at most `u16::MAX`.
const SYMBOL_SIZES: [u16; 5] = [1, 16, 256, 4096, u16::MAX];

fn basic_encoding(c: &mut Criterion) {
    let mut group = c.benchmark_group("basic_encoding");

    for symbol_count in SYMBOL_COUNTS {
        let encoding_plan = SourceBlockEncodingPlan::generate(symbol_count);

        for symbol_size in SYMBOL_SIZES {
            let data_length = symbol_size as usize * symbol_count as usize;
            let data = random_data(data_length);
            group.throughput(criterion::Throughput::Bytes(data_length as u64));

            group.bench_with_input(
                BenchmarkId::from_parameter({
                    let res = format!("symbol_count={},symbol_size={}", symbol_count, symbol_size);
                    res
                }),
                &(symbol_count, data),
                |b, (symbol_count, data)| {
                    b.iter(|| {
                        let encoder =
                            Encoder::new(&data, *symbol_count, N_SHARDS, &encoding_plan).unwrap();
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
    for symbol_count in SYMBOL_COUNTS {
        let encoding_plan = SourceBlockEncodingPlan::generate(symbol_count);
        for symbol_size in SYMBOL_SIZES {
            let data_length = symbol_size as usize * symbol_count as usize;
            let data = random_data(data_length);
            group.throughput(criterion::Throughput::Bytes(data_length as u64));
            let encoder = Encoder::new(&data, symbol_count, N_SHARDS, &encoding_plan).unwrap();
            let symbols: Vec<_> = random_subset(
                encoder
                    .encode_all()
                    .enumerate()
                    .map(|(i, s)| DecodingSymbol::<Primary>::new(i as u32, s.into())),
                symbol_count as usize + 1,
            )
            .collect();
            group.bench_with_input(
                BenchmarkId::from_parameter({
                    let res = format!("symbol_count={},symbol_size={}", symbol_count, symbol_size);
                    res
                }),
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

criterion_group!(benches, basic_encoding, basic_decoding);
criterion_main!(benches);
