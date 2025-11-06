// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
#![allow(missing_docs)]
use std::{
    fs::OpenOptions,
    io::Write,
    num::NonZero,
    str::FromStr,
    sync::{Arc, Mutex},
};

use divan::{Bencher, counter::BytesCount};
use memmap2::MmapOptions;
use rand::{RngCore, SeedableRng, rngs::SmallRng, seq::SliceRandom as _};
use tempfile::NamedTempFile;
use walrus_core::encoding::{
    EncodingConfigEnum,
    ReedSolomonEncodingConfig,
    disk_encoder::{
        BlobExpansionParameters,
        Region,
        symbol_file::stream_order::StreamOrderSymbolFile,
        test_blob::ExpandedTestBlob,
    },
};

const SHARDS: NonZero<u16> = NonZero::new(1000).expect("1000 is non-zero");
const FILE_SIZES: &[&str] = &["50 MiB", "100 MiB", "500 MiB", "1 GiB", "4 GiB", "12 GiB"];

fn thread_counts() -> Vec<usize> {
    vec![1, /*max parallelism*/ 0]
}

fn parse_iec_bytes(size: &str) -> usize {
    let (amount, unit) = size.split_once(' ').expect("valid input");
    let factor = match unit {
        "KiB" => 1 << 10,
        "MiB" => 1 << 20,
        "GiB" => 1 << 30,
        _ => panic!("expect valid input"),
    };

    usize::from_str(amount).expect("valid input") * factor
}

mod write {
    use super::*;

    #[divan::bench(args = FILE_SIZES, sample_size=1, sample_count=2)]
    fn stream_order(bencher: Bencher, file_size: &str) {
        let file_size = parse_iec_bytes(file_size);
        let params = BlobExpansionParameters::new(
            file_size,
            EncodingConfigEnum::ReedSolomon(ReedSolomonEncodingConfig::new(SHARDS)),
        )
        .expect("params are valid");

        let test_blob = ExpandedTestBlob::new(params);

        bencher
            .with_inputs(|| NamedTempFile::new().expect("able to create tempfile"))
            .counter(BytesCount::new(file_size))
            .bench_local_values(|named_file| {
                let mut symbol_file = StreamOrderSymbolFile::create(named_file.path(), params)
                    .expect("file creation must succeed");

                for data in test_blob.iter_row_data(Region::Secondary) {
                    symbol_file
                        .write_expanded_source_primary_sliver_data(&data)
                        .expect("writing source sliver must succeed");
                }
                for data in test_blob.iter_column_data(Region::SourceColumnExpansion) {
                    symbol_file
                        .write_secondary_sliver_expansion(&data)
                        .expect("writing expansion column must succeed");
                }
            });
    }
}

#[divan::bench_group(threads = thread_counts(), sample_count = 20, sample_size = 30)]
mod read_primary_sliver {

    use rand::seq::SliceRandom;

    use super::*;

    #[divan::bench(args = FILE_SIZES, sample_count=20, sample_size=30)]
    fn stream_order_seek(bencher: Bencher, file_size: &str) {
        let file_size = parse_iec_bytes(file_size);
        let params = BlobExpansionParameters::new(
            file_size,
            EncodingConfigEnum::ReedSolomon(ReedSolomonEncodingConfig::new(SHARDS)),
        )
        .expect("params are valid");

        let named_file = NamedTempFile::new().expect("able to create tempfile");
        let test_blob = ExpandedTestBlob::new(params);
        let mut symbol_file = StreamOrderSymbolFile::create(named_file.path(), params)
            .expect("file creation must succeed");

        for data in test_blob.iter_row_data(Region::Secondary) {
            symbol_file
                .write_expanded_source_primary_sliver_data(&data)
                .expect("writing source sliver must succeed");
        }
        for data in test_blob.iter_column_data(Region::SourceColumnExpansion) {
            symbol_file
                .write_secondary_sliver_expansion(&data)
                .expect("writing expansion column must succeed");
        }

        let mut rng = SmallRng::seed_from_u64(1);
        let mut indices: Vec<_> = (0..params.n_shards).rev().collect();
        indices.shuffle(&mut rng);

        let symbol_file = Arc::new(Mutex::new(symbol_file));
        let indices = Arc::new(Mutex::new(indices));

        bencher
            .counter(BytesCount::new(file_size))
            .with_inputs(|| {
                (
                    indices.lock().unwrap().pop().expect("not run 1000 times"),
                    vec![0u8; params.row_bytes(Region::Primary)],
                )
            })
            .bench_values(|(index, mut buffer)| {
                let mut symbol_file = symbol_file.lock().expect("not poisoned");
                symbol_file
                    .read_primary_sliver(index, &mut buffer)
                    .expect("read succeeds");
            });
    }

    #[divan::bench(args = FILE_SIZES, sample_count=20, sample_size=30)]
    fn stream_order_mmap(bencher: Bencher, file_size: &str) {
        let file_size = parse_iec_bytes(file_size);
        let params = BlobExpansionParameters::new(
            file_size,
            EncodingConfigEnum::ReedSolomon(ReedSolomonEncodingConfig::new(SHARDS)),
        )
        .expect("params are valid");

        let named_file = NamedTempFile::new().expect("able to create tempfile");
        let test_blob = ExpandedTestBlob::new(params);
        let mut symbol_file = StreamOrderSymbolFile::create(named_file.path(), params)
            .expect("file creation must succeed");

        for data in test_blob.iter_row_data(Region::Secondary) {
            symbol_file
                .write_expanded_source_primary_sliver_data(&data)
                .expect("writing source sliver must succeed");
        }
        for data in test_blob.iter_column_data(Region::SourceColumnExpansion) {
            symbol_file
                .write_secondary_sliver_expansion(&data)
                .expect("writing expansion column must succeed");
        }

        symbol_file.switch_to_mmap_reads().expect("switch succeeds");

        let mut rng = SmallRng::seed_from_u64(1);
        let mut indices: Vec<_> = (0..params.n_shards).rev().collect();
        indices.shuffle(&mut rng);

        let indices = Arc::new(Mutex::new(indices));

        bencher
            .counter(BytesCount::new(file_size))
            .with_inputs(|| {
                (
                    indices.lock().unwrap().pop().expect("not run 1000 times"),
                    vec![0u8; params.row_bytes(Region::Primary)],
                )
            })
            .bench_values(|(index, mut buffer)| {
                symbol_file
                    .read_primary_sliver_using_mmap(index, &mut buffer)
                    .expect("read succeeds");
            });
    }
}

#[divan::bench_group(threads = thread_counts(), sample_count = 20, sample_size = 30)]
mod read_secondary_sliver {
    use super::*;

    #[divan::bench(args = FILE_SIZES)]
    fn stream_order_seek(bencher: Bencher, file_size: &str) {
        let file_size = parse_iec_bytes(file_size);
        let params = BlobExpansionParameters::new(
            file_size,
            EncodingConfigEnum::ReedSolomon(ReedSolomonEncodingConfig::new(SHARDS)),
        )
        .expect("params are valid");

        let named_file = NamedTempFile::new().expect("able to create tempfile");
        let test_blob = ExpandedTestBlob::new(params);
        let mut symbol_file = StreamOrderSymbolFile::create(named_file.path(), params)
            .expect("file creation must succeed");

        for data in test_blob.iter_row_data(Region::Secondary) {
            symbol_file
                .write_expanded_source_primary_sliver_data(&data)
                .expect("writing source sliver must succeed");
        }
        for data in test_blob.iter_column_data(Region::SourceColumnExpansion) {
            symbol_file
                .write_secondary_sliver_expansion(&data)
                .expect("writing expansion column must succeed");
        }

        let mut rng = SmallRng::seed_from_u64(2);
        let mut indices: Vec<_> = (0..params.n_shards).rev().collect();
        indices.shuffle(&mut rng);

        let symbol_file = Arc::new(Mutex::new(symbol_file));
        let indices = Arc::new(Mutex::new(indices));

        bencher
            .counter(BytesCount::new(file_size))
            .with_inputs(|| {
                (
                    indices.lock().unwrap().pop().expect("not run 1000 times"),
                    vec![0u8; params.column_bytes(Region::Secondary)],
                )
            })
            .bench_values(|(index, mut buffer)| {
                let mut symbol_file = symbol_file.lock().expect("not poisoned");
                symbol_file
                    .read_secondary_sliver(index, &mut buffer)
                    .expect("read succeeds");
            });
    }

    #[divan::bench(args = FILE_SIZES)]
    fn stream_order_mmap(bencher: Bencher, file_size: &str) {
        let file_size = parse_iec_bytes(file_size);
        let params = BlobExpansionParameters::new(
            file_size,
            EncodingConfigEnum::ReedSolomon(ReedSolomonEncodingConfig::new(SHARDS)),
        )
        .expect("params are valid");

        let named_file = NamedTempFile::new().expect("able to create tempfile");
        let test_blob = ExpandedTestBlob::new(params);
        let mut symbol_file = StreamOrderSymbolFile::create(named_file.path(), params)
            .expect("file creation must succeed");

        for data in test_blob.iter_row_data(Region::Secondary) {
            symbol_file
                .write_expanded_source_primary_sliver_data(&data)
                .expect("writing source sliver must succeed");
        }
        for data in test_blob.iter_column_data(Region::SourceColumnExpansion) {
            symbol_file
                .write_secondary_sliver_expansion(&data)
                .expect("writing expansion column must succeed");
        }

        symbol_file.switch_to_mmap_reads().expect("switch succeeds");

        let mut rng = SmallRng::seed_from_u64(2);
        let mut indices: Vec<_> = (0..params.n_shards).rev().collect();
        indices.shuffle(&mut rng);

        let indices = Arc::new(Mutex::new(indices));

        bencher
            .counter(BytesCount::new(file_size))
            .with_inputs(|| {
                (
                    indices.lock().unwrap().pop().expect("not run 1000 times"),
                    vec![0u8; params.column_bytes(Region::Secondary)],
                )
            })
            .bench_values(|(index, mut buffer)| {
                symbol_file
                    .read_secondary_sliver_using_mmap(index, &mut buffer)
                    .expect("read succeeds");
            });
    }
}

#[divan::bench_group(sample_size = 2, sample_count = 2)]
mod direct_writes {
    use rand::thread_rng;

    use super::*;

    #[divan::bench]
    fn write_lots_of_data_mmap(bencher: Bencher) {
        let mut rng = SmallRng::seed_from_u64(thread_rng().next_u64());

        bencher
            .with_inputs(|| {
                let mut named_file = NamedTempFile::new().expect("able to create tempfile");
                // This is because we mmap and want it to be the full length.
                named_file.as_file_mut().set_len(50 << 30).unwrap();

                let mmap = unsafe {
                    MmapOptions::new()
                        .map_mut(named_file.as_file())
                        .expect("hopefully succeeds")
                };

                (mmap, named_file)
            })
            .bench_local_values(|(mut mmap_mut, _guard)| {
                let mut buffer = vec![0u8; 8 << 20];
                let mut remaining: usize = 50 << 30;
                let mut offset = 0;

                while remaining > 0 {
                    rng.fill_bytes(&mut buffer);
                    let write_amount = buffer.len().min(remaining);

                    mmap_mut[offset..offset + write_amount]
                        .copy_from_slice(&buffer[..write_amount]);
                    remaining -= write_amount;
                    offset += write_amount;
                }
            });
    }

    #[divan::bench]
    fn write_lots_of_data_write(bencher: Bencher) {
        let mut rng = SmallRng::seed_from_u64(thread_rng().next_u64());

        bencher
            .with_inputs(|| NamedTempFile::new().expect("able to create tempfile"))
            .bench_local_values(|mut file| {
                let mut buffer = vec![0u8; 8 << 20];
                let mut remaining: usize = 50 << 30;
                while remaining > 0 {
                    rng.fill_bytes(&mut buffer);
                    let write_amount = buffer.len().min(remaining);

                    file.write_all(&buffer[..write_amount])
                        .expect("shold succeed");
                    remaining -= write_amount;
                }
            });
    }
}

fn main() {
    divan::main();
}
