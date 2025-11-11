// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
#![allow(missing_docs)]
use std::{
    io::Write,
    num::NonZero,
    str::FromStr,
    sync::{Arc, Mutex},
};

use divan::Bencher;
use memmap2::MmapOptions;
use rand::{RngCore, SeedableRng, rngs::SmallRng, seq::SliceRandom};
use tempfile::NamedTempFile;
use walrus_core::encoding::{
    EncodingConfigEnum,
    ReedSolomonEncodingConfig,
    disk_encoder::{
        BlobExpansionParameters,
        Region,
        symbol_file::{
            stream_order::StreamOrderSymbolFile,
            transposed_order::TransposedOrderSymbolFile,
        },
        test_blob::ExpandedTestBlob,
    },
};

const SHARDS: NonZero<u16> = NonZero::new(1000).expect("1000 is non-zero");
const FILE_SIZES: &[&str] = &["50 MiB", "100 MiB", "500 MiB", "1 GiB", "4 GiB", "12 GiB"];
const EXPANDED_FILE_SIZES: &[&str] = &[
    "35 MiB", "70 MiB", "175 MiB", "350 MiB", "1750 MiB", "4 GiB", "14 GiB", "42 GiB",
];

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

fn init_symbol_file(
    file_size: &str,
) -> (
    NamedTempFile,
    StreamOrderSymbolFile,
    BlobExpansionParameters,
) {
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

    (named_file, symbol_file, params)
}

fn init_transpose_order_symbol_file(
    file_size: &str,
) -> (
    NamedTempFile,
    TransposedOrderSymbolFile,
    BlobExpansionParameters,
) {
    let file_size = parse_iec_bytes(file_size);
    let params = BlobExpansionParameters::new(
        file_size,
        EncodingConfigEnum::ReedSolomon(ReedSolomonEncodingConfig::new(SHARDS)),
    )
    .expect("params are valid");

    let named_file = NamedTempFile::new().expect("able to create tempfile");
    let test_blob = ExpandedTestBlob::new(params);
    let mut symbol_file = TransposedOrderSymbolFile::create(named_file.path(), params, 64 << 20)
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

    (named_file, symbol_file, params)
}

#[divan::bench_group(threads = thread_counts(), sample_count = 20, sample_size = 10)]
mod read_primary_sliver {
    use super::*;

    enum ReadMethod {
        Seek,
        ReadAt,
        Mmap,
    }

    fn stream_order_bench_impl(
        bencher: Bencher,
        file_size: &str,
        method: ReadMethod,
        region: Region,
    ) {
        let (_named_file, mut file, params) = init_symbol_file(file_size);

        let mut indices: Vec<_> = match region {
            Region::Source => (0..params.row_count(region)).collect(),
            Region::SourceColumnExpansion => {
                (params.n_shards - params.row_count(region)..params.n_shards).collect()
            }
            _ => panic!("what are you doing?!?"),
        };
        indices.shuffle(&mut SmallRng::seed_from_u64(1));

        let indices = Arc::new(Mutex::new(indices));
        let common_inputs = || {
            let index = indices.lock().unwrap().pop();
            let buffer = vec![0u8; params.row_bytes(Region::Primary)];
            (index.expect("not run more times than indicies"), buffer)
        };

        match method {
            ReadMethod::Seek => {
                let file = Arc::new(Mutex::new(file));

                bencher
                    .with_inputs(common_inputs)
                    .bench_values(|(index, mut buffer)| {
                        let mut file = file.lock().unwrap();
                        file.read_primary_sliver_using_seek(index, &mut buffer)
                            .unwrap()
                    })
            }
            ReadMethod::ReadAt => {
                bencher
                    .with_inputs(common_inputs)
                    .bench_values(|(index, mut buffer)| {
                        file.read_primary_sliver_using_read_at(index, &mut buffer)
                            .unwrap()
                    })
            }
            ReadMethod::Mmap => {
                file.switch_to_mmap_reads().expect("switch succeeds");

                bencher
                    .with_inputs(common_inputs)
                    .bench_values(|(index, mut buffer)| {
                        file.read_primary_sliver_using_mmap(index, &mut buffer)
                            .unwrap()
                    })
            }
        }
    }

    fn transpose_order_bench_impl(
        bencher: Bencher,
        file_size: &str,
        method: ReadMethod,
        region: Region,
    ) {
        let (_named_file, file, params) = init_transpose_order_symbol_file(file_size);

        let mut indices: Vec<_> = match region {
            Region::Source => (0..params.row_count(region)).collect(),
            Region::SourceColumnExpansion => {
                (params.n_shards - params.row_count(region)..params.n_shards).collect()
            }
            _ => panic!("what are you doing?!?"),
        };
        indices.shuffle(&mut SmallRng::seed_from_u64(1));

        let indices = Arc::new(Mutex::new(indices));
        let common_inputs = || {
            let index = indices.lock().unwrap().pop();
            let buffer = vec![0u8; params.row_bytes(Region::Primary)];
            (index.expect("not run more times than indicies"), buffer)
        };

        match method {
            ReadMethod::Seek => {
                // let file = Arc::new(Mutex::new(file));

                // bencher
                //     .with_inputs(common_inputs)
                //     .bench_values(|(index, mut buffer)| {
                //         let mut file = file.lock().unwrap();
                //         file.read_primary_sliver_using_seek(index, &mut buffer)
                //             .unwrap()
                //     })
            }
            ReadMethod::ReadAt => {
                bencher
                    .with_inputs(common_inputs)
                    .bench_values(|(index, mut buffer)| {
                        file.read_primary_sliver_using_read_at(index, &mut buffer)
                            .unwrap()
                    })
            }
            ReadMethod::Mmap => {
                // file.switch_to_mmap_reads().expect("switch succeeds");

                // bencher
                //     .with_inputs(common_inputs)
                //     .bench_values(|(index, mut buffer)| {
                //         file.read_primary_sliver_using_mmap(index, &mut buffer)
                //             .unwrap()
                //     })
            }
        }
    }

    #[divan::bench_group]
    mod first_section {
        use super::*;

        #[divan::bench(args = FILE_SIZES, ignore)]
        fn stream_order_seek(bencher: Bencher, file_size: &str) {
            stream_order_bench_impl(bencher, file_size, ReadMethod::Seek, Region::Source);
        }

        #[divan::bench(args = FILE_SIZES)]
        fn stream_order_read_at(bencher: Bencher, file_size: &str) {
            stream_order_bench_impl(bencher, file_size, ReadMethod::ReadAt, Region::Source);
        }

        #[divan::bench(args = FILE_SIZES)]
        fn stream_order_mmap(bencher: Bencher, file_size: &str) {
            stream_order_bench_impl(bencher, file_size, ReadMethod::Mmap, Region::Source);
        }

        #[divan::bench(args = FILE_SIZES)]
        fn transposed_order_read_at(bencher: Bencher, file_size: &str) {
            transpose_order_bench_impl(bencher, file_size, ReadMethod::ReadAt, Region::Source);
        }
    }

    #[divan::bench_group]
    mod second_section {
        use super::*;

        #[divan::bench(args = FILE_SIZES, ignore)]
        fn stream_order_seek(bencher: Bencher, file_size: &str) {
            stream_order_bench_impl(
                bencher,
                file_size,
                ReadMethod::Seek,
                Region::SourceColumnExpansion,
            );
        }

        #[divan::bench(args = FILE_SIZES)]
        fn stream_order_read_at(bencher: Bencher, file_size: &str) {
            stream_order_bench_impl(
                bencher,
                file_size,
                ReadMethod::ReadAt,
                Region::SourceColumnExpansion,
            );
        }

        #[divan::bench(args = FILE_SIZES)]
        fn stream_order_mmap(bencher: Bencher, file_size: &str) {
            stream_order_bench_impl(
                bencher,
                file_size,
                ReadMethod::Mmap,
                Region::SourceColumnExpansion,
            );
        }

        #[divan::bench(args = FILE_SIZES)]
        fn tranposed_order_read_at(bencher: Bencher, file_size: &str) {
            transpose_order_bench_impl(
                bencher,
                file_size,
                ReadMethod::ReadAt,
                Region::SourceColumnExpansion,
            );
        }
    }
}

#[divan::bench_group(threads = thread_counts(), sample_count = 20, sample_size = 30)]
mod read_secondary_sliver {
    use super::*;

    fn common_inputs(params: BlobExpansionParameters) -> impl Fn() -> (usize, Vec<u8>) {
        let mut indices: Vec<_> = (0..params.n_shards).collect();
        indices.shuffle(&mut SmallRng::seed_from_u64(2));
        let indices = Arc::new(Mutex::new(indices));

        move || {
            let index = indices.lock().unwrap().pop();
            let buffer = vec![0u8; params.column_bytes(Region::Secondary)];
            (index.expect("not run more times than indicies"), buffer)
        }
    }

    #[divan::bench(args = FILE_SIZES)]
    fn stream_order_read_at(bencher: Bencher, file_size: &str) {
        let (_named_file, file, params) = init_symbol_file(file_size);

        bencher
            .with_inputs(common_inputs(params))
            .bench_values(|(index, mut buffer)| {
                file.read_secondary_sliver_using_read_at(index, &mut buffer)
                    .unwrap()
            });
    }

    #[divan::bench(args = FILE_SIZES)]
    fn tranposed_order_read_at(bencher: Bencher, file_size: &str) {
        let (_named_file, file, params) = init_transpose_order_symbol_file(file_size);

        bencher
            .with_inputs(common_inputs(params))
            .bench_values(|(index, mut buffer)| {
                file.read_secondary_sliver_using_read_at(index, &mut buffer)
                    .unwrap()
            });
    }

    #[divan::bench(args = FILE_SIZES, ignore)]
    fn stream_order_seek(bencher: Bencher, file_size: &str) {
        let (_named_file, file, params) = init_symbol_file(file_size);
        let file = Arc::new(Mutex::new(file));

        bencher
            .with_inputs(common_inputs(params))
            .bench_values(|(index, mut buffer)| {
                let mut file = file.lock().unwrap();
                file.read_secondary_sliver_using_seek(index, &mut buffer)
                    .unwrap()
            });
    }

    #[divan::bench(args = FILE_SIZES)]
    fn stream_order_mmap(bencher: Bencher, file_size: &str) {
        let (_named_file, mut file, params) = init_symbol_file(file_size);

        file.switch_to_mmap_reads().unwrap();

        bencher
            .with_inputs(common_inputs(params))
            .bench_values(|(index, mut buffer)| {
                file.read_secondary_sliver_using_mmap(index, &mut buffer)
                    .unwrap()
            });
    }
}

#[divan::bench_group(sample_size = 1, sample_count = 2)]
mod direct_writes {
    use rand::thread_rng;

    use super::*;

    const BUFFER_SIZE: usize = 8 << 20;

    #[divan::bench(args = EXPANDED_FILE_SIZES)]
    fn write_lots_of_data_mmap(bencher: Bencher, file_size: &str) {
        let file_size = parse_iec_bytes(file_size);
        let mut rng = SmallRng::seed_from_u64(thread_rng().next_u64());

        bencher
            .with_inputs(|| {
                let mut named_file = NamedTempFile::new().expect("able to create tempfile");
                // This is because we mmap and want it to be the full length.
                named_file.as_file_mut().set_len(file_size as u64).unwrap();

                let mmap = unsafe {
                    MmapOptions::new()
                        .map_mut(named_file.as_file())
                        .expect("hopefully succeeds")
                };

                (mmap, named_file, vec![0u8; BUFFER_SIZE])
            })
            .bench_local_values(|(mut mmap_mut, _guard, mut buffer)| {
                let mut remaining: usize = file_size;
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

    #[divan::bench(args = EXPANDED_FILE_SIZES)]
    fn write_lots_of_data_write(bencher: Bencher, file_size: &str) {
        let file_size = parse_iec_bytes(file_size);
        let mut rng = SmallRng::seed_from_u64(thread_rng().next_u64());

        bencher
            .with_inputs(|| {
                (
                    NamedTempFile::new().expect("able to create tempfile"),
                    vec![0u8; BUFFER_SIZE],
                )
            })
            .bench_local_values(|(mut file, mut buffer)| {
                let mut remaining: usize = file_size;
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
