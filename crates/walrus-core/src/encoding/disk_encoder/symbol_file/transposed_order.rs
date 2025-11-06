use alloc::vec::Vec;
use core::num::NonZero;
use std::{
    boxed::Box,
    fs::File,
    io::{Cursor, Write as _},
    path::Path,
    vec,
};

use crate::encoding::disk_encoder::{
    BlobExpansionParameters,
    Region,
    symbol_array::{
        ArrayOrder,
        SymbolArray2d,
        SymbolArray2dCursor,
        UNSPECIFIED_DIMENSION,
        WriteOrder,
    },
};

#[derive(Default, Debug)]
enum WritePhase {
    #[default]
    Unset,
    WantsExpandedPrimarySlivers {
        remaining_bytes: usize,
        buffer: SymbolArray2dCursor<Box<[u8]>>,
    },
    WantsSecondaryExpansions {
        remaining_bytes: usize,
        write_buffer: SymbolArray2dCursor<Box<[u8]>>,
    },
    WritingComplete,
}

/// This stores the symbols in stream-order, i.e., the order in which they are generated.
struct TransposedOrderSymbolFile {
    params: BlobExpansionParameters,
    max_buffer_size: usize,
    n_buffered_expanded_primary_slivers: usize,
    n_buffered_secondary_sliver_expansions: usize,
    phase: WritePhase,
    file: File,
}

fn calculate_tile_dimension(
    max_buffer_size: usize,
    fixed_dimension: usize,
    params: &BlobExpansionParameters,
) -> Option<NonZero<usize>> {
    let memory_required_for_fixed_dimension = params.symbol_size * fixed_dimension;
    NonZero::new(max_buffer_size / memory_required_for_fixed_dimension)
}

// TODO(jsmith): Try memory mapped reads.
///
///
/// Primary slivers are the rows
/// Secondary slivers are the unexpanded columns.
///
/// We add expanded primary slivers.
/// We add recovery symbols for secondary slivers.
impl TransposedOrderSymbolFile {
    fn create<P>(
        path: P,
        params: BlobExpansionParameters,
        max_buffer_size: usize,
    ) -> std::io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let n_buffered_expanded_primary_slivers = calculate_tile_dimension(
            max_buffer_size,
            params.column_count(Region::Secondary),
            &params,
        )
        .map(NonZero::get)
        .unwrap_or(1);

        let n_buffered_secondary_sliver_expansions = calculate_tile_dimension(
            max_buffer_size.saturating_sub(params.column_bytes(Region::Secondary)),
            params.row_count(Region::SourceColumnExpansion),
            &params,
        )
        .map(NonZero::get)
        .unwrap_or(1);

        // The array containing the data and defining the memory layout.
        //
        // We store the data in column-major order, as that is the order in which
        // we want it stored in the binary file.
        let array = SymbolArray2d::new(
            (
                n_buffered_expanded_primary_slivers,
                params.column_count(Region::Secondary),
            ),
            params.symbol_size,
            ArrayOrder::ColumnMajor,
        );
        // The cursor that tracks the writing progress.
        //
        // We set it to write in row-major order as that is the order in which the data arrives.
        let cursor = SymbolArray2dCursor::new(array, WriteOrder::RowMajor);

        Ok(Self {
            file: File::create(path)?,
            max_buffer_size,
            n_buffered_expanded_primary_slivers,
            n_buffered_secondary_sliver_expansions,
            phase: WritePhase::WantsExpandedPrimarySlivers {
                remaining_bytes: params.region_bytes(Region::Secondary),
                buffer: cursor,
            },
            params,
        })
    }

    fn write_expanded_source_primary_sliver_data(
        &mut self,
        mut data: &[u8],
    ) -> std::io::Result<()> {
        let WritePhase::WantsExpandedPrimarySlivers {
            ref mut buffer,
            ref mut remaining_bytes,
        } = self.phase
        else {
            panic!("no longer expecting primary slivers");
        };
        assert!(data.len() <= *remaining_bytes, "too much data");

        while !data.is_empty() {
            let written = buffer
                .write(data)
                .expect("writing to an in-memory buffer cannot fail");
            *remaining_bytes -= written;

            data = &data[written..];

            if *remaining_bytes == 0 {
                assert!(data.is_empty());
                self.file.write_all(buffer.get_ref().as_ref())?;
            } else if buffer.is_full() {
                self.file.write_all(buffer.get_ref().as_ref())?;

                if *remaining_bytes < buffer.capacity_bytes() {
                    assert!(*remaining_bytes % self.params.row_bytes(Region::Secondary) == 0);
                    let remaining_rows =
                        *remaining_bytes / self.params.row_bytes(Region::Secondary);
                    assert!(remaining_rows < self.n_buffered_expanded_primary_slivers);

                    buffer.clear_and_shrink_to((
                        remaining_rows,
                        self.params.column_count(Region::Secondary),
                    ));
                } else {
                    buffer.clear();
                }
            }
        }

        if *remaining_bytes == 0 {
            self.wants_secondary_expansions();
        }

        Ok(())
    }

    fn wants_secondary_expansions(&mut self) {
        assert!(
            matches!(self.phase, WritePhase::WantsExpandedPrimarySlivers { .. }),
            "must not be called unless was taking primary slivers"
        );
        // Free the buffers from the previously allocated stage.
        drop(std::mem::take(&mut self.phase));

        // The array containing the data and defining the memory layout.
        //
        // We store the data in row-major order, as that is the order in which we want it
        // stored in the binary file.
        let array = SymbolArray2d::new(
            (
                self.params.row_count(Region::SourceColumnExpansion),
                self.n_buffered_secondary_sliver_expansions,
            ),
            self.params.symbol_size,
            ArrayOrder::RowMajor,
        );

        // The cursor that tracks the writing progress.
        //
        // We set it to write in column-major order as that is the order in which the
        // expansions of the source columns arrive.
        let cursor = SymbolArray2dCursor::new(array, WriteOrder::ColumnMajor);

        self.phase = WritePhase::WantsSecondaryExpansions {
            remaining_bytes: self.params.region_bytes(Region::SourceColumnExpansion),
            write_buffer: cursor,
        };
    }
}

//     fn write_secondary_sliver_expansion(&mut self, mut data: &[u8]) -> std::io::Result<()> {
//         let WritePhase::WantsSecondaryExpansions {
//             ref mut remaining_bytes,
//             ref mut write_buffer,
//             ..
//         } = self.phase
//         else {
//             panic!("not expecting secondary sliver expansions");
//         };
//         assert!(data.len() <= *remaining_bytes, "too much data");
//
//         while !data.is_empty() {
//             let written = write_buffer
//                 .write(data)
//                 .expect("writing to an in-memory buffer cannot fail");
//
//             *remaining_bytes -= written;
//
//             let is_complete = *remaining_bytes == 0;
//             let is_buffer_full = write_buffer.get_ref().len()
//                 == usize::try_from(write_buffer.position()).expect("buffer position fits in usize");
//
//             if is_complete || is_buffer_full {
//                 let position_usize = usize::try_from(write_buffer.position())
//                     .expect("buffer position fits in usize");
//                 let data_to_flush = &mut write_buffer.get_mut()[..position_usize];
//
//                 let matrix = SymbolArray2d::from_buffer(
//                     data_to_flush,
//                     (
//                         self.params.row_count(Region::SourceColumnExpansion),
//                         UNSPECIFIED_DIMENSION,
//                     ),
//                     self.params.symbol_size,
//                     ArrayOrder::ColumnMajor,
//                 );
//
//                 if !self.use_stream_order {
//                     unimplemented!("need to impelement the transpose");
//                 }
//
//                 self.file.write_all(matrix.as_ref())?;
//                 write_buffer.set_position(0);
//             }
//             data = &data[written..];
//         }
//
//         if *remaining_bytes == 0 {
//             self.phase = WritePhase::WritingComplete;
//         }
//
//         Ok(())
//     }
//
//     fn read_secondary_sliver(&self, index: usize) -> std::io::Result<Vec<u8>> {
//         // This reads columns from the section with source data.
//         todo!()
//     }
//
//     fn read_primary_sliver(&mut self, index: usize) -> std::io::Result<Vec<u8>> {
//         todo!()
//         // // This reads rows from the section with source data, as well as rows from the section with
//         // // column data.
//         // let mut vec = vec![0; self.params.source_row_length_bytes()];
//
//         // if index < self.params.n_source_primary_slivers {
//         //     let mut vec = vec![0; self.params.source_row_length_bytes()];
//         //     self.file.seek(SeekFrom::Start(
//         //         (index * self.params.n_shards * self.params.symbol_size) as u64,
//         //     ))?;
//         //     self.file.read_exact(&mut vec)?;
//         // } else {
//         //     let matrix = Matrix {
//         //         n_rows: self.params.n_shards - self.params.n_source_primary_slivers,
//         //         n_cols: self.params.source_columns,
//         //         order: Order::ColMajor,
//         //     };
//
//         //     assert!(vec.len().is_multiple_of(self.params.symbol_size));
//         //     let chunks = vec.chunks_exact_mut(self.params.symbol_size);
//         //     let offset_factors =
//         //         matrix.row_cell_indices(index - self.params.n_source_primary_slivers);
//         //     let base_offset = (self.params.n_shards * self.params.symbol_size)
//         //         * self.params.n_source_primary_slivers;
//
//         //     for (chunk, offset_factor) in chunks.zip(offset_factors) {
//         //         self.file.seek(SeekFrom::Start(
//         //             (base_offset + offset_factor * self.params.symbol_size) as u64,
//         //         ))?;
//         //         self.file.read_exact(chunk)?;
//         //     }
//         // }
//
//         // Ok(vec)
//     }
// }
//
// #[derive(Debug, Eq, PartialEq, Clone, Copy)]
// enum Order {
//     RowMajor,
//     ColMajor,
// }
//
// #[derive(Debug, Clone, Copy)]
// struct Matrix {
//     n_rows: usize,
//     n_cols: usize,
//     order: Order,
// }
//
// impl Matrix {
//     fn cell_index(self, row: usize, col: usize) -> usize {
//         match self.order {
//             Order::RowMajor => row * self.n_cols + col,
//             Order::ColMajor => col * self.n_rows + row,
//         }
//     }
//
//     fn row_cell_indices(self, row: usize) -> impl Iterator<Item = usize> {
//         (0..self.n_cols).map(move |col| self.cell_index(row, col))
//     }
// }
#[cfg(test)]
mod test {
    use core::num::NonZero;
    use std::{boxed::Box, collections::HashMap, vec::Vec};

    use rand::{RngCore, SeedableRng, rngs::SmallRng};
    use walrus_test_utils::param_test;

    use super::*;
    use crate::encoding::{
        EncodingConfigEnum,
        ReedSolomonEncodingConfig,
        disk_encoder::test_blob::ExpandedTestBlob,
    };

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    param_test! {
        test_transposed_write -> TestResult: [
            small: (10, 8, 64 << 20),
            hundred_mib: (1000, 100 << 20, 64 << 20),
            one_gib: (1000, 1 << 30, 64 << 20),
            four_gib: (1000, 4 << 30, 64 << 20),
        ]

    }
    fn test_transposed_write(
        shards: u16,
        blob_size: usize,
        max_buffer_size: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let params = BlobExpansionParameters::new(
            blob_size,
            EncodingConfigEnum::ReedSolomon(ReedSolomonEncodingConfig::new(
                NonZero::new(shards).expect("value is non-zero"),
            )),
        )?;

        let test_blob = ExpandedTestBlob::new(params);
        let file = tempfile::NamedTempFile::new()?;

        let mut symbol_file =
            TransposedOrderSymbolFile::create(file.path(), params, max_buffer_size)?;

        for data in test_blob.iter_row_data(Region::Secondary) {
            symbol_file.write_expanded_source_primary_sliver_data(&data)?;
        }
        // for data in test_blob.iter_column_data(Region::SourceColumnExpansion) {
        //     symbol_file.write_secondary_sliver_expansion(&data)?;
        // }

        let read_from_disk = std::fs::read(file.path())?;
        let mut read_from_disk = read_from_disk.as_slice();

        for data_chunk in test_blob
            .iter_row_data(Region::Secondary)
            // .chain(test_blob.iter_column_data(Region::SourceColumnExpansion))
        {
            assert!(!data_chunk.is_empty(), "chunks are never empty");

            let chunk_read_from_disk = read_from_disk
                .split_off(..data_chunk.len())
                .expect("should be sufficient data");

            assert!(
                data_chunk == chunk_read_from_disk,
                "data written to the first section must be correct"
            );
        }

        Ok(())
    }

    // param_test! {
    //     read_slivers -> TestResult: [
    //         small_primary: (10, 8, true),
    //         hundred_mib_primary: (1000, 100 << 20, true),
    //         one_gib_primary: (1000, 1 << 30, true),
    //         small_secondary: (10, 8, false),
    //         hundred_mib_secondary: (1000, 100 << 20, false),
    //         one_gib_secondary: (1000, 1 << 30, false),
    //     ]
    // }
    // fn read_slivers(
    //     shards: u16,
    //     blob_size: usize,
    //     primary_read: bool,
    // ) -> Result<(), Box<dyn std::error::Error>> {
    //     let params = BlobExpansionParameters::new(
    //         blob_size,
    //         EncodingConfigEnum::ReedSolomon(ReedSolomonEncodingConfig::new(
    //             NonZero::new(shards).expect("value is non-zero"),
    //         )),
    //     )?;

    //     let test_blob = ExpandedTestBlob::new(params);
    //     let file = tempfile::NamedTempFile::new()?;

    //     let mut symbol_file = StreamOrderSymbolFile::create(file.path(), params)?;

    //     for data in test_blob.iter_row_data(Region::Secondary) {
    //         symbol_file.write_expanded_source_primary_sliver_data(&data)?;
    //     }
    //     for data in test_blob.iter_column_data(Region::SourceColumnExpansion) {
    //         symbol_file.write_secondary_sliver_expansion(&data)?;
    //     }

    //     if primary_read {
    //         let rows: Vec<_> = test_blob.iter_row_data(Region::Primary).collect();
    //         let row_indices: Vec<_> = (0..params.n_shards).collect();
    //         assert_eq!(rows.len(), row_indices.len());

    //         let mut sliver_buffer = std::vec![0; params.row_bytes(Region::Primary)];
    //         for index in row_indices {
    //             let expected = rows[index];
    //             symbol_file.read_primary_sliver(index, &mut sliver_buffer)?;

    //             assert!(sliver_buffer.as_slice() == expected.as_slice());
    //         }
    //     } else {
    //         let columns: Vec<_> = test_blob.iter_column_data(Region::Secondary).collect();
    //         let column_indices: Vec<_> = (0..params.n_shards).collect();
    //         assert_eq!(columns.len(), column_indices.len());

    //         let mut sliver_buffer = std::vec![0; params.column_bytes(Region::Secondary)];
    //         for index in column_indices {
    //             let expected = columns[index];
    //             symbol_file.read_secondary_sliver(index, &mut sliver_buffer)?;

    //             assert!(sliver_buffer.as_slice() == expected.as_slice());
    //         }
    //     }

    //     Ok(())
    // }
}
