use alloc::vec::Vec;
use core::{cmp, hash::Hasher, num::NonZero};
use std::{
    borrow::ToOwned as _,
    boxed::Box,
    dbg,
    format,
    fs::{File, OpenOptions},
    hash::DefaultHasher,
    io::{Cursor, Write as _},
    os::unix::fs::FileExt,
    path::Path,
    print,
    println,
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
enum State {
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
pub struct TransposedOrderSymbolFile {
    params: BlobExpansionParameters,
    max_buffer_size: usize,
    n_buffered_expanded_primary_slivers: usize,
    n_buffered_secondary_sliver_expansions: usize,
    state: State,
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
    pub fn create<P>(
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
        .unwrap_or(1)
        .min(params.row_count(Region::Secondary));

        let n_buffered_secondary_sliver_expansions = calculate_tile_dimension(
            max_buffer_size,
            params.row_count(Region::SourceColumnExpansion),
            &params,
        )
        .map(NonZero::get)
        .unwrap_or(1)
        .min(params.row_count(Region::SourceColumnExpansion));

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

        tracing::debug!(
            rows = n_buffered_expanded_primary_slivers,
            capacity_bytes = cursor.get_ref().capacity_bytes(),
            "created a buffer for the incoming expanded primary sliver"
        );

        Ok(Self {
            file: OpenOptions::new()
                .create(true)
                .truncate(true)
                .read(true)
                .write(true)
                .open(path)?,
            max_buffer_size,
            n_buffered_expanded_primary_slivers,
            n_buffered_secondary_sliver_expansions,
            state: State::WantsExpandedPrimarySlivers {
                remaining_bytes: params.region_bytes(Region::Secondary),
                buffer: cursor,
            },
            params,
        })
    }

    pub fn write_expanded_source_primary_sliver_data(
        &mut self,
        mut data: &[u8],
    ) -> std::io::Result<()> {
        let State::WantsExpandedPrimarySlivers {
            ref mut buffer,
            ref mut remaining_bytes,
        } = self.state
        else {
            panic!("no longer expecting primary slivers");
        };
        assert!(data.len() <= *remaining_bytes, "too much data");

        tracing::trace!(
            len = data.len(),
            remaining_bytes,
            "writing expanded primary sliver data"
        );

        while !data.is_empty() {
            let written = buffer
                .write(data)
                .expect("writing to an in-memory buffer cannot fail");
            *remaining_bytes -= written;

            tracing::trace!(written, "wrote bytes to inner buffer");

            data = &data[written..];

            if *remaining_bytes == 0 {
                let buffer_bytes = buffer.get_ref().as_ref();
                tracing::debug!(
                    buffer_len_bytes = buffer_bytes.len(),
                    buffer_len_rows = buffer_bytes.len() / self.params.row_bytes(Region::Secondary),
                    "all the expected data has been written or buffered, flushing buffer"
                );
                assert_eq!(
                    buffer_bytes.len() % self.params.row_bytes(Region::Secondary),
                    0
                );
                assert!(data.is_empty());
                self.file.write_all(buffer.get_ref().as_ref())?;
            } else if buffer.is_full() {
                let buffer_bytes = buffer.get_ref().as_ref();
                tracing::debug!(
                    len_row = self.params.row_bytes(Region::Secondary),
                    buffer_len_bytes = buffer_bytes.len(),
                    buffer_len_rows = buffer_bytes.len() / self.params.row_bytes(Region::Secondary),
                    "buffer is full, flushing"
                );
                self.file.write_all(buffer_bytes)?;

                if *remaining_bytes < buffer.capacity_bytes() {
                    tracing::debug!(
                        remaining_bytes,
                        remaining_bytes_rows =
                            *remaining_bytes / self.params.row_bytes(Region::Secondary),
                        "remaining bytes is less than 1 buffer, resizing the buffer to accomodate"
                    );
                    assert!(*remaining_bytes % self.params.row_bytes(Region::Secondary) == 0);

                    let remaining_rows =
                        *remaining_bytes / self.params.row_bytes(Region::Secondary);
                    assert!(remaining_rows < self.n_buffered_expanded_primary_slivers);

                    let new_shape = (remaining_rows, self.params.column_count(Region::Secondary));
                    tracing::debug!(?new_shape, "shrinking to new shape");
                    buffer.clear_and_shrink_to(new_shape);
                    tracing::debug!(
                        capacity_bytes = buffer.capacity_bytes(),
                        "new buffer capacity"
                    );
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

    pub fn read_secondary_sliver_using_read_at(
        &self,
        index: usize,
        buffer: &mut [u8],
    ) -> std::io::Result<()> {
        assert!(
            matches!(
                self.state,
                State::WantsSecondaryExpansions { .. } | State::WritingComplete
            ),
            "reading secondary slivers is only supported once they've all be written"
        );
        assert!(index < self.params.n_shards, "index out of range");
        assert_eq!(
            buffer.len(),
            self.params.column_bytes(Region::Secondary),
            "buffer size must be exactly that of a secondary sliver"
        );
        tracing::debug!(
            index,
            "reading secondary sliver symbols from the secondary region"
        );

        let n_sequential_symbols = self.n_buffered_expanded_primary_slivers;
        let sequential_symbols_byte_count = n_sequential_symbols * self.params.symbol_size;

        let tile_size_bytes =
            self.n_buffered_expanded_primary_slivers * self.params.row_bytes(Region::Secondary);
        tracing::debug!(
            "there are {n_sequential_symbols} symbols over {sequential_symbols_byte_count} bytes, and the size of a single tile is {tile_size_bytes}"
        );

        for (i, chunk) in buffer.chunks_mut(sequential_symbols_byte_count).enumerate() {
            let offset_to_start_of_tile = i * tile_size_bytes;
            // The last tile may have been smaller than the previous ones, which changes the column
            // offsets within that tile. We therefore take the chunk len, which is an indication of
            // how many symbols are sequential, to compute the offset within the tile.
            let offset_within_tile = index * chunk.len();

            tracing::trace!(
                offset_to_start_of_tile,
                offset_within_tile,
                read_size = chunk.len(),
                "reading symbols"
            );

            self.file
                .read_exact_at(chunk, (offset_to_start_of_tile + offset_within_tile) as u64)?;
        }

        Ok(())
    }

    fn wants_secondary_expansions(&mut self) {
        assert!(
            matches!(self.state, State::WantsExpandedPrimarySlivers { .. }),
            "must not be called unless was taking primary slivers"
        );
        // Free the buffers from the previously allocated stage.
        drop(std::mem::take(&mut self.state));

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

        self.state = State::WantsSecondaryExpansions {
            remaining_bytes: self.params.region_bytes(Region::SourceColumnExpansion),
            write_buffer: cursor,
        };
    }

    pub fn write_secondary_sliver_expansion(&mut self, mut data: &[u8]) -> std::io::Result<()> {
        let State::WantsSecondaryExpansions {
            ref mut remaining_bytes,
            write_buffer: ref mut buffer,
            ..
        } = self.state
        else {
            panic!("not expecting secondary sliver expansions");
        };
        assert!(data.len() <= *remaining_bytes, "too much data");
        let bytes_per_column = self.params.column_bytes(Region::SourceColumnExpansion);
        tracing::debug!("writing secondary sliver expansion");

        while !data.is_empty() {
            let written = buffer
                .write(data)
                .expect("writing to an in-memory buffer cannot fail");
            *remaining_bytes -= written;

            data = &data[written..];

            if *remaining_bytes == 0 {
                assert!(data.is_empty());
                tracing::debug!("no remaining bytes, flushing buffer");
                self.file.write_all(buffer.get_ref().as_ref())?;
            } else if buffer.is_full() {
                let buffer_bytes = buffer.get_ref().as_ref();
                self.file.write_all(buffer_bytes)?;

                if *remaining_bytes < buffer.capacity_bytes() {
                    assert_eq!(*remaining_bytes % bytes_per_column, 0);

                    let remaining_columns = *remaining_bytes / bytes_per_column;
                    assert!(remaining_columns < self.n_buffered_secondary_sliver_expansions);

                    let new_shape = (
                        self.params.row_count(Region::SourceColumnExpansion),
                        remaining_columns,
                    );
                    tracing::debug!(?new_shape, "shrinking to new shape");
                    buffer.clear_and_shrink_to(new_shape);
                    tracing::debug!(
                        capacity_bytes = buffer.capacity_bytes(),
                        "new buffer capacity"
                    );
                } else {
                    buffer.clear();
                }
            }
        }

        if *remaining_bytes == 0 {
            self.state = State::WritingComplete;
        }

        Ok(())
    }

    pub fn read_primary_sliver_using_read_at(
        &self,
        index: usize,
        buffer: &mut [u8],
    ) -> std::io::Result<()> {
        assert!(
            matches!(self.state, State::WritingComplete),
            "reading primary slivers is only supported when writing is complete"
        );
        assert!(index < self.params.n_shards, "index out of range");
        assert_eq!(
            buffer.len(),
            self.params.row_bytes(Region::Primary),
            "buffer size must be exactly that of a primary sliver"
        );

        if index < self.params.row_count(Region::Secondary) {
            // In this section, it's wholly within a tile.
            let rows_in_section = self.params.row_count(Region::Secondary);
            let n_rows_in_full_tile = self.n_buffered_expanded_primary_slivers;
            let n_rows_in_last_tile = rows_in_section % n_rows_in_full_tile;

            let tile_offset = index / n_rows_in_full_tile;
            let index_in_tile = index % n_rows_in_full_tile;
            let first_index_in_last_tile = rows_in_section - n_rows_in_last_tile;

            let n_rows_in_tile = if index >= first_index_in_last_tile {
                n_rows_in_last_tile
            } else {
                n_rows_in_full_tile
            };
            let n_sequential_symbols_in_tile = n_rows_in_tile;

            let bytes_in_full_tile = n_rows_in_full_tile * self.params.row_bytes(Region::Secondary);
            let tile_offset_bytes = tile_offset * bytes_in_full_tile;

            for (i, symbol_buffer) in buffer.chunks_mut(self.params.symbol_size).enumerate() {
                let offset = tile_offset_bytes
                    + (index_in_tile * self.params.symbol_size)
                    + (i * n_sequential_symbols_in_tile * self.params.symbol_size);

                self.file.read_exact_at(symbol_buffer, offset as u64)?;
            }
        } else {
            tracing::debug!(index, "attempting to get primary sliver from second region");

            let tile_size_bytes = self.n_buffered_secondary_sliver_expansions
                * self.params.column_bytes(Region::SourceColumnExpansion);

            let n_sequential_symbols = self.n_buffered_secondary_sliver_expansions;
            let sequential_symbols_byte_count = n_sequential_symbols * self.params.symbol_size;
            tracing::debug!("");

            let index_in_region = index - self.params.row_count(Region::Secondary);
            tracing::debug!(index_in_region, "index in the region");

            for (i, chunk) in buffer.chunks_mut(sequential_symbols_byte_count).enumerate() {
                let offset_to_region = self.params.region_bytes(Region::Secondary);
                let offset_to_start_of_tile = i * tile_size_bytes;
                // The last tile may have been smaller than the previous ones, which changes the column
                // offsets within that tile. We therefore take the chunk len, which is an indication of
                // how many symbols are sequential, to compute the offset within the tile.
                let offset_within_tile = index_in_region * chunk.len();

                self.file.read_exact_at(
                    chunk,
                    (offset_to_region + offset_to_start_of_tile + offset_within_tile) as u64,
                )?;
            }
        }
        Ok(())
    }
}

fn debug_print_symbols(symbol_size: usize, data: &[u8]) {
    println!("[");
    for row_chunk in data.chunks(symbol_size * 15) {
        for symbol in row_chunk.chunks(symbol_size) {
            let mut hasher = DefaultHasher::new();
            hasher.write(symbol);
            let hash_str = format!("{:x?}", hasher.finish());
            print!("{} ", &hash_str[..4]);
        }
        println!()
    }
    println!("]");
}

#[cfg(test)]
mod test {
    use core::num::NonZero;
    use std::{boxed::Box, collections::HashMap, format, vec::Vec};

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
        read_slivers -> TestResult: [
            small_primary: (10, 8, true),
            hundred_mib_primary: (1000, 100 << 20, true),
            one_gib_primary: (1000, 1 << 30, true),
            small_secondary: (10, 8, false),
            hundred_mib_secondary: (1000, 100 << 20, false),
            one_gib_secondary: (1000, 1 << 30, false),
        ]
    }
    fn read_slivers(
        shards: u16,
        blob_size: usize,
        primary_read: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let params = BlobExpansionParameters::new(
            blob_size,
            EncodingConfigEnum::ReedSolomon(ReedSolomonEncodingConfig::new(
                NonZero::new(shards).expect("value is non-zero"),
            )),
        )?;

        tracing_subscriber::fmt::init();
        let test_blob = ExpandedTestBlob::new(params);
        let file = tempfile::NamedTempFile::new()?;

        let mut symbol_file = TransposedOrderSymbolFile::create(file.path(), params, 64 << 20)?;

        for data in test_blob.iter_row_data(Region::Secondary) {
            symbol_file.write_expanded_source_primary_sliver_data(&data)?;
        }
        for data in test_blob.iter_column_data(Region::SourceColumnExpansion) {
            symbol_file.write_secondary_sliver_expansion(&data)?;
        }

        if primary_read {
            let rows: Vec<_> = test_blob.iter_row_data(Region::Primary).collect();
            let row_indices: Vec<_> = (0..params.n_shards).collect();
            assert_eq!(rows.len(), row_indices.len());

            let mut sliver_buffer = std::vec![0; params.row_bytes(Region::Primary)];
            for index in row_indices {
                let expected = rows[index];

                println!("expecting: ");
                debug_print_symbols(params.symbol_size, &expected);

                symbol_file.read_primary_sliver_using_read_at(index, &mut sliver_buffer)?;

                println!("received: ");
                debug_print_symbols(params.symbol_size, &sliver_buffer);

                assert!(sliver_buffer.as_slice() == expected.as_slice());
            }
        } else {
            let columns: Vec<_> = test_blob.iter_column_data(Region::Secondary).collect();
            let column_indices: Vec<_> = (0..params.n_shards).collect();
            assert_eq!(columns.len(), column_indices.len());

            let mut sliver_buffer = std::vec![0; params.column_bytes(Region::Secondary)];
            for index in column_indices {
                let expected = columns[index];

                println!("expecting: ");
                debug_print_symbols(params.symbol_size, &expected);

                symbol_file.read_secondary_sliver_using_read_at(index, &mut sliver_buffer)?;

                println!("received: ");
                debug_print_symbols(params.symbol_size, &sliver_buffer);

                assert!(sliver_buffer.as_slice() == expected.as_slice());
            }
        }

        Ok(())
    }
}
