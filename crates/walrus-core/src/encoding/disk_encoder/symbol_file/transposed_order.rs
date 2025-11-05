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
    symbol_array::{ArrayOrder, SymbolArray2d, UNSPECIFIED_DIMENSION}, BlobExpansionParameters, Region
};

#[derive(Default, Debug)]
enum WritePhase {
    #[default]
    Unset,
    WantsExpandedPrimarySlivers {
        remaining_bytes: usize,
        buffer: Cursor<Box<[u8]>>,
    },
    WantsSecondaryExpansions {
        remaining_bytes: usize,
        /// A read buffer, as we allow reading the columns of the prior section.
        // TODO(jsmith): The use of this read-buffer should impact the selected size of the write buffer.
        read_buffer: Vec<u8>,
        /// Write buffer for writing the secondary expansions.
        write_buffer: Cursor<Box<[u8]>>,
    },
    WritingComplete,
}

/// This stores the symbols in stream-order, i.e., the order in which they are generated.
struct StreamOrderSymbolFile {
    params: BlobExpansionParameters,
    max_buffer_size: usize,
    n_buffered_expanded_primary_slivers: usize,
    n_buffered_secondary_sliver_expansions: usize,
    phase: WritePhase,
    file: File,
    use_stream_order: bool,
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
impl StreamOrderSymbolFile {
    fn create<P>(
        path: P,
        params: BlobExpansionParameters,
        max_buffer_size: usize,
        use_stream_order: bool,
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

        // n_buffered_expanded_primary_slivers: usize,
        // n_buffered_secondary_sliver_expansions: usize,
        let buffer_len = n_buffered_expanded_primary_slivers * params.row_bytes(Region::Secondary);
        let inital_buffer = vec![0; buffer_len].into_boxed_slice();

        Ok(Self {
            file: File::create(path)?,
            max_buffer_size,
            n_buffered_expanded_primary_slivers,
            n_buffered_secondary_sliver_expansions,
            phase: WritePhase::WantsExpandedPrimarySlivers {
                remaining_bytes: params.region_bytes(Region::Secondary),
                buffer: Cursor::new(inital_buffer),
            },
            params,
            use_stream_order,
        })
    }

    // TODO(jsmith): For writing these sections in stream order, it's sufficient to use a bufwriter.
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

            let is_complete = *remaining_bytes == 0;
            let is_buffer_full = buffer.get_ref().len()
                == usize::try_from(buffer.position()).expect("buffer position fits in usize");

            if is_complete || is_buffer_full {
                let position_usize =
                    usize::try_from(buffer.position()).expect("buffer position fits in usize");

                let data_to_flush = &mut buffer.get_mut()[..position_usize];

                let matrix = SymbolArray2d::from_buffer(
                    data_to_flush,
                    (
                        UNSPECIFIED_DIMENSION,
                        self.params.column_count(Region::Secondary),
                    ),
                    self.params.symbol_size,
                    ArrayOrder::RowMajor,
                );

                if !self.use_stream_order {
                    unimplemented!("need to impelement the transpose");
                }

                self.file.write_all(matrix.as_ref())?;
                buffer.set_position(0);
            }
            data = &data[written..];
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

        // The read-buffer size for a column is just the total symbols required for that column.
        let read_buffer = vec![0; self.params.column_bytes(Region::Secondary)];

        let write_buffer_len = self.n_buffered_secondary_sliver_expansions
            * self.params.column_bytes(Region::SourceColumnExpansion);
        let write_buffer = Cursor::new(vec![0; write_buffer_len].into_boxed_slice());

        self.phase = WritePhase::WantsSecondaryExpansions {
            read_buffer,
            write_buffer,
            remaining_bytes: self.params.region_bytes(Region::SourceColumnExpansion),
        }
    }

    fn write_secondary_sliver_expansion(&mut self, mut data: &[u8]) -> std::io::Result<()> {
        let WritePhase::WantsSecondaryExpansions {
            ref mut remaining_bytes,
            ref mut write_buffer,
            ..
        } = self.phase
        else {
            panic!("not expecting secondary sliver expansions");
        };
        assert!(data.len() <= *remaining_bytes, "too much data");

        while !data.is_empty() {
            let written = write_buffer
                .write(data)
                .expect("writing to an in-memory buffer cannot fail");

            *remaining_bytes -= written;

            let is_complete = *remaining_bytes == 0;
            let is_buffer_full = write_buffer.get_ref().len()
                == usize::try_from(write_buffer.position()).expect("buffer position fits in usize");

            if is_complete || is_buffer_full {
                let position_usize = usize::try_from(write_buffer.position())
                    .expect("buffer position fits in usize");
                let data_to_flush = &mut write_buffer.get_mut()[..position_usize];

                let matrix = SymbolArray2d::from_buffer(
                    data_to_flush,
                    (
                        self.params.row_count(Region::SourceColumnExpansion),
                        UNSPECIFIED_DIMENSION,
                    ),
                    self.params.symbol_size,
                    ArrayOrder::ColumnMajor,
                );

                if !self.use_stream_order {
                    unimplemented!("need to impelement the transpose");
                }

                self.file.write_all(matrix.as_ref())?;
                write_buffer.set_position(0);
            }
            data = &data[written..];
        }

        if *remaining_bytes == 0 {
            self.phase = WritePhase::WritingComplete;
        }

        Ok(())
    }

    fn read_secondary_sliver(&self, index: usize) -> std::io::Result<Vec<u8>> {
        // This reads columns from the section with source data.
        todo!()
    }

    fn read_primary_sliver(&mut self, index: usize) -> std::io::Result<Vec<u8>> {
        todo!()
        // // This reads rows from the section with source data, as well as rows from the section with
        // // column data.
        // let mut vec = vec![0; self.params.source_row_length_bytes()];

        // if index < self.params.n_source_primary_slivers {
        //     let mut vec = vec![0; self.params.source_row_length_bytes()];
        //     self.file.seek(SeekFrom::Start(
        //         (index * self.params.n_shards * self.params.symbol_size) as u64,
        //     ))?;
        //     self.file.read_exact(&mut vec)?;
        // } else {
        //     let matrix = Matrix {
        //         n_rows: self.params.n_shards - self.params.n_source_primary_slivers,
        //         n_cols: self.params.source_columns,
        //         order: Order::ColMajor,
        //     };

        //     assert!(vec.len().is_multiple_of(self.params.symbol_size));
        //     let chunks = vec.chunks_exact_mut(self.params.symbol_size);
        //     let offset_factors =
        //         matrix.row_cell_indices(index - self.params.n_source_primary_slivers);
        //     let base_offset = (self.params.n_shards * self.params.symbol_size)
        //         * self.params.n_source_primary_slivers;

        //     for (chunk, offset_factor) in chunks.zip(offset_factors) {
        //         self.file.seek(SeekFrom::Start(
        //             (base_offset + offset_factor * self.params.symbol_size) as u64,
        //         ))?;
        //         self.file.read_exact(chunk)?;
        //     }
        // }

        // Ok(vec)
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
enum Order {
    RowMajor,
    ColMajor,
}

#[derive(Debug, Clone, Copy)]
struct Matrix {
    n_rows: usize,
    n_cols: usize,
    order: Order,
}

impl Matrix {
    fn cell_index(self, row: usize, col: usize) -> usize {
        match self.order {
            Order::RowMajor => row * self.n_cols + col,
            Order::ColMajor => col * self.n_rows + row,
        }
    }

    fn row_cell_indices(self, row: usize) -> impl Iterator<Item = usize> {
        (0..self.n_cols).map(move |col| self.cell_index(row, col))
    }
}

#[cfg(test)]
mod tests {
    use core::num::NonZero;
    use std::{boxed::Box, collections::HashMap, println, time::Instant};

    use rand::{RngCore, SeedableRng, rngs::SmallRng};
    use walrus_test_utils::param_test;

    use super::*;
    use crate::encoding::{EncodingConfigEnum, ReedSolomonEncodingConfig};

    #[derive(Debug, Default)]
    struct ExpandedTestBlob {
        rows: HashMap<Region, Vec<Vec<u8>>>,
        columns: HashMap<Region, Vec<Vec<u8>>>,
    }

    impl ExpandedTestBlob {
        fn iter_row_data(&self, region: Region) -> Box<dyn Iterator<Item = &Vec<u8>> + '_> {
            match region {
                Region::Source | Region::SourceColumnExpansion | Region::SourceRowExpansion => {
                    Box::new(self.rows[&region].iter())
                }
                Region::Primary => Box::new(
                    self.iter_row_data(Region::Source)
                        .chain(self.iter_row_data(Region::SourceColumnExpansion)),
                ),
                Region::Secondary => Box::new(
                    self.iter_row_data(Region::Source)
                        .zip(self.iter_row_data(Region::SourceRowExpansion))
                        .flat_map(|(first, second)| [first, second]),
                ),
            }
        }

        fn iter_column_data(&self, region: Region) -> Box<dyn Iterator<Item = &Vec<u8>> + '_> {
            match region {
                Region::Source | Region::SourceColumnExpansion | Region::SourceRowExpansion => {
                    Box::new(self.columns[&region].iter())
                }
                Region::Primary => Box::new(
                    self.iter_column_data(Region::Source)
                        .zip(self.iter_column_data(Region::SourceColumnExpansion))
                        .flat_map(|(first, second)| [first, second]),
                ),
                Region::Secondary => Box::new(
                    self.iter_column_data(Region::Source)
                        .chain(self.iter_column_data(Region::SourceRowExpansion)),
                ),
            }
        }

        fn new(params: BlobExpansionParameters) -> ExpandedTestBlob {
            let mut rng = SmallRng::seed_from_u64(73);
            let disjoint_regions = [
                Region::Source,
                Region::SourceRowExpansion,
                Region::SourceColumnExpansion,
            ];

            let mut rows: HashMap<Region, Vec<Vec<u8>>> = HashMap::new();

            for region in disjoint_regions {
                let region_rows = rows.entry(region).or_default();

                for _ in 0..params.row_count(region) {
                    let mut row_data = vec![0u8; params.row_bytes(region)];
                    rng.fill_bytes(&mut row_data);
                    region_rows.push(row_data);
                }
            }

            let mut columns: HashMap<Region, Vec<Vec<u8>>> = HashMap::new();

            for region in disjoint_regions {
                let region_rows = &rows[&region];
                let region_columns = columns.entry(region).or_default();

                region_columns.resize_with(params.column_count(region), Default::default);

                for row in region_rows {
                    let symbols = row.chunks_exact(params.symbol_size);

                    for (symbol, column) in symbols.zip(region_columns.iter_mut()) {
                        column.extend(symbol);
                    }
                }
            }

            Self { rows, columns }
        }
    }

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    param_test! {
        test_stream_order_write -> TestResult: [
            small: (10, 8, 1),
            hundred_mib: (1000, 100 << 20, 64 << 20),
            one_gib: (1000, 1 << 30, 64 << 20),
            four_gib: (1000, 4 << 30, 64 << 20),
        ]

    }
    fn test_stream_order_write(
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
            StreamOrderSymbolFile::create(file.path(), params, max_buffer_size, true)?;

        let start = Instant::now();
        for data in test_blob.iter_row_data(Region::Secondary) {
            symbol_file.write_expanded_source_primary_sliver_data(&data)?;
        }
        for data in test_blob.iter_column_data(Region::SourceColumnExpansion) {
            symbol_file.write_secondary_sliver_expansion(&data)?;
        }
        println!("write duration = {:?}", start.elapsed());

        let read_from_disk = std::fs::read(file.path())?;
        let mut read_from_disk = read_from_disk.as_slice();

        for data_chunk in test_blob
            .iter_row_data(Region::Secondary)
            .chain(test_blob.iter_column_data(Region::SourceColumnExpansion))
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
}
