#![allow(missing_docs)]
use std::{
    fs::File,
    io::{BufReader, BufWriter, Read as _, Seek as _, SeekFrom, Write as _},
    os::unix::fs::FileExt as _,
    path::Path,
};

use memmap2::{Mmap, MmapOptions};

use crate::encoding::disk_encoder::{BlobExpansionParameters, Region};

#[derive(Default, Debug)]
enum State {
    #[default]
    Unset,
    WantsExpandedPrimarySlivers {
        remaining_bytes: usize,
        writer: BufWriter<File>,
    },
    WantsSecondaryExpansions {
        remaining_bytes: usize,
        writer: BufWriter<File>,
    },
    WritingComplete,
}

/// This stores the symbols in stream-order, i.e., the order in which they are generated.
#[derive(Debug)]
pub struct StreamOrderSymbolFile {
    params: BlobExpansionParameters,
    reader: BufReader<File>,
    state: State,
    mmap: Option<Mmap>,
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
    pub fn create<P>(path: P, params: BlobExpansionParameters) -> std::io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let writer = BufWriter::new(File::create(path.as_ref())?);
        let reader = BufReader::new(File::open(path.as_ref())?);

        Ok(Self {
            state: State::WantsExpandedPrimarySlivers {
                remaining_bytes: params.region_bytes(Region::Secondary),
                writer,
            },
            params,
            reader,
            mmap: None,
        })
    }

    pub fn write_expanded_source_primary_sliver_data(
        &mut self,
        data: &[u8],
    ) -> std::io::Result<()> {
        assert!(
            matches!(self.state, State::WantsExpandedPrimarySlivers { .. }),
            "no longer expecting primary slivers"
        );
        self.write_data(data)
    }

    pub fn write_secondary_sliver_expansion(&mut self, data: &[u8]) -> std::io::Result<()> {
        assert!(
            matches!(self.state, State::WantsSecondaryExpansions { .. }),
            "not expecting secondary sliver expansions",
        );
        self.write_data(data)
    }

    pub fn write_data(&mut self, data: &[u8]) -> std::io::Result<()> {
        let (writer, remaining_bytes) = match &mut self.state {
            State::WantsExpandedPrimarySlivers {
                remaining_bytes,
                writer,
            }
            | State::WantsSecondaryExpansions {
                remaining_bytes,
                writer,
            } => (writer, remaining_bytes),
            state => panic!("writing not supported in state {:?}", state),
        };

        assert!(data.len() <= *remaining_bytes, "too much data");

        writer.write_all(data)?;
        *remaining_bytes -= data.len();

        if *remaining_bytes == 0 {
            writer.flush()?;

            match &self.state {
                State::WantsExpandedPrimarySlivers { .. } => self.wants_secondary_expansions(),
                State::WantsSecondaryExpansions { .. } => self.state = State::WritingComplete,
                _ => unreachable!("state checked above"),
            }
        }

        Ok(())
    }

    fn wants_secondary_expansions(&mut self) {
        let State::WantsExpandedPrimarySlivers { writer, .. } = std::mem::take(&mut self.state)
        else {
            panic!("must not be called unless was taking primary slivers");
        };

        self.state = State::WantsSecondaryExpansions {
            writer,
            remaining_bytes: self.params.region_bytes(Region::SourceColumnExpansion),
        };
    }

    pub fn switch_to_mmap_reads(&mut self) -> std::io::Result<()> {
        assert!(matches!(self.state, State::WritingComplete));
        let file = self.reader.get_ref();
        let mmap = unsafe { MmapOptions::new().no_reserve_swap().map(file)? };

        self.mmap = Some(mmap);

        Ok(())
    }

    pub fn read_secondary_sliver(
        &mut self,
        index: usize,
        buffer: &mut [u8],
    ) -> std::io::Result<()> {
        if self.mmap.is_some() && matches!(self.state, State::WritingComplete { .. }) {
            self.read_secondary_sliver_using_mmap(index, buffer)
        } else {
            self.read_secondary_sliver_using_seek(index, buffer)
        }
    }

    pub fn read_secondary_sliver_using_mmap(
        &self,
        index: usize,
        buffer: &mut [u8],
    ) -> std::io::Result<()> {
        assert!(index < self.params.n_shards, "index out of range");
        assert_eq!(
            buffer.len(),
            self.params.column_bytes(Region::Secondary),
            "buffer size must be exactly that of a secondary sliver"
        );
        let mmap = self.mmap.as_ref().expect("method called so mmap exists");

        let mut offset = index * self.params.symbol_size;

        for (i, chunk) in buffer.chunks_exact_mut(self.params.symbol_size).enumerate() {
            if i != 0 {
                offset += self.params.row_bytes(Region::Secondary);
            }
            let end = offset + self.params.symbol_size;

            chunk.copy_from_slice(&mmap[offset..end]);
        }

        Ok(())
    }

    pub fn read_secondary_sliver_using_seek(
        &mut self,
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

        // Seek to the first symbol in the row, just after the Secondary region.
        tracing::debug!(
            index,
            "reading secondary sliver symbols from the secondary region"
        );

        let initial_position = index * self.params.symbol_size;

        for (i, chunk) in buffer.chunks_exact_mut(self.params.symbol_size).enumerate() {
            let offset = if i == 0 {
                self.reader.seek(SeekFrom::Start(initial_position as u64))?
            } else {
                let change = self.params.row_bytes(Region::Secondary)
                        // Subtract the symbol size to account for the reads.
                        - self.params.symbol_size;
                self.reader.seek(SeekFrom::Current(change as i64))?
            };
            tracing::trace!(i, offset, "reading sliver symbol");

            self.reader.read_exact(chunk)?;
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

        // Seek to the first symbol in the row, just after the Secondary region.
        tracing::debug!(
            index,
            "reading secondary sliver symbols from the secondary region"
        );

        let initial_position = index * self.params.symbol_size;

        for (i, chunk) in buffer.chunks_exact_mut(self.params.symbol_size).enumerate() {
            let offset = initial_position + i * self.params.row_bytes(Region::Secondary);

            self.reader.get_ref().read_exact_at(chunk, offset as u64)?;
        }

        Ok(())
    }

    pub fn read_primary_sliver(&mut self, index: usize, buffer: &mut [u8]) -> std::io::Result<()> {
        if self.mmap.is_some() && matches!(self.state, State::WritingComplete { .. }) {
            self.read_primary_sliver_using_mmap(index, buffer)
        } else {
            self.read_primary_sliver_using_seek(index, buffer)
        }
    }

    pub fn read_primary_sliver_using_mmap(
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

        let mmap = self.mmap.as_ref().expect("method called so mmap exists");

        if index < self.params.row_count(Region::Secondary) {
            let offset = index * self.params.row_bytes(Region::Secondary);
            tracing::debug!(
                index,
                offset,
                "reading primary sliver from the soure region"
            );
            let end = offset + self.params.row_bytes(Region::Source);

            buffer.copy_from_slice(&mmap[offset..end]);
        } else {
            assert!(index >= self.params.row_count(Region::Source));

            // Seek to the first symbol in the row, just after the Secondary region.
            let relative_index = index - self.params.row_count(Region::Source);
            let mut start = self.params.region_bytes(Region::Secondary)
                + relative_index * self.params.symbol_size;

            for (i, chunk) in buffer.chunks_exact_mut(self.params.symbol_size).enumerate() {
                if i != 0 {
                    start += self.params.column_bytes(Region::SourceColumnExpansion);
                }
                let end = start + self.params.symbol_size;

                chunk.copy_from_slice(&mmap[start..end]);
            }
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
            let offset = index * self.params.row_bytes(Region::Secondary);
            tracing::debug!(
                index,
                offset,
                "reading primary sliver from the soure region"
            );

            self.reader.get_ref().read_exact_at(buffer, offset as u64)?;
        } else {
            assert!(index >= self.params.row_count(Region::Source));
            // Seek to the first symbol in the row, just after the Secondary region.
            let relative_index = index - self.params.row_count(Region::Source);
            tracing::debug!(
                index,
                relative_index,
                "reading primary sliver symbols from expansion region"
            );

            let initial_position = self.params.region_bytes(Region::Secondary)
                + relative_index * self.params.symbol_size;

            for (i, chunk) in buffer.chunks_exact_mut(self.params.symbol_size).enumerate() {
                let offset =
                    initial_position + i * self.params.column_bytes(Region::SourceColumnExpansion);

                self.reader.get_ref().read_exact_at(chunk, offset as u64)?;
            }
        }

        Ok(())
    }

    pub fn read_primary_sliver_using_seek(
        &mut self,
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
            let offset = index * self.params.row_bytes(Region::Secondary);
            tracing::debug!(
                index,
                offset,
                "reading primary sliver from the soure region"
            );

            self.reader.seek(SeekFrom::Start(offset as u64))?;
            self.reader.read_exact(buffer)?;
        } else {
            assert!(index >= self.params.row_count(Region::Source));
            // Seek to the first symbol in the row, just after the Secondary region.
            let relative_index = index - self.params.row_count(Region::Source);
            tracing::debug!(
                index,
                relative_index,
                "reading primary sliver symbols from expansion region"
            );

            let initial_position = self.params.region_bytes(Region::Secondary)
                + relative_index * self.params.symbol_size;

            for (i, chunk) in buffer.chunks_exact_mut(self.params.symbol_size).enumerate() {
                let offset = if i == 0 {
                    self.reader.seek(SeekFrom::Start(initial_position as u64))?
                } else {
                    let change = self.params.column_bytes(Region::SourceColumnExpansion)
                        // Subtract the symbol size to account for the reads.
                        - self.params.symbol_size;
                    self.reader.seek(SeekFrom::Current(change as i64))?
                };
                tracing::trace!(i, offset, "reading sliver symbol");

                self.reader.read_exact(chunk)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use core::num::NonZero;
    use std::{boxed::Box, collections::HashMap, vec::Vec};

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
                    let mut row_data = std::vec![0u8; params.row_bytes(region)];
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
            small: (10, 8),
            hundred_mib: (1000, 100 << 20),
            one_gib: (1000, 1 << 30),
            four_gib: (1000, 4 << 30),
        ]

    }
    fn test_stream_order_write(
        shards: u16,
        blob_size: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let params = BlobExpansionParameters::new(
            blob_size,
            EncodingConfigEnum::ReedSolomon(ReedSolomonEncodingConfig::new(
                NonZero::new(shards).expect("value is non-zero"),
            )),
        )?;

        let test_blob = ExpandedTestBlob::new(params);
        let file = tempfile::NamedTempFile::new()?;

        let mut symbol_file = StreamOrderSymbolFile::create(file.path(), params)?;

        for data in test_blob.iter_row_data(Region::Secondary) {
            symbol_file.write_expanded_source_primary_sliver_data(&data)?;
        }
        for data in test_blob.iter_column_data(Region::SourceColumnExpansion) {
            symbol_file.write_secondary_sliver_expansion(&data)?;
        }

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

    param_test! {
        read_slivers -> TestResult: [
            small_primary: (10, 8, true, false),
            hundred_mib_primary: (1000, 100 << 20, true, false),
            one_gib_primary: (1000, 1 << 30, true, false),
            small_secondary: (10, 8, false, false),
            hundred_mib_secondary: (1000, 100 << 20, false, false),
            one_gib_secondary: (1000, 1 << 30, false, false),
            small_primary_mmap: (10, 8, true, true),
            hundred_mib_primary_mmap: (1000, 100 << 20, true, true),
            one_gib_primary_mmap: (1000, 1 << 30, true, true),
            small_secondary_mmap: (10, 8, false, true),
            hundred_mib_secondary_mmap: (1000, 100 << 20, false, true),
            one_gib_secondary_mmap: (1000, 1 << 30, false, true),
        ]
    }
    fn read_slivers(
        shards: u16,
        blob_size: usize,
        primary_read: bool,
        use_mmap: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let params = BlobExpansionParameters::new(
            blob_size,
            EncodingConfigEnum::ReedSolomon(ReedSolomonEncodingConfig::new(
                NonZero::new(shards).expect("value is non-zero"),
            )),
        )?;

        let test_blob = ExpandedTestBlob::new(params);
        let file = tempfile::NamedTempFile::new()?;

        let mut symbol_file = StreamOrderSymbolFile::create(file.path(), params)?;

        for data in test_blob.iter_row_data(Region::Secondary) {
            symbol_file.write_expanded_source_primary_sliver_data(&data)?;
        }
        for data in test_blob.iter_column_data(Region::SourceColumnExpansion) {
            symbol_file.write_secondary_sliver_expansion(&data)?;
        }

        if use_mmap {
            symbol_file.switch_to_mmap_reads()?;
        }

        if primary_read {
            let rows: Vec<_> = test_blob.iter_row_data(Region::Primary).collect();
            let row_indices: Vec<_> = (0..params.n_shards).collect();
            assert_eq!(rows.len(), row_indices.len());

            let mut sliver_buffer = std::vec![0; params.row_bytes(Region::Primary)];
            for index in row_indices {
                let expected = rows[index];
                symbol_file.read_primary_sliver(index, &mut sliver_buffer)?;

                assert!(sliver_buffer.as_slice() == expected.as_slice());
            }
        } else {
            let columns: Vec<_> = test_blob.iter_column_data(Region::Secondary).collect();
            let column_indices: Vec<_> = (0..params.n_shards).collect();
            assert_eq!(columns.len(), column_indices.len());

            let mut sliver_buffer = std::vec![0; params.column_bytes(Region::Secondary)];
            for index in column_indices {
                let expected = columns[index];
                symbol_file.read_secondary_sliver(index, &mut sliver_buffer)?;

                assert!(sliver_buffer.as_slice() == expected.as_slice());
            }
        }

        Ok(())
    }
}
