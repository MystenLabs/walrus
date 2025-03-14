// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use alloc::{
    borrow::ToOwned,
    format,
    string::{String, ToString},
    vec,
    vec::Vec,
};
use core::{cmp, fmt, marker::PhantomData, num::NonZeroU16, slice::Chunks};

use fastcrypto::hash::Blake2b256;
use hex;
use serde::{Deserialize, Serialize};
use tracing::{Level, Span};

use super::{
    basic_encoding::{raptorq::RaptorQDecoder, reed_solomon::ReedSolomonDecoder, Decoder},
    utils,
    DataTooLargeError,
    DecodingSymbol,
    DecodingVerificationError,
    EncodingAxis,
    EncodingConfig,
    EncodingConfigEnum,
    Primary,
    Secondary,
    SliverData,
    SliverPair,
    Symbols,
};
use crate::{
    encoding::config::EncodingConfigTrait as _,
    merkle::{leaf_hash, MerkleTree},
    metadata::{
        BlobMetadata,
        QuiltBlock,
        QuiltIndex,
        QuiltMetadata,
        SliverPairMetadata,
        VerifiedBlobMetadataWithId,
    },
    BlobId,
    SliverIndex,
    SliverPairIndex,
};

/// Data layout of a quilt.
#[derive(Default, Serialize, Deserialize)]
pub struct Quilt {
    pub data: Vec<u8>,
    pub row_size: usize,
    pub blocks: Vec<QuiltBlock>,
    pub symbol_size: usize,
    pub quilt_index_end_index: Option<u16>,
}

impl Quilt {
    pub fn get_blob(&self, index: usize) -> Option<Vec<u8>> {
        if index >= self.blocks.len() {
            return None;
        }
        let block = &self.blocks[index];
        let start_col = if index == 0 {
            0
        } else {
            self.blocks[index - 1].end_index as usize
        };
        let end_col = block.end_index as usize;
        let mut blob = vec![0u8; block.unencoded_length as usize];

        let mut written = 0;
        for col in start_col..end_col {
            for row in 0..(self.data.len() / self.row_size) {
                let remaining = blob.len() - written;
                if remaining == 0 {
                    break;
                }
                let chunk_size = cmp::min(self.symbol_size, remaining);
                let start_idx = row * self.row_size + col * self.symbol_size;
                let end_idx = start_idx + chunk_size;

                blob[written..written + chunk_size].copy_from_slice(&self.data[start_idx..end_idx]);
                written += chunk_size;
            }
        }
        Some(blob)
    }

    pub fn get_blob_by_id(&self, id: &BlobId) -> Option<Vec<u8>> {
        let index = self
            .blocks
            .iter()
            .enumerate()
            .find(|(_, block)| block.blob_id == *id)
            .map(|(i, _)| i);
        index.and_then(|i| self.get_blob(i))
    }
}

impl fmt::Debug for Quilt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut ds = f.debug_struct("Quilt");

        // Add newline before data field
        ds.field(
            "\ndata", // <-- Added newline here
            &format_args!(
                "\n{:#?}",
                DebugMatrix {
                    data: &self.data,
                    row_size: self.row_size,
                    symbol_size: self.symbol_size
                }
            ),
        );

        // Format blocks with newline
        ds.field(
            "blocks",
            &format_args!("\n{:#?}", DebugBlocks(&self.blocks)),
        );

        ds.field("symbol_size", &self.symbol_size).finish()?;

        // Add final newline after entire struct
        writeln!(f)
    }
}

struct DebugMatrix<'a> {
    data: &'a [u8],
    row_size: usize,
    symbol_size: usize,
}

impl fmt::Debug for DebugMatrix<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut list = f.debug_list();
        for (i, row) in self.data.chunks(self.row_size).enumerate() {
            let entries = row
                .chunks(self.symbol_size)
                .map(|chunk| format!("0x{}", hex::encode(chunk)))
                .collect::<Vec<_>>();
            list.entry(&DebugRow {
                index: i,
                entries: &entries,
            });
        }
        list.finish()?;
        writeln!(f)
    }
}

struct DebugRow<'a> {
    index: usize,
    entries: &'a [String],
}

impl fmt::Debug for DebugRow<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let hex_width = self.entries.first().map_or(4, |e| e.len());
        let entries_per_line = 200 / (hex_width + 2); // +2 for ", " separator

        write!(f, "\nRow {:0>2}:\n", self.index)?;
        for (i, entry) in self.entries.iter().enumerate() {
            if i % entries_per_line == 0 {
                if i > 0 {
                    writeln!(f)?;
                }
                write!(f, "    ")?;
            }

            // Pad entries to fixed width for alignment
            write!(f, "{:width$}", entry, width = hex_width)?;

            if i < self.entries.len() - 1 {
                write!(f, ", ")?;
            }

            // Add truncation indicator
            if i == 5 && self.entries.len() > 8 {
                write!(f, "... (+{} more)", self.entries.len() - i - 1)?;
                break;
            }
        }
        Ok(())
    }
}

struct DebugBlocks<'a>(&'a [QuiltBlock]);

impl fmt::Debug for DebugBlocks<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut list = f.debug_list();
        for block in self.0 {
            list.entry(&format_args!(
                "\nQuiltBlock {{\n    blob_id: {:x?},\n    unencoded_length: {},\
                \n    end_index: {}\n    desc: {:?}\n}}",
                block.blob_id, block.unencoded_length, block.end_index, block.desc
            ));
        }
        list.finish()?;
        writeln!(f)
    }
}

/// Encodes a set of blobs into a single quilt blob.
pub struct QuiltEncoder<'a> {
    /// The quilt data.
    blobs: &'a [BlobWithDesc<'a>],
    config: EncodingConfigEnum<'a>,
    span: Span,
}

pub struct BlobWithDesc<'a> {
    blob: &'a [u8],
    desc: &'a str,
}

impl<'a> BlobWithDesc<'a> {
    pub fn new(blob: &'a [u8], desc: &'a str) -> Self {
        Self { blob, desc }
    }

    pub fn new_from_blob(blob: &'a [u8]) -> Self {
        Self { blob, desc: "" }
    }

    pub fn len(&self) -> usize {
        self.blob.len()
    }
}

impl<'a> QuiltEncoder<'a> {
    pub fn new(config: EncodingConfigEnum<'a>, blobs: &'a [BlobWithDesc<'a>]) -> Self {
        Self {
            blobs,
            config,
            span: tracing::span!(Level::ERROR, "QuiltEncoder"),
        }
    }

    /// Constructs a quilt from a set of blobs.
    pub fn construct_quilt(&self) -> Result<Quilt, DataTooLargeError> {
        let n_rows = self.config.n_source_symbols::<Primary>().get().into();
        let n_columns = self.config.n_source_symbols::<Secondary>().get().into();
        tracing::info!(
            "Constructing quilt blob with n_columns: {}, n_rows: {}",
            n_columns,
            n_rows
        );

        // 1. Compute blob_ids and create mapping (updated for BlobWithDesc)
        let mut blob_with_ids: Vec<_> = self
            .blobs
            .iter()
            .map(|blob_with_desc| {
                let encoder = BlobEncoder::new(self.config.clone(), blob_with_desc.blob).unwrap();
                let metadata = encoder.compute_metadata();
                (blob_with_desc, *metadata.blob_id())
            })
            .collect();

        // 2. Sort blobs based on their blob_ids (unchanged)
        blob_with_ids.sort_by_key(|(_, id)| *id);

        // 3. Calculate symbol size (updated blob access)
        let blob_sizes = blob_with_ids
            .iter()
            .map(|(b, _)| b.blob.len())
            .collect::<Vec<_>>();
        let Some(symbol_size) = compute_symbol_size(&blob_sizes, n_columns, n_rows, usize::MAX)
        else {
            tracing::error!("Impossible to fit blobs in {} columns.", n_columns);
            return Err(DataTooLargeError);
        };
        tracing::info!("Symbol size: {}", symbol_size);
        let required_alignment = self.config.encoding_type().required_alignment() as usize;
        let symbol_size = symbol_size.div_ceil(required_alignment) * required_alignment;

        let row_size = symbol_size * n_columns;
        let mut data = vec![0u8; row_size * n_rows];
        let mut quilt_blocks = Vec::new();
        let mut current_col = 0;

        // 4. Data filling section (preserved with blob access updates)
        for (blob_with_desc, blob_id) in &blob_with_ids {
            let mut cur = current_col;
            let cols_needed = blob_with_desc.blob.len().div_ceil(symbol_size * n_rows);
            tracing::info!(
                "Blob: {:?} needs {} columns, current_col: {}",
                blob_id,
                cols_needed,
                cur
            );
            if cur + cols_needed > n_columns {
                return Err(DataTooLargeError);
            }

            // Copy blob data into columns (updated blob access)
            let mut row = 0;
            let mut offset = 0;
            while offset < blob_with_desc.blob.len() {
                let end = cmp::min(offset + symbol_size, blob_with_desc.blob.len());
                let chunk = &blob_with_desc.blob[offset..end];
                let dest_idx = row * row_size + cur * symbol_size;

                data[dest_idx..dest_idx + chunk.len()].copy_from_slice(chunk);

                row = (row + 1) % n_rows;
                if row == 0 {
                    cur += 1;
                }
                offset += symbol_size;
            }

            current_col += cols_needed;

            // Create QuiltBlock with description (new field)
            quilt_blocks.push(QuiltBlock {
                blob_id: *blob_id,
                unencoded_length: blob_with_desc.blob.len() as u64,
                end_index: current_col as u16,
                desc: blob_with_desc.desc.to_string(),
            });
        }

        Ok(Quilt {
            data,
            row_size,
            blocks: quilt_blocks,
            symbol_size,
            quilt_index_end_index: None,
        })
    }

    /// Constructs a quilt blob with a container quilt index.
    pub fn construct_container_quilt(&self) -> Result<Quilt, DataTooLargeError> {
        let n_rows = self.config.n_source_symbols::<Primary>().get().into();
        let n_columns = self.config.n_source_symbols::<Secondary>().get().into();
        tracing::info!(
            "Constructing quilt blob with n_columns: {}, n_rows: {}",
            n_columns,
            n_rows
        );

        // 1. Compute blob_ids and create mapping
        let mut blob_with_ids: Vec<_> = self
            .blobs
            .iter()
            .map(|blob_with_desc| {
                let encoder = BlobEncoder::new(self.config.clone(), blob_with_desc.blob).unwrap();
                let metadata = encoder.compute_metadata();
                (blob_with_desc, *metadata.blob_id())
            })
            .collect();

        // 2. Sort blobs based on their blob_ids
        blob_with_ids.sort_by_key(|(_, id)| *id);

        // Create initial QuiltBlocks with default end_index
        let quilt_blocks: Vec<QuiltBlock> = blob_with_ids
            .iter()
            .map(|(blob_with_desc, blob_id)| QuiltBlock {
                blob_id: *blob_id,
                unencoded_length: blob_with_desc.blob.len() as u64,
                end_index: Default::default(),
                desc: blob_with_desc.desc.to_string(),
            })
            .collect();

        // Create the container quilt index
        let mut container_quilt_index = QuiltIndex { quilt_blocks };

        // Get just the serialized size without actually serializing
        let serialized_index_size = bcs::serialized_size(&container_quilt_index)
            .expect("Size calculation should succeed") as u64;

        // Calculate total size including the 8-byte size prefix
        let final_index_data_size = 8 + serialized_index_size as usize;

        // Calculate blob sizes for symbol size computation
        let blob_sizes: Vec<usize> = blob_with_ids
            .iter()
            .map(|(bwd, _)| bwd.blob.len())
            .collect();
        let mut all_sizes = vec![final_index_data_size];
        all_sizes.extend(blob_sizes.clone());

        let Some(symbol_size) = compute_symbol_size(&all_sizes, n_columns, n_rows, usize::MAX)
        else {
            tracing::error!("Impossible to fit blobs in {} columns.", n_columns);
            return Err(DataTooLargeError);
        };
        tracing::info!("Symbol size: {}", symbol_size);

        let required_alignment = self.config.encoding_type().required_alignment() as usize;
        let symbol_size = symbol_size.div_ceil(required_alignment) * required_alignment;

        let row_size = symbol_size * n_columns;
        let mut data = vec![0u8; row_size * n_rows];

        // Calculate columns needed for the index
        let index_cols_needed = final_index_data_size.div_ceil(symbol_size * n_rows);
        let mut current_col = index_cols_needed;

        // First pass: Fill data with actual blobs and collect quilt blocks
        for (i, (blob_with_desc, blob_id)) in blob_with_ids.iter().enumerate() {
            let mut cur = current_col;
            let cols_needed = blob_with_desc.blob.len().div_ceil(symbol_size * n_rows);
            tracing::info!(
                "Blob: {:?} needs {} columns, current_col: {}",
                blob_id,
                cols_needed,
                cur
            );
            if cur + cols_needed > n_columns {
                return Err(DataTooLargeError);
            }

            // Copy blob data into columns
            let mut row = 0;
            let mut offset = 0;
            while offset < blob_with_desc.blob.len() {
                let end = cmp::min(offset + symbol_size, blob_with_desc.blob.len());
                let chunk = &blob_with_desc.blob[offset..end];
                let dest_idx = row * row_size + cur * symbol_size;

                data[dest_idx..dest_idx + chunk.len()].copy_from_slice(chunk);

                row = (row + 1) % n_rows;
                if row == 0 {
                    cur += 1;
                }
                offset += symbol_size;
            }

            current_col += cols_needed;

            assert_eq!(blob_id, &container_quilt_index.quilt_blocks[i].blob_id);
            assert_eq!(blob_id, &container_quilt_index.quilt_blocks[i].blob_id);
            container_quilt_index.quilt_blocks[i].end_index = current_col as u16;
        }

        // Create the final index data with size prefix
        let mut final_index_data = Vec::new();
        final_index_data.extend_from_slice(&final_index_data_size.to_le_bytes());
        tracing::info!("final_index_data_size: {}", final_index_data_size);
        final_index_data.extend_from_slice(
            &bcs::to_bytes(&container_quilt_index).expect("Serialization should succeed"),
        );

        // Second pass: Fill the beginning of data with the index blob
        let mut row = 0;
        let mut offset = 0;
        let mut cur = 0;

        while offset < final_index_data_size {
            let end = cmp::min(offset + symbol_size, final_index_data_size);
            let chunk = &final_index_data[offset..end];
            let dest_idx = row * row_size + cur * symbol_size;

            data[dest_idx..dest_idx + chunk.len()].copy_from_slice(chunk);

            row = (row + 1) % n_rows;
            if row == 0 {
                cur += 1;
            }
            offset += symbol_size;
        }

        let mut quilt_blocks = Vec::new();
        quilt_blocks.push(QuiltBlock {
            blob_id: BlobId::ZERO,
            unencoded_length: final_index_data.len() as u64,
            end_index: index_cols_needed as u16,
            desc: "QuiltBlobIndex".to_string(),
        });
        quilt_blocks.extend(container_quilt_index.quilt_blocks);

        Ok(Quilt {
            data,
            row_size,
            blocks: quilt_blocks,
            symbol_size,
            quilt_index_end_index: Some(index_cols_needed as u16),
        })
    }

    pub fn encode(&self) -> Result<Vec<SliverPair>, DataTooLargeError> {
        let _guard = self.span.enter();
        let quilt = self
            .construct_quilt()
            .expect("should be able to construct quilt");

        let encoder = BlobEncoder::new(self.config.clone(), quilt.data.as_slice())?;
        assert_eq!(encoder.symbol_usize(), quilt.symbol_size);
        Ok(encoder.encode())
    }

    pub fn encode_with_quilt_index(&self) -> Result<Vec<SliverPair>, DataTooLargeError> {
        let _guard = self.span.enter();
        let quilt = self
            .construct_container_quilt()
            .expect("should be able to construct quilt");
        let encoder = BlobEncoder::new(self.config.clone(), quilt.data.as_slice())?;
        assert_eq!(encoder.symbol_usize(), quilt.symbol_size);
        Ok(encoder.encode())
    }

    pub fn encode_with_metadata(&self) -> (Vec<SliverPair>, QuiltMetadata) {
        let _guard = self.span.enter();
        tracing::debug!("starting to encode blob with metadata");
        let quilt = self
            .construct_quilt()
            .expect("should be able to construct quilt");
        let encoder = BlobEncoder::new(self.config.clone(), quilt.data.as_slice())
            .expect("should be able to create encoder");
        assert_eq!(encoder.symbol_usize(), quilt.symbol_size);
        let (sliver_pairs, metadata) = encoder.encode_with_metadata();
        let quilt_metadata = QuiltMetadata::new(
            metadata.blob_id().clone(),
            metadata.metadata().clone(),
            quilt.blocks,
        );
        (sliver_pairs, quilt_metadata)
    }

    /// Encodes the blobs into a quilt and returns the slivers and metadata.
    pub fn encode_with_quilt_index_and_metadata(
        &self,
    ) -> Result<(Vec<SliverPair>, QuiltMetadata), DataTooLargeError> {
        let _guard = self.span.enter();
        tracing::debug!("starting to encode blob with metadata");
        let quilt = self.construct_container_quilt()?;
        let encoder = BlobEncoder::new(self.config.clone(), quilt.data.as_slice())?;
        assert_eq!(encoder.symbol_usize(), quilt.symbol_size);
        let (sliver_pairs, metadata) = encoder.encode_with_metadata();
        let quilt_metadata = QuiltMetadata::new(
            metadata.blob_id().clone(),
            metadata
                .metadata()
                .clone()
                .with_quilt_index_end_index(quilt.quilt_index_end_index.unwrap()),
            quilt.blocks,
        );
        Ok((sliver_pairs, quilt_metadata))
    }
}

/// Finds the minimum length needed to store blobs in a fixed number of columns.
/// Each blob must be stored in consecutive columns.
///
/// # Arguments
/// * `blobs` - Slice of blob lengths.
/// * `nc` - Number of columns available.
/// * `base` - Base of the encoding, the column size must be a multiple of this.
///
/// # Returns
/// * `Option<usize>` - The minimum length needed, or None if impossible.
fn compute_symbol_size(
    blobs: &[usize],
    nc: usize,
    nr: usize,
    max_quilt_size: usize,
) -> Option<usize> {
    // If any blob requires more columns than available, it's impossible
    tracing::info!("Blobs: {:?}, nc: {}, nr: {}", blobs, nc, nr);
    if blobs.len() > nc {
        tracing::info!("Impossible to fit blobs in nc columns");
        return None;
    }

    let min_len = blobs.iter().sum::<usize>().div_ceil(nc);
    let mut min_val = (min_len - 1) / nr + 1;
    let max_len = blobs.iter().max().expect("blobs not empty").to_owned();
    let mut max_val = (max_len - 1) / nr + 1;

    while min_val < max_val {
        tracing::info!("min_val: {}, max_val: {}", min_val, max_val);
        let mid = (min_val + max_val) / 2;
        if can_fit(blobs, nc, mid * nr) {
            max_val = mid;
        } else {
            min_val = mid + 1;
        }
    }

    Some(min_val).filter(|&size| size * nc * nr <= max_quilt_size)
}

fn can_fit(blobs: &[usize], nc: usize, length: usize) -> bool {
    tracing::info!("Blobs: {:?}, nc: {}, length: {}", blobs, nc, length);
    let mut used_cols = 0;
    for &blob in blobs {
        let cur = (blob - 1) / length + 1;
        if used_cols + cur > nc {
            return false;
        }
        used_cols += cur;
    }
    tracing::info!("Can fit: {}, length: {}", used_cols, length);
    true
}

/// Struct to perform the full blob encoding.
#[derive(Debug)]
pub struct BlobEncoder<'a> {
    /// A reference to the blob.
    // INV: `blob.len() > 0`
    blob: &'a [u8],
    /// The size of the encoded and decoded symbols.
    symbol_size: NonZeroU16,
    /// The number of rows of the message matrix.
    ///
    /// Stored as a `usize` for convenience, but guaranteed to be non-zero.
    n_rows: usize,
    /// The number of columns of the message matrix.
    ///
    /// Stored as a `usize` for convenience, but guaranteed to be non-zero.
    n_columns: usize,
    /// Reference to the encoding configuration of this encoder.
    config: EncodingConfigEnum<'a>,
    /// A tracing span associated with this blob encoder.
    span: Span,
}

impl<'a> BlobEncoder<'a> {
    /// Creates a new `BlobEncoder` to encode the provided `blob` with the provided configuration.
    ///
    /// The actual encoding can be performed with the [`encode()`][Self::encode] method.
    ///
    /// # Errors
    ///
    /// Returns a [`DataTooLargeError`] if the blob is too large to be encoded. This can happen in
    /// two cases:
    ///
    /// 1. If the blob is too large to fit into the message matrix with valid symbols. The maximum
    ///    blob size for a given [`EncodingConfigEnum`] is accessible through the
    ///    [`EncodingConfigEnum::max_blob_size`] method.
    /// 2. On 32-bit architectures, the maximally supported blob size can actually be smaller than
    ///    that due to limitations of the address space.
    pub fn new(config: EncodingConfigEnum<'a>, blob: &'a [u8]) -> Result<Self, DataTooLargeError> {
        tracing::debug!("creating new blob encoder");
        let symbol_size = utils::compute_symbol_size_from_usize(
            blob.len(),
            config.source_symbols_per_blob(),
            config.encoding_type().required_alignment(),
        )?;
        let n_rows = config.n_source_symbols::<Primary>().get().into();
        let n_columns = config.n_source_symbols::<Secondary>().get().into();

        Ok(Self {
            blob,
            symbol_size,
            n_rows,
            n_columns,
            config,
            span: tracing::span!(
                Level::ERROR,
                "BlobEncoder",
                blob_size = blob.len(),
                blob = crate::utils::data_prefix_string(blob, 5),
            ),
        })
    }

    /// Encodes the blob with which `self` was created to a vector of [`SliverPair`s][SliverPair].
    ///
    /// # Panics
    ///
    /// This function can panic if there is insufficient virtual memory for the encoded data,
    /// notably on 32-bit architectures. As there is an expansion factor of approximately 4.5, blobs
    /// larger than roughly 800 MiB cannot be encoded on 32-bit architectures.
    pub fn encode(&self) -> Vec<SliverPair> {
        tracing::debug!(parent: &self.span, "starting to encode blob");
        let mut primary_slivers: Vec<_> = self.empty_slivers::<Primary>();
        let mut secondary_slivers: Vec<_> = self.empty_slivers::<Secondary>();

        // The first `n_rows` primary slivers and the last `n_columns` secondary slivers can be
        // directly copied from the blob.
        for (row, sliver) in self.rows().zip(primary_slivers.iter_mut()) {
            sliver.symbols.data_mut()[..row.len()].copy_from_slice(row);
        }
        for (column, sliver) in self.column_symbols().zip(secondary_slivers.iter_mut()) {
            sliver
                .symbols
                .to_symbols_mut()
                .zip(column)
                .for_each(|(dest, src)| dest[..src.len()].copy_from_slice(src))
        }

        // Compute the remaining primary slivers by encoding the columns (i.e., secondary slivers).
        for (col_index, column) in secondary_slivers.iter().take(self.n_columns).enumerate() {
            for (symbol, sliver) in self
                .config
                .encode_all_repair_symbols::<Primary>(column.symbols.data())
                .expect("size has already been checked")
                .into_iter()
                .zip(primary_slivers.iter_mut().skip(self.n_rows))
            {
                sliver.copy_symbol_to(col_index, &symbol);
            }
        }

        // Compute the remaining secondary slivers by encoding the rows (i.e., primary slivers).
        for (r, row) in primary_slivers.iter().take(self.n_rows).enumerate() {
            for (symbol, sliver) in self
                .config
                .encode_all_repair_symbols::<Secondary>(row.symbols.data())
                .expect("size has already been checked")
                .into_iter()
                .zip(secondary_slivers.iter_mut().skip(self.n_columns))
            {
                sliver.copy_symbol_to(r, &symbol);
            }
        }

        primary_slivers
            .into_iter()
            .zip(secondary_slivers.into_iter().rev())
            .map(|(primary, secondary)| SliverPair { primary, secondary })
            .collect()
    }

    /// Encodes the blob with which `self` was created to a vector of [`SliverPair`s][SliverPair],
    /// and provides the relative [`VerifiedBlobMetadataWithId`].
    ///
    /// This function operates on the fully expanded message matrix for the blob. This matrix is
    /// used to compute the Merkle trees for the metadata, and to extract the sliver pairs. The
    /// returned blob metadata is considered to be verified as it is directly built from the data.
    ///
    /// # Panics
    ///
    /// This function can panic if there is insufficient virtual memory for the encoded data,
    /// notably on 32-bit architectures. As there is an expansion factor of approximately 4.5, blobs
    /// larger than roughly 800 MiB cannot be encoded on 32-bit architectures.
    pub fn encode_with_metadata(&self) -> (Vec<SliverPair>, VerifiedBlobMetadataWithId) {
        let _guard = self.span.enter();
        tracing::debug!("starting to encode blob with metadata");
        let mut expanded_matrix = self.get_expanded_matrix();
        let metadata = expanded_matrix.get_metadata();

        // This is just an optimization to free memory that is no longer needed at this point.
        expanded_matrix.drop_recovery_symbols();

        let mut sliver_pairs = self.empty_sliver_pairs();
        // First compute the secondary slivers -- does not require consuming the matrix.
        expanded_matrix.write_secondary_slivers(&mut sliver_pairs);
        // Then consume the matrix to get the primary slivers.
        expanded_matrix.write_primary_slivers(&mut sliver_pairs);
        tracing::debug!(
            blob_id = %metadata.blob_id(),
            "successfully encoded blob"
        );

        (sliver_pairs, metadata)
    }

    /// Computes the metadata (blob ID, hashes) for the blob, without returning the slivers.
    pub fn compute_metadata(&self) -> VerifiedBlobMetadataWithId {
        tracing::debug!(parent: &self.span, "starting to compute metadata");
        self.get_expanded_matrix().get_metadata()
    }

    fn symbol_usize(&self) -> usize {
        self.symbol_size.get().into()
    }

    /// Returns a reference to the symbol at the provided indices in the message matrix.
    ///
    /// The length of the returned slice can be lower than `self.symbol_size` if the blob needs to
    /// be padded.
    fn symbol_at(&self, row_index: usize, col_index: usize) -> &[u8] {
        let start_index = cmp::min(
            self.symbol_usize() * (self.n_columns * row_index + col_index),
            self.blob.len(),
        );
        let end_index = cmp::min(start_index + self.symbol_usize(), self.blob.len());
        self.blob[start_index..end_index].as_ref()
    }

    fn column_symbols(
        &self,
    ) -> impl ExactSizeIterator<Item = impl ExactSizeIterator<Item = &[u8]>> {
        (0..self.n_columns).map(move |col_index| {
            (0..self.n_rows).map(move |row_index| self.symbol_at(row_index, col_index))
        })
    }

    fn rows(&self) -> Chunks<u8> {
        self.blob.chunks(self.n_columns * self.symbol_usize())
    }

    fn empty_slivers<T: EncodingAxis>(&self) -> Vec<SliverData<T>> {
        (0..self.config.n_shards().get())
            .map(|i| {
                SliverData::<T>::new_empty(
                    self.config.n_source_symbols::<T::OrthogonalAxis>().get(),
                    self.symbol_size,
                    SliverIndex(i),
                )
            })
            .collect()
    }

    /// Returns a vector of empty [`SliverPair`] of length `n_shards`. Primary and secondary slivers
    /// are initialized with the appropriate `symbol_size` and `length`.
    fn empty_sliver_pairs(&self) -> Vec<SliverPair> {
        (0..self.config.n_shards().get())
            .map(|i| SliverPair::new_empty(&self.config, self.symbol_size, SliverPairIndex(i)))
            .collect()
    }

    /// Computes the fully expanded message matrix by encoding rows and columns.
    fn get_expanded_matrix(&self) -> ExpandedMessageMatrix {
        self.span
            .in_scope(|| ExpandedMessageMatrix::new(&self.config, self.symbol_size, self.blob))
    }
}

/// The representation of the expanded message matrix.
///
/// The expanded message matrix is represented as vector of rows, where each row is a [`Symbols`]
/// object. This choice simplifies indexing, and the rows of [`Symbols`] can then be directly
/// truncated into primary slivers.
struct ExpandedMessageMatrix<'a> {
    matrix: Vec<Symbols>,
    // INV: `blob.len() > 0`
    blob: &'a [u8],
    config: &'a EncodingConfigEnum<'a>,
    /// The number of rows in the non-expanded message matrix.
    n_rows: usize,
    /// The number of columns in the non-expanded message matrix.
    n_columns: usize,
    symbol_size: NonZeroU16,
}

impl<'a> ExpandedMessageMatrix<'a> {
    fn new(config: &'a EncodingConfigEnum<'a>, symbol_size: NonZeroU16, blob: &'a [u8]) -> Self {
        tracing::debug!("computing expanded message matrix");
        let matrix = vec![
            Symbols::zeros(config.n_shards_as_usize(), symbol_size);
            config.n_shards_as_usize()
        ];
        let mut expanded_matrix = Self {
            matrix,
            blob,
            config,
            n_rows: config.n_source_symbols::<Primary>().get().into(),
            n_columns: config.n_source_symbols::<Secondary>().get().into(),
            symbol_size,
        };
        expanded_matrix.fill_systematic_with_rows();
        expanded_matrix.expand_rows_for_secondary();
        expanded_matrix.expand_all_columns();
        expanded_matrix
    }

    /// Fills the systematic part of the matrix using `self.rows`.
    fn fill_systematic_with_rows(&mut self) {
        for (destination_row, row) in self.matrix.iter_mut().zip(
            self.blob
                .chunks(self.n_columns * usize::from(self.symbol_size.get())),
        ) {
            destination_row.data_mut()[0..row.len()].copy_from_slice(row);
        }
    }

    fn expanded_column_symbols(
        &'a self,
    ) -> impl Iterator<Item = impl ExactSizeIterator<Item = &'a [u8]> + 'a> {
        (0..self.matrix.len()).map(move |col_index| {
            self.matrix
                .iter()
                // Get the columns in reverse order `n_shards - col_index - 1`.
                .map(move |row| {
                    row[SliverPairIndex::try_from(col_index)
                        .expect("size has already been checked")
                        .to_sliver_index::<Secondary>(self.config.n_shards())
                        .as_usize()]
                    .as_ref()
                })
        })
    }

    /// Expands all columns to completely fill the `n_shards * n_shards` expanded message matrix.
    fn expand_all_columns(&mut self) {
        for col_index in 0..self.config.n_shards().get().into() {
            let mut column = Symbols::with_capacity(self.n_rows, self.symbol_size);
            self.matrix.iter().take(self.n_rows).for_each(|row| {
                let _ = column.extend(&row[col_index]);
            });

            for (row_index, symbol) in self
                .config
                .encode_all_repair_symbols::<Primary>(column.data())
                .expect("size has already been checked")
                .into_iter()
                .enumerate()
            {
                self.matrix[self.n_rows + row_index][col_index].copy_from_slice(&symbol);
            }
        }
    }

    /// Expands the first `source_symbols_primary` primary slivers (rows) to get all remaining
    /// secondary slivers.
    fn expand_rows_for_secondary(&mut self) {
        for row in self.matrix.iter_mut().take(self.n_rows) {
            for (col_index, symbol) in self
                .config
                .encode_all_repair_symbols::<Secondary>(&row[0..self.n_columns])
                .expect("size has already been checked")
                .into_iter()
                .enumerate()
            {
                row[self.n_columns + col_index].copy_from_slice(&symbol)
            }
        }
    }

    /// Computes the sliver pair metadata from the expanded message matrix.
    fn get_metadata(&self) -> VerifiedBlobMetadataWithId {
        tracing::debug!("computing blob metadata and ID");

        let n_shards = self.config.n_shards_as_usize();
        let mut leaf_hashes = Vec::with_capacity(n_shards * n_shards);
        for row in 0..n_shards {
            for col in 0..n_shards {
                leaf_hashes.push(leaf_hash::<Blake2b256>(&self.matrix[row][col]));
            }
        }

        let mut metadata = Vec::with_capacity(n_shards);
        for sliver_index in 0..n_shards {
            let primary_hash = MerkleTree::<Blake2b256>::build_from_leaf_hashes(
                leaf_hashes[n_shards * sliver_index..n_shards * (sliver_index + 1)]
                    .iter()
                    .cloned(),
            )
            .root();
            let secondary_hash = MerkleTree::<Blake2b256>::build_from_leaf_hashes(
                (0..n_shards).map(|symbol_index| {
                    leaf_hashes[n_shards * symbol_index + n_shards - 1 - sliver_index].clone()
                }),
            )
            .root();
            metadata.push(SliverPairMetadata {
                primary_hash,
                secondary_hash,
            })
        }

        VerifiedBlobMetadataWithId::new_verified_from_metadata(
            metadata,
            self.config.encoding_type(),
            u64::try_from(self.blob.len()).expect("any valid blob size fits into a `u64`"),
        )
    }

    /// Writes the secondary metadata to the provided mutable slice.
    ///
    /// This is no longer used in the actual code and just kept for testing.
    #[cfg(test)]
    fn write_secondary_metadata(&self, metadata: &mut [SliverPairMetadata]) {
        metadata
            .iter_mut()
            .zip(self.expanded_column_symbols())
            .for_each(|(metadata, symbols)| {
                metadata.secondary_hash = MerkleTree::<Blake2b256>::build(symbols).root();
            });
    }

    /// Writes the secondary slivers to the provided mutable slice.
    fn write_secondary_slivers(&self, sliver_pairs: &mut [SliverPair]) {
        sliver_pairs
            .iter_mut()
            .zip(self.expanded_column_symbols())
            .for_each(|(sliver_pair, symbols)| {
                for (target_slice, symbol) in
                    sliver_pair.secondary.symbols.to_symbols_mut().zip(symbols)
                {
                    target_slice.copy_from_slice(symbol);
                }
            })
    }

    /// Drops the part of the matrix that only contains recovery symbols.
    ///
    /// This part is only necessary for the metadata but not for any of the slivers.
    ///
    /// After this function is called, the functions [`get_metadata`][Self::get_metadata],
    /// [`write_secondary_metadata`][Self::write_secondary_metadata], and
    /// [`write_primary_metadata`][Self::write_primary_metadata] no longer produce meaningful
    /// results.
    fn drop_recovery_symbols(&mut self) {
        self.matrix
            .iter_mut()
            .skip(self.n_rows)
            .for_each(|row| row.truncate(self.n_columns));
    }

    /// Writes the primary metadata to the provided mutable slice.
    ///
    /// This is no longer used in the actual code and just kept for testing.
    #[cfg(test)]
    fn write_primary_metadata(&self, metadata: &mut [SliverPairMetadata]) {
        for (metadata, row) in metadata.iter_mut().zip(self.matrix.iter()) {
            metadata.primary_hash = MerkleTree::<Blake2b256>::build(row.to_symbols()).root();
        }
    }

    /// Writes the primary slivers to the provided mutable slice.
    ///
    /// Consumes the original matrix, as it creates the primary slivers by truncating the rows of
    /// the matrix.
    fn write_primary_slivers(self, sliver_pairs: &mut [SliverPair]) {
        for (sliver_pair, mut row) in sliver_pairs.iter_mut().zip(self.matrix.into_iter()) {
            row.truncate(self.config.n_source_symbols::<Secondary>().get().into());
            sliver_pair.primary.symbols = row;
        }
    }
}

/// A decoder for the quilt index.
pub struct QuiltDecoder<'a> {
    n_shards: NonZeroU16,
    slivers: Vec<&'a SliverData<Secondary>>,
    quilt_index: Option<QuiltIndex>,
    start_index: u16,
}

impl<'a> QuiltDecoder<'a> {
    pub fn new(n_shards: NonZeroU16, slivers: &'a [&'a SliverData<Secondary>]) -> Self {
        Self {
            n_shards,
            slivers: slivers.to_vec(),
            quilt_index: None,
            start_index: 0,
        }
    }

    /// Decodes the quilt index from the provided slivers.
    pub fn decode_quilt_index(&mut self) -> Option<&QuiltIndex> {
        // Get the sliver index for the quilt index (index 0)

        // Get the sliver index for the quilt index (index 0)
        let index = SliverIndex(0);

        // Find the secondary sliver with the matching index
        let first_sliver = self.slivers.iter().find(|s| s.index == index)?;

        // Check if the sliver has at least 8 bytes for the size prefix
        if first_sliver.symbols.data().len() < 8 {
            return None;
        }

        // Read the first 8 bytes to get the data size
        let data_size = u64::from_le_bytes(
            first_sliver.symbols.data()[0..8]
                .try_into()
                .expect("slice with incorrect length"),
        );

        tracing::info!("data_size: {}", data_size);
        // Calculate how many slivers we need based on the data size
        let sliver_size = first_sliver.symbols.data().len();
        let total_size_needed = data_size as usize; // 8 bytes for prefix + data
        let num_slivers_needed = total_size_needed.div_ceil(sliver_size); // Ceiling division
        self.start_index = num_slivers_needed as u16;

        // Otherwise, we need to collect data from multiple slivers
        let mut combined_data = Vec::with_capacity((data_size - 8) as usize);

        // Add data from the first sliver (skipping the 8-byte size prefix)
        combined_data.extend_from_slice(&first_sliver.symbols.data()[8..]);

        // Find and add data from subsequent slivers
        for i in 1..num_slivers_needed {
            let next_index = SliverIndex(i as u16);
            let next_sliver = self.slivers.iter().find(|s| s.index == next_index)?;

            // Add data from this sliver
            combined_data.extend_from_slice(next_sliver.symbols.data());

            // Check if we have enough data
            if combined_data.len() >= (data_size - 8) as usize {
                break;
            }
        }

        // Ensure we have enough data and truncate if necessary
        if combined_data.len() < (data_size - 8) as usize {
            return None; // Not enough data available
        }

        // Truncate to the exact size needed
        combined_data.truncate((data_size - 8) as usize);

        // Decode the QuiltIndex from the collected data
        self.quilt_index = bcs::from_bytes(&combined_data).ok();

        // After successful deserialization, sort the blocks by end_index
        if let Some(quilt_index) = &mut self.quilt_index {
            quilt_index
                .quilt_blocks
                .sort_by_key(|block| block.end_index);
        }

        self.quilt_index.as_ref()
    }

    pub fn get_quilt_index(&self) -> Option<&QuiltIndex> {
        self.quilt_index.as_ref()
    }

    pub fn get_blob_by_id(&self, id: &BlobId) -> Option<Vec<u8>> {
        self.get_block_by_predicate(|block| &block.blob_id == id)
    }

    pub fn get_blob_by_desc(&self, desc: &str) -> Option<Vec<u8>> {
        self.get_block_by_predicate(|block| &block.desc == desc)
    }

    fn get_block_by_predicate<F>(&self, predicate: F) -> Option<Vec<u8>>
    where
        F: Fn(&QuiltBlock) -> bool,
    {
        let quilt_index = self.get_quilt_index()?;

        // Find the block matching the predicate
        let (block_idx, block) = quilt_index
            .quilt_blocks
            .iter()
            .enumerate()
            .find(|(_, block)| predicate(block))?;

        // Determine start index (0 for first block, previous block's end_index otherwise)
        let start_idx = if block_idx == 0 {
            self.start_index
        } else {
            quilt_index.quilt_blocks[block_idx - 1].end_index
        };

        let end_idx = block.end_index;

        // Extract and reconstruct the blob
        let mut blob = Vec::with_capacity(block.unencoded_length as usize);

        // Collect data from the appropriate slivers
        for i in start_idx..end_idx {
            let sliver_idx = SliverIndex(i as u16);
            if let Some(sliver) = self.slivers.iter().find(|s| s.index == sliver_idx) {
                blob.extend_from_slice(sliver.symbols.data());
            }
        }

        // Truncate to the exact size
        blob.truncate(block.unencoded_length as usize);
        Some(blob)
    }

    /// Adds slivers to the decoder.
    fn add_slivers(mut self, slivers: &'a [&'a SliverData<Secondary>]) -> Self {
        self.slivers.extend(slivers);
        self
    }
}

/// A wrapper around the blob decoder for different encoding types.
#[derive(Debug)]
pub enum BlobDecoderEnum<'a, E: EncodingAxis> {
    /// The RaptorQ decoder.
    RaptorQ(BlobDecoder<'a, RaptorQDecoder, E>),
    /// The Reed-Solomon decoder.
    ReedSolomon(BlobDecoder<'a, ReedSolomonDecoder, E>),
}

impl<E: EncodingAxis> BlobDecoderEnum<'_, E> {
    /// Attempts to decode the source blob from the provided slivers.
    ///
    /// Returns the source blob as a byte vector if decoding succeeds or `None` if decoding fails.
    pub fn decode_and_verify(
        &mut self,
        blob_id: &BlobId,
        slivers: impl IntoIterator<Item = SliverData<E>>,
    ) -> Result<Option<(Vec<u8>, VerifiedBlobMetadataWithId)>, DecodingVerificationError> {
        match self {
            Self::RaptorQ(d) => d.decode_and_verify(blob_id, slivers),
            Self::ReedSolomon(d) => d.decode_and_verify(blob_id, slivers),
        }
    }
}

/// Struct to reconstruct a blob from either [`Primary`] (default) or [`Secondary`]
/// [`Sliver`s][SliverData].
#[derive(Debug)]
pub struct BlobDecoder<'a, D: Decoder, E: EncodingAxis = Primary> {
    _decoding_axis: PhantomData<E>,
    decoders: Vec<D>,
    blob_size: usize,
    symbol_size: NonZeroU16,
    config: &'a D::Config,
    /// A tracing span associated with this blob decoder.
    span: Span,
}

impl<'a, D: Decoder, E: EncodingAxis> BlobDecoder<'a, D, E> {
    /// Creates a new `BlobDecoder` to decode a blob of size `blob_size` using the provided
    /// configuration.
    ///
    /// The generic parameter specifies from which type of slivers the decoding will be performed.
    ///
    /// This function creates the necessary decoders for the decoding; actual decoding can be
    /// performed with the [`decode()`][Self::decode] method.
    ///
    /// # Errors
    ///
    /// Returns a [`DataTooLargeError`] if the `blob_size` is too large to be decoded.
    pub fn new(config: &'a D::Config, blob_size: u64) -> Result<Self, DataTooLargeError> {
        tracing::debug!("creating new blob decoder");
        let symbol_size = config.symbol_size_for_blob(blob_size)?;
        let blob_size = blob_size.try_into().map_err(|_| DataTooLargeError)?;

        let decoders = (0..config.n_source_symbols::<E::OrthogonalAxis>().get())
            .map(|_| {
                D::new(
                    config.n_source_symbols::<E>(),
                    config.n_shards(),
                    symbol_size,
                )
            })
            .collect();

        Ok(Self {
            _decoding_axis: PhantomData,
            decoders,
            blob_size,
            symbol_size,
            config,
            span: tracing::span!(Level::ERROR, "BlobDecoder", blob_size),
        })
    }

    /// Attempts to decode the source blob from the provided slivers.
    ///
    /// Returns the source blob as a byte vector if decoding succeeds or `None` if decoding fails.
    ///
    /// Slivers of incorrect length are dropped with a warning.
    ///
    /// If decoding failed due to an insufficient number of provided slivers, it can be continued by
    /// additional calls to [`decode`][Self::decode] providing more slivers.
    ///
    /// # Panics
    ///
    /// This function can panic if there is insufficient virtual memory for the decoded blob in
    /// addition to the slivers, notably on 32-bit architectures.
    pub fn decode<S>(&mut self, slivers: S) -> Option<Vec<u8>>
    where
        S: IntoIterator<Item = SliverData<E>>,
        E: EncodingAxis,
    {
        let _guard = self.span.enter();
        tracing::debug!(axis = E::NAME, "starting to decode");
        // Depending on the decoding axis, this represents the message matrix's columns (primary)
        // or rows (secondary).
        let mut columns_or_rows = Vec::with_capacity(self.decoders.len());
        let mut decoding_successful = false;

        for sliver in slivers {
            let expected_len = self.decoders.len();
            let expected_symbol_size = self.symbol_size;
            if sliver.symbols.len() != expected_len
                || sliver.symbols.symbol_size() != expected_symbol_size
            {
                // Drop slivers of incorrect length or incorrect symbol size and log a warning.
                tracing::warn!(
                    %sliver,
                    expected_len,
                    expected_symbol_size,
                    "sliver has incorrect length or symbol size"
                );
                continue;
            }
            for (decoder, symbol) in self.decoders.iter_mut().zip(sliver.symbols.to_symbols()) {
                if let Some(decoded_data) = decoder
                    // NOTE: The encoding axis of the following symbol is irrelevant, but since we
                    // are reconstructing from slivers of type `T`, it should be of type `T`.
                    .decode([DecodingSymbol::<E>::new(sliver.index.0, symbol.into())])
                {
                    // If one decoding succeeds, all succeed as they have identical
                    // encoding/decoding matrices.
                    decoding_successful = true;
                    columns_or_rows.push(decoded_data);
                }
            }
            // Stop decoding as soon as we are done.
            if decoding_successful {
                tracing::debug!("decoding finished successfully");
                break;
            }
        }

        if !decoding_successful {
            tracing::debug!("decoding attempt unsuccessful");
            return None;
        }

        let mut blob: Vec<_> = if E::IS_PRIMARY {
            // Primary decoding: transpose columns to get to the original blob.
            let mut columns: Vec<_> = columns_or_rows
                .into_iter()
                .map(|col_index| col_index.into_iter())
                .collect();
            (0..self.config.n_source_symbols::<E>().get())
                .flat_map(|_| {
                    {
                        columns
                            .iter_mut()
                            .map(|column| column.take(self.symbol_size.get().into()))
                    }
                    .flatten()
                    .collect::<Vec<u8>>()
                })
                .collect()
        } else {
            // Secondary decoding: these are the rows and can be used directly as the blob.
            columns_or_rows.into_iter().flatten().collect()
        };

        blob.truncate(self.blob_size);
        tracing::debug!("returning truncated decoded blob");
        Some(blob)
    }

    /// Attempts to decode the source blob from the provided slivers, and to verify that the decoded
    /// blob matches the blob ID.
    ///
    /// Internally, this function uses a [`BlobEncoder`] to recompute the metadata. This metadata is
    /// then compared against the provided [`BlobId`].
    ///
    /// If the decoding and the checks are successful, the function returns a tuple of two values:
    /// * the reconstructed source blob as a byte vector; and
    /// * the [`VerifiedBlobMetadataWithId`] corresponding to the source blob.
    ///
    /// It returns `None` if the decoding fails. If decoding failed due to an insufficient number of
    /// provided slivers, the decoding can be continued by additional calls to
    /// [`decode_and_verify`][Self::decode_and_verify] providing more slivers.
    ///
    /// # Errors
    ///
    /// If, upon successful decoding, the recomputed blob ID does not match the input blob ID,
    /// returns a [`DecodingVerificationError`].
    ///
    /// # Panics
    ///
    /// This function can panic if there is insufficient virtual memory for the encoded data,
    /// notably on 32-bit architectures. As this function re-encodes the blob to verify the
    /// metadata, similar limits apply as in [`BlobEncoder::encode_with_metadata`].
    #[tracing::instrument(skip_all,err(level = Level::INFO))]
    pub fn decode_and_verify(
        &mut self,
        blob_id: &BlobId,
        slivers: impl IntoIterator<Item = SliverData<E>>,
    ) -> Result<Option<(Vec<u8>, VerifiedBlobMetadataWithId)>, DecodingVerificationError> {
        let Some(decoded_blob) = self.decode(slivers) else {
            return Ok(None);
        };
        let blob_metadata = self
            .config
            .compute_metadata(&decoded_blob)
            .expect("the blob size cannot be too large since we were able to decode");
        if blob_metadata.blob_id() == blob_id {
            Ok(Some((decoded_blob, blob_metadata)))
        } else {
            Err(DecodingVerificationError)
        }
    }
}

#[cfg(test)]
mod tests {
    use tracing_subscriber;
    use walrus_test_utils::{param_test, random_data, random_subset};

    use super::*;
    use crate::{
        encoding::{EncodingConfig, RaptorQEncodingConfig, ReedSolomonEncodingConfig},
        metadata::{BlobMetadataApi as _, UnverifiedBlobMetadataWithId},
        EncodingType,
    };

    param_test! {
        test_matrix_construction: [
            aligned_square_single_byte_symbols: (
                2,
                2,
                &[1,2,3,4],
                &[&[1,2], &[3,4]],
                &[&[1,3], &[2,4]]
            ),
            aligned_square_double_byte_symbols: (
                2,
                2,
                &[1,2,3,4,5,6,7,8],
                &[&[1,2,3,4], &[5,6,7,8]],
                &[&[1,2,5,6],&[3,4,7,8]]
            ),
            aligned_rectangle_single_byte_symbols: (
                2,
                4,
                &[1,2,3,4,5,6,7,8],
                &[&[1,2,3,4], &[5,6,7,8]],
                &[&[1,5], &[2,6], &[3,7], &[4,8]]
            ),
            aligned_rectangle_double_byte_symbols: (
                2,
                3,
                &[1,2,3,4,5,6,7,8,9,10,11,12],
                &[&[1,2,3,4,5,6], &[7,8,9,10,11,12]],
                &[&[1,2,7,8], &[3,4,9,10], &[5,6,11,12]]
            ),
            misaligned_square_double_byte_symbols: (
                2,
                2,
                &[1,2,3,4,5],
                &[&[1,2,3,4], &[5,0,0,0]],
                &[&[1,2,5,0],&[3,4,0,0]]
            ),
            misaligned_rectangle_double_byte_symbols: (
                2,
                3,
                &[1,2,3,4,5,6,7,8],
                &[&[1,2,3,4,5,6], &[7,8,0,0,0,0]],
                &[&[1,2,7,8], &[3,4,0,0], &[5,6,0,0]]
            ),
        ]
    }
    fn test_matrix_construction(
        source_symbols_primary: u16,
        source_symbols_secondary: u16,
        blob: &[u8],
        expected_rows: &[&[u8]],
        expected_columns: &[&[u8]],
    ) {
        let config = RaptorQEncodingConfig::new_for_test(
            source_symbols_primary,
            source_symbols_secondary,
            3 * (source_symbols_primary + source_symbols_secondary),
        );
        let blob_encoder = config.get_blob_encoder(blob).unwrap();
        let sliver_pairs = blob_encoder.encode();
        let rows: Vec<_> = sliver_pairs
            .iter()
            .take(blob_encoder.n_rows)
            .map(|pair| pair.primary.symbols.data())
            .collect();
        let columns: Vec<_> = sliver_pairs
            .iter()
            .rev()
            .take(blob_encoder.n_columns)
            .map(|pair| pair.secondary.symbols.data())
            .collect();

        assert_eq!(rows, expected_rows);
        assert_eq!(columns, expected_columns);
    }

    #[test]
    fn test_metadata_computations_are_equal() {
        let blob = random_data(1000);
        let config = RaptorQEncodingConfig::new(NonZeroU16::new(10).unwrap());
        let encoder = config.get_blob_encoder(&blob).unwrap();
        let matrix = encoder.get_expanded_matrix();

        let mut expected_metadata = vec![SliverPairMetadata::new_empty(); matrix.matrix.len()];
        matrix.write_primary_metadata(&mut expected_metadata);
        matrix.write_secondary_metadata(&mut expected_metadata);

        assert_eq!(
            matrix.get_metadata().metadata().hashes(),
            &expected_metadata
        );
    }

    #[test]
    fn test_blob_encode_decode() {
        let blob = random_data(31415);
        let blob_size = blob.len().try_into().unwrap();

        let config = RaptorQEncodingConfig::new(NonZeroU16::new(102).unwrap());

        let slivers_for_decoding: Vec<_> = random_subset(
            config.get_blob_encoder(&blob).unwrap().encode(),
            cmp::max(
                config.source_symbols_primary.get(),
                config.source_symbols_secondary.get(),
            )
            .into(),
        )
        .collect();

        let mut primary_decoder = config.get_blob_decoder::<Primary>(blob_size).unwrap();
        assert_eq!(
            primary_decoder
                .decode(
                    slivers_for_decoding
                        .iter()
                        .cloned()
                        .map(|p| p.primary)
                        .take(config.source_symbols_primary.get().into())
                )
                .unwrap(),
            blob
        );

        let mut secondary_decoder = config.get_blob_decoder::<Secondary>(blob_size).unwrap();
        assert_eq!(
            secondary_decoder
                .decode(
                    slivers_for_decoding
                        .into_iter()
                        .map(|p| p.secondary)
                        .take(config.source_symbols_secondary.get().into())
                )
                .unwrap(),
            blob
        );
    }

    param_test! {
        test_encode_with_metadata: [
            raptorq: (EncodingType::RedStuffRaptorQ),
            reed_solomon: (EncodingType::RS2),
        ]
    }
    fn test_encode_with_metadata(encoding_type: EncodingType) {
        // A big test checking that:
        // 1. The sliver pairs produced by `encode_with_metadata` are the same as the ones produced
        //    by `encode`;
        // 2. the metadata produced by `encode_with_metadata` is the same as
        //    the metadata that can be computed from the sliver pairs directly.
        // 3. the metadata produced by `encode_with_metadata` is the same as
        //    the metadata produced by `compute_metadata_only`.
        // Takes long (O(1s)) to run.
        let blob = random_data(27182);
        let n_shards = 102;

        let config = EncodingConfig::new(NonZeroU16::new(n_shards).unwrap());
        let config_enum = config.get_for_type(encoding_type);

        // Check that the encoding with and without metadata are identical.
        let blob_encoder = match encoding_type {
            EncodingType::RedStuffRaptorQ => config.raptorq.get_blob_encoder(&blob).unwrap(),
            EncodingType::RS2 => config.reed_solomon.get_blob_encoder(&blob).unwrap(),
        };
        let sliver_pairs_1 = blob_encoder.encode();
        let blob_metadata_1 = blob_encoder.compute_metadata();

        let (sliver_pairs_2, blob_metadata_2) = config_enum.encode_with_metadata(&blob).unwrap();
        assert_eq!(sliver_pairs_1, sliver_pairs_2);
        assert_eq!(blob_metadata_1, blob_metadata_2);

        // Check that the hashes obtained by re-encoding the sliver pairs are equivalent to the ones
        // obtained in the `encode_with_metadata` function.
        for (sliver_pair, pair_meta) in sliver_pairs_2
            .iter()
            .zip(blob_metadata_2.metadata().hashes().iter())
        {
            let pair_hash = sliver_pair
                .pair_leaf_input::<Blake2b256>(&config_enum)
                .expect("should be able to encode");
            let meta_hash = pair_meta.pair_leaf_input::<Blake2b256>();
            assert_eq!(pair_hash, meta_hash);
        }

        // Check that the blob metadata verifies.
        let unverified = UnverifiedBlobMetadataWithId::new(
            *blob_metadata_2.blob_id(),
            blob_metadata_2.metadata().clone(),
        );
        assert!(unverified.verify(&config).is_ok());
    }

    #[test]
    fn test_encode_decode_and_verify() {
        let blob = random_data(16180);
        let blob_size = blob.len().try_into().unwrap();
        let n_shards = 102;

        let config = RaptorQEncodingConfig::new(NonZeroU16::new(n_shards).unwrap());

        let (slivers, metadata_enc) = config
            .get_blob_encoder(&blob)
            .unwrap()
            .encode_with_metadata();
        let slivers_for_decoding =
            random_subset(slivers, config.source_symbols_primary.get().into())
                .map(|s| s.primary)
                .collect::<Vec<_>>();
        let (blob_dec, metadata_dec) = config
            .get_blob_decoder(blob_size)
            .unwrap()
            .decode_and_verify(metadata_enc.blob_id(), slivers_for_decoding)
            .unwrap()
            .unwrap();

        assert_eq!(blob, blob_dec);
        assert_eq!(metadata_enc, metadata_dec);
    }

    fn min_required_columns(blobs: &[usize], length: usize) -> usize {
        if length == 0 {
            return usize::MAX;
        }
        let mut used_cols = 0;
        for &blob in blobs {
            used_cols += (blob - 1) / length + 1;
        }
        used_cols
    }

    param_test! {
        test_find_min_length: [
            not_fit: (&[2, 1, 2, 1], 3, 3, None),
            single_large_blob: (&[1000, 1, 1], 4, 7, Some(72)),
            // empty_input: (&[], 3, 1, Some(0)),
            single_small_blob: (&[1], 3, 2, Some(1)),
            impossible_case: (&[15, 8, 4], 4, 2, Some(4)),
            perfect_fit: (&[2, 2, 2], 3, 1, Some(2)),
            with_empty_columns: (&[5, 5, 5], 5, 1, Some(5)),
            with_many_columns: (&[25, 35, 45], 200, 1, Some(1))
        ]
    }
    fn test_find_min_length(blobs: &[usize], nc: usize, nr: usize, expected: Option<usize>) {
        // Initialize tracing subscriber for this test
        let _guard = tracing_subscriber::fmt().try_init();
        let res = compute_symbol_size(blobs, nc, nr, usize::MAX);
        tracing::info!("res: {:?}", res);
        assert_eq!(res, expected);
        if let Some(min_size) = res {
            assert!(min_required_columns(blobs, min_size * nr) <= nc);
            assert!(min_required_columns(blobs, min_size * nr - nr) > nc);
        }
    }

    #[test]
    fn test_quilt_encoder_rows() {
        let _guard = tracing_subscriber::fmt().try_init();
        tracing::info!("Starting test_quilt_encoder_rows");

        // Create BlobWithDesc directly with static strings.
        let blobs_with_desc = &[
            BlobWithDesc {
                blob: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..],
                desc: "test blob 0",
            },
            BlobWithDesc {
                blob: &[5, 68, 3, 2, 5][..],
                desc: "test blob 1",
            },
            BlobWithDesc {
                blob: &[5, 68, 3, 2, 5, 6, 78, 8][..],
                desc: "test blob 2",
            },
        ];

        let config = RaptorQEncodingConfig::new_for_test(3, 5, 7);
        let config_enum = EncodingConfigEnum::RaptorQ(&config);
        let nr = config.source_symbols_primary.get() as usize;

        // Calculate blob IDs directly from blobs_with_desc.
        let blob_ids: Vec<BlobId> = blobs_with_desc
            .iter()
            .map(|blob_with_desc| {
                let encoder = BlobEncoder::new(config_enum.clone(), blob_with_desc.blob)
                    .expect("Should create encoder");
                *encoder.compute_metadata().blob_id()
            })
            .collect();

        let encoder = QuiltEncoder::new(config_enum, blobs_with_desc);
        let quilt = encoder
            .construct_quilt()
            .expect("should be able to construct quilt");
        tracing::info!("Quilt: {:?}", quilt);

        // Verify each blob and its description.
        for (blob_with_desc, blob_id) in blobs_with_desc.iter().zip(blob_ids.iter()) {
            let retrieved_blob = quilt
                .get_blob_by_id(blob_id)
                .expect("Should find blob by ID");

            // Verify blob data matches
            assert_eq!(
                &retrieved_blob, blob_with_desc.blob,
                "Mismatch in encoded blob"
            );

            // Verify description matches
            let block = quilt
                .blocks
                .iter()
                .find(|block| block.blob_id == *blob_id)
                .expect("Block should exist for this blob ID");

            assert_eq!(
                block.desc, blob_with_desc.desc,
                "Mismatch in blob description"
            );
        }

        let (sliver_pairs, metadata) = encoder.encode_with_metadata();

        // Format primary slivers as a 2D matrix (rows)
        let mut primary_matrix = String::from("Primary Slivers Matrix (rows):\n");

        // Add column headers
        primary_matrix.push_str("     ");
        for col_idx in 0..sliver_pairs[0].primary.symbols.data().len() {
            primary_matrix.push_str(&format!("{:02} ", col_idx));
        }
        primary_matrix.push('\n');

        // Add separator line
        primary_matrix.push_str("    ");
        for _ in 0..sliver_pairs[0].primary.symbols.data().len() {
            primary_matrix.push_str("---");
        }
        primary_matrix.push('\n');

        // Add each row with row number
        for (row_idx, sliver_pair) in sliver_pairs.iter().enumerate() {
            let primary_data = sliver_pair.primary.symbols.data();

            // Add row number
            primary_matrix.push_str(&format!("{:02}: ", row_idx));

            // Format each byte in the row
            for byte in primary_data {
                primary_matrix.push_str(&format!("{:02x} ", byte));
            }
            primary_matrix.push('\n');
        }

        // Log the primary matrix
        tracing::info!("\n{}", primary_matrix);

        // Format secondary slivers as a 2D matrix (columns)
        let mut secondary_matrix = String::from("Secondary Slivers Matrix (columns):\n");

        // Add column headers
        secondary_matrix.push_str("     ");
        for col_idx in 0..sliver_pairs.len() {
            secondary_matrix.push_str(&format!("{:02} ", col_idx));
        }
        secondary_matrix.push('\n');

        // Add separator line
        secondary_matrix.push_str("    ");
        for _ in 0..sliver_pairs.len() {
            secondary_matrix.push_str("---");
        }
        secondary_matrix.push('\n');

        // Find the maximum length of any secondary sliver
        let max_secondary_len = sliver_pairs
            .iter()
            .map(|pair| pair.secondary.symbols.data().len())
            .max()
            .unwrap_or(0);

        // Add each row of the secondary slivers matrix with row number
        for row_idx in 0..max_secondary_len {
            // Add row number
            secondary_matrix.push_str(&format!("{:02}: ", row_idx));

            for sliver_pair in &sliver_pairs {
                let secondary_data = sliver_pair.secondary.symbols.data();

                // Format the byte at this position, or a placeholder if out of bounds
                if row_idx < secondary_data.len() {
                    secondary_matrix.push_str(&format!("{:02x} ", secondary_data[row_idx]));
                } else {
                    secondary_matrix.push_str("-- ");
                }
            }
            secondary_matrix.push('\n');
        }

        // Log the secondary matrix

        let row_size = quilt.row_size;

        // Verify primary source slivers match quilt data.
        for (i, sliver) in sliver_pairs.iter().take(nr).enumerate() {
            let expected_row = &quilt.data[i * row_size..(i + 1) * row_size];
            assert_eq!(
                sliver.primary.symbols.data(),
                expected_row,
                "Row {} mismatch in primary sliver",
                i
            );
        }

        // Sort QuiltBlocks by end_index to ensure we process them in order
        let quilt_blocks = metadata.blocks.clone();
        // quilt_blocks.sort_by_key(|block| block.end_index);

        // Verify that each blob can be reconstructed from the secondary slivers
        let mut prev_end_index = 0;

        for quilt_block in quilt_blocks {
            // Find the original blob with this blob_id for comparison
            let original_blob = blobs_with_desc
                .iter()
                .zip(blob_ids.iter())
                .find(|(_blob_with_desc, id)| *id == &quilt_block.blob_id)
                .expect("Should find original blob")
                .0
                .blob;

            // Extract the blob data from the secondary slivers
            let mut reconstructed_data = Vec::new();

            // Simply concatenate all secondary slivers from prev_end_index to end_index
            let n_slivers = sliver_pairs.len();
            for i in prev_end_index..quilt_block.end_index {
                let idx = i as usize;
                if idx < n_slivers {
                    let secondary_data = sliver_pairs[n_slivers - 1 - idx].secondary.symbols.data();
                    reconstructed_data.extend_from_slice(secondary_data);
                }
            }

            // Truncate to unencoded_length if needed
            if reconstructed_data.len() > quilt_block.unencoded_length as usize {
                reconstructed_data.truncate(quilt_block.unencoded_length as usize);
            }

            // Verify the reconstructed blob matches the original
            assert_eq!(
                reconstructed_data, original_blob,
                "Blob with ID {:?} could not be correctly reconstructed from secondary slivers",
                quilt_block.blob_id
            );

            // Update prev_end_index for the next blob
            prev_end_index = quilt_block.end_index;
        }
    }

    param_test! {
        test_quilt_encoder: [
            case_0: (
                &[
                    BlobWithDesc {
                        blob: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..],
                        desc: "test blob 0",
                    },
                    BlobWithDesc {
                        blob: &[5, 68, 3, 2, 5][..],
                        desc: "test blob 1",
                    },
                    BlobWithDesc {
                        blob: &[5, 68, 3, 2, 5, 6, 78, 8][..],
                        desc: "test blob 2",
                    },
                ],
                3, 5, 7
            ),
            case_0_random_order: (
                &[
                    BlobWithDesc {
                        blob: &[5, 68, 3, 2, 5, 6, 78, 8][..],
                        desc: "test blob 0",
                    },
                    BlobWithDesc {
                        blob: &[5, 68, 3, 2, 5][..],
                        desc: "test blob 1",
                    },
                    BlobWithDesc {
                        blob: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..],
                        desc: "test blob 2",
                    },
                ],
                3, 5, 7
            ),
            case_1: (
                &[
                    BlobWithDesc {
                        blob: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..],
                        desc: "test blob 0",
                    },
                    BlobWithDesc {
                        blob: &[5, 68, 3, 2, 5][..],
                        desc: "test blob 1",
                    },
                    BlobWithDesc {
                        blob: &[5, 68, 3, 2, 5, 6, 78, 8][..],
                        desc: "test blob 2",
                    },
                ],
                3, 5, 7
            ),
            case_1_random_order: (
                &[
                    BlobWithDesc {
                        blob: &[5, 68, 3, 2, 5][..],
                        desc: "test blob 0",
                    },
                    BlobWithDesc {
                        blob: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..],
                        desc: "test blob 1",
                    },
                    BlobWithDesc {
                        blob: &[5, 68, 3, 2, 5, 6, 78, 8][..],
                        desc: "test blob 2",
                    },
                ],
                3, 5, 7
            ),
            case_2: (
                &[
                    BlobWithDesc { blob: &[1, 3][..], desc: "test blob 0" },
                    BlobWithDesc { blob: &[255u8; 1024][..], desc: "test blob 1" },
                    BlobWithDesc { blob: &[1, 2, 3][..], desc: "test blob 2" },
                ],
                4, 8, 12
            ),
            case_3: (
                &[
                    BlobWithDesc { blob: &[9, 8, 7, 6, 5, 4, 3, 2, 1][..], desc: "test blob 0" },
                ],
                3, 5, 7
            ),
        ]
    }
    fn test_quilt_encoder(
        blobs_with_desc: &[BlobWithDesc<'_>],
        source_symbols_primary: u16,
        source_symbols_secondary: u16,
        n_shards: u16,
    ) {
        let raptorq_config = RaptorQEncodingConfig::new_for_test(
            source_symbols_primary,
            source_symbols_secondary,
            n_shards,
        );
        let reed_solomon_config = ReedSolomonEncodingConfig::new_for_test(
            source_symbols_primary,
            source_symbols_secondary,
            n_shards,
        );

        test_construct_quilt(
            blobs_with_desc,
            EncodingConfigEnum::RaptorQ(&raptorq_config),
        );
        test_construct_quilt(
            blobs_with_desc,
            EncodingConfigEnum::ReedSolomon(&reed_solomon_config),
        );
    }

    fn test_construct_quilt(blobs_with_desc: &[BlobWithDesc<'_>], config: EncodingConfigEnum) {
        let _guard = tracing_subscriber::fmt().try_init();
        let nr = config.n_source_symbols::<Primary>().get() as usize;

        // Calculate blob IDs directly from blobs_with_desc.
        let blob_ids: Vec<BlobId> = blobs_with_desc
            .iter()
            .map(|blob_with_desc| {
                let encoder = BlobEncoder::new(config.clone(), blob_with_desc.blob)
                    .expect("Should create encoder");
                *encoder.compute_metadata().blob_id()
            })
            .collect();

        let encoder = QuiltEncoder::new(config, blobs_with_desc);
        let quilt = encoder.construct_quilt().expect("Should construct quilt");
        tracing::info!("Quilt: {:?}", quilt);

        // Verify each blob and its description.
        for (blob_with_desc, blob_id) in blobs_with_desc.iter().zip(blob_ids.iter()) {
            let retrieved_blob = quilt
                .get_blob_by_id(blob_id)
                .expect("Should find blob by ID");

            // Verify blob data matches
            assert_eq!(
                &retrieved_blob, blob_with_desc.blob,
                "Mismatch in encoded blob"
            );

            // Verify description matches
            let block = quilt
                .blocks
                .iter()
                .find(|block| block.blob_id == *blob_id)
                .expect("Block should exist for this blob ID");

            assert_eq!(
                block.desc, blob_with_desc.desc,
                "Mismatch in blob description"
            );
        }

        let (sliver_pairs, metadata) = encoder.encode_with_metadata();

        // Format primary slivers as a 2D matrix (rows)
        let mut primary_matrix = String::from("Primary Slivers Matrix (rows):\n");

        // Add column headers
        primary_matrix.push_str("     ");
        for col_idx in 0..sliver_pairs[0].primary.symbols.data().len() {
            primary_matrix.push_str(&format!("{:02} ", col_idx));
        }
        primary_matrix.push('\n');

        // Add separator line
        primary_matrix.push_str("    ");
        for _ in 0..sliver_pairs[0].primary.symbols.data().len() {
            primary_matrix.push_str("---");
        }
        primary_matrix.push('\n');

        // Add each row with row number
        for (row_idx, sliver_pair) in sliver_pairs.iter().enumerate() {
            let primary_data = sliver_pair.primary.symbols.data();

            // Add row number
            primary_matrix.push_str(&format!("{:02}: ", row_idx));

            // Format each byte in the row
            for byte in primary_data {
                primary_matrix.push_str(&format!("{:02x} ", byte));
            }
            primary_matrix.push('\n');
        }

        // Log the primary matrix
        tracing::info!("\n{}", primary_matrix);

        // Format secondary slivers as a 2D matrix (columns)
        let mut secondary_matrix = String::from("Secondary Slivers Matrix (columns):\n");

        // Add column headers
        secondary_matrix.push_str("     ");
        for col_idx in 0..sliver_pairs.len() {
            secondary_matrix.push_str(&format!("{:02} ", col_idx));
        }
        secondary_matrix.push('\n');

        // Add separator line
        secondary_matrix.push_str("    ");
        for _ in 0..sliver_pairs.len() {
            secondary_matrix.push_str("---");
        }
        secondary_matrix.push('\n');

        // Find the maximum length of any secondary sliver
        let max_secondary_len = sliver_pairs
            .iter()
            .map(|pair| pair.secondary.symbols.data().len())
            .max()
            .unwrap_or(0);

        // Add each row of the secondary slivers matrix with row number
        for row_idx in 0..max_secondary_len {
            // Add row number
            secondary_matrix.push_str(&format!("{:02}: ", row_idx));

            for sliver_pair in &sliver_pairs {
                let secondary_data = sliver_pair.secondary.symbols.data();

                // Format the byte at this position, or a placeholder if out of bounds
                if row_idx < secondary_data.len() {
                    secondary_matrix.push_str(&format!("{:02x} ", secondary_data[row_idx]));
                } else {
                    secondary_matrix.push_str("-- ");
                }
            }
            secondary_matrix.push('\n');
        }

        // Log the secondary matrix
        tracing::info!("\n{}", secondary_matrix);

        let row_size = quilt.row_size;

        // Verify primary source slivers match quilt data.
        for (i, sliver) in sliver_pairs.iter().take(nr).enumerate() {
            let expected_row = &quilt.data[i * row_size..(i + 1) * row_size];
            assert_eq!(
                sliver.primary.symbols.data(),
                expected_row,
                "Row {} mismatch in primary sliver",
                i
            );
        }

        // Sort QuiltBlocks by end_index to ensure we process them in order
        let mut quilt_blocks = metadata.blocks.clone();
        quilt_blocks.sort_by_key(|block| block.end_index);

        // Verify that each blob can be reconstructed from the secondary slivers
        let mut prev_end_index = 0;

        for quilt_block in quilt_blocks {
            // Find the original blob with this blob_id for comparison
            let original_blob = blobs_with_desc
                .iter()
                .zip(blob_ids.iter())
                .find(|(_blob_with_desc, id)| *id == &quilt_block.blob_id)
                .expect("Should find original blob")
                .0
                .blob;

            // Extract the blob data from the secondary slivers
            let mut reconstructed_data = Vec::new();

            // Simply concatenate all secondary slivers from prev_end_index to end_index
            for i in prev_end_index..quilt_block.end_index {
                let idx = i as usize;
                if idx < sliver_pairs.len() {
                    let secondary_data = sliver_pairs[sliver_pairs.len() - 1 - idx]
                        .secondary
                        .symbols
                        .data();
                    reconstructed_data.extend_from_slice(secondary_data);
                }
            }

            // Truncate to unencoded_length if needed
            if reconstructed_data.len() > quilt_block.unencoded_length as usize {
                reconstructed_data.truncate(quilt_block.unencoded_length as usize);
            }

            // Verify the reconstructed blob matches the original
            assert_eq!(
                reconstructed_data, original_blob,
                "Blob with ID {:?} could not be correctly reconstructed from secondary slivers",
                quilt_block.blob_id
            );

            // Update prev_end_index for the next blob
            prev_end_index = quilt_block.end_index;
        }
    }
    param_test! {
        test_construct_container_quilt: [
            case_0: (
                &[
                    BlobWithDesc {
                        blob: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..],
                        desc: "test blob 0",
                    },
                    BlobWithDesc {
                        blob: &[5, 68, 3, 2, 5][..],
                        desc: "test blob 1",
                    },
                    BlobWithDesc {
                        blob: &[5, 68, 3, 2, 5, 6, 78, 8][..],
                        desc: "test blob 2",
                    },
                ],
                3, 5, 7
            ),
            case_0_random_order: (
                &[
                    BlobWithDesc {
                        blob: &[5, 68, 3, 2, 5, 6, 78, 8][..],
                        desc: "test blob 0",
                    },
                    BlobWithDesc {
                        blob: &[5, 68, 3, 2, 5][..],
                        desc: "test blob 1",
                    },
                    BlobWithDesc {
                        blob: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..],
                        desc: "test blob 2",
                    },
                ],
                3, 5, 7
            ),
            case_1: (
                &[
                    BlobWithDesc {
                        blob: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..],
                        desc: "test blob 0",
                    },
                    BlobWithDesc {
                        blob: &[5, 68, 3, 2, 5][..],
                        desc: "test blob 1",
                    },
                    BlobWithDesc {
                        blob: &[5, 68, 3, 2, 5, 6, 78, 8][..],
                        desc: "test blob 2",
                    },
                ],
                3, 5, 7
            ),
            case_1_random_order: (
                &[
                    BlobWithDesc {
                        blob: &[5, 68, 3, 2, 5][..],
                        desc: "test blob 0",
                    },
                    BlobWithDesc {
                        blob: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..],
                        desc: "test blob 1",
                    },
                    BlobWithDesc {
                        blob: &[5, 68, 3, 2, 5, 6, 78, 8][..],
                        desc: "test blob 2",
                    },
                ],
                3, 5, 7
            ),
            case_2: (
                &[
                    BlobWithDesc { blob: &[1, 3][..], desc: "test blob 0" },
                    BlobWithDesc { blob: &[255u8; 1024][..], desc: "test blob 1" },
                    BlobWithDesc { blob: &[1, 2, 3][..], desc: "test blob 2" },
                ],
                4, 8, 12
            ),
            case_3: (
                &[
                    BlobWithDesc { blob: &[9, 8, 7, 6, 5, 4, 3, 2, 1][..], desc: "test blob 0" },
                ],
                3, 5, 7
            ),
        ]
    }
    fn test_construct_container_quilt(
        blobs_with_desc: &[BlobWithDesc<'_>],
        source_symbols_primary: u16,
        source_symbols_secondary: u16,
        n_shards: u16,
    ) {
        let raptorq_config = RaptorQEncodingConfig::new_for_test(
            source_symbols_primary,
            source_symbols_secondary,
            n_shards,
        );
        let reed_solomon_config = ReedSolomonEncodingConfig::new_for_test(
            source_symbols_primary,
            source_symbols_secondary,
            n_shards,
        );

        construct_container_quilt(
            blobs_with_desc,
            EncodingConfigEnum::RaptorQ(&raptorq_config),
        );
        construct_container_quilt(
            blobs_with_desc,
            EncodingConfigEnum::ReedSolomon(&reed_solomon_config),
        );
    }

    fn construct_container_quilt(blobs_with_desc: &[BlobWithDesc<'_>], config: EncodingConfigEnum) {
        let _guard = tracing_subscriber::fmt().try_init();

        // Calculate blob IDs directly from blobs_with_desc.
        let blob_ids: Vec<BlobId> = blobs_with_desc
            .iter()
            .map(|blob_with_desc| {
                let encoder = BlobEncoder::new(config.clone(), blob_with_desc.blob)
                    .expect("Should create encoder");
                *encoder.compute_metadata().blob_id()
            })
            .collect();

        let encoder = QuiltEncoder::new(config, blobs_with_desc);
        let quilt = encoder
            .construct_container_quilt()
            .expect("Should construct quilt");
        tracing::info!("Quilt: {:?}", quilt);

        // Verify each blob and its description.
        for (blob_with_desc, blob_id) in blobs_with_desc.iter().zip(blob_ids.iter()) {
            let retrieved_blob = quilt
                .get_blob_by_id(blob_id)
                .expect("Should find blob by ID");

            // Verify blob data matches
            assert_eq!(
                &retrieved_blob, blob_with_desc.blob,
                "Mismatch in encoded blob"
            );

            // Verify description matches
            let block = quilt
                .blocks
                .iter()
                .find(|block| block.blob_id == *blob_id)
                .expect("Block should exist for this blob ID");

            assert_eq!(
                block.desc, blob_with_desc.desc,
                "Mismatch in blob description"
            );
        }

        let quilt_index_blob = quilt
            .get_blob_by_id(&BlobId::ZERO)
            .expect("Should find quilt index blob");

        // When decoding the quilt index:

        // First, read the first 8 bytes to get the data size
        let data_size = u64::from_le_bytes(quilt_index_blob[0..8].try_into().unwrap());

        // Then, decode the QuiltIndex from the next data_size bytes
        let container_quilt_index: QuiltIndex =
            bcs::from_bytes(&quilt_index_blob[8..(data_size as usize)])
                .expect("Failed to decode QuiltIndex");
        tracing::info!("QuiltIndex: {:?}", container_quilt_index);
        assert_eq!(
            container_quilt_index.quilt_blocks.len(),
            blobs_with_desc.len()
        );
    }

    param_test! {
        test_quilt_decoder_with_quilt_index: [
            case_0: (
                &[
                    BlobWithDesc {
                        blob: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..],
                        desc: "test blob 0",
                    },
                    BlobWithDesc {
                        blob: &[5, 68, 3, 2, 5][..],
                        desc: "test blob 1",
                    },
                    BlobWithDesc {
                        blob: &[5, 68, 3, 2, 5, 6, 78, 8][..],
                        desc: "test blob 2",
                    },
                ],
                3, 5, 7
            ),
            case_0_random_order: (
                &[
                    BlobWithDesc {
                        blob: &[5, 68, 3, 2, 5, 6, 78, 8][..],
                        desc: "test blob 0",
                    },
                    BlobWithDesc {
                        blob: &[5, 68, 3, 2, 5][..],
                        desc: "test blob 1",
                    },
                    BlobWithDesc {
                        blob: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..],
                        desc: "test blob 2",
                    },
                ],
                3, 5, 7
            ),
            case_1: (
                &[
                    BlobWithDesc {
                        blob: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..],
                        desc: "test blob 0",
                    },
                    BlobWithDesc {
                        blob: &[5, 68, 3, 2, 5][..],
                        desc: "test blob 1",
                    },
                    BlobWithDesc {
                        blob: &[5, 68, 3, 2, 5, 6, 78, 8][..],
                        desc: "test blob 2",
                    },
                ],
                3, 5, 7
            ),
            case_1_random_order: (
                &[
                    BlobWithDesc {
                        blob: &[5, 68, 3, 2, 5][..],
                        desc: "test blob 0",
                    },
                    BlobWithDesc {
                        blob: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..],
                        desc: "test blob 1",
                    },
                    BlobWithDesc {
                        blob: &[5, 68, 3, 2, 5, 6, 78, 8][..],
                        desc: "test blob 2",
                    },
                ],
                3, 5, 7
            ),
            case_2: (
                &[
                    BlobWithDesc { blob: &[1, 3][..], desc: "test blob 0" },
                    BlobWithDesc { blob: &[255u8; 1024][..], desc: "test blob 1" },
                    BlobWithDesc { blob: &[1, 2, 3][..], desc: "test blob 2" },
                ],
                4, 8, 12
            ),
            case_3: (
                &[
                    BlobWithDesc { blob: &[9, 8, 7, 6, 5, 4, 3, 2, 1][..], desc: "test blob 0" },
                ],
                3, 5, 7
            ),
        ]
    }
    fn test_quilt_decoder_with_quilt_index(
        blobs_with_desc: &[BlobWithDesc<'_>],
        source_symbols_primary: u16,
        source_symbols_secondary: u16,
        n_shards: u16,
    ) {
        let raptorq_config = RaptorQEncodingConfig::new_for_test(
            source_symbols_primary,
            source_symbols_secondary,
            n_shards,
        );
        let reed_solomon_config = ReedSolomonEncodingConfig::new_for_test(
            source_symbols_primary,
            source_symbols_secondary,
            n_shards,
        );

        quilt_decoder_with_quilt_index(
            blobs_with_desc,
            EncodingConfigEnum::RaptorQ(&raptorq_config),
        );
        quilt_decoder_with_quilt_index(
            blobs_with_desc,
            EncodingConfigEnum::ReedSolomon(&reed_solomon_config),
        );
    }

    fn quilt_decoder_with_quilt_index(
        blobs_with_desc: &[BlobWithDesc<'_>],
        config: EncodingConfigEnum,
    ) {
        let _guard = tracing_subscriber::fmt().try_init();
        let n_shards = config.n_shards();

        // Calculate blob IDs directly from blobs_with_desc.
        let blob_ids: Vec<BlobId> = blobs_with_desc
            .iter()
            .map(|blob_with_desc| {
                let encoder = BlobEncoder::new(config.clone(), blob_with_desc.blob)
                    .expect("Should create encoder");
                *encoder.compute_metadata().blob_id()
            })
            .collect();

        let encoder = QuiltEncoder::new(config, blobs_with_desc);
        let quilt = encoder
            .construct_container_quilt()
            .expect("Should construct quilt");
        tracing::info!("Quilt: {:?}", quilt);

        let (sliver_pairs, quilt_metadata) = encoder
            .encode_with_quilt_index_and_metadata()
            .expect("Should encode with quilt index and metadata");
        tracing::info!("Sliver pairs: {:?}", sliver_pairs);
        tracing::info!("Quilt metadata: {:?}", quilt_metadata);
        let slivers: Vec<&SliverData<Secondary>> = sliver_pairs
            .iter()
            .map(|sliver_pair| &sliver_pair.secondary)
            .collect();

        let mut decoder = QuiltDecoder::new(n_shards, &[]);
        // decoder = decoder.add_slivers(&slivers);
        let quilt_index = decoder.decode_quilt_index();
        assert!(quilt_index.is_none());
        decoder = decoder.add_slivers(&slivers);
        let quilt_index = decoder.decode_quilt_index();
        assert!(quilt_index.is_some());
        tracing::info!("Quilt index: {:?}", quilt_index);

        for (blob_with_desc, blob_id) in blobs_with_desc.iter().zip(blob_ids.iter()) {
            let blob = decoder.get_blob_by_id(blob_id);
            assert_eq!(blob, Some(blob_with_desc.blob.to_vec()));
            let blob = decoder.get_blob_by_desc(blob_with_desc.desc);
            assert_eq!(blob, Some(blob_with_desc.blob.to_vec()));
        }
    }
}
