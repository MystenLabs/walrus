// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use alloc::{
    format,
    string::{String, ToString},
    vec,
    vec::Vec,
};
use core::{cmp, fmt, num::NonZeroU16};

use hex;
use serde::{Deserialize, Serialize};
use tracing::{Level, Span};

use super::{EncodingConfig, EncodingConfigEnum, Primary, Secondary, SliverData, SliverPair};
use crate::{
    encoding::{blob_encoding::BlobEncoder, config::EncodingConfigTrait as _, QuiltError},
    metadata::{QuiltBlockV1, QuiltIndexV1, QuiltMetadataV1},
    SliverIndex,
};

/// The number of bytes to store the size of the quilt index.
const QUILT_INDEX_SIZE_PREFIX_SIZE: usize = 8;

/// The number of bytes used to store the type of the quilt.
const QUILT_TYPE_SIZE: usize = 1;

/// The maximum number of columns a quilt index can have.
const MAX_NUM_COLUMNS_FOR_QUILT_INDEX: usize = 1;

pub trait QuiltVersion {
    type QuiltConfig;
    type QuiltEncoder<'a>;
    type QuiltDecoder<'a>;
    type Quilt;
    type QuiltIndex;
    type QuiltMetadataV1;

    /// The serialized bytes of the quilt type.
    fn quilt_type_bytes() -> &'static [u8];
}

/// The version of the quilt.
#[allow(dead_code)] // TODO: remove this once follow up PRs are merged.
pub enum QuiltVersionEnum {
    V1,
    Invalid,
}

impl QuiltVersionEnum {
    /// Creates a new `QuiltVersionEnum` from its serialized bytes.
    #[allow(dead_code)] // TODO: remove this once follow up PRs are merged.
    pub fn new_from_bytes(type_bytes: &[u8]) -> QuiltVersionEnum {
        match type_bytes {
            &[0x00] => QuiltVersionEnum::V1,
            _ => QuiltVersionEnum::Invalid,
        }
    }
}

/// The configuration of the quilt.
#[allow(dead_code)] // TODO: remove this once follow up PRs are merged.
pub trait QuiltConfigApi<'a, V: QuiltVersion> {
    /// Returns a new encoder for the given configuration and blobs.
    fn get_encoder(
        encoding_config: EncodingConfigEnum<'a>,
        blobs: &'a [BlobWithIdentifier<'a>],
    ) -> V::QuiltEncoder<'a>;

    /// Returns a new decoder for the given slivers.
    fn get_decoder(slivers: &'a [&'a SliverData<Secondary>]) -> V::QuiltDecoder<'a>;

    /// Constructs a quilt from a quilt blob.
    ///
    /// `quilt_blob` is a quilt constructed from a set of blobs.
    /// This function loads the quilt blob to access its internal structures without having
    /// to re-encode the blobs.
    fn parse_from_quilt_blob(
        quilt_blob: Vec<u8>,
        metadata: &V::QuiltMetadataV1,
        n_shards: NonZeroU16,
    ) -> Result<V::Quilt, QuiltError>;
}

/// The encoder of the quilt.
#[allow(dead_code)] // TODO: remove this once follow up PRs are merged.
pub trait QuiltEncoderApi<V: QuiltVersion> {
    /// Constructs a quilt by encoding the blobs.
    fn construct_quilt(&self) -> Result<V::Quilt, QuiltError>;

    /// Encodes the blobs into a quilt and returns the slivers.
    fn encode(&self) -> Result<Vec<SliverPair>, QuiltError>;

    /// Encodes the blobs into a quilt and returns the slivers and metadata.
    fn encode_with_metadata(&self) -> Result<(Vec<SliverPair>, V::QuiltMetadataV1), QuiltError>;
}

/// The decoder of the quilt.
#[allow(dead_code)] // TODO: remove this once follow up PRs are merged.
pub trait QuiltDecoderApi<'a, V: QuiltVersion> {
    /// Decodes the quilt index from received slivers.
    ///
    /// The decoded quilt index is stored in the decoder and can be retrieved
    /// using the `get_quilt_index` method after this method returns.
    fn decode_quilt_index(&mut self) -> Result<&V::QuiltIndex, QuiltError>;

    /// Returns the decoded quilt index, if available.
    fn get_quilt_index(&self) -> Option<&V::QuiltIndex>;

    /// Gets a blob by its identifier from the quilt.
    fn get_blob_by_identifier(&self, identifier: &str) -> Result<Vec<u8>, QuiltError>;

    /// Adds slivers to the decoder.
    fn add_slivers(&mut self, slivers: &'a [&'a SliverData<Secondary>]);
}

/// The API of the quilt.
pub trait QuiltApi<V: QuiltVersion> {
    /// Gets a blob by its identifier from the quilt.
    #[allow(dead_code)] // TODO: remove this once follow up PRs are merged.
    fn get_blob_by_identifier(&self, identifier: &str) -> Result<Vec<u8>, QuiltError>;

    /// Returns the quilt index.
    fn quilt_index(&self) -> &V::QuiltIndex;

    /// Returns the data of the quilt.
    fn data(&self) -> &[u8];

    /// Returns the symbol size of the quilt.
    fn symbol_size(&self) -> usize;
}

/// A wrapper around a blob and its identifier.
#[derive(Debug)]
pub struct BlobWithIdentifier<'a> {
    blob: &'a [u8],
    identifier: String,
}

impl<'a> BlobWithIdentifier<'a> {
    /// Creates a new `BlobWithIdentifier` from a blob and an identifier.
    pub fn new(blob: &'a [u8], identifier: impl Into<String>) -> Self {
        Self {
            blob,
            identifier: identifier.into(),
        }
    }

    /// Returns the length of the blob.
    pub fn len(&self) -> usize {
        self.blob.len()
    }
}

/// Quilt version 1.
pub struct QuiltVersionV1;

impl QuiltVersionV1 {
    const QUILT_TYPE_BYTES: &'static [u8] = &[0x00];
}

impl QuiltVersion for QuiltVersionV1 {
    type QuiltConfig = QuiltConfigV1;
    type QuiltEncoder<'a> = QuiltEncoderV1<'a>;
    type QuiltDecoder<'a> = QuiltDecoderV1<'a>;
    type Quilt = QuiltV1;
    type QuiltIndex = QuiltIndexV1;
    type QuiltMetadataV1 = QuiltMetadataV1;

    fn quilt_type_bytes() -> &'static [u8] {
        QuiltVersionV1::QUILT_TYPE_BYTES
    }
}

/// The configuration of a quilt.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct QuiltConfigV1 {}

impl<'a> QuiltConfigApi<'a, QuiltVersionV1> for QuiltConfigV1 {
    fn get_encoder(
        encoding_config: EncodingConfigEnum<'a>,
        blobs: &'a [BlobWithIdentifier<'a>],
    ) -> QuiltEncoderV1<'a> {
        QuiltEncoderV1::new(encoding_config, blobs)
    }

    fn get_decoder(slivers: &'a [&'a SliverData<Secondary>]) -> QuiltDecoderV1<'a> {
        QuiltDecoderV1::new(slivers)
    }

    fn parse_from_quilt_blob(
        quilt_blob: Vec<u8>,
        metadata: &QuiltMetadataV1,
        n_shards: NonZeroU16,
    ) -> Result<QuiltV1, QuiltError> {
        let encoding_config = EncodingConfig::new(n_shards);
        let config = encoding_config.get_for_type(metadata.metadata.encoding_type());

        let n_primary_source_symbols = config.n_primary_source_symbols().get();
        let n_secondary_source_symbols = config.n_secondary_source_symbols().get();
        let n_source_symbols = n_primary_source_symbols * n_secondary_source_symbols;

        // Verify data alignment.
        if quilt_blob.len() % usize::from(n_source_symbols) != 0 {
            return Err(QuiltError::invalid_format_not_aligned());
        }

        // Calculate matrix dimensions.
        let row_size = quilt_blob.len() / usize::from(n_primary_source_symbols);
        let symbol_size = row_size / usize::from(n_secondary_source_symbols);

        // Extract quilt index size and calculate required columns.
        let quilt_index = utils::get_quilt_index_v1_from_data(&quilt_blob, row_size, symbol_size)?;
        assert_eq!(quilt_index, metadata.index);

        Ok(QuiltV1 {
            data: quilt_blob,
            row_size,
            quilt_index,
            symbol_size,
        })
    }
}

/// A quilt is a collection of encoded blobs stored together in a unified structure.
///
/// The data is organized as a 2D matrix where:
/// - Each blob occupies a continuous range of columns (secondary slivers).
/// - The first column's initial `QUILT_INDEX_SIZE_PREFIX_SIZE` bytes contain the unencoded
///   length of the [`QuiltIndexV1`]. It is guaranteed the column size is more than
///   `QUILT_INDEX_SIZE_PREFIX_SIZE`.
/// - The [`QuiltIndexV1`] is stored in the first one or multiple columns.
/// - The blob layout is defined by the [`QuiltIndexV1`].
///
/// INV: `data.len()` is an integer multiple of `row_size * symbol_size`.
#[derive(Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct QuiltV1 {
    /// The data of the quilt.
    data: Vec<u8>,
    /// The size of each row in bytes.
    row_size: usize,
    /// The size of each symbol in bytes.
    symbol_size: usize,
    /// The internal structure of the quilt.
    quilt_index: QuiltIndexV1,
}

impl QuiltApi<QuiltVersionV1> for QuiltV1 {
    /// Gets the blob by description.
    fn get_blob_by_identifier(&self, identifier: &str) -> Result<Vec<u8>, QuiltError> {
        self.quilt_index
            .get_quilt_block_by_identifier(identifier)
            .and_then(|quilt_block| self.get_blob(quilt_block))
    }

    /// Returns the quilt index.
    fn quilt_index(&self) -> &QuiltIndexV1 {
        &self.quilt_index
    }

    /// Returns the data of the quilt.
    fn data(&self) -> &[u8] {
        &self.data
    }

    /// Returns the symbol size of the quilt.
    fn symbol_size(&self) -> usize {
        self.symbol_size
    }
}

impl QuiltV1 {
    /// Gets the blob represented by the given quilt block.
    fn get_blob(&self, quilt_block: &QuiltBlockV1) -> Result<Vec<u8>, QuiltError> {
        let start_idx = usize::from(quilt_block.start_index);
        let end_idx = usize::from(quilt_block.end_index);
        let mut blob = vec![0u8; quilt_block.unencoded_length as usize];

        let mut written = 0;
        for col in start_idx..end_idx {
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
        Ok(blob)
    }
}

impl fmt::Debug for QuiltV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut ds = f.debug_struct("QuiltV1");

        ds.field(
            "\ndata",
            &format_args!(
                "\n{:#?}",
                DebugMatrix {
                    data: &self.data,
                    row_size: self.row_size,
                    symbol_size: self.symbol_size
                }
            ),
        );

        ds.field(
            "quilt_index",
            &format_args!("\n{:#?}", DebugQuiltIndex(&self.quilt_index)),
        );

        ds.field("symbol_size", &self.symbol_size).finish()?;

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
        let entries_per_line = 200 / (hex_width + 2); // +2 for ", " separator.

        write!(f, "\nRow {:0>2}:\n", self.index)?;
        for (i, entry) in self.entries.iter().enumerate() {
            if i % entries_per_line == 0 {
                if i > 0 {
                    writeln!(f)?;
                }
                write!(f, "    ")?;
            }

            write!(f, "{:width$}", entry, width = hex_width)?;

            if i < self.entries.len() - 1 {
                write!(f, ", ")?;
            }

            if i == 5 && self.entries.len() > QUILT_INDEX_SIZE_PREFIX_SIZE {
                write!(f, "... (+{} more)", self.entries.len() - i - 1)?;
                break;
            }
        }
        Ok(())
    }
}

struct DebugQuiltIndex<'a>(&'a QuiltIndexV1);

impl fmt::Debug for DebugQuiltIndex<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut list = f.debug_list();
        for block in self.0.quilt_blocks.iter() {
            list.entry(&format_args!(
                "\nQuiltBlock {{\n    unencoded_length: {},\
                \n    end_index: {}\n    identifier: {:?}\n}}",
                block.unencoded_length,
                block.end_index,
                block.identifier()
            ));
        }
        list.finish()?;
        writeln!(f)
    }
}

/// Encodes a set of blobs into a single quilt blob.
///
/// The blobs are first quilted into a 2D matrix, then the matrix is encoded into a single
/// quilt blob.
/// The quilt blob can be decoded into the original blobs by using the [`QuiltDecoderV1`].
#[derive(Debug)]
pub struct QuiltEncoderV1<'a> {
    /// The blobs to encode.
    blobs: &'a [BlobWithIdentifier<'a>],
    /// The encoding configuration.
    config: EncodingConfigEnum<'a>,
    /// A tracing span associated with this quilt encoder.
    span: Span,
}

impl<'a> QuiltEncoderV1<'a> {
    /// Creates a new [`QuiltEncoderV1`] from a encoding config and a set of blobs.
    pub fn new(config: EncodingConfigEnum<'a>, blobs: &'a [BlobWithIdentifier<'a>]) -> Self {
        Self {
            blobs,
            config,
            span: tracing::span!(Level::ERROR, "QuiltEncoderV1"),
        }
    }
}

impl QuiltEncoderApi<QuiltVersionV1> for QuiltEncoderV1<'_> {
    /// Constructs a [`QuiltV1`].
    fn construct_quilt(&self) -> Result<QuiltV1, QuiltError> {
        let _guard = self.span.enter();

        let n_rows = self.config.n_source_symbols::<Primary>().get().into();
        let n_columns = self.config.n_source_symbols::<Secondary>().get().into();
        tracing::event!(
            Level::DEBUG,
            "Constructing quilt with n_columns: {}, n_rows: {}",
            n_columns,
            n_rows
        );

        let mut ordered_blobs = Vec::new();
        for blob_with_identifier in self.blobs.iter() {
            ordered_blobs.push(blob_with_identifier);
        }

        // Create initial QuiltBlocks.
        let quilt_blocks: Vec<QuiltBlockV1> = ordered_blobs
            .iter()
            .map(|blob_with_identifier| {
                QuiltBlockV1::new(
                    blob_with_identifier.blob.len() as u64,
                    blob_with_identifier.identifier.clone(),
                )
            })
            .collect();

        let mut quilt_index = QuiltIndexV1 { quilt_blocks };

        // Get the serialized quilt index size.
        let serialized_index_size = bcs::serialized_size(&quilt_index).map_err(|e| {
            QuiltError::quilt_index_der_ser_error(format!("failed to serialize quilt index: {}", e))
        })? as u64;

        // Calculate total size including the 8-byte size prefix.
        let index_total_size = QUILT_INDEX_SIZE_PREFIX_SIZE
            + QUILT_TYPE_SIZE
            + usize::try_from(serialized_index_size)
                .expect("serialized_index_size should fit in usize");

        // Collect blob sizes for symbol size computation.
        let all_sizes: Vec<usize> = core::iter::once(index_total_size)
            .chain(ordered_blobs.iter().map(|bwd| bwd.blob.len()))
            .collect();

        let required_alignment = self.config.encoding_type().required_alignment() as usize;
        let symbol_size = utils::compute_symbol_size(
            &all_sizes,
            n_columns,
            n_rows,
            MAX_NUM_COLUMNS_FOR_QUILT_INDEX,
            required_alignment,
        )?;

        let row_size = symbol_size * n_columns;
        let column_size = symbol_size * n_rows;
        let mut data = vec![0u8; row_size * n_rows];

        // Calculate columns needed for the index.
        let index_cols_needed = index_total_size.div_ceil(column_size);
        assert!(index_cols_needed <= MAX_NUM_COLUMNS_FOR_QUILT_INDEX);
        let mut current_col = index_cols_needed;

        // Adds a blob to the data as consecutive columns, starting at the given column.
        let mut add_blob_to_data = |blob: &[u8], current_col: usize| {
            let mut offset = 0;
            let mut row = 0;
            let mut col = current_col;
            while offset < blob.len() {
                let end = cmp::min(offset + symbol_size, blob.len());
                let chunk = &blob[offset..end];
                let dest_idx = row * row_size + col * symbol_size;
                data[dest_idx..dest_idx + chunk.len()].copy_from_slice(chunk);
                row = (row + 1) % n_rows;
                if row == 0 {
                    col += 1;
                }
                offset += chunk.len();
            }
        };

        // First pass: Fill data with actual blobs and populate quilt blocks.
        for (i, blob_with_identifier) in ordered_blobs.iter().enumerate() {
            let cols_needed = blob_with_identifier.blob.len().div_ceil(column_size);
            tracing::event!(
                Level::DEBUG,
                "Blob: {:?} needs {} columns, current_col: {}",
                blob_with_identifier.identifier,
                cols_needed,
                current_col
            );
            assert!(current_col + cols_needed <= n_columns);

            add_blob_to_data(blob_with_identifier.blob, current_col);

            quilt_index.quilt_blocks[i].start_index =
                u16::try_from(current_col).expect("current_col should fit in u16");
            current_col += cols_needed;
            quilt_index.quilt_blocks[i].end_index =
                u16::try_from(current_col).expect("current_col should fit in u16");
        }

        let mut final_index_data = Vec::with_capacity(index_total_size);
        let index_size_u64 = index_total_size as u64;
        final_index_data.extend_from_slice(&index_size_u64.to_le_bytes());
        final_index_data.extend_from_slice(QuiltVersionV1::quilt_type_bytes());
        final_index_data
            .extend_from_slice(&bcs::to_bytes(&quilt_index).expect("Serialization should succeed"));

        // Add the index data to the data.
        add_blob_to_data(&final_index_data, 0);

        Ok(QuiltV1 {
            data,
            row_size,
            quilt_index,
            symbol_size,
        })
    }

    /// Encodes the blobs into a quilt and returns the slivers.
    fn encode(&self) -> Result<Vec<SliverPair>, QuiltError> {
        let _guard = self.span.enter();
        tracing::event!(Level::DEBUG, "starting to encode quilt");

        let quilt = self.construct_quilt()?;
        let encoder = BlobEncoder::new(self.config.clone(), quilt.data()).map_err(|_| {
            QuiltError::quilt_oversize(format!("quilt is too large: {}", quilt.data().len()))
        })?;
        assert_eq!(encoder.symbol_usize(), quilt.symbol_size());
        Ok(encoder.encode())
    }

    /// Encodes the blobs into a quilt and returns the slivers and metadata.
    fn encode_with_metadata(&self) -> Result<(Vec<SliverPair>, QuiltMetadataV1), QuiltError> {
        let _guard = self.span.enter();
        tracing::debug!("starting to encode quilt with metadata");

        let quilt = self.construct_quilt()?;
        let encoder = BlobEncoder::new(self.config.clone(), quilt.data()).map_err(|_| {
            QuiltError::quilt_oversize(format!("quilt is too large: {}", quilt.data.len()))
        })?;

        assert_eq!(encoder.symbol_usize(), quilt.symbol_size);

        let (sliver_pairs, metadata) = encoder.encode_with_metadata();
        let quilt_metadata = QuiltMetadataV1 {
            quilt_blob_id: *metadata.blob_id(),
            metadata: metadata.metadata().clone(),
            index: QuiltIndexV1 {
                quilt_blocks: quilt.quilt_index().quilt_blocks.clone(),
            },
        };

        Ok((sliver_pairs, quilt_metadata))
    }
}

/// A quilt decoder of version V1.
#[derive(Debug)]
pub struct QuiltDecoderV1<'a> {
    slivers: Vec<&'a SliverData<Secondary>>,
    quilt_index: Option<QuiltIndexV1>,
}

impl<'a> QuiltDecoderApi<'a, QuiltVersionV1> for QuiltDecoderV1<'a> {
    fn decode_quilt_index(&mut self) -> Result<&QuiltIndexV1, QuiltError> {
        let index = SliverIndex(0);

        let first_sliver = self
            .slivers
            .iter()
            .find(|s| s.index == index)
            .ok_or_else(|| QuiltError::missing_sliver(index))?;

        assert_eq!(
            QuiltVersionV1::quilt_type_bytes(),
            utils::get_quilt_version_bytes(first_sliver.symbols.data())
        );

        let data_size = utils::get_quilt_index_data_size(first_sliver.symbols.data())?;

        // Calculate how many slivers we need based on the data size.
        let num_slivers_needed = data_size.div_ceil(first_sliver.symbols.data().len());
        let prefix_size = QUILT_INDEX_SIZE_PREFIX_SIZE + QUILT_TYPE_SIZE;
        let index_size = data_size - prefix_size;
        let mut combined_data = Vec::with_capacity(index_size);

        let end = data_size.min(first_sliver.symbols.data().len());
        combined_data.extend_from_slice(&first_sliver.symbols.data()[prefix_size..end]);

        // Collect data from subsequent slivers if needed.
        for i in 1..num_slivers_needed {
            let next_index = SliverIndex(i as u16);
            let next_sliver = self
                .slivers
                .iter()
                .find(|s| s.index == next_index)
                .ok_or_else(|| {
                    utils::missing_sliver_error(
                        SliverIndex(i as u16),
                        SliverIndex(num_slivers_needed as u16),
                    )
                })?;

            let remaining_needed = index_size - combined_data.len();
            let sliver_data = next_sliver.symbols.data();
            let to_take = remaining_needed.min(sliver_data.len());
            combined_data.extend_from_slice(&sliver_data[..to_take]);
        }

        debug_assert_eq!(combined_data.len(), index_size);

        // Decode the QuiltIndexV1 from the collected data.
        let mut index: QuiltIndexV1 = bcs::from_bytes(&combined_data)
            .map_err(|e| QuiltError::quilt_index_der_ser_error(e.to_string()))?;

        // After successful deserialization, sort the blocks by end_index.
        #[cfg(debug_assertions)]
        for i in 1..index.quilt_blocks.len() {
            assert!(index.quilt_blocks[i].end_index >= index.quilt_blocks[i - 1].end_index);
        }
        index.populate_start_indices(
            u16::try_from(num_slivers_needed).expect("num_slivers_needed should fit in u16"),
        );

        self.quilt_index = Some(index);

        Ok(self
            .quilt_index
            .as_ref()
            .expect("quilt index should be decoded"))
    }

    fn get_quilt_index(&self) -> Option<&QuiltIndexV1> {
        self.quilt_index.as_ref()
    }

    /// Get the blob by identifier.
    fn get_blob_by_identifier(&self, identifier: &str) -> Result<Vec<u8>, QuiltError> {
        self.quilt_index
            .as_ref()
            .ok_or(QuiltError::missing_quilt_index())
            .and_then(|quilt_index| quilt_index.get_quilt_block_by_identifier(identifier))
            .and_then(|quilt_block| self.get_blob_by_quilt_block(quilt_block))
    }

    /// Adds slivers to the decoder.
    fn add_slivers(&mut self, slivers: &'a [&'a SliverData<Secondary>]) {
        self.slivers.extend(slivers);
    }
}

impl<'a> QuiltDecoderV1<'a> {
    /// Creates a new QuiltDecoderV1 with the given slivers.
    pub fn new(slivers: &'a [&'a SliverData<Secondary>]) -> Self {
        Self {
            slivers: slivers.to_vec(),
            quilt_index: None,
        }
    }

    /// Creates a new QuiltDecoderV1 with the given slivers, and a quilt index.
    pub fn new_with_quilt_index(
        slivers: &'a [&'a SliverData<Secondary>],
        quilt_index: QuiltIndexV1,
    ) -> Self {
        Self {
            slivers: slivers.to_vec(),
            quilt_index: Some(quilt_index),
        }
    }

    /// Get the blob represented by the quilt block.
    fn get_blob_by_quilt_block(&self, quilt_block: &QuiltBlockV1) -> Result<Vec<u8>, QuiltError> {
        let start_idx = usize::from(quilt_block.start_index);
        let end_idx = usize::from(quilt_block.end_index);

        let unencoded_length = usize::try_from(quilt_block.unencoded_length)
            .expect("unencoded_length should fit in usize");

        // Extract and reconstruct the blob.
        let mut blob = Vec::with_capacity(unencoded_length);

        // Collect data from the appropriate slivers.
        for i in start_idx..end_idx {
            let sliver_idx = SliverIndex(i as u16);
            if let Some(sliver) = self.slivers.iter().find(|s| s.index == sliver_idx) {
                let remaining_needed = unencoded_length - blob.len();
                blob.extend_from_slice(
                    &sliver.symbols.data()[..remaining_needed.min(sliver.symbols.data().len())],
                );
            } else {
                return Err(utils::missing_sliver_error(
                    sliver_idx,
                    SliverIndex(end_idx as u16),
                ));
            }
        }

        Ok(blob)
    }
}

mod utils {
    use super::*;

    /// Finds the minimum symbol size needed to store blobs in a fixed number of columns.
    /// Each blob must be stored in consecutive columns exclusively.
    ///
    /// # Arguments
    /// * `blobs_sizes` - Slice of blob lengths.
    /// * `n_columns` - Number of columns available.
    /// * `n_rows` - Number of rows available.
    /// * `max_num_columns_for_quilt_index` - The maximum number of columns that can
    ///   be used to store the quilt index.
    /// * `required_alignment` - The alignment of the symbol size.
    ///
    /// # Returns
    /// * `Result<usize, QuiltError>` - The minimum symbol size needed, or an error if impossible.
    pub fn compute_symbol_size(
        blobs_sizes: &[usize],
        n_columns: usize,
        n_rows: usize,
        max_num_columns_for_quilt_index: usize,
        required_alignment: usize,
    ) -> Result<usize, QuiltError> {
        if blobs_sizes.len() > n_columns {
            // The first column is not user data.
            return Err(QuiltError::too_many_blobs(blobs_sizes.len(), n_columns - 1));
        }

        if blobs_sizes.is_empty() {
            return Err(QuiltError::other(
                "failed to compute symbol size: blobs are empty".to_string(),
            ));
        }

        let mut min_val = cmp::max(
            blobs_sizes
                .iter()
                .sum::<usize>()
                .div_ceil(n_columns)
                .div_ceil(n_rows),
            blobs_sizes
                .first()
                .expect("blobs_sizes is not empty")
                .div_ceil(n_rows * max_num_columns_for_quilt_index),
        );
        min_val = cmp::max(
            min_val,
            (QUILT_INDEX_SIZE_PREFIX_SIZE + QUILT_TYPE_SIZE).div_ceil(n_rows),
        );
        let mut max_val = blobs_sizes
            .iter()
            .max()
            .copied()
            .unwrap_or(0)
            .div_ceil(n_rows);

        while min_val < max_val {
            let mid = (min_val + max_val) / 2;
            if can_blobs_fit_into_matrix(blobs_sizes, n_columns, mid * n_rows) {
                max_val = mid;
            } else {
                min_val = mid + 1;
            }
        }

        let symbol_size = min_val.div_ceil(required_alignment) * required_alignment;
        if symbol_size > u16::MAX as usize {
            return Err(QuiltError::quilt_oversize(format!(
                "the resulting symbol size {} is too large, remove some blobs",
                symbol_size
            )));
        }

        Ok(symbol_size)
    }

    /// Checks if the blobs can fit in the given number of columns.
    ///
    /// # Arguments
    /// * `blobs_sizes` - The sizes of the blobs.
    /// * `n_columns` - The number of columns available.
    /// * `length` - The size of the column.
    ///
    /// # Returns
    fn can_blobs_fit_into_matrix(
        blobs_sizes: &[usize],
        n_columns: usize,
        column_size: usize,
    ) -> bool {
        let mut used_cols = 0;
        for &blob in blobs_sizes {
            let cur = blob.div_ceil(column_size);
            if used_cols + cur > n_columns {
                return false;
            }
            used_cols += cur;
        }

        true
    }

    /// Returns the missing sliver error.
    pub fn missing_sliver_error(begin: SliverIndex, end: SliverIndex) -> QuiltError {
        if begin == end {
            QuiltError::missing_sliver(begin)
        } else {
            QuiltError::missing_sliver_range(begin, end)
        }
    }

    /// Get the data size of the quilt index.
    pub fn get_quilt_index_data_size(combined_data: &[u8]) -> Result<usize, QuiltError> {
        if combined_data.len() < QUILT_INDEX_SIZE_PREFIX_SIZE {
            return Err(QuiltError::failed_to_extract_quilt_index_size());
        }

        let data_size = u64::from_le_bytes(
            combined_data[0..QUILT_INDEX_SIZE_PREFIX_SIZE]
                .try_into()
                .map_err(|_| QuiltError::failed_to_extract_quilt_index_size())?,
        );
        let data_size = usize::try_from(data_size).expect("data_size should fit in usize");

        Ok(data_size)
    }

    /// Gets the quilt version enum from the data.
    #[allow(dead_code)] // TODO: remove this once follow up PRs are merged.
    pub fn get_quilt_version_enum(data: &[u8]) -> QuiltVersionEnum {
        QuiltVersionEnum::new_from_bytes(get_quilt_version_bytes(data))
    }

    pub fn get_quilt_version_bytes(data: &[u8]) -> &[u8] {
        &data[QUILT_INDEX_SIZE_PREFIX_SIZE..QUILT_INDEX_SIZE_PREFIX_SIZE + QUILT_TYPE_SIZE]
    }

    /// Gets the ith column of data, as if `data` is a 2D matrix.
    ///
    /// # Arguments
    /// * `i` - The column index.
    /// * `data` - The data to extract the column from.
    /// * `row_size` - The size of each row in bytes.
    /// * `symbol_size` - The size of each symbol in bytes.
    ///
    /// # Returns
    /// A vector containing the bytes from the ith column.
    fn get_column(
        i: usize,
        data: &[u8],
        row_size: usize,
        symbol_size: usize,
    ) -> Result<Vec<u8>, QuiltError> {
        // Verify inputs make sense.
        if row_size == 0
            || data.is_empty()
            || symbol_size == 0
            || row_size % symbol_size != 0
            || data.len() % row_size != 0
        {
            return Err(QuiltError::invalid_format_not_aligned());
        }

        let n_rows = data.len() / row_size;
        if i >= (row_size / symbol_size) {
            return Err(QuiltError::index_out_of_bounds(i, row_size / symbol_size));
        }

        let mut column = Vec::with_capacity(n_rows * symbol_size);

        for row in 0..n_rows {
            let start_idx = row * row_size + i * symbol_size;
            let end_idx = start_idx + symbol_size;

            // Check if we have enough data for this chunk.
            if end_idx > data.len() {
                break;
            }

            column.extend_from_slice(&data[start_idx..end_idx]);
        }

        Ok(column)
    }

    /// Extracts the quilt index from the quilt blob data.
    pub fn get_quilt_index_v1_from_data(
        data: &[u8],
        row_size: usize,
        symbol_size: usize,
    ) -> Result<QuiltIndexV1, QuiltError> {
        // Get the first column and extract the size prefix.
        let first_column = get_column(0, data, row_size, symbol_size).map_err(|_| {
            QuiltError::QuiltIndexDerSerError("failed to extract first column".to_string())
        })?;

        let quilt_type_bytes = get_quilt_version_bytes(&first_column);
        assert_eq!(quilt_type_bytes, QuiltVersionV1::quilt_type_bytes());

        let data_size = get_quilt_index_data_size(&first_column)?;
        let prefix_size = QUILT_INDEX_SIZE_PREFIX_SIZE + QUILT_TYPE_SIZE;
        let quilt_index_size = data_size - prefix_size;

        let mut collected_data = Vec::with_capacity(quilt_index_size);
        collected_data.extend_from_slice(&first_column[prefix_size..]);

        // Keep collecting data from subsequent columns until we have enough bytes.
        let mut current_column = 1;
        while collected_data.len() < quilt_index_size {
            let column_data = get_column(current_column, data, row_size, symbol_size)?;
            collected_data.extend_from_slice(&column_data);
            current_column += 1;
        }

        // Truncate to exact size needed.
        collected_data.truncate(quilt_index_size);

        // Decode the QuiltIndexV1.
        let mut quilt_index: QuiltIndexV1 = bcs::from_bytes(&collected_data)
            .map_err(|e| QuiltError::quilt_index_der_ser_error(e.to_string()))?;

        quilt_index.populate_start_indices(
            u16::try_from(current_column).expect("current_column should fit in u16"),
        );
        Ok(quilt_index)
    }
}

#[cfg(test)]
mod tests {
    use walrus_test_utils::param_test;

    use super::*;
    use crate::{
        encoding::{RaptorQEncodingConfig, ReedSolomonEncodingConfig},
        metadata::BlobMetadataApi as _,
    };

    /// Get the minimum required columns.
    fn min_required_columns(blobs: &[usize], length: usize) -> usize {
        if length == 0 {
            return usize::MAX;
        }
        let mut used_cols = 0;
        for &blob in blobs {
            used_cols += blob.div_ceil(length);
        }
        used_cols
    }

    param_test! {
        test_quilt_find_min_length: [
            case_1: (&[2, 1, 2, 1], 3, 3, 1, Err(QuiltError::too_many_blobs(4, 2))),
            case_2: (&[1000, 1, 1], 4, 7, 2, Ok(144)),
            case_3: (
                &[],
                3,
                1,
                1,
                Err(QuiltError::Other(
                    "failed to compute symbol size: blobs are empty".to_string(),
                )),
            ),
            case_4: (&[1], 3, 2, 1, Ok(5)),
            case_5: (&[115, 80, 4], 17, 9, 1, Ok(13)),
            case_6: (&[20, 20, 20], 3, 5, 1, Ok(4)),
            case_7: (&[5, 5, 5], 5, 1, 2, Ok(10)),
            case_8: (&[25, 35, 45], 200, 1, 2, Ok(26)),
            case_9: (&[10, 0, 0, 0], 17, 9, 1, Ok(2)),
            case_10: (&[10, 0, 0, 0], 17, 9, 2, Ok(2)),
        ]
    }
    fn test_quilt_find_min_length(
        blobs: &[usize],
        n_columns: usize,
        n_rows: usize,
        required_alignment: usize,
        expected: Result<usize, QuiltError>,
    ) {
        // Initialize tracing subscriber for this test
        let _guard = tracing_subscriber::fmt().try_init();
        let res = utils::compute_symbol_size(
            blobs,
            n_columns,
            n_rows,
            MAX_NUM_COLUMNS_FOR_QUILT_INDEX,
            required_alignment,
        );
        assert_eq!(res, expected);
        if let Ok(min_size) = res {
            assert!(min_required_columns(blobs, min_size * n_rows) <= n_columns);
        }
    }

    param_test! {
        test_quilt_construct_quilt: [
            case_0: (
                &[
                    BlobWithIdentifier {
                        blob: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..],
                        identifier: "test blob 0".to_string(),
                    },
                    BlobWithIdentifier {
                        blob: &[5, 68, 3, 2, 5][..],
                        identifier: "test blob 1".to_string(),
                    },
                    BlobWithIdentifier {
                        blob: &[5, 68, 3, 2, 5, 6, 78, 8][..],
                        identifier: "test blob 2".to_string(),
                    },
                ],
                3, 5, 7
            ),
            case_0_random_order: (
                &[
                    BlobWithIdentifier {
                        blob: &[5, 68, 3, 2, 5, 6, 78, 8][..],
                        identifier: "test blob 0".to_string(),
                    },
                    BlobWithIdentifier {
                        blob: &[5, 68, 3, 2, 5][..],
                        identifier: "test blob 1".to_string(),
                    },
                    BlobWithIdentifier {
                        blob: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..],
                        identifier: "test blob 2".to_string(),
                    },
                ],
                3, 5, 7
            ),
            case_1: (
                &[
                    BlobWithIdentifier {
                        blob: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..],
                        identifier: "test blob 0".to_string(),
                    },
                    BlobWithIdentifier {
                        blob: &[5, 68, 3, 2, 5][..],
                        identifier: "test blob 1".to_string(),
                    },
                    BlobWithIdentifier {
                        blob: &[5, 68, 3, 2, 5, 6, 78, 8][..],
                        identifier: "test blob 2".to_string(),
                    },
                ],
                3, 5, 7
            ),
            case_1_random_order: (
                &[
                    BlobWithIdentifier {
                        blob: &[5, 68, 3, 2, 5][..],
                        identifier: "test blob 0".to_string(),
                    },
                    BlobWithIdentifier {
                        blob: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..],
                        identifier: "test blob 1".to_string(),
                    },
                    BlobWithIdentifier {
                        blob: &[5, 68, 3, 2, 5, 6, 78, 8][..],
                        identifier: "test blob 2".to_string(),
                    },
                ],
                3, 5, 7
            ),
            case_2: (
                &[
                    BlobWithIdentifier {
                        blob: &[1, 3][..],
                        identifier: "test blob 0".to_string(),
                    },
                    BlobWithIdentifier {
                        blob: &[255u8; 1024][..],
                        identifier: "test blob 1".to_string(),
                    },
                    BlobWithIdentifier {
                        blob: &[1, 2, 3][..],
                        identifier: "test blob 2".to_string(),
                    },
                ],
                4, 8, 12
            ),
            case_3: (
                &[
                    BlobWithIdentifier {
                        blob: &[9, 8, 7, 6, 5, 4, 3, 2, 1][..],
                        identifier: "test blob 0".to_string(),
                    },
                ],
                3, 5, 7
            ),
        ]
    }
    fn test_quilt_construct_quilt(
        blobs_with_identifiers: &[BlobWithIdentifier<'_>],
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

        construct_quilt(
            blobs_with_identifiers,
            EncodingConfigEnum::RaptorQ(&raptorq_config),
        );
        construct_quilt(
            blobs_with_identifiers,
            EncodingConfigEnum::ReedSolomon(&reed_solomon_config),
        );
    }

    fn construct_quilt(
        blobs_with_identifiers: &[BlobWithIdentifier<'_>],
        config: EncodingConfigEnum,
    ) {
        let _guard = tracing_subscriber::fmt().try_init();

        let encoder = QuiltConfigV1::get_encoder(config, blobs_with_identifiers);

        let quilt = encoder.construct_quilt().expect("Should construct quilt");
        tracing::debug!("QuiltV1: {:?}", quilt);

        // Verify each blob and its description.
        for blob_with_identifier in blobs_with_identifiers {
            // Verify blob data matches.
            let extracted_blob = quilt
                .get_blob_by_identifier(blob_with_identifier.identifier.as_str())
                .expect("Block should exist for this blob identifier");
            assert_eq!(
                extracted_blob, blob_with_identifier.blob,
                "Mismatch in encoded blob"
            );

            assert_eq!(
                quilt
                    .quilt_index()
                    .get_quilt_block_by_identifier(blob_with_identifier.identifier.as_str())
                    .expect("Block should exist for this blob ID")
                    .identifier(),
                &blob_with_identifier.identifier,
                "Mismatch in blob description"
            );
        }

        assert_eq!(quilt.quilt_index().len(), blobs_with_identifiers.len());
    }

    param_test! {
        test_quilt_encoder_and_decoder: [
            case_0: (
                &[
                    BlobWithIdentifier {
                        blob: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..],
                        identifier: "test blob 0".to_string(),
                    },
                    BlobWithIdentifier {
                        blob: &[5, 68, 3, 2, 5][..],
                        identifier: "test blob 1".to_string(),
                    },
                    BlobWithIdentifier {
                        blob: &[5, 68, 3, 2, 5, 6, 78, 8][..],
                        identifier: "test blob 2".to_string(),
                    },
                ],
                3, 5, 7
            ),
            case_0_random_order: (
                &[
                    BlobWithIdentifier {
                        blob: &[5, 68, 3, 2, 5, 6, 78, 8][..],
                        identifier: "test blob 0".to_string(),
                    },
                    BlobWithIdentifier {
                        blob: &[5, 68, 3, 2, 5][..],
                        identifier: "test blob 1".to_string(),
                    },
                    BlobWithIdentifier {
                        blob: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..],
                        identifier: "test blob 2".to_string(),
                    },
                ],
                3, 5, 7
            ),
            case_1: (
                &[
                    BlobWithIdentifier {
                        blob: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..],
                        identifier: "test blob 0".to_string(),
                    },
                    BlobWithIdentifier {
                        blob: &[5, 68, 3, 2, 5][..],
                        identifier: "test blob 1".to_string(),
                    },
                    BlobWithIdentifier {
                        blob: &[5, 68, 3, 2, 5, 6, 78, 8][..],
                        identifier: "test blob 2".to_string(),
                    },
                ],
                3, 5, 7
            ),
            case_1_random_order: (
                &[
                    BlobWithIdentifier {
                        blob: &[5, 68, 3, 2, 5][..],
                        identifier: "test blob 0".to_string(),
                    },
                    BlobWithIdentifier {
                        blob: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..],
                        identifier: "test blob 1".to_string(),
                    },
                    BlobWithIdentifier {
                        blob: &[5, 68, 3, 2, 5, 6, 78, 8][..],
                        identifier: "test blob 2".to_string(),
                    },
                ],
                3, 5, 7
            ),
            case_2: (
                &[
                    BlobWithIdentifier {
                        blob: &[1, 3][..],
                        identifier: "test blob 0".to_string(),
                    },
                    BlobWithIdentifier {
                        blob: &[255u8; 1024][..],
                        identifier: "test blob 1".to_string(),
                    },
                    BlobWithIdentifier {
                        blob: &[1, 2, 3][..],
                        identifier: "test blob 2".to_string(),
                    },
                ],
                5, 9, 13
            ),
            case_3: (
                &[
                    BlobWithIdentifier {
                        blob: &[9, 8, 7, 6, 5, 4, 3, 2, 1][..],
                        identifier: "test blob 0".to_string(),
                    },
                ],
                3, 5, 7
            ),
            case_4: (
                &[
                    BlobWithIdentifier {
                        blob: &[][..],
                        identifier: "empty blob 0".to_string(),
                    },
                    BlobWithIdentifier {
                        blob: &[][..],
                        identifier: "empty blob 1".to_string(),
                    },
                    BlobWithIdentifier {
                        blob: &[][..],
                        identifier: "empty blob 2".to_string(),
                    }
                ],
                5, 9, 13
            ),
        ]
    }
    fn test_quilt_encoder_and_decoder(
        blobs_with_identifiers: &[BlobWithIdentifier<'_>],
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

        encode_decode_quilt(
            blobs_with_identifiers,
            EncodingConfigEnum::RaptorQ(&raptorq_config),
        );
        encode_decode_quilt(
            blobs_with_identifiers,
            EncodingConfigEnum::ReedSolomon(&reed_solomon_config),
        );
    }

    fn encode_decode_quilt(
        blobs_with_identifiers: &[BlobWithIdentifier<'_>],
        config: EncodingConfigEnum,
    ) {
        let _guard = tracing_subscriber::fmt().try_init();

        let encoder = QuiltConfigV1::get_encoder(config.clone(), blobs_with_identifiers);

        let (sliver_pairs, quilt_metadata) = encoder
            .encode_with_metadata()
            .expect("Should encode with quilt index and metadata");
        tracing::debug!(
            "Sliver pairs: {:?}\nQuilt metadata: {:?}",
            sliver_pairs,
            quilt_metadata
        );

        let slivers: Vec<&SliverData<Secondary>> = sliver_pairs
            .iter()
            .map(|sliver_pair| &sliver_pair.secondary)
            .collect();

        let first_sliver = slivers
            .iter()
            .find(|sliver| sliver.index == SliverIndex::new(0))
            .expect("Should find first sliver");
        let quilt_version: QuiltVersionEnum =
            utils::get_quilt_version_enum(first_sliver.symbols.data());
        assert!(matches!(quilt_version, QuiltVersionEnum::V1));
        let mut quilt_decoder = QuiltConfigV1::get_decoder(&[]);
        assert!(matches!(
            quilt_decoder.decode_quilt_index(),
            Err(QuiltError::MissingSliver(_))
        ));

        quilt_decoder.add_slivers(&slivers);
        assert_eq!(
            quilt_decoder.decode_quilt_index(),
            Ok(&quilt_metadata.index)
        );

        for blob_with_identifier in blobs_with_identifiers {
            let blob =
                quilt_decoder.get_blob_by_identifier(blob_with_identifier.identifier.as_str());
            assert_eq!(blob, Ok(blob_with_identifier.blob.to_vec()));
        }

        let mut decoder = config
            .get_blob_decoder::<Secondary>(quilt_metadata.metadata.unencoded_length())
            .expect("Should create decoder");

        let (quilt_blob, metadata_with_id) = decoder
            .decode_and_verify(
                &quilt_metadata.quilt_blob_id,
                sliver_pairs
                    .iter()
                    .map(|s| s.secondary.clone())
                    .collect::<Vec<_>>(),
            )
            .expect("Should decode and verify quilt")
            .expect("Should decode quilt");

        assert_eq!(metadata_with_id.metadata(), &quilt_metadata.metadata);

        let quilt =
            QuiltConfigV1::parse_from_quilt_blob(quilt_blob, &quilt_metadata, config.n_shards())
                .expect("Should create quilt");
        assert_eq!(
            quilt.data(),
            encoder
                .construct_quilt()
                .expect("Should construct quilt")
                .data()
        );
    }
}
