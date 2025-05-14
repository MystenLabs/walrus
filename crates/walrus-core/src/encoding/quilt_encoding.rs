// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// *******************************************************************************
// DISCLAIMER: The quilt format is still under active development.
// Please use with caution as it is subject to change without prior notice.
// *******************************************************************************

use alloc::{
    format,
    string::{String, ToString},
    vec,
    vec::Vec,
};
use core::{cmp, fmt};
use std::collections::HashMap;

use hex;
use serde::{Deserialize, Serialize};
use tracing::{Level, Span};

use super::{EncodingConfigEnum, Primary, Secondary, SliverData, SliverPair};
use crate::{
    SliverIndex,
    encoding::{QuiltError, blob_encoding::BlobEncoder, config::EncodingConfigTrait as _},
    metadata::{QuiltIndex, QuiltIndexV1, QuiltMetadata, QuiltMetadataV1, QuiltPatchV1},
};

/// The number of bytes to store the size of the quilt index.
const QUILT_INDEX_SIZE_PREFIX_SIZE: usize = 8;

/// The number of bytes used to store the type of the quilt.
const QUILT_TYPE_SIZE: usize = 1;

/// The maximum number of columns a quilt index can have.
const MAX_NUM_COLUMNS_FOR_QUILT_INDEX: usize = 1;

/// Gets the quilt version enum from the data.
#[allow(dead_code)] // TODO: remove this once follow up PRs are merged.
pub fn get_quilt_version_enum(data: &[u8]) -> Result<QuiltVersionEnum, QuiltError> {
    QuiltVersionEnum::try_from(utils::get_quilt_version_byte(data)?)
}

/// The version of the quilt.
pub trait QuiltVersion: Sized {
    /// The type of the quilt config.
    type QuiltConfig: for<'a> QuiltConfigApi<'a, Self>;
    /// The type of the quilt encoder.
    type QuiltEncoder<'a>: QuiltEncoderApi<Self>;
    /// The type of the quilt decoder.
    type QuiltDecoder<'a>: QuiltDecoderApi<'a, Self>;
    /// The type of the quilt.
    type Quilt: QuiltApi<Self>;
    /// The type of the quilt index.
    type QuiltIndex: Clone;
    /// The type of the quilt metadata.
    type QuiltMetadata;

    /// The serialized bytes of the quilt type.
    fn quilt_version_byte() -> u8;
}

/// API to access a quilt.
#[allow(dead_code)] // TODO: remove this once follow up PRs are merged.
pub trait QuiltApi<V: QuiltVersion> {
    /// Returns a new quilt from a quilt blob.
    fn new_from_quilt_blob(
        quilt_blob: Vec<u8>,
        encoding_config: &EncodingConfigEnum<'_>,
    ) -> Result<V::Quilt, QuiltError>;

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
}

/// Encoder to construct a quilt and encode the blobs into slivers.
#[allow(dead_code)] // TODO: remove this once follow up PRs are merged.
pub trait QuiltEncoderApi<V: QuiltVersion> {
    /// Constructs a quilt by encoding the blobs.
    fn construct_quilt(&self) -> Result<V::Quilt, QuiltError>;

    /// Encodes the blobs into a quilt and returns the slivers.
    fn encode(&self) -> Result<Vec<SliverPair>, QuiltError>;

    /// Encodes the blobs into a quilt and returns the slivers and metadata.
    fn encode_with_metadata(&self) -> Result<(Vec<SliverPair>, QuiltMetadata), QuiltError>;
}

/// Decoder to decode a quilt from slivers.
#[allow(dead_code)] // TODO: remove this once follow up PRs are merged.
pub trait QuiltDecoderApi<'a, V: QuiltVersion> {
    /// Decodes the quilt index from received slivers.
    ///
    /// The decoded quilt index is stored in the decoder and can be retrieved
    /// using the `get_quilt_index` method after this method returns.
    fn get_or_decode_quilt_index(&mut self) -> Result<&V::QuiltIndex, QuiltError>;

    /// Gets a blob by its identifier from the quilt.
    fn get_blob_by_identifier(&self, identifier: &str) -> Result<Vec<u8>, QuiltError>;

    /// Adds slivers to the decoder.
    fn add_slivers(&mut self, slivers: &'a [&'a SliverData<Secondary>]);
}

/// The version of the quilt.
#[derive(Debug, Clone, PartialEq, Eq)]
#[repr(u8)]
pub enum QuiltVersionEnum {
    /// QuiltVersionV1.
    V1,
}

impl From<QuiltVersionEnum> for u8 {
    fn from(value: QuiltVersionEnum) -> Self {
        value as u8
    }
}

impl TryFrom<u8> for QuiltVersionEnum {
    type Error = QuiltError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x00 => Ok(QuiltVersionEnum::V1),
            _ => Err(QuiltError::Other(format!(
                "Invalid quilt version byte: {}",
                value
            ))),
        }
    }
}

impl QuiltVersionEnum {
    /// Creates a new `QuiltVersionEnum` from a sliver.
    #[allow(dead_code)] // TODO: remove this once follow up PRs are merged.
    pub fn new_from_sliver(sliver: &[u8]) -> Result<QuiltVersionEnum, QuiltError> {
        if sliver.is_empty() {
            return Err(QuiltError::Other("Sliver is empty".to_string()));
        }
        QuiltVersionEnum::try_from(sliver[0])
    }
}

/// The quilt enum.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum QuiltEnum {
    /// The quilt version 1.
    V1(QuiltV1),
}

impl QuiltEnum {
    /// Returns the blob identified by the given identifier.
    #[allow(dead_code)] // TODO: remove this once follow up PRs are merged.
    pub fn get_blob_by_identifier(&self, identifier: &str) -> Result<Vec<u8>, QuiltError> {
        match self {
            QuiltEnum::V1(quilt_v1) => quilt_v1.get_blob_by_identifier(identifier),
        }
    }

    /// Returns the quilt index.
    #[allow(dead_code)] // TODO: remove this once follow up PRs are merged.
    pub fn get_quilt_index(&self) -> Result<QuiltIndex, QuiltError> {
        match self {
            QuiltEnum::V1(quilt_v1) => Ok(QuiltIndex::V1(quilt_v1.quilt_index.clone())),
        }
    }
}

/// A wrapper around a blob and its identifier.
///
/// A valid identifier is a string that contains only alphanumeric characters,
/// underscores, hyphens, and periods.
#[derive(Debug, Clone)]
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

    /// Returns a reference to the blob data.
    pub fn data(&self) -> &'a [u8] {
        self.blob
    }

    /// Returns a reference to the identifier.
    pub fn identifier(&self) -> &str {
        &self.identifier
    }
}

/// A wrapper around an owned blob and its identifier.
///
/// A valid identifier is a string that contains only alphanumeric characters,
/// underscores, hyphens, and periods.
#[derive(Debug, Clone)]
#[allow(dead_code)] // TODO: remove this once follow up PRs are merged.
pub struct BlobWithIdentifierOwned {
    blob: Vec<u8>,
    identifier: String,
}

#[allow(dead_code)] // TODO: remove this once follow up PRs are merged.
impl BlobWithIdentifierOwned {
    /// Creates a new `BlobWithIdentifierOwned` from an owned blob and an identifier.
    pub fn new(blob: Vec<u8>, identifier: impl Into<String>) -> Self {
        Self {
            blob,
            identifier: identifier.into(),
        }
    }

    /// Returns a reference to the blob data.
    pub fn data(&self) -> &[u8] {
        &self.blob
    }

    /// Returns a reference to the identifier.
    pub fn identifier(&self) -> &str {
        &self.identifier
    }
}

/// Quilt version 1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuiltVersionV1;

impl QuiltVersionV1 {
    const QUILT_VERSION_BYTE: u8 = 0x00;
}

impl QuiltVersion for QuiltVersionV1 {
    type QuiltConfig = QuiltConfigV1;
    type QuiltEncoder<'a> = QuiltEncoderV1<'a>;
    type QuiltDecoder<'a> = QuiltDecoderV1<'a>;
    type Quilt = QuiltV1;
    type QuiltIndex = QuiltIndexV1;
    type QuiltMetadata = QuiltMetadataV1;

    fn quilt_version_byte() -> u8 {
        QuiltVersionV1::QUILT_VERSION_BYTE
    }
}

/// A quilt is a collection of blobs encoded into a single blob.
///
/// For QuiltVersionV1:
/// The data is organized as a 2D matrix where:
/// - Each blob occupies a consecutive range of columns (secondary slivers).
/// - The first column's initial `QUILT_INDEX_SIZE_PREFIX_SIZE` bytes contain the unencoded
///   length of the [`QuiltIndexV1`]. It is guaranteed the column size is more than
///   `QUILT_INDEX_SIZE_PREFIX_SIZE`.
/// - The [`QuiltIndexV1`] is stored in the first one or multiple columns, up to
///   `MAX_NUM_COLUMNS_FOR_QUILT_INDEX`.
/// - The blob layout is defined by the [`QuiltIndexV1`].
///
// INV:
//  - `data.len()` is an integer multiple of `row_size`.
//  - `row_size` is an integer multiple of `symbol_size`.
//  - Blobs are stored in the order of their identifiers in the quilt.
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
    fn new_from_quilt_blob(
        quilt_blob: Vec<u8>,
        encoding_config: &EncodingConfigEnum<'_>,
    ) -> Result<QuiltV1, QuiltError> {
        let n_primary_source_symbols =
            usize::from(encoding_config.n_source_symbols::<Primary>().get());
        let n_secondary_source_symbols =
            usize::from(encoding_config.n_source_symbols::<Secondary>().get());
        let n_source_symbols: usize = n_primary_source_symbols * n_secondary_source_symbols;

        if quilt_blob.len() % n_source_symbols != 0 {
            return Err(QuiltError::InvalidFormatNotAligned(format!(
                "quilt_blob length {} is not a multiple of n_source_symbols {}",
                quilt_blob.len(),
                n_source_symbols
            )));
        }

        let symbol_size = quilt_blob.len() / n_source_symbols;
        let row_size = symbol_size * n_secondary_source_symbols;
        let quilt_index = utils::get_quilt_index_v1_from_data(&quilt_blob, row_size, symbol_size)?;

        Ok(QuiltV1 {
            data: quilt_blob,
            row_size,
            quilt_index,
            symbol_size,
        })
    }

    fn get_blob_by_identifier(&self, identifier: &str) -> Result<Vec<u8>, QuiltError> {
        self.quilt_index
            .get_quilt_patch_by_identifier(identifier)
            .and_then(|quilt_patch| self.get_blob(quilt_patch))
    }

    fn quilt_index(&self) -> &QuiltIndexV1 {
        &self.quilt_index
    }

    fn data(&self) -> &[u8] {
        &self.data
    }

    fn symbol_size(&self) -> usize {
        self.symbol_size
    }
}

impl QuiltV1 {
    /// Gets the blob represented by the given quilt patch.
    fn get_blob(&self, quilt_patch: &QuiltPatchV1) -> Result<Vec<u8>, QuiltError> {
        let start_idx = usize::from(quilt_patch.start_index);
        let end_idx = usize::from(quilt_patch.end_index);
        let mut blob = vec![0u8; quilt_patch.unencoded_length as usize];

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
        for patch in self.0.quilt_patches.iter() {
            list.entry(&format_args!(
                "\nQuiltPatch {{\n    unencoded_length: {},\
                \n    end_index: {}\n    identifier: {:?}\n}}",
                patch.unencoded_length, patch.end_index, patch.identifier
            ));
        }
        list.finish()?;
        writeln!(f)
    }
}

/// Configuration for the quilt version 1.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct QuiltConfigV1;

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
}

/// EncoderV1.
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
        tracing::debug!(
            "Constructing quilt with n_columns: {}, n_rows: {}",
            n_columns,
            n_rows
        );

        let mut blob_pairs = self.blobs.iter().collect::<Vec<_>>();

        // Sort blobs by their identifiers.
        blob_pairs.sort_by(|a, b| a.identifier.cmp(&b.identifier));

        // Create initial QuiltPatches.
        let quilt_patches = blob_pairs
            .iter()
            .map(|blob| QuiltPatchV1::new(blob.blob.len() as u64, blob.identifier.clone()))
            .collect::<Result<Vec<QuiltPatchV1>, QuiltError>>()?;

        let mut quilt_index = QuiltIndexV1 { quilt_patches };

        // Get the serialized quilt index size.
        let serialized_index_size = bcs::serialized_size(&quilt_index).map_err(|e| {
            QuiltError::QuiltIndexSerDerError(format!("failed to serialize quilt index: {:?}", e))
        })? as u64;

        // Calculate total size including the size prefix and the quilt type.
        let index_total_size = QUILT_INDEX_SIZE_PREFIX_SIZE
            + QUILT_TYPE_SIZE
            + usize::try_from(serialized_index_size)
                .expect("serialized_index_size should fit in usize");

        // Collect blob sizes for symbol size computation.
        let all_sizes: Vec<usize> = core::iter::once(index_total_size)
            .chain(blob_pairs.iter().map(|bwd| bwd.blob.len()))
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
        let mut data = vec![0u8; row_size * n_rows];

        // Calculate columns needed for the index.
        let column_size = symbol_size * n_rows;
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

        // First pass: Fill data with actual blobs and populate quilt patches.
        for (i, blob) in blob_pairs.iter().enumerate() {
            let cols_needed = blob.blob.len().div_ceil(column_size);
            tracing::debug!(
                "Blob: {:?} needs {} columns, current_col: {}",
                blob.identifier,
                cols_needed,
                current_col
            );
            assert!(current_col + cols_needed <= n_columns);

            add_blob_to_data(blob.blob, current_col);

            quilt_index.quilt_patches[i].start_index =
                u16::try_from(current_col).expect("current_col should fit in u16");
            current_col += cols_needed;
            quilt_index.quilt_patches[i].end_index =
                u16::try_from(current_col).expect("current_col should fit in u16");
        }

        let mut final_index_data = Vec::with_capacity(index_total_size);
        let index_size_u64 = index_total_size as u64;
        final_index_data.push(QuiltVersionV1::quilt_version_byte());
        final_index_data.extend_from_slice(&index_size_u64.to_le_bytes());
        final_index_data
            .extend_from_slice(&bcs::to_bytes(&quilt_index).expect("Serialization should succeed"));

        // Add the index to the quilt.
        add_blob_to_data(&final_index_data, 0);

        tracing::debug!("construct quilt success {}", data.len());

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
        tracing::debug!("starting to encode quilt");

        let quilt = self.construct_quilt()?;
        let encoder = BlobEncoder::new(self.config.clone(), quilt.data()).map_err(|_| {
            QuiltError::QuiltOversize(format!("quilt is too large: {}", quilt.data().len()))
        })?;
        assert_eq!(encoder.symbol_usize(), quilt.symbol_size());
        Ok(encoder.encode())
    }

    /// Encodes the blobs into a quilt and returns the slivers and metadata.
    fn encode_with_metadata(&self) -> Result<(Vec<SliverPair>, QuiltMetadata), QuiltError> {
        let _guard = self.span.enter();
        tracing::debug!("starting to encode quilt with metadata");

        let quilt = self.construct_quilt()?;
        let encoder = BlobEncoder::new(self.config.clone(), quilt.data()).map_err(|_| {
            QuiltError::QuiltOversize(format!("quilt is too large: {}", quilt.data.len()))
        })?;

        assert_eq!(encoder.symbol_usize(), quilt.symbol_size);

        let (sliver_pairs, metadata) = encoder.encode_with_metadata();
        let quilt_metadata = QuiltMetadata::V1(QuiltMetadataV1 {
            quilt_blob_id: *metadata.blob_id(),
            metadata: metadata.metadata().clone(),
            index: QuiltIndexV1 {
                quilt_patches: quilt.quilt_index().quilt_patches.clone(),
            },
        });

        Ok((sliver_pairs, quilt_metadata))
    }
}

/// A quilt decoder of version V1.
#[derive(Debug)]
pub struct QuiltDecoderV1<'a> {
    slivers: HashMap<SliverIndex, &'a SliverData<Secondary>>,
    quilt_index: Option<QuiltIndexV1>,
}

impl<'a> QuiltDecoderApi<'a, QuiltVersionV1> for QuiltDecoderV1<'a> {
    fn get_or_decode_quilt_index(&mut self) -> Result<&QuiltIndexV1, QuiltError> {
        let first_sliver_index = SliverIndex(0);
        let first_sliver = self
            .slivers
            .get(&first_sliver_index)
            .ok_or(QuiltError::MissingSlivers(vec![first_sliver_index]))?;

        utils::check_quilt_version::<QuiltVersionV1>(first_sliver.symbols.data())?;

        let data_size = utils::get_quilt_index_data_size(first_sliver.symbols.data())?;

        // Calculate how many slivers we need based on the data size.
        let num_slivers_needed = data_size.div_ceil(first_sliver.symbols.data().len());
        let prefix_size = QUILT_INDEX_SIZE_PREFIX_SIZE + QUILT_TYPE_SIZE;
        let index_size = data_size - prefix_size;
        let mut combined_data = Vec::with_capacity(index_size);

        let end = data_size.min(first_sliver.symbols.data().len());
        combined_data.extend_from_slice(&first_sliver.symbols.data()[prefix_size..end]);

        self.check_missing_slivers(1, num_slivers_needed)?;

        // Collect data from subsequent slivers if needed.
        for i in 1..num_slivers_needed {
            let next_index = SliverIndex(i as u16);
            let next_sliver = self
                .slivers
                .get(&next_index)
                .expect("sliver should be present");

            let remaining_needed = index_size - combined_data.len();
            let sliver_data = next_sliver.symbols.data();
            let to_take = remaining_needed.min(sliver_data.len());
            combined_data.extend_from_slice(&sliver_data[..to_take]);
        }

        debug_assert_eq!(combined_data.len(), index_size);

        // Decode the QuiltIndexV1 from the collected data.
        let mut index: QuiltIndexV1 = bcs::from_bytes(&combined_data)
            .map_err(|e| QuiltError::QuiltIndexSerDerError(e.to_string()))?;

        // After successful deserialization, sort the patches by end_index.
        #[cfg(debug_assertions)]
        for i in 1..index.quilt_patches.len() {
            assert!(index.quilt_patches[i].end_index >= index.quilt_patches[i - 1].end_index);
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

    fn get_blob_by_identifier(&self, identifier: &str) -> Result<Vec<u8>, QuiltError> {
        self.quilt_index
            .as_ref()
            .ok_or(QuiltError::MissingQuiltIndex)
            .and_then(|quilt_index| quilt_index.get_quilt_patch_by_identifier(identifier))
            .and_then(|quilt_patch| self.get_blob_by_quilt_patch(quilt_patch))
    }

    fn add_slivers(&mut self, slivers: &'a [&'a SliverData<Secondary>]) {
        for sliver in slivers {
            self.slivers.insert(sliver.index, sliver);
        }
    }
}

impl<'a> QuiltDecoderV1<'a> {
    /// Creates a new QuiltDecoderV1 with the given slivers.
    pub fn new(slivers: &'a [&'a SliverData<Secondary>]) -> Self {
        Self {
            slivers: slivers
                .iter()
                .map(|s| (s.index, *s))
                .collect::<HashMap<_, _>>(),
            quilt_index: None,
        }
    }

    /// Creates a new QuiltDecoderV1 with the given slivers, and a quilt index.
    pub fn new_with_quilt_index(
        slivers: &'a [&'a SliverData<Secondary>],
        quilt_index: QuiltIndexV1,
    ) -> Self {
        Self {
            slivers: slivers.iter().map(|s| (s.index, *s)).collect(),
            quilt_index: Some(quilt_index),
        }
    }

    /// Get the blob represented by the quilt patch.
    fn get_blob_by_quilt_patch(&self, quilt_patch: &QuiltPatchV1) -> Result<Vec<u8>, QuiltError> {
        let start_idx = usize::from(quilt_patch.start_index);
        let end_idx = usize::from(quilt_patch.end_index);

        self.check_missing_slivers(start_idx, end_idx)?;

        let unencoded_length = usize::try_from(quilt_patch.unencoded_length)
            .expect("unencoded_length should fit in usize");

        // Extract and reconstruct the blob.
        let mut blob = Vec::with_capacity(unencoded_length);

        // Collect data from the appropriate slivers.
        for i in start_idx..end_idx {
            let sliver_idx = SliverIndex(i as u16);
            let sliver = self
                .slivers
                .get(&sliver_idx)
                .expect("sliver should be present");

            let remaining_needed = unencoded_length - blob.len();
            blob.extend_from_slice(
                &sliver.symbols.data()[..remaining_needed.min(sliver.symbols.data().len())],
            );
        }

        Ok(blob)
    }

    /// Checks if the desired slivers are missing.
    fn check_missing_slivers(&self, start_idx: usize, end_idx: usize) -> Result<(), QuiltError> {
        let mut missing_slivers = Vec::new();
        for i in start_idx..end_idx {
            let sliver_idx = SliverIndex(i as u16);
            if !self.slivers.contains_key(&sliver_idx) {
                missing_slivers.push(sliver_idx);
            }
        }
        if !missing_slivers.is_empty() {
            return Err(QuiltError::MissingSlivers(missing_slivers));
        }
        Ok(())
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
    /// * `max_num_columns_for_quilt_index` - The maximum number of columns that can be used to
    ///   store the quilt index.
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
            return Err(QuiltError::TooManyBlobs(blobs_sizes.len(), n_columns - 1));
        }

        if blobs_sizes.is_empty() {
            return Err(QuiltError::Other(
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
            return Err(QuiltError::QuiltOversize(format!(
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
    /// * `bool` - True if the blobs can fit in the given number of columns, false otherwise.
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

    /// Get the data size of the quilt index.
    pub fn get_quilt_index_data_size(combined_data: &[u8]) -> Result<usize, QuiltError> {
        if combined_data.len() < QUILT_INDEX_SIZE_PREFIX_SIZE + QUILT_TYPE_SIZE {
            return Err(QuiltError::FailedToExtractQuiltIndexSize);
        }

        let data_size = u64::from_le_bytes(
            combined_data[QUILT_TYPE_SIZE..QUILT_INDEX_SIZE_PREFIX_SIZE + QUILT_TYPE_SIZE]
                .try_into()
                .map_err(|_| QuiltError::FailedToExtractQuiltIndexSize)?,
        );
        let data_size = usize::try_from(data_size).expect("data_size should fit in usize");

        Ok(data_size)
    }

    pub fn get_quilt_version_byte(data: &[u8]) -> Result<u8, QuiltError> {
        if data.is_empty() {
            return Err(QuiltError::Other(
                "failed to get quilt version byte: data is empty".to_string(),
            ));
        }
        Ok(data[0])
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
            return Err(QuiltError::InvalidFormatNotAligned(format!(
                "row_size: {}, symbol_size: {}, data.len(): {}",
                row_size,
                symbol_size,
                data.len()
            )));
        }

        let n_rows = data.len() / row_size;
        if i >= (row_size / symbol_size) {
            return Err(QuiltError::IndexOutOfBounds(i, row_size / symbol_size));
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
            QuiltError::QuiltIndexSerDerError("failed to extract first column".to_string())
        })?;

        utils::check_quilt_version::<QuiltVersionV1>(&first_column)?;

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
            .map_err(|e| QuiltError::QuiltIndexSerDerError(e.to_string()))?;

        quilt_index.populate_start_indices(
            u16::try_from(current_column).expect("current_column should fit in u16"),
        );
        Ok(quilt_index)
    }

    /// Checks the quilt version.
    pub fn check_quilt_version<V: QuiltVersion>(data: &[u8]) -> Result<(), QuiltError> {
        let quilt_version_byte = get_quilt_version_byte(data)?;
        if quilt_version_byte != V::quilt_version_byte() {
            return Err(QuiltError::QuiltVersionMismatch(
                quilt_version_byte,
                V::quilt_version_byte(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use alloc::boxed::Box;
    use core::num::NonZeroU16;

    use walrus_test_utils::{param_test, random_data};

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
            case_1: (&[2, 1, 2, 1], 3, 3, 1, Err(QuiltError::TooManyBlobs(4, 2))),
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
                        identifier: "test-blob-0".to_string(),
                    },
                    BlobWithIdentifier {
                        blob: &[5, 68, 3, 2, 5][..],
                        identifier: "test-blob-1".to_string(),
                    },
                    BlobWithIdentifier {
                        blob: &[5, 68, 3, 2, 5, 6, 78, 8][..],
                        identifier: "test-blob-2".to_string(),
                    },
                ],
                7
            ),
            case_0_random_order: (
                &[
                    BlobWithIdentifier {
                        blob: &[5, 68, 3, 2, 5, 6, 78, 8][..],
                        identifier: "test-blob-0".to_string(),
                    },
                    BlobWithIdentifier {
                        blob: &[5, 68, 3, 2, 5][..],
                        identifier: "test-blob-1".to_string(),
                    },
                    BlobWithIdentifier {
                        blob: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..],
                        identifier: "test-blob-2".to_string(),
                    },
                ],
                7
            ),
            case_1: (
                &[
                    BlobWithIdentifier {
                        blob: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..],
                        identifier: "test-blob-0".to_string(),
                    },
                    BlobWithIdentifier {
                        blob: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..],
                        identifier: "test-blob-1".to_string(),
                    },
                    BlobWithIdentifier {
                        blob: &[5, 68, 3, 2, 5, 6, 78, 8][..],
                        identifier: "test-blob-2".to_string(),
                    },
                ],
                7
            ),
            case_1_random_order: (
                &[
                    BlobWithIdentifier {
                        blob: &[5, 68, 3, 2, 5][..],
                        identifier: "test-blob-0".to_string(),
                    },
                    BlobWithIdentifier {
                        blob: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..],
                        identifier: "test-blob-1".to_string(),
                    },
                    BlobWithIdentifier {
                        blob: &[5, 68, 3, 2, 5, 6, 78, 8][..],
                        identifier: "test-blob-2".to_string(),
                    },
                ],
                7
            ),
            case_2: (
                &[
                    BlobWithIdentifier {
                        blob: &[1, 3][..],
                        identifier: "test-blob-0".to_string(),
                    },
                    BlobWithIdentifier {
                        blob: &[255u8; 1024][..],
                        identifier: "test-blob-1".to_string(),
                    },
                    BlobWithIdentifier {
                        blob: &[1, 2, 3][..],
                        identifier: "test-blob-2".to_string(),
                    },
                ],
                12
            ),
            case_3: (
                &[
                    BlobWithIdentifier {
                        blob: &[9, 8, 7, 6, 5, 4, 3, 2, 1][..],
                        identifier: "test-blob-0".to_string(),
                    },
                ],
                7
            ),
        ]
    }
    fn test_quilt_construct_quilt(
        blobs_with_identifiers: &[BlobWithIdentifier<'_>],
        n_shards: u16,
    ) {
        let raptorq_config = RaptorQEncodingConfig::new(NonZeroU16::try_from(n_shards).unwrap());
        let reed_solomon_config =
            ReedSolomonEncodingConfig::new(NonZeroU16::try_from(n_shards).unwrap());

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

        let encoder = QuiltConfigV1::get_encoder(config.clone(), blobs_with_identifiers);

        let quilt = encoder.construct_quilt().expect("Should construct quilt");

        // Verify each blob and its description.
        for blob_with_identifier in blobs_with_identifiers {
            // Verify blob data matches.
            let extracted_blob = quilt
                .get_blob_by_identifier(blob_with_identifier.identifier.as_str())
                .expect("Patch should exist for this blob identifier");
            assert_eq!(
                extracted_blob, blob_with_identifier.blob,
                "Mismatch in encoded blob"
            );

            let quilt_patch = quilt
                .quilt_index()
                .get_quilt_patch_by_identifier(blob_with_identifier.identifier.as_str())
                .expect("Patch should exist for this blob ID");
            assert_eq!(
                quilt_patch.identifier, blob_with_identifier.identifier,
                "Mismatch in blob description"
            );

            let blob_by_identifier = quilt
                .get_blob_by_identifier(blob_with_identifier.identifier.as_str())
                .expect("Should be able to get blob by identifier");
            assert_eq!(blob_by_identifier, blob_with_identifier.blob);
        }

        assert_eq!(quilt.quilt_index().len(), blobs_with_identifiers.len());
    }

    param_test! {
        test_quilt_encoder_and_decoder: [
            case_0: (3, 5, 16, 7),
            case_1: (3, 3, 800, 7),
            case_2: (3, 1024, 10240, 7),
            case_3: (1, 10, 1000, 7),
            case_4: (60, 1, 1000, 100),
        ]
    }
    fn test_quilt_encoder_and_decoder(
        num_blobs: usize,
        min_blob_size: usize,
        max_blob_size: usize,
        n_shards: u16,
    ) {
        let blobs_with_identifiers = generate_random_blobs(num_blobs, max_blob_size, min_blob_size);

        let raptorq_config = RaptorQEncodingConfig::new(NonZeroU16::try_from(n_shards).unwrap());
        let reed_solomon_config =
            ReedSolomonEncodingConfig::new(NonZeroU16::try_from(n_shards).unwrap());

        encode_decode_quilt(
            &blobs_with_identifiers,
            EncodingConfigEnum::RaptorQ(&raptorq_config),
        );
        encode_decode_quilt(
            &blobs_with_identifiers,
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
        tracing::trace!(
            "Sliver pairs: {:?}\nQuilt metadata: {:?}",
            sliver_pairs,
            quilt_metadata
        );

        let QuiltMetadata::V1(quilt_metadata_v1) = quilt_metadata;

        let slivers: Vec<&SliverData<Secondary>> = sliver_pairs
            .iter()
            .map(|sliver_pair| &sliver_pair.secondary)
            .collect();

        let first_sliver = slivers
            .iter()
            .find(|sliver| sliver.index == SliverIndex::new(0))
            .expect("Should find first sliver");
        let quilt_version =
            get_quilt_version_enum(first_sliver.symbols.data()).expect("Should get quilt version");
        assert!(matches!(quilt_version, QuiltVersionEnum::V1));
        let mut quilt_decoder = QuiltConfigV1::get_decoder(&[]);
        assert!(matches!(
            quilt_decoder.get_or_decode_quilt_index(),
            Err(QuiltError::MissingSlivers(_))
        ));

        let sliver_vec = vec![*first_sliver];
        quilt_decoder.add_slivers(&sliver_vec);
        assert_eq!(
            quilt_decoder.get_or_decode_quilt_index(),
            Ok(&quilt_metadata_v1.index)
        );

        let identifier = blobs_with_identifiers
            .first()
            .expect("Test requires at least one blob")
            .identifier
            .as_str();
        let patch = quilt_decoder
            .get_or_decode_quilt_index()
            .expect("quilt index should exist")
            .get_quilt_patch_by_identifier(identifier)
            .expect("quilt patch should exist");
        assert_eq!(patch.identifier, identifier);

        let missing_indices: Vec<SliverIndex> = (patch.start_index..patch.end_index)
            .map(SliverIndex)
            .collect();
        assert_eq!(
            quilt_decoder.get_blob_by_identifier(identifier),
            Err(QuiltError::MissingSlivers(missing_indices))
        );

        // Now, add all slivers to the decoder, all the blobs should be reconstructed.
        quilt_decoder.add_slivers(&slivers);

        for blob_with_identifier in blobs_with_identifiers {
            tracing::info!("decoding blob {}", blob_with_identifier.identifier);
            let blob =
                quilt_decoder.get_blob_by_identifier(blob_with_identifier.identifier.as_str());
            assert_eq!(blob, Ok(blob_with_identifier.blob.to_vec()));
        }

        let mut decoder = config
            .get_blob_decoder::<Secondary>(quilt_metadata_v1.metadata.unencoded_length())
            .expect("Should create decoder");

        let (quilt_blob, metadata_with_id) = decoder
            .decode_and_verify(
                &quilt_metadata_v1.quilt_blob_id,
                sliver_pairs
                    .iter()
                    .map(|s| s.secondary.clone())
                    .collect::<Vec<_>>(),
            )
            .expect("Should decode and verify quilt")
            .expect("Should decode quilt");

        assert_eq!(metadata_with_id.metadata(), &quilt_metadata_v1.metadata);

        let quilt = QuiltV1::new_from_quilt_blob(quilt_blob, &config).expect("Should create quilt");
        assert_eq!(
            quilt.data(),
            encoder
                .construct_quilt()
                .expect("Should construct quilt")
                .data()
        );
    }

    /// Generate random blobs with sizes in the specified range.
    ///
    /// # Arguments
    ///
    /// * `num_blobs` - Number of blobs to generate
    /// * `max_blob_size` - Maximum size of each blob
    /// * `min_blob_size` - Minimum size of each blob
    ///
    /// # Returns
    ///
    /// A vector of BlobWithIdentifier objects with random content.
    fn generate_random_blobs(
        num_blobs: usize,
        max_blob_size: usize,
        min_blob_size: usize,
    ) -> Vec<BlobWithIdentifier<'static>> {
        use rand::{Rng, SeedableRng, rngs::StdRng};

        // Create a deterministic RNG with a fixed seed for reproducibility.
        let mut rng = StdRng::seed_from_u64(42);

        // Store both blobs and their BlobWithIdentifier wrappers.
        let mut result = Vec::with_capacity(num_blobs);

        // Generate random blobs with sizes in the specified range.
        for i in 0..num_blobs {
            // Generate a random size in the range [min_blob_size, max_blob_size).
            let blob_size = if min_blob_size == max_blob_size {
                min_blob_size
            } else {
                rng.gen_range(min_blob_size..max_blob_size)
            };

            let blob_data = random_data(blob_size);

            // Convert to static lifetime using Box::leak.
            let static_data = Box::leak(blob_data.into_boxed_slice());

            // Create and store the BlobWithIdentifier.
            result.push(BlobWithIdentifier::new(
                static_data,
                format!("test-blob-{}", i),
            ));
        }

        result
    }
}
