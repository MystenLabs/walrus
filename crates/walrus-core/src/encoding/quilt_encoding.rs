// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// *******************************************************************************
// DISCLAIMER: The quilt format is still under active development.
// Please use with caution as it is subject to change without prior notice.
// *******************************************************************************

#![allow(dead_code)] // TODO: remove this once follow up PRs are merged.

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
    SliverType,
    encoding::{
        EncodingAxis,
        MAX_SYMBOL_SIZE,
        QuiltError,
        blob_encoding::BlobEncoder,
        config::EncodingConfigTrait as _,
    },
    metadata::{QuiltIndex, QuiltIndexV1, QuiltMetadata, QuiltMetadataV1, QuiltPatchV1},
};

/// The number of bytes to store the size of the quilt index.
const QUILT_INDEX_SIZE_BYTES_LENGTH: usize = 4;

/// The number of bytes used to store the version of the quilt.
const QUILT_VERSION_BYTES_LENGTH: usize = 1;

/// The number of bytes used to store the identifier of the blob.
const BLOB_IDENTIFIER_BYTES_LENGTH: usize = 2;

/// The number of bytes stored before the quilt index data.
const QUILT_INDEX_PREFIX_SIZE: usize = QUILT_INDEX_SIZE_BYTES_LENGTH + QUILT_VERSION_BYTES_LENGTH;

/// The maximum number of columns a quilt index can have.
const MAX_NUM_COLUMNS_FOR_QUILT_INDEX: usize = 1;

/// Gets the quilt version enum from the data.
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
    type QuiltIndex: Clone + Iterator<Item = Self::QuiltPatch> + Into<QuiltIndex>;
    /// The type of the quilt patch.
    type QuiltPatch: Clone + QuiltPatchApi<Self>;
    /// The type of the quilt patch id.
    type QuiltPatchId: QuiltPatchIdApi;
    /// The type of the quilt metadata.
    type QuiltMetadata;
    /// The sliver type used by this quilt version.
    type SliverAxis: EncodingAxis;

    /// The serialized bytes of the quilt type.
    fn quilt_version_byte() -> u8;
}

/// API to access a quilt.
pub trait QuiltApi<V: QuiltVersion> {
    /// Returns a new quilt from a quilt blob.
    fn new_from_quilt_blob(
        quilt_blob: Vec<u8>,
        encoding_config: &EncodingConfigEnum<'_>,
    ) -> Result<V::Quilt, QuiltError>;

    /// Gets a blob by its identifier from the quilt.
    fn get_blob_by_identifier(&self, identifier: &str) -> Result<QuiltBlobOwned, QuiltError>;

    /// Returns the quilt index.
    fn quilt_index(&self) -> &V::QuiltIndex;

    /// Returns the data of the quilt.
    fn data(&self) -> &[u8];

    /// Returns the symbol size of the quilt.
    fn symbol_size(&self) -> usize;
}

/// API for QuiltPatch.
pub trait QuiltPatchApi<V: QuiltVersion>: Clone {
    /// Returns the quilt patch id.
    fn quilt_patch_id(&self) -> V::QuiltPatchId;

    /// Returns the identifier of the quilt patch.
    fn identifier(&self) -> &str;
}
/// API for QuiltPatchId.
pub trait QuiltPatchIdApi: Clone {
    /// The serialized bytes of the quilt patch id.
    fn to_bytes(&self) -> Vec<u8>;

    /// Deserializes the quilt patch id from bytes.
    fn from_bytes(bytes: &[u8]) -> Result<Self, QuiltError>;
}

/// The configuration of the quilt.
pub trait QuiltConfigApi<'a, V: QuiltVersion> {
    /// Returns a new encoder for the given configuration and blobs.
    fn get_encoder(
        encoding_config: EncodingConfigEnum<'a>,
        blobs: &'a [QuiltStoreBlob<'a>],
    ) -> V::QuiltEncoder<'a>;

    /// Returns a new decoder for the given slivers.
    fn get_decoder(slivers: &'a [&'a SliverData<V::SliverAxis>]) -> V::QuiltDecoder<'a>;

    /// Returns a new decoder for the given slivers and quilt index.
    fn get_decoder_with_quilt_index(
        slivers: &'a [&'a SliverData<V::SliverAxis>],
        quilt_index: &QuiltIndex,
    ) -> V::QuiltDecoder<'a>;
}

/// Encoder to construct a quilt and encode the blobs into slivers.
pub trait QuiltEncoderApi<V: QuiltVersion> {
    /// Constructs a quilt by encoding the blobs.
    fn construct_quilt(&self) -> Result<V::Quilt, QuiltError>;

    /// Encodes the blobs into a quilt and returns the slivers.
    fn encode(&self) -> Result<Vec<SliverPair>, QuiltError>;

    /// Encodes the blobs into a quilt and returns the slivers and metadata.
    fn encode_with_metadata(&self) -> Result<(Vec<SliverPair>, QuiltMetadata), QuiltError>;
}

/// Decoder to decode a quilt from slivers.
pub trait QuiltDecoderApi<'a, V: QuiltVersion> {
    /// Decodes the quilt index from received slivers.
    ///
    /// The decoded quilt index is stored in the decoder and can be retrieved
    /// using the `get_quilt_index` method after this method returns.
    fn get_or_decode_quilt_index(&mut self) -> Result<QuiltIndex, QuiltError>;

    /// Gets a blob by its identifier from the quilt.
    fn get_blob_by_identifier(&self, identifier: &str) -> Result<QuiltBlobOwned, QuiltError>;

    /// Adds slivers to the decoder.
    fn add_slivers(&mut self, slivers: &'a [&'a SliverData<V::SliverAxis>]);

    /// Returns the sliver axis used by the decoder.
    fn sliver_type(&self) -> SliverType;
}

/// A unified trait for iterating over column-based quilt data from different sources.
pub trait QuiltColumnDataIterator {
    type RangeIter<'a>: Iterator<Item = &'a [u8]> + 'a
    where
        Self: 'a;

    /// Returns an iterator over byte in the columns identified by the given start and end indices.
    ///
    /// Assuming a 0-indexed bytes, the iterator will return the bytes in the range
    /// `[start_byte, start_byte + limit_bytes)`.
    fn iter_column_bytes(
        &self,
        start_col: usize,
        end_col: usize,
        start_byte: usize,
        limit_bytes: usize,
    ) -> Result<Self::RangeIter<'_>, QuiltError>;
}

/// An iterator over symbols within a specified column range of a Quilt.
#[derive(Debug)]
pub struct SymbolIter<'a> {
    quilt_data: &'a [u8],
    row_size: usize,
    symbol_size: usize,
    n_rows: usize,

    // Track the iter position.
    current_col: usize,
    end_col: usize,
    current_row: usize,
    bytes_to_skip: usize,
    bytes_remaining: usize,
}

impl<'a> SymbolIter<'a> {
    /// Creates a new iterator for symbols within a defined columnar slice of the quilt.
    fn new(
        quilt_data: &'a [u8],
        row_size: usize,
        symbol_size: usize,
        start_col: usize,
        end_col: usize,
        begin_position: usize,
        limit: usize,
    ) -> Result<Self, QuiltError> {
        if symbol_size == 0 || row_size == 0 || quilt_data.is_empty() {
            return Err(QuiltError::Other("empty quilt data".to_string()));
        }

        let total_cols_in_quilt = row_size / symbol_size;
        if end_col > total_cols_in_quilt {
            return Err(QuiltError::IndexOutOfBounds(end_col, total_cols_in_quilt));
        }

        let n_rows = quilt_data.len() / row_size;

        Ok(Self {
            quilt_data,
            row_size,
            symbol_size,
            n_rows,
            current_col: start_col,
            end_col,
            current_row: 0,
            bytes_to_skip: begin_position,
            bytes_remaining: limit,
        })
    }
}

impl<'a> Iterator for SymbolIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.bytes_remaining == 0 {
            return None;
        }

        while self.bytes_to_skip > 0 {
            if let Some(symbol_slice) = self.next_internal() {
                if symbol_slice.len() > self.bytes_to_skip {
                    let start_index = self.bytes_to_skip;
                    let end_index = symbol_slice.len().min(self.bytes_remaining + start_index);
                    self.bytes_to_skip = 0;
                    self.bytes_remaining -= end_index - start_index;
                    return Some(&symbol_slice[start_index..end_index]);
                }
                self.bytes_to_skip -= symbol_slice.len();
            } else {
                return None;
            }
        }

        if let Some(symbol_slice) = self.next_internal() {
            let end_index = symbol_slice.len().min(self.bytes_remaining);
            self.bytes_remaining -= end_index;
            return Some(&symbol_slice[..end_index]);
        }
        None
    }
}

impl<'a> SymbolIter<'a> {
    fn next_internal(&mut self) -> Option<&'a [u8]> {
        if self.current_col >= self.end_col
            || self.current_row >= self.n_rows
            || self.bytes_remaining == 0
        {
            return None;
        }

        let matrix_idx_start =
            (self.current_row * self.row_size) + (self.current_col * self.symbol_size);
        let matrix_idx_end = (matrix_idx_start + self.symbol_size).min(self.quilt_data.len());

        let symbol_slice = &self.quilt_data[matrix_idx_start..matrix_idx_end];

        self.current_row += 1;
        if self.current_row == self.n_rows {
            self.current_row = 0;
            self.current_col += 1;
        }

        Some(symbol_slice)
    }
}

/// Iterator over bytes from slivers.
#[derive(Debug)]
pub struct SliverBytesIterator<'a> {
    slivers: Vec<&'a SliverData<Secondary>>,
    current_sliver_idx: usize,
    current_offset: usize,
    bytes_to_skip: usize,
    bytes_remaining: usize,
}

impl<'a> SliverBytesIterator<'a> {
    fn new(
        slivers: Vec<&'a SliverData<Secondary>>,
        skip_bytes: usize,
        limit_bytes: usize,
    ) -> Result<Self, QuiltError> {
        Ok(Self {
            slivers,
            current_sliver_idx: 0,
            current_offset: 0,
            bytes_to_skip: skip_bytes,
            bytes_remaining: limit_bytes,
        })
    }

    fn next_n_bytes(&mut self, n_bytes: usize) -> Option<&'a [u8]> {
        while self.current_sliver_idx < self.slivers.len()
            && self.current_offset >= self.slivers[self.current_sliver_idx].symbols.data().len()
        {
            self.current_sliver_idx += 1;
            self.current_offset = 0;
        }

        let sliver = self.slivers.get(self.current_sliver_idx)?;
        let sliver_data = sliver.symbols.data();

        let end_index = sliver_data.len().min(self.current_offset + n_bytes);
        let result = &sliver_data[self.current_offset..end_index];
        self.current_offset = end_index;

        Some(result)
    }
}

impl<'a> Iterator for SliverBytesIterator<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.bytes_remaining == 0 {
            return None;
        }

        // Handle skipping
        while self.bytes_to_skip > 0 {
            match self.next_n_bytes(self.bytes_to_skip) {
                Some(slice) => {
                    if slice.len() > self.bytes_to_skip {
                        // Partially skip this slice.
                        let start = self.bytes_to_skip;
                        let result = &slice[start..];
                        self.bytes_to_skip = 0;

                        let yield_len = result.len().min(self.bytes_remaining);
                        self.bytes_remaining -= yield_len;
                        return Some(&result[..yield_len]);
                    } else {
                        // Skip entire slice
                        self.bytes_to_skip -= slice.len();
                    }
                }
                None => return None,
            }
        }

        // Regular iteration after skipping
        match self.next_n_bytes(self.bytes_remaining) {
            Some(slice) => {
                let yield_len = slice.len().min(self.bytes_remaining);
                self.bytes_remaining -= yield_len;
                Some(&slice[..yield_len])
            }
            None => None,
        }
    }
}

/// The version of the quilt.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
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

impl fmt::Display for QuiltVersionEnum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<String> for QuiltVersionEnum {
    fn from(value: String) -> Self {
        match value.as_str() {
            "V1" | "v1" | "1" => QuiltVersionEnum::V1,
            _ => QuiltVersionEnum::V1, // Default or consider error
        }
    }
}

impl QuiltVersionEnum {
    /// Creates a new `QuiltVersionEnum` from a sliver.
    pub fn new_from_sliver(sliver: &[u8]) -> Result<QuiltVersionEnum, QuiltError> {
        if sliver.is_empty() {
            return Err(QuiltError::EmptyInput("Sliver".to_string()));
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
    /// Construct a new `QuiltEnum`.
    pub fn new(
        quilt_blob: Vec<u8>,
        encoding_config: &EncodingConfigEnum<'_>,
    ) -> Result<QuiltEnum, QuiltError> {
        let quilt_version = QuiltVersionEnum::new_from_sliver(&quilt_blob)?;
        match quilt_version {
            QuiltVersionEnum::V1 => {
                QuiltV1::new_from_quilt_blob(quilt_blob, encoding_config).map(QuiltEnum::V1)
            }
        }
    }

    /// Returns the blob identified by the given identifier.
    pub fn get_blob_by_identifier(&self, identifier: &str) -> Result<QuiltBlobOwned, QuiltError> {
        match self {
            QuiltEnum::V1(quilt_v1) => quilt_v1.get_blob_by_identifier(identifier),
        }
    }

    /// Returns the quilt index.
    pub fn get_quilt_index(&self) -> Result<QuiltIndex, QuiltError> {
        match self {
            QuiltEnum::V1(quilt_v1) => Ok(QuiltIndex::V1(quilt_v1.quilt_index.clone())),
        }
    }
}

impl From<QuiltV1> for QuiltEnum {
    fn from(quilt_v1: QuiltV1) -> Self {
        QuiltEnum::V1(quilt_v1)
    }
}

/// A wrapper around a blob and its identifier.
///
/// A valid identifier is a string that contains only alphanumeric characters,
/// underscores, hyphens, and periods.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuiltStoreBlob<'a> {
    blob: &'a [u8],
    identifier: String,
    /// The attributes of the blob.
    pub attributes: HashMap<String, String>,
}

impl<'a> QuiltStoreBlob<'a> {
    /// Creates a new `QuiltStoreBlob` from a blob and an identifier.
    pub fn new(blob: &'a [u8], identifier: impl Into<String>) -> Self {
        Self {
            blob,
            identifier: identifier.into(),
            attributes: HashMap::new(),
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
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct QuiltBlobOwned {
    /// The blob data.
    pub blob: Vec<u8>,
    /// The identifier of the blob.
    pub identifier: String,
    /// The attributes of the blob.
    pub attributes: HashMap<String, String>,
    /// QuiltBlobId of the blob.
    pub quilt_batch_id_bytes: Vec<u8>,
}

// Implement cross-type equality between QuiltStoreBlob and QuiltBlobOwned
impl PartialEq<QuiltBlobOwned> for QuiltStoreBlob<'_> {
    fn eq(&self, other: &QuiltBlobOwned) -> bool {
        self.blob == other.blob.as_slice()
            && self.identifier == other.identifier
            && self.attributes == other.attributes
    }
}

impl PartialEq<QuiltStoreBlob<'_>> for QuiltBlobOwned {
    fn eq(&self, other: &QuiltStoreBlob<'_>) -> bool {
        self.blob.as_slice() == other.blob
            && self.identifier == other.identifier
            && self.attributes == other.attributes
    }
}

impl QuiltBlobOwned {
    /// Creates a new `QuiltBlobOwned` from an owned blob and an identifier.
    pub fn new(blob: Vec<u8>, identifier: impl Into<String>) -> Self {
        Self {
            blob,
            identifier: identifier.into(),
            attributes: HashMap::new(),
            quilt_batch_id_bytes: Vec::new(),
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
    const BLOB_HEADER_SIZE: usize = 7;
    const EXTENSION_SIZE_BYTES_LENGTH: usize = 4;

    /// Returns the total size of the encoded blob.
    pub fn compute_encoded_blob_size(blob: &QuiltStoreBlob) -> Result<usize, QuiltError> {
        let mut extension_size = 0;

        let identifier_bytes_size = bcs::serialized_size(&blob.identifier)
            .map_err(|e| QuiltError::Other(format!("Failed to compute identifier size: {}", e)))?;
        let identifier_size = u16::try_from(identifier_bytes_size).map_err(|e| {
            QuiltError::Other(format!(
                "Identifier size exceeds maximum allowed value: {}",
                e
            ))
        })?;

        extension_size += identifier_size as usize + BLOB_IDENTIFIER_BYTES_LENGTH;

        if !blob.attributes.is_empty() {
            let attributes_size = bcs::serialized_size(&blob.attributes).map_err(|e| {
                QuiltError::Other(format!("Failed to compute attributes size: {}", e))
            })?;
            extension_size += attributes_size + QuiltVersionV1::EXTENSION_SIZE_BYTES_LENGTH;
        }

        let total_size = extension_size + blob.data().len() + QuiltVersionV1::BLOB_HEADER_SIZE;
        Ok(total_size)
    }

    /// Decodes a blob from a column data source.
    pub fn decode_blob<T>(
        data_source: &T,
        start_col: usize,
        end_col: usize,
    ) -> Result<QuiltBlobOwned, QuiltError>
    where
        T: QuiltColumnDataIterator,
    {
        let mut blob = QuiltBlobOwned::default();

        let mut header_bytes = Vec::new();
        {
            let iter =
                data_source.iter_column_bytes(start_col, end_col, 0, Self::BLOB_HEADER_SIZE)?;
            for symbol in iter {
                header_bytes.extend_from_slice(symbol);
            }
        }
        assert!(header_bytes.len() == Self::BLOB_HEADER_SIZE);
        let blob_header = BlobHeaderV1::from_bytes(
            header_bytes
                .try_into()
                .expect("header_bytes should be 7 bytes"),
        );

        let mut offset = Self::BLOB_HEADER_SIZE;
        let mut identifier_bytes = Vec::new();
        let mut blob_bytes_size = usize::try_from(blob_header.length).map_err(|e| {
            QuiltError::Other(format!("Blob length exceeds maximum allowed value: {}", e))
        })?;
        {
            let iter = data_source.iter_column_bytes(
                start_col,
                end_col,
                offset,
                BLOB_IDENTIFIER_BYTES_LENGTH,
            )?;
            let mut size_buffer = Vec::new();
            for symbol in iter {
                size_buffer.extend_from_slice(symbol);
            }
            offset += BLOB_IDENTIFIER_BYTES_LENGTH;
            let identifier_size = u16::from_le_bytes(
                size_buffer
                    .try_into()
                    .expect("size_buffer should be 2 bytes"),
            );
            let identifier_iter = data_source.iter_column_bytes(
                start_col,
                end_col,
                offset,
                identifier_size as usize,
            )?;
            for symbol in identifier_iter {
                identifier_bytes.extend_from_slice(symbol);
            }
            offset += identifier_size as usize;
            blob_bytes_size -= identifier_size as usize + BLOB_IDENTIFIER_BYTES_LENGTH;
        }
        blob.identifier = bcs::from_bytes(&identifier_bytes)
            .map_err(|_| QuiltError::Other("Failed to deserialize identifier".into()))?;

        if blob_header.has_attributes() {
            let mut size_bytes = Vec::new();
            let iter = data_source.iter_column_bytes(
                start_col,
                end_col,
                offset,
                Self::EXTENSION_SIZE_BYTES_LENGTH,
            )?;
            for symbol in iter {
                size_bytes.extend_from_slice(symbol);
            }
            assert!(size_bytes.len() == Self::EXTENSION_SIZE_BYTES_LENGTH);
            blob_bytes_size -= size_bytes.len();
            let attributes_size =
                u32::from_le_bytes(size_bytes.try_into().expect("size_bytes should be 4 bytes"));
            offset += Self::EXTENSION_SIZE_BYTES_LENGTH;
            let attribute_iter = data_source.iter_column_bytes(
                start_col,
                end_col,
                offset,
                attributes_size as usize,
            )?;
            let mut attributes_bytes = Vec::new();
            for symbol in attribute_iter {
                attributes_bytes.extend_from_slice(symbol);
            }
            assert!(attributes_bytes.len() == attributes_size as usize);
            blob.attributes = bcs::from_bytes(&attributes_bytes)
                .map_err(|_| QuiltError::Other("Failed to deserialize attributes".into()))?;
            offset += attributes_size as usize;
            blob_bytes_size -= attributes_size as usize;
        }

        let mut data_bytes = Vec::new();
        let iter = data_source.iter_column_bytes(start_col, end_col, offset, blob_bytes_size)?;
        for symbol in iter {
            data_bytes.extend_from_slice(symbol);
        }
        assert!(data_bytes.len() == blob_bytes_size);
        blob.blob = data_bytes;
        let start_index = u16::try_from(start_col)
            .map_err(|_| QuiltError::Other("Start index exceeds maximum allowed value".into()))?;
        let end_index = u16::try_from(end_col).map_err(|e| {
            QuiltError::Other(format!("End index exceeds maximum allowed value: {}", e))
        })?;
        blob.quilt_batch_id_bytes = QuiltPatchIdV1::new(start_index, end_index).to_bytes();

        Ok(blob)
    }
}

impl QuiltVersion for QuiltVersionV1 {
    type QuiltConfig = QuiltConfigV1;
    type QuiltEncoder<'a> = QuiltEncoderV1<'a>;
    type QuiltDecoder<'a> = QuiltDecoderV1<'a>;
    type Quilt = QuiltV1;
    type QuiltIndex = QuiltIndexV1;
    type QuiltPatch = QuiltPatchV1;
    type QuiltPatchId = QuiltPatchIdV1;
    type QuiltMetadata = QuiltMetadataV1;
    type SliverAxis = Secondary;

    fn quilt_version_byte() -> u8 {
        QuiltVersionV1::QUILT_VERSION_BYTE
    }
}

/// QuiltPatchIdV1.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuiltPatchIdV1 {
    /// The start index of the patch.
    pub start_index: u16,
    /// The end index of the patch.
    pub end_index: u16,
}

/// Definition of the layout of the quilt patch id in QuiltVersionV1.
//┌─────────────┬──────────────────┬─────────────────────────────┬─────────────────────────┐
//│    Byte 0   │      Byte 1      │           Byte 2            │         Byte 3          │
//├─────────────┼──────────────────┼─────────────────────────────┼─────────────────────────┤
//│  Version    │   start_index    │ start_index │   end_index   │  end_index  │  Padding  │
//│    Byte     │  (upper 8 bits)  │ (lower 2)   │ (upper 6 bits)│ (lower 4)   │  (zeros)  │
//├─────────────┼──────────────────┼─────────────┼───────────────┼─────────────┼───────────┤
//│     8 bits  │     8 bits       │   2 bits    │    6 bits     │   4 bits    │  4 bits   │
//└─────────────┴──────────────────┴─────────────┴───────────────┴─────────────┴───────────┘
impl QuiltPatchIdApi for QuiltPatchIdV1 {
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(4);

        // First byte is the version byte.
        bytes.push(QuiltVersionV1::quilt_version_byte());

        // Ensure indexes don't exceed 10 bits (0-1023).
        debug_assert!(
            self.start_index <= 0x3FF,
            "start_index exceeds 10 bits: {}",
            self.start_index
        );
        debug_assert!(
            self.end_index <= 0x3FF,
            "end_index exceeds 10 bits: {}",
            self.end_index
        );

        // Truncate to 10 bits each.
        let start = self.start_index & 0x3FF;
        let end = self.end_index & 0x3FF;

        // Pack the 20 bits into 3 bytes:
        // Byte 1: upper 8 bits of start_index.
        bytes.push((start >> 2) as u8);

        // Byte 2: lower 2 bits of start_index + upper 6 bits of end_index.
        bytes.push(((start & 0x3) << 6 | (end >> 4)) as u8);

        // Byte 3: lower 4 bits of end_index + 4 bits of padding (zeros)
        bytes.push(((end & 0xF) << 4) as u8);

        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, QuiltError> {
        if bytes.len() < 4 {
            return Err(QuiltError::Other(
                "QuiltPatchIdV1 requires 4 bytes".to_string(),
            ));
        }

        // Check version byte.
        if bytes[0] != QuiltVersionV1::quilt_version_byte() {
            return Err(QuiltError::QuiltVersionMismatch(
                bytes[0],
                QuiltVersionV1::quilt_version_byte(),
            ));
        }

        // Extract start_index (10 bits).
        // Upper 8 bits from byte 1.
        let start_high = (bytes[1] as u16) << 2;
        // Lower 2 bits from byte 2 (highest 2 bits).
        let start_low = (bytes[2] >> 6) as u16;
        let start_index = start_high | start_low;

        // Extract end_index (10 bits).
        // Upper 6 bits from byte 2 (lowest 6 bits).
        let end_high = ((bytes[2] & 0x3F) as u16) << 4;
        // Lower 4 bits from byte 3 (highest 4 bits).
        let end_low = (bytes[3] >> 4) as u16;
        let end_index = end_high | end_low;

        Ok(Self {
            start_index,
            end_index,
        })
    }
}

impl QuiltPatchIdV1 {
    /// Creates a new quilt patch id.
    pub fn new(start_index: u16, end_index: u16) -> Self {
        Self {
            start_index,
            end_index,
        }
    }
}

/// The header of a encoded blob in QuiltVersionV1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BlobHeaderV1 {
    pub length: u64, // the lower 34 bits are used for the length.
    pub mask: u16,   // the lower 14 bits are used for the mask.
}

impl BlobHeaderV1 {
    const LENGTH_BITS: u32 = 34;
    const MASK_BITS: u32 = 14;
    const METADATA_ENABLED: u16 = 1;

    const LENGTH_MAX: u64 = (1u64 << Self::LENGTH_BITS) - 1;
    const MASK_MAX: u16 = (1u16 << Self::MASK_BITS) - 1;

    /// Creates a new header with length and mask initialized to 0.
    pub fn new(length: u64, mask: u16) -> Result<Self, QuiltError> {
        if length > Self::LENGTH_MAX {
            return Err(QuiltError::Other(
                "Length exceeds maximum allowed value".to_string(),
            ));
        }
        if mask > Self::MASK_MAX {
            return Err(QuiltError::Other(
                "Mask exceeds maximum allowed value".to_string(),
            ));
        }
        Ok(Self { length, mask })
    }

    /// Creates a `BlobHeaderV1` from a 7-byte array.
    /// The layout is: version (1 byte), length (next 34 bits), mask (next 14 bits).
    pub fn from_bytes(bytes: [u8; QuiltVersionV1::BLOB_HEADER_SIZE]) -> Self {
        let version = bytes[0];
        assert_eq!(version, QuiltVersionV1::QUILT_VERSION_BYTE);
        let mut length: u64 = 0;
        length |= u64::from(bytes[1]);
        length |= u64::from(bytes[2]) << 8;
        length |= u64::from(bytes[3]) << 16;
        length |= u64::from(bytes[4]) << 24;
        // Lower 2 bits of bytes[5] for length.
        length |= (u64::from(bytes[5]) & 0x03) << 32;

        let mut mask: u16 = 0;
        // Upper 6 bits from bytes[5] for the lower 6 bits of the mask.
        mask |= (u16::from(bytes[5]) >> 2) & 0x3F;
        // All 8 bits from bytes[6] for the upper 8 bits of the mask.
        mask |= u16::from(bytes[6]) << 6;

        Self {
            length: length & Self::LENGTH_MAX,
            mask: mask & Self::MASK_MAX,
        }
    }

    /// Converts the `BlobHeaderV1` to a 7-byte array.
    pub fn as_bytes(&self) -> [u8; QuiltVersionV1::BLOB_HEADER_SIZE] {
        let mut data = [0u8; QuiltVersionV1::BLOB_HEADER_SIZE];

        data[0] = QuiltVersionV1::QUILT_VERSION_BYTE;

        data[1] = (self.length & 0xFF) as u8;
        data[2] = ((self.length >> 8) & 0xFF) as u8;
        data[3] = ((self.length >> 16) & 0xFF) as u8;
        data[4] = ((self.length >> 24) & 0xFF) as u8;

        // data[5] contains the two highest bits of length (32,33) in its lowest 2 bits (0,1).
        // and the lowest 6 bits of mask (0-5) in its highest 6 bits (2-7).
        data[5] = ((self.length >> 32) & 0x03) as u8;
        data[5] |= ((self.mask & 0x3F) << 2) as u8;

        // data[6] contains the highest 8 bits of mask (6-13).
        data[6] = ((self.mask >> 6) & 0xFF) as u8;

        data
    }

    /// Returns true if the blob has attributes.
    pub fn has_attributes(&self) -> bool {
        self.mask & Self::METADATA_ENABLED != 0
    }

    /// Set the attributes flag to true.
    pub fn set_has_attributes(&mut self, has_attributes: bool) {
        if has_attributes {
            self.mask |= Self::METADATA_ENABLED;
        } else {
            self.mask &= !Self::METADATA_ENABLED;
        }
    }
}
/// A quilt is a collection of blobs encoded into a single blob.
///
/// For QuiltVersionV1:
/// The data is organized as a 2D matrix where:
/// - Each blob occupies a consecutive range of columns (secondary slivers).
/// - The first column's initial `QUILT_INDEX_SIZE_BYTES_LENGTH` bytes contain the unencoded
///   length of the [`QuiltIndexV1`]. It is guaranteed the column size is more than
///   `QUILT_INDEX_SIZE_BYTES_LENGTH`.
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

        if n_source_symbols == 0 {
            return Err(QuiltError::Other(
                "n_source_symbols cannot be zero".to_string(),
            ));
        }
        if quilt_blob.len() % n_source_symbols != 0 {
            return Err(QuiltError::InvalidFormatNotAligned(format!(
                "quilt_blob length {} is not a multiple of n_source_symbols {}",
                quilt_blob.len(),
                n_source_symbols
            )));
        }

        let symbol_size = quilt_blob.len() / n_source_symbols;
        let row_size = symbol_size * n_secondary_source_symbols;
        let quilt_index = Self::get_quilt_index(&quilt_blob, row_size, symbol_size)?;

        Ok(QuiltV1 {
            data: quilt_blob,
            row_size,
            quilt_index,
            symbol_size,
        })
    }

    fn get_blob_by_identifier(&self, identifier: &str) -> Result<QuiltBlobOwned, QuiltError> {
        self.quilt_index
            .get_quilt_patch_by_identifier(identifier)
            .and_then(|quilt_patch| self.get_blob_by_quilt_patch(quilt_patch))
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

// Implementation of QuiltColumnDataIterator for QuiltV1
impl QuiltColumnDataIterator for QuiltV1 {
    type RangeIter<'a>
        = SymbolIter<'a>
    where
        Self: 'a;

    fn iter_column_bytes(
        &self,
        start_col: usize,
        end_col: usize,
        start_byte: usize,
        limit_bytes: usize,
    ) -> Result<Self::RangeIter<'_>, QuiltError> {
        SymbolIter::new(
            &self.data,
            self.row_size,
            self.symbol_size,
            start_col,
            end_col,
            start_byte,
            limit_bytes,
        )
    }
}

impl QuiltV1 {
    /// Returns an iterator over the symbols for a given column range.
    fn iter_symbols(
        data: &[u8],
        row_size: usize,
        symbol_size: usize,
        start_col: usize,
        end_col: usize,
        begin_position: usize,
        limit: usize,
    ) -> Result<SymbolIter<'_>, QuiltError> {
        SymbolIter::new(
            data,
            row_size,
            symbol_size,
            start_col,
            end_col,
            begin_position,
            limit,
        )
    }

    fn get_quilt_index(
        data: &[u8],
        row_size: usize,
        symbol_size: usize,
    ) -> Result<QuiltIndexV1, QuiltError> {
        assert!(data.len() % row_size == 0);

        let size_iterator = Self::iter_symbols(
            data,
            row_size,
            symbol_size,
            0,
            1,
            QUILT_VERSION_BYTES_LENGTH,
            QUILT_INDEX_SIZE_BYTES_LENGTH,
        )?;
        let mut size_cache = Vec::with_capacity(QUILT_INDEX_SIZE_BYTES_LENGTH);
        for symbol_slice in size_iterator {
            size_cache.extend_from_slice(symbol_slice);
        }
        assert!(size_cache.len() == QUILT_INDEX_SIZE_BYTES_LENGTH);

        let index_size = usize::try_from(u32::from_le_bytes(
            size_cache
                .try_into()
                .map_err(|_| QuiltError::FailedToExtractQuiltIndexSize)?,
        ))
        .map_err(|_| QuiltError::FailedToExtractQuiltIndexSize)?;
        let total_size = index_size + QUILT_INDEX_SIZE_BYTES_LENGTH + QUILT_VERSION_BYTES_LENGTH;
        let columns_needed = total_size.div_ceil(data.len() / row_size * symbol_size);
        let data_iter = Self::iter_symbols(
            data,
            row_size,
            symbol_size,
            0,
            columns_needed,
            QUILT_VERSION_BYTES_LENGTH + QUILT_INDEX_SIZE_BYTES_LENGTH,
            index_size,
        )?;

        let mut collected_data = Vec::with_capacity(index_size);
        for symbol_slice in data_iter {
            collected_data.extend_from_slice(symbol_slice);
        }
        assert!(collected_data.len() == index_size);

        // Decode the QuiltIndexV1.
        let mut quilt_index: QuiltIndexV1 = bcs::from_bytes(&collected_data)
            .map_err(|e| QuiltError::QuiltIndexSerDerError(e.to_string()))?;

        quilt_index.populate_start_indices(
            u16::try_from(columns_needed).expect("columns_needed should fit in u16"),
        );

        Ok(quilt_index)
    }

    /// Gets a blob by its start and end indices.
    pub fn get_blob_by_range(
        &self,
        start_col: usize,
        end_col: usize,
    ) -> Result<QuiltBlobOwned, QuiltError> {
        if start_col >= end_col {
            return Err(QuiltError::Other(format!(
                "Invalid column range in quilt patch: start_col {} >= end_col {}",
                start_col, end_col
            )));
        }

        let blob: QuiltBlobOwned = QuiltVersionV1::decode_blob(self, start_col, end_col)?;

        Ok(blob)
    }

    /// Gets the blob represented by the given quilt patch.
    fn get_blob_by_quilt_patch(
        &self,
        quilt_patch: &QuiltPatchV1,
    ) -> Result<QuiltBlobOwned, QuiltError> {
        let start_col = usize::from(quilt_patch.start_index());
        let end_col = usize::from(quilt_patch.end_index());

        self.get_blob_by_range(start_col, end_col)
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

            if i == 5 && self.entries.len() > QUILT_INDEX_SIZE_BYTES_LENGTH {
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
                "\nQuiltPatch {{\n    end_index: {}\n    identifier: {:?}\n}}",
                patch.end_index(),
                patch.identifier()
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
        blobs: &'a [QuiltStoreBlob<'a>],
    ) -> QuiltEncoderV1<'a> {
        QuiltEncoderV1::new(encoding_config, blobs)
    }

    fn get_decoder(slivers: &'a [&'a SliverData<Secondary>]) -> QuiltDecoderV1<'a> {
        QuiltDecoderV1::new(slivers)
    }

    fn get_decoder_with_quilt_index(
        slivers: &'a [&'a SliverData<Secondary>],
        quilt_index: &QuiltIndex,
    ) -> QuiltDecoderV1<'a> {
        let QuiltIndex::V1(quilt_index) = quilt_index;
        QuiltDecoderV1::new_with_quilt_index(slivers, quilt_index.clone())
    }
}

struct MergeIterator<'a> {
    iterators: Vec<&'a [u8]>,
    current_slice: usize,
    current_index: usize,
    symbol_size: usize,
}

impl<'a> MergeIterator<'a> {
    fn new(iterators: Vec<&'a [u8]>, symbol_size: usize) -> Self {
        Self {
            iterators,
            current_slice: 0,
            current_index: 0,
            symbol_size,
        }
    }

    fn next_n_bytes(&mut self, limit: usize) -> Option<&'a [u8]> {
        while self.current_slice < self.iterators.len()
            && self.current_index >= self.iterators[self.current_slice].len()
        {
            self.current_slice += 1;
            self.current_index = 0;
        }
        if self.current_slice >= self.iterators.len() {
            return None;
        }

        let end_index = self.iterators[self.current_slice]
            .len()
            .min(self.current_index + limit);
        let next_bytes = &self.iterators[self.current_slice][self.current_index..end_index];
        self.current_index = end_index;
        Some(next_bytes)
    }
}

impl<'a> Iterator for MergeIterator<'a> {
    type Item = Vec<&'a [u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut total_size = 0;

        let mut result = Vec::new();
        while total_size < self.symbol_size {
            if let Some(next_bytes) = self.next_n_bytes(self.symbol_size - total_size) {
                total_size += next_bytes.len();
                result.push(next_bytes);
            } else {
                break;
            }
        }

        (total_size > 0).then_some(result)
    }
}

/// EncoderV1.
#[derive(Debug)]
pub struct QuiltEncoderV1<'a> {
    /// The blobs to encode.
    blobs: &'a [QuiltStoreBlob<'a>],
    /// The encoding configuration.
    config: EncodingConfigEnum<'a>,
    /// A tracing span associated with this quilt encoder.
    span: Span,
}

impl<'a> QuiltEncoderV1<'a> {
    /// Creates a new [`QuiltEncoderV1`] from a encoding config and a set of blobs.
    pub fn new(config: EncodingConfigEnum<'a>, blobs: &'a [QuiltStoreBlob<'a>]) -> Self {
        Self {
            blobs,
            config,
            span: tracing::span!(Level::ERROR, "QuiltEncoderV1"),
        }
    }

    /// Returns the extension bytes of the blob.
    pub fn get_header_and_extension_bytes(blob: &QuiltStoreBlob) -> Result<Vec<u8>, QuiltError> {
        let mut identifier_bytes = Vec::new();
        let mut extension_bytes = Vec::new();
        let mut header = BlobHeaderV1::default();

        let identifier_size =
            u16::try_from(bcs::serialized_size(&blob.identifier).map_err(|e| {
                QuiltError::Other(format!("Failed to serialize identifier: {}", e))
            })?)
            .map_err(|e| {
                QuiltError::Other(format!("Failed to convert identifier size to u16: {}", e))
            })?;
        identifier_bytes.extend_from_slice(&identifier_size.to_le_bytes());
        identifier_bytes
            .extend_from_slice(&bcs::to_bytes(&blob.identifier).map_err(|e| {
                QuiltError::Other(format!("Failed to serialize identifier: {}", e))
            })?);
        extension_bytes.push(identifier_bytes);

        if !blob.attributes.is_empty() {
            header.set_has_attributes(true);
            let mut serialized_attributes = bcs::to_bytes(&blob.attributes)
                .map_err(|e| QuiltError::Other(format!("Failed to serialize attributes: {}", e)))?;

            let attributes_size = u32::try_from(serialized_attributes.len()).map_err(|e| {
                QuiltError::Other(format!(
                    "Failed to convert attributes payload length to u32: {}",
                    e
                ))
            })?;

            let mut result_bytes = Vec::new();
            result_bytes.extend_from_slice(&attributes_size.to_le_bytes());
            result_bytes.append(&mut serialized_attributes);
            extension_bytes.push(result_bytes);
        }

        let total_size = extension_bytes.iter().map(|b| b.len()).sum::<usize>() + blob.data().len();
        header.length = total_size as u64;
        let header_bytes = header.as_bytes();
        debug_assert_eq!(header_bytes.len(), QuiltVersionV1::BLOB_HEADER_SIZE);

        let mut result_bytes = Vec::with_capacity(header_bytes.len() + total_size);
        result_bytes.extend_from_slice(&header_bytes);
        for mut inner_extension_vec in extension_bytes {
            result_bytes.append(&mut inner_extension_vec);
        }

        Ok(result_bytes)
    }

    /// Adds a blob to the quilt as consecutive columns.
    fn add_blob_to_quilt(
        data: &mut [u8],
        blob: &QuiltStoreBlob,
        include_blob_size: bool,
        current_col: usize,
        column_size: usize,
        row_size: usize,
        symbol_size: usize,
    ) -> Result<usize, QuiltError> {
        assert!(column_size % symbol_size == 0);
        let n_rows = column_size / symbol_size;
        let mut row = 0;
        let mut col = current_col;
        let mut max_col = current_col;

        let mut prefix_bytes = Vec::new();
        if include_blob_size {
            prefix_bytes = Self::get_header_and_extension_bytes(blob)?;
        };

        let merge_iterator = MergeIterator::new(vec![&prefix_bytes, blob.data()], symbol_size);
        for data_slice in merge_iterator {
            let mut dest_idx = row * row_size + col * symbol_size;
            for chunk in data_slice {
                data[dest_idx..dest_idx + chunk.len()].copy_from_slice(chunk);
                dest_idx += chunk.len();
            }
            max_col = max_col.max(col);
            row = (row + 1) % n_rows;
            if row == 0 {
                col += 1;
            }
        }

        Ok(max_col - current_col + 1)
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
            .map(|blob| QuiltPatchV1::new(blob.identifier.clone()))
            .collect::<Result<Vec<QuiltPatchV1>, QuiltError>>()?;

        let mut quilt_index = QuiltIndexV1 { quilt_patches };

        // Get the serialized quilt index size.
        let serialized_index_size =
            u32::try_from(bcs::serialized_size(&quilt_index).map_err(|e| {
                QuiltError::QuiltIndexSerDerError(format!(
                    "failed to serialize quilt index: {:?}",
                    e
                ))
            })?)
            .expect("serialized_index_size should fit in u32");

        // Calculate total size including the size prefix and the quilt type.
        let index_total_size = QUILT_INDEX_PREFIX_SIZE
            + usize::try_from(serialized_index_size)
                .expect("serialized_index_size should fit in usize");

        // Collect blob sizes for symbol size computation.
        let all_sizes: Vec<usize> = core::iter::once(Ok(index_total_size))
            .chain(
                blob_pairs
                    .iter()
                    .map(|blob| QuiltVersionV1::compute_encoded_blob_size(blob)),
            )
            .collect::<Result<Vec<usize>, QuiltError>>()?;

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

        // First pass: Fill data with actual blobs and populate quilt patches.
        for (i, quilt_store_blob) in blob_pairs.iter().enumerate() {
            let cols_needed = Self::add_blob_to_quilt(
                &mut data,
                quilt_store_blob,
                true,
                current_col,
                column_size,
                row_size,
                symbol_size,
            )?;

            quilt_index.quilt_patches[i].set_range(
                u16::try_from(current_col).expect("current_col should fit in u16"),
                u16::try_from(current_col + cols_needed)
                    .expect("current_col + cols_needed should fit in u16"),
            );
            current_col += cols_needed;
        }

        let mut meta_blob_data = Vec::with_capacity(index_total_size);
        meta_blob_data.push(QuiltVersionV1::quilt_version_byte());
        meta_blob_data.extend_from_slice(&serialized_index_size.to_le_bytes());
        meta_blob_data
            .extend_from_slice(&bcs::to_bytes(&quilt_index).expect("Serialization should succeed"));
        assert_eq!(meta_blob_data.len(), index_total_size);

        let meta_blob = QuiltStoreBlob::new(&meta_blob_data, "quilt_index");
        // Add the index to the quilt.
        let index_cols_used = Self::add_blob_to_quilt(
            &mut data,
            &meta_blob,
            false,
            0,
            column_size,
            row_size,
            symbol_size,
        )?;
        debug_assert_eq!(index_cols_used, index_cols_needed);
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
    fn get_or_decode_quilt_index(&mut self) -> Result<QuiltIndex, QuiltError> {
        if self.quilt_index.is_some() {
            return Ok(self
                .quilt_index
                .as_ref()
                .expect("quilt index should exist")
                .into());
        }
        self.check_missing_slivers(0, 1)?;
        let first_sliver = self
            .slivers
            .get(&SliverIndex(0))
            .expect("first sliver should exist");
        utils::check_quilt_version::<QuiltVersionV1>(first_sliver.symbols.data())?;

        let first_sliver_iter = self.iter_column_bytes(
            0,
            1,
            QUILT_VERSION_BYTES_LENGTH,
            QUILT_INDEX_SIZE_BYTES_LENGTH,
        )?;

        let mut index_size_bytes = Vec::new();
        for next_bytes in first_sliver_iter {
            index_size_bytes.extend_from_slice(next_bytes);
        }
        assert_eq!(index_size_bytes.len(), QUILT_INDEX_SIZE_BYTES_LENGTH);
        let index_size = usize::try_from(u32::from_le_bytes(
            index_size_bytes
                .try_into()
                .expect("index size prefix should be 8 bytes"),
        ))
        .expect("index size should fit in usize");

        let total_size = index_size + QUILT_INDEX_PREFIX_SIZE;

        // Calculate how many slivers we need based on the data size.
        let num_slivers_needed = total_size.div_ceil(first_sliver.symbols.data().len());
        let mut combined_data = Vec::with_capacity(index_size);
        self.check_missing_slivers(1, num_slivers_needed)?;

        let index_data_iter =
            self.iter_column_bytes(0, num_slivers_needed, QUILT_INDEX_PREFIX_SIZE, index_size)?;
        for next_bytes in index_data_iter {
            combined_data.extend_from_slice(next_bytes);
        }
        assert_eq!(combined_data.len(), index_size);

        // Decode the QuiltIndexV1 from the collected data.
        let mut index: QuiltIndexV1 = bcs::from_bytes(&combined_data)
            .map_err(|e| QuiltError::QuiltIndexSerDerError(e.to_string()))?;

        // After successful deserialization, sort the patches by end_index.
        #[cfg(debug_assertions)]
        for i in 1..index.quilt_patches.len() {
            assert!(index.quilt_patches[i].end_index() >= index.quilt_patches[i - 1].end_index());
        }
        index.populate_start_indices(
            u16::try_from(num_slivers_needed).expect("num_slivers_needed should fit in u16"),
        );

        self.quilt_index = Some(index);

        Ok(self
            .quilt_index
            .as_ref()
            .expect("quilt index should be decoded")
            .into())
    }

    fn get_blob_by_identifier(&self, identifier: &str) -> Result<QuiltBlobOwned, QuiltError> {
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

    fn sliver_type(&self) -> SliverType {
        SliverType::Secondary
    }
}

// Implementation of QuiltColumnDataIterator for QuiltDecoderV1
impl QuiltColumnDataIterator for QuiltDecoderV1<'_> {
    type RangeIter<'a>
        = SliverBytesIterator<'a>
    where
        Self: 'a;

    fn iter_column_bytes(
        &self,
        start_col: usize,
        end_col: usize,
        start_byte: usize,
        limit_bytes: usize,
    ) -> Result<Self::RangeIter<'_>, QuiltError> {
        self.check_missing_slivers(start_col, end_col)?;
        let mut slivers: Vec<&SliverData<Secondary>> = Vec::new();
        for col in start_col..end_col {
            let sliver = self
                .slivers
                .get(&SliverIndex::new(col as u16))
                .expect("Sliver should exist");
            slivers.push(sliver);
        }

        SliverBytesIterator::new(slivers, start_byte, limit_bytes)
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
    fn get_blob_by_quilt_patch(
        &self,
        quilt_patch: &QuiltPatchV1,
    ) -> Result<QuiltBlobOwned, QuiltError> {
        let start_idx = usize::from(quilt_patch.start_index());
        let end_idx = usize::from(quilt_patch.end_index());
        self.get_blob_by_range(start_idx, end_idx)
    }

    /// Get the blob represented by the range.
    pub fn get_blob_by_range(
        &self,
        start_idx: usize,
        end_idx: usize,
    ) -> Result<QuiltBlobOwned, QuiltError> {
        self.check_missing_slivers(start_idx, end_idx)?;

        let blob: QuiltBlobOwned = QuiltVersionV1::decode_blob(self, start_idx, end_idx)?;

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
    /// A binary search is used to find the minimum symbol size:
    /// 1. Compute the upper and lower bounds for the symbol size.
    /// 2. Check if the all the blobs can be fit into the quilt with the current symbol size.
    /// 3. Adjust the bounds based on the result and repeat until the symbol size is found.
    ///
    /// # Arguments
    /// * `blobs_sizes` - Slice of blob lengths, including the index size as the first element.
    ///   Note that the len of the blob_size should be between 1 and n_columns.
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
            return Err(QuiltError::TooManyBlobs(
                blobs_sizes.len() - 1,
                n_columns - 1,
            ));
        }

        if blobs_sizes.is_empty() {
            return Err(QuiltError::EmptyInput("blobs".to_string()));
        }

        let mut min_val = cmp::max(
            blobs_sizes
                .iter()
                .sum::<usize>()
                .div_ceil(n_columns * n_rows),
            blobs_sizes
                .first()
                .expect("blobs_sizes is not empty")
                .div_ceil(n_rows * max_num_columns_for_quilt_index),
        );
        min_val = cmp::max(min_val, QUILT_INDEX_PREFIX_SIZE.div_ceil(n_rows));
        let mut max_val = blobs_sizes
            .iter()
            .max()
            .copied()
            .expect("blobs_sizes is not empty")
            .div_ceil(n_columns / blobs_sizes.len() * n_rows);

        while min_val < max_val {
            let mid = (min_val + max_val) / 2;
            if can_blobs_fit_into_matrix(blobs_sizes, n_columns, mid * n_rows) {
                max_val = mid;
            } else {
                min_val = mid + 1;
            }
        }

        let symbol_size = min_val.next_multiple_of(required_alignment);
        debug_assert!(can_blobs_fit_into_matrix(
            blobs_sizes,
            n_columns,
            symbol_size * n_rows
        ));
        if symbol_size > MAX_SYMBOL_SIZE as usize {
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
        let required_columns = blobs_sizes
            .iter()
            .map(|blob_size| blob_size.div_ceil(column_size))
            .sum::<usize>();
        n_columns >= required_columns
    }

    pub fn get_quilt_version_byte(data: &[u8]) -> Result<u8, QuiltError> {
        data.first()
            .copied()
            .ok_or(QuiltError::EmptyInput("data".to_string()))
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

    use rand::Rng;
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
            case_1: (&[2, 1, 2, 1], 3, 3, 1, Err(QuiltError::TooManyBlobs(3, 2))),
            case_2: (&[1000, 1, 1], 4, 7, 2, Ok(144)),
            case_3: (
                &[],
                3,
                1,
                1,
                Err(QuiltError::EmptyInput("blobs".to_string())),
            ),
            case_4: (&[1], 3, 2, 1, Ok(3)),
            case_5: (&[115, 80, 4], 17, 9, 1, Ok(13)),
            case_6: (&[20, 20, 20], 3, 5, 1, Ok(4)),
            case_7: (&[5, 5, 5], 5, 1, 2, Ok(6)),
            case_8: (&[25, 35, 45], 200, 1, 2, Ok(26)),
            case_9: (&[10, 0, 0, 0], 17, 9, 1, Ok(2)),
            case_10: (&[10, 0, 0, 0], 17, 9, 2, Ok(2)),
            case_11: (
                &[
                    416, 253, 258, 384, 492, 303, 276, 464, 143, 251, 388, 263, 515, 433, 505,
                    385, 346, 69, 48, 495, 329, 450, 494, 104, 539, 245, 109, 317, 60
                ],
                34,
                16,
                1,
                Ok(31)
            ),
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
                    QuiltStoreBlob {
                        blob: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..],
                        identifier: "test-blob-0".to_string(),
                        attributes: HashMap::from([
                            ("key1".to_string(), "value1".to_string()),
                            ("key2".to_string(), "value2".to_string()),
                        ]),
                    },
                    QuiltStoreBlob {
                        blob: &[5, 68, 3, 2, 5][..],
                        identifier: "test-blob-1".to_string(),
                        attributes: HashMap::new(),
                    },
                    QuiltStoreBlob {
                        blob: &[5, 68, 3, 2, 5, 6, 78, 8][..],
                        identifier: "test-blob-2".to_string(),
                        attributes: HashMap::new(),
                    },
                ],
                7
            ),
            case_0_random_order: (
                &[
                    QuiltStoreBlob {
                        blob: &[5, 68, 3, 2, 5, 6, 78, 8][..],
                        identifier: "test-blob-0".to_string(),
                        attributes: HashMap::new(),
                    },
                    QuiltStoreBlob {
                        blob: &[5, 68, 3, 2, 5][..],
                        identifier: "test-blob-1".to_string(),
                        attributes: HashMap::new(),
                    },
                    QuiltStoreBlob {
                        blob: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..],
                        identifier: "test-blob-2".to_string(),
                        attributes: HashMap::new(),
                    },
                ],
                7
            ),
            case_1: (
                &[
                    QuiltStoreBlob {
                        blob: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..],
                        identifier: "test-blob-0".to_string(),
                        attributes: HashMap::new(),
                    },
                    QuiltStoreBlob {
                        blob: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..],
                        identifier: "test-blob-1".to_string(),
                        attributes: HashMap::new(),
                    },
                    QuiltStoreBlob {
                        blob: &[5, 68, 3, 2, 5, 6, 78, 8][..],
                        identifier: "test-blob-2".to_string(),
                        attributes: HashMap::new(),
                    },
                ],
                7
            ),
            case_1_random_order: (
                &[
                    QuiltStoreBlob {
                        blob: &[5, 68, 3, 2, 5][..],
                        identifier: "test-blob-0".to_string(),
                        attributes: HashMap::new(),
                    },
                    QuiltStoreBlob {
                        blob: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..],
                        identifier: "".to_string(),
                        attributes: HashMap::new(),
                    },
                    QuiltStoreBlob {
                        blob: &[5, 68, 3, 2, 5, 6, 78, 8][..],
                        identifier: "test-blob-2".to_string(),
                        attributes: HashMap::new(),
                    },
                ],
                7
            ),
            case_2: (
                &[
                    QuiltStoreBlob {
                        blob: &[1, 3][..],
                        identifier: "test-blob-0".to_string(),
                        attributes: HashMap::new(),
                    },
                    QuiltStoreBlob {
                        blob: &[255u8; 1024][..],
                        identifier: "test-blob-1".to_string(),
                        attributes: HashMap::new(),
                    },
                    QuiltStoreBlob {
                        blob: &[1, 2, 3][..],
                        identifier: "test-blob-2".to_string(),
                        attributes: HashMap::new(),
                    },
                ],
                12
            ),
            case_3: (
                &[
                    QuiltStoreBlob {
                        blob: &[9, 8, 7, 6, 5, 4, 3, 2, 1][..],
                        identifier: "test-blob-0".to_string(),
                        attributes: HashMap::new(),
                    },
                ],
                7
            ),
        ]
    }
    fn test_quilt_construct_quilt(quilt_store_blobs: &[QuiltStoreBlob<'_>], n_shards: u16) {
        let raptorq_config = RaptorQEncodingConfig::new(NonZeroU16::try_from(n_shards).unwrap());
        let reed_solomon_config =
            ReedSolomonEncodingConfig::new(NonZeroU16::try_from(n_shards).unwrap());

        construct_quilt(
            quilt_store_blobs,
            EncodingConfigEnum::RaptorQ(&raptorq_config),
        );
        construct_quilt(
            quilt_store_blobs,
            EncodingConfigEnum::ReedSolomon(&reed_solomon_config),
        );
    }

    fn construct_quilt(quilt_store_blobs: &[QuiltStoreBlob<'_>], config: EncodingConfigEnum) {
        let _guard = tracing_subscriber::fmt().try_init();

        let encoder = QuiltConfigV1::get_encoder(config.clone(), quilt_store_blobs);

        let quilt = encoder.construct_quilt().expect("Should construct quilt");

        // Verify each blob and its description.
        for quilt_store_blob in quilt_store_blobs {
            // Verify blob data matches.
            let extracted_blob = quilt
                .get_blob_by_identifier(quilt_store_blob.identifier.as_str())
                .expect("Patch should exist for this blob identifier");
            assert_eq!(
                extracted_blob, *quilt_store_blob,
                "Mismatch in encoded blob"
            );

            let quilt_patch = quilt
                .quilt_index()
                .get_quilt_patch_by_identifier(quilt_store_blob.identifier.as_str())
                .expect("Patch should exist for this blob ID");
            assert_eq!(
                quilt_patch.identifier(),
                quilt_store_blob.identifier,
                "Mismatch in blob description"
            );

            let blob_by_identifier = quilt
                .get_blob_by_identifier(quilt_store_blob.identifier.as_str())
                .expect("Should be able to get blob by identifier");
            assert_eq!(blob_by_identifier, *quilt_store_blob);
        }

        assert_eq!(quilt.quilt_index().len(), quilt_store_blobs.len());
    }

    #[test]
    fn test_quilt_with_random_blobs() {
        for _ in 0..1000 {
            let mut rng = rand::thread_rng();
            let num_blobs = rng.gen_range(1..50) as usize;
            let n_shards = rng.gen_range(((num_blobs + 5) * 3).div_ceil(2)..100) as u16;
            let min_blob_size = rng.gen_range(1..100);
            let max_blob_size = rng.gen_range(min_blob_size..1000);
            std::println!(
                "test_quilt_with_random_blobs: {}, {}, {}, {}",
                num_blobs,
                min_blob_size,
                max_blob_size,
                n_shards
            );
            // test_quilt_encoder_and_decoder(num_blobs, min_blob_size, max_blob_size, n_shards);
            test_quilt_encoder_and_decoder(28, 25, 568, 55);
        }
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
        let quilt_store_blobs = generate_random_blobs(num_blobs, max_blob_size, min_blob_size);

        let raptorq_config = RaptorQEncodingConfig::new(NonZeroU16::try_from(n_shards).unwrap());
        let reed_solomon_config =
            ReedSolomonEncodingConfig::new(NonZeroU16::try_from(n_shards).unwrap());

        encode_decode_quilt(
            &quilt_store_blobs,
            EncodingConfigEnum::RaptorQ(&raptorq_config),
        );
        encode_decode_quilt(
            &quilt_store_blobs,
            EncodingConfigEnum::ReedSolomon(&reed_solomon_config),
        );
    }

    fn encode_decode_quilt(quilt_store_blobs: &[QuiltStoreBlob<'_>], config: EncodingConfigEnum) {
        let _guard = tracing_subscriber::fmt().try_init();

        let encoder = QuiltConfigV1::get_encoder(config.clone(), quilt_store_blobs);

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
            Ok(quilt_metadata_v1.index.into())
        );

        let identifier = quilt_store_blobs
            .first()
            .expect("Test requires at least one blob")
            .identifier
            .as_str();
        let QuiltIndex::V1(quilt_index_v1) = quilt_decoder
            .get_or_decode_quilt_index()
            .expect("quilt index should exist");
        let patch = quilt_index_v1
            .get_quilt_patch_by_identifier(identifier)
            .expect("quilt patch should exist");
        assert_eq!(patch.identifier(), identifier);

        let missing_indices: Vec<SliverIndex> = (patch.start_index()..patch.end_index())
            .map(SliverIndex)
            .collect();
        assert_eq!(
            quilt_decoder.get_blob_by_identifier(identifier),
            Err(QuiltError::MissingSlivers(missing_indices))
        );

        // Now, add all slivers to the decoder, all the blobs should be reconstructed.
        quilt_decoder.add_slivers(&slivers);

        for quilt_store_blob in quilt_store_blobs {
            tracing::debug!("decoding blob {}", quilt_store_blob.identifier);
            let blob = quilt_decoder
                .get_blob_by_identifier(quilt_store_blob.identifier.as_str())
                .expect("Should get blob by identifier");
            assert_eq!(blob, *quilt_store_blob);
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

    param_test! {
        test_quilt_blob_header: [
            case_0: (10233, 5),
            case_1: (10, 3),
            case_2: (125, 10),
            case_3: (1, 1),
            case_4: (0, 0), // Zero all
            case_5: (BlobHeaderV1::LENGTH_MAX, 0), // Max length, zero mask
            case_6: (0, BlobHeaderV1::MASK_MAX), // Zero length, max mask
            case_7: (BlobHeaderV1::LENGTH_MAX, BlobHeaderV1::MASK_MAX), // Max all
            case_8: (1, BlobHeaderV1::MASK_MAX), // Min length, max mask
            case_9: (BlobHeaderV1::LENGTH_MAX, 1), // Max length, min mask
        ]
    }
    fn test_quilt_blob_header(length: u64, mask: u16) {
        {
            let mut header = BlobHeaderV1::new(100, 0).expect("Should create header");
            assert_eq!(header.length, 100);
            assert_eq!(header.mask, 0);
            assert!(!header.has_attributes());
            header.set_has_attributes(true);
            assert!(header.has_attributes());
            let bytes = header.as_bytes();
            let reconstructed_header = BlobHeaderV1::from_bytes(bytes);
            assert!(reconstructed_header.has_attributes());
        }

        let header = BlobHeaderV1::new(length, mask).expect("Should create header");

        assert_eq!(
            header.length, length,
            "Getter for length failed after new_with_values"
        );
        assert_eq!(
            header.mask, mask,
            "Getter for mask failed after new_with_values"
        );

        let bytes = header.as_bytes();
        assert_eq!(
            bytes.len(),
            QuiltVersionV1::BLOB_HEADER_SIZE,
            "Byte array size is incorrect"
        );

        // BlobHeaderV1::from_bytes takes [u8; N], not a reference, and does not return Result.
        let reconstructed_header = BlobHeaderV1::from_bytes(bytes);
        assert_eq!(
            reconstructed_header, header,
            "Reconstructed header does not match original"
        );
    }

    param_test! {
        test_quilt_patch_id: [
            case_0: (0, 0),
            case_1: (1, 1),
            case_2: (10, 20),
        ]
    }
    fn test_quilt_patch_id(start_index: u16, end_index: u16) {
        let patch_id = QuiltPatchIdV1::new(start_index, end_index);
        let bytes = patch_id.to_bytes();
        let reconstructed_patch_id =
            QuiltPatchIdV1::from_bytes(&bytes).expect("Should reconstruct patch id");
        assert_eq!(patch_id, reconstructed_patch_id);
        assert_eq!(reconstructed_patch_id.start_index, start_index);
        assert_eq!(reconstructed_patch_id.end_index, end_index);
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
    /// A vector of QuiltStoreBlob objects with random content.
    fn generate_random_blobs(
        num_blobs: usize,
        max_blob_size: usize,
        min_blob_size: usize,
    ) -> Vec<QuiltStoreBlob<'static>> {
        use rand::{Rng, SeedableRng, rngs::StdRng};

        // Create a deterministic RNG with a fixed seed for reproducibility.
        let mut rng = StdRng::seed_from_u64(42);

        // Store both blobs and their QuiltStoreBlob wrappers.
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

            // Create and store the QuiltStoreBlob.
            let mut quilt_store_blob = QuiltStoreBlob::new(static_data, format!("test-blob-{}", i));
            let num_attributes = rng.gen_range(0..15);
            for _ in 0..num_attributes {
                // Generate random data and convert to hex string
                let value_bytes_length = rng.gen_range(0..100);
                let random_bytes = random_data(value_bytes_length);
                let random_value = hex::encode(random_bytes);
                let random_bytes = random_data(value_bytes_length);
                let random_key = hex::encode(random_bytes);
                quilt_store_blob.attributes.insert(random_key, random_value);
            }
            result.push(quilt_store_blob);
        }

        result
    }
}
