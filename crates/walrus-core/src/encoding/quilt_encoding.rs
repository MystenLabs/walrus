// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// *******************************************************************************
// DISCLAIMER: The quilt format is still under active development.
// Please use with caution as it is subject to change without prior notice.
// *******************************************************************************

//! Quilt encoding.

// TODO: remove this once follow up PRs are merged.
#![allow(dead_code)]
// TODO(WAL-869): Remove this attribute and fix corresponding warnings.
#![allow(clippy::cast_possible_truncation)]

use alloc::{
    borrow::Cow,
    collections::BTreeMap,
    format,
    string::{String, ToString},
    vec,
    vec::Vec,
};
use core::{cmp, fmt};
use std::collections::{HashMap, HashSet};

use hex;
use serde::{Deserialize, Serialize};
use tracing::{Level, Span};

use super::{EncodingConfigEnum, Primary, Secondary, SliverData, SliverPair};
use crate::{
    EncodingType,
    SliverIndex,
    encoding::{
        EncodingAxis,
        QuiltError,
        blob_encoding::BlobEncoder,
        config::EncodingConfigTrait as _,
    },
    metadata::{
        QuiltIndex,
        QuiltIndexV1,
        QuiltMetadata,
        QuiltMetadataV1,
        QuiltPatchInternalIdV1,
        QuiltPatchV1,
    },
};

/// The number of bytes to store the size of the quilt index.
const QUILT_INDEX_SIZE_BYTES_LENGTH: usize = 4;

/// The number of bytes used to store the version of the quilt.
const QUILT_VERSION_BYTES_LENGTH: usize = 1;

/// The number of bytes used to store the identifier of the blob.
const BLOB_IDENTIFIER_SIZE_BYTES_LENGTH: usize = 2;

/// The number of bytes used to store the size of the extension.
const TAGS_SIZE_BYTES_LENGTH: usize = 2;

/// The maximum number of bytes for the identifier of the blob.
const MAX_BLOB_IDENTIFIER_BYTES_LENGTH: usize = (1 << (8 * BLOB_IDENTIFIER_SIZE_BYTES_LENGTH)) - 1;

/// The number of bytes stored before the quilt index data.
const QUILT_INDEX_PREFIX_SIZE: usize = QUILT_INDEX_SIZE_BYTES_LENGTH + QUILT_VERSION_BYTES_LENGTH;

/// The maximum number of slivers a quilt index can be stored in.
const MAX_NUM_SLIVERS_FOR_QUILT_INDEX: usize = 10;

/// Gets the quilt version enum from the data.
pub fn get_quilt_version_enum(data: &[u8]) -> Result<QuiltVersionEnum, QuiltError> {
    QuiltVersionEnum::try_from(utils::get_quilt_version_byte(data)?)
}

/// Returns if an identifier is valid.
///
/// Validates that:
/// - The identifier is not empty
/// - The identifier is not too long (max length enforced)
/// - The identifier has no trailing whitespace
///
/// # Examples
///
/// Valid identifiers:
/// ```
/// # use walrus_core::encoding::quilt_encoding::validate_quilt_identifier;
/// assert!(validate_quilt_identifier("test").is_ok());
/// assert!(validate_quilt_identifier("test123").is_ok());
/// assert!(validate_quilt_identifier("123test").is_ok());
/// assert!(validate_quilt_identifier("a").is_ok());
/// assert!(validate_quilt_identifier("A123").is_ok());
/// assert!(validate_quilt_identifier("_test").is_ok());
/// assert!(validate_quilt_identifier("-test").is_ok());
/// assert!(validate_quilt_identifier(".test").is_ok());
/// assert!(validate_quilt_identifier("test blob").is_ok());
/// assert!(validate_quilt_identifier("test-blob").is_ok());
/// assert!(validate_quilt_identifier("test_blob").is_ok());
/// assert!(validate_quilt_identifier("test.blob").is_ok());
/// ```
///
/// Invalid identifier - empty:
/// ```
/// # use walrus_core::encoding::quilt_encoding::validate_quilt_identifier;
/// assert!(validate_quilt_identifier("").is_err());
/// ```
///
/// Invalid identifiers - has trailing whitespace:
/// ```
/// # use walrus_core::encoding::quilt_encoding::validate_quilt_identifier;
/// assert!(validate_quilt_identifier("test ").is_err());
/// assert!(validate_quilt_identifier("test\t").is_err());
/// assert!(validate_quilt_identifier("test\n").is_err());
/// assert!(validate_quilt_identifier("test\r").is_err());
/// assert!(validate_quilt_identifier("test   ").is_err());
/// ```
///
/// Invalid identifiers - contains control characters:
/// ```
/// # use walrus_core::encoding::quilt_encoding::validate_quilt_identifier;
/// assert!(validate_quilt_identifier("te\x08st").is_err());  // Backspace in middle
/// assert!(validate_quilt_identifier("\x1Btest").is_err());  // ESC at start
/// assert!(validate_quilt_identifier("test\x00").is_err());  // Null at end
/// assert!(validate_quilt_identifier("test\x01").is_err());  // SOH at end
/// assert!(validate_quilt_identifier("test\x1F").is_err());  // US (Unit Separator) at end
/// assert!(validate_quilt_identifier("test\x7F").is_err());  // DEL at end
/// assert!(validate_quilt_identifier("test\u{0080}").is_err());  // PAD (C1 control) at end
/// assert!(validate_quilt_identifier("test\u{0081}").is_err());  // HOP (C1 control) at end
/// assert!(validate_quilt_identifier("test\u{009F}").is_err());  // APC (C1 control) at end
/// ```
pub fn validate_quilt_identifier(identifier: &str) -> Result<(), QuiltError> {
    if identifier.len() > MAX_BLOB_IDENTIFIER_BYTES_LENGTH {
        return Err(QuiltError::InvalidIdentifier(format!(
            "identifier too long: {}",
            identifier.len()
        )));
    }
    if identifier.is_empty() {
        return Err(QuiltError::InvalidIdentifier(
            "identifier is empty".to_string(),
        ));
    }
    if identifier.trim_end() != identifier {
        return Err(QuiltError::InvalidIdentifier(format!(
            "identifier contains trailing whitespace: {identifier}",
        )));
    }
    if identifier.chars().any(|c| c.is_control()) {
        return Err(QuiltError::InvalidIdentifier(format!(
            "identifier contains control characters: {identifier}",
        )));
    }
    Ok(())
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
    type Quilt: QuiltApi<Self> + Send + Sync;
    /// The type of the quilt index.
    type QuiltIndex: QuiltIndexApi<Self>;
    /// The type of the quilt patch.
    type QuiltPatch: Clone + QuiltPatchApi<Self>;
    /// The type of the quilt patch internal id.
    type QuiltPatchInternalId: QuiltPatchInternalIdApi;
    /// The type of the quilt metadata.
    type QuiltMetadata;
    /// The type of the slivers, primary or secondary.
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

    /// Gets blobs by their identifiers from the quilt.
    ///
    /// If the quilt contains duplicate identifiers, the first matching patch is returned.
    /// TODO(WAL-862): Deduplicate the `get_blob*` functions.
    fn get_blobs_by_identifiers(
        &self,
        identifiers: &[&str],
    ) -> Result<Vec<QuiltStoreBlob<'static>>, QuiltError>;

    /// Gets a blob by its patch internal id.
    fn get_blob_by_patch_internal_id(
        &self,
        patch_internal_id: &[u8],
    ) -> Result<QuiltStoreBlob<'static>, QuiltError>;

    /// Gets blobs matching the given tag from the quilt.
    fn get_blobs_by_tag(
        &self,
        target_tag: &str,
        target_value: &str,
    ) -> Result<Vec<QuiltStoreBlob<'static>>, QuiltError>;

    /// Returns all the blobs in the quilt.
    fn get_all_blobs(&self) -> Result<Vec<QuiltStoreBlob<'static>>, QuiltError>;

    /// Returns the quilt index.
    fn quilt_index(&self) -> Result<&V::QuiltIndex, QuiltError>;

    /// Returns the data of the quilt.
    fn data(&self) -> &[u8];

    /// Returns the symbol size of the quilt.
    fn symbol_size(&self) -> usize;
}

/// API for QuiltIndex.
pub trait QuiltIndexApi<V: QuiltVersion>: Clone + Into<QuiltIndex> {
    /// Returns the quilt patches matching the given identifiers.
    ///
    /// If the quilt contains duplicate identifiers, the first matching patch is returned.
    fn get_quilt_patches_by_identifiers(
        &self,
        identifiers: &[&str],
    ) -> Result<Vec<&V::QuiltPatch>, QuiltError> {
        let mut identifiers: HashSet<&str> = identifiers.iter().copied().collect();
        let patches = self
            .patches()
            .iter()
            .filter(|patch| identifiers.remove(&patch.identifier()))
            .collect::<Vec<_>>();

        if !identifiers.is_empty() {
            return Err(QuiltError::BlobsNotFoundInQuilt(
                identifiers.into_iter().map(|id| id.to_string()).collect(),
            ));
        }

        Ok(patches)
    }

    /// Returns the quilt patches matching the given tag.
    fn get_quilt_patches_by_tag(
        &self,
        target_tag: &str,
        target_value: &str,
    ) -> Vec<&V::QuiltPatch> {
        self.patches()
            .iter()
            .filter(|patch| patch.has_matched_tag(target_tag, target_value))
            .collect()
    }

    /// Returns the sliver indices of the quilt patches matching the given tag.
    fn get_sliver_indices_for_tag(&self, target_tag: &str, target_value: &str) -> Vec<SliverIndex> {
        self.patches()
            .iter()
            .filter(|patch| patch.has_matched_tag(target_tag, target_value))
            .flat_map(|patch| patch.sliver_indices())
            .collect()
    }

    /// Returns the sliver indices of the quilt patches stored in.
    fn get_sliver_indices_for_identifiers(
        &self,
        identifiers: &[&str],
    ) -> Result<Vec<SliverIndex>, QuiltError> {
        let mut identifiers: HashSet<&str> = identifiers.iter().copied().collect();
        let patches = self
            .patches()
            .iter()
            .filter(|patch| identifiers.remove(&patch.identifier()))
            .collect::<Vec<_>>();

        if !identifiers.is_empty() {
            return Err(QuiltError::BlobsNotFoundInQuilt(
                identifiers.into_iter().map(|id| id.to_string()).collect(),
            ));
        }

        Ok(patches
            .iter()
            .flat_map(|patch| patch.sliver_indices())
            .collect())
    }

    /// Returns the quilt patches.
    fn patches(&self) -> &[V::QuiltPatch];

    /// Returns the identifiers of the quilt patches.
    fn identifiers(&self) -> impl Iterator<Item = &str>;

    /// Returns the number of quilt patches.
    fn len(&self) -> usize;

    /// Returns true if the quilt index has no patches.
    fn is_empty(&self) -> bool;
}

/// API for QuiltPatch.
pub trait QuiltPatchApi<V: QuiltVersion>: Clone {
    /// Returns the quilt patch internal id.
    fn quilt_patch_internal_id(&self) -> V::QuiltPatchInternalId;

    /// Returns the identifier of the quilt patch.
    fn identifier(&self) -> &str;

    /// Returns true if the quilt patch has the given tag.
    fn has_matched_tag(&self, target_tag: &str, target_value: &str) -> bool;

    /// Returns the sliver indices that the patch is stored in.
    fn sliver_indices(&self) -> Vec<SliverIndex>;
}
/// API for QuiltPatchInternalId.
///
/// A quilt patch internal id is a unique identifier for a quilt patch.
/// It can be used to identify the quilt patch in a quilt, and when combined with the quilt
/// id, it can be used to identify the quilt patch in Walrus.
pub trait QuiltPatchInternalIdApi: Clone {
    /// The serialized bytes of the quilt patch internal id.
    fn to_bytes(&self) -> Vec<u8>;

    /// Deserializes the quilt patch internal id from bytes.
    fn from_bytes(bytes: &[u8]) -> Result<Self, QuiltError>;

    /// Returns the sliver indices that the patch is stored in.
    fn sliver_indices(&self) -> Vec<SliverIndex>;
}

/// The configuration of the quilt.
pub trait QuiltConfigApi<'a, V: QuiltVersion> {
    /// Returns a new encoder for the given configuration and blobs.
    fn get_encoder(
        encoding_config: EncodingConfigEnum<'a>,
        blobs: &'a [QuiltStoreBlob<'a>],
    ) -> V::QuiltEncoder<'a>;

    /// Returns a new decoder.
    fn get_decoder(
        slivers: impl IntoIterator<Item = &'a SliverData<V::SliverAxis>>,
    ) -> V::QuiltDecoder<'a>
    where
        V::SliverAxis: 'a;

    /// Returns a new decoder for the given slivers and quilt index.
    fn get_decoder_with_quilt_index(
        slivers: impl IntoIterator<Item = &'a SliverData<V::SliverAxis>>,
        quilt_index: &'a QuiltIndex,
    ) -> V::QuiltDecoder<'a>
    where
        V::SliverAxis: 'a;
}

/// Encoder to construct a quilt and encode the blobs into slivers.
pub trait QuiltEncoderApi<V: QuiltVersion> {
    /// Constructs a quilt by encoding the blobs.
    ///
    /// Note: This function returns an error if the blobs have duplicate identifiers.
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

    /// Gets blobs by their identifiers from the quilt.
    ///
    /// If there are duplicate identifiers, the first one in the sort order will be returned.
    /// Note that the sort could be unstable.
    fn get_blobs_by_identifiers(
        &self,
        identifiers: &[&str],
    ) -> Result<Vec<QuiltStoreBlob<'static>>, QuiltError>;

    /// Gets a blob by its patch internal id.
    fn get_blob_by_patch_internal_id(
        &self,
        patch_internal_id: &[u8],
    ) -> Result<QuiltStoreBlob<'static>, QuiltError>;

    /// Gets blobs matching the given tag from the quilt.
    fn get_blobs_by_tag(
        &self,
        target_tag: &str,
        target_value: &str,
    ) -> Result<Vec<QuiltStoreBlob<'static>>, QuiltError>;

    /// Adds slivers to the decoder.
    fn add_slivers(&mut self, slivers: impl IntoIterator<Item = &'a SliverData<V::SliverAxis>>)
    where
        V::SliverAxis: 'a;
}

/// A trait to read bytes from quilt columns.
pub trait QuiltColumnRangeReader {
    /// Returns a vector of bytes in the columns starting from the given column index `start_col`.
    ///
    /// Assuming a 0-indexed bytes, the vector will contain the bytes in the range
    /// `[bytes_to_skip, bytes_to_skip + bytes_to_return)`.
    fn range_read_from_columns(
        &self,
        start_col: usize,
        bytes_to_skip: usize,
        bytes_to_return: usize,
    ) -> Result<Vec<u8>, QuiltError>;
}

/// The version of the quilt.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
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
            QuiltVersionV1::QUILT_VERSION_BYTE => Ok(QuiltVersionEnum::V1),
            _ => Err(QuiltError::Other(format!(
                "Invalid quilt version byte: {value}"
            ))),
        }
    }
}

impl fmt::Display for QuiltVersionEnum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
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

    /// Returns the blobs identified by the given identifiers.
    pub fn get_blobs_by_identifiers(
        &self,
        identifiers: &[&str],
    ) -> Result<Vec<QuiltStoreBlob<'static>>, QuiltError> {
        match self {
            QuiltEnum::V1(quilt_v1) => quilt_v1.get_blobs_by_identifiers(identifiers),
        }
    }

    /// Gets a blob by its patch internal id.
    pub fn get_blob_by_patch_internal_id(
        &self,
        patch_internal_id: &[u8],
    ) -> Result<QuiltStoreBlob<'static>, QuiltError> {
        match self {
            QuiltEnum::V1(quilt_v1) => quilt_v1.get_blob_by_patch_internal_id(patch_internal_id),
        }
    }

    /// Gets blobs matching the given tag from the quilt.
    pub fn get_blobs_by_tag(
        &self,
        target_tag: &str,
        target_value: &str,
    ) -> Result<Vec<QuiltStoreBlob<'static>>, QuiltError> {
        match self {
            QuiltEnum::V1(quilt_v1) => quilt_v1.get_blobs_by_tag(target_tag, target_value),
        }
    }

    /// Returns all the blobs in the quilt.
    pub fn get_all_blobs(&self) -> Result<Vec<QuiltStoreBlob<'static>>, QuiltError> {
        match self {
            QuiltEnum::V1(quilt_v1) => quilt_v1.get_all_blobs(),
        }
    }

    /// Returns the quilt index.
    pub fn get_quilt_index(&self) -> Result<QuiltIndex, QuiltError> {
        match self {
            QuiltEnum::V1(quilt_v1) => Ok(QuiltIndex::V1(quilt_v1.quilt_index()?.clone())),
        }
    }
}

/// A wrapper around a blob and its identifier.
///
/// A valid identifier is a string that contains only alphanumeric characters,
/// underscores, hyphens, and periods.
#[derive(Debug, Serialize, Clone, PartialEq, Eq)]
pub struct QuiltStoreBlob<'a> {
    /// The blob data, either borrowed or owned.
    blob: Cow<'a, [u8]>,
    /// The identifier of the blob.
    identifier: String,
    /// The tags of the blob.
    pub tags: BTreeMap<String, String>,
}

impl<'a> QuiltStoreBlob<'a> {
    /// Creates a new `QuiltStoreBlob` from a borrowed blob and an identifier.
    pub fn new(blob: &'a [u8], identifier: impl Into<String>) -> Result<Self, QuiltError> {
        let identifier = identifier.into();
        validate_quilt_identifier(&identifier)?;

        Ok(Self {
            blob: Cow::Borrowed(blob),
            identifier,
            tags: BTreeMap::new(),
        })
    }

    /// Returns the unencoded length of the blob.
    pub fn unencoded_length(&self) -> u64 {
        self.blob.len().try_into().expect("32 or 64-bit arch")
    }

    /// Creates a new `QuiltStoreBlob` from an owned blob and an identifier.
    pub fn new_owned(blob: Vec<u8>, identifier: impl Into<String>) -> Result<Self, QuiltError> {
        let identifier = identifier.into();
        validate_quilt_identifier(&identifier)?;

        Ok(Self {
            blob: Cow::Owned(blob),
            identifier,
            tags: BTreeMap::new(),
        })
    }

    /// Adds tags to the blob.
    pub fn with_tags(mut self, tags: impl IntoIterator<Item = (String, String)>) -> Self {
        self.tags = tags.into_iter().collect();
        self
    }

    /// Returns a reference to the blob data.
    pub fn data(&self) -> &[u8] {
        &self.blob
    }

    /// Returns the blob data by moving it out, consuming the QuiltStoreBlob.
    pub fn into_data(self) -> Vec<u8> {
        self.blob.into_owned()
    }

    /// Returns the blob data by moving it out.
    pub fn take_blob(&mut self) -> Cow<'a, [u8]> {
        core::mem::take(&mut self.blob)
    }

    /// Returns a reference to the identifier.
    pub fn identifier(&self) -> &str {
        &self.identifier
    }

    /// Returns a reference to the tags.
    pub fn tags(&self) -> &BTreeMap<String, String> {
        &self.tags
    }

    /// Converts the blob to owned data if it isn't already.
    pub fn into_owned(self) -> Self {
        match self.blob {
            Cow::Borrowed(_) => Self {
                blob: Cow::Owned(self.blob.into_owned()),
                identifier: self.identifier,
                tags: self.tags,
            },
            Cow::Owned(_) => self,
        }
    }
}

/// Quilt version 1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuiltVersionV1;

impl QuiltVersionV1 {
    const QUILT_VERSION_BYTE: u8 = 0x01;
    const BLOB_HEADER_SIZE: usize = 6;

    /// Decodes the quilt index from a column data source.
    pub fn decode_quilt_index<T>(
        data_source: &T,
        column_size: usize,
    ) -> Result<QuiltIndexV1, QuiltError>
    where
        T: QuiltColumnRangeReader,
    {
        let quilt_version_bytes =
            data_source.range_read_from_columns(0, 0, QUILT_VERSION_BYTES_LENGTH)?;
        assert!(quilt_version_bytes.len() == QUILT_VERSION_BYTES_LENGTH);
        utils::check_quilt_version::<QuiltVersionV1>(&quilt_version_bytes)?;

        let size_bytes = data_source.range_read_from_columns(
            0,
            QUILT_VERSION_BYTES_LENGTH,
            QUILT_INDEX_SIZE_BYTES_LENGTH,
        )?;
        assert!(size_bytes.len() == QUILT_INDEX_SIZE_BYTES_LENGTH);

        let index_size = usize::try_from(u32::from_le_bytes(
            size_bytes
                .try_into()
                .map_err(|_| QuiltError::FailedToExtractQuiltIndexSize)?,
        ))
        .map_err(|_| QuiltError::FailedToExtractQuiltIndexSize)?;

        let index_bytes =
            data_source.range_read_from_columns(0, QUILT_INDEX_PREFIX_SIZE, index_size)?;
        assert!(index_bytes.len() == index_size);

        // Decode the QuiltIndexV1.
        let mut quilt_index: QuiltIndexV1 = bcs::from_bytes(&index_bytes)?;

        let total_size = index_size + QUILT_INDEX_PREFIX_SIZE;
        let columns_needed = total_size.div_ceil(column_size);
        quilt_index.populate_start_indices(
            u16::try_from(columns_needed).expect("columns_needed should fit in u16"),
        );

        Ok(quilt_index)
    }

    /// Returns the total size of the serialized blob.
    pub fn serialized_blob_size(blob: &QuiltStoreBlob) -> Result<usize, QuiltError> {
        let identifier_size = bcs::serialized_size(&blob.identifier)
            .map_err(|e| QuiltError::Other(format!("Failed to compute identifier size: {e}")))?;

        if identifier_size >= MAX_BLOB_IDENTIFIER_BYTES_LENGTH {
            return Err(QuiltError::InvalidIdentifier(format!(
                "identifier size exceeds maximum allowed value: {MAX_BLOB_IDENTIFIER_BYTES_LENGTH}"
            )));
        }
        let mut prefix_size = identifier_size as usize + BLOB_IDENTIFIER_SIZE_BYTES_LENGTH;

        if !blob.tags.is_empty() {
            let tags_size = bcs::serialized_size(&blob.tags)
                .map_err(|e| QuiltError::Other(format!("Failed to compute tags size: {e}")))?;
            prefix_size += tags_size + TAGS_SIZE_BYTES_LENGTH;
        }

        Ok(prefix_size + blob.data().len() + QuiltVersionV1::BLOB_HEADER_SIZE)
    }

    /// Decodes a blob from a column data source.
    pub fn decode_blob<T>(
        data_source: &T,
        start_col: usize,
    ) -> Result<QuiltStoreBlob<'static>, QuiltError>
    where
        T: QuiltColumnRangeReader,
    {
        let header_bytes =
            data_source.range_read_from_columns(start_col, 0, Self::BLOB_HEADER_SIZE)?;
        assert!(header_bytes.len() == Self::BLOB_HEADER_SIZE);
        let blob_header = BlobHeaderV1::from_bytes(
            header_bytes
                .try_into()
                .expect("header_bytes should be 6 bytes"),
        );

        let mut offset = Self::BLOB_HEADER_SIZE;
        let mut blob_bytes_size =
            usize::try_from(blob_header.length).expect("length should fit in usize");

        let (identifier, bytes_consumed) =
            Self::decode_blob_identifier(data_source, start_col, offset)?;
        offset += bytes_consumed;
        blob_bytes_size -= bytes_consumed;

        let tags = if blob_header.has_tags() {
            let (tags, bytes_consumed) = Self::decode_blob_tags(data_source, start_col, offset)?;
            offset += bytes_consumed;
            blob_bytes_size -= bytes_consumed;
            tags
        } else {
            BTreeMap::new()
        };

        let data_bytes = data_source.range_read_from_columns(start_col, offset, blob_bytes_size)?;
        assert!(data_bytes.len() == blob_bytes_size);

        Ok(QuiltStoreBlob::new_owned(data_bytes, identifier)?.with_tags(tags))
    }

    /// Decodes the blob identifier from a column data source.
    /// Returns a tuple containing the decoded identifier string and the total number
    /// of bytes consumed.
    fn decode_blob_identifier<T>(
        data_source: &T,
        start_col: usize,
        initial_offset: usize,
    ) -> Result<(String, usize), QuiltError>
    where
        T: QuiltColumnRangeReader,
    {
        let mut offset = initial_offset;

        // Read identifier size (2 bytes).
        let size_buffer = data_source.range_read_from_columns(
            start_col,
            offset,
            BLOB_IDENTIFIER_SIZE_BYTES_LENGTH,
        )?;
        offset += BLOB_IDENTIFIER_SIZE_BYTES_LENGTH;

        // Parse identifier size.
        let identifier_size = usize::from(u16::from_le_bytes(
            size_buffer
                .try_into()
                .expect("size_buffer should be 2 bytes"),
        ));

        // Read the actual identifier.
        let identifier_bytes =
            data_source.range_read_from_columns(start_col, offset, identifier_size)?;
        debug_assert!(identifier_bytes.len() == identifier_size);

        // Deserialize the identifier bytes into a String.
        let identifier = bcs::from_bytes(&identifier_bytes).map_err(|_| {
            QuiltError::InvalidIdentifier("Failed to deserialize identifier".into())
        })?;

        // Calculate total bytes consumed.
        let bytes_consumed = BLOB_IDENTIFIER_SIZE_BYTES_LENGTH + identifier_size;

        Ok((identifier, bytes_consumed))
    }

    /// Decodes the blob tags from a column data source.
    fn decode_blob_tags<T>(
        data_source: &T,
        start_col: usize,
        initial_offset: usize,
    ) -> Result<(BTreeMap<String, String>, usize), QuiltError>
    where
        T: QuiltColumnRangeReader,
    {
        let size_bytes = data_source.range_read_from_columns(
            start_col,
            initial_offset,
            TAGS_SIZE_BYTES_LENGTH,
        )?;
        assert!(size_bytes.len() == TAGS_SIZE_BYTES_LENGTH);
        let tags_size =
            u16::from_le_bytes(size_bytes.try_into().expect("size_bytes should be 2 bytes"));
        let mut offset = initial_offset + TAGS_SIZE_BYTES_LENGTH;
        let tags_bytes =
            data_source.range_read_from_columns(start_col, offset, tags_size as usize)?;
        let tags = bcs::from_bytes(&tags_bytes)
            .map_err(|error| QuiltError::FailedToDecodeExtension("tags".into(), error))?;
        offset += tags_size as usize;
        Ok((tags, offset - initial_offset))
    }
}

impl QuiltVersion for QuiltVersionV1 {
    type QuiltConfig = QuiltConfigV1;
    type QuiltEncoder<'a> = QuiltEncoderV1<'a>;
    type QuiltDecoder<'a> = QuiltDecoderV1<'a>;
    type Quilt = QuiltV1;
    type QuiltIndex = QuiltIndexV1;
    type QuiltPatch = QuiltPatchV1;
    type QuiltPatchInternalId = QuiltPatchInternalIdV1;
    type QuiltMetadata = QuiltMetadataV1;
    type SliverAxis = Secondary;

    fn quilt_version_byte() -> u8 {
        QuiltVersionV1::QUILT_VERSION_BYTE
    }
}

/// The header of a encoded blob in QuiltVersionV1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BlobHeaderV1 {
    /// The length of the serialized blob.
    pub length: u32,
    /// The mask of the blob.
    pub mask: u8,
}

impl BlobHeaderV1 {
    /// The number of bytes used to store the size of the serialized blob.
    const BLOB_SIZE_BYTES_LENGTH: usize = 4;
    /// The mask bit that indicates whether the blob has attributes.
    const TAGS_ENABLED: u8 = 1;

    /// The maximum value of the length.
    const MAX_SERIALIZED_BLOB_SIZE: u32 = u32::MAX;
    /// The maximum value of the mask.
    const MAX_MASK_VALUE: u8 = u8::MAX;

    /// Creates a new blob header with the given length and mask.
    pub fn new(length: u32, mask: u8) -> Self {
        Self { length, mask }
    }

    /// Creates a `BlobHeaderV1` from a 6-byte array.
    /// The layout is: version (1 byte), length (next 32 bits), mask (next 1 bit).
    pub fn from_bytes(bytes: [u8; QuiltVersionV1::BLOB_HEADER_SIZE]) -> Self {
        let version = bytes[0];
        assert_eq!(version, QuiltVersionV1::QUILT_VERSION_BYTE);

        // Read 4 bytes for length (bytes 1-4).
        let length = u32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);

        // Read 1 byte for mask (byte 5).
        let mask = bytes[5];

        Self { length, mask }
    }

    /// Converts the `BlobHeaderV1` to a 6-byte array.
    pub fn as_bytes(&self) -> [u8; QuiltVersionV1::BLOB_HEADER_SIZE] {
        let mut data = [0u8; QuiltVersionV1::BLOB_HEADER_SIZE];

        // Set version byte.
        data[0] = QuiltVersionV1::QUILT_VERSION_BYTE;

        // Set length bytes (1-4).
        let length_bytes = self.length.to_le_bytes();
        data[1..5].copy_from_slice(&length_bytes);

        // Set mask byte (5).
        data[5] = self.mask;

        data
    }

    /// Returns true if the blob has tags.
    pub fn has_tags(&self) -> bool {
        self.mask & Self::TAGS_ENABLED != 0
    }

    /// Set the tags flag to true.
    pub fn set_has_tags(&mut self, has_tags: bool) {
        if has_tags {
            self.mask |= Self::TAGS_ENABLED;
        } else {
            self.mask &= !Self::TAGS_ENABLED;
        }
    }
}
/// A quilt is a collection of blobs encoded into a single blob.
///
/// For QuiltVersionV1:
/// The data is organized as a 2D matrix where:
/// - Each blob occupies a consecutive range of columns (secondary slivers).
/// - The first column's initial `QUILT_VERSION_BYTES_LENGTH` bytes contain the version byte.
/// - The next `QUILT_INDEX_SIZE_BYTES_LENGTH` bytes contain the unencoded length of the
///   [`QuiltIndexV1`].
/// - The [`QuiltIndexV1`] is stored in the first one or multiple columns, following the version
///   byte and the length of the [`QuiltIndexV1`].
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
    quilt_index: Option<QuiltIndexV1>,
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
        let mut quilt = QuiltV1 {
            data: quilt_blob,
            row_size,
            symbol_size,
            quilt_index: None,
        };
        let _ = quilt.get_or_decode_quilt_index()?;

        Ok(quilt)
    }

    fn get_blobs_by_identifiers(
        &self,
        identifiers: &[&str],
    ) -> Result<Vec<QuiltStoreBlob<'static>>, QuiltError> {
        self.quilt_index()?
            .get_quilt_patches_by_identifiers(identifiers)?
            .iter()
            .map(|patch| QuiltVersionV1::decode_blob(self, usize::from(patch.start_index)))
            .collect()
    }

    fn get_blob_by_patch_internal_id(
        &self,
        patch_internal_id: &[u8],
    ) -> Result<QuiltStoreBlob<'static>, QuiltError> {
        let patch_internal_id = QuiltPatchInternalIdV1::from_bytes(patch_internal_id)?;
        let start_col = usize::from(patch_internal_id.start_index);
        QuiltVersionV1::decode_blob(self, start_col)
    }

    fn get_blobs_by_tag(
        &self,
        target_tag: &str,
        target_value: &str,
    ) -> Result<Vec<QuiltStoreBlob<'static>>, QuiltError> {
        self.quilt_index()?
            .get_quilt_patches_by_tag(target_tag, target_value)
            .iter()
            .map(|patch| {
                let start_col = usize::from(patch.start_index);
                QuiltVersionV1::decode_blob(self, start_col)
            })
            .collect()
    }

    fn get_all_blobs(&self) -> Result<Vec<QuiltStoreBlob<'static>>, QuiltError> {
        self.quilt_index()?
            .patches()
            .iter()
            .map(|patch| QuiltVersionV1::decode_blob(self, usize::from(patch.start_index)))
            .collect()
    }

    fn quilt_index(&self) -> Result<&QuiltIndexV1, QuiltError> {
        self.quilt_index
            .as_ref()
            .ok_or(QuiltError::MissingQuiltIndex)
    }

    fn data(&self) -> &[u8] {
        &self.data
    }

    fn symbol_size(&self) -> usize {
        self.symbol_size
    }
}

// Implementation of QuiltColumnRangeReader for QuiltV1.
// Returns `IndexOutOfBounds` if there is not enough data to read.
impl QuiltColumnRangeReader for QuiltV1 {
    fn range_read_from_columns(
        &self,
        start_col: usize,
        mut bytes_to_skip: usize,
        mut bytes_to_return: usize,
    ) -> Result<Vec<u8>, QuiltError> {
        if self.symbol_size == 0 || self.row_size == 0 || self.data.is_empty() {
            return Err(QuiltError::Other("empty quilt data".to_string()));
        }

        // Initialize state
        let n_rows = self.data.len() / self.row_size;
        let symbols_to_skip = bytes_to_skip / self.symbol_size;
        let mut current_col = start_col + symbols_to_skip / n_rows;
        let mut current_row = symbols_to_skip % n_rows;
        let mut result = Vec::with_capacity(bytes_to_return);
        bytes_to_skip -= symbols_to_skip * self.symbol_size;

        // Helper function to calculate data slice for current position.
        let get_slice = |col: usize, row: usize, skip: usize, limit: usize| {
            let base_index = row * self.row_size + col * self.symbol_size;
            let start_index = base_index + skip;
            let end_index = (base_index + self.symbol_size)
                .min(start_index + limit)
                .min(self.data.len());
            if start_index >= self.data.len() {
                return Err(QuiltError::IndexOutOfBounds(start_index, self.data.len()));
            }
            Ok(&self.data[start_index..end_index])
        };

        // Helper function to advance to the next position.
        let advance_position = |col: &mut usize, row: &mut usize| {
            *row = (*row + 1) % n_rows;
            if *row == 0 {
                *col += 1;
            }
        };

        // Process cells until we've collected enough bytes or run out of columns.
        while bytes_to_return > 0 {
            let slice = get_slice(current_col, current_row, bytes_to_skip, bytes_to_return)?;
            result.extend_from_slice(slice);
            bytes_to_return -= slice.len();

            advance_position(&mut current_col, &mut current_row);
            bytes_to_skip = 0;
        }

        Ok(result)
    }
}

impl QuiltV1 {
    /// Returns the quilt index.
    ///
    /// If the quilt index is not set, it will decode it from the quilt data, and set it.
    fn get_or_decode_quilt_index(&mut self) -> Result<&QuiltIndexV1, QuiltError> {
        if self.quilt_index.is_some() {
            return Ok(self
                .quilt_index
                .as_ref()
                .expect("quilt index should be set"));
        }

        let columns_size = self.data.len() / self.row_size * self.symbol_size;
        let quilt_index = QuiltVersionV1::decode_quilt_index(self, columns_size)?;
        self.quilt_index = Some(quilt_index);

        Ok(self
            .quilt_index
            .as_ref()
            .expect("quilt index should be set"))
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
            &format_args!("\n{:#?}", DebugQuiltIndex(self.quilt_index().ok())),
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

            write!(f, "{entry:hex_width$}")?;

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

struct DebugQuiltIndex<'a>(Option<&'a QuiltIndexV1>);

impl fmt::Debug for DebugQuiltIndex<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(quilt_index) => {
                let mut list = f.debug_list();
                for patch in quilt_index.quilt_patches.iter() {
                    list.entry(&format_args!(
                        "\nQuiltPatch {{\n    end_index: {}\n    identifier: {:?}\n}}",
                        patch.end_index, patch.identifier
                    ));
                }
                list.finish()?;
            }
            None => {
                writeln!(f, "index does not exist")?;
            }
        }
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

    fn get_decoder(
        slivers: impl IntoIterator<Item = &'a SliverData<Secondary>>,
    ) -> QuiltDecoderV1<'a>
    where
        Secondary: 'a,
    {
        QuiltDecoderV1::new(slivers)
    }

    fn get_decoder_with_quilt_index(
        slivers: impl IntoIterator<Item = &'a SliverData<Secondary>>,
        quilt_index: &QuiltIndex,
    ) -> QuiltDecoderV1<'a>
    where
        Secondary: 'a,
    {
        let QuiltIndex::V1(quilt_index) = quilt_index;
        QuiltDecoderV1::new_with_quilt_index(slivers, quilt_index.clone())
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

    /// Returns the header and extension bytes of the blob.
    pub fn get_header_and_extension_bytes(blob: &QuiltStoreBlob) -> Result<Vec<u8>, QuiltError> {
        let mut identifier_bytes = Vec::new();
        let mut extension_bytes = Vec::new();
        let mut header = BlobHeaderV1::default();

        let identifier_size =
            u16::try_from(bcs::serialized_size(&blob.identifier).map_err(|e| {
                QuiltError::InvalidIdentifier(format!("Failed to serialize identifier: {e}"))
            })?)
            .map_err(|e| {
                QuiltError::InvalidIdentifier(format!(
                    "Failed to convert identifier size to u16: {e}"
                ))
            })?;
        identifier_bytes.extend_from_slice(&identifier_size.to_le_bytes());
        identifier_bytes.extend_from_slice(&bcs::to_bytes(&blob.identifier).map_err(|e| {
            QuiltError::InvalidIdentifier(format!("Failed to serialize identifier: {e}"))
        })?);
        extension_bytes.push(identifier_bytes);

        if !blob.tags.is_empty() {
            header.set_has_tags(true);
            let serialized_tags = bcs::to_bytes(&blob.tags)
                .map_err(|e| QuiltError::Other(format!("Failed to serialize tags: {e}")))?;

            // This must be the same as TAGS_SIZE_BYTES_LENGTH.
            let tags_size = u16::try_from(serialized_tags.len()).map_err(|e| {
                QuiltError::Other(format!("Failed to convert tags size to u16: {e}"))
            })?;

            let mut result_bytes = Vec::with_capacity(tags_size as usize + serialized_tags.len());
            result_bytes.extend_from_slice(&tags_size.to_le_bytes());
            result_bytes.extend_from_slice(&serialized_tags);
            extension_bytes.push(result_bytes);
        }

        let total_size = extension_bytes.iter().map(|b| b.len()).sum::<usize>() + blob.data().len();
        header.length = total_size as u32;
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
    ///
    /// Returns the number of columns used to store the blob.
    ///
    /// The blob data layout is as follows:
    ///
    /// ```text
    /// +------------------+-----------------------------+----------------------+------------------+
    /// | Blob Header      | Identifier Section          | Feature Section      | Blob Data        |
    /// | (6 bytes)        | (variable length)           | (optional)           | (variable length)|
    /// +------------------+-----------------------------+----------------------+------------------+
    ///                    |                             |                      |
    ///                    v                             v                      v
    /// +------------------+----------------+------------+----------------------+------------------+
    /// | BlobHeaderV1     | Identifier Size| Serialized | Feature Data         | Actual blob      |
    /// | (version, length,| (2 bytes)      | Identifier | (Feature size +      | data             |
    /// |  mask flags)     | u16            | (variable) | serialized features) | (variable)       |
    /// +------------------+----------------+------------+----------------------+------------------+
    /// ```
    ///
    /// - BlobHeaderV1: Contains version byte, length , and mask feature flags.
    /// - Identifier Size: 2-byte length of the serialized identifier.
    /// - Serialized Identifier: BCS-encoded identifier string.
    /// - Feature Data: Optional section for feature data, such as attributes,
    ///   (when mask flag is set).
    /// - Blob Data: The actual blob contents.
    fn add_blob_to_quilt(
        data: &mut [u8],
        blob: &QuiltStoreBlob,
        is_meta_blob: bool, // True if `blob` is the quilt index.
        current_col: usize,
        column_size: usize,
        row_size: usize,
        symbol_size: usize,
    ) -> Result<usize, QuiltError> {
        assert!(column_size % symbol_size == 0);

        let mut total_bytes_written = 0;
        if !is_meta_blob {
            let prefix_bytes = Self::get_header_and_extension_bytes(blob)?;
            total_bytes_written += prefix_bytes.len();

            Self::write_bytes_to_columns(
                data,
                &prefix_bytes,
                current_col,
                row_size,
                column_size,
                symbol_size,
                0,
            )?;
        }

        Self::write_bytes_to_columns(
            data,
            blob.data(),
            current_col,
            row_size,
            column_size,
            symbol_size,
            total_bytes_written,
        )?;

        total_bytes_written += blob.data().len();

        Ok(total_bytes_written.div_ceil(column_size))
    }

    fn write_bytes_to_columns(
        data: &mut [u8],
        bytes: &[u8],
        start_col: usize,
        row_size: usize,
        column_size: usize,
        symbol_size: usize,
        bytes_to_skip: usize,
    ) -> Result<(), QuiltError> {
        let n_rows = column_size / symbol_size;
        let n_cols = row_size / symbol_size;

        let mut current_col = start_col + bytes_to_skip / column_size;
        assert!(current_col < n_cols);
        let mut current_row = (bytes_to_skip / symbol_size) % n_rows;
        let mut offset = bytes_to_skip % symbol_size;
        assert!(offset < symbol_size);
        let mut idx = 0;

        while idx < bytes.len() {
            let base_idx = current_row * row_size + current_col * symbol_size;
            let start_idx = base_idx + offset;
            let len = (symbol_size - offset).min(bytes.len() - idx);

            data[start_idx..start_idx + len].copy_from_slice(&bytes[idx..idx + len]);
            idx += len;
            current_row = (current_row + 1) % n_rows;
            if current_row == 0 {
                current_col += 1;
            }

            // Only the first symbol requires offset.
            offset = 0;
        }

        Ok(())
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

        // Check for duplicate identifiers.
        for adjacent_blobs in blob_pairs.windows(2) {
            if adjacent_blobs[0].identifier == adjacent_blobs[1].identifier {
                return Err(QuiltError::DuplicateIdentifier(
                    adjacent_blobs[0].identifier.clone(),
                ));
            }
        }

        // Create initial QuiltPatches.
        let quilt_patches = blob_pairs
            .iter()
            .map(|blob| QuiltPatchV1::new_with_tags(blob.identifier.clone(), blob.tags.clone()))
            .collect::<Result<Vec<QuiltPatchV1>, QuiltError>>()?;

        let mut quilt_index = QuiltIndexV1 { quilt_patches };

        // Get the serialized quilt index size.
        let serialized_index_size = u32::try_from(bcs::serialized_size(&quilt_index)?)
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
                    .map(|blob| QuiltVersionV1::serialized_blob_size(blob)),
            )
            .collect::<Result<Vec<usize>, QuiltError>>()?;

        let symbol_size = utils::compute_symbol_size(
            &all_sizes,
            n_columns,
            n_rows,
            MAX_NUM_SLIVERS_FOR_QUILT_INDEX,
            self.config.encoding_type(),
        )?;

        let row_size = symbol_size * n_columns;
        let mut data = vec![0u8; row_size * n_rows];

        // Calculate columns needed for the index.
        let column_size = symbol_size * n_rows;
        let index_cols_needed = index_total_size.div_ceil(column_size);
        assert!(index_cols_needed <= MAX_NUM_SLIVERS_FOR_QUILT_INDEX);
        let mut current_col = index_cols_needed;

        // Fill data with actual blobs and populate quilt patches.
        for (i, quilt_store_blob) in blob_pairs.iter().enumerate() {
            let cols_needed = Self::add_blob_to_quilt(
                &mut data,
                quilt_store_blob,
                false,
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
            .extend_from_slice(&bcs::to_bytes(&quilt_index).expect("serialization should succeed"));
        assert_eq!(meta_blob_data.len(), index_total_size);

        let meta_blob = QuiltStoreBlob::new(&meta_blob_data, "quilt_index")?;
        // Add the index to the quilt.
        let index_cols_used = Self::add_blob_to_quilt(
            &mut data,
            &meta_blob,
            true,
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
            quilt_index: Some(quilt_index),
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
            quilt_id: *metadata.blob_id(),
            metadata: metadata.metadata().clone(),
            index: QuiltIndexV1 {
                quilt_patches: quilt.quilt_index()?.quilt_patches.clone(),
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
    column_size: Option<usize>,
}

impl<'a> QuiltDecoderApi<'a, QuiltVersionV1> for QuiltDecoderV1<'a> {
    fn get_or_decode_quilt_index(&mut self) -> Result<QuiltIndex, QuiltError> {
        if let Some(quilt_index) = self.quilt_index.as_ref() {
            return Ok(quilt_index.clone().into());
        }

        self.check_missing_slivers(0, 1)?;
        let column_size = self.column_size.expect("column size should be set");
        let quilt_index = QuiltVersionV1::decode_quilt_index(self, column_size)?;
        self.quilt_index = Some(quilt_index.clone());

        Ok(quilt_index.into())
    }

    fn get_blobs_by_identifiers(
        &self,
        identifiers: &[&str],
    ) -> Result<Vec<QuiltStoreBlob<'static>>, QuiltError> {
        self.quilt_index
            .as_ref()
            .ok_or(QuiltError::MissingQuiltIndex)
            .and_then(|quilt_index| quilt_index.get_quilt_patches_by_identifiers(identifiers))?
            .iter()
            .map(|patch| self.get_blob_by_quilt_patch(patch))
            .collect()
    }

    fn get_blob_by_patch_internal_id(
        &self,
        patch_internal_id: &[u8],
    ) -> Result<QuiltStoreBlob<'static>, QuiltError> {
        let patch_internal_id = QuiltPatchInternalIdV1::from_bytes(patch_internal_id)?;
        let start_col = usize::from(patch_internal_id.start_index);
        let end_col = usize::from(patch_internal_id.end_index);
        self.check_missing_slivers(start_col, end_col)?;
        QuiltVersionV1::decode_blob(self, start_col)
    }

    fn get_blobs_by_tag(
        &self,
        target_tag: &str,
        target_value: &str,
    ) -> Result<Vec<QuiltStoreBlob<'static>>, QuiltError> {
        self.quilt_index
            .as_ref()
            .ok_or(QuiltError::MissingQuiltIndex)
            .and_then(|quilt_index| {
                quilt_index
                    .get_quilt_patches_by_tag(target_tag, target_value)
                    .iter()
                    .map(|patch| self.get_blob_by_quilt_patch(patch))
                    .collect()
            })
    }

    fn add_slivers(&mut self, slivers: impl IntoIterator<Item = &'a SliverData<Secondary>>)
    where
        Secondary: 'a,
    {
        for sliver in slivers {
            self.column_size
                .get_or_insert_with(|| sliver.symbols.data().len());
            self.slivers.insert(sliver.index, sliver);
        }
    }
}

// Implementation of QuiltColumnRangeReader for QuiltDecoderV1.
impl QuiltColumnRangeReader for QuiltDecoderV1<'_> {
    fn range_read_from_columns(
        &self,
        start_col: usize,
        mut bytes_to_skip: usize,
        mut bytes_to_return: usize,
    ) -> Result<Vec<u8>, QuiltError> {
        self.check_missing_slivers(start_col, start_col + 1)?;
        let column_size = self.column_size.expect("column size should be set");
        let end_col = start_col + (bytes_to_skip + bytes_to_return).div_ceil(column_size);
        self.check_missing_slivers(start_col, end_col)?;

        let slivers: Vec<&SliverData<Secondary>> = (start_col..end_col)
            .map(|col| {
                *self
                    .slivers
                    .get(&SliverIndex::new(col as u16))
                    .expect("sliver exists")
            })
            .collect();

        let mut result = Vec::with_capacity(bytes_to_return);

        for sliver in slivers {
            if bytes_to_return == 0 {
                break;
            }

            let data = sliver.symbols.data();

            if bytes_to_skip >= data.len() {
                bytes_to_skip -= data.len();
                continue;
            }

            let start_offset = bytes_to_skip;
            let available = data.len() - start_offset;
            let copy_len = available.min(bytes_to_return);
            let end_offset = start_offset + copy_len;

            result.extend_from_slice(&data[start_offset..end_offset]);

            bytes_to_return -= copy_len;
            bytes_to_skip = 0;
        }

        Ok(result)
    }
}

impl<'a> QuiltDecoderV1<'a> {
    /// Creates a new QuiltDecoderV1 without slivers.
    pub fn new(slivers: impl IntoIterator<Item = &'a SliverData<Secondary>>) -> Self
    where
        Secondary: 'a,
    {
        let slivers = slivers
            .into_iter()
            .map(|s| (s.index, s))
            .collect::<HashMap<_, _>>();
        let column_size = slivers.values().next().map(|s| s.symbols.data().len());
        Self {
            slivers,
            quilt_index: None,
            column_size,
        }
    }

    /// Creates a new QuiltDecoderV1 with the given slivers, and a quilt index.
    pub fn new_with_quilt_index(
        slivers: impl IntoIterator<Item = &'a SliverData<Secondary>>,
        quilt_index: QuiltIndexV1,
    ) -> Self
    where
        Secondary: 'a,
    {
        let slivers = slivers
            .into_iter()
            .map(|s| (s.index, s))
            .collect::<HashMap<_, _>>();
        let column_size = slivers.values().next().map(|s| s.symbols.data().len());
        Self {
            slivers,
            quilt_index: Some(quilt_index),
            column_size,
        }
    }

    /// Gets the blob by QuiltPatchV1.
    fn get_blob_by_quilt_patch(
        &self,
        quilt_patch: &QuiltPatchV1,
    ) -> Result<QuiltStoreBlob<'static>, QuiltError> {
        let start_col = usize::from(quilt_patch.start_index);
        let end_col = usize::from(quilt_patch.end_index);
        self.check_missing_slivers(start_col, end_col)?;
        QuiltVersionV1::decode_blob(self, start_col)
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
        encoding_type: EncodingType,
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

        let symbol_size = min_val.next_multiple_of(encoding_type.required_alignment().into());
        debug_assert!(can_blobs_fit_into_matrix(
            blobs_sizes,
            n_columns,
            symbol_size * n_rows
        ));
        let max_symbol_size = usize::from(encoding_type.max_symbol_size());
        if symbol_size > max_symbol_size {
            return Err(QuiltError::QuiltOversize(format!(
                "the resulting symbol size {symbol_size} is larger than the maximum symbol size \
                {max_symbol_size}; remove some blobs"
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
    use core::num::NonZeroU16;
    use std::collections::HashSet;

    use rand::Rng;
    use walrus_test_utils::param_test;

    use super::*;
    use crate::{
        encoding::ReedSolomonEncodingConfig,
        metadata::BlobMetadataApi as _,
        test_utils::QuiltTestData,
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
            case_2: (&[1000, 1, 1], 4, 7, 2, Ok(72)),
            case_3: (
                &[],
                3,
                1,
                1,
                Err(QuiltError::EmptyInput("blobs".to_string())),
            ),
            case_4: (&[1], 3, 2, 1, Ok(4)),
            case_5: (&[115, 80, 4], 17, 9, 3, Ok(6)),
            case_6: (&[20, 20, 20], 3, 5, 2, Ok(4)),
            case_7: (&[5, 5, 5], 5, 1, 1, Ok(6)),
            case_8: (&[25, 35, 45], 200, 1, 3, Ok(10)),
            case_9: (&[10, 0, 0, 0], 17, 9, 2, Ok(2)),
            case_10: (&[10, 0, 0, 0], 17, 9, 2, Ok(2)),
            case_11: (
                &[
                    416, 253, 258, 384, 492, 303, 276, 464, 143, 251, 388, 263, 515, 433, 505,
                    385, 346, 69, 48, 495, 329, 450, 494, 104, 539, 245, 109, 317, 60
                ],
                34,
                16,
                3,
                Ok(32)
            ),
        ]
    }
    fn test_quilt_find_min_length(
        blobs: &[usize],
        n_columns: usize,
        n_rows: usize,
        max_num_slivers_for_quilt_index: usize,
        expected: Result<usize, QuiltError>,
    ) {
        let _ = tracing_subscriber::fmt().try_init();
        let res = utils::compute_symbol_size(
            blobs,
            n_columns,
            n_rows,
            max_num_slivers_for_quilt_index,
            EncodingType::RS2,
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
                        blob: Cow::Borrowed(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..]),
                        identifier: "test-blob-0".to_string(),
                        tags: BTreeMap::from([("tag1".to_string(), "value1".to_string())]),
                    },
                    QuiltStoreBlob {
                        blob: Cow::Borrowed(&[5, 68, 3, 2, 5][..]),
                        identifier: "test-blob-1".to_string(),
                        tags: BTreeMap::from([("tag1".to_string(), "value1".to_string())]),
                    },
                    QuiltStoreBlob {
                        blob: Cow::Borrowed(&[5, 68, 3, 2, 5, 6, 78, 8][..]),
                        identifier: "test-blob-2".to_string(),
                        tags: BTreeMap::from([("tag1".to_string(), "value1".to_string())]),
                    },
                ],
                7
            ),
            case_0_random_order: (
                &[
                    QuiltStoreBlob {
                        blob: Cow::Borrowed(&[5, 68, 3, 2, 5, 6, 78, 8][..]),
                        identifier: "test-blob-0".to_string(),
                        tags: BTreeMap::from([("tag1".to_string(), "value1".to_string())]),
                    },
                    QuiltStoreBlob {
                        blob: Cow::Borrowed(&[5, 68, 3, 2, 5][..]),
                        identifier: "test-blob-1".to_string(),
                        tags: BTreeMap::new(),
                    },
                    QuiltStoreBlob {
                        blob: Cow::Borrowed(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..]),
                        identifier: "test-blob-2".to_string(),
                        tags: BTreeMap::from([("tag2".to_string(), "value2".to_string())]),
                    },
                ],
                7
            ),
            case_1: (
                &[
                    QuiltStoreBlob {
                        blob: Cow::Borrowed(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..]),
                        identifier: "test-blob-0".to_string(),
                        tags: BTreeMap::from([
                            ("tag1".to_string(), "value1".to_string()),
                            ("tag2".to_string(), "value1".to_string()),
                        ]),
                    },
                    QuiltStoreBlob {
                        blob: Cow::Borrowed(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..]),
                        identifier: "test-blob-1".to_string(),
                        tags: BTreeMap::from([
                            ("tag3".to_string(), "value3".to_string()),
                            ("tag2".to_string(), "value2".to_string()),
                        ]),
                    },
                    QuiltStoreBlob {
                        blob: Cow::Borrowed(&[5, 68, 3, 2, 5, 6, 78, 8][..]),
                        identifier: "test-blob-2".to_string(),
                        tags: BTreeMap::from([
                            ("tag3".to_string(), "value1".to_string()),
                            ("tag2".to_string(), "value3".to_string()),
                        ]),
                    },
                ],
                7
            ),
            case_1_random_order: (
                &[
                    QuiltStoreBlob {
                        blob: Cow::Borrowed(&[5, 68, 3, 2, 5][..]),
                        identifier: "test-blob-0".to_string(),
                        tags: BTreeMap::new(),
                    },
                    QuiltStoreBlob {
                        blob: Cow::Borrowed(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..]),
                        identifier: "short".to_string(),
                        tags: BTreeMap::new(),
                    },
                    QuiltStoreBlob {
                        blob: Cow::Borrowed(&[5, 68, 3, 2, 5, 6, 78, 8][..]),
                        identifier: "test-blob-2".to_string(),
                        tags: BTreeMap::new(),
                    },
                ],
                7
            ),
            case_2: (
                &[
                    QuiltStoreBlob {
                        blob: Cow::Borrowed(&[1, 3][..]),
                        identifier: "test-blob-0".to_string(),
                        tags: BTreeMap::new(),
                    },
                    QuiltStoreBlob {
                        blob: Cow::Borrowed(&[255u8; 1024][..]),
                        identifier: "test-blob-1".to_string(),
                        tags: BTreeMap::new(),
                    },
                    QuiltStoreBlob {
                        blob: Cow::Borrowed(&[1, 2, 3][..]),
                        identifier: "test-blob-2".to_string(),
                        tags: BTreeMap::new(),
                    },
                ],
                12
            ),
            case_3: (
                &[
                    QuiltStoreBlob {
                        blob: Cow::Borrowed(&[9, 8, 7, 6, 5, 4, 3, 2, 1][..]),
                        identifier: "test-blob-0".to_string(),
                        tags: BTreeMap::new(),
                    },
                ],
                7
            ),
        ]
    }
    fn test_quilt_construct_quilt(quilt_store_blobs: &[QuiltStoreBlob<'_>], n_shards: u16) {
        let reed_solomon_config =
            ReedSolomonEncodingConfig::new(NonZeroU16::try_from(n_shards).unwrap());

        construct_quilt(
            quilt_store_blobs,
            EncodingConfigEnum::ReedSolomon(&reed_solomon_config),
        );
    }

    fn construct_quilt(quilt_store_blobs: &[QuiltStoreBlob<'_>], config: EncodingConfigEnum) {
        let _ = tracing_subscriber::fmt().try_init();

        let encoder = QuiltConfigV1::get_encoder(config.clone(), quilt_store_blobs);

        let quilt = encoder.construct_quilt().expect("Should construct quilt");

        // Verify each blob and its description.
        for quilt_store_blob in quilt_store_blobs {
            // Verify blob data matches.
            let identifiers = vec![quilt_store_blob.identifier.as_str()];
            let extracted_blob = quilt
                .get_blobs_by_identifiers(&identifiers)
                .expect("Patch should exist for this blob identifier")
                .pop()
                .expect("Should be able to get blob by identifier");
            assert_eq!(
                extracted_blob, *quilt_store_blob,
                "Mismatch in encoded blob"
            );

            let quilt_patch = quilt
                .quilt_index()
                .expect("Quilt index should exist")
                .get_quilt_patches_by_identifiers(&identifiers)
                .expect("Patch should exist for this blob ID")
                .pop()
                .expect("Should be able to get quilt patch by identifier");
            assert_eq!(
                quilt_patch.identifier(),
                quilt_store_blob.identifier(),
                "Mismatch in blob description"
            );

            let blob_by_identifier = quilt
                .get_blobs_by_identifiers(&identifiers)
                .expect("Should be able to get blob by identifier")
                .pop()
                .expect("Should be able to get blob by identifier");
            assert_eq!(blob_by_identifier, *quilt_store_blob);
        }

        assert_eq!(
            quilt
                .quilt_index()
                .expect("Quilt index should exist")
                .quilt_patches
                .len(),
            quilt_store_blobs.len()
        );
    }

    #[test]
    #[ignore = "ignore long-running test by default"]
    fn test_quilt_with_random_blobs() {
        for _ in 0..1000 {
            let mut rng = rand::thread_rng();
            let num_blobs = rng.gen_range(1..50) as usize;
            let n_shards = rng.gen_range(((num_blobs + 5) * 3).div_ceil(2)..100) as u16;
            let min_blob_size = rng.gen_range(1..100);
            let max_blob_size = rng.gen_range(min_blob_size..1000);
            std::println!(
                "test_quilt_with_random_blobs: \
                {num_blobs}, {min_blob_size}, {max_blob_size}, {n_shards}"
            );
            // test_quilt_encoder_and_decoder(num_blobs, min_blob_size, max_blob_size, n_shards);
            test_quilt_encoder_and_decoder(num_blobs, min_blob_size, max_blob_size, n_shards);
        }
    }

    param_test! {
        test_quilt_encoder_and_decoder: [
            case_0: (3, 5, 16, 7),
            case_1: (3, 3, 800, 7),
            case_2: (3, 1024, 10240, 7),
            case_3: (1, 10, 1000, 7),
            case_4: (60, 1, 1000, 100),
            case_5: (2, 1, 5, 100),
            case_6: (10, 0, 2, 100),
        ]
    }
    fn test_quilt_encoder_and_decoder(
        num_blobs: usize,
        min_blob_size: usize,
        max_blob_size: usize,
        n_shards: u16,
    ) {
        let _ = tracing_subscriber::fmt().try_init();

        const MAX_NUM_TAGS: usize = 10;
        const MAX_VALUE_LENGTH: usize = 100;
        let test_data = QuiltTestData::new_owned(
            num_blobs,
            min_blob_size,
            max_blob_size,
            MAX_VALUE_LENGTH,
            MAX_NUM_TAGS,
        )
        .expect("Should create test data");

        let reed_solomon_config =
            ReedSolomonEncodingConfig::new(NonZeroU16::try_from(n_shards).unwrap());

        encode_decode_quilt(
            test_data,
            EncodingConfigEnum::ReedSolomon(&reed_solomon_config),
        );
    }

    fn encode_decode_quilt(mut test_data: QuiltTestData<'_>, config: EncodingConfigEnum) {
        let _ = tracing_subscriber::fmt().try_init();

        let quilt_store_blobs = test_data.take_blobs();

        let encoder = QuiltConfigV1::get_encoder(config.clone(), quilt_store_blobs.as_slice());

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

        let mut quilt_decoder = QuiltConfigV1::get_decoder(core::iter::empty());
        let decode_index_result = quilt_decoder.get_or_decode_quilt_index();
        assert!(matches!(
            decode_index_result,
            Err(QuiltError::MissingSlivers(_))
        ));

        quilt_decoder.add_slivers(vec![*first_sliver]);
        let decode_index_result = quilt_decoder.get_or_decode_quilt_index();
        let missing_slivers =
            if let Err(QuiltError::MissingSlivers(missing_indices)) = decode_index_result {
                tracing::info!("missing_indices: {:?}", missing_indices);
                slivers
                    .iter()
                    .filter(|sliver| missing_indices.contains(&sliver.index))
                    .copied()
                    .collect()
            } else {
                vec![]
            };

        if !missing_slivers.is_empty() {
            quilt_decoder.add_slivers(missing_slivers);
            assert!(quilt_decoder.get_or_decode_quilt_index().is_ok());
        }

        assert_eq!(
            quilt_decoder.get_or_decode_quilt_index(),
            Ok(quilt_metadata_v1.index.clone().into())
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
            .get_quilt_patches_by_identifiers(&[identifier])
            .expect("quilt patch should exist")
            .pop()
            .expect("quilt patch should exist");
        assert_eq!(patch.identifier, identifier);

        let missing_indices: Vec<SliverIndex> = (patch.start_index..patch.end_index)
            .map(SliverIndex)
            .collect();
        assert_eq!(
            quilt_decoder.get_blobs_by_identifiers(&[identifier]),
            Err(QuiltError::MissingSlivers(missing_indices.clone()))
        );

        let required_indices = quilt_index_v1
            .get_sliver_indices_for_identifiers(&[identifier])
            .expect("Should get sliver indices by identifiers");
        assert_eq!(required_indices, missing_indices);

        // Add only the missing slivers needed for this blob.
        let missing_slivers: Vec<&SliverData<Secondary>> = slivers
            .iter()
            .filter(|sliver| missing_indices.contains(&sliver.index))
            .copied()
            .collect();
        quilt_decoder.add_slivers(missing_slivers);

        // Check we can decode the blob with the slivers we added.
        let decoded_blob = quilt_decoder
            .get_blobs_by_identifiers(&[identifier])
            .expect("Should be able to decode blob after adding missing slivers")
            .pop()
            .expect("Should be able to decode blob after adding missing slivers");
        let expected_blob = quilt_store_blobs
            .iter()
            .find(|blob| blob.identifier == identifier)
            .expect("Should find blob in test data");
        assert_eq!(decoded_blob, *expected_blob);

        // Now, add all slivers to the decoder, all the blobs should be reconstructed.
        quilt_decoder.add_slivers(slivers);
        assert_eq!(
            quilt_decoder.get_or_decode_quilt_index(),
            Ok(quilt_metadata_v1.index.into())
        );

        for quilt_store_blob in &quilt_store_blobs {
            tracing::debug!("decoding blob {}", quilt_store_blob.identifier);
            let blob = quilt_decoder
                .get_blobs_by_identifiers(&[quilt_store_blob.identifier.as_str()])
                .expect("Should get blob by identifier")
                .pop()
                .expect("Should get blob by identifier");
            assert_eq!(blob, *quilt_store_blob);
        }

        for (tag, val) in &test_data.blob_identifiers_by_tag {
            for (tag_value, identifiers) in val {
                let blobs = quilt_decoder
                    .get_blobs_by_tag(tag, tag_value)
                    .expect("Should get blobs by tag");
                assert_eq!(blobs.len(), identifiers.len());
                let identifiers_set = blobs
                    .iter()
                    .map(|blob| blob.identifier.as_str())
                    .collect::<HashSet<_>>();
                assert_eq!(
                    identifiers_set,
                    identifiers
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<HashSet<_>>()
                );
            }
        }

        let (quilt_blob, metadata_with_id) = config
            .decode_and_verify(
                &quilt_metadata_v1.quilt_id,
                quilt_metadata_v1.metadata.unencoded_length(),
                sliver_pairs
                    .iter()
                    .map(|s| s.secondary.clone())
                    .collect::<Vec<_>>(),
            )
            .expect("data is not too large")
            .expect("should decode and verify quilt")
            .expect("should decode quilt");

        assert_eq!(metadata_with_id.metadata(), &quilt_metadata_v1.metadata);

        let quilt = QuiltV1::new_from_quilt_blob(quilt_blob, &config).expect("Should create quilt");
        assert_eq!(
            quilt.data(),
            encoder
                .construct_quilt()
                .expect("Should construct quilt")
                .data()
        );

        for (tag, val) in &test_data.blob_identifiers_by_tag {
            for (tag_value, identifiers) in val {
                let blobs = quilt
                    .get_blobs_by_tag(tag, tag_value)
                    .expect("Should get blobs by tag");
                assert_eq!(blobs.len(), identifiers.len());
                let identifiers_set = blobs
                    .iter()
                    .map(|blob| blob.identifier.as_str())
                    .collect::<HashSet<_>>();
                assert_eq!(
                    identifiers_set,
                    identifiers
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<HashSet<_>>()
                );
            }
        }
    }

    param_test! {
        test_quilt_blob_header: [
            case_0: (10233, 5),
            case_1: (10, 3),
            case_2: (125, 10),
            case_3: (1, 1),
            case_4: (0, 0),
            case_5: (BlobHeaderV1::MAX_SERIALIZED_BLOB_SIZE, 0),
            case_6: (0, BlobHeaderV1::MAX_MASK_VALUE),
            case_7: (BlobHeaderV1::MAX_SERIALIZED_BLOB_SIZE, BlobHeaderV1::MAX_MASK_VALUE),
            case_8: (1, BlobHeaderV1::MAX_MASK_VALUE),
            case_9: (BlobHeaderV1::MAX_SERIALIZED_BLOB_SIZE, 1),
        ]
    }
    fn test_quilt_blob_header(length: u32, mask: u8) {
        let header = BlobHeaderV1::new(length, mask);

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

        assert_eq!(reconstructed_header.length, length);
        assert_eq!(reconstructed_header.mask, mask);
    }

    #[test]
    fn test_quilt_store_blob_equality() -> Result<(), QuiltError> {
        // Create blobs with same content but different storage (borrowed vs owned)
        let data = vec![1, 2, 3, 4, 5];
        let borrowed = QuiltStoreBlob::new(&data, "test-blob")?;
        let owned = QuiltStoreBlob::new_owned(data.clone(), "test-blob")?;

        // They should be equal because content is the same
        assert_eq!(borrowed, owned);

        // Add same tags to both
        let mut tags = BTreeMap::new();
        tags.insert("key1".to_string(), "value1".to_string());
        let borrowed_with_tags = borrowed.clone().with_tags(tags.clone());
        let owned_with_tags = owned.clone().with_tags(tags);

        // They should still be equal with same tags
        assert_eq!(borrowed_with_tags, owned_with_tags);

        // Different content should not be equal
        let different = QuiltStoreBlob::new(&[1, 2, 3, 4, 6], "test-blob")?;
        assert_ne!(borrowed, different);

        // Different identifier should not be equal
        let different_id = QuiltStoreBlob::new(&data, "different-id")?;
        assert_ne!(borrowed, different_id);

        // Different tags should not be equal
        let mut different_tags = BTreeMap::new();
        different_tags.insert("key1".to_string(), "different-value".to_string());
        let borrowed_different_tags = borrowed.with_tags(different_tags);
        assert_ne!(borrowed_with_tags, borrowed_different_tags);

        Ok(())
    }
}
