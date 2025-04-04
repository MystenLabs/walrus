// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use alloc::{
    fmt::Display,
    string::{String, ToString},
};
use core::num::NonZeroU16;

use thiserror::Error;

use crate::SliverIndex;

/// Error indicating that the data is too large to be encoded/decoded.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
#[error("the data is too large to be encoded/decoded")]
pub struct DataTooLargeError;

/// Error returned when encoding/decoding is impossible due to the given data size.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum InvalidDataSizeError {
    /// The data is too large to be encoded/decoded.
    #[error("the data is too large")]
    DataTooLarge,
    /// The data to be encoded/decoded is empty.
    #[error("the data is empty")]
    EmptyData,
}

impl From<DataTooLargeError> for InvalidDataSizeError {
    fn from(_value: DataTooLargeError) -> Self {
        Self::DataTooLarge
    }
}

/// Error type returned when encoding fails.
#[derive(Debug, Error, PartialEq, Clone)]
pub enum EncodeError {
    /// The data size is invalid for this encoder.
    #[error(transparent)]
    InvalidDataSize(#[from] InvalidDataSizeError),
    /// The data is not properly aligned; i.e., it is not a multiple of the symbol size or symbol
    /// count.
    #[error("the data is not properly aligned (must be a multiple of {0})")]
    MisalignedData(NonZeroU16),
    /// The parameters are incompatible with the Reed-Solomon encoder.
    #[error("the parameters are incompatible with the Reed-Solomon encoder: {0}")]
    IncompatibleParameters(#[from] reed_solomon_simd::Error),
}

/// Error type returned when computing recovery symbols fails.
#[derive(Debug, Error, PartialEq, Clone)]
pub enum RecoverySymbolError {
    /// The index of the recovery symbol can be at most `n_shards`.
    #[error("the index of the recovery symbol can be at most `n_shards`")]
    IndexTooLarge,
    /// The underlying basic encoder returned an error.
    #[error(transparent)]
    EncodeError(#[from] EncodeError),
}

impl From<InvalidDataSizeError> for RecoverySymbolError {
    fn from(value: InvalidDataSizeError) -> Self {
        EncodeError::from(value).into()
    }
}

/// Error type returned when attempting to recover a sliver from recovery symbols fails.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum SliverRecoveryError {
    /// The blob size in the metadata is is too large.
    #[error("the blob size in the metadata is too large")]
    BlobSizeTooLarge(#[from] DataTooLargeError),
    /// The sliver decoding failed.
    #[error("the decoding failed")]
    DecodingFailure,
}

/// Error type returned when attempting to recover a sliver from recovery symbols fails or the
/// resulting sliver cannot be verified.
#[derive(Debug, Error, PartialEq, Clone)]
pub enum SliverRecoveryOrVerificationError {
    /// Recovery of the sliver failed.
    #[error(transparent)]
    RecoveryError(#[from] SliverRecoveryError),
    /// The verification of the decoded sliver failed.
    #[error(transparent)]
    VerificationError(#[from] SliverVerificationError),
}

impl From<DataTooLargeError> for SliverRecoveryOrVerificationError {
    fn from(value: DataTooLargeError) -> Self {
        SliverRecoveryError::from(value).into()
    }
}

/// Error returned when the size of input symbols does not match the size of existing symbols.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
#[error("the size of the symbols provided does not match the size of the existing symbols")]
pub struct WrongSymbolSizeError;

/// Error returned when the verification of a reconstructed blob fails. Verification failure occurs
/// when the provided blob ID does not match the blob ID computed from the reconstructed blob.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
#[error("decoding verification failed because the blob ID does not match the provided metadata")]
pub struct DecodingVerificationError;

/// Error returned when trying to extract the wrong variant (primary or secondary) of
/// [`Sliver`][super::SliverData] from it.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
#[error("cannot convert the `Sliver` to the sliver variant requested")]
pub struct WrongSliverVariantError;

/// Error returned when sliver verification fails.
#[derive(Debug, Error, PartialEq, Clone)]
pub enum SliverVerificationError {
    /// The sliver index is too large for the number of shards in the metadata.
    #[error("the sliver index is too large for the number of shards in the metadata")]
    IndexTooLarge,
    /// The length of the provided sliver does not match the number of source symbols in the
    /// metadata.
    #[error("the length of the provided sliver does not match the metadata")]
    SliverSizeMismatch,
    /// The symbol size of the provided sliver does not match the symbol size that can be computed
    /// from the metadata.
    #[error("the symbol size of the provided sliver does not match the metadata")]
    SymbolSizeMismatch,
    /// The recomputed Merkle root of the provided sliver does not match the root stored in the
    /// metadata.
    #[error("the recomputed Merkle root of the provided sliver does not match the metadata")]
    MerkleRootMismatch,
}

/// Error returned when verification of a recovery symbol fails.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum SymbolVerificationError {
    /// The sliver index is too large for the number of shards in the metadata.
    #[error("the sliver index is too large for the number of shards in the metadata")]
    IndexTooLarge,
    /// The symbol size does not match the symbol size that can be computed from the metadata.
    #[error("the symbol size does not match the metadata")]
    SymbolSizeMismatch,
    /// The verification of the Merkle proof failed.
    #[error("verification of the Merkle proof failed")]
    InvalidProof,
    /// The provided metadata is itself invalid.
    #[error("the metadata is invalid")]
    InvalidMetadata,
    /// The recovery symbol cannot be used to recover the identified sliver,
    /// as it is not one of the symbols along the identified sliver's axis.
    #[error("the symbol is not one of the symbols along the identified sliver's axis")]
    SymbolNotUsable,
}

/// Errors that may be encountered while interacting with quilt.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum QuiltError {
    /// The blob is not found in the quilt.
    #[error("the blob is not found in the quilt: {0}")]
    BlobNotFoundInQuilt(String),
    /// The blob is not aligned with the quilt.
    #[error("the blob is not aligned with the quilt")]
    InvalidFormatNotAligned,
    /// Failed to extract the quilt index size.
    #[error("failed to extract the quilt index size")]
    FailedToExtractQuiltIndexSize,
    /// Failed to decode the quilt index.
    #[error("failed to decode the quilt index: {0}")]
    QuiltIndexDerSerError(String),
    /// Missing sliver.
    #[error("missing sliver: {0}")]
    MissingSliver(SliverIndex),
    /// Missing sliver range.
    #[error("missing sliver range: {0}..{1}")]
    MissingSliverRange(SliverIndex, SliverIndex),
    /// [`QuiltIndex`][crate::metadata::QuiltIndex] is missing.
    #[error("quilt index is missing")]
    MissingQuiltIndex,
    /// Too many blobs to fit in the quilt.
    #[error("too many blobs to fit in the quilt: {0} > max number of blobs: {1}")]
    TooManyBlobs(usize, usize),
    /// The quilt is too large.
    #[error("the quilt is too large: {0}")]
    QuiltOversize(String),
    /// Index is out of bounds.
    #[error("index is out of bounds: {0} > max index: {1}")]
    IndexOutOfBounds(usize, usize),
}

impl QuiltError {
    /// The blob is not found in the quilt.
    pub fn blob_not_found_in_quilt<T: Display>(blob_identifier: &T) -> Self {
        Self::BlobNotFoundInQuilt(blob_identifier.to_string())
    }
    /// The blob is not aligned with the quilt.
    pub fn invalid_format_not_aligned() -> Self {
        Self::InvalidFormatNotAligned
    }
    /// Failed to extract the quilt index size.
    pub fn failed_to_extract_quilt_index_size() -> Self {
        Self::FailedToExtractQuiltIndexSize
    }
    /// Failed to extract the quilt index.
    pub fn quilt_index_der_ser_error(reason: String) -> Self {
        Self::QuiltIndexDerSerError(reason)
    }
    /// [`QuiltIndex`][crate::metadata::QuiltIndex] is missing.
    pub fn missing_quilt_index() -> Self {
        Self::MissingQuiltIndex
    }
    /// Missing sliver.
    pub fn missing_sliver(sliver_index: SliverIndex) -> Self {
        Self::MissingSliver(sliver_index)
    }
    /// Missing sliver range.
    pub fn missing_sliver_range(begin: SliverIndex, end: SliverIndex) -> Self {
        Self::MissingSliverRange(begin, end)
    }
    /// Too many blobs to fit in the quilt.
    pub fn too_many_blobs(num_blobs: usize, max_blobs: usize) -> Self {
        Self::TooManyBlobs(num_blobs, max_blobs)
    }
    /// The quilt is too large.
    pub fn quilt_oversize(reason: String) -> Self {
        Self::QuiltOversize(reason)
    }
    /// Index is out of bounds.
    pub fn index_out_of_bounds(index: usize, max_index: usize) -> Self {
        Self::IndexOutOfBounds(index, max_index)
    }
}
