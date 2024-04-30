// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::num::NonZeroU16;

use thiserror::Error;

/// Error returned when encoding/decoding is impossible due to the given data size.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum InvalidDataSizeError {
    /// The data is too large to be encoded/decoded.
    #[error("the data is to large")]
    DataTooLarge,
    /// The data to be encoded/decoded is empty.
    #[error("the data is empty")]
    EmptyData,
}

/// Error type returned when encoding fails.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum EncodeError {
    /// The data size is invalid for this encoder.
    #[error(transparent)]
    InvalidDataSize(#[from] InvalidDataSizeError),
    /// The data is not properly aligned; i.e., it is not a multiple of the symbol size or symbol
    /// count.
    #[error("the data is not properly aligned (must be a multiple of {0})")]
    MisalignedData(NonZeroU16),
}

/// Error type returned when sliver recovery fails.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum RecoveryError {
    /// The symbols provided are empty or have different sizes.
    #[error("the symbols provided are empty or have different sizes")]
    InvalidSymbolSizes,
    /// The index of the recovery symbol can be at most `n_shards`
    #[error("the index of the recovery symbol can be at most `n_shards`")]
    IndexTooLarge,
    /// The underlying [`Encoder`][super::Encoder] returned an error.
    #[error(transparent)]
    EncodeError(#[from] EncodeError),
}

impl From<InvalidDataSizeError> for RecoveryError {
    fn from(value: InvalidDataSizeError) -> Self {
        EncodeError::from(value).into()
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
/// [`Sliver`][super::Sliver] from it.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
#[error("cannot convert the `Sliver` to the sliver variant requested")]
pub struct WrongSliverVariantError;

/// Error returned when sliver verification fails.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
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
    /// Error resulting from the Merkle tree computation. The Merkle root could not be computed.
    #[error(transparent)]
    RecoveryFailed(#[from] RecoveryError),
}
