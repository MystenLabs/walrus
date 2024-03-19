// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use thiserror::Error;

/// Error returned when the data is too large to be encoded with this encoder.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("the data is too large to be encoded")]
pub struct DataTooLargeError;

/// Error type returned when encoding fails.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum EncodeError {
    /// The data is too large to be encoded with this encoder.
    #[error("the data is to large to be encoded (max size: {0})")]
    DataTooLarge(usize),
    /// The data to be encoded is empty.
    #[error("empty data cannot be encoded")]
    EmptyData,
    /// The data is not properly aligned; i.e., it is not a multiple of the symbol size or symbol
    /// count.
    #[error("the data is not properly aligned (must be a multiple of {0})")]
    MisalignedData(u16),
}

/// Error type returned when sliver recovery fails.
#[derive(Debug, Error, PartialEq, Eq)]
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
    /// The underlying [`Decoder`][super::Decoder] returned an error.
    #[error(transparent)]
    DataTooLarge(#[from] DataTooLargeError),
}

/// Error returned when the size of input symbols does not match the size of existing symbols.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("the size of the symbols provided does not match the size of the existing symbols")]
pub struct WrongSymbolSizeError;

/// Error returned when the verification of a reconstructed blob fails. Verification failure occurs
/// when the provided blob ID does not match the blob ID computed from the reconstructed blob.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("decoding verification failed because the blob ID does not match the provided metadata")]
pub struct DecodingVerificationError;
