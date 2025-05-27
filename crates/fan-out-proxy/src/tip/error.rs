// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Errors for the tipping system.

use thiserror;
use walrus_core::BlobId;
use walrus_sui::client::SuiClientError;

/// An error that occurs while the proxy is executing or checking a transaction.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TipError {
    /// The received transaction response does not match what is expected.
    #[error("an unexpected transaction response was received: {0}")]
    UnexpectedResponse(String),
    /// The provided tip is insufficient.
    #[error("the tip is insufficient: balance change {0}; expected {1}")]
    InsufficientTip(i128, i128),
    /// There was no transfer to the proxy account.
    #[error("no tip was transferred to the proxy account")]
    NoTipSent,
    /// The blob ID of the blob that was sent was not registered.
    #[error("the blob ID ({0}) for the data provided was not registered")]
    BlobIdNotRegistered(BlobId),
    /// The encoded blob length cannot be computed.
    #[error("the encoded blob length cannot be computed")]
    EncodedBlobLengthFailed,
    /// Error in the Sui client.
    #[error(transparent)]
    SuiClient(#[from] SuiClientError),
}
