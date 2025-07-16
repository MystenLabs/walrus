// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Error type for the Walrus Upload Relay.
//!
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use thiserror::Error;
use walrus_sdk::{
    core::{BlobId, BlobIdParseError, encoding::DataTooLargeError},
    error::ClientError,
    sui::client::SuiClientError,
};

use crate::tip::error::TipError;

/// Walrus Upload Relay error type.
#[derive(Debug, Error)]
pub enum WalrusUploadRelayError {
    /// The provided blob ID and the blob ID resulting from the blob encoding do not match.
    #[error("the provided blob ID and the blob ID resulting from the blob encoding do not match")]
    BlobIdMismatch,

    /// The provided blob digest and the blob digest resulting from the uploaded blob do not match.
    #[error(
        "the provided blob digest and the blob digest resulting from the uploaded blob do not match"
    )]
    BlobDigestMismatch,

    /// The provided blob length and the blob length resulting from the uploaded blob do not match.
    #[error(
        "the provided blob length and the blob length resulting from the uploaded blob do not match"
    )]
    BlobLengthMismatch,

    /// The query parameters are missing the transaction ID or the nonce, but the proxy requires
    /// them to check the tip payment.
    #[error(
        "The query parameters are missing the transaction ID or the nonce, but the proxy requires \
        them to check the tip payment."
    )]
    MissingTxIdOrNonce,

    /// BlobId was not registered in the given transaction.
    #[allow(unused)]
    #[error("blob_id {0} was not registered in the referenced transaction")]
    BlobIdNotRegistered(BlobId),

    /// The provided auth package hash is invalid.
    #[error("the provided nonce hash in the ptb is invalid")]
    InvalidNonceHash,

    /// The auth package is missing from the first input slot of the PTB.
    #[error("the auth package is missing from the first input slot of the PTB")]
    MissingAuthPackage,

    /// The auth package is missing from the first input slot of the PTB.
    #[error("the auth package was found but could not be read")]
    InvalidAuthPackage,

    /// The auth package is missing from the first input slot of the PTB.
    #[error("the tip transaction is not a ptb or could not be read")]
    InvalidTipTransaction,

    /// A Walrus client error occurred.
    #[error(transparent)]
    ClientError(#[from] ClientError),

    /// A Sui client error has occurred. Note that this is boxed to avoid the large size of the
    /// SuiClientError type affecting the size of the WalrusUploadRelayError type.
    #[error(transparent)]
    SuiClientError(#[from] Box<SuiClientError>),

    /// Blob is too large error.
    #[error(transparent)]
    DataTooLargeError(#[from] DataTooLargeError),

    /// Invalid BlobId error.
    #[error(transparent)]
    BlobIdParseError(#[from] BlobIdParseError),

    /// Error in processing the transaction or the tip
    #[error(transparent)]
    TipError(#[from] TipError),

    /// Internal server error.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl WalrusUploadRelayError {
    /// Creates a new error of `Other` kind, from the given message.
    pub(crate) fn other(msg: &'static str) -> Self {
        Self::Other(anyhow::anyhow!(msg))
    }
}

impl IntoResponse for WalrusUploadRelayError {
    fn into_response(self) -> Response {
        match self {
            WalrusUploadRelayError::TipError(
                error @ (TipError::NoTipSent | TipError::InsufficientTip { .. }),
            ) => (StatusCode::PAYMENT_REQUIRED, error.to_string()).into_response(),
            WalrusUploadRelayError::TipError(_)
            | WalrusUploadRelayError::MissingTxIdOrNonce
            | WalrusUploadRelayError::BlobIdMismatch
            | WalrusUploadRelayError::DataTooLargeError(_)
            | WalrusUploadRelayError::BlobIdParseError(_) => {
                (StatusCode::BAD_REQUEST, self.to_string()).into_response()
            }
            WalrusUploadRelayError::ClientError(_) | WalrusUploadRelayError::SuiClientError(_) => {
                tracing::error!(error = ?self, "client error during upload relay");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal client error").into_response()
            }
            WalrusUploadRelayError::Other(error) => {
                tracing::error!(?error, "unknown error during upload relay");
                (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response()
            }
            WalrusUploadRelayError::BlobDigestMismatch
            | WalrusUploadRelayError::BlobLengthMismatch
            | WalrusUploadRelayError::BlobIdNotRegistered(_)
            | WalrusUploadRelayError::InvalidNonceHash
            | WalrusUploadRelayError::InvalidAuthPackage
            | WalrusUploadRelayError::InvalidTipTransaction
            | WalrusUploadRelayError::MissingAuthPackage => {
                tracing::error!(error = ?self, "failure relating to authentication of payload");
                (StatusCode::UNAUTHORIZED, self.to_string()).into_response()
            }
        }
    }
}
