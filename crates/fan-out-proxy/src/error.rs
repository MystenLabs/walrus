// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Error type for the fan-out proxy.
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

use crate::tip::TipError;

/// Fan-out Proxy Errors.
#[derive(Debug, Error)]
pub enum FanOutError {
    /// The provided blob ID and the blob ID resulting from the blob encoding do not match.
    #[error("the provided blob ID and the blob ID resulting from the blob encoding do not match")]
    BlobIdMismatch,

    /// The provided blob digest and the blob digest resulting from the uploaded blob do not match.
    #[error(
        "the provided blob digest and the blob digest resulting from the uploaded blob do not match"
    )]
    BlobDigestMismatch,

    /// The provided auth package and the one found in the on-chain PTB input do not match.
    #[error("the provided auth package and the transaction's auth package hash do not match")]
    AuthPackageMismatch,

    /// BlobId was not registered in the given transaction.
    #[allow(unused)]
    #[error("blob_id {0} was not registered in the referenced transaction")]
    BlobIdNotRegistered(BlobId),

    /// The provided auth package hash is invalid.
    #[error("the provided auth package hash is invalid")]
    InvalidPtbAuthPackageHash,

    /// A Walrus client error occurred.
    #[error(transparent)]
    ClientError(#[from] ClientError),

    /// A Sui client error occurred.
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

impl FanOutError {
    /// Creates a new error of `Other` kind, from the given message.
    pub(crate) fn other(msg: &'static str) -> Self {
        Self::Other(anyhow::anyhow!(msg))
    }
}

// TODO: Implement this using the `RestApiError` proc macro when fixed.
impl IntoResponse for FanOutError {
    fn into_response(self) -> Response {
        match self {
            FanOutError::BlobIdMismatch => (
                StatusCode::BAD_REQUEST,
                FanOutError::BlobIdMismatch.to_string(),
            )
                .into_response(),
            FanOutError::ClientError(_) | FanOutError::SuiClientError(_) => {
                tracing::error!(error = ?self, "client error during fan out");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal client error").into_response()
            }
            FanOutError::DataTooLargeError(error) => {
                (StatusCode::BAD_REQUEST, error.to_string()).into_response()
            }
            FanOutError::BlobIdParseError(error) => {
                (StatusCode::BAD_REQUEST, error.to_string()).into_response()
            }
            FanOutError::TipError(
                error @ (TipError::NoTipSent | TipError::InsufficientTip { .. }),
            ) => (StatusCode::PAYMENT_REQUIRED, error.to_string()).into_response(),
            FanOutError::TipError(error) => {
                (StatusCode::BAD_REQUEST, error.to_string()).into_response()
            }
            FanOutError::Other(error) => {
                tracing::error!(?error, "unknown error during fan out");
                (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response()
            }
            FanOutError::AuthPackageMismatch
            | FanOutError::BlobDigestMismatch
            | FanOutError::BlobIdNotRegistered(_)
            | FanOutError::InvalidPtbAuthPackageHash => {
                tracing::error!(error = ?self, "failure relating to authentication of payload");
                (StatusCode::UNAUTHORIZED, self.to_string()).into_response()
            }
        }
    }
}
