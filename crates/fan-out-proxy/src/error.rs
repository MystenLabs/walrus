// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Error type for the fan-out proxy.
//!
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use thiserror::Error;
use walrus_core::{BlobIdParseError, encoding::DataTooLargeError};
use walrus_sdk::error::ClientError;

use crate::tip::TipError;

/// Fan-out Proxy Errors.
#[derive(Debug, Error)]
pub(crate) enum FanOutError {
    /// The provided  blob ID and the blob ID resulting from the blob encoding do not match.
    #[error("the provided  blob ID and the blob ID resulting from the blob encoding do not match")]
    BlobIdMismatch,

    /// A Walrus client error occurred.
    #[error(transparent)]
    ClientError(#[from] ClientError),

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

// TODO: Implement this using the `RestApiError` proc macro when fixed.
impl IntoResponse for FanOutError {
    fn into_response(self) -> Response {
        match self {
            FanOutError::BlobIdMismatch => (
                StatusCode::BAD_REQUEST,
                FanOutError::BlobIdMismatch.to_string(),
            )
                .into_response(),
            FanOutError::ClientError(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "internal client error").into_response()
            }
            FanOutError::DataTooLargeError(error) => {
                (StatusCode::BAD_REQUEST, error.to_string()).into_response()
            }
            FanOutError::BlobIdParseError(error) => {
                (StatusCode::BAD_REQUEST, error.to_string()).into_response()
            }
            FanOutError::TipError(error) => {
                (StatusCode::BAD_REQUEST, error.to_string()).into_response()
            }
            FanOutError::Other(error) => {
                (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response()
            }
        }
    }
}
