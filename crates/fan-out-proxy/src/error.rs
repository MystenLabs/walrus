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

/// Fan-out Proxy Errors
#[derive(Debug, Error)]
pub(crate) enum FanOutError {
    /// Invalid input error.
    #[error("Bad input: {0}")]
    BadRequest(String),

    /// A Sui ClientError occurred.
    #[error(transparent)]
    ClientError(#[from] ClientError),

    /// Blob is too large error.
    #[error(transparent)]
    DataTooLargeError(#[from] DataTooLargeError),

    /// Invalid BlobId error.
    #[error(transparent)]
    BlobIdParseError(#[from] BlobIdParseError),

    /// Internal server error.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl IntoResponse for FanOutError {
    fn into_response(self) -> Response {
        match self {
            FanOutError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg).into_response(),
            FanOutError::ClientError(error) => {
                // REVIEW: Unclear immediately whether this is really a 400, but for now let's treat
                // it as such.
                (StatusCode::BAD_REQUEST, error.to_string()).into_response()
            }
            FanOutError::DataTooLargeError(error) => {
                (StatusCode::BAD_REQUEST, error.to_string()).into_response()
            }
            FanOutError::BlobIdParseError(error) => {
                (StatusCode::BAD_REQUEST, error.to_string()).into_response()
            }
            FanOutError::Other(error) => {
                (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response()
            }
        }
    }
}
