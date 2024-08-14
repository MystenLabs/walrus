// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Errors that may be encountered while interacting with a storage node.

use reqwest::StatusCode;

/// Error raised during communication with a node.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct NodeError {
    #[from]
    kind: Kind,
}

impl NodeError {
    /// Returns the HTTP error status code associated with the error, if any.
    pub fn http_status_code(&self) -> Option<StatusCode> {
        if let Kind::Reqwest(inner) | Kind::StatusWithMessage { inner, .. } = &self.kind {
            inner.status()
        } else {
            None
        }
    }

    /// Returns true if the HTTP error status code associated with the error is
    /// [`StatusCode::NOT_FOUND`].
    pub fn is_status_not_found(&self) -> bool {
        Some(StatusCode::NOT_FOUND) == self.http_status_code()
    }

    /// Returns true if the HTTP error status code associated with the error is
    /// [`StatusCode::MISDIRECTED_REQUEST`].
    pub fn is_shard_not_assigned(&self) -> bool {
        self.http_status_code() == Some(StatusCode::MISDIRECTED_REQUEST)
    }

    pub(crate) fn other<E>(err: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Kind::Other(err.into()).into()
    }

    pub(crate) fn reqwest(err: reqwest::Error) -> Self {
        Kind::Reqwest(err).into()
    }
}

/// Errors returned during the communication with a storage node.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Kind {
    #[error("failed to decode the response body as BCS")]
    Bcs(#[from] bcs::Error),
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    #[error("{inner}: {message}")]
    StatusWithMessage {
        inner: reqwest::Error,
        message: String,
    },
    #[error("node returned an error in a non-error response {code}: {message}")]
    ErrorInNonErrorMessage { code: u16, message: String },
    #[error("invalid content type in response")]
    InvalidContentType,
    #[error(transparent)]
    Other(Box<dyn std::error::Error + Send + Sync>),
}
