// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Errors that may be encountered while interacting with a storage node.

use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use walrus_core::Epoch;

use crate::tls::VerifierBuildError;

/// Error raised during communication with a node.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct NodeError {
    #[from]
    kind: Kind,
}

impl NodeError {
    /// Returns true if the error is related to connecting to the server.
    pub fn is_connect(&self) -> bool {
        let Kind::Reqwest(ref err) = self.kind else {
            return false;
        };
        err.is_connect()
    }

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

    /// Returns the reason for the error, if any.
    pub fn reason(&self) -> Option<ServiceErrorReason> {
        match &self.kind {
            Kind::StatusWithReason { reason, .. } => Some(*reason),
            _ => None,
        }
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
    #[error("{inner}: {message}. Detailed reason: {reason:?}")]
    StatusWithReason {
        inner: reqwest::Error,
        message: String,
        reason: ServiceErrorReason,
    },
    #[error(transparent)]
    Other(Box<dyn std::error::Error + Send + Sync>),
}

/// An error returned when building the client with a
/// [`ClientBuilder`][crate::client::ClientBuilder] has failed.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct ClientBuildError {
    #[from]
    kind: BuildErrorKind,
}

impl ClientBuildError {
    pub(crate) fn reqwest(err: reqwest::Error) -> Self {
        BuildErrorKind::Reqwest(err).into()
    }
}

/// Errors returned during the communication with a storage node.
#[derive(Debug, thiserror::Error)]
pub(crate) enum BuildErrorKind {
    #[error("unable to secure the client with TLS: {0}")]
    Tls(#[from] VerifierBuildError),
    #[error("invalid storage node authority")]
    InvalidHostOrPort,
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    #[error("unable to load trusted certificates from the OS: {0}")]
    FailedToLoadCerts(#[from] std::io::Error),
}

/// Defines a more detailed server side reason that can be returned with an error.
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum ServiceErrorReason {
    /// The requested epoch is invalid because it is too old.
    InvalidEpochTooOld(Epoch),
    /// The requested epoch is invalid because it is too new.
    InvalidEpochTooNew(Epoch),
}
