// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Errors that may be encountered while interacting with a storage node.

use reqwest::StatusCode;
use walrus_core::Epoch;

use crate::{
    api::errors::{STORAGE_NODE_ERROR_DOMAIN, Status},
    tls::VerifierBuildError,
};

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

    /// Returns true if the error is related to the request.
    ///
    /// This includes all networking related errors.
    pub fn is_reqwest(&self) -> bool {
        matches!(self.kind, Kind::Reqwest(_))
    }

    /// Returns the HTTP error status code associated with the error, if any.
    pub fn http_status_code(&self) -> Option<StatusCode> {
        if let Kind::Reqwest(inner) | Kind::Status { inner, .. } = &self.kind {
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

    /// Returns true if the error is due to the blob being blocked.
    pub fn is_blob_blocked(&self) -> bool {
        // TODO(jsmith): use a constant shared between client and server.
        self.status()
            .map(|status| status.is_for_reason("FORBIDDEN_BLOB", STORAGE_NODE_ERROR_DOMAIN))
            .unwrap_or(false)
    }

    /// Returns true if the error is due to the shard not being assigned to the storage node.
    pub fn is_shard_not_assigned(&self) -> bool {
        // TODO(jsmith): use a constant shared between client and server.
        self.status()
            .map(|status| status.is_for_reason("SHARD_NOT_ASSIGNED", STORAGE_NODE_ERROR_DOMAIN))
            .unwrap_or(false)
    }

    /// Returns true if the error was caused by the pending upload cache being full.
    pub fn is_cache_saturated(&self) -> bool {
        self.has_reason("CACHE_SATURATED")
    }

    /// Returns true if the error indicates the blob is not yet registered.
    pub fn is_not_registered(&self) -> bool {
        self.has_reason("NOT_REGISTERED")
    }

    /// Returns true if the HTTP error status code associated with number 499
    pub fn is_expired(&self) -> bool {
        Some(StatusCode::from_u16(499).expect("status code is in a valid range"))
            == self.http_status_code()
    }

    /// Returns true if the HTTP error status code associated with the error is
    /// [`StatusCode::UNAUTHORIZED`].
    pub fn is_unauthorized(&self) -> bool {
        Some(StatusCode::UNAUTHORIZED) == self.http_status_code()
    }

    /// Returns true if the HTTP error status code associated with the error is
    /// [`StatusCode::BAD_REQUEST`].
    pub fn is_user_error(&self) -> bool {
        Some(StatusCode::BAD_REQUEST) == self.http_status_code()
    }

    /// Returns true if the HTTP error status code associated with the error is
    /// [`StatusCode::SERVICE_UNAVAILABLE`].
    pub fn is_crypto_error(&self) -> bool {
        Some(StatusCode::SERVICE_UNAVAILABLE) == self.http_status_code()
    }

    /// Returns true if the HTTP error status code associated with the error is
    /// [`StatusCode::INTERNAL_SERVER_ERROR`].
    pub fn is_unknown_issue(&self) -> bool {
        Some(StatusCode::INTERNAL_SERVER_ERROR) == self.http_status_code()
    }

    /// Wrap a standard error as a Node error.
    pub fn other<E>(err: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Kind::Other(err.into()).into()
    }

    pub(crate) fn reqwest(err: reqwest::Error) -> Self {
        Kind::Reqwest(err).into()
    }

    /// Returns the reason for the error, if any.
    pub fn service_error(&self) -> Option<ServiceError> {
        if let Kind::Status { ref status, .. } = self.kind {
            ServiceError::try_from(status).ok()
        } else {
            None
        }
    }

    /// Returns the error status provided by the server.
    ///
    /// If the server responded to the request with an error, the response should
    /// contain an error status.
    // TODO(jsmith): Make this always true by formatting all axum errors correctly at the server.
    pub fn status(&self) -> Option<&Status> {
        if let Kind::Status { ref status, .. } = self.kind {
            Some(status)
        } else {
            None
        }
    }

    fn has_reason(&self, reason: &str) -> bool {
        self.status()
            .map(|status| status.is_for_reason(reason, STORAGE_NODE_ERROR_DOMAIN))
            .unwrap_or(false)
    }
}

/// Errors returned during the communication with a storage node.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Kind {
    #[error("failed to decode the response body as BCS")]
    Bcs(#[from] bcs::Error),
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    #[error("{inner}: {status}")]
    Status {
        inner: reqwest::Error,
        status: Status,
    },
    #[error("node returned an error in a non-error response {0}")]
    ErrorInNonErrorMessage(Status),
    #[error("invalid content type in response")]
    InvalidContentType,
    #[error(transparent)]
    Other(Box<dyn std::error::Error + Send + Sync>),
}

/// An error returned when building the client with a
/// [`StorageNodeClientBuilder`][crate::StorageNodeClientBuilder] has failed.
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
    #[error("unable to load trusted certificates from the OS: {0:?}")]
    FailedToLoadCerts(Vec<rustls_native_certs::Error>),
}

/// Defines a more detailed server side error that can be returned to the client.
#[derive(Debug, Clone)]
pub enum ServiceError {
    /// The requested epoch is invalid.
    InvalidEpoch {
        /// The epoch client is in.
        request_epoch: Epoch,
        /// The epoch server is in.
        server_epoch: Epoch,
    },
    /// The request is unauthorized.
    RequestUnauthorized,
}

/// Returning the status as a service error is unsupported.
#[derive(Debug, Clone, thiserror::Error)]
#[error("the status does not represent an implemented service error")]
pub struct UnsupportedErrorStatus;

impl TryFrom<&Status> for ServiceError {
    type Error = UnsupportedErrorStatus;

    fn try_from(status: &Status) -> Result<Self, Self::Error> {
        let info = status.error_info().ok_or(UnsupportedErrorStatus)?;

        match (info.reason(), info.domain()) {
            ("INVALID_EPOCH", STORAGE_NODE_ERROR_DOMAIN) => info
                .field::<Epoch>("request_epoch")
                .zip(info.field::<Epoch>("server_epoch"))
                .map(|(request_epoch, server_epoch)| ServiceError::InvalidEpoch {
                    request_epoch,
                    server_epoch,
                })
                .ok_or(UnsupportedErrorStatus),
            ("REQUEST_UNAUTHORIZED", STORAGE_NODE_ERROR_DOMAIN) => {
                Ok(ServiceError::RequestUnauthorized)
            }
            _ => Err(UnsupportedErrorStatus),
        }
    }
}

/// Private errors for the `list_and_verify_recovery_symbols` endpoint that may lead to a
/// `NodeError`.
#[derive(Debug, Clone, thiserror::Error)]
pub(crate) enum ListAndVerifyRecoverySymbolsError {
    #[error("the server returned an empty list of symbols")]
    EmptyResponse,
    #[error("the background task verifying symbols failed")]
    BackgroundWorkerFailed,
}
