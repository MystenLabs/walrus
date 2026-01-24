// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! The `retriable_rpc_error` module defines the `RetriableRpcError` trait and its implementations.
//! This trait is used to determine if an error is a retriable RPC error.
use std::fmt::Debug;

use super::{super::SuiClientError, CheckpointRpcError, FallbackError, RetriableClientError};

/// The list of HTTP status codes that are retriable.
const RETRIABLE_RPC_ERRORS: &[&str] = &["408", "429", "500", "502", "503", "504"];
/// The list of gRPC status codes that are retriable.
const RETRIABLE_GRPC_ERRORS: &[tonic::Code] = &[
    tonic::Code::ResourceExhausted,
    tonic::Code::Internal,
    tonic::Code::Unavailable,
    tonic::Code::DeadlineExceeded,
];

/// Trait to test if an error is produced by a temporary RPC failure and can be retried.
pub trait RetriableRpcError: Debug {
    /// Returns `true` if the error is a retriable network error.
    fn is_retriable_rpc_error(&self) -> bool;
}

impl RetriableRpcError for anyhow::Error {
    fn is_retriable_rpc_error(&self) -> bool {
        if let Some(error) = self.downcast_ref::<sui_sdk::error::Error>() {
            error.is_retriable_rpc_error()
        } else if let Some(sui_client_error) = self.downcast_ref::<SuiClientError>() {
            sui_client_error.is_retriable_rpc_error()
        } else if let Some(tonic_status) = self.downcast_ref::<tonic::Status>() {
            tonic_status.is_retriable_rpc_error()
        } else {
            false
        }
    }
}

impl RetriableRpcError for sui_sdk::error::Error {
    fn is_retriable_rpc_error(&self) -> bool {
        if let sui_sdk::error::Error::RpcError(rpc_error) = self {
            match rpc_error {
                jsonrpsee::core::ClientError::RequestTimeout => {
                    return true;
                }
                jsonrpsee::core::ClientError::Transport(_) => {
                    return true;
                }
                _ => {
                    let error_string = rpc_error.to_string();
                    if RETRIABLE_RPC_ERRORS
                        .iter()
                        .any(|&s| error_string.contains(s))
                    {
                        return true;
                    }
                }
            }
        }
        false
    }
}

impl RetriableRpcError for SuiClientError {
    fn is_retriable_rpc_error(&self) -> bool {
        match self {
            SuiClientError::SuiSdkError(error) => error.is_retriable_rpc_error(),
            SuiClientError::GrpcError(status) => status.is_retriable_rpc_error(),
            SuiClientError::Internal(error) => error.is_retriable_rpc_error(),
            _ => false,
        }
    }
}

impl RetriableRpcError for tonic::Status {
    fn is_retriable_rpc_error(&self) -> bool {
        RETRIABLE_GRPC_ERRORS.contains(&self.code())
    }
}

impl RetriableRpcError for CheckpointRpcError {
    fn is_retriable_rpc_error(&self) -> bool {
        if self.status.code() == tonic::Code::Internal {
            // Only retry if the error is not due to missing events.
            return !self.status.message().contains("missing event");
        }

        // If the checkpoint is not produced, this is retryable.
        if self.is_checkpoint_not_produced() {
            return true;
        }

        self.status.is_retriable_rpc_error()
    }
}

impl RetriableRpcError for FallbackError {
    fn is_retriable_rpc_error(&self) -> bool {
        match self {
            FallbackError::RequestFailed(error) => error
                .status()
                .map(|status| status.is_server_error() || status.as_u16() == 429)
                .unwrap_or(false),
            _ => false,
        }
    }
}

impl RetriableRpcError for RetriableClientError {
    fn is_retriable_rpc_error(&self) -> bool {
        match self {
            Self::RpcError(rpc_error) => rpc_error.is_retriable_rpc_error(),
            Self::RetryableTimeoutError => true,
            Self::NonRetryableTimeoutError => false,
            Self::FallbackError(fallback_error) => fallback_error.is_retriable_rpc_error(),
            Self::FailoverError(_) => false,
            Self::Other(_) => false,
        }
    }
}
