// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Implements a fallback client for downloading checkpoints from a remote server.
use std::{
    fmt::Debug,
    time::{Duration, Instant},
};

use reqwest::Url;
use sui_storage::blob::Blob;
use sui_types::full_checkpoint_content::CheckpointData;
use thiserror::Error;
use url::ParseError;

use super::RetriableClientError;

/// Error type for checkpoint download errors.
#[derive(Error, Debug)]
pub enum FallbackError {
    /// Failed to construct URL.
    #[error("failed to construct URL: {0}")]
    UrlConstruction(#[from] ParseError),

    /// HTTP request failed.
    #[error("HTTP request failed: {0}")]
    RequestFailed(#[from] reqwest::Error),

    /// Failed to deserialize checkpoint data.
    #[error("failed to deserialize checkpoint data: {0}")]
    DeserializationError(String),
}

/// A client for downloading checkpoint data from a remote server.
#[derive(Clone, Debug)]
pub(crate) struct FallbackClient {
    client: reqwest::Client,
    base_url: Url,
    skip_rpc_for_checkpoint_duration: Duration,
    min_failures_to_start_fallback: usize,
    failure_window_to_start_fallback_duration: Duration,
}

impl FallbackClient {
    /// Creates a new fallback client.
    pub fn new(
        base_url: Url,
        timeout: Duration,
        skip_rpc_for_checkpoint_duration: Duration,
        min_failures_to_start_fallback: usize,
        failure_window_to_start_fallback_duration: Duration,
    ) -> Self {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .expect("should be able to build reqwest client");
        Self {
            client,
            base_url,
            skip_rpc_for_checkpoint_duration,
            min_failures_to_start_fallback,
            failure_window_to_start_fallback_duration,
        }
    }

    /// Downloads a checkpoint from the remote server.
    pub async fn get_full_checkpoint(
        &self,
        sequence_number: u64,
    ) -> Result<CheckpointData, FallbackError> {
        let url = self.base_url.join(&format!("{}.chk", sequence_number))?;
        tracing::debug!(%url, "downloading checkpoint from fallback bucket");
        let response = self.client.get(url).send().await?.error_for_status()?;
        let bytes = response.bytes().await?;
        let checkpoint = Blob::from_bytes::<CheckpointData>(&bytes)
            .map_err(|e| FallbackError::DeserializationError(e.to_string()))?;
        tracing::debug!(sequence_number, "checkpoint download successful");
        Ok(checkpoint)
    }

    pub fn skip_rpc_for_checkpoint_duration(&self) -> Duration {
        self.skip_rpc_for_checkpoint_duration
    }

    /// Returns `true` if
    ///   - the error indicates that a immediate fallback is needed, or
    ///   - the failure window has been exceeded.
    pub fn is_eligible_for_fallback(
        &self,
        next_checkpoint: u64,
        error: &RetriableClientError,
        last_success: Instant,
        num_failures: usize,
    ) -> bool {
        if error.is_eligible_for_fallback_immediately(next_checkpoint) {
            return true;
        }

        self.is_failure_window_exceeded(last_success, num_failures)
    }

    /// Returns `true` if the failure window has been exceeded. Failure window is exceeded if
    /// the number of failures exceeds `self.min_failures_to_start_fallback` and if the last
    /// successful RPC call was more than `self.failure_window_to_start_fallback_duration` minutes
    /// ago.
    pub fn is_failure_window_exceeded(&self, last_success: Instant, num_failures: usize) -> bool {
        last_success.elapsed() > self.failure_window_to_start_fallback_duration
            && num_failures > self.min_failures_to_start_fallback
    }
}
