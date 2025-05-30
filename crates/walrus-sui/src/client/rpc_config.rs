// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Configuration for the RPC client.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use walrus_utils::backoff::ExponentialBackoffConfig;

/// Configuration for the RPC endpoint fallback.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RpcFallbackConfig {
    /// The endpoint of the checkpoint bucket that will be
    /// used to download the checkpoint if the RPC endpoint
    /// is not available.
    pub checkpoint_bucket: reqwest::Url,
    /// The configuration for the backoff strategy for the RPC
    /// endpoint before falling back to the checkpoint bucket.
    #[serde(default = "RpcFallbackConfig::default_quick_retry_config")]
    pub quick_retry_config: ExponentialBackoffConfig,

    /// The minimum number of failures to start the fallback.
    #[serde(default = "RpcFallbackConfig::default_min_failures_to_start_fallback")]
    pub min_failures_to_start_fallback: usize,

    /// The duration of the failure window to start the fallback.
    #[serde(default = "RpcFallbackConfig::default_failure_window_to_start_fallback_duration")]
    pub failure_window_to_start_fallback_duration: Duration,

    /// The duration to skip the RPC for checkpoint download if fallback is configured.
    #[serde(default = "RpcFallbackConfig::default_skip_rpc_for_checkpoint_duration")]
    pub skip_rpc_for_checkpoint_duration: Duration,

    /// The maximum number of consecutive failures to tolerate for a single checkpoint
    /// before falling back to the checkpoint bucket.
    #[serde(default = "RpcFallbackConfig::default_max_consecutive_failures")]
    pub max_consecutive_failures: usize,
}

impl RpcFallbackConfig {
    fn default_quick_retry_config() -> ExponentialBackoffConfig {
        ExponentialBackoffConfig {
            min_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_millis(300),
            max_retries: None,
        }
    }

    fn default_min_failures_to_start_fallback() -> usize {
        10
    }

    fn default_failure_window_to_start_fallback_duration() -> Duration {
        Duration::from_secs(300)
    }

    fn default_skip_rpc_for_checkpoint_duration() -> Duration {
        Duration::from_secs(300)
    }

    fn default_max_consecutive_failures() -> usize {
        10
    }
}

/// Command line arguments for the RPC fallback configuration.
#[derive(Debug, Clone, clap::Args)]
pub struct RpcFallbackConfigArgs {
    /// The fallback checkpoint bucket URL.
    #[arg(long)]
    pub checkpoint_bucket: Option<reqwest::Url>,

    /// The minimum backoff interval in milliseconds
    /// to retry the fullnode RPC before falling back
    /// to the checkpoint bucket when available.
    #[arg(long)]
    pub min_backoff: Option<u64>,

    /// The maximum backoff interval in milliseconds
    /// to retry the fullnode RPC before falling back
    /// to the checkpoint bucket when available.
    #[arg(long)]
    pub max_backoff: Option<u64>,

    /// The maximum number of retries
    /// to retry the fullnode RPC before falling back
    /// to the checkpoint bucket when available.
    #[arg(long)]
    pub max_retries: Option<u32>,
}

impl RpcFallbackConfigArgs {
    /// Converts the command line arguments to a [`RpcFallbackConfig`].
    pub fn to_config(&self) -> Option<RpcFallbackConfig> {
        self.checkpoint_bucket.as_ref().map(|url| {
            let backoff = ExponentialBackoffConfig {
                min_backoff: self
                    .min_backoff
                    .map(Duration::from_millis)
                    .unwrap_or(Duration::from_millis(100)),
                max_backoff: self
                    .max_backoff
                    .map(Duration::from_millis)
                    .unwrap_or(Duration::from_millis(300)),
                max_retries: self.max_retries,
            };
            RpcFallbackConfig {
                checkpoint_bucket: url.clone(),
                quick_retry_config: backoff,
                min_failures_to_start_fallback:
                    RpcFallbackConfig::default_min_failures_to_start_fallback(),
                failure_window_to_start_fallback_duration:
                    RpcFallbackConfig::default_failure_window_to_start_fallback_duration(),
                skip_rpc_for_checkpoint_duration:
                    RpcFallbackConfig::default_skip_rpc_for_checkpoint_duration(),
                max_consecutive_failures: RpcFallbackConfig::default_max_consecutive_failures(),
            }
        })
    }
}
