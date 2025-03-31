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
}

impl RpcFallbackConfig {
    fn default_quick_retry_config() -> ExponentialBackoffConfig {
        ExponentialBackoffConfig {
            min_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_millis(300),
            max_retries: None,
        }
    }
}

/// Command line arguments for the RPC fallback configuration.
#[derive(Debug, Clone, clap::Args)]
pub struct RpcFallbackConfigArgs {
    /// The fallback checkpoint bucket URL.
    #[clap(long)]
    pub checkpoint_bucket: Option<reqwest::Url>,

    /// The minimum backoff interval in milliseconds
    /// to retry the fullnode RPC before falling back
    /// to the checkpoint bucket when available.
    #[clap(long)]
    pub min_backoff: Option<u64>,

    /// The maximum backoff interval in milliseconds
    /// to retry the fullnode RPC before falling back
    /// to the checkpoint bucket when available.
    #[clap(long)]
    pub max_backoff: Option<u64>,

    /// The maximum number of retries
    /// to retry the fullnode RPC before falling back
    /// to the checkpoint bucket when available.
    #[clap(long)]
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
            }
        })
    }
}
