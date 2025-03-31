// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Checkpoint downloader configuration.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DurationMilliSeconds, DurationSeconds};
use sui_types::messages_checkpoint::TrustedCheckpoint;
use typed_store::rocks::DBMap;
use walrus_sui::client::retry_client::RetriableRpcClient;

use crate::metrics::AdaptiveDownloaderMetrics;

/// Fetcher configuration options for the parallel checkpoint fetcher.
#[serde_as]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct ParallelDownloaderConfig {
    /// Number of retries per checkpoint before giving up.
    pub min_retries: u32,
    /// Initial delay before the first retry.
    #[serde_as(as = "DurationMilliSeconds")]
    #[serde(rename = "initial_delay_millis")]
    pub initial_delay: Duration,
    /// Maximum delay between retries. Once this delay is reached, the
    /// fetcher will keep retrying with this duration.
    #[serde_as(as = "DurationMilliSeconds")]
    #[serde(rename = "max_delay_millis")]
    pub max_delay: Duration,
}

impl Default for ParallelDownloaderConfig {
    fn default() -> Self {
        // Checkpoint downloader configuration tuned for:
        // - Checkpoint generation rate: 1 per 200ms (5 per second)
        // - Network RTT: ~100-300ms
        //
        // Retry timing example with 100ms RTT:
        // T+0ms:   Client requests checkpoint N
        // T+100ms: Client learns checkpoint N unavailable
        // T+200ms: Server generates checkpoint N
        // T+250ms: Client retries after retry_delay
        // T+350ms: Client gets response which should contain checkpoint N.

        // Retry timing example with 300ms RTT:
        // T+0ms:   Client requests checkpoint N
        // T+200ms: Server generates checkpoint N
        // T+300ms: Client learns checkpoint N unavailable
        // T+450ms: Client retries after retry_delay
        // T+750ms: Client gets response which should contain checkpoint N.
        // The retry_delay of 150ms is chosen to balance in steady state with
        // fully caught up client:
        // - Not retrying too quickly (which could waste network resources)
        // - Not waiting too long (which would cause us to fall behind)
        // TODO: Use adaptive retry delay based on RTT and error type (#1123)
        Self {
            min_retries: 10,
            initial_delay: Duration::from_millis(150),
            max_delay: Duration::from_secs(2),
        }
    }
}

/// Configuration for the channel.
///
/// This is used to configure the size of the channel buffers for the worker pool.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ChannelConfig {
    /// Additional buffer factor for work queue
    /// Helps keep the worker busy in case of delays
    /// in draining the checkpoint queue.
    pub work_queue_buffer_factor: usize,
    /// Buffer factor for result queue
    /// Helps handle delays in processing the results.
    pub result_queue_buffer_factor: usize,
}

impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            work_queue_buffer_factor: 3,
            result_queue_buffer_factor: 3,
        }
    }
}

/// Configuration for the adaptive worker pool.
#[serde_as]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct AdaptiveDownloaderConfig {
    /// Minimum number of workers.
    pub min_workers: usize,
    /// Maximum number of workers.
    pub max_workers: usize,
    /// Initial number of workers.
    pub initial_workers: usize,
    /// Checkpoint lag threshold for scaling up.
    pub scale_up_lag_threshold: u64,
    /// Checkpoint lag threshold for scaling down.
    pub scale_down_lag_threshold: u64,
    /// Minimum time between scaling operations.
    #[serde(rename = "scale_cooldown_secs")]
    #[serde_as(as = "DurationSeconds")]
    pub scale_cooldown: Duration,
    /// Base configuration.
    pub base_config: ParallelDownloaderConfig,
    /// Channel configuration.
    pub channel_config: ChannelConfig,
}

impl Default for AdaptiveDownloaderConfig {
    fn default() -> Self {
        Self {
            min_workers: 1,
            max_workers: 10,
            initial_workers: 5,
            scale_up_lag_threshold: 100,
            scale_down_lag_threshold: 10,
            scale_cooldown: Duration::from_secs(10),
            base_config: ParallelDownloaderConfig::default(),
            channel_config: ChannelConfig::default(),
        }
    }
}

impl AdaptiveDownloaderConfig {
    pub(crate) fn message_queue_size(&self) -> usize {
        self.max_workers * self.channel_config.work_queue_buffer_factor
    }

    pub(crate) fn checkpoint_queue_size(&self) -> usize {
        self.max_workers * self.channel_config.work_queue_buffer_factor
    }

    pub(crate) fn result_queue_size(&self) -> usize {
        self.max_workers
            * self.channel_config.work_queue_buffer_factor
            * self.channel_config.result_queue_buffer_factor
    }
}

/// Configuration for the pool monitor.
#[derive(Clone)]
pub(crate) struct PoolMonitorConfig {
    pub(crate) downloader_config: AdaptiveDownloaderConfig,
    pub(crate) checkpoint_store: DBMap<(), TrustedCheckpoint>,
    pub(crate) client: RetriableRpcClient,
    pub(crate) metrics: AdaptiveDownloaderMetrics,
}
