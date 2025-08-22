// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Configuration module for event processor.

use std::{fmt, path::PathBuf, time::Duration};

use checkpoint_downloader::AdaptiveDownloaderConfig;
use serde::{Deserialize, Serialize};
use serde_with::{DurationSeconds, serde_as};
use sui_types::base_types::ObjectID;
use walrus_sui::client::{
    retry_client::{RetriableRpcClient, RetriableSuiClient},
    rpc_config::RpcFallbackConfig,
};

use crate::node::DatabaseConfig;

/// The arguments for the authenticated events service.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventServiceConfig {
    /// The host name or public IP address of the node. This is used in the on-chain data and for
    /// generating self-signed certificates if TLS is enabled.
    pub public_host: String,
    /// The port on which the storage node will serve requests.
    pub public_port: u16,
    /// The path where the Walrus database will be stored.
    pub storage_path: PathBuf,
    /// The Sui RPC URL to use for catchup.
    pub sui_rpc_urls: Vec<String>,
    /// The timeout for each request to the Sui RPC node.
    pub checkpoint_request_timeout: Duration,
    /// The config for RPC fallback.
    pub rpc_fallback_config_args: Option<RpcFallbackConfig>,
    /// The interval at which to poll for new events.
    pub event_polling_interval: Duration,
    /// The maximum number of events to process in a single batch.
    pub max_events_per_batch: usize,
}

/// Struct to group system-related parameters.
#[derive(Debug, Clone)]
pub struct SystemConfig {
    /// The package ID of the system package.
    pub system_pkg_id: ObjectID,
    /// The object ID of the system object.
    pub system_object_id: ObjectID,
    /// The object ID of the staking object.
    pub staking_object_id: ObjectID,
}

impl SystemConfig {
    /// Creates a new instance of the system configuration.
    pub fn new(
        system_pkg_id: ObjectID,
        system_object_id: ObjectID,
        staking_object_id: ObjectID,
    ) -> Self {
        Self {
            system_pkg_id,
            system_object_id,
            staking_object_id,
        }
    }
}

/// Struct to group general configuration parameters.
#[derive(Debug, Clone)]
pub struct EventProcessorRuntimeConfig {
    /// The address of the RPC server.
    pub rpc_addresses: Vec<String>,
    /// The event polling interval.
    pub event_polling_interval: Duration,
    /// The path to the database.
    pub db_path: PathBuf,
    /// The path to the rpc fallback config.
    pub rpc_fallback_config: Option<RpcFallbackConfig>,
    /// The database config.
    pub db_config: DatabaseConfig,
}

/// Configuration for event processing.
#[serde_as]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct EventProcessorConfig {
    /// Event pruning interval.
    #[serde_as(as = "DurationSeconds")]
    #[serde(rename = "pruning_interval_secs")]
    pub pruning_interval: Duration,
    /// The timeout for the RPC client.
    #[serde_as(as = "DurationSeconds")]
    #[serde(rename = "checkpoint_request_timeout_secs")]
    pub checkpoint_request_timeout: Duration,
    /// Configuration options for the pipelined checkpoint fetcher.
    pub adaptive_downloader_config: AdaptiveDownloaderConfig,
    /// Minimum checkpoint lag threshold for event blob based catch-up.
    ///
    /// Specifies the minimum number of checkpoints the system must be behind
    /// the latest checkpoint before initiating catch-up using event blobs.
    /// This helps balance between catchup for small lags using checkpoints vs
    /// using event streams for longer checkpoint lags.
    pub event_stream_catchup_min_checkpoint_lag: u64,
    /// The interval at which to sample high-frequency tracing logs.
    #[serde_as(as = "DurationSeconds")]
    #[serde(rename = "sampled_tracing_interval_secs")]
    pub sampled_tracing_interval: Duration,
    /// Enable runtime catchup functionality.
    pub enable_runtime_catchup: bool,
    /// Runtime catchup lag threshold for triggering catchup at runtime.
    /// When the lag exceeds this threshold during runtime, the catchup manager
    /// will be invoked in parallel with the checkpoint downloader to accelerate
    /// recovery. Should typically be the same as event_stream_catchup_min_checkpoint_lag.
    pub runtime_catchup_lag_threshold: u64,
    /// Interval for checking lag during runtime to trigger catchup if needed.
    #[serde_as(as = "DurationSeconds")]
    #[serde(rename = "runtime_lag_check_interval_secs")]
    pub runtime_lag_check_interval: Duration,
    /// Timeout for catchup operations to prevent indefinite blocking.
    #[serde_as(as = "DurationSeconds")]
    #[serde(rename = "catchup_coordination_timeout_secs")]
    pub catchup_coordination_timeout: Duration,
    /// Timeout for catchup processing to prevent indefinite blocking.
    #[serde_as(as = "DurationSeconds")]
    #[serde(rename = "catchup_processing_timeout_secs")]
    pub catchup_processing_timeout: Duration,
}

impl Default for EventProcessorConfig {
    fn default() -> Self {
        Self {
            pruning_interval: Duration::from_secs(3600),
            checkpoint_request_timeout: Duration::from_secs(60),
            adaptive_downloader_config: Default::default(),
            event_stream_catchup_min_checkpoint_lag: 20_000,
            sampled_tracing_interval: Duration::from_secs(3600),
            runtime_catchup_lag_threshold: 20_000,
            runtime_lag_check_interval: Duration::from_secs(300),
            enable_runtime_catchup: true,
            catchup_coordination_timeout: Duration::from_secs(3000),
            catchup_processing_timeout: Duration::from_secs(3000),
        }
    }
}

/// Struct to group client-related parameters.
#[derive(Clone)]
pub struct SuiClientSet {
    /// Sui client.
    pub sui_client: RetriableSuiClient,
    /// Rest client for the full node.
    pub rpc_client: RetriableRpcClient,
}

impl SuiClientSet {
    /// Creates a new instance of the Sui client set.
    pub fn new(sui_client: RetriableSuiClient, rpc_client: RetriableRpcClient) -> Self {
        Self {
            sui_client,
            rpc_client,
        }
    }
}
impl fmt::Debug for SuiClientSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientConfig").finish()
    }
}
