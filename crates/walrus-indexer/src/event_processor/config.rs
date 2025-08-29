// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Configuration module for the indexer event processor.

use std::{fmt, path::PathBuf, time::Duration};

use checkpoint_downloader::AdaptiveDownloaderConfig;
use serde::{Deserialize, Serialize};
use serde_with::{DurationSeconds, serde_as};
use sui_types::base_types::ObjectID;
use walrus_sui::client::{
    retry_client::{RetriableRpcClient, RetriableSuiClient},
    rpc_config::RpcFallbackConfig,
};

/// Configuration for the indexer event processor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexerEventProcessorConfig {
    /// Package ID to filter events from
    pub walrus_package_id: ObjectID,
    /// Buffer size for event channel
    pub event_buffer_size: usize,
    /// Checkpoint downloader configuration
    pub downloader_config: AdaptiveDownloaderConfig,
    /// Event processor configuration
    pub processor_config: EventProcessorConfig,
}

impl Default for IndexerEventProcessorConfig {
    fn default() -> Self {
        Self {
            // This should be configured with the actual Walrus package ID
            walrus_package_id: ObjectID::ZERO,
            event_buffer_size: 10000,
            downloader_config: AdaptiveDownloaderConfig::default(),
            processor_config: EventProcessorConfig::default(),
        }
    }
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
    /// The interval at which to sample high-frequency tracing logs.
    #[serde_as(as = "DurationSeconds")]
    #[serde(rename = "sampled_tracing_interval_secs")]
    pub sampled_tracing_interval: Duration,
}

impl Default for EventProcessorConfig {
    fn default() -> Self {
        Self {
            pruning_interval: Duration::from_secs(3600),
            checkpoint_request_timeout: Duration::from_secs(60),
            adaptive_downloader_config: Default::default(),
            sampled_tracing_interval: Duration::from_secs(3600),
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
        f.debug_struct("SuiClientSet").finish()
    }
}

/// Struct to group system-related parameters for indexing.
#[derive(Debug, Clone)]
pub struct SystemConfig {
    /// The package ID of the system package.
    pub system_pkg_id: ObjectID,
    /// The object ID of the system object.
    pub system_object_id: ObjectID,
    /// The object ID of the staking object.
    pub staking_object_id: ObjectID,
    /// The Walrus package ID to index events from.
    pub walrus_package_id: ObjectID,
}

impl SystemConfig {
    /// Creates a new instance of the system configuration.
    pub fn new(
        system_pkg_id: ObjectID,
        system_object_id: ObjectID,
        staking_object_id: ObjectID,
        walrus_package_id: ObjectID,
    ) -> Self {
        Self {
            system_pkg_id,
            system_object_id,
            staking_object_id,
            walrus_package_id,
        }
    }
}

/// Struct to group general configuration parameters for the indexer.
#[derive(Debug, Clone)]
pub struct IndexerRuntimeConfig {
    /// The address of the RPC server.
    pub rpc_addresses: Vec<String>,
    /// The event polling interval.
    pub event_polling_interval: Duration,
    /// The path to the database.
    pub db_path: PathBuf,
    /// The path to the rpc fallback config.
    pub rpc_fallback_config: Option<RpcFallbackConfig>,
}
