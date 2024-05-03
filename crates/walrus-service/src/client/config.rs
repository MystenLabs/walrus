// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{path::PathBuf, time::Duration};

use reqwest::ClientBuilder;
use serde::{de::Error as _, Deserialize, Serialize};
use sui_types::base_types::ObjectID;

use crate::config::LoadConfig;

/// Config for the client.
#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    /// The walrus package id.
    pub system_pkg: ObjectID,
    /// The system walrus system object id.
    pub system_object: ObjectID,
    /// Path to the wallet configuration.
    ///
    /// If set, this MUST be an absolute path.
    #[serde(deserialize_with = "deserialize_wallet_config")]
    pub wallet_config: Option<PathBuf>,
    /// Configuration for the client's network communication.
    #[serde(default)]
    pub communication_config: ClientCommunicationConfig,
}

impl LoadConfig for Config {}

/// Configuration for the communication parameters of the client
#[derive(Debug, Serialize, Deserialize)]
pub struct ClientCommunicationConfig {
    /// The maximum number of storage nodes the client contacts in parallel to write slivers and
    /// metadata. If `None`, the value is set by the client to `n - f`, depending on the number of
    /// shards `n`.
    pub concurrent_writes: Option<usize>,
    /// The maximum number of slivers the client requests in parallel. If `None`, the value is set
    /// by the client to `n - 2f`, depending on the number of shards `n`.
    pub concurrent_sliver_reads: Option<usize>,
    /// The maximum number of nodes the client contacts to get the blob metadata in parallel.
    #[serde(default = "default::concurrent_metadata_reads")]
    pub concurrent_metadata_reads: usize,
    /// The configuration for the `reqwest` client.
    #[serde(default)]
    pub reqwest_config: ReqwestConfig,
}

impl Default for ClientCommunicationConfig {
    fn default() -> Self {
        Self {
            concurrent_writes: None,
            concurrent_sliver_reads: None,
            concurrent_metadata_reads: default::concurrent_metadata_reads(),
            reqwest_config: ReqwestConfig::default(),
        }
    }
}

/// Configuration for the parameters of the `reqwest` client.
#[derive(Debug, Serialize, Deserialize)]
pub struct ReqwestConfig {
    /// Total request timeout, applied from when the request starts connecting until the response
    /// body has finished.
    #[serde(default = "default::total_timeout")]
    pub total_timeout: Duration,
    /// Timeout for idle sockets to be kept alive. Pass `None` to disable.
    #[serde(default = "default::pool_idle_timeout")]
    pub pool_idle_timeout: Option<Duration>,
    /// Timeout for receiving an acknowledgement of the keep-alive ping.
    #[serde(default = "default::http2_keep_alive_timeout")]
    pub http2_keep_alive_timeout: Duration,
    /// Ping every such interval to keep the connection alive.
    #[serde(default = "default::http2_keep_alive_interval")]
    pub http2_keep_alive_interval: Option<Duration>,
    /// Sets whether HTTP2 keep-alive should apply while the connection is idle.
    #[serde(default = "default::http2_keep_alive_while_idle")]
    pub http2_keep_alive_while_idle: bool,
}

impl Default for ReqwestConfig {
    fn default() -> Self {
        Self {
            total_timeout: default::total_timeout(),
            pool_idle_timeout: default::pool_idle_timeout(),
            http2_keep_alive_timeout: default::http2_keep_alive_timeout(),
            http2_keep_alive_interval: default::http2_keep_alive_interval(),
            http2_keep_alive_while_idle: default::http2_keep_alive_while_idle(),
        }
    }
}

impl ReqwestConfig {
    /// Applies the configurations in [`Self`] to the provided client builder.
    pub fn apply(&self, builder: ClientBuilder) -> ClientBuilder {
        builder
            .timeout(self.total_timeout)
            .pool_idle_timeout(self.pool_idle_timeout)
            .http2_keep_alive_timeout(self.http2_keep_alive_timeout)
            .http2_keep_alive_interval(self.http2_keep_alive_interval)
            .http2_keep_alive_while_idle(self.http2_keep_alive_while_idle)
    }
}

fn deserialize_wallet_config<'de, D>(deserializer: D) -> Result<Option<PathBuf>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let path = Option::<PathBuf>::deserialize(deserializer)?;
    if let Some(path) = &path {
        if !path.is_absolute() {
            return Err(D::Error::custom(format!(
                "an absolute path is required for the wallet config (found {})",
                path.display()
            )));
        }
    }
    Ok(path)
}

/// Returns the default paths for the Walrus configuration file.
pub fn default_configuration_paths() -> Vec<PathBuf> {
    let mut default_paths = vec!["./config.yaml".into()];
    if let Some(home_dir) = home::home_dir() {
        default_paths.push(home_dir.join(".walrus").join("config.yaml"))
    }
    default_paths
}

pub(crate) mod default {
    use std::{num::NonZeroU16, time::Duration};

    use walrus_core::bft;

    pub fn concurrent_writes(n_shards: NonZeroU16) -> usize {
        (n_shards.get() - bft::max_n_faulty(n_shards)).into()
    }

    pub fn concurrent_sliver_reads(n_shards: NonZeroU16) -> usize {
        (n_shards.get() - 2 * bft::max_n_faulty(n_shards)).into()
    }

    pub fn concurrent_metadata_reads() -> usize {
        3
    }

    pub fn total_timeout() -> Duration {
        Duration::from_secs(10)
    }

    /// Disabled by default, i.e., connections are kept alive.
    pub fn pool_idle_timeout() -> Option<Duration> {
        None
    }

    /// Close the connection if the answer to the ping is not received within this deadline.
    pub fn http2_keep_alive_timeout() -> Duration {
        Duration::from_secs(5)
    }

    /// Ping every 30 secs.
    pub fn http2_keep_alive_interval() -> Option<Duration> {
        Some(Duration::from_secs(30))
    }

    /// Keep-alive pings are sent to idle connections.
    pub fn http2_keep_alive_while_idle() -> bool {
        true
    }
}
