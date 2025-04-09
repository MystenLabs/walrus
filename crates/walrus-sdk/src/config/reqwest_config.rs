// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Configuration for the [`reqwest`] client.
use std::time::Duration;

use reqwest::ClientBuilder;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DurationMilliSeconds};
use walrus_utils::backoff::ExponentialBackoffConfig;

/// Configuration for the parameters of the `reqwest` client.
#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(default)]
pub struct ReqwestConfig {
    /// Total request timeout, applied from when the request starts connecting until the response
    /// body has finished.
    #[serde_as(as = "DurationMilliSeconds")]
    #[serde(rename = "total_timeout_millis")]
    pub total_timeout: Duration,
    /// Timeout for idle sockets to be kept alive. Pass `None` to disable.
    #[serde_as(as = "Option<DurationMilliSeconds>")]
    #[serde(rename = "pool_idle_timeout_millis")]
    pub pool_idle_timeout: Option<Duration>,
    /// Timeout for receiving an acknowledgement of the keep-alive ping.
    #[serde_as(as = "DurationMilliSeconds")]
    #[serde(rename = "http2_keep_alive_timeout_millis")]
    pub http2_keep_alive_timeout: Duration,
    /// Ping every such interval to keep the connection alive.
    #[serde_as(as = "Option<DurationMilliSeconds>")]
    #[serde(rename = "http2_keep_alive_interval_millis")]
    pub http2_keep_alive_interval: Option<Duration>,
    /// Sets whether HTTP2 keep-alive should apply while the connection is idle.
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
            .http2_prior_knowledge()
            .http2_keep_alive_timeout(self.http2_keep_alive_timeout)
            .http2_keep_alive_interval(self.http2_keep_alive_interval)
            .http2_keep_alive_while_idle(self.http2_keep_alive_while_idle)
    }
}

/// Configuration for retries towards the storage nodes.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(default)]
pub struct RequestRateConfig {
    /// The maximum number of connections the client can open towards each node.
    pub max_node_connections: usize,
    /// The configuration for the backoff strategy.
    pub backoff_config: ExponentialBackoffConfig,
}

impl Default for RequestRateConfig {
    fn default() -> Self {
        Self {
            max_node_connections: 10,
            backoff_config: Default::default(),
        }
    }
}

pub(crate) mod default {
    use std::time::Duration;

    /// Allows for enough time to transfer big slivers on the other side of the world.
    pub fn total_timeout() -> Duration {
        Duration::from_secs(30)
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
