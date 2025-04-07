// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
use core::time::Duration;
use std::{collections::HashMap, net::SocketAddr, path::PathBuf};

use anyhow::{Context, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_with::{serde_as, DurationSeconds};
use tracing::debug;

/// ProxyConfig is the walrus-proxy config type used when reading the yaml
/// config
#[serde_as]
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct ProxyConfig {
    /// labels that will be applied to all metrics a given proxy relays
    pub labels: HashMap<String, String>,
    /// labels that will be removed from all metrics a given proxy relays
    pub remove_labels: Vec<String>,
    /// what address to bind to
    pub listen_address: SocketAddr,
    /// remote write configuration
    pub remote_write: RemoteWriteConfig,
    /// configuration for how we fetch peer information
    pub dynamic_peers: DynamicPeerValidationConfig,
    /// metrics address for the service itself
    pub metrics_address: String,
    /// histogram scrape address to get histogram data form clients that push
    /// data to us
    pub histogram_address: String,
}

/// RemoteWriteConfig defines the mimir config items for connecting to mimir
#[serde_as]
#[derive(Clone, Debug, Deserialize, Serialize, Default)]
#[serde(rename_all = "kebab-case")]
pub struct RemoteWriteConfig {
    /// the remote_write url to post data to
    pub url: String,
    /// username is used for posting data to the remote_write api
    pub username: String,
    /// password to submit metrics
    pub password: String,
    /// Sets the maximum idle connection per host allowed in the pool.
    #[serde(default = "pool_max_idle_per_host_default")]
    pub pool_max_idle_per_host: usize,
}

/// DynamicPeerValidationConfig controls what walrus-nodes we'll speak with.
#[serde_as]
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct DynamicPeerValidationConfig {
    /// url is the json-rpc url we use to obtain valid peers on the blockchain
    pub url: String,
    /// the interval we will update our peer cache
    #[serde_as(as = "DurationSeconds<u64>")]
    pub interval: Duration,
    /// system object id to query for current nodes
    pub system_object_id: String,
    /// staking object id to query for current nodes
    pub staking_object_id: String,
    /// path to the allow list file
    pub allowlist_path: Option<PathBuf>,
}

/// the default idle worker per host (reqwest to remote write url call)
fn pool_max_idle_per_host_default() -> usize {
    8
}

/// load our config file from a path
pub fn load<P: AsRef<std::path::Path>, T: DeserializeOwned + Serialize>(path: P) -> Result<T> {
    let path = path.as_ref();
    debug!("Reading config from {:?}", path);
    Ok(serde_yaml::from_reader(
        std::fs::File::open(path).context(format!("cannot open {:?}", path))?,
    )?)
}

/// lib tests
#[cfg(test)]
mod tests {
    use super::*;

    /// test loading our config
    #[test]
    fn config_load() {
        const TEMPLATE: &str = include_str!("./fixtures/config.yaml");

        let _template: ProxyConfig = serde_yaml::from_str(TEMPLATE).unwrap();
    }
}
