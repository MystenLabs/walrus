// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{path::PathBuf, time::Duration};

use serde::{de::Error as _, Deserialize, Serialize};
use sui_types::base_types::ObjectID;

use crate::config::LoadConfig;

/// Config for the client.
#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
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
    /// Timeout for the `reqwest` client used by the client,
    #[serde(default = "default::connection_timeout")]
    pub connection_timeout: Duration,
    /// The walrus package id.
    pub system_pkg: ObjectID,
    /// The system walrus system object id.
    pub system_object: ObjectID,
    /// Path to the wallet configuration.
    ///
    /// If set, this MUST be an absolute path.
    #[serde(deserialize_with = "deserialize_wallet_config")]
    pub wallet_config: Option<PathBuf>,
}

impl LoadConfig for Config {}

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
        (n_shards.get() - bft::max_n_faulty(n_shards)) as usize
    }

    pub fn concurrent_sliver_reads(n_shards: NonZeroU16) -> usize {
        (n_shards.get() - 2 * bft::max_n_faulty(n_shards)) as usize
    }

    pub fn concurrent_metadata_reads() -> usize {
        3
    }

    pub fn connection_timeout() -> Duration {
        Duration::from_secs(10)
    }
}
