// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{path::PathBuf, time::Duration};

use serde::{de::Error as _, Deserialize, Serialize};
use sui_types::base_types::ObjectID;

use crate::config::LoadConfig;

/// Config for the client.
#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    /// The number of parallel requests the client makes.
    #[serde(default = "defaults::default_concurrent_requests")]
    pub concurrent_requests: usize,
    /// Timeout for the `reqwest` client used by the client,
    #[serde(default = "defaults::default_connection_timeout")]
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

mod defaults {
    use std::time::Duration;

    pub fn default_concurrent_requests() -> usize {
        10
    }

    pub fn default_connection_timeout() -> Duration {
        Duration::from_secs(10)
    }
}
