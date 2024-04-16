// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use serde::{Deserialize, Serialize};
use sui_types::base_types::ObjectID;
use walrus_core::encoding::EncodingConfig;
use walrus_sui::types::Committee;

use crate::config::LoadConfig;

/// Config for the client.
#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    /// The number of parallel requests the client makes.
    pub concurrent_requests: usize,
    /// Timeout for the `reqwest` client used by the client,
    pub connection_timeout: Duration,
    /// The walrus package id.
    pub system_pkg: ObjectID,
    /// The system walrus system object id.
    pub system_object: ObjectID,
}

impl LoadConfig for Config {}

/// Temporary config with information that can be eventually fetched from the chain.
// TODO: remove as soon as the information is fetched from the chain.
#[derive(Debug, Serialize, Deserialize)]
pub struct LocalCommitteeConfig {
    /// The committee information.
    pub committee: Committee,
}

impl LoadConfig for LocalCommitteeConfig {}

impl LocalCommitteeConfig {
    /// Returns the [`EncodingConfig`] for this configuration.
    pub fn encoding_config(&self) -> EncodingConfig {
        EncodingConfig::new(self.committee.n_shards())
    }
}
