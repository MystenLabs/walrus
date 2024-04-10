// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{num::NonZeroU16, time::Duration};

use serde::{Deserialize, Serialize};
use sui_types::base_types::ObjectID;
use walrus_core::encoding::EncodingConfig;
use walrus_sui::types::Committee;

use crate::config::LoadConfig;

/// Config for the client.
#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    // TODO(giac): the number of symbols will be computable from the number of shards after #206
    // lands. This information can then be removed.
    /// The number of source symbols for the primary encoding.
    pub source_symbols_primary: NonZeroU16,
    /// The number of source symbols for the secondary encoding.
    pub source_symbols_secondary: NonZeroU16,
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
    /// The number of source symbols for the primary encoding.
    pub source_symbols_primary: NonZeroU16,
    /// The number of source symbols for the secondary encoding.
    pub source_symbols_secondary: NonZeroU16,
}

impl LoadConfig for LocalCommitteeConfig {}

impl LocalCommitteeConfig {
    /// Returns the [`EncodingConfig`] for this configuration.
    pub fn encoding_config(&self) -> EncodingConfig {
        EncodingConfig::new(
            self.source_symbols_primary.get(),
            self.source_symbols_secondary.get(),
            self.committee.total_weight,
        )
    }
}
