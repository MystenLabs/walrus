// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{num::NonZeroUsize, time::Duration};

use serde::{Deserialize, Serialize};
use walrus_core::{encoding::EncodingConfig, ShardIndex};
use walrus_sui::types::Committee;

use crate::config::LoadConfig;

/// Temporary config for the client.
#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    /// The committee.
    pub committee: Committee,
    /// The number of source symbols for the primary encoding.
    pub source_symbols_primary: u16,
    /// The number of source symbols for the secondary encoding.
    pub source_symbols_secondary: u16,
    /// The number of parallel requests the client makes.
    pub concurrent_requests: usize,
    /// Timeout for the `reqwest` client used by the client,
    pub connection_timeout: Duration,
}

impl Config {
    /// Return the shards handed by the specified storage node.
    pub fn shards_for_node(&self, node_id: usize) -> Vec<ShardIndex> {
        self.committee
            .members
            .get(node_id)
            .map(|node| node.shard_ids.clone())
            .unwrap_or_default()
    }

    /// Return the total number of shards in the committee.
    /// Panic if the committee has no shards.
    pub fn total_shards(&self) -> NonZeroUsize {
        let shards = self
            .committee
            .members
            .iter()
            .map(|node| node.shard_ids.len())
            .sum();
        NonZeroUsize::new(shards).expect("committee has no shards")
    }

    /// Returns the [`EncodingConfig`] for this configuration.
    pub fn encoding_config(&self) -> EncodingConfig {
        EncodingConfig::new(
            self.source_symbols_primary,
            self.source_symbols_secondary,
            self.committee.total_weight as u32,
        )
    }
}

impl LoadConfig for Config {}
