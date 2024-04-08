// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{num::NonZeroU16, time::Duration};

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
    pub source_symbols_primary: NonZeroU16,
    /// The number of source symbols for the secondary encoding.
    pub source_symbols_secondary: NonZeroU16,
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
    pub fn n_shards(&self) -> NonZeroU16 {
        let shards = self
            .committee
            .members
            .iter()
            .map(|node| node.shard_ids.len())
            .sum::<usize>()
            .try_into()
            .expect("should fit into a `u16`");
        NonZeroU16::new(shards).expect("committee has no shards")
    }

    /// Returns the [`EncodingConfig`] for this configuration.
    pub fn encoding_config(&self) -> EncodingConfig {
        EncodingConfig::new(
            self.source_symbols_primary.get(),
            self.source_symbols_secondary.get(),
            self.committee.total_weight,
        )
    }
}

impl LoadConfig for Config {}
