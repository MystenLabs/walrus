// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use serde::{Deserialize, Serialize};
use walrus_core::ShardIndex;
use walrus_sui::types::Committee;

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
    pub fn shards_by_node(&self, node_id: usize) -> Vec<ShardIndex> {
        self.committee
            .members
            .get(node_id)
            .map(|node| node.shard_ids.clone())
            .unwrap_or_default()
    }
}
