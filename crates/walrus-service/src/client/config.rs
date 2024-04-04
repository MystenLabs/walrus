// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use serde::{Deserialize, Serialize};
use walrus_core::encoding::EncodingConfig;
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
    /// The number of nodes the client contacts in parallel to store slivers. Since the slivers are
    /// then stored in parallel, the number of active connections may be higher.
    pub concurrent_store_requests: usize,
    /// The number of nodes the client contacts in parallel to retrieve the metadata.
    pub concurrent_metadata_requests: usize,
    /// The number of _shards_ the client contacts in parallel to retrieve slivers.
    pub concurrent_sliver_read_requests: usize,
    /// Timeout for the `reqwest` client used by the client,
    pub connection_timeout: Duration,
}

impl Config {
    /// Returns the [`EncodingConfig`] for this configuration.
    pub fn encoding_config(&self) -> EncodingConfig {
        EncodingConfig::new(
            self.source_symbols_primary,
            self.source_symbols_secondary,
            self.committee.total_weight as u32,
        )
    }
}
