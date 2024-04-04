// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{num::NonZeroU16, time::Duration};

use serde::{Deserialize, Serialize};
use walrus_core::encoding::EncodingConfig;
use walrus_sui::types::Committee;

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
    /// Returns the [`EncodingConfig`] for this configuration.
    pub fn encoding_config(&self) -> EncodingConfig {
        EncodingConfig::new(
            self.source_symbols_primary.get(),
            self.source_symbols_secondary.get(),
            self.committee.total_weight,
        )
    }
}
