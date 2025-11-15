// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Module for the configuration of contract packages and shared objects.

use std::{num::NonZeroU16, time::Duration};

use serde::{Deserialize, Serialize};
use serde_with::{DurationSeconds, serde_as};
use sui_types::base_types::ObjectID;
use walrus_core::EpochCount;

/// The default TTL for caching system and staking objects.
///
/// The TTL is chosen such that during typical individual client actions, the system and staking
/// objects are only fetched a single time, while at the same time making sure they are fresh.
pub const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(10);

#[serde_as]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
/// Configuration for the contract packages and shared objects.
pub struct ContractConfig {
    /// Object ID of the Walrus system object.
    pub system_object: ObjectID,
    /// Object ID of the Walrus staking object.
    pub staking_object: ObjectID,
    /// Object ID of the credits object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credits_object: Option<ObjectID>,
    /// Object ID of the walrus subsidies object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub walrus_subsidies_object: Option<ObjectID>,
    /// The number of shards to use for the client.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub n_shards: Option<NonZeroU16>,
    /// The maximum number of epochs a blob can be stored for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_epochs_ahead: Option<EpochCount>,
    /// The TTL for cached system and staking objects.
    #[serde(default = "defaults::default_cache_ttl", rename = "cache_ttl_secs")]
    #[serde_as(as = "DurationSeconds")]
    pub cache_ttl: Duration,
}

impl ContractConfig {
    /// Creates a basic [`ContractConfig`] with just the system and staking objects.
    pub fn new(system_object: ObjectID, staking_object: ObjectID) -> Self {
        Self {
            system_object,
            staking_object,
            credits_object: None,
            walrus_subsidies_object: None,
            n_shards: None,
            max_epochs_ahead: None,
            cache_ttl: DEFAULT_CACHE_TTL,
        }
    }
}

mod defaults {
    use super::*;

    pub fn default_cache_ttl() -> Duration {
        DEFAULT_CACHE_TTL
    }
}
