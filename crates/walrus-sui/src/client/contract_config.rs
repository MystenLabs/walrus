// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Module for the configuration of contract packages and shared objects.

use serde::{Deserialize, Serialize};
use sui_types::base_types::ObjectID;

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
}

impl ContractConfig {
    /// Creates a basic [`ContractConfig`] with just the system and staking objects.
    pub fn new(system_object: ObjectID, staking_object: ObjectID) -> Self {
        Self {
            system_object,
            staking_object,
            credits_object: None,
            walrus_subsidies_object: None,
        }
    }
}
