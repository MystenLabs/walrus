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
    /// Object ID of the Walrus subsidies object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subsidies_object: Option<ObjectID>,
    /// The original system package ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_system_package_id: Option<ObjectID>,
}

impl ContractConfig {
    /// Creates a [`ContractConfig`] with the system, staking, and subsidies objects.
    pub fn new_with_subsidies(
        system_object: ObjectID,
        staking_object: ObjectID,
        subsidies_object: Option<ObjectID>,
        original_system_package_id: Option<ObjectID>,
    ) -> Self {
        Self {
            system_object,
            staking_object,
            subsidies_object,
            original_system_package_id,
        }
    }
    /// Creates a basic [`ContractConfig`] with just the system and staking objects.
    pub fn new(system_object: ObjectID, staking_object: ObjectID) -> Self {
        Self {
            system_object,
            staking_object,
            subsidies_object: None,
            original_system_package_id: None,
        }
    }
}
