// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Module for the configuration of contract packages and shared objects.

use serde::{Deserialize, Serialize};
use sui_types::base_types::ObjectID;

#[derive(Debug, Clone, Deserialize, Serialize)]
/// Configuration for the contract packages and shared objects.
pub struct ContractConfig {
    /// Object ID of the Walrus system object.
    pub system_object: ObjectID,
    /// Object ID of the Walrus staking object.
    pub staking_object: ObjectID,
    // TODO(WAL-438): Add package and object for subsidies
}

impl ContractConfig {
    /// Creates a basic [`ContractConfig`] with just the system and staking objects.
    pub fn new(system_object: ObjectID, staking_object: ObjectID) -> Self {
        Self {
            system_object,
            staking_object,
        }
    }
}
