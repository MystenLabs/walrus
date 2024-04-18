// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use serde::{Deserialize, Serialize};
use sui_types::base_types::ObjectID;

use crate::config::LoadConfig;

/// Config for the client.
#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
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
