// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use walrus_core::{PublicKey, ShardIndex};
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
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeConfig {
    pub public_key: PublicKey,
    pub address: SocketAddr,
    pub shards: Vec<ShardIndex>,
}
