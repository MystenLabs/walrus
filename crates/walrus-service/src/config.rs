// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, net::SocketAddr, path::PathBuf};

use serde::{Deserialize, Serialize};
use walrus_core::Epoch;

use crate::crypto::{KeyPair, PublicKey};

/// Represents the index of a shard.
pub type ShardIndex = u32;

/// Represents a storage node identifier.
#[derive(Serialize, Deserialize)]
pub struct StorageNodeIdentifier {
    /// The public key of the storage node.
    pub public_key: PublicKey,
    /// The shards that the storage node is responsible for.
    pub shards: Vec<ShardIndex>,
}

/// Represents a committee of storage nodes, that is, the Walrus committee.
#[derive(Serialize, Deserialize)]
pub struct Committee {
    /// The members of the committee.
    pub members: Vec<StorageNodeIdentifier>,
    /// The epoch of the committee.
    pub epoch: Epoch,
}

impl Committee {
    /// Returns the total number of shards in the system.
    pub fn number_of_shards(&self) -> usize {
        self.members.iter().map(|node| node.shards.len()).sum()
    }

    /// Returns the number of shards required to reach a quorum (2f+1).
    pub fn quorum_threshold(&self) -> usize {
        self.number_of_shards() * 2 / 3 + 1
    }

    /// Returns the number of shards required to reach validity (f+1).
    pub fn validity_threshold(&self) -> usize {
        (self.number_of_shards() + 2) / 3
    }
}

/// Represents the global public parameters of the system.
pub struct GlobalPublicParameters {
    /// The network addresses of the shards.
    pub network_addresses: HashMap<ShardIndex, SocketAddr>,
    /// The metrics addresses of the shards.
    pub metrics_addresses: HashMap<ShardIndex, SocketAddr>,
}

/// Represents the private parameters of a shard.
pub struct ShardPrivateParameters {
    /// The network address of the shard.
    pub network_address: SocketAddr,
    /// The metrics address of the shard.
    pub metrics_address: SocketAddr,
    /// The storage directory of the shard.
    pub storage_directory: PathBuf,
}

/// Represents the private parameters of a storage node.
pub struct StorageNodePrivateParameters {
    /// The keypair of the storage node.
    pub keypair: KeyPair,
    /// The private parameters shards that the storage node is responsible for.
    pub shards: HashMap<ShardIndex, ShardPrivateParameters>,
}
