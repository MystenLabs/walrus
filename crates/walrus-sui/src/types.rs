// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Walrus move type bindings. Replicates the move types in Rust and provides some convenient
//! structs to use with the sdk.

use std::{
    fmt::Display,
    net::{SocketAddr, ToSocketAddrs},
    num::NonZeroU16,
    str::FromStr,
    vec,
};

use anyhow::bail;
use thiserror::Error;

mod events;
pub use events::{
    BlobCertified,
    BlobDeleted,
    BlobEvent,
    BlobRegistered,
    ContractEvent,
    EpochChangeDone,
    EpochChangeEvent,
    EpochChangeStart,
    EpochParametersSelected,
    InvalidBlobId,
};

pub mod move_structs;
pub use move_structs::{
    Blob,
    EpochStatus,
    StakedWal,
    StakingObject,
    StorageNode,
    StorageNodeCap,
    StorageResource,
    SystemObject,
};
use serde::{Deserialize, Serialize};
use walrus_core::{bft, ensure, Epoch, NetworkPublicKey, PublicKey, ShardIndex};

/// Network address consisting of host name or IP and port.
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct NetworkAddress {
    /// Host name or IP address.
    pub host: String,
    /// Port.
    pub port: u16,
}

impl FromStr for NetworkAddress {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let Some((host, port)) = s.split_once(':') else {
            bail!("invalid network address")
        };
        let port = port.parse()?;
        Ok(Self {
            host: host.to_owned(),
            port,
        })
    }
}

impl ToSocketAddrs for NetworkAddress {
    type Iter = vec::IntoIter<SocketAddr>;

    fn to_socket_addrs(&self) -> std::io::Result<Self::Iter> {
        (self.host.as_str(), self.port).to_socket_addrs()
    }
}

impl Display for NetworkAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

impl From<SocketAddr> for NetworkAddress {
    fn from(value: SocketAddr) -> Self {
        Self {
            host: value.ip().to_string(),
            port: value.port(),
        }
    }
}

/// Node parameters needed to register a node.
#[derive(Debug, Deserialize, Serialize)]
pub struct NodeRegistrationParams {
    /// Name of the storage node.
    pub name: String,
    /// The network address of the storage node.
    pub network_address: NetworkAddress,
    /// The public key of the storage node.
    pub public_key: PublicKey,
    /// The network key of the storage node.
    pub network_public_key: NetworkPublicKey,
    /// The commission rate of the storage node.
    pub commission_rate: u64,
    /// The vote for the storage price per unit.
    pub storage_price: u64,
    /// The vote for the write price per unit.
    pub write_price: u64,
    /// The capacity of the node that determines the vote for the capacity
    /// after shards are assigned.
    pub node_capacity: u64,
}

/// Error returned when trying to create a committee with no shards.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum InvalidCommittee {
    /// Error resulting if the committee does not contain the specified number of shards,
    /// except for an empty committee in epoch zero.
    #[error("unexpected total number of shards in committee members")]
    IncorrectNumberOfShards,
    /// Error resulting if one of the nodes has no shards.
    #[error("trying to create a committee with an empty node")]
    EmptyNode,
}

/// Convenience type that contains the committee including full storage node information.
#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
pub struct Committee {
    /// The current epoch
    pub epoch: Epoch,
    /// The members of the committee
    // INV: `members.iter().all(|m| !m.shard_ids.is_empty())`
    members: Vec<StorageNode>,
    /// The number of shards held by the committee. Is zero in epoch 0.
    n_shards: NonZeroU16,
}

impl Committee {
    /// Create a new committee for `epoch` consisting of `members`.
    pub fn new(
        members: Vec<StorageNode>,
        epoch: Epoch,
        n_shards: NonZeroU16,
    ) -> Result<Self, InvalidCommittee> {
        let mut shards_in_committee = 0;
        for node in members.iter() {
            let node_shards = node.shard_ids.len();
            ensure!(node_shards > 0, InvalidCommittee::EmptyNode,);
            shards_in_committee += node_shards
        }
        ensure!(
            epoch == 0 && shards_in_committee == 0
                || n_shards.get() as usize == shards_in_committee,
            InvalidCommittee::IncorrectNumberOfShards
        );
        Ok(Self {
            members,
            epoch,
            n_shards,
        })
    }

    /// Checks if the number is larger or equal to the minimum number of correct shards.
    ///
    /// See [`min_n_correct`][Self::min_n_correct] for further details.
    #[inline]
    pub fn is_at_least_min_n_correct(&self, num: usize) -> bool {
        num >= self.min_n_correct()
    }

    /// Returns the minimum number of correct shards.
    ///
    /// This is (`n_shards - f`), where `f` is the maximum number of faulty shards, given
    /// `n_shards`. See [walrus_core::bft] for further details.
    #[inline]
    pub fn min_n_correct(&self) -> usize {
        bft::min_n_correct(self.n_shards).get().into()
    }

    /// Checks if the number is large enough to reach a quorum (`2f + 1`).
    ///
    /// `f` is the maximum number of faulty shards, given `n_shards`. See [walrus_core::bft] for
    /// further details.
    #[inline]
    pub fn is_quorum(&self, num: usize) -> bool {
        num > 2 * usize::from(bft::max_n_faulty(self.n_shards))
    }

    /// Checks if the number is larger or equal to the validity threshold
    ///
    /// The validity threshold is `f + 1`, where `f` is the maximum number of faulty shards. See
    /// [walrus_core::bft] for further details.
    #[inline]
    pub fn is_above_validity(&self, num: usize) -> bool {
        num > usize::from(bft::max_n_faulty(self.n_shards))
    }

    /// Return the shards handed by the specified storage node, based on its public key.
    /// If empty, the node is not in the committee.
    pub fn shards_for_node_public_key(&self, public_key: &PublicKey) -> &[ShardIndex] {
        self.members
            .iter()
            .find_map(|node| (node.public_key == *public_key).then_some(node.shard_ids.as_slice()))
            .unwrap_or_default()
    }

    /// Return the total number of shards in the committee.
    pub fn n_shards(&self) -> NonZeroU16 {
        self.n_shards
    }

    /// Returns the members of the committee.
    pub fn members(&self) -> &[StorageNode] {
        &self.members
    }

    /// Returns the number of members in the committee.
    pub fn n_members(&self) -> usize {
        self.members.len()
    }

    /// Computes the minimum number of nodes that are necessary to get above the threshold of
    /// Byzantine shards `f`.
    pub fn min_nodes_above_f(&self) -> (usize, usize) {
        let mut shards_per_node: Vec<_> = self
            .members()
            .iter()
            .map(|node| node.shard_ids.len())
            .collect();
        shards_per_node.sort_unstable();
        let mut total = 0;
        let threshold = bft::max_n_faulty(self.n_shards()).into();
        for (idx, count) in shards_per_node.iter().rev().enumerate() {
            total += count;
            if total > threshold {
                return (idx + 1, total);
            }
        }
        unreachable!("threshold < n_shards")
    }

    /// Returns the index of the member that holds the specified shard.
    pub fn member_index_for_shard(&self, shard: ShardIndex) -> Option<usize> {
        // TODO(#703): add a system invariant check so that we can assert the shard
        // must exist.
        self.members
            .iter()
            .position(|node| node.shard_ids.contains(&shard))
    }

    /// Returns true if the committee contains a member with the specified public key.
    pub fn contains(&self, public_key: &PublicKey) -> bool {
        self.members
            .iter()
            .any(|node| node.public_key == *public_key)
    }

    /// Returns the node with the specified public key, if any.
    pub fn find(&self, public_key: &PublicKey) -> Option<&StorageNode> {
        self.members
            .iter()
            .find(|node| node.public_key == *public_key)
    }

    /// Returns the node response for the specified shard.
    pub fn find_by_shard<S: Into<ShardIndex>>(&self, shard: S) -> Option<&StorageNode> {
        self.member_index_for_shard(shard.into())
            .map(|index| &self.members[index])
    }
}
