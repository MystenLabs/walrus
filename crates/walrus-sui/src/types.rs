// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Walrus Move type bindings. Replicates the Move types in Rust and provides some convenient
//! structs to use with the SDK.

use std::{
    fmt::Display,
    net::SocketAddr,
    num::{NonZeroU16, ParseIntError},
};

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
    StakedWal,
    StakingObject,
    StorageNode,
    StorageNodeCap,
    StorageResource,
    SystemObject,
};

pub(crate) mod move_errors;

use serde::{Deserialize, Serialize};
use walrus_core::{bft, ensure, Epoch, NetworkPublicKey, PublicKey, ShardIndex};

/// The zero-th Epoch is special in that it's the genesis epoch.
pub const GENESIS_EPOCH: Epoch = 0;

/// Network address consisting of host name or IP and port.
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct NetworkAddress(pub String);

impl Display for NetworkAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<SocketAddr> for NetworkAddress {
    fn from(value: SocketAddr) -> Self {
        Self(value.to_string())
    }
}

impl NetworkAddress {
    /// Tries to get the port from the address, assuming the format `host:port`. Returns an
    /// error if a port is present but cannot be parsed. If no port is present, returns `Ok(None)`.
    pub fn try_get_port(&self) -> Result<Option<u16>, ParseIntError> {
        if let Some((_, port)) = self.0.split_once(':') {
            let port = port.parse()?;
            Ok(Some(port))
        } else {
            Ok(None)
        }
    }

    /// Returns the host from the network address, assuming the format `host:port` or `host`
    /// if no `:` is present in the string. Does not perform any validation.
    pub fn get_host(&self) -> &str {
        if let Some((host, _)) = self.0.split_once(':') {
            host
        } else {
            self.0.as_str()
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

impl NodeRegistrationParams {
    /// Creates a new node registration parameters for testing.
    pub fn new_for_test(
        protocol_public_key: &PublicKey,
        network_public_key: &NetworkPublicKey,
    ) -> Self {
        Self {
            name: "Test0".to_owned(),
            network_address: NetworkAddress("127.0.0.1:8080".to_owned()),
            public_key: protocol_public_key.clone(),
            network_public_key: network_public_key.clone(),
            commission_rate: 0,
            storage_price: 5,
            write_price: 1,
            node_capacity: 1_000_000_000_000,
        }
    }
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

    /// Returns the network address and network public key of the committee members.
    pub fn network_addresses_and_pks(
        &self,
    ) -> impl Iterator<Item = (&NetworkAddress, &NetworkPublicKey)> {
        self.members
            .iter()
            .map(|member| (&member.network_address, &member.network_public_key))
    }
}
