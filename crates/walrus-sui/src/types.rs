// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Walrus move type bindings. Replicates the move types in Rust.

use std::{
    fmt::Display,
    net::{SocketAddr, ToSocketAddrs},
    num::NonZeroU16,
    str::FromStr,
    vec,
};

use anyhow::{anyhow, bail};
use fastcrypto::traits::ToFromBytes;
use serde::{de::Error, Deserialize, Deserializer, Serialize, Serializer};
use serde_with::{serde_as, DisplayFromStr};
use sui_sdk::rpc_types::SuiEvent;
use sui_types::{base_types::ObjectID, event::EventID};
use thiserror::Error;
use tracing::instrument;
use walrus_core::{
    bft,
    ensure,
    BlobId,
    EncodingType,
    Epoch,
    NetworkPublicKey,
    PublicKey,
    ShardIndex,
};

use crate::contracts::{
    self,
    AssociatedContractStruct,
    AssociatedSuiEvent,
    MoveConversionError,
    StructTag,
};
/// Sui object for storage resources.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct StorageResource {
    /// Object ID of the Sui object.
    pub id: ObjectID,
    /// The start epoch of the resource (inclusive).
    pub start_epoch: Epoch,
    /// The end epoch of the resource (exclusive).
    pub end_epoch: Epoch,
    /// The total amount of reserved storage.
    pub storage_size: u64,
}

impl AssociatedContractStruct for StorageResource {
    const CONTRACT_STRUCT: StructTag<'static> = contracts::storage_resource::Storage;
}

/// Sui object for a blob.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct Blob {
    /// Object ID of the Sui object.
    pub id: ObjectID,
    /// The epoch in which the blob has been registered.
    pub stored_epoch: Epoch,
    /// The blob ID.
    #[serde(serialize_with = "serialize_blob_id")]
    pub blob_id: BlobId,
    /// The (unencoded) size of the blob.
    pub size: u64,
    /// The erasure coding type used for the blob.
    pub erasure_code_type: EncodingType,
    /// The epoch in which the blob was first certified, `None` if the blob is uncertified.
    pub certified_epoch: Option<Epoch>,
    /// The [`StorageResource`] used to store the blob.
    pub storage: StorageResource,
}

/// Serialize as string to make sure that the json output uses the base64 encoding.
fn serialize_blob_id<S>(blob_id: &BlobId, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.collect_str(blob_id)
}

impl AssociatedContractStruct for Blob {
    const CONTRACT_STRUCT: StructTag<'static> = contracts::blob::Blob;
}

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

/// Sui type for storage node.
#[serde_as]
#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
pub struct StorageNode {
    /// Name of the storage node.
    pub name: String,
    /// The network address of the storage node.
    #[serde_as(as = "DisplayFromStr")]
    pub network_address: NetworkAddress,
    /// The public key of the storage node.
    #[serde(deserialize_with = "deserialize_bls_key")]
    pub public_key: PublicKey,
    /// The network key of the storage node.
    #[serde(deserialize_with = "deserialize_network_key")]
    pub network_public_key: NetworkPublicKey,
    /// The indices of the shards held by the storage node.
    pub shard_ids: Vec<ShardIndex>,
}

impl AssociatedContractStruct for StorageNode {
    const CONTRACT_STRUCT: StructTag<'static> = contracts::storage_node::StorageNodeInfo;
}

#[instrument(err, skip_all)]
fn deserialize_bls_key<'de, D>(deserializer: D) -> Result<PublicKey, D::Error>
where
    D: Deserializer<'de>,
{
    let key: Vec<u8> = Deserialize::deserialize(deserializer)?;
    PublicKey::from_bytes(&key).map_err(D::Error::custom)
}

#[instrument(err, skip_all)]
fn deserialize_network_key<'de, D>(deserializer: D) -> Result<NetworkPublicKey, D::Error>
where
    D: Deserializer<'de>,
{
    let key: Vec<u8> = Deserialize::deserialize(deserializer)?;
    NetworkPublicKey::from_bytes(&key).map_err(D::Error::custom)
}

/// Sui type for storage committee
#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
pub struct Committee {
    /// The current epoch
    pub epoch: Epoch,
    /// The members of the committee
    // INV: `!members.is_empty()`
    // INV: `members.iter().all(|m| !m.shard_ids.is_empty())`
    members: Vec<StorageNode>,
    /// The number of shards held by the committee
    n_shards: NonZeroU16,
}

impl Committee {
    /// Create a new committee for `epoch` consisting of `members`.
    ///
    /// `members` must contain at least one storage node holding at least one shard.
    pub fn new(members: Vec<StorageNode>, epoch: Epoch) -> Result<Self, InvalidCommittee> {
        let mut n_shards = 0;
        for node in members.iter() {
            let node_shards = node.shard_ids.len();
            ensure!(node_shards > 0, InvalidCommittee::EmptyNode,);
            n_shards += node_shards
        }
        let n_shards = u16::try_from(n_shards).map_err(|_| InvalidCommittee::TooManyShards)?;
        let n_shards = NonZeroU16::new(n_shards).ok_or(InvalidCommittee::EmptyCommittee)?;
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

    /// Return the shards handed by the specified storage node,
    /// based on its index in the committee list.
    pub fn shards_for_node(&self, node_id: usize) -> Vec<ShardIndex> {
        self.members
            .get(node_id)
            .map(|node| node.shard_ids.clone())
            .unwrap_or_default()
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
}

impl AssociatedContractStruct for Committee {
    const CONTRACT_STRUCT: StructTag<'static> = contracts::committee::Committee;
}

/// Error returned for an invalid conversion to an EpochStatus.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("the provided value is not a valid EpochStatus")]
pub struct InvalidEpochStatus;

/// Error returned when trying to create a committee with no shards.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum InvalidCommittee {
    #[error("too many shards for a valid committee")]
    /// Error resulting if the total number of shards in the committee exceeds u16::MAX.
    TooManyShards,
    #[error("trying to create an empty committee")]
    /// Error resulting if the committee contains no shards.
    EmptyCommittee,
    /// Error resulting if one of the nodes has no shards.
    #[error("trying to create a committee with an empty node")]
    EmptyNode,
}

/// The status of the epoch
#[derive(Debug, PartialEq, Eq, Clone, Copy, Deserialize)]
#[repr(u8)]
pub enum EpochStatus {
    /// A sufficient number of the new epoch shards have been transferred
    Done = 0,
    /// The storage nodes are currently transferring shards to the new committee
    Sync = 1,
}

impl From<EpochStatus> for u8 {
    fn from(value: EpochStatus) -> Self {
        value as u8
    }
}

impl TryFrom<u8> for EpochStatus {
    type Error = InvalidEpochStatus;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(EpochStatus::Done),
            1 => Ok(EpochStatus::Sync),
            _ => Err(InvalidEpochStatus),
        }
    }
}

/// Holds information about a future epoch, namely how much
/// storage needs to be reclaimed and the rewards to be distributed.
#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
pub struct FutureAccounting {
    epoch: u64,
    storage_to_reclaim: u64,
    rewards_to_distribute: u64,
}

/// A ring buffer holding future accounts for a continuous range of epochs.
#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
pub struct FutureAccountingRingBuffer {
    current_index: u64,
    length: u64,
    ring_buffer: Vec<FutureAccounting>,
}

impl FutureAccountingRingBuffer {
    /// Creates an empty accounting ring buffer with length 0.
    pub fn empty() -> Self {
        FutureAccountingRingBuffer {
            current_index: 0,
            length: 0,
            ring_buffer: vec![],
        }
    }
}

/// Sui type for system object
#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
pub struct SystemObject {
    /// Object id of the Sui object
    pub id: ObjectID,
    /// The current committee of the Walrus instance
    pub current_committee: Option<Committee>,
    /// The status of the epoch
    pub epoch_status: EpochStatus,
    /// Total storage capacity of the Walrus instance
    pub total_capacity_size: u64,
    /// Used storage capacity of the Walrus instance
    pub used_capacity_size: u64,
    /// The price per unit of storage per epoch
    pub price_per_unit_size: u64,
    /// The object ID of the table storing past committees
    #[serde(deserialize_with = "deserialize_table")]
    pub past_committees_object: ObjectID,
    /// The future accounting ring buffer to keep track of future rewards.
    pub future_accounting: FutureAccountingRingBuffer,
}

impl AssociatedContractStruct for SystemObject {
    const CONTRACT_STRUCT: StructTag<'static> = contracts::system::System;
}

#[instrument(err, skip_all)]
fn deserialize_table<'de, D>(deserializer: D) -> Result<ObjectID, D::Error>
where
    D: Deserializer<'de>,
{
    let (object_id, _length): (ObjectID, u64) = Deserialize::deserialize(deserializer)?;
    Ok(object_id)
}

// Events

fn ensure_event_type(
    sui_event: &SuiEvent,
    struct_tag: &StructTag,
) -> Result<(), MoveConversionError> {
    ensure!(
        sui_event.type_.name.as_str() == struct_tag.name
            && sui_event.type_.module.as_str() == struct_tag.module,
        MoveConversionError::TypeMismatch {
            expected: struct_tag.to_string(),
            actual: format!(
                "{}::{}",
                sui_event.type_.module.as_str(),
                sui_event.type_.name.as_str()
            ),
        }
    );
    Ok(())
}

/// Sui event that blob has been registered.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct BlobRegistered {
    /// The epoch in which the blob has been registered.
    pub epoch: Epoch,
    /// The blob ID.
    pub blob_id: BlobId,
    /// The (unencoded) size of the blob.
    pub size: u64,
    /// The erasure coding type used for the blob.
    pub erasure_code_type: EncodingType,
    /// The end epoch of the associated storage resource (exclusive).
    pub end_epoch: Epoch,
    /// The ID of the event.
    pub event_id: EventID,
}

impl AssociatedSuiEvent for BlobRegistered {
    const EVENT_STRUCT: StructTag<'static> = contracts::blob_events::BlobRegistered;
}

impl TryFrom<SuiEvent> for BlobRegistered {
    type Error = MoveConversionError;

    fn try_from(sui_event: SuiEvent) -> Result<Self, Self::Error> {
        ensure_event_type(&sui_event, &Self::EVENT_STRUCT)?;

        #[derive(Deserialize)]
        struct Inner {
            epoch: Epoch,
            blob_id: BlobId,
            size: u64,
            erasure_code_type: EncodingType,
            end_epoch: Epoch,
        }

        let inner: Inner = bcs::from_bytes(&sui_event.bcs)?;
        Ok(Self {
            epoch: inner.epoch,
            blob_id: inner.blob_id,
            size: inner.size,
            erasure_code_type: inner.erasure_code_type,
            end_epoch: inner.end_epoch,
            event_id: sui_event.id,
        })
    }
}

/// Sui event that blob has been certified.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct BlobCertified {
    /// The epoch in which the blob was certified.
    pub epoch: Epoch,
    /// The blob ID.
    pub blob_id: BlobId,
    /// The end epoch of the associated storage resource (exclusive).
    pub end_epoch: Epoch,
    /// The ID of the event.
    pub event_id: EventID,
}

impl AssociatedSuiEvent for BlobCertified {
    const EVENT_STRUCT: StructTag<'static> = contracts::blob_events::BlobCertified;
}

impl TryFrom<SuiEvent> for BlobCertified {
    type Error = MoveConversionError;

    fn try_from(sui_event: SuiEvent) -> Result<Self, Self::Error> {
        ensure_event_type(&sui_event, &Self::EVENT_STRUCT)?;

        #[derive(Deserialize)]
        struct Inner {
            epoch: Epoch,
            blob_id: BlobId,
            end_epoch: Epoch,
        }

        let inner: Inner = bcs::from_bytes(&sui_event.bcs)?;
        Ok(Self {
            epoch: inner.epoch,
            blob_id: inner.blob_id,
            end_epoch: inner.end_epoch,
            event_id: sui_event.id,
        })
    }
}

/// Sui event that a blob ID is invalid.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct InvalidBlobId {
    /// The epoch in which the blob was marked as invalid.
    pub epoch: Epoch,
    /// The blob ID.
    pub blob_id: BlobId,
    /// The ID of the event.
    pub event_id: EventID,
}

impl AssociatedSuiEvent for InvalidBlobId {
    const EVENT_STRUCT: StructTag<'static> = contracts::blob_events::InvalidBlobID;
}

impl TryFrom<SuiEvent> for InvalidBlobId {
    type Error = MoveConversionError;

    fn try_from(sui_event: SuiEvent) -> Result<Self, Self::Error> {
        ensure_event_type(&sui_event, &Self::EVENT_STRUCT)?;

        #[derive(Deserialize)]
        struct Inner {
            epoch: Epoch,
            blob_id: BlobId,
        }

        let inner: Inner = bcs::from_bytes(&sui_event.bcs)?;
        Ok(Self {
            epoch: inner.epoch,
            blob_id: inner.blob_id,
            event_id: sui_event.id,
        })
    }
}

/// Enum to wrap blob events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlobEvent {
    /// A registration event.
    Registered(BlobRegistered),
    /// A certification event.
    Certified(BlobCertified),
    /// An invalid blob ID event.
    InvalidBlobID(InvalidBlobId),
}

impl From<BlobRegistered> for BlobEvent {
    fn from(value: BlobRegistered) -> Self {
        Self::Registered(value)
    }
}

impl From<BlobCertified> for BlobEvent {
    fn from(value: BlobCertified) -> Self {
        Self::Certified(value)
    }
}

impl From<InvalidBlobId> for BlobEvent {
    fn from(value: InvalidBlobId) -> Self {
        Self::InvalidBlobID(value)
    }
}

impl BlobEvent {
    /// Returns the blob ID contained in the wrapped event.
    pub fn blob_id(&self) -> BlobId {
        match self {
            BlobEvent::Registered(event) => event.blob_id,
            BlobEvent::Certified(event) => event.blob_id,
            BlobEvent::InvalidBlobID(event) => event.blob_id,
        }
    }

    /// Returns the event ID of the wrapped event.
    pub fn event_id(&self) -> EventID {
        match self {
            BlobEvent::Registered(event) => event.event_id,
            BlobEvent::Certified(event) => event.event_id,
            BlobEvent::InvalidBlobID(event) => event.event_id,
        }
    }
}

impl TryFrom<SuiEvent> for BlobEvent {
    type Error = anyhow::Error;

    fn try_from(value: SuiEvent) -> Result<Self, Self::Error> {
        match (&value.type_).into() {
            contracts::blob_events::BlobRegistered => Ok(BlobEvent::Registered(value.try_into()?)),
            contracts::blob_events::BlobCertified => Ok(BlobEvent::Certified(value.try_into()?)),
            contracts::blob_events::InvalidBlobID => {
                Ok(BlobEvent::InvalidBlobID(value.try_into()?))
            }
            _ => Err(anyhow!("could not convert event: {}", value)),
        }
    }
}
