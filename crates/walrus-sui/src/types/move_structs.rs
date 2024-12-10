// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Walrus move type bindings. Replicates the move types in Rust.

use std::{fmt::Display, num::NonZeroU16};

use fastcrypto::traits::ToFromBytes;
use serde::{
    de::{DeserializeOwned, Error},
    Deserialize,
    Deserializer,
    Serialize,
    Serializer,
};
use sui_types::{base_types::ObjectID, messages_checkpoint::CheckpointSequenceNumber};
use walrus_core::{BlobId, EncodingType, Epoch, NetworkPublicKey, PublicKey, ShardIndex};

use super::NetworkAddress;
use crate::contracts::{self, AssociatedContractStruct, StructTag};

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
    pub registered_epoch: Epoch,
    /// The blob ID.
    // TODO(#795): Consider serialize as human readable string.
    #[serde(serialize_with = "serialize_blob_id")]
    pub blob_id: BlobId,
    /// The (unencoded) size of the blob.
    pub size: u64,
    /// The encoding coding type used for the blob.
    pub encoding_type: EncodingType,
    /// The epoch in which the blob was first certified, `None` if the blob is uncertified.
    pub certified_epoch: Option<Epoch>,
    /// The [`StorageResource`] used to store the blob.
    pub storage: StorageResource,
    /// Marks the blob as deletable.
    pub deletable: bool,
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

/// Event blob attestation.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct EventBlobAttestation {
    /// Last Sui checkpoint sequence number of the event blob.
    pub checkpoint_sequence_num: CheckpointSequenceNumber,
    /// Walrus epoch in which blob is attested.
    pub epoch: Epoch,
}

/// Sui type for storage node.
#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
pub struct StorageNode {
    /// Name of the storage node.
    pub name: String,
    /// The node id.
    pub node_id: ObjectID,
    /// The network address of the storage node.
    pub network_address: NetworkAddress,
    /// The public key of the storage node.
    #[serde(deserialize_with = "deserialize_public_key")]
    pub public_key: PublicKey,
    /// The public key of the storage node for the next epoch.
    #[serde(deserialize_with = "deserialize_public_key_option")]
    pub next_epoch_public_key: Option<PublicKey>,
    /// The network key of the storage node.
    #[serde(deserialize_with = "deserialize_public_key")]
    pub network_public_key: NetworkPublicKey,
    /// The indices of the shards held by the storage node.
    #[serde(default, skip_deserializing)]
    pub shard_ids: Vec<ShardIndex>,
}

impl AssociatedContractStruct for StorageNode {
    const CONTRACT_STRUCT: StructTag<'static> = contracts::storage_node::StorageNodeInfo;
}

#[tracing::instrument(err, skip_all)]
fn deserialize_public_key<'de, D, K>(deserializer: D) -> Result<K, D::Error>
where
    D: Deserializer<'de>,
    K: ToFromBytes,
{
    let key: Vec<u8> = Deserialize::deserialize(deserializer)?;
    K::from_bytes(&key).map_err(D::Error::custom)
}

#[tracing::instrument(err, skip_all)]
fn deserialize_public_key_option<'de, D, K>(deserializer: D) -> Result<Option<K>, D::Error>
where
    D: Deserializer<'de>,
    K: ToFromBytes,
{
    let key: Option<Vec<u8>> = Deserialize::deserialize(deserializer)?;
    key.map(|bytes| K::from_bytes(&bytes).map_err(D::Error::custom))
        .transpose()
}

/// Sui type for the capability that authorizes the holder to perform certain actions.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct StorageNodeCap {
    /// The object ID of the capability.
    pub id: ObjectID,
    /// The node ID of the node.
    pub node_id: ObjectID,
    /// The latest epoch in which the node sent an "epoch sync done" message to the contract.
    pub last_epoch_sync_done: Epoch,
    /// The last event blob attestation from the storage node.
    pub last_event_blob_attestation: Option<EventBlobAttestation>,
}

impl AssociatedContractStruct for StorageNodeCap {
    const CONTRACT_STRUCT: StructTag<'static> = contracts::storage_node::StorageNodeCap;
}

impl Display for StorageNodeCap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "StorageNodeCap: object ID: {}, node ID: {}, last_epoch_sync_done: {}",
            self.id, self.node_id, self.last_epoch_sync_done
        )
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
enum PoolState {
    #[cfg(not(feature = "mainnet-contracts"))]
    // The pool is new and awaits the stake to be added.
    New,
    // The pool is active and can accept stakes.
    Active,
    // The pool awaits the stake to be withdrawn. The value inside the
    // variant is the epoch in which the pool will be withdrawn.
    Withdrawing(u32),
    // The pool is empty and can be destroyed.
    Withdrawn,
}

/// The parameters for the staking pool. Stored for the next epoch.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct VotingParams {
    /// Voting: storage price for the next epoch.
    pub storage_price: u64,
    /// Voting: write price for the next epoch.
    pub write_price: u64,
    /// Voting: node capacity for the next epoch.
    pub node_capacity: u64,
}

#[cfg(feature = "mainnet-contracts")]
#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
/// The receiver of the commission.
enum CommissionReceiver {
    /// Address receiver.
    Address(ObjectID),
    /// Object receiver.
    ObjectID(ObjectID),
}

/// Represents a single staking pool.
#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
pub(crate) struct StakingPool {
    id: ObjectID,
    /// The current state of the pool.
    state: PoolState,
    /// Current epoch's pool parameters.
    voting_params: VotingParams,
    /// The storage node info for the pool.
    pub node_info: StorageNode,
    /// The epoch when the pool is / will be activated.
    activation_epoch: Epoch,
    /// Epoch when the pool was last updated.
    last_update_epoch: Epoch,
    /// Currently active stake.
    active_stake: u64,
    /// Pool token balance.
    pool_token_balance: u64,
    /// Pending withdrawals from the pool token balance indexed by epoch.
    pending_pool_token_withdraw: Vec<(Epoch, u64)>,
    #[cfg(feature = "mainnet-contracts")]
    /// Pending early withdrawals for which we cannot calculate the pool tokens.
    pending_early_withdrawals: Vec<(Epoch, u64)>,
    #[cfg(feature = "mainnet-contracts")]
    /// Pending commission rate changes indexed by epoch.
    pending_commission_rate: Vec<(Epoch, u64)>,
    #[cfg(not(feature = "mainnet-contracts"))]
    /// The commission rate for the pool.
    commission_rate: u64,
    #[cfg(feature = "mainnet-contracts")]
    /// The commission rate for the pool.
    commission_rate: u16,
    /// Exchange rates table ID.
    #[serde(deserialize_with = "deserialize_bag_or_table")]
    exchange_rates: ObjectID,
    /// The amount of stake that will be added to the `active_stake` in the given epoch.
    pending_stake: Vec<(Epoch, u64)>,
    /// The rewards that the pool has received.
    rewards: u64,
    #[cfg(feature = "mainnet-contracts")]
    /// Collected commission.
    commission: u64,
    #[cfg(feature = "mainnet-contracts")]
    /// The receiver of the commission.
    commission_receiver: CommissionReceiver,
    #[cfg(feature = "mainnet-contracts")]
    #[serde(deserialize_with = "deserialize_bag_or_table")]
    extra_fields: ObjectID,
}

impl AssociatedContractStruct for StakingPool {
    const CONTRACT_STRUCT: StructTag<'static> = contracts::staking_pool::StakingPool;
}

/// Holds information about a future epoch, namely how much
/// storage needs to be reclaimed and the rewards to be distributed.
#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
pub struct FutureAccounting {
    epoch: Epoch,
    storage_to_reclaim: u64,
    rewards_to_distribute: u64,
}

/// A ring buffer holding future accounts for a continuous range of epochs.
#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
pub struct FutureAccountingRingBuffer {
    current_index: u32,
    length: u32,
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

    /// Returns the `length` field of the ring buffer.
    pub fn length(&self) -> u32 {
        self.length
    }
}

/// Event blob.
#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
pub struct EventBlob {
    /// The blob ID.
    // TODO(#795): Consider serialize as human readable string.
    #[serde(serialize_with = "serialize_blob_id")]
    pub blob_id: BlobId,
    /// Sui checkpoint sequence number of the last event.
    pub ending_checkpoint_sequence_number: u64,
}

/// State holding the certification of event blobs.
#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
pub struct EventBlobCertificationState {
    /// The object ID of the inner object.
    pub id: ObjectID,
    /// Latest certified blob
    pub latest_certified_blob: Option<EventBlob>,
    /// Total weight of the blobs undergoing certification.
    pub aggregate_weight_per_blob: Vec<(BlobId, u16)>,
}

/// Sui type for staking object
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct StakingObject {
    /// Object id of the Sui object.
    pub id: ObjectID,
    /// The version of the system object.
    pub version: u64,
    /// The inner system state.
    pub(crate) inner: StakingInnerV1,
}

/// Sui type for outer staking object. Used for deserialization.
#[derive(Debug, Deserialize)]
pub(crate) struct StakingObjectForDeserialization {
    pub(crate) id: ObjectID,
    pub(crate) version: u64,
}

impl AssociatedContractStruct for StakingObjectForDeserialization {
    const CONTRACT_STRUCT: StructTag<'static> = contracts::staking::Staking;
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
pub(crate) struct ActiveSet {
    /// The maximum number of storage nodes in the active set.
    /// Potentially remove this field.
    max_size: u16,
    /// The minimum amount of staked WAL needed to enter the active set. This is used to
    /// determine if a storage node can be added to the active set.
    threshold_stake: u64,
    /// The amount of staked WAL for each storage node in the active set.
    pub(crate) nodes: Vec<(ObjectID, u64)>,
    /// The total amount of staked WAL in the active set.
    total_stake: u64,
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
pub(crate) struct EpochParams {
    /// The storage capacity of the system.
    total_capacity_size: u64,
    /// The price per unit size of storage.
    storage_price_per_unit_size: u64,
    /// The write price per unit size.
    write_price_per_unit_size: u64,
}

/// The epoch state.
#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
pub enum EpochState {
    /// The epoch change is currently in progress.
    ///
    /// Contains the weight of the nodes that have already attested that they finished the sync.
    EpochChangeSync(u16),
    /// The epoch change has been completed at the contained timestamp.
    #[serde(deserialize_with = "chrono::serde::ts_milliseconds::deserialize")]
    EpochChangeDone(chrono::DateTime<chrono::Utc>),
    /// The parameters for the next epoch have been selected.
    ///
    /// The contained timestamp is the start of the current epoch.
    #[serde(deserialize_with = "chrono::serde::ts_milliseconds::deserialize")]
    NextParamsSelected(chrono::DateTime<chrono::Utc>),
}

impl EpochState {
    /// Returns `true` if an epoch change is in progress.
    pub fn is_transitioning(&self) -> bool {
        matches!(self, Self::EpochChangeSync(_))
    }
}

/// The committee shard assignment.
type CommitteeShardAssignment = Vec<(ObjectID, Vec<u16>)>;

/// Sui type for inner staking object
#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
pub(crate) struct StakingInnerV1 {
    /// The object ID
    pub(crate) id: ObjectID,
    /// The number of shards in the system.
    pub(crate) n_shards: NonZeroU16,
    /// The duration of an epoch in ms. Does not affect the first (zero) epoch.
    pub(crate) epoch_duration: u64,
    /// Special parameter, used only for the first epoch. The timestamp when the
    /// first epoch can be started.
    pub(crate) first_epoch_start: u64,
    /// Object ID of the object table storing the staking pools.
    #[serde(deserialize_with = "deserialize_bag_or_table")]
    pub(crate) pools: ObjectID,
    /// The current epoch of the Walrus system.
    pub(crate) epoch: Epoch,
    /// Stores the active set of storage nodes. Provides automatic sorting and
    /// tracks the total amount of staked WAL.
    pub(crate) active_set: ActiveSet,
    /// The next committee in the system.
    pub(crate) next_committee: Option<CommitteeShardAssignment>,
    /// The current committee in the system.
    pub(crate) committee: CommitteeShardAssignment,
    /// The previous committee in the system.
    pub(crate) previous_committee: CommitteeShardAssignment,
    /// The next epoch parameters.
    pub(crate) next_epoch_params: Option<EpochParams>,
    /// The state of the current epoch.
    pub(crate) epoch_state: EpochState,
    /// Rewards left over from the previous epoch that couldn't be distributed due to rounding.
    pub(crate) leftover_rewards: u64,
}

impl AssociatedContractStruct for StakingInnerV1 {
    const CONTRACT_STRUCT: StructTag<'static> = contracts::staking_inner::StakingInnerV1;
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
pub(crate) struct BlsCommitteeMember {
    #[serde(deserialize_with = "deserialize_public_key")]
    public_key: PublicKey,
    weight: u16,
    node_id: ObjectID,
}

/// Sui type for BlsCommittee.
#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
pub(crate) struct BlsCommittee {
    /// The members of the committee
    members: Vec<BlsCommitteeMember>,
    /// The number of shards held by the committee
    n_shards: u16,
    /// The current epoch
    epoch: Epoch,
}

/// Sui type for system object
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SystemObject {
    /// Object id of the Sui object.
    pub id: ObjectID,
    /// The version of the system object.
    pub version: u64,
    /// The inner system state.
    pub(crate) inner: SystemStateInnerV1,
}

/// Sui type for outer system object. Used for deserialization.
#[derive(Debug, Deserialize)]
pub(crate) struct SystemObjectForDeserialization {
    pub(crate) id: ObjectID,
    pub(crate) version: u64,
}
impl AssociatedContractStruct for SystemObjectForDeserialization {
    const CONTRACT_STRUCT: StructTag<'static> = contracts::system::System;
}

/// Sui type for inner system object.
#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
pub(crate) struct SystemStateInnerV1 {
    /// The object ID of the inner object.
    pub id: ObjectID,
    /// The current committee of the Walrus instance.
    pub committee: BlsCommittee,
    /// Total storage capacity of the Walrus instance.
    pub total_capacity_size: u64,
    /// Used storage capacity of the Walrus instance.
    pub used_capacity_size: u64,
    /// The price per unit of storage per epoch.
    pub storage_price_per_unit_size: u64,
    /// The write price per unit.
    pub write_price_per_unit_size: u64,
    /// The future accounting ring buffer to keep track of future rewards.
    pub future_accounting: FutureAccountingRingBuffer,
    /// Event blob certification state.
    pub event_blob_certification_state: EventBlobCertificationState,
}

impl AssociatedContractStruct for SystemStateInnerV1 {
    const CONTRACT_STRUCT: StructTag<'static> = contracts::system_state_inner::SystemStateInnerV1;
}

#[tracing::instrument(err, skip_all)]
fn deserialize_bag_or_table<'de, D>(deserializer: D) -> Result<ObjectID, D::Error>
where
    D: Deserializer<'de>,
{
    let (object_id, _length): (ObjectID, u64) = Deserialize::deserialize(deserializer)?;
    Ok(object_id)
}

/// The state of the staked WAL.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum StakedWalState {
    /// The WAL is staked.
    Staked,
    #[cfg(not(feature = "mainnet-contracts"))]
    /// The WAL is unstaked and can be withdrawn.
    Withdrawing(Epoch, u64),
    #[cfg(feature = "mainnet-contracts")]
    /// The WAL is unstaked and can be withdrawn.
    Withdrawing(Epoch, Option<u64>),
}

impl Display for StakedWalState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StakedWalState::Staked => write!(f, "Staked"),
            #[cfg(not(feature = "mainnet-contracts"))]
            StakedWalState::Withdrawing(epoch, amount) => {
                write!(f, "Withdrawing: epoch={}, amount={}", epoch, amount)
            }
            #[cfg(feature = "mainnet-contracts")]
            StakedWalState::Withdrawing(epoch, Some(amount)) => {
                write!(
                    f,
                    "Withdrawing: epoch={}, pool token amount={:?}",
                    epoch, amount
                )
            }
            #[cfg(feature = "mainnet-contracts")]
            StakedWalState::Withdrawing(epoch, None) => {
                write!(f, "Withdrawing: epoch={}, pool token amount=Unknown", epoch)
            }
        }
    }
}

/// Sui type for the StakedWal object.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct StakedWal {
    /// The object ID of the staked WAL.
    pub id: ObjectID,
    /// The state of the staked WAL.
    pub state: StakedWalState,
    /// The node ID of the staked WAL.
    pub node_id: ObjectID,
    /// The principal of the staked WAL.
    pub principal: u64,
    /// The epoch in which the WAL was staked.
    pub activation_epoch: Epoch,
}

impl AssociatedContractStruct for StakedWal {
    const CONTRACT_STRUCT: StructTag<'static> = contracts::staked_wal::StakedWal;
}

impl Display for StakedWal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "StakedWal:")?;
        writeln!(f, "    object id: {},", self.id)?;
        writeln!(f, "    state: {},", self.state)?;
        writeln!(f, "    node_id: {},", self.node_id)?;
        writeln!(f, "    principal: {},", self.principal)?;
        writeln!(f, "    activation_epoch: {}", self.activation_epoch)
    }
}

/// Sui type for an exchange rate: `wal` WAL = `sui` SUI.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct ExchangeRate {
    wal: u64,
    sui: u64,
}

/// Sui type for an exchange that allows exchanging SUI for WAL at a fixed exchange rate.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub(crate) struct WalExchange {
    id: ObjectID,
    wal_balance: u64,
    sui_balance: u64,
    pub exchange_rate: ExchangeRate,
    admin: ObjectID,
}

impl AssociatedContractStruct for WalExchange {
    const CONTRACT_STRUCT: StructTag<'static> = contracts::wal_exchange::Exchange;
}

impl ExchangeRate {
    /// Converts an amount of SUI to WAL.
    pub fn sui_to_wal(&self, amount: u64) -> u64 {
        amount * self.wal / self.sui
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
/// Sui type for a dynamic field.
pub struct SuiDynamicField<N, V> {
    /// The object ID of the dynamic field.
    pub id: ObjectID,
    /// The name of the dynamic field.
    pub name: N,
    /// The value of the dynamic field.
    pub value: V,
}

impl<N, V> AssociatedContractStruct for SuiDynamicField<N, V>
where
    N: DeserializeOwned,
    V: DeserializeOwned,
{
    const CONTRACT_STRUCT: StructTag<'static> = contracts::dynamic_field::Field;
}
