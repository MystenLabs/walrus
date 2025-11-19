// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Walrus move type bindings. Replicates the move types in Rust.

use std::{
    fmt::Display,
    num::NonZeroU16,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use chrono::{DateTime, Utc};
use fastcrypto::traits::ToFromBytes;
use serde::{
    Deserialize,
    Deserializer,
    Serialize,
    Serializer,
    de::{DeserializeOwned, Error},
};
pub use sui_types::base_types::ObjectID;
use sui_types::{
    base_types::SuiAddress,
    collection_types::{Entry, VecMap},
    messages_checkpoint::CheckpointSequenceNumber,
};
#[cfg(feature = "utoipa")]
use utoipa::openapi::schema;
use walrus_core::{
    BlobId,
    EncodingType,
    Epoch,
    NetworkPublicKey,
    PublicKey,
    ShardIndex,
    messages::BlobPersistenceType,
};

use super::NetworkAddress;
use crate::contracts::{
    self,
    AssociatedContractStruct,
    AssociatedContractStructWithPkgId,
    StructTag,
};

/// Sui object for storage resources.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct StorageResource {
    /// Object ID of the Sui object.
    #[cfg_attr(feature = "utoipa", schema(schema_with = object_id_schema))]
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
    #[cfg_attr(feature = "utoipa", schema(schema_with = object_id_schema))]
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

impl Blob {
    /// Returns the blob persistence type of the blob object.
    pub fn blob_persistence_type(&self) -> BlobPersistenceType {
        if self.deletable {
            BlobPersistenceType::Deletable {
                object_id: self.id.into(),
            }
        } else {
            BlobPersistenceType::Permanent
        }
    }

    /// Returns true if the blob is certified.
    pub fn is_certified(&self) -> bool {
        self.certified_epoch.is_some()
    }
}

/// Serialize as string to make sure that the json output uses the base64 encoding.
fn serialize_blob_id<S>(blob_id: &BlobId, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.collect_str(blob_id)
}

#[cfg(feature = "utoipa")]
fn object_id_schema() -> schema::Ref {
    schema::Ref::new("#/components/schemas/ObjectID")
}

impl AssociatedContractStruct for Blob {
    const CONTRACT_STRUCT: StructTag<'static> = contracts::blob::Blob;
}

/// The attribute struct for Blob objects.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct BlobAttribute {
    /// The metadata key-value pairs.
    pub metadata: VecMap<String, String>,
}

impl Default for BlobAttribute {
    fn default() -> Self {
        Self {
            metadata: VecMap { contents: vec![] },
        }
    }
}

impl<I, K, V> From<I> for BlobAttribute
where
    I: IntoIterator<Item = (K, V)>,
    K: Into<String>,
    V: Into<String>,
{
    fn from(iter: I) -> Self {
        let contents = iter
            .into_iter()
            .map(|(key, value)| Entry {
                key: key.into(),
                value: value.into(),
            })
            .collect();

        Self {
            metadata: VecMap { contents },
        }
    }
}

impl BlobAttribute {
    /// Insert a key-value pair into the metadata.
    pub fn insert(&mut self, key: String, value: String) {
        if let Some(idx) = self.metadata.contents.iter().position(|e| e.key == key) {
            self.metadata.contents[idx].value = value;
        } else {
            self.metadata.contents.push(Entry { key, value });
        }
    }

    /// Get a value from the metadata.
    pub fn get<T: AsRef<str>>(&self, key: T) -> Option<&str> {
        self.metadata
            .contents
            .iter()
            .find(|e| e.key == key.as_ref())
            .map(|e| e.value.as_str())
    }

    /// Returns an iterator over the key-value pairs in the metadata.
    pub fn iter(&self) -> MetadataIter<'_> {
        MetadataIter {
            inner: self.metadata.contents.iter(),
        }
    }

    /// Returns the number of key-value pairs in the attribute.
    pub fn len(&self) -> usize {
        self.metadata.contents.len()
    }

    /// Returns `true` if the attribute is empty.
    pub fn is_empty(&self) -> bool {
        self.metadata.contents.is_empty()
    }
}

/// Iterator for BlobAttribute key-value pairs.
#[derive(Debug)]
pub struct MetadataIter<'a> {
    inner: std::slice::Iter<'a, Entry<String, String>>,
}

impl<'a> Iterator for MetadataIter<'a> {
    type Item = (&'a String, &'a String);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|entry| (&entry.key, &entry.value))
    }
}

impl<'a> IntoIterator for &'a BlobAttribute {
    type Item = (&'a String, &'a String);
    type IntoIter = MetadataIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl AssociatedContractStruct for BlobAttribute {
    const CONTRACT_STRUCT: StructTag<'static> = contracts::metadata::Metadata;
}

/// A blob with its metadata.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct BlobWithAttribute {
    /// The blob.
    pub blob: Blob,
    /// The attribute associated with the blob.
    pub attribute: Option<BlobAttribute>,
}

/// Event blob attestation.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct EventBlobAttestation {
    /// Last Sui checkpoint sequence number of the event blob.
    pub checkpoint_sequence_num: CheckpointSequenceNumber,
    /// Walrus epoch in which blob is attested.
    pub epoch: Epoch,
}

/// Node metadata.
#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize, Default)]
#[serde(default)]
pub struct NodeMetadata {
    /// The image URL of the storage node.
    pub image_url: String,
    /// The project URL of the storage node.
    pub project_url: String,
    /// The description of the storage node.
    pub description: String,
    /// Extra fields of the storage node for future use.
    extra_fields: Vec<(String, String)>,
}

impl NodeMetadata {
    /// Creates a new node metadata object.
    pub fn new(image_url: String, project_url: String, description: String) -> Self {
        Self {
            image_url,
            project_url,
            description,
            extra_fields: vec![],
        }
    }
}

impl AssociatedContractStruct for NodeMetadata {
    const CONTRACT_STRUCT: StructTag<'static> = contracts::node_metadata::NodeMetadata;
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
    /// The metadata for the pool.
    pub metadata: ObjectID,
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
    /// The root of the deny list.
    pub deny_list_root: [u8; 32],
    /// The sequence number of the deny list.
    pub deny_list_sequence_number: u64,
    /// The size of the deny list.
    pub deny_list_size: u64,
}

impl StorageNodeCap {
    /// Creates a new `StorageNodeCap` for testing.
    pub fn new_for_testing() -> Self {
        Self {
            id: ObjectID::random(),
            node_id: ObjectID::random(),
            last_epoch_sync_done: 1,
            last_event_blob_attestation: None,
            deny_list_root: [0; 32],
            deny_list_sequence_number: 0,
            deny_list_size: 0,
        }
    }
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

/// Sui type for the emergency upgrade capability.
#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
pub struct EmergencyUpgradeCap {
    /// The object ID of the capability.
    pub id: ObjectID,
    /// The object ID of the upgrade manager.
    pub upgrade_manager_id: ObjectID,
}

impl AssociatedContractStruct for EmergencyUpgradeCap {
    const CONTRACT_STRUCT: StructTag<'static> = contracts::upgrade::EmergencyUpgradeCap;
}

impl Display for EmergencyUpgradeCap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EmergencyUpgradeCap: object ID: {}, upgrade manager ID: {}",
            self.id, self.upgrade_manager_id
        )
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
enum PoolState {
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

#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
/// The authorized object or address for commission or governance.
pub enum Authorized {
    /// Address receiver.
    Address(SuiAddress),
    /// Object receiver.
    Object(ObjectID),
}

impl Display for Authorized {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Address(address) => write!(f, "sui address {address}"),
            Self::Object(object) => write!(f, "object id {object}"),
        }
    }
}

/// Represents a single staking pool.
#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
pub struct StakingPool {
    /// The object ID of the staking pool, also known as the node ID.
    pub id: ObjectID,
    /// The current state of the pool.
    state: PoolState,
    /// Current epoch's pool parameters.
    pub voting_params: VotingParams,
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
    /// Pending early withdrawals for which we cannot calculate the pool tokens.
    pending_early_withdrawals: Vec<(Epoch, u64)>,
    /// Pending commission rate changes indexed by epoch.
    pub pending_commission_rate: Vec<(Epoch, u64)>,
    /// The commission rate for the pool.
    pub commission_rate: u16,
    /// Exchange rates table ID.
    #[serde(deserialize_with = "deserialize_bag_or_table")]
    exchange_rates: ObjectID,
    /// The amount of stake that will be added to the `active_stake` in the given epoch.
    pending_stake: Vec<(Epoch, u64)>,
    /// The rewards that the pool has received.
    rewards: u64,
    /// Collected commission.
    pub commission: u64,
    /// The receiver of the commission.
    pub commission_receiver: Authorized,
    /// The authorization object to vote for governance actions, e.g. upgrade the contract.
    pub governance_authorized: Authorized,
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
    /// Latest certified blob
    pub latest_certified_blob: Option<EventBlob>,
    /// Total weight of the blobs undergoing certification.
    pub aggregate_weight_per_blob: Vec<(EventBlob, u16)>,
}

/// State holding the certification of event blobs.
#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
pub struct EventBlobCertificationStateTestnet {
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
    /// The version of the staking object.
    pub version: u64,
    /// The package ID of the staking object.
    pub package_id: ObjectID,
    /// The new package ID of the staking object.
    pub(crate) new_package_id: Option<ObjectID>,
    /// The inner staking state.
    pub(crate) inner: StakingInnerV1,
}

impl StakingObject {
    /// Returns the number of epochs ahead that can be used to extend a blob.
    pub fn epoch_state(&self) -> &EpochState {
        &self.inner.epoch_state
    }

    /// Returns the epoch duration in milliseconds.
    pub fn epoch_duration_millis(&self) -> u64 {
        self.inner.epoch_duration
    }

    /// Returns the epoch duration.
    pub fn epoch_duration(&self) -> Duration {
        self.inner.epoch_duration()
    }

    /// Returns the current epoch.
    pub fn epoch(&self) -> Epoch {
        self.inner.epoch
    }

    /// Returns the earliest time at which the epoch state can transition to a different state.
    pub fn earliest_state_transition_time(&self) -> SystemTime {
        match (self.epoch_state(), self.epoch()) {
            (EpochState::EpochChangeSync(_), _) => SystemTime::now(),
            (_, 0) => UNIX_EPOCH + Duration::from_millis(self.inner.first_epoch_start),
            (EpochState::EpochChangeDone(epoch_start_time), _) => {
                // The next epoch's parameters can be selected after half the epoch duration.
                SystemTime::from(*epoch_start_time) + self.epoch_duration() / 2
            }
            (EpochState::NextParamsSelected(epoch_start_time), _) => {
                // The next epoch can start one epoch duration after the current epoch.
                SystemTime::from(*epoch_start_time) + self.epoch_duration()
            }
        }
    }
}

/// Sui type for outer staking object. Used for deserialization.
#[derive(Debug, Deserialize)]
pub(crate) struct StakingObjectForDeserialization {
    pub(crate) id: ObjectID,
    pub(crate) version: u64,
    pub(crate) package_id: ObjectID,
    pub(crate) new_package_id: Option<ObjectID>,
}

impl AssociatedContractStruct for StakingObjectForDeserialization {
    const CONTRACT_STRUCT: StructTag<'static> = contracts::staking::Staking;
}

impl AssociatedContractStructWithPkgId for StakingObjectForDeserialization {
    fn package_id(&self) -> ObjectID {
        self.package_id
    }
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

    /// Returns the start time of the current epoch if it is known.
    ///
    /// Returns `None` if the epoch change is in progress.
    pub fn start_of_current_epoch(&self) -> Option<DateTime<Utc>> {
        match self {
            Self::EpochChangeDone(epoch_start) | Self::NextParamsSelected(epoch_start) => {
                Some(*epoch_start)
            }
            Self::EpochChangeSync(_) => None,
        }
    }
}

/// The committee shard assignment.
type CommitteeShardAssignment = Vec<(ObjectID, Vec<u16>)>;

/// Sui type for inner staking object
#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
pub(crate) struct StakingInnerV1 {
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
    pub(crate) active_set: ObjectID,
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
    /// Extended field holding public keys for the next epoch.
    pub(crate) next_epoch_public_keys: ObjectID,
}

impl StakingInnerV1 {
    /// Returns the epoch duration.
    pub fn epoch_duration(&self) -> Duration {
        Duration::from_millis(self.epoch_duration)
    }
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
    /// Aggregated key for all committee members
    #[serde(deserialize_with = "deserialize_public_key")]
    aggregated_keys: PublicKey,
}

/// Sui type for system object
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SystemObject {
    /// Object id of the Sui object.
    pub id: ObjectID,
    /// The version of the system object.
    pub version: u64,
    /// The package ID of the system object.
    pub package_id: ObjectID,
    /// The new package ID of the system object.
    pub(crate) new_package_id: Option<ObjectID>,
    /// The inner system state.
    pub(crate) inner: SystemStateInnerV1,
}

impl SystemObject {
    /// Returns the number of members in the committee.
    pub(crate) fn committee_size(&self) -> u16 {
        self.inner
            .committee
            .members
            .len()
            .try_into()
            .expect("committee size always fits in a u16")
    }

    /// Returns the number of epochs ahead that can be used to extend a blob.
    pub fn max_epochs_ahead(&self) -> u32 {
        self.inner.future_accounting.length()
    }

    /// Returns the storage price per unit size.
    pub fn storage_price_per_unit_size(&self) -> u64 {
        self.inner.storage_price_per_unit_size
    }

    /// Returns the write price per unit size.
    pub fn write_price_per_unit_size(&self) -> u64 {
        self.inner.write_price_per_unit_size
    }

    /// Returns the latest certified event blob.
    pub fn latest_certified_event_blob(&self) -> Option<EventBlob> {
        self.inner
            .event_blob_certification_state
            .latest_certified_blob
            .clone()
    }

    /// Returns the future accounting ring buffer.
    pub fn future_accounting(&self) -> &FutureAccountingRingBuffer {
        &self.inner.future_accounting
    }

    /// Returns the current epoch.
    pub fn epoch(&self) -> Epoch {
        self.inner.committee.epoch
    }
}

/// Sui type for outer system object. Used for deserialization.
#[derive(Debug, Deserialize)]
pub(crate) struct SystemObjectForDeserialization {
    pub(crate) id: ObjectID,
    pub(crate) version: u64,
    pub(crate) package_id: ObjectID,
    pub(crate) new_package_id: Option<ObjectID>,
}

impl AssociatedContractStruct for SystemObjectForDeserialization {
    const CONTRACT_STRUCT: StructTag<'static> = contracts::system::System;
}

impl AssociatedContractStructWithPkgId for SystemObjectForDeserialization {
    fn package_id(&self) -> ObjectID {
        self.package_id
    }
}

/// Sui type for inner system object.
#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
pub(crate) struct SystemStateInnerV1 {
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
    /// Extended field with the size of the deny list for committee members.
    pub deny_list_sized: ObjectID,
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
    /// The WAL is unstaked and can be withdrawn.
    Withdrawing(Epoch, Option<u64>),
}

impl Display for StakedWalState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StakedWalState::Staked => write!(f, "Staked"),
            StakedWalState::Withdrawing(epoch, Some(amount)) => {
                write!(
                    f,
                    "Withdrawing: epoch={epoch}, pool token amount={amount:?}"
                )
            }
            StakedWalState::Withdrawing(epoch, None) => {
                write!(f, "Withdrawing: epoch={epoch}, pool token amount=Unknown")
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

/// Sui type for a `subsidies::Subsidies` object. Called `Credits` here to avoid confusion with
/// the new Walrus Subsidies contract.
///
/// The "credits" were previously used to subsidize clients purchasing storage. In this function
/// they were superseded by the `walrus_subsidies` contract, which only provides subsidies on the
/// system side.
/// The same interfaces are still used as part of a credit system developed by the DevRel team that
/// allows additionally subsidizing clients. Because of this, we keep the corresponding code-paths
/// in the codebase, to provide backwards compatibility.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Credits {
    /// The object ID of the object
    pub id: ObjectID,
    /// The subsidy rate for blob storage buyers
    pub buyer_subsidy_rate: u16,
    /// The subsidy rate for system operations
    pub system_subsidy_rate: u16,
    /// The total amount of WAL available for subsidies
    pub subsidy_pool: u64,
    /// The package ID of the contract
    pub package_id: ObjectID,
    /// The version of the object
    pub version: u64,
}

impl AssociatedContractStruct for Credits {
    const CONTRACT_STRUCT: StructTag<'static> = contracts::credits::Subsidies;
}

impl AssociatedContractStructWithPkgId for Credits {
    fn package_id(&self) -> ObjectID {
        self.package_id
    }
}

/// Sui type for a `WalrusSubsidies` object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalrusSubsidies {
    /// The object ID of the object
    pub id: ObjectID,
    /// The version of the object
    pub version: u64,
    /// The package ID of the contract
    pub package_id: ObjectID,
    /// The inner state of the object
    pub(crate) inner: Option<WalrusSubsidiesInner>,
}

#[cfg(feature = "test-utils")]
impl WalrusSubsidies {
    /// Returns the subsidy pool funds for the WalrusSubsidies object if it was requested with
    /// the inner object.
    pub fn subsidy_pool_funds(&self) -> Option<u64> {
        self.inner.as_ref().map(|inner| inner.subsidy_pool)
    }
}

/// Sui type for outer system object. Used for deserialization.
#[derive(Debug, Deserialize)]
pub(crate) struct WalrusSubsidiesForDeserialization {
    pub(crate) id: ObjectID,
    pub(crate) version: u64,
    pub(crate) package_id: ObjectID,
}

impl AssociatedContractStruct for WalrusSubsidiesForDeserialization {
    const CONTRACT_STRUCT: StructTag<'static> = contracts::walrus_subsidies::WalrusSubsidies;
}

impl AssociatedContractStructWithPkgId for WalrusSubsidiesForDeserialization {
    fn package_id(&self) -> ObjectID {
        self.package_id
    }
}

/// A pair mapping an epoch to a balance. Used for deserialization of the WalrusSubsidiesInner.
#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
pub(crate) struct EpochBalance {
    epoch: Epoch,
    balance: u64,
}

/// A ring buffer holding the epoch balances for a continuous range of epochs.
/// Used for deserialization of the WalrusSubsidiesInner.
#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
pub(crate) struct EpochBalanceRingBuffer {
    current_index: u32,
    ring_buffer: Vec<EpochBalance>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct WalrusSubsidiesInner {
    /// The subsidy rate applied to the price paid to the system and added to the per-epoch rewards
    /// pool of the system. Subsidy rates are expressed in basis points (1/100 of a percent). A
    /// subsidy rate of 100 basis points means a 1% subsidy.
    pub system_subsidy_rate: u32,
    /// The balance of funds available in the subsidy pool.
    pub subsidy_pool: u64,
    // TODO(WAL-788): Use a specific type to represent different denominations.
    /// The base subsidy (in FROST) paid directly per storage node per epoch.
    pub base_subsidy: u64,
    /// The additional subsidy (in FROST) paid to each storage node directly per shard.
    pub subsidy_per_shard: u64,
    /// The last epoch for which the usage-independent subsidies were paid.
    pub latest_epoch: u32,
    /// Ring buffer to track how much of the per-epoch balance of the walrus system object has
    /// already had subsidies added.
    pub already_subsidized_balances: EpochBalanceRingBuffer,
    /// Timestamp of the last time the subsidies were processed. Enables storage nodes to
    /// easily check if subsidies were processed recently.
    #[serde(deserialize_with = "chrono::serde::ts_milliseconds::deserialize")]
    pub last_subsidized: DateTime<Utc>,
    /// Reserved for future use and migrations.
    #[serde(deserialize_with = "deserialize_bag_or_table")]
    pub extra_fields: ObjectID,
}

impl AssociatedContractStruct for WalrusSubsidiesInner {
    const CONTRACT_STRUCT: StructTag<'static> = contracts::walrus_subsidies::WalrusSubsidiesInnerV1;
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

/// Sui type for a `SharedBlob`.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct SharedBlob {
    /// The object ID of the shared blob.
    pub id: ObjectID,
    /// The blob.
    pub blob: Blob,
    /// The funds that can be used to store the blob.
    pub funds: u64,
}

impl AssociatedContractStruct for SharedBlob {
    const CONTRACT_STRUCT: StructTag<'static> = contracts::shared_blob::SharedBlob;
}

/// Sui type for the key of an extended field.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub(crate) struct Key {
    /// To match empty struct in Move.
    pub dummy_field: bool,
}

impl AssociatedContractStruct for Key {
    const CONTRACT_STRUCT: StructTag<'static> = contracts::extended_field::Key;
}

/// Sui type for the key of the dynamic field of the WalrusSubsidiesInner object.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub(crate) struct SubsidiesInnerKey {
    /// To match empty struct in Move.
    pub dummy_field: bool,
}

impl AssociatedContractStruct for SubsidiesInnerKey {
    const CONTRACT_STRUCT: StructTag<'static> = contracts::walrus_subsidies::SubsidiesInnerKey;
}
