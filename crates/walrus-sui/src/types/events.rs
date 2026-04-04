// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Walrus event type bindings. Replicates the move event types in Rust.

use anyhow::anyhow;
use move_core_types::language_storage::StructTag as MoveStructTag;
use serde::{Deserialize, Serialize};
use sui_sdk::rpc_types::SuiEvent;
use sui_types::{base_types::ObjectID, event::EventID};
use walrus_core::{BlobId, EncodingType, Epoch, ShardIndex, ensure};

use crate::contracts::{self, AssociatedContractEvent, MoveConversionError, StructTag};

/// A protocol-agnostic Sui event with only the fields Walrus needs for deserialization.
#[derive(Debug, Clone)]
pub struct EventEnvelope {
    /// The event ID (transaction digest + sequence number).
    pub id: EventID,
    /// The Move struct type of the event.
    pub type_: MoveStructTag,
    /// The BCS-serialized event contents.
    pub bcs: Vec<u8>,
}

impl From<SuiEvent> for EventEnvelope {
    fn from(event: SuiEvent) -> Self {
        Self {
            id: event.id,
            type_: event.type_,
            bcs: event.bcs.bytes().to_vec(),
        }
    }
}

fn ensure_event_type(
    envelope: &EventEnvelope,
    struct_tag: &StructTag,
) -> Result<(), MoveConversionError> {
    ensure!(
        envelope.type_.name.as_str() == struct_tag.name
            && envelope.type_.module.as_str() == struct_tag.module,
        MoveConversionError::TypeMismatch {
            expected: struct_tag.to_string(),
            actual: format!(
                "{}::{}",
                envelope.type_.module.as_str(),
                envelope.type_.name.as_str()
            ),
        }
    );
    Ok(())
}

/// Sui event that blob has been registered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobRegistered {
    /// The epoch in which the blob has been registered.
    pub epoch: Epoch,
    /// The blob ID.
    pub blob_id: BlobId,
    /// The (unencoded) size of the blob.
    pub size: u64,
    /// The erasure coding type used for the blob.
    pub encoding_type: EncodingType,
    /// The end epoch of the associated storage resource (exclusive).
    pub end_epoch: Epoch,
    /// Marks the blob as deletable.
    pub deletable: bool,
    /// The object ID of the related `Blob` object.
    pub object_id: ObjectID,
    /// The ID of the event.
    pub event_id: EventID,
}

impl AssociatedContractEvent for BlobRegistered {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::BlobRegistered;
}

impl TryFrom<EventEnvelope> for BlobRegistered {
    type Error = MoveConversionError;

    fn try_from(envelope: EventEnvelope) -> Result<Self, Self::Error> {
        ensure_event_type(&envelope, &Self::EVENT_STRUCT)?;

        let (epoch, blob_id, size, encoding_type, end_epoch, deletable, object_id) =
            bcs::from_bytes(&envelope.bcs)?;

        Ok(Self {
            epoch,
            blob_id,
            size,
            encoding_type,
            end_epoch,
            deletable,
            object_id,
            event_id: envelope.id,
        })
    }
}

/// Sui event that blob has been certified.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobCertified {
    /// The epoch in which the blob was certified.
    pub epoch: Epoch,
    /// The blob ID.
    pub blob_id: BlobId,
    /// The end epoch of the associated storage resource (exclusive).
    pub end_epoch: Epoch,
    /// Marks the blob as deletable.
    pub deletable: bool,
    /// The object id of the related `Blob` object
    pub object_id: ObjectID,
    /// Marks if this is an extension for explorers, etc.
    pub is_extension: bool,
    /// The ID of the event.
    pub event_id: EventID,
}

impl AssociatedContractEvent for BlobCertified {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::BlobCertified;
}

impl TryFrom<EventEnvelope> for BlobCertified {
    type Error = MoveConversionError;

    fn try_from(envelope: EventEnvelope) -> Result<Self, Self::Error> {
        ensure_event_type(&envelope, &Self::EVENT_STRUCT)?;

        let (epoch, blob_id, end_epoch, deletable, object_id, is_extension) =
            bcs::from_bytes(&envelope.bcs)?;
        Ok(Self {
            epoch,
            blob_id,
            end_epoch,
            deletable,
            object_id,
            is_extension,
            event_id: envelope.id,
        })
    }
}

/// Sui event that blob has been deleted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobDeleted {
    /// The epoch in which the blob was deleted.
    pub epoch: Epoch,
    /// The blob ID.
    pub blob_id: BlobId,
    /// The end epoch of the associated storage resource (exclusive).
    pub end_epoch: Epoch,
    /// The object id of the related `Blob` object
    pub object_id: ObjectID,
    /// If the blob object was previously certified.
    pub was_certified: bool,
    /// The ID of the event.
    pub event_id: EventID,
}

impl AssociatedContractEvent for BlobDeleted {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::BlobDeleted;
}

impl TryFrom<EventEnvelope> for BlobDeleted {
    type Error = MoveConversionError;

    fn try_from(envelope: EventEnvelope) -> Result<Self, Self::Error> {
        ensure_event_type(&envelope, &Self::EVENT_STRUCT)?;

        let (epoch, blob_id, end_epoch, object_id, was_certified) = bcs::from_bytes(&envelope.bcs)?;
        Ok(Self {
            epoch,
            blob_id,
            end_epoch,
            object_id,
            was_certified,
            event_id: envelope.id,
        })
    }
}

/// Sui event that a blob ID is invalid.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvalidBlobId {
    /// The epoch in which the blob was marked as invalid.
    pub epoch: Epoch,
    /// The blob ID.
    pub blob_id: BlobId,
    /// The ID of the event.
    pub event_id: EventID,
}

impl AssociatedContractEvent for InvalidBlobId {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::InvalidBlobID;
}

impl TryFrom<EventEnvelope> for InvalidBlobId {
    type Error = MoveConversionError;

    fn try_from(envelope: EventEnvelope) -> Result<Self, Self::Error> {
        ensure_event_type(&envelope, &Self::EVENT_STRUCT)?;

        let (epoch, blob_id) = bcs::from_bytes(&envelope.bcs)?;
        Ok(Self {
            epoch,
            blob_id,
            event_id: envelope.id,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Sui event that a deny listed blob has been deleted.
pub struct DenyListBlobDeleted {
    /// The epoch in which the deny listed blob was deleted.
    pub epoch: Epoch,
    /// The blob ID of the deny listed blob.
    pub blob_id: BlobId,
    /// The ID of the event.
    pub event_id: EventID,
}

impl AssociatedContractEvent for DenyListBlobDeleted {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::DenyListBlobDeleted;
}

impl TryFrom<EventEnvelope> for DenyListBlobDeleted {
    type Error = MoveConversionError;

    fn try_from(envelope: EventEnvelope) -> Result<Self, Self::Error> {
        ensure_event_type(&envelope, &Self::EVENT_STRUCT)?;

        let (epoch, blob_id) = bcs::from_bytes(&envelope.bcs)?;
        Ok(Self {
            epoch,
            blob_id,
            event_id: envelope.id,
        })
    }
}

/// Enum to wrap blob events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlobEvent {
    /// A registration event.
    Registered(BlobRegistered),
    /// A certification event.
    Certified(BlobCertified),
    /// A deletion event.
    Deleted(BlobDeleted),
    /// An invalid blob ID event.
    InvalidBlobID(InvalidBlobId),
    /// A deny list blob deleted event.
    DenyListBlobDeleted(DenyListBlobDeleted),
    /// A blob was registered against a storage pool.
    PooledBlobRegistered(PooledBlobRegistered),
    /// A blob in a storage pool was certified.
    PooledBlobCertified(PooledBlobCertified),
    /// A blob was deleted from a storage pool.
    PooledBlobDeleted(PooledBlobDeleted),
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

impl From<BlobDeleted> for BlobEvent {
    fn from(value: BlobDeleted) -> Self {
        Self::Deleted(value)
    }
}

impl From<InvalidBlobId> for BlobEvent {
    fn from(value: InvalidBlobId) -> Self {
        Self::InvalidBlobID(value)
    }
}

impl From<BlobEvent> for ContractEvent {
    fn from(value: BlobEvent) -> Self {
        Self::BlobEvent(value)
    }
}

impl From<BlobRegistered> for ContractEvent {
    fn from(value: BlobRegistered) -> Self {
        Self::BlobEvent(value.into())
    }
}

impl From<BlobCertified> for ContractEvent {
    fn from(value: BlobCertified) -> Self {
        Self::BlobEvent(value.into())
    }
}

impl From<BlobDeleted> for ContractEvent {
    fn from(value: BlobDeleted) -> Self {
        Self::BlobEvent(value.into())
    }
}

impl From<InvalidBlobId> for ContractEvent {
    fn from(value: InvalidBlobId) -> Self {
        Self::BlobEvent(value.into())
    }
}

impl From<DenyListBlobDeleted> for BlobEvent {
    fn from(value: DenyListBlobDeleted) -> Self {
        Self::DenyListBlobDeleted(value)
    }
}

impl BlobEvent {
    /// Returns the blob ID contained in the wrapped event.
    pub fn blob_id(&self) -> BlobId {
        match self {
            BlobEvent::Registered(event) => event.blob_id,
            BlobEvent::Certified(event) => event.blob_id,
            BlobEvent::Deleted(event) => event.blob_id,
            BlobEvent::InvalidBlobID(event) => event.blob_id,
            BlobEvent::DenyListBlobDeleted(event) => event.blob_id,
            BlobEvent::PooledBlobRegistered(event) => event.blob_id,
            BlobEvent::PooledBlobCertified(event) => event.blob_id,
            BlobEvent::PooledBlobDeleted(event) => event.blob_id,
        }
    }

    /// Returns the object ID contained in the wrapped event.
    pub fn object_id(&self) -> Option<ObjectID> {
        match self {
            BlobEvent::Registered(event) => Some(event.object_id),
            BlobEvent::Certified(event) => Some(event.object_id),
            BlobEvent::Deleted(event) => Some(event.object_id),
            BlobEvent::InvalidBlobID(_) => None,
            BlobEvent::DenyListBlobDeleted(_) => None,
            BlobEvent::PooledBlobRegistered(event) => Some(event.object_id),
            BlobEvent::PooledBlobCertified(event) => Some(event.object_id),
            BlobEvent::PooledBlobDeleted(event) => Some(event.object_id),
        }
    }

    /// Returns the event ID of the wrapped event.
    pub fn event_id(&self) -> EventID {
        match self {
            BlobEvent::Registered(event) => event.event_id,
            BlobEvent::Certified(event) => event.event_id,
            BlobEvent::Deleted(event) => event.event_id,
            BlobEvent::InvalidBlobID(event) => event.event_id,
            BlobEvent::DenyListBlobDeleted(event) => event.event_id,
            BlobEvent::PooledBlobRegistered(event) => event.event_id,
            BlobEvent::PooledBlobCertified(event) => event.event_id,
            BlobEvent::PooledBlobDeleted(event) => event.event_id,
        }
    }

    /// The epoch in which the event was generated.
    ///
    /// This returns `None` for blob extensions, as the corresponding events contain the epoch in
    /// which the blob was first certified.
    pub fn event_epoch(&self) -> Option<Epoch> {
        match self {
            BlobEvent::Registered(event) => Some(event.epoch),
            BlobEvent::Certified(event) => {
                if event.is_extension {
                    None
                } else {
                    Some(event.epoch)
                }
            }
            BlobEvent::Deleted(event) => Some(event.epoch),
            BlobEvent::InvalidBlobID(event) => Some(event.epoch),
            BlobEvent::DenyListBlobDeleted(event) => Some(event.epoch),
            BlobEvent::PooledBlobRegistered(event) => Some(event.epoch),
            BlobEvent::PooledBlobCertified(event) => Some(event.epoch),
            BlobEvent::PooledBlobDeleted(event) => Some(event.epoch),
        }
    }

    /// The name of the event.
    pub fn name(&self) -> &'static str {
        match self {
            BlobEvent::Registered(_) => "BlobRegistered",
            BlobEvent::Certified(_) => "BlobCertified",
            BlobEvent::Deleted(_) => "BlobDeleted",
            BlobEvent::InvalidBlobID(_) => "InvalidBlobID",
            BlobEvent::DenyListBlobDeleted(_) => "DenyListBlobDeleted",
            BlobEvent::PooledBlobRegistered(_) => "PooledBlobRegistered",
            BlobEvent::PooledBlobCertified(_) => "PooledBlobCertified",
            BlobEvent::PooledBlobDeleted(_) => "PooledBlobDeleted",
        }
    }
}

impl TryFrom<EventEnvelope> for BlobEvent {
    type Error = anyhow::Error;

    fn try_from(envelope: EventEnvelope) -> Result<Self, Self::Error> {
        match (&envelope.type_).into() {
            contracts::events::BlobRegistered => Ok(BlobEvent::Registered(envelope.try_into()?)),
            contracts::events::BlobCertified => Ok(BlobEvent::Certified(envelope.try_into()?)),
            contracts::events::InvalidBlobID => Ok(BlobEvent::InvalidBlobID(envelope.try_into()?)),
            contracts::events::BlobDeleted => Ok(BlobEvent::Deleted(envelope.try_into()?)),
            contracts::events::DenyListBlobDeleted => {
                Ok(BlobEvent::DenyListBlobDeleted(envelope.try_into()?))
            }
            contracts::events::PooledBlobRegistered => {
                Ok(BlobEvent::PooledBlobRegistered(envelope.try_into()?))
            }
            contracts::events::PooledBlobCertified => {
                Ok(BlobEvent::PooledBlobCertified(envelope.try_into()?))
            }
            contracts::events::PooledBlobDeleted => {
                Ok(BlobEvent::PooledBlobDeleted(envelope.try_into()?))
            }
            _ => Err(anyhow!(
                "could not convert to blob event: {:?}",
                envelope.type_
            )),
        }
    }
}

/// Sui event that epoch parameters have been selected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpochParametersSelected {
    /// The next epoch.
    pub next_epoch: Epoch,
    /// The ID of the event.
    pub event_id: EventID,
}

impl AssociatedContractEvent for EpochParametersSelected {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::EpochParametersSelected;
}

impl TryFrom<EventEnvelope> for EpochParametersSelected {
    type Error = MoveConversionError;

    fn try_from(envelope: EventEnvelope) -> Result<Self, Self::Error> {
        ensure_event_type(&envelope, &Self::EVENT_STRUCT)?;

        let next_epoch = bcs::from_bytes(&envelope.bcs)?;
        Ok(Self {
            next_epoch,
            event_id: envelope.id,
        })
    }
}

/// Sui event that epoch change has started.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpochChangeStart {
    /// The new epoch.
    pub epoch: Epoch,
    /// The ID of the event.
    pub event_id: EventID,
}

impl AssociatedContractEvent for EpochChangeStart {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::EpochChangeStart;
}

impl TryFrom<EventEnvelope> for EpochChangeStart {
    type Error = MoveConversionError;

    fn try_from(envelope: EventEnvelope) -> Result<Self, Self::Error> {
        ensure_event_type(&envelope, &Self::EVENT_STRUCT)?;

        let epoch = bcs::from_bytes(&envelope.bcs)?;
        Ok(Self {
            epoch,
            event_id: envelope.id,
        })
    }
}

/// Sui event that epoch change has completed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpochChangeDone {
    /// The new epoch.
    pub epoch: Epoch,
    /// The ID of the event.
    pub event_id: EventID,
}

impl AssociatedContractEvent for EpochChangeDone {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::EpochChangeDone;
}

impl TryFrom<EventEnvelope> for EpochChangeDone {
    type Error = MoveConversionError;

    fn try_from(envelope: EventEnvelope) -> Result<Self, Self::Error> {
        ensure_event_type(&envelope, &Self::EVENT_STRUCT)?;

        let epoch = bcs::from_bytes(&envelope.bcs)?;
        Ok(Self {
            epoch,
            event_id: envelope.id,
        })
    }
}

/// Sui event that shards have been successfully received in the assigned storage node in the
/// new epoch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardsReceived {
    pub epoch: Epoch,
    pub shards: Vec<ShardIndex>,
    /// The ID of the event.
    pub event_id: EventID,
}

impl AssociatedContractEvent for ShardsReceived {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::ShardsReceived;
}

impl TryFrom<EventEnvelope> for ShardsReceived {
    type Error = MoveConversionError;

    fn try_from(envelope: EventEnvelope) -> Result<Self, Self::Error> {
        ensure_event_type(&envelope, &Self::EVENT_STRUCT)?;

        let (epoch, shards) = bcs::from_bytes(&envelope.bcs)?;
        Ok(Self {
            epoch,
            shards,
            event_id: envelope.id,
        })
    }
}

/// Sui event signaling that shards start using recovery mechanism to recover data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardRecoveryStart {
    pub epoch: Epoch,
    pub shards: Vec<ShardIndex>,
    /// The ID of the event.
    pub event_id: EventID,
}

impl AssociatedContractEvent for ShardRecoveryStart {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::ShardRecoveryStart;
}

impl TryFrom<EventEnvelope> for ShardRecoveryStart {
    type Error = MoveConversionError;

    fn try_from(envelope: EventEnvelope) -> Result<Self, Self::Error> {
        ensure_event_type(&envelope, &Self::EVENT_STRUCT)?;

        let (epoch, shards) = bcs::from_bytes(&envelope.bcs)?;
        Ok(Self {
            epoch,
            shards,
            event_id: envelope.id,
        })
    }
}

/// Enum to wrap epoch change events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EpochChangeEvent {
    /// Epoch parameters selected event.
    EpochParametersSelected(EpochParametersSelected),
    /// Epoch change start event.
    EpochChangeStart(EpochChangeStart),
    /// Epoch change done event.
    EpochChangeDone(EpochChangeDone),
    /// Shards received event.
    ShardsReceived(ShardsReceived),
    /// Shard recovery start event.
    ShardRecoveryStart(ShardRecoveryStart),
}

impl EpochChangeEvent {
    /// Returns the event ID of the wrapped event.
    pub fn event_id(&self) -> EventID {
        match self {
            EpochChangeEvent::EpochParametersSelected(event) => event.event_id,
            EpochChangeEvent::EpochChangeStart(event) => event.event_id,
            EpochChangeEvent::EpochChangeDone(event) => event.event_id,
            EpochChangeEvent::ShardsReceived(event) => event.event_id,
            EpochChangeEvent::ShardRecoveryStart(event) => event.event_id,
        }
    }

    /// The epoch corresponding to the contract change event.
    pub fn event_epoch(&self) -> Epoch {
        match self {
            EpochChangeEvent::EpochParametersSelected(event) => event.next_epoch - 1,
            EpochChangeEvent::EpochChangeStart(event) => event.epoch,
            EpochChangeEvent::EpochChangeDone(event) => event.epoch,
            EpochChangeEvent::ShardsReceived(event) => event.epoch,
            EpochChangeEvent::ShardRecoveryStart(event) => event.epoch,
        }
    }

    /// The name of the event.
    pub fn name(&self) -> &'static str {
        match self {
            EpochChangeEvent::EpochParametersSelected(_) => "EpochParametersSelected",
            EpochChangeEvent::EpochChangeStart(_) => "EpochChangeStart",
            EpochChangeEvent::EpochChangeDone(_) => "EpochChangeDone",
            EpochChangeEvent::ShardsReceived(_) => "ShardsReceived",
            EpochChangeEvent::ShardRecoveryStart(_) => "ShardRecoveryStart",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Sui event that a contract has been upgraded.
pub struct ContractUpgradedEvent {
    /// The epoch in which the contract was upgraded.
    pub epoch: Epoch,
    /// The new package ID of the contract.
    pub package_id: ObjectID,
    /// The new version of the contract.
    pub version: u64,
    /// The ID of the event.
    pub event_id: EventID,
}

impl AssociatedContractEvent for ContractUpgradedEvent {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::ContractUpgraded;
}

impl TryFrom<EventEnvelope> for ContractUpgradedEvent {
    type Error = MoveConversionError;

    fn try_from(envelope: EventEnvelope) -> Result<Self, Self::Error> {
        ensure_event_type(&envelope, &Self::EVENT_STRUCT)?;

        let (epoch, package_id, version) = bcs::from_bytes(&envelope.bcs)?;
        Ok(Self {
            epoch,
            package_id,
            version,
            event_id: envelope.id,
        })
    }
}

pub type PackageDigest = Vec<u8>;

/// Sui event that a contract upgrade has been proposed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContractUpgradeProposedEvent {
    /// The epoch in which the contract upgrade was proposed.
    pub epoch: Epoch,
    /// The digest of the package to upgrade to.
    pub package_digest: PackageDigest,
    /// The ID of the event.
    pub event_id: EventID,
}

impl AssociatedContractEvent for ContractUpgradeProposedEvent {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::ContractUpgradeProposed;
}

impl TryFrom<EventEnvelope> for ContractUpgradeProposedEvent {
    type Error = MoveConversionError;

    fn try_from(envelope: EventEnvelope) -> Result<Self, Self::Error> {
        ensure_event_type(&envelope, &Self::EVENT_STRUCT)?;

        let (epoch, package_digest) = bcs::from_bytes(&envelope.bcs)?;
        Ok(Self {
            epoch,
            package_digest,
            event_id: envelope.id,
        })
    }
}

/// Sui event that a contract upgrade has received a quorum of votes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContractUpgradeQuorumReachedEvent {
    /// The epoch in which the contract upgrade was proposed.
    pub epoch: Epoch,
    /// The digest of the package to upgrade to.
    pub package_digest: PackageDigest,
    /// The ID of the event.
    pub event_id: EventID,
}

impl AssociatedContractEvent for ContractUpgradeQuorumReachedEvent {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::ContractUpgradeQuorumReached;
}

impl TryFrom<EventEnvelope> for ContractUpgradeQuorumReachedEvent {
    type Error = MoveConversionError;

    fn try_from(envelope: EventEnvelope) -> Result<Self, Self::Error> {
        ensure_event_type(&envelope, &Self::EVENT_STRUCT)?;

        let (epoch, package_digest) = bcs::from_bytes(&envelope.bcs)?;
        Ok(Self {
            epoch,
            package_digest,
            event_id: envelope.id,
        })
    }
}

/// Sui event that a contract upgrade has received a quorum of votes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolVersionUpdatedEvent {
    /// The epoch in which the contract upgrade was proposed.
    pub epoch: Epoch,
    /// The start epoch of the protocol version update.
    pub start_epoch: Epoch,
    /// The protocol version.
    pub protocol_version: u64,
    /// The ID of the event.
    pub event_id: EventID,
}

impl AssociatedContractEvent for ProtocolVersionUpdatedEvent {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::ProtocolVersionUpdated;
}

impl TryFrom<EventEnvelope> for ProtocolVersionUpdatedEvent {
    type Error = MoveConversionError;

    fn try_from(envelope: EventEnvelope) -> Result<Self, Self::Error> {
        ensure_event_type(&envelope, &Self::EVENT_STRUCT)?;

        let (epoch, start_epoch, protocol_version) = bcs::from_bytes(&envelope.bcs)?;
        Ok(Self {
            epoch,
            start_epoch,
            protocol_version,
            event_id: envelope.id,
        })
    }
}

/// Sui event that storage and write prices have been updated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PricesUpdatedEvent {
    /// The epoch in which the prices were updated.
    pub epoch: Epoch,
    /// The new storage price.
    pub storage_price: u64,
    /// The new write price.
    pub write_price: u64,
    /// The ID of the event.
    pub event_id: EventID,
}

impl AssociatedContractEvent for PricesUpdatedEvent {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::PricesUpdated;
}

impl TryFrom<EventEnvelope> for PricesUpdatedEvent {
    type Error = MoveConversionError;

    fn try_from(envelope: EventEnvelope) -> Result<Self, Self::Error> {
        ensure_event_type(&envelope, &Self::EVENT_STRUCT)?;

        let (epoch, storage_price, write_price) = bcs::from_bytes(&envelope.bcs)?;
        Ok(Self {
            epoch,
            storage_price,
            write_price,
            event_id: envelope.id,
        })
    }
}

/// Sui event that a storage pool has been created.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoragePoolCreatedEvent {
    /// The epoch in which the pool was created.
    pub epoch: Epoch,
    /// The object ID of the created storage pool.
    pub storage_pool_id: ObjectID,
    /// The total reserved encoded capacity of the pool.
    pub reserved_encoded_capacity_bytes: u64,
    /// The start epoch of the pool (inclusive).
    pub start_epoch: Epoch,
    /// The end epoch of the pool (exclusive).
    pub end_epoch: Epoch,
    /// The ID of the event.
    pub event_id: EventID,
}

impl AssociatedContractEvent for StoragePoolCreatedEvent {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::StoragePoolCreated;
}

impl TryFrom<EventEnvelope> for StoragePoolCreatedEvent {
    type Error = MoveConversionError;

    fn try_from(envelope: EventEnvelope) -> Result<Self, Self::Error> {
        ensure_event_type(&envelope, &Self::EVENT_STRUCT)?;

        let (epoch, storage_pool_id, reserved_encoded_capacity_bytes, start_epoch, end_epoch) =
            bcs::from_bytes(&envelope.bcs)?;
        Ok(Self {
            epoch,
            storage_pool_id,
            reserved_encoded_capacity_bytes,
            start_epoch,
            end_epoch,
            event_id: envelope.id,
        })
    }
}

/// Sui event that a blob has been registered against a storage pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PooledBlobRegistered {
    /// The epoch in which the blob was registered.
    pub epoch: Epoch,
    /// The blob ID.
    pub blob_id: BlobId,
    /// The (unencoded) size of the blob.
    pub unencoded_size: u64,
    /// The erasure coding type used for the blob.
    pub encoding_type: EncodingType,
    /// Whether the blob is deletable.
    pub deletable: bool,
    /// The object ID of the related `PooledBlob` object.
    pub object_id: ObjectID,
    /// The object ID of the storage pool.
    pub storage_pool_id: ObjectID,
    /// The ID of the event.
    pub event_id: EventID,
}

impl AssociatedContractEvent for PooledBlobRegistered {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::PooledBlobRegistered;
}

impl TryFrom<EventEnvelope> for PooledBlobRegistered {
    type Error = MoveConversionError;

    fn try_from(envelope: EventEnvelope) -> Result<Self, Self::Error> {
        ensure_event_type(&envelope, &Self::EVENT_STRUCT)?;

        let (epoch, blob_id, unencoded_size, encoding_type, deletable, object_id, pool_id) =
            bcs::from_bytes(&envelope.bcs)?;
        Ok(Self {
            epoch,
            blob_id,
            unencoded_size,
            encoding_type,
            deletable,
            object_id,
            storage_pool_id: pool_id,
            event_id: envelope.id,
        })
    }
}

/// Sui event that a blob in a storage pool has been certified.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PooledBlobCertified {
    /// The epoch in which the blob was certified.
    pub epoch: Epoch,
    /// The blob ID.
    pub blob_id: BlobId,
    /// Whether the blob is deletable.
    pub deletable: bool,
    /// The object ID of the related `PooledBlob` object.
    pub object_id: ObjectID,
    /// The object ID of the storage pool.
    pub storage_pool_id: ObjectID,
    /// The ID of the event.
    pub event_id: EventID,
}

impl AssociatedContractEvent for PooledBlobCertified {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::PooledBlobCertified;
}

impl TryFrom<EventEnvelope> for PooledBlobCertified {
    type Error = MoveConversionError;

    fn try_from(envelope: EventEnvelope) -> Result<Self, Self::Error> {
        ensure_event_type(&envelope, &Self::EVENT_STRUCT)?;

        let (epoch, blob_id, deletable, object_id, storage_pool_id) =
            bcs::from_bytes(&envelope.bcs)?;
        Ok(Self {
            epoch,
            blob_id,
            deletable,
            object_id,
            storage_pool_id,
            event_id: envelope.id,
        })
    }
}

/// Sui event that a blob has been deleted from a storage pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PooledBlobDeleted {
    /// The epoch in which the blob was deleted.
    pub epoch: Epoch,
    /// The blob ID.
    pub blob_id: BlobId,
    /// The object ID of the related `PooledBlob` object.
    pub object_id: ObjectID,
    /// Whether the blob was previously certified.
    pub was_certified: bool,
    /// The object ID of the storage pool.
    pub storage_pool_id: ObjectID,
    /// The ID of the event.
    pub event_id: EventID,
}

impl AssociatedContractEvent for PooledBlobDeleted {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::PooledBlobDeleted;
}

impl TryFrom<EventEnvelope> for PooledBlobDeleted {
    type Error = MoveConversionError;

    fn try_from(envelope: EventEnvelope) -> Result<Self, Self::Error> {
        ensure_event_type(&envelope, &Self::EVENT_STRUCT)?;

        let (epoch, blob_id, object_id, was_certified, storage_pool_id) =
            bcs::from_bytes(&envelope.bcs)?;
        Ok(Self {
            epoch,
            blob_id,
            object_id,
            was_certified,
            storage_pool_id,
            event_id: envelope.id,
        })
    }
}

/// Sui event that a storage pool's lifetime has been extended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoragePoolExtendedEvent {
    /// The epoch in which the extension happened.
    pub epoch: Epoch,
    /// The object ID of the storage pool.
    pub storage_pool_id: ObjectID,
    /// The new end epoch (exclusive).
    pub new_end_epoch: Epoch,
    /// The ID of the event.
    pub event_id: EventID,
}

impl AssociatedContractEvent for StoragePoolExtendedEvent {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::StoragePoolExtended;
}

impl TryFrom<EventEnvelope> for StoragePoolExtendedEvent {
    type Error = MoveConversionError;

    fn try_from(envelope: EventEnvelope) -> Result<Self, Self::Error> {
        ensure_event_type(&envelope, &Self::EVENT_STRUCT)?;

        let (epoch, storage_pool_id, new_end_epoch) = bcs::from_bytes(&envelope.bcs)?;
        Ok(Self {
            epoch,
            storage_pool_id,
            new_end_epoch,
            event_id: envelope.id,
        })
    }
}

/// Enum to wrap storage pool events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoragePoolEvent {
    /// A storage pool was created.
    StoragePoolCreated(StoragePoolCreatedEvent),
    /// A storage pool's lifetime was extended.
    StoragePoolExtended(StoragePoolExtendedEvent),
}

impl StoragePoolEvent {
    /// Returns the event ID of the wrapped event.
    pub fn event_id(&self) -> EventID {
        match self {
            StoragePoolEvent::StoragePoolCreated(event) => event.event_id,
            StoragePoolEvent::StoragePoolExtended(event) => event.event_id,
        }
    }

    /// The epoch corresponding to the storage pool event.
    pub fn event_epoch(&self) -> Option<Epoch> {
        match self {
            StoragePoolEvent::StoragePoolCreated(event) => Some(event.epoch),
            StoragePoolEvent::StoragePoolExtended(event) => Some(event.epoch),
        }
    }

    /// The name of the event.
    pub fn name(&self) -> &'static str {
        match self {
            StoragePoolEvent::StoragePoolCreated(_) => "StoragePoolCreated",
            StoragePoolEvent::StoragePoolExtended(_) => "StoragePoolExtended",
        }
    }

    /// Returns the storage pool ID.
    pub fn storage_pool_id(&self) -> ObjectID {
        match self {
            StoragePoolEvent::StoragePoolCreated(event) => event.storage_pool_id,
            StoragePoolEvent::StoragePoolExtended(event) => event.storage_pool_id,
        }
    }
}

impl From<StoragePoolEvent> for ContractEvent {
    fn from(value: StoragePoolEvent) -> Self {
        Self::StoragePoolEvent(value)
    }
}

/// Enum to wrap package events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum PackageEvent {
    /// Contract upgraded event.
    ContractUpgraded(ContractUpgradedEvent),
    /// Contract upgrade proposed event.
    ContractUpgradeProposed(ContractUpgradeProposedEvent),
    /// Contract upgrade quorum reached event.
    ContractUpgradeQuorumReached(ContractUpgradeQuorumReachedEvent),
}

impl PackageEvent {
    /// Returns the event ID of the wrapped event.
    pub fn event_id(&self) -> EventID {
        match self {
            PackageEvent::ContractUpgraded(event) => event.event_id,
            PackageEvent::ContractUpgradeProposed(event) => event.event_id,
            PackageEvent::ContractUpgradeQuorumReached(event) => event.event_id,
        }
    }

    /// The epoch corresponding to the contract change event.
    pub fn event_epoch(&self) -> Epoch {
        match self {
            PackageEvent::ContractUpgraded(event) => event.epoch,
            PackageEvent::ContractUpgradeProposed(event) => event.epoch,
            PackageEvent::ContractUpgradeQuorumReached(event) => event.epoch,
        }
    }

    /// The name of the event.
    pub fn name(&self) -> &'static str {
        match self {
            PackageEvent::ContractUpgraded(_) => "ContractUpgraded",
            PackageEvent::ContractUpgradeProposed(_) => "ContractUpgradeProposed",
            PackageEvent::ContractUpgradeQuorumReached(_) => "ContractUpgradeQuorumReached",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Sui event that a deny list update has been registered.
pub struct RegisterDenyListUpdateEvent {
    /// The epoch in which the deny list update was registered.
    pub epoch: Epoch,
    /// The root hash of the deny list update.
    pub root: [u8; 32],
    /// The deny list update ID.
    pub sequence_number: u64,
    /// The ID of the event.
    pub node_id: ObjectID,
    /// The ID of the event.
    pub event_id: EventID,
}

impl AssociatedContractEvent for RegisterDenyListUpdateEvent {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::RegisterDenyListUpdate;
}

impl TryFrom<EventEnvelope> for RegisterDenyListUpdateEvent {
    type Error = MoveConversionError;

    fn try_from(envelope: EventEnvelope) -> Result<Self, Self::Error> {
        ensure_event_type(&envelope, &Self::EVENT_STRUCT)?;

        let (epoch, root, sequence_number, node_id) = bcs::from_bytes(&envelope.bcs)?;

        Ok(Self {
            epoch,
            root,
            sequence_number,
            node_id,
            event_id: envelope.id,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Sui event that a deny list has been updated.
pub struct DenyListUpdateEvent {
    /// The epoch in which the deny list update was registered.
    pub epoch: Epoch,
    /// The root hash of the deny list update.
    pub root: [u8; 32],
    /// The deny list update ID.
    pub sequence_number: u64,
    /// The ID of the event.
    pub node_id: ObjectID,
    /// The ID of the event.
    pub event_id: EventID,
}

impl AssociatedContractEvent for DenyListUpdateEvent {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::DenyListUpdate;
}

impl TryFrom<EventEnvelope> for DenyListUpdateEvent {
    type Error = MoveConversionError;

    fn try_from(envelope: EventEnvelope) -> Result<Self, Self::Error> {
        ensure_event_type(&envelope, &Self::EVENT_STRUCT)?;

        let (epoch, root, sequence_number, node_id) = bcs::from_bytes(&envelope.bcs)?;

        Ok(Self {
            epoch,
            root,
            sequence_number,
            node_id,
            event_id: envelope.id,
        })
    }
}

/// Enum to wrap deny list events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DenyListEvent {
    /// Deny list update registered.
    RegisterDenyListUpdate(RegisterDenyListUpdateEvent),
    /// Deny list updated.
    DenyListUpdate(DenyListUpdateEvent),
}

impl DenyListEvent {
    /// Returns the event ID of the wrapped event.
    pub fn event_id(&self) -> EventID {
        match self {
            DenyListEvent::RegisterDenyListUpdate(event) => event.event_id,
            DenyListEvent::DenyListUpdate(event) => event.event_id,
        }
    }

    /// The epoch corresponding to the deny list event.
    pub fn event_epoch(&self) -> Epoch {
        match self {
            DenyListEvent::RegisterDenyListUpdate(event) => event.epoch,
            DenyListEvent::DenyListUpdate(event) => event.epoch,
        }
    }

    /// The name of the event.
    pub fn name(&self) -> &'static str {
        match self {
            DenyListEvent::RegisterDenyListUpdate(_) => "RegisterDenyListUpdate",
            DenyListEvent::DenyListUpdate(_) => "DenyListUpdate",
        }
    }
}

/// Enum to wrap protocol events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProtocolEvent {
    /// Protocol version updated.
    ProtocolVersionUpdated(ProtocolVersionUpdatedEvent),
    /// Prices updated.
    PricesUpdated(PricesUpdatedEvent),
}

impl ProtocolEvent {
    /// Returns the event ID of the wrapped event.
    pub fn event_id(&self) -> EventID {
        match self {
            ProtocolEvent::ProtocolVersionUpdated(event) => event.event_id,
            ProtocolEvent::PricesUpdated(event) => event.event_id,
        }
    }

    /// The epoch corresponding to the protocol event.
    pub fn event_epoch(&self) -> Epoch {
        match self {
            ProtocolEvent::ProtocolVersionUpdated(event) => event.epoch,
            ProtocolEvent::PricesUpdated(event) => event.epoch,
        }
    }

    /// The name of the event.
    pub fn name(&self) -> &'static str {
        match self {
            ProtocolEvent::ProtocolVersionUpdated(_) => "ProtocolVersionUpdated",
            ProtocolEvent::PricesUpdated(_) => "PricesUpdated",
        }
    }

    /// The protocol version.
    pub fn protocol_version(&self) -> u64 {
        match self {
            ProtocolEvent::ProtocolVersionUpdated(event) => event.protocol_version,
            ProtocolEvent::PricesUpdated(_) => 0,
        }
    }
}

/// Enum to wrap contract events used in event streaming.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContractEvent {
    /// Blob event.
    BlobEvent(BlobEvent),
    /// Epoch change event.
    EpochChangeEvent(EpochChangeEvent),
    /// Events related to package maintenance.
    PackageEvent(PackageEvent),
    /// Events related to deny list.
    DenyListEvent(DenyListEvent),
    /// Events related to protocol version.
    ProtocolEvent(ProtocolEvent),
    /// Events related to storage pools.
    StoragePoolEvent(StoragePoolEvent),
}

impl ContractEvent {
    /// Returns the event ID of the wrapped event.
    pub fn event_id(&self) -> EventID {
        match self {
            ContractEvent::BlobEvent(event) => event.event_id(),
            ContractEvent::EpochChangeEvent(event) => event.event_id(),
            ContractEvent::PackageEvent(event) => event.event_id(),
            ContractEvent::DenyListEvent(event) => event.event_id(),
            ContractEvent::ProtocolEvent(event) => event.event_id(),
            ContractEvent::StoragePoolEvent(event) => event.event_id(),
        }
    }

    /// Returns the blob ID of the wrapped event.
    pub fn blob_id(&self) -> Option<BlobId> {
        match self {
            ContractEvent::BlobEvent(event) => Some(event.blob_id()),
            ContractEvent::EpochChangeEvent(_) => None,
            ContractEvent::PackageEvent(_) => None,
            ContractEvent::DenyListEvent(_) => None,
            ContractEvent::ProtocolEvent(_) => None,
            ContractEvent::StoragePoolEvent(_) => None,
        }
    }

    /// Returns the epoch in which the event was issued.
    ///
    /// This returns `None` for blob extensions, as the corresponding events contain the epoch in
    /// which the blob was first certified.
    pub fn event_epoch(&self) -> Option<Epoch> {
        match self {
            ContractEvent::BlobEvent(event) => event.event_epoch(),
            ContractEvent::EpochChangeEvent(event) => Some(event.event_epoch()),
            ContractEvent::PackageEvent(event) => Some(event.event_epoch()),
            ContractEvent::DenyListEvent(event) => Some(event.event_epoch()),
            ContractEvent::ProtocolEvent(event) => Some(event.event_epoch()),
            ContractEvent::StoragePoolEvent(event) => event.event_epoch(),
        }
    }
}

impl TryFrom<EventEnvelope> for ContractEvent {
    type Error = anyhow::Error;

    fn try_from(envelope: EventEnvelope) -> Result<Self, Self::Error> {
        match (&envelope.type_).into() {
            contracts::events::BlobRegistered => Ok(ContractEvent::BlobEvent(
                BlobEvent::Registered(envelope.try_into()?),
            )),
            contracts::events::BlobCertified => Ok(ContractEvent::BlobEvent(BlobEvent::Certified(
                envelope.try_into()?,
            ))),
            contracts::events::BlobDeleted => Ok(ContractEvent::BlobEvent(BlobEvent::Deleted(
                envelope.try_into()?,
            ))),
            contracts::events::InvalidBlobID => Ok(ContractEvent::BlobEvent(
                BlobEvent::InvalidBlobID(envelope.try_into()?),
            )),
            contracts::events::EpochParametersSelected => Ok(ContractEvent::EpochChangeEvent(
                EpochChangeEvent::EpochParametersSelected(envelope.try_into()?),
            )),
            contracts::events::EpochChangeStart => Ok(ContractEvent::EpochChangeEvent(
                EpochChangeEvent::EpochChangeStart(envelope.try_into()?),
            )),
            contracts::events::EpochChangeDone => Ok(ContractEvent::EpochChangeEvent(
                EpochChangeEvent::EpochChangeDone(envelope.try_into()?),
            )),

            contracts::events::ShardsReceived => Ok(ContractEvent::EpochChangeEvent(
                EpochChangeEvent::ShardsReceived(envelope.try_into()?),
            )),
            contracts::events::ShardRecoveryStart => Ok(ContractEvent::EpochChangeEvent(
                EpochChangeEvent::ShardRecoveryStart(envelope.try_into()?),
            )),
            contracts::events::ContractUpgraded => Ok(ContractEvent::PackageEvent(
                PackageEvent::ContractUpgraded(envelope.try_into()?),
            )),
            contracts::events::ContractUpgradeProposed => Ok(ContractEvent::PackageEvent(
                PackageEvent::ContractUpgradeProposed(envelope.try_into()?),
            )),
            contracts::events::ContractUpgradeQuorumReached => Ok(ContractEvent::PackageEvent(
                PackageEvent::ContractUpgradeQuorumReached(envelope.try_into()?),
            )),
            contracts::events::RegisterDenyListUpdate => Ok(ContractEvent::DenyListEvent(
                DenyListEvent::RegisterDenyListUpdate(envelope.try_into()?),
            )),
            contracts::events::DenyListUpdate => Ok(ContractEvent::DenyListEvent(
                DenyListEvent::DenyListUpdate(envelope.try_into()?),
            )),
            contracts::events::DenyListBlobDeleted => Ok(ContractEvent::BlobEvent(
                BlobEvent::DenyListBlobDeleted(envelope.try_into()?),
            )),
            contracts::events::ProtocolVersionUpdated => Ok(ContractEvent::ProtocolEvent(
                ProtocolEvent::ProtocolVersionUpdated(envelope.try_into()?),
            )),
            contracts::events::PricesUpdated => Ok(ContractEvent::ProtocolEvent(
                ProtocolEvent::PricesUpdated(envelope.try_into()?),
            )),
            contracts::events::StoragePoolCreated => Ok(ContractEvent::StoragePoolEvent(
                StoragePoolEvent::StoragePoolCreated(envelope.try_into()?),
            )),
            contracts::events::PooledBlobRegistered => Ok(ContractEvent::BlobEvent(
                BlobEvent::PooledBlobRegistered(envelope.try_into()?),
            )),
            contracts::events::PooledBlobCertified => Ok(ContractEvent::BlobEvent(
                BlobEvent::PooledBlobCertified(envelope.try_into()?),
            )),
            contracts::events::PooledBlobDeleted => Ok(ContractEvent::BlobEvent(
                BlobEvent::PooledBlobDeleted(envelope.try_into()?),
            )),
            contracts::events::StoragePoolExtended => Ok(ContractEvent::StoragePoolEvent(
                StoragePoolEvent::StoragePoolExtended(envelope.try_into()?),
            )),
            _ => unreachable!(
                "Encountered unexpected unrecognized events {:?}",
                envelope.type_
            ),
        }
    }
}
