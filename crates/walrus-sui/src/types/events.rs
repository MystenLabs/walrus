// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Walrus event type bindings. Replicates the move event types in Rust.

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use sui_sdk::rpc_types::SuiEvent;
use sui_types::{base_types::ObjectID, event::EventID};
use walrus_core::{BlobId, EncodingType, Epoch, ShardIndex, ensure};

use crate::contracts::{self, AssociatedSuiEvent, MoveConversionError, StructTag};

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

impl AssociatedSuiEvent for BlobRegistered {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::BlobRegistered;
}

impl TryFrom<SuiEvent> for BlobRegistered {
    type Error = MoveConversionError;

    fn try_from(sui_event: SuiEvent) -> Result<Self, Self::Error> {
        ensure_event_type(&sui_event, &Self::EVENT_STRUCT)?;

        let (epoch, blob_id, size, encoding_type, end_epoch, deletable, object_id) =
            bcs::from_bytes(sui_event.bcs.bytes())?;

        Ok(Self {
            epoch,
            blob_id,
            size,
            encoding_type,
            end_epoch,
            deletable,
            object_id,
            event_id: sui_event.id,
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

impl AssociatedSuiEvent for BlobCertified {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::BlobCertified;
}

impl TryFrom<SuiEvent> for BlobCertified {
    type Error = MoveConversionError;

    fn try_from(sui_event: SuiEvent) -> Result<Self, Self::Error> {
        ensure_event_type(&sui_event, &Self::EVENT_STRUCT)?;

        let (epoch, blob_id, end_epoch, deletable, object_id, is_extension) =
            bcs::from_bytes(sui_event.bcs.bytes())?;
        Ok(Self {
            epoch,
            blob_id,
            end_epoch,
            deletable,
            object_id,
            is_extension,
            event_id: sui_event.id,
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

impl AssociatedSuiEvent for BlobDeleted {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::BlobDeleted;
}

impl TryFrom<SuiEvent> for BlobDeleted {
    type Error = MoveConversionError;

    fn try_from(sui_event: SuiEvent) -> Result<Self, Self::Error> {
        ensure_event_type(&sui_event, &Self::EVENT_STRUCT)?;

        let (epoch, blob_id, end_epoch, object_id, was_certified) =
            bcs::from_bytes(sui_event.bcs.bytes())?;
        Ok(Self {
            epoch,
            blob_id,
            end_epoch,
            object_id,
            was_certified,
            event_id: sui_event.id,
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

impl AssociatedSuiEvent for InvalidBlobId {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::InvalidBlobID;
}

impl TryFrom<SuiEvent> for InvalidBlobId {
    type Error = MoveConversionError;

    fn try_from(sui_event: SuiEvent) -> Result<Self, Self::Error> {
        ensure_event_type(&sui_event, &Self::EVENT_STRUCT)?;

        let (epoch, blob_id) = bcs::from_bytes(sui_event.bcs.bytes())?;
        Ok(Self {
            epoch,
            blob_id,
            event_id: sui_event.id,
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

impl AssociatedSuiEvent for DenyListBlobDeleted {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::DenyListBlobDeleted;
}

impl TryFrom<SuiEvent> for DenyListBlobDeleted {
    type Error = MoveConversionError;

    fn try_from(sui_event: SuiEvent) -> Result<Self, Self::Error> {
        ensure_event_type(&sui_event, &Self::EVENT_STRUCT)?;

        let (epoch, blob_id) = bcs::from_bytes(sui_event.bcs.bytes())?;
        Ok(Self {
            epoch,
            blob_id,
            event_id: sui_event.id,
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
        }
    }

    /// The epoch in which the event was generated.
    pub fn event_epoch(&self) -> Epoch {
        match self {
            BlobEvent::Registered(event) => event.epoch,
            BlobEvent::Certified(event) => event.epoch,
            BlobEvent::Deleted(event) => event.epoch,
            BlobEvent::InvalidBlobID(event) => event.epoch,
            BlobEvent::DenyListBlobDeleted(event) => event.epoch,
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
        }
    }

    /// Returns true if the event is a blob extension.
    pub fn is_blob_extension(&self) -> bool {
        match self {
            BlobEvent::Certified(event) => event.is_extension,
            _ => false,
        }
    }
}

impl TryFrom<SuiEvent> for BlobEvent {
    type Error = anyhow::Error;

    fn try_from(value: SuiEvent) -> Result<Self, Self::Error> {
        match (&value.type_).into() {
            contracts::events::BlobRegistered => Ok(BlobEvent::Registered(value.try_into()?)),
            contracts::events::BlobCertified => Ok(BlobEvent::Certified(value.try_into()?)),
            contracts::events::InvalidBlobID => Ok(BlobEvent::InvalidBlobID(value.try_into()?)),
            contracts::events::BlobDeleted => Ok(BlobEvent::Deleted(value.try_into()?)),
            contracts::events::DenyListBlobDeleted => {
                Ok(BlobEvent::DenyListBlobDeleted(value.try_into()?))
            }
            _ => Err(anyhow!("could not convert to blob event: {}", value)),
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

impl AssociatedSuiEvent for EpochParametersSelected {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::EpochParametersSelected;
}

impl TryFrom<SuiEvent> for EpochParametersSelected {
    type Error = MoveConversionError;

    fn try_from(sui_event: SuiEvent) -> Result<Self, Self::Error> {
        ensure_event_type(&sui_event, &Self::EVENT_STRUCT)?;

        let next_epoch = bcs::from_bytes(sui_event.bcs.bytes())?;
        Ok(Self {
            next_epoch,
            event_id: sui_event.id,
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

impl AssociatedSuiEvent for EpochChangeStart {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::EpochChangeStart;
}

impl TryFrom<SuiEvent> for EpochChangeStart {
    type Error = MoveConversionError;

    fn try_from(sui_event: SuiEvent) -> Result<Self, Self::Error> {
        ensure_event_type(&sui_event, &Self::EVENT_STRUCT)?;

        let epoch = bcs::from_bytes(sui_event.bcs.bytes())?;
        Ok(Self {
            epoch,
            event_id: sui_event.id,
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

impl AssociatedSuiEvent for EpochChangeDone {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::EpochChangeDone;
}

impl TryFrom<SuiEvent> for EpochChangeDone {
    type Error = MoveConversionError;

    fn try_from(sui_event: SuiEvent) -> Result<Self, Self::Error> {
        ensure_event_type(&sui_event, &Self::EVENT_STRUCT)?;

        let epoch = bcs::from_bytes(sui_event.bcs.bytes())?;
        Ok(Self {
            epoch,
            event_id: sui_event.id,
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

impl AssociatedSuiEvent for ShardsReceived {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::ShardsReceived;
}

impl TryFrom<SuiEvent> for ShardsReceived {
    type Error = MoveConversionError;

    fn try_from(sui_event: SuiEvent) -> Result<Self, Self::Error> {
        ensure_event_type(&sui_event, &Self::EVENT_STRUCT)?;

        let (epoch, shards) = bcs::from_bytes(sui_event.bcs.bytes())?;
        Ok(Self {
            epoch,
            shards,
            event_id: sui_event.id,
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

impl AssociatedSuiEvent for ShardRecoveryStart {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::ShardRecoveryStart;
}

impl TryFrom<SuiEvent> for ShardRecoveryStart {
    type Error = MoveConversionError;

    fn try_from(sui_event: SuiEvent) -> Result<Self, Self::Error> {
        ensure_event_type(&sui_event, &Self::EVENT_STRUCT)?;

        let (epoch, shards) = bcs::from_bytes(sui_event.bcs.bytes())?;
        Ok(Self {
            epoch,
            shards,
            event_id: sui_event.id,
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

impl AssociatedSuiEvent for ContractUpgradedEvent {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::ContractUpgraded;
}

impl TryFrom<SuiEvent> for ContractUpgradedEvent {
    type Error = MoveConversionError;

    fn try_from(sui_event: SuiEvent) -> Result<Self, Self::Error> {
        ensure_event_type(&sui_event, &Self::EVENT_STRUCT)?;

        let (epoch, package_id, version) = bcs::from_bytes(sui_event.bcs.bytes())?;
        Ok(Self {
            epoch,
            package_id,
            version,
            event_id: sui_event.id,
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

impl AssociatedSuiEvent for ContractUpgradeProposedEvent {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::ContractUpgradeProposed;
}

impl TryFrom<SuiEvent> for ContractUpgradeProposedEvent {
    type Error = MoveConversionError;

    fn try_from(sui_event: SuiEvent) -> Result<Self, Self::Error> {
        ensure_event_type(&sui_event, &Self::EVENT_STRUCT)?;

        let (epoch, package_digest) = bcs::from_bytes(sui_event.bcs.bytes())?;
        Ok(Self {
            epoch,
            package_digest,
            event_id: sui_event.id,
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

impl AssociatedSuiEvent for ContractUpgradeQuorumReachedEvent {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::ContractUpgradeQuorumReached;
}

impl TryFrom<SuiEvent> for ContractUpgradeQuorumReachedEvent {
    type Error = MoveConversionError;

    fn try_from(sui_event: SuiEvent) -> Result<Self, Self::Error> {
        ensure_event_type(&sui_event, &Self::EVENT_STRUCT)?;

        let (epoch, package_digest) = bcs::from_bytes(sui_event.bcs.bytes())?;
        Ok(Self {
            epoch,
            package_digest,
            event_id: sui_event.id,
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

impl AssociatedSuiEvent for ProtocolVersionUpdatedEvent {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::ProtocolVersionUpdated;
}

impl TryFrom<SuiEvent> for ProtocolVersionUpdatedEvent {
    type Error = MoveConversionError;

    fn try_from(sui_event: SuiEvent) -> Result<Self, Self::Error> {
        ensure_event_type(&sui_event, &Self::EVENT_STRUCT)?;

        let (epoch, start_epoch, protocol_version) = bcs::from_bytes(sui_event.bcs.bytes())?;
        Ok(Self {
            epoch,
            start_epoch,
            protocol_version,
            event_id: sui_event.id,
        })
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

impl AssociatedSuiEvent for RegisterDenyListUpdateEvent {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::RegisterDenyListUpdate;
}

impl TryFrom<SuiEvent> for RegisterDenyListUpdateEvent {
    type Error = MoveConversionError;

    fn try_from(sui_event: SuiEvent) -> Result<Self, Self::Error> {
        ensure_event_type(&sui_event, &Self::EVENT_STRUCT)?;

        let (epoch, root, sequence_number, node_id) = bcs::from_bytes(sui_event.bcs.bytes())?;

        Ok(Self {
            epoch,
            root,
            sequence_number,
            node_id,
            event_id: sui_event.id,
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

impl AssociatedSuiEvent for DenyListUpdateEvent {
    const EVENT_STRUCT: StructTag<'static> = contracts::events::DenyListUpdate;
}

impl TryFrom<SuiEvent> for DenyListUpdateEvent {
    type Error = MoveConversionError;

    fn try_from(sui_event: SuiEvent) -> Result<Self, Self::Error> {
        ensure_event_type(&sui_event, &Self::EVENT_STRUCT)?;

        let (epoch, root, sequence_number, node_id) = bcs::from_bytes(sui_event.bcs.bytes())?;

        Ok(Self {
            epoch,
            root,
            sequence_number,
            node_id,
            event_id: sui_event.id,
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
}

impl ProtocolEvent {
    /// Returns the event ID of the wrapped event.
    pub fn event_id(&self) -> EventID {
        match self {
            ProtocolEvent::ProtocolVersionUpdated(event) => event.event_id,
        }
    }

    /// The epoch corresponding to the protocol event.
    pub fn event_epoch(&self) -> Epoch {
        match self {
            ProtocolEvent::ProtocolVersionUpdated(event) => event.epoch,
        }
    }

    /// The name of the event.
    pub fn name(&self) -> &'static str {
        match self {
            ProtocolEvent::ProtocolVersionUpdated(_) => "ProtocolVersionUpdated",
        }
    }

    /// The protocol version.
    pub fn protocol_version(&self) -> u64 {
        match self {
            ProtocolEvent::ProtocolVersionUpdated(event) => event.protocol_version,
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
        }
    }

    /// Returns the epoch in which the event was issued.
    pub fn event_epoch(&self) -> Epoch {
        match self {
            ContractEvent::BlobEvent(event) => event.event_epoch(),
            ContractEvent::EpochChangeEvent(event) => event.event_epoch(),
            ContractEvent::PackageEvent(event) => event.event_epoch(),
            ContractEvent::DenyListEvent(event) => event.event_epoch(),
            ContractEvent::ProtocolEvent(event) => event.event_epoch(),
        }
    }

    /// Returns true if the event is a blob extension.
    pub fn is_blob_extension(&self) -> bool {
        match self {
            ContractEvent::BlobEvent(event) => event.is_blob_extension(),
            ContractEvent::EpochChangeEvent(_) => false,
            ContractEvent::PackageEvent(_) => false,
            ContractEvent::DenyListEvent(_) => false,
            ContractEvent::ProtocolEvent(_) => false,
        }
    }
}

impl TryFrom<SuiEvent> for ContractEvent {
    type Error = anyhow::Error;

    fn try_from(value: SuiEvent) -> Result<Self, Self::Error> {
        match (&value.type_).into() {
            contracts::events::BlobRegistered => Ok(ContractEvent::BlobEvent(
                BlobEvent::Registered(value.try_into()?),
            )),
            contracts::events::BlobCertified => Ok(ContractEvent::BlobEvent(BlobEvent::Certified(
                value.try_into()?,
            ))),
            contracts::events::BlobDeleted => Ok(ContractEvent::BlobEvent(BlobEvent::Deleted(
                value.try_into()?,
            ))),
            contracts::events::InvalidBlobID => Ok(ContractEvent::BlobEvent(
                BlobEvent::InvalidBlobID(value.try_into()?),
            )),
            contracts::events::EpochParametersSelected => Ok(ContractEvent::EpochChangeEvent(
                EpochChangeEvent::EpochParametersSelected(value.try_into()?),
            )),
            contracts::events::EpochChangeStart => Ok(ContractEvent::EpochChangeEvent(
                EpochChangeEvent::EpochChangeStart(value.try_into()?),
            )),
            contracts::events::EpochChangeDone => Ok(ContractEvent::EpochChangeEvent(
                EpochChangeEvent::EpochChangeDone(value.try_into()?),
            )),
            contracts::events::ShardsReceived => Ok(ContractEvent::EpochChangeEvent(
                EpochChangeEvent::ShardsReceived(value.try_into()?),
            )),
            contracts::events::ShardRecoveryStart => Ok(ContractEvent::EpochChangeEvent(
                EpochChangeEvent::ShardRecoveryStart(value.try_into()?),
            )),
            contracts::events::ContractUpgraded => Ok(ContractEvent::PackageEvent(
                PackageEvent::ContractUpgraded(value.try_into()?),
            )),
            contracts::events::ContractUpgradeProposed => Ok(ContractEvent::PackageEvent(
                PackageEvent::ContractUpgradeProposed(value.try_into()?),
            )),
            contracts::events::ContractUpgradeQuorumReached => Ok(ContractEvent::PackageEvent(
                PackageEvent::ContractUpgradeQuorumReached(value.try_into()?),
            )),
            contracts::events::RegisterDenyListUpdate => Ok(ContractEvent::DenyListEvent(
                DenyListEvent::RegisterDenyListUpdate(value.try_into()?),
            )),
            contracts::events::DenyListUpdate => Ok(ContractEvent::DenyListEvent(
                DenyListEvent::DenyListUpdate(value.try_into()?),
            )),
            contracts::events::DenyListBlobDeleted => Ok(ContractEvent::BlobEvent(
                BlobEvent::DenyListBlobDeleted(value.try_into()?),
            )),
            contracts::events::ProtocolVersionUpdated => Ok(ContractEvent::ProtocolEvent(
                ProtocolEvent::ProtocolVersionUpdated(value.try_into()?),
            )),
            _ => unreachable!("Encountered unexpected unrecognized events {}", value),
        }
    }
}
