// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Walrus event type bindings. Replicates the move event types in Rust.

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use sui_sdk::rpc_types::SuiEvent;
use sui_types::{base_types::ObjectID, event::EventID};
use walrus_core::{ensure, BlobId, EncodingType, Epoch};

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
            bcs::from_bytes(&sui_event.bcs)?;

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
            bcs::from_bytes(&sui_event.bcs)?;
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

        let (epoch, blob_id) = bcs::from_bytes(&sui_event.bcs)?;
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
            contracts::events::BlobRegistered => Ok(BlobEvent::Registered(value.try_into()?)),
            contracts::events::BlobCertified => Ok(BlobEvent::Certified(value.try_into()?)),
            contracts::events::InvalidBlobID => Ok(BlobEvent::InvalidBlobID(value.try_into()?)),
            _ => Err(anyhow!("could not convert event: {}", value)),
        }
    }
}
