// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Structures of client results returned by the daemon or through the JSON API.

use std::{fmt::Display, path::PathBuf};

use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use sui_types::{base_types::ObjectID, event::EventID};
use utoipa::ToSchema;
use walrus_core::{BlobId, Epoch};
use walrus_sui::{EventIdSchema, ObjectIdSchema, types::move_structs::Blob};

use super::{client_types::StoredQuiltPatch, resource::RegisterBlobOp};

/// Either an event ID or an object ID.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum EventOrObjectId {
    /// The variant representing an event ID.
    #[schema(value_type = EventIdSchema)]
    Event(EventID),
    /// The variant representing an object ID.
    #[schema(value_type = ObjectIdSchema)]
    Object(ObjectID),
}

impl Display for EventOrObjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventOrObjectId::Event(event_id) => {
                write!(
                    f,
                    "Certification event ID: {}",
                    crate::format_event_id(event_id)
                )
            }
            EventOrObjectId::Object(object_id) => {
                write!(f, "Owned Blob registration object ID: {object_id}")
            }
        }
    }
}

/// Blob store result with its file path.
#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct BlobStoreResultWithPath {
    /// The result of the store operation.
    pub blob_store_result: BlobStoreResult,
    /// The file path to the blob.
    pub path: PathBuf,
}

/// Result when attempting to store a blob.
#[serde_as]
#[derive(Debug, Clone, Serialize, ToSchema, PartialEq)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum BlobStoreResult {
    /// The blob already exists within Walrus, was certified, and is stored for at least the
    /// intended duration.
    AlreadyCertified {
        /// The blob ID.
        #[serde_as(as = "DisplayFromStr")]
        blob_id: BlobId,
        /// The event where the blob was certified, or the object ID of the registered blob.
        ///
        /// The object ID of the registered blob is used in place of the event ID when the blob is
        /// deletable, already certified, and owned by the client.
        #[serde(flatten)]
        event_or_object: EventOrObjectId,
        /// The epoch until which the blob is stored (exclusive).
        #[schema(value_type = u64)]
        end_epoch: Epoch,
    },
    /// The blob was newly created; this contains the newly created Sui object associated with the
    /// blob.
    NewlyCreated {
        /// The Sui blob object that holds the newly created blob.
        blob_object: Blob,
        /// The operation that created the blob.
        resource_operation: RegisterBlobOp,
        /// The storage cost, excluding gas.
        cost: u64,
        /// The shared blob object ID if created.
        #[serde_as(as = "Option<DisplayFromStr>")]
        #[serde(skip_serializing_if = "Option::is_none")]
        #[schema(value_type = Option<ObjectIdSchema>)]
        shared_blob_object: Option<ObjectID>,
    },
    /// The blob is known to Walrus but was marked as invalid.
    ///
    /// This indicates a bug within the client, the storage nodes, or more than a third malicious
    /// storage nodes.
    MarkedInvalid {
        /// The blob ID.
        #[serde_as(as = "DisplayFromStr")]
        blob_id: BlobId,
        /// The event where the blob was marked as invalid.
        #[schema(value_type = EventIdSchema)]
        event: EventID,
    },
    /// The blob is managed by a BlobManager (BlobManager owns the blob object).
    ManagedByBlobManager {
        /// The blob ID.
        #[serde_as(as = "DisplayFromStr")]
        blob_id: BlobId,
        /// The object ID of the blob in BlobManager's table.
        #[serde_as(as = "DisplayFromStr")]
        #[schema(value_type = ObjectIdSchema)]
        blob_manager_object_id: ObjectID,
        /// The operation that created or reused the blob.
        resource_operation: RegisterBlobOp,
        /// The storage cost, excluding gas.
        cost: u64,
        /// The epoch until which the blob is stored (exclusive).
        #[schema(value_type = u64)]
        end_epoch: Epoch,
    },
    /// Operation failed.
    Error {
        /// The blob ID.
        #[serde_as(as = "Option<DisplayFromStr>")]
        blob_id: Option<BlobId>,
        /// The phase after the error occurred.
        failure_phase: String,
        /// The error message.
        error_msg: String,
    },
}

impl BlobStoreResult {
    /// Returns the blob ID.
    pub fn blob_id(&self) -> Option<BlobId> {
        match self {
            Self::AlreadyCertified { blob_id, .. } => Some(*blob_id),
            Self::MarkedInvalid { blob_id, .. } => Some(*blob_id),
            Self::NewlyCreated {
                blob_object: Blob { blob_id, .. },
                ..
            } => Some(*blob_id),
            Self::ManagedByBlobManager { blob_id, .. } => Some(*blob_id),
            Self::Error { blob_id, .. } => *blob_id,
        }
    }

    /// Returns the end epoch of the blob.
    pub fn end_epoch(&self) -> Option<Epoch> {
        match self {
            Self::AlreadyCertified { end_epoch, .. } => Some(*end_epoch),
            Self::NewlyCreated { blob_object, .. } => Some(blob_object.storage.end_epoch),
            Self::ManagedByBlobManager { end_epoch, .. } => Some(*end_epoch),
            Self::MarkedInvalid { .. } => None,
            Self::Error { .. } => None,
        }
    }

    /// Returns true if the blob is not stored.
    pub fn is_not_stored(&self) -> bool {
        matches!(self, Self::MarkedInvalid { .. } | Self::Error { .. })
    }

    /// Returns a [`BlobStoreResultWithPath`] with the given path.
    pub fn with_path(self, path: PathBuf) -> BlobStoreResultWithPath {
        BlobStoreResultWithPath {
            path,
            blob_store_result: self,
        }
    }
}

/// Result when attempting to store a quilt.
#[derive(Serialize, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QuiltStoreResult {
    /// The result of storing the quilt data as a blob.
    pub blob_store_result: BlobStoreResult,
    /// The structure of the quilt.
    pub stored_quilt_blobs: Vec<StoredQuiltPatch>,
}
