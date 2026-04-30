// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Protocol-agnostic types for paginated owned-object listing.
//!
//! These mirror the JSON-RPC `ObjectsPage` envelope but live in `walrus-sui` so callers do not
//! depend on `sui_sdk::rpc_types`. Both the JSON-RPC fallback and the gRPC `ListOwnedObjects`
//! path produce the same shape.

use anyhow::Context as _;
use bytes::Bytes;
use move_core_types::language_storage::StructTag;
use sui_sdk::rpc_types::{SuiData as _, SuiObjectResponse};
use sui_types::base_types::{ObjectDigest, ObjectID, SequenceNumber};

use crate::contracts::MoveConversionError;

/// A single owned object as returned by either RPC backend.
#[derive(Debug, Clone)]
pub struct OwnedObjectEntry {
    /// The on-chain object id.
    pub object_id: ObjectID,
    /// Object version.
    pub version: SequenceNumber,
    /// Object digest.
    pub digest: ObjectDigest,
    /// Move struct tag of the object.
    pub object_type: StructTag,
    /// BCS bytes of the Move object contents (the `bcs_bytes` of the underlying Move object).
    pub bcs_bytes: Vec<u8>,
}

/// Opaque page cursor returned by the previous page; pass it back into the next call.
#[derive(Debug, Clone)]
pub struct OwnedObjectsCursor(OwnedObjectsCursorInner);

#[derive(Debug, Clone)]
enum OwnedObjectsCursorInner {
    JsonRpc(ObjectID),
    Grpc(Bytes),
}

impl OwnedObjectsCursor {
    /// Wraps a JSON-RPC `ObjectID` cursor.
    pub fn from_json_rpc(cursor: ObjectID) -> Self {
        Self(OwnedObjectsCursorInner::JsonRpc(cursor))
    }

    /// Wraps an opaque gRPC page-token.
    pub fn from_grpc(token: Bytes) -> Self {
        Self(OwnedObjectsCursorInner::Grpc(token))
    }

    /// Extracts the JSON-RPC cursor variant. Errors if the cursor is from the gRPC backend.
    pub fn into_json_rpc(self) -> anyhow::Result<ObjectID> {
        match self.0 {
            OwnedObjectsCursorInner::JsonRpc(id) => Ok(id),
            OwnedObjectsCursorInner::Grpc(_) => Err(anyhow::anyhow!(
                "expected JSON-RPC owned-objects cursor, got gRPC variant"
            )),
        }
    }

    /// Extracts the gRPC cursor variant. Errors if the cursor is from the JSON-RPC backend.
    pub fn into_grpc(self) -> anyhow::Result<Bytes> {
        match self.0 {
            OwnedObjectsCursorInner::Grpc(bytes) => Ok(bytes),
            OwnedObjectsCursorInner::JsonRpc(_) => Err(anyhow::anyhow!(
                "expected gRPC owned-objects cursor, got JSON-RPC variant"
            )),
        }
    }
}

/// A page of owned objects. Drop-in replacement for `sui_sdk::rpc_types::ObjectsPage` that does
/// not leak `sui_sdk` types.
#[derive(Debug, Clone, Default)]
pub struct OwnedObjectsPage {
    /// Owned objects in this page.
    pub data: Vec<OwnedObjectEntry>,
    /// Cursor for the next page, if any.
    pub next_cursor: Option<OwnedObjectsCursor>,
    /// Whether the underlying RPC says there is at least one more page.
    pub has_next_page: bool,
}

impl TryFrom<SuiObjectResponse> for OwnedObjectEntry {
    type Error = anyhow::Error;

    fn try_from(response: SuiObjectResponse) -> Result<Self, Self::Error> {
        let data = response.data.with_context(|| {
            format!(
                "response does not contain object data [err={:?}]",
                response.error
            )
        })?;
        let raw = data
            .bcs
            .as_ref()
            .ok_or(MoveConversionError::NoBcs)?
            .try_as_move()
            .ok_or(MoveConversionError::NotMoveObject)?;
        Ok(OwnedObjectEntry {
            object_id: data.object_id,
            version: data.version,
            digest: data.digest,
            object_type: raw.type_.clone(),
            bcs_bytes: raw.bcs_bytes.clone(),
        })
    }
}
