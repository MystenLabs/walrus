// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Types for paginated owned-object listing.

use bytes::Bytes;
use move_core_types::language_storage::StructTag;
use sui_types::base_types::{ObjectDigest, ObjectID, SequenceNumber};

/// A single owned object as returned by the gRPC `ListOwnedObjects` RPC.
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
pub struct OwnedObjectsCursor(Bytes);

impl OwnedObjectsCursor {
    /// Wraps an opaque gRPC page-token.
    pub fn from_grpc(token: Bytes) -> Self {
        Self(token)
    }

    /// Extracts the gRPC page-token.
    pub fn into_grpc(self) -> Bytes {
        self.0
    }
}

/// A page of owned objects.
#[derive(Debug, Clone, Default)]
pub struct OwnedObjectsPage {
    /// Owned objects in this page.
    pub data: Vec<OwnedObjectEntry>,
    /// Cursor for the next page, if any.
    pub next_cursor: Option<OwnedObjectsCursor>,
    /// Whether the underlying RPC says there is at least one more page.
    pub has_next_page: bool,
}
