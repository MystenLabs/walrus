// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use alloc::vec::Vec;

use serde::{Deserialize, Serialize};

use super::{Intent, InvalidIntent, ProtocolMessage, SignedMessage};
use crate::{BlobId, Epoch, ShardIndex, Sliver, SliverType, messages::IntentType};

/// Represents a version 1 of the sync shard request for transferring an entire shard from
/// one storage node to another.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncShardRequestV1 {
    /// The shard index that is requested to be synced.
    shard_index: ShardIndex,

    /// The type of sliver to fetch.
    /// Note that storage node stores primary and secondary slivers in separate
    /// RocksDB column family, so it's more efficient to transfer them separately.
    sliver_type: SliverType,

    /// The ID of the blob to start syncing from.
    starting_blob_id: BlobId,

    /// The number of slivers to sync starting from `starting_blob_id`.
    /// Since the blobs are stored in RocksDB ordered by the key, the sync basically
    /// scans the RocksDB from `starting_blob_id` and reads `sliver_count` slivers for
    /// efficient scanning.
    ///
    /// Note that only blobs certified at the moment of epoch change are synced.
    sliver_count: u64,

    /// The epoch up until which blobs were certified, e.g. sync all certified blobs whose
    /// certification epoch < `epoch`. In the context of an epoch change, this is the epoch
    /// that the storage node is transitioning to.
    epoch: Epoch,
}

/// Represents a request to sync a shard from a storage node.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum SyncShardRequest {
    /// Version 1 of the sync shard request.
    V1(SyncShardRequestV1),
}

impl SyncShardRequest {
    /// Creates a new `SyncShardRequest` with the specified parameters.
    pub fn new(
        shard_index: ShardIndex,
        sliver_type: SliverType,
        starting_blob_id: BlobId,
        sliver_count: u64,
        epoch: Epoch,
    ) -> SyncShardRequest {
        Self::V1(SyncShardRequestV1 {
            shard_index,
            sliver_type,
            starting_blob_id,
            sliver_count,
            epoch,
        })
    }

    /// Returns the shard index of the request.
    pub fn shard_index(&self) -> ShardIndex {
        match self {
            Self::V1(request) => request.shard_index,
        }
    }

    /// Returns the sliver type of the request.
    pub fn sliver_type(&self) -> SliverType {
        match self {
            Self::V1(request) => request.sliver_type,
        }
    }

    /// Returns the starting blob ID of the request.
    pub fn starting_blob_id(&self) -> BlobId {
        match self {
            Self::V1(request) => request.starting_blob_id,
        }
    }

    /// Returns the number of slivers to sync starting from the starting blob ID.
    pub fn sliver_count(&self) -> usize {
        match self {
            Self::V1(request) => request
                .sliver_count
                .try_into()
                .expect("nodes are expected to run on 64-bit architectures"),
        }
    }

    /// Returns the epoch of the request.
    pub fn epoch(&self) -> Epoch {
        match self {
            Self::V1(request) => request.epoch,
        }
    }
}

/// A message stating that a Blob Id is invalid.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "ProtocolMessage<SyncShardRequest>")]
pub struct SyncShardMsg(pub(crate) ProtocolMessage<SyncShardRequest>);

impl SyncShardMsg {
    const INTENT: Intent = Intent::storage(IntentType::SYNC_SHARD_MSG);

    /// Creates a new InvalidBlobIdMsg message for the provided blob ID.
    pub fn new(epoch: Epoch, request: SyncShardRequest) -> Self {
        Self(ProtocolMessage {
            intent: Intent::storage(IntentType::SYNC_SHARD_MSG),
            epoch,
            message_contents: request,
        })
    }
}

impl TryFrom<ProtocolMessage<SyncShardRequest>> for SyncShardMsg {
    type Error = InvalidIntent;
    fn try_from(protocol_message: ProtocolMessage<SyncShardRequest>) -> Result<Self, Self::Error> {
        if protocol_message.intent == Self::INTENT {
            Ok(Self(protocol_message))
        } else {
            Err(InvalidIntent {
                expected: Self::INTENT,
                actual: protocol_message.intent,
            })
        }
    }
}

impl AsRef<ProtocolMessage<SyncShardRequest>> for SyncShardMsg {
    fn as_ref(&self) -> &ProtocolMessage<SyncShardRequest> {
        &self.0
    }
}

/// Represents a signed sync shard request.
pub type SignedSyncShardRequest = SignedMessage<SyncShardMsg>;

/// The sync shard response for transferring a shard from one storage node to another.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum SyncShardResponse {
    /// Version 1 of the sync shard response.
    V1(Vec<(BlobId, Sliver)>),
}

impl Default for SyncShardResponse {
    fn default() -> Self {
        Self::V1(Vec::new())
    }
}

impl From<Vec<(BlobId, Sliver)>> for SyncShardResponse {
    fn from(val: Vec<(BlobId, Sliver)>) -> Self {
        Self::V1(val)
    }
}

impl From<SyncShardResponse> for Vec<(BlobId, Sliver)> {
    fn from(val: SyncShardResponse) -> Vec<(BlobId, Sliver)> {
        match val {
            SyncShardResponse::V1(val) => val,
        }
    }
}
