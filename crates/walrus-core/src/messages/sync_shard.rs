// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

use super::{Intent, InvalidIntent, ProtocolMessage, SignedMessage};
use crate::{messages::IntentType, BlobId, Epoch, ShardIndex};

/// Represents a version 1 of the sync shard request for transferring an entire shard from
/// one storage node to another.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncShardRequestV1 {
    /// The shard index that is requested to be synced.
    shard_index: ShardIndex,

    /// Whether the sync is for the primary sliver or the secondary sliver in the shard.
    /// Note that storage node stores primary and secondary slivers in separate
    /// RocksDB column family, so it's more efficient to transfer them separately.
    primary_sliver: bool,

    /// The ID of the blob to start syncing from.
    starting_blob_id: BlobId,

    /// The number of slivers to sync starting from `starting_blob_id`.
    /// Since the blobs are stored in RocksDB ordered by the key, the sync basically
    /// scans the RocksDB from `starting_blob_id` and reads `sliver_count` slivers for
    /// efficient scanning.
    ///
    /// Note that only blobs certified at the moment of epoch change are synced.
    sliver_count: u64,

    /// The epoch up until which blobs were certified. In the context of
    /// an epoch change, this is the previous epoch.
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
        primary_sliver: bool,
        starting_blob_id: BlobId,
        sliver_count: u64,
        epoch: Epoch,
    ) -> SyncShardRequest {
        Self::V1(SyncShardRequestV1 {
            shard_index,
            primary_sliver,
            starting_blob_id,
            sliver_count,
            epoch,
        })
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
