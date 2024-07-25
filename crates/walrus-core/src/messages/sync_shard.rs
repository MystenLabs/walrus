// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

use super::SignedMessage;
use crate::{BlobId, Epoch, ShardIndex};

#[derive(Debug, Serialize, Deserialize)]
pub struct SyncShardRequestV1 {
    /// The shard index that is requested to be synced.
    shard_index: ShardIndex,

    /// Whether the sync is for the primary sliver or the secondary sliver in the shard.
    /// Note that storage node stores primary and secondary slivers in separate
    /// RocksDB column family, so it's more efficient to transfer them separately.
    primary_sliver: bool,

    /// The ID of the blob to start syncing from.
    starting_blob_id: BlobId,

    /// The number of blobs to sync starting from `starting_blob_id`.
    /// Since the blobs are stored in RocksDB ordered by the key, the sync basically
    /// scans the RocksDB from `starting_blob_id` and reads `num_blobs` blobs for
    /// efficient scanning.
    ///
    /// Note that only blobs certified at the moment of epoch change are synced.
    num_blobs: u64,

    /// The epoch up until which blobs were certified. In the context of
    /// an epoch change, this is the previous epoch.
    epoch: Epoch,
}

/// Represents a request to sync a shard from a storage node.
#[derive(Debug, Serialize, Deserialize)]
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
        num_blobs: u64,
        epoch: Epoch,
    ) -> SyncShardRequest {
        Self::V1(SyncShardRequestV1 {
            shard_index,
            primary_sliver,
            starting_blob_id,
            num_blobs,
            epoch,
        })
    }
}

/// Represents a signed sync shard request.
pub type SignedSyncShardRequest = SignedMessage<SyncShardRequest>;
