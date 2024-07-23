// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

use super::SignedMessage;
use crate::{BlobId, Epoch, ShardIndex};

#[derive(Debug, Serialize, Deserialize)]
pub struct SyncShardRequestV1 {
    shard_index: ShardIndex,
    pub primary_sliver: bool,
    starting_blob_id: BlobId,
    num_blobs: u64,
    epoch: Epoch,
}

#[derive(Debug, Serialize, Deserialize)]
/// Represents a request to sync a shard.
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
