// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use serde::Serialize;
use sui_types::event::EventID;
use typed_store::TypedStoreError;
use walrus_core::{
    encoding::SliverVerificationError,
    inconsistency::InconsistencyVerificationError,
    messages::MessageVerificationError,
    metadata::VerificationError,
    Epoch,
    ShardIndex,
};
use walrus_sdk::error::NodeError;

use super::storage::ShardStatus;

/// Type used for internal errors.
pub type InternalError = anyhow::Error;

#[derive(Debug, thiserror::Error)]
#[error("shard {0} is not assigned to this node in epoch {1}")]
pub struct ShardNotAssigned(pub ShardIndex, pub Epoch);

#[derive(Debug, thiserror::Error)]
#[error("requires 0 <= index ({index}) < {max}")]
pub struct IndexOutOfRange {
    pub index: u16,
    pub max: u16,
}

#[derive(Debug, thiserror::Error)]
pub enum RetrieveMetadataError {
    #[error("the requested metadata is unavailable")]
    Unavailable,
    #[error("the requested metadata is blocked")]
    Forbidden,
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Debug, thiserror::Error)]
pub enum RetrieveSliverError {
    #[error("the requested sliver is unavailable")]
    Unavailable,
    #[error("the requested sliver is forbidden")]
    Forbidden,
    #[error(transparent)]
    ShardNotAssigned(#[from] ShardNotAssigned),
    #[error("the requested sliver index is out of range: {0}")]
    SliverOutOfRange(#[from] IndexOutOfRange),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Debug, thiserror::Error)]
pub enum ComputeStorageConfirmationError {
    #[error("the blob has not been registered or has already expired")]
    NotCurrentlyRegistered,
    #[error("the required slivers are not all stored")]
    NotFullyStored,
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Debug, thiserror::Error)]
pub enum StoreMetadataError {
    #[error(transparent)]
    InvalidMetadata(#[from] VerificationError),
    #[error("the blob for this metadata is invalid: {0:?}")]
    InvalidBlob(EventID),
    #[error("the blob for this metadata has not been registered or has already expired")]
    NotCurrentlyRegistered,
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Debug, thiserror::Error)]
pub enum RetrieveSymbolError {
    #[error("the requested recovery symbol is invalid for the committee size: {0}")]
    RecoverySymbolOutOfRange(#[from] IndexOutOfRange),
    #[error("the sliver from which to extract the recovery symbol could not be retrieved: {0}")]
    RetrieveSliver(#[from] RetrieveSliverError),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Debug, thiserror::Error)]
pub enum StoreSliverError {
    #[error("the requested sliver index is out of range: {0}")]
    SliverOutOfRange(#[from] IndexOutOfRange),
    #[error("the blob is not registered")]
    NotCurrentlyRegistered,
    #[error("blob metadata is required but missing")]
    MissingMetadata,
    #[error(transparent)]
    InvalidSliver(#[from] SliverVerificationError),
    #[error(transparent)]
    ShardNotAssigned(#[from] ShardNotAssigned),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Debug, thiserror::Error)]
pub enum InconsistencyProofError {
    #[error("blob metadata is required but missing")]
    MissingMetadata,
    #[error(transparent)]
    InvalidProof(#[from] InconsistencyVerificationError),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl From<RetrieveMetadataError> for InconsistencyProofError {
    fn from(value: RetrieveMetadataError) -> Self {
        match value {
            RetrieveMetadataError::Unavailable => Self::MissingMetadata,
            RetrieveMetadataError::Forbidden => Self::MissingMetadata,
            RetrieveMetadataError::Internal(error) => Self::Internal(error),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BlobStatusError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

/// Error returned when the epoch in a request is invalid.
#[derive(Debug, thiserror::Error, Serialize, Clone)]
#[error("Invalid epoch. Client epoch: {request_epoch}. Server epoch: {server_epoch}")]
pub struct InvalidEpochError {
    pub request_epoch: Epoch,
    pub server_epoch: Epoch,
}

#[derive(Debug, thiserror::Error)]
pub enum SyncShardServiceError {
    #[error("The client is not authorized to perform sync shard operation")]
    Unauthorized,
    #[error(transparent)]
    MessageVerificationError(#[from] MessageVerificationError),
    #[error(transparent)]
    ShardNotAssigned(#[from] ShardNotAssigned),
    #[error(transparent)]
    InvalidEpoch(#[from] InvalidEpochError),
    #[error(transparent)]
    Internal(#[from] InternalError),
    #[error(transparent)]
    StorageError(#[from] TypedStoreError),
}

#[derive(Debug, thiserror::Error)]
pub enum SyncShardClientError {
    #[error("The destination node does not have a valid client to talk to the source node")]
    NoSyncClient,
    #[error("Unable to find the owner for shard {0}")]
    NoOwnerForShard(ShardIndex),
    #[error("The shard {0} is not in a valid status for syncing: {1}")]
    InvalidShardStatusToSync(ShardIndex, ShardStatus),
    #[error(transparent)]
    ShardNotAssigned(#[from] ShardNotAssigned),
    #[error(transparent)]
    StorageError(#[from] TypedStoreError),
    #[error(transparent)]
    Internal(#[from] InternalError),
    #[error(transparent)]
    RequestError(#[from] NodeError),
}
