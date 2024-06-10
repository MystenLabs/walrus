// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use sui_types::event::EventID;
use walrus_core::{
    encoding::SliverVerificationError,
    inconsistency::InconsistencyVerificationError,
    metadata::VerificationError,
    Epoch,
    ShardIndex,
};

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
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Debug, thiserror::Error)]
pub enum RetrieveSliverError {
    #[error("the requested sliver is unavailable")]
    Unavailable,
    #[error(transparent)]
    ShardNotAssigned(#[from] ShardNotAssigned),
    #[error("the requested sliver index is out of range: {0}")]
    SliverOutOfRange(#[from] IndexOutOfRange),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Debug, thiserror::Error)]
pub enum ComputeStorageConfirmationError {
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
    #[error("the blob for this metadata has already expired")]
    BlobExpired,
    #[error("the blob for this metadata has not been registered")]
    NotRegistered,
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
            RetrieveMetadataError::Internal(error) => Self::Internal(error),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BlobStatusError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}
