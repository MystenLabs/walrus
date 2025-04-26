// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use anyhow::anyhow;
use opentelemetry::trace::TraceContextExt as _;
use serde::Serialize;
use sui_types::event::EventID;
use thiserror::Error;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;
use typed_store::TypedStoreError;
use walrus_core::{
    EncodingType,
    Epoch,
    SUPPORTED_ENCODING_TYPES,
    ShardIndex,
    encoding::SliverVerificationError,
    inconsistency::InconsistencyVerificationError,
    messages::MessageVerificationError,
    metadata::VerificationError,
};
use walrus_proc_macros::RestApiError;
use walrus_rest_client::{
    api::errors::{
        DebugInfo,
        GLOBAL_ERROR_DOMAIN,
        STORAGE_NODE_ERROR_DOMAIN as ERROR_DOMAIN,
        Status,
        StatusCode as ApiStatusCode,
    },
    error::NodeError,
};
use walrus_sui::client::SuiClientError;

use crate::common::api::RestApiError;

/// Type used for internal errors.
pub type InternalError = anyhow::Error;

impl RestApiError for InternalError {
    fn message(&self) -> String {
        // Override the message for internal errors so that we do not give the user error details.
        "an internal error has occurred, please report it".to_owned()
    }

    fn status_code(&self) -> ApiStatusCode {
        ApiStatusCode::Internal
    }

    fn domain(&self) -> String {
        GLOBAL_ERROR_DOMAIN.to_owned()
    }

    fn reason(&self) -> String {
        "INTERNAL_ERROR".to_owned()
    }

    fn response_descriptions() -> HashMap<reqwest::StatusCode, Vec<String>> {
        [(
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            vec!["An internal server error has occurred. Please report this error.".to_owned()],
        )]
        .into()
    }

    fn add_details(&self, status: &mut Status) {
        let otel_context = Span::current().context();
        let trace_id = otel_context.span().span_context().trace_id();

        status.insert_details(
            DebugInfo {
                stack_entries: vec![],
                detail: format!("TraceID: {trace_id:x}"),
            }
            .into(),
        );
    }
}

/// The shard associated with the operation is not assigned to this storage node.
#[derive(Debug, thiserror::Error, RestApiError)]
#[error("shard {0} is not assigned to this node in epoch {1}")]
#[rest_api_error(
    reason = "SHARD_NOT_ASSIGNED", status = ApiStatusCode::FailedPrecondition, domain = ERROR_DOMAIN
)]
pub struct ShardNotAssigned(pub ShardIndex, pub Epoch);

/// The index identifying the resource is out-of-range for the system.
#[derive(Debug, thiserror::Error, RestApiError)]
#[error("requires 0 <= index ({index}) < {max}")]
#[rest_api_error(
    reason = "INDEX_OUT_OF_RANGE", status = ApiStatusCode::InvalidArgument, domain = ERROR_DOMAIN
)]
pub struct IndexOutOfRange {
    pub index: u16,
    pub max: u16,
}

#[derive(Debug, thiserror::Error, RestApiError)]
#[error("the service is currently unavailable")]
#[rest_api_error(
    reason = "UNAVAILABLE", status = ApiStatusCode::Unavailable, domain = GLOBAL_ERROR_DOMAIN
)]
pub struct Unavailable;

#[derive(Debug, thiserror::Error, RestApiError)]
#[rest_api_error(domain = ERROR_DOMAIN)]
pub enum RetrieveMetadataError {
    /// The requested metadata could not be found at this storage node. It has either not been
    /// uploaded, does not exist, or has already been deleted.
    #[error("the requested metadata is unavailable")]
    #[rest_api_error(reason = "METADATA_NOT_FOUND", status = ApiStatusCode::NotFound)]
    Unavailable,

    /// The metadata cannot be returned, as the associated blob has been
    /// blocked on this storage node.
    #[error("the requested metadata is blocked")]
    #[rest_api_error(reason = "FORBIDDEN_BLOB", status = ApiStatusCode::UnavailableForLegalReasons)]
    Forbidden,

    #[error(transparent)]
    #[rest_api_error(delegate)]
    Internal(#[from] InternalError),
}

#[derive(Debug, thiserror::Error, RestApiError)]
#[rest_api_error(domain = ERROR_DOMAIN)]
pub enum RetrieveSliverError {
    /// The requested sliver could not be found at this storage node. It has either not been
    /// uploaded, does not exist, or has already been deleted.
    #[error("the requested sliver is unavailable")]
    #[rest_api_error(reason = "SLIVER_NOT_FOUND", status = ApiStatusCode::NotFound)]
    Unavailable,

    /// The sliver cannot be returned, as the associated blob has been blocked on this storage node.
    #[error("the requested sliver is forbidden")]
    #[rest_api_error(reason = "FORBIDDEN_BLOB", status = ApiStatusCode::UnavailableForLegalReasons)]
    Forbidden,

    /// The index of the identified sliver is out of range for the system.
    #[error("the requested sliver index is out of range: {0}")]
    #[rest_api_error(delegate)]
    SliverOutOfRange(#[from] IndexOutOfRange),

    #[error(transparent)]
    #[rest_api_error(delegate)]
    ShardNotAssigned(#[from] ShardNotAssigned),

    #[error(transparent)]
    #[rest_api_error(delegate)]
    Internal(#[from] InternalError),
}

#[derive(Debug, thiserror::Error, RestApiError)]
#[rest_api_error(domain = ERROR_DOMAIN)]
pub enum ComputeStorageConfirmationError {
    /// The blob has not been registered or has already expired.
    #[error("the blob has not been registered or has already expired")]
    #[rest_api_error(reason = "NOT_REGISTERED", status = ApiStatusCode::FailedPrecondition)]
    NotCurrentlyRegistered,

    /// The storage node cannot produce a certificate, as it does not have the slivers for all of
    /// its shards. Complete the uploading of the slivers and then try again.
    #[error("the required slivers are not all stored")]
    #[rest_api_error(reason = "MISSING_SLIVERS", status = ApiStatusCode::FailedPrecondition)]
    NotFullyStored,

    #[error(transparent)]
    #[rest_api_error(delegate)]
    Internal(#[from] InternalError),
}

#[derive(Debug, thiserror::Error, RestApiError)]
#[rest_api_error(domain = ERROR_DOMAIN)]
pub enum StoreMetadataError {
    /// The blob has not been registered or has already expired.
    #[error("the blob has not been registered or has already expired")]
    #[rest_api_error(reason = "NOT_REGISTERED", status = ApiStatusCode::FailedPrecondition)]
    NotCurrentlyRegistered,

    /// The provided metadata is not valid for the blob.
    #[error("the provided metadata failed to verify: {0}")]
    #[rest_api_error(reason = "INVALID_METADATA", status = ApiStatusCode::InvalidArgument)]
    InvalidMetadata(#[from] VerificationError),

    /// Storing the metadata cannot be completed because the blob has been marked
    /// as invalid by the system.
    #[error("the blob for this metadata is invalid: {0:?}")]
    #[rest_api_error(reason = "INVALID_BLOB", status = ApiStatusCode::FailedPrecondition)]
    InvalidBlob(EventID),

    #[error("unsupported encoding type {0}, supported types are: {SUPPORTED_ENCODING_TYPES:?}")]
    #[rest_api_error(reason = "UNSUPPORTED_ENCODING_TYPE", status = ApiStatusCode::InvalidArgument)]
    UnsupportedEncodingType(EncodingType),

    #[error(transparent)]
    #[rest_api_error(delegate)]
    Internal(#[from] InternalError),
}

#[derive(Debug, thiserror::Error, RestApiError)]
#[rest_api_error(domain = ERROR_DOMAIN)]
pub enum RetrieveSymbolError {
    /// The index of the requested recovery symbol is out of range for the system.
    #[error("the requested recovery symbol is invalid for the system: {0}")]
    #[rest_api_error(delegate)]
    RecoverySymbolOutOfRange(#[from] IndexOutOfRange),

    #[error("the requested recovery symbol is not the responsibility of this node's shards")]
    #[rest_api_error(
        reason = "SYMBOL_NOT_PRESENT_AT_SHARDS", status = ApiStatusCode::FailedPrecondition,
    )]
    SymbolNotPresentAtShards,

    /// The sliver from which the recovery symbol is extracted could not be retrieved.
    #[error("the sliver from which to extract the recovery symbol could not be retrieved: {0}")]
    #[rest_api_error(delegate)]
    RetrieveSliver(#[from] RetrieveSliverError),

    #[error(transparent)]
    #[rest_api_error(delegate)]
    Unavailable(#[from] Unavailable),

    #[error(transparent)]
    #[rest_api_error(delegate)]
    Internal(#[from] InternalError),
}

#[derive(Debug, thiserror::Error, RestApiError)]
#[rest_api_error(domain = ERROR_DOMAIN)]
pub enum ListSymbolsError {
    #[error("at least one symbol ID must be specified")]
    #[rest_api_error(
        reason = "NO_SYMBOLS_SPECIFIED", status = ApiStatusCode::FailedPrecondition,
    )]
    NoSymbolsSpecified,

    #[error(transparent)]
    #[rest_api_error(delegate)]
    Last(#[from] RetrieveSymbolError),

    #[error(transparent)]
    #[rest_api_error(delegate)]
    Internal(#[from] InternalError),
}

#[derive(Debug, thiserror::Error, RestApiError)]
#[rest_api_error(domain = ERROR_DOMAIN)]
pub enum StoreSliverError {
    /// The index of the identified sliver is out of range for the system.
    #[error("the requested sliver index is out of range: {0}")]
    #[rest_api_error(delegate)]
    SliverOutOfRange(#[from] IndexOutOfRange),

    /// The blob has not been registered or has already expired.
    #[error("the blob has not been registered or has already expired")]
    #[rest_api_error(reason = "NOT_REGISTERED", status = ApiStatusCode::FailedPrecondition)]
    NotCurrentlyRegistered,

    /// The metadata for the blob is required but missing.
    #[error("blob metadata is required but missing")]
    #[rest_api_error(reason = "METADATA_NOT_FOUND", status = ApiStatusCode::FailedPrecondition)]
    MissingMetadata,

    /// The provided sliver failed verification against the previously uploaded metadata
    /// for that blob ID.
    #[error("the provided sliver is invalid: {0}")]
    #[rest_api_error(reason = "INVALID_SLIVER", status = ApiStatusCode::InvalidArgument)]
    InvalidSliver(#[from] SliverVerificationError),

    #[error(transparent)]
    #[rest_api_error(delegate)]
    ShardNotAssigned(#[from] ShardNotAssigned),

    #[error("unsupported encoding type {0}, supported types are: {SUPPORTED_ENCODING_TYPES:?}")]
    #[rest_api_error(reason = "UNSUPPORTED_ENCODING_TYPE", status = ApiStatusCode::InvalidArgument)]
    UnsupportedEncodingType(EncodingType),

    #[error(transparent)]
    #[rest_api_error(delegate)]
    Internal(#[from] InternalError),
}

#[derive(Debug, thiserror::Error, RestApiError)]
#[rest_api_error(domain = ERROR_DOMAIN)]
pub enum InconsistencyProofError {
    /// The metadata for the blob is required but missing.
    #[error("blob metadata is required but missing")]
    #[rest_api_error(reason = "METADATA_NOT_FOUND", status = ApiStatusCode::FailedPrecondition)]
    MissingMetadata,

    /// The provided inconsistency proof is not valid.
    #[error(transparent)]
    #[rest_api_error(reason = "INVALID_PROOF", status = ApiStatusCode::InvalidArgument)]
    InvalidProof(#[from] InconsistencyVerificationError),

    #[error(transparent)]
    #[rest_api_error(delegate)]
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

#[derive(Debug, thiserror::Error, RestApiError)]
#[rest_api_error(domain = ERROR_DOMAIN)]
pub enum BlobStatusError {
    #[error(transparent)]
    #[rest_api_error(delegate)]
    Internal(#[from] InternalError),
}

/// Error returned when the epoch in a request is invalid.
#[derive(Debug, Clone, thiserror::Error, Serialize, RestApiError)]
#[error("the request's epoch ({request_epoch}) is invalid, server epoch {server_epoch}")]
#[rest_api_error(
    reason = "INVALID_EPOCH", status = ApiStatusCode::InvalidArgument, domain = ERROR_DOMAIN,
    details(serialize)
)]
pub struct InvalidEpochError {
    pub request_epoch: Epoch,
    pub server_epoch: Epoch,
}

#[derive(Debug, thiserror::Error, RestApiError)]
#[rest_api_error(domain = ERROR_DOMAIN)]
pub enum SyncShardServiceError {
    /// The client cannot initiate a sync as it is not a storage node in the current committee.
    #[error(
        "the client is not authorized to perform the sync shard operation as it is not a \
        storage node in the current committee"
    )]
    #[rest_api_error(reason = "REQUEST_UNAUTHORIZED", status = ApiStatusCode::PermissionDenied)]
    Unauthorized,

    /// The client cannot initiate a sync as it is not a storage node in the current committee.
    #[error("verification of the request to start the sync failed: {0}")]
    #[rest_api_error(
        reason = "MESSAGE_VERIFICATION_FAILED", status = ApiStatusCode::InvalidArgument
    )]
    MessageVerificationError(#[from] MessageVerificationError),

    #[error(transparent)]
    #[rest_api_error(delegate)]
    ShardNotAssigned(#[from] ShardNotAssigned),

    /// The client is attempting to sync the state for an epoch that differs from the servers.
    #[error("the request's epoch must be the same as the server's epoch: {0}")]
    #[rest_api_error(delegate)]
    InvalidEpoch(#[from] InvalidEpochError),

    #[error(transparent)]
    #[rest_api_error(delegate)]
    Internal(#[from] InternalError),
}

impl From<TypedStoreError> for SyncShardServiceError {
    fn from(value: TypedStoreError) -> Self {
        Self::Internal(anyhow!(value))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SyncShardClientError {
    #[error("The destination node does not have a valid client to talk to the source node")]
    NoSyncClient,
    #[error("Unable to find the owner for shard {0}")]
    NoOwnerForShard(ShardIndex),
    #[error(transparent)]
    ShardNotAssigned(#[from] ShardNotAssigned),
    #[error(transparent)]
    StorageError(#[from] TypedStoreError),
    #[error(transparent)]
    Internal(#[from] InternalError),
    #[error(transparent)]
    RequestError(#[from] NodeError),
}

/// Errors returned by the storage node config synchronizer.
#[derive(Debug, Error)]
pub enum SyncNodeConfigError {
    /// The protocol key pair rotation is required.
    #[error("Node protocol key pair rotation is required")]
    ProtocolKeyPairRotationRequired,
    /// The node configuration has changed.
    #[error("Node needs reboot")]
    NodeNeedsReboot,
    /// A SuiClientError occurred.
    #[error(transparent)]
    SuiClientError(#[from] SuiClientError),
    /// The node configuration is inconsistent with the on-chain configuration.
    #[error("Node config is inconsistent: {0}")]
    NodeConfigInconsistent(String),
    /// An unexpected error occurred.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum CheckpointError {
    #[error("Failed to create backup: {0}")]
    BackupCreationFailed(String),
    #[error("Failed to restore from backup: {0}")]
    BackupRestoreFailed(String),
    #[error("Failed to delete old backups: {0}")]
    BackupDeletionFailed(String),
    #[error("Failed to get existing backups: {0}")]
    BackupListingFailed(String),
    #[error("Backup already in progress")]
    BackupInProgress,
    #[error("Other checkpoint error: {0}")]
    Other(#[from] anyhow::Error),
}
