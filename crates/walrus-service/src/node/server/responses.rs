// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, sync::Arc};

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use utoipa::{
    openapi::{response::Response as OpenApiResponse, RefOr},
    IntoResponses,
};
use walrus_sdk::error::ServiceError;

use super::extract::BcsRejection;
use crate::{
    common::api::RestApiError,
    node::{
        errors::InvalidEpochError,
        BlobStatusError,
        ComputeStorageConfirmationError,
        InconsistencyProofError,
        RetrieveMetadataError,
        RetrieveSliverError,
        RetrieveSymbolError,
        StoreMetadataError,
        StoreSliverError,
        SyncShardServiceError,
    },
    rest_api_error,
};

rest_api_error! {
    RetrieveMetadataError: [
        (Unavailable, NOT_FOUND, None, Self::Unavailable.to_string()),
        (Internal(_), INTERNAL_SERVER_ERROR, None, @canonical),
    ]
}

rest_api_error! {
    StoreMetadataError: [
        (NotCurrentlyRegistered, CONFLICT, None, Self::NotCurrentlyRegistered.to_string()),
        (InvalidMetadata(_), BAD_REQUEST, None, "the provided metadata cannot be verified"),
        (InvalidBlob(_), CONFLICT, None, "the blob for the provided metadata is invalid"),
        (Internal(_), INTERNAL_SERVER_ERROR, None, @canonical),
    ]
}

rest_api_error! {
    RetrieveSliverError: [
        (ShardNotAssigned(_), MISDIRECTED_REQUEST, None,
        "the requested sliver is not stored at a shard assigned to this storage node"),
        (SliverOutOfRange(_), BAD_REQUEST, None, "the requested sliver index is out of range"),
        (Unavailable, NOT_FOUND, None, Self::Unavailable.to_string()),
        (Internal(_), INTERNAL_SERVER_ERROR, None, @canonical)
    ]
}

rest_api_error! {
    StoreSliverError: [
        (SliverOutOfRange(_), BAD_REQUEST, None, "the requested sliver index is out of range"),
        (NotCurrentlyRegistered, CONFLICT, None, Self::NotCurrentlyRegistered.to_string()),
        (MissingMetadata, CONFLICT, None, Self::MissingMetadata.to_string()),
        (InvalidSliver(_), BAD_REQUEST, None, "the provided sliver failed verification"),
        (ShardNotAssigned(_), MISDIRECTED_REQUEST, None,
        "the requested sliver is not stored at a shard assigned to this storage node"),
        (Internal(_), INTERNAL_SERVER_ERROR, None, @canonical)
    ]
}

rest_api_error! {
    RetrieveSymbolError: [
        (RecoverySymbolOutOfRange(_), BAD_REQUEST, None,
        "the requested recovery symbol is out of range"),
        (RetrieveSliver(RetrieveSliverError::Internal(_)), INTERNAL_SERVER_ERROR, None, @canonical),
        (RetrieveSliver(RetrieveSliverError::SliverOutOfRange(_)), BAD_REQUEST, None,
        "invalid index for the sliver providing the recovery symbol"),
        (RetrieveSliver(RetrieveSliverError::ShardNotAssigned(_)), MISDIRECTED_REQUEST, None,
        "the sliver providing the requested symbol is not stored at this node's shards"),
        (RetrieveSliver(RetrieveSliverError::Unavailable), NOT_FOUND, None,
        "the sliver providing the requested symbol was not found"),
        (Internal(_), INTERNAL_SERVER_ERROR, None, @canonical)
    ]
}

rest_api_error! {
    ComputeStorageConfirmationError: [
        (NotCurrentlyRegistered, CONFLICT, None, Self::NotCurrentlyRegistered.to_string()),
        (NotFullyStored, NOT_FOUND, None, Self::NotFullyStored.to_string()),
        (Internal(_), INTERNAL_SERVER_ERROR, None, @canonical)
    ]
}

rest_api_error! {
    InconsistencyProofError: [
        (MissingMetadata, NOT_FOUND, None, Self::MissingMetadata.to_string()),
        (InvalidProof(_), BAD_REQUEST, None, "the provided inconsistency proof was invalid"),
        (Internal(_), INTERNAL_SERVER_ERROR, None, @canonical)
    ]
}

rest_api_error! {
    BlobStatusError: [
        (Internal(_), INTERNAL_SERVER_ERROR, None, @canonical)
    ]
}

rest_api_error!(
    SyncShardServiceError: [
        (Unauthorized, UNAUTHORIZED, None, Self::Unauthorized.to_string()),
        (MessageVerificationError(_), BAD_REQUEST, None, "Request verification failed"),
        (ShardNotAssigned(_), MISDIRECTED_REQUEST, None,
        "the requested sliver is not stored at a shard assigned to this storage node"),
        (InvalidEpoch(InvalidEpochError{request_epoch, server_epoch}), BAD_REQUEST,
        Some(ServiceError::InvalidEpoch{
            request_epoch: *request_epoch,
            server_epoch: *server_epoch}),
        "The requested epoch is invalid"),
        (Internal(_), INTERNAL_SERVER_ERROR, None, @canonical),
        (StorageError(_), INTERNAL_SERVER_ERROR, None, "Storage error"),
    ]
);

/// Helper type for attaching [`BcsRejection`]s to errors.
#[derive(Debug, thiserror::Error)]
pub(crate) enum OrRejection<T> {
    #[error(transparent)]
    Err(T),
    #[error(transparent)]
    BcsRejection(#[from] BcsRejection),
}

impl<T: RestApiError> RestApiError for OrRejection<T> {
    fn status(&self) -> StatusCode {
        match self {
            OrRejection::Err(err) => err.status(),
            OrRejection::BcsRejection(err) => err.status(),
        }
    }

    fn body_text(&self) -> String {
        match self {
            OrRejection::Err(err) => err.body_text(),
            OrRejection::BcsRejection(err) => err.body_text(),
        }
    }

    fn service_error(&self) -> Option<ServiceError> {
        match self {
            OrRejection::Err(err) => err.service_error(),
            OrRejection::BcsRejection(err) => err.service_error(),
        }
    }

    fn to_response(&self) -> Response {
        match self {
            OrRejection::Err(err) => err.to_response(),
            OrRejection::BcsRejection(err) => err.to_response(),
        }
    }
}

impl<T: RestApiError> IntoResponse for OrRejection<T> {
    fn into_response(self) -> Response {
        self.to_response()
    }
}

impl<T: IntoResponses + RestApiError> IntoResponses for OrRejection<T> {
    fn responses() -> BTreeMap<String, RefOr<OpenApiResponse>> {
        T::responses()
    }
}

// We manually implement these from conversions to avoid multiple `From<BcsRejection>`
// implementations caused by the generic.

impl From<StoreSliverError> for OrRejection<StoreSliverError> {
    fn from(value: StoreSliverError) -> Self {
        Self::Err(value)
    }
}
impl From<InconsistencyProofError> for OrRejection<InconsistencyProofError> {
    fn from(value: InconsistencyProofError) -> Self {
        Self::Err(value)
    }
}

impl From<SyncShardServiceError> for OrRejection<SyncShardServiceError> {
    fn from(value: SyncShardServiceError) -> Self {
        Self::Err(value)
    }
}
