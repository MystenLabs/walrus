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

use super::extract::BcsRejection;
use crate::{
    api::{rest_api_error, RestApiError},
    node::{
        BlobStatusError,
        ComputeStorageConfirmationError,
        InconsistencyProofError,
        RetrieveMetadataError,
        RetrieveSliverError,
        RetrieveSymbolError,
        StoreMetadataError,
        StoreSliverError,
    },
};

rest_api_error! {
    RetrieveMetadataError: [
        (Unavailable, NOT_FOUND, Self::Unavailable.to_string()),
        (Internal(_), INTERNAL_SERVER_ERROR, @canonical)
    ]
}

rest_api_error! {
    StoreMetadataError: [
        (NotRegistered, CONFLICT, Self::NotRegistered.to_string()),
        (InvalidMetadata(_), BAD_REQUEST, "the provided metadata cannot be verified"),
        (InvalidBlob(_), CONFLICT, "the blob for the provided metadata is invalid"),
        (BlobExpired, GONE, Self::BlobExpired.to_string()),
        (Internal(_), INTERNAL_SERVER_ERROR, @canonical)
    ]
}

rest_api_error! {
    RetrieveSliverError: [
        (ShardNotAssigned(_), MISDIRECTED_REQUEST,
        "the requested sliver is not stored at a shard assigned to this storage node"),
        (SliverOutOfRange(_), BAD_REQUEST, "the requested sliver index is out of range"),
        (Unavailable, NOT_FOUND, Self::Unavailable.to_string()),
        (Internal(_), INTERNAL_SERVER_ERROR, @canonical)
    ]
}

rest_api_error! {
    StoreSliverError: [
        (SliverOutOfRange(_), BAD_REQUEST, "the requested sliver index is out of range"),
        (MissingMetadata, CONFLICT, Self::MissingMetadata.to_string()),
        (InvalidSliver(_), BAD_REQUEST, "the provided sliver failed verification"),
        (ShardNotAssigned(_), MISDIRECTED_REQUEST,
        "the requested sliver is not stored at a shard assigned to this storage node"),
        (Internal(_), INTERNAL_SERVER_ERROR, @canonical)
    ]
}

rest_api_error! {
    RetrieveSymbolError: [
        (RecoverySymbolOutOfRange(_), BAD_REQUEST, "the requested recovery symbol is out of range"),
        (RetrieveSliver(RetrieveSliverError::Internal(_)), INTERNAL_SERVER_ERROR, @canonical),
        (RetrieveSliver(RetrieveSliverError::SliverOutOfRange(_)), BAD_REQUEST,
        "invalid index for the sliver providing the recovery symbol"),
        (RetrieveSliver(RetrieveSliverError::ShardNotAssigned(_)), MISDIRECTED_REQUEST,
        "the sliver providing the requested symbol is not stored at this node's shards"),
        (RetrieveSliver(RetrieveSliverError::Unavailable), NOT_FOUND,
        "the sliver providing the requested symbol was not found"),
        (Internal(_), INTERNAL_SERVER_ERROR, @canonical)
    ]
}

rest_api_error! {
    ComputeStorageConfirmationError: [
        (NotFullyStored, NOT_FOUND, Self::NotFullyStored.to_string()),
        (Internal(_), INTERNAL_SERVER_ERROR, @canonical)
    ]
}

rest_api_error! {
    InconsistencyProofError: [
        (MissingMetadata, NOT_FOUND, Self::MissingMetadata.to_string()),
        (InvalidProof(_), BAD_REQUEST, "the provided inconsistency proof was invalid"),
        (Internal(_), INTERNAL_SERVER_ERROR, @canonical)
    ]
}

rest_api_error! {
    BlobStatusError: [
        (Internal(_), INTERNAL_SERVER_ERROR, @canonical)
    ]
}

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
