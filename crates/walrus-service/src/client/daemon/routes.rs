// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use anyhow::anyhow;
use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use reqwest::header::{
    ACCESS_CONTROL_ALLOW_HEADERS,
    ACCESS_CONTROL_ALLOW_METHODS,
    ACCESS_CONTROL_ALLOW_ORIGIN,
    ACCESS_CONTROL_MAX_AGE,
    CACHE_CONTROL,
    CONTENT_TYPE,
    ETAG,
    X_CONTENT_TYPE_OPTIONS,
};
use serde::Deserialize;
use sui_types::base_types::SuiAddress;
use tracing::Level;
use utoipa::IntoParams;
use walrus_core::EpochCount;
use walrus_proc_macros::RestApiError;
use walrus_sdk::api::errors::DAEMON_ERROR_DOMAIN as ERROR_DOMAIN;
use walrus_sui::client::BlobPersistence;

use super::{WalrusReadClient, WalrusWriteClient};
use crate::{
    client::{daemon::PostStoreAction, BlobStoreResult, ClientError, ClientErrorKind, StoreWhen},
    common::api::{self, BlobIdString, RestApiError},
};

/// The status endpoint, which always returns a 200 status when it is available.
pub const STATUS_ENDPOINT: &str = "/status";
/// OpenAPI documentation endpoint.
pub const API_DOCS: &str = "/v1/api";
/// The path to get the blob with the given blob ID.
pub const BLOB_GET_ENDPOINT: &str = "/v1/blobs/:blobId";
/// The path to store a blob.
pub const BLOB_PUT_ENDPOINT: &str = "/v1/blobs";

/// Retrieve a Walrus blob.
///
/// Reconstructs the blob identified by the provided blob ID from Walrus and return it binary data.
#[tracing::instrument(level = Level::ERROR, skip_all, fields(%blob_id))]
#[utoipa::path(
    get,
    path = api::rewrite_route(BLOB_GET_ENDPOINT),
    params(("blob_id" = BlobId,)),
    responses(
        (status = 200, description = "The blob was reconstructed successfully", body = [u8]),
        GetBlobError,
    ),
)]
pub(super) async fn get_blob<T: WalrusReadClient>(
    request_headers: HeaderMap,
    State(client): State<Arc<T>>,
    Path(BlobIdString(blob_id)): Path<BlobIdString>,
) -> Response {
    tracing::debug!("starting to read blob");
    match client.read_blob(&blob_id).await {
        Ok(blob) => {
            tracing::debug!("successfully retrieved blob");
            let mut response = (StatusCode::OK, blob).into_response();
            let headers = response.headers_mut();
            // Allow requests from any origin, s.t. content can be loaded in browsers.
            headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
            // Prevent the browser from trying to guess the MIME type to avoid dangerous inferences.
            headers.insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
            // Insert headers that help caches distribute Walrus blobs.
            //
            // Cache for 1 day, and allow refreshig on the client side. Refreshes use the ETag to
            // check if the content has changed. This allows invalidated blobs to be removed from
            // caches. `stale-while-revalidate` allows stale content to be served for 1 hour while
            // the browser tries to validate it (async revalidation).
            headers.insert(
                CACHE_CONTROL,
                HeaderValue::from_static("public, max-age=86400, stale-while-revalidate=3600"),
            );
            // The `ETag` is the blob ID itself.
            headers.insert(
                ETAG,
                HeaderValue::from_str(&blob_id.to_string())
                    .expect("the blob ID string only contains visible ASCII characters"),
            );
            // Mirror the content type.
            if let Some(content_type) = request_headers.get(CONTENT_TYPE) {
                tracing::debug!(?content_type, "mirroring the request's content type");
                headers.insert(CONTENT_TYPE, content_type.clone());
            }
            response
        }
        Err(error) => {
            let error = GetBlobError::from(error);

            match &error {
                GetBlobError::BlobNotFound => {
                    tracing::debug!(?blob_id, "the requested blob ID does not exist")
                }
                GetBlobError::Internal(error) => tracing::error!(?error, "error retrieving blob"),
                _ => (),
            }

            error.to_response()
        }
    }
}

#[derive(Debug, thiserror::Error, RestApiError)]
#[rest_api_error(domain = ERROR_DOMAIN)]
pub(crate) enum GetBlobError {
    /// The requested blob has not yet been stored on Walrus.
    #[error(
        "the requested blob ID does not exist on Walrus, ensure that it was entered correctly"
    )]
    #[rest_api_error(reason = "BLOB_NOT_FOUND", status = ApiStatusCode::NotFound)]
    BlobNotFound,

    /// The blob cannot be returned as has been blocked.
    #[error("the requested metadata is blocked")]
    #[rest_api_error(reason = "FORBIDDEN_BLOB", status = ApiStatusCode::UnavailableForLegalReasons)]
    Blocked,

    #[error(transparent)]
    #[rest_api_error(delegate)]
    Internal(#[from] anyhow::Error),
}

impl From<ClientError> for GetBlobError {
    fn from(error: ClientError) -> Self {
        match error.kind() {
            ClientErrorKind::BlobIdDoesNotExist => Self::BlobNotFound,
            ClientErrorKind::BlobIdBlocked(_) => Self::Blocked,
            _ => anyhow::anyhow!(error).into(),
        }
    }
}

/// Store a blob on Walrus.
///
/// Store a (potentially deletable) blob on Walrus for 1 or more epochs. The associated on-Sui
/// object can be sent to a specified Sui address.
#[tracing::instrument(level = Level::ERROR, skip_all, fields(%epochs))]
#[utoipa::path(
    put,
    path = api::rewrite_route(BLOB_PUT_ENDPOINT),
    request_body(content = [u8], description = "Unencoded blob"),
    params(PublisherQuery),
    responses(
        (status = 200, description = "The blob was stored successfully", body = BlobStoreResult),
        (status = 400, description = "The request is malformed"),
        (status = 413, description = "The blob is too large"),
        StoreBlobError,
    ),
)]
pub(super) async fn put_blob<T: WalrusWriteClient>(
    State(client): State<Arc<T>>,
    Query(PublisherQuery {
        epochs,
        deletable,
        send_object_to,
    }): Query<PublisherQuery>,
    blob: Bytes,
) -> Response {
    let post_store_action = if let Some(address) = send_object_to {
        PostStoreAction::TransferTo(address)
    } else {
        client.default_post_store_action()
    };
    tracing::debug!(?post_store_action, "starting to store received blob");

    let mut response = match client
        .write_blob(
            &blob[..],
            epochs,
            StoreWhen::NotStoredIgnoreResources,
            BlobPersistence::from_deletable(deletable),
            post_store_action,
        )
        .await
    {
        Ok(result) => {
            if let BlobStoreResult::MarkedInvalid { .. } = result {
                StoreBlobError::Internal(anyhow!(
                    "the blob was marked invalid, which is likely a system error, please report it"
                ))
                .into_response()
            } else {
                (StatusCode::OK, Json(result)).into_response()
            }
        }
        Err(error) => {
            tracing::error!(?error, "error storing blob");
            StoreBlobError::from(error).into_response()
        }
    };

    response
        .headers_mut()
        .insert(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
    response
}

#[derive(Debug, thiserror::Error, RestApiError)]
#[rest_api_error(domain = ERROR_DOMAIN)]
pub(crate) enum StoreBlobError {
    /// The service failed to store the blob to sufficient Walrus storage nodes before a timeout,
    /// please retry the operation.
    #[error("the service timed-out while waiting for confirmations, please try again")]
    #[rest_api_error(
        reason = "INSUFFICIENT_CONFIRMATIONS", status = ApiStatusCode::DeadlineExceeded
    )]
    NotEnoughConfirmations,

    /// The blob cannot be returned as has been blocked.
    #[error("the requested metadata is blocked")]
    #[rest_api_error(reason = "FORBIDDEN_BLOB", status = ApiStatusCode::UnavailableForLegalReasons)]
    Blocked,

    #[error(transparent)]
    #[rest_api_error(delegate)]
    Internal(#[from] anyhow::Error),
}

impl From<ClientError> for StoreBlobError {
    fn from(error: ClientError) -> Self {
        match error.kind() {
            ClientErrorKind::NotEnoughConfirmations(_, _) => Self::NotEnoughConfirmations,
            ClientErrorKind::BlobIdBlocked(_) => Self::Blocked,
            _ => Self::Internal(anyhow!(error)),
        }
    }
}

#[tracing::instrument(level = Level::ERROR, skip_all)]
pub(super) async fn store_blob_options() -> impl IntoResponse {
    [
        (ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
        (ACCESS_CONTROL_ALLOW_METHODS, "PUT, OPTIONS"),
        (ACCESS_CONTROL_MAX_AGE, "86400"),
        (ACCESS_CONTROL_ALLOW_HEADERS, "*"),
    ]
}

#[tracing::instrument(level = Level::ERROR, skip_all)]
#[utoipa::path(
    get,
    path = api::rewrite_route(STATUS_ENDPOINT),
    responses(
        (status = 200, description = "The service is running"),
    ),
)]
pub(super) async fn status() -> Response {
    "OK".into_response()
}

#[derive(Debug, Deserialize, IntoParams)]
pub(super) struct PublisherQuery {
    /// The number of epochs, ahead of the current one, for which to store the blob.
    ///
    /// The default is 1 epoch.
    #[serde(default = "default_epochs")]
    epochs: EpochCount,
    /// If true, the publisher creates a deletable blob instead of a permanent one.
    #[serde(default)]
    deletable: bool,
    #[serde(default)]
    /// If specified, the publisher will send the Blob object resulting from the store operation to
    /// this Sui address.
    send_object_to: Option<SuiAddress>,
}

pub(super) fn default_epochs() -> EpochCount {
    1
}
