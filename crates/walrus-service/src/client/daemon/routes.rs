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
    CONTENT_DISPOSITION,
    CONTENT_ENCODING,
    CONTENT_LANGUAGE,
    CONTENT_LOCATION,
    CONTENT_TYPE,
    ETAG,
    LINK,
    X_CONTENT_TYPE_OPTIONS,
};
use serde::Deserialize;
use sui_types::base_types::{ObjectID, SuiAddress};
use tracing::Level;
use utoipa::IntoParams;
use walrus_core::{BlobId, EpochCount};
use walrus_proc_macros::RestApiError;
use walrus_sdk::api::errors::DAEMON_ERROR_DOMAIN as ERROR_DOMAIN;
use walrus_sui::{
    client::BlobPersistence,
    types::{
        move_errors::{BlobError, MoveExecutionError},
        move_structs::{BlobWithMetadata, Metadata},
    },
    SuiAddressSchema,
};

use super::{WalrusReadClient, WalrusWriteClient};
use crate::{
    client::{daemon::PostStoreAction, BlobStoreResult, ClientError, ClientErrorKind, StoreWhen},
    common::api::{Binary, BlobIdString, RestApiError},
};

/// The status endpoint, which always returns a 200 status when it is available.
pub const STATUS_ENDPOINT: &str = "/status";
/// OpenAPI documentation endpoint.
pub const API_DOCS: &str = "/v1/api";
/// The path to get the blob with the given blob ID.
pub const BLOB_GET_ENDPOINT: &str = "/v1/blobs/{blob_id}";
/// The path to get the metadata with the given blob ID.
pub const BLOB_WITH_METADATA_GET_ENDPOINT: &str = "/v1/blobs/{blob_object_id}/with-metadata";
/// The path to store a blob.
pub const BLOB_PUT_ENDPOINT: &str = "/v1/blobs";
/// The path to update the metadata for a blob.
pub const BLOB_METADATA_PUT_ENDPOINT: &str = "/v1/blobs/{blob_object_id}/metadata";

/// Retrieve a Walrus blob.
///
/// Reconstructs the blob identified by the provided blob ID from Walrus and return it binary data.
#[tracing::instrument(level = Level::ERROR, skip_all, fields(%blob_id))]
#[utoipa::path(
    get,
    path = BLOB_GET_ENDPOINT,
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

/// Retrieve a Walrus blob with its associated metadata.
///
/// Reconstructs the blob identified by the provided blob object ID from Sui and then reconstructs
/// the blob from Walrus by the blob_id in the object and returns its binary data along with
/// metadata information in the response headers.
#[tracing::instrument(level = Level::ERROR, skip_all, fields(%blob_object_id))]
#[utoipa::path(
    get,
    path = BLOB_WITH_METADATA_GET_ENDPOINT,
    params(("blob_object_id" = BlobId,)),
    responses(
        (
            status = 200,
            description = "The blob was reconstructed successfully with metadata headers",
            body = [u8],
            headers(
                ("content-disposition" = String,
                    description = "Content disposition header if provided in metadata"),
                ("content-encoding" = String,
                    description = "Content encoding header if provided in metadata"),
                ("content-language" = String,
                    description = "Content language header if provided in metadata"),
                ("content-location" = String,
                    description = "Content location header if provided in metadata"),
                ("content-type" = String,
                    description = "Content type header if provided in metadata"),
                ("link" = String,
                    description = "Link header if provided in metadata")
            )
        ),
        GetBlobError,
    ),
)]
pub(super) async fn get_blob_with_metadata<T: WalrusReadClient>(
    request_headers: HeaderMap,
    State(client): State<Arc<T>>,
    Path(blob_object_id): Path<ObjectID>,
) -> Response {
    tracing::debug!("starting to read blob with metadata");
    match client.get_blob_with_metadata(&blob_object_id).await {
        Ok(BlobWithMetadata { blob, metadata }) => {
            // Get the blob data using the existing get_blob function
            let mut response = get_blob(
                request_headers.clone(),
                State(client),
                Path(BlobIdString(blob.blob_id)),
            )
            .await;

            // If the response was successful, add our additional metadata headers
            if response.status() == StatusCode::OK {
                let headers = response.headers_mut();

                // Add metadata headers if available
                if let Some(metadata) = metadata {
                    let standard_headers = [
                        (String::from("content-disposition"), CONTENT_DISPOSITION),
                        (String::from("content-encoding"), CONTENT_ENCODING),
                        (String::from("content-language"), CONTENT_LANGUAGE),
                        (String::from("content-location"), CONTENT_LOCATION),
                        (String::from("content-type"), CONTENT_TYPE),
                        (String::from("link"), LINK),
                    ];

                    for (key, header) in &standard_headers {
                        if let Some(value) = &metadata.get(key) {
                            if let Ok(header_value) = HeaderValue::from_str(value) {
                                headers.insert(header, header_value);
                            }
                        }
                    }
                }
            }

            response
        }
        Err(error) => {
            let error = GetBlobError::from(error);

            match &error {
                GetBlobError::BlobNotFound => {
                    tracing::debug!(
                        ?blob_object_id,
                        "the requested blob object ID does not exist"
                    )
                }
                GetBlobError::Internal(error) => {
                    tracing::error!(?error, "error retrieving blob metadata")
                }
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
    path = BLOB_PUT_ENDPOINT,
    request_body(
        content = Binary,
        content_type = "application/octet-stream",
        description = "Binary data of the unencoded blob to be stored."),
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
    path = STATUS_ENDPOINT,
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
    #[param(value_type = Option<SuiAddressSchema>)]
    send_object_to: Option<SuiAddress>,
}

pub(super) fn default_epochs() -> EpochCount {
    1
}

/// Update metadata for a Walrus blob.
///
/// Updates the metadata associated with a blob object identified by the provided object ID.
/// If metadata already exists, it will be updated.
#[tracing::instrument(level = Level::ERROR, skip_all, fields(%blob_object_id))]
#[utoipa::path(
    put,
    path = BLOB_METADATA_PUT_ENDPOINT,
    request_body(
        content = HashMap<String, String>,
        description = "Metadata key-value pairs to be stored with the blob."),
    params(("blob_object_id" = ObjectID,)),
    responses(
        (status = 200, description = "The metadata was added/updated successfully"),
        (status = 400, description = "The request is malformed"),
        UpdateBlobMetadataError,
    ),
)]
pub(super) async fn put_blob_metadata<T: WalrusWriteClient>(
    State(client): State<Arc<T>>,
    Path(blob_object_id): Path<ObjectID>,
    Json(metadata_map): Json<std::collections::HashMap<String, String>>,
) -> Response {
    tracing::debug!(?blob_object_id, "starting to update blob metadata");

    let mut metadata = Metadata::new();
    for (key, value) in metadata_map {
        metadata.insert(key, value);
    }

    let mut response = match client
        .add_blob_metadata(&blob_object_id, metadata.clone())
        .await
    {
        Ok(_) => {
            tracing::info!(?blob_object_id, "successfully added metadata");
            StatusCode::OK.into_response()
        }
        Err(error) => {
            // Handle duplicate metadata case
            if let ClientError {
                kind:
                    ClientErrorKind::SuiClient(SuiClientError::TransactionExecutionError(
                        MoveExecutionError::Blob(BlobError::EDuplicateMetadata(_)),
                    )),
                ..
            } = &error
            {
                // Try to update existing metadata
                match client
                    .update_blob_metadata_pairs(&blob_object_id, metadata)
                    .await
                {
                    Ok(_) => {
                        tracing::info!(?blob_object_id, "successfully updated existing metadata");
                        StatusCode::OK.into_response()
                    }
                    Err(update_error) => {
                        tracing::error!(?update_error, "error updating existing metadata");
                        UpdateBlobMetadataError::from(update_error).into_response()
                    }
                }
            } else {
                let error = UpdateBlobMetadataError::from(error);
                match &error {
                    UpdateBlobMetadataError::BlobNotFound => {
                        tracing::debug!(?blob_object_id, "blob object ID not found")
                    }
                    UpdateBlobMetadataError::Internal(error) => {
                        tracing::error!(?error, "error adding blob metadata")
                    }
                    _ => (),
                }
                error.into_response()
            }
        }
    };

    response
        .headers_mut()
        .insert(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
    response
}

#[derive(Debug, thiserror::Error, RestApiError)]
#[rest_api_error(domain = ERROR_DOMAIN)]
pub(crate) enum UpdateBlobMetadataError {
    /// The requested blob object does not exist.
    #[error("the requested blob object ID does not exist")]
    #[rest_api_error(reason = "BLOB_NOT_FOUND", status = ApiStatusCode::NotFound)]
    BlobNotFound,

    /// The blob cannot be updated as it has been blocked.
    #[error("the requested blob is blocked")]
    #[rest_api_error(reason = "FORBIDDEN_BLOB", status = ApiStatusCode::UnavailableForLegalReasons)]
    Blocked,

    /// The metadata operation failed.
    #[error("failed to update metadata: {0}")]
    #[rest_api_error(reason = "METADATA_ERROR", status = ApiStatusCode::BadRequest)]
    MetadataError(String),

    #[error(transparent)]
    #[rest_api_error(delegate)]
    Internal(#[from] anyhow::Error),
}

impl From<ClientError> for UpdateBlobMetadataError {
    fn from(error: ClientError) -> Self {
        match error.kind() {
            ClientErrorKind::BlobIdDoesNotExist => Self::BlobNotFound,
            ClientErrorKind::BlobIdBlocked(_) => Self::Blocked,
            ClientErrorKind::SuiClient(SuiClientError::TransactionExecutionError(
                MoveExecutionError::Blob(blob_error),
            )) => Self::MetadataError(blob_error.to_string()),
            _ => anyhow::anyhow!(error).into(),
        }
    }
}
