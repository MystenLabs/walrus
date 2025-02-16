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
use axum_extra::{
    headers::{authorization::Bearer, Authorization},
    TypedHeader,
};
use jsonwebtoken::{DecodingKey, Validation};
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
    types::move_structs::BlobWithAttribute,
    SuiAddressSchema,
};

use super::{WalrusReadClient, WalrusWriteClient};
use crate::{
    client::{
        daemon::{
            auth::{Claim, PublisherAuthError},
            PostStoreAction,
        },
        BlobStoreResult,
        ClientError,
        ClientErrorKind,
        StoreWhen,
    },
    common::api::{Binary, BlobIdString, RestApiError},
};

/// The status endpoint, which always returns a 200 status when it is available.
pub const STATUS_ENDPOINT: &str = "/status";
/// OpenAPI documentation endpoint.
pub const API_DOCS: &str = "/v1/api";
/// The path to get the blob with the given blob ID.
pub const BLOB_GET_ENDPOINT: &str = "/v1/blobs/{blob_id}";
/// The path to get the metadata with the given blob ID.
pub const BLOB_WITH_ATTRIBUTE_GET_ENDPOINT: &str = "/v1/blobs/{blob_object_id}/with-attribute";
/// The path to store a blob.
pub const BLOB_PUT_ENDPOINT: &str = "/v1/blobs";

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

/// Retrieve a Walrus blob with its associated attribute.
///
/// Reconstructs the blob identified by the provided blob object ID from Sui and then reconstructs
/// the blob from Walrus by the blob_id in the object and returns its binary data along with
/// attribute information in the response headers.
#[tracing::instrument(level = Level::ERROR, skip_all, fields(%blob_object_id))]
#[utoipa::path(
    get,
    path = BLOB_WITH_ATTRIBUTE_GET_ENDPOINT,
    params(("blob_object_id" = BlobId,)),
    responses(
        (
            status = 200,
            description = "The blob was reconstructed successfully with attribute headers",
            body = [u8],
            headers(
                ("content-disposition" = String,
                    description = "Content disposition header if provided in attribute"),
                ("content-encoding" = String,
                    description = "Content encoding header if provided in attribute"),
                ("content-language" = String,
                    description = "Content language header if provided in attribute"),
                ("content-location" = String,
                    description = "Content location header if provided in attribute"),
                ("content-type" = String,
                    description = "Content type header if provided in attribute"),
                ("link" = String,
                    description = "Link header if provided in attribute")
            )
        ),
        GetBlobError,
    ),
)]
pub(super) async fn get_blob_with_attribute<T: WalrusReadClient>(
    request_headers: HeaderMap,
    State(client): State<Arc<T>>,
    Path(blob_object_id): Path<ObjectID>,
) -> Response {
    tracing::debug!("starting to read blob with attribute");
    match client.get_blob_with_attribute(&blob_object_id).await {
        Ok(BlobWithAttribute { blob, attribute }) => {
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
                if let Some(attribute) = attribute {
                    let allowed_headers = [
                        (String::from("content-disposition"), CONTENT_DISPOSITION),
                        (String::from("content-encoding"), CONTENT_ENCODING),
                        (String::from("content-language"), CONTENT_LANGUAGE),
                        (String::from("content-location"), CONTENT_LOCATION),
                        (String::from("content-type"), CONTENT_TYPE),
                        (String::from("link"), LINK),
                    ];

                    for (key, header) in &allowed_headers {
                        if let Some(value) = &attribute.get(key) {
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
    bearer_header: Option<TypedHeader<Authorization<Bearer>>>,
    blob: Bytes,
) -> Response {
    // Check if there is an authorization claim, and use it to check the size.
    if let Some(TypedHeader(header)) = bearer_header {
        if let Err(error) = check_blob_size(header, blob.len()) {
            return error.into_response();
        }
    }

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

/// Checks if the JWT claim has a maximum size and if the blob exceeds it.
///
/// IMPORTANT: This function does _not_ check the validity of the claim (i.e., does not
/// authenticate the signature). The assumption is that a previous middleware has already done
/// so.
///
/// The function just decodes the token and checks that the size in the claim is not exceeded.
fn check_blob_size(
    bearer_header: Authorization<Bearer>,
    blob_size: usize,
) -> Result<(), PublisherAuthError> {
    // Note: We disable validation and use a default key because, if the authorization
    // header is present, it must have been checked by a previous middleware.
    let mut validation = Validation::default();
    validation.insecure_disable_signature_validation();
    let default_key = DecodingKey::from_secret(&[]);

    match Claim::from_token(bearer_header.token().trim(), &default_key, &validation) {
        Ok(claim) if claim.max_size.is_some() => {
            if blob_size as u64 > claim.max_size.expect("just checked") {
                Err(PublisherAuthError::MaxSizeExceeded)
            } else {
                Ok(())
            }
        }
        // We return an internal error here, because the claim should have been checked by a
        // previous middleware, and therefore we should be able to decode it.
        Err(error) => Err(PublisherAuthError::Internal(error.into())),
        Ok(_) => Ok(()), // No need to check the size.
    }
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
pub(crate) struct PublisherQuery {
    /// The number of epochs, ahead of the current one, for which to store the blob.
    ///
    /// The default is 1 epoch.
    #[serde(default = "default_epochs")]
    pub epochs: EpochCount,
    /// If true, the publisher creates a deletable blob instead of a permanent one.
    #[serde(default)]
    pub deletable: bool,
    #[serde(default)]
    /// If specified, the publisher will send the Blob object resulting from the store operation to
    /// this Sui address.
    #[param(value_type = Option<SuiAddressSchema>)]
    pub send_object_to: Option<SuiAddress>,
}

pub(super) fn default_epochs() -> EpochCount {
    1
}
