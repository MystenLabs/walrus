// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashSet, str::FromStr, sync::Arc, time::Duration};

use anyhow::anyhow;
use axum::{
    Json,
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use axum_extra::{
    TypedHeader,
    headers::{Authorization, authorization::Bearer},
};
use jsonwebtoken::{DecodingKey, Validation};
use reqwest::header::{CACHE_CONTROL, CONTENT_TYPE, ETAG, X_CONTENT_TYPE_OPTIONS};
use serde::Deserialize;
use sui_types::base_types::{ObjectID, SuiAddress};
use tower_http::cors::{Any, CorsLayer};
use tracing::Level;
use utoipa::IntoParams;
use walrus_core::{BlobId, EncodingType, EpochCount};
use walrus_proc_macros::RestApiError;
use walrus_rest_client::api::errors::DAEMON_ERROR_DOMAIN as ERROR_DOMAIN;
use walrus_sdk::{
    client::responses::BlobStoreResult,
    error::{ClientError, ClientErrorKind},
    store_when::StoreWhen,
};
use walrus_sui::{
    ObjectIdSchema,
    SuiAddressSchema,
    client::BlobPersistence,
    types::move_structs::{BlobAttribute, BlobWithAttribute},
};

use super::{WalrusReadClient, WalrusWriteClient};
use crate::{
    client::daemon::{
        PostStoreAction,
        auth::{Claim, PublisherAuthError},
    },
    common::api::{Binary, BlobIdString, RestApiError},
};

/// The status endpoint, which always returns a 200 status when it is available.
pub const STATUS_ENDPOINT: &str = "/status";
/// OpenAPI documentation endpoint.
pub const API_DOCS: &str = "/v1/api";
/// The path to get the blob with the given blob ID.
pub const BLOB_GET_ENDPOINT: &str = "/v1/blobs/{blob_id}";
/// The path to get the blob and its attribute with the given object ID.
pub const BLOB_OBJECT_GET_ENDPOINT: &str = "/v1/blobs/by-object-id/{blob_object_id}";
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

fn populate_response_headers(
    headers: &mut HeaderMap,
    attribute: &BlobAttribute,
    allowed_headers: &HashSet<String>,
) {
    for (key, value) in attribute.iter() {
        if allowed_headers.contains(key) {
            if let (Ok(header_name), Ok(header_value)) =
                (HeaderName::from_str(key), HeaderValue::from_str(value))
            {
                headers.insert(header_name, header_value);
            }
        }
    }
}

/// Retrieve a Walrus blob with its associated attribute.
///
/// First retrieves the blob metadata from Sui using the provided object ID (either of the blob
/// object or a shared blob), then uses the blob_id from that metadata to fetch the actual blob
/// data via the get_blob function. The response includes the binary data along with any attribute
/// headers from the metadata that are present in the configured allowed_headers set.
#[tracing::instrument(level = Level::ERROR, skip_all, fields(%blob_object_id))]
#[utoipa::path(
    get,
    path = BLOB_OBJECT_GET_ENDPOINT,
    params(("blob_object_id" = ObjectIdSchema,)),
    responses(
        (
            status = 200,
            description = "The blob was reconstructed successfully. Any attribute headers present \
                        in the allowed_headers configuration will be included in the response.",
            body = [u8]
        ),
        GetBlobError,
    ),
)]
pub(super) async fn get_blob_by_object_id<T: WalrusReadClient>(
    State((client, allowed_headers)): State<(Arc<T>, Arc<HashSet<String>>)>,
    request_headers: HeaderMap,
    Path(blob_object_id): Path<ObjectID>,
) -> Response {
    tracing::debug!("starting to read blob with attribute");
    match client.get_blob_by_object_id(&blob_object_id).await {
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
                if let Some(attribute) = attribute {
                    populate_response_headers(response.headers_mut(), &attribute, &allowed_headers);
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
    #[error("the requested blob ID does not exist on Walrus, ensure that it was entered correctly")]
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
        encoding_type,
        epochs,
        deletable,
        send_object_to,
        share,
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
        if share {
            return StoreBlobError::BadRequest("cannot specify both `send_object_to` and `share`")
                .into_response();
        }
        PostStoreAction::TransferTo(address)
    } else if share {
        PostStoreAction::Share
    } else {
        client.default_post_store_action()
    };
    tracing::debug!(?post_store_action, "starting to store received blob");

    match client
        .write_blob(
            &blob[..],
            encoding_type,
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
    }
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
        Ok(claim) => {
            if let Some(max_size) = claim.max_size {
                if blob_size as u64 > max_size {
                    return Err(PublisherAuthError::InvalidSize);
                }
            }
            if let Some(size) = claim.size {
                if blob_size as u64 != size {
                    return Err(PublisherAuthError::InvalidSize);
                }
            }
            Ok(())
        }
        // We return an internal error here, because the claim should have been checked by a
        // previous middleware, and therefore we should be able to decode it.
        Err(error) => Err(PublisherAuthError::Internal(error.into())),
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

    #[error("invalid request: {0}")]
    #[rest_api_error(reason = "BAD_REQUEST", status = ApiStatusCode::FailedPrecondition)]
    BadRequest(&'static str),

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

/// Returns a `CorsLayer` for the blob store endpoint.
pub(super) fn daemon_cors_layer() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .max_age(Duration::from_secs(86400))
        .allow_headers(Any)
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

/// The query parameters for a publisher.
#[derive(Debug, Deserialize, IntoParams)]
pub struct PublisherQuery {
    /// The encoding type to use for the blob.
    #[serde(default)]
    pub encoding_type: Option<EncodingType>,
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
    /// If true, the publisher will share the blob. Cannot be true if `send_object_to` is specified.
    #[serde(default)]
    pub share: bool,
}

pub(super) fn default_epochs() -> EpochCount {
    1
}
