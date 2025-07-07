// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashSet, str::FromStr, sync::Arc};

use anyhow::anyhow;
use axum::{
    Json,
    body::{Body, Bytes},
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use axum_extra::{
    TypedHeader,
    headers::{Authorization, authorization::Bearer},
};
use futures::stream;
use jsonwebtoken::{DecodingKey, Validation};
use reqwest::header::{CACHE_CONTROL, CONTENT_TYPE, ETAG, X_CONTENT_TYPE_OPTIONS};
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use sui_types::base_types::{ObjectID, SuiAddress};
use tokio::time::Duration;
use tower_http::cors::{Any, CorsLayer};
use tracing::Level;
use utoipa::IntoParams;
use walrus_core::{
    BlobId,
    EncodingType,
    EpochCount,
    QuiltPatchId,
    encoding::{QuiltError, quilt_encoding::QuiltStoreBlob},
};
use walrus_proc_macros::RestApiError;
use walrus_sdk::{
    client::responses::BlobStoreResult,
    error::{ClientError, ClientErrorKind},
    store_optimizations::StoreOptimizations,
};
use walrus_storage_node_client::api::errors::DAEMON_ERROR_DOMAIN as ERROR_DOMAIN;
use walrus_sui::{
    ObjectIdSchema,
    SuiAddressSchema,
    client::{BlobPersistence, InvalidBlobPersistenceError},
    types::move_structs::{BlobAttribute, BlobWithAttribute},
};

use super::{AggregatorResponseHeaderConfig, WalrusReadClient, WalrusWriteClient};
use crate::{
    client::daemon::{
        PostStoreAction,
        auth::{Claim, PublisherAuthError},
    },
    common::api::{Binary, BlobIdString, QuiltPatchIdString, RestApiError},
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
/// The path to get blobs from quilt by IDs.
pub const QUILT_PATCH_BY_ID_GET_ENDPOINT: &str = "/v1/blobs/by-quilt-patch-id/{quilt_patch_id}";
/// The path to get blob from quilt by quilt ID and identifier.
pub const QUILT_PATCH_BY_IDENTIFIER_GET_ENDPOINT: &str =
    "/v1/blobs/by-quilt-id/{quilt_id}/{identifier}";
/// The path to stream video from quilt with push-based streaming.
pub const QUILT_STREAM_ENDPOINT: &str = "/v1/stream/{quilt_id}";
/// The path to stream video chunks from quilt.
pub const QUILT_STREAM_CHUNKED_ENDPOINT: &str = "/v1/stream/{quilt_id}/chunked";
/// The path to get HLS playlist for video streaming.
pub const QUILT_HLS_PLAYLIST_ENDPOINT: &str = "/v1/stream/{quilt_id}/playlist.m3u8";
/// The path to get individual video segments for HLS.
pub const QUILT_HLS_SEGMENT_ENDPOINT: &str = "/v1/stream/{quilt_id}/segments/{segment_index}";
/// Custom header for quilt patch identifier.
const X_QUILT_PATCH_IDENTIFIER: &str = "X-Quilt-Patch-Identifier";

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
            populate_response_headers_from_request(&request_headers, &blob_id.to_string(), headers);
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

fn populate_response_headers_from_request(
    request_headers: &HeaderMap,
    etag: &str,
    headers: &mut HeaderMap,
) {
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
        HeaderValue::from_str(etag)
            .expect("the blob ID string only contains visible ASCII characters"),
    );
    // Mirror the content type.
    if let Some(content_type) = request_headers.get(CONTENT_TYPE) {
        tracing::debug!(?content_type, "mirroring the request's content type");
        headers.insert(CONTENT_TYPE, content_type.clone());
    } // Cache for 1 day, and allow refreshig on the client side. Refreshes use the ETag to
}

fn populate_response_headers_from_attributes(
    headers: &mut HeaderMap,
    attribute: &BlobAttribute,
    allowed_headers: Option<&HashSet<String>>,
) {
    for (key, value) in attribute.iter() {
        if !key.is_empty() && allowed_headers.is_none_or(|headers| headers.contains(key)) {
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
    State((client, response_header_config)): State<(Arc<T>, Arc<AggregatorResponseHeaderConfig>)>,
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
                    populate_response_headers_from_attributes(
                        response.headers_mut(),
                        &attribute,
                        Some(&response_header_config.allowed_headers),
                    );
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

    /// The requested quilt patch does not exist on Walrus.
    #[error("the requested quilt patch does not exist on Walrus")]
    #[rest_api_error(reason = "QUILT_PATCH_NOT_FOUND", status = ApiStatusCode::NotFound)]
    QuiltPatchNotFound,

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
            ClientErrorKind::QuiltError(QuiltError::BlobsNotFoundInQuilt(_)) => {
                Self::QuiltPatchNotFound
            }
            _ => anyhow::anyhow!(error).into(),
        }
    }
}

/// Store a blob on Walrus.
///
/// Store a (potentially deletable) blob on Walrus for 1 or more epochs. The associated on-Sui
/// object can be sent to a specified Sui address.
#[tracing::instrument(level = Level::ERROR, skip_all, fields(epochs=%query.epochs))]
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
    Query(query): Query<PublisherQuery>,
    bearer_header: Option<TypedHeader<Authorization<Bearer>>>,
    blob: Bytes,
) -> Response {
    // Check if there is an authorization claim, and use it to check the size.
    if let Some(TypedHeader(header)) = bearer_header {
        if let Err(error) = check_blob_size(header, blob.len()) {
            return error.into_response();
        }
    }

    let blob_persistence = match query.blob_persistence() {
        Ok(blob_persistence) => blob_persistence,
        Err(error) => return error.into_response(),
    };

    tracing::debug!("starting to store received blob");
    match client
        .write_blob(
            &blob[..],
            query.encoding_type,
            query.epochs,
            query.optimizations(),
            blob_persistence,
            query.post_store_action(client.default_post_store_action()),
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

    /// The blob cannot be returned as it has been blocked.
    #[error("the requested metadata is blocked")]
    #[rest_api_error(reason = "FORBIDDEN_BLOB", status = ApiStatusCode::UnavailableForLegalReasons)]
    Blocked,

    /// The blob cannot be defined as both deletable and permanent.
    #[error(transparent)]
    #[rest_api_error(reason = "INVALID_BLOB_PERSISTENCE", status = ApiStatusCode::InvalidArgument)]
    InvalidBlobPersistence(#[from] InvalidBlobPersistenceError),

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

/// Retrieve a blob from quilt by its QuiltPatchId.
///
/// Takes a quilt patch ID and returns the corresponding blob from the quilt.
/// The blob content is returned as raw bytes in the response body, while metadata
/// such as the patch identifier and tags are returned in response headers.
///
/// # Example
/// ```bash
/// curl -X GET "http://localhost:31415/v1/blobs/by-quilt-patch-id/\
/// DJHLsgUoKQKEPcw3uehNQwuJjMu5a2sRdn8r-f7iWSAAC8Pw"
/// ```
///
/// Response:
/// ```text
/// HTTP/1.1 200 OK
/// Content-Type: application/octet-stream
/// X-Quilt-Patch-Identifier: my-file.txt
/// ETag: "DJHLsgUoKQKEPcw3uehNQwuJjMu5a2sRdn8r-f7iWSAAC8Pw"
///
/// [raw blob bytes]
/// ```
#[tracing::instrument(level = Level::ERROR, skip_all)]
#[utoipa::path(
    get,
    path = QUILT_PATCH_BY_ID_GET_ENDPOINT,
    params(
        (
            "quilt_patch_id" = String, Path,
            description = "The QuiltPatchId encoded as URL-safe base64",
            example = "DJHLsgUoKQKEPcw3uehNQwuJjMu5a2sRdn8r-f7iWSAAC8Pw"
        )
    ),
    responses(
        (
            status = 200,
            description = "The blob was retrieved successfully. Returns the raw blob bytes, \
                        the identifier and other attributes are returned as headers.",
            body = [u8]
        ),
        GetBlobError,
    ),
    summary = "Get blob from quilt",
    description = "Retrieve a specific blob from a quilt using its QuiltPatchId. Returns the \
                raw blob bytes, the identifier and other attributes are returned as headers.",
)]
pub(super) async fn get_blob_by_quilt_patch_id<T: WalrusReadClient>(
    request_headers: HeaderMap,
    State((client, response_header_config)): State<(Arc<T>, Arc<AggregatorResponseHeaderConfig>)>,
    Path(QuiltPatchIdString(quilt_patch_id)): Path<QuiltPatchIdString>,
) -> Response {
    let quilt_patch_id_str = quilt_patch_id.to_string();
    tracing::debug!("starting to read quilt patch: {}", quilt_patch_id_str);

    match client.get_blobs_by_quilt_patch_ids(&[quilt_patch_id]).await {
        Ok(mut blobs) => {
            if let Some(blob) = blobs.pop() {
                build_quilt_patch_response(
                    blob,
                    &request_headers,
                    &quilt_patch_id_str,
                    &response_header_config,
                )
            } else {
                tracing::debug!(
                    ?quilt_patch_id_str,
                    "no blob returned for the requested quilt patchID"
                );
                let error = GetBlobError::QuiltPatchNotFound;
                error.to_response()
            }
        }
        Err(error) => {
            let error = GetBlobError::from(error);

            match &error {
                GetBlobError::BlobNotFound => {
                    tracing::debug!(
                        ?quilt_patch_id_str,
                        "requested quilt patch ID does not exist"
                    )
                }
                GetBlobError::QuiltPatchNotFound => {
                    tracing::debug!(
                        ?quilt_patch_id_str,
                        "requested quilt patch ID does not exist"
                    )
                }
                GetBlobError::Internal(error) => {
                    tracing::error!(?error, ?quilt_patch_id_str, "error retrieving quilt patch")
                }
                _ => (),
            }

            error.to_response()
        }
    }
}

/// Builds a response for a quilt patch.
fn build_quilt_patch_response(
    blob: QuiltStoreBlob<'static>,
    request_headers: &HeaderMap,
    etag: &str,
    response_header_config: &AggregatorResponseHeaderConfig,
) -> Response {
    let identifier = blob.identifier().to_string();
    let blob_attribute: BlobAttribute = blob.tags().clone().into();
    let blob_data = blob.into_data();
    let mut response = (StatusCode::OK, blob_data).into_response();
    populate_response_headers_from_request(request_headers, etag, response.headers_mut());
    populate_response_headers_from_attributes(
        response.headers_mut(),
        &blob_attribute,
        if response_header_config.allow_quilt_patch_tags_in_response {
            None
        } else {
            Some(&response_header_config.allowed_headers)
        },
    );
    if let (Ok(header_name), Ok(header_value)) = (
        HeaderName::from_str(X_QUILT_PATCH_IDENTIFIER),
        HeaderValue::from_str(&identifier),
    ) {
        response.headers_mut().insert(header_name, header_value);
    }
    response
}

/// Retrieve a blob by quilt ID and identifier.
///
/// Takes a quilt ID and an identifier and returns the corresponding blob from the quilt.
/// The blob content is returned as raw bytes in the response body, while metadata
/// such as the blob identifier and tags are returned in response headers.
///
/// # Example
/// ```bash
/// curl -X GET "http://localhost:31415/v1/blobs/by-quilt-id/\
/// rkcHpHQrornOymttgvSq3zvcmQEsMqzmeUM1HSY4ShU/my-file.txt"
/// ```
///
/// Response:
/// ```text
/// HTTP/1.1 200 OK
/// Content-Type: application/octet-stream
/// X-Quilt-Patch-Identifier: my-file.txt
/// ETag: "rkcHpHQrornOymttgvSq3zvcmQEsMqzmeUM1HSY4ShU"
///
/// [raw blob bytes]
/// ```
#[tracing::instrument(level = Level::ERROR, skip_all)]
#[utoipa::path(
    get,
    path = QUILT_PATCH_BY_IDENTIFIER_GET_ENDPOINT,
    params(
        (
            "quilt_id" = String, Path,
            description = "The quilt ID encoded as URL-safe base64",
            example = "rkcHpHQrornOymttgvSq3zvcmQEsMqzmeUM1HSY4ShU"
        ),
        (
            "identifier" = String, Path,
            description = "The identifier of the blob within the quilt",
            example = "my-file.txt"
        )
    ),
    responses(
        (
            status = 200,
            description = "The blob was retrieved successfully. Returns the raw blob bytes, \
                        the identifier and other attributes are returned as headers.",
            body = [u8]
        ),
        GetBlobError,
    ),
    summary = "Get blob from quilt by ID and identifier",
    description = "Retrieve a specific blob from a quilt using the quilt ID and its identifier. \
                Returns the raw blob bytes, the identifier and other attributes are returned as \
                headers. If the quilt ID or identifier is not found, the response is 404.",
)]
pub(super) async fn get_blob_by_quilt_id_and_identifier<T: WalrusReadClient>(
    request_headers: HeaderMap,
    State((client, response_header_config)): State<(Arc<T>, Arc<AggregatorResponseHeaderConfig>)>,
    Path((quilt_id_str, identifier)): Path<(String, String)>,
) -> Response {
    tracing::debug!(
        "starting to read quilt blob by ID and identifier: {} / {}",
        quilt_id_str,
        identifier
    );

    let quilt_id = match BlobId::from_str(&quilt_id_str) {
        Ok(id) => id,
        Err(_) => {
            tracing::error!("invalid quilt ID format: {}", quilt_id_str);
            return GetBlobError::BlobNotFound.to_response();
        }
    };

    match client
        .get_blob_by_quilt_id_and_identifier(&quilt_id, &identifier)
        .await
    {
        Ok(blob) => build_quilt_patch_response(
            blob,
            &request_headers,
            &quilt_id_str,
            &response_header_config,
        ),
        Err(error) => {
            let error = GetBlobError::from(error);

            match &error {
                GetBlobError::BlobNotFound => {
                    tracing::info!(
                        "requested quilt blob with ID {} does not exist",
                        quilt_id_str,
                    )
                }
                GetBlobError::QuiltPatchNotFound => {
                    tracing::info!(
                        "requested quilt patch {} does not exist in quilt {}",
                        identifier,
                        quilt_id_str,
                    )
                }
                GetBlobError::Internal(error) => {
                    tracing::info!(?error, "error retrieving quilt blob by ID and identifier")
                }
                _ => (),
            }

            error.to_response()
        }
    }
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

/// Stream video from quilt with HTML player.
///
/// Returns an HTML page with an embedded video player that streams video from the quilt.
/// The video player will automatically start streaming when the page loads.
#[tracing::instrument(level = Level::ERROR, skip_all)]
#[utoipa::path(
    get,
    path = QUILT_STREAM_ENDPOINT,
    params(
        (
            "quilt_id" = BlobId, Path,
            description = "The quilt ID encoded as URL-safe base64",
            example = "rkcHpHQrornOymttgvSq3zvcmQEsMqzmeUM1HSY4ShU"
        )
    ),
    responses(
        (
            status = 200,
            description = "HTML page with embedded video player for streaming",
            body = String
        ),
        GetBlobError,
    ),
    summary = "Stream video from quilt",
    description = "Returns an HTML page with an embedded video player that streams video \
                from the specified quilt. The video will start playing automatically.",
)]
pub(super) async fn stream_quilt_video(
    Path(BlobIdString(quilt_id)): Path<BlobIdString>,
) -> Response {
    let quilt_id_str = quilt_id.to_string();
    let html = format!(
        r#"
        <!DOCTYPE html>
        <html>
        <head>
            <title>Walrus Video Stream - {}</title>
            <script src="https://cdn.jsdelivr.net/npm/hls.js@latest"></script>
            <style>
                body {{
                    margin: 0;
                    padding: 20px;
                    background: #000;
                    font-family: Arial, sans-serif;
                }}
                .container {{
                    max-width: 1920px;
                    margin: 0 auto;
                }}
                video {{
                    width: 100%;
                    height: auto;
                    border-radius: 8px;
                    background: #111;
                }}
                .info {{
                    color: white;
                    margin-top: 10px;
                    text-align: center;
                }}
                .controls {{
                    margin-top: 20px;
                    text-align: center;
                }}
                button {{
                    background: #4CAF50;
                    color: white;
                    border: none;
                    padding: 10px 20px;
                    margin: 0 5px;
                    border-radius: 4px;
                    cursor: pointer;
                }}
                button:hover {{
                    background: #45a049;
                }}
                .status {{
                    color: #ccc;
                    font-size: 14px;
                    margin-top: 10px;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <video id="video" controls autoplay muted>
                    Your browser does not support video streaming.
                </video>
                <div class="info">
                    <p>Streaming from Walrus Quilt: {}</p>
                </div>
                <div class="controls">
                    <button onclick="restartStream()">Restart Stream</button>
                    <button onclick="toggleMute()">Toggle Mute</button>
                    <button onclick="useChunkedStream()">Use Chunked Stream</button>
                    <button onclick="useCustomPlayer()">Use Custom Player</button>
                </div>
                <div class="status" id="status">
                    Initializing stream...
                </div>
            </div>
            <script>
                const video = document.getElementById('video');
                const status = document.getElementById('status');
                let hls = null;
                const quiltId = '{}';

                function updateStatus(message) {{
                    status.textContent = message;
                    console.log('Stream status:', message);
                }}

                function restartStream() {{
                    if (hls) {{
                        hls.destroy();
                    }}
                    initHLS();
                    updateStatus('Restarting stream...');
                }}

                function toggleMute() {{
                    video.muted = !video.muted;
                    updateStatus(video.muted ? 'Muted' : 'Unmuted');
                }}

                function useChunkedStream() {{
                    if (hls) {{
                        hls.destroy();
                        hls = null;
                    }}
                    
                    const chunkedUrl = '/v1/stream/' + quiltId + '/chunked';
                    video.src = chunkedUrl;
                    video.load();
                    updateStatus('Switched to chunked streaming...');
                }}

                function useCustomPlayer() {{
                    if (hls) {{
                        hls.destroy();
                        hls = null;
                    }}
                    
                    // Create a custom video source that loads segments sequentially
                    const customSource = '/v1/stream/' + quiltId + '/segments/0';
                    video.src = customSource;
                    video.load();
                    
                    // Set up custom segment loading
                    let currentSegment = 0;
                    const totalSegments = 7;
                    
                    video.addEventListener('ended', function() {{
                        currentSegment++;
                        if (currentSegment < totalSegments) {{
                            const nextSegment = '/v1/stream/' + quiltId + '/segments/' + currentSegment;
                            video.src = nextSegment;
                            video.load();
                            video.play().catch(e => console.log('Autoplay prevented:', e));
                            updateStatus('Playing segment ' + currentSegment + ' of ' + totalSegments);
                        }} else {{
                            updateStatus('All segments played');
                        }}
                    }});
                    
                    updateStatus('Custom player: Playing segment 0 of ' + totalSegments);
                }}

                function initHLS() {{
                    const playlistUrl = '/v1/stream/' + quiltId + '/playlist.m3u8';
                    
                    if (Hls.isSupported()) {{
                        hls = new Hls({{
                            debug: true,
                            enableWorker: true,
                            lowLatencyMode: false
                        }});
                        hls.loadSource(playlistUrl);
                        hls.attachMedia(video);
                        
                        hls.on(Hls.Events.MANIFEST_PARSED, function() {{
                            updateStatus('Playlist loaded, ready to play');
                            video.play().catch(e => {{
                                console.log('Autoplay prevented:', e);
                                updateStatus('Click play to start stream (autoplay blocked)');
                            }});
                        }});
                        
                        hls.on(Hls.Events.ERROR, function(event, data) {{
                            console.error('HLS error:', event, data);
                            updateStatus('Error: ' + data.details + ' - ' + data.fatal);
                            
                            if (data.fatal) {{
                                switch(data.type) {{
                                    case Hls.ErrorTypes.NETWORK_ERROR:
                                        updateStatus('Network error - trying to recover...');
                                        hls.startLoad();
                                        break;
                                    case Hls.ErrorTypes.MEDIA_ERROR:
                                        updateStatus('Media error - trying to recover...');
                                        hls.recoverMediaError();
                                        break;
                                    default:
                                        updateStatus('Fatal error - cannot recover');
                                        break;
                                }}
                            }}
                        }});
                        
                        hls.on(Hls.Events.FRAG_LOADING, function(event, data) {{
                            console.log('Loading fragment:', data.frag.url);
                        }});
                        
                        hls.on(Hls.Events.FRAG_LOADED, function(event, data) {{
                            console.log('Fragment loaded:', data.frag.url, 'size:', data.payload.byteLength);
                        }});
                        
                        hls.on(Hls.Events.FRAG_PARSING_ERROR, function(event, data) {{
                            console.error('Fragment parsing error:', data);
                            updateStatus('Fragment parsing error: ' + data.details);
                        }});
                    }} else if (video.canPlayType('application/vnd.apple.mpegurl')) {{
                        // Native HLS support (Safari)
                        video.src = playlistUrl;
                        video.addEventListener('loadedmetadata', function() {{
                            updateStatus('Playlist loaded, ready to play');
                            video.play().catch(e => {{
                                console.log('Autoplay prevented:', e);
                                updateStatus('Click play to start stream (autoplay blocked)');
                            }});
                        }});
                    }} else {{
                        updateStatus('HLS not supported in this browser');
                    }}
                }}

                video.addEventListener('loadstart', () => {{
                    updateStatus('Loading video stream...');
                }});

                video.addEventListener('loadedmetadata', () => {{
                    updateStatus('Stream metadata loaded');
                }});

                video.addEventListener('canplay', () => {{
                    updateStatus('Stream ready to play');
                }});

                video.addEventListener('playing', () => {{
                    updateStatus('Stream playing');
                }});

                video.addEventListener('waiting', () => {{
                    updateStatus('Buffering...');
                }});

                video.addEventListener('error', (e) => {{
                    console.error('Video error:', e);
                    updateStatus('Error: ' + (video.error ? video.error.message : 'Unknown error'));
                }});

                video.addEventListener('ended', () => {{
                    updateStatus('Stream ended');
                }});

                // Initialize HLS when page loads
                initHLS();
            </script>
        </body>
        </html>
    "#,
        quilt_id_str, quilt_id_str, quilt_id_str
    );

    let mut response = (StatusCode::OK, html).into_response();
    response.headers_mut().insert(
        "Content-Type",
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    response
}

/// Stream video chunks from quilt with push-based streaming.
///
/// Continuously streams video segments from the quilt using HTTP chunked transfer encoding.
/// This endpoint pushes video data to the client without requiring the client to specify
/// individual segments.
#[tracing::instrument(level = Level::ERROR, skip_all)]
#[utoipa::path(
    get,
    path = QUILT_STREAM_CHUNKED_ENDPOINT,
    params(
        (
            "quilt_id" = BlobId, Path,
            description = "The quilt ID encoded as URL-safe base64",
            example = "rkcHpHQrornOymttgvSq3zvcmQEsMqzmeUM1HSY4ShU"
        )
    ),
    responses(
        (
            status = 200,
            description = "Continuous video stream using chunked transfer encoding",
            body = [u8]
        ),
        GetBlobError,
    ),
    summary = "Stream video chunks from quilt",
    description = "Continuously streams video segments from the quilt using HTTP chunked \
                transfer encoding. The server pushes video data to the client.",
)]
pub(super) async fn stream_quilt_video_chunked<T: WalrusReadClient + Send + Sync + 'static>(
    State(state): State<(Arc<T>, Arc<AggregatorResponseHeaderConfig>)>,
    Path(BlobIdString(quilt_id)): Path<BlobIdString>,
) -> Response {
    let (client, _) = state;
    let quilt_id_str = quilt_id.to_string();
    let total_segments = get_available_segments_count();
    
    tracing::info!(
        "starting video stream for quilt {} with {} available segments",
        quilt_id_str,
        total_segments
    );

    // Create a stream of video segments
    let quilt_id_str_clone = quilt_id_str.clone();
    let video_stream = stream::unfold(
        (client, quilt_id, 0, false),
        move |(client, quilt_id, segment_index, stream_ended)| {
            let quilt_id_str = quilt_id_str_clone.clone();
            async move {
                // If we've already determined the stream has ended, stop
                if stream_ended {
                    return None;
                }

                let segment_id = format!("segment_{:05}.ts", segment_index);

                tracing::info!("getting quilt patch id for segment {}", segment_id);
                if let Some(quilt_patch_id) = get_quilt_patch_id(&quilt_id, &segment_id) {
                    tracing::info!(
                        "found quilt patch id {} for segment {}",
                        quilt_patch_id,
                        segment_id
                    );
                    match client
                        .get_blobs_by_quilt_patch_ids(&[quilt_patch_id])
                        .await
                    {
                        Ok(mut blobs) => {
                            if blobs.is_empty() {
                                tracing::info!(
                                    "no blobs returned for segment {} - stream complete",
                                    segment_id
                                );
                                return Some((
                                    Ok::<Bytes, std::io::Error>(Bytes::new()),
                                    (client, quilt_id, segment_index + 1, true),
                                ));
                            }

                            let blob = blobs.pop().expect("expected at least one blob");
                            let video_data = blob.into_data();
                            
                            tracing::info!(
                                "streaming segment {} ({} bytes) for quilt {}",
                                segment_id,
                                video_data.len(),
                                quilt_id_str
                            );

                            // Add a small delay between segments to allow buffering
                            tokio::time::sleep(Duration::from_millis(100)).await;

                            // Stream the complete MP4 segment
                            Some((
                                Ok::<Bytes, std::io::Error>(Bytes::from(video_data)),
                                (client, quilt_id, segment_index + 1, false),
                            ))
                        }
                        Err(error) => {
                            tracing::warn!(
                                "error retrieving segment {}: {:?} - stream complete",
                                segment_id,
                                error
                            );

                            // Return an empty chunk to signal end of stream gracefully
                            Some((
                                Ok::<Bytes, std::io::Error>(Bytes::new()),
                                (client, quilt_id, segment_index + 1, true),
                            ))
                        }
                    }
                } else {
                    tracing::info!(
                        "no mapping found for segment {} - stream complete after {} segments",
                        segment_id,
                        segment_index
                    );
                    return Some((
                        Ok::<Bytes, std::io::Error>(Bytes::new()),
                        (client, quilt_id, segment_index + 1, true),
                    ));
                }
            }
        },
    );

    // Convert the stream to a body that can be used in the response
    let stream_body = Body::from_stream(video_stream);

    let mut response = Response::new(stream_body);
    let headers = response.headers_mut();

    // Set appropriate headers for video streaming
    headers.insert("Content-Type", HeaderValue::from_static("video/mp4"));
    headers.insert("Transfer-Encoding", HeaderValue::from_static("chunked"));
    headers.insert("Cache-Control", HeaderValue::from_static("no-cache"));
    headers.insert("Accept-Ranges", HeaderValue::from_static("bytes"));
    headers.insert("Connection", HeaderValue::from_static("keep-alive"));

    // CORS headers for browser compatibility
    headers.insert("Access-Control-Allow-Origin", HeaderValue::from_static("*"));
    headers.insert(
        "Access-Control-Allow-Headers",
        HeaderValue::from_static("Range, Content-Type"),
    );

    response
}

fn get_quilt_patch_id(quilt_id: &BlobId, name: &str) -> Option<QuiltPatchId> {
    // Map of segment names to their corresponding QuiltPatchIds (first 30 only)
    let segment_map: std::collections::HashMap<&str, &str> = [
        ("segment_00000.ts", "QFGqa50FCknuqsRRpH_z2fx5MVpf5wCC_29aW3JLv0IBAQACAA"),
        ("segment_00001.ts", "QFGqa50FCknuqsRRpH_z2fx5MVpf5wCC_29aW3JLv0IBAgADAA"),
        ("segment_00002.ts", "QFGqa50FCknuqsRRpH_z2fx5MVpf5wCC_29aW3JLv0IBAwAEAA"),
        ("segment_00003.ts", "QFGqa50FCknuqsRRpH_z2fx5MVpf5wCC_29aW3JLv0IBBAAFAA"),
        ("segment_00004.ts", "QFGqa50FCknuqsRRpH_z2fx5MVpf5wCC_29aW3JLv0IBBQAGAA"),
        ("segment_00005.ts", "QFGqa50FCknuqsRRpH_z2fx5MVpf5wCC_29aW3JLv0IBBgAHAA"),
        ("segment_00006.ts", "QFGqa50FCknuqsRRpH_z2fx5MVpf5wCC_29aW3JLv0IBBwAIAA"),
        ("segment_00007.ts", "QFGqa50FCknuqsRRpH_z2fx5MVpf5wCC_29aW3JLv0IBCAAJAA"),
        ("segment_00008.ts", "QFGqa50FCknuqsRRpH_z2fx5MVpf5wCC_29aW3JLv0IBCQAKAA"),
        ("segment_00009.ts", "QFGqa50FCknuqsRRpH_z2fx5MVpf5wCC_29aW3JLv0IBCgALAA"),
        ("segment_00010.ts", "QFGqa50FCknuqsRRpH_z2fx5MVpf5wCC_29aW3JLv0IBCwAMAA"),
        ("segment_00011.ts", "QFGqa50FCknuqsRRpH_z2fx5MVpf5wCC_29aW3JLv0IBDAANAA"),
        ("segment_00012.ts", "QFGqa50FCknuqsRRpH_z2fx5MVpf5wCC_29aW3JLv0IBDQAOAA"),
        ("segment_00013.ts", "QFGqa50FCknuqsRRpH_z2fx5MVpf5wCC_29aW3JLv0IBDgAPAA"),
        ("segment_00014.ts", "QFGqa50FCknuqsRRpH_z2fx5MVpf5wCC_29aW3JLv0IBDwAQAA"),
        ("segment_00015.ts", "QFGqa50FCknuqsRRpH_z2fx5MVpf5wCC_29aW3JLv0IBEAARAA"),
        ("segment_00016.ts", "QFGqa50FCknuqsRRpH_z2fx5MVpf5wCC_29aW3JLv0IBEQASAA"),
        ("segment_00017.ts", "QFGqa50FCknuqsRRpH_z2fx5MVpf5wCC_29aW3JLv0IBEgATAA"),
        ("segment_00018.ts", "QFGqa50FCknuqsRRpH_z2fx5MVpf5wCC_29aW3JLv0IBEwAUAA"),
        ("segment_00019.ts", "QFGqa50FCknuqsRRpH_z2fx5MVpf5wCC_29aW3JLv0IBFAAVAA"),
        ("segment_00020.ts", "QFGqa50FCknuqsRRpH_z2fx5MVpf5wCC_29aW3JLv0IBFQAWAA"),
        ("segment_00021.ts", "QFGqa50FCknuqsRRpH_z2fx5MVpf5wCC_29aW3JLv0IBFgAXAA"),
        ("segment_00022.ts", "QFGqa50FCknuqsRRpH_z2fx5MVpf5wCC_29aW3JLv0IBFwAYAA"),
        ("segment_00023.ts", "QFGqa50FCknuqsRRpH_z2fx5MVpf5wCC_29aW3JLv0IBGAAZAA"),
        ("segment_00024.ts", "QFGqa50FCknuqsRRpH_z2fx5MVpf5wCC_29aW3JLv0IBGQAaAA"),
        ("segment_00025.ts", "QFGqa50FCknuqsRRpH_z2fx5MVpf5wCC_29aW3JLv0IBGgAbAA"),
        ("segment_00026.ts", "QFGqa50FCknuqsRRpH_z2fx5MVpf5wCC_29aW3JLv0IBGwAcAA"),
        ("segment_00027.ts", "QFGqa50FCknuqsRRpH_z2fx5MVpf5wCC_29aW3JLv0IBHAAdAA"),
        ("segment_00028.ts", "QFGqa50FCknuqsRRpH_z2fx5MVpf5wCC_29aW3JLv0IBHQAeAA"),
        ("segment_00029.ts", "QFGqa50FCknuqsRRpH_z2fx5MVpf5wCC_29aW3JLv0IBHgAfAA"),
    ].into_iter().collect();

    segment_map.get(name).and_then(|patch_id_str| {
        QuiltPatchId::from_str(patch_id_str).ok()
    })
}

fn get_available_segments_count() -> usize {
    // This should match the number of entries in the segment_map above
    30
}

/// The exclusive option to share the blob or to send it to an address.
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SendOrShare {
    /// Send the blob to the specified Sui address.
    #[schema(value_type = SuiAddressSchema)]
    SendObjectTo(SuiAddress),
    /// Turn the created blob into a shared blob.
    Share(#[serde_as(as = "DisplayFromStr")] bool),
}

/// The query parameters for a publisher.
#[derive(Debug, Deserialize, Serialize, IntoParams, PartialEq, Eq)]
#[into_params(parameter_in = Query, style = Form)]
#[serde(deny_unknown_fields)]
pub struct PublisherQuery {
    /// The encoding type to use for the blob.
    #[serde(default)]
    pub encoding_type: Option<EncodingType>,
    /// The number of epochs, ahead of the current one, for which to store the blob.
    ///
    /// The default is 1 epoch.
    #[serde(default = "default_epochs")]
    pub epochs: EpochCount,
    /// If true, the publisher creates a deletable blob instead of a permanent one. *This will
    /// become the default behavior in the future.*
    #[serde(default)]
    pub deletable: bool,
    /// If true, the publisher creates a permanent blob. This is currently the default behavior;
    /// but *in the future, blobs will be deletable by default*.
    #[serde(default)]
    pub permanent: bool,
    /// If true, the publisher will always store the blob, creating a new Blob object.
    ///
    /// The blob will be stored even if the blob is already certified on Walrus for the specified
    /// number of epochs.
    #[serde(default)]
    pub force: bool,

    #[serde(flatten, default)]
    #[param(inline)]
    send_or_share: Option<SendOrShare>,
}

pub(super) fn default_epochs() -> EpochCount {
    1
}

impl Default for PublisherQuery {
    fn default() -> Self {
        PublisherQuery {
            encoding_type: None,
            epochs: default_epochs(),
            deletable: false,
            permanent: false,
            force: false,
            send_or_share: None,
        }
    }
}

impl PublisherQuery {
    /// Returns the [`StoreOptimizations`] value based on the query parameters.
    ///
    /// The publisher always ignores existing resources.
    fn optimizations(&self) -> StoreOptimizations {
        StoreOptimizations::none().with_check_status(!self.force)
    }

    /// Returns the [`BlobPersistence`] value based on the query parameters.
    fn blob_persistence(&self) -> Result<BlobPersistence, StoreBlobError> {
        BlobPersistence::from_deletable_and_permanent(self.deletable, self.permanent)
            .map_err(StoreBlobError::from)
    }

    /// Returns the [`PostStoreAction`] value based on the query parameters.
    ///
    /// Assumes that the `validate` method has been called, i.e., that only one of `send_object_to`
    /// and `share` is set. Otherwise, the `send_object_to` value is used.
    fn post_store_action(&self, default_action: PostStoreAction) -> PostStoreAction {
        if let Some(send_or_share) = &self.send_or_share {
            match send_or_share {
                SendOrShare::SendObjectTo(address) => PostStoreAction::TransferTo(*address),
                SendOrShare::Share(share) => {
                    if *share {
                        PostStoreAction::Share
                    } else {
                        default_action
                    }
                }
            }
        } else {
            default_action
        }
    }

    /// Returns the value for the `send_or_share` field.
    pub fn send_or_share(&self) -> Option<SendOrShare> {
        self.send_or_share.clone()
    }
}

/// Generate HLS playlist for video streaming.
///
/// Returns an M3U8 playlist file that references individual video segments.
/// This approach works much better with browsers than concatenating MP4 segments.
#[tracing::instrument(level = Level::ERROR, skip_all)]
#[utoipa::path(
    get,
    path = QUILT_HLS_PLAYLIST_ENDPOINT,
    params(
        (
            "quilt_id" = BlobId, Path,
            description = "The quilt ID encoded as URL-safe base64",
            example = "rkcHpHQrornOymttgvSq3zvcmQEsMqzmeUM1HSY4ShU"
        )
    ),
    responses(
        (
            status = 200,
            description = "HLS playlist file (M3U8 format)",
            body = String
        ),
        GetBlobError,
    ),
    summary = "Get HLS playlist for video streaming",
    description = "Returns an M3U8 playlist file that references individual video segments \
                for proper HLS streaming in browsers.",
)]
pub(super) async fn get_hls_playlist(
    Path(BlobIdString(quilt_id)): Path<BlobIdString>,
) -> Response {
    let quilt_id_str = quilt_id.to_string();
    let total_segments = get_available_segments_count();
    
    tracing::info!(
        "generating HLS playlist for quilt {} with {} segments",
        quilt_id_str,
        total_segments
    );

    // Generate M3U8 playlist content
    let mut playlist = String::new();
    playlist.push_str("#EXTM3U\n");
    playlist.push_str("#EXT-X-VERSION:3\n");
    playlist.push_str("#EXT-X-TARGETDURATION:10\n"); // Assuming 10 seconds per segment
    playlist.push_str("#EXT-X-MEDIA-SEQUENCE:0\n");
    
    // Add segments to playlist
    for i in 0..total_segments {
        playlist.push_str(&format!("#EXTINF:10.0,\n")); // Duration in seconds
        playlist.push_str(&format!("/v1/stream/{}/segments/{}\n", quilt_id_str, i));
    }
    
    playlist.push_str("#EXT-X-ENDLIST\n");

    let mut response = (StatusCode::OK, playlist).into_response();
    response.headers_mut().insert(
        "Content-Type",
        HeaderValue::from_static("application/vnd.apple.mpegurl"),
    );
    response
}

/// Get individual video segment for HLS streaming.
///
/// Returns a single MP4 video segment by its index.
#[tracing::instrument(level = Level::ERROR, skip_all)]
#[utoipa::path(
    get,
    path = QUILT_HLS_SEGMENT_ENDPOINT,
    params(
        (
            "quilt_id" = BlobId, Path,
            description = "The quilt ID encoded as URL-safe base64",
            example = "rkcHpHQrornOymttgvSq3zvcmQEsMqzmeUM1HSY4ShU"
        ),
        (
            "segment_index" = u32, Path,
            description = "The segment index (0-based)",
            example = 0
        )
    ),
    responses(
        (
            status = 200,
            description = "MP4 video segment",
            body = [u8]
        ),
        GetBlobError,
    ),
    summary = "Get individual video segment",
    description = "Returns a single MP4 video segment by its index for HLS streaming.",
)]
pub(super) async fn get_hls_segment<T: WalrusReadClient + Send + Sync + 'static>(
    State(state): State<(Arc<T>, Arc<AggregatorResponseHeaderConfig>)>,
    Path((quilt_id_str, segment_index)): Path<(String, u32)>,
) -> Response {
    let (client, _) = state;
    
    tracing::info!(
        "getting HLS segment {} for quilt {}",
        segment_index,
        quilt_id_str
    );

    let quilt_id = match BlobId::from_str(&quilt_id_str) {
        Ok(id) => id,
        Err(_) => {
            tracing::error!("invalid quilt ID format: {}", quilt_id_str);
            return GetBlobError::BlobNotFound.to_response();
        }
    };

            let segment_id = format!("segment_{:05}.ts", segment_index);

    if let Some(quilt_patch_id) = get_quilt_patch_id(&quilt_id, &segment_id) {
        match client.get_blobs_by_quilt_patch_ids(&[quilt_patch_id]).await {
            Ok(mut blobs) => {
                if let Some(blob) = blobs.pop() {
                    let video_data = blob.into_data();
                    tracing::info!(
                        "returning segment {} ({} bytes) for quilt {}",
                        segment_id,
                        video_data.len(),
                        quilt_id_str
                    );

                    let mut response = (StatusCode::OK, video_data).into_response();
                    response.headers_mut().insert(
                        "Content-Type",
                        HeaderValue::from_static("video/mp4"),
                    );
                    response.headers_mut().insert(
                        "Cache-Control",
                        HeaderValue::from_static("public, max-age=3600"),
                    );
                    response
                } else {
                    tracing::warn!("no blob data for segment {}", segment_id);
                    GetBlobError::QuiltPatchNotFound.to_response()
                }
            }
            Err(error) => {
                tracing::error!(
                    "error retrieving segment {}: {:?}",
                    segment_id,
                    error
                );
                GetBlobError::from(error).to_response()
            }
        }
    } else {
        tracing::warn!("no mapping found for segment {}", segment_id);
        GetBlobError::QuiltPatchNotFound.to_response()
    }
}

#[cfg(test)]
mod tests {
    use axum::http::Uri;
    use serde_test::{Token, assert_de_tokens};
    use walrus_test_utils::param_test;

    use super::*;
    const ADDRESS: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";

    #[test]
    fn test_deserialization_publisher_query_empty() {
        let publisher_query = PublisherQuery::default();

        assert_de_tokens(
            &publisher_query,
            &[
                Token::Struct {
                    name: "PublisherQuery",
                    len: 4,
                },
                Token::Str("encoding_type"),
                Token::None,
                Token::Str("epochs"),
                Token::U32(1),
                Token::Str("deletable"),
                Token::Bool(false),
                Token::Str("force"),
                Token::Bool(false),
                Token::StructEnd,
            ],
        );
    }

    #[test]
    fn test_deserialization_publisher_query_share() {
        let publisher_query = PublisherQuery {
            send_or_share: Some(SendOrShare::Share(true)),
            ..Default::default()
        };
        let tokens = [
            Token::Struct {
                name: "PublisherQuery",
                len: 1,
            },
            Token::Str("share"),
            Token::Str("true"),
            Token::StructEnd,
        ];

        assert_de_tokens(&publisher_query, &tokens);
    }

    #[test]
    fn test_deserialization_publisher_query_send() {
        let publisher_query = PublisherQuery {
            send_or_share: Some(SendOrShare::SendObjectTo(
                SuiAddress::from_str(ADDRESS).expect("valid address"),
            )),
            ..Default::default()
        };
        let tokens = [
            Token::Struct {
                name: "PublisherQuery",
                len: 1,
            },
            Token::Str("send_object_to"),
            Token::Str(ADDRESS),
            Token::StructEnd,
        ];

        assert_de_tokens(&publisher_query, &tokens);
    }

    param_test! {
        test_parse_publisher_query: [
            many_epochs: (
                "epochs=11",
                Some(
                    PublisherQuery {
                        epochs: 11,
                        ..Default::default()
            })),
            send_to: (
                &format!("send_object_to={ADDRESS}"),
                Some(
                    PublisherQuery {
                        send_or_share: Some(
                            SendOrShare::SendObjectTo(
                                SuiAddress::from_str(ADDRESS).expect("valid address")
                                )),
                        ..Default::default()
            })),
            force: (
                "force=true",
                Some(
                    PublisherQuery {
                        force: true,
                        ..Default::default()
            })),
            share: (
                "share=true",
                Some(
                    PublisherQuery {
                        send_or_share: Some(SendOrShare::Share(true)),
                            ..Default::default()
            })),
            dont_share: (
                "share=false",
                Some(
                    PublisherQuery {
                        send_or_share: Some(SendOrShare::Share(false)),
                            ..Default::default()
            })),
            conflicting_share: (
                &format!("share=true&send_object_to={ADDRESS}"),
                None
            ),
            conflicting_send: (
                &format!("send_object_to={ADDRESS}&share=true"),
                None
            ),
            conflicting_double_share: (
                "share=false&share=true",
                None
            )
        ]
    }
    fn test_parse_publisher_query(query_str: &str, expected: Option<PublisherQuery>) {
        let uri_str = format!("http://localhost/test?{query_str}");
        let uri: Uri = uri_str.parse().expect("the uri is valid");

        let result = Query::<PublisherQuery>::try_from_uri(&uri);
        match result {
            Ok(Query(publisher_query)) => assert_eq!(
                publisher_query,
                expected.expect("result is ok => expected result is some")
            ),
            Err(_) => {
                assert!(
                    expected.is_none(),
                    "result is err => expected result is none"
                )
            }
        }
    }
}
