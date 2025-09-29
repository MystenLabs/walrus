// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

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
    extract::Multipart,
    headers::{Authorization, authorization::Bearer},
};
use jsonwebtoken::{DecodingKey, Validation};
use reqwest::header::{CACHE_CONTROL, CONTENT_TYPE, ETAG, X_CONTENT_TYPE_OPTIONS};
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use sui_types::base_types::{ObjectID, SuiAddress};
use tower_http::cors::{Any, CorsLayer};
use tracing::Level;
use utoipa::IntoParams;
use walrus_core::{
    BlobId,
    EncodingType,
    EpochCount,
    QuiltPatchId,
    encoding::{
        QuiltError,
        quilt_encoding::{QuiltApi, QuiltStoreBlob, QuiltVersionEnum, QuiltVersionV1},
    },
};
use walrus_proc_macros::RestApiError;
use walrus_sdk::{
    client::responses::{BlobStoreResult, QuiltStoreResult},
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
/// The path to store multiple files as a quilt using multipart/form-data.
pub const QUILT_PUT_ENDPOINT: &str = "/v1/quilts";
/// The path to get blobs from quilt by IDs.
pub const QUILT_PATCH_BY_ID_GET_ENDPOINT: &str = "/v1/blobs/by-quilt-patch-id/{quilt_patch_id}";
/// The path to get blob from quilt by quilt ID and identifier.
pub const QUILT_PATCH_BY_IDENTIFIER_GET_ENDPOINT: &str =
    "/v1/blobs/by-quilt-id/{quilt_id}/{identifier}";
/// The path to list patches in a quilt.
pub const LIST_PATCHES_IN_QUILT_ENDPOINT: &str = "/v1/quilts/{quilt_id}/patches";
/// Custom header for quilt patch identifier.
const X_QUILT_PATCH_IDENTIFIER: &str = "X-Quilt-Patch-Identifier";

const WALRUS_NATIVE_METADATA_FIELD_NAME: &str = "_metadata";

/// Retrieve a Walrus blob.
///
/// Reconstructs the blob identified by the provided blob ID from Walrus and returns its raw data.
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
        if !key.is_empty()
            && allowed_headers.is_none_or(|headers| headers.contains(key))
            && let (Ok(header_name), Ok(header_value)) =
                (HeaderName::from_str(key), HeaderValue::from_str(value))
        {
            headers.insert(header_name, header_value);
        }
    }
}

/// Retrieve a Walrus blob with its associated attribute.
///
/// First retrieves the blob metadata from Sui using the provided object ID (either of the blob
/// object or a shared blob), then uses the blob ID from that metadata to fetch the actual blob
/// data via the get_blob function. The response includes the raw data along with any attribute
/// headers from the metadata that are present in the configured allowed_headers set.
#[tracing::instrument(level = Level::ERROR, skip_all, fields(%blob_object_id))]
#[utoipa::path(
    get,
    path = BLOB_OBJECT_GET_ENDPOINT,
    params(("blob_object_id" = ObjectIdSchema,)),
    responses(
        (
            status = 200,
            description =
                "The blob was reconstructed successfully. Any attribute headers present in this \
                aggregator's allowed_headers configuration will be included in the response.",
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
            if response.status() == StatusCode::OK
                && let Some(attribute) = attribute
            {
                populate_response_headers_from_attributes(
                    response.headers_mut(),
                    &attribute,
                    Some(&response_header_config.allowed_headers),
                );
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

    /// The blob size exceeds the maximum allowed size that was configured for this service.
    ///
    /// The blob is still stored on Walrus and may be accessible through a different aggregator or
    /// the CLI.
    #[error("the blob size exceeds the maximum allowed size: {0}")]
    #[rest_api_error(reason = "BLOB_TOO_LARGE", status = ApiStatusCode::SizeExceeded)]
    BlobTooLarge(u64),

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
            ClientErrorKind::BlobTooLarge(max_blob_size) => Self::BlobTooLarge(*max_blob_size),
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
    if let Some(TypedHeader(header)) = bearer_header
        && let Err(error) = check_blob_size(header, blob.len())
    {
        return error.into_response();
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
            if let Some(max_size) = claim.max_size
                && blob_size as u64 > max_size
            {
                return Err(PublisherAuthError::InvalidSize);
            }
            if let Some(size) = claim.size
                && blob_size as u64 != size
            {
                return Err(PublisherAuthError::InvalidSize);
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

    /// The request is malformed.
    #[error("the request is malformed: {message}")]
    #[rest_api_error(reason = "MALFORMED_REQUEST", status = ApiStatusCode::InvalidArgument)]
    MalformedRequest { message: String },

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
            ClientErrorKind::QuiltError(_) => Self::MalformedRequest {
                message: format!("the quilt patch is not found: {error:?}"),
            },
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

/// Retrieve a patch from a Walrus quilt by its quilt patch ID.
///
/// Takes a quilt patch ID and returns the corresponding patch from the quilt.
/// The patch content is returned as raw bytes in the response body, while metadata
/// such as the patch identifier and tags are returned in response headers.
///
/// # Example
/// ```bash
/// curl -X GET "$AGGREGATOR/v1/blobs/by-quilt-patch-id/\
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
/// [raw data]
/// ```
#[tracing::instrument(level = Level::ERROR, skip_all)]
#[utoipa::path(
    get,
    path = QUILT_PATCH_BY_ID_GET_ENDPOINT,
    params(
        (
            "quilt_patch_id" = QuiltPatchId,
            description =
                "The quilt patch ID encoded as a URL-safe base64 string, without the trailing \
                equal (=) signs.",
            example = "DJHLsgUoKQKEPcw3uehNQwuJjMu5a2sRdn8r-f7iWSAAC8Pw",
        )
    ),
    responses(
        (
            status = 200,
            description =
                "The patch was retrieved successfully. Returns the raw bytes of the patch; the \
                identifier and other attributes are returned as header.",
            body = [u8],
        ),
        GetBlobError,
    ),
)]
pub(super) async fn get_patch_by_quilt_patch_id<T: WalrusReadClient>(
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

/// Retrieve a patch from a Walrus quilt by its quilt ID and identifier.
///
/// Takes a quilt ID and an identifier and returns the corresponding patch from the quilt.
/// The patch content is returned as raw bytes in the response body, while metadata
/// such as the patch identifier and tags are returned in response headers.
///
/// # Example
/// ```bash
/// curl -X GET "$aggregator/v1/blobs/by-quilt-id/\
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
/// [raw data]
/// ```
#[tracing::instrument(level = Level::ERROR, skip_all)]
#[utoipa::path(
    get,
    path = QUILT_PATCH_BY_IDENTIFIER_GET_ENDPOINT,
    params(
        (
            "quilt_id" = BlobId, Path,
            description = "The quilt's blob ID.",
        ), (
            "identifier" = String,
            description = "The identifier of the patch within the quilt",
            example = "my-file.txt"
        )
    ),
    responses(
        (
            status = 200,
            description =
                "The patch was retrieved successfully. Returns the raw bytes of the patch; \
                the identifier and other attributes are returned as headers.",
            body = [u8]
        ),
        GetBlobError,
    ),
)]
pub(super) async fn get_patch_by_quilt_id_and_identifier<T: WalrusReadClient>(
    request_headers: HeaderMap,
    State((client, response_header_config)): State<(Arc<T>, Arc<AggregatorResponseHeaderConfig>)>,
    Path((quilt_id, identifier)): Path<(BlobIdString, String)>,
) -> Response {
    let quilt_id = quilt_id.0;
    tracing::debug!(
        "starting to read quilt blob by ID and identifier: {} / {}",
        quilt_id,
        identifier
    );

    match client
        .get_patch_by_quilt_id_and_identifier(&quilt_id, &identifier)
        .await
    {
        Ok(blob) => build_quilt_patch_response(
            blob,
            &request_headers,
            &quilt_id.to_string(),
            &response_header_config,
        ),
        Err(error) => {
            let error = GetBlobError::from(error);

            match &error {
                GetBlobError::BlobNotFound => {
                    tracing::info!("requested quilt blob with ID {} does not exist", quilt_id,)
                }
                GetBlobError::QuiltPatchNotFound => {
                    tracing::info!(
                        "requested quilt patch {} does not exist in quilt {}",
                        identifier,
                        quilt_id,
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

/// Response item for a patch in a quilt.
#[serde_as]
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct QuiltPatchItem {
    /// The identifier of the patch (e.g., filename).
    pub identifier: String,
    /// The patch ID for this patch, encoded as a URL-safe base64 string, without the trailing equal
    /// (=) signs.
    #[serde_as(as = "DisplayFromStr")]
    pub patch_id: QuiltPatchId,
    /// Tags for the patch.
    pub tags: BTreeMap<String, String>,
}

/// List patches in a quilt.
///
/// Returns a list of identifiers and quilt patch IDs for all patches contained in the specified
/// quilt. Each quilt patch ID can be used with the `/v1/blobs/by-quilt-patch-id` endpoint to
/// retrieve the actual blob data.
///
/// # Example
/// ```bash
/// curl -X GET "$aggregator/v1/quilts/patches-by-id/\
/// rkcHpHQrornOymttgvSq3zvcmQEsMqzmeUM1HSY4ShU"
/// ```
///
/// Response:
/// ```json
/// [
///   {
///     "identifier": "walrus-38.jpeg",
///     "patch_id": "uIiEbhP2qgZYygEGxJX1GeB-rQATo2yufC2DCp7B4iABAQANAA"
///   },
///   {
///     "identifier": "walrus-39.avif",
///     "patch_id": "uIiEbhP2qgZYygEGxJX1GeB-rQATo2yufC2DCp7B4iABDQBnAA"
///   }
/// ]
/// ```
#[tracing::instrument(level = Level::ERROR, skip_all)]
#[utoipa::path(
    get,
    path = LIST_PATCHES_IN_QUILT_ENDPOINT,
    params(
        (
            "quilt_id" = BlobId,
            description = "The quilt ID encoded as URL-safe base64",
            example = "rkcHpHQrornOymttgvSq3zvcmQEsMqzmeUM1HSY4ShU"
        )
    ),
    responses(
        (
            status = 200,
            description = "Successfully retrieved the list of patches in the quilt",
            body = Vec<QuiltPatchItem>
        ),
        GetBlobError,
    ),
)]
pub(super) async fn list_patches_in_quilt<T: WalrusReadClient>(
    State(client): State<Arc<T>>,
    Path(BlobIdString(quilt_id)): Path<BlobIdString>,
) -> Response {
    tracing::debug!("starting to list patches in quilt: {}", quilt_id);

    match client.list_patches_in_quilt(&quilt_id).await {
        Ok(patches) => (StatusCode::OK, Json(patches)).into_response(),
        Err(error) => {
            let error = GetBlobError::from(error);

            match &error {
                GetBlobError::BlobNotFound => {
                    tracing::debug!(?quilt_id, "the requested quilt ID does not exist")
                }
                GetBlobError::Internal(error) => {
                    tracing::error!(?error, "error retrieving quilt patches")
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
#[derive(Debug, Deserialize, Serialize, IntoParams, utoipa::ToSchema, PartialEq, Eq)]
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
    /// If true, the publisher creates a deletable blob.
    ///
    /// New blobs are created as deletable by default since v1.33, and this flag is no longer
    /// required.
    #[serde(default)]
    pub deletable: bool,
    /// If true, the publisher creates a permanent blob instead of a deletable one.
    ///
    /// This was the default behavior before v1.33, but *blobs are now deletable by default*.
    #[serde(default)]
    pub permanent: bool,
    /// If true, the publisher will always store the blob, creating a new Blob object.
    ///
    /// The blob will be stored even if the blob is already certified on Walrus for the specified
    /// number of epochs.
    #[serde(default)]
    pub force: bool,
    /// The quilt version to use (for quilt endpoints only).
    /// Valid values: "v1", "V1", or "1". Defaults to "v1" if not specified.
    #[serde(default)]
    #[serde(deserialize_with = "deserialize_quilt_version")]
    pub quilt_version: Option<QuiltVersionEnum>,

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
            quilt_version: None,
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

/// A helper structure to hold metadata for a quilt patch.
#[derive(Debug, Deserialize)]
pub struct QuiltPatchMetadata {
    pub identifier: String,
    #[serde(default)]
    pub tags: serde_json::Map<String, serde_json::Value>,
}

/// Store multiple files as a quilt using multipart/form-data.
///
/// Accepts a multipart form with files and optional per-file Walrus-native metadata.
/// The form contains:
/// - Files identified by their identifiers as field names
/// - An optional `_metadata` field containing a JSON array with per-file Walrus-native metadata
///
/// # Contents of Walrus-native metadata
/// - `identifier`: The identifier of the file, must match the corresponding file field name
/// - `tags`: JSON object with string key-value pairs (optional)
///
/// Files without corresponding metadata entries will be stored with empty tags.
///
/// # Examples
///
/// ## Files without Walrus-native metadata, with quilt version V1
/// ```bash
/// curl -X PUT "$PUBLISHER/v1/quilts?epochs=5&quilt_version=V1" \
///   -F "contract-v2=@document.pdf" \
///   -F "logo-2024=@image.png"
/// ```
///
/// ## Files with Walrus-native metadata, with default quilt version
/// ```bash
/// curl -X PUT "$PUBLISHER/v1/quilts?epochs=5" \
///   -F "quilt-manual=@document.pdf" \
///   -F "logo-2025=@image.png" \
///   -F "_metadata=[
///     {"identifier": "quilt-manual", "tags": {"creator": "walrus", "version": "1.0"}},
///     {"identifier": "logo-2025", "tags": {"type": "logo", "format": "png"}}
///   ]'
/// ```
#[tracing::instrument(level = Level::ERROR, skip_all, fields(epochs=%query.epochs))]
#[utoipa::path(
    put,
    path = QUILT_PUT_ENDPOINT,
    request_body(
        content_type = "multipart/form-data",
        description = "Multipart form with files and their Walrus-native metadata"),
    params(PublisherQuery),
    responses(
        (status = 200, description = "The quilt was stored successfully", body = QuiltStoreResult),
        (status = 400, description = "The request is malformed"),
        (status = 413, description = "The quilt is too large"),
        StoreBlobError,
    ),
)]
pub(super) async fn put_quilt<T: WalrusWriteClient>(
    State(client): State<Arc<T>>,
    Query(query): Query<PublisherQuery>,
    bearer_header: Option<TypedHeader<Authorization<Bearer>>>,
    multipart: Multipart,
) -> Response {
    tracing::debug!("starting to process quilt upload");

    // Parse the quilt version, defaulting to V1 if not specified.
    let quilt_version = query.quilt_version.clone().unwrap_or(QuiltVersionEnum::V1);

    let quilt_store_blobs = match parse_multipart_quilt(multipart).await {
        Ok(blobs) => blobs,
        Err(error) => {
            tracing::debug!(?error, "failed to parse multipart form");
            return StoreBlobError::MalformedRequest {
                message: format!("failed to parse multipart form: {error:?}"),
            }
            .into_response();
        }
    };

    if quilt_store_blobs.is_empty() {
        return StoreBlobError::MalformedRequest {
            message: "no files provided in multipart form".to_string(),
        }
        .into_response();
    }

    let blob_persistence = match query.blob_persistence() {
        Ok(blob_persistence) => blob_persistence,
        Err(error) => return error.into_response(),
    };

    // For now, we only support V1 quilts.
    assert_eq!(quilt_version, QuiltVersionEnum::V1);

    let quilt = match client
        .construct_quilt::<QuiltVersionV1>(&quilt_store_blobs, query.encoding_type)
        .await
    {
        Ok(quilt) => quilt,
        Err(e) => {
            return StoreBlobError::MalformedRequest {
                message: format!("failed to construct quilt: {e:?}"),
            }
            .into_response();
        }
    };

    if let Some(TypedHeader(header)) = bearer_header
        && let Err(error) = check_blob_size(header, quilt.data().len())
    {
        return error.into_response();
    }

    let result = client
        .write_quilt::<QuiltVersionV1>(
            quilt,
            query.encoding_type,
            query.epochs,
            query.optimizations(),
            blob_persistence,
            query.post_store_action(client.default_post_store_action()),
        )
        .await;

    match result {
        Ok(result) => (StatusCode::OK, Json(result)).into_response(),
        Err(error) => {
            tracing::error!(?error, "error storing quilt");
            StoreBlobError::from(error).into_response()
        }
    }
}

/// Parse multipart form data and extract files with their metadata.
async fn parse_multipart_quilt(
    mut multipart: Multipart,
) -> Result<Vec<QuiltStoreBlob<'static>>, anyhow::Error> {
    let mut blobs_with_identifiers = Vec::new();
    let mut metadata_map: HashMap<String, QuiltPatchMetadata> = HashMap::new();

    while let Some(field) = multipart.next_field().await? {
        let field_name = field.name().unwrap_or("").to_string();
        if field_name == WALRUS_NATIVE_METADATA_FIELD_NAME {
            let metadata_json = field.text().await?;
            for meta in serde_json::from_str::<Vec<QuiltPatchMetadata>>(&metadata_json)? {
                let identifier = meta.identifier.clone();
                if let Some(existing) = metadata_map.insert(identifier, meta) {
                    return Err(StoreBlobError::MalformedRequest {
                        message: format!("duplicate identifiers found in _metadata: {existing:?}"),
                    }
                    .into());
                }
            }
        } else {
            let data = field.bytes().await?.to_vec();
            blobs_with_identifiers.push((field_name, data));
        }
    }

    let mut res = Vec::with_capacity(blobs_with_identifiers.len());
    for (identifier, data) in blobs_with_identifiers {
        let tags = if let Some(meta) = metadata_map.get(&identifier) {
            meta.tags
                .iter()
                .map(|(k, v)| (k.clone(), v.to_string()))
                .collect()
        } else {
            BTreeMap::new()
        };

        res.push(QuiltStoreBlob::new_owned(data, identifier)?.with_tags(tags));
    }

    Ok(res)
}

/// Custom deserializer for QuiltVersionEnum that uses From<String>.
fn deserialize_quilt_version<'de, D>(deserializer: D) -> Result<Option<QuiltVersionEnum>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt_str: Option<String> = Option::deserialize(deserializer)?;
    Ok(opt_str.map(QuiltVersionEnum::from))
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
                    len: 5,
                },
                Token::Str("encoding_type"),
                Token::None,
                Token::Str("epochs"),
                Token::U32(1),
                Token::Str("deletable"),
                Token::Bool(false),
                Token::Str("force"),
                Token::Bool(false),
                Token::Str("quilt_version"),
                Token::None,
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

    #[test]
    fn test_quilt_file_metadata_deserialization() {
        let json = r#"[
                {
                    "identifier": "contract-v2",
                    "tags": {
                        "author": "alice",
                        "version": "2.0"
                    }
                },
                {
                    "identifier": "logo-2024",
                    "tags": {
                        "type": 3,
                        "format": "png"
                    }
                }
            ]"#;

        let metadata: Vec<QuiltPatchMetadata> = serde_json::from_str(json).expect("should parse");
        assert_eq!(metadata.len(), 2);
        assert_eq!(metadata[0].identifier, "contract-v2");
        assert_eq!(metadata[1].identifier, "logo-2024");
        assert_eq!(
            metadata[0]
                .tags
                .get("author")
                .expect("should be some")
                .as_str()
                .expect("should be some"),
            "alice"
        );
        assert_eq!(
            metadata[1]
                .tags
                .get("type")
                .expect("should be some")
                .to_string(),
            "3"
        );
    }
}
