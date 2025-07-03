// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, HashSet},
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
    encoding::{QuiltError, quilt_encoding::QuiltStoreBlob},
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
    client::BlobPersistence,
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

    tracing::debug!("starting to store received blob");
    match client
        .write_blob(
            &blob[..],
            query.encoding_type,
            query.epochs,
            query.optimizations(),
            query.blob_persistence(),
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

    /// The blob cannot be returned as has been blocked.
    #[error("the requested metadata is blocked")]
    #[rest_api_error(reason = "FORBIDDEN_BLOB", status = ApiStatusCode::UnavailableForLegalReasons)]
    Blocked,

    /// The request is malformed.
    #[error("the request is malformed: {message}")]
    #[rest_api_error(reason = "MALFORMED_REQUEST", status = ApiStatusCode::InvalidArgument)]
    MalformedRequest { message: String },

    #[error(transparent)]
    #[rest_api_error(delegate)]
    Internal(#[from] anyhow::Error),
}

impl From<ClientError> for StoreBlobError {
    fn from(error: ClientError) -> Self {
        match error.kind() {
            ClientErrorKind::NotEnoughConfirmations(_, _) => Self::NotEnoughConfirmations,
            ClientErrorKind::BlobIdBlocked(_) => Self::Blocked,
            ClientErrorKind::QuiltError(_) => {
                Self::MalformedRequest {
                    message: format!("the quilt patch is not found: {}", error),
                }
            }
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
    /// If true, the publisher creates a deletable blob instead of a permanent one.
    #[serde(default)]
    pub deletable: bool,
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
    fn blob_persistence(&self) -> BlobPersistence {
        BlobPersistence::from_deletable(self.deletable)
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
    pub tags: Option<BTreeMap<String, serde_json::Value>>,
}

/// Store multiple blobs as a quilt using multipart/form-data.
///
/// Accepts a multipart form with blobs and optional per blob Walrus-native metadata.
/// The form contains:
/// - Blobs identified by their identifiers as field names (required)
/// - A `metadata` field containing a JSON array with per blob metadata (optional)
///
/// # Supported Walrus-native metadata fields
/// - `identifier`: The identifier of the blob, must match the corresponding blob field name
/// - `tags`: JSON object with string key-value pairs (optional)
///
/// Blobs without corresponding metadata entries will be stored with empty tags.
///
/// # Examples
///
/// ## Blobs without Walrus-native metadata
/// ```bash
/// curl -X PUT "http://localhost:8080/v1/quilts?epochs=5" \
///   -F "contract-v2=@document.pdf" \
///   -F "logo-2024=@image.png"
/// ```
///
/// ## Blobs with Walrus-native metadata
/// ```bash
/// curl -X PUT "http://localhost:8080/v1/quilts?epochs=5" \
///   -F "contract-v2=@document.pdf" \
///   -F "logo-2024=@image.png" \
///   -F 'metadata=[
///     {"identifier": "contract-v2", "tags": {"author": "alice", "version": "2.0"}},
///     {"identifier": "logo-2024", "tags": {"type": "logo", "format": "png"}}
///   ]'
/// ```
#[tracing::instrument(level = Level::ERROR, skip_all, fields(epochs=%query.epochs))]
#[utoipa::path(
    put,
    path = QUILT_PUT_ENDPOINT,
    request_body(
        content_type = "multipart/form-data",
        description = "Multipart form with blobs and their Walrus-native metadata"),
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
    tracing::debug!("starting to process multipart quilt upload");

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

    // Check authorization if present.
    if let Some(TypedHeader(header)) = bearer_header {
        let total_size: usize = quilt_store_blobs.iter().map(|blob| blob.data().len()).sum();
        if let Err(error) = check_blob_size(header, total_size) {
            return error.into_response();
        }
    }

    tracing::debug!("storing quilt with {} blobs", quilt_store_blobs.len());

    match client
        .write_quilt(
            quilt_store_blobs,
            query.encoding_type,
            query.epochs,
            query.optimizations(),
            query.blob_persistence(),
            query.post_store_action(client.default_post_store_action()),
        )
        .await
    {
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
    let mut file_data_map: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    let mut metadata_map: BTreeMap<String, QuiltPatchMetadata> = BTreeMap::new();

    while let Some(field) = multipart.next_field().await? {
        let field_name = field.name().unwrap_or("").to_string();
        if field_name == "metadata" {
            let metadata_json = field.text().await?;
            for meta in serde_json::from_str::<Vec<QuiltPatchMetadata>>(&metadata_json)? {
                let identifier = meta.identifier.clone();
                metadata_map.insert(identifier, meta);
            }
        } else {
            let data = field.bytes().await?.to_vec();
            file_data_map.insert(field_name, data);
        }
    }

    let mut res = Vec::with_capacity(file_data_map.len());

    if !metadata_map.is_empty() {
        for (identifier, data) in file_data_map {
            let tags = if let Some(meta) = metadata_map.get(&identifier) {
                meta.tags
                    .clone()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(k, v)| (k, v.to_string()))
                    .collect()
            } else {
                BTreeMap::new()
            };

            res.push(QuiltStoreBlob::new_owned(data, identifier).with_tags(tags));
        }
    } else {
        // No metadata provided - process files without metadata
        for (identifier, data) in file_data_map {
            res.push(QuiltStoreBlob::new_owned(data, identifier));
        }
    }

    Ok(res)
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
                    "type": "logo",
                    "format": "png"
                }
            }
        ]"#;

        let metadata: Vec<QuiltPatchMetadata> = serde_json::from_str(json).expect("should parse");
        assert_eq!(metadata.len(), 2);
        assert_eq!(metadata[0].identifier, "contract-v2");
        assert_eq!(metadata[1].identifier, "logo-2024");
        assert_eq!(
            metadata[0].tags.as_ref().unwrap().get("author").unwrap(),
            "alice"
        );
        assert_eq!(
            metadata[1].tags.as_ref().unwrap().get("type").unwrap(),
            "logo"
        );
    }
}
