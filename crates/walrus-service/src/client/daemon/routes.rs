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
    body::{Body, Bytes},
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use axum_extra::{
    TypedHeader,
    extract::Multipart,
    headers::{Authorization, authorization::Bearer},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use fastcrypto::hash::{HashFunction, Sha256};
use futures::stream::{self, StreamExt};
use headers::HeaderMapExt;
use http_range_header::{EndPosition, StartPosition};
use jsonwebtoken::{DecodingKey, Validation};
use reqwest::{
    Method,
    header::{ACCEPT, CACHE_CONTROL, CONTENT_TYPE, ETAG, RANGE, X_CONTENT_TYPE_OPTIONS},
};
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
        ConsistencyCheckType,
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
    client::{
        daemon::{
            PostStoreAction,
            auth::{Claim, PublisherAuthError},
        },
        utils::{InvalidConsistencyCheck, consistency_check_type_from_flags},
    },
    common::api::{Binary, BlobIdString, QuiltPatchIdString, RestApiError},
};

/// The status endpoint, which always returns a 200 status when it is available.
pub const STATUS_ENDPOINT: &str = "/status";
/// OpenAPI documentation endpoint.
pub const API_DOCS: &str = "/v1/api";
/// The path to get the blob with the given blob ID.
pub const BLOB_GET_ENDPOINT: &str = "/v1/blobs/{blob_id}";
/// The path to get and concatenate multiple blobs by their IDs or blob IDs.
pub const BLOB_CONCAT_ENDPOINT: &str = "/v1alpha/blobs/concat";
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
/// The path to read a byte range from a blob.
pub const BLOB_BYTE_RANGE_GET_ENDPOINT: &str = "/v1/blobs/{blob_id}/byte-range";
/// Custom header for quilt patch identifier.
const X_QUILT_PATCH_IDENTIFIER: &str = "X-Quilt-Patch-Identifier";

const WALRUS_NATIVE_METADATA_FIELD_NAME: &str = "_metadata";

/// The query parameters for a read operation.
#[derive(Debug, Deserialize, Serialize, IntoParams, utoipa::ToSchema, PartialEq, Eq)]
#[into_params(parameter_in = Query, style = Form)]
#[serde(deny_unknown_fields)]
pub struct ReadOptions {
    /// Whether to perform a strict consistency check.
    ///
    /// This was the default before `v1.37`. In `v1.37`, the default consistency was changed to a
    /// more performant one, which is sufficient for the majority of cases. This flag can be used to
    /// enable the previous strict consistency check. See
    /// <https://docs.wal.app/design/encoding.html#data-integrity-and-consistency> for more details.
    #[serde(default)]
    pub strict_consistency_check: bool,
    /// Whether to skip consistency checks entirely.
    ///
    /// When enabled, this flag bypasses all consistency verification during blob reading. This
    /// should be used only when the writer of the blob is trusted. Does *not* affect any
    /// authentication checks for data received from storage nodes, which are always performed.
    #[serde(default)]
    pub skip_consistency_check: bool,
}

impl ReadOptions {
    pub fn consistency_check(&self) -> Result<ConsistencyCheckType, InvalidConsistencyCheck> {
        consistency_check_type_from_flags(
            self.strict_consistency_check,
            self.skip_consistency_check,
        )
    }
}

/// The query parameters for concatenating multiple blobs.
#[derive(Debug, Deserialize, Serialize, IntoParams, utoipa::ToSchema, PartialEq, Eq)]
#[into_params(parameter_in = Query, style = Form)]
#[serde(deny_unknown_fields)]
pub struct ConcatQueryParams {
    /// Comma-separated list of blob IDs or object IDs to concatenate.
    pub ids: String,
    /// Consistency check options for reading the blobs.
    #[serde(flatten)]
    pub read_options: ReadOptions,
}

/// The request body for concatenating multiple blobs via POST.
#[derive(Debug, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ConcatRequestBody {
    /// List of blob IDs or object IDs to concatenate.
    pub ids: Vec<String>,
    /// Whether to perform a strict consistency check.
    #[serde(default)]
    pub strict_consistency_check: Option<bool>,
    /// Whether to skip consistency checks entirely.
    #[serde(default)]
    pub skip_consistency_check: Option<bool>,
}

/// The query parameters for reading a byte range from a blob.
#[derive(Debug, Deserialize, Serialize, IntoParams, utoipa::ToSchema, PartialEq, Eq)]
#[into_params(parameter_in = Query, style = Form)]
#[serde(deny_unknown_fields)]
pub struct ByteRangeQueryParams {
    /// The starting byte position (0-indexed, inclusive).
    pub start: u64,
    /// The number of bytes to read.
    pub length: u64,
}

#[derive(Debug, PartialEq, Eq)]
struct ParsedRangeHeader {
    start: u64,
    end: u64,
    length: u64,
}

/// Parses an HTTP Range header value.
/// Supports the format "bytes=start-end" for range to end.
/// Returns (start_byte_position, byte_length) if valid, otherwise returns an error.
fn parse_range_header(range_value: &HeaderValue) -> Result<ParsedRangeHeader, GetBlobError> {
    let range_value = range_value
        .to_str()
        .map_err(|e| GetBlobError::InvalidByteRange {
            message: format!("invalid range header format: {}", e),
        })?
        .trim();

    let parsed_range = http_range_header::parse_range_header(range_value).map_err(|e| {
        GetBlobError::InvalidByteRange {
            message: format!("invalid range header format: {}", e),
        }
    })?;

    if parsed_range.ranges.len() != 1 {
        return Err(GetBlobError::InvalidByteRange {
            message: "only support one range per request".to_string(),
        });
    }

    let range = parsed_range.ranges[0];
    let StartPosition::Index(start_index) = range.start else {
        return Err(GetBlobError::InvalidByteRange {
            message: "must provide start index".to_string(),
        });
    };
    let EndPosition::Index(end_index) = range.end else {
        return Err(GetBlobError::InvalidByteRange {
            message: "must provide end index".to_string(),
        });
    };

    if start_index > end_index {
        return Err(GetBlobError::InvalidByteRange {
            message: "start index must be less than end index".to_string(),
        });
    }

    let length = end_index.saturating_sub(start_index).saturating_add(1);
    Ok(ParsedRangeHeader {
        start: start_index,
        end: end_index,
        length,
    })
}

/// Helper function to parse an ID string and resolve it to a BlobId.
/// Tries to parse as BlobId first, then as ObjectID if that fails.
/// Returns the BlobId and optionally the BlobAttribute if the ID was an ObjectID.
async fn parse_and_resolve_id<T: WalrusReadClient>(
    id_str: &str,
    client: &T,
) -> Result<(BlobId, Option<BlobAttribute>), ConcatBlobError> {
    // Try parsing as BlobId first
    if let Ok(blob_id) = BlobId::from_str(id_str) {
        return Ok((blob_id, None));
    }

    // Try parsing as ObjectID
    if let Ok(object_id) = ObjectID::from_str(id_str) {
        let blob_info = client
            .get_blob_by_object_id(&object_id)
            .await
            .map_err(|e| ConcatBlobError::ObjectIdResolutionFailed {
                object_id: object_id.to_string(),
                message: e.to_string(),
            })?;
        return Ok((blob_info.blob.blob_id, blob_info.attribute));
    }

    Err(ConcatBlobError::InvalidId {
        id: id_str.to_string(),
    })
}

/// Retrieve a Walrus blob.
///
/// Reconstructs the blob identified by the provided blob ID from Walrus and returns its raw data.
///
/// # HTTP Range Support
///
/// This endpoint also accepts the HTTP `Range` header to request only a portion of the blob.
/// When a `Range` header is provided with a specific byte range (e.g., `bytes=100-199`), the
/// endpoint returns HTTP 206 Partial Content with the requested byte range.
///
/// **Important**: When using byte range requests, only the consistency of the individual fetched
/// slivers is validated. The entire blob's consistency is NOT validated, which provides better
/// performance for partial reads but with reduced integrity guarantees compared to full blob reads.
///
/// # Range Header Format
///
/// - `bytes=start-end`: Returns bytes from start to end (inclusive)
/// - `bytes=start-`: Not supported yet (TODO(WAL-1108))
/// - `bytes=-end`: Not supported yet
///
/// # Examples
///
/// ```bash
/// # Get full blob
/// curl "$AGGREGATOR/v1/blobs/{blob_id}"
///
/// # Get first 1000 bytes
/// curl -H "Range: bytes=0-999" "$AGGREGATOR/v1/blobs/{blob_id}"
#[tracing::instrument(level = Level::ERROR, skip_all, fields(%blob_id))]
#[utoipa::path(
    get,
    path = BLOB_GET_ENDPOINT,
    params(("blob_id" = BlobId,), ReadOptions),
    responses(
        (status = 200, description = "The blob was reconstructed successfully", body = [u8]),
        GetBlobError,
        InvalidConsistencyCheck,
    ),
)]
pub(super) async fn get_blob<T: WalrusReadClient>(
    request_method: Method,
    request_headers: HeaderMap,
    State(client): State<Arc<T>>,
    Path(BlobIdString(blob_id)): Path<BlobIdString>,
    Query(read_options): Query<ReadOptions>,
) -> Response {
    tracing::debug!("starting to read blob");
    let consistency_check = match read_options.consistency_check() {
        Ok(consistency_check) => consistency_check,
        Err(error) => return error.into_response(),
    };

    let mut response = if let Some(range_header) = request_headers.get(RANGE) {
        let ParsedRangeHeader { start, end, length } = match parse_range_header(range_header) {
            Ok(v) => v,
            Err(error) => {
                tracing::debug!(?error, "invalid byte range header");
                return error.to_response();
            }
        };

        // TODO(WAL-1107): support streaming the response sliver by sliver.
        let read_result = match client.read_byte_range(&blob_id, start, length).await {
            Ok(read_result) => read_result,
            Err(error) => {
                tracing::debug!(?error, "failed to read byte range");
                return GetBlobError::from(error).to_response();
            }
        };

        let mut response = (StatusCode::PARTIAL_CONTENT, read_result.data).into_response();

        // Create the Content-Range header: "bytes start-end/total"
        let content_range_header =
            match headers::ContentRange::bytes(start..=end, read_result.unencoded_blob_size) {
                Ok(header) => header,
                Err(error) => {
                    return GetBlobError::InvalidByteRange {
                        message: error.to_string(),
                    }
                    .to_response();
                }
            };
        response.headers_mut().typed_insert(content_range_header);

        let content_length_header = headers::ContentLength(length);
        response.headers_mut().typed_insert(content_length_header);
        response
    } else {
        match client.read_blob(&blob_id, consistency_check).await {
            Ok(blob) => (StatusCode::OK, blob).into_response(),
            Err(error) => {
                tracing::debug!(?error, "failed to read blob");
                return GetBlobError::from(error).to_response();
            }
        }
    };

    tracing::debug!("successfully retrieved blob");
    let headers = response.headers_mut();
    populate_response_headers_from_request(
        request_method,
        &request_headers,
        &blob_id.to_string(),
        headers,
    );
    response
}

fn populate_response_headers_from_request(
    request_method: Method,
    request_headers: &HeaderMap,
    etag: &str,
    headers: &mut HeaderMap,
) {
    // Prevent the browser from trying to guess the MIME type to avoid dangerous inferences.
    headers.insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    // Insert headers that help caches distribute Walrus blobs.
    //
    // Cache for 1 day, and allow refreshing on the client side. Refreshes use the ETag to
    // check if the content has changed. This allows invalidated blobs to be removed from
    // caches. `stale-while-revalidate` allows stale content to be served for 1 hour while
    // the browser tries to validate it (async revalidation).
    headers.insert(
        CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=86400, stale-while-revalidate=3600"),
    );
    // The `ETag` is some unique key to identify the body data.
    headers.insert(
        ETAG,
        HeaderValue::from_str(etag)
            .expect("the blob ID string only contains visible ASCII characters"),
    );
    // Mirror the content type in various ways.
    if let Some(accept) = request_headers.get(ACCEPT)
        && !accept.as_bytes().contains(&b'*')
    {
        tracing::debug!(
            ?accept,
            "mirroring the request's accept header as our content type"
        );
        headers.insert(CONTENT_TYPE, accept.clone());
    } else if request_method == Method::GET
        && let Some(content_type) = request_headers.get(CONTENT_TYPE)
    {
        tracing::debug!(?content_type, "mirroring the request's content type");
        headers.insert(CONTENT_TYPE, content_type.clone());
    } // Cache for 1 day, and allow refreshig on the client side. Refreshes use the ETag to
}

fn populate_response_headers_from_attributes(
    headers: &mut HeaderMap,
    attribute: &BlobAttribute,
    allowed_headers: Option<&HashSet<HeaderName>>,
) {
    for (key, value) in attribute.iter() {
        if key.is_empty() {
            continue;
        }
        let Ok(header_name) = HeaderName::from_str(key) else {
            tracing::warn!("Invalid header name '{}' in blob attribute", key);
            continue;
        };
        let Ok(header_value) = HeaderValue::from_str(value) else {
            tracing::warn!("Invalid header value '{}' in blob attribute", value);
            continue;
        };
        if headers.contains_key(key) {
            // Do not overwrite existing headers
            continue;
        }
        if allowed_headers.is_none_or(|headers| headers.contains(&header_name)) {
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
        InvalidConsistencyCheck,
    ),
)]
pub(super) async fn get_blob_by_object_id<T: WalrusReadClient>(
    request_method: Method,
    request_headers: HeaderMap,
    State((client, response_header_config)): State<(Arc<T>, Arc<AggregatorResponseHeaderConfig>)>,
    Path(blob_object_id): Path<ObjectID>,
    read_options: Query<ReadOptions>,
) -> Response {
    tracing::debug!("starting to read blob with attribute");
    match client.get_blob_by_object_id(&blob_object_id).await {
        Ok(BlobWithAttribute { blob, attribute }) => {
            // Get the blob data using the existing get_blob function
            let mut response = get_blob(
                request_method,
                request_headers.clone(),
                State(client),
                Path(BlobIdString(blob.blob_id)),
                read_options,
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

/// Errors that can occur when reading a byte range from a blob.
#[derive(Debug, thiserror::Error, RestApiError)]
#[rest_api_error(domain = ERROR_DOMAIN)]
pub(crate) enum ByteRangeReadError {
    /// The requested blob has not yet been stored on Walrus.
    #[error("the requested blob ID does not exist on Walrus, ensure that it was entered correctly")]
    #[rest_api_error(reason = "BLOB_NOT_FOUND", status = ApiStatusCode::NotFound)]
    BlobNotFound,

    /// The blob cannot be returned as it has been blocked.
    #[error("the requested blob is blocked")]
    #[rest_api_error(reason = "FORBIDDEN_BLOB", status = ApiStatusCode::UnavailableForLegalReasons)]
    Blocked,

    /// The byte range parameters are invalid.
    #[error("invalid byte range parameters: {message}")]
    #[rest_api_error(reason = "INVALID_BYTE_RANGE", status = ApiStatusCode::InvalidArgument)]
    InvalidByteRange { message: String },

    /// The blob size exceeds the maximum allowed size that was configured for this service.
    #[error("the blob size exceeds the maximum allowed size: {0}")]
    #[rest_api_error(reason = "BLOB_TOO_LARGE", status = ApiStatusCode::SizeExceeded)]
    BlobTooLarge(u64),

    #[error(transparent)]
    #[rest_api_error(delegate)]
    Internal(#[from] anyhow::Error),
}

impl From<ClientError> for ByteRangeReadError {
    fn from(error: ClientError) -> Self {
        match error.kind() {
            ClientErrorKind::BlobIdDoesNotExist => Self::BlobNotFound,
            ClientErrorKind::BlobIdBlocked(_) => Self::Blocked,
            ClientErrorKind::BlobTooLarge(max_blob_size) => Self::BlobTooLarge(*max_blob_size),
            ClientErrorKind::ByteRangeReadInputError(msg) => Self::InvalidByteRange {
                message: msg.to_string(),
            },
            _ => anyhow::anyhow!(error).into(),
        }
    }
}

/// Retrieve a specific byte range from a Walrus blob.
///
/// Reads a specific portion of a blob identified by the blob ID, starting from a given byte
/// position and reading a specified number of bytes. This is more efficient than retrieving the
/// entire blob when only a portion is needed.
///
/// Note that this endpoint has reduced consistency guarantees compared to the full blob read
/// since it will only validate the consistency of individual fetched slivers.
///
/// # Query Parameters
/// - `start`: The starting byte position (0-indexed, inclusive)
/// - `length`: The number of bytes to read
///
/// # Validation
/// - `start` and `length` must not overflow when added together
/// - The byte range `[start, start + length)` must be within the blob's size
/// - `length` must be greater than 0
///
/// # Example
/// ```bash
/// # Read 1024 bytes starting from position 0
/// curl "$AGGREGATOR/v1/blobs/{blob_id}/byte-range?start=0&length=1024"
/// ```
#[tracing::instrument(
    level = Level::ERROR,
    skip_all,
    fields(
        %blob_id,
        start=%params.start,
        length=%params.length,
    ),
)]
#[utoipa::path(
    get,
    path = BLOB_BYTE_RANGE_GET_ENDPOINT,
    params(
        ("blob_id" = BlobId,),
        ByteRangeQueryParams,
    ),
    responses(
        (status = 200, description = "The byte range was read successfully", body = [u8]),
        ByteRangeReadError,
    ),
)]
pub(super) async fn get_blob_byte_range<T: WalrusReadClient>(
    request_method: Method,
    request_headers: HeaderMap,
    State(client): State<Arc<T>>,
    Path(BlobIdString(blob_id)): Path<BlobIdString>,
    Query(params): Query<ByteRangeQueryParams>,
) -> Response {
    tracing::debug!(
        "starting to read byte range from blob: start={}, length={}",
        params.start,
        params.length
    );

    // Validate parameters upfront
    if params.length == 0 {
        return ByteRangeReadError::InvalidByteRange {
            message: "length must be greater than 0".to_string(),
        }
        .into_response();
    }

    // Check for overflow when adding start and length
    if params.start.checked_add(params.length).is_none() {
        return ByteRangeReadError::InvalidByteRange {
            message: "start + length overflows".to_string(),
        }
        .into_response();
    }

    // Combine blob id and the query into etag.
    let etag = format!("{}-{}-{}", blob_id, params.start, params.length);

    // Perform the byte range read using the trait method
    match client
        .read_byte_range(&blob_id, params.start, params.length)
        .await
    {
        Ok(result) => {
            tracing::debug!(
                "successfully retrieved byte range of {} bytes",
                result.data.len()
            );

            // Use StatusCode::OK instead of StatusCode::PARTIAL_CONTENT so that the response
            // can be cached by the CDN.
            let mut response = (StatusCode::OK, result.data).into_response();
            let headers = response.headers_mut();
            populate_response_headers_from_request(
                request_method,
                &request_headers,
                &etag,
                headers,
            );
            response
        }
        Err(error) => {
            let error = ByteRangeReadError::from(error);

            tracing::debug!(?etag, ?error, "error retrieving byte range");

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

    /// The byte range parameters are invalid.
    ///
    /// This error is returned when the byte range parameters are invalid.
    #[error("invalid byte range parameters: {message}")]
    #[rest_api_error(reason = "INVALID_BYTE_RANGE", status = ApiStatusCode::RangeNotSatisfiable)]
    InvalidByteRange { message: String },

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
            ClientErrorKind::ByteRangeReadInputError(message) => Self::InvalidByteRange {
                message: message.to_string(),
            },
            _ => anyhow::anyhow!(error).into(),
        }
    }
}

/// Errors that can occur when concatenating blobs.
#[derive(Debug, thiserror::Error, RestApiError)]
#[rest_api_error(domain = ERROR_DOMAIN)]
pub(crate) enum ConcatBlobError {
    /// The provided ID string is neither a valid blob ID nor a valid object ID.
    #[error("invalid ID format: {id}")]
    #[rest_api_error(reason = "INVALID_ID_FORMAT", status = ApiStatusCode::InvalidArgument)]
    InvalidId { id: String },

    /// Failed to resolve an object ID to a blob ID.
    #[error("failed to resolve object ID {object_id}: {message}")]
    #[rest_api_error(reason = "OBJECT_ID_RESOLUTION_FAILED", status = ApiStatusCode::NotFound)]
    ObjectIdResolutionFailed { object_id: String, message: String },

    /// One of the requested blobs was not found.
    #[error("blob not found: {blob_id}")]
    #[rest_api_error(reason = "BLOB_NOT_FOUND", status = ApiStatusCode::NotFound)]
    BlobNotFound { blob_id: String },

    /// One of the blobs is blocked.
    #[error("blob is blocked: {blob_id}")]
    #[rest_api_error(reason = "FORBIDDEN_BLOB", status = ApiStatusCode::UnavailableForLegalReasons)]
    BlobBlocked { blob_id: String },

    /// One of the blobs exceeds the maximum allowed size.
    #[error("blob exceeds maximum size: {blob_id} (size: {size})")]
    #[rest_api_error(reason = "BLOB_TOO_LARGE", status = ApiStatusCode::SizeExceeded)]
    BlobTooLarge { blob_id: String, size: u64 },

    /// Failed to read a blob.
    #[error("failed to read blob {blob_id}: {message}")]
    #[rest_api_error(reason = "BLOB_READ_FAILED", status = ApiStatusCode::Internal)]
    BlobReadFailed { blob_id: String, message: String },
}

impl ConcatBlobError {
    fn from_client_error(error: ClientError, blob_id: BlobId) -> Self {
        // Map ClientError to appropriate ConcatBlobError
        match error.kind() {
            ClientErrorKind::BlobIdDoesNotExist => Self::BlobNotFound {
                blob_id: blob_id.to_string(),
            },
            ClientErrorKind::BlobIdBlocked(_) => Self::BlobBlocked {
                blob_id: blob_id.to_string(),
            },
            ClientErrorKind::BlobTooLarge(size) => Self::BlobTooLarge {
                blob_id: blob_id.to_string(),
                size: *size,
            },
            _ => Self::BlobReadFailed {
                blob_id: blob_id.to_string(),
                message: error.to_string(),
            },
        }
    }
}

/// Shared implementation for concatenating and streaming multiple blobs.
///
/// Takes a list of ID strings (blob IDs or object IDs), resolves them to blob IDs,
/// and returns a streaming response with the concatenated blob data.
async fn concat_blobs_impl<T: WalrusReadClient + Send + Sync + 'static>(
    client: Arc<T>,
    id_strings: Vec<String>,
    strict_consistency_check: bool,
    skip_consistency_check: bool,
    request_method: Method,
    request_headers: HeaderMap,
    response_header_config: Arc<AggregatorResponseHeaderConfig>,
) -> Response {
    if id_strings.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            "at least one blob ID or object ID must be provided",
        )
            .into_response();
    }

    let consistency_check =
        match consistency_check_type_from_flags(strict_consistency_check, skip_consistency_check) {
            Ok(check) => check,
            Err(e) => return e.into_response(),
        };

    // There is an implicit trade-off here between performance and correctness. We use the list of
    // ids as provided by the user, which means that if a different user indexed the same blobs
    // using blob IDs instead of object IDs, the ETag will be different. However,
    let etag = URL_SAFE_NO_PAD.encode(Sha256::digest(id_strings.join(",").as_bytes()).digest);

    // Resolve all IDs to blob IDs upfront (also fail fast)
    // Track the first blob's attribute for header population
    let mut blob_ids = Vec::new();
    let mut first_attribute: Option<BlobAttribute> = None;
    for (index, id_str) in id_strings.iter().enumerate() {
        match parse_and_resolve_id(id_str.as_str(), client.as_ref()).await {
            Ok((blob_id, attribute)) => {
                blob_ids.push(blob_id);
                // Only capture the first blob's attribute
                if index == 0 {
                    first_attribute = attribute;
                }
            }
            Err(error) => return error.into_response(),
        }
    }

    // Create streaming response
    let client_clone = client.clone();
    let stream = stream::iter(blob_ids).then(move |blob_id| {
        let client = client_clone.clone();
        async move {
            match client.read_blob(&blob_id, consistency_check).await {
                Ok(blob_data) => Ok(Bytes::from(blob_data)),
                Err(error) => Err(ConcatBlobError::from_client_error(error, blob_id)),
            }
        }
    });

    let mut response = (StatusCode::OK, Body::from_stream(stream)).into_response();
    let headers = response.headers_mut();
    populate_response_headers_from_request(request_method, &request_headers, &etag, headers);

    if let Some(attribute) = first_attribute {
        populate_response_headers_from_attributes(
            response.headers_mut(),
            &attribute,
            Some(&response_header_config.allowed_headers),
        );
    }

    response
}

/// Concatenate and stream multiple blobs via GET.
///
/// Retrieves multiple blobs specified by comma-separated blob IDs or object IDs in the query
/// parameter, and streams them concatenated in the order specified. Each blob is loaded,
/// streamed, and then freed from memory before the next blob is processed.
#[tracing::instrument(level = Level::ERROR, skip_all)]
#[utoipa::path(
    get,
    path = BLOB_CONCAT_ENDPOINT,
    params(ConcatQueryParams),
    responses(
        (status = 200, description = "Blobs concatenated and streamed successfully", body = [u8]),
        ConcatBlobError,
        InvalidConsistencyCheck,
    ),
)]
pub(super) async fn get_blobs_concat<T: WalrusReadClient + Send + Sync + 'static>(
    State((client, response_header_config)): State<(Arc<T>, Arc<AggregatorResponseHeaderConfig>)>,
    request_headers: HeaderMap,
    Query(params): Query<ConcatQueryParams>,
) -> Response {
    // Parse comma-separated IDs
    let id_strings: Vec<String> = params
        .ids
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    concat_blobs_impl(
        client,
        id_strings,
        params.read_options.strict_consistency_check,
        params.read_options.skip_consistency_check,
        Method::GET,
        request_headers,
        response_header_config,
    )
    .await
}

/// Concatenate and stream multiple blobs via POST.
///
/// Retrieves multiple blobs specified in a JSON request body containing an array of blob IDs
/// or object IDs, and streams them concatenated in the order specified. Each blob is loaded,
/// streamed, and then freed from memory before the next blob is processed.
#[tracing::instrument(level = Level::ERROR, skip_all)]
#[utoipa::path(
    post,
    path = BLOB_CONCAT_ENDPOINT,
    request_body = ConcatRequestBody,
    responses(
        (status = 200, description = "Blobs concatenated and streamed successfully", body = [u8]),
        ConcatBlobError,
        InvalidConsistencyCheck,
    ),
)]
pub(super) async fn post_blobs_concat<T: WalrusReadClient + Send + Sync + 'static>(
    request_method: Method,
    request_headers: HeaderMap,
    State((client, response_header_config)): State<(Arc<T>, Arc<AggregatorResponseHeaderConfig>)>,
    Json(body): Json<ConcatRequestBody>,
) -> Response {
    concat_blobs_impl(
        client,
        body.ids,
        body.strict_consistency_check.unwrap_or(false),
        body.skip_consistency_check.unwrap_or(false),
        request_method,
        request_headers,
        response_header_config,
    )
    .await
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
            blob.into(),
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
        .max_age(Duration::from_hours(24))
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
    request_method: Method,
    request_headers: HeaderMap,
    State((client, response_header_config)): State<(Arc<T>, Arc<AggregatorResponseHeaderConfig>)>,
    Path(QuiltPatchIdString(quilt_patch_id)): Path<QuiltPatchIdString>,
) -> Response {
    let quilt_patch_id_str = quilt_patch_id.to_string();
    tracing::debug!("starting to read quilt patch: {}", quilt_patch_id_str);

    let quilt_id = quilt_patch_id.quilt_id;

    match client.get_blobs_by_quilt_patch_ids(&[quilt_patch_id]).await {
        Ok(mut blobs) => {
            if let Some(blob) = blobs.pop() {
                build_quilt_patch_response(
                    blob,
                    request_method,
                    &request_headers,
                    &quilt_id.to_string(),
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
    request_method: Method,
    request_headers: &HeaderMap,
    etag: &str,
    response_header_config: &AggregatorResponseHeaderConfig,
) -> Response {
    let identifier = blob.identifier().to_string();
    let blob_attribute: BlobAttribute = blob.tags().clone().into();
    let blob_data = blob.into_data();
    let mut response = (StatusCode::OK, blob_data).into_response();
    populate_response_headers_from_request(
        request_method,
        request_headers,
        etag,
        response.headers_mut(),
    );
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
            Method::GET,
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
    #[deprecated(
        since = "1.35.0",
        note = "blobs are now deletable by default; this flag has no effect and may be removed in \
        the future"
    )]
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
            #[allow(deprecated)]
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
        #[allow(deprecated)]
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
                .map(|(k, v)| match v {
                    serde_json::Value::String(s) => (k.clone(), s.clone()),
                    _ => (k.clone(), v.to_string()),
                })
                .collect()
        } else {
            BTreeMap::new()
        };

        res.push(QuiltStoreBlob::new_owned(data, identifier)?.with_tags(tags));
    }

    Ok(res)
}

/// Custom deserializer for QuiltVersionEnum that uses `From<String>`.
fn deserialize_quilt_version<'de, D>(deserializer: D) -> Result<Option<QuiltVersionEnum>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt_str: Option<String> = Option::deserialize(deserializer)?;
    Ok(opt_str.map(QuiltVersionEnum::from))
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderValue, Uri};
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
    fn test_parse_range_header() {
        // Test valid range with both start and end
        let result = parse_range_header(&HeaderValue::from_static("bytes=0-499"));
        assert_eq!(
            result.expect("should be ok"),
            ParsedRangeHeader {
                start: 0,
                end: 499,
                length: 500
            }
        );

        let result = parse_range_header(&HeaderValue::from_static("bytes=100-199"));
        assert_eq!(
            result.expect("should be ok"),
            ParsedRangeHeader {
                start: 100,
                end: 199,
                length: 100
            }
        );

        // Test open-ended range (should return error since we require end value)
        assert!(parse_range_header(&HeaderValue::from_static("bytes=500-")).is_err());

        // Test invalid formats
        assert!(parse_range_header(&HeaderValue::from_static("bytes=")).is_err());
        assert!(parse_range_header(&HeaderValue::from_static("invalid")).is_err());
        assert!(parse_range_header(&HeaderValue::from_static("bytes=100-50")).is_err());

        // Test with spaces
        let result = parse_range_header(&HeaderValue::from_static(" bytes=10-20 "));
        assert_eq!(
            result.expect("should be ok"),
            ParsedRangeHeader {
                start: 10,
                end: 20,
                length: 11,
            }
        );
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

    #[test]
    fn test_populate_response_headers_case_insensitive() {
        use walrus_sui::types::move_structs::BlobAttribute;

        let mut headers = HeaderMap::new();

        let blob_attribute = BlobAttribute::from([
            ("Content-Type", "text/html"),
            ("X-Custom-Header", "custom-value"),
            ("cache-control", "no-cache"),
            ("AUTHORIZATION", "Bearer token"),
            ("Not-Allowed-Header", "should-not-appear"),
        ]);

        // Create allowed headers with different casing.
        let allowed_headers_input = [
            "content-type".to_string(),
            "X-CUSTOM-HEADER".to_string(),
            "Cache-Control".to_string(),
            "authorization".to_string(),
        ];

        let allowed_headers: HashSet<HeaderName> = allowed_headers_input
            .iter()
            .filter_map(|h| h.parse::<HeaderName>().ok())
            .collect();

        populate_response_headers_from_attributes(
            &mut headers,
            &blob_attribute,
            Some(&allowed_headers),
        );

        // Verify that all allowed headers are present (case-insensitive matching worked).
        assert_eq!(headers.get("content-type").unwrap(), "text/html");
        assert_eq!(headers.get("x-custom-header").unwrap(), "custom-value");
        assert_eq!(headers.get("cache-control").unwrap(), "no-cache");
        assert_eq!(headers.get("authorization").unwrap(), "Bearer token");

        // Verify that non-allowed header is not present.
        assert!(headers.get("not-allowed-header").is_none());

        // Test with no allowed headers filter (all headers should be included).
        let mut headers_all = HeaderMap::new();
        populate_response_headers_from_attributes(&mut headers_all, &blob_attribute, None);

        // All headers should be present when no filter is applied.
        assert_eq!(headers_all.len(), 5);
        assert!(headers_all.get("not-allowed-header").is_some());
    }
}
