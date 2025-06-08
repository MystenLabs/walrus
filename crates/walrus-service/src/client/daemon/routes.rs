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
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use sui_types::base_types::{ObjectID, SuiAddress};
use tower_http::cors::{Any, CorsLayer};
use tracing::Level;
use utoipa::IntoParams;
use walrus_core::{BlobId, EncodingType, EpochCount, QuiltPatchId};
use walrus_proc_macros::RestApiError;
use walrus_sdk::{
    client::responses::BlobStoreResult,
    error::{ClientError, ClientErrorKind},
    store_when::StoreWhen,
};
use walrus_storage_node_client::api::errors::DAEMON_ERROR_DOMAIN as ERROR_DOMAIN;
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
/// The path to get blobs from quilt by IDs.
pub const QUILT_BLOBS_ENDPOINT: &str = "/v1/quilt/blobs";
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
            populate_common_response_headers(&request_headers, &blob_id.to_string(), headers);
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

fn populate_common_response_headers(
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
            query.store_when(),
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

/// Request to get a blob from quilt by its ID.
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema, utoipa::IntoParams)]
pub struct GetQuiltBlobRequest {
    /// Quilt blob ID to retrieve.
    #[serde_as(as = "DisplayFromStr")]
    pub quilt_patch_id: QuiltPatchId,
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
/// Takes a quilt blob ID and returns the corresponding blob from the quilt.
#[tracing::instrument(level = Level::ERROR, skip_all)]
#[utoipa::path(
    get,
    path = QUILT_BLOBS_ENDPOINT,
    params(GetQuiltBlobRequest),
    responses(
        (status = 200, description = "The blob was retrieved successfully",
            body = [u8]),
        GetBlobError,
    ),
)]
pub(super) async fn get_blob_in_quilt<T: WalrusReadClient>(
    request_headers: HeaderMap,
    State((client, allowed_headers)): State<(Arc<T>, Arc<HashSet<String>>)>,
    Query(request): Query<GetQuiltBlobRequest>,
) -> Response {
    let quilt_patch_id_str = request.quilt_patch_id.to_string();
    tracing::info!("starting to read quilt blob: {}", quilt_patch_id_str);

    match client
        .get_blobs_by_quilt_ids(&[request.quilt_patch_id])
        .await
    {
        Ok(mut blobs) => {
            if let Some(blob) = blobs.pop() {
                tracing::info!("successfully retrieved blob");
                let mut blob_attribute: BlobAttribute = blob.attributes.into();
                blob_attribute.insert(X_QUILT_PATCH_IDENTIFIER.to_string(), blob.identifier);
                let mut response = (StatusCode::OK, blob.blob).into_response();
                populate_common_response_headers(
                    &request_headers,
                    &quilt_patch_id_str,
                    response.headers_mut(),
                );
                populate_response_headers(
                    response.headers_mut(),
                    &blob_attribute,
                    &allowed_headers,
                );
                response
            } else {
                tracing::info!("no blob returned for the requested quilt blob ID");
                let error = GetBlobError::BlobNotFound;
                error.to_response()
            }
        }
        Err(error) => {
            let error = GetBlobError::from(error);

            match &error {
                GetBlobError::BlobNotFound => {
                    tracing::info!("requested quilt blob ID does not exist")
                }
                GetBlobError::Internal(error) => {
                    tracing::info!(?error, "error retrieving quilt blob")
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
    /// Returns the [`StoreWhen`] value based on the query parameters.
    ///
    /// The publisher always ignores existing resources.
    fn store_when(&self) -> StoreWhen {
        if self.force {
            StoreWhen::AlwaysIgnoreResources
        } else {
            StoreWhen::NotStoredIgnoreResources
        }
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
        let uri_str = format!("http://localhost/test?{}", query_str);
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
