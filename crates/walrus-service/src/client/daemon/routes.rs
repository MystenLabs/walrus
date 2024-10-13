// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

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
use tracing::Level;
use utoipa::IntoParams;
use walrus_core::{encoding::Primary, EpochCount};
use walrus_sui::client::{BlobPersistence, ContractClient, ReadClient};

use crate::{
    client::{BlobStoreResult, Client, ClientErrorKind, StoreWhen},
    common::api::{self, BlobIdString},
};

/// The status endpoint, which always returns a 200 status when it is available.
pub const STATUS_ENDPOINT: &str = "/status";
/// OpenAPI documentation endpoint.
pub const API_DOCS: &str = "/v1/api";
/// The path to get the blob with the given blob ID.
pub const BLOB_GET_ENDPOINT: &str = "/v1/:blobId";
/// The path to store a blob.
pub const BLOB_PUT_ENDPOINT: &str = "/v1/store";

#[tracing::instrument(level = Level::ERROR, skip_all, fields(%blob_id))]
#[utoipa::path(
    get,
    path = api::rewrite_route(BLOB_GET_ENDPOINT),
    params(("blob_id" = BlobIdString,)),
    responses(
        (status = 200, description = "The blob was reconstructed successfully", body = [u8]),
        (status = 404, description = "The requested blob does not exist"),
        (status = 500, description = "Internal server error" ),
        // TODO(mlegner): Improve error responses. (#178, #462)
    ),
)]
pub(super) async fn get_blob<T: ReadClient + Send + Sync>(
    request_headers: HeaderMap,
    State(client): State<Arc<Client<T>>>,
    Path(BlobIdString(blob_id)): Path<BlobIdString>,
) -> Response {
    tracing::debug!("starting to read blob");
    match client.read_blob_retry_epoch::<Primary>(&blob_id).await {
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
        Err(error) => match error.kind() {
            ClientErrorKind::BlobIdDoesNotExist => {
                tracing::debug!(?blob_id, "the requested blob ID does not exist");
                StatusCode::NOT_FOUND.into_response()
            }
            ClientErrorKind::BlobIdBlocked(_) => StatusCode::FORBIDDEN.into_response(),
            _ => {
                tracing::error!(%error, "error retrieving blob");
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        },
    }
}

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
        (status = 500, description = "Internal server error"),
        (status = 504, description = "Communication problem with Walrus storage nodes"),
        // TODO(mlegner): Document error responses. (#178, #462)
    ),
)]
pub(super) async fn put_blob<T: ContractClient>(
    State(client): State<Arc<Client<T>>>,
    Query(PublisherQuery {
        epochs,
        force,
        deletable,
    }): Query<PublisherQuery>,
    blob: Bytes,
) -> Response {
    tracing::debug!("starting to store received blob");
    let mut response = match client
        .reserve_and_store_blob_retry_epoch(
            &blob[..],
            epochs,
            StoreWhen::always(force),
            BlobPersistence::from_deletable(deletable),
        )
        .await
    {
        Ok(result) => {
            let status_code = if matches!(result, BlobStoreResult::MarkedInvalid { .. }) {
                StatusCode::INTERNAL_SERVER_ERROR
            } else {
                StatusCode::OK
            };
            (status_code, Json(result)).into_response()
        }
        Err(error) => {
            tracing::error!(%error, "error storing blob");
            match error.kind() {
                ClientErrorKind::NotEnoughConfirmations(_, _) => {
                    StatusCode::GATEWAY_TIMEOUT.into_response()
                }
                ClientErrorKind::BlobIdBlocked(_) => StatusCode::FORBIDDEN.into_response(),
                _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            }
        }
    };

    response
        .headers_mut()
        .insert(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
    response
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
    #[serde(default = "default_epochs")]
    epochs: EpochCount,
    #[serde(default)]
    force: bool,
    #[serde(default)]
    deletable: bool,
}

pub(super) fn default_epochs() -> EpochCount {
    1
}
