// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{net::SocketAddr, sync::Arc};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json,
    Router,
};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;
use walrus_core::{
    messages::StorageConfirmation,
    metadata::{BlobMetadata, UnverifiedBlobMetadataWithId},
    BlobId,
    Sliver,
    SliverPairIndex,
    SliverType,
};

use self::extract::{Bcs, BcsRejection};
use crate::node::{ServiceState, StoreMetadataError, StoreSliverError};

mod extract;

/// The path to get and store blob metadata.
pub const METADATA_ENDPOINT: &str = "/v1/blobs/:blobId/metadata";
/// The path to get and store slivers.
pub const SLIVER_ENDPOINT: &str = "/v1/blobs/:blobId/slivers/:sliverPairIdx/:sliverType";
/// The path to get storage confirmations.
pub const STORAGE_CONFIRMATION_ENDPOINT: &str = "/v1/blobs/:blobId/confirmation";
/// The path to get recovery symbols.
pub const RECOVERY_ENDPOINT: &str =
    "/v1/blobs/:blobId/slivers/:sliverPairIdx/:sliverType/:targetPairIndex";

/// A blob ID encoded as a Base64 string designed to be used in URLs.
#[serde_as]
#[derive(Deserialize, Serialize)]
struct BlobIdString(#[serde_as(as = "DisplayFromStr")] BlobId);

/// Error message returned by the service.
#[derive(Debug, Serialize, Deserialize)]
pub enum ServiceResponse<T> {
    /// The request was successful.
    Success {
        /// The success code.
        code: u16,
        /// The data returned by the service.
        data: T,
    },
    /// The error message returned by the service.
    Error {
        /// The error code.
        code: u16,
        /// The error message.
        message: String,
    },
}

impl<T: Serialize> IntoResponse for ServiceResponse<T> {
    fn into_response(self) -> axum::response::Response {
        (
            StatusCode::from_u16(self.code()).expect("valid u16 status code"),
            Json(self),
        )
            .into_response()
    }
}

impl<T: Serialize> ServiceResponse<T> {
    /// Creates a new success response.
    fn success(code: StatusCode, data: T) -> Self {
        Self::Success {
            code: code.as_u16(),
            data,
        }
    }
}

impl<T> ServiceResponse<T> {
    fn code(&self) -> u16 {
        match self {
            Self::Success { code, .. } | Self::Error { code, .. } => *code,
        }
    }

    fn error<S: Into<String>>(code: StatusCode, message: S) -> Self {
        Self::Error {
            code: code.as_u16(),
            message: message.into(),
        }
    }

    fn not_found() -> Self {
        Self::error(StatusCode::NOT_FOUND, "Not found")
    }

    fn internal_error() -> Self {
        Self::error(StatusCode::INTERNAL_SERVER_ERROR, "Internal Error")
    }
}

/// Represents a user server.
#[derive(Debug)]
pub struct UserServer<S> {
    state: Arc<S>,
    cancel_token: CancellationToken,
}

impl<S: ServiceState + Send + Sync + 'static> UserServer<S> {
    /// Creates a new user server.
    pub fn new(state: Arc<S>, cancel_token: CancellationToken) -> Self {
        Self {
            state,
            cancel_token,
        }
    }

    /// Creates a new user server.
    pub async fn run(&self, network_address: &SocketAddr) -> Result<(), std::io::Error> {
        let app = Router::new()
            .route(
                METADATA_ENDPOINT,
                get(Self::retrieve_metadata).put(Self::store_metadata),
            )
            .route(
                SLIVER_ENDPOINT,
                get(Self::retrieve_sliver).put(Self::store_sliver),
            )
            .route(
                STORAGE_CONFIRMATION_ENDPOINT,
                get(Self::retrieve_storage_confirmation),
            )
            .route(RECOVERY_ENDPOINT, get(Self::retrieve_recovery_symbol))
            .with_state(self.state.clone())
            .layer(TraceLayer::new_for_http());

        let listener = tokio::net::TcpListener::bind(network_address).await?;
        axum::serve(listener, app)
            .with_graceful_shutdown(self.cancel_token.clone().cancelled_owned())
            .await
    }

    async fn retrieve_metadata(
        State(state): State<Arc<S>>,
        Path(BlobIdString(blob_id)): Path<BlobIdString>,
    ) -> Response {
        match state.retrieve_metadata(&blob_id) {
            Ok(Some(metadata)) => {
                tracing::debug!("Retrieved metadata for {blob_id:?}");
                (StatusCode::OK, Bcs(metadata)).into_response()
            }
            Ok(None) => {
                tracing::debug!("Metadata not found for {blob_id:?}");
                ServiceResponse::<()>::not_found().into_response()
            }
            Err(message) => {
                tracing::error!("Internal server error: {message}");
                ServiceResponse::<()>::internal_error().into_response()
            }
        }
    }

    async fn store_metadata(
        State(state): State<Arc<S>>,
        Path(BlobIdString(blob_id)): Path<BlobIdString>,
        Bcs(metadata): Bcs<BlobMetadata>,
    ) -> ServiceResponse<String> {
        let unverified_metadata_with_id = UnverifiedBlobMetadataWithId::new(blob_id, metadata);
        match state.store_metadata(unverified_metadata_with_id) {
            Ok(()) => {
                let msg = format!("Stored metadata for {blob_id:?}");
                tracing::debug!(msg);
                ServiceResponse::success(StatusCode::CREATED, msg)
            }
            Err(StoreMetadataError::AlreadyStored) => {
                let msg = format!("Metadata for {blob_id:?} was already stored");
                tracing::debug!(msg);
                ServiceResponse::success(StatusCode::OK, msg)
            }
            Err(StoreMetadataError::InvalidMetadata(message)) => {
                tracing::debug!("Received invalid metadata: {message}");
                ServiceResponse::error(StatusCode::BAD_REQUEST, message.to_string())
            }
            Err(StoreMetadataError::NotRegistered) => {
                let msg = format!("Blob {blob_id:?} has not been registered");
                tracing::debug!(msg);
                ServiceResponse::error(StatusCode::BAD_REQUEST, msg)
            }
            Err(StoreMetadataError::BlobExpired) => {
                let msg = format!("Blob {blob_id:?} is expired");
                tracing::debug!(msg);
                ServiceResponse::error(StatusCode::BAD_REQUEST, msg)
            }
            Err(StoreMetadataError::Internal(message)) => {
                tracing::error!("Internal server error: {message}");
                ServiceResponse::internal_error()
            }
        }
    }

    async fn retrieve_sliver(
        State(state): State<Arc<S>>,
        Path((BlobIdString(blob_id), sliver_pair_idx, sliver_type)): Path<(
            BlobIdString,
            SliverPairIndex,
            SliverType,
        )>,
    ) -> Response {
        match state.retrieve_sliver(&blob_id, sliver_pair_idx, sliver_type) {
            Ok(Some(sliver)) => {
                tracing::debug!("Retrieved {sliver_type:?} sliver for {blob_id:?}");
                assert_eq!(
                    sliver.r#type(),
                    sliver_type,
                    "service must never return a invalid type"
                );

                match sliver {
                    Sliver::Primary(inner) => (StatusCode::OK, Bcs(inner)).into_response(),
                    Sliver::Secondary(inner) => (StatusCode::OK, Bcs(inner)).into_response(),
                }
            }
            Ok(None) => {
                tracing::debug!("{sliver_type:?} sliver not found for {blob_id:?}");
                ServiceResponse::<()>::not_found().into_response()
            }
            Err(message) => {
                tracing::error!("Internal server error: {message}");
                ServiceResponse::<()>::internal_error().into_response()
            }
        }
    }

    /// Retrieves a recovery symbol for a shard held by this storage node.
    /// The sliver_type is the target type of the sliver that will be recovered
    /// The sliver_pair_idx is the index of the sliver pair that we want to access
    /// Index is the requesters index in the established order of storage nodes
    async fn retrieve_recovery_symbol(
        State(state): State<Arc<S>>,
        Path((BlobIdString(blob_id), sliver_pair_idx, sliver_type, target_pair_index)): Path<(
            BlobIdString,
            SliverPairIndex,
            SliverType,
            SliverPairIndex,
        )>,
    ) -> Response {
        match state.retrieve_recovery_symbol(
            &blob_id,
            sliver_pair_idx,
            sliver_type,
            target_pair_index,
        ) {
            Ok(symbol) => {
                tracing::debug!("Retrieved recovery symbol for {blob_id:?}");
                (StatusCode::OK, Bcs(symbol)).into_response()
            }
            Err(message) => {
                tracing::debug!("Symbol not found with error {message}");
                ServiceResponse::<()>::not_found().into_response()
            }
        }
    }

    async fn store_sliver(
        State(state): State<Arc<S>>,
        Path((BlobIdString(blob_id), sliver_pair_idx, sliver_type)): Path<(
            BlobIdString,
            SliverPairIndex,
            SliverType,
        )>,
        body: axum::body::Bytes,
    ) -> Response {
        let sliver = match decode_sliver(body, sliver_type) {
            Ok(sliver) => sliver,
            Err(rejection) => return rejection.into_response(),
        };

        match state.store_sliver(&blob_id, sliver_pair_idx, &sliver) {
            Ok(()) => {
                tracing::debug!("Stored {sliver_type:?} sliver for {blob_id:?}");
                ServiceResponse::success(StatusCode::OK, ())
            }
            Err(StoreSliverError::Internal(message)) => {
                tracing::error!("Internal server error: {message}");
                ServiceResponse::error(StatusCode::INTERNAL_SERVER_ERROR, "Internal error")
            }
            Err(client_error) => {
                tracing::debug!("Received invalid {sliver_type:?} sliver: {client_error}");
                ServiceResponse::error(StatusCode::BAD_REQUEST, client_error.to_string())
            }
        }
        .into_response()
    }

    async fn retrieve_storage_confirmation(
        State(state): State<Arc<S>>,
        Path(BlobIdString(blob_id)): Path<BlobIdString>,
    ) -> ServiceResponse<StorageConfirmation> {
        match state.compute_storage_confirmation(&blob_id).await {
            Ok(Some(confirmation)) => {
                tracing::debug!("Retrieved storage confirmation for {blob_id:?}");
                ServiceResponse::success(StatusCode::OK, confirmation)
            }
            Ok(None) => {
                tracing::debug!("Storage confirmation not found for {blob_id:?}");
                ServiceResponse::not_found()
            }
            Err(message) => {
                tracing::error!("Internal server error: {message}");
                ServiceResponse::error(StatusCode::INTERNAL_SERVER_ERROR, "Internal error")
            }
        }
    }
}

fn decode_sliver(
    bytes: axum::body::Bytes,
    sliver_type: SliverType,
) -> Result<Sliver, BcsRejection> {
    Ok(match sliver_type {
        SliverType::Primary => Sliver::Primary(Bcs::from_bytes(&bytes)?.0),
        SliverType::Secondary => Sliver::Secondary(Bcs::from_bytes(&bytes)?.0),
    })
}

#[cfg(test)]
mod test {
    use anyhow::anyhow;
    use reqwest::Url;
    use tokio::task::JoinHandle;
    use tokio_util::sync::CancellationToken;
    use walrus_core::{
        encoding::Primary,
        merkle::MerkleProof,
        messages::StorageConfirmation,
        metadata::UnverifiedBlobMetadataWithId,
        BlobId,
        DecodingSymbol,
        Sliver,
        SliverPairIndex,
        SliverType,
    };
    use walrus_sdk::client::Client;
    use walrus_test_utils::{async_param_test, WithTempDir};

    use super::*;
    use crate::{
        config::StorageNodeConfig,
        node::{RetrieveSliverError, RetrieveSymbolError},
        test_utils,
    };

    pub struct MockServiceState;

    impl ServiceState for MockServiceState {
        /// Returns a valid response only for blob IDs with the first byte 0, None for those
        /// starting with 1, and otherwise an error.
        fn retrieve_metadata(
            &self,
            blob_id: &BlobId,
        ) -> Result<Option<UnverifiedBlobMetadataWithId>, anyhow::Error> {
            if blob_id.0[0] == 0 {
                Ok(Some(
                    walrus_core::test_utils::verified_blob_metadata().into_unverified(),
                ))
            } else if blob_id.0[0] == 1 {
                Ok(None)
            } else {
                Err(anyhow::anyhow!("Invalid shard"))
            }
        }

        fn store_metadata(
            &self,
            _metadata: UnverifiedBlobMetadataWithId,
        ) -> Result<(), StoreMetadataError> {
            Ok(())
        }

        fn retrieve_sliver(
            &self,
            _blob_id: &BlobId,
            _sliver_pair_idx: SliverPairIndex,
            _sliver_type: SliverType,
        ) -> Result<Option<Sliver>, RetrieveSliverError> {
            Ok(Some(walrus_core::test_utils::sliver()))
        }

        /// Returns a valid response only for the pair index 0, otherwise, returns
        /// an internal error.
        fn retrieve_recovery_symbol(
            &self,
            _blob_id: &BlobId,
            sliver_pair_idx: SliverPairIndex,
            _sliver_type: SliverType,
            _target_pair_index: SliverPairIndex,
        ) -> Result<DecodingSymbol<MerkleProof>, RetrieveSymbolError> {
            if sliver_pair_idx == SliverPairIndex(0) {
                Ok(walrus_core::test_utils::recovery_symbol())
            } else {
                Err(RetrieveSymbolError::Internal(anyhow!("Invalid shard")))
            }
        }

        /// Successful only for the pair index 0, otherwise, returns an internal error.
        fn store_sliver(
            &self,
            _blob_id: &BlobId,
            sliver_pair_idx: SliverPairIndex,
            _sliver: &Sliver,
        ) -> Result<(), StoreSliverError> {
            if sliver_pair_idx.as_usize() == 0 {
                Ok(())
            } else {
                Err(StoreSliverError::Internal(anyhow!("Invalid shard")))
            }
        }

        /// Returns a confirmation for blob ID starting with zero, None when starting with 1,
        /// and otherwise an error.
        async fn compute_storage_confirmation(
            &self,
            blob_id: &BlobId,
        ) -> Result<Option<StorageConfirmation>, anyhow::Error> {
            if blob_id.0[0] == 0 {
                let confirmation = walrus_core::test_utils::signed_storage_confirmation();
                Ok(Some(StorageConfirmation::Signed(confirmation)))
            } else if blob_id.0[0] == 1 {
                Ok(None)
            } else {
                Err(anyhow::anyhow!("Invalid shard"))
            }
        }
    }

    async fn start_rest_api_with_config(
        config: &StorageNodeConfig,
    ) -> JoinHandle<Result<(), std::io::Error>> {
        let server = UserServer::new(Arc::new(MockServiceState), CancellationToken::new());
        let network_address = config.rest_api_address;
        let handle = tokio::spawn(async move { server.run(&network_address).await });

        tokio::task::yield_now().await;
        handle
    }

    async fn start_rest_api_with_test_config() -> (
        WithTempDir<StorageNodeConfig>,
        JoinHandle<Result<(), std::io::Error>>,
    ) {
        let config = test_utils::storage_node_config();
        let handle = start_rest_api_with_config(config.as_ref()).await;
        (config, handle)
    }

    fn storage_node_client(config: &StorageNodeConfig) -> Client {
        let network_address = config.rest_api_address;
        let url = Url::parse(&format!("http://{network_address}")).unwrap();

        // Do not load any proxy information from the system, as it's slow (at least on MacOs).
        let inner = reqwest::Client::builder().no_proxy().build().unwrap();

        Client::from_url(url, inner)
    }

    fn blob_id_for_valid_response() -> BlobId {
        let mut blob_id = walrus_core::test_utils::random_blob_id();
        blob_id.0[0] = 0; // Triggers a valid response
        blob_id
    }

    fn blob_id_for_not_found() -> BlobId {
        let mut blob_id = walrus_core::test_utils::random_blob_id();
        blob_id.0[0] = 1; // Triggers a not found response
        blob_id
    }

    fn blob_id_for_internal_server_error() -> BlobId {
        let mut blob_id = walrus_core::test_utils::random_blob_id();
        blob_id.0[0] = 2; // Triggers an internal server error.
        blob_id
    }

    #[tokio::test]
    async fn retrieve_metadata() {
        let (config, _handle) = start_rest_api_with_test_config().await;
        let client = storage_node_client(config.as_ref());

        let blob_id = blob_id_for_valid_response();
        let _metadata = client
            .get_metadata(&blob_id)
            .await
            .expect("should successfully return metadata");
    }

    #[tokio::test]
    async fn retrieve_metadata_not_found() {
        let (config, _handle) = start_rest_api_with_test_config().await;
        let client = storage_node_client(config.as_ref());

        let blob_id = blob_id_for_not_found();
        let err = client
            .get_metadata(&blob_id)
            .await
            .expect_err("metadata request mut fail");

        assert_eq!(err.http_status_code(), Some(StatusCode::NOT_FOUND));
    }

    #[tokio::test]
    async fn retrieve_metadata_internal_error() {
        let (config, _handle) = start_rest_api_with_test_config().await;
        let client = storage_node_client(config.as_ref());

        let blob_id = blob_id_for_internal_server_error();
        let err = client
            .get_metadata(&blob_id)
            .await
            .expect_err("metadata request must fail");

        assert_eq!(
            err.http_status_code(),
            Some(StatusCode::INTERNAL_SERVER_ERROR)
        );
    }

    #[tokio::test]
    async fn store_metadata() {
        let (config, _handle) = start_rest_api_with_test_config().await;

        let metadata_with_blob_id = walrus_core::test_utils::unverified_blob_metadata();
        let metadata = metadata_with_blob_id.metadata();

        let blob_id = metadata_with_blob_id.blob_id().to_string();
        let path = METADATA_ENDPOINT.replace(":blobId", &blob_id);
        let url = format!("http://{}{path}", config.as_ref().rest_api_address);

        let client = storage_node_client(config.as_ref()).into_inner();
        let res = client
            .put(url)
            .body(bcs::to_bytes(metadata).unwrap())
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn retrieve_sliver() {
        let (config, _handle) = start_rest_api_with_test_config().await;
        let client = storage_node_client(config.as_ref());

        let blob_id = walrus_core::test_utils::random_blob_id();
        let sliver_pair_id = SliverPairIndex(0); // Triggers an valid response

        let _sliver = client
            .get_sliver::<Primary>(&blob_id, sliver_pair_id)
            .await
            .expect("should successfully retrieve sliver");
    }

    #[tokio::test]
    async fn store_sliver() {
        let (config, _handle) = start_rest_api_with_test_config().await;
        let client = storage_node_client(config.as_ref());

        let sliver = walrus_core::test_utils::sliver();

        let blob_id = walrus_core::test_utils::random_blob_id();
        let sliver_pair_id = SliverPairIndex(0); // Triggers an ok response

        client
            .store_sliver(&blob_id, sliver_pair_id, &sliver)
            .await
            .expect("sliver should be successfully stored");
    }

    #[tokio::test]
    async fn store_sliver_error() {
        let (config, _handle) = start_rest_api_with_test_config().await;
        let client = storage_node_client(config.as_ref());

        let blob_id = walrus_core::test_utils::random_blob_id();
        let sliver = walrus_core::test_utils::sliver();
        let sliver_pair_id = SliverPairIndex(1); // Triggers an internal server error

        let err = client
            .store_sliver(&blob_id, sliver_pair_id, &sliver)
            .await
            .expect_err("store sliver should fail");

        assert_eq!(
            err.http_status_code(),
            Some(StatusCode::INTERNAL_SERVER_ERROR)
        );
    }

    #[tokio::test]
    async fn retrieve_storage_confirmation() {
        let (config, _handle) = start_rest_api_with_test_config().await;
        let client = storage_node_client(config.as_ref());

        let blob_id = blob_id_for_valid_response();
        let _confirmation = client
            .get_confirmation(&blob_id)
            .await
            .expect("should return a signed confirmation");
    }

    async_param_test! {
        retrieve_storage_confirmation_fails: [
            not_found: (blob_id_for_not_found(), StatusCode::NOT_FOUND),
            internal_error: (blob_id_for_internal_server_error(), StatusCode::INTERNAL_SERVER_ERROR)
        ]
    }
    async fn retrieve_storage_confirmation_fails(blob_id: BlobId, code: StatusCode) {
        let (config, _handle) = start_rest_api_with_test_config().await;
        let client = storage_node_client(config.as_ref());

        let err = client
            .get_confirmation(&blob_id)
            .await
            .expect_err("confirmation request should fail");

        assert_eq!(err.http_status_code(), Some(code));
    }

    #[tokio::test]
    async fn shutdown_server() {
        let cancel_token = CancellationToken::new();
        let server = UserServer::new(Arc::new(MockServiceState), cancel_token.clone());
        let config = test_utils::storage_node_config();
        let handle = tokio::spawn(async move {
            let network_address = config.as_ref().rest_api_address;
            server.run(&network_address).await
        });

        cancel_token.cancel();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn get_decoding_symbol() {
        let (config, _handle) = start_rest_api_with_test_config().await;

        let blob_id = walrus_core::test_utils::random_blob_id();
        let sliver_pair_id = 0; // Triggers an valid response
        let index = 0;
        let path = RECOVERY_ENDPOINT
            .replace(":blobId", &blob_id.to_string())
            .replace(":sliverPairIdx", &sliver_pair_id.to_string())
            .replace(":sliverType", "primary")
            .replace(":targetPairIndex", &index.to_string());
        let url = format!("http://{}{path}", config.inner.rest_api_address);
        // TODO(lef): Extract the path creation into a function with optional arguments

        let client = storage_node_client(config.as_ref()).into_inner();
        let res = client.get(url).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let bytes = res
            .bytes()
            .await
            .expect("must be able to retrieve the body as binary data");

        let _symbol: DecodingSymbol<MerkleProof> =
            bcs::from_bytes(&bytes).expect("symbol should successfully decode");
    }

    #[tokio::test]
    async fn decoding_symbol_not_found() {
        let (config, _handle) = start_rest_api_with_test_config().await;

        let blob_id = walrus_core::test_utils::random_blob_id();

        let sliver_pair_id = 1; // Triggers a not found response
        let path = RECOVERY_ENDPOINT
            .replace(":blobId", &blob_id.to_string())
            .replace(":sliverPairIdx", &sliver_pair_id.to_string())
            .replace(":sliverType", "primary")
            .replace(":targetPairIndex", "0");
        let url = format!("http://{}{path}", config.inner.rest_api_address);
        let client = storage_node_client(config.as_ref()).into_inner();
        let res = client.get(url).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }
}
