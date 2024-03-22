// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{net::SocketAddr, sync::Arc};

use axum::{
    extract::{Path, State},
    http::StatusCode,
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
    SliverType,
};

use crate::node::{ServiceState, StoreMetadataError, StoreSliverError};

/// The path to get and store blob metadata.
pub const METADATA_ENDPOINT: &str = "/v1/blobs/:blobId/metadata";
/// The path to get and store slivers.
pub const SLIVER_ENDPOINT: &str = "/v1/blobs/:blobId/slivers/:sliverPairIdx/:sliverType";
/// The path to get storage confirmations.
pub const STORAGE_CONFIRMATION_ENDPOINT: &str = "/v1/blobs/:blobId/confirmation";

/// A blob ID encoded as a hex string designed to be used in URLs.
#[serde_as]
#[derive(Deserialize, Serialize)]
pub struct HexBlobId(#[serde_as(as = "DisplayFromStr")] BlobId);

impl From<HexBlobId> for BlobId {
    fn from(hex_blob_id: HexBlobId) -> Self {
        hex_blob_id.0
    }
}

/// Error message returned by the service.
#[derive(Serialize, Deserialize)]
pub enum ServiceResponse<T: Serialize> {
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

impl<T: Serialize> ServiceResponse<T> {
    /// Creates a new serialized success response. This response must be a tuple containing the
    /// status code (so that axum includes it in the HTTP header) and the JSON response.
    pub fn serialized_success(code: StatusCode, data: T) -> (StatusCode, Json<Self>) {
        let response = Self::Success {
            code: code.as_u16(),
            data,
        };
        (code, Json(response))
    }

    /// Creates a new serialized error response. This response must be a tuple containing the status
    /// code (so that axum includes it in the HTTP header) and the JSON response.
    pub fn serialized_error<S: Into<String>>(
        code: StatusCode,
        message: S,
    ) -> (StatusCode, Json<Self>) {
        let response = Self::Error {
            code: code.as_u16(),
            message: message.into(),
        };
        (code, Json(response))
    }

    /// Creates a new serialized bad request response for 'not found' errors.
    pub fn serialized_not_found() -> (StatusCode, Json<Self>) {
        Self::serialized_error(StatusCode::NOT_FOUND, "Not found")
    }
}

/// Represents a user server.
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
    pub async fn run(self, network_address: &SocketAddr) -> Result<(), std::io::Error> {
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
            .with_state(self.state.clone())
            .layer(TraceLayer::new_for_http());

        let listener = tokio::net::TcpListener::bind(network_address).await?;
        axum::serve(listener, app)
            .with_graceful_shutdown(self.cancel_token.cancelled_owned())
            .await
    }

    async fn retrieve_metadata(
        State(state): State<Arc<S>>,
        Path(hex_blob_id): Path<HexBlobId>,
    ) -> (
        StatusCode,
        Json<ServiceResponse<UnverifiedBlobMetadataWithId>>,
    ) {
        let blob_id = hex_blob_id.into();
        match state.retrieve_metadata(&blob_id) {
            Ok(Some(metadata)) => {
                tracing::debug!("Retrieved metadata for {blob_id:?}");
                ServiceResponse::serialized_success(StatusCode::OK, metadata)
            }
            Ok(None) => {
                tracing::debug!("Metadata not found for {blob_id:?}");
                ServiceResponse::serialized_not_found()
            }
            Err(message) => {
                tracing::error!("Internal server error: {message}");
                ServiceResponse::serialized_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal error",
                )
            }
        }
    }

    async fn store_metadata(
        State(state): State<Arc<S>>,
        Path(hex_blob_id): Path<HexBlobId>,
        Json(metadata): Json<BlobMetadata>,
    ) -> (StatusCode, Json<ServiceResponse<()>>) {
        let blob_id = hex_blob_id.into();
        let unverified_metadata_with_id = UnverifiedBlobMetadataWithId::new(blob_id, metadata);
        match state.store_metadata(unverified_metadata_with_id) {
            Ok(()) => {
                tracing::debug!("Stored metadata for {blob_id:?}");
                ServiceResponse::serialized_success(StatusCode::OK, ())
            }
            Err(StoreMetadataError::InvalidMetadata(message)) => {
                tracing::debug!("Received invalid metadata: {message}");
                ServiceResponse::serialized_error(StatusCode::BAD_REQUEST, message.to_string())
            }
            Err(StoreMetadataError::Internal(message)) => {
                tracing::error!("Internal server error: {message}");
                ServiceResponse::serialized_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal error",
                )
            }
        }
    }

    async fn retrieve_sliver(
        State(state): State<Arc<S>>,
        Path((hex_blob_id, sliver_pair_idx, sliver_type)): Path<(HexBlobId, u16, SliverType)>,
    ) -> (StatusCode, Json<ServiceResponse<Sliver>>) {
        let blob_id = hex_blob_id.into();
        match state.retrieve_sliver(&blob_id, sliver_pair_idx, sliver_type) {
            Ok(Some(sliver)) => {
                tracing::debug!("Retrieved {sliver_type:?} sliver for {blob_id:?}");
                ServiceResponse::serialized_success(StatusCode::OK, sliver)
            }
            Ok(None) => {
                tracing::debug!("{sliver_type:?} sliver not found for {blob_id:?}");
                ServiceResponse::serialized_not_found()
            }
            Err(message) => {
                tracing::error!("Internal server error: {message}");
                ServiceResponse::serialized_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal error",
                )
            }
        }
    }

    async fn store_sliver(
        State(state): State<Arc<S>>,
        Path((hex_blob_id, sliver_pair_idx, sliver_type)): Path<(HexBlobId, u16, SliverType)>,
        Json(sliver): Json<Sliver>,
    ) -> (StatusCode, Json<ServiceResponse<()>>) {
        if sliver.r#type() != sliver_type {
            return ServiceResponse::serialized_error(
                StatusCode::MISDIRECTED_REQUEST,
                "Invalid sliver type",
            );
        }

        let blob_id = hex_blob_id.into();
        match state.store_sliver(&blob_id, sliver_pair_idx, &sliver) {
            Ok(()) => {
                tracing::debug!("Stored {sliver_type:?} sliver for {blob_id:?}");
                ServiceResponse::serialized_success(StatusCode::OK, ())
            }
            Err(StoreSliverError::Internal(message)) => {
                tracing::error!("Internal server error: {message}");
                ServiceResponse::serialized_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal error",
                )
            }
            Err(client_error) => {
                tracing::debug!("Received invalid {sliver_type:?} sliver: {client_error}");
                ServiceResponse::serialized_error(StatusCode::BAD_REQUEST, client_error.to_string())
            }
        }
    }

    async fn retrieve_storage_confirmation(
        State(state): State<Arc<S>>,
        Path(hex_blob_id): Path<HexBlobId>,
    ) -> (StatusCode, Json<ServiceResponse<StorageConfirmation>>) {
        let blob_id = hex_blob_id.into();
        match state.compute_storage_confirmation(&blob_id).await {
            Ok(Some(confirmation)) => {
                tracing::debug!("Retrieved storage confirmation for {blob_id:?}");
                ServiceResponse::serialized_success(StatusCode::OK, confirmation)
            }
            Ok(None) => {
                tracing::debug!("Storage confirmation not found for {blob_id:?}");
                ServiceResponse::serialized_not_found()
            }
            Err(message) => {
                tracing::error!("Internal server error: {message}");
                ServiceResponse::serialized_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal error",
                )
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use anyhow::anyhow;
    use reqwest::StatusCode;
    use tokio_util::sync::CancellationToken;
    use walrus_core::{
        messages::StorageConfirmation,
        metadata::{UnverifiedBlobMetadataWithId, VerifiedBlobMetadataWithId},
        BlobId,
        Sliver,
        SliverType,
    };

    use crate::{
        node::{RetrieveSliverError, ServiceState, StoreMetadataError, StoreSliverError},
        server::{
            ServiceResponse,
            UserServer,
            METADATA_ENDPOINT,
            SLIVER_ENDPOINT,
            STORAGE_CONFIRMATION_ENDPOINT,
        },
        test_utils,
    };

    pub struct MockServiceState;

    impl ServiceState for MockServiceState {
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
            _sliver_pair_idx: u16,
            _sliver_type: SliverType,
        ) -> Result<Option<Sliver>, RetrieveSliverError> {
            Ok(Some(walrus_core::test_utils::sliver()))
        }

        fn store_sliver(
            &self,
            _blob_id: &BlobId,
            sliver_pair_idx: u16,
            _sliver: &Sliver,
        ) -> Result<(), StoreSliverError> {
            if sliver_pair_idx == 0 {
                Ok(())
            } else {
                Err(StoreSliverError::Internal(anyhow!("Invalid shard")))
            }
        }

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

    #[tokio::test]
    async fn retrieve_metadata() {
        let server = UserServer::new(Arc::new(MockServiceState), CancellationToken::new());
        let test_private_parameters = test_utils::storage_node_private_parameters();
        let _handle = tokio::spawn(async move {
            let network_address = test_private_parameters.network_address;
            server.run(&network_address).await
        });

        tokio::task::yield_now().await;

        let mut blob_id = walrus_core::test_utils::random_blob_id();
        blob_id.0[0] = 0; // Triggers a valid response
        let path = METADATA_ENDPOINT.replace(":blobId", &blob_id.to_string());
        let url = format!("http://{}{path}", test_private_parameters.network_address);

        let client = reqwest::Client::new();
        let res = client.get(url).json(&blob_id).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let body = res
            .json::<ServiceResponse<VerifiedBlobMetadataWithId>>()
            .await;
        match body.unwrap() {
            ServiceResponse::Success { code, data: _data } => {
                assert_eq!(code, StatusCode::OK.as_u16());
            }
            ServiceResponse::Error { code, message } => {
                panic!("Unexpected error response: {code} {message}");
            }
        }
    }

    #[tokio::test]
    async fn retrieve_metadata_not_found() {
        let server = UserServer::new(Arc::new(MockServiceState), CancellationToken::new());
        let test_private_parameters = test_utils::storage_node_private_parameters();
        let _handle = tokio::spawn(async move {
            let network_address = test_private_parameters.network_address;
            server.run(&network_address).await
        });

        tokio::task::yield_now().await;

        let mut blob_id = walrus_core::test_utils::random_blob_id();
        blob_id.0[0] = 1; // Triggers a not found response
        let path = METADATA_ENDPOINT.replace(":blobId", &blob_id.to_string());
        let url = format!("http://{}{path}", test_private_parameters.network_address);

        let client = reqwest::Client::new();
        let res = client.get(url).json(&blob_id).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn retrieve_metadata_internal_error() {
        let server = UserServer::new(Arc::new(MockServiceState), CancellationToken::new());
        let test_private_parameters = test_utils::storage_node_private_parameters();
        let _handle = tokio::spawn(async move {
            let network_address = test_private_parameters.network_address;
            server.run(&network_address).await
        });

        tokio::task::yield_now().await;

        let mut blob_id = walrus_core::test_utils::random_blob_id();
        blob_id.0[0] = 2; // Triggers an internal server error
        let path = METADATA_ENDPOINT.replace(":blobId", &blob_id.to_string());
        let url = format!("http://{}{path}", test_private_parameters.network_address);

        let client = reqwest::Client::new();
        let res = client.get(url).json(&blob_id).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn store_metadata() {
        let server = UserServer::new(Arc::new(MockServiceState), CancellationToken::new());
        let test_private_parameters = test_utils::storage_node_private_parameters();
        let _handle = tokio::spawn(async move {
            let network_address = test_private_parameters.network_address;
            server.run(&network_address).await
        });

        tokio::task::yield_now().await;

        let metadata_with_blob_id = walrus_core::test_utils::unverified_blob_metadata();
        let metadata = metadata_with_blob_id.metadata();

        let blob_id = metadata_with_blob_id.blob_id().to_string();
        let path = METADATA_ENDPOINT.replace(":blobId", &blob_id);
        let url = format!("http://{}{path}", test_private_parameters.network_address);

        let client = reqwest::Client::new();
        let res = client.put(url).json(metadata).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn retrieve_sliver() {
        let server = UserServer::new(Arc::new(MockServiceState), CancellationToken::new());
        let test_private_parameters = test_utils::storage_node_private_parameters();
        let _handle = tokio::spawn(async move {
            let network_address = test_private_parameters.network_address;
            server.run(&network_address).await
        });

        tokio::task::yield_now().await;

        let blob_id = walrus_core::test_utils::random_blob_id();
        let sliver_pair_id = 0; // Triggers an valid response
        let path = SLIVER_ENDPOINT
            .replace(":blobId", &blob_id.to_string())
            .replace(":sliverPairIdx", &sliver_pair_id.to_string())
            .replace(":sliverType", "primary");
        let url = format!("http://{}{path}", test_private_parameters.network_address);

        let client = reqwest::Client::new();
        let res = client.get(url).json(&blob_id).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let body = res.json::<ServiceResponse<Sliver>>().await;
        match body.unwrap() {
            ServiceResponse::Success { code, data: _data } => {
                assert_eq!(code, StatusCode::OK.as_u16());
            }
            ServiceResponse::Error { code, message } => {
                panic!("Unexpected error response: {code} {message}");
            }
        }
    }

    #[tokio::test]
    async fn store_sliver() {
        let server = UserServer::new(Arc::new(MockServiceState), CancellationToken::new());
        let test_private_parameters = test_utils::storage_node_private_parameters();
        let _handle = tokio::spawn(async move {
            let network_address = test_private_parameters.network_address;
            server.run(&network_address).await
        });

        tokio::task::yield_now().await;

        let sliver = walrus_core::test_utils::sliver();

        let blob_id = walrus_core::test_utils::random_blob_id();
        let sliver_pair_id = 0; // Triggers an ok response
        let path = SLIVER_ENDPOINT
            .replace(":blobId", &blob_id.to_string())
            .replace(":sliverPairIdx", &sliver_pair_id.to_string())
            .replace(":sliverType", "primary");
        let url = format!("http://{}{path}", test_private_parameters.network_address);

        let client = reqwest::Client::new();
        let res = client.put(url).json(&sliver).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn store_sliver_error() {
        let server = UserServer::new(Arc::new(MockServiceState), CancellationToken::new());
        let test_private_parameters = test_utils::storage_node_private_parameters();
        let _handle = tokio::spawn(async move {
            let network_address = test_private_parameters.network_address;
            server.run(&network_address).await
        });

        tokio::task::yield_now().await;

        let sliver = walrus_core::test_utils::sliver();

        let blob_id = walrus_core::test_utils::random_blob_id();
        let sliver_pair_id = 1; // Triggers an internal server error
        let path = SLIVER_ENDPOINT
            .replace(":blobId", &blob_id.to_string())
            .replace(":sliverPairIdx", &sliver_pair_id.to_string())
            .replace(":sliverType", "primary");
        let url = format!("http://{}{path}", test_private_parameters.network_address);

        let client = reqwest::Client::new();
        let res = client.put(url).json(&sliver).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn retrieve_storage_confirmation() {
        let server = UserServer::new(Arc::new(MockServiceState), CancellationToken::new());
        let test_private_parameters = test_utils::storage_node_private_parameters();
        let _handle = tokio::spawn(async move {
            let network_address = test_private_parameters.network_address;
            server.run(&network_address).await
        });

        tokio::task::yield_now().await;

        let mut blob_id = walrus_core::test_utils::random_blob_id();
        blob_id.0[0] = 0; // Triggers a valid response
        let path = STORAGE_CONFIRMATION_ENDPOINT.replace(":blobId", &blob_id.to_string());
        let url = format!("http://{}{path}", test_private_parameters.network_address);

        let client = reqwest::Client::new();
        let res = client.get(url).json(&blob_id).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let body = res.json::<ServiceResponse<StorageConfirmation>>().await;
        match body.unwrap() {
            ServiceResponse::Success { code, data } => {
                assert_eq!(code, StatusCode::OK.as_u16());
                assert!(matches!(data, StorageConfirmation::Signed(_)));
            }
            ServiceResponse::Error { code, message } => {
                panic!("Unexpected error response: {code} {message}");
            }
        }
    }

    #[tokio::test]
    async fn retrieve_storage_confirmation_not_found() {
        let server = UserServer::new(Arc::new(MockServiceState), CancellationToken::new());
        let test_private_parameters = test_utils::storage_node_private_parameters();
        let _handle = tokio::spawn(async move {
            let network_address = test_private_parameters.network_address;
            server.run(&network_address).await
        });

        tokio::task::yield_now().await;

        let mut blob_id = walrus_core::test_utils::random_blob_id();
        blob_id.0[0] = 1; // Triggers a not found response
        let path = STORAGE_CONFIRMATION_ENDPOINT.replace(":blobId", &blob_id.to_string());
        let url = format!("http://{}{path}", test_private_parameters.network_address);

        let client = reqwest::Client::new();
        let res = client.get(url).json(&blob_id).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn retrieve_storage_confirmation_internal_error() {
        let server = UserServer::new(Arc::new(MockServiceState), CancellationToken::new());
        let test_private_parameters = test_utils::storage_node_private_parameters();
        let _handle = tokio::spawn(async move {
            let network_address = test_private_parameters.network_address;
            server.run(&network_address).await
        });

        tokio::task::yield_now().await;

        let mut blob_id = walrus_core::test_utils::random_blob_id();
        blob_id.0[0] = 2; // Triggers an internal server error
        let path = STORAGE_CONFIRMATION_ENDPOINT.replace(":blobId", &blob_id.to_string());
        let url = format!("http://{}{path}", test_private_parameters.network_address);

        let client = reqwest::Client::new();
        let res = client.get(url).json(&blob_id).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn shutdown_server() {
        let cancel_token = CancellationToken::new();
        let server = UserServer::new(Arc::new(MockServiceState), cancel_token.clone());
        let test_private_parameters = test_utils::storage_node_private_parameters();
        let handle = tokio::spawn(async move {
            let network_address = test_private_parameters.network_address;
            server.run(&network_address).await
        });

        cancel_token.cancel();
        handle.await.unwrap().unwrap();
    }
}
