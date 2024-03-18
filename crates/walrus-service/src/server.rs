// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{net::SocketAddr, sync::Arc};

use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json,
    Router,
};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use walrus_core::{
    messages::StorageConfirmation,
    metadata::BlobMetadataWithId,
    BlobId,
    ShardIndex,
    Sliver,
};

use crate::node::{ServiceState, StoreMetadataError};

/// The path to the store metadata endpoint.
pub const POST_STORE_BLOB_METADATA: &str = "/storage/metadata";
/// The path to the store sliver endpoint.
pub const POST_STORE_SLIVER: &str = "/storage/sliver";
/// The path to the storage confirmation endpoint.
pub const GET_STORAGE_CONFIRMATION: &str = "/storage/confirmation";

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
}

/// Represents a storage sliver request.
#[derive(Serialize, Deserialize)]
pub struct StoreSliverRequest {
    /// The shard index.
    shard: ShardIndex,
    /// The blob identifier.
    blob_id: BlobId,
    /// The sliver to store.
    sliver: Sliver,
}

/// Represents a user server.
pub struct UserServer<S> {
    state: Arc<S>,
    shutdown_signal: oneshot::Receiver<()>,
}

impl<S: ServiceState + Send + Sync + 'static> UserServer<S> {
    /// Creates a new user server.
    pub fn new(state: Arc<S>, shutdown_signal: oneshot::Receiver<()>) -> Self {
        Self {
            state,
            shutdown_signal,
        }
    }

    /// Creates a new user server.
    pub async fn run(self, network_address: &SocketAddr) -> Result<(), std::io::Error> {
        let app = Router::new()
            .route(POST_STORE_BLOB_METADATA, post(Self::store_blob_metadata))
            .with_state(self.state.clone())
            .route(POST_STORE_SLIVER, post(Self::store_sliver))
            .with_state(self.state.clone())
            .route(GET_STORAGE_CONFIRMATION, get(Self::storage_confirmation))
            .with_state(self.state.clone());

        let listener = tokio::net::TcpListener::bind(network_address).await?;
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = self.shutdown_signal.await;
            })
            .await
    }

    async fn store_blob_metadata(
        State(state): State<Arc<S>>,
        Json(request): Json<BlobMetadataWithId>,
    ) -> (StatusCode, Json<ServiceResponse<()>>) {
        match state.store_blob_metadata(request) {
            Ok(()) => ServiceResponse::serialized_success(StatusCode::OK, ()),
            Err(StoreMetadataError::InvalidMetadata(message)) => {
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

    async fn store_sliver(
        State(state): State<Arc<S>>,
        Json(request): Json<StoreSliverRequest>,
    ) -> (StatusCode, Json<ServiceResponse<()>>) {
        match state.store_sliver(request.shard, &request.blob_id, &request.sliver) {
            Ok(()) => ServiceResponse::serialized_success(StatusCode::OK, ()),
            Err(message) => {
                tracing::error!("Internal server error: {message}");
                ServiceResponse::serialized_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal error",
                )
            }
        }
    }

    async fn storage_confirmation(
        State(state): State<Arc<S>>,
        Json(request): Json<BlobId>,
    ) -> (StatusCode, Json<ServiceResponse<StorageConfirmation>>) {
        match state.get_storage_confirmation(&request).await {
            Ok(Some(confirmation)) => {
                ServiceResponse::serialized_success(StatusCode::OK, confirmation)
            }
            Ok(None) => ServiceResponse::serialized_error(StatusCode::NOT_FOUND, "Not found"),
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

    use reqwest::StatusCode;
    use tokio::sync::oneshot;
    use walrus_core::{
        messages::StorageConfirmation,
        metadata::BlobMetadataWithId,
        test_utils as core_test_utils,
        BlobId,
        ShardIndex,
        Sliver,
    };

    use crate::{
        node::{ServiceState, StoreMetadataError},
        server::{
            ServiceResponse,
            StoreSliverRequest,
            UserServer,
            GET_STORAGE_CONFIRMATION,
            POST_STORE_BLOB_METADATA,
            POST_STORE_SLIVER,
        },
        test_utils,
    };

    pub struct MockServiceState;

    impl ServiceState for MockServiceState {
        fn store_blob_metadata(
            &self,
            _metadata: BlobMetadataWithId,
        ) -> Result<(), StoreMetadataError> {
            Ok(())
        }

        fn store_sliver(
            &self,
            shard: ShardIndex,
            _blob_id: &BlobId,
            _sliver: &Sliver,
        ) -> Result<(), anyhow::Error> {
            if shard == ShardIndex(0) {
                Ok(())
            } else {
                Err(anyhow::anyhow!("Invalid shard"))
            }
        }

        async fn get_storage_confirmation(
            &self,
            blob_id: &BlobId,
        ) -> Result<Option<StorageConfirmation>, anyhow::Error> {
            if blob_id.0[0] == 0 {
                let confirmation = core_test_utils::signed_storage_confirmation();
                Ok(Some(StorageConfirmation::Signed(confirmation)))
            } else if blob_id.0[0] == 1 {
                Ok(None)
            } else {
                Err(anyhow::anyhow!("Invalid shard"))
            }
        }
    }

    #[tokio::test]
    async fn store_blob_metadata() {
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = UserServer::new(Arc::new(MockServiceState), shutdown_rx);
        let test_private_parameters = test_utils::storage_node_private_parameters();
        let _handle = tokio::spawn(async move {
            let network_address = test_private_parameters.network_address;
            server.run(&network_address).await
        });

        tokio::task::yield_now().await;

        let client = reqwest::Client::new();
        let url = format!(
            "http://{}{}",
            test_private_parameters.network_address, POST_STORE_BLOB_METADATA
        );
        let metadata = core_test_utils::unverified_blob_metadata();

        let res = client.post(url).json(&metadata).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn store_sliver() {
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = UserServer::new(Arc::new(MockServiceState), shutdown_rx);
        let test_private_parameters = test_utils::storage_node_private_parameters();
        let _handle = tokio::spawn(async move {
            let network_address = test_private_parameters.network_address;
            server.run(&network_address).await
        });

        tokio::task::yield_now().await;

        let client = reqwest::Client::new();
        let url = format!(
            "http://{}{}",
            test_private_parameters.network_address, POST_STORE_SLIVER
        );
        let request = StoreSliverRequest {
            shard: ShardIndex(0), // Triggers a valid response
            blob_id: core_test_utils::random_blob_id(),
            sliver: core_test_utils::sliver(),
        };

        let res = client.post(url).json(&request).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn store_sliver_error() {
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = UserServer::new(Arc::new(MockServiceState), shutdown_rx);
        let test_private_parameters = test_utils::storage_node_private_parameters();
        let _handle = tokio::spawn(async move {
            let network_address = test_private_parameters.network_address;
            server.run(&network_address).await
        });
        tokio::task::yield_now().await;

        let client = reqwest::Client::new();
        let url = format!(
            "http://{}{}",
            test_private_parameters.network_address, POST_STORE_SLIVER
        );
        let request = StoreSliverRequest {
            shard: ShardIndex(1), // Triggers a 'shard not found' response
            blob_id: core_test_utils::random_blob_id(),
            sliver: core_test_utils::sliver(),
        };

        let res = client.post(url).json(&request).send().await.unwrap();
        println!("{:?}", res.status());
        assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn get_storage_confirmation() {
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = UserServer::new(Arc::new(MockServiceState), shutdown_rx);
        let test_private_parameters = test_utils::storage_node_private_parameters();
        let _handle = tokio::spawn(async move {
            let network_address = test_private_parameters.network_address;
            server.run(&network_address).await
        });

        tokio::task::yield_now().await;

        let client = reqwest::Client::new();
        let url = format!(
            "http://{}{}",
            test_private_parameters.network_address, GET_STORAGE_CONFIRMATION
        );
        let mut blob_id = core_test_utils::random_blob_id();
        blob_id.0[0] = 0; // Triggers a valid response

        let res = client.get(url).json(&blob_id).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let body = res.json::<ServiceResponse<StorageConfirmation>>().await;
        match body.unwrap() {
            ServiceResponse::Success { code, data } => {
                assert_eq!(code, StatusCode::OK.as_u16());
                assert!(matches!(data, StorageConfirmation::Signed(_)));
            }
            ServiceResponse::Error { code, message } => {
                panic!("Unexpected error response: {} {}", code, message);
            }
        }
    }

    #[tokio::test]
    async fn get_storage_confirmation_not_found() {
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = UserServer::new(Arc::new(MockServiceState), shutdown_rx);
        let test_private_parameters = test_utils::storage_node_private_parameters();
        let _handle = tokio::spawn(async move {
            let network_address = test_private_parameters.network_address;
            server.run(&network_address).await
        });

        tokio::task::yield_now().await;

        let client = reqwest::Client::new();
        let url = format!(
            "http://{}{}",
            test_private_parameters.network_address, GET_STORAGE_CONFIRMATION
        );
        let mut blob_id = core_test_utils::random_blob_id();
        blob_id.0[0] = 1; // Triggers not found response

        let res = client.get(url).json(&blob_id).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn get_storage_confirmation_error() {
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = UserServer::new(Arc::new(MockServiceState), shutdown_rx);
        let test_private_parameters = test_utils::storage_node_private_parameters();
        let _handle = tokio::spawn(async move {
            let network_address = test_private_parameters.network_address;
            server.run(&network_address).await
        });

        tokio::task::yield_now().await;

        let client = reqwest::Client::new();
        let url = format!(
            "http://{}{}",
            test_private_parameters.network_address, GET_STORAGE_CONFIRMATION
        );
        let mut blob_id = core_test_utils::random_blob_id();
        blob_id.0[0] = 2; // Triggers internal error response

        let res = client.get(url).json(&blob_id).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn shutdown_server() {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = UserServer::new(Arc::new(MockServiceState), shutdown_rx);
        let test_private_parameters = test_utils::storage_node_private_parameters();
        let handle = tokio::spawn(async move {
            let network_address = test_private_parameters.network_address;
            server.run(&network_address).await
        });

        drop(shutdown_tx);
        handle.await.unwrap().unwrap();
    }
}
