// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
};

use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use walrus_core::{
    messages::StorageConfirmation, metadata::BlobMetadataWithId, BlobId, ShardIndex, Sliver,
};

use crate::{
    config::StorageNodePrivateParameters,
    node::{ServiceState, StoreMetadataError},
};

/// The path to the store metadata endpoint.
pub const POST_STORE_BLOB_METADATA: &str = "/storage/metadata";
/// The path to the store sliver endpoint.
pub const POST_STORE_SLIVER: &str = "/storage/sliver";
/// The path to the storage confirmation endpoint.
pub const GET_STORAGE_CONFIRMATION: &str = "/storage/confirmation";

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
}

impl<S: ServiceState + Send + Sync + 'static> UserServer<S> {
    /// Creates a new user server.
    pub fn new(state: Arc<S>) -> Self {
        Self { state }
    }

    /// Creates a new user server.
    pub async fn run(
        &self,
        storage_node_private_parameters: &StorageNodePrivateParameters,
    ) -> JoinHandle<Result<(), std::io::Error>> {
        let app = Router::new()
            .route(POST_STORE_BLOB_METADATA, post(Self::store_blob_metadata))
            .with_state(self.state.clone())
            .route(POST_STORE_SLIVER, post(Self::store_sliver))
            .with_state(self.state.clone())
            .route(GET_STORAGE_CONFIRMATION, get(Self::storage_confirmation))
            .with_state(self.state.clone());

        let mut network_address = storage_node_private_parameters.network_address;
        network_address.set_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED));

        tokio::spawn(async move {
            let listener = tokio::net::TcpListener::bind(network_address).await?;
            axum::serve(listener, app).await
        })
    }

    async fn store_blob_metadata(
        State(state): State<Arc<S>>,
        Json(request): Json<BlobMetadataWithId>,
    ) -> (StatusCode, Result<(), String>) {
        match state.store_blob_metadata(request) {
            Ok(()) => (StatusCode::OK, Ok(())),
            Err(StoreMetadataError::InvalidMetadata(message)) => {
                (StatusCode::BAD_REQUEST, Err(message.to_string()))
            }
            Err(StoreMetadataError::Internal(message)) => {
                (StatusCode::INTERNAL_SERVER_ERROR, Err(message.to_string()))
            }
        }
    }

    async fn store_sliver(
        State(state): State<Arc<S>>,
        Json(request): Json<StoreSliverRequest>,
    ) -> (StatusCode, Result<(), String>) {
        match state.store_sliver(request.shard, &request.blob_id, &request.sliver) {
            Ok(()) => (StatusCode::OK, Ok(())),
            Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, Err(error.to_string())),
        }
    }

    async fn storage_confirmation(
        State(state): State<Arc<S>>,
        Json(request): Json<BlobId>,
    ) -> (StatusCode, Result<Json<StorageConfirmation>, String>) {
        match state.get_storage_confirmation(&request).await {
            Ok(Some(confirmation)) => (StatusCode::OK, Ok(Json(confirmation))),
            Ok(None) => (StatusCode::NOT_FOUND, Err("Not found".to_string())),
            Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, Err(error.to_string())),
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use reqwest::StatusCode;
    use walrus_core::{
        messages::{SignedStorageConfirmation, StorageConfirmation},
        metadata::{BlobMetadataWithId, UnverifiedBlobMetadataWithId},
        BlobId, ShardIndex, Sliver,
    };

    use crate::{
        config::StorageNodePrivateParameters,
        node::{ServiceState, StoreMetadataError},
        server::{
            StoreSliverRequest, UserServer, GET_STORAGE_CONFIRMATION, POST_STORE_BLOB_METADATA,
            POST_STORE_SLIVER,
        },
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
                let confirmation = SignedStorageConfirmation::arbitrary_for_test();
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
        let server = UserServer::new(Arc::new(MockServiceState));
        let test_private_parameters = StorageNodePrivateParameters::new_for_test();
        let _handle = server.run(&test_private_parameters).await;

        tokio::task::yield_now().await;

        let client = reqwest::Client::new();
        let url = format!(
            "http://{}{}",
            test_private_parameters.network_address, POST_STORE_BLOB_METADATA
        );
        let metadata = UnverifiedBlobMetadataWithId::arbitrary_metadata_for_test();

        let res = client.post(url).json(&metadata).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn store_sliver() {
        let server = UserServer::new(Arc::new(MockServiceState));
        let test_private_parameters = StorageNodePrivateParameters::new_for_test();
        let _handle = server.run(&test_private_parameters).await;

        tokio::task::yield_now().await;

        let client = reqwest::Client::new();
        let url = format!(
            "http://{}{}",
            test_private_parameters.network_address, POST_STORE_SLIVER
        );
        let request = StoreSliverRequest {
            shard: ShardIndex(0),
            blob_id: BlobId::arbitrary_for_test(),
            sliver: Sliver::arbitrary_for_test(),
        };

        let res = client.post(url).json(&request).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn store_sliver_error() {
        let server = UserServer::new(Arc::new(MockServiceState));
        let test_private_parameters = StorageNodePrivateParameters::new_for_test();
        let _handle = server.run(&test_private_parameters).await;

        tokio::task::yield_now().await;

        let client = reqwest::Client::new();
        let url = format!(
            "http://{}{}",
            test_private_parameters.network_address, POST_STORE_SLIVER
        );
        let request = StoreSliverRequest {
            shard: ShardIndex(1),
            blob_id: BlobId::arbitrary_for_test(),
            sliver: Sliver::arbitrary_for_test(),
        };

        let res = client.post(url).json(&request).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn get_storage_confirmation() {
        let server = UserServer::new(Arc::new(MockServiceState));
        let test_private_parameters = StorageNodePrivateParameters::new_for_test();
        let _handle = server.run(&test_private_parameters).await;

        tokio::task::yield_now().await;

        let client = reqwest::Client::new();
        let url = format!(
            "http://{}{}",
            test_private_parameters.network_address, GET_STORAGE_CONFIRMATION
        );
        let mut blob_id = BlobId::arbitrary_for_test();
        blob_id.0[0] = 0; // Triggers a valid response

        let res = client.get(url).json(&blob_id).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let confirmation = res.json::<StorageConfirmation>().await.unwrap();
        assert!(matches!(confirmation, StorageConfirmation::Signed(_)));
    }

    #[tokio::test]
    async fn get_storage_confirmation_not_found() {
        let server = UserServer::new(Arc::new(MockServiceState));
        let test_private_parameters = StorageNodePrivateParameters::new_for_test();
        let _handle = server.run(&test_private_parameters).await;

        tokio::task::yield_now().await;

        let client = reqwest::Client::new();
        let url = format!(
            "http://{}{}",
            test_private_parameters.network_address, GET_STORAGE_CONFIRMATION
        );
        let mut blob_id = BlobId::arbitrary_for_test();
        blob_id.0[0] = 1; // Triggers not found response

        let res = client.get(url).json(&blob_id).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn get_storage_confirmation_error() {
        let server = UserServer::new(Arc::new(MockServiceState));
        let test_private_parameters = StorageNodePrivateParameters::new_for_test();
        let _handle = server.run(&test_private_parameters).await;

        tokio::task::yield_now().await;

        let client = reqwest::Client::new();
        let url = format!(
            "http://{}{}",
            test_private_parameters.network_address, GET_STORAGE_CONFIRMATION
        );
        let mut blob_id = BlobId::arbitrary_for_test();
        blob_id.0[0] = 2; // Triggers internal error response

        let res = client.get(url).json(&blob_id).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
