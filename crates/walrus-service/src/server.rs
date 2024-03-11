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
use walrus_core::{
    messages::StorageConfirmation, metadata::BlobMetadataWithId, BlobId, ShardIndex, Sliver,
};

use crate::{config::StorageNodePrivateParameters, node::StoreMetadataError, StorageNode};

/// Represents a storage metadata request.
pub type StoreMetadataRequest = BlobMetadataWithId;

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

/// Represents a storage confirmation request.
pub type StorageConfirmationRequest = BlobId;

/// Represents a user server.
pub struct UserServer {
    node: Arc<StorageNode>,
}

impl UserServer {
    /// The path to the store metadata endpoint.
    pub const POST_STORE_BLOB_METADATA: &'static str = "/storage/metadata";
    /// The path to the store sliver endpoint.
    pub const POST_STORE_SLIVER: &'static str = "/storage/sliver";
    /// The path to the storage confirmation endpoint.
    pub const GET_STORAGE_CONFIRMATION: &'static str = "/storage/confirmation";

    /// Creates a new user server.
    pub async fn run(
        &self,
        storage_node_private_parameters: &StorageNodePrivateParameters,
    ) -> Result<(), std::io::Error> {
        let app = Router::new()
            .route(
                Self::POST_STORE_BLOB_METADATA,
                post(Self::store_blob_metadata),
            )
            .with_state(self.node.clone())
            .route(Self::POST_STORE_SLIVER, post(Self::store_sliver))
            .with_state(self.node.clone())
            .route(
                Self::GET_STORAGE_CONFIRMATION,
                get(Self::storage_confirmation),
            )
            .with_state(self.node.clone());

        let mut network_address = storage_node_private_parameters.network_address;
        network_address.set_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        let listener = tokio::net::TcpListener::bind(network_address).await?;

        axum::serve(listener, app).await?;
        Ok(())
    }

    async fn store_blob_metadata(
        State(node): State<Arc<StorageNode>>,
        Json(request): Json<StoreMetadataRequest>,
    ) -> (StatusCode, Result<(), String>) {
        match node.store_blob_metadata(request) {
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
        State(node): State<Arc<StorageNode>>,
        Json(request): Json<StoreSliverRequest>,
    ) -> (StatusCode, Result<(), String>) {
        match node.store_sliver(request.shard, &request.blob_id, &request.sliver) {
            Ok(()) => (StatusCode::OK, Ok(())),
            Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, Err(error.to_string())),
        }
    }

    async fn storage_confirmation(
        State(node): State<Arc<StorageNode>>,
        Json(request): Json<StorageConfirmationRequest>,
    ) -> (StatusCode, Result<Json<StorageConfirmation>, String>) {
        match node.get_storage_confirmation(&request).await {
            Ok(Some(confirmation)) => (StatusCode::OK, Ok(Json(confirmation))),
            Ok(None) => (StatusCode::NOT_FOUND, Err("Not found".to_string())),
            Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, Err(error.to_string())),
        }
    }
}

#[cfg(test)]
mod test {

    #[tokio::test]
    async fn test_server() {
        //todo!()
    }
}
