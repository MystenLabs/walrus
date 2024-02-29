use std::net::{IpAddr, Ipv4Addr};

use axum::{
    http::StatusCode,
    routing::{get, post},
    Json,
    Router,
};
use serde::{Deserialize, Serialize};
use walrus_core::{BlobId, ShardIndex};

use crate::{
    config::StorageNodePrivateParameters,
    storage::{Metadata, Storage},
};

/// Represents a storage metadata request.
#[derive(Serialize, Deserialize)]
pub struct StoreMetadataRequest {
    /// The blob identifier.
    blob_id: BlobId,
    /// The metadata of the blob.
    metadata: Metadata,
}

/// Represents a storage sliver request.
#[derive(Serialize, Deserialize)]
pub struct StoreSliverRequest {
    /// The blob identifier.
    blob_id: BlobId,
    /// The sliver to store.
    sliver: Vec<u8>,
}

/// Represents a storage confirmation request.
#[derive(Serialize, Deserialize)]
pub struct StorageConfirmationRequest {
    /// The blob identifier.
    blob_id: BlobId,
}

/// Represents a user server.
pub struct UserServer {
    /// The storage of the server.
    _storage: Storage,
}

impl UserServer {
    /// The path to the store metadata endpoint.
    pub const POST_STORE_METADATA: &'static str = "/storage/metadata";
    /// The path to the store sliver endpoint.
    pub const POST_STORE_SLIVER: &'static str = "/storage/sliver";
    /// The path to the storage confirmation endpoint.
    pub const GET_STORAGE_CONFIRMATION: &'static str = "/storage/confirmation";

    /// Creates a new user server.
    pub async fn run(
        &self,
        storage_node_private_parameters: &StorageNodePrivateParameters,
        shard_index: ShardIndex,
    ) -> Result<(), std::io::Error> {
        let app = Router::new()
            .route(Self::POST_STORE_METADATA, post(Self::store_metadata))
            .route(Self::POST_STORE_SLIVER, post(Self::store_sliver))
            .route(
                Self::GET_STORAGE_CONFIRMATION,
                get(Self::storage_confirmation),
            );

        let mut network_address = storage_node_private_parameters
            .network_address(shard_index)
            .expect("Shard index not found in network address");
        network_address.set_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        let listener = tokio::net::TcpListener::bind(network_address).await?;

        axum::serve(listener, app).await?;
        Ok(())
    }

    async fn store_metadata(Json(_request): Json<StoreMetadataRequest>) -> StatusCode {
        StatusCode::OK
    }

    async fn store_sliver(
        Json(_request): Json<StoreSliverRequest>,
    ) -> Result<Json<()>, StatusCode> {
        todo!()
    }

    async fn storage_confirmation(Json(_request): Json<StorageConfirmationRequest>) -> StatusCode {
        StatusCode::OK
    }
}

#[cfg(test)]
mod test {

    #[tokio::test]
    async fn test_server() {
        //todo!()
    }
}
