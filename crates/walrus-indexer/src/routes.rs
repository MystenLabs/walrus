// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! HTTP routes for the Walrus Indexer (Octopus Index) Web API.
//!
//! This module provides REST endpoints for querying the Octopus Index according
//! to the design specification.

use std::collections::HashMap;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
};
use serde::{Deserialize, Serialize};
use sui_types::base_types::ObjectID;

use crate::{
    storage::{BlobIdentity, BucketStats, PrimaryIndexValue},
    server::IndexerRestApiState,
};

/// Octopus Index API endpoints.
pub const GET_BLOB_ENDPOINT: &str = "/v1/blobs/{bucket_id}/{primary_key}";
pub const GET_BLOB_BY_OBJECT_ID_ENDPOINT: &str = "/v1/object/{object_id}";
pub const LIST_BUCKET_ENDPOINT: &str = "/v1/bucket/{bucket_id}";
pub const LIST_BUCKET_PREFIX_ENDPOINT: &str = "/v1/bucket/{bucket_id}/{prefix}";
pub const GET_BUCKET_STATS_ENDPOINT: &str = "/v1/bucket/{bucket_id}/stats";
pub const HEALTH_ENDPOINT: &str = "/v1/health";

// State is now provided by IndexerRestApiState in server.rs.

/// API response wrapper.
#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub message: String,
}

impl<T> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            message: "OK".to_string(),
        }
    }

    pub fn error(message: String) -> ApiResponse<()> {
        ApiResponse {
            success: false,
            data: None,
            message,
        }
    }
}

/// Error type for indexer operations.
#[derive(Debug, Serialize)]
pub struct IndexerError {
    pub error: String,
    pub details: Option<String>,
}

impl IndexerError {
    pub fn new(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            details: None,
        }
    }

    pub fn with_details(error: impl Into<String>, details: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            details: Some(details.into()),
        }
    }
}

impl axum::response::IntoResponse for IndexerError {
    fn into_response(self) -> axum::response::Response {
        let body = Json(ApiResponse::<()>::error(self.error));
        (StatusCode::INTERNAL_SERVER_ERROR, body).into_response()
    }
}

impl From<anyhow::Error> for IndexerError {
    fn from(err: anyhow::Error) -> Self {
        IndexerError::new(err.to_string())
    }
}

/// Response for blob lookup by index.
#[derive(Debug, Serialize)]
pub struct BlobByIndexResponse {
    pub blob_id: String,
    pub object_id: String,
}

impl From<PrimaryIndexValue> for BlobByIndexResponse {
    fn from(value: PrimaryIndexValue) -> Self {
        Self {
            blob_id: hex::encode(value.blob_identity.blob_id.0),
            object_id: value.blob_identity.object_id.to_string(),
        }
    }
}

impl From<BlobIdentity> for BlobByIndexResponse {
    fn from(value: BlobIdentity) -> Self {
        Self {
            blob_id: hex::encode(value.blob_id.0),
            object_id: value.object_id.to_string(),
        }
    }
}

/// Response for listing bucket contents.
#[derive(Debug, Serialize)]
pub struct ListBucketResponse {
    pub entries: HashMap<String, BlobByIndexResponse>,
    pub total_count: usize,
}

/// Query parameters for pagination.
#[derive(Debug, Deserialize)]
pub struct PaginationQuery {
    /// Maximum number of results to return.
    pub limit: Option<usize>,
    /// Offset for pagination.
    pub offset: Option<usize>,
}

/// Get blob by primary key.
///
/// Endpoint: GET /v1/blobs/{bucket_id}/{primary_key}.
///
/// This endpoint maps from a bucket_id/primary_key to the blob information.
pub async fn get_blob(
    State(state): State<IndexerRestApiState>,
    Path((bucket_id, primary_key)): Path<(ObjectID, String)>,
) -> Result<Json<ApiResponse<BlobByIndexResponse>>, IndexerError> {
    match state
        .indexer
        .get_blob_by_index(&bucket_id, &primary_key)
        .await?
    {
        Some(entry) => Ok(Json(ApiResponse::success(entry.into()))),
        None => {
            let error_response = ApiResponse::<BlobByIndexResponse> {
                success: false,
                data: None,
                message: "Blob not found".to_string(),
            };
            Ok(Json(error_response))
        }
    }
}

/// Get a blob by its object_id (implements read_blob_by_object_id from PDF).
///
/// Endpoint: GET /v1/object/{object_id}.
/// Returns: BlobByIndexResponse with blob_id and object_id.
pub async fn get_blob_by_object_id(
    State(state): State<IndexerRestApiState>,
    Path(object_id): Path<ObjectID>,
) -> Result<Json<ApiResponse<BlobByIndexResponse>>, IndexerError> {
    match state
        .indexer
        .get_blob_by_object_id(&object_id)
        .await?
    {
        Some(entry) => Ok(Json(ApiResponse::success(entry.into()))),
        None => {
            let error_response = ApiResponse::<BlobByIndexResponse> {
                success: false,
                data: None,
                message: "Blob not found".to_string(),
            };
            Ok(Json(error_response))
        }
    }
}

/// List all blobs in a bucket with optional prefix filtering.
///
/// Endpoint: GET /v1/bucket/{bucket_id}/{prefix}.
///
/// Returns all blobs in the bucket that start with the given prefix.
/// Supports pagination via limit and offset parameters.
pub async fn list_bucket_with_prefix(
    State(state): State<IndexerRestApiState>,
    Path((bucket_id, prefix)): Path<(ObjectID, String)>,
    Query(pagination): Query<PaginationQuery>,
) -> Result<Json<ApiResponse<ListBucketResponse>>, IndexerError> {
    let all_entries = state.indexer.list_bucket(&bucket_id).await?;

    // Filter by prefix
    let filtered_entries: HashMap<String, _> = all_entries
        .into_iter()
        .filter(|(key, _)| key.starts_with(&prefix))
        .collect();

    // Apply pagination
    let total_count = filtered_entries.len();
    let offset = pagination.offset.unwrap_or(0);
    let limit = pagination.limit.unwrap_or(100).min(1000); // Cap at 1000

    let paginated_entries: HashMap<String, BlobByIndexResponse> = filtered_entries
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|(key, value)| (key, value.into()))
        .collect();

    let response = ListBucketResponse {
        entries: paginated_entries,
        total_count,
    };

    Ok(Json(ApiResponse::success(response)))
}

/// List all entries in a bucket.
///
/// Endpoint: GET /v1/bucket/{bucket_id}.
///
/// Returns all primary index entries in the specified bucket.
/// Supports pagination via limit and offset parameters.
pub async fn list_bucket(
    State(state): State<IndexerRestApiState>,
    Path(bucket_id): Path<ObjectID>,
    Query(pagination): Query<PaginationQuery>,
) -> Result<Json<ApiResponse<ListBucketResponse>>, IndexerError> {
    let all_entries = state.indexer.list_bucket(&bucket_id).await?;

    // Apply pagination
    let total_count = all_entries.len();
    let offset = pagination.offset.unwrap_or(0);
    let limit = pagination.limit.unwrap_or(100).min(1000); // Cap at 1000

    let paginated_entries: HashMap<String, BlobByIndexResponse> = all_entries
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|(key, value)| (key, value.into()))
        .collect();

    let response = ListBucketResponse {
        entries: paginated_entries,
        total_count,
    };

    Ok(Json(ApiResponse::success(response)))
}

/// Get bucket statistics.
///
/// Endpoint: GET /v1/bucket/{bucket_id}/stats.
///
/// Returns statistics about the bucket including entry counts.
pub async fn get_bucket_stats(
    State(state): State<IndexerRestApiState>,
    Path(bucket_id): Path<ObjectID>,
) -> Result<Json<ApiResponse<BucketStats>>, IndexerError> {
    let stats = state.indexer.get_bucket_stats(&bucket_id).await?;
    Ok(Json(ApiResponse::success(stats)))
}

/// Health check endpoint.
///
/// Endpoint: GET /v1/health.
///
/// Returns the health status of the indexer service.
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub uptime_seconds: u64,
}

pub async fn health(
    State(_state): State<IndexerRestApiState>,
) -> Json<ApiResponse<HealthResponse>> {
    let response = HealthResponse {
        status: "healthy".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_seconds: 0, // TODO: Track actual uptime.
    };

    Json(ApiResponse::success(response))
}

/// Create the indexer router with all endpoints.
pub fn create_indexer_router(state: IndexerRestApiState) -> axum::Router {
    axum::Router::new()
        .route(GET_BLOB_ENDPOINT, axum::routing::get(get_blob))
        .route(GET_BLOB_BY_OBJECT_ID_ENDPOINT, axum::routing::get(get_blob_by_object_id))
        .route(LIST_BUCKET_ENDPOINT, axum::routing::get(list_bucket))
        .route(
            LIST_BUCKET_PREFIX_ENDPOINT,
            axum::routing::get(list_bucket_with_prefix),
        )
        .route(
            GET_BUCKET_STATS_ENDPOINT,
            axum::routing::get(get_bucket_stats),
        )
        .route(HEALTH_ENDPOINT, axum::routing::get(health))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use walrus_core::BlobId;

    use std::sync::Arc;
    use super::*;
    use crate::{IndexerConfig, WalrusIndexer};

    async fn create_test_indexer() -> Result<Arc<WalrusIndexer>, anyhow::Error> {
        let temp_dir = TempDir::new()?;
        let config = IndexerConfig {
            db_path: temp_dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        };

        WalrusIndexer::new(config).await
    }

    async fn setup_test_data(
        indexer: &Arc<WalrusIndexer>,
    ) -> Result<(ObjectID, ObjectID, ObjectID), anyhow::Error> {
        let bucket_id = ObjectID::from_hex_literal(
            "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
        ).unwrap();
        
        // Add a blob entry
        let blob_id = BlobId([42; 32]);
        let object_id1 = ObjectID::from_hex_literal(
            "0x1111111111111111111111111111111111111111111111111111111111111111",
        ).unwrap();
        indexer.storage.put_index_entry(&bucket_id, "/files/document.pdf", &object_id1, blob_id)
            .map_err(|e| anyhow::anyhow!("Failed to add index entry: {}", e))?;

        // Add another blob entry
        let blob_id2 = BlobId([99; 32]);
        let object_id2 = ObjectID::from_hex_literal(
            "0x2222222222222222222222222222222222222222222222222222222222222222",
        ).unwrap();
        indexer.storage.put_index_entry(&bucket_id, "/files/image.jpg", &object_id2, blob_id2)
            .map_err(|e| anyhow::anyhow!("Failed to add index entry: {}", e))?;

        Ok((bucket_id, object_id1, object_id2))
    }

    #[tokio::test]
    async fn test_health_endpoint() -> Result<(), anyhow::Error> {
        let indexer = create_test_indexer().await?;
        let state = IndexerRestApiState {
            indexer: indexer,
            config: std::sync::Arc::new(crate::server::IndexerRestApiServerConfig {
                bind_address: "127.0.0.1:8080".parse().unwrap(),
                metrics_address: "127.0.0.1:9184".parse().unwrap(),
                graceful_shutdown_period: None,
            }),
        };
        
        // Call the handler directly
        let response = health(axum::extract::State(state)).await;
        
        // Serialize and check the response
        let json_str = serde_json::to_string(&response.0)?;
        let json: serde_json::Value = serde_json::from_str(&json_str)?;
        
        assert_eq!(json["success"], true);
        assert_eq!(json["data"]["status"], "healthy");
        assert!(json["data"]["version"].is_string());
        
        Ok(())
    }

    #[tokio::test]
    async fn test_get_blob_endpoint() -> Result<(), anyhow::Error> {
        let indexer = create_test_indexer().await?;
        let (bucket_id, object_id1, _object_id2) = setup_test_data(&indexer).await?;
        let state = IndexerRestApiState {
            indexer: indexer,
            config: std::sync::Arc::new(crate::server::IndexerRestApiServerConfig {
                bind_address: "127.0.0.1:8080".parse().unwrap(),
                metrics_address: "127.0.0.1:9184".parse().unwrap(),
                graceful_shutdown_period: None,
            }),
        };
        
        // Test getting an existing blob
        let response = get_blob(
            axum::extract::State(state.clone()),
            axum::extract::Path((bucket_id, "/files/document.pdf".to_string())),
        ).await;
        
        if let Ok(json_response) = response {
            let json_str = serde_json::to_string(&json_response.0)?;
            let json: serde_json::Value = serde_json::from_str(&json_str)?;
            
            assert_eq!(json["success"], true);
            assert!(json["data"]["blob_id"].is_string());
            assert_eq!(json["data"]["object_id"], object_id1.to_string());
        } else {
            panic!("Expected Ok response");
        }
        
        // Test getting a non-existent blob
        let response = get_blob(
            axum::extract::State(state),
            axum::extract::Path((bucket_id, "nonexistent".to_string())),
        ).await;
        
        if let Ok(json_response) = response {
            let json_str = serde_json::to_string(&json_response.0)?;
            let json: serde_json::Value = serde_json::from_str(&json_str)?;
            
            assert_eq!(json["success"], false);
            assert_eq!(json["message"], "Blob not found");
        } else {
            panic!("Expected Ok response");
        }
        
        Ok(())
    }

    #[tokio::test]
    async fn test_get_blob_by_object_id_endpoint() -> Result<(), anyhow::Error> {
        let indexer = create_test_indexer().await?;
        let (_bucket_id, object_id1, _object_id2) = setup_test_data(&indexer).await?;
        let state = IndexerRestApiState {
            indexer: indexer,
            config: std::sync::Arc::new(crate::server::IndexerRestApiServerConfig {
                bind_address: "127.0.0.1:8080".parse().unwrap(),
                metrics_address: "127.0.0.1:9184".parse().unwrap(),
                graceful_shutdown_period: None,
            }),
        };
        
        // Test getting a blob by object_id
        let response = get_blob_by_object_id(
            axum::extract::State(state.clone()),
            axum::extract::Path(object_id1),
        ).await;
        
        if let Ok(json_response) = response {
            let json_str = serde_json::to_string(&json_response.0)?;
            let json: serde_json::Value = serde_json::from_str(&json_str)?;
            
            assert_eq!(json["success"], true);
            assert!(json["data"]["blob_id"].is_string());
            assert_eq!(json["data"]["object_id"], object_id1.to_string());
        } else {
            panic!("Expected Ok response");
        }
        
        // Test getting a non-existent object
        let non_existent_id = ObjectID::from_hex_literal(
            "0x9999999999999999999999999999999999999999999999999999999999999999",
        ).unwrap();
        let response = get_blob_by_object_id(
            axum::extract::State(state),
            axum::extract::Path(non_existent_id),
        ).await;
        
        if let Ok(json_response) = response {
            let json_str = serde_json::to_string(&json_response.0)?;
            let json: serde_json::Value = serde_json::from_str(&json_str)?;
            
            assert_eq!(json["success"], false);
            assert_eq!(json["message"], "Blob not found");
        } else {
            panic!("Expected Ok response");
        }
        
        Ok(())
    }

    #[tokio::test]
    async fn test_list_bucket_endpoint() -> Result<(), anyhow::Error> {
        let indexer = create_test_indexer().await?;
        let (bucket_id, _object_id1, _object_id2) = setup_test_data(&indexer).await?;
        let state = IndexerRestApiState {
            indexer: indexer,
            config: std::sync::Arc::new(crate::server::IndexerRestApiServerConfig {
                bind_address: "127.0.0.1:8080".parse().unwrap(),
                metrics_address: "127.0.0.1:9184".parse().unwrap(),
                graceful_shutdown_period: None,
            }),
        };
        
        // List all entries in bucket
        let response = list_bucket(
            axum::extract::State(state.clone()),
            axum::extract::Path(bucket_id),
            axum::extract::Query(PaginationQuery { limit: None, offset: None }),
        ).await;
        
        if let Ok(json_response) = response {
            let json_str = serde_json::to_string(&json_response.0)?;
            let json: serde_json::Value = serde_json::from_str(&json_str)?;
            
            assert_eq!(json["success"], true);
            assert_eq!(json["data"]["total_count"], 2);
            assert_eq!(json["data"]["entries"].as_object().unwrap().len(), 2);
        } else {
            panic!("Expected Ok response");
        }
        
        // Test with pagination
        let response = list_bucket(
            axum::extract::State(state),
            axum::extract::Path(bucket_id),
            axum::extract::Query(PaginationQuery { limit: Some(1), offset: Some(1) }),
        ).await;
        
        if let Ok(json_response) = response {
            let json_str = serde_json::to_string(&json_response.0)?;
            let json: serde_json::Value = serde_json::from_str(&json_str)?;
            
            assert_eq!(json["data"]["total_count"], 2);
            assert_eq!(json["data"]["entries"].as_object().unwrap().len(), 1);
        } else {
            panic!("Expected Ok response");
        }
        
        Ok(())
    }

    #[tokio::test]
    async fn test_list_bucket_with_prefix_endpoint() -> Result<(), anyhow::Error> {
        let indexer = create_test_indexer().await?;
        let (bucket_id, _object_id1, _object_id2) = setup_test_data(&indexer).await?;
        let state = IndexerRestApiState {
            indexer: indexer,
            config: std::sync::Arc::new(crate::server::IndexerRestApiServerConfig {
                bind_address: "127.0.0.1:8080".parse().unwrap(),
                metrics_address: "127.0.0.1:9184".parse().unwrap(),
                graceful_shutdown_period: None,
            }),
        };
        
        // List entries with prefix "/files"
        let response = list_bucket_with_prefix(
            axum::extract::State(state.clone()),
            axum::extract::Path((bucket_id, "/files".to_string())),
            axum::extract::Query(PaginationQuery { limit: None, offset: None }),
        ).await;
        
        if let Ok(json_response) = response {
            let json_str = serde_json::to_string(&json_response.0)?;
            let json: serde_json::Value = serde_json::from_str(&json_str)?;
            
            assert_eq!(json["success"], true);
            assert_eq!(json["data"]["total_count"], 2);
            assert!(json["data"]["entries"]
                .as_object()
                .unwrap()
                .contains_key("/files/document.pdf"));
            assert!(json["data"]["entries"].as_object().unwrap().contains_key("/files/image.jpg"));
        } else {
            panic!("Expected Ok response");
        }
        
        // List entries with prefix "/nonexistent"
        let response = list_bucket_with_prefix(
            axum::extract::State(state),
            axum::extract::Path((bucket_id, "/nonexistent".to_string())),
            axum::extract::Query(PaginationQuery { limit: None, offset: None }),
        ).await;
        
        if let Ok(json_response) = response {
            let json_str = serde_json::to_string(&json_response.0)?;
            let json: serde_json::Value = serde_json::from_str(&json_str)?;
            
            assert_eq!(json["data"]["total_count"], 0);
            assert_eq!(json["data"]["entries"].as_object().unwrap().len(), 0);
        } else {
            panic!("Expected Ok response");
        }
        
        Ok(())
    }

    #[tokio::test]
    async fn test_get_bucket_stats_endpoint() -> Result<(), anyhow::Error> {
        let indexer = create_test_indexer().await?;
        let (bucket_id, _object_id1, _object_id2) = setup_test_data(&indexer).await?;
        let state = IndexerRestApiState {
            indexer: indexer,
            config: std::sync::Arc::new(crate::server::IndexerRestApiServerConfig {
                bind_address: "127.0.0.1:8080".parse().unwrap(),
                metrics_address: "127.0.0.1:9184".parse().unwrap(),
                graceful_shutdown_period: None,
            }),
        };
        
        let response = get_bucket_stats(
            axum::extract::State(state),
            axum::extract::Path(bucket_id),
        ).await;
        
        if let Ok(json_response) = response {
            let json_str = serde_json::to_string(&json_response.0)?;
            let json: serde_json::Value = serde_json::from_str(&json_str)?;
            
            assert_eq!(json["success"], true);
            assert_eq!(json["data"]["primary_count"], 2);
            assert_eq!(json["data"]["secondary_count"], 0);
        } else {
            panic!("Expected Ok response");
        }
        
        Ok(())
    }

    #[tokio::test]
    async fn test_api_response_serialization() -> Result<(), anyhow::Error> {
        let response = ApiResponse::success(HealthResponse {
            status: "healthy".to_string(),
            version: "1.0.0".to_string(),
            uptime_seconds: 123,
        });

        let json = serde_json::to_string(&response)?;
        assert!(json.contains("\"success\":true"));
        assert!(json.contains("\"status\":\"healthy\""));

        Ok(())
    }

    #[tokio::test]
    async fn test_error_response() -> Result<(), anyhow::Error> {
        let error = IndexerError::with_details("Database error", "Connection failed");
        let response = ApiResponse::<()>::error(error.error);
        
        let json = serde_json::to_string(&response)?;
        assert!(json.contains("\"success\":false"));
        assert!(json.contains("Database error"));
        
        Ok(())
    }
}

// Handler function wrappers for the REST API server
// These wrap the actual implementations to provide consistent naming

pub async fn get_blob_handler(
    state: State<IndexerRestApiState>,
    params: Path<HashMap<String, String>>,
) -> Result<Json<ApiResponse<BlobByIndexResponse>>, StatusCode> {
    // Extract bucket_id and primary_key from params
    let bucket_id_str = params.get("bucket_id")
        .ok_or(StatusCode::BAD_REQUEST)?;
    let primary_key = params.get("primary_key")
        .ok_or(StatusCode::BAD_REQUEST)?;
    
    // Parse bucket_id
    let bucket_id = ObjectID::from_hex_literal(bucket_id_str)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    
    // Call the actual implementation
    get_blob(state, Path((bucket_id, primary_key.clone()))).await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

pub async fn get_blob_by_object_id_handler(
    state: State<IndexerRestApiState>,
    object_id_str: Path<String>,
) -> Result<Json<ApiResponse<BlobByIndexResponse>>, StatusCode> {
    // Parse object_id
    let object_id = ObjectID::from_hex_literal(&object_id_str)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    
    get_blob_by_object_id(state, Path(object_id)).await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

pub async fn list_bucket_handler(
    state: State<IndexerRestApiState>,
    bucket_id_str: Path<String>,
    params: Query<HashMap<String, String>>,
) -> Result<Json<ApiResponse<ListBucketResponse>>, StatusCode> {
    // Parse bucket_id
    let bucket_id = ObjectID::from_hex_literal(&bucket_id_str)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    
    // Extract pagination params
    let limit = params.get("limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);
    let offset = params.get("offset")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    
    let pagination = PaginationQuery { 
        limit: Some(limit), 
        offset: Some(offset) 
    };
    
    list_bucket(state, Path(bucket_id), Query(pagination)).await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

pub async fn get_bucket_stats_handler(
    state: State<IndexerRestApiState>,
    bucket_id_str: Path<String>,
) -> Result<Json<ApiResponse<BucketStats>>, StatusCode> {
    // Parse bucket_id
    let bucket_id = ObjectID::from_hex_literal(&bucket_id_str)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    
    get_bucket_stats(state, Path(bucket_id)).await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

pub async fn list_all_buckets_handler(
    State(_state): State<IndexerRestApiState>,
) -> Result<Json<ApiResponse<Vec<String>>>, StatusCode> {
    // TODO: Implement listing all buckets
    Ok(Json(ApiResponse {
        success: true,
        data: Some(vec![]),
        message: "OK".to_string(),
    }))
}

pub async fn create_bucket_handler(
    State(state): State<IndexerRestApiState>,
    Json(bucket): Json<crate::Bucket>,
) -> Result<Json<ApiResponse<()>>, StatusCode> {
    match state.indexer.create_bucket(bucket).await {
        Ok(_) => Ok(Json(ApiResponse {
            success: true,
            data: Some(()),
            message: "Bucket created successfully".to_string(),
        })),
        Err(e) => Ok(Json(ApiResponse {
            success: false,
            data: None,
            message: format!("Failed to create bucket: {}", e),
        })),
    }
}
