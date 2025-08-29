// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! HTTP routes for the Walrus Indexer (Octopus Index) Web API.
//!
//! This module provides REST endpoints for querying the Octopus Index according
//! to the design specification.

use std::{collections::HashMap, sync::Arc};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
};
use serde::{Deserialize, Serialize};
use sui_types::base_types::ObjectID;

use crate::{
    WalrusIndexer,
    storage::{BucketStats, PrimaryIndexValue, IndexTarget},
};

/// Octopus Index API endpoints
pub const GET_BLOB_ENDPOINT: &str = "/v1/blobs/{bucket_id}/{primary_key}";
pub const LIST_BUCKET_ENDPOINT: &str = "/v1/bucket/{bucket_id}";
pub const LIST_BUCKET_PREFIX_ENDPOINT: &str = "/v1/bucket/{bucket_id}/{prefix}";
pub const GET_BUCKET_STATS_ENDPOINT: &str = "/v1/bucket/{bucket_id}/stats";
pub const HEALTH_ENDPOINT: &str = "/v1/health";

/// Shared state for the indexer API
#[derive(Clone)]
pub struct IndexerState {
    pub indexer: Arc<WalrusIndexer>,
}

impl IndexerState {
    pub fn new(indexer: WalrusIndexer) -> Self {
        Self {
            indexer: Arc::new(indexer),
        }
    }
}

/// API response wrapper
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

/// Error type for indexer operations
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

/// Response for blob lookup by index
#[derive(Debug, Serialize)]
pub struct BlobByIndexResponse {
    pub target_id: String,
    pub target_type: String,
    pub secondary_indices: HashMap<String, Vec<String>>,
}

impl From<PrimaryIndexValue> for BlobByIndexResponse {
    fn from(value: PrimaryIndexValue) -> Self {
        let (target_id, target_type) = match value.target {
            IndexTarget::BlobId(blob_id) => (hex::encode(blob_id.0), "blob".to_string()),
            IndexTarget::QuiltPatchId(quilt_patch_id) => {
                (format!("{}:{}", hex::encode(quilt_patch_id.quilt_id.0), hex::encode(quilt_patch_id.patch_id_bytes)), "quilt_patch".to_string())
            }
        };
        
        Self {
            target_id,
            target_type,
            secondary_indices: value.secondary_indices,
        }
    }
}

/// Response for listing bucket contents
#[derive(Debug, Serialize)]
pub struct ListBucketResponse {
    pub entries: HashMap<String, BlobByIndexResponse>,
    pub total_count: usize,
}

/// Query parameters for pagination
#[derive(Debug, Deserialize)]
pub struct PaginationQuery {
    /// Maximum number of results to return
    pub limit: Option<usize>,
    /// Offset for pagination
    pub offset: Option<usize>,
}

/// Get blob by primary key
///
/// Endpoint: GET /v1/blobs/{bucket_id}/{primary_key}
///
/// This endpoint maps from a bucket_id/primary_key to the blob information.
pub async fn get_blob(
    State(state): State<IndexerState>,
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

/// List all blobs in a bucket with optional prefix filtering
///
/// Endpoint: GET /v1/bucket/{bucket_id}/{prefix}
///
/// Returns all blobs in the bucket that start with the given prefix.
/// Supports pagination via limit and offset parameters.
pub async fn list_bucket_with_prefix(
    State(state): State<IndexerState>,
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

/// List all entries in a bucket
///
/// Endpoint: GET /v1/bucket/{bucket_id}
///
/// Returns all primary index entries in the specified bucket.
/// Supports pagination via limit and offset parameters.
pub async fn list_bucket(
    State(state): State<IndexerState>,
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

/// Get bucket statistics
///
/// Endpoint: GET /v1/bucket/{bucket_id}/stats
///
/// Returns statistics about the bucket including entry counts.
pub async fn get_bucket_stats(
    State(state): State<IndexerState>,
    Path(bucket_id): Path<ObjectID>,
) -> Result<Json<ApiResponse<BucketStats>>, IndexerError> {
    let stats = state.indexer.get_bucket_stats(&bucket_id).await?;
    Ok(Json(ApiResponse::success(stats)))
}

/// Health check endpoint
///
/// Endpoint: GET /v1/health
///
/// Returns the health status of the indexer service.
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub uptime_seconds: u64,
}

pub async fn health_check(State(_state): State<IndexerState>) -> Json<ApiResponse<HealthResponse>> {
    let response = HealthResponse {
        status: "healthy".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_seconds: 0, // TODO: Track actual uptime
    };

    Json(ApiResponse::success(response))
}

/// Create the indexer router with all endpoints
pub fn create_indexer_router(state: IndexerState) -> axum::Router {
    axum::Router::new()
        .route(GET_BLOB_ENDPOINT, axum::routing::get(get_blob))
        .route(LIST_BUCKET_ENDPOINT, axum::routing::get(list_bucket))
        .route(
            LIST_BUCKET_PREFIX_ENDPOINT,
            axum::routing::get(list_bucket_with_prefix),
        )
        .route(
            GET_BUCKET_STATS_ENDPOINT,
            axum::routing::get(get_bucket_stats),
        )
        .route(HEALTH_ENDPOINT, axum::routing::get(health_check))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use walrus_core::{BlobId, QuiltPatchId};

    use super::*;
    use crate::{IndexerConfig, WalrusIndexer, IndexOperation, storage};

    async fn create_test_indexer() -> Result<WalrusIndexer, anyhow::Error> {
        let temp_dir = TempDir::new()?;
        let config = IndexerConfig {
            db_path: temp_dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        };

        WalrusIndexer::new(config).await
    }

    async fn setup_test_data(indexer: &WalrusIndexer) -> Result<ObjectID, anyhow::Error> {
        let bucket_id = ObjectID::from_hex_literal(
            "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
        ).unwrap();
        
        // Add a blob entry
        let blob_id = BlobId([42; 32]);
        indexer.process_operation(IndexOperation::IndexAdded {
            bucket_id,
            primary_key: "/files/document.pdf".to_string(),
            blob_id,
            secondary_indices: vec![
                ("type".to_string(), "pdf".to_string()),
                ("size".to_string(), "large".to_string()),
                ("category".to_string(), "work".to_string()),
            ],
        }).await?;

        // Add another blob entry
        let blob_id2 = BlobId([99; 32]);
        indexer.process_operation(IndexOperation::IndexAdded {
            bucket_id,
            primary_key: "/files/image.jpg".to_string(),
            blob_id: blob_id2,
            secondary_indices: vec![
                ("type".to_string(), "jpg".to_string()),
                ("size".to_string(), "small".to_string()),
                ("category".to_string(), "personal".to_string()),
            ],
        }).await?;

        // Add a quilt patch entry
        let quilt_id = BlobId([77; 32]);
        let quilt_patch_id = QuiltPatchId {
            quilt_id,
            patch_id_bytes: vec![1, 2, 3, 4],
        };
        indexer.storage().put_primary_index(
            &bucket_id,
            "/quilts/patch1",
            storage::IndexTarget::QuiltPatchId(quilt_patch_id),
            {
                let mut indices = HashMap::new();
                indices.insert("type".to_string(), vec!["quilt".to_string()]);
                indices
            },
        )?;

        Ok(bucket_id)
    }

    #[tokio::test]
    async fn test_health_endpoint() -> Result<(), anyhow::Error> {
        let indexer = create_test_indexer().await?;
        let state = IndexerState::new(indexer);
        
        // Call the handler directly
        let response = health_check(axum::extract::State(state)).await;
        
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
        let bucket_id = setup_test_data(&indexer).await?;
        let state = IndexerState::new(indexer);
        
        // Test getting an existing blob
        let response = get_blob(
            axum::extract::State(state.clone()),
            axum::extract::Path((bucket_id, "/files/document.pdf".to_string())),
        ).await;
        
        if let Ok(json_response) = response {
            let json_str = serde_json::to_string(&json_response.0)?;
            let json: serde_json::Value = serde_json::from_str(&json_str)?;
            
            assert_eq!(json["success"], true);
            assert_eq!(json["data"]["target_type"], "blob");
            assert!(json["data"]["target_id"].is_string());
            assert_eq!(json["data"]["secondary_indices"]["type"][0], "pdf");
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
    async fn test_get_quilt_patch_endpoint() -> Result<(), anyhow::Error> {
        let indexer = create_test_indexer().await?;
        let bucket_id = setup_test_data(&indexer).await?;
        let state = IndexerState::new(indexer);
        
        // Test getting a quilt patch
        let response = get_blob(
            axum::extract::State(state),
            axum::extract::Path((bucket_id, "/quilts/patch1".to_string())),
        ).await;
        
        if let Ok(json_response) = response {
            let json_str = serde_json::to_string(&json_response.0)?;
            let json: serde_json::Value = serde_json::from_str(&json_str)?;
            
            assert_eq!(json["success"], true);
            assert_eq!(json["data"]["target_type"], "quilt_patch");
            assert!(json["data"]["target_id"].is_string());
            assert!(json["data"]["target_id"].as_str().unwrap().contains(":"));
        } else {
            panic!("Expected Ok response");
        }
        
        Ok(())
    }

    #[tokio::test]
    async fn test_list_bucket_endpoint() -> Result<(), anyhow::Error> {
        let indexer = create_test_indexer().await?;
        let bucket_id = setup_test_data(&indexer).await?;
        let state = IndexerState::new(indexer);
        
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
            assert_eq!(json["data"]["total_count"], 3);
            assert_eq!(json["data"]["entries"].as_object().unwrap().len(), 3);
        } else {
            panic!("Expected Ok response");
        }
        
        // Test with pagination
        let response = list_bucket(
            axum::extract::State(state),
            axum::extract::Path(bucket_id),
            axum::extract::Query(PaginationQuery { limit: Some(2), offset: Some(1) }),
        ).await;
        
        if let Ok(json_response) = response {
            let json_str = serde_json::to_string(&json_response.0)?;
            let json: serde_json::Value = serde_json::from_str(&json_str)?;
            
            assert_eq!(json["data"]["total_count"], 3);
            assert_eq!(json["data"]["entries"].as_object().unwrap().len(), 2);
        } else {
            panic!("Expected Ok response");
        }
        
        Ok(())
    }

    #[tokio::test]
    async fn test_list_bucket_with_prefix_endpoint() -> Result<(), anyhow::Error> {
        let indexer = create_test_indexer().await?;
        let bucket_id = setup_test_data(&indexer).await?;
        let state = IndexerState::new(indexer);
        
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
            assert!(json["data"]["entries"].as_object().unwrap().contains_key("/files/document.pdf"));
            assert!(json["data"]["entries"].as_object().unwrap().contains_key("/files/image.jpg"));
        } else {
            panic!("Expected Ok response");
        }
        
        // List entries with prefix "/quilts"
        let response = list_bucket_with_prefix(
            axum::extract::State(state),
            axum::extract::Path((bucket_id, "/quilts".to_string())),
            axum::extract::Query(PaginationQuery { limit: None, offset: None }),
        ).await;
        
        if let Ok(json_response) = response {
            let json_str = serde_json::to_string(&json_response.0)?;
            let json: serde_json::Value = serde_json::from_str(&json_str)?;
            
            assert_eq!(json["data"]["total_count"], 1);
            assert!(json["data"]["entries"].as_object().unwrap().contains_key("/quilts/patch1"));
        } else {
            panic!("Expected Ok response");
        }
        
        Ok(())
    }

    #[tokio::test]
    async fn test_get_bucket_stats_endpoint() -> Result<(), anyhow::Error> {
        let indexer = create_test_indexer().await?;
        let bucket_id = setup_test_data(&indexer).await?;
        let state = IndexerState::new(indexer);
        
        let response = get_bucket_stats(
            axum::extract::State(state),
            axum::extract::Path(bucket_id),
        ).await;
        
        if let Ok(json_response) = response {
            let json_str = serde_json::to_string(&json_response.0)?;
            let json: serde_json::Value = serde_json::from_str(&json_str)?;
            
            assert_eq!(json["success"], true);
            assert_eq!(json["data"]["primary_count"], 3);
            assert!(json["data"]["secondary_count"].as_u64().unwrap() > 0);
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
