// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Walrus Indexer Library
//!
//! This crate provides indexing functionality for Walrus blobs, implementing
//! an inverted index mapping from user-specified names to blob-id/patch-id.
//!
//! # Example Usage
//!
//! ```no_run
//! use walrus_indexer::{IndexerConfig, WalrusIndexer};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create configuration
//!     let config = IndexerConfig {
//!         db_path: "./indexer-db".into(),
//!         ..Default::default()
//!     };
//!
//!     // Create the indexer
//!     let indexer = WalrusIndexer::new(config).await?;
//!
//!     // The indexer is now running and processing Sui events in the background
//!     // You can use it to query data:
//!     // let entry = indexer.get_blob_from_bucket(&bucket_id, "/path/to/file").await?;
//!
//!     Ok(())
//! }
//! ```

// Module declarations
pub mod checkpoint_downloader;
pub mod indexer;
pub mod storage;

// Configuration modules
use std::{net::SocketAddr, path::PathBuf};

// Re-export main types for convenience
pub use indexer::WalrusIndexer;
use serde::{Deserialize, Serialize};
pub use storage::{BlobIdentity, BucketStats, WalrusIndexStore};
use sui_types::base_types::ObjectID;

/// Configuration for indexer event processor functionality.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexerEventProcessorConfig {
    /// Event processor configuration.
    pub event_processor_config:
        walrus_service::event::event_processor::config::EventProcessorConfig,

    /// Sui configuration for event processing.
    pub sui_config: walrus_service::common::config::SuiConfig,
}

/// Configuration for the indexer.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexerConfig {
    /// Path to the database storing the indexer data.
    #[serde(default = "default::db_path")]
    pub db_path: PathBuf,

    /// Socket address on which the Prometheus server should export its metrics.
    #[serde(default = "default::metrics_address")]
    pub metrics_address: SocketAddr,

    /// Optional configuration for event processing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_processor_config: Option<IndexerEventProcessorConfig>,
}

impl Default for IndexerConfig {
    fn default() -> Self {
        Self {
            db_path: default::db_path(),
            metrics_address: default::metrics_address(),
            event_processor_config: None,
        }
    }
}

/// Represents a bucket for index entries (matches design spec).
// TODO(blob_manager): What type should be used here?
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Bucket {
    /// Bucket ID as Sui object.
    pub bucket_id: ObjectID,

    /// Bucket name for user reference.
    pub name: String,

    /// Secondary index definitions.
    pub secondary_indices: Vec<String>,
}

/// Default configuration values for the indexer.
pub mod default {
    use std::{
        net::{Ipv4Addr, SocketAddr},
        path::PathBuf,
    };

    /// Default database path.
    pub fn db_path() -> PathBuf {
        PathBuf::from("opt/walrus/db/indexer-db")
    }

    /// Default metrics address.
    pub fn metrics_address() -> SocketAddr {
        (Ipv4Addr::LOCALHOST, 9186).into()
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn test_walrus_index_workflow() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().to_path_buf();

        // Create test data references.
        let bucket1_id = ObjectID::from_hex_literal(
            "0x42a8f3dc1234567890abcdef1234567890abcdef1234567890abcdef12345678",
        )
        .unwrap();
        let bucket2_id = ObjectID::from_hex_literal(
            "0x52a8f3dc1234567890abcdef1234567890abcdef1234567890abcdef12345679",
        )
        .unwrap();

        // Test data for bucket 1 (photos).
        let photos_data = vec![
            (
                "/photos/2024/sunset.jpg",
                [1; 32],
                "0xf3eda3f4deb7618d0fab06f7e90755afeabbeb8b33106f43c7f2d25c6ef6a3b3",
            ),
            (
                "/photos/2024/beach.jpg",
                [2; 32],
                "0xa1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890",
            ),
            (
                "/photos/2023/mountain.png",
                [3; 32],
                "0xb2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890ab",
            ),
            (
                "/photos/2023/forest.png",
                [4; 32],
                "0xc3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890abcd",
            ),
        ];

        // Test data for bucket 2 (documents).
        let docs_data = vec![
            (
                "/docs/reports/2024/q1.pdf",
                [5; 32],
                "0xd4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            ),
            (
                "/docs/reports/2024/q2.pdf",
                [6; 32],
                "0xe5f67890abcdef1234567890abcdef1234567890abcdef1234567890abcdef12",
            ),
            (
                "/docs/contracts/agreement.doc",
                [7; 32],
                "0xf67890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234",
            ),
            (
                "/docs/manuals/user-guide.txt",
                [8; 32],
                "0x67890abcdef1234567890abcdef1234567890abcdef1234567890abcdef12345",
            ),
        ];

        // Phase 1: Create indexer and write data.
        {
            let config = IndexerConfig {
                db_path: db_path.clone(),
                ..Default::default()
            };

            let indexer = WalrusIndexer::new(config).await?;

            // Create bucket 1.
            let bucket1 = Bucket {
                bucket_id: bucket1_id,
                name: "test-photos".to_string(),
                secondary_indices: vec![],
            };
            indexer.create_bucket(bucket1).await?;

            // Create bucket 2.
            let bucket2 = Bucket {
                bucket_id: bucket2_id,
                name: "test-documents".to_string(),
                secondary_indices: vec![],
            };
            indexer.create_bucket(bucket2).await?;

            // Add photos to bucket 1.
            for (path, blob_bytes, obj_id_hex) in &photos_data {
                let blob_id = walrus_core::BlobId(*blob_bytes);
                let object_id = ObjectID::from_hex_literal(obj_id_hex).unwrap();
                indexer
                    .storage
                    .put_index_entry(&bucket1_id, path, &object_id, blob_id)
                    .map_err(|e| anyhow::anyhow!("Failed to add photo entry: {}", e))?;
            }

            // Add documents to bucket 2.
            for (path, blob_bytes, obj_id_hex) in &docs_data {
                let blob_id = walrus_core::BlobId(*blob_bytes);
                let object_id = ObjectID::from_hex_literal(obj_id_hex).unwrap();
                indexer
                    .storage
                    .put_index_entry(&bucket2_id, path, &object_id, blob_id)
                    .map_err(|e| anyhow::anyhow!("Failed to add doc entry: {}", e))?;
            }

            // Verify bucket 1 stats.
            let stats1 = indexer.get_bucket_stats(&bucket1_id).await?;
            assert_eq!(stats1.primary_count, 4);
            assert_eq!(stats1.secondary_count, 0);

            // Verify bucket 2 stats.
            let stats2 = indexer.get_bucket_stats(&bucket2_id).await?;
            assert_eq!(stats2.primary_count, 4);
            assert_eq!(stats2.secondary_count, 0);

            // List all entries in bucket 1.
            let bucket1_entries = indexer.list_blobs_in_bucket(&bucket1_id).await?;
            assert_eq!(bucket1_entries.len(), 4);

            // List all entries in bucket 2.
            let bucket2_entries = indexer.list_blobs_in_bucket(&bucket2_id).await?;
            assert_eq!(bucket2_entries.len(), 4);

            // Remove one entry from each bucket to test deletion.
            indexer
                .storage
                .delete_by_bucket_identifier(&bucket1_id, "/photos/2023/forest.png")
                .map_err(|e| anyhow::anyhow!("Failed to remove photo entry: {}", e))?;

            indexer
                .storage
                .delete_by_bucket_identifier(&bucket2_id, "/docs/manuals/user-guide.txt")
                .map_err(|e| anyhow::anyhow!("Failed to remove doc entry: {}", e))?;
            // Explicitly stop the indexer to ensure clean shutdown.
            indexer.stop().await;
        }
        // Indexer is dropped here, which should close RocksDB.

        // Add a small delay to ensure RocksDB fully releases the lock.
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Phase 2: Reopen indexer and verify data persistence.
        {
            let config = IndexerConfig {
                db_path: db_path.clone(),
                ..Default::default()
            };

            let indexer = WalrusIndexer::new(config).await?;

            // Verify bucket 1 data after reopening.
            let stats1 = indexer.get_bucket_stats(&bucket1_id).await?;
            assert_eq!(stats1.primary_count, 3); // One was deleted.

            // Verify specific entries in bucket 1.
            for (i, (path, blob_bytes, obj_id_hex)) in photos_data.iter().enumerate() {
                if i == 3 {
                    continue;
                } // Skip the deleted entry.

                let entry = indexer.get_blob_from_bucket(&bucket1_id, path).await?;
                assert!(entry.is_some(), "Entry {} should exist", path);
                let retrieved = entry.unwrap();
                assert_eq!(retrieved.blob_id, walrus_core::BlobId(*blob_bytes));
                assert_eq!(
                    retrieved.object_id,
                    ObjectID::from_hex_literal(obj_id_hex).unwrap()
                );
            }

            // Verify deleted entry is gone.
            let deleted_entry = indexer
                .get_blob_from_bucket(&bucket1_id, "/photos/2023/forest.png")
                .await?;
            assert!(deleted_entry.is_none());

            // Verify bucket 2 data after reopening.
            let stats2 = indexer.get_bucket_stats(&bucket2_id).await?;
            assert_eq!(stats2.primary_count, 3); // One was deleted.

            // Test query by object ID.
            let obj_id = ObjectID::from_hex_literal(docs_data[0].2).unwrap();
            let entry_by_obj = indexer.get_blob_by_object_id(&obj_id).await?;
            assert!(entry_by_obj.is_some());
            let retrieved = entry_by_obj.unwrap();
            assert_eq!(retrieved.blob_id, walrus_core::BlobId(docs_data[0].1));

            // Final cleanup: remove both buckets.
            indexer.remove_bucket(&bucket1_id).await?;
            indexer.remove_bucket(&bucket2_id).await?;

            // Verify buckets are gone by checking stats return 0 entries.
            let stats1_after = indexer.get_bucket_stats(&bucket1_id).await?;
            assert_eq!(stats1_after.primary_count, 0);

            let stats2_after = indexer.get_bucket_stats(&bucket2_id).await?;
            assert_eq!(stats2_after.primary_count, 0);
        }

        Ok(())
    }
}
