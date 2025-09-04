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
//!         db_path: "./indexer-db".to_string(),
//!         walrus_indexer_config: None,
//!     };
//!
//!     // Create the indexer
//!     let indexer = WalrusIndexer::new(config).await?;
//!
//!     // The indexer is now running and processing Sui events in the background
//!     // You can use it to query data:
//!     // let entry = indexer.get_blob_by_index(&bucket_id, "/path/to/file").await?;
//!
//!     Ok(())
//! }
//! ```

pub mod checkpoint_downloader;
pub mod storage;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sui_types::base_types::ObjectID;
use tokio::select;
use tokio::sync::RwLock;
use tracing::{info, warn};
use walrus_sui::types::ContractEvent;

use walrus_service::{
    event::{
        event_processor::processor::EventProcessor,
        events::EventStreamElement,
    },
    node::system_events::SystemEventProvider,
};

use self::storage::BlobIdentity;

/// Default configuration values for the indexer.
pub mod default {
    /// Default database path.
    pub fn db_path() -> String {
        "opt/walrus/db/indexer-db".to_string()
    }
}

/// Configuration for Walrus-specific indexer functionality.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WalrusIndexerConfig {
    /// Event processor configuration.
    pub event_processor_config: walrus_service::event::event_processor::config::EventProcessorConfig,
    
    /// Sui configuration for event processing and blockchain interaction.
    pub sui_config: walrus_service::common::config::SuiConfig,
}

/// Configuration for the indexer.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexerConfig {
    /// Path to the database directory.
    #[serde(default = "default::db_path")]
    pub db_path: String,

    /// Optional Walrus-specific configuration for event processing and Sui integration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub walrus_indexer_config: Option<WalrusIndexerConfig>,
}

impl Default for IndexerConfig {
    fn default() -> Self {
        Self {
            db_path: default::db_path(),
            walrus_indexer_config: None,
        }
    }
}

/// Represents a bucket for index entries (matches design spec).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Bucket {
    /// Bucket ID as Sui object.
    pub bucket_id: ObjectID,

    /// Bucket name for user reference.
    pub name: String,

    /// Secondary index definitions.
    pub secondary_indices: Vec<String>,
}

/// The main Walrus Indexer interface (Octopus Index).
#[derive(Clone)]
pub struct WalrusIndexer {
    config: IndexerConfig,

    /// Storage layer for index data.
    pub storage: Arc<storage::OctopusIndexStore>,

    /// Event processor for pulling events from Sui (if configured).
    event_processor: Arc<RwLock<Option<Arc<EventProcessor>>>>,

    /// Cancellation token for graceful shutdown.
    cancellation_token: tokio_util::sync::CancellationToken,
}

impl WalrusIndexer {
    /// Create a new indexer instance.
    /// This creates a simple key-value store with indexing logic.
    /// The indexer will not start any background services until run() is called.
    pub async fn new(config: IndexerConfig) -> Result<Arc<Self>> {
        // Create storage layer by opening the database
        let storage = Arc::new(storage::OctopusIndexStore::open(&config.db_path).await?);

        Ok(Arc::new(Self {
            config,
            storage,
            event_processor: Arc::new(RwLock::new(None)),
            cancellation_token: tokio_util::sync::CancellationToken::new(),
        }))
    }


    /// Run the indexer as a full-featured service.
    /// This method:
    /// 1. Initializes the event processor (if configured).
    /// 2. Processes events from Sui blockchain (if configured).
    /// 
    /// When run() is not called, the Indexer acts as a simple key-value store.
    /// and relies on the caller to use apply_mutations() for updates.
    pub async fn run(
        self: Arc<Self>,
        cancel_token: tokio_util::sync::CancellationToken,
    ) -> Result<()> {
        // Initialize event processor if configured.
        self.start_event_processor().await?;
        
        // Get event processor for the main event loop
        let event_processor = self.event_processor.read().await.clone();
        
        select! {
            _ = cancel_token.cancelled() => {
                info!("Indexer received shutdown signal");
            }
            
            res = async {
                if let Some(processor) = event_processor {
                    self.process_events(processor).await
                } else {
                    // No event processor, just wait for cancellation
                    std::future::pending::<Result<()>>().await
                }
            } => {
                if let Err(e) = res {
                    warn!("Event processing error: {}", e);
                }
            }
        }
        
        info!("Indexer shutdown complete");
        Ok(())
    }
    
    /// Start the event processor if configured.
    async fn start_event_processor(&self) -> Result<()> {
        if let Some(ref walrus_config) = self.config.walrus_indexer_config {
            use walrus_service::event::event_processor::runtime::EventProcessorRuntime;
            use walrus_utils::metrics::Registry;
            use walrus_service::node::DatabaseConfig;
            use walrus_service::common::config::SuiReaderConfig;
            
            info!("Creating indexer event processor using EventProcessorRuntime");
            
            // Create metrics registry for event processor
            let prometheus_registry = prometheus::Registry::new();
            let metrics_registry = Registry::new(prometheus_registry);
            
            // Create cancellation token for event processor
            let event_cancel_token = self.cancellation_token.child_token();
            
            // Convert SuiConfig to SuiReaderConfig for the event processor
            let sui_reader_config: SuiReaderConfig = (&walrus_config.sui_config).into();
            
            // Use EventProcessorRuntime to create the event processor
            let event_processor = EventProcessorRuntime::start_async(
                sui_reader_config,
                walrus_config.event_processor_config.clone(),
                &std::path::PathBuf::from(&self.config.db_path).join("event_processor"),
                &metrics_registry,
                event_cancel_token,
                &DatabaseConfig::default(),
            ).await?;
            
            *self.event_processor.write().await = Some(event_processor);
        }
        Ok(())
    }

    /// Process a contract event from Sui.
    pub async fn process_event(&self, event: ContractEvent) -> Result<()> {
        // Convert ContractEvent to IndexOperation based on event type
        // This is a simplified conversion - in reality you'd parse the actual event data
        // For now, we'll just return Ok since ContractEvent structure varies
        // In production, you'd parse the actual event fields to create proper IndexOperation
        tracing::info!(?event, "Processing contract event in indexer");
        Ok(())
    }
    

    /// Get blob identity by bucket_id and identifier.
    pub async fn get_blob_by_index(
        &self,
        bucket_id: &ObjectID,
        identifier: &str,
    ) -> Result<Option<BlobIdentity>> {
        self.storage
            .get_by_bucket_identifier(bucket_id, identifier)
            .map_err(|e| anyhow::anyhow!("Failed to get blob by bucket identifier: {}", e))
    }

    /// Get index entry by object_id (implements read_blob_by_object_id from PDF).
    pub async fn get_blob_by_object_id(
        &self,
        object_id: &ObjectID,
    ) -> Result<Option<BlobIdentity>> {
        self.storage
            .get_by_object_id(object_id)
            .map_err(|e| anyhow::anyhow!("Failed to get blob by object_id: {}", e))
    }

    /// List all entries in a bucket.
    pub async fn list_bucket(
        &self,
        bucket_id: &ObjectID,
    ) -> Result<HashMap<String, BlobIdentity>> {
        let storage_result = self.storage
            .list_bucket_entries(bucket_id)
            .map_err(|e| anyhow::anyhow!("Failed to list bucket entries: {}", e))?;
            
        // Return the HashMap<String, BlobIdentity> directly
        Ok(storage_result)
    }

    /// Create a new bucket.
    pub async fn create_bucket(&self, bucket: Bucket) -> Result<()> {
        // In a real implementation, this would interact with Sui to create the bucket object
        // For now, we just validate the bucket can be used
        println!(
            "Creating bucket: {} with ID: {}",
            bucket.name, bucket.bucket_id
        );
        Ok(())
    }

    /// Remove a bucket and all its entries.
    pub async fn remove_bucket(&self, bucket_id: &ObjectID) -> Result<()> {
        self.storage
            .delete_bucket(bucket_id)
            .map_err(|e| anyhow::anyhow!("Failed to delete bucket: {}", e))
    }

    /// Get storage layer for direct operations.
    pub fn storage(&self) -> &Arc<storage::OctopusIndexStore> {
        &self.storage
    }

    /// Get statistics for a bucket.
    pub async fn get_bucket_stats(&self, bucket_id: &ObjectID) -> Result<storage::BucketStats> {
        self.storage
            .get_bucket_stats(bucket_id)
            .map_err(|e| anyhow::anyhow!("Failed to get bucket stats: {}", e))
    }

    /// Stream events from the event processor using EventStreamCursor pattern.
    /// This follows the same pattern as the backup orchestrator.
    async fn process_events(
        &self,
        event_processor: Arc<EventProcessor>,
    ) -> Result<()> {
        // Get the event cursor for resumption
        let event_cursor = self.get_indexer_event_cursor().await?;
        tracing::info!(?event_cursor, "[stream_events] starting");
        
        // Get event stream from the event processor  
        let event_stream = std::pin::Pin::from(event_processor.events(event_cursor).await?);
        let next_event_index = event_cursor.element_index;
        let index_stream = futures::stream::iter(next_event_index..);
        let mut indexed_element_stream = index_stream.zip(event_stream);
        
        while let Some((
            element_index,
            positioned_stream_event,
        )) = indexed_element_stream.next().await
        {
            match &positioned_stream_event.element {
                EventStreamElement::ContractEvent(contract_event) => {
                    // Process the contract event and update storage
                    self.process_event(contract_event.clone()).await?;
                    
                    // Update the last processed index in storage
                    self.storage
                        .set_last_processed_event_index(element_index)
                        .map_err(|e| anyhow::anyhow!("Failed to update event cursor: {}", e))?;
                        
                    tracing::debug!(element_index, "Processed indexer event");
                }
                EventStreamElement::CheckpointBoundary => {
                    // Skip checkpoint boundaries as they are not relevant for the indexer
                    continue;
                }
            }
        }

        anyhow::bail!("event stream for indexer stopped")
    }
    
    /// Get the event cursor for resumption, similar to backup orchestrator's
    /// get_backup_node_cursor.
    async fn get_indexer_event_cursor(
        &self,
    ) -> Result<walrus_service::event::events::EventStreamCursor> {
        use walrus_service::event::events::EventStreamCursor;
        
        if let Some(last_processed_index) = self.storage
            .get_last_processed_event_index()
            .map_err(|e| anyhow::anyhow!("Failed to get last processed index: {}", e))?
        {
            // Resume from the next event after the last processed one
            return Ok(EventStreamCursor::new(None, last_processed_index + 1));
        }
        
        // Start from the beginning if no events have been processed yet
        Ok(EventStreamCursor::new(None, 0))
    }

    
    /// Stop the indexer and its event processor.
    pub async fn stop(&self) {
        info!("Stopping indexer");
        self.cancellation_token.cancel();
    }

}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn test_octopus_index_workflow() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().to_str().unwrap().to_string();
        
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
                "0xf3eda3f4deb7618d0fab06f7e90755afeabbeb8b33106f43c7f2d25c6ef6a3b3"
            ),
            (
                "/photos/2024/beach.jpg",
                [2; 32],
                "0xa1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890"
            ),
            (
                "/photos/2023/mountain.png",
                [3; 32],
                "0xb2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890ab"
            ),
            (
                "/photos/2023/forest.png",
                [4; 32],
                "0xc3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890abcd"
            ),
        ];
        
        // Test data for bucket 2 (documents).
        let docs_data = vec![
            (
                "/docs/reports/2024/q1.pdf",
                [5; 32],
                "0xd4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            ),
            (
                "/docs/reports/2024/q2.pdf",
                [6; 32],
                "0xe5f67890abcdef1234567890abcdef1234567890abcdef1234567890abcdef12"
            ),
            (
                "/docs/contracts/agreement.doc",
                [7; 32],
                "0xf67890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234"
            ),
            (
                "/docs/manuals/user-guide.txt",
                [8; 32],
                "0x67890abcdef1234567890abcdef1234567890abcdef1234567890abcdef12345"
            ),
        ];

        // Phase 1: Create indexer and write data.
        {
            let config = IndexerConfig {
                db_path: db_path.clone(),
                walrus_indexer_config: None,
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
                indexer.storage
                    .put_index_entry(&bucket1_id, path, &object_id, blob_id)
                    .map_err(|e| anyhow::anyhow!("Failed to add photo entry: {}", e))?;
            }

            // Add documents to bucket 2.
            for (path, blob_bytes, obj_id_hex) in &docs_data {
                let blob_id = walrus_core::BlobId(*blob_bytes);
                let object_id = ObjectID::from_hex_literal(obj_id_hex).unwrap();
                indexer.storage
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
            let bucket1_entries = indexer.list_bucket(&bucket1_id).await?;
            assert_eq!(bucket1_entries.len(), 4);

            // List all entries in bucket 2.
            let bucket2_entries = indexer.list_bucket(&bucket2_id).await?;
            assert_eq!(bucket2_entries.len(), 4);

            // Remove one entry from each bucket to test deletion.
            indexer.storage
                .delete_by_bucket_identifier(&bucket1_id, "/photos/2023/forest.png")
                .map_err(|e| anyhow::anyhow!("Failed to remove photo entry: {}", e))?;
            
            indexer.storage
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
                walrus_indexer_config: None,
            };

            let indexer = WalrusIndexer::new(config).await?;

            // Verify bucket 1 data after reopening.
            let stats1 = indexer.get_bucket_stats(&bucket1_id).await?;
            assert_eq!(stats1.primary_count, 3); // One was deleted.

            // Verify specific entries in bucket 1.
            for (i, (path, blob_bytes, obj_id_hex)) in photos_data.iter().enumerate() {
                if i == 3 { continue; } // Skip the deleted entry.
                
                let entry = indexer.get_blob_by_index(&bucket1_id, path).await?;
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
                .get_blob_by_index(&bucket1_id, "/photos/2023/forest.png")
                .await?;
            assert!(deleted_entry.is_none());

            // Verify bucket 2 data after reopening.
            let stats2 = indexer.get_bucket_stats(&bucket2_id).await?;
            assert_eq!(stats2.primary_count, 3); // One was deleted.

            // Test query by object ID.
            let obj_id = ObjectID::from_hex_literal(&docs_data[0].2).unwrap();
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
