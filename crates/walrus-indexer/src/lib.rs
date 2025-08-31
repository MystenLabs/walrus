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
//!         sui_rpc_url: "https://fullnode.devnet.sui.io:443".to_string(),
//!         use_buckets: true,
//!         api_port: 8080,
//!     };
//!
//!     // Create and start the indexer with event processing
//!     let indexer = WalrusIndexer::new_and_start(config).await?;
//!
//!     // The indexer is now running and processing Sui events in the background
//!     // You can use it to query data:
//!     // let entry = indexer.get_blob_by_index(&bucket_id, "/path/to/file").await?;
//!
//!     Ok(())
//! }
//! ```

pub mod checkpoint_downloader;
pub mod event_processor;
pub mod routes;
pub mod storage;

use std::{collections::HashMap, path::Path, sync::Arc};

use anyhow::Result;
use rocksdb::Options;
use serde::{Deserialize, Serialize};
use sui_types::base_types::ObjectID;
use tokio::sync::RwLock;
use tracing::{info, warn};
use typed_store::rocks::{DBMap, MetricConf, open_cf_opts};
use walrus_core::BlobId;

use self::storage::{MutationSet, PrimaryIndexValue};

/// Configuration for the indexer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexerConfig {
    /// Path to the database directory
    pub db_path: String,

    /// RPC URL for Sui node
    pub sui_rpc_url: String,

    /// Whether to use bucket namespacing
    pub use_buckets: bool,

    /// Port for the indexer API server
    pub api_port: u16,

    /// Event processor configuration (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_processor_config: Option<event_processor::config::IndexerEventProcessorConfig>,
}

impl Default for IndexerConfig {
    fn default() -> Self {
        Self {
            db_path: "./indexer-db".to_string(),
            sui_rpc_url: "https://fullnode.devnet.sui.io:443".to_string(),
            use_buckets: true,
            api_port: 8080,
            event_processor_config: None,
        }
    }
}

/// Represents a bucket for index entries (matches design spec)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Bucket {
    /// Bucket ID as Sui object
    pub bucket_id: ObjectID,

    /// Bucket name for user reference
    pub name: String,

    /// Secondary index definitions
    pub secondary_indices: Vec<String>,
}

/// Index entry mapping a primary key to a blob with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexEntry {
    /// User-specified primary key (path-like)
    pub primary_key: String,

    /// The bucket this entry belongs to
    pub bucket_id: ObjectID,

    /// Target blob ID
    pub blob_id: BlobId,

    /// Timestamp when this entry was created
    pub created_at: u64,

    /// Additional metadata
    pub metadata: HashMap<String, String>,

    /// Secondary index values
    pub secondary_indices: HashMap<String, Vec<String>>,
}

/// Index operation to be processed (matches design spec events)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexOperation {
    /// Add index entry from Sui event
    IndexAdded {
        bucket_id: ObjectID,
        identifier: String,
        object_id: ObjectID,
        blob_id: BlobId,
    },

    /// Remove index entry from Sui event (by object_id)
    IndexRemovedByObjectId {
        object_id: ObjectID,
    },

    /// Remove index entry from Sui event (by bucket_id + identifier)
    IndexRemovedByIdentifier {
        bucket_id: ObjectID,
        identifier: String,
    },

    /// Batch of mutations from Sui events
    ApplyMutations(Vec<MutationSet>),
}

/// The main Walrus Indexer interface (Octopus Index)
#[derive(Clone)]
pub struct WalrusIndexer {
    /// Configuration
    config: IndexerConfig,

    /// Storage layer for index data
    storage: Arc<storage::OctopusIndexStore>,

    /// Cache for frequently accessed entries
    cache: Arc<RwLock<HashMap<String, PrimaryIndexValue>>>,
}

impl WalrusIndexer {
    /// Create a new indexer instance
    pub async fn new(config: IndexerConfig) -> Result<Self> {
        // Initialize the database with both column families
        let db_options = Options::default();
        let db = Arc::new(open_cf_opts(
            Path::new(&config.db_path),
            None,
            MetricConf::default(),
            &[
                ("octopus_index_primary", db_options.clone()),
                ("octopus_index_object", db_options),
            ],
        )?);

        // Initialize primary index
        let primary_index = DBMap::reopen(
            &db,
            Some("octopus_index_primary"),
            &typed_store::rocks::ReadWriteOptions::default(),
            false,
        )?;

        // Initialize object index
        let object_index = DBMap::reopen(
            &db,
            Some("octopus_index_object"),
            &typed_store::rocks::ReadWriteOptions::default(),
            false,
        )?;

        // Create storage layer
        let storage = Arc::new(storage::OctopusIndexStore::new(primary_index, object_index));

        Ok(Self {
            config,
            storage,
            cache: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Process an index operation from Sui events
    pub async fn process_operation(&self, operation: IndexOperation) -> Result<()> {
        match operation {
            IndexOperation::IndexAdded {
                bucket_id,
                identifier,
                object_id,
                blob_id,
            } => {
                // Store in both primary and object indices
                self.storage.put_index_entry(
                    &bucket_id,
                    &identifier,
                    &object_id,
                    blob_id,
                )?;
                
                // Update cache
                let cache_key = format!("{}/{}", bucket_id, identifier);
                let blob_identity = storage::BlobIdentity { blob_id, object_id };
                let primary_value = storage::PrimaryIndexValue { blob_identity };
                let mut cache = self.cache.write().await;
                cache.insert(cache_key, primary_value);
            }
            IndexOperation::IndexRemovedByObjectId { object_id } => {
                // Remove by object_id
                self.storage.delete_by_object_id(&object_id)?;
                
                // Clear cache (we'd need to find the cache key, but it's complex)
                // For now, let's clear the entire cache to be safe
                let mut cache = self.cache.write().await;
                cache.clear();
            }
            IndexOperation::IndexRemovedByIdentifier {
                bucket_id,
                identifier,
            } => {
                // Remove by bucket_id and identifier
                self.storage.delete_by_bucket_identifier(&bucket_id, &identifier)?;
                
                // Remove from cache
                let cache_key = format!("{}/{}", bucket_id, identifier);
                let mut cache = self.cache.write().await;
                cache.remove(&cache_key);
            }
            IndexOperation::ApplyMutations(mutations) => {
                self.storage.apply_index_mutations(mutations)?;
                
                // Clear cache after mutations to ensure consistency
                let mut cache = self.cache.write().await;
                cache.clear();
            }
        }
        Ok(())
    }

    /// Get index entry by bucket_id and identifier
    pub async fn get_blob_by_index(
        &self,
        bucket_id: &ObjectID,
        identifier: &str,
    ) -> Result<Option<PrimaryIndexValue>> {
        // Check cache first
        let cache_key = format!("{}/{}", bucket_id, identifier);
        {
            let cache = self.cache.read().await;
            if let Some(entry) = cache.get(&cache_key) {
                return Ok(Some(entry.clone()));
            }
        }

        // Get from storage
        let result = self.storage.get_by_bucket_identifier(bucket_id, identifier)?;

        // Update cache if found
        if let Some(ref entry) = result {
            let mut cache = self.cache.write().await;
            cache.insert(cache_key, entry.clone());
        }

        Ok(result)
    }

    /// Get index entry by object_id (implements read_blob_by_object_id from PDF)
    pub async fn get_blob_by_object_id(
        &self,
        object_id: &ObjectID,
    ) -> Result<Option<PrimaryIndexValue>> {
        // For object_id lookups, we can't use cache easily as we don't know the bucket_identifier
        // Get directly from storage
        self.storage
            .get_by_object_id(object_id)
            .map_err(|e| anyhow::anyhow!("Failed to get blob by object_id: {}", e))
    }

    /// List all entries in a bucket
    pub async fn list_bucket(
        &self,
        bucket_id: &ObjectID,
    ) -> Result<HashMap<String, PrimaryIndexValue>> {
        self.storage
            .list_bucket_entries(bucket_id)
            .map_err(|e| anyhow::anyhow!("Failed to list bucket entries: {}", e))
    }

    /// Create a new bucket
    pub async fn create_bucket(&self, bucket: Bucket) -> Result<()> {
        // In a real implementation, this would interact with Sui to create the bucket object
        // For now, we just validate the bucket can be used
        println!(
            "Creating bucket: {} with ID: {}",
            bucket.name, bucket.bucket_id
        );
        Ok(())
    }

    /// Remove a bucket and all its entries
    pub async fn remove_bucket(&self, bucket_id: &ObjectID) -> Result<()> {
        self.storage
            .delete_bucket(bucket_id)
            .map_err(|e| anyhow::anyhow!("Failed to delete bucket: {}", e))
    }

    /// Get storage layer for direct operations
    pub fn storage(&self) -> &Arc<storage::OctopusIndexStore> {
        &self.storage
    }

    /// Get statistics for a bucket
    pub async fn get_bucket_stats(&self, bucket_id: &ObjectID) -> Result<storage::BucketStats> {
        self.storage
            .get_bucket_stats(bucket_id)
            .map_err(|e| anyhow::anyhow!("Failed to get bucket stats: {}", e))
    }

    /// Start processing Sui events in the background
    /// This spawns a background task that continuously processes Sui events
    pub fn start_event_processor(self: Arc<Self>) {
        let sui_rpc_url = self.config.sui_rpc_url.clone();

        tokio::spawn(async move {
            if let Err(e) = self.process_sui_events(sui_rpc_url).await {
                warn!("Event processor error: {}", e);
            }
        });
    }

    /// Create a new indexer and start it with event processing
    /// This is a convenience method for starting an indexer with a single call
    pub async fn new_and_start(config: IndexerConfig) -> Result<Arc<Self>> {
        let indexer = Arc::new(Self::new(config).await?);
        indexer.clone().start_event_processor();
        Ok(indexer)
    }

    /// Process Sui events to update the index
    /// This is the main event processing loop that connects to Sui and processes events
    async fn process_sui_events(&self, sui_rpc_url: String) -> Result<()> {
        info!("🔄 Starting Sui event processor");
        info!("Connecting to Sui RPC: {}", sui_rpc_url);

        // Check if event processor is configured
        if let Some(ref event_config) = self.config.event_processor_config {
            use event_processor::{config::IndexerRuntimeConfig, processor::IndexerEventProcessor};
            use walrus_utils::metrics::Registry;

            // Create metrics registry
            let prometheus_registry = prometheus::Registry::new();
            let metrics_registry = Registry::new(prometheus_registry);

            // Create runtime config
            let runtime_config = IndexerRuntimeConfig {
                rpc_addresses: vec![sui_rpc_url],
                event_polling_interval: std::time::Duration::from_secs(1),
                db_path: std::path::PathBuf::from(&self.config.db_path).join("event_processor"),
                rpc_fallback_config: None,
            };

            // Create event processor
            let event_processor =
                IndexerEventProcessor::new(event_config, runtime_config, &metrics_registry).await?;

            // Start processing events in background
            let processor_clone = event_processor.clone();
            let cancel_token = tokio_util::sync::CancellationToken::new();
            let mut processor_task = {
                let cancel_token = cancel_token.clone();
                tokio::spawn(async move {
                    if let Err(e) = processor_clone.start(cancel_token).await {
                        warn!("Event processor error: {}", e);
                    }
                })
            };

            // Process received events
            loop {
                tokio::select! {
                    Some(index_event) = event_processor.receive_event() => {
                        info!("Processing index event from checkpoint {}", index_event.checkpoint);

                        // Process the operation
                        if let Err(e) = self.process_operation(index_event.operation).await {
                            warn!("Failed to process index operation: {}", e);
                        }
                    }
                    _ = &mut processor_task => {
                        info!("Event processor task completed");
                        break;
                    }
                }
            }
        } else {
            // Fallback to heartbeat if event processor is not configured
            info!("Event processor not configured, running in heartbeat mode");
            let mut counter = 0;
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
                counter += 1;
                info!("📡 Event processor heartbeat #{}", counter);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn test_octopus_index_workflow() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let config = IndexerConfig {
            db_path: temp_dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        };

        let indexer = WalrusIndexer::new(config).await?;

        // Create a bucket
        let bucket_id = ObjectID::from_hex_literal(
            "0x42a8f3dc1234567890abcdef1234567890abcdef1234567890abcdef12345678",
        )
        .unwrap();
        let bucket = Bucket {
            bucket_id,
            name: "test-photos".to_string(),
            secondary_indices: vec![], // No secondary indices
        };

        indexer.create_bucket(bucket).await?;

        // Add an index entry (simulating Sui event)
        let blob_id = BlobId([1; 32]);
        let object_id = ObjectID::from_hex_literal(
            "0xf3eda3f4deb7618d0fab06f7e90755afeabbeb8b33106f43c7f2d25c6ef6a3b3",
        ).unwrap();
        let operation = IndexOperation::IndexAdded {
            bucket_id,
            identifier: "/photos/2024/sunset.jpg".to_string(),
            object_id,
            blob_id,
        };

        indexer.process_operation(operation).await?;

        // Query by primary index
        let entry = indexer
            .get_blob_by_index(&bucket_id, "/photos/2024/sunset.jpg")
            .await?;
        assert!(entry.is_some());
        let retrieved_entry = entry.unwrap();
        assert_eq!(retrieved_entry.blob_identity.blob_id, blob_id);
        assert_eq!(retrieved_entry.blob_identity.object_id, object_id);

        // Secondary index functionality removed - primary index only

        // List all entries in bucket
        let all_entries = indexer.list_bucket(&bucket_id).await?;
        assert_eq!(all_entries.len(), 1);

        // Get bucket stats
        let stats = indexer.get_bucket_stats(&bucket_id).await?;
        assert_eq!(stats.primary_count, 1);
        assert_eq!(stats.secondary_count, 0);

        // Remove the entry (test removal by identifier)
        let remove_operation = IndexOperation::IndexRemovedByIdentifier {
            bucket_id,
            identifier: "/photos/2024/sunset.jpg".to_string(),
        };

        indexer.process_operation(remove_operation).await?;

        // Verify removal
        let entry = indexer
            .get_blob_by_index(&bucket_id, "/photos/2024/sunset.jpg")
            .await?;
        assert!(entry.is_none());

        // Secondary index functionality removed

        Ok(())
    }
}
