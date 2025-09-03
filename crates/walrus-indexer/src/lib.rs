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
//! use walrus_indexer::{IndexerConfig, WalrusIndexer, RestApiConfig};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create configuration
//!     let rest_api_config = RestApiConfig {
//!         bind_address: "127.0.0.1:8080".parse().unwrap(),
//!         metrics_address: "127.0.0.1:9184".parse().unwrap(),
//!     };
//!
//!     let config = IndexerConfig {
//!         db_path: "./indexer-db".to_string(),
//!         rest_api_config: Some(rest_api_config),
//!         event_processor_config: None,
//!         sui: None,
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
pub mod routes;
pub mod server;
pub mod storage;

use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use walrus_service::event::event_processor::processor::EventProcessor;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use sui_types::base_types::ObjectID;
use tokio::{sync::RwLock, select, task::JoinHandle};
use tracing::{info, warn};
use walrus_sui::types::ContractEvent;

use self::storage::{BlobIdentity, PrimaryIndexValue};
pub use walrus_service::event::events::EventStreamElement;
pub use crate::server::IndexerRestApiServer;

/// Default configuration values for the indexer.
pub mod default {
    use std::net::SocketAddr;

    /// Default database path.
    pub fn db_path() -> String {
        "opt/walrus/db/indexer-db".to_string()
    }


    /// Default API bind address.
    pub fn bind_address() -> SocketAddr {
        "127.0.0.1:21345"
            .parse()
            .expect("this is a correct socket address")
    }

    /// Default metrics bind address.
    pub fn metrics_address() -> SocketAddr {
        "127.0.0.1:9184"
            .parse()
            .expect("this is a correct socket address")
    }
}

/// Configuration for the REST API server.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RestApiConfig {
    /// The address to which to bind the service.
    #[serde(default = "default::bind_address")]
    pub bind_address: SocketAddr,
    
    /// Socket address on which the Prometheus server should export its metrics.
    #[serde(default = "default::metrics_address")]
    pub metrics_address: SocketAddr,
}

/// Configuration for the indexer.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexerConfig {
    /// Path to the database directory.
    #[serde(default = "default::db_path")]
    pub db_path: String,
    
    /// REST API configuration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rest_api_config: Option<RestApiConfig>,

    /// Event processor configuration (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_processor_config: Option<
        walrus_service::event::event_processor::config::EventProcessorConfig,
    >,
    
    /// Sui configuration for event processing and blockchain interaction (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sui: Option<walrus_service::common::config::SuiConfig>,

}

impl Default for IndexerConfig {
    fn default() -> Self {
        Self {
            db_path: default::db_path(),
            rest_api_config: Some(RestApiConfig {
                bind_address: default::bind_address(),
                metrics_address: default::metrics_address(),
            }),
            event_processor_config: None,
            sui: None,
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

    /// Cache for frequently accessed entries.
    cache: Arc<RwLock<HashMap<String, PrimaryIndexValue>>>,

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
            cache: Arc::new(RwLock::new(HashMap::new())),
            event_processor: Arc::new(RwLock::new(None)),
            cancellation_token: tokio_util::sync::CancellationToken::new(),
        }))
    }


    /// Run the indexer as a full-featured service.
    /// This method:
    /// 1. Initializes the event processor (if configured).
    /// 2. Starts the REST API server (if configured).
    /// 3. Processes events from Sui blockchain (if configured).
    /// 
    /// When run() is not called, the Indexer acts as a simple key-value store.
    /// and relies on the caller to use apply_mutations() for updates.
    pub async fn run(
        self: Arc<Self>,
        cancel_token: tokio_util::sync::CancellationToken,
    ) -> Result<()> {
        // Initialize event processor if configured.
        self.start_event_processor().await?;
        
        // Start REST API server in a separate task if configured.
        let rest_api_handle = self.start_rest_api(cancel_token.clone()).await?;
        
        // Get event processor for the main event loop
        let event_processor = self.event_processor.read().await.clone();
        
        select! {
            _ = cancel_token.cancelled() => {
                info!("Indexer received shutdown signal");
            }
            
            res = async {
                if let Some(processor) = event_processor {
                    self.stream_events(processor).await
                } else {
                    // No event processor, just wait for cancellation
                    std::future::pending::<Result<()>>().await
                }
            } => {
                if let Err(e) = res {
                    warn!("Event processing error: {}", e);
                }
            }
            
            // Monitor REST API if it was started
            res = async {
                if let Some(handle) = rest_api_handle {
                    match handle.await {
                        Ok(rest_result) => rest_result,
                        Err(e) => Err(anyhow::anyhow!("REST API task join error: {}", e))
                    }
                } else {
                    std::future::pending::<Result<()>>().await
                }
            } => {
                if let Err(e) = res {
                    warn!("REST API error: {}", e);
                } else {
                    info!("REST API shutdown cleanly");
                }
            }
        }
        
        info!("Indexer shutdown complete");
        Ok(())
    }
    
    /// Start the event processor if configured.
    async fn start_event_processor(&self) -> Result<()> {
        if let Some(ref event_config) = self.config.event_processor_config {
            if let Some(ref sui_config) = self.config.sui {
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
                let sui_reader_config: SuiReaderConfig = sui_config.into();
                
                // Use EventProcessorRuntime to create the event processor
                let event_processor = EventProcessorRuntime::start_async(
                    sui_reader_config,
                    event_config.clone(),
                    &std::path::PathBuf::from(&self.config.db_path).join("event_processor"),
                    &metrics_registry,
                    event_cancel_token,
                    &DatabaseConfig::default(),
                ).await?;
                
                *self.event_processor.write().await = Some(event_processor);
            } else {
                warn!(
                    "Event processor config provided but sui config is missing. \
                     Event processor will not be started."
                );
            }
        }
        Ok(())
    }
    
    /// Start the REST API server if configured.
    async fn start_rest_api(
        self: &Arc<Self>,
        cancel_token: tokio_util::sync::CancellationToken,
    ) -> Result<Option<JoinHandle<Result<()>>>> {
        if let Some(ref rest_config) = self.config.rest_api_config {
            use walrus_utils::metrics::Registry;
            use server::IndexerRestApiServerConfig;
            
            info!("Starting REST API server on {}", rest_config.bind_address);
            
            // Create metrics registry for REST API
            let prometheus_registry = prometheus::Registry::new();
            let metrics_registry = Registry::new(prometheus_registry);
            
            let server_config = IndexerRestApiServerConfig::from(rest_config);
            let rest_api_server = Arc::new(server::IndexerRestApiServer::new(
                self.clone(),
                cancel_token.child_token(),
                server_config,
                &metrics_registry,
            ));
            
            // Spawn REST API in its own task for workload isolation
            Ok(Some(tokio::spawn(async move {
                rest_api_server.run().await
                    .map_err(|e| anyhow::anyhow!("REST API error: {}", e))
            })))
        } else {
            Ok(None)
        }
    }
    
    /// Process a contract event from Sui.
    pub async fn process_event(&self, _event: ContractEvent) -> Result<()> {
        // Convert ContractEvent to IndexOperation based on event type
        // This is a simplified conversion - in reality you'd parse the actual event data
        // For now, we'll just return Ok since ContractEvent structure varies
        // In production, you'd parse the actual event fields to create proper IndexOperation
        Ok(())
    }
    

    /// Get blob identity by bucket_id and identifier.
    pub async fn get_blob_by_index(
        &self,
        bucket_id: &ObjectID,
        identifier: &str,
    ) -> Result<Option<BlobIdentity>> {
        // Check cache first
        let cache_key = format!("{}/{}", bucket_id, identifier);
        {
            let cache = self.cache.read().await;
            if let Some(entry) = cache.get(&cache_key) {
                return Ok(Some(entry.blob_identity.clone()));
            }
        }

        // Get from storage
        let storage_result = self.storage.get_by_bucket_identifier(bucket_id, identifier)?;

        // Update cache if found
        if let Some(ref blob_identity) = storage_result {
            let mut cache = self.cache.write().await;
            cache.insert(cache_key, PrimaryIndexValue { blob_identity: blob_identity.clone() });
        }

        Ok(storage_result)
    }

    /// Get index entry by object_id (implements read_blob_by_object_id from PDF).
    pub async fn get_blob_by_object_id(
        &self,
        object_id: &ObjectID,
    ) -> Result<Option<BlobIdentity>> {
        // For object_id lookups, we can't use cache easily as we don't know the bucket_identifier
        // Get directly from storage
        let storage_result = self.storage
            .get_by_object_id(object_id)
            .map_err(|e| anyhow::anyhow!("Failed to get blob by object_id: {}", e))?;
            
        // Return the BlobIdentity directly
        Ok(storage_result)
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
    async fn stream_events(
        &self,
        event_processor: Arc<EventProcessor>,
    ) -> Result<()> {
        use futures::StreamExt;
        use walrus_service::event::events::EventStreamElement;
        use walrus_service::node::system_events::SystemEventProvider;
        
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
        let blob_id = walrus_core::BlobId([1; 32]);
        let object_id = ObjectID::from_hex_literal(
            "0xf3eda3f4deb7618d0fab06f7e90755afeabbeb8b33106f43c7f2d25c6ef6a3b3",
        ).unwrap();

        indexer.storage.put_index_entry(&bucket_id, "/photos/2024/sunset.jpg", &object_id, blob_id)
            .map_err(|e| anyhow::anyhow!("Failed to add index entry: {}", e))?;

        // Query by primary index
        let entry = indexer
            .get_blob_by_index(&bucket_id, "/photos/2024/sunset.jpg")
            .await?;
        assert!(entry.is_some());
        let retrieved_entry = entry.unwrap();
        assert_eq!(retrieved_entry.blob_id, blob_id);
        assert_eq!(retrieved_entry.object_id, object_id);

        // Secondary index functionality removed - primary index only

        // List all entries in bucket
        let all_entries = indexer.list_bucket(&bucket_id).await?;
        assert_eq!(all_entries.len(), 1);

        // Get bucket stats
        let stats = indexer.get_bucket_stats(&bucket_id).await?;
        assert_eq!(stats.primary_count, 1);
        assert_eq!(stats.secondary_count, 0);

        // Remove the entry (test removal by identifier)
        indexer.storage.delete_by_bucket_identifier(&bucket_id, "/photos/2024/sunset.jpg")
            .map_err(|e| anyhow::anyhow!("Failed to remove index entry: {}", e))?;

        // Invalidate cache for the deleted entry
        {
            let cache_key = format!("{}/{}", bucket_id, "/photos/2024/sunset.jpg");
            let mut cache = indexer.cache.write().await;
            cache.remove(&cache_key);
        }

        // Verify removal
        let entry = indexer
            .get_blob_by_index(&bucket_id, "/photos/2024/sunset.jpg")
            .await?;
        assert!(entry.is_none());

        // Secondary index functionality removed

        Ok(())
    }
}
