// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Walrus Indexer implementation.

use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use futures::StreamExt;
use sui_types::base_types::ObjectID;
use tokio::{select, sync::RwLock, task::JoinHandle};
use tracing::{info, warn};
use walrus_core::{BlobId, encoding::quilt_encoding::QuiltIndexApi};
use walrus_sdk::client::WalrusNodeClient;
use walrus_service::{
    common::config::SuiReaderConfig,
    event::{
        event_processor::{processor::EventProcessor, runtime::EventProcessorRuntime},
        events::EventStreamElement,
    },
    node::{DatabaseConfig, system_events::SystemEventProvider},
};
use walrus_sui::{
    client::SuiContractClient,
    types::{ContractEvent, IndexEvent, IndexMutation},
};
use walrus_utils::metrics::Registry;

use crate::{
    Bucket,
    IndexerConfig,
    storage::{BlobIdentity, BucketStats, WalrusIndexStore},
};

/// The main Walrus Indexer interface.
#[derive(Clone)]
pub struct WalrusIndexer {
    config: IndexerConfig,

    /// Storage layer for index data.
    pub storage: Arc<WalrusIndexStore>,

    /// Event processor for pulling events from Sui (if configured).
    event_processor: Arc<RwLock<Option<Arc<EventProcessor>>>>,

    /// Walrus client for reading blob data (if configured).
    walrus_client: Option<Arc<WalrusNodeClient<SuiContractClient>>>,

    /// Cancellation token for graceful shutdown.
    cancellation_token: tokio_util::sync::CancellationToken,

    /// Background task handles for quilt processing.
    quilt_task_handles: Arc<RwLock<Vec<JoinHandle<()>>>>,
}

impl WalrusIndexer {
    /// Create a new indexer instance.
    /// This creates a simple key-value store with indexing logic.
    /// The indexer will not start any background services until run() is called.
    pub async fn new(config: IndexerConfig) -> Result<Arc<Self>> {
        // Create storage layer by opening the database
        let mut storage = WalrusIndexStore::open(&config.db_path).await?;

        // Create SuiReadClient and WalrusNodeClient if sui_config is provided
        let walrus_client = if let Some(ref sui_config) = config.sui_config {
            let read_client = Arc::new(
                sui_config
                    .new_read_client()
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to create SuiReadClient: {}", e))?,
            );
            storage = storage.with_read_client(read_client.clone());

            // Create WalrusNodeClient for reading blob data
            let contract_client = sui_config
                .new_contract_client(None)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to create contract client: {}", e))?;

            // Create client config from the contract config
            let client_config = walrus_sdk::config::ClientConfig::new_from_contract_config(
                sui_config.contract_config.clone(),
            );

            let walrus_node_client = Arc::new(
                WalrusNodeClient::new_contract_client_with_refresher(
                    client_config,
                    contract_client,
                )
                .await
                .map_err(|e| anyhow::anyhow!("Failed to create WalrusNodeClient: {}", e))?,
            );
            Some(walrus_node_client)
        } else {
            None
        };

        Ok(Arc::new(Self {
            config,
            storage: Arc::new(storage),
            event_processor: Arc::new(RwLock::new(None)),
            walrus_client,
            cancellation_token: tokio_util::sync::CancellationToken::new(),
            quilt_task_handles: Arc::new(RwLock::new(Vec::new())),
        }))
    }

    /// Run the indexer as a full-featured service.
    /// This method:
    /// 1. Initializes the event processor (if configured).
    /// 2. Start processing events from Sui blockchain (if configured).
    pub async fn run(
        self: Arc<Self>,
        registry: &Registry,
        cancel_token: tokio_util::sync::CancellationToken,
    ) -> Result<()> {
        self.start_event_processor(registry).await?;

        let event_processor = self.event_processor.read().await.clone();

        select! {
            _ = cancel_token.cancelled() => {
                info!("Indexer received shutdown signal");
            }

            res = async {
                if let Some(processor) = event_processor {
                    self.process_events(processor).await
                } else {
                    // No event processor, just wait for cancellation.
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

    /// Get blob identity from a bucket by bucket_id and identifier.
    pub async fn get_blob_from_bucket(
        &self,
        bucket_id: &ObjectID,
        identifier: &str,
    ) -> Result<Option<BlobIdentity>> {
        match self.storage
            .get_by_bucket_identifier(bucket_id, identifier)
            .map_err(|e| anyhow::anyhow!("Failed to get blob by bucket identifier: {}", e))?
        {
            Some(crate::storage::IndexTarget::Blob(blob_identity)) => Ok(Some(blob_identity)),
            Some(_) => Ok(None), // Other variants are not BlobIdentity
            None => Ok(None),
        }
    }

    /// Get index target from a bucket by bucket_id and identifier.
    /// Returns the actual IndexTarget which could be Blob, QuiltPatchId, or QuiltId.
    pub async fn get_index_target_from_bucket(
        &self,
        bucket_id: &ObjectID,
        identifier: &str,
    ) -> Result<Option<crate::storage::IndexTarget>> {
        self.storage
            .get_by_bucket_identifier(bucket_id, identifier)
            .map_err(|e| anyhow::anyhow!("Failed to get index target by bucket identifier: {}", e))
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

    /// List all blob entries in a bucket.
    pub async fn list_blobs_in_bucket(
        &self,
        bucket_id: &ObjectID,
    ) -> Result<HashMap<String, BlobIdentity>> {
        self.storage
            .list_blobs_in_bucket_entries(bucket_id)
            .map_err(|e| anyhow::anyhow!("Failed to list bucket entries: {}", e))
    }

    /// Create a new bucket.
    /// This is a no-op in the current implementation.
    /// We can store bucket object after refactoring the underlying storage layout.
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
    pub fn storage(&self) -> &Arc<WalrusIndexStore> {
        &self.storage
    }

    /// Get statistics for a bucket.
    pub async fn get_bucket_stats(&self, bucket_id: &ObjectID) -> Result<BucketStats> {
        self.storage
            .get_bucket_stats(bucket_id)
            .map_err(|e| anyhow::anyhow!("Failed to get bucket stats: {}", e))
    }

    /// Start the event processor if configured.
    async fn start_event_processor(&self, registry: &Registry) -> Result<()> {
        if let (Some(ref event_proc_config), Some(ref sui_config)) =
            (&self.config.event_processor_config, &self.config.sui_config)
        {
            info!("Starting Walrus indexer event processor");

            let event_cancel_token = self.cancellation_token.child_token();
            let sui_reader_config: SuiReaderConfig = sui_config.into();
            let event_processor = EventProcessorRuntime::start_async(
                sui_reader_config,
                event_proc_config.clone(),
                &self.config.db_path.join("event_processor"),
                registry,
                event_cancel_token,
                &DatabaseConfig::default(),
            )
            .await?;

            *self.event_processor.write().await = Some(event_processor);
        }

        Ok(())
    }

    async fn process_events(&self, event_processor: Arc<EventProcessor>) -> Result<()> {
        // Get the event cursor for resumption
        let event_cursor = self.get_indexer_event_cursor().await?;
        tracing::info!(?event_cursor, "[stream_events] starting");

        // Get event stream from the event processor.
        let event_stream = std::pin::Pin::from(event_processor.events(event_cursor).await?);
        let next_event_index = event_cursor.element_index;
        let index_stream = futures::stream::iter(next_event_index..);
        let mut indexed_element_stream = index_stream.zip(event_stream);

        while let Some((element_index, positioned_stream_event)) =
            indexed_element_stream.next().await
        {
            match &positioned_stream_event.element {
                EventStreamElement::ContractEvent(contract_event) => {
                    // Process the contract event and update storage with cursor atomically
                    self.process_event_with_cursor(contract_event.clone(), element_index)
                        .await?;

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

    /// Process a contract event from Sui.
    pub async fn process_event(&self, event: ContractEvent) -> Result<()> {
        tracing::info!(?event, "Processing contract event in indexer");
        match event {
            ContractEvent::IndexEvent(index_event) => {
                self.process_index_event(index_event).await?;
            }
            _ => {
                tracing::warn!("Skipping non-index event: {:?}", event);
            }
        }
        Ok(())
    }

    /// Process a contract event from Sui with atomic cursor update.
    pub async fn process_event_with_cursor(&self, event: ContractEvent, cursor: u64) -> Result<()> {
        tracing::info!(
            ?event,
            cursor,
            "Processing contract event in indexer with cursor"
        );
        match event {
            ContractEvent::IndexEvent(index_event) => {
                self.process_index_event_with_cursor(index_event, cursor)
                    .await?;
            }
            _ => {
                tracing::warn!("Skipping non-index event: {:?}", event);
            }
        }
        Ok(())
    }

    /// Process an index event from Sui.
    pub async fn process_index_event(&self, index_event: IndexEvent) -> Result<()> {
        match index_event {
            IndexEvent::BlobIndexOperation(mutation_set) => {
                // Note: Without cursor, we cannot track quilts for background processing
                // since we don't have the event index. Quilts will be stored but patches
                // won't be indexed automatically.
                self.storage.apply_index_mutations(vec![mutation_set])?;

                warn!(
                    "Processing index event without cursor - quilt patches won't be indexed automatically"
                );
            }
        }
        Ok(())
    }

    /// Process an index event from Sui with atomic cursor update.
    pub async fn process_index_event_with_cursor(
        &self,
        index_event: IndexEvent,
        cursor: u64,
    ) -> Result<()> {
        match index_event {
            IndexEvent::BlobIndexOperation(mutation_set) => {
                // Collect quilt insertions for background processing
                let mut quilts_to_process = Vec::new();

                // Check for quilt insertions in the mutation set
                for mutation in &mutation_set.mutations {
                    if let IndexMutation::Insert {
                        blob_id,
                        object_id,
                        is_quilt: true,
                        ..
                    } = mutation
                    {
                        quilts_to_process.push((*blob_id, *object_id, mutation_set.bucket_id));
                    }
                }
                // let expanded_mutations = self.expand_quilt_mutations(vec![mutation_set]).await?;

                // Apply mutations with cursor - this will also create pending quilt index tasks
                self.storage
                    .apply_index_mutations_with_cursor(vec![mutation_set.clone()], Some(cursor))?;

                // Spawn background tasks to process quilt patches
                self.spawn_quilt_processing_tasks(quilts_to_process, cursor)
                    .await;
            }
        }
        Ok(())
    }

    /// Get the event cursor for resumption, similar to backup orchestrator's
    /// get_backup_node_cursor.
    async fn get_indexer_event_cursor(
        &self,
    ) -> Result<walrus_service::event::events::EventStreamCursor> {
        use walrus_service::event::events::EventStreamCursor;

        if let Some(last_processed_index) = self
            .storage
            .get_last_processed_event_index()
            .map_err(|e| anyhow::anyhow!("Failed to get last processed index: {}", e))?
        {
            // Resume from the next event after the last processed one
            return Ok(EventStreamCursor::new(None, last_processed_index + 1));
        }

        // Start from the beginning if no events have been processed yet
        Ok(EventStreamCursor::new(None, 0))
    }

    /// Process quilt index in background - fetches quilt data and populates patch indices.
    async fn process_quilt_index_background(
        &self,
        quilt_id: BlobId,
        object_id: ObjectID,
        bucket_id: ObjectID,
        index: u64,
    ) -> Result<()> {
        tracing::info!(
            "Processing quilt index in background for blob {} (object: {}, bucket: {})",
            quilt_id,
            object_id,
            bucket_id
        );

        // Check if we have a Walrus client configured
        if self.walrus_client.is_none() {
            warn!("No Walrus client configured, skipping quilt index processing");
            return Ok(());
        }

        let quilt_metadata = self
            .walrus_client
            .as_ref()
            .unwrap()
            .quilt_client()
            .get_quilt_metadata(&quilt_id)
            .await?;
        let quilt_index = quilt_metadata.get_quilt_index();
        tracing::info!("Indexer fetched quilt index: {:?}", quilt_index);
        tracing::info!(
            "quilt index in background for blob {} (object: {}, bucket: {})",
            quilt_id,
            object_id,
            bucket_id
        );

        self.storage
            .populate_quilt_patch_index(&bucket_id, &quilt_id, &object_id, &quilt_index, index)
            .map_err(|e| anyhow::anyhow!("Failed to populate quilt patch index: {}", e))?;

        tracing::info!(
            "Successfully processed quilt index for quilt {} with {} patches",
            quilt_id,
            match &quilt_index {
                walrus_core::metadata::QuiltIndex::V1(v1) => v1.patches().len(),
            }
        );

        Ok(())
    }

    /// Spawn background tasks to process quilt indices.
    async fn spawn_quilt_processing_tasks(
        &self,
        quilts_to_process: Vec<(BlobId, ObjectID, ObjectID)>,
        cursor: u64,
    ) {
        let mut handles = self.quilt_task_handles.write().await;

        // Clean up completed tasks.
        handles.retain(|handle| !handle.is_finished());

        for (quilt_blob_id, object_id, bucket_id) in quilts_to_process {
            let indexer = self.clone();
            let index = cursor;
            let cancel_token = self.cancellation_token.child_token();

            let handle = tokio::spawn(async move {
                select! {
                    _ = cancel_token.cancelled() => {
                        info!("Quilt processing task cancelled for blob {}", quilt_blob_id);
                    }
                    result = indexer.process_quilt_index_background(
                        quilt_blob_id,
                        object_id,
                        bucket_id,
                        index,
                    ) => {
                        if let Err(e) = result {
                            warn!(
                                "Failed to process quilt index for blob {} in background: {}",
                                quilt_blob_id, e
                            );
                        }
                    }
                }
            });

            handles.push(handle);
        }
    }

    /// Stop the indexer and its event processor.
    pub async fn stop(&self) {
        info!("Stopping indexer");
        self.cancellation_token.cancel();

        // Wait for all background tasks to complete
        let mut handles = self.quilt_task_handles.write().await;
        for handle in handles.drain(..) {
            let _ = handle.await;
        }
    }
}

impl Drop for WalrusIndexer {
    fn drop(&mut self) {
        // Cancel all background tasks when the indexer is dropped
        self.cancellation_token.cancel();

        // Note: We can't await the tasks here since drop is not async,
        // but cancelling the token will signal them to stop
    }
}
