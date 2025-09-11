// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Walrus Indexer implementation.

use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sui_types::base_types::ObjectID;
use tokio::{select, sync::RwLock};
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
    AsyncTask,
    Bucket,
    IndexerConfig,
    TaskExecutor,
    async_task_manager::{AsyncTaskManager, AsyncTaskManagerConfig},
    storage::{BlobIdentity, BucketStats, WalrusIndexStore},
};

/// Unique identifier for quilt index tasks.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct QuiltIndexTaskId {
    pub sequence: u64,
    pub quilt_id: BlobId,
}

impl QuiltIndexTaskId {
    pub fn new(sequence: u64, quilt_id: BlobId) -> Self {
        Self { sequence, quilt_id }
    }
}

/// A quilt indexing task.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QuiltIndexTask {
    pub sequence_number: u64,
    pub quilt_blob_id: BlobId,
    pub object_id: ObjectID,
    pub bucket_id: ObjectID,
    pub identifier: String,
    pub event_index: u64,
}

impl QuiltIndexTask {
    pub fn new(
        sequence_number: u64,
        quilt_blob_id: BlobId,
        object_id: ObjectID,
        bucket_id: ObjectID,
        identifier: String,
        event_index: u64,
    ) -> Self {
        Self {
            sequence_number,
            quilt_blob_id,
            object_id,
            bucket_id,
            identifier,
            event_index,
        }
    }

    /// Create a QuiltIndexTask from an IndexMutation::Insert and event index.
    /// This is used when processing quilt index events to create async tasks.
    pub fn from_quilt_insert(
        insert: &walrus_sui::types::IndexMutation,
        bucket_id: &ObjectID,
        event_index: u64,
        sequence_number: u64,
    ) -> Option<Self> {
        match insert {
            walrus_sui::types::IndexMutation::Insert {
                identifier,
                object_id,
                blob_id,
                is_quilt: true,
            } => Some(Self::new(
                sequence_number,
                *blob_id,
                *object_id,
                *bucket_id,
                identifier.clone(),
                event_index,
            )),
            _ => None,
        }
    }
}

impl AsyncTask for QuiltIndexTask {
    type TaskId = QuiltIndexTaskId;

    fn task_id(&self) -> Self::TaskId {
        QuiltIndexTaskId::new(self.sequence_number, self.quilt_blob_id)
    }

    fn sequence_number(&self) -> u64 {
        self.sequence_number
    }
}

/// Executor for quilt indexing tasks.
#[derive(Clone)]
pub struct QuiltTaskExecutor {
    storage: Arc<WalrusIndexStore>,
    walrus_client: Option<Arc<WalrusNodeClient<SuiContractClient>>>,
}

impl QuiltTaskExecutor {
    pub fn new(
        storage: Arc<WalrusIndexStore>,
        walrus_client: Option<Arc<WalrusNodeClient<SuiContractClient>>>,
    ) -> Self {
        Self {
            storage,
            walrus_client,
        }
    }
}

#[async_trait]
impl TaskExecutor<QuiltIndexTask> for QuiltTaskExecutor {
    async fn execute(&self, task: QuiltIndexTask) -> Result<()> {
        tracing::info!(
            "Executing quilt index task: sequence={}, quilt_id={}, object_id={}",
            task.sequence_number,
            task.quilt_blob_id,
            task.object_id
        );

        // Step 1: Fetch the quilt index from storage nodes
        if self.walrus_client.is_none() {
            tracing::warn!("No Walrus client configured, skipping quilt index processing");
            return Ok(());
        }

        let quilt_metadata = self
            .walrus_client
            .as_ref()
            .unwrap()
            .quilt_client()
            .get_quilt_metadata(&task.quilt_blob_id)
            .await?;
        let quilt_index = quilt_metadata.get_quilt_index();

        // Step 2: Populate quilt index and remove task atomically
        self.storage
            .populate_quilt_patch_index(&task, &quilt_index)
            .map_err(|e| anyhow::anyhow!("Failed to populate quilt patch index: {}", e))?;

        tracing::info!(
            "Successfully processed quilt index for quilt {} with patches",
            task.quilt_blob_id
        );

        Ok(())
    }
}

/// Alias for the quilt task manager using the async task manager.
pub type QuiltTaskManager = crate::async_task_manager::AsyncTaskManager<
    QuiltIndexTask,
    WalrusIndexStore,
    QuiltTaskExecutor,
>;

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

    /// Async task manager for quilt processing.
    quilt_task_manager: Arc<RwLock<Option<Arc<QuiltTaskManager>>>>,

    /// Sequence counter for generating task IDs.
    task_sequence_counter: Arc<std::sync::atomic::AtomicU64>,
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

        // Create the indexer
        let storage = Arc::new(storage);
        let indexer = Arc::new(Self {
            config,
            storage: storage.clone(),
            event_processor: Arc::new(RwLock::new(None)),
            walrus_client,
            cancellation_token: tokio_util::sync::CancellationToken::new(),
            quilt_task_manager: Arc::new(RwLock::new(None)), // Will be set in run() method
            task_sequence_counter: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        });

        Ok(indexer)
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

        // Initialize and start the async task manager for quilt processing
        self.initialize_task_manager().await?;

        let event_processor = self.event_processor.read().await.clone();

        select! {
            _ = cancel_token.cancelled() => {
                tracing::info!("Indexer received shutdown signal");
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
                    tracing::warn!("Event processing error: {}", e);
                }
            }
        }

        // Shutdown the task manager
        if let Some(ref task_manager) = *self.quilt_task_manager.read().await {
            task_manager.shutdown().await;
            tracing::info!("Shut down quilt task manager");
        }

        tracing::info!("Indexer shutdown complete");
        Ok(())
    }

    /// Get blob identity from a bucket by bucket_id and identifier.
    pub async fn get_blob_from_bucket(
        &self,
        bucket_id: &ObjectID,
        identifier: &str,
    ) -> Result<Option<BlobIdentity>> {
        match self
            .storage
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

    /// Get a reference to the Walrus client for blob operations.
    pub fn walrus_client(&self) -> Option<&Arc<WalrusNodeClient<SuiContractClient>>> {
        self.walrus_client.as_ref()
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
            tracing::info!("Starting Walrus indexer event processor");

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

    /// Initialize the async task manager for quilt processing.
    async fn initialize_task_manager(self: &Arc<Self>) -> Result<()> {
        // Create QuiltTaskExecutor
        let executor = QuiltTaskExecutor::new(self.storage.clone(), self.walrus_client.clone());

        let task_manager_config = AsyncTaskManagerConfig::default();

        let task_manager: Arc<QuiltTaskManager> = Arc::new(
            AsyncTaskManager::new(
                task_manager_config,
                self.storage.clone(),
                Arc::new(executor),
            )
            .await?,
        );

        // Start the task manager
        task_manager.start().await?;
        tracing::info!("Started quilt task manager");

        // Store the task manager
        *self.quilt_task_manager.write().await = Some(task_manager);

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

                tracing::warn!(
                    "Processing index event without cursor - quilt patches won't be \
                     indexed automatically"
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
                // Separate quilt and non-quilt mutations
                let mut non_quilt_mutations = Vec::new();
                let mut quilt_tasks = Vec::new();

                for mutation in &mutation_set.mutations {
                    match mutation {
                        IndexMutation::Insert { is_quilt: true, .. } => {
                            // For quilt insertions, create async tasks instead of immediate
                            // processing
                            if let Some(task) = QuiltIndexTask::from_quilt_insert(
                                mutation,
                                &mutation_set.bucket_id,
                                cursor,
                                self.task_sequence_counter
                                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                            ) {
                                quilt_tasks.push(task);
                            }
                        }
                        _ => {
                            // Non-quilt mutations (regular blobs, deletes) are processed
                            // immediately
                            non_quilt_mutations.push(mutation.clone());
                        }
                    }
                }

                // Check if we have any non-quilt mutations to process
                let has_non_quilt_mutations = !non_quilt_mutations.is_empty();
                let has_quilt_tasks = !quilt_tasks.is_empty();

                // Apply non-quilt mutations immediately if any exist
                if has_non_quilt_mutations {
                    let non_quilt_mutation_set = walrus_sui::types::IndexMutationSet {
                        bucket_id: mutation_set.bucket_id,
                        mutations: non_quilt_mutations,
                        event_id: mutation_set.event_id,
                    };
                    self.storage.apply_index_mutations_with_cursor(
                        vec![non_quilt_mutation_set],
                        Some(cursor),
                    )?;
                }

                // Submit quilt processing tasks to the async task manager
                if has_quilt_tasks {
                    self.submit_quilt_tasks(quilt_tasks).await?;
                }

                // Update cursor even if no mutations were processed
                if !has_non_quilt_mutations && !has_quilt_tasks {
                    self.storage
                        .set_last_processed_event_index(cursor)
                        .map_err(|e| anyhow::anyhow!("Failed to update event cursor: {}", e))?;
                }
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
    pub async fn process_quilt_index_background(
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
            tracing::warn!("No Walrus client configured, skipping quilt index processing");
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

        // Create a temporary QuiltIndexTask for the populate call
        let temp_task = QuiltIndexTask::new(
            0, // sequence_number
            quilt_id,
            object_id,
            bucket_id,
            format!("quilt_{}", quilt_id), // identifier
            index,                         // event_index
        );

        self.storage
            .populate_quilt_patch_index(&temp_task, &quilt_index)
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

    /// Submit quilt processing tasks to the async task manager.
    async fn submit_quilt_tasks(&self, tasks: Vec<QuiltIndexTask>) -> Result<()> {
        if let Some(ref task_manager) = *self.quilt_task_manager.read().await {
            for task in tasks {
                // Submit the task to the async task manager
                task_manager.submit(task.clone()).await?;

                tracing::info!(
                    "Submitted quilt processing task for blob_id={}, sequence={}, event_index={}",
                    task.quilt_blob_id,
                    task.sequence_number,
                    task.event_index
                );
            }
        } else {
            tracing::warn!("No task manager available, skipping quilt processing");
        }

        Ok(())
    }

    /// Stop the indexer and its event processor.
    pub async fn stop(&self) {
        tracing::info!("Stopping indexer");
        self.cancellation_token.cancel();

        // Shutdown the task manager
        if let Some(ref task_manager) = *self.quilt_task_manager.read().await {
            task_manager.shutdown().await;
            tracing::info!("Task manager shutdown complete");
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
