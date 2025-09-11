// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Concrete implementation of async task queue for quilt patch indexing.

use std::sync::Arc;
use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sui_types::base_types::ObjectID;
use typed_store::{Map, TypedStoreError, rocks::DBMap};
use walrus_core::BlobId;

use crate::async_task_queue::{
    Task, TaskPersistence, TaskExecutor, AsyncTaskQueue, TaskQueueConfig
};

struct QuiltIndexTaskId {
    pub sequence: u64,
    pub quilt_id: BlobId,
}

impl QuiltIndexTaskId {
    pub fn new(sequence: u64, quilt_id: BlobId) -> Self {
        Self { sequence, quilt_id }
    }
}

/// Quilt patch indexing task with proper task ID implementation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuiltIndexTask {
    pub bucket_id: ObjectID,
    pub identifier: String,
    pub sequence: u64,
    pub quilt_id: BlobId,
    pub object_id: ObjectID,
}

impl Task for QuiltIndexTask {
    type TaskId = QuiltIndexTaskId;
    type SequenceNumber = u64;

    fn task_id(&self) -> Self::TaskId {
        QuiltIndexTaskId::new(self.sequence, self.quilt_id)
    }

    fn sequence_number(&self) -> Self::SequenceNumber {
        self.sequence
    }
}

/// RocksDB-based persistence for quilt indexing tasks.
pub struct QuiltTaskPersistence {
    pending_tasks: DBMap<QuiltTaskKey, QuiltIndexTask>,
    sequence_counter: DBMap<String, u64>,
}

impl QuiltTaskPersistence {
    /// Create a new persistence layer.
    pub fn new(
        pending_tasks: DBMap<QuiltTaskKey, QuiltIndexTask>,
        sequence_counter: DBMap<String, u64>,
    ) -> Self {
        Self {
            pending_tasks,
            sequence_counter,
        }
    }

    /// Get the next sequence number.
    async fn next_sequence(&self) -> Result<u64, TypedStoreError> {
        let current = self.sequence_counter
            .get(&"quilt_task_sequence".to_string())?
            .unwrap_or(0);
        let next = current + 1;
        self.sequence_counter
            .insert(&"quilt_task_sequence".to_string(), &next)?;
        Ok(next)
    }
}

#[async_trait]
impl TaskPersistence<QuiltIndexTask> for QuiltTaskPersistence {
    async fn persist_task(&self, task: &QuiltIndexTask) -> Result<(), TypedStoreError> {
        let key = QuiltTaskKey::from(task);
        self.pending_tasks.insert(&key, task)
    }

    async fn remove_task(&self, task_id: &(u64, BlobId)) -> Result<(), TypedStoreError> {
        let key = QuiltTaskKey {
            sequence: task_id.0,
            quilt_id: task_id.1,
        };
        self.pending_tasks.remove(&key)?;
        Ok(())
    }

    async fn load_tasks(
        &self,
        from_seq: Option<u64>,
        to_seq: Option<u64>,
        limit: usize,
    ) -> Result<Vec<QuiltIndexTask>, TypedStoreError> {
        let mut tasks = Vec::new();
        let mut count = 0;

        for entry in self.pending_tasks.safe_iter()? {
            let (key, task) = entry?;

            // Filter by from_seq if provided
            if let Some(from) = from_seq {
                if key.sequence <= from {
                    continue;
                }
            }

            // Filter by to_seq if provided
            if let Some(to) = to_seq {
                if key.sequence > to {
                    break;
                }
            }

            tasks.push(task);
            count += 1;

            if count >= limit {
                break;
            }
        }

        // Sort by sequence number to ensure proper ordering
        tasks.sort_by_key(|t| t.sequence);
        Ok(tasks)
    }
}

/// Executor for quilt indexing tasks.
pub struct QuiltTaskExecutor {
    indexer: Arc<crate::WalrusIndexer>,
}

impl QuiltTaskExecutor {
    /// Create a new executor.
    pub fn new(indexer: Arc<crate::WalrusIndexer>) -> Self {
        Self { indexer }
    }
}

#[async_trait]
impl TaskExecutor<QuiltIndexTask> for QuiltTaskExecutor {
    async fn execute(&self, task: QuiltIndexTask) -> Result<()> {
        tracing::info!(
            "Executing quilt index task: sequence={}, quilt_id={}",
            task.sequence,
            task.quilt_id
        );

        // This is the same logic as in process_quilt_index_background
        if let Some(walrus_client) = self.indexer.walrus_client() {
            let quilt_metadata = walrus_client
                .quilt_client()
                .get_quilt_metadata(&task.quilt_id)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to get quilt metadata: {}", e))?;

            let quilt_index = quilt_metadata.get_quilt_index();

            self.indexer
                .storage
                .populate_quilt_patch_index(
                    &task.bucket_id,
                    &task.quilt_id,
                    &task.object_id,
                    &quilt_index,
                    task.sequence,
                )
                .map_err(|e| anyhow::anyhow!("Failed to populate quilt patch index: {}", e))?;

            tracing::info!(
                "Successfully processed quilt index for quilt {} with sequence {}",
                task.quilt_id,
                task.sequence
            );
        } else {
            return Err(anyhow::anyhow!("No Walrus client configured"));
        }

        Ok(())
    }
}

/// Manager for quilt indexing tasks using the generic task queue.
pub struct QuiltTaskManager {
    task_queue: AsyncTaskQueue<QuiltIndexTask, QuiltTaskPersistence, QuiltTaskExecutor>,
    sequence_counter: u64,
}

impl QuiltTaskManager {
    /// Create a new quilt task manager.
    pub async fn new(
        pending_tasks: DBMap<QuiltTaskKey, QuiltIndexTask>,
        sequence_counter: DBMap<String, u64>,
        indexer: Arc<crate::WalrusIndexer>,
        config: Option<TaskQueueConfig>,
    ) -> Result<Self> {
        let persistence = Arc::new(QuiltTaskPersistence::new(
            pending_tasks,
            sequence_counter.clone(),
        ));
        let executor = Arc::new(QuiltTaskExecutor::new(indexer));
        let config = config.unwrap_or_default();

        // Get current sequence counter
        let current_sequence = sequence_counter
            .get(&"quilt_task_sequence".to_string())
            .map_err(|e| anyhow::anyhow!("Failed to get sequence counter: {}", e))?
            .unwrap_or(0);

        let task_queue = AsyncTaskQueue::new(config, persistence, executor).await?;

        // Start the background processing
        task_queue.start();

        Ok(Self {
            task_queue,
            sequence_counter: current_sequence,
        })
    }

    /// Submit a quilt indexing task.
    pub async fn submit_quilt_task(
        &mut self,
        quilt_id: BlobId,
        object_id: ObjectID,
        bucket_id: ObjectID,
    ) -> Result<()> {
        // Generate next sequence number
        self.sequence_counter += 1;

        let task = QuiltIndexTask {
            sequence: self.sequence_counter,
            quilt_id,
            object_id,
            bucket_id,
        };

        self.task_queue.submit_task(task).await
    }

    /// Get task queue statistics.
    pub async fn get_stats(&self) -> crate::async_task_queue::TaskQueueStats {
        self.task_queue.get_stats().await
    }
}
