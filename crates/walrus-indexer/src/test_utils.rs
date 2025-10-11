// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Test utilities for walrus-indexer tests.
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::Result;
use async_trait::async_trait;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::{AsyncTask, AsyncTaskStore, AsyncTaskStoreError, TaskExecutor};

/// Simple test task.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[allow(dead_code)]
pub struct TestTask {
    pub sequence: u64,
    /// A number between 0 and 100, indicating the failure rate.
    pub failure_rate: u8,
    /// A duration in milliseconds, indicating the duration of the task.
    pub duration: Duration,
    pub is_in_retry_queue: bool,
}

#[allow(dead_code)]
impl TestTask {
    pub fn new(sequence: u64) -> Self {
        Self {
            sequence,
            failure_rate: 0,
            duration: Duration::from_millis(100),
            is_in_retry_queue: false,
        }
    }

    pub fn with_failure_rate(self, failure_rate: u8) -> Self {
        Self {
            failure_rate,
            ..self
        }
    }

    pub fn with_duration(self, duration: Duration) -> Self {
        Self { duration, ..self }
    }
}

impl AsyncTask for TestTask {
    type TaskId = u64;

    fn task_id(&self) -> Self::TaskId {
        self.sequence
    }
}

#[allow(dead_code)]
pub struct TestTaskGenerator {
    sequence: u64,
    enable_random_failure: bool,
    enable_duration: bool,
}

#[allow(dead_code)]
impl TestTaskGenerator {
    pub fn new(enable_random_failure: bool, enable_duration: bool) -> Self {
        Self {
            sequence: 0,
            enable_random_failure,
            enable_duration,
        }
    }
}

impl Iterator for TestTaskGenerator {
    type Item = TestTask;

    fn next(&mut self) -> Option<Self::Item> {
        self.sequence += 1;
        let failure_rate = if self.enable_random_failure {
            rand::thread_rng().gen_range(0..40) // Cap at 40% for more reliable test completion
        } else {
            0
        };
        let duration = if self.enable_duration {
            Duration::from_millis(rand::thread_rng().gen_range(100..1000))
        } else {
            Duration::from_millis(100)
        };
        Some(
            TestTask::new(self.sequence)
                .with_failure_rate(failure_rate)
                .with_duration(duration),
        )
    }
}

/// Storage for both regular and retry tasks.
#[allow(dead_code)]
struct TaskStorage {
    tasks: BTreeMap<u64, TestTask>,
    retry_tasks: BTreeMap<u64, TestTask>,
}

/// Ordered in-memory store.
#[allow(dead_code)]
pub struct OrderedTestStore {
    storage: Arc<Mutex<TaskStorage>>,
}

#[allow(dead_code)]
impl OrderedTestStore {
    pub fn new() -> Self {
        Self {
            storage: Arc::new(Mutex::new(TaskStorage {
                tasks: BTreeMap::new(),
                retry_tasks: BTreeMap::new(),
            })),
        }
    }

    /// Pre-populate the store with tasks.
    pub async fn populate_with_tasks(&self, tasks: Vec<TestTask>) {
        let mut storage = self.storage.lock().await;
        for task in tasks {
            storage.tasks.insert(task.task_id(), task);
        }
    }

    /// Get current task count.
    pub async fn task_count(&self) -> usize {
        let storage = self.storage.lock().await;
        let total = storage.tasks.len() + storage.retry_tasks.len();
        tracing::debug!(
            "Task count: regular={}, retry={}, total={}",
            storage.tasks.len(),
            storage.retry_tasks.len(),
            total
        );
        total
    }

    /// Helper method to read from a BTreeMap with pagination.
    fn read_from_map(
        map: &BTreeMap<u64, TestTask>,
        from_task_id: Option<u64>,
        to_task_id: Option<u64>,
        limit: usize,
    ) -> Vec<TestTask> {
        let mut result = Vec::new();
        let mut count = 0;

        for (task_id, task) in map.iter() {
            // Exclusive lower bound.
            if let Some(from) = from_task_id
                && *task_id <= from
            {
                continue;
            }

            // Exclusive upper bound (only used in read_range).
            if let Some(to) = to_task_id
                && *task_id >= to
            {
                break;
            }

            result.push(task.clone());
            count += 1;

            if count >= limit {
                break;
            }
        }

        result
    }
}

impl Default for OrderedTestStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl AsyncTaskStore<TestTask> for OrderedTestStore {
    async fn store_task(&self, task: &TestTask) -> Result<(), AsyncTaskStoreError> {
        let mut storage = self.storage.lock().await;
        storage.tasks.insert(task.task_id(), task.clone());
        tracing::debug!("Stored task: id={}", task.task_id());
        Ok(())
    }

    async fn remove_task(&self, task_id: &u64) -> Result<(), AsyncTaskStoreError> {
        let mut storage = self.storage.lock().await;
        storage.tasks.remove(task_id);
        Ok(())
    }

    async fn read_tasks(
        &self,
        from_task_id: Option<u64>,
        to_task_id: Option<u64>,
        limit: usize,
    ) -> Result<Vec<TestTask>, AsyncTaskStoreError> {
        let storage = self.storage.lock().await;
        let result = Self::read_from_map(&storage.tasks, from_task_id, to_task_id, limit);

        tracing::debug!(
            "Read range: from_task_id={:?}, to_task_id={:?}, limit={}, returned={}",
            from_task_id,
            to_task_id,
            limit,
            result.len()
        );

        Ok(result)
    }

    async fn move_to_retry_queue(&self, task: &TestTask) -> Result<(), AsyncTaskStoreError> {
        let mut storage = self.storage.lock().await;
        let mut task_clone = task.clone();
        storage.tasks.remove(&task.task_id());
        task_clone.is_in_retry_queue = true;
        storage.retry_tasks.insert(task.task_id(), task_clone);
        tracing::debug!("Added task to retry queue: id={}", task.task_id());
        Ok(())
    }

    async fn read_retry_tasks(
        &self,
        from_task_id: Option<u64>,
        limit: usize,
    ) -> Result<Vec<TestTask>, AsyncTaskStoreError> {
        let storage = self.storage.lock().await;
        let result = Self::read_from_map(&storage.retry_tasks, from_task_id, None, limit);

        tracing::debug!(
            "Read retry tasks: from_task_id={:?}, limit={}, returned={}",
            from_task_id,
            limit,
            result.len()
        );

        Ok(result)
    }

    async fn remove_retry_task(&self, task_id: &u64) -> Result<(), AsyncTaskStoreError> {
        let mut storage = self.storage.lock().await;
        storage.retry_tasks.remove(task_id);
        tracing::debug!("Deleted task from retry queue: id={}", task_id);
        Ok(())
    }
}

#[allow(dead_code)]
pub struct TestExecutor {
    executed: Arc<AtomicU64>,
    storage: Arc<OrderedTestStore>,
}

#[allow(dead_code)]
impl TestExecutor {
    pub fn new(storage: Arc<OrderedTestStore>) -> Self {
        Self {
            executed: Arc::new(AtomicU64::new(0)),
            storage,
        }
    }

    pub fn executed(&self) -> u64 {
        self.executed.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl TaskExecutor<TestTask> for TestExecutor {
    async fn execute(&self, task: TestTask) -> Result<()> {
        let random_number = rand::thread_rng().gen_range(0..100);
        if random_number < task.failure_rate {
            // If this is a retry task that failed, we need to re-add it to the retry queue
            // because it was removed before execution in async_task_manager.
            if task.is_in_retry_queue {
                // Re-add to retry queue since it was removed before execution.
                if let Err(e) = self.storage.move_to_retry_queue(&task).await {
                    tracing::error!("Failed to re-add task to retry queue: {}", e);
                }
            }
            return Err(anyhow::anyhow!("Task failed"));
        }

        tokio::time::sleep(task.duration).await;

        let count = self.executed.fetch_add(1, Ordering::SeqCst) + 1;
        tracing::info!("Executed task #{}: id={}", count, task.task_id());

        // Remove the task from storage after successful execution.
        // Note: Retry tasks are already removed from retry queue before execution
        // in async_task_manager, so we don't need to delete them here.
        if !task.is_in_retry_queue {
            self.storage.remove_task(&task.task_id()).await?;
        }

        Ok(())
    }
}
