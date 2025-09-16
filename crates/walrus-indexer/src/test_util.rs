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
use typed_store::TypedStoreError;

use crate::{AsyncTask, OrderedStore, TaskExecutor};

/// Simple test task.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TestTask {
    pub sequence: u64,
    /// A number between 0 and 100, indicating the failure rate.
    pub failure_rate: u8,
    /// A duration in milliseconds, indicating the duration of the task.
    pub duration: Duration,
}

impl TestTask {
    pub fn new(sequence: u64) -> Self {
        Self {
            sequence,
            failure_rate: 0,
            duration: Duration::from_millis(100),
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

pub struct TestTaskGenerator {
    sequence: u64,
    enable_random_failure: bool,
    enable_duration: bool,
}

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
            rand::thread_rng().gen_range(0..75) // Cap at 75% to ensure eventual success
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

/// Ordered in-memory store.
pub struct OrderedTestStore {
    tasks: Arc<Mutex<BTreeMap<u64, TestTask>>>,
}

impl OrderedTestStore {
    pub fn new() -> Self {
        Self {
            tasks: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Pre-populate the store with tasks.
    pub async fn populate_with_tasks(&self, tasks: Vec<TestTask>) {
        let mut store = self.tasks.lock().await;
        for task in tasks {
            store.insert(task.task_id(), task);
        }
    }

    /// Get current task count.
    pub async fn task_count(&self) -> usize {
        self.tasks.lock().await.len()
    }
}

impl Default for OrderedTestStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl OrderedStore<TestTask> for OrderedTestStore {
    async fn store(&self, task: &TestTask) -> Result<(), TypedStoreError> {
        let mut tasks = self.tasks.lock().await;
        tasks.insert(task.task_id(), task.clone());
        tracing::debug!("Stored task: id={}", task.task_id());
        Ok(())
    }

    async fn remove(&self, task_id: &u64) -> Result<(), TypedStoreError> {
        let mut tasks = self.tasks.lock().await;
        tasks.remove(task_id);
        Ok(())
    }

    async fn read_range(
        &self,
        from_task_id: Option<u64>,
        to_task_id: Option<u64>,
        limit: usize,
    ) -> Result<Vec<TestTask>, TypedStoreError> {
        let tasks = self.tasks.lock().await;
        let mut result = Vec::new();
        let mut count = 0;

        for (task_id, task) in tasks.iter() {
            if let Some(from) = from_task_id {
                if *task_id <= from {
                    continue;
                }
            }

            if let Some(to) = to_task_id {
                if *task_id >= to {
                    break;
                }
            }

            result.push(task.clone());
            count += 1;

            if count >= limit {
                break;
            }
        }

        tracing::debug!(
            "Read range: from_task_id={:?}, to_task_id={:?}, limit={}, returned={}",
            from_task_id,
            to_task_id,
            limit,
            result.len()
        );

        Ok(result)
    }

    async fn add_to_retry_queue(&self, task: &TestTask) -> Result<(), TypedStoreError> {
        // For testing, just store in the same map with a special prefix
        self.store(task).await
    }

    async fn read_retry_tasks(
        &self,
        from_task_id: Option<u64>,
        limit: usize,
    ) -> Result<Vec<TestTask>, TypedStoreError> {
        // For testing, use same read_range logic
        self.read_range(from_task_id, None, limit).await
    }

    async fn delete_retry_task(&self, task_id: &u64) -> Result<(), TypedStoreError> {
        self.remove(task_id).await
    }
}

pub struct TestExecutor {
    executed: Arc<AtomicU64>,
}

impl TestExecutor {
    pub fn new() -> Self {
        Self {
            executed: Arc::new(AtomicU64::new(0)),
        }
    }
}

#[async_trait]
impl TaskExecutor<TestTask> for TestExecutor {
    async fn execute(&self, task: TestTask) -> Result<()> {
        let random_number = rand::thread_rng().gen_range(0..100);
        if random_number < task.failure_rate {
            return Err(anyhow::anyhow!("Task failed"));
        }

        tokio::time::sleep(task.duration).await;

        let count = self.executed.fetch_add(1, Ordering::SeqCst) + 1;

        tracing::info!("Executed task #{}: id={}", count, task.task_id());

        Ok(())
    }
}
