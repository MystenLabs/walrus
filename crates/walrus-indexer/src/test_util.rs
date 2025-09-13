// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Test utilities for walrus-indexer tests.

use std::{collections::BTreeMap, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use typed_store::TypedStoreError;

use crate::{AsyncTask, OrderedStore};

/// Simple test task.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TestTask {
    pub sequence: u64,
}

impl TestTask {
    pub fn new(sequence: u64) -> Self {
        Self { sequence }
    }
}

impl AsyncTask for TestTask {
    type TaskId = u64;

    fn task_id(&self) -> Self::TaskId {
        self.sequence
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
}
