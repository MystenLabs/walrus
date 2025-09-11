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

    fn sequence_number(&self) -> u64 {
        self.sequence
    }
}

/// Ordered in-memory store.
pub struct OrderedTestStore {
    tasks: Arc<Mutex<BTreeMap<u64, TestTask>>>,
    // Track read_range calls.
    read_calls: Arc<Mutex<Vec<(Option<u64>, Option<u64>, usize)>>>,
}

impl OrderedTestStore {
    pub fn new() -> Self {
        Self {
            tasks: Arc::new(Mutex::new(BTreeMap::new())),
            read_calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Pre-populate the store with tasks.
    pub async fn populate_with_tasks(&self, tasks: Vec<TestTask>) {
        let mut store = self.tasks.lock().await;
        for task in tasks {
            store.insert(task.sequence_number(), task);
        }
    }

    /// Get read call history for testing.
    pub async fn get_read_calls(&self) -> Vec<(Option<u64>, Option<u64>, usize)> {
        self.read_calls.lock().await.clone()
    }

    /// Clear read call history.
    #[allow(dead_code)]
    pub async fn clear_read_calls(&self) {
        self.read_calls.lock().await.clear();
    }

    /// Get current task count.
    pub async fn task_count(&self) -> usize {
        self.tasks.lock().await.len()
    }
}

#[async_trait]
impl OrderedStore<TestTask> for OrderedTestStore {
    async fn store(&self, task: &TestTask) -> Result<(), TypedStoreError> {
        let mut tasks = self.tasks.lock().await;
        tasks.insert(task.sequence_number(), task.clone());
        tracing::debug!("Stored task: seq={}", task.sequence_number());
        Ok(())
    }

    async fn remove(&self, task_id: &u64) -> Result<(), TypedStoreError> {
        let mut tasks = self.tasks.lock().await;
        tasks.remove(task_id);
        Ok(())
    }

    async fn read_range(
        &self,
        from_seq: Option<u64>,
        to_seq: Option<u64>,
        limit: usize,
    ) -> Result<Vec<TestTask>, TypedStoreError> {
        {
            let mut calls = self.read_calls.lock().await;
            calls.push((from_seq, to_seq, limit));
        }

        let tasks = self.tasks.lock().await;
        let mut result = Vec::new();
        let mut count = 0;

        for (seq, task) in tasks.iter() {
            if let Some(from) = from_seq {
                if *seq < from {
                    continue;
                }
            }

            if let Some(to) = to_seq {
                if *seq >= to {
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
            "Read range: from_seq={:?}, to_seq={:?}, limit={}, returned={}",
            from_seq,
            to_seq,
            limit,
            result.len()
        );

        Ok(result)
    }
}
