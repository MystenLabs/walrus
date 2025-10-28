// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Walrus Indexer Library

use std::fmt::Debug;

use anyhow::Result;
use async_trait::async_trait;

mod async_task_sorter;
mod async_task_manager;
mod errors;
mod storage;
mod test_utils;

pub use errors::AsyncTaskStoreError;
pub use storage::{BlobIdentity, IndexTarget, ObjectIndexValue, Patch, WalrusIndexStore};

/// Represents a task that is persisted and executed asynchronously.
pub trait AsyncTask: Send + Sync + Clone + Debug {
    /// Unique ID for the task, tasks are sorted by their task IDs.
    type TaskId: Send + Sync + Clone + Ord + std::fmt::Debug + std::hash::Hash + Eq;

    /// Returns the task ID.
    fn task_id(&self) -> Self::TaskId;
}

/// Persistent storage for async tasks.
/// It provides the APIs for a task queue and a retry queue, where each queue is a sorted list of
/// tasks by their task IDs.
#[async_trait]
pub trait AsyncTaskStore<T>: Send + Sync
where
    T: AsyncTask,
{
    /// Stores a task.
    async fn store_task(&self, task: &T) -> Result<(), AsyncTaskStoreError>;

    /// Removes a task from storage by its ID.
    async fn remove_task(&self, task_id: &T::TaskId) -> Result<(), AsyncTaskStoreError>;

    /// Load tasks within a task_id range, ordered by task_id.
    /// Returns tasks in (from_task_id, to_task_id), **EXCLUSIVE** on both ends, with a limit.
    /// If from_task_id is None, starts from beginning. If to_task_id is None, goes to end.
    async fn read_tasks(
        &self,
        from_task_id: Option<T::TaskId>,
        to_task_id: Option<T::TaskId>,
        limit: usize,
    ) -> Result<Vec<T>, AsyncTaskStoreError>;

    /// Moves a task to the retry queue.
    async fn move_to_retry_queue(&self, task: &T) -> Result<(), AsyncTaskStoreError>;

    /// Reads tasks from retry queue starting from the given task ID.
    async fn read_retry_tasks(
        &self,
        from_task_id: Option<T::TaskId>,
        limit: usize,
    ) -> Result<Vec<T>, AsyncTaskStoreError>;

    /// Deletes a task from the retry queue.
    async fn remove_retry_task(&self, task_id: &T::TaskId) -> Result<(), AsyncTaskStoreError>;
}

/// Trait for executing tasks asynchronously.
///
/// IMPORTANT: The executor is responsible for removing successfully executed tasks
/// from storage (either from the regular queue or retry queue). This is an optimization to reduce
/// the number of transactions.
#[async_trait]
pub trait TaskExecutor<T>: Send + Sync
where
    T: AsyncTask,
{
    /// Execute a task.
    ///
    /// The implementation MUST:
    /// 1. Execute the task
    /// 2. If successful, remove the task from storage
    /// 3. Return Ok(()) only after both execution and removal succeed
    async fn execute(&self, task: T) -> Result<()>;
}
