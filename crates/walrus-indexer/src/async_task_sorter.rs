// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! A persistent queue that transparently handles disk storage and memory caching.

use std::{collections::VecDeque, sync::Arc};

use anyhow::Result;
use thiserror::Error;
use tokio::{
    sync::{Notify, RwLock},
    time::{Duration, Instant},
};

/// Error type for queue buffer operations.
#[derive(Error, Debug)]
enum BufferError<T: AsyncTask> {
    #[error("Catchup queue is empty, need to load from storage")]
    CatchupQueueEmpty(Option<T::TaskId>, Option<T::TaskId>),
}

// Import traits from lib.rs
use crate::{AsyncTask, OrderedStore};

/// Configuration for the async task sorter.
#[derive(Debug, Clone)]
pub struct AsyncTaskSorterConfig {
    /// Maximum number of tasks to keep in latest queue.
    pub max_num_latest_tasks: usize,
    /// Batch size when loading from disk.
    pub load_batch_size: usize,
    /// Grace period after catch-up to prevent drops.
    pub grace_period: Duration,
}

impl Default for AsyncTaskSorterConfig {
    fn default() -> Self {
        Self {
            max_num_latest_tasks: 1000,
            load_batch_size: 100,
            grace_period: Duration::from_secs(60),
        }
    }
}

/// Internal state of the queue.
#[derive(Debug, Clone, PartialEq)]
enum QueueState {
    /// Queue is catching up with persisted items.
    CatchingUp,
    /// Queue is up-to-date with real-time items.
    UpToDate,
}

/// Two-queue memory buffer system for async tasks.
struct TaskBuffer<T: AsyncTask> {
    /// Lock for the buffer.
    lock: std::sync::Mutex<()>,
    /// Queue for tasks loaded from disk during catch-up.
    catchup_queue: VecDeque<T>,
    /// Queue for latest tasks (real-time).
    latest_queue: VecDeque<T>,
    /// Highest task_id seen in catchup queue.
    last_catchup_task_id: Option<T::TaskId>,
    /// Grace period start time.
    grace_period_start: Option<Instant>,
    /// State of the queue.
    state: QueueState,
}

impl<T: AsyncTask> TaskBuffer<T> {
    fn new() -> Self {
        Self {
            lock: std::sync::Mutex::new(()),
            catchup_queue: VecDeque::new(),
            latest_queue: VecDeque::new(),
            last_catchup_task_id: None,
            grace_period_start: Some(Instant::now()),
            state: QueueState::CatchingUp,
        }
    }

    /// Push a task to the latest queue, potentially dropping old tasks if at capacity.
    fn push_to_latest(&mut self, task: T, max_size: usize, grace_period: Duration) {
        let _lock = self.lock.lock().expect("Failed to lock buffer");
        // Check if we're in grace period.
        let in_grace_period = self
            .grace_period_start
            .map(|start| start.elapsed() < grace_period)
            .unwrap_or(false);

        // Only drop items if not in grace period and at capacity.
        while !in_grace_period && self.latest_queue.len() >= max_size {
            self.latest_queue
                .pop_front()
                .expect("Failed to pop oldest task");
            self.state = QueueState::CatchingUp;
        }

        self.latest_queue.push_back(task);
    }

    /// Add newly loaded tasks to the buffer.
    /// Tasks should be pre-sorted by task_id.
    fn add_loaded_tasks(&mut self, tasks: Vec<T>) {
        let _lock = self.lock.lock().expect("Failed to lock buffer");
        assert!(self.state == QueueState::CatchingUp);
        if tasks.is_empty() {
            self.state = QueueState::UpToDate;
            self.grace_period_start = Some(Instant::now());
            return;
        }

        let max_task_id = tasks.last().expect("Tasks is not empty").task_id();
        let up_to_date = self
            .latest_queue
            .front()
            .is_some_and(|task| task.task_id() <= max_task_id);
        if up_to_date {
            let upper_task_id = self
                .latest_queue
                .front()
                .expect("Latest queue is not empty")
                .task_id();
            tasks.iter().rev().for_each(|task| {
                if task.task_id() < upper_task_id {
                    self.latest_queue.push_front(task.clone());
                }
            });
            self.state = QueueState::UpToDate;
            self.grace_period_start = Some(Instant::now());
        } else {
            // Update last_catchup_task_id with the max task_id we're adding.
            self.last_catchup_task_id = Some(max_task_id.clone());
            self.catchup_queue.extend(tasks);
        }
    }

    /// Pull the task with the smallest task_id from either queue.
    /// Prioritizes catchup queue over latest queue.
    /// Returns Err(BufferError::CatchupQueueEmpty) when in CatchingUp state, catchup queue
    /// is empty, and latest queue is also empty (indicating we should try loading from storage).
    fn pull(&mut self) -> Result<Option<T>, BufferError<T>> {
        let _lock = self.lock.lock().expect("Failed to lock buffer");

        // If we're up to date, only use latest queue.
        if self.state == QueueState::UpToDate {
            return Ok(self.latest_queue.pop_front().inspect(|task| {
                self.last_catchup_task_id = Some(task.task_id().clone());
            }));
        }

        // If we're catching up, prioritize catchup queue.
        if !self.catchup_queue.is_empty() {
            assert!(self.state == QueueState::CatchingUp);
            let task = self.catchup_queue.pop_front();
            return Ok(task);
        }

        // Both queues empty, need to load more - provide the range hints.
        let from_task_id = self.last_catchup_task_id.clone();
        let to_task_id = self.latest_queue.front().map(|task| task.task_id().clone());
        Err(BufferError::CatchupQueueEmpty(from_task_id, to_task_id))
    }

    /// Check if both queues are empty.
    fn is_empty(&self) -> bool {
        let _lock = self.lock.lock().expect("Failed to lock buffer");
        self.catchup_queue.is_empty()
            && self.latest_queue.is_empty()
            && self.state == QueueState::UpToDate
    }
}

/// A persistent queue with two-queue approach for catch-up and real-time processing.
///
/// This queue:
/// - Loads tasks from disk when lagging behind.
/// - Accepts real-time tasks, and persists them to disk.
/// - Guarantees that tasks are processed in order of their sequence numbers.
pub struct AsyncTaskSorter<T, S>
where
    T: AsyncTask,
    S: OrderedStore<T>,
{
    config: AsyncTaskSorterConfig,
    store: Arc<S>,
    buffer: Arc<RwLock<TaskBuffer<T>>>,
    /// Notifier for when new items are available.
    item_available: Arc<Notify>,
    /// Background task handle for loading tasks from storage.
    background_task_handle: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
}

impl<T, S> AsyncTaskSorter<T, S>
where
    T: AsyncTask + 'static,
    S: OrderedStore<T> + 'static,
{
    /// Create a new persistent queue.
    pub async fn new(config: AsyncTaskSorterConfig, store: Arc<S>) -> Self {
        Self {
            config,
            store,
            buffer: Arc::new(RwLock::new(TaskBuffer::new())),
            item_available: Arc::new(Notify::new()),
            background_task_handle: Arc::new(RwLock::new(None)),
        }
    }

    pub fn init(&self) {
        // Spawn a background task to load tasks from the store with a specific range.
        self.spawn_background_load_task(None, None);
    }

    /// Push a task to the queue.
    pub async fn persist_and_enqueue_task(&self, task: T) -> Result<()> {
        self.store.store(&task).await?;

        // Add to latest queue.
        let mut buffer = self.buffer.write().await;
        buffer.push_to_latest(
            task,
            self.config.max_num_latest_tasks,
            self.config.grace_period,
        );

        // Notify waiting pullers.
        self.item_available.notify_waiters();

        Ok(())
    }

    /// Try to get the next task from the queue without blocking.
    /// Returns None immediately if the queue is empty.
    pub async fn get_next_task(&self) -> Result<Option<T>> {
        loop {
            // First try to get the next task from memory.
            let pull_result = {
                let mut buffer = self.buffer.write().await;
                buffer.pull()
            };

            match pull_result {
                Ok(result) => {
                    return Ok(result);
                }
                Err(BufferError::CatchupQueueEmpty(from_task_id, to_task_id)) => {
                    // Catchup queue is empty, trigger background loading task.
                    self.spawn_background_load_task(from_task_id, to_task_id);

                    // Wait for notification that items are available.
                    self.item_available.notified().await;
                }
            }
        }
    }

    /// Spawn a background task to load tasks from the store with a specific range.
    fn spawn_background_load_task(
        &self,
        from_task_id: Option<T::TaskId>,
        to_task_id: Option<T::TaskId>,
    ) {
        // Check if there's already a running background task.
        if let Ok(mut handle_guard) = self.background_task_handle.try_write() {
            // Clean up finished task.
            if let Some(ref handle) = *handle_guard {
                if handle.is_finished() {
                    *handle_guard = None;
                }
            }

            // If there's still a running task, don't spawn another one.
            if handle_guard.is_some() {
                tracing::debug!("Background loading already in progress, skipping new task spawn");
                return;
            }
        } else {
            tracing::warn!("Failed to check background task handle - unable to acquire write lock");
            return;
        }

        let store = Arc::clone(&self.store);
        let config = self.config.clone();
        let buffer = Arc::clone(&self.buffer);
        let item_available = Arc::clone(&self.item_available);

        let handle = tokio::spawn(async move {
            // Load tasks from storage with the provided range.
            let result = store
                .read_range(
                    from_task_id.clone(),
                    to_task_id.clone(),
                    config.load_batch_size,
                )
                .await;

            match result {
                Ok(tasks) => {
                    tracing::debug!(
                        "Loaded {} tasks from storage (from: {:?}, to: {:?})",
                        tasks.len(),
                        from_task_id,
                        to_task_id
                    );

                    let mut buffer_guard = buffer.write().await;
                    buffer_guard.add_loaded_tasks(tasks);

                    // Notify waiting pullers that tasks are now available.
                    item_available.notify_waiters();
                }
                Err(e) => {
                    tracing::error!("Background load task failed: {}", e);
                }
            }
        });

        // Store the task handle for proper cleanup.
        if let Ok(mut handle_guard) = self.background_task_handle.try_write() {
            *handle_guard = Some(handle);
        } else {
            tracing::warn!("Failed to store background task handle - unable to acquire write lock");
        }
    }

    /// Check if the queue is empty (in memory).
    pub async fn is_empty(&self) -> bool {
        self.buffer.read().await.is_empty()
    }

    /// Check if the queue is up to date.
    pub async fn is_up_to_date(&self) -> bool {
        let buffer = self.buffer.read().await;
        matches!(buffer.state, QueueState::UpToDate)
    }

    /// Shutdown the task sorter and clean up background task.
    pub async fn shutdown(&self) {
        let mut handle_guard = self.background_task_handle.write().await;

        // Cancel the background task if it exists.
        if let Some(handle) = handle_guard.take() {
            handle.abort();
            let _ = handle.await; // Ignore cancellation errors.
        }

        tracing::info!("AsyncTaskSorter shutdown completed");
    }
}

#[cfg(test)]
mod tests {
    use walrus_test_utils::async_param_test;

    use super::*;
    use crate::test_util::{OrderedTestStore, TestTask};

    async_param_test! {
        test_async_task_sorter_basic: [
            no_tasks: (3, 0, 2, 0),
            one_batch_catchup_no_drops: (3, 2, 2, 1),
            two_batchs_catchup_no_drops: (3, 4, 2, 1),
            one_batch_catchup_with_drops: (3, 2, 2, 3),
            two_batches_catchup_with_drops: (3, 4, 2, 3),
            two_batches_catchup_no_updates: (3, 4, 2, 0),
        ]
    }
    /// Parameterized test for AsyncTaskSorter with various configurations.
    async fn test_async_task_sorter_basic(
        load_batch_size: usize,
        num_existing_tasks: usize,
        max_num_latest_tasks: usize,
        num_new_tasks: usize,
    ) {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_test_writer()
            .try_init();

        let config = AsyncTaskSorterConfig {
            max_num_latest_tasks,
            load_batch_size,
            grace_period: Duration::from_millis(50),
        };

        let store = Arc::new(OrderedTestStore::new());

        // Pre-populate store with existing tasks (numbered 0, 1, 2, ...).
        if num_existing_tasks > 0 {
            let existing_tasks: Vec<TestTask> = (0..num_existing_tasks)
                .map(|i| TestTask::new(i as u64))
                .collect();
            store.populate_with_tasks(existing_tasks).await;
        }

        // Construct the AsyncTaskSorter.
        let sorter = AsyncTaskSorter::new(config, store.clone()).await;

        // Add new tasks.
        let new_tasks: Vec<TestTask> = (num_existing_tasks..(num_existing_tasks + num_new_tasks))
            .map(|i| TestTask::new(i as u64))
            .collect();

        for task in &new_tasks {
            sorter
                .persist_and_enqueue_task(task.clone())
                .await
                .expect("Failed to persist and enqueue task");
        }

        // Get all tasks.
        let total_expected = num_existing_tasks + num_new_tasks;
        for i in 0..total_expected {
            match sorter.get_next_task().await {
                Ok(Some(task)) => {
                    assert_eq!(task.task_id(), i as u64);
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Ok(None) => {
                    tracing::info!("No more tasks available at iteration {}", i);
                    break;
                }
                Err(e) => {
                    tracing::warn!("Error getting next task: {}", e);
                    break;
                }
            }
        }

        // Try to get one more task to ensure background loading process completes.
        // This should trigger the final loading attempt that finds no more tasks.
        match sorter.get_next_task().await {
            Ok(None) => {
                // Timeout is acceptable - it means no more tasks are coming.
            }
            _ => {
                panic!("Unexpected task found after all tasks should have been retrieved");
            }
        }
    }

    #[tokio::test]
    async fn test_async_task_sorter_recovery() -> Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_test_writer()
            .try_init();

        let config = AsyncTaskSorterConfig {
            max_num_latest_tasks: 3,
            load_batch_size: 2,
            grace_period: Duration::from_millis(50),
        };

        let store = Arc::new(OrderedTestStore::new());

        let sorter = AsyncTaskSorter::new(config, store.clone()).await;
        sorter.init();
        tokio::time::sleep(Duration::from_millis(100)).await;
        // Should be up-to-date initially.
        assert!(sorter.is_up_to_date().await);

        let mut cur = 0;
        let mut read = 0;
        sorter
            .persist_and_enqueue_task(TestTask::new(cur))
            .await
            .expect("Failed to submit task");
        cur += 1;
        sorter
            .persist_and_enqueue_task(TestTask::new(cur))
            .await
            .expect("Failed to submit task");

        assert_eq!(
            sorter
                .get_next_task()
                .await
                .expect("Failed to get next task"),
            Some(TestTask::new(read))
        );
        read += 1;

        assert!(sorter.is_up_to_date().await);

        for _ in 0..5 {
            sorter
                .persist_and_enqueue_task(TestTask::new(cur))
                .await
                .expect("Failed to submit task");
            cur += 1;
        }

        assert!(!sorter.is_up_to_date().await);

        while let Some(task) = sorter
            .get_next_task()
            .await
            .expect("Failed to get next task")
        {
            assert_eq!(task.task_id(), read);
            read += 1;
        }

        assert_eq!(read, cur);

        assert!(sorter.is_up_to_date().await);

        Ok(())
    }
}
