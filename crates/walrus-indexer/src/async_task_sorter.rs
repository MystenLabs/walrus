// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! A simple queue APIs for async tasks.
#![allow(dead_code)]
use std::{collections::VecDeque, sync::Arc};

use anyhow::Result;
use tokio::sync::Notify;

use crate::{AsyncTask, AsyncTaskStore};

/// Configuration for the async task sorter.
#[derive(Debug, Clone)]
pub struct AsyncTaskSorterConfig {
    /// Maximum number of tasks to keep in latest queue.
    pub latest_queue_max: usize,
    /// Maximum number of tasks to keep in catchup queue.
    pub catchup_queue_max: usize,
    /// The number of tasks to drop when latest queue is full.
    pub drop_batch_size: usize,
}

impl AsyncTaskSorterConfig {
    /// Calculate the threshold for triggering background fetch.
    /// Returns catchup_queue_max / 2.
    pub fn fetch_threshold(&self) -> usize {
        self.catchup_queue_max / 2
    }
}

impl Default for AsyncTaskSorterConfig {
    fn default() -> Self {
        Self {
            latest_queue_max: 300,
            catchup_queue_max: 200,
            drop_batch_size: 10,
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

/// Internal state for the task buffer.
struct TaskBufferInner<T: AsyncTask> {
    /// Queue for oldest tasks, loaded from disk during catch-up.
    catchup_queue: VecDeque<T>,
    /// Queue for latest tasks, where the oldest tasks are purged when at capacity.
    latest_queue: VecDeque<T>,
    /// Highest task_id seen in catchup queue (for tracking fetch progress).
    catchup_low_watermark: Option<T::TaskId>,
    /// State of the queue.
    state: QueueState,
    /// Background task handle for loading tasks from storage.
    background_task_handle: Option<tokio::task::JoinHandle<()>>,
    /// Notifies when background fetch completes.
    fetch_completed: Arc<Notify>,
    /// Notifies when new tasks are enqueued to latest queue.
    item_enqueued: Arc<Notify>,
}

impl<T: AsyncTask> TaskBufferInner<T> {
    fn new() -> Self {
        Self {
            catchup_queue: VecDeque::new(),
            latest_queue: VecDeque::new(),
            catchup_low_watermark: None,
            state: QueueState::CatchingUp,
            background_task_handle: None,
            fetch_completed: Arc::new(Notify::new()),
            item_enqueued: Arc::new(Notify::new()),
        }
    }

    /// Get item_enqueued notification.
    fn get_item_enqueued(&self) -> Arc<Notify> {
        Arc::clone(&self.item_enqueued)
    }

    /// Get fetch_completed notification.
    fn get_fetch_completed(&self) -> Arc<Notify> {
        Arc::clone(&self.fetch_completed)
    }

    /// Drop tasks from the latest queue.
    /// In CatchingUp state, drop tasks directly.
    /// In UpToDate state, try to move tasks to catchup queue if it has space, otherwise drop tasks,
    /// and transition to CatchingUp state.
    fn drop_tasks(&mut self, drop_batch_size: usize, catchup_queue_max: usize) {
        let mut drop_batch_size = drop_batch_size;
        while !self.latest_queue.is_empty() && drop_batch_size > 0 {
            let Some(task) = self.latest_queue.pop_front() else {
                break;
            };
            if self.state == QueueState::CatchingUp {
                tracing::debug!(?task, "dropping task in catching up state");
            } else if self.catchup_queue.len() >= catchup_queue_max {
                tracing::debug!(?task, "dropping task in catchup queue full state");
                self.state = QueueState::CatchingUp;
            } else {
                tracing::debug!(?task, "moving task to catchup queue");
                self.catchup_low_watermark = Some(task.task_id().clone());
                self.catchup_queue.push_back(task);
            }
            drop_batch_size -= 1;
        }
    }

    /// Add newly loaded tasks to the buffer.
    /// Tasks should be pre-sorted by task_id from storage.
    fn add_loaded_tasks(&mut self, mut tasks: Vec<T>) {
        if tasks.is_empty() {
            // Empty fetch means we've caught up.
            self.state = QueueState::UpToDate;
            tracing::debug!("empty fetch means we've caught up");
            return;
        }

        // Get boundary IDs for filtering.
        let last_catchup_id = self.catchup_queue.back().map(|t| t.task_id());
        let first_latest_id = self.latest_queue.front().map(|t| t.task_id());

        // Find the valid range of tasks to add.
        // Skip tasks that are duplicates or out-of-order.
        let skip_count = if let Some(ref last_id) = last_catchup_id {
            tasks
                .iter()
                .position(|t| t.task_id() > *last_id)
                .unwrap_or(tasks.len())
        } else {
            0
        };

        if skip_count > 0 {
            tracing::warn!(
                skip_count,
                ?last_catchup_id,
                "skipping duplicate or out-of-order tasks"
            );
            tasks.drain(..skip_count);
        }

        if tasks.is_empty() {
            return;
        }

        // Find where we catch up to latest_queue.
        let take_count = if let Some(ref first_id) = first_latest_id {
            tasks
                .iter()
                .position(|t| t.task_id() >= *first_id)
                .unwrap_or(tasks.len())
        } else {
            tasks.len()
        };

        // Check if we've caught up.
        if take_count < tasks.len() {
            let caught_up_id = tasks[take_count].task_id();
            self.state = QueueState::UpToDate;
            tracing::debug!(
                ?caught_up_id,
                ?first_latest_id,
                "caught up to latest queue, transitioning to UpToDate"
            );
        }

        // Truncate to valid range.
        tasks.truncate(take_count);

        if !tasks.is_empty() {
            // Update watermark with the last task's ID.
            self.catchup_low_watermark = Some(tasks.last().unwrap().task_id().clone());

            tracing::debug!(
                num_tasks = tasks.len(),
                first_task_id = ?tasks.first().unwrap().task_id(),
                last_task_id = ?tasks.last().unwrap().task_id(),
                "adding tasks to catchup queue"
            );

            // Batch add all tasks at once.
            self.catchup_queue.extend(tasks);
        }
    }
}

/// In-memory buffer for async tasks.
/// It maintains two queues:
/// - catchup_queue: for oldest tasks, loaded from disk during catch-up.
/// - latest_queue: for latest tasks, where the oldest tasks are purged when at capacity.
///
/// This buffer automatically prefetches tasks from storage to catchup queue when it is lagging
/// behind.
struct TaskBuffer<T, S>
where
    T: AsyncTask,
    S: AsyncTaskStore<T>,
{
    /// Inner state protected by mutex, wrapped in Arc for sharing with background tasks.
    inner: Arc<std::sync::Mutex<TaskBufferInner<T>>>,
    /// Config for the buffer.
    config: AsyncTaskSorterConfig,
    /// Storage backend for loading tasks.
    store: Arc<S>,
}

impl<T, S> TaskBuffer<T, S>
where
    T: AsyncTask + 'static,
    S: AsyncTaskStore<T> + 'static,
{
    fn new(config: AsyncTaskSorterConfig, store: Arc<S>) -> Self {
        Self {
            inner: Arc::new(std::sync::Mutex::new(TaskBufferInner::new())),
            config,
            store,
        }
    }

    /// Push a task to the latest queue, potentially moving overflow to catchup queue.
    /// TaskBuffer is always operating in async mode, even if it is in up-to-date state, tasks are
    /// added to the latest queue, and pulled by the puller later.
    /// When latest_queue overflows:
    /// - If catchup_queue has space: move old tasks to catchup queue (stay UpToDate)
    /// - If catchup_queue is full: drop tasks and transition to CatchingUp
    fn push_to_latest(&self, task: T) {
        let mut inner = self.inner.lock().expect("failed to lock buffer");

        // Validate: new task must have higher task_id than current highest.
        if let Some(last) = inner.latest_queue.back()
            && task.task_id() <= last.task_id()
        {
            tracing::warn!(?task, ?last, "out-of-order task, could be a duplicate");
            return;
        }

        // Handle overflow when at capacity.
        if inner.latest_queue.len() >= self.config.latest_queue_max {
            inner.drop_tasks(self.config.drop_batch_size, self.config.catchup_queue_max);
        }

        tracing::debug!(?task, "added task to latest queue");
        inner.latest_queue.push_back(task);

        inner.item_enqueued.notify_one();
    }

    /// Pull the next task from the buffer, waiting asynchronously if necessary.
    /// This method handles all waiting internally and never returns an error.
    /// Returns None only when explicitly requested (future enhancement).
    async fn pull(&self) -> Option<T> {
        loop {
            // Try to get a task from queues and determine if we need to wait.
            let (notify, should_fetch, from_task_id, to_task_id) = {
                let mut inner = self.inner.lock().expect("failed to lock buffer");

                if inner.state == QueueState::UpToDate {
                    if let Some(task) = inner.catchup_queue.pop_front() {
                        tracing::debug!(?task, "popped task from catchup queue");
                        return Some(task);
                    }
                    if let Some(task) = inner.latest_queue.pop_front() {
                        tracing::debug!(?task, "popped task from latest queue");
                        return Some(task);
                    }
                    // No tasks available, wait for new tasks.
                    (inner.get_item_enqueued(), false, None, None)
                } else if let Some(task) = inner.catchup_queue.pop_front() {
                    let should_fetch = inner.catchup_queue.len() <= self.config.fetch_threshold();
                    let from_task_id = if should_fetch {
                        inner.catchup_low_watermark.clone()
                    } else {
                        None
                    };
                    let to_task_id = if should_fetch {
                        inner.latest_queue.front().map(|t| t.task_id().clone())
                    } else {
                        None
                    };

                    tracing::debug!(?task, "popped task from catchup queue");
                    // Return immediately after releasing the lock
                    drop(inner);
                    if should_fetch {
                        self.trigger_fetch(from_task_id, to_task_id);
                    }
                    return Some(task);
                } else {
                    tracing::debug!("no tasks in catchup queue, waiting for fetch");
                    let from_task_id = inner.catchup_low_watermark.clone();
                    let to_task_id = inner.latest_queue.front().map(|t| t.task_id().clone());
                    (inner.get_fetch_completed(), true, from_task_id, to_task_id)
                }
            };

            if should_fetch {
                self.trigger_fetch(from_task_id, to_task_id);
            }

            // Wait for notification.
            notify.notified().await;
        }
    }

    /// Spawn a background fetch task if needed and not already running.
    fn trigger_fetch(&self, from_task_id: Option<T::TaskId>, to_task_id: Option<T::TaskId>) {
        // Skip fetch if from_task_id is greater than to_task_id.
        if let (Some(from_task_id), Some(to_task_id)) = (from_task_id.as_ref(), to_task_id.as_ref())
            && from_task_id >= to_task_id
        {
            tracing::debug!("from_task_id >= to_task_id, skipping fetch");
            return;
        }

        // Check conditions and calculate fetch size while holding the lock briefly
        let fetch_size = {
            let inner = self.inner.lock().expect("failed to lock buffer");

            // Skip fetch if already running.
            if let Some(handle) = &inner.background_task_handle
                && !handle.is_finished()
            {
                tracing::debug!("background fetch already in progress");
                return;
            }

            // Calculate fetch size based on available space in catchup queue.
            let fetch_size = self
                .config
                .catchup_queue_max
                .saturating_sub(inner.catchup_queue.len());

            // Skip fetch if catchup queue is full.
            if fetch_size == 0 {
                tracing::debug!("catchup queue is full, skipping fetch");
                return;
            }

            fetch_size
        };
        // Lock is released here

        let store = Arc::clone(&self.store);
        let inner_arc = Arc::clone(&self.inner);
        let from_task_id_clone = from_task_id.clone();
        let to_task_id_clone = to_task_id.clone();

        // Spawn the background task without holding the lock
        let handle = tokio::spawn(async move {
            let result = store
                .read_tasks(from_task_id_clone, to_task_id_clone, fetch_size)
                .await;

            let mut inner_guard = inner_arc.lock().expect("failed to lock buffer");

            match result {
                Ok(tasks) => {
                    tracing::debug!(
                        num_tasks = tasks.len(),
                        ?from_task_id,
                        ?to_task_id,
                        "loaded tasks from storage"
                    );
                    inner_guard.add_loaded_tasks(tasks);
                }
                Err(e) => {
                    tracing::error!(error = ?e, "background load task failed");
                }
            }

            inner_guard.background_task_handle.take();
            // Notify waiting pullers.
            inner_guard.fetch_completed.notify_one();
        });

        // Re-acquire the lock to store the handle
        {
            let mut inner = self.inner.lock().expect("failed to lock buffer");
            inner.background_task_handle = Some(handle);
        }
    }

    /// Check if both queues are empty.
    fn is_empty(&self) -> bool {
        let inner = self.inner.lock().expect("failed to lock buffer");
        tracing::debug!(
            catchup_queue_len = ?inner.catchup_queue.len(),
            latest_queue_len = ?inner.latest_queue.len(),
            state = ?inner.state,
            "checking if buffer is empty"
        );
        inner.catchup_queue.is_empty()
            && inner.latest_queue.is_empty()
            && inner.state == QueueState::UpToDate
    }
}

/// Provides a simple queue API for async tasks.
///
/// This AsyncTaskSorter:
/// - Loads tasks from disk when lagging behind.
/// - Accepts real-time tasks, and persists them to disk.
/// - Guarantees that tasks are processed in order of their task IDs.
pub struct AsyncTaskSorter<T, S>
where
    T: AsyncTask,
    S: AsyncTaskStore<T>,
{
    store: Arc<S>,
    buffer: Arc<TaskBuffer<T, S>>,
}

impl<T, S> AsyncTaskSorter<T, S>
where
    T: AsyncTask + 'static,
    S: AsyncTaskStore<T> + 'static,
{
    /// Create a new task sorter.
    pub async fn new(config: AsyncTaskSorterConfig, store: Arc<S>) -> Self {
        Self {
            store: Arc::clone(&store),
            buffer: Arc::new(TaskBuffer::new(config, store)),
        }
    }

    /// Initialize the sorter by triggering initial catchup fetch.
    pub fn init(&self) {
        self.buffer.trigger_fetch(None, None);
    }

    /// Enqueue a task to the queue, the task will be persisted to storage.
    pub async fn enqueue_task(&self, task: T) -> Result<()> {
        self.store.store_task(&task).await?;
        self.buffer.push_to_latest(task);
        Ok(())
    }

    /// Get the next task from the queue, waiting if necessary.
    ///
    /// This method handles all waiting internally via the buffer.
    /// It will block until a task is available.
    ///
    /// **Important**: this is supposed to be used by a single consumer.
    pub async fn get_next_task(&self) -> Result<Option<T>> {
        Ok(self.buffer.pull().await)
    }

    /// Check if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Check if the queue is up-to-date.
    pub fn is_up_to_date(&self) -> bool {
        let inner = self.buffer.inner.lock().expect("failed to lock buffer");
        matches!(inner.state, QueueState::UpToDate)
    }

    /// Shutdown the task sorter and clean up background task.
    pub async fn shutdown(&self) {
        let handle = {
            let mut inner = self.buffer.inner.lock().expect("failed to lock buffer");
            inner.background_task_handle.take()
        };

        // Cancel the background task if it exists.
        if let Some(handle) = handle {
            handle.abort();
            let _ = handle.await; // Ignore cancellation errors.
        }

        tracing::info!("AsyncTaskSorter shutdown completed");
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use walrus_test_utils::async_param_test;

    use super::*;
    use crate::test_utils::{OrderedTestStore, TestTask};

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
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_test_writer()
            .try_init();

        let config = AsyncTaskSorterConfig {
            latest_queue_max: max_num_latest_tasks,
            catchup_queue_max: load_batch_size * 2,
            drop_batch_size: 1,
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
                .enqueue_task(task.clone())
                .await
                .expect("Failed to persist and enqueue task");
        }

        sorter.init();
        tokio::time::sleep(Duration::from_secs(10)).await;

        // Get all tasks.
        let total_expected = num_existing_tasks + num_new_tasks;
        for i in 0..total_expected {
            match sorter.get_next_task().await {
                Ok(Some(task)) => {
                    assert_eq!(task.task_id(), i as u64);
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Ok(None) => {
                    tracing::debug!("No more tasks available at iteration {}", i);
                    break;
                }
                Err(e) => {
                    tracing::warn!("Error getting next task: {}", e);
                    break;
                }
            }
        }

        assert!(sorter.is_up_to_date());
        assert!(sorter.is_empty());
    }

    #[tokio::test]
    async fn test_async_task_sorter_recovery() -> Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_test_writer()
            .try_init();

        let config = AsyncTaskSorterConfig {
            latest_queue_max: 3,
            catchup_queue_max: 4,
            drop_batch_size: 1,
        };

        let store = Arc::new(OrderedTestStore::new());

        let sorter = AsyncTaskSorter::new(config, store.clone()).await;
        sorter.init();
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Should be up-to-date initially.
        assert!(sorter.is_up_to_date());

        let mut cur = 0;
        let mut read = 0;
        for _ in 0..2 {
            sorter
                .enqueue_task(TestTask::new(cur))
                .await
                .expect("Failed to submit task");
            cur += 1;
        }

        assert_eq!(
            sorter
                .get_next_task()
                .await
                .expect("Failed to get next task"),
            Some(TestTask::new(read))
        );
        read += 1;

        assert!(sorter.is_up_to_date());

        for _ in 0..10 {
            sorter
                .enqueue_task(TestTask::new(cur))
                .await
                .expect("Failed to submit task");
            cur += 1;
        }

        assert!(!sorter.is_up_to_date());

        while read < cur {
            let task = sorter.get_next_task().await;
            if let Ok(Some(task)) = task {
                assert_eq!(task.task_id(), read);
                read += 1;
            } else {
                tracing::warn!("Error getting next task: {:?}", task);
                break;
            }
        }

        assert_eq!(read, cur);
        assert!(sorter.is_empty());
        assert!(sorter.is_up_to_date());

        Ok(())
    }
}
