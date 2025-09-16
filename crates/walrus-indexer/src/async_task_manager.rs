// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Async task manager built on top of PersistentQueue.

use std::sync::Arc;

use anyhow::Result;
// async_trait is now used via lib.rs imports.
use serde::{Deserialize, Serialize};
use tokio::{sync::Mutex, time::Duration};

use crate::{
    AsyncTask,
    OrderedStore,
    TaskExecutor,
    async_task_sorter::{AsyncTaskSorter, AsyncTaskSorterConfig},
};

/// Handles retry queue processing with channel-based signaling.
struct RetryQueueHandler<T, S, E>
where
    T: AsyncTask,
    S: OrderedStore<T>,
    E: TaskExecutor<T>,
{
    store: Arc<S>,
    executor: Arc<E>,
    config: AsyncTaskManagerConfig,
    signal_receiver: tokio::sync::mpsc::UnboundedReceiver<()>,
    shutdown_token: tokio_util::sync::CancellationToken,
    task_handle: Option<tokio::task::JoinHandle<()>>, // The actual retry processing task.
    retry_needed: Arc<Mutex<bool>>,                   // Shared flag for retry task to check.
    _phantom: std::marker::PhantomData<T>,
}

impl<T, S, E> RetryQueueHandler<T, S, E>
where
    T: AsyncTask + 'static,
    S: OrderedStore<T> + 'static,
    E: TaskExecutor<T> + 'static,
{
    fn new(
        store: Arc<S>,
        executor: Arc<E>,
        config: AsyncTaskManagerConfig,
        signal_receiver: tokio::sync::mpsc::UnboundedReceiver<()>,
        shutdown_token: tokio_util::sync::CancellationToken,
    ) -> Self {
        Self {
            store,
            executor,
            config,
            signal_receiver,
            shutdown_token,
            task_handle: None,
            retry_needed: Arc::new(Mutex::new(false)),
            _phantom: std::marker::PhantomData,
        }
    }

    fn spawn(
        store: Arc<S>,
        executor: Arc<E>,
        config: AsyncTaskManagerConfig,
        signal_receiver: tokio::sync::mpsc::UnboundedReceiver<()>,
        shutdown_token: tokio_util::sync::CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let mut handler = Self::new(store, executor, config, signal_receiver, shutdown_token);

        tokio::spawn(async move {
            handler.run().await;
        })
    }

    async fn run(&mut self) {
        loop {
            self.start_retry_processing_if_needed().await;

            tokio::select! {
                // Shutdown signal - highest priority.
                _ = self.shutdown_token.cancelled() => {
                    tracing::info!("Retry handler shutting down");
                    if let Some(handle) = self.task_handle.take() {
                        handle.abort();
                    }
                    break;
                }

                // Listen for retry signals.
                Some(_) = self.signal_receiver.recv() => {
                    *self.retry_needed.lock().await = true;
                }

                // Check if current task finished.
                _ = async {
                    if let Some(ref mut handle) = self.task_handle {
                        handle.await.ok()
                    } else {
                        // No task running, wait forever.
                        futures::future::pending::<Option<()>>().await
                    }
                } => {
                    // Task finished, clear the handle.
                    self.task_handle = None;
                    // The task itself checked the flag and decided to stop.
                }
            }
        }
    }

    /// Spawn a task that processes the retry queue and checks the flag.
    async fn start_retry_processing_if_needed(&mut self) {
        if self.task_handle.is_some() {
            return;
        }
        if !*self.retry_needed.lock().await {
            return;
        }

        let store = self.store.clone();
        let executor = self.executor.clone();
        let config = self.config.clone();
        let shutdown_token = self.shutdown_token.clone();
        let retry_needed = self.retry_needed.clone();

        let task_handle = tokio::spawn(async move {
            // Clear the flag at the start of each iteration.
            {
                let mut flag = retry_needed.lock().await;
                *flag = false;
            }

            let had_failures = Self::process_retry_queue_once(
                store.clone(),
                executor.clone(),
                config.clone(),
                shutdown_token.clone(),
            )
            .await;

            if had_failures {
                tracing::info!("Retry queue had failures, will retry again");
                *retry_needed.lock().await = true;
            } else {
                tracing::debug!("Retry queue processing completed successfully");
            }
        });

        self.task_handle = Some(task_handle);
    }

    /// Iterate over the retry queue and execute tasks.
    async fn process_retry_queue_once(
        store: Arc<S>,
        executor: Arc<E>,
        config: AsyncTaskManagerConfig,
        shutdown_token: tokio_util::sync::CancellationToken,
    ) -> bool {
        let mut last_task_id = None;
        let mut had_failures = false;

        while !shutdown_token.is_cancelled() {
            let tasks = match store.read_retry_tasks(last_task_id.clone(), 10).await {
                Ok(tasks) if tasks.is_empty() => break,
                Ok(tasks) => tasks,
                Err(e) => {
                    tracing::error!("Failed to read retry tasks: {}", e);
                    had_failures = true;
                    break;
                }
            };

            for task in tasks {
                if shutdown_token.is_cancelled() {
                    break;
                }

                let task_id = task.task_id();
                last_task_id = Some(task_id.clone());

                // Execute task and track failures.
                // Note: The executor is responsible for removing the task from storage
                // upon successful execution.
                if let Err(e) = executor.execute(task).await {
                    had_failures = true;
                    tracing::warn!("Retry task {:?} failed: {}", task_id, e);
                } else {
                    tracing::debug!("Retry task {:?} executed successfully", task_id);
                }
            }

            // Respect configured delay between tasks.
            tokio::time::sleep(config.task_delay).await;
        }

        had_failures
    }
}

/// Configuration for the async task manager.
#[derive(Debug, Clone)]
pub struct AsyncTaskManagerConfig {
    /// Configuration for the task sorter.
    pub queue_config: AsyncTaskSorterConfig,
    /// Delay between processing tasks (to avoid tight loops).
    pub task_delay: Duration,
}

impl Default for AsyncTaskManagerConfig {
    fn default() -> Self {
        Self {
            queue_config: AsyncTaskSorterConfig::default(),
            task_delay: Duration::from_millis(100),
        }
    }
}

/// An async task manager.
///
/// This manager:
/// - Uses AsyncTaskSorter for task storage and ordering.
/// - Executes tasks in serial asynchronously.
/// - Manages retry queue separately for failed tasks.
pub struct AsyncTaskManager<T, S, E>
where
    T: AsyncTask,
    S: OrderedStore<T>,
    E: TaskExecutor<T>,
{
    task_sorter: Arc<AsyncTaskSorter<T, S>>,
    store: Arc<S>,
    executor: Arc<E>,
    config: AsyncTaskManagerConfig,
    /// Handle to the currently active processing task, if any.
    active_task_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    /// Handle to the retry queue processing task, if any.
    retry_task_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    /// Cancellation token for shutting down processing.
    shutdown_token: tokio_util::sync::CancellationToken,
    /// Channel to signal retry processing needs.
    retry_signal_sender: tokio::sync::mpsc::UnboundedSender<()>,
}

impl<T, S, E> AsyncTaskManager<T, S, E>
where
    T: AsyncTask + 'static,
    S: OrderedStore<T> + 'static,
    E: TaskExecutor<T> + 'static,
{
    /// Create a new async task manager.
    pub async fn new(
        config: AsyncTaskManagerConfig,
        store: Arc<S>,
        executor: Arc<E>,
    ) -> Result<Self> {
        let task_sorter =
            Arc::new(AsyncTaskSorter::new(config.queue_config.clone(), store.clone()).await);
        let (retry_signal_sender, retry_signal_receiver) = tokio::sync::mpsc::unbounded_channel();
        let shutdown_token = tokio_util::sync::CancellationToken::new();

        // Start the retry handler with a child cancellation token.
        let retry_task_handle = RetryQueueHandler::spawn(
            store.clone(),
            executor.clone(),
            config.clone(),
            retry_signal_receiver,
            shutdown_token.child_token(), // Use child token for the retry handler.
        );

        // Create the active_task_handle first.
        let active_task_handle = Arc::new(Mutex::new(None));

        // Start the initial processing task.
        let handle = Self::start_processing_task(
            task_sorter.clone(),
            store.clone(),
            executor.clone(),
            config.clone(),
            active_task_handle.clone(),
            shutdown_token.child_token(),
            retry_signal_sender.clone(),
        );

        // Set the handle in the mutex.
        *active_task_handle.lock().await = Some(handle);

        Ok(Self {
            task_sorter,
            store,
            executor,
            config,
            active_task_handle,
            retry_task_handle: Arc::new(Mutex::new(Some(retry_task_handle))),
            shutdown_token,
            retry_signal_sender,
        })
    }

    /// Submit a task to the manager.
    /// The task will be persisted, enqueued and eventually executed.
    pub async fn submit(&self, task: T) -> Result<()> {
        // Enqueue the task.
        self.task_sorter.persist_and_enqueue_task(task).await?;

        // Try to start processing if not already active.
        self.maybe_start_processing().await?;

        Ok(())
    }

    /// Start a processing task.
    fn start_processing_task(
        task_sorter: Arc<AsyncTaskSorter<T, S>>,
        store: Arc<S>,
        executor: Arc<E>,
        config: AsyncTaskManagerConfig,
        active_task_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
        cancel_token: tokio_util::sync::CancellationToken,
        retry_signal_sender: tokio::sync::mpsc::UnboundedSender<()>,
    ) -> tokio::task::JoinHandle<()> {
        // Generate unique processor ID for debugging.
        let processor_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        tracing::debug!("Starting new processing task with ID: {}", processor_id);

        let handle = tokio::spawn(async move {
            tracing::debug!("Processor {} starting", processor_id);
            let result = Self::process_until_empty(
                task_sorter,
                store,
                executor,
                config,
                cancel_token,
                retry_signal_sender,
            )
            .await;

            if let Err(e) = result {
                tracing::error!("Error in processing task: {}", e);
            }

            // Clear the handle when done.
            let mut handle_guard = active_task_handle.lock().await;
            *handle_guard = None;
        });

        handle
    }

    /// Try to start processing if not already active.
    async fn maybe_start_processing(&self) -> Result<()> {
        let mut active_handle = self.active_task_handle.lock().await;

        // Check if we already have an active task.
        if let Some(ref handle) = *active_handle {
            if !handle.is_finished() {
                tracing::debug!("Processing task already active, skipping");
                return Ok(());
            }
            // Clean up finished handle.
            *active_handle = None;
        }

        let handle = Self::start_processing_task(
            Arc::clone(&self.task_sorter),
            Arc::clone(&self.store),
            Arc::clone(&self.executor),
            self.config.clone(),
            self.active_task_handle.clone(),
            self.shutdown_token.child_token(),
            self.retry_signal_sender.clone(),
        );

        // Store the new handle before releasing the lock.
        *active_handle = Some(handle);

        Ok(())
    }

    /// Static version of process_until_empty that doesn't borrow self.
    async fn process_until_empty(
        task_sorter: Arc<AsyncTaskSorter<T, S>>,
        store: Arc<S>,
        executor: Arc<E>,
        config: AsyncTaskManagerConfig,
        cancel_token: tokio_util::sync::CancellationToken,
        retry_signal_sender: tokio::sync::mpsc::UnboundedSender<()>,
    ) -> Result<()> {
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    tracing::info!("Task processing cancelled");
                    break;
                }

                task_result = task_sorter.get_next_task() => {
                    match task_result? {
                        Some(task) => {
                            // Execute the task.
                            // Note: The executor is responsible for removing the task from
                            // storage upon successful execution.
                            if let Err(e) = executor.execute(task.clone()).await {
                                tracing::info!(
                                    "Executing task failed, adding to retry queue: {}", e
                                );
                                store.add_to_retry_queue(&task).await?;

                                // Signal that retry processing may be needed..
                                let _ = retry_signal_sender.send(());
                            }

                            // Small delay to avoid tight loop.
                            tokio::time::sleep(config.task_delay).await;
                        }
                        None => {
                            // No tasks available, check if queue is really empty.
                            if task_sorter.is_empty().await {
                                // Really empty, exit the loop.
                                break;
                            } else {
                                // Queue might have tasks loading from disk, wait a bit.
                                tokio::time::sleep(config.task_delay).await;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Start processing in the background.
    /// This is useful if you want to start processing without submitting a task.
    pub async fn start(&self) -> Result<()> {
        // Don't call init() - let get_next_task handle loading.
        self.maybe_start_processing().await?;

        Ok(())
    }

    /// Shutdown the task manager gracefully.
    pub async fn shutdown(&self) {
        // Cancel the shutdown token to stop processing.
        // This will also cancel the retry handler via its child token.
        self.shutdown_token.cancel();

        let _ = self.active_task_handle.lock().await.take();
        let _ = self.retry_task_handle.lock().await.take();
    }

    /// Get queue statistics.
    pub async fn stats(&self) -> TaskManagerStats {
        let is_processing = self.is_processing().await;

        TaskManagerStats {
            processing_state: if is_processing {
                "Active".to_string()
            } else {
                "Idle".to_string()
            },
            queue_memory_tasks: 0, // AsyncTaskSorter doesn't track detailed stats.
        }
    }

    /// Check if the manager is currently processing tasks.
    pub async fn is_processing(&self) -> bool {
        let active_handle = self.active_task_handle.lock().await;
        if let Some(ref handle) = *active_handle {
            !handle.is_finished()
        } else {
            false
        }
    }

    // Retry queue operations - work directly with store.

    /// Add a task to the retry queue.
    pub async fn add_to_retry_queue(&self, task: &T) -> Result<()> {
        self.store
            .add_to_retry_queue(task)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to add task to retry queue: {}", e))?;

        tracing::debug!("Added task to retry queue: {:?}", task.task_id());

        // Signal that retry processing may be needed.
        let _ = self.retry_signal_sender.send(());

        Ok(())
    }

    /// Read tasks from the retry queue.
    pub async fn read_retry_tasks(
        &self,
        from_task_id: Option<T::TaskId>,
        limit: usize,
    ) -> Result<Vec<T>> {
        self.store
            .read_retry_tasks(from_task_id, limit)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read retry tasks: {}", e))
    }
}

/// Statistics about the task manager.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskManagerStats {
    pub processing_state: String,
    pub queue_memory_tasks: usize,
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::test_util::{OrderedTestStore, TestExecutor, TestTask, TestTaskGenerator};

    #[tokio::test]
    async fn test_task_manager_cancellation() -> Result<()> {
        let config = AsyncTaskManagerConfig::default();
        let store = Arc::new(OrderedTestStore::new());
        let executor = Arc::new(TestExecutor::new(store.clone()));

        let task_manager = AsyncTaskManager::new(config, store.clone(), executor).await?;

        // Submit a task.
        let task = TestTask::new(1);
        task_manager.submit(task).await?;

        // Immediately shutdown.
        task_manager.shutdown().await;

        // Verify the manager is no longer processing.
        assert!(!task_manager.is_processing().await);

        Ok(())
    }

    #[tokio::test]
    async fn test_retry_queue_with_failures() -> Result<()> {
        // Initialize tracing for test visibility.
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_test_writer()
            .try_init();

        let config = AsyncTaskManagerConfig {
            queue_config: crate::async_task_sorter::AsyncTaskSorterConfig {
                max_num_latest_tasks: 10,
                load_batch_size: 5,
                grace_period: Duration::from_millis(50),
            },
            task_delay: Duration::from_millis(10),
        };

        let store = Arc::new(OrderedTestStore::new());
        let executor = Arc::new(TestExecutor::new(store.clone()));

        // Create tasks with moderate failure rates (will eventually succeed through retries).
        let tasks: Vec<TestTask> = (1..=5)
            .map(|i| {
                TestTask::new(i)
                    .with_failure_rate(30) // 30% failure rate - will eventually succeed.
                    .with_duration(Duration::from_millis(20))
            })
            .collect();

        let task_manager = AsyncTaskManager::new(config, store.clone(), executor.clone()).await?;
        task_manager.start().await?;

        // Submit all tasks.
        for task in tasks {
            task_manager.submit(task).await?;
        }

        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(60) && store.task_count().await > 0 {
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }

        // Check that all tasks were processed and removed from storage.
        // With 30% failure rate, tasks should eventually succeed and be removed.
        let remaining_tasks = store.task_count().await;

        assert_eq!(
            remaining_tasks, 0,
            "All tasks should be processed and removed"
        );

        task_manager.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_processing_with_mixed_tasks() -> Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_test_writer()
            .try_init();

        let config = AsyncTaskManagerConfig {
            queue_config: crate::async_task_sorter::AsyncTaskSorterConfig {
                max_num_latest_tasks: 20,
                load_batch_size: 10,
                grace_period: Duration::from_millis(50),
            },
            task_delay: Duration::from_millis(5),
        };

        let store = Arc::new(OrderedTestStore::new());
        let executor = Arc::new(TestExecutor::new(store.clone()));

        let task_manager = AsyncTaskManager::new(config, store.clone(), executor.clone()).await?;

        // Use TaskGenerator to create tasks with random properties.
        // Enable random failure and duration.
        let mut generator = TestTaskGenerator::new(true, true);

        // Pre-populate with some tasks.
        let historical_tasks: Vec<TestTask> = generator.by_ref().take(10).collect();
        store.populate_with_tasks(historical_tasks).await;

        task_manager.start().await?;

        // Submit more tasks while processing.
        for task in generator.by_ref().take(10) {
            task_manager.submit(task).await?;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(60) && store.task_count().await > 0 {
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }

        // Check that all tasks were processed.
        // The store should be empty after all tasks are processed.
        let remaining_tasks = store.task_count().await;
        assert_eq!(remaining_tasks, 0, "All tasks should be processed");

        assert_eq!(executor.executed(), 20, "All tasks should be processed");

        task_manager.shutdown().await;
        Ok(())
    }
}
