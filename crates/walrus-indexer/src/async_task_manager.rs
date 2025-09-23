// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Async task manager built on top of PersistentQueue.

use std::sync::Arc;

use anyhow::Result;
use tokio::{
    sync::{Mutex, Semaphore, mpsc},
    task::JoinHandle,
    time::Duration,
};
use tokio_util::sync::CancellationToken;

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
    retry_notification_receiver: mpsc::UnboundedReceiver<()>,
    retry_notification_sender: mpsc::UnboundedSender<()>,
    shutdown_token: CancellationToken,
    task_handle: Option<JoinHandle<()>>, // The actual retry processing task.
    retry_notified: Arc<Mutex<bool>>,    // Shared flag for retry task to check.
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
        retry_notification_receiver: mpsc::UnboundedReceiver<()>,
        retry_notification_sender: mpsc::UnboundedSender<()>,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            store,
            executor,
            config,
            retry_notification_receiver,
            retry_notification_sender,
            shutdown_token,
            task_handle: None,
            retry_notified: Arc::new(Mutex::new(false)),
            _phantom: std::marker::PhantomData,
        }
    }

    fn spawn(
        store: Arc<S>,
        executor: Arc<E>,
        config: AsyncTaskManagerConfig,
        retry_notification_receiver: mpsc::UnboundedReceiver<()>,
        retry_notification_sender: mpsc::UnboundedSender<()>,
        shutdown_token: CancellationToken,
    ) -> JoinHandle<()> {
        let mut handler = Self::new(
            store,
            executor,
            config,
            retry_notification_receiver,
            retry_notification_sender,
            shutdown_token,
        );

        tokio::spawn(async move {
            handler.run().await;
        })
    }

    async fn run(&mut self) {
        loop {
            self.start_processing_retry_tasks().await;

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
                Some(_) = self.retry_notification_receiver.recv() => {
                    *self.retry_notified.lock().await = true;
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

    /// Spawn a task that processes the retry queue once and exits.
    async fn start_processing_retry_tasks(&mut self) {
        if self.task_handle.is_some() {
            // Already processing, don't start another.
            return;
        }
        if !*self.retry_notified.lock().await {
            return;
        }

        let store = self.store.clone();
        let executor = self.executor.clone();
        let config = self.config.clone();
        let shutdown_token = self.shutdown_token.clone();
        let retry_notified = self.retry_notified.clone();
        let retry_notification_sender = self.retry_notification_sender.clone();
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent_tasks));
        let read_batch_size = config.read_batch_size;

        let task_handle = tokio::spawn(async move {
            // Clear the flag at the start.
            {
                let mut flag = retry_notified.lock().await;
                *flag = false;
            }

            // Process the retry queue once completely.
            Self::process_retry_queue(
                store.clone(),
                executor.clone(),
                read_batch_size,
                semaphore.clone(),
                retry_notification_sender.clone(),
                shutdown_token.clone(),
            )
            .await;
        });

        self.task_handle = Some(task_handle);
    }

    /// The main retry queue processing loop that reads and processes retry tasks in batches.
    async fn process_retry_queue(
        store: Arc<S>,
        executor: Arc<E>,
        read_batch_size: usize,
        semaphore: Arc<Semaphore>,
        retry_notification_sender: mpsc::UnboundedSender<()>,
        shutdown_token: tokio_util::sync::CancellationToken,
    ) {
        let mut last_task_id = None;

        // Process tasks in batches.
        loop {
            if shutdown_token.is_cancelled() {
                break;
            }

            // Read batch of tasks from the retry queue.
            let tasks = match store
                .read_retry_tasks(last_task_id.clone(), read_batch_size)
                .await
            {
                Ok(tasks) => tasks,
                Err(e) => {
                    tracing::error!("Failed to read retry tasks: {}", e);
                    break;
                }
            };

            if tasks.is_empty() {
                // No more tasks in retry queue.
                break;
            }

            tracing::debug!("Processing batch of {} retry tasks", tasks.len());

            // Process each task in the batch, waiting for permit for each.
            for task in tasks {
                if shutdown_token.is_cancelled() {
                    break;
                }

                last_task_id = Some(task.task_id());

                // Wait for permit (blocks until available).
                let permit = match semaphore.clone().acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => break,
                };

                tracing::debug!("Processing retry task {:?}", task);

                // Remove task from retry queue before processing to prevent re-reading.
                if let Err(e) = store.delete_retry_task(&task.task_id()).await {
                    tracing::error!("Failed to delete retry task: {}", e);
                    continue;
                }

                // Spawn retry task processing in background.
                Self::spawn_retry_task_worker(
                    task,
                    permit,
                    store.clone(),
                    executor.clone(),
                    Some(retry_notification_sender.clone()),
                    shutdown_token.clone(),
                );
            }
            // After dispatching all tasks in this batch, continue to next batch.
            // The semaphore will limit concurrent execution across all batches.
        }
    }

    /// Spawn a worker to execute a retry task with the given permit.
    fn spawn_retry_task_worker(
        task: T,
        _permit: tokio::sync::OwnedSemaphorePermit,
        _store: Arc<S>,
        executor: Arc<E>,
        retry_notification_sender: Option<mpsc::UnboundedSender<()>>,
        shutdown_token: tokio_util::sync::CancellationToken,
    ) {
        tokio::spawn(async move {
            tracing::debug!("Worker started for retry task {:?}", task);

            // Execute the task.
            let result = executor.execute(task.clone()).await;

            // Handle the result.
            match result {
                Ok(_) => {
                    tracing::debug!("Retry task {:?} executed successfully", task);
                }
                Err(e) => {
                    if !shutdown_token.is_cancelled() {
                        tracing::warn!("Retry task {:?} failed: {}", task, e);

                        // Task was already removed from retry queue before execution.
                        // We don't need to update it - it's gone from storage.
                        // The executor should handle re-adding if needed.

                        // Signal that retry processing may be needed.
                        if let Some(sender) = retry_notification_sender {
                            let _ = sender.send(());
                        }
                    }
                }
            }

            tracing::debug!("Worker finished for retry task {:?}", task);
        });
    }
}

/// Configuration for the async task manager.
#[derive(Debug, Clone)]
pub struct AsyncTaskManagerConfig {
    /// Configuration for the task sorter.
    pub config: AsyncTaskSorterConfig,
    /// Delay between processing tasks (to avoid tight loops).
    pub inter_task_delay: Duration,
    /// Maximum number of concurrent tasks (applies to both regular and retry tasks).
    pub max_concurrent_tasks: usize,
    /// Batch size for reading entries from disk.
    pub read_batch_size: usize,
}

impl Default for AsyncTaskManagerConfig {
    fn default() -> Self {
        Self {
            config: AsyncTaskSorterConfig::default(),
            inter_task_delay: Duration::from_millis(100),
            max_concurrent_tasks: 4,
            read_batch_size: 10,
        }
    }
}

/// An async task manager.
///
/// This manager:
/// - Uses AsyncTaskSorter for task storage and ordering.
/// - Executes tasks concurrently up to configured limit.
/// - Manages retry queue separately for failed tasks.
pub struct AsyncTaskManager<T, S, E>
where
    T: AsyncTask,
    S: OrderedStore<T>,
    E: TaskExecutor<T>,
{
    task_sorter: Arc<AsyncTaskSorter<T, S>>,
    /// Handle to the main processing task.
    processing_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    /// Handle to the retry queue processing task.
    retry_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    /// Cancellation token for shutting down processing.
    shutdown_token: CancellationToken,
    _phantom: std::marker::PhantomData<(T, S, E)>,
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
            Arc::new(AsyncTaskSorter::new(config.config.clone(), store.clone()).await);
        let (retry_notification_sender, retry_notification_receiver) = mpsc::unbounded_channel();
        let shutdown_token = CancellationToken::new();

        // Start the retry handler with a child cancellation token.
        let retry_handle = RetryQueueHandler::spawn(
            store.clone(),
            executor.clone(),
            config.clone(),
            retry_notification_receiver,
            retry_notification_sender.clone(),
            shutdown_token.child_token(),
        );

        // Start the main processing loop.
        let processing_handle = Self::spawn_processing_loop(
            task_sorter.clone(),
            store,
            executor,
            config,
            shutdown_token.child_token(),
            retry_notification_sender,
        );

        Ok(Self {
            task_sorter,
            processing_handle: Arc::new(Mutex::new(Some(processing_handle))),
            retry_handle: Arc::new(Mutex::new(Some(retry_handle))),
            shutdown_token,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Submit a task to the manager.
    /// The task will be persisted, enqueued and eventually executed.
    pub async fn submit(&self, task: T) -> Result<()> {
        // Enqueue the task.
        self.task_sorter
            .persist_and_enqueue_task(task.clone())
            .await?;

        tracing::debug!("Submitted task {:?}", task);

        // The processing loop will pick it up automatically.
        Ok(())
    }

    /// Spawn the main processing loop.
    fn spawn_processing_loop(
        task_sorter: Arc<AsyncTaskSorter<T, S>>,
        store: Arc<S>,
        executor: Arc<E>,
        config: AsyncTaskManagerConfig,
        cancel_token: CancellationToken,
        retry_notification_sender: mpsc::UnboundedSender<()>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            tracing::info!("Starting task processing loop");

            // Create a semaphore for limiting concurrent tasks.
            let semaphore = Arc::new(Semaphore::new(config.max_concurrent_tasks));

            // The processing loop function.
            let processing_loop = Self::task_processing_loop(
                task_sorter,
                store,
                executor,
                config.inter_task_delay,
                semaphore.clone(),
                retry_notification_sender,
            );

            // Outer select for cancellation.
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    tracing::info!("Task processing cancelled.");
                }
                _ = processing_loop => {
                    tracing::info!("Processing loop ended.");
                }
            }

            // Wait for all tasks to complete before exiting.
            // We do this by acquiring all permits.
            for _ in 0..config.max_concurrent_tasks {
                let _ = semaphore.acquire().await;
            }

            tracing::info!("Task processing loop ended");
        })
    }

    /// The main task processing loop that fetches and processes tasks.
    async fn task_processing_loop(
        task_sorter: Arc<AsyncTaskSorter<T, S>>,
        store: Arc<S>,
        executor: Arc<E>,
        inter_task_delay: Duration,
        semaphore: Arc<Semaphore>,
        retry_notification_sender: mpsc::UnboundedSender<()>,
    ) {
        loop {
            // Wait to get the next task.
            let task = match task_sorter.get_next_task().await {
                Ok(Some(task)) => task,
                Ok(None) => {
                    // No more tasks available, wait a bit and try again.
                    tokio::time::sleep(inter_task_delay).await;
                    continue;
                }
                Err(e) => {
                    tracing::error!("Failed to get next task: {}", e);
                    tokio::time::sleep(inter_task_delay).await;
                    continue;
                }
            };

            // Wait for a permit (this blocks until one is available).
            let permit = match semaphore.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => break, // Semaphore was closed.
            };

            tracing::debug!("Processing task {:?}", task);

            // Spawn task processing in background.
            Self::spawn_task_worker(
                task,
                permit,
                store.clone(),
                executor.clone(),
                Some(retry_notification_sender.clone()),
            );
        }
    }

    /// Spawn a worker to execute a task with the given permit.
    fn spawn_task_worker(
        task: T,
        permit: tokio::sync::OwnedSemaphorePermit,
        store: Arc<S>,
        executor: Arc<E>,
        retry_notification_sender: Option<mpsc::UnboundedSender<()>>,
    ) {
        tokio::spawn(async move {
            tracing::debug!("Worker started for task {:?}", task);

            // Execute the task.
            let result = executor.execute(task.clone()).await;

            // Handle the result.
            match result {
                Ok(_) => {
                    tracing::debug!("Task {:?} executed successfully", task);
                }
                Err(e) => {
                    tracing::info!("Task {:?} failed: {}, adding to retry queue", task, e);

                    // Move to retry queue for retry processing.
                    if let Err(e) = store.move_to_retry_queue(&task).await {
                        tracing::error!("Failed to move task to retry queue: {}", e);
                    }

                    // Signal that retry processing is needed.
                    if let Some(sender) = retry_notification_sender {
                        let _ = sender.send(());
                    }
                }
            }

            tracing::debug!("Worker finished for task {:?}", task);
            // Permit is automatically dropped when this async block ends.
            drop(permit);
        });
    }

    /// Start processing in the background.
    /// This is useful if you want to start processing without submitting a task.
    pub async fn start(&self) -> Result<()> {
        // Processing loop is already running continuously.
        Ok(())
    }

    /// Shutdown the task manager gracefully.
    pub async fn shutdown(&self) {
        // Cancel the shutdown token to stop processing.
        // This will also cancel the retry handler via its child token.
        self.shutdown_token.cancel();

        // Wait for handles to finish.
        if let Some(handle) = self.processing_handle.lock().await.take() {
            let _ = handle.await;
        }
        if let Some(handle) = self.retry_handle.lock().await.take() {
            let _ = handle.await;
        }
    }

    /// Check if the manager is currently processing tasks.
    #[cfg(test)]
    async fn is_processing(&self) -> bool {
        let processing_handle = self.processing_handle.lock().await;
        if let Some(ref handle) = *processing_handle {
            !handle.is_finished()
        } else {
            false
        }
    }
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
            config: crate::async_task_sorter::AsyncTaskSorterConfig {
                max_num_latest_tasks: 10,
                load_batch_size: 5,
                grace_period: Duration::from_millis(50),
            },
            inter_task_delay: Duration::from_millis(10),
            max_concurrent_tasks: 2,
            read_batch_size: 10,
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
    async fn test_concurrent_retry_processing() -> Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_test_writer()
            .try_init();

        let config = AsyncTaskManagerConfig {
            config: crate::async_task_sorter::AsyncTaskSorterConfig::default(),
            inter_task_delay: Duration::from_millis(10),
            max_concurrent_tasks: 4, // Allow 4 concurrent workers.
            read_batch_size: 10,
        };

        let store = Arc::new(OrderedTestStore::new());
        let executor = Arc::new(TestExecutor::new(store.clone()));

        // Create tasks with moderate failure rates that will eventually succeed.
        let tasks: Vec<TestTask> = (1..=8)
            .map(|i| {
                TestTask::new(i)
                    .with_failure_rate(50) // 50% failure rate - will eventually succeed.
                    .with_duration(Duration::from_millis(100))
            })
            .collect();

        let task_manager = AsyncTaskManager::new(config, store.clone(), executor.clone()).await?;

        // Submit all tasks.
        for task in tasks {
            task_manager.submit(task).await?;
        }

        // Start processing.
        task_manager.start().await?;

        // Wait for all tasks to complete.
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(60) && store.task_count().await > 0 {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        // All tasks should eventually be processed.
        assert_eq!(store.task_count().await, 0, "All tasks should be processed");
        assert_eq!(executor.executed(), 8, "All 8 tasks should be executed");

        // Verify that concurrent retry processing is working.
        // With 50% failure rate, some tasks will go to retry queue and be processed concurrently.
        tracing::info!("All tasks completed in {:?}", start.elapsed());

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
            config: crate::async_task_sorter::AsyncTaskSorterConfig {
                max_num_latest_tasks: 20,
                load_batch_size: 10,
                grace_period: Duration::from_millis(50),
            },
            inter_task_delay: Duration::from_millis(5),
            max_concurrent_tasks: 4,
            read_batch_size: 10,
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
