// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Async task manager built on top of PersistentQueue.

use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use tokio::{
    sync::{Mutex, mpsc},
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

/// Request to the task scheduler to check for work.
#[derive(Debug)]
struct ScheduleRequest;

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
    signal_receiver: mpsc::UnboundedReceiver<()>,
    shutdown_token: CancellationToken,
    task_handle: Option<JoinHandle<()>>, // The actual retry processing task.
    retry_needed: Arc<Mutex<bool>>,      // Shared flag for retry task to check.
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
        signal_receiver: mpsc::UnboundedReceiver<()>,
        shutdown_token: CancellationToken,
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
        signal_receiver: mpsc::UnboundedReceiver<()>,
        shutdown_token: CancellationToken,
    ) -> JoinHandle<()> {
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

                tracing::debug!("Processing retry task {:?}", task);

                // Execute task and track failures.
                // Note: The executor is responsible for removing the task from storage
                // upon successful execution.
                if let Err(e) = executor.execute(task.clone()).await {
                    had_failures = true;
                    tracing::warn!("Retry task {:?} failed: {}", task, e);
                } else {
                    tracing::debug!("Retry task {:?} executed successfully", task);
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
    /// Maximum number of concurrent tasks (not including retry tasks).
    pub max_concurrent_tasks: usize,
}

impl Default for AsyncTaskManagerConfig {
    fn default() -> Self {
        Self {
            queue_config: AsyncTaskSorterConfig::default(),
            task_delay: Duration::from_millis(100),
            max_concurrent_tasks: 4,
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
    /// Channel to signal task scheduling needs.
    schedule_tx: mpsc::UnboundedSender<ScheduleRequest>,
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
            Arc::new(AsyncTaskSorter::new(config.queue_config.clone(), store.clone()).await);
        let (retry_signal_sender, retry_signal_receiver) = mpsc::unbounded_channel();
        let (schedule_tx, schedule_rx) = mpsc::unbounded_channel::<ScheduleRequest>();
        let shutdown_token = CancellationToken::new();

        // Start the retry handler with a child cancellation token.
        let retry_handle = RetryQueueHandler::spawn(
            store.clone(),
            executor.clone(),
            config.clone(),
            retry_signal_receiver,
            shutdown_token.child_token(),
        );

        // Start the main processing loop.
        let processing_handle = Self::spawn_processing_loop(
            task_sorter.clone(),
            store.clone(),
            executor.clone(),
            config.clone(),
            shutdown_token.child_token(),
            retry_signal_sender.clone(),
            schedule_rx,
            schedule_tx.clone(),
        );

        Ok(Self {
            task_sorter,
            processing_handle: Arc::new(Mutex::new(Some(processing_handle))),
            retry_handle: Arc::new(Mutex::new(Some(retry_handle))),
            shutdown_token,
            schedule_tx,
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

        // Notify the scheduler to check for work.
        let _ = self.schedule_tx.send(ScheduleRequest);

        Ok(())
    }

    /// Spawn the main processing loop.
    #[allow(clippy::too_many_arguments)]
    fn spawn_processing_loop(
        task_sorter: Arc<AsyncTaskSorter<T, S>>,
        store: Arc<S>,
        executor: Arc<E>,
        config: AsyncTaskManagerConfig,
        cancel_token: CancellationToken,
        retry_signal_sender: mpsc::UnboundedSender<()>,
        schedule_rx: mpsc::UnboundedReceiver<ScheduleRequest>,
        schedule_tx: mpsc::UnboundedSender<ScheduleRequest>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            tracing::info!("Starting task processing loop");

            let result = Self::schedule_loop(
                task_sorter,
                store,
                executor,
                config,
                cancel_token,
                retry_signal_sender,
                schedule_rx,
                schedule_tx,
            )
            .await;

            if let Err(e) = result {
                tracing::error!("Error in processing loop: {}", e);
            }

            tracing::info!("Task processing loop ended");
        })
    }

    /// Process tasks until cancelled.
    #[allow(clippy::too_many_arguments)]
    async fn schedule_loop(
        task_sorter: Arc<AsyncTaskSorter<T, S>>,
        store: Arc<S>,
        executor: Arc<E>,
        config: AsyncTaskManagerConfig,
        cancel_token: CancellationToken,
        retry_signal_sender: mpsc::UnboundedSender<()>,
        mut schedule_rx: mpsc::UnboundedReceiver<ScheduleRequest>,
        schedule_tx: mpsc::UnboundedSender<ScheduleRequest>,
    ) -> Result<()> {
        // Map to track running worker tasks.
        let mut worker_handles: HashMap<T::TaskId, JoinHandle<Result<()>>> = HashMap::new();

        // Send initial request to check for tasks.
        let _ = schedule_tx.send(ScheduleRequest);

        // Main event loop - runs forever until cancelled.
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    tracing::info!("Task processing cancelled, aborting workers.");

                    // Cancel all running workers.
                    for (_, handle) in worker_handles.drain() {
                        handle.abort();
                    }

                    break;
                }

                // Handle scheduling requests.
                Some(_request) = schedule_rx.recv() => {
                    tracing::debug!("Received scheduling request");

                    // Clean up finished workers.
                    worker_handles.retain(|task_id, handle| {
                        if handle.is_finished() {
                            tracing::debug!("Cleaning up finished worker for task {:?}", task_id);
                            false
                        } else {
                            true
                        }
                    });

                    // Try to schedule more tasks up to the limit.
                    while worker_handles.len() < config.max_concurrent_tasks {
                        match task_sorter.get_next_task().await? {
                            Some(task) => {
                                tracing::debug!("Spawning worker for task {:?}", task);
                                worker_handles.insert(task.task_id(),
                                Self::spawn_worker(
                                    task,
                                    executor.clone(),
                                    store.clone(),
                                    retry_signal_sender.clone(),
                                    schedule_tx.clone(),
                                    config.task_delay,
                                ));
                            }
                            None => {
                                // No more tasks available right now.
                                break;
                            }
                        }
                    }

                    tracing::debug!(
                        "Active workers: {}/{}",
                        worker_handles.len(),
                        config.max_concurrent_tasks
                    );
                }
            }
        }

        Ok(())
    }

    /// Helper function to spawn a worker task.
    fn spawn_worker(
        task: T,
        executor: Arc<E>,
        store: Arc<S>,
        retry_signal_sender: mpsc::UnboundedSender<()>,
        schedule_tx: mpsc::UnboundedSender<ScheduleRequest>,
        task_delay: Duration,
    ) -> JoinHandle<Result<()>> {
        let task_id = task.task_id();
        let task_id_for_worker = task_id.clone();
        let task_id_for_logging = task_id.clone();

        // Spawn a worker for this task.
        let worker_handle = tokio::spawn(async move {
            tracing::debug!("Worker started for task {:?}", task);

            // Execute the task.
            // Note: The executor is responsible for removing the task from
            // storage upon successful execution.
            let result = if let Err(e) = executor.execute(task.clone()).await {
                tracing::info!(
                    "Executing task {:?} failed, adding to retry queue: {}",
                    task,
                    e
                );
                store.add_to_retry_queue(&task).await?;

                // Signal that retry processing may be needed.
                let _ = retry_signal_sender.send(());
                Ok(())
            } else {
                Ok(())
            };

            // Small delay to avoid tight loop.
            tokio::time::sleep(task_delay).await;

            // Signal that we're done - scheduler should check for more work.
            let _ = schedule_tx.send(ScheduleRequest);

            tracing::debug!("Worker finished for task {:?}", task_id_for_worker);
            result
        });

        tracing::debug!("Started worker for task {:?}", task_id_for_logging);

        worker_handle
    }

    /// Start processing in the background.
    /// This is useful if you want to start processing without submitting a task.
    pub async fn start(&self) -> Result<()> {
        // Just trigger the scheduler to check for work.
        let _ = self.schedule_tx.send(ScheduleRequest);
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
            queue_config: crate::async_task_sorter::AsyncTaskSorterConfig {
                max_num_latest_tasks: 10,
                load_batch_size: 5,
                grace_period: Duration::from_millis(50),
            },
            task_delay: Duration::from_millis(10),
            max_concurrent_tasks: 2,
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
            max_concurrent_tasks: 4,
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
