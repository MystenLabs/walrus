// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Async task manager built on top of PersistentQueue.

use std::sync::Arc;

use anyhow::Result;
// async_trait is now used via lib.rs imports
use serde::{Deserialize, Serialize};
use tokio::{sync::Mutex, time::Duration};

use crate::{
    AsyncTask,
    OrderedStore,
    TaskExecutor,
    async_task_sorter::{AsyncTaskSorter, AsyncTaskSorterConfig},
};

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
/// - Uses AsyncTaskSorter for task storage and ordering
/// - Executes tasks in serial asynchronously
pub struct AsyncTaskManager<T, S, E>
where
    T: AsyncTask,
    S: OrderedStore<T>,
    E: TaskExecutor<T>,
{
    task_sorter: Arc<AsyncTaskSorter<T, S>>,
    executor: Arc<E>,
    config: AsyncTaskManagerConfig,
    /// Handle to the currently active processing task, if any.
    active_task_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    /// Cancellation token for shutting down processing.
    shutdown_token: tokio_util::sync::CancellationToken,
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
        let task_sorter = Arc::new(AsyncTaskSorter::new(config.queue_config.clone(), store).await);

        Ok(Self {
            task_sorter,
            executor,
            config,
            active_task_handle: Arc::new(Mutex::new(None)),
            shutdown_token: tokio_util::sync::CancellationToken::new(),
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

    /// Try to start processing if not already active.
    async fn maybe_start_processing(&self) -> Result<()> {
        let mut active_handle = self.active_task_handle.lock().await;

        // Check if we already have an active task.
        if let Some(ref handle) = *active_handle {
            if !handle.is_finished() {
                return Ok(());
            }
        }

        // Start a new task.
        let task_sorter = Arc::clone(&self.task_sorter);
        let executor = Arc::clone(&self.executor);
        let config = self.config.clone();
        let active_task_handle = Arc::clone(&self.active_task_handle);
        let cancel_token = self.shutdown_token.child_token();

        let handle = tokio::spawn(async move {
            let result =
                Self::process_until_empty_static(task_sorter, executor, config, cancel_token).await;

            if let Err(e) = result {
                tracing::error!("Error in processing task: {}", e);
            }

            let mut active_handle = active_task_handle.lock().await;
            *active_handle = None;
        });

        // Store the new handle.
        *active_handle = Some(handle);

        Ok(())
    }

    /// Process tasks until the queue is empty or cancellation is requested.
    /// Tasks are processed one at a time to ensure serialized execution.
    async fn process_until_empty(
        &self,
        cancel_token: tokio_util::sync::CancellationToken,
    ) -> Result<()> {
        Self::process_until_empty_static(
            Arc::clone(&self.task_sorter),
            Arc::clone(&self.executor),
            self.config.clone(),
            cancel_token,
        )
        .await
    }

    /// Static version of process_until_empty that doesn't borrow self.
    async fn process_until_empty_static(
        task_sorter: Arc<AsyncTaskSorter<T, S>>,
        executor: Arc<E>,
        config: AsyncTaskManagerConfig,
        cancel_token: tokio_util::sync::CancellationToken,
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
                            let result = executor.execute(task).await;
                            if let Err(e) = result {
                                tracing::error!("Error processing task: {}", e);
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
        self.task_sorter.init();
        self.maybe_start_processing().await
    }

    /// Shutdown the task manager gracefully.
    pub async fn shutdown(&self) {
        // Cancel the shutdown token to stop processing.
        self.shutdown_token.cancel();

        // Wait for the active task to complete or timeout.
        let mut active_handle = self.active_task_handle.lock().await;
        if let Some(handle) = active_handle.take() {
            // Wait for the task to complete with a timeout.
            let timeout_duration = Duration::from_millis(5000); // 5 second timeout.
            match tokio::time::timeout(timeout_duration, handle).await {
                Ok(_) => {
                    tracing::info!("Active task completed gracefully");
                }
                Err(_) => {
                    tracing::warn!(
                        "Active task did not complete within timeout, may have been cancelled"
                    );
                }
            }
        }
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

    /// Get the underlying task sorter (for advanced usage).
    pub fn task_sorter(&self) -> &Arc<AsyncTaskSorter<T, S>> {
        &self.task_sorter
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
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    use async_trait::async_trait;

    use super::*;
    use crate::test_util::{OrderedTestStore, TestTask};

    /// Global execution counter for testing.
    static EXECUTION_COUNTER: AtomicU64 = AtomicU64::new(0);
    static LAST_EXECUTED_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    struct CountingExecutor;

    impl CountingExecutor {
        fn new() -> Self {
            Self
        }

        fn reset_counter() {
            EXECUTION_COUNTER.store(0, Ordering::SeqCst);
            LAST_EXECUTED_SEQUENCE.store(0, Ordering::SeqCst);
        }

        fn get_execution_count() -> u64 {
            EXECUTION_COUNTER.load(Ordering::SeqCst)
        }

        fn get_last_executed_sequence() -> u64 {
            LAST_EXECUTED_SEQUENCE.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl TaskExecutor<TestTask> for CountingExecutor {
        async fn execute(&self, task: TestTask) -> Result<()> {
            // Small delay to simulate work.
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

            let count = EXECUTION_COUNTER.fetch_add(1, Ordering::SeqCst) + 1;
            assert_eq!(
                task.sequence_number(),
                LAST_EXECUTED_SEQUENCE.load(Ordering::SeqCst) + 1
            );
            LAST_EXECUTED_SEQUENCE.store(task.sequence_number(), Ordering::SeqCst);

            tracing::info!("Executed task #{}: seq={}", count, task.sequence_number());

            Ok(())
        }
    }

    struct TestExecutor {
        executed: Arc<AtomicBool>,
    }

    impl TestExecutor {
        fn new() -> Self {
            Self {
                executed: Arc::new(AtomicBool::new(false)),
            }
        }
    }

    #[async_trait]
    impl TaskExecutor<TestTask> for TestExecutor {
        async fn execute(&self, _task: TestTask) -> Result<()> {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            self.executed.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_task_manager_cancellation() -> Result<()> {
        let config = AsyncTaskManagerConfig::default();
        let store = Arc::new(OrderedTestStore::new());
        let executor = Arc::new(TestExecutor::new());

        let task_manager = AsyncTaskManager::new(config, store, executor).await?;

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
    async fn test_async_task_manager_comprehensive() -> Result<()> {
        // Initialize tracing for test visibility
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_test_writer()
            .try_init();

        CountingExecutor::reset_counter();

        // Small config for easy testing.
        let config = AsyncTaskManagerConfig {
            queue_config: crate::async_task_sorter::AsyncTaskSorterConfig {
                max_num_latest_tasks: 3, // Small buffer to test drops.
                load_batch_size: 2,      // Small batches.
                grace_period: Duration::from_millis(100),
            },
            task_delay: Duration::from_millis(5), // Fast processing.
        };

        let store = Arc::new(OrderedTestStore::new());
        let executor = Arc::new(CountingExecutor::new());

        // Pre-populate storage with some tasks (simulating previous session).
        let historical_tasks = vec![
            TestTask::new(1),
            TestTask::new(2),
            TestTask::new(3),
            TestTask::new(4),
        ];
        store.populate_with_tasks(historical_tasks).await;

        tracing::info!("Creating task manager...");
        let task_manager = AsyncTaskManager::new(config, store.clone(), executor).await?;

        // Start processing (this should trigger loading from storage).
        tracing::info!("Starting task manager...");
        task_manager.start().await?;

        // Give it time to load some historical tasks.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Now submit some real-time tasks with higher sequence numbers.
        tracing::info!("Submitting real-time tasks...");
        let realtime_tasks = vec![TestTask::new(5), TestTask::new(6), TestTask::new(7)];

        for task in realtime_tasks {
            task_manager.submit(task).await?;
            tokio::time::sleep(Duration::from_millis(20)).await; // Small delays.
        }

        // Let the system process for a while.
        tracing::info!("Letting system process...");
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Check execution statistics.
        let execution_count = CountingExecutor::get_execution_count();
        let last_sequence = CountingExecutor::get_last_executed_sequence();

        tracing::info!(
            "Test results: executed={} tasks, last_sequence={}",
            execution_count,
            last_sequence
        );

        // Verify we processed some tasks.
        assert_eq!(execution_count, 7);

        // Check manager stats.
        let stats = task_manager.stats().await;
        tracing::info!("Manager stats: {:?}", stats);

        // Clean shutdown.
        tracing::info!("Shutting down task manager...");
        task_manager.shutdown().await;
        assert!(!task_manager.is_processing().await);

        tracing::info!("Test completed successfully!");

        Ok(())
    }
}
