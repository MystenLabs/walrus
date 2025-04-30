// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration as StdDuration, Instant},
};

use chrono::Utc;
use rocksdb::{
    Env,
    backup::{BackupEngine, BackupEngineOptions, RestoreOptions},
};
use serde::{Deserialize, Serialize};
use tokio::{task::JoinHandle, time};
use tokio_util::sync::CancellationToken;
use typed_store::rocks::RocksDB;

use crate::node::errors::CheckpointError;

/// Configuration for RocksDB checkpoint management.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CheckpointConfig {
    /// Directory where checkpoints will be stored.
    pub checkpoint_dir: Option<PathBuf>,
    /// Maximum number of checkpoints to keep.
    pub max_checkpoints: usize,
    /// How often to create checkpoints (in seconds).
    pub checkpoint_interval_secs: u64,
    /// Whether to sync files to disk before each checkpoint.
    pub sync: bool,
    /// Number of background operations for checkpoint/restore.
    pub max_background_operations: i32,
    /// Whether to schedule a background task to create checkpoints.
    pub periodic_checkpoints: bool,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            checkpoint_dir: None,
            max_checkpoints: 3,
            checkpoint_interval_secs: 86400, // 1 day.
            sync: true,
            max_background_operations: 2,
            periodic_checkpoints: false,
        }
    }
}

/// Status of a delayed task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskStatus {
    /// No task is currently running.
    Idle,
    /// The task is scheduled to run at a specific time.
    Scheduled,
    /// The task is running, the value is the start time.
    Running(tokio::time::Instant),
    /// The task completed successfully.
    Success,
    /// The task failed with an error message.
    Failed(String),
    /// The task was cancelled.
    Cancelled,
    /// The task panicked.
    TaskError(String),
    /// The task timed out.
    Timeout,
}

/// The result of a task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskResult<E> {
    /// The task completed successfully.
    Success,
    /// The task failed with an error message.
    Failed(E),
    /// The task panicked.
    TaskError(String),
}

/// A task scheduled to run at a specific time.
#[derive(Debug)]
pub struct DelayedTask {
    status: Arc<std::sync::Mutex<TaskStatus>>,
    handle: JoinHandle<()>,
}

impl DelayedTask {
    /// Create a new delayed task to run at the given time.
    pub fn new<F, E>(
        target_time: time::Instant,
        timeout_duration: StdDuration,
        task_fn: F,
        response: tokio::sync::oneshot::Sender<TaskResult<E>>,
    ) -> Self
    where
        F: FnOnce() -> Result<(), E> + Send + 'static,
        E: std::fmt::Debug + Send + 'static,
    {
        let status = Arc::new(std::sync::Mutex::new(TaskStatus::Scheduled));

        let timer_handle = tokio::spawn(Self::execute_delayed_task(
            target_time,
            timeout_duration,
            task_fn,
            status.clone(),
            response,
        ));

        Self {
            status,
            handle: timer_handle,
        }
    }

    /// Execute a task after a delay.
    async fn execute_delayed_task<F, E>(
        start_time: tokio::time::Instant,
        timeout_duration: StdDuration,
        task_fn: F,
        status: Arc<std::sync::Mutex<TaskStatus>>,
        response: tokio::sync::oneshot::Sender<TaskResult<E>>,
    ) where
        F: FnOnce() -> Result<(), E> + Send + 'static,
        E: std::fmt::Debug + Send + 'static,
    {
        time::sleep_until(start_time).await;

        {
            let mut status_guard = status.lock().expect("Failed to lock status");
            *status_guard = TaskStatus::Running(start_time);
        }

        // Execute the task in a blocking thread.
        let worker_task = tokio::task::spawn_blocking(task_fn);

        // Wait for cancel, completion, or timeout.
        tokio::select! {
            result = worker_task => {
                match result {
                    Ok(Ok(_)) => {
                        let mut status_guard = status.lock().expect("Failed to lock status");
                        *status_guard = TaskStatus::Success;
                        let _ = response.send(TaskResult::Success);
                    }
                    Ok(Err(e)) => {
                        let mut status_guard = status.lock().expect("Failed to lock status");
                        *status_guard = TaskStatus::Failed(format!("Task failed: {:?}", e));
                        let _ = response.send(TaskResult::Failed(e));
                    }
                    Err(e) => {
                        let mut status_guard = status.lock().expect("Failed to lock status");
                        *status_guard = TaskStatus::TaskError(format!("Task panicked: {}", e));
                        let _ = response.send(
                            TaskResult::TaskError(format!("Task panicked: {}", e))
                        );
                    }
                };
            }

            _ = tokio::time::sleep(timeout_duration) => {
                let mut status_guard = status.lock().expect("Failed to lock status");
                *status_guard = TaskStatus::Timeout;
            }
        }
    }

    /// Cancel the task.
    pub async fn cancel(&self) {
        if let Ok(mut status_guard) = self.status.lock() {
            *status_guard = TaskStatus::Cancelled;
            self.handle.abort();
        }
    }

    /// Get the status of the task.
    pub fn get_status(&self) -> TaskStatus {
        let state = self.status.lock().unwrap();
        state.clone()
    }
}

/// This enum defines the requests that can be sent to the checkpoint manager.
#[derive(Debug)]
pub enum CheckpointRequest {
    /// Create a checkpoint.
    CreateCheckpoint {
        /// The response channel.
        response: tokio::sync::oneshot::Sender<TaskResult<CheckpointError>>,
        /// The directory to create the checkpoint in.
        checkpoint_dir: PathBuf,
        /// Delay before creating the checkpoint.
        delay: Option<time::Duration>,
    },
    /// Get the status of the current task.
    GetStatus {
        /// The response channel.
        response: tokio::sync::oneshot::Sender<TaskStatus>,
    },
    /// Cancel the current task.
    CancelBackup {
        /// The response channel.
        /// returns true if the backup was canceled.
        response: tokio::sync::oneshot::Sender<bool>,
    },
}

/// Manages the creation/cleanup of checkpoints.
#[derive(Debug)]
pub struct CheckpointManager {
    /// Driver for handling checkpoint requests.
    execution_loop: JoinHandle<Result<(), CheckpointError>>,
    /// A simple loop that schedules checkpoint creation at a fixed interval.
    schedule_loop_handle: Option<JoinHandle<Result<(), CheckpointError>>>,
    /// Cancellation token.
    cancel_token: CancellationToken,
    /// Channel to send commands to the checkpoint manager.
    command_tx: tokio::sync::mpsc::Sender<CheckpointRequest>,
    /// The configuration.
    config: CheckpointConfig,
}

impl CheckpointManager {
    /// Initial delay before first checkpoint creation, to avoid resource contention.
    const CHECKPOINT_CREATION_INITIAL_DELAY: time::Duration = time::Duration::from_secs(15 * 60);
    /// Delay between checkpoint creation retries.
    const CHECKPOINT_CREATION_RETRY_DELAY: time::Duration = time::Duration::from_secs(300);
    /// Default timeout for tasks.
    const DEFAULT_TASK_TIMEOUT: StdDuration = time::Duration::from_secs(60 * 60);

    /// Create a new checkpoint manager for RocksDB.
    pub async fn new(db: Arc<RocksDB>, config: CheckpointConfig) -> Result<Self, CheckpointError> {
        if let Some(checkpoint_dir) = config.checkpoint_dir.as_ref() {
            Self::create_checkpoint_dir_if_not_exists(checkpoint_dir)?;
        }

        let cancel_token = CancellationToken::new();
        let db_clone = db.clone();
        let config_clone = config.clone();
        let cancel_token_clone = cancel_token.clone();

        let (command_tx, command_rx) = tokio::sync::mpsc::channel(10);

        let execution_loop: JoinHandle<Result<(), CheckpointError>> = tokio::spawn(async move {
            Self::execution_loop(db_clone, config_clone, cancel_token_clone, command_rx).await?;
            Ok(())
        });

        let config_clone = config.clone();
        let schedule_loop_handle = config.periodic_checkpoints.then(|| {
            let cancel_token_clone = cancel_token.clone();
            let command_tx_clone = command_tx.clone();

            tokio::spawn(async move {
                Self::schedule_loop(config_clone, cancel_token_clone, command_tx_clone).await?;
                Ok(())
            })
        });

        Ok(Self {
            execution_loop,
            schedule_loop_handle,
            cancel_token,
            command_tx,
            config,
        })
    }

    /// Schedule a checkpoint creation and wait for it to complete.
    ///
    /// Args:
    ///     checkpoint_dir: The directory to create the checkpoint in, if not provided the
    ///     directory configured in CheckpointConfig will be used. If none of these are provided
    ///     an error will be returned.
    ///     delay: The delay before creating the checkpoint.
    ///
    /// The checkpoint creation task starts in the background asynchronously, and cancelling the
    /// wait won't cancel the task.
    pub async fn schedule_and_wait_for_checkpoint_creation(
        &self,
        checkpoint_dir: Option<&Path>,
        delay: Option<time::Duration>,
    ) -> Result<(), CheckpointError> {
        let checkpoint_path = if let Some(dir) = checkpoint_dir {
            dir.to_path_buf()
        } else if let Some(config_dir) = self.config.checkpoint_dir.as_ref() {
            config_dir.clone()
        } else {
            return Err(CheckpointError::CheckpointCreationError(
                "No checkpoint directory specified, either provide one explicitly or configure \
                it in CheckpointConfig"
                    .to_string(),
            ));
        };

        Self::create_checkpoint_dir_if_not_exists(&checkpoint_path)?;

        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        self.command_tx
            .send(CheckpointRequest::CreateCheckpoint {
                response: response_tx,
                checkpoint_dir: checkpoint_path,
                delay,
            })
            .await
            .map_err(|e| CheckpointError::Other(e.into()))?;

        let result = response_rx.await;
        match result {
            Ok(TaskResult::Success) => Ok(()),
            Ok(TaskResult::Failed(e)) => Err(e),
            Ok(TaskResult::TaskError(e)) => Err(CheckpointError::Other(anyhow::anyhow!(e))),
            Err(e) => Err(CheckpointError::Other(e.into())),
        }
    }

    /// Get the status of the current checkpoint creation task.
    pub async fn get_status(&self) -> anyhow::Result<TaskStatus> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        self.command_tx
            .send(CheckpointRequest::GetStatus {
                response: response_tx,
            })
            .await
            .map_err(|e| CheckpointError::Other(e.into()))?;
        let result = response_rx.await?;
        Ok(result)
    }

    /// Cancel the current checkpoint creation task, if any.
    pub async fn cancel_checkpoint_creation(&self) -> Result<bool, CheckpointError> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        self.command_tx
            .send(CheckpointRequest::CancelBackup {
                response: response_tx,
            })
            .await
            .map_err(|e| CheckpointError::Other(e.into()))?;
        let result = response_rx
            .await
            .map_err(|e| CheckpointError::Other(e.into()))?;
        Ok(result)
    }

    /// The background task that handles checkpoint requests.
    async fn execution_loop(
        db: Arc<RocksDB>,
        config: CheckpointConfig,
        cancel_token: CancellationToken,
        mut command_rx: tokio::sync::mpsc::Receiver<CheckpointRequest>,
    ) -> Result<(), CheckpointError> {
        let mut current_task: Option<Arc<DelayedTask>> = None;

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    tracing::info!("checkpoint manager loop cancelled");
                    break;
                }

                Some(cmd) = command_rx.recv() => {
                    match cmd {
                        CheckpointRequest::CreateCheckpoint { response, checkpoint_dir, delay } => {
                            if current_task.as_ref().is_some_and(|task| {
                                matches!(task.get_status(), TaskStatus::Running(_))
                            }) {
                                let _ = response.send(
                                    TaskResult::Failed(CheckpointError::CheckpointInProgress)
                                );
                            } else {
                                let db_clone = db.clone();
                                let config_clone = config.clone();
                                current_task = Some(Arc::new(DelayedTask::new(
                                    time::Instant::now() + delay.unwrap_or_default(),
                                    Self::DEFAULT_TASK_TIMEOUT,
                                    move || {
                                        let result = Self::create_backup_impl(
                                            &db_clone, &checkpoint_dir, config_clone.sync
                                        );
                                        match &result {
                                            Ok(_) => {
                                                Self::purge_old_checkpoints(
                                                    &checkpoint_dir, config.max_checkpoints
                                                );
                                            },
                                            Err(e) =>
                                                tracing::error!(?e, "Failed to create checkpoint"),
                                        }
                                        result
                                    },
                                    response,
                                )));
                            }
                        },
                        CheckpointRequest::GetStatus { response } => {
                            let status = current_task.as_ref().map_or(TaskStatus::Idle, |task| {
                                task.get_status()
                            });
                            let _ = response.send(status);
                        },
                        CheckpointRequest::CancelBackup { response } => {
                            if let Some(task) = current_task.as_ref() {
                                task.cancel().await;
                                let _ = response.send(true);
                            } else {
                                let _ = response.send(false);
                            }
                        }
                    }
                },
            }
        }

        Ok(())
    }

    async fn schedule_loop(
        config: CheckpointConfig,
        cancel_token: CancellationToken,
        command_tx: tokio::sync::mpsc::Sender<CheckpointRequest>,
    ) -> Result<(), CheckpointError> {
        tracing::info!("checkpoint manager schedule loop started.");
        let Some(checkpoint_dir) = config.checkpoint_dir.as_ref() else {
            return Err(CheckpointError::Other(anyhow::anyhow!(
                "Checkpoint directory not set"
            )));
        };

        time::sleep(Self::CHECKPOINT_CREATION_INITIAL_DELAY).await;

        // Try to calculate the next checkpoint time in a loop until successful.
        let mut next_checkpoint_time = loop {
            match Self::calculate_first_checkpoint_time(
                checkpoint_dir,
                config.checkpoint_interval_secs,
            )
            .await
            {
                Ok(time) => break time,
                Err(e) => {
                    tracing::error!(?e, "Failed to calculate next checkpoint time, retrying...");
                    // Wait 10 minutes before retrying.
                    time::sleep(time::Duration::from_secs(600)).await;
                }
            }
        };

        loop {
            tracing::info!("Next checkpoint scheduled at: {:?}", next_checkpoint_time);
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    tracing::info!("checkpoint scheduler cancelled");
                    break;
                }
                _ = time::sleep_until(next_checkpoint_time) => {
                    let (response_tx, response_rx) = tokio::sync::oneshot::channel();

                    if let Err(e) = command_tx.send(CheckpointRequest::CreateCheckpoint {
                        response: response_tx,
                        checkpoint_dir: checkpoint_dir.to_path_buf(),
                        delay: None,
                    }).await {
                        tracing::error!(?e, "Failed to send checkpoint creation request");
                        next_checkpoint_time = time::Instant::now() +
                            Self::CHECKPOINT_CREATION_RETRY_DELAY;
                        continue;
                    }

                    let result = match response_rx.await {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::error!(?e, "Failed to receive checkpoint creation result");
                            next_checkpoint_time = time::Instant::now() +
                                StdDuration::from_secs(300);
                            continue;
                        }
                    };

                    next_checkpoint_time = Self::get_next_checkpoint_time(
                        config.checkpoint_interval_secs,
                        &result
                    );
                }
            }
        }

        Ok(())
    }

    /// Get the next checkpoint time based on the result of the previous checkpoint task.
    fn get_next_checkpoint_time(
        checkpoint_interval_secs: u64,
        result: &TaskResult<CheckpointError>,
    ) -> time::Instant {
        if let TaskResult::Success = result {
            time::Instant::now() + StdDuration::from_secs(checkpoint_interval_secs)
        } else {
            time::Instant::now() + Self::CHECKPOINT_CREATION_RETRY_DELAY
        }
    }

    /// Calculate the first checkpoint time.
    async fn calculate_first_checkpoint_time(
        checkpoint_dir: &Path,
        checkpoint_interval_secs: u64,
    ) -> Result<time::Instant, CheckpointError> {
        let latest_timestamp = Self::get_latest_checkpoint_timestamp(checkpoint_dir)?;
        let now = Utc::now().timestamp();
        let interval_secs = i64::try_from(checkpoint_interval_secs)
            .map_err(|e| CheckpointError::Other(e.into()))?;
        let next_ts = if let Some(last_ts) = latest_timestamp {
            std::cmp::max(last_ts + interval_secs, now)
        } else {
            now
        };

        let seconds_from_now = next_ts - now;
        let duration = std::time::Duration::from_secs(seconds_from_now as u64);

        Ok(time::Instant::now() + duration)
    }

    /// Get the timestamp of the latest checkpoint, if any.
    pub fn get_latest_checkpoint_timestamp(
        checkpoint_dir: &Path,
    ) -> Result<Option<i64>, CheckpointError> {
        let engine = Self::create_backup_engine(checkpoint_dir)?;
        let backup_info = engine.get_backup_info();

        if backup_info.is_empty() {
            return Ok(None);
        }

        let latest_timestamp = backup_info.iter().map(|info| info.timestamp).max();

        Ok(latest_timestamp)
    }

    /// Create a BackupEngine instance.
    fn create_backup_engine(checkpoint_dir: &Path) -> Result<BackupEngine, CheckpointError> {
        let env = Env::new().map_err(|e| CheckpointError::Other(e.into()))?;

        let backup_opts = BackupEngineOptions::new(checkpoint_dir)
            .map_err(|e| CheckpointError::Other(e.into()))?;

        BackupEngine::open(&backup_opts, &env).map_err(|e| CheckpointError::Other(e.into()))
    }

    /// Delete old checkpoints to maintain the max_checkpoints limit.
    fn purge_old_checkpoints(checkpoint_dir: &Path, max_checkpoints: usize) {
        if max_checkpoints == 0 {
            return;
        }

        let Ok(mut engine) = Self::create_backup_engine(checkpoint_dir) else {
            tracing::error!("Failed to create backup engine");
            return;
        };
        let result = engine
            .purge_old_backups(max_checkpoints)
            .map_err(|e| CheckpointError::Other(anyhow::anyhow!("Purge error: {}", e)));

        match result {
            Ok(_) => tracing::info!(
                "purged old checkpoints, keeping {} most recent",
                max_checkpoints
            ),
            Err(e) => tracing::error!(?e, "Failed to purge old checkpoints"),
        }
    }

    /// Restore from the most recent backup.
    pub async fn restore_latest(
        checkpoint_dir: &Path,
        db_path: &Path,
        wal_dir: Option<&Path>,
    ) -> Result<(), CheckpointError> {
        // Create a fresh BackupEngine for this operation.
        let mut engine = Self::create_backup_engine(checkpoint_dir)?;

        let restore_opts = RestoreOptions::default();
        let wal_path = wal_dir.unwrap_or(db_path);

        tracing::info!("restoring database from latest backup");
        engine
            .restore_from_latest_backup(db_path, wal_path, &restore_opts)
            .map_err(|e| CheckpointError::Other(anyhow::anyhow!("restore error: {}", e)))?;

        tracing::info!("database restored successfully");
        Ok(())
    }

    fn create_backup_impl(
        db: &Arc<RocksDB>,
        checkpoint_dir: &Path,
        flush_before_backup: bool,
    ) -> Result<(), CheckpointError> {
        let mut engine = Self::create_backup_engine(checkpoint_dir)?;

        let start_time = Instant::now();
        tracing::info!(
            checkpoint_dir = ?checkpoint_dir,
            "start creating RocksDB backup"
        );

        let db_ref = &db.underlying;
        let backup_result = engine
            .create_new_backup_flush(db_ref, flush_before_backup)
            .map_err(|e| CheckpointError::Other(anyhow::anyhow!("Backup error: {}", e)));

        let duration = start_time.elapsed();

        match backup_result {
            Ok(_) => {
                tracing::info!(
                    duration = ?duration,
                    "rocksDB backup created successfully"
                );
                Ok(())
            }
            Err(e) => {
                tracing::error!(
                    ?e,
                    duration = ?duration,
                    "rocksDB backup failed",
                );
                Err(e)
            }
        }
    }

    /// Create the checkpoint directory if it doesn't exist.
    fn create_checkpoint_dir_if_not_exists(checkpoint_dir: &Path) -> Result<(), CheckpointError> {
        match checkpoint_dir.try_exists() {
            Ok(true) => Ok(()),
            Ok(false) => std::fs::create_dir_all(checkpoint_dir)
                .map_err(|e| CheckpointError::Other(e.into())),
            Err(e) => Err(CheckpointError::Other(e.into())),
        }
    }

    /// Cancel checkpoint manager.
    pub fn cancel(&self) {
        self.cancel_token.cancel();
    }

    /// Join the background tasks and clean up resources.
    pub async fn join(&mut self) -> Result<(), CheckpointError> {
        if let Err(e) = (&mut self.execution_loop).await {
            tracing::warn!(?e, "Error joining execution loop");
            return Err(CheckpointError::Other(e.into()));
        }

        if let Some(handle) = self.schedule_loop_handle.take() {
            if let Err(e) = handle.await {
                tracing::warn!(?e, "Error joining schedule loop");
                return Err(CheckpointError::Other(e.into()));
            }
        }

        Ok(())
    }

    /// Shutdown the background tasks.
    pub fn shutdown(&self) {
        self.cancel_token.cancel();
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, fs};

    use rand::Rng;
    use rocksdb::Options;
    use tempfile::tempdir;
    use typed_store::{
        Map,
        rocks::{self, DBMap, MetricConf, ReadWriteOptions},
    };

    use super::*;

    const MIN_BLOB_SIZE: u64 = 10;
    const BLOB_FILE_SIZE: u64 = 1024 * 1024;

    /// Generates key/value pairs with random sizes:
    /// - Half with values of random size between 1 and median_size bytes.
    /// - Half with values of random size between (median_size+1) and (median_size*2) bytes.
    pub fn generate_test_data(num_keys: usize, median_size: usize) -> HashMap<Vec<u8>, Vec<u8>> {
        let mut data = HashMap::new();
        let mut rng = rand::thread_rng();

        // Number of small and large values (half each).
        let small_count = num_keys / 2;
        let large_count = num_keys - small_count;

        for i in 0..small_count {
            let key = format!("small_key_{:03}", i).into_bytes();

            let small_size = rng.gen_range(1..=median_size);
            let value = vec![b's'; small_size];

            data.insert(key, value);
        }

        for i in 0..large_count {
            let key = format!("large_key_{:03}", i).into_bytes();

            let size = rng.gen_range(median_size + 1..=median_size * 2);
            let value = vec![b'l'; size];

            data.insert(key, value);
        }

        data
    }

    fn cf_options_with_blobs() -> Options {
        let mut opts = Options::default();
        opts.set_enable_blob_files(true);
        opts.set_min_blob_size(MIN_BLOB_SIZE);
        opts.set_blob_file_size(BLOB_FILE_SIZE);
        opts
    }

    #[tokio::test]
    async fn test_basic_backup_restore() -> Result<(), Box<dyn std::error::Error>> {
        let db_dir = tempdir()?;
        let checkpoint_dir = tempdir()?;
        let restore_dir = tempdir()?;

        let test_data = generate_test_data(100, MIN_BLOB_SIZE as usize);

        {
            let mut db_opts = Options::default();
            db_opts.create_missing_column_families(true);
            db_opts.create_if_missing(true);

            let default_cf_opts = cf_options_with_blobs();

            let db = rocks::open_cf_opts(
                db_dir.path(),
                Some(db_opts),
                MetricConf::default(),
                &[("default", default_cf_opts)],
            )?;

            let db_map = DBMap::<Vec<u8>, Vec<u8>>::reopen(
                &db,
                Some("default"),
                &ReadWriteOptions::default(),
                false,
            )?;

            for (key, value) in &test_data {
                db_map.insert(key, value)?;
            }

            let checkpoint_manager = CheckpointManager::new(
                db,
                CheckpointConfig {
                    checkpoint_dir: Some(checkpoint_dir.path().to_path_buf()),
                    ..Default::default()
                },
            )
            .await?;

            checkpoint_manager
                .schedule_and_wait_for_checkpoint_creation(Some(checkpoint_dir.path()), None)
                .await?;
        }

        assert!(
            fs::read_dir(&checkpoint_dir)?.count() > 0,
            "backup directory should not be empty"
        );

        // Restore from backup to a new location.
        CheckpointManager::restore_latest(checkpoint_dir.path(), restore_dir.path(), None).await?;

        // Reopen restored DB and verify contents.
        {
            let mut db_opts = Options::default();
            db_opts.create_missing_column_families(true);
            db_opts.create_if_missing(true);

            // Create column family options from the config
            let default_cf_opts = cf_options_with_blobs();

            let db = rocks::open_cf_opts(
                restore_dir.path(),
                Some(db_opts),
                MetricConf::default(),
                &[("default", default_cf_opts)],
            )?;

            let db_map = DBMap::<Vec<u8>, Vec<u8>>::reopen(
                &db,
                Some("default"),
                &ReadWriteOptions::default(),
                false,
            )?;

            // Check if all data is restored correctly.
            for (key, expected_value) in &test_data {
                let value = db_map.get(key)?.expect("key should exist in restored DB");
                assert_eq!(
                    &value, expected_value,
                    "restored value should match original"
                );
            }
        }

        Ok(())
    }
}
