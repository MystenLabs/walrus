// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration as StdDuration},
};

use chrono::{Duration, Utc};
use std::time::Instant;

use rocksdb::{
    backup::{BackupEngine, BackupEngineOptions, RestoreOptions},
    Env,
};
use serde::{Deserialize, Serialize};
use tokio::{
    task::JoinHandle,
    time,
};
use tokio_util::sync::CancellationToken;
use typed_store::rocks::RocksDB;

use crate::node::errors::CheckpointError;

/// Configuration for RocksDB checkpoint management.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointConfig {
    /// Directory where backups will be stored.
    pub checkpoint_dir: Option<PathBuf>,
    /// Maximum number of backups to keep.
    pub max_backups: usize,
    /// How often to create backups (in seconds).
    pub checkpoint_interval: Duration,
    /// Whether to sync files to disk after each backup.
    pub sync: bool,
    /// Number of background operations for backup/restore.
    pub max_background_operations: i32,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            checkpoint_dir: None,
            max_backups: 5,
            checkpoint_interval: Duration::days(1),
            sync: true,
            max_background_operations: 2,
        }
    }
}

/// Result of a delayed task execution
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskResult<E> 
where 
    E: std::fmt::Debug + Send + 'static
{
    Success,
    Failed(E),
    TaskError(String), 
    Timeout,
}

/// A task scheduled to run at a specific time.
pub struct DelayedTask<E> 
where 
    E: std::fmt::Debug + Send + 'static
{
    /// Channel for task completion notification.
    completion_rx: tokio::sync::oneshot::Receiver<TaskResult<E>>,
    /// Timer task handle.
    timer_handle: JoinHandle<()>,
}

impl<E> DelayedTask<E> 
where 
    E: std::fmt::Debug + Send + 'static
{
    /// Create a new delayed task to run at the given time.
    pub fn new<F>(
        target_time: time::Instant,
        timeout_duration: StdDuration,
        task_fn: F,
    ) -> Self 
    where
        F: FnOnce() -> Result<(), E> + Send + 'static,
    {
        // Notification channel.
        let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();

        // Spawn the timer task
        let timer_handle = tokio::spawn(
            Self::execute_timer_task(
                target_time,
                timeout_duration,
                task_fn,
                completion_tx,
            )
        );
        
        Self {
            completion_rx,
            timer_handle,
        }
    }

    /// Executes the timer task logic without spawning the task itself.
    async fn execute_timer_task<F>(
        start_time: tokio::time::Instant,
        timeout_duration: StdDuration,
        task_fn: F,
        completion_tx: tokio::sync::oneshot::Sender<TaskResult<E>>,
    )
    where
        F: FnOnce() -> Result<(), E> + Send + 'static,
    {
        time::sleep_until(start_time).await;
        
        // Execute the task in a blocking thread.
        let worker_task = tokio::task::spawn_blocking(move || task_fn());

        // Wait for cancel, completion, or timeout.
        tokio::select! {
            result = worker_task => {
                let task_result = match result {
                    Ok(Ok(_)) => TaskResult::Success,
                    Ok(Err(e)) => TaskResult::Failed(e),
                    Err(e) => TaskResult::TaskError(format!("Task panicked: {}", e)),
                };
                
                let _ = completion_tx.send(task_result);
            }
            
            _ = tokio::time::sleep(timeout_duration) => {
                let _ = completion_tx.send(TaskResult::Timeout);
            }
        }
    }
}

impl<E> Future for DelayedTask<E>
where
    E: std::fmt::Debug + Send + 'static
{
    type Output = TaskResult<E>;
    
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Just poll the completion channel directly
        match Pin::new(&mut self.completion_rx).poll(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            Poll::Ready(Err(_)) => Poll::Ready(TaskResult::TaskError("Channel closed".to_string())),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<E> Drop for DelayedTask<E>
where
    E: std::fmt::Debug + Send + 'static
{
    fn drop(&mut self) {
        // Just abort the timer handle
        self.timer_handle.abort();
    }
}

/// Manages the creation/cleanup of checkpoints.
pub struct CheckpointManager {
    /// Checkpoint configuration.
    config: CheckpointConfig,
    /// Loop task handle.
    loop_handle: Option<JoinHandle<Result<(), CheckpointError>>>,
    /// Cancellation token for shutdown.
    cancel_token: CancellationToken,
    /// Database reference.
    db: Arc<RocksDB>,
}

impl CheckpointManager {
    /// Initial delay before first checkpoint creation, to avoid resource contention.
    const CHECKPOINT_CREATION_INITIAL_DELAY: Duration = Duration::minutes(15);

    pub async fn new(
        db: Arc<RocksDB>,
        config: CheckpointConfig,
        shutdown_token: CancellationToken,
    ) -> Result<Self, CheckpointError> {
        if let Some(checkpoint_dir) = config.checkpoint_dir.as_ref() {
            match checkpoint_dir.try_exists() {
                Ok(true) => (),
                Ok(false) => {
                    std::fs::create_dir_all(checkpoint_dir)
                    .map_err(|e| CheckpointError::Other(e.into()))?;
            }
                Err(e) => return Err(CheckpointError::Other(e.into())),
            }
        }

        let db_clone = db.clone();
        let config_clone = config.clone();
        let shutdown_token_clone = shutdown_token.clone();
        let loop_handle: JoinHandle<Result<(), CheckpointError>> = tokio::spawn(async move {
            Self::schedule_loop(
                db_clone,
                config_clone,
                shutdown_token_clone,
            ).await?;
            Ok(())
        });

        Ok(Self {
            config,
            loop_handle: Some(loop_handle),
            cancel_token: shutdown_token,
            db,
        })
    }

    /// Starts the background periodic checkpoint creation task.
    pub async fn schedule_loop(
        db: Arc<RocksDB>,
        config: CheckpointConfig,
        cancel_token: CancellationToken,
    ) -> Result<(), CheckpointError> {
        tracing::info!("checkpoint manager scheduled loop started.");
        let Some(checkpoint_dir) = config.checkpoint_dir.as_ref() else {
            return Err(CheckpointError::Other(anyhow::anyhow!("Checkpoint directory not set")));
        };

        // Try to calculate the first checkpoint time in a loop until successful.
        let mut next_checkpoint_time = loop {
            match Self::calculate_first_checkpoint_time(
                checkpoint_dir,
                config.checkpoint_interval,
            ).await {
                Ok(time) => break time,
                Err(e) => {
                    tracing::error!(?e, "Failed to calculate first checkpoint time, retrying...");
                    // Wait 10 minutes before retrying.
                    time::sleep(time::Duration::from_secs(600)).await;
                }
            }
        };
        tracing::info!("checkpoint manager next checkpoint time: {:?}", next_checkpoint_time);

        loop {
            // Create a new checkpoint creation task.
            let db = db.clone();
            let config = config.clone();
            let checkpoint_interval = config.checkpoint_interval;
            let delayed_task = DelayedTask::new(
                next_checkpoint_time,
                StdDuration::from_secs(3600), // 1 hour timeout
                move || Self::create_backup_impl(&db, &config),
            );

            tokio::select! {
                _ = cancel_token.cancelled() => {
                    break;
                }
                result = delayed_task => {
                    tracing::info!("Checkpoint task completed: {:?}", result);
                    next_checkpoint_time = Self::get_next_checkpoint_time(
                        checkpoint_interval,
                        &result,
                    );
                }
            }
        }
        
        Ok(())
    }

    /// Get the next checkpoint time based on the result of the previous checkpoint task.
    fn get_next_checkpoint_time(
        checkpoint_interval: Duration,
        result: &TaskResult<CheckpointError>,
    ) -> time::Instant {
        if let TaskResult::Success = result {
            time::Instant::now() + StdDuration::from_secs(
                checkpoint_interval.num_seconds() as u64
            )
        } else {
            time::Instant::now() + StdDuration::from_secs(
                Self::CHECKPOINT_CREATION_INITIAL_DELAY.num_seconds() as u64
            )
        }
    } 

    /// Calculate the first checkpoint time.
    async fn calculate_first_checkpoint_time(
        checkpoint_dir: &Path,
        checkpoint_interval: Duration,
    ) -> Result<time::Instant, CheckpointError> {
        let latest_timestamp = Self::get_latest_backup_timestamp(checkpoint_dir)?;
        let now = Utc::now().timestamp();
        
        let earliest = now + Self::CHECKPOINT_CREATION_INITIAL_DELAY.num_seconds();
        let next_ts = if let Some(last_ts) = latest_timestamp {
            std::cmp::max(last_ts + checkpoint_interval.num_seconds(), earliest)
        } else {
            earliest
        };
        
        let seconds_from_now = next_ts - now;
        let duration = std::time::Duration::from_secs(seconds_from_now as u64);
        
        // Return a tokio::time::Instant in the future
        Ok(time::Instant::now() + duration)
    }

    /// Get the timestamp of the latest backup, if any.
    pub fn get_latest_backup_timestamp(
        checkpoint_dir: &Path,
    ) -> Result<Option<i64>, CheckpointError> {
        // Use spawn_blocking for this operation
        let engine = Self::create_backup_engine(checkpoint_dir)?;
        let backup_info = engine.get_backup_info();

        if backup_info.is_empty() {
            return Ok(None);
        }

        // Find the backup with the latest timestamp
        let latest_timestamp = backup_info.iter().map(|info| info.timestamp).max();

        Ok(latest_timestamp)
    }

    /// Create a BackupEngine instance
    fn create_backup_engine(checkpoint_dir: &Path) -> Result<BackupEngine, CheckpointError> {
        let env = Env::new().map_err(|e| CheckpointError::Other(e.into()))?;

        let backup_opts =
            BackupEngineOptions::new(checkpoint_dir).map_err(|e| CheckpointError::Other(e.into()))?;

        BackupEngine::open(&backup_opts, &env).map_err(|e| CheckpointError::Other(e.into()))
    }

    /// Create a new backup.
    pub async fn create_backup(
        db: Arc<RocksDB>,
        checkpoint_dir: &Path,
    ) -> Result<(), CheckpointError> {
        let config = CheckpointConfig {
            checkpoint_dir: Some(checkpoint_dir.to_path_buf()),
            ..Default::default()
        };
        Self::create_backup_impl(&db, &config)
    }

    /// Delete old backups to maintain the max_backups limit (synchronous version)
    fn purge_old_backups(
        checkpoint_dir: &Path, 
        max_backups: usize,
    ) -> Result<(), CheckpointError> {
        if max_backups == 0 {
            return Ok(());
        }

        let mut engine = Self::create_backup_engine(checkpoint_dir)?;
        engine
            .purge_old_backups(max_backups)
            .map_err(|e| CheckpointError::Other(anyhow::anyhow!("Purge error: {}", e)))?;

        tracing::info!("purged old backups, keeping {} most recent", max_backups);
        Ok(())
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
        config: &CheckpointConfig,
    ) -> Result<(), CheckpointError> {
        let checkpoint_dir = config.checkpoint_dir.as_ref().unwrap();
        let mut engine = Self::create_backup_engine(&checkpoint_dir)?;

        let start_time = Instant::now();
        tracing::info!(
            checkpoint_dir = ?checkpoint_dir,
            "start creating RocksDB backup"
        );

        let db_ref = &db.underlying;
        let backup_result = engine
            .create_new_backup_flush(db_ref, config.sync)
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
}


#[cfg(test)]
mod tests {
    use std::{collections::HashMap, fs};

    use rand::Rng;
    use rocksdb::Options;
    use tempfile::tempdir;
    use typed_store::{
        rocks::{self, DBMap, MetricConf, ReadWriteOptions},
        Map,
    };

    use super::*;

    const MIN_BLOB_SIZE: u64 = 10;
    const BLOB_FILE_SIZE: u64 = 1024 * 1024;

    /// Generates key/value pairs with random sizes:
    /// - Half with values of random size between 1 and median_size bytes
    /// - Half with values of random size between (median_size+1) and (median_size*2) bytes
    pub fn generate_test_data(num_keys: usize, median_size: usize) -> HashMap<Vec<u8>, Vec<u8>> {
        let mut data = HashMap::new();
        let mut rng = rand::thread_rng();

        // Number of small and large values (half each)
        let small_count = num_keys / 2;
        let large_count = num_keys - small_count;

        // Generate small values (up to median_size bytes)
        for i in 0..small_count {
            let key = format!("small_key_{:03}", i).into_bytes();

            // Random size between 1 and median_size (inclusive)
            let small_size = rng.gen_range(1..=median_size);
            let value = vec![b's'; small_size];

            data.insert(key, value);
        }

        // Generate large values (more than median_size bytes)
        for i in 0..large_count {
            let key = format!("large_key_{:03}", i).into_bytes();

            // Random size between (median_size+1) and (median_size*2) inclusive
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

            // Create backup.
            CheckpointManager::create_backup(db, checkpoint_dir.path()).await?;
        }

        // Verify backup files exist
        assert!(
            fs::read_dir(&checkpoint_dir)?.count() > 0,
            "Backup directory should not be empty"
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

            // Open the restored database with properly configured options
            let db = rocks::open_cf_opts(
                restore_dir.path(),
                Some(db_opts),
                MetricConf::default(),
                &[("default", default_cf_opts)],
            )?;

            // Access the data through a DBMap
            let db_map = DBMap::<Vec<u8>, Vec<u8>>::reopen(
                &db,
                Some("default"),
                &ReadWriteOptions::default(),
                false,
            )?;

            // Check if all data is restored correctly

            for (key, expected_value) in &test_data {
                let value = db_map.get(key)?.expect("Key should exist in restored DB");
                assert_eq!(
                    &value, expected_value,
                    "Restored value should match original"
                );
            }
        }

        Ok(())
    }
}