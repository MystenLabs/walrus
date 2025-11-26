// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Garbage-collection functionality running in the background.

use std::{hash::Hasher as _, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use rand::{Rng, SeedableRng, rngs::StdRng};
use serde::{Deserialize, Serialize};
use serde_with::{DurationSeconds, serde_as};
use tokio::sync::Mutex;
use walrus_core::Epoch;
use walrus_sui::types::GENESIS_EPOCH;

use crate::node::{StorageNodeInner, metrics::NodeMetricSet};

/// Configuration for garbage collection and related tasks.
#[serde_as]
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq)]
#[serde(default)]
pub struct GarbageCollectionConfig {
    /// Whether to enable the blob info cleanup at the beginning of each epoch.
    pub enable_blob_info_cleanup: bool,
    /// Whether to delete metadata and slivers of expired or deleted blobs.
    pub enable_data_deletion: bool,
    /// Whether to add a random delay before starting garbage collection.
    /// The delay is deterministically computed based on the node's public key and epoch,
    /// uniformly distributed between 0 and half the epoch duration.
    pub enable_random_delay: bool,
    /// The time window for randomization of the garbage collection start time.
    ///
    /// Half the epoch duration is used if not specified or if this is larger than half the epoch
    /// duration.
    #[serde_as(as = "Option<DurationSeconds>")]
    #[serde(rename = "randomization_time_window_secs")]
    pub randomization_time_window: Option<Duration>,
    /// The batch size for processing expired blob objects.
    ///
    /// Items are processed in batches using `spawn_blocking` to avoid blocking the async runtime
    /// and make it possible to abort the task if the node is shutting down.
    pub blob_objects_batch_size: usize,
    /// The batch size for deleting expired blob data.
    ///
    /// Items are processed in batches using `spawn_blocking` to avoid blocking the async runtime
    /// and make it possible to abort the task if the node is shutting down.
    pub data_deletion_batch_size: usize,
}

impl Default for GarbageCollectionConfig {
    fn default() -> Self {
        Self {
            // TODO(WAL-1105): Enable this by default.
            enable_blob_info_cleanup: false,
            // TODO(WAL-1105): Enable this by default.
            enable_data_deletion: false,
            enable_random_delay: true,
            randomization_time_window: None,
            blob_objects_batch_size: 10_000,
            data_deletion_batch_size: 1000,
        }
    }
}

impl GarbageCollectionConfig {
    /// Returns a default configuration for testing.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn default_for_test() -> Self {
        Self {
            enable_blob_info_cleanup: true,
            enable_data_deletion: true,
            enable_random_delay: true,
            randomization_time_window: Some(Duration::from_secs(1)),
            blob_objects_batch_size: 1000,
            data_deletion_batch_size: 1000,
        }
    }
}

/// Garbage collector running in the background.
#[derive(Debug, Clone)]
pub(super) struct GarbageCollector {
    /// Configuration for garbage collection.
    config: GarbageCollectionConfig,
    /// The node that the garbage collector is running on.
    node: Arc<StorageNodeInner>,
    /// The metrics for the garbage collector.
    metrics: Arc<NodeMetricSet>,
    /// Handle to the background task performing database cleanup.
    task_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl GarbageCollector {
    pub fn new(
        config: GarbageCollectionConfig,
        node: Arc<StorageNodeInner>,
        metrics: Arc<NodeMetricSet>,
    ) -> Self {
        Self {
            config,
            node,
            metrics,
            task_handle: Arc::new(Mutex::new(None)),
        }
    }

    /// Schedules database cleanup operations to run in a background task.
    ///
    /// If a cleanup task is already running, it will be aborted and replaced with a new one.
    ///
    /// Must only be called *after* the epoch change for the provided epoch is complete.
    ///
    /// The actual cleanup work starts after a deterministic delay computed by
    /// [`Self::cleanup_target_time`] based on the node's public key and epoch This is to avoid
    /// multiple nodes performing cleanup operations at the same time, which could impact the
    /// performance of the network.
    ///
    /// # Errors
    ///
    /// Returns an error if the garbage-collection task cannot be started.
    #[tracing::instrument(skip_all)]
    pub async fn start_garbage_collection_task(
        &self,
        epoch: Epoch,
        epoch_start: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        if epoch == GENESIS_EPOCH {
            tracing::info!("garbage collection is not relevant in the genesis epoch");
            return Ok(());
        }

        let garbage_collection_config = self.config;

        if !garbage_collection_config.enable_blob_info_cleanup {
            if garbage_collection_config.enable_data_deletion {
                tracing::warn!(
                    "data deletion is enabled, but requires blob info cleanup to be enabled; \
                    skipping data deletion",
                );
            } else {
                tracing::info!("garbage collection is disabled, skipping cleanup");
            }
            return Ok(());
        }

        let mut task_handle = self.task_handle.lock().await;
        let garbage_collector = self.clone();

        // If there is an existing task, we need to abort it first before starting a new one.
        if let Some(old_task) = task_handle.take() {
            tracing::info!("aborting existing garbage-collection task before starting a new one");
            old_task.abort();
        }

        // Store the current epoch in the DB before spawning the background task.
        self.node
            .storage
            .set_garbage_collector_last_started_epoch(epoch)?;
        self.metrics
            .set_garbage_collection_last_started_epoch(epoch);

        // Calculate target time and update metric before spawning the background task.
        let target_time = self.cleanup_target_time(epoch, epoch_start);
        self.metrics
            .garbage_collection_task_start_time
            .set(target_time.timestamp().try_into().unwrap_or_default());

        let new_task = tokio::spawn(async move {
            // Sleep until the target time.
            let sleep_duration = (target_time - Utc::now())
                .to_std()
                .unwrap_or(Duration::ZERO);
            if !sleep_duration.is_zero() {
                tracing::info!(
                    target_time = target_time.to_rfc3339(),
                    ?sleep_duration,
                    "sleeping before performing garbage collection",
                );
                tokio::time::sleep(sleep_duration).await;
            }

            if let Err(error) = garbage_collector.perform_db_cleanup_task(epoch).await {
                tracing::error!(?error, epoch, "garbage-collection task failed");
            }
        });

        *task_handle = Some(new_task);

        Ok(())
    }

    /// Aborts any running garbage-collection task.
    pub(crate) async fn abort(&self) {
        if let Some(task_handle) = self.task_handle.lock().await.take() {
            task_handle.abort();
        }
    }

    /// Calculates the target time for cleanup by optionally adding a deterministic delay (based on
    /// the node's public key and epoch) to the epoch start time.
    ///
    /// The random delay is uniformly distributed between 0 and half the epoch duration (or the
    /// configured randomization time window if specified). If random delay is disabled, returns the
    /// epoch start time immediately.
    fn cleanup_target_time(&self, epoch: Epoch, epoch_start: DateTime<Utc>) -> DateTime<Utc> {
        if !self.config.enable_random_delay {
            return epoch_start;
        }

        let half_epoch_duration = self.node.system_parameters.epoch_duration / 2;
        let max_delay = self
            .config
            .randomization_time_window
            .unwrap_or(half_epoch_duration)
            .min(half_epoch_duration);
        let public_key = self.node.protocol_key_pair.public();

        // Create a deterministic seed from the public key bytes and epoch.
        // Use a hash function to combine them into a u64 seed.
        let public_key_bytes = bcs::to_bytes(public_key).unwrap_or_default();
        let mut hasher = twox_hash::XxHash64::with_seed(u64::from(epoch));
        hasher.write(public_key_bytes.as_ref());
        let random_seed = hasher.finish();

        // Generate delay uniformly distributed between 0 and half the epoch duration
        let max_delay_millis = max_delay
            .as_millis()
            .try_into()
            .expect("epoch duration is shorter than 500M years");

        epoch_start
            + Duration::from_millis(
                StdRng::seed_from_u64(random_seed).gen_range(0..max_delay_millis),
            )
    }

    /// Performs database cleanup operations including blob info cleanup and data deletion.
    ///
    /// Must only be run if blob info cleanup is enabled.
    ///
    /// # Errors
    ///
    /// Returns an error if a DB operation fails or one of the cleanup tasks fails.
    ///
    /// # Panics
    ///
    /// Panics if blob info cleanup is not enabled.
    async fn perform_db_cleanup_task(&self, epoch: Epoch) -> anyhow::Result<()> {
        assert!(
            self.config.enable_blob_info_cleanup,
            "garbage-collection task must only be run if blob info cleanup is enabled"
        );

        // Disable DB compactions during cleanup to improve performance. DB compactions are
        // automatically re-enabled when the guard is dropped.
        let _guard = self.node.storage.temporarily_disable_auto_compactions()?;
        tracing::info!("starting garbage collection");

        self.node
            .storage
            .process_expired_blob_objects(epoch, &self.metrics, self.config.blob_objects_batch_size)
            .await?;

        if self.config.enable_data_deletion {
            if self
                .node
                .storage
                .delete_expired_blob_data(
                    epoch,
                    &self.metrics,
                    self.config.data_deletion_batch_size,
                )
                .await?
            {
                // Update the last completed epoch after successful cleanup.
                self.node
                    .storage
                    .set_garbage_collector_last_completed_epoch(epoch)?;
                self.metrics
                    .set_garbage_collection_last_completed_epoch(epoch);
            }
        }

        Ok(())
    }
}
