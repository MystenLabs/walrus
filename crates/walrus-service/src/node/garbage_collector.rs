// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Garbage-collection functionality running in the background.

use std::{hash::Hasher as _, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use rand::{Rng, SeedableRng, rngs::StdRng};
use serde::{Deserialize, Serialize};
use serde_with::{DurationSeconds, serde_as};
use sui_macros::fail_point_async;
use tokio::sync::Mutex;
use walrus_core::Epoch;
use walrus_sui::types::GENESIS_EPOCH;

use crate::node::{StorageNodeInner, metrics::NodeMetricSet};

/// Configuration for garbage collection and related tasks.
#[serde_as]
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq)]
#[serde(default)]
pub struct GarbageCollectionConfig {
    /// Whether to enable blob info cleanup (phase 1) at the beginning of each epoch.
    ///
    /// This is a prerequisite for data deletion (phase 2): disabling blob info cleanup
    /// implicitly disables data deletion as well, because data deletion relies on up-to-date
    /// aggregate blob info refcounts produced by phase 1.
    pub enable_blob_info_cleanup: bool,
    /// Whether to delete metadata and slivers of expired or deleted blobs (phase 2).
    ///
    /// Only effective when `enable_blob_info_cleanup` is also `true`.
    pub enable_data_deletion: bool,
    /// Whether to immediately delete blob data when a `BlobDeleted` event is processed.
    ///
    /// When disabled, data is only deleted during periodic garbage collection. Only relevant if
    /// `enable_data_deletion` is `true`.
    pub enable_immediate_data_deletion: bool,
    /// Whether to add a random delay before the data deletion phase of garbage collection.
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
            enable_blob_info_cleanup: true,
            enable_data_deletion: true,
            enable_immediate_data_deletion: false,
            enable_random_delay: true,
            randomization_time_window: None,
            blob_objects_batch_size: 5000,
            data_deletion_batch_size: 2000,
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
            enable_immediate_data_deletion: true,
            enable_random_delay: true,
            randomization_time_window: Some(Duration::from_secs(1)),
            blob_objects_batch_size: 10,
            data_deletion_batch_size: 5,
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

    /// Phase 1: Cleans up per-object blob info for expired blobs.
    ///
    /// This phase processes the `storage_pool_info` and `per_object_blob_info` tables to remove
    /// entries for blobs that are no longer registered. These tables are critical for node
    /// recovery, so this phase is designed to run promptly at the epoch boundary (without random
    /// delay) to produce a consistent snapshot across nodes.
    ///
    /// Must only be called *after* the epoch change for the provided epoch is complete.
    ///
    /// # Errors
    ///
    /// Returns an error if a DB operation fails.
    #[tracing::instrument(skip_all)]
    pub(crate) async fn perform_blob_info_cleanup(&self, epoch: Epoch) -> anyhow::Result<()> {
        if epoch == GENESIS_EPOCH {
            tracing::info!("garbage collection is not relevant in the genesis epoch");
            return Ok(());
        }

        if !self.config.enable_blob_info_cleanup {
            tracing::info!("blob info cleanup is disabled, skipping");
            return Ok(());
        }

        // Disable DB compactions during cleanup to improve performance. DB compactions are
        // automatically re-enabled when the guard is dropped.
        let _guard = self.node.storage.temporarily_disable_auto_compactions()?;
        tracing::info!("starting garbage collection phase 1: blob info cleanup");

        self.node
            .storage
            .process_expired_storage_pools(
                epoch,
                &self.metrics,
                self.config.blob_objects_batch_size,
            )
            .await?;

        self.node
            .storage
            .process_expired_blob_objects(epoch, &self.metrics, self.config.blob_objects_batch_size)
            .await?;

        self.metrics
            .set_garbage_collection_blob_info_cleanup_completed_epoch(epoch);

        Ok(())
    }

    /// Schedules the data deletion phase to run in a background task.
    ///
    /// If a data deletion task is already running, it will be aborted and replaced with a new one.
    ///
    /// Must only be called *after* [`Self::perform_blob_info_cleanup`] for the same epoch.
    ///
    /// The actual deletion starts after a deterministic delay computed by
    /// [`Self::cleanup_target_time`] based on the node's public key and epoch. This is to avoid
    /// multiple nodes performing heavy I/O at the same time.
    ///
    /// # Errors
    ///
    /// Returns an error if the data deletion task cannot be started.
    #[tracing::instrument(skip_all)]
    pub(crate) async fn start_data_deletion_task(
        &self,
        epoch: Epoch,
        epoch_start: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        if epoch == GENESIS_EPOCH {
            return Ok(());
        }

        if !self.config.enable_blob_info_cleanup {
            if self.config.enable_data_deletion {
                tracing::warn!(
                    "data deletion is enabled, but requires blob info cleanup to be enabled; \
                    skipping data deletion",
                );
            } else {
                tracing::info!("garbage collection is disabled, skipping data deletion");
            }
            return Ok(());
        }

        if !self.config.enable_data_deletion {
            return Ok(());
        }

        let mut task_handle = self.task_handle.lock().await;
        let garbage_collector = self.clone();

        // If there is an existing task, we need to abort it first before starting a new one.
        if let Some(old_task) = task_handle.take() {
            tracing::info!("aborting existing data-deletion task before starting a new one");
            old_task.abort();
            let _ = old_task.await;
        }

        // Calculate target time for data deletion and update the metric.
        // Note: the metric name `garbage_collection_task_start_time` is kept for backwards
        // compatibility, but it now refers specifically to the data deletion phase (phase 2).
        let data_deletion_target_time = self.cleanup_target_time(epoch, epoch_start);
        self.metrics.garbage_collection_task_start_time.set(
            data_deletion_target_time
                .timestamp()
                .try_into()
                .unwrap_or_default(),
        );

        let new_task = tokio::spawn(async move {
            let sleep_duration = (data_deletion_target_time - Utc::now())
                .to_std()
                .unwrap_or(Duration::ZERO);
            if !sleep_duration.is_zero() {
                tracing::info!(
                    target_time = data_deletion_target_time.to_rfc3339(),
                    ?sleep_duration,
                    "sleeping before performing data deletion",
                );
                tokio::time::sleep(sleep_duration).await;
            }

            garbage_collector
                .metrics
                .set_garbage_collection_data_deletion_started_epoch(epoch);
            if let Err(error) = garbage_collector.perform_data_deletion(epoch).await {
                tracing::error!(
                    ?error,
                    epoch,
                    "garbage collection phase 2 (data deletion) failed"
                );
            }
        });

        *task_handle = Some(new_task);

        Ok(())
    }

    /// Aborts any running data-deletion task.
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

    /// Phase 2: Deletes metadata and slivers for expired/deleted blobs.
    ///
    /// This phase performs the heavy I/O work of deleting actual blob data (metadata and slivers)
    /// from storage. It is intended to run after a random delay to avoid multiple nodes performing
    /// heavy I/O simultaneously.
    ///
    /// # Errors
    ///
    /// Returns an error if a DB operation fails.
    async fn perform_data_deletion(&self, epoch: Epoch) -> anyhow::Result<()> {
        // Disable DB compactions during data deletion to improve performance. DB compactions are
        // automatically re-enabled when the guard is dropped.
        let _guard = self.node.storage.temporarily_disable_auto_compactions()?;
        tracing::info!("starting garbage collection phase 2: data deletion");
        fail_point_async!("data_deletion_start");

        if self
            .node
            .storage
            .delete_expired_blob_data(epoch, &self.metrics, self.config.data_deletion_batch_size)
            .await?
        {
            // Update the last completed epoch after successful data deletion.
            self.node
                .storage
                .set_garbage_collector_last_completed_epoch(epoch)?;
            self.metrics
                .set_garbage_collection_last_completed_epoch(epoch);
        }

        Ok(())
    }
}
