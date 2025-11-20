//! Garbage-collection functionality running in the background.

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::Arc,
    time::Duration,
};

use chrono::{DateTime, Utc};
use rand::{Rng, SeedableRng, rngs::StdRng};
use tokio::sync::Mutex;
use walrus_core::Epoch;
use walrus_sui::types::GENESIS_EPOCH;

use crate::node::{StorageNodeInner, config::GarbageCollectionConfig, metrics::NodeMetricSet};

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
    /// The actual cleanup work starts after a deterministic delay based on the node's public key
    /// and epoch, uniformly distributed between 0 and half the epoch duration. This is to avoid
    /// multiple nodes performing cleanup operations at the same time, which could impact the
    /// performance of the network.
    #[tracing::instrument(skip_all)]
    pub async fn start_garbage_collection_task(&self, epoch: Epoch) -> anyhow::Result<()> {
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
            old_task.abort();
        }

        // Store the current epoch in the DB before spawning the background task.
        self.node
            .storage
            .set_garbage_collector_last_started_epoch(epoch)?;

        // Calculate target time and update metric before spawning the background task.
        let target_time = self.cleanup_target_time(epoch).await?;
        self.metrics
            .cleanup_task_start_time
            .set(target_time.timestamp().try_into().unwrap_or_default());

        let new_task = tokio::spawn(async move {
            // Sleep until the target time (relative to epoch start)
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
                tracing::error!(?error, epoch, "garbage collection task failed");
            }
        });

        *task_handle = Some(new_task);

        Ok(())
    }

    /// Calculates the target time for cleanup.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The epoch is the genesis epoch.
    /// - The current epoch does not match the requested epoch.
    /// - The epoch change is in progress.
    async fn cleanup_target_time(&self, epoch: Epoch) -> anyhow::Result<DateTime<Utc>> {
        anyhow::ensure!(
            epoch != GENESIS_EPOCH,
            "garbage collection cannot be performed in the genesis epoch"
        );
        let (current_epoch, state) = self.node.contract_service.get_epoch_and_state().await?;
        anyhow::ensure!(
            current_epoch == epoch,
            "epoch mismatch while attempting to start garbage collection: current epoch is \
            {current_epoch}, requested epoch is {epoch}",
        );

        let epoch_start = state.start_of_current_epoch().ok_or(anyhow::anyhow!(
            "cannot schedule garbage collection while epoch change is in progress"
        ))?;
        Ok(epoch_start + self.compute_deterministic_delay(epoch))
    }

    /// Computes a deterministic delay based on the node's public key and epoch.
    /// The delay is uniformly distributed between 0 and half the epoch duration.
    fn compute_deterministic_delay(&self, epoch: Epoch) -> Duration {
        let epoch_duration = self.node.system_parameters.epoch_duration;
        let max_delay = epoch_duration / 2;
        let public_key = self.node.protocol_key_pair.public();

        // Create a deterministic seed from the public key bytes and epoch.
        // Use a hash function to combine them into a u64 seed.
        let public_key_bytes = bcs::to_bytes(public_key).unwrap_or_default();
        let mut hasher = DefaultHasher::new();
        public_key_bytes.hash(&mut hasher);
        epoch.hash(&mut hasher);
        let seed = hasher.finish();

        // Generate delay uniformly distributed between 0 and half the epoch duration
        let max_delay_millis = max_delay.as_millis() as u64;
        Duration::from_millis(StdRng::seed_from_u64(seed).gen_range(0..max_delay_millis))
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
            .process_expired_blob_objects(epoch, &self.metrics)
            .await?;

        if self.config.enable_data_deletion {
            self.node
                .storage
                .delete_expired_blob_data(epoch, &self.metrics)
                .await?;
        }

        // Update the last completed epoch after successful cleanup.
        // TODO(mlegner): Should we do this even if data deletion is not enabled?
        self.node
            .storage
            .set_garbage_collector_last_completed_epoch(epoch)?;

        Ok(())
    }
}
