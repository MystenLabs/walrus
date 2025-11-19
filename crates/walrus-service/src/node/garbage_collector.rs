//! Garbage-collection functionality running in the background.

use std::{sync::Arc, time::Duration};

use rand::Rng;
use tokio::sync::Mutex;
use walrus_core::Epoch;

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
    /// If a cleanup task is already running, it will be aborted and replaced with a new one.
    /// The actual cleanup work starts after a random delay (0-60 seconds).
    #[tracing::instrument(skip_all)]
    pub async fn perform_db_cleanup(&self, epoch: Epoch) -> anyhow::Result<()> {
        let mut task_handle = self.task_handle.lock().await;
        let garbage_collector = self.clone();

        // If there is an existing task, we need to abort it first before starting a new one.
        if let Some(old_task) = task_handle.take() {
            old_task.abort();
        }

        let new_task = tokio::spawn(async move {
            // Wait for a random delay before starting the cleanup work.
            let delay_seconds = rand::thread_rng().gen_range(0..60);
            tokio::time::sleep(Duration::from_secs(delay_seconds)).await;

            if let Err(err) = garbage_collector.perform_db_cleanup_task(epoch).await {
                tracing::error!(?err, epoch = epoch, "garbage collection task failed");
            }
        });

        *task_handle = Some(new_task);

        Ok(())
    }

    /// Performs database cleanup operations including blob info cleanup and data deletion.
    async fn perform_db_cleanup_task(&self, epoch: Epoch) -> anyhow::Result<()> {
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

        // Disable DB compactions during cleanup to improve performance. DB compactions are
        // automatically re-enabled when the guard is dropped.
        let _guard = self.node.storage.temporarily_disable_auto_compactions()?;

        // The if condition is redundant, but it's kept in case we change the if condition above.
        if garbage_collection_config.enable_blob_info_cleanup {
            self.node
                .storage
                .process_expired_blob_objects(epoch, &self.metrics)
                .await?;
        }

        if garbage_collection_config.enable_data_deletion {
            self.node
                .storage
                .delete_expired_blob_data(epoch, &self.metrics)
                .await?;
        }

        Ok(())
    }
}
