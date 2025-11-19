//! Garbage-collection functionality running in the background.

use std::sync::Arc;

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
        }
    }

    // TODO(WAL-1040): Move this to a background task.
    /// Performs database cleanup operations including blob info cleanup and data deletion.
    #[tracing::instrument(skip_all)]
    pub async fn perform_db_cleanup(&self, epoch: Epoch) -> anyhow::Result<()> {
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
