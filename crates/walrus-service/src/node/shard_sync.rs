// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};

use tokio::sync::Mutex;
use walrus_core::ShardIndex;
use walrus_sdk::error::ServiceError;
use walrus_utils::backoff::{self, ExponentialBackoff};

use super::{
    config::ShardSyncConfig,
    errors::SyncShardClientError,
    storage::{ShardStatus, ShardStorage},
    StorageNodeInner,
};
use crate::node::{errors::ShardNotAssigned, metrics};

/// Manages tasks for syncing shards during epoch change.
#[derive(Debug, Clone)]
pub struct ShardSyncHandler {
    node: Arc<StorageNodeInner>,
    shard_sync_in_progress: Arc<Mutex<HashMap<ShardIndex, tokio::task::JoinHandle<()>>>>,
    config: ShardSyncConfig,
}

impl ShardSyncHandler {
    pub fn new(node: Arc<StorageNodeInner>, config: ShardSyncConfig) -> Self {
        Self {
            node,
            shard_sync_in_progress: Arc::new(Mutex::new(HashMap::new())),
            config,
        }
    }

    /// Starts syncing a new shard. This method is used when a new shard is assigned to the node.
    pub async fn start_new_shard_sync(
        &self,
        shard_index: ShardIndex,
    ) -> Result<(), SyncShardClientError> {
        // restart_syncs() is called before event processor starts processing events. So, for any
        // resumed shard syncs, we should be able to observe them here, unless they have finished.
        if self
            .shard_sync_in_progress
            .lock()
            .await
            .contains_key(&shard_index)
        {
            tracing::info!(
                walrus.shard_index = %shard_index,
                "shard is already being synced; skipping starting new shard sync"
            );
            return Ok(());
        }

        match self.node.storage.shard_storage(shard_index) {
            Some(shard_storage) => {
                let shard_status = shard_storage.status()?;

                // When a shard is in active state, it has synced to the epoch that corresponding to
                // the epoch start event.
                if shard_status == ShardStatus::Active {
                    tracing::info!(
                        walrus.shard_index = %shard_index,
                        "shard has already been synced; skipping sync"
                    );
                    return Ok(());
                }

                if shard_status != ShardStatus::None {
                    return Err(SyncShardClientError::InvalidShardStatusToSync(
                        shard_index,
                        shard_status,
                    ));
                }

                // Update shard's status so that after this function returns, we can always restart
                // the sync upon node restart.
                shard_storage.set_start_sync_status()?;
                self.start_shard_sync_impl(shard_storage.clone()).await;
                Ok(())
            }
            None => {
                tracing::error!(
                    "{shard_index} is not assigned to this node; cannot start shard sync",
                );
                Err(ShardNotAssigned(shard_index, self.node.current_epoch()).into())
            }
        }
    }

    /// Restarts syncing shards that were previously syncing. This method is used when restarting
    /// the node.
    pub async fn restart_syncs(&self) -> Result<(), anyhow::Error> {
        for shard_storage in self.node.storage.existing_shard_storages() {
            // Restart the syncing task for shards that were previously syncing (in ActiveSync
            // status).
            let shard_status = shard_storage.status()?;
            if shard_status == ShardStatus::ActiveSync || shard_status == ShardStatus::ActiveRecover
            {
                self.start_shard_sync_impl(shard_storage.clone()).await;
            }
        }
        Ok(())
    }

    async fn start_shard_sync_impl(&self, shard_storage: Arc<ShardStorage>) {
        // This epoch must be the same as the epoch in the committee we refreshed when processing
        // epoch start event, or when the node starts up.
        let current_epoch = self.node.current_epoch();

        tracing::info!(
            walrus.shard_index = %shard_storage.id(),
            "syncing shard to the beginning of epoch {}",
            current_epoch
        );

        // TODO(#705): implement rate limiting for shard syncs.
        let mut shard_sync_in_progress = self.shard_sync_in_progress.lock().await;
        let Entry::Vacant(entry) = shard_sync_in_progress.entry(shard_storage.id()) else {
            // We have checked the shard_sync_in_progress map before starting the sync task. So,
            // this is an unexpected state.
            tracing::error!(
                shard_index=%shard_storage.id(),
                "shard is already being synced; skipping starting new sync task",
            );
            return;
        };

        let node_clone = self.node.clone();
        let shard_sync_handler_clone = self.clone();
        let shard_sync_task = tokio::spawn(async move {
            let shard_index = shard_storage.id();

            let backoff = ExponentialBackoff::new_with_seed(
                shard_sync_handler_clone.config.shard_sync_retry_min_backoff,
                shard_sync_handler_clone.config.shard_sync_retry_max_backoff,
                None,
                shard_index.0 as u64, // Seed the backoff with the shard index.
            );

            backoff::retry(backoff, || async {
                metrics::with_label!(node_clone.metrics.shard_sync_total, "start").inc();
                let sync_result = shard_storage
                    .start_sync_shard_before_epoch(
                        current_epoch,
                        node_clone.clone(),
                        &shard_sync_handler_clone.config,
                    )
                    .await;

                if let Err(error) = sync_result {
                    metrics::with_label!(node_clone.metrics.shard_sync_total, "error").inc();
                    tracing::error!(
                        ?error,
                        "failed to sync {shard_index} to before epoch {current_epoch}"
                    );

                    if let SyncShardClientError::RequestError(node_error) = error {
                        if let Some(ServiceError::InvalidEpoch {
                            request_epoch,
                            server_epoch,
                        }) = node_error.service_error()
                        {
                            if request_epoch > server_epoch {
                                tracing::info!(
                                    request_epoch,
                                    server_epoch,
                                    "source storage node hasn't reached the epoch yet"
                                );
                                // Retry the sync after backoff.
                                return false;
                            }
                        }
                    }
                } else {
                    metrics::with_label!(node_clone.metrics.shard_sync_total, "complete").inc();
                    tracing::info!(
                        walrus.shard_index = %shard_index,
                        "successfully synced shard to before epoch {}",
                        current_epoch
                    );
                }
                // TODO(#705): also do retries for other retriable errors. E.g. RPC error.
                true
            })
            .await;

            // Remove the task from the shard_sync_in_progress map upon completion.
            let epoch_sync_done;
            {
                let mut shard_sync_map =
                    shard_sync_handler_clone.shard_sync_in_progress.lock().await;
                shard_sync_map.remove(&shard_index);
                epoch_sync_done = shard_sync_map.is_empty();
            }

            if epoch_sync_done {
                shard_sync_handler_clone
                    .node
                    .contract_service
                    .epoch_sync_done(current_epoch)
                    .await;
            }
        });
        entry.insert(shard_sync_task);
    }

    #[cfg(test)]
    pub async fn current_sync_task_count(&self) -> usize {
        self.shard_sync_in_progress.lock().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{StorageNodeHandle, TestCluster};

    async fn create_test_cluster<'a>(assignment: &[&[u16]]) -> TestCluster {
        TestCluster::<StorageNodeHandle>::builder()
            .with_shard_assignment(assignment)
            .build()
            .await
            .unwrap()
    }

    #[tokio::test(start_paused = false)]
    async fn test_restart_syncs() {
        let cluster = create_test_cluster(&[&[0, 1, 2]]).await;
        for i in [0, 2] {
            cluster.nodes[0]
                .storage_node
                .inner
                .storage
                .shard_storage(ShardIndex(i))
                .expect("Failed to get shard storage")
                .update_status_in_test(ShardStatus::ActiveSync)
                .expect("Failed to update shard status");
        }
        let shard_sync_handler = ShardSyncHandler::new(
            cluster.nodes[0].storage_node.inner.clone(),
            ShardSyncConfig::default(),
        );
        shard_sync_handler
            .restart_syncs()
            .await
            .expect("Failed to restart syncs");
        assert_eq!(shard_sync_handler.current_sync_task_count().await, 2);
        assert!(shard_sync_handler
            .shard_sync_in_progress
            .lock()
            .await
            .contains_key(&ShardIndex(0)));
        assert!(shard_sync_handler
            .shard_sync_in_progress
            .lock()
            .await
            .contains_key(&ShardIndex(2)));
    }

    #[tokio::test(start_paused = false)]
    async fn test_start_new_shard_sync() {
        let cluster = create_test_cluster(&[&[0]]).await;
        let shard_sync_handler = ShardSyncHandler::new(
            cluster.nodes[0].storage_node.inner.clone(),
            ShardSyncConfig::default(),
        );

        cluster.nodes[0]
            .storage_node
            .inner
            .storage
            .shard_storage(ShardIndex(0))
            .expect("Failed to get shard storage")
            .update_status_in_test(ShardStatus::LockedToMove)
            .expect("Failed to update shard status");

        assert!(matches!(
            shard_sync_handler.start_new_shard_sync(ShardIndex(0)).await,
            Err(SyncShardClientError::InvalidShardStatusToSync(..))
        ));

        cluster.nodes[0]
            .storage_node
            .inner
            .storage
            .shard_storage(ShardIndex(0))
            .expect("Failed to get shard storage")
            .update_status_in_test(ShardStatus::None)
            .expect("Failed to update shard status");

        assert_eq!(shard_sync_handler.current_sync_task_count().await, 0);
        shard_sync_handler
            .start_new_shard_sync(ShardIndex(0))
            .await
            .expect("Failed to start new shard sync");
        assert_eq!(shard_sync_handler.current_sync_task_count().await, 1);

        assert!(matches!(
            shard_sync_handler.start_new_shard_sync(ShardIndex(1)).await,
            Err(SyncShardClientError::ShardNotAssigned(..))
        ));
    }
}
