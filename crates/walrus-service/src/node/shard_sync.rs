// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};

use tokio::sync::Mutex;
use walrus_core::ShardIndex;

use super::{
    errors::SyncShardError,
    storage::{ShardStatus, ShardStorage},
    StorageNodeInner,
};
use crate::node::errors::ShardNotAssigned;

/// Manages tasks for syncing shards during epoch change.
#[derive(Debug, Clone)]
pub struct ShardSyncHandler {
    node: Arc<StorageNodeInner>,
    shard_sync_in_progress: Arc<Mutex<HashMap<ShardIndex, tokio::task::JoinHandle<()>>>>,
}

impl ShardSyncHandler {
    pub fn new(node: Arc<StorageNodeInner>) -> Self {
        Self {
            node,
            shard_sync_in_progress: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    #[allow(dead_code)]
    /// Starts syncing a new shard. This method is used when a new shard is assigned to the node.
    pub async fn start_new_shard_sync(
        &self,
        shard_index: ShardIndex,
    ) -> Result<(), SyncShardError> {
        match self.node.storage.shard_storage(shard_index) {
            Some(shard_storage) => {
                let shard_status = shard_storage.status()?;
                if shard_status != ShardStatus::None {
                    return Err(SyncShardError::InvalidShardStatusToSync(
                        shard_index,
                        shard_status,
                    ));
                }
                self.start_shard_sync_impl(shard_storage.clone()).await;
                Ok(())
            }
            None => {
                tracing::error!(
                    "Shard index: {} is not assigned to this node. Cannot start shard sync.",
                    shard_index
                );
                Err(ShardNotAssigned(shard_index, self.node.current_epoch()).into())
            }
        }
    }

    /// Restarts syncing shards that were previously syncing. This method is used when restarting
    /// the node.
    pub async fn restart_syncs(&self) -> Result<(), anyhow::Error> {
        for shard_index in self.node.storage.shards() {
            let shard_storage = self.node.storage.shard_storage(shard_index).unwrap();

            // Restart the syncing task for shards that were previously syncing (in ActiveSync
            // status).
            if shard_storage.status()? == ShardStatus::ActiveSync {
                self.start_shard_sync_impl(shard_storage.clone()).await;
            }
        }
        Ok(())
    }

    async fn start_shard_sync_impl(&self, shard_storage: Arc<ShardStorage>) {
        let current_epoch = self.node.current_epoch();
        tracing::info!(
            "Syncing shard index {} to the end of epoch {}",
            shard_storage.id(),
            current_epoch
        );

        // TODO(#705): implement rate limiting for shard syncs.
        let mut shard_sync_in_progress = self.shard_sync_in_progress.lock().await;
        let Entry::Vacant(entry) = shard_sync_in_progress.entry(shard_storage.id()) else {
            tracing::info!(
                "Shard index: {} is already being synced. Skipping starting new sync task.",
                shard_storage.id()
            );
            return;
        };

        let node_clone = self.node.clone();
        let shard_sync_handler_clone = self.clone();
        let shard_sync_task = tokio::spawn(async move {
            let shard_index = shard_storage.id();
            let sync_result = shard_storage
                .start_sync_shard_before_epoch(current_epoch, node_clone)
                .await;

            if let Err(err) = sync_result {
                tracing::error!(
                    "Failed to sync shard index: {} to before epoch: {}. Error: {}",
                    shard_index,
                    current_epoch,
                    err
                );
            } else {
                tracing::info!(
                    "Successfully synced shard index: {} to before epoch: {}",
                    shard_index,
                    current_epoch
                );
            }

            // Remove the task from the shard_sync_in_progress map upon completion.
            shard_sync_handler_clone
                .shard_sync_in_progress
                .lock()
                .await
                .remove(&shard_index);
        });
        entry.insert(shard_sync_task);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::TestCluster;

    async fn create_test_cluster<'a>(assignment: &[&[u16]]) -> TestCluster {
        TestCluster::builder()
            .with_shard_assignment(assignment)
            .build()
            .await
            .unwrap()
    }

    #[tokio::test(start_paused = true)]
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
        let shard_sync_handler = ShardSyncHandler::new(cluster.nodes[0].storage_node.inner.clone());
        shard_sync_handler
            .restart_syncs()
            .await
            .expect("Failed to restart syncs");
        assert_eq!(
            shard_sync_handler.shard_sync_in_progress.lock().await.len(),
            2
        );
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

    #[tokio::test(start_paused = true)]
    async fn test_start_new_shard_sync() {
        let cluster = create_test_cluster(&[&[0]]).await;
        let shard_sync_handler = ShardSyncHandler::new(cluster.nodes[0].storage_node.inner.clone());

        assert!(matches!(
            shard_sync_handler.start_new_shard_sync(ShardIndex(0)).await,
            Err(SyncShardError::InvalidShardStatusToSync(..))
        ));

        cluster.nodes[0]
            .storage_node
            .inner
            .storage
            .shard_storage(ShardIndex(0))
            .expect("Failed to get shard storage")
            .update_status_in_test(ShardStatus::None)
            .expect("Failed to update shard status");

        assert_eq!(
            shard_sync_handler.shard_sync_in_progress.lock().await.len(),
            0
        );
        shard_sync_handler
            .start_new_shard_sync(ShardIndex(0))
            .await
            .expect("Failed to start new shard sync");
        assert_eq!(
            shard_sync_handler.shard_sync_in_progress.lock().await.len(),
            1
        );

        assert!(matches!(
            shard_sync_handler.start_new_shard_sync(ShardIndex(1)).await,
            Err(SyncShardError::ShardNotAssigned(..))
        ));
    }
}
