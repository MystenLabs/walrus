// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};

#[cfg(msim)]
use sui_macros::fail_point_if;
use tokio::sync::Mutex;
use walrus_core::ShardIndex;
use walrus_sdk::error::ServiceError;
use walrus_utils::backoff::{self, ExponentialBackoff};

use super::{
    config::ShardSyncConfig,
    errors::SyncShardClientError,
    storage::{ShardStatus, ShardStorage},
    NodeStatus,
    StorageNodeInner,
};
use crate::node::{errors::ShardNotAssigned, metrics, storage::blob_info::BlobInfoApi};

/// Manages tasks for syncing shards during epoch change.
#[derive(Debug, Clone)]
pub struct ShardSyncHandler {
    node: Arc<StorageNodeInner>,
    shard_sync_in_progress: Arc<Mutex<HashMap<ShardIndex, tokio::task::JoinHandle<()>>>>,
    task_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    config: ShardSyncConfig,
}

impl ShardSyncHandler {
    pub fn new(node: Arc<StorageNodeInner>, config: ShardSyncConfig) -> Self {
        Self {
            node,
            shard_sync_in_progress: Arc::new(Mutex::new(HashMap::new())),
            task_handle: Arc::new(Mutex::new(None)),
            config,
        }
    }

    /// Starts sync shards. If `recover_metadata` is true, sync certified blob metadata before
    /// syncing shards.
    pub async fn start_sync_shards(
        &self,
        shards: Vec<ShardIndex>,
        recover_metadata: bool,
    ) -> Result<(), SyncShardClientError> {
        let mut task_handle = self.task_handle.lock().await;
        let sync_handler_clone = self.clone();
        let old_task = task_handle.replace(tokio::spawn(async move {
            sync_handler_clone
                .sync_shards_task(shards, recover_metadata)
                .await
        }));

        if let Some(old_task) = old_task {
            old_task.abort();
        }

        Ok(())
    }

    async fn sync_shards_task(&self, shards: Vec<ShardIndex>, recover_metadata: bool) {
        if recover_metadata {
            assert!(
                self.node
                    .storage
                    .node_status()
                    .expect("reading db should not fail")
                    == NodeStatus::RecoverMetadata
            );

            if let Err(sync_metadata_error) = self.sync_certified_blob_metadata().await {
                tracing::error!(
                    ?sync_metadata_error,
                    "failed to sync blob metadata; aborting shard sync"
                );
                return;
            }
        }

        for shard in shards {
            if let Err(error) = self.start_new_shard_sync(shard).await {
                tracing::error!(
                    ?error,
                    %shard,
                    "failed to start shard sync; aborting shard sync"
                );
                continue;
            }
        }

        // Once we have started the shard sync task, the shard status has been persisted to
        // disk, so we can mark the node as active. Any restart from this point will re-start
        // the shard sync tasks only without syncing metadata again.
        if self
            .node
            .storage
            .node_status()
            .expect("reading db should not fail")
            == NodeStatus::RecoverMetadata
        {
            self.node
                .set_node_status(NodeStatus::Active)
                .expect("setting node status should not fail");
        }
    }

    /// Syncs the certified blob metadata before the current epoch.
    async fn sync_certified_blob_metadata(&self) -> Result<(), SyncShardClientError> {
        tracing::info!("start syncing blob metadata");
        let blob_infos = self
            .node
            .storage
            .certified_blob_info_iter_before_epoch(self.node.current_epoch());

        #[cfg(msim)]
        {
            let mut sync_blob_metadata_error = false;
            fail_point_if!("fail_point_shard_sync_recovery_metadata_error", || {
                sync_blob_metadata_error = true
            });
            if sync_blob_metadata_error {
                return Err(SyncShardClientError::Internal(anyhow::anyhow!(
                    "fail point triggered sync blob metadata error"
                )));
            }
        }

        // TODO(WAL-478):
        //   - create a end point that can transfer multiple blob metadata at once.
        //   - do this in parallel to speed up the sync.
        for blob_info in blob_infos {
            let (blob_id, blob_info) = blob_info?;

            self.node
                .get_or_recover_blob_metadata(
                    &blob_id,
                    blob_info
                        .initial_certified_epoch()
                        .expect("certified blob must have certified epoch set"),
                )
                .await?;
        }
        tracing::info!("finished syncing blob metadata");
        Ok(())
    }

    /// Starts syncing a new shard. This method is used when a new shard is assigned to the node.
    // TODO: make this function private.
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
        let current_node_status = self.node.storage.node_status()?;
        if current_node_status == NodeStatus::RecoverMetadata {
            let shards_to_sync = self
                .node
                .storage
                .existing_shard_storages()
                .iter()
                .map(|s| s.id())
                .collect::<Vec<_>>();

            let sync_handler_clone = self.clone();
            self.task_handle
                .lock()
                .await
                .replace(tokio::spawn(async move {
                    sync_handler_clone
                        .sync_shards_task(shards_to_sync, true)
                        .await
                }));
        } else {
            for shard_storage in self.node.storage.existing_shard_storages() {
                // Restart the syncing task for shards that were previously syncing (in ActiveSync
                // status).
                let shard_status = shard_storage.status()?;
                if shard_status == ShardStatus::ActiveSync
                    || shard_status == ShardStatus::ActiveRecover
                {
                    self.start_shard_sync_impl(shard_storage.clone()).await;
                }
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

            // TODO(WAL-444): see if we can remove the mutex.
            let directly_recover_shard = Arc::new(Mutex::new(false));

            backoff::retry(backoff, || async {
                metrics::with_label!(node_clone.metrics.shard_sync_total, "start").inc();
                let recover_shard = *directly_recover_shard.lock().await;
                let sync_result = shard_storage
                    .start_sync_shard_before_epoch(
                        current_epoch,
                        node_clone.clone(),
                        &shard_sync_handler_clone.config,
                        recover_shard,
                    )
                    .await;

                match sync_result {
                    Ok(_) => {
                        metrics::with_label!(node_clone.metrics.shard_sync_total, "complete").inc();
                        tracing::info!(
                            walrus.shard_index = %shard_index,
                            "successfully synced shard to before epoch {}",
                            current_epoch
                        );
                        true // Exit retry loop
                    }

                    Err(error) => {
                        metrics::with_label!(node_clone.metrics.shard_sync_total, "error").inc();
                        tracing::error!(
                            ?error,
                            "failed to sync {shard_index} to before epoch {current_epoch}"
                        );

                        #[cfg(msim)]
                        if check_no_retry_fail_point() {
                            return true;
                        }

                        // Check for invalid epoch error
                        if let SyncShardClientError::RequestError(node_error) = &error {
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
                                    return false; // Retry after backoff
                                }
                            }
                        }

                        // Try direct recovery if not already doing so
                        if !recover_shard {
                            *directly_recover_shard.lock().await = true;
                            tracing::info!(
                                walrus.shard_index = %shard_index,
                                "shard sync failed; directly recovering shard next time"
                            );
                            false // Retry with direct recovery
                        } else {
                            // TODO(#705): also do retries for other retriable errors. E.g. RPC
                            // error.
                            true // Exit retry loop if direct recovery also failed
                        }
                    }
                }
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

    #[cfg(all(msim, test, feature = "test-utils"))]
    pub async fn no_pending_recover_metadata(&self) -> bool {
        let task_handle = self.task_handle.lock().await;
        task_handle.is_none() || task_handle.as_ref().unwrap().is_finished()
    }
}

// Helper function for fail point testing
#[cfg(msim)]
fn check_no_retry_fail_point() -> bool {
    let mut no_retry = false;
    sui_macros::fail_point_if!("fail_point_shard_sync_no_retry", || { no_retry = true });
    no_retry
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
