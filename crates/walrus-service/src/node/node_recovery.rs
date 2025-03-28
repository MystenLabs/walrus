// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::sync::{Arc, Mutex};

use futures::future::join_all;
use sui_macros::fail_point_async;
use typed_store::TypedStoreError;
use walrus_core::Epoch;

use super::{blob_sync::BlobSyncHandler, StorageNodeInner};
use crate::node::{storage::blob_info::BlobInfoApi, NodeStatus};

#[derive(Debug, Clone)]
pub struct NodeRecoveryHandler {
    node: Arc<StorageNodeInner>,
    blob_sync_handler: Arc<BlobSyncHandler>,

    // There can be at most one background shard removal task at a time.
    task_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl NodeRecoveryHandler {
    pub fn new(node: Arc<StorageNodeInner>, blob_sync_handler: Arc<BlobSyncHandler>) -> Self {
        Self {
            node,
            blob_sync_handler,
            task_handle: Arc::new(Mutex::new(None)),
        }
    }

    /// Starts the node recovery process to recover blobs that are certified before the given epoch.
    /// For blobs that are certified after `certified_before_epoch`, the event processing is in
    /// charge of making sure the blob is stored at all shards.
    ///
    /// # Panics
    ///
    /// Panics if the task handle mutex is poisoned or if a recovery task is already running.
    pub async fn start_node_recovery(
        &self,
        certified_before_epoch: Epoch,
    ) -> Result<(), TypedStoreError> {
        let mut locked_task_handle = self
            .task_handle
            .lock()
            .expect("Task handle mutex should not be poisoned");
        assert!(
            locked_task_handle.is_none(),
            "Node recovery task is already running"
        );

        let node = self.node.clone();
        let blob_sync_handler = self.blob_sync_handler.clone();
        let task_handle = tokio::spawn(async move {
            fail_point_async!("start_node_recovery_entry");
            loop {
                let mut all_blob_syncs = Vec::new();
                tracing::info!(
                    "scanning blobs to recover certified blobs before epoch {}",
                    certified_before_epoch
                );
                for (blob_id, blob_info) in node
                    .storage
                    .certified_blob_info_iter_before_epoch(certified_before_epoch)
                    .filter_map(|blob_result| {
                        blob_result
                            .inspect_err(|error| {
                                tracing::error!(?error, "failed to read certified blob")
                            })
                            .ok()
                    })
                {
                    // Note that here we need to use the current epoch to check if the blob is
                    // still certified. If the blob is retired, we don't need to recover it anymore.
                    if !blob_info.is_certified(node.current_epoch()) {
                        // Skip blobs that are not certified in the given epoch. This
                        // includes blobs that are invalid or expired.
                        tracing::debug!(
                            walrus.blob_id = %blob_id,
                            walrus.blob_certified_before_epoch = certified_before_epoch,
                            walrus.current_epoch = node.current_epoch(),
                            "skip non-certified blob"
                        );
                        continue;
                    }

                    if let Ok(stored_at_all_shards) = node.is_stored_at_all_shards(&blob_id).await {
                        if stored_at_all_shards {
                            tracing::debug!(
                                walrus.blob_certified_before_epoch = certified_before_epoch,
                                walrus.current_epoch = node.current_epoch(),
                                "blob is stored at all shards; skip recovery"
                            );
                            continue;
                        }
                    } else {
                        tracing::warn!(
                            walrus.blob_id = %blob_id,
                            "failed to check if blob is stored at all shards; start blob sync"
                        );
                    }

                    tracing::debug!(
                        walrus.blob_id = %blob_id,
                        "start recovery sync for blob"
                    );
                    // TODO: rate limit start sync to avoid OOM.
                    let start_sync_result = blob_sync_handler
                        .start_sync(
                            blob_id,
                            blob_info.initial_certified_epoch().expect(
                                "certified blob should have an initial certified epoch set",
                            ),
                            None,
                        )
                        .await;
                    match start_sync_result {
                        Ok(notify) => {
                            all_blob_syncs.push(notify);
                        }
                        Err(err) => {
                            // The only place where start_sync can fail is when marking the
                            // event complete, which is not applicable here since the there
                            // is no event associated with the recovery task.
                            panic!(
                                "failed to start recovery sync for blob {}: {}",
                                blob_id, err,
                            );
                        }
                    }
                }

                if all_blob_syncs.is_empty() {
                    tracing::info!("no recovery blob found; stop recovery task");
                    break;
                }

                let notify_futures: Vec<_> = all_blob_syncs
                    .iter()
                    .map(|notify| notify.notified())
                    .collect();
                join_all(notify_futures).await;

                // TODO(WAL-669): right now, we have to do one more loop to check if all the blobs
                // are recovered. This is not efficient because checking blob existence is
                // expensive. It's better that blob sync handler can return the blob sync status
                // and we can avoid the extra loop of all the blob syncs finished successfully.
            }

            let current_node_status = node
                .storage
                .node_status()
                .expect("reading node status should not fail");
            if current_node_status == NodeStatus::RecoveryInProgress(certified_before_epoch) {
                tracing::info!("node recovery task finished; set node status to active");
                match node.set_node_status(NodeStatus::Active) {
                    Ok(()) => {
                        node.contract_service
                            .epoch_sync_done(certified_before_epoch, node.node_capability())
                            .await
                    }
                    Err(error) => {
                        tracing::error!(?error, "failed to set node status to active");
                    }
                }
            } else {
                tracing::warn!(
                    node_status = %current_node_status,
                    "node recovery task finished; but node status is not RecoveryInProgress; \
                    skip setting node status to active"
                );
            }
        });
        *locked_task_handle = Some(task_handle);

        Ok(())
    }

    /// Restarts any in progress recovery.
    pub async fn restart_recovery(&self) -> Result<(), TypedStoreError> {
        if let NodeStatus::RecoveryInProgress(recovering_epoch) = self.node.storage.node_status()? {
            if recovering_epoch == self.node.current_epoch() {
                return self.start_node_recovery(self.node.current_epoch()).await;
            } else {
                assert!(recovering_epoch < self.node.current_epoch());
                tracing::warn!(
                    recovering_epoch,
                    current_epoch = self.node.current_epoch(),
                    "recovery epoch mismatch; skip recovery restart; next epoch change start event \
                    will bring node to the latest state"
                );
                self.node.set_node_status(NodeStatus::RecoveryCatchUp)?;
            }
        }
        Ok(())
    }
}
