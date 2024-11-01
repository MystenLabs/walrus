// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::{Arc, Mutex};

use futures::future::{join_all, try_join_all};
use typed_store::TypedStoreError;
use walrus_core::Epoch;

use super::{blob_sync::BlobSyncHandler, StorageNodeInner};
use crate::node::storage::blob_info::BlobInfoApi;

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

    pub fn start_node_recovery(&self, epoch: Epoch) -> Result<(), TypedStoreError> {
        let start = tokio::time::Instant::now();
        let mut locked_task_handle = self.task_handle.lock().unwrap();
        assert!(locked_task_handle.is_none());

        let node: Arc<StorageNodeInner> = self.node.clone();
        let blob_sync_handler = self.blob_sync_handler.clone();
        let task_handle = tokio::spawn(async move {
            loop {
                let mut all_blob_syncs = Vec::new();
                let mut contain_recovery_blob = false;
                for blob_result in node.storage.certified_blob_info_iter_before_epoch(epoch) {
                    match blob_result {
                        Ok((blob_id, blob_info)) => {
                            if !blob_info.is_certified(epoch) {
                                // Skip blobs that are not certified in the given epoch. This includes blobs that are
                                // invalid or expired.
                                tracing::info!(
                                    blob_id = ?blob_id,
                                    epoch = epoch,
                                    "skip non-certified blob"
                                );
                                continue;
                            }

                            if let Ok(stored_at_all_shards) =
                                node.storage.is_stored_at_all_shards(&blob_id)
                            {
                                if stored_at_all_shards {
                                    tracing::info!(
                                        blob_id = ?blob_id,
                                        epoch = epoch,
                                        "blob is stored at all shards; skip recovery"
                                    );
                                    continue;
                                }
                            } else {
                                tracing::warn!(blob_id = ?blob_id, "failed to check if blob is stored at all shards; start blob sync");
                            }

                            contain_recovery_blob = true;

                            tracing::info!("ZZZZZZ start recovery sync for blob");

                            // TODO: rate limit start sync to avoid OOM.
                            let start_sync_result = blob_sync_handler
                                .start_sync(
                                    blob_id,
                                    blob_info.initial_certified_epoch().expect(
                                        "certifed blob should have an initial certified epoch set",
                                    ),
                                    None,
                                    start,
                                )
                                .await;
                            match start_sync_result {
                                Ok(notify) => {
                                    all_blob_syncs.push(notify);
                                }
                                Err(err) => {
                                    tracing::error!(
                                        blob_id = ?blob_id,
                                        error = ?err,
                                        "failed to start recovery sync for blob",
                                    );
                                }
                            }
                        }
                        Err(err) => {
                            tracing::error!(error = ?err, "failed to read certified blob");
                        }
                    }
                }

                if !contain_recovery_blob {
                    tracing::info!("no recovery blob found; stop recovery task");
                    break;
                }

                let notify_futures: Vec<_> = all_blob_syncs
                    .iter()
                    .map(|notify| notify.notified())
                    .collect();
                join_all(notify_futures).await;
            }

            tracing::info!("node recovery task finished; set node status to active");

            match node
                .storage
                .set_node_status(crate::node::NodeStatus::Active)
            {
                Ok(()) => node.contract_service.epoch_sync_done(epoch).await,
                Err(error) => {
                    tracing::error!(error = ?error, "failed to set node status to active");
                }
            }
        });
        *locked_task_handle = Some(task_handle);

        Ok(())
    }

    pub fn restart_recovery(&self) -> Result<(), TypedStoreError> {
        if self.node.storage.node_status()? == crate::node::NodeStatus::RecoveryInProgress {
            return self.start_node_recovery(self.node.current_epoch());
        }
        Ok(())
    }
}
