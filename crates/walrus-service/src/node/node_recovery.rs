// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{sync::Arc, time::Duration};

use futures::stream::{FuturesUnordered, StreamExt};
use sui_macros::fail_point_async;
use tokio::sync::Mutex;
use typed_store::TypedStoreError;
use walrus_core::Epoch;

use super::{
    StorageNodeInner,
    blob_sync::{BlobSyncHandler, SyncStatus},
    config::NodeRecoveryConfig,
};
use crate::node::{NodeStatus, storage::blob_info::CertifiedBlobInfoApi};

/// How long node recovery backs off before re-scanning when it owns a shard at the latest epoch
/// whose local storage has not been created yet. The sleep also yields the executor so event
/// processing can create the shard and restart recovery.
const SHARD_NOT_CREATED_BACKOFF: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub struct NodeRecoveryHandler {
    node: Arc<StorageNodeInner>,
    blob_sync_handler: Arc<BlobSyncHandler>,

    // There can be at most one background shard removal task at a time.
    task_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,

    // Configuration for node recovery.
    config: NodeRecoveryConfig,
}

impl NodeRecoveryHandler {
    pub fn new(
        node: Arc<StorageNodeInner>,
        blob_sync_handler: Arc<BlobSyncHandler>,
        config: NodeRecoveryConfig,
    ) -> Self {
        Self {
            node,
            blob_sync_handler,
            task_handle: Arc::new(Mutex::new(None)),
            config,
        }
    }

    /// Starts the node recovery process to recover blobs that are certified before the given epoch.
    /// For blobs that are certified after `certified_before_epoch`, the event processing is in
    /// charge of making sure the blob is stored at all shards.
    ///
    /// Any existing recovery task will be canceled.
    // TODO(WAL-864): Refactor this function to make it readable.
    pub async fn start_node_recovery(
        &self,
        certified_before_epoch: Epoch,
    ) -> Result<(), TypedStoreError> {
        let mut locked_task_handle = self.task_handle.lock().await;

        // Cancel any existing recovery task
        if let Some(old_task) = locked_task_handle.take() {
            tracing::info!("canceling existing node recovery task");
            old_task.abort();
            // Wait for the old task to complete (it will return a JoinError due to cancellation)
            let _ = old_task.await;
        }

        let node = self.node.clone();
        let blob_sync_handler = self.blob_sync_handler.clone();
        let max_concurrent_blob_syncs_during_recovery =
            self.config.max_concurrent_blob_syncs_during_recovery;
        let task_handle = tokio::spawn(async move {
            tracing::info!("waiting for latest event epoch to be set to restart node recovery");
            // When current_event_epoch() returns during node start up, if the node is lagging
            // behind, the node status will also be set to RecoveryCatchUp. So the recovery
            // task does not need to run in this case.
            node.current_event_epoch()
                .await
                .expect("current event epoch watch channel should not be dropped");

            tracing::info!(
                "starting node recovery task to recover blobs certified before epoch {}",
                certified_before_epoch
            );

            fail_point_async!("start_node_recovery_entry");

            // Clear any backoff count left over from a previous (aborted) recovery task on this
            // node; `wait_until_owned_shards_exist` maintains it from here.
            node.metrics.node_recovery_shard_not_created_backoff.set(0);

            loop {
                // Block until the node is ready to run a recovery scan pass. Stops the recovery
                // task if the node started catching up.
                match wait_until_ready_to_scan(&node).await {
                    ScanReadiness::Ready => {}
                    ScanReadiness::CatchingUp => return,
                }

                // Keep track of ongoing blob syncs. Note that the memory usage of this list
                // is capped by `max_concurrent_blob_syncs_during_recovery`.
                let mut ongoing_syncs = FuturesUnordered::new();

                // Keep track of whether there are more blobs to recover.
                let mut has_more_blobs = false;
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
                    node.metrics
                        .node_recovery_recover_blob_progress
                        .set(i64::from(blob_id.first_two_bytes()));

                    // Note that here we need to use the current epoch to check if the blob is
                    // still certified. If the blob is retired, we don't need to recover it anymore.
                    if !blob_info.is_certified(node.current_committee_epoch()) {
                        // Skip blobs that are not certified in the given epoch. This
                        // includes blobs that are invalid or expired.
                        tracing::debug!(
                            walrus.blob_id = %blob_id,
                            walrus.blob_certified_before_epoch = certified_before_epoch,
                            walrus.current_epoch = node.current_committee_epoch(),
                            "skip non-certified blob"
                        );
                        continue;
                    }

                    // The node will only enter recovery mode if it has caught up to the latest
                    // epoch. So we only need to check the latest epoch for the shard assignment.
                    if let Ok(stored_at_all_shards) =
                        node.is_stored_at_all_shards_at_latest_epoch(&blob_id).await
                    {
                        if stored_at_all_shards {
                            tracing::debug!(
                                walrus.blob_certified_before_epoch = certified_before_epoch,
                                walrus.current_epoch = node.current_committee_epoch(),
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

                    // There are more blobs to recover.
                    has_more_blobs = true;

                    // Limit the number of concurrent blob syncs to avoid overwhelming the system.
                    // Note that checking the length of `ongoing_syncs` is sufficient since the loop
                    // adds blob sync tasks sequentially.
                    if ongoing_syncs.len() >= max_concurrent_blob_syncs_during_recovery {
                        tracing::debug!(
                            walrus.blob_id = %blob_id,
                            number_of_tasks = %ongoing_syncs.len(),
                            "max concurrent blob syncs reached; wait for one to complete"
                        );
                        while ongoing_syncs.len() >= max_concurrent_blob_syncs_during_recovery {
                            ongoing_syncs.next().await;
                        }

                        // Since there is a wait, the blob might not be certified anymore. Check
                        // again before starting the sync.
                        if !blob_info.is_certified(node.current_committee_epoch()) {
                            // Skip blobs that are not certified in the given epoch. This
                            // includes blobs that are invalid or expired.
                            tracing::debug!(
                                walrus.blob_id = %blob_id,
                                walrus.blob_certified_before_epoch = certified_before_epoch,
                                walrus.current_epoch = node.current_committee_epoch(),
                                "skip non-certified blob, post concurrency limit wait"
                            );
                            continue;
                        }
                    }

                    tracing::debug!(
                        walrus.blob_id = %blob_id,
                        recoverying_epoch = certified_before_epoch,
                        "start recovery sync for blob"
                    );
                    node.metrics.node_recovery_ongoing_blob_syncs.inc();
                    let start_sync_result = blob_sync_handler
                        .start_sync(
                            blob_id,
                            blob_info.initial_certified_epoch().expect(
                                "certified blob should have an initial certified epoch set",
                            ),
                            None,
                        )
                        .await;
                    sui_macros::fail_point!("fail_point_node_recovery_start_sync");
                    match start_sync_result {
                        Ok(mut receiver) => {
                            let node_clone = node.clone();
                            // Create a future that releases the permit when the sync completes
                            let notify_with_permit = async move {
                                // We don't care about the outcome here — this loop re-scans
                                // and re-evaluates whether the blob still needs recovery.
                                let _ = receiver
                                    .wait_for(|status| matches!(status, SyncStatus::Done(_)))
                                    .await;
                                node_clone.metrics.node_recovery_ongoing_blob_syncs.dec();
                            };
                            ongoing_syncs.push(notify_with_permit);
                        }
                        Err(err) => {
                            // The only place where start_sync can fail is when marking the
                            // event complete, which is not applicable here since the there
                            // is no event associated with the recovery task.
                            panic!("failed to start recovery sync for blob {blob_id}: {err}",);
                        }
                    }
                }

                if !has_more_blobs {
                    tracing::info!("no recovery blob found; stop recovery task");
                    break;
                }

                // Wait for all ongoing syncs to complete
                while (ongoing_syncs.next().await).is_some() {
                    // Each sync completion automatically releases its permit
                }

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
    pub async fn restart_recovery(&self) -> anyhow::Result<()> {
        if let NodeStatus::RecoveryInProgress(recovering_epoch) = self.node.storage.node_status()? {
            tracing::info!(
                "restarting node recovery to recover to the epoch {}",
                recovering_epoch
            );

            self.start_node_recovery(recovering_epoch).await?;
        }

        Ok(())
    }
}

/// Outcome of waiting for the node to become ready to run a recovery scan pass.
enum ScanReadiness {
    /// The node has local storage for every shard it owns at the latest committee epoch; the
    /// caller should run a scan pass.
    Ready,
    /// The node started catching up; the recovery task should stop. It is restarted once the node
    /// has caught up to the latest epoch.
    CatchingUp,
}

/// Blocks until the node is ready to run a recovery scan pass ([`ScanReadiness::Ready`]), or
/// returns [`ScanReadiness::CatchingUp`] if the node started catching up. Both conditions are
/// re-checked on every backoff iteration, so catch-up that begins while waiting also stops the
/// task.
///
/// A node can own a shard at the latest committee epoch whose local storage does not exist yet: the
/// committee advanced to a newer epoch, but event processing has not yet handled that epoch's
/// `EpochChangeStart`, which is what creates the shard. Recovery must not create it here: blob sync
/// targets `owned_shards_at_epoch(current_event_epoch)`, which still excludes the shard, and shard
/// creation is part of the ordered epoch transition owned by event processing. If the scan ran in
/// this state, every blob would read "not stored" on the missing shard, producing futile syncs and
/// repeated full re-scans (and, on the single-threaded simulator, starving event processing
/// entirely). So back off and re-check until the shard exists; the sleep also yields the executor.
async fn wait_until_ready_to_scan(node: &StorageNodeInner) -> ScanReadiness {
    let mut backoff_count: i64 = 0;
    loop {
        if node
            .storage
            .node_status()
            .expect("reading node status should not fail")
            .is_catching_up()
        {
            tracing::info!("node recovery encountered node is in catching up; skip recovery");
            return ScanReadiness::CatchingUp;
        }

        let existing_shards = node.storage.existing_shards().await;
        let uncreated_owned_shard = node
            .owned_shards_at_latest_epoch()
            .into_iter()
            .find(|shard| !existing_shards.contains(shard));

        let Some(shard) = uncreated_owned_shard else {
            // All owned shards exist; clear the stall gauge (if it was raised) and let the caller
            // proceed with the scan.
            if backoff_count != 0 {
                node.metrics.node_recovery_shard_not_created_backoff.set(0);
            }
            return ScanReadiness::Ready;
        };

        backoff_count += 1;
        node.metrics
            .node_recovery_shard_not_created_backoff
            .set(backoff_count);
        tracing::warn!(
            %shard,
            backoff_count,
            "owned shard storage not created yet; backing off for event processing"
        );
        tokio::time::sleep(SHARD_NOT_CREATED_BACKOFF).await;
    }
}
