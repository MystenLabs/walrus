// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{sync::Arc, time::Duration};

use futures::stream::{FuturesUnordered, StreamExt};
use sui_macros::fail_point_async;
use tokio::sync::Mutex;
use typed_store::TypedStoreError;
use walrus_core::{Epoch, ShardIndex};
use walrus_utils::backoff::{BackoffStrategy, ExponentialBackoff};

use super::{
    StorageNodeInner,
    blob_sync::{BlobSyncHandler, SyncStatus},
    config::NodeRecoveryConfig,
    shard_sync::ShardSyncHandler,
};
use crate::node::{
    NodeStatus,
    storage::{ShardStatus, blob_info::CertifiedBlobInfoApi},
};

/// Exponential backoff bounds for re-checking shard creation while node recovery waits for event
/// processing to create a shard it owns at the latest epoch. The wait starts small so the common
/// case resumes quickly, and is capped at one minute. The sleep also yields the executor so
/// event processing can create the shard and self-heal.
const SHARD_NOT_CREATED_BACKOFF_MIN: Duration = Duration::from_secs(1);
const SHARD_NOT_CREATED_BACKOFF_MAX: Duration = Duration::from_secs(60);

/// Interval at which a recovery task that has finished blob recovery re-checks the status of
/// owned shards that are not `Active` while no shard sync is running (a terminally failed shard
/// sync requires a node restart to be retried).
const UNSYNCED_SHARD_RECHECK_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug, Clone)]
pub struct NodeRecoveryHandler {
    node: Arc<StorageNodeInner>,
    blob_sync_handler: Arc<BlobSyncHandler>,
    // Used to wait for ongoing shard syncs before recovering blobs: shard sync fills
    // newly gained shards far more cheaply than per-blob recovery, so recovery defers
    // to it.
    shard_sync_handler: ShardSyncHandler,

    // There can be at most one background shard removal task at a time.
    task_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,

    // Serializes recovery-related node status transitions: the epoch-change path advances the
    // recovery target and starts shard syncs while holding this mutex, the catch-up path holds
    // it across writing a new recovery target and aborting the previous recovery task, and the
    // recovery task transitions the node to `Active` while holding it. This guarantees that a
    // completing recovery task either observes the advanced target together with the new shard
    // syncs, or has completed entirely before the transition (in which case the epoch-change
    // path starts a new task) — and that a task from before a catch-up can never complete a
    // recovery target written after the catch-up, which its scan does not cover (blob certified
    // events are skipped while catching up).
    status_mutex: Arc<Mutex<()>>,

    // Configuration for node recovery.
    config: NodeRecoveryConfig,
}

impl NodeRecoveryHandler {
    pub fn new(
        node: Arc<StorageNodeInner>,
        blob_sync_handler: Arc<BlobSyncHandler>,
        shard_sync_handler: ShardSyncHandler,
        config: NodeRecoveryConfig,
    ) -> Self {
        Self {
            node,
            blob_sync_handler,
            shard_sync_handler,
            task_handle: Arc::new(Mutex::new(None)),
            status_mutex: Arc::new(Mutex::new(())),
            config,
        }
    }

    /// Locks the recovery status mutex.
    ///
    /// The epoch-change path must hold the returned guard across advancing the recovery target
    /// (setting the node status to a newer `RecoveryInProgress` epoch), starting the shard
    /// syncs for newly gained shards, and locking the shards that moved away, so that these are
    /// atomic with respect to the recovery task's completion; otherwise, the task could attest
    /// epoch sync done while the node still accepts slivers for shards it no longer owns. The
    /// catch-up path must hold it across writing its recovery target and aborting the previous
    /// recovery task (via [`Self::start_node_recovery`]), so that a task from before the
    /// catch-up cannot complete a target whose blobs it never scanned.
    ///
    /// Lock ordering: this mutex must be acquired *before* the storage shard map lock. The
    /// recovery task's completion attempt reads shard statuses (which takes the shard map read
    /// lock) while holding this mutex, so acquiring this mutex while holding the shard map lock
    /// deadlocks with it.
    pub async fn lock_status(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.status_mutex.lock().await
    }

    /// Ensures a recovery task is running to recover to the given epoch.
    ///
    /// The recovery task keeps running across epoch changes and picks up the advanced recovery
    /// target on its own, so this normally does nothing. A new task is only started if the
    /// caller observed that the previous task completed concurrently with the epoch change
    /// (`task_completed_concurrently`), or if no task is running (for example, because it
    /// stopped unexpectedly).
    pub async fn ensure_recovery_task_running(
        &self,
        epoch: Epoch,
        task_completed_concurrently: bool,
    ) -> Result<(), TypedStoreError> {
        let task_running = self
            .task_handle
            .lock()
            .await
            .as_ref()
            .is_some_and(|handle| !handle.is_finished());

        if task_completed_concurrently || !task_running {
            tracing::info!(
                walrus.epoch = epoch,
                task_completed_concurrently,
                task_running,
                "no running node recovery task; starting a new one"
            );
            self.start_node_recovery(epoch).await?;
        }

        Ok(())
    }

    /// Starts the node recovery process to recover blobs that are certified before the given epoch.
    /// For blobs that are certified after `certified_before_epoch`, the event processing is in
    /// charge of making sure the blob is stored at all shards.
    ///
    /// The task keeps running across epoch changes: blobs certified after the scan bound are
    /// covered by event processing, and shards gained at later epoch changes are covered by shard
    /// sync (which the task waits for), so the scan bound stays valid. The task completes only
    /// once blob recovery is done and every owned shard is in `Active` status, that is, shard
    /// sync has delivered every gained shard; it then attests epoch sync done for the recovery
    /// target currently recorded in the node status, which the epoch-change path advances.
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
        let shard_sync_handler = self.shard_sync_handler.clone();
        let status_mutex = self.status_mutex.clone();
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

            loop {
                // Block until the node is ready to run a recovery scan pass. Stops the recovery
                // task if the node started catching up.
                match wait_until_ready_to_scan(&node, &shard_sync_handler).await {
                    ScanReadiness::Ready => {}
                    ScanReadiness::CatchingUp => return,
                }

                // Keep track of ongoing blob syncs. Note that the memory usage of this list
                // is capped by `max_concurrent_blob_syncs_during_recovery`.
                let mut ongoing_syncs = FuturesUnordered::new();

                // Keep track of whether there are more blobs to recover.
                let mut has_more_blobs = false;
                // Whether this scan pass was interrupted because a shard sync started. An
                // interrupted pass has not verified all blobs, so a new pass is required even if
                // no blob needing recovery was found. This is deliberately kept separate from
                // `has_more_blobs`: once blob sync outcomes are tracked directly and the
                // verification re-scan is removed (WAL-669), an interrupted pass still requires
                // a new round of blob recovery.
                let mut scan_pass_interrupted = false;
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
                    // An epoch change may have started shard syncs for newly gained shards while
                    // this pass is running. Stop starting new blob syncs and park: per-blob
                    // recovery would redundantly decode slivers for the shards that shard sync is
                    // copying in bulk. The loop drains the in-flight syncs and starts a new scan
                    // pass after the park.
                    if shard_sync_handler.has_sync_in_progress() {
                        tracing::info!(
                            "shard sync started during recovery scan pass; pausing blob recovery"
                        );
                        scan_pass_interrupted = true;
                        break;
                    }

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

                // Wait for all ongoing syncs to complete
                while (ongoing_syncs.next().await).is_some() {
                    // Each sync completion automatically releases its permit
                }

                // An interrupted pass has not verified all blobs; run a new pass (which parks
                // until the shard syncs that interrupted it have finished).
                if scan_pass_interrupted {
                    continue;
                }

                if has_more_blobs {
                    // TODO(WAL-669): right now, we have to do one more loop to check if all the
                    // blobs are recovered. This is not efficient because checking blob existence
                    // is expensive. It's better that blob sync handler can return the blob sync
                    // status and we can avoid the extra loop of all the blob syncs finished
                    // successfully.
                    continue;
                }

                // Blob recovery (this task's scan) is done; wait for shard sync to deliver
                // every owned shard and then complete the recovery.
                complete_recovery_once_shards_synced(&node, &shard_sync_handler, &status_mutex)
                    .await;
                return;
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
/// returns [`ScanReadiness::CatchingUp`] if the node started catching up. All conditions are
/// re-checked on every iteration, so catch-up that begins while waiting also stops the task.
///
/// Readiness requires, besides the node not catching up:
///
/// - No shard sync is in progress. Shard sync fills newly gained shards far more cheaply than
///   per-blob recovery (and per-blob recovery would redundantly decode slivers for shards that
///   shard sync is copying in bulk), so recovery defers to it.
/// - Local storage exists for every shard the node owns at the latest committee epoch. A node can
///   own a shard whose local storage does not exist yet: the committee advanced to a newer epoch,
///   but event processing has not yet handled that epoch's `EpochChangeStart`, which is what
///   creates the shard. Recovery must not create it here: blob sync targets
///   `owned_shards_at_epoch(current_event_epoch)`, which still excludes the shard, and shard
///   creation is part of the ordered epoch transition owned by event processing. If the scan ran
///   in this state, every blob would read "not stored" on the missing shard, producing futile
///   syncs and repeated full re-scans (and, on the single-threaded simulator, starving event
///   processing entirely). So back off and re-check until the shard exists; the sleep also yields
///   the executor.
async fn wait_until_ready_to_scan(
    node: &StorageNodeInner,
    shard_sync_handler: &ShardSyncHandler,
) -> ScanReadiness {
    // Exponential backoff, recreated per stall episode so a fresh stall starts at the small bound.
    // The seed only feeds the jitter offset; a constant keeps it deterministic under the simulator.
    let mut backoff = ExponentialBackoff::new_with_seed(
        SHARD_NOT_CREATED_BACKOFF_MIN,
        SHARD_NOT_CREATED_BACKOFF_MAX,
        None,
        0,
    );
    let mut total_wait = Duration::ZERO;
    let readiness = loop {
        // Wait for ongoing shard syncs to finish first, so that the status check below always
        // runs after the (possibly long) wait and picks up catch-up that began while parked.
        if shard_sync_handler.has_sync_in_progress() {
            tracing::info!("waiting for ongoing shard syncs before scanning blobs to recover");
            shard_sync_handler.wait_until_no_sync_in_progress().await;
        }

        if node
            .storage
            .node_status()
            .expect("reading node status should not fail")
            .is_catching_up()
        {
            tracing::info!("node recovery encountered node is in catching up; skip recovery");
            break ScanReadiness::CatchingUp;
        }

        let existing_shards = node.storage.existing_shards().await;
        let missing_owned_shards: Vec<_> = node
            .owned_shards_at_latest_epoch()
            .into_iter()
            .filter(|shard| !existing_shards.contains(shard))
            .collect();

        let Some(first_missing) = missing_owned_shards.first().copied() else {
            break ScanReadiness::Ready;
        };

        let delay = backoff
            .next_delay()
            .unwrap_or(SHARD_NOT_CREATED_BACKOFF_MAX);
        total_wait += delay;
        node.metrics
            .node_recovery_shard_wait_seconds
            .set(i64::try_from(total_wait.as_secs()).unwrap_or(i64::MAX));
        node.metrics
            .node_recovery_missing_owned_shards
            .set(i64::try_from(missing_owned_shards.len()).unwrap_or(i64::MAX));
        tracing::warn!(
            shard = %first_missing,
            missing_owned_shards = missing_owned_shards.len(),
            wait_secs = total_wait.as_secs(),
            "owned shard storage not created yet; waiting for event processing"
        );
        tokio::time::sleep(delay).await;
    };

    node.metrics.node_recovery_shard_wait_seconds.set(0);
    node.metrics.node_recovery_missing_owned_shards.set(0);

    readiness
}

/// Completes node recovery once shard sync has delivered every owned shard, then attests epoch
/// sync done. Called after the recovery task's blob scan has finished cleanly; returns when the
/// recovery task should exit.
///
/// Completion waits for shard sync even though recovering blobs and syncing shards are separate
/// responsibilities (the recovery task only recovers blobs certified before its scan bound,
/// while shard sync fills a gained shard with all of its blobs), because the `epoch_sync_done`
/// attestation sent on completion is a joint claim: it states that the node holds *all* the data
/// it should have for the epoch, gained shards included. While the node is recovering, the
/// recovery task is the sole attester (shard sync suppresses its own attestation, see the
/// epoch_sync_done handling in shard_sync.rs), so it must not attest before shard sync's half of
/// the claim is true — measured directly by every owned shard being in `Active` status. The
/// separation still holds for the work itself: a shard that reached `Active` needs no re-scan,
/// and a shard that has not is shard sync's job to finish — never the recovery task's.
///
/// The checks run under the status mutex, which serializes completion against the epoch-change
/// path: either this task observes the advanced target together with the not-yet-`Active` gained
/// shards, or it completes entirely before the target is advanced (and the epoch-change path
/// starts a new task).
async fn complete_recovery_once_shards_synced(
    node: &StorageNodeInner,
    shard_sync_handler: &ShardSyncHandler,
    status_mutex: &Mutex<()>,
) {
    loop {
        let status_guard = status_mutex.lock().await;

        let unsynced_shards = unsynced_owned_shards(node).await;
        if unsynced_shards.is_empty() {
            let current_node_status = node
                .storage
                .node_status()
                .expect("reading node status should not fail");
            let NodeStatus::RecoveryInProgress(target_epoch) = current_node_status else {
                tracing::warn!(
                    node_status = %current_node_status,
                    "node recovery task finished; but node status is not RecoveryInProgress; \
                    skip setting node status to active"
                );
                return;
            };

            tracing::info!(
                walrus.epoch = target_epoch,
                "node recovery task finished; set node status to active"
            );
            if let Err(error) = node.set_node_status(NodeStatus::Active) {
                tracing::error!(?error, "failed to set node status to active");
                return;
            }
            drop(status_guard);

            // While the node is recovering, this is the only place that attests epoch sync
            // done: shard sync skips its own attestation in RecoveryInProgress state (see the
            // epoch_sync_done handling in shard_sync.rs), so that the attestation covers both
            // the recovered blobs and all synced shards. The attested epoch is the recovery
            // target currently recorded in the node status, which may be newer than the epoch
            // this task was started with.
            node.contract_service
                .epoch_sync_done(target_epoch, node.node_capability())
                .await;
            return;
        }
        drop(status_guard);

        if shard_sync_handler.has_sync_in_progress() {
            tracing::info!(
                ?unsynced_shards,
                "waiting for ongoing shard syncs before completing node recovery"
            );
            shard_sync_handler.wait_until_no_sync_in_progress().await;
        } else {
            // The syncs for these shards stopped without reaching `Active` status (a terminally
            // failed shard sync requires a node restart to be retried). Completing now would
            // attest epoch sync done while the shard data is still missing, so park and
            // re-check instead.
            tracing::warn!(
                ?unsynced_shards,
                "owned shards are not active and no shard sync is running; node recovery \
                cannot complete; restart the node to retry shard sync"
            );
            tokio::time::sleep(UNSYNCED_SHARD_RECHECK_INTERVAL).await;
        }
    }
}

/// Returns the shards owned at the latest committee epoch whose local storage is missing or whose
/// status is not [`ShardStatus::Active`], meaning shard sync has not (yet) completed for them.
///
/// A shard whose status cannot be read is conservatively reported as unsynced.
async fn unsynced_owned_shards(node: &StorageNodeInner) -> Vec<ShardIndex> {
    let mut unsynced = Vec::new();
    for shard in node.owned_shards_at_latest_epoch() {
        let status = match node.storage.shard_storage(shard).await {
            Some(shard_storage) => shard_storage
                .status()
                .await
                .inspect_err(|error| {
                    tracing::warn!(
                        walrus.shard_index = %shard,
                        ?error,
                        "failed to read shard status; treating shard as unsynced"
                    );
                })
                .ok(),
            None => None,
        };
        if !matches!(status, Some(ShardStatus::Active)) {
            unsynced.push(shard);
        }
    }
    unsynced
}
