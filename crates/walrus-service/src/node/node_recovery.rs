// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{sync::Arc, time::Duration};

use futures::stream::{FuturesUnordered, StreamExt};
use sui_macros::fail_point_async;
use tokio::sync::{Mutex, watch};
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
    epoch_change::{
        completion::{BackgroundSyncTaskCompletionInstruction, CompletionSlot},
        goal::EpochChangeSyncAndRecoveryInfo,
    },
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

    // The permanent recovery service task, spawned once at startup.
    service_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,

    // Holds the completion instruction while node recovery owns the epoch-change completion
    // (that is, while the node status is `RecoveryInProgress`): the status transition to
    // perform and the `epoch_sync_done` attestation to send. The recovery task consumes it on
    // completion.
    completion_instruction: CompletionSlot,

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
            service_handle: Arc::new(Mutex::new(None)),
            completion_instruction: CompletionSlot::default(),
            config,
        }
    }

    /// Hands node recovery its completion instruction — the status transition to perform and
    /// the `epoch_sync_done` attestation to send when the recovery completes. Called by the
    /// epoch-change apply step, from inside the epoch-change critical section, whenever it sets
    /// or advances the recovery target.
    pub(crate) fn set_completion_instruction(
        &self,
        instruction: BackgroundSyncTaskCompletionInstruction,
    ) {
        self.completion_instruction.put(instruction);
    }

    /// Revokes any pending completion instruction held by node recovery. Called when a
    /// transition supersedes the running recovery.
    pub(crate) fn clear_completion_instruction(&self) {
        self.completion_instruction.clear();
    }

    /// Spawns the permanent node-recovery service.
    ///
    /// The service is a reconciliation loop: it watches the sync-and-recovery info and,
    /// whenever the node's persisted status names a recovery target (and the info names the
    /// node a committee member that is not catching up), runs a recovery toward it. A run is
    /// bound to the sync baseline generation it started from and can outlive several epoch
    /// changes: a publication that leaves the
    /// baseline unchanged (an epoch change advancing the recovery target) extends the run —
    /// its frozen scan bound stays sufficient, since blobs certified at later epochs are
    /// synced through live event processing, and its completion instruction is replaced with
    /// one attesting the new epoch — while a baseline-changing publication (the node entered
    /// catch-up or left the committee) supersedes the run, which abandons its work and
    /// re-evaluates against the latest info (completed per-blob work persists, so a superseded
    /// run only repeats its scan). This replaces the previous start/ensure/abort lifecycle
    /// driven by the epoch-change logic: epoch changes only publish infos and mint completion
    /// instructions; the service makes its own decisions.
    // TODO(WAL-1263): cancel the node-recovery service when the node is shut down.
    pub(crate) fn spawn_background_recovery(
        &self,
        info_receiver: watch::Receiver<EpochChangeSyncAndRecoveryInfo>,
    ) {
        let mut service_handle = self
            .service_handle
            .try_lock()
            .expect("spawn_background_recovery is called once, at startup");
        assert!(
            service_handle.is_none(),
            "the node recovery service is already running"
        );

        let node = self.node.clone();
        let blob_sync_handler = self.blob_sync_handler.clone();
        let shard_sync_handler = self.shard_sync_handler.clone();
        let completion_instruction = self.completion_instruction.clone();
        let max_concurrent_blob_syncs_during_recovery =
            self.config.max_concurrent_blob_syncs_during_recovery;
        *service_handle = Some(tokio::spawn(run_service(
            node,
            blob_sync_handler,
            shard_sync_handler,
            completion_instruction,
            max_concurrent_blob_syncs_during_recovery,
            info_receiver,
        )));
    }
}

/// Returns `true` if a publication has superseded the recovery run bound to `run_baseline`
/// (or the info channel closed). Publications that leave the sync baseline unchanged — an
/// epoch change advancing the recovery target — extend the run instead of superseding it: the
/// run's frozen scan bound stays sufficient because the node syncs blobs certified at later
/// epochs through live event processing, and its completion attests the advanced target via
/// the replaced completion instruction.
fn run_is_superseded(
    info_receiver: &mut watch::Receiver<EpochChangeSyncAndRecoveryInfo>,
    run_baseline: u64,
) -> bool {
    match info_receiver.has_changed() {
        Err(_) => true,
        Ok(true) => {
            info_receiver
                .borrow_and_update()
                .node_recovery_baseline_generation
                != run_baseline
        }
        Ok(false) => false,
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

/// The outcome of one recovery run.
enum RunOutcome {
    /// The recovery completed: the completion instruction was applied and epoch sync done
    /// attested.
    Completed,
    /// A newer info was published (or the node started catching up); the run abandoned its
    /// work and the service re-evaluates against the latest info.
    Superseded,
}

/// The permanent recovery service loop.
async fn run_service(
    node: Arc<StorageNodeInner>,
    blob_sync_handler: Arc<BlobSyncHandler>,
    shard_sync_handler: ShardSyncHandler,
    completion_instruction: CompletionSlot,
    max_concurrent_blob_syncs_during_recovery: usize,
    mut info_receiver: watch::Receiver<EpochChangeSyncAndRecoveryInfo>,
) {
    tracing::info!("waiting for the latest event epoch to start the node recovery service");
    // When current_event_epoch() returns during node startup, a lagging node has already been
    // moved to `RecoveryCatchUp`, so the loop below parks instead of recovering.
    node.current_event_epoch()
        .await
        .expect("current event epoch watch channel should not be dropped");

    // The `generation` of the info the most recent run started from. A run normally ends by
    // changing the world — completing (the status leaves `RecoveryInProgress`) or being
    // superseded by a newer info — but it can also end with the state it started from fully
    // intact: its completion found no instruction (revoked through an error path) while the
    // persisted status still names a recovery target. The guard below keeps such a run from
    // restarting against the very info it just ran under, which would busy-loop identical
    // full certified-blob scans; the service instead parks until the next publication.
    let mut last_run_generation: Option<u64> = None;

    loop {
        // Bind the next run to the current sync baseline generation; mark the info as seen so
        // that `has_changed` inside the run detects only newer publications.
        let (generation, baseline, may_run_recovery) = {
            let info = info_receiver.borrow_and_update();
            // Recovery only runs for a committee member that is not catching up. In
            // particular, a node that was dropped from the committee while recovering can
            // restart with a persisted `RecoveryInProgress` status but no completion
            // instruction (deliberately — its recovery is moot); running would scan the whole
            // certified-blob table for zero owned shards and complete to nothing. The node
            // parks here until event replay processes the epoch change that dropped it, which
            // moves it to `Standby`.
            (
                info.generation,
                info.node_recovery_baseline_generation,
                !info.catching_up && info.membership.is_member(),
            )
        };

        let target_epoch = match node.storage.node_status() {
            Ok(NodeStatus::RecoveryInProgress(target_epoch))
                if may_run_recovery && last_run_generation != Some(generation) =>
            {
                target_epoch
            }
            Ok(_) => {
                // Nothing to run: the status names no recovery target, the info forbids
                // recovery (catching up, or not a committee member), or a run already ended
                // under exactly this info. Wait for the next publication.
                if info_receiver.changed().await.is_err() {
                    // TODO(WAL-1269): terminate the node when the recovery service loop exits;
                    // a node without the recovery service can never complete a recovery.
                    tracing::info!("info channel closed; stopping the node recovery service");
                    return;
                }
                continue;
            }
            Err(error) => {
                tracing::error!(?error, "failed to read the node status; retrying");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        last_run_generation = Some(generation);

        tracing::info!(
            walrus.epoch = target_epoch,
            baseline,
            "starting a node recovery run"
        );
        tracing::error!(
            walrus.epoch = target_epoch,
            baseline,
            "DIAG node recovery run starting"
        );
        fail_point_async!("start_node_recovery_entry");

        match run_recovery(
            &node,
            &blob_sync_handler,
            &shard_sync_handler,
            &completion_instruction,
            max_concurrent_blob_syncs_during_recovery,
            &mut info_receiver,
            baseline,
            target_epoch,
        )
        .await
        {
            RunOutcome::Completed => {
                tracing::error!(
                    walrus.epoch = target_epoch,
                    "DIAG node recovery run completed"
                )
            }
            RunOutcome::Superseded => {
                tracing::error!("DIAG node recovery run superseded by a newer info")
            }
        }
    }
}

/// One recovery run: recovers all blobs certified before `target_epoch` that are not stored at
/// all owned shards, then completes the recovery. The run continues across publications that
/// leave the sync baseline unchanged and returns early with [`RunOutcome::Superseded`] when
/// the baseline changes (see [`run_is_superseded`]).
#[allow(clippy::too_many_arguments)]
async fn run_recovery(
    node: &Arc<StorageNodeInner>,
    blob_sync_handler: &BlobSyncHandler,
    shard_sync_handler: &ShardSyncHandler,
    completion_instruction: &CompletionSlot,
    max_concurrent_blob_syncs_during_recovery: usize,
    info_receiver: &mut watch::Receiver<EpochChangeSyncAndRecoveryInfo>,
    baseline: u64,
    target_epoch: Epoch,
) -> RunOutcome {
    loop {
        // Block until the node is ready to run a recovery scan pass. Stops the recovery
        // task if the node started catching up.
        // A baseline-changing publication supersedes this run before it even scans.
        if run_is_superseded(info_receiver, baseline) {
            return RunOutcome::Superseded;
        }

        match wait_until_ready_to_scan(node, shard_sync_handler).await {
            ScanReadiness::Ready => {}
            ScanReadiness::CatchingUp => return RunOutcome::Superseded,
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
        // Whether a newly published info superseded this run mid-pass.
        let mut superseded = false;
        tracing::info!(
            "scanning blobs to recover certified blobs before epoch {}",
            target_epoch
        );
        // TODO(WAL-1272): dropping read errors here is unsound in one case. A persistent
        // RocksDB status error re-yields on every pass, so the scan spins rather than falsely
        // completing — but a key or value deserialization failure makes the underlying
        // `SafeIter` yield `None` and silently *end* the iterator, so a truncated pass reads
        // as clean and, with all owned shards active, the run completes and attests epoch
        // sync done despite unknown coverage. Treat any `Err` as a pass failure (retry the
        // pass with backoff) and make `SafeIter` surface deserialization failures as errors
        // instead of iterator end.
        for (blob_id, blob_info) in node
            .storage
            .certified_blob_info_iter_before_epoch(target_epoch)
            .filter_map(|blob_result| {
                blob_result
                    .inspect_err(|error| tracing::error!(?error, "failed to read certified blob"))
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

            // A baseline-changing publication supersedes this run entirely: stop scanning,
            // drain the in-flight syncs below, and let the service re-evaluate. A publication
            // that keeps the baseline (a target advancement) extends the run instead.
            if run_is_superseded(info_receiver, baseline) {
                superseded = true;
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
                    walrus.blob_target_epoch = target_epoch,
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
                        walrus.blob_target_epoch = target_epoch,
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
                        walrus.blob_target_epoch = target_epoch,
                        walrus.current_epoch = node.current_committee_epoch(),
                        "skip non-certified blob, post concurrency limit wait"
                    );
                    continue;
                }
            }

            tracing::debug!(
                walrus.blob_id = %blob_id,
                recoverying_epoch = target_epoch,
                "start recovery sync for blob"
            );
            node.metrics.node_recovery_ongoing_blob_syncs.inc();
            let start_sync_result = blob_sync_handler
                .start_sync(
                    blob_id,
                    blob_info
                        .initial_certified_epoch()
                        .expect("certified blob should have an initial certified epoch set"),
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

        if superseded {
            return RunOutcome::Superseded;
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

        // Blob recovery (this run's scan) is done; wait for shard sync to deliver
        // every owned shard and then complete the recovery.
        return if complete_recovery_once_shards_synced(
            node,
            shard_sync_handler,
            completion_instruction,
            info_receiver,
            baseline,
        )
        .await
        {
            RunOutcome::Completed
        } else {
            RunOutcome::Superseded
        };
    }
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
/// The checks run inside the epoch-change critical section, which serializes completion against
/// the epoch-change path: either this task observes the advanced target together with the
/// not-yet-`Active` gained shards (and keeps waiting for their syncs), or it completes
/// entirely before the target is advanced.
async fn complete_recovery_once_shards_synced(
    node: &StorageNodeInner,
    shard_sync_handler: &ShardSyncHandler,
    completion_instruction: &CompletionSlot,
    info_receiver: &mut watch::Receiver<EpochChangeSyncAndRecoveryInfo>,
    run_baseline: u64,
) -> bool {
    loop {
        let critical_section_guard = node.epoch_change_critical_section.enter().await;

        // Validate that this run is still current, inside the critical section: goals are
        // published inside the same critical section, so an unchanged sync baseline here means
        // no invalidating transition (entering catch-up or leaving the committee, however
        // briefly) has superseded the run since it started. Target advancements leave the
        // baseline unchanged: the completion below then consumes the instruction minted for
        // the advanced target and attests the newest epoch.
        if info_receiver.borrow().node_recovery_baseline_generation != run_baseline {
            drop(critical_section_guard);
            tracing::info!(
                "recovery completion superseded by an invalidating info; abandoning the run"
            );
            return false;
        }

        let unsynced_shards = unsynced_owned_shards(node).await;
        if unsynced_shards.is_empty() {
            let current_node_status = node
                .storage
                .node_status()
                .expect("reading node status should not fail");

            // Consume the completion instruction while still inside the critical section, so
            // that it belongs to the recovery target observed by this pass: the epoch-change
            // path replaces the instruction and the target atomically inside the same critical
            // section. No instruction means a concurrent transition (dropping out of the
            // committee, entering recovery mode) superseded this recovery; the task then
            // finishes without touching the node status.
            let Some(instruction) = completion_instruction.take() else {
                tracing::error!(
                    node_status = %current_node_status,
                    "DIAG node recovery task finished, but its completion was superseded by a \
                    concurrent transition; skipping the status change and attestation"
                );
                return false;
            };

            tracing::error!(
                node_status = %current_node_status,
                ?instruction,
                "DIAG node recovery task finished; applying its completion instruction"
            );
            let attestation = match instruction.apply_status(node) {
                Ok(attestation) => attestation,
                Err(error) => {
                    tracing::error!(?error, "failed to apply the recovery completion status");
                    return false;
                }
            };
            drop(critical_section_guard);

            // While the node is recovering, node recovery holds the attestation (bundled in the
            // completion instruction), so this is the only place that attests epoch sync done,
            // and the attestation covers both the recovered blobs and all synced shards. The
            // attested epoch is the recovery target most recently recorded by the epoch-change
            // path, which may be newer than the epoch this task was started with.
            if let Some(token) = attestation {
                token.attest(node).await;
            }
            return true;
        }
        drop(critical_section_guard);

        if shard_sync_handler.has_sync_in_progress() {
            tracing::info!(
                ?unsynced_shards,
                "waiting for ongoing shard syncs before completing node recovery"
            );
            tokio::select! {
                _ = shard_sync_handler.wait_until_no_sync_in_progress() => {}
                changed = info_receiver.changed() => {
                    // Re-evaluate: only a baseline-changing publication supersedes the run; a
                    // target advancement (possibly gaining shards) just widens what to wait
                    // for, which the next iteration picks up.
                    if changed.is_err()
                        || info_receiver.borrow_and_update().node_recovery_baseline_generation
                            != run_baseline
                    {
                        return false;
                    }
                }
            }
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
            tokio::select! {
                _ = tokio::time::sleep(UNSYNCED_SHARD_RECHECK_INTERVAL) => {}
                changed = info_receiver.changed() => {
                    if changed.is_err()
                        || info_receiver.borrow_and_update().node_recovery_baseline_generation
                            != run_baseline
                    {
                        return false;
                    }
                }
            }
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
