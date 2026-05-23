// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use futures::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
use serde::{Deserialize, Serialize};
use sui_macros::fail_point_async;
use tokio::{sync::Mutex, time::Instant};
use typed_store::TypedStoreError;
use walrus_core::{BlobId, Epoch, ShardIndex};

use super::{
    StorageNodeInner,
    blob_sync::{BlobSyncHandler, SyncOutcome, SyncStatus},
    config::NodeRecoveryConfig,
};
use crate::node::{NodeStatus, storage::blob_info::CertifiedBlobInfoApi};

const RETRY_BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const RETRY_BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Persisted progress of an in-flight node recovery.
///
/// Snapshotted at recovery task start and updated as the scan settles blobs. On crash and
/// restart, the next recovery task resumes from `last_settled_blob_id` if `owned_shards`
/// matches the current owned-shards set (otherwise the checkpoint is discarded — see
/// [`NodeRecoveryHandler::start_node_recovery`]).
// Important: this struct is committed to database. Do not modify the existing fields. Only add
// new fields at the end with `#[serde(default)]`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeRecoveryProgress {
    /// The `certified_before_epoch` argument that the recovery task was started with.
    pub epoch: Epoch,
    /// The set of shards owned by the node at the moment the checkpoint was written.
    pub owned_shards: BTreeSet<ShardIndex>,
    /// All blobs with `blob_id <= last_settled_blob_id` have been observed in a scan as either
    /// stored at all owned shards or no longer certified.
    pub last_settled_blob_id: BlobId,
}

#[derive(Debug, Clone)]
pub struct NodeRecoveryHandler {
    node: Arc<StorageNodeInner>,
    blob_sync_handler: Arc<BlobSyncHandler>,

    // There can be at most one background recovery task at a time.
    task_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,

    config: NodeRecoveryConfig,
}

/// Result of awaiting a single blob's sync watch channel.
#[derive(Debug)]
enum BlobSyncResult {
    /// The blob is stored at all currently-owned shards (or was already, or an inconsistency
    /// proof was filed).
    Success(BlobId),
    /// The blob's sync was cancelled — typically because the blob expired during the sync.
    /// The recovery loop must re-check whether the blob is still certified or stored.
    Cancelled(BlobId),
    /// The sync was skipped because the node entered `RecoveryCatchUp` before the sync ran.
    /// Recovery cannot make further progress and must bail out.
    Skipped(BlobId),
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

    /// Starts the node recovery process to recover blobs that are certified before the given
    /// epoch. For blobs that are certified at or after `certified_before_epoch`, the event
    /// processing path is in charge of making sure the blob is stored at all owned shards.
    ///
    /// Any existing recovery task will be canceled.
    ///
    /// # Checkpoint resume semantics
    ///
    /// Recovery persists a [`NodeRecoveryProgress`] checkpoint as the scan settles blobs, so
    /// a crash mid-recovery does not force a rescan from the first blob.
    ///
    /// On entry, the persisted checkpoint is kept (and the scan resumes from
    /// `last_settled_blob_id`) iff the node's currently-owned shard set equals
    /// `checkpoint.owned_shards`. Otherwise the checkpoint is deleted and the scan restarts
    /// from `Unbounded`. We deliberately do *not* gate the resume on the epoch matching.
    ///
    /// The invariant maintained for every settled blob_id is: "at scan time the blob was
    /// either stored at all currently-owned shards, or not currently certified". This holds
    /// across `certified_before_epoch` increases without rescanning because:
    ///
    /// 1. **New certifications during recovery** have `initial_certified_epoch >=
    ///    certified_before_epoch_old` and are filtered out of the recovery iterator entirely;
    ///    they're driven by the event-processing path
    ///    (`BlobEventProcessor::process_blob_certified_event`), which calls `start_sync`
    ///    independently. The `is_catching_up()` skip there does not fire during
    ///    `RecoveryInProgress`, so this path is active throughout node recovery.
    ///
    /// 2. **Re-certifications of previously-uncertified blobs** also emit a `BlobCertified`
    ///    event, handled the same way by event processing.
    ///
    /// 3. **Data on currently-owned shards is never wiped while owned**. Only shards listed
    ///    in `shards_to_remove` (no longer owned) have their data deleted. Garbage collection
    ///    only removes data for expired (uncertified) blobs, which still satisfies the
    ///    invariant via the "not currently certified" clause.
    ///
    /// 4. **Shard ownership changes** break the invariant: a previously-acquired new shard is
    ///    empty, and a removed-then-regained shard had its data wiped. The owned-shards
    ///    equality check at task start catches this and discards the checkpoint.
    pub async fn start_node_recovery(
        &self,
        certified_before_epoch: Epoch,
    ) -> Result<(), TypedStoreError> {
        let mut locked_task_handle = self.task_handle.lock().await;

        if let Some(old_task) = locked_task_handle.take() {
            tracing::info!("canceling existing node recovery task");
            old_task.abort();
            let _ = old_task.await;
        }

        let node = self.node.clone();
        let blob_sync_handler = self.blob_sync_handler.clone();
        let config = self.config.clone();
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

            RecoveryPass::new(node, blob_sync_handler, certified_before_epoch, config)
                .run()
                .await;
        });
        *locked_task_handle = Some(task_handle);

        Ok(())
    }

    /// Restarts any in-progress recovery.
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

/// Encapsulates the state of one recovery task run. A single pass over the certified-blob
/// iterator (from the resume bound to the end), driving syncs concurrently and persisting
/// checkpoints as blobs settle.
struct RecoveryPass {
    node: Arc<StorageNodeInner>,
    blob_sync_handler: Arc<BlobSyncHandler>,
    certified_before_epoch: Epoch,
    /// Snapshot of shards owned at task start. Used consistently throughout the run for both
    /// the "is stored at all shards" check and the checkpoint's `owned_shards` field. Any
    /// epoch change that mutates this set re-invokes `start_node_recovery` (aborting this
    /// task); the next task's owned-shards equality check will then discard the checkpoint.
    owned_shards: BTreeSet<ShardIndex>,
    owned_shards_vec: Vec<ShardIndex>,
    config: NodeRecoveryConfig,

    /// Blobs whose sync watch we are still awaiting.
    in_flight_blob_ids: BTreeSet<BlobId>,
    /// Blobs whose sync ended with Cancelled, awaiting the next retry attempt.
    retry_pending: BTreeMap<BlobId, Instant>,
    /// Backoff currently applied to each retry-pending blob (doubled on each consecutive
    /// failure, capped at `RETRY_BACKOFF_MAX`).
    retry_backoff: BTreeMap<BlobId, Duration>,

    /// Highest blob_id known to be settled and safe to checkpoint past (assuming no smaller
    /// blob is in flight or pending retry).
    last_settled_blob_id: Option<BlobId>,
    /// The most recent value persisted via `set_node_recovery_progress`, used to decide when
    /// the next persist is due.
    last_persisted_blob_id: Option<BlobId>,
    /// Most recent blob_id yielded by the scan iterator.
    scan_pos: Option<BlobId>,
}

impl RecoveryPass {
    fn new(
        node: Arc<StorageNodeInner>,
        blob_sync_handler: Arc<BlobSyncHandler>,
        certified_before_epoch: Epoch,
        config: NodeRecoveryConfig,
    ) -> Self {
        let owned_shards_vec = node.owned_shards_at_latest_epoch();
        let owned_shards = owned_shards_vec.iter().copied().collect();
        Self {
            node,
            blob_sync_handler,
            certified_before_epoch,
            owned_shards,
            owned_shards_vec,
            config,
            in_flight_blob_ids: BTreeSet::new(),
            retry_pending: BTreeMap::new(),
            retry_backoff: BTreeMap::new(),
            last_settled_blob_id: None,
            last_persisted_blob_id: None,
            scan_pos: None,
        }
    }

    async fn run(mut self) {
        let resume_bound = self.load_or_reset_checkpoint();
        self.last_persisted_blob_id = match resume_bound {
            Bound::Excluded(id) => Some(id),
            _ => None,
        };
        self.scan_pos = self.last_persisted_blob_id;

        if self.is_catching_up() {
            tracing::info!("node recovery encountered node is in catching up; skip recovery");
            return;
        }

        tracing::info!(
            "scanning blobs to recover certified blobs before epoch {}",
            self.certified_before_epoch
        );

        // Hold the iterator's backing storage in a separate Arc clone so the iterator's
        // borrow is on `node_for_iter.storage`, not on `self.node.storage` — that lets us
        // mutably borrow `self` inside the loop body.
        let node_for_iter = self.node.clone();
        let iter = node_for_iter
            .storage
            .certified_blob_info_iter_before_epoch_from(self.certified_before_epoch, resume_bound);
        let mut iter = iter.filter_map(|res| match res {
            Ok(pair) => Some(pair),
            Err(error) => {
                tracing::error!(?error, "failed to read certified blob during scan");
                None
            }
        });

        let mut ongoing_syncs: FuturesUnordered<BoxFuture<'static, BlobSyncResult>> =
            FuturesUnordered::new();
        let mut scan_done = false;
        let mut aborted = false;

        while !aborted {
            // Drain any ready completions before doing more work.
            while let Some(result) = poll_ready(&mut ongoing_syncs) {
                if self.handle_sync_result(result).await {
                    aborted = true;
                    break;
                }
            }
            if aborted {
                break;
            }

            // Issue retries whose backoff has elapsed.
            self.poll_due_retries(&mut ongoing_syncs).await;

            // Pull more blobs from the scan to fill the in-flight pool.
            while !scan_done
                && ongoing_syncs.len() < self.config.max_concurrent_blob_syncs_during_recovery
            {
                match iter.next() {
                    Some((blob_id, blob_info)) => {
                        self.scan_pos = Some(blob_id);
                        self.node
                            .metrics
                            .node_recovery_recover_blob_progress
                            .set(i64::from(blob_id.first_two_bytes()));

                        let is_certified =
                            blob_info.is_certified(self.node.current_committee_epoch());
                        if !is_certified || self.is_stored_at_owned_shards(&blob_id).await {
                            self.advance_settled(blob_id);
                        } else {
                            self.in_flight_blob_ids.insert(blob_id);
                            self.node.metrics.node_recovery_ongoing_blob_syncs.inc();
                            self.spawn_sync(
                                blob_id,
                                blob_info.initial_certified_epoch().expect(
                                    "certified blob should have an initial certified epoch set",
                                ),
                                &mut ongoing_syncs,
                            )
                            .await;
                        }
                    }
                    None => scan_done = true,
                }
            }

            self.maybe_persist_checkpoint();

            // Termination: scan exhausted, no in-flight syncs, no retries pending.
            if scan_done && ongoing_syncs.is_empty() && self.retry_pending.is_empty() {
                break;
            }

            // Wait for the next event: a sync completion or a retry becoming due.
            let next_retry_deadline = self.retry_pending.values().min().copied();
            tokio::select! {
                Some(result) = ongoing_syncs.next(), if !ongoing_syncs.is_empty() => {
                    if self.handle_sync_result(result).await {
                        aborted = true;
                    }
                }
                _ = sleep_until_opt(next_retry_deadline), if next_retry_deadline.is_some() => {}
                // If neither arm is active (no ongoing syncs and no retries), the loop will
                // exit via the termination check above on the next iteration — but we still
                // need to make progress, so fall through with a yield.
                else => {
                    tokio::task::yield_now().await;
                }
            }

            if self.is_catching_up() {
                tracing::info!("node entered catching-up state mid-recovery; aborting");
                aborted = true;
            }
        }

        self.finalize(aborted).await;
    }

    /// Decides the resume bound based on the persisted checkpoint and current owned shards.
    /// Discards the checkpoint if owned shards have changed.
    fn load_or_reset_checkpoint(&self) -> Bound<BlobId> {
        let progress = self
            .node
            .storage
            .get_node_recovery_progress()
            .unwrap_or_else(|error| {
                tracing::error!(
                    ?error,
                    "failed to read node recovery checkpoint; starting from the beginning"
                );
                None
            });
        let decision = resume_decision(progress.as_ref(), &self.owned_shards);
        match &decision {
            ResumeDecision::Resume(blob_id) => {
                tracing::info!(
                    last_settled_blob_id = %blob_id,
                    "resuming node recovery from persisted checkpoint"
                );
                sui_macros::fail_point!("fail_point_node_recovery_resumed_from_checkpoint");
            }
            ResumeDecision::Reset {
                had_checkpoint: true,
            } => {
                tracing::info!(
                    "discarding node recovery checkpoint: owned shards have changed since \
                    it was written"
                );
                if let Err(error) = self.node.storage.clear_node_recovery_progress() {
                    tracing::error!(?error, "failed to clear stale node recovery checkpoint");
                }
            }
            ResumeDecision::Reset {
                had_checkpoint: false,
            } => {}
        }
        match decision {
            ResumeDecision::Resume(blob_id) => Bound::Excluded(blob_id),
            ResumeDecision::Reset { .. } => Bound::Unbounded,
        }
    }

    fn is_catching_up(&self) -> bool {
        self.node
            .storage
            .node_status()
            .expect("reading node status should not fail")
            .is_catching_up()
    }

    /// Returns the highest blob_id that is safe to checkpoint right now: bounded above by
    /// the smallest in-flight or retry-pending blob.
    fn eligible_checkpoint(&self) -> Option<BlobId> {
        let smallest_unfinished = self
            .in_flight_blob_ids
            .iter()
            .next()
            .copied()
            .into_iter()
            .chain(self.retry_pending.keys().next().copied())
            .min();
        match smallest_unfinished {
            Some(_) => self.last_settled_blob_id,
            None => self.scan_pos.or(self.last_settled_blob_id),
        }
    }

    /// Advances `last_settled_blob_id` to `blob_id` if doing so does not jump past any blob
    /// still in flight or pending retry.
    fn advance_settled(&mut self, blob_id: BlobId) {
        let blocked = self
            .in_flight_blob_ids
            .iter()
            .next()
            .copied()
            .into_iter()
            .chain(self.retry_pending.keys().next().copied())
            .min()
            .is_some_and(|smallest| smallest < blob_id);
        if blocked {
            return;
        }
        match self.last_settled_blob_id {
            None => self.last_settled_blob_id = Some(blob_id),
            Some(current) if blob_id > current => self.last_settled_blob_id = Some(blob_id),
            _ => {}
        }
    }

    fn maybe_persist_checkpoint(&mut self) {
        let Some(candidate) = self.eligible_checkpoint() else {
            return;
        };
        let advance_blobs = match self.last_persisted_blob_id {
            None => u64::MAX, // first persist is always allowed
            Some(prev) if candidate > prev => blob_id_distance(prev, candidate),
            _ => return,
        };
        if advance_blobs < self.config.checkpoint_persist_interval {
            return;
        }
        let progress = NodeRecoveryProgress {
            epoch: self.certified_before_epoch,
            owned_shards: self.owned_shards.clone(),
            last_settled_blob_id: candidate,
        };
        if let Err(error) = self.node.storage.set_node_recovery_progress(&progress) {
            tracing::error!(?error, "failed to persist node recovery checkpoint");
            return;
        }
        self.last_persisted_blob_id = Some(candidate);
    }

    async fn is_stored_at_owned_shards(&self, blob_id: &BlobId) -> bool {
        match self
            .node
            .is_stored_at_specific_shards(blob_id, &self.owned_shards_vec)
            .await
        {
            Ok(stored) => stored,
            Err(error) => {
                tracing::warn!(
                    walrus.blob_id = %blob_id,
                    ?error,
                    "failed to check if blob is stored at owned shards; will start blob sync"
                );
                false
            }
        }
    }

    fn is_blob_still_certified(&self, blob_id: &BlobId) -> bool {
        match self.node.storage.get_blob_info(blob_id) {
            Ok(Some(info)) => info.is_certified(self.node.current_committee_epoch()),
            _ => false,
        }
    }

    async fn spawn_sync(
        &self,
        blob_id: BlobId,
        certified_epoch: Epoch,
        ongoing_syncs: &mut FuturesUnordered<BoxFuture<'static, BlobSyncResult>>,
    ) {
        tracing::debug!(walrus.blob_id = %blob_id, "start recovery sync for blob");
        let start_sync_result = self
            .blob_sync_handler
            .start_sync(blob_id, certified_epoch, None)
            .await;
        sui_macros::fail_point!("fail_point_node_recovery_start_sync");
        match start_sync_result {
            Ok(mut receiver) => {
                let fut: BoxFuture<'static, BlobSyncResult> = Box::pin(async move {
                    let outcome = match receiver
                        .wait_for(|status| matches!(status, SyncStatus::Done(_)))
                        .await
                    {
                        Ok(guard) => match &*guard {
                            SyncStatus::Done(outcome) => *outcome,
                            SyncStatus::Pending => unreachable!(),
                        },
                        // All senders dropped without a final status — treat as cancellation
                        // so the recovery loop re-evaluates the blob.
                        Err(_) => SyncOutcome::Cancelled,
                    };
                    match outcome {
                        SyncOutcome::Success => BlobSyncResult::Success(blob_id),
                        SyncOutcome::Cancelled => BlobSyncResult::Cancelled(blob_id),
                        SyncOutcome::Skipped => BlobSyncResult::Skipped(blob_id),
                    }
                });
                ongoing_syncs.push(fut);
            }
            Err(err) => {
                panic!("failed to start recovery sync for blob {blob_id}: {err}");
            }
        }
    }

    /// Returns true if the recovery task should abort.
    async fn handle_sync_result(&mut self, result: BlobSyncResult) -> bool {
        match result {
            BlobSyncResult::Success(blob_id) => {
                self.in_flight_blob_ids.remove(&blob_id);
                self.retry_backoff.remove(&blob_id);
                self.node.metrics.node_recovery_ongoing_blob_syncs.dec();
                self.advance_settled(blob_id);
                false
            }
            BlobSyncResult::Cancelled(blob_id) => {
                self.in_flight_blob_ids.remove(&blob_id);
                self.node.metrics.node_recovery_ongoing_blob_syncs.dec();
                if !self.is_blob_still_certified(&blob_id)
                    || self.is_stored_at_owned_shards(&blob_id).await
                {
                    self.retry_backoff.remove(&blob_id);
                    self.advance_settled(blob_id);
                } else {
                    let backoff = next_retry_backoff(self.retry_backoff.get(&blob_id).copied());
                    tracing::warn!(
                        walrus.blob_id = %blob_id,
                        backoff_secs = backoff.as_secs(),
                        "blob sync was cancelled but blob still needs recovery; will retry"
                    );
                    self.retry_backoff.insert(blob_id, backoff);
                    self.retry_pending.insert(blob_id, Instant::now() + backoff);
                }
                false
            }
            BlobSyncResult::Skipped(blob_id) => {
                self.in_flight_blob_ids.remove(&blob_id);
                self.node.metrics.node_recovery_ongoing_blob_syncs.dec();
                tracing::info!(
                    walrus.blob_id = %blob_id,
                    "blob sync was skipped because node is in RecoveryCatchUp; \
                    aborting node recovery"
                );
                true
            }
        }
    }

    /// Re-issues `start_sync` for any retry whose backoff deadline has elapsed.
    async fn poll_due_retries(
        &mut self,
        ongoing_syncs: &mut FuturesUnordered<BoxFuture<'static, BlobSyncResult>>,
    ) {
        let now = Instant::now();
        let due: Vec<BlobId> = self
            .retry_pending
            .iter()
            .filter_map(|(&id, &deadline)| (deadline <= now).then_some(id))
            .collect();
        for blob_id in due {
            self.retry_pending.remove(&blob_id);

            if !self.is_blob_still_certified(&blob_id)
                || self.is_stored_at_owned_shards(&blob_id).await
            {
                self.retry_backoff.remove(&blob_id);
                self.advance_settled(blob_id);
                continue;
            }

            let Ok(Some(info)) = self.node.storage.get_blob_info(&blob_id) else {
                self.retry_backoff.remove(&blob_id);
                continue;
            };
            let Some(certified_epoch) = info.initial_certified_epoch() else {
                self.retry_backoff.remove(&blob_id);
                continue;
            };

            self.in_flight_blob_ids.insert(blob_id);
            self.node.metrics.node_recovery_ongoing_blob_syncs.inc();
            self.spawn_sync(blob_id, certified_epoch, ongoing_syncs)
                .await;
        }
    }

    async fn finalize(&self, aborted: bool) {
        // Persist whatever progress we have, even on abort.
        if let Some(blob_id) = self.eligible_checkpoint() {
            let progress = NodeRecoveryProgress {
                epoch: self.certified_before_epoch,
                owned_shards: self.owned_shards.clone(),
                last_settled_blob_id: blob_id,
            };
            if let Err(error) = self.node.storage.set_node_recovery_progress(&progress) {
                tracing::error!(?error, "failed to persist final node recovery checkpoint");
            }
        }

        if aborted {
            return;
        }

        let current_node_status = self
            .node
            .storage
            .node_status()
            .expect("reading node status should not fail");
        if current_node_status == NodeStatus::RecoveryInProgress(self.certified_before_epoch) {
            tracing::info!("node recovery task finished; set node status to active");
            match self.node.set_node_status(NodeStatus::Active) {
                Ok(()) => {
                    if let Err(error) = self.node.storage.clear_node_recovery_progress() {
                        tracing::error!(
                            ?error,
                            "failed to clear node recovery checkpoint after activation"
                        );
                    }
                    self.node
                        .contract_service
                        .epoch_sync_done(self.certified_before_epoch, self.node.node_capability())
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
    }
}

/// Polls a `FuturesUnordered` without blocking; returns `Some` if a result is immediately
/// available.
fn poll_ready<F: std::future::Future + Unpin>(
    stream: &mut FuturesUnordered<F>,
) -> Option<F::Output> {
    use std::task::{Context, Poll};

    use futures::Stream;

    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    match Pin::new(stream).poll_next(&mut cx) {
        Poll::Ready(Some(v)) => Some(v),
        _ => None,
    }
}

/// `tokio::time::sleep_until` for `Option<Instant>`, becoming a pending future for `None`.
async fn sleep_until_opt(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending::<()>().await,
    }
}

/// Rough distance between two blob ids: numeric difference of their first 8 bytes. Used only
/// to gate persistence frequency, so a coarse estimate is sufficient. The first byte is
/// already exposed for metrics (`first_two_bytes`), so this is a natural extension.
fn blob_id_distance(a: BlobId, b: BlobId) -> u64 {
    let prefix_a = u64::from_be_bytes(a.0[..8].try_into().expect("32-byte BlobId"));
    let prefix_b = u64::from_be_bytes(b.0[..8].try_into().expect("32-byte BlobId"));
    prefix_b.saturating_sub(prefix_a)
}

/// Pure decision: should the current recovery task resume from a persisted checkpoint or
/// discard it and start over? Extracted from [`RecoveryPass::load_or_reset_checkpoint`] so it
/// can be unit-tested without spinning up a full node.
#[derive(Debug, PartialEq, Eq)]
enum ResumeDecision {
    /// Resume the iterator with `Excluded(blob_id)` — every blob `<= blob_id` was settled in
    /// a prior run.
    Resume(BlobId),
    /// Start the iterator from `Unbounded`. `had_checkpoint` indicates whether there was a
    /// stale checkpoint that should be cleared from storage.
    Reset { had_checkpoint: bool },
}

fn resume_decision(
    progress: Option<&NodeRecoveryProgress>,
    current_owned_shards: &BTreeSet<ShardIndex>,
) -> ResumeDecision {
    match progress {
        Some(p) if &p.owned_shards == current_owned_shards => {
            ResumeDecision::Resume(p.last_settled_blob_id)
        }
        Some(_) => ResumeDecision::Reset {
            had_checkpoint: true,
        },
        None => ResumeDecision::Reset {
            had_checkpoint: false,
        },
    }
}

/// Returns the next backoff for a blob whose sync just ended in Cancelled. Starts at
/// `RETRY_BACKOFF_INITIAL` and doubles on each consecutive cancellation, capped at
/// `RETRY_BACKOFF_MAX`. Extracted as a pure function for testability.
fn next_retry_backoff(previous: Option<Duration>) -> Duration {
    match previous {
        None => RETRY_BACKOFF_INITIAL,
        Some(prev) => (prev * 2).min(RETRY_BACKOFF_MAX),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, time::Duration};

    use walrus_core::{BlobId, ShardIndex};

    use super::{
        NodeRecoveryProgress,
        RETRY_BACKOFF_INITIAL,
        RETRY_BACKOFF_MAX,
        ResumeDecision,
        blob_id_distance,
        next_retry_backoff,
        resume_decision,
    };

    fn blob(byte: u8) -> BlobId {
        BlobId([byte; 32])
    }

    fn shards(ids: &[u16]) -> BTreeSet<ShardIndex> {
        ids.iter().copied().map(ShardIndex).collect()
    }

    #[test]
    fn resume_decision_resumes_when_owned_shards_match() {
        // Property: when the persisted owned_shards equals the current owned_shards, the
        // checkpoint is kept (regardless of epoch — see start_node_recovery doc comment).
        let progress = NodeRecoveryProgress {
            epoch: 10,
            owned_shards: shards(&[1, 3, 5]),
            last_settled_blob_id: blob(42),
        };
        assert_eq!(
            resume_decision(Some(&progress), &shards(&[1, 3, 5])),
            ResumeDecision::Resume(blob(42)),
            "same shards at same epoch should resume"
        );

        let progress_diff_epoch = NodeRecoveryProgress {
            epoch: 99,
            ..progress.clone()
        };
        assert_eq!(
            resume_decision(Some(&progress_diff_epoch), &shards(&[1, 3, 5])),
            ResumeDecision::Resume(blob(42)),
            "same shards across different epochs should still resume"
        );
    }

    #[test]
    fn resume_decision_resets_when_owned_shards_change() {
        // Property: any difference in the owned-shards set invalidates the checkpoint —
        // gained shards are empty, and removed-then-regained shards may have been wiped.
        let progress = NodeRecoveryProgress {
            epoch: 10,
            owned_shards: shards(&[1, 3, 5]),
            last_settled_blob_id: blob(42),
        };
        for current in [
            shards(&[1, 3, 5, 7]), // gained a shard
            shards(&[1, 3]),       // lost a shard
            shards(&[2, 4, 6]),    // entirely different
            shards(&[]),           // empty
        ] {
            assert_eq!(
                resume_decision(Some(&progress), &current),
                ResumeDecision::Reset {
                    had_checkpoint: true
                },
                "changed shards {:?} should reset",
                current,
            );
        }
    }

    #[test]
    fn resume_decision_resets_when_no_checkpoint() {
        assert_eq!(
            resume_decision(None, &shards(&[1, 2, 3])),
            ResumeDecision::Reset {
                had_checkpoint: false
            },
        );
    }

    #[test]
    fn blob_id_distance_is_zero_when_destination_is_not_greater() {
        // saturating_sub returns zero for equal or decreasing prefixes, which gates persists
        // (we never persist a checkpoint going backwards).
        assert_eq!(blob_id_distance(blob(5), blob(5)), 0);
        assert_eq!(blob_id_distance(blob(10), blob(5)), 0);
    }

    #[test]
    fn blob_id_distance_grows_with_prefix_difference() {
        // Property: a larger prefix gap means a larger reported distance — used to decide
        // when enough advancement has accumulated for the next persist.
        assert!(blob_id_distance(blob(1), blob(2)) > 0);
        assert!(blob_id_distance(blob(1), blob(100)) > blob_id_distance(blob(1), blob(2)));
    }

    #[test]
    fn retry_backoff_doubles_each_cancellation_up_to_cap() {
        // Property: each consecutive Cancelled outcome for the same blob doubles the
        // backoff, capped at RETRY_BACKOFF_MAX. The first retry waits the initial backoff.
        assert_eq!(next_retry_backoff(None), RETRY_BACKOFF_INITIAL);
        let mut current = RETRY_BACKOFF_INITIAL;
        for _ in 0..20 {
            let next = next_retry_backoff(Some(current));
            assert!(next >= current, "backoff should not decrease");
            assert!(next <= RETRY_BACKOFF_MAX, "backoff should not exceed cap");
            current = next;
        }
        assert_eq!(
            current, RETRY_BACKOFF_MAX,
            "backoff should saturate at the cap after enough retries"
        );
        // Once at the cap, subsequent retries stay at the cap (not over).
        assert_eq!(
            next_retry_backoff(Some(RETRY_BACKOFF_MAX)),
            RETRY_BACKOFF_MAX
        );
        // And we don't lose track if a caller supplies an already-capped value.
        assert_eq!(
            next_retry_backoff(Some(Duration::from_secs(60))),
            RETRY_BACKOFF_MAX
        );
    }
}
