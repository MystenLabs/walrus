// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{HashMap, hash_map::Entry},
    sync::Arc,
    time::Duration,
};

use futures::{StreamExt, stream::FuturesUnordered};
#[cfg(msim)]
use sui_macros::{fail_point_arg, fail_point_async, fail_point_if};
use tokio::{
    sync::{Mutex, Semaphore, watch},
    time::Instant,
};
use walrus_core::{BlobId, Epoch, ShardIndex};
use walrus_storage_node_client::error::ServiceError;
use walrus_utils::backoff::{BackoffStrategy, ExponentialBackoff};

use super::{
    NodeStatus,
    StorageNodeInner,
    blob_retirement_notifier::ExecutionResultWithRetirementCheck,
    config::ShardSyncConfig,
    errors::SyncShardClientError,
    storage::{ShardStatus, ShardStorage, blob_info::BlobInfo},
};
use crate::node::{
    epoch_change::{
        attestation::{EpochSyncDoneToken, ShardSyncAttestation},
        completion::{BackgroundSyncTaskCompletionInstruction, CompletionSlot},
        goal::EpochChangeSyncAndRecoveryInfo,
        plan::ShardFill,
    },
    errors::ShardNotAssigned,
    storage::blob_info::CertifiedBlobInfoApi,
};

/// The interval at which to sample high-frequency tracing logs related to shard sync operations.
pub(crate) const SAMPLED_TRACING_INTERVAL: Duration = Duration::from_mins(10);

/// The result of syncing a shard.
enum SyncShardResult {
    /// The shard sync finished successfully.
    Success,
    /// The shard sync is not finished and should be retried after a backoff.
    /// The first bool indicates whether to directly recover the shard instead of using shard sync.
    /// The second bool indicates whether the shard sync made progress.
    RetryAfterBackoff {
        force_recovery: bool,
        shard_sync_made_progress: bool,
    },
    /// The shard sync contains errors and should be stopped.
    Failed,
}

/// RAII guard tracking a running shard-sync-related task in the sync task count watch channel.
///
/// Increments the count on creation and decrements it on drop, so that the count stays accurate
/// even if the tracked task is aborted or panics.
#[derive(Debug)]
struct SyncTaskCountGuard(Arc<watch::Sender<usize>>);

impl SyncTaskCountGuard {
    fn new(counter: Arc<watch::Sender<usize>>) -> Self {
        counter.send_modify(|count| *count += 1);
        sui_macros::fail_point!("fail_point_shard_sync_task_started");
        Self(counter)
    }
}

impl Drop for SyncTaskCountGuard {
    fn drop(&mut self) {
        self.0.send_modify(|count| *count = count.saturating_sub(1));
        sui_macros::fail_point!("fail_point_shard_sync_task_finished");
    }
}

/// Manages tasks for syncing shards during epoch change.
#[derive(Debug, Clone)]
pub struct ShardSyncHandler {
    node: Arc<StorageNodeInner>,
    shard_sync_in_progress: Arc<Mutex<HashMap<ShardIndex, tokio::task::JoinHandle<()>>>>,
    task_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    // The permanent shard-sync reconciler task, spawned once at startup.
    service_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    // Whether the next sync-driving run applies the restart-only resume policy (optionally
    // retrying the sliver transfer before recovery). Armed at construction (covering the
    // reconciler's first pass after startup) and re-armed by [`Self::restart_syncs`]; consumed
    // by the next driver run.
    apply_restart_resume_policy: Arc<std::sync::atomic::AtomicBool>,
    shard_sync_semaphore: Arc<Semaphore>,
    // Tracks the number of currently running shard sync tasks, including the task that starts
    // the individual per-shard syncs. Used by node recovery to wait until all shard syncs have
    // finished (successfully or not) before recovering blobs.
    sync_task_count: Arc<watch::Sender<usize>>,
    // Holds the `epoch_sync_done` attestation token — together with the shards whose syncs
    // must complete before it may be consumed — while shard sync owns the attestation (that is,
    // shard syncs were started at an epoch change and node recovery is not in progress).
    epoch_sync_attestation: ShardSyncAttestation,
    // Holds the completion instruction of a pending blob-metadata recovery: the status
    // transition (to `Active`) the metadata-recovery task performs when it finishes. Minted
    // when the node status is set to `RecoverMetadata`; revoked by transitions that supersede
    // the metadata recovery.
    metadata_recovery_completion: CompletionSlot,
    config: ShardSyncConfig,
}

impl ShardSyncHandler {
    pub fn new(node: Arc<StorageNodeInner>, config: ShardSyncConfig) -> Self {
        Self {
            node,
            shard_sync_in_progress: Arc::new(Mutex::new(HashMap::new())),
            task_handle: Arc::new(Mutex::new(None)),
            service_handle: Arc::new(Mutex::new(None)),
            apply_restart_resume_policy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            shard_sync_semaphore: Arc::new(Semaphore::new(config.shard_sync_concurrency)),
            sync_task_count: Arc::new(watch::channel(0).0),
            epoch_sync_attestation: ShardSyncAttestation::default(),
            metadata_recovery_completion: CompletionSlot::default(),
            config,
        }
    }

    /// Hands the `epoch_sync_done` attestation token to shard sync, atomically recording the
    /// shards whose syncs must complete before the token may be consumed. Called by the
    /// epoch-change apply step *before* starting the syncs, so that a sync task from an earlier
    /// epoch — which may observe the in-progress task map as empty while the new tasks are not
    /// yet spawned — cannot consume the new epoch's token.
    pub(crate) fn set_epoch_sync_done_token(
        &self,
        token: EpochSyncDoneToken,
        pending_shards: impl IntoIterator<Item = ShardIndex>,
    ) {
        self.epoch_sync_attestation.set(token, pending_shards);
    }

    /// Invalidates any unconsumed attestation token held by shard sync. Called by the
    /// epoch-change apply step when another component owns the attestation.
    pub(crate) fn clear_epoch_sync_done_token(&self) {
        self.epoch_sync_attestation.clear();
    }

    /// Hands the metadata-recovery task its completion instruction — the status transition to
    /// perform once metadata recovery finishes and the shard syncs are started. Called from
    /// inside the epoch-change critical section when the node status is set to
    /// `RecoverMetadata` (and by the startup resumption path).
    pub(crate) fn set_metadata_recovery_completion(
        &self,
        instruction: BackgroundSyncTaskCompletionInstruction,
    ) {
        self.metadata_recovery_completion.put(instruction);
    }

    /// Revokes any pending metadata-recovery completion instruction. Called when a transition
    /// supersedes the metadata recovery.
    pub(crate) fn clear_metadata_recovery_completion(&self) {
        self.metadata_recovery_completion.clear();
    }

    /// Returns `true` if any shard sync task is currently running.
    pub fn has_sync_in_progress(&self) -> bool {
        *self.sync_task_count.borrow() > 0
    }

    /// Waits until no shard sync task is running.
    ///
    /// A shard sync that failed terminally (requiring a node restart to be retried) does not
    /// count as running; its shard remains in `ActiveSync` or `ActiveRecover` status, which
    /// blocks node recovery from completing until the sync is retried.
    pub async fn wait_until_no_sync_in_progress(&self) {
        let mut receiver = self.sync_task_count.subscribe();
        receiver
            .wait_for(|count| *count == 0)
            .await
            .expect("the sender is owned by self and cannot be dropped while waiting");
    }

    /// Starts sync shards. If the node status is [`NodeStatus::RecoverMetadata`], syncs certified
    /// blob metadata before syncing shards.
    pub async fn start_sync_shards(
        &self,
        shards: Vec<ShardIndex>,
    ) -> Result<(), SyncShardClientError> {
        let mut task_handle = self.task_handle.lock().await;
        let sync_handler = self.clone();

        // If there is an existing task, we need to abort it first before starting a new one.
        // Aborting is safe: the task derives its work from the persisted node status and shard
        // statuses, so the new task picks up everything the old task has not finished yet,
        // including blob metadata recovery. Note that aborting the task does not cancel the
        // individual shard sync tasks it has already started; those are tracked separately in
        // `shard_sync_in_progress`.
        if let Some(old_task) = task_handle.take() {
            old_task.abort();
        }

        // Count the task that starts the individual shard syncs as a running sync, so that
        // waiters cannot observe a zero count before the per-shard sync tasks are spawned.
        let count_guard = SyncTaskCountGuard::new(self.sync_task_count.clone());
        let new_task = tokio::spawn(async move {
            let _count_guard = count_guard;
            sync_handler.sync_shards_task(shards).await
        });

        *task_handle = Some(new_task);

        Ok(())
    }

    async fn sync_shards_task(&self, shards: Vec<ShardIndex>) {
        let node_status = self
            .node
            .storage
            .node_status()
            .expect("failed to read node status from db");

        let shards = if node_status == NodeStatus::RecoverMetadata {
            if let Err(err) = self.sync_certified_blob_metadata().await {
                tracing::error!(?err, "failed to sync blob metadata; aborting shard sync");
                return;
            }

            // While the node is recovering metadata, sync all shards the node currently owns
            // instead of only the shards passed by the caller: this task may have aborted a
            // previous sync-shards task (for example, when gaining shards in a subsequent epoch
            // while metadata recovery is still in progress) before that task could start the
            // syncs for its own shards. Shards that are already active are skipped in
            // `start_new_shard_sync`. Stored shards that the node does not own in the current
            // committee (for example, shards locked for transfer to another node) must not be
            // synced and are filtered out here.
            let owned_shards = self.node.owned_shards_at_latest_epoch();
            self.node
                .storage
                .existing_shard_storages()
                .await
                .iter()
                .map(|s| s.id())
                .filter(|shard| owned_shards.contains(shard))
                .collect()
        } else {
            shards
        };

        // On the first run after a restart, apply the restart-only resume policy
        // (optionally retrying the sliver transfer before falling back to recovery).
        let apply_restart_resume_policy = self
            .apply_restart_resume_policy
            .swap(false, std::sync::atomic::Ordering::SeqCst)
            && self.config.restart_shard_sync_always_retry_transfer_first;

        // Ensure a sync is running for each shard, dispatching on the persisted status: shards
        // mid-sync resume from their persisted progress, others start a fresh sync. All sync
        // work is quiesced inside the critical section at every epoch change, so the statuses
        // observed here are settled.
        for shard in shards {
            if self
                .shard_sync_in_progress
                .lock()
                .await
                .contains_key(&shard)
            {
                continue;
            }
            let Some(shard_storage) = self.node.storage.shard_storage(shard).await else {
                tracing::warn!(
                    walrus.shard_index = %shard,
                    "shard storage does not exist; cannot sync shard"
                );
                continue;
            };
            let status = match shard_storage
                .shard_status_resume_active_shard_sync(apply_restart_resume_policy)
                .await
            {
                Ok(status) => status,
                Err(error) => {
                    tracing::error!(?error, %shard, "failed to read shard status; skipping");
                    continue;
                }
            };
            match status {
                ShardStatus::Active => {}
                // Resume an interrupted sync from its persisted progress.
                ShardStatus::ActiveSync | ShardStatus::ActiveRecover => {
                    self.start_shard_sync_impl(shard_storage).await;
                }
                // Start a fresh sync for a newly created (or re-gained) shard.
                _ => {
                    if let Err(err) = self.start_new_shard_sync(shard).await {
                        tracing::error!(?err, %shard, "failed to start shard sync; skipping");
                        continue;
                    }
                }
            }
        }

        // Once we have started the shard sync tasks, the shard statuses have been persisted to
        // disk, so the node can leave `RecoverMetadata`: any restart from this point will
        // re-start the shard syncs only, without syncing metadata again. The completion
        // instruction authorizes exactly this transition: metadata recovery can run for a long
        // time, during which a concurrent path (for example, entering recovery or dropping out
        // of the committee at an epoch change) may have superseded it — such a path revokes the
        // instruction, and this task then finishes without touching the node status.
        //
        // The take and the status write happen inside the epoch-change critical section:
        // clearing the slot only revokes an instruction that has not been taken yet, so taking
        // and applying must be atomic with respect to a superseding transition. This mirrors
        // the recovery task's completion in node_recovery.rs.
        let critical_section_guard = self.node.epoch_change_critical_section.enter().await;
        if let Some(instruction) = self.metadata_recovery_completion.take() {
            sui_macros::fail_point_async!("fail_point_metadata_completion_in_critical_section");
            let attestation = instruction
                .apply_status(&self.node)
                .expect("failed to apply the metadata-recovery completion status");
            debug_assert!(
                attestation.is_none(),
                "metadata recovery does not own the epoch sync done attestation"
            );
        }
        drop(critical_section_guard);

        // Metadata recovery can be the last outstanding work of the attestation (every
        // pending shard already synced): consume the token if so.
        if let Some(token) = self.epoch_sync_attestation.take_if_complete() {
            token.attest(&self.node).await;
        }
    }

    /// Syncs the certified blob metadata before the current epoch.
    ///
    /// This function performs the following steps:
    /// 1. Retrieves all certified blob info from storage before the current epoch
    /// 2. Processes blobs concurrently up to max_concurrent_metadata_fetch limit
    /// 3. For each blob, syncs its metadata using sync_single_blob_metadata
    async fn sync_certified_blob_metadata(&self) -> Result<(), SyncShardClientError> {
        tracing::info!("start syncing blob metadata");
        // Bounded by the event epoch for the same reason as the shard-sync bound in
        // `start_shard_sync_impl`: the local certified-blob list is only complete up to the
        // processed events.
        let Ok(epoch_bound) = self.node.current_event_epoch().await else {
            tracing::info!("event epoch channel closed; not syncing blob metadata");
            return Ok(());
        };
        tracing::error!(epoch_bound, "DIAG metadata sync starting");
        let blob_infos = self
            .node
            .storage
            .certified_blob_info_iter_before_epoch(epoch_bound);

        #[cfg(msim)]
        {
            inject_recovery_metadata_failure_before_fetch()?;
            fail_point_async!("fail_point_shard_sync_recovery_metadata_pause");
        }

        let mut futures = FuturesUnordered::new();
        let mut active_count = 0;

        #[cfg(msim)]
        let mut scan_count = 0; // Used to trigger fail point

        for blob_info in blob_infos {
            let (blob_id, blob_info) = blob_info?;
            let node_clone = self.node.clone();

            // TODO(WAL-478):
            //   - create a end point that can transfer multiple blob metadata at once.
            futures.push(Self::sync_single_blob_metadata(
                node_clone, blob_id, blob_info,
            ));
            active_count += 1;

            #[cfg(msim)]
            {
                scan_count += 1;
                inject_recovery_metadata_failure_during_fetch(scan_count)?;
            }

            // Wait for a task to complete if we've reached max concurrent limit
            while active_count >= self.config.max_concurrent_metadata_fetch {
                // Process one completed future
                if let Some(result) = futures.next().await {
                    result.map_err(|e| SyncShardClientError::Internal(e.into()))?;
                }
                active_count -= 1;
            }
        }

        // Wait for remaining tasks to complete
        while let Some(result) = futures.next().await {
            result.map_err(|e| SyncShardClientError::Internal(e.into()))?;
        }

        tracing::info!("finished syncing blob metadata");
        Ok(())
    }

    /// Syncs a single blob metadata.
    async fn sync_single_blob_metadata(
        node: Arc<StorageNodeInner>,
        blob_id: BlobId,
        blob_info: BlobInfo,
    ) -> Result<(), SyncShardClientError> {
        node.metrics
            .sync_blob_metadata_progress
            .set(i64::from(blob_id.first_two_bytes()));

        let result = node
            .blob_retirement_notifier
            .execute_with_retirement_check(&node, blob_id, || {
                node.get_or_recover_blob_metadata(
                    &blob_id,
                    blob_info
                        .initial_certified_epoch()
                        .expect("certified blob must have certified epoch set"),
                )
            })
            .await?;

        match result {
            ExecutionResultWithRetirementCheck::Executed(result) => {
                result?;
                node.metrics.sync_blob_metadata_count.inc();
            }
            ExecutionResultWithRetirementCheck::BlobRetired => {
                tracing::debug!(%blob_id, "blob retired; skipping sync");
                node.metrics.sync_blob_metadata_skipped.inc();
            }
        }

        Ok(())
    }

    /// Starts syncing a new shard. This method is used when a new shard is assigned to the node.
    async fn start_new_shard_sync(
        &self,
        shard_index: ShardIndex,
    ) -> Result<(), SyncShardClientError> {
        // All sync work is quiesced inside the critical section at every epoch change, so any
        // task observed here belongs to the current info.
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

        // Get shard storage
        let shard_storage = self
            .node
            .storage
            .shard_storage(shard_index)
            .await
            .ok_or_else(|| {
                tracing::error!(
                    "{shard_index} is not assigned to this node; cannot start shard sync"
                );
                ShardNotAssigned(shard_index, self.node.current_committee_epoch())
            })?;

        let shard_status = shard_storage.status().await?;

        // Skip if shard is already active. The attestation's pending set never lists active
        // shards (it is computed from the persisted statuses inside the same critical section
        // that quiesced all sync work), so there is nothing to record here.
        if shard_status == ShardStatus::Active {
            tracing::info!(
                walrus.shard_index = %shard_index,
                "shard has already been synced; skipping sync"
            );
            return Ok(());
        }

        // Update status and start sync. After this function returns, we can always restart
        // the sync upon node restart.
        shard_storage.record_start_shard_sync().await?;
        self.start_shard_sync_impl(shard_storage.clone()).await;
        Ok(())
    }

    /// Restarts syncing shards that were previously syncing. This method is used when restarting
    /// the node.
    /// Spawns the permanent shard-sync reconciler.
    ///
    /// The reconciler watches the sync-and-recovery info: on an info describing the node as
    /// a committee member with shards to fill via shard sync, it starts the syncs; on a
    /// catching-up or non-member info, it quiesces all sync work (the node's view of its
    /// assignment is not authoritative, or the node makes no epoch-sync claim). The
    /// epoch-change logic no longer starts syncs directly: it publishes the info, and this
    /// service reconciles toward it.
    ///
    /// Each reconciliation pass runs inside the epoch-change critical section, re-reading the
    /// latest info after entering it. A pass therefore never straddles an epoch change: it
    /// either completes before the change enters the critical section (and the change then
    /// quiesces whatever the pass started), or runs after the change and observes the new
    /// info. Without this, a pass could relaunch syncs derived from a superseded info after
    /// the change's quiesce — including for a shard the node just lost, which would
    /// re-activate the shard's locked storage.
    ///
    /// The initial reconciliation runs synchronously, before this function returns: resumed
    /// sync work is then already visible to `has_sync_in_progress` when the event processors
    /// start, so the first epoch-change event cannot be planned against a zero live-task
    /// count while a resumed sync is still incomplete (which would hand the attestation to
    /// the finisher prematurely).
    // TODO(WAL-1263): cancel the shard-sync service when the node is shut down.
    pub(crate) async fn spawn_background_shard_sync_driver(
        &self,
        mut info_receiver: watch::Receiver<EpochChangeSyncAndRecoveryInfo>,
    ) {
        let mut service_handle = self
            .service_handle
            .try_lock()
            .expect("spawn_background_shard_sync_driver is called once, at startup");
        assert!(
            service_handle.is_none(),
            "the shard sync reconciler is already running"
        );

        self.reconcile_in_critical_section(&mut info_receiver).await;

        let handler = self.clone();
        *service_handle = Some(tokio::spawn(async move {
            loop {
                if info_receiver.changed().await.is_err() {
                    // TODO(WAL-1269): terminate the node when the shard-sync reconciler loop
                    // exits; a node without the reconciler silently stops syncing its shards.
                    tracing::info!("info channel closed; stopping the shard sync reconciler");
                    return;
                }
                handler
                    .reconcile_in_critical_section(&mut info_receiver)
                    .await;
            }
        }));
    }

    /// Runs one reconciliation pass inside the epoch-change critical section, against the
    /// latest published info (re-read after entering the critical section, where it is stable:
    /// the info is only ever published from inside the same critical section).
    async fn reconcile_in_critical_section(
        &self,
        info_receiver: &mut watch::Receiver<EpochChangeSyncAndRecoveryInfo>,
    ) {
        let _critical_section_guard = self.node.epoch_change_critical_section.enter().await;
        let info = info_receiver.borrow_and_update().clone();
        self.reconcile_toward_info(&info).await;
    }

    /// Reconciles the running sync work toward the given info.
    ///
    /// Quiescing is not done here: the executor quiesces all sync work synchronously, inside
    /// the critical section that publishes each goal, so no task from an earlier goal exists
    /// by the time a goal becomes visible. This service only rebuilds: it derives the shards
    /// that still need syncing from the info's owned set and the persisted shard statuses
    /// (interrupted syncs resume from their persisted progress) and starts them.
    async fn reconcile_toward_info(&self, info: &EpochChangeSyncAndRecoveryInfo) {
        if info.catching_up || !info.membership.is_member() {
            // Nothing to rebuild: while catching up the assignment is not authoritative, and a
            // non-member makes no epoch-sync claim.
            return;
        }
        if info
            .shards_to_fill
            .as_ref()
            .is_some_and(|new_shards| new_shards.fill == ShardFill::ForceActive)
        {
            // The shards are filled by node recovery, not by shard sync.
            return;
        }

        let mut shards_to_sync = Vec::new();
        for shard in &info.owned_shards {
            let Some(shard_storage) = self.node.storage.shard_storage(*shard).await else {
                continue;
            };
            match shard_storage.status().await {
                Ok(ShardStatus::Active) => {}
                Ok(_) => shards_to_sync.push(*shard),
                Err(error) => {
                    tracing::error!(?error, walrus.shard_index = %shard, "failed to read status");
                    shards_to_sync.push(*shard);
                }
            }
        }

        // A node recovering metadata needs the sync-driving task even without shards to sync:
        // the task performs the metadata recovery and applies its completion instruction (the
        // transition to `Active`).
        let metadata_recovery_pending = matches!(
            self.node.storage.node_status(),
            Ok(NodeStatus::RecoverMetadata)
        );
        if shards_to_sync.is_empty() && !metadata_recovery_pending {
            return;
        }
        if let Err(error) = self.start_sync_shards(shards_to_sync).await {
            tracing::error!(
                ?error,
                "failed to start the shard syncs for the published info"
            );
        }
    }

    /// Aborts the sync-driving task and every per-shard sync task, waiting for them to
    /// exit. Called by the epoch-change executor inside the critical section, so that no
    /// sync task from an earlier epoch change coexists with a newly published info or with
    /// the attestation's pending set. Sync progress is persisted per batch: at most the
    /// in-flight batches are repeated when the reconciler rebuilds the still-needed syncs.
    pub(crate) async fn quiesce_all_syncs(&self) {
        if let Some(task) = self.task_handle.lock().await.take() {
            task.abort();
            let _ = task.await;
        }

        let sync_tasks: Vec<_> = {
            let mut shard_sync_in_progress = self.shard_sync_in_progress.lock().await;
            shard_sync_in_progress.drain().collect()
        };
        if sync_tasks.is_empty() {
            return;
        }
        tracing::info!(
            count = sync_tasks.len(),
            "quiescing all in-progress shard syncs"
        );
        for (shard_index, task) in sync_tasks {
            tracing::info!(walrus.shard_index = %shard_index, "aborting shard sync");
            task.abort();
            let _ = task.await;
        }
    }

    /// Ensures syncs are running for the persisted sync state, as the production startup
    /// path does through the executor's startup goal publication and the reconciler's first
    /// pass: shards whose persisted status is mid-sync are resumed, and a node in metadata
    /// recovery re-runs the sync-driving task. Tests simulating a restart on a standalone
    /// handler call this directly.
    #[cfg(any(test, feature = "test-utils"))]
    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn restart_syncs(&self) -> Result<(), anyhow::Error> {
        self.apply_restart_resume_policy
            .store(true, std::sync::atomic::Ordering::SeqCst);
        if self.node.storage.node_status()? == NodeStatus::RecoverMetadata {
            self.set_epoch_sync_done_token(
                EpochSyncDoneToken::new_for_epoch(self.node.current_committee_epoch()),
                [],
            );
            self.set_metadata_recovery_completion(BackgroundSyncTaskCompletionInstruction::new(
                NodeStatus::Active,
                None,
            ));
            // The task derives the shards to sync from the owned shard storages itself.
            self.start_sync_shards(Vec::new()).await?;
            return Ok(());
        }
        let mut shards_to_resume = Vec::new();
        for shard_storage in self.node.storage.existing_shard_storages().await {
            if matches!(
                shard_storage.status().await,
                Ok(ShardStatus::ActiveSync | ShardStatus::ActiveRecover)
            ) {
                shards_to_resume.push(shard_storage.id());
            }
        }
        if !shards_to_resume.is_empty() {
            self.set_epoch_sync_done_token(
                EpochSyncDoneToken::new_for_epoch(self.node.current_committee_epoch()),
                shards_to_resume.iter().copied(),
            );
            self.start_sync_shards(shards_to_resume).await?;
        }
        Ok(())
    }

    async fn start_shard_sync_impl(&self, shard_storage: Arc<ShardStorage>) {
        // Sync up to the node's current *event* epoch, not the fetched on-chain epoch: the sync
        // enumerates its work from the local certified-blob list, which is only complete up to
        // the events the node has processed. After a restart, syncs can resume while event
        // replay is still behind the on-chain epoch; a bound beyond the replay position would
        // silently skip blobs certified in the unreplayed gap — they are absent from the local
        // list, so they are neither fetched from the source nor scheduled for recovery, and the
        // later replay of their certified events skips them too (the node did not own the shard
        // at the event's epoch). See the data-loss analysis in issue #3609. Each replayed epoch
        // change re-quiesces and restarts the sync with an advanced bound, so the sync
        // converges to the on-chain epoch as replay completes. For epoch changes processed at
        // the head of the stream, the executor records the event epoch inside the critical
        // section before publishing, so this equals the new epoch.
        let Ok(current_committee_epoch) = self.node.current_event_epoch().await else {
            tracing::info!("event epoch channel closed; not starting the shard sync");
            return;
        };

        tracing::info!(
            walrus.shard_index = %shard_storage.id(),
            "syncing shard to the beginning of epoch {}",
            current_committee_epoch
        );
        let diag_status = shard_storage.status().await;
        tracing::error!(
            shard = %shard_storage.id(),
            epoch_bound = current_committee_epoch,
            shard_status = ?diag_status,
            "DIAG shard sync starting"
        );

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

        let shard_sync_handler_clone = self.clone();
        let count_guard = SyncTaskCountGuard::new(self.sync_task_count.clone());
        let shard_sync_task = tokio::spawn(async move {
            let _count_guard = count_guard;
            let shard_index = shard_storage.id();
            let mut last_progress_time = Instant::now();

            let mut backoff = ExponentialBackoff::new_with_seed(
                shard_sync_handler_clone.config.shard_sync_retry_min_backoff,
                shard_sync_handler_clone.config.shard_sync_retry_max_backoff,
                None,
                u64::from(shard_index.0), // Seed the backoff with the shard index.
            );

            // Whether to directly recover the shard instead of using shard sync.
            let mut directly_recover_shard = false;
            let mut shard_sync_success = false;
            loop {
                tracing::info!(
                    shard_index=%shard_index,
                    ?directly_recover_shard,
                    "syncing shard to the beginning of epoch {}",
                    current_committee_epoch
                );
                match shard_sync_handler_clone
                    .sync_shard_impl(
                        shard_storage.clone(),
                        current_committee_epoch,
                        directly_recover_shard,
                    )
                    .await
                {
                    SyncShardResult::Success => {
                        shard_sync_success = true;
                        break;
                    }
                    SyncShardResult::Failed => {
                        tracing::warn!(
                            shard_index=%shard_index,
                            "shard sync stopped due to errors; restart node to retry shard sync"
                        );
                        break;
                    }
                    SyncShardResult::RetryAfterBackoff {
                        force_recovery,
                        shard_sync_made_progress,
                    } => {
                        let backoff_duration = backoff.next_delay();
                        let Some(backoff_duration) = backoff_duration else {
                            tracing::warn!(
                                shard_index=%shard_index,
                                "maximum number of retries reached; stop shard sync; \
                                restart node to retry shard sync"
                            );
                            break;
                        };
                        tokio::time::sleep(backoff_duration).await;

                        if shard_sync_made_progress {
                            tracing::debug!(
                                shard_index=%shard_index,
                                "shard sync made progress"
                            );
                            last_progress_time = Instant::now();
                        }
                        if last_progress_time.elapsed()
                            > shard_sync_handler_clone
                                .config
                                .shard_sync_retry_switch_to_recovery_interval
                            || force_recovery
                        {
                            tracing::info!(
                                shard_index=%shard_index,
                                "shard sync failed; directly recovering shard"
                            );
                            directly_recover_shard = true;
                        }
                    }
                }
            }

            let diag_final_status = shard_storage.status().await;
            tracing::error!(
                shard = %shard_index,
                shard_sync_success,
                epoch_bound = current_committee_epoch,
                final_status = ?diag_final_status,
                "DIAG shard sync task ended"
            );

            // Remove the task from the shard_sync_in_progress map upon completion, and record
            // the synced shard with the attestation. All sync work is quiesced inside the
            // critical section at every epoch change, so every running task belongs to the
            // current pending set; the completion that empties the pending set consumes
            // the token. While the node is recovering, node recovery holds the attestation
            // instead (bundled in its completion instruction; see node_recovery.rs) and there
            // is no token here to consume.
            //
            // The removal and the recording happen under one hold of the map lock, which
            // quiesce_all_syncs also takes to drain the map: a completing task therefore
            // either records against the pending set it belongs to before the quiesce
            // returns, or is aborted before it can record — it can never record against a
            // later epoch's pending set.
            if shard_sync_success {
                let token = {
                    let mut shard_sync_in_progress =
                        shard_sync_handler_clone.shard_sync_in_progress.lock().await;
                    shard_sync_in_progress.remove(&shard_index);
                    shard_sync_handler_clone
                        .epoch_sync_attestation
                        .record_shard_synced(shard_index)
                };
                if let Some(token) = token {
                    token.attest(&shard_sync_handler_clone.node).await;
                }
            }
        });
        entry.insert(shard_sync_task);
    }

    /// Syncs a shard using shard sync. If `directly_recover_shard` is true, the shard will be
    /// directly recovered instead of using shard sync.
    async fn sync_shard_impl(
        &self,
        shard_storage: Arc<ShardStorage>,
        current_epoch: Epoch,
        directly_recover_shard: bool,
    ) -> SyncShardResult {
        // The rate limit is enforced by the semaphore, without considering
        // the priority of the syncs.
        let Ok(_permit) = self.shard_sync_semaphore.acquire().await else {
            tracing::error!("failed to acquire shard sync semaphore.");
            return SyncShardResult::RetryAfterBackoff {
                force_recovery: false,
                shard_sync_made_progress: false,
            };
        };

        walrus_utils::with_label!(self.node.metrics.shard_sync_total, "start").inc();
        let shard_index = shard_storage.id();
        let (shard_sync_made_progress, sync_result) = shard_storage
            .start_sync_shard_before_epoch(
                current_epoch,
                self.node.clone(),
                &self.config,
                directly_recover_shard,
            )
            .await;
        match sync_result {
            Ok(_) => {
                walrus_utils::with_label!(self.node.metrics.shard_sync_total, "complete").inc();
                tracing::info!(
                    walrus.shard_index = %shard_index,
                    "successfully synced shard to before epoch {}",
                    current_epoch
                );
                SyncShardResult::Success
            }
            Err(error) => {
                walrus_utils::with_label!(self.node.metrics.shard_sync_total, "error").inc();
                tracing::error!(
                    ?error,
                    "failed to sync {shard_index} to before epoch {current_epoch}"
                );

                #[cfg(msim)]
                if check_no_retry_fail_point() {
                    return SyncShardResult::Success;
                }

                Self::handle_sync_error(
                    &error,
                    shard_index,
                    directly_recover_shard,
                    shard_sync_made_progress,
                )
            }
        }
    }

    /// Handles sync shard errors and determines whether/how to retry
    fn handle_sync_error(
        error: &SyncShardClientError,
        shard_index: ShardIndex,
        directly_recover_shard: bool,
        shard_sync_made_progress: bool,
    ) -> SyncShardResult {
        if let SyncShardClientError::RequestError(node_error) = error {
            // Handle epoch-related errors
            match node_error.service_error() {
                Some(ServiceError::InvalidEpoch {
                    request_epoch,
                    server_epoch,
                }) => {
                    if request_epoch > server_epoch {
                        tracing::info!(
                            request_epoch,
                            server_epoch,
                            shard_sync_made_progress,
                            "source storage node hasn't reached the epoch yet"
                        );
                        return SyncShardResult::RetryAfterBackoff {
                            force_recovery: false,
                            shard_sync_made_progress,
                        };
                    }
                }
                Some(ServiceError::RequestUnauthorized) => {
                    tracing::info!(
                        ?error,
                        shard_sync_made_progress,
                        "source storage node may not reach to the epoch where the \
                        destination storage node is in the committee; retry shard sync"
                    );
                    return SyncShardResult::RetryAfterBackoff {
                        force_recovery: false,
                        shard_sync_made_progress,
                    };
                }
                _ => {}
            }

            // Handle network errors. This means to capture all the networking related errors.
            // We want to retry shard sync instead of directly recovering the shard.
            if node_error.is_reqwest() {
                tracing::info!(
                    ?error,
                    shard_sync_made_progress,
                    "encounter reqwest error; retry shard sync"
                );
                return SyncShardResult::RetryAfterBackoff {
                    force_recovery: false,
                    shard_sync_made_progress,
                };
            }
        }

        if cfg!(msim)
            && error
                .to_string()
                .contains("fetch_sliver simulated sync failure, retryable: true")
        {
            return SyncShardResult::RetryAfterBackoff {
                force_recovery: false,
                shard_sync_made_progress,
            };
        }

        // Shard sync encountered non-retryable error. Try direct recovery if not already doing so
        if !directly_recover_shard {
            tracing::warn!(
                walrus.shard_index = %shard_index,
                ?error,
                shard_sync_made_progress,
                "shard sync failed; directly recovering shard next time"
            );
            SyncShardResult::RetryAfterBackoff {
                force_recovery: true,
                shard_sync_made_progress,
            }
        } else {
            tracing::warn!(
                walrus.shard_index = %shard_index,
                ?error,
                shard_sync_made_progress,
                "shard recovery also failed; stop shard sync"
            );
            SyncShardResult::Failed
        }
    }

    #[cfg(test)]
    pub async fn current_sync_task_count(&self) -> usize {
        self.shard_sync_in_progress
            .lock()
            .await
            .values()
            .filter(|task| !task.is_finished())
            .count()
    }

    #[cfg(all(msim, test, feature = "test-utils"))]
    pub async fn no_pending_recover_metadata(&self) -> bool {
        let task_handle = self.task_handle.lock().await;
        task_handle.is_none() || task_handle.as_ref().unwrap().is_finished()
    }

    #[cfg(all(msim, test, feature = "test-utils"))]
    pub async fn clear_shard_sync_tasks(&self) {
        self.shard_sync_in_progress.lock().await.clear();
    }
}

// Helper function for fail point testing
#[cfg(msim)]
fn check_no_retry_fail_point() -> bool {
    let mut no_retry = false;
    sui_macros::fail_point_if!("fail_point_shard_sync_no_retry", || { no_retry = true });
    no_retry
}

// Inject a failure point to simulate a sync failure.
#[cfg(msim)]
fn inject_recovery_metadata_failure_before_fetch() -> Result<(), SyncShardClientError> {
    let mut sync_blob_metadata_error = false;
    fail_point_if!(
        "fail_point_shard_sync_recovery_metadata_error_before_fetch",
        || {
            sync_blob_metadata_error = true;
        }
    );

    if sync_blob_metadata_error {
        return Err(SyncShardClientError::Internal(anyhow::anyhow!(
            "fail point triggered sync blob metadata error before fetching"
        )));
    }
    Ok(())
}

// Inject a failure point to simulate a sync failure.
#[cfg(msim)]
fn inject_recovery_metadata_failure_during_fetch(
    scan_count: u64,
) -> Result<(), SyncShardClientError> {
    let mut sync_blob_metadata_error = false;
    fail_point_arg!(
        "fail_point_shard_sync_recovery_metadata_error_during_fetch",
        |trigger_at: u64| {
            tracing::info!(
                trigger_index = ?trigger_at,
                blob_count = ?scan_count,
                fail_point = "fail_point_shard_sync_recovery_metadata_error_during_fetch",
            );
            if trigger_at == scan_count {
                sync_blob_metadata_error = true;
            }
        }
    );

    if sync_blob_metadata_error {
        return Err(SyncShardClientError::Internal(anyhow::anyhow!(
            "fail point triggered sync blob metadata error during fetching"
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{StorageNodeHandle, TestCluster};

    async fn create_test_cluster(assignment: &[&[u16]]) -> TestCluster {
        let cluster: TestCluster<StorageNodeHandle> = TestCluster::<StorageNodeHandle>::builder()
            .with_shard_assignment(assignment)
            .build()
            .await
            .unwrap();
        // The shard-sync driver bounds its work by the node's event epoch and waits until it
        // is set; these tests drive standalone handlers without event processing, so mark the
        // nodes as having processed events up to the current committee epoch.
        for node in &cluster.nodes {
            let inner = &node.storage_node.inner;
            let _ = inner
                .latest_event_epoch_sender
                .send(Some(inner.current_committee_epoch()));
        }
        cluster
    }

    #[tokio::test(start_paused = false)]
    async fn test_restart_syncs() {
        let cluster = create_test_cluster(&[&[0, 1, 2]]).await;
        // The node's own reconciler starts syncs for its shards at startup; wait for those to
        // settle before manipulating the shard statuses with a standalone handler.
        cluster.nodes[0]
            .storage_node
            ._shard_sync_handler
            .wait_until_no_sync_in_progress()
            .await;
        for i in [0, 2] {
            cluster.nodes[0]
                .storage_node
                .inner
                .storage
                .shard_storage(ShardIndex(i))
                .await
                .expect("Failed to get shard storage")
                .update_status_in_test(ShardStatus::ActiveSync)
                .await
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

        // The restarted syncs resume the two mid-sync shards and run them to completion,
        // returning their statuses to `Active`.
        for _ in 0..100 {
            let mut all_active = true;
            for i in [0, 2] {
                let status = cluster.nodes[0]
                    .storage_node
                    .inner
                    .storage
                    .shard_storage(ShardIndex(i))
                    .await
                    .expect("Failed to get shard storage")
                    .status()
                    .await
                    .expect("Failed to read shard status");
                all_active &= status == ShardStatus::Active;
            }
            if all_active {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        for i in [0, 2] {
            assert_eq!(
                cluster.nodes[0]
                    .storage_node
                    .inner
                    .storage
                    .shard_storage(ShardIndex(i))
                    .await
                    .expect("Failed to get shard storage")
                    .status()
                    .await
                    .expect("Failed to read shard status"),
                ShardStatus::Active
            );
        }
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
            .await
            .expect("Failed to get shard storage")
            .update_status_in_test(ShardStatus::None)
            .await
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
