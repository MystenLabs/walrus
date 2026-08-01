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
        completion::{CompletionInstruction, CompletionSlot},
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

/// A running per-shard sync task, together with the epoch its sync targets.
///
/// The epoch distinguishes a still-running sync from an earlier epoch (whose data stops at that
/// epoch's bound) from a sync for the current epoch: when a shard is lost and later re-gained,
/// the stale sync is aborted and replaced instead of blocking (or being credited as) the new
/// epoch's sync.
#[derive(Debug)]
struct InProgressShardSync {
    task_handle: tokio::task::JoinHandle<()>,
    target_epoch: Epoch,
}

/// Manages tasks for syncing shards during epoch change.
#[derive(Debug, Clone)]
pub struct ShardSyncHandler {
    node: Arc<StorageNodeInner>,
    shard_sync_in_progress: Arc<Mutex<HashMap<ShardIndex, InProgressShardSync>>>,
    task_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
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
            shard_sync_semaphore: Arc::new(Semaphore::new(config.shard_sync_concurrency)),
            sync_task_count: Arc::new(watch::channel(0).0),
            epoch_sync_attestation: ShardSyncAttestation::default(),
            metadata_recovery_completion: CompletionSlot::default(),
            config,
        }
    }

    /// Hands the `epoch_sync_done` attestation token to shard sync, atomically registering the
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

    /// Takes the registered attestation token back if its registration is already complete (no
    /// pending shards and no running sync task). Called by the epoch-change apply step right
    /// after registering a token with no newly gained shards: the draining syncs that made
    /// shard sync the attestation owner may have finished in the meantime, leaving no future
    /// completion to consume the token; the caller then hands it to the finisher instead.
    pub(crate) async fn try_take_idle_epoch_sync_attestation(&self) -> Option<EpochSyncDoneToken> {
        // Hold the task-map lock across the check so it is atomic with respect to completing
        // tasks, which remove themselves from the map before recording their shard as synced.
        let shard_sync_map = self.shard_sync_in_progress.lock().await;
        let token = self
            .epoch_sync_attestation
            .take_if_complete(shard_sync_map.is_empty());
        drop(shard_sync_map);
        token
    }

    /// Hands the metadata-recovery task its completion instruction — the status transition to
    /// perform once metadata recovery finishes and the shard syncs are started. Called from
    /// inside the epoch-change critical section when the node status is set to
    /// `RecoverMetadata` (and by the startup resumption path).
    pub(crate) fn set_metadata_recovery_completion(&self, instruction: CompletionInstruction) {
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

    /// Restarts blob metadata recovery, and the subsequent syncs of the shards the node owns.
    ///
    /// The caller must ensure the persisted node status is [`NodeStatus::RecoverMetadata`]; the
    /// spawned task re-reads the status and only performs metadata recovery in that state,
    /// otherwise it is a no-op (an empty shard list is passed because the task derives the real
    /// shards from the owned shard storages itself).
    async fn restart_metadata_sync(&self) -> Result<(), SyncShardClientError> {
        self.start_sync_shards(Vec::new()).await
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

        // Register the full work list with the attestation before spawning any sync: during
        // metadata recovery, the shards derived above may exceed the newly gained shards the
        // epoch change registered (syncs re-derived after an aborted sync-shards task), and the
        // token must not be consumable while any of them is still unsynced. A no-op when
        // another component owns the attestation.
        self.epoch_sync_attestation
            .register_pending_shards(shards.iter().copied());

        // Start sync for each shard
        for shard in shards {
            if let Err(err) = self.start_new_shard_sync(shard).await {
                tracing::error!(?err, %shard, "failed to start shard sync; skipping shard");
                continue;
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
    }

    /// Syncs the certified blob metadata before the current epoch.
    ///
    /// This function performs the following steps:
    /// 1. Retrieves all certified blob info from storage before the current epoch
    /// 2. Processes blobs concurrently up to max_concurrent_metadata_fetch limit
    /// 3. For each blob, syncs its metadata using sync_single_blob_metadata
    async fn sync_certified_blob_metadata(&self) -> Result<(), SyncShardClientError> {
        tracing::info!("start syncing blob metadata");
        let blob_infos = self
            .node
            .storage
            .certified_blob_info_iter_before_epoch(self.node.current_committee_epoch());

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
        let current_committee_epoch = self.node.current_committee_epoch();

        // restart_syncs() is called before event processor starts processing events. So, for any
        // resumed shard syncs, we should be able to observe them here, unless they have finished.
        let stale_sync = {
            let mut shard_sync_in_progress = self.shard_sync_in_progress.lock().await;
            match shard_sync_in_progress.get(&shard_index) {
                Some(existing) if existing.target_epoch >= current_committee_epoch => {
                    tracing::info!(
                        walrus.shard_index = %shard_index,
                        "shard is already being synced; skipping starting new shard sync"
                    );
                    return Ok(());
                }
                // The shard was lost and re-gained while a sync targeting an earlier epoch is
                // still running. That sync's data stops at its own epoch bound, so it must
                // neither block nor be credited as the new epoch's sync: abort it and start a
                // fresh sync targeting the current epoch.
                Some(_) => shard_sync_in_progress.remove(&shard_index),
                None => None,
            }
        };
        if let Some(stale_sync) = stale_sync {
            tracing::info!(
                walrus.shard_index = %shard_index,
                stale_target_epoch = stale_sync.target_epoch,
                current_committee_epoch,
                "aborting a stale shard sync from an earlier epoch before re-syncing the shard"
            );
            stale_sync.task_handle.abort();
            // Wait for the task to exit, so that it cannot record its stale-epoch completion
            // concurrently with the new sync.
            let _ = stale_sync.task_handle.await;
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

        // Skip if shard is already active
        if shard_status == ShardStatus::Active {
            tracing::info!(
                walrus.shard_index = %shard_index,
                "shard has already been synced; skipping sync"
            );
            // The shard's data is present, so it counts toward the attestation registration
            // (relevant when re-processing an epoch change after a crash).
            let no_other_sync_running = self.shard_sync_in_progress.lock().await.is_empty();
            if let Some(token) = self.epoch_sync_attestation.record_shard_synced(
                shard_index,
                current_committee_epoch,
                no_other_sync_running,
            ) {
                token.attest(&self.node).await;
            }
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
    pub async fn restart_syncs(&self) -> Result<(), anyhow::Error> {
        let current_node_status = self.node.storage.node_status()?;
        if current_node_status == NodeStatus::RecoverMetadata {
            // The node restarted in the middle of metadata recovery, whose `EpochChangeStart`
            // event handed the attestation to shard sync and authorized the metadata task's
            // completion: re-mint the token and the instruction (the slots are in-memory and
            // were lost with the restart). The sync-shards task derives and registers the
            // pending shards itself before spawning the syncs.
            self.set_epoch_sync_done_token(
                EpochSyncDoneToken::new_for_epoch(self.node.current_committee_epoch()),
                [],
            );
            self.set_metadata_recovery_completion(CompletionInstruction::new(
                NodeStatus::Active,
                None,
            ));
            // The task observes the `RecoverMetadata` status and derives the shards to sync from
            // the existing shard storages.
            self.restart_metadata_sync().await?;
        } else {
            let mut shard_storages_to_sync = Vec::new();
            for shard_storage in self.node.storage.existing_shard_storages().await {
                let shard_status = shard_storage
                    .shard_status_resume_active_shard_sync(
                        self.config.restart_shard_sync_always_retry_transfer_first,
                    )
                    .await?;

                match shard_status {
                    // Restart the syncing task for shards that were previously syncing.
                    ShardStatus::ActiveSync | ShardStatus::ActiveRecover => {
                        shard_storages_to_sync.push(shard_storage.clone());
                    }
                    _ => {}
                }
            }

            // Re-mint the attestation token lost with the restart, unless the node is
            // recovering: then node recovery owns the attestation and mints its own token when
            // it is resumed. The token must be in place before the first sync task starts, so
            // that a quickly completing sync cannot miss it.
            if !shard_storages_to_sync.is_empty() && !current_node_status.is_recovering() {
                self.set_epoch_sync_done_token(
                    EpochSyncDoneToken::new_for_epoch(self.node.current_committee_epoch()),
                    shard_storages_to_sync
                        .iter()
                        .map(|shard_storage| shard_storage.id()),
                );
            }

            for shard_storage in shard_storages_to_sync {
                self.start_shard_sync_impl(shard_storage).await;
            }
        }
        Ok(())
    }

    async fn start_shard_sync_impl(&self, shard_storage: Arc<ShardStorage>) {
        // This epoch must be the same as the epoch in the committee we refreshed when processing
        // epoch start event, or when the node starts up.
        let current_committee_epoch = self.node.current_committee_epoch();

        tracing::info!(
            walrus.shard_index = %shard_storage.id(),
            "syncing shard to the beginning of epoch {}",
            current_committee_epoch
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

            // Remove the task from the shard_sync_in_progress map upon completion, and record
            // the synced shard with the attestation. The token is consumed by the completion
            // that both finishes the registered shard set and observes no other running sync
            // (draining tasks from earlier epochs defer the attestation until they finish, as
            // the node may still own their shards). While the node is recovering, node recovery
            // holds the attestation instead (bundled in its completion instruction; see
            // node_recovery.rs) and there is no token here to consume.
            if shard_sync_success {
                let no_other_sync_running = {
                    let mut shard_sync_map =
                        shard_sync_handler_clone.shard_sync_in_progress.lock().await;
                    shard_sync_map.remove(&shard_index);
                    shard_sync_map.is_empty()
                };
                if let Some(token) = shard_sync_handler_clone
                    .epoch_sync_attestation
                    .record_shard_synced(
                        shard_index,
                        current_committee_epoch,
                        no_other_sync_running,
                    )
                {
                    token.attest(&shard_sync_handler_clone.node).await;
                }
            }
        });
        entry.insert(InProgressShardSync {
            task_handle: shard_sync_task,
            target_epoch: current_committee_epoch,
        });
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
            .filter(|sync| !sync.task_handle.is_finished())
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
        assert_eq!(shard_sync_handler.current_sync_task_count().await, 2);
        assert!(
            shard_sync_handler
                .shard_sync_in_progress
                .lock()
                .await
                .contains_key(&ShardIndex(0))
        );
        assert!(
            shard_sync_handler
                .shard_sync_in_progress
                .lock()
                .await
                .contains_key(&ShardIndex(2))
        );
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
