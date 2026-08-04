// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Epoch-change orchestration for the storage node.
//!
//! This module contains the processing of [`EpochChangeEvent`]s: reacting to
//! `EpochChangeStart` and `EpochChangeDone`, transitioning the committee, computing and applying
//! shard changes, and coordinating node recovery and the `epoch_sync_done` attestation.

use super::*;

pub(crate) mod attestation;
pub(crate) mod completion;
pub(crate) mod goal;
pub(crate) mod plan;

use attestation::EpochSyncDoneToken;
use completion::CompletionInstruction;
use goal::EpochSyncGoal;
use plan::{NewShards, ShardFill};

/// Threshold above which we emit a warning that the foreground portion of an
/// `EpochChangeStart` event took unexpectedly long to process. The shard-sync work
/// kicked off by the event continues in the background, so this only measures the
/// in-line bookkeeping (committee change, garbage collection scheduling, and the
/// initial shard moves) that blocks the event processor.
const EPOCH_CHANGE_START_SLOW_THRESHOLD: Duration = Duration::from_secs(300);

/// Aborts the wrapped task when dropped, used to cancel a deadline-warning task once the
/// guarded scope finishes within its budget.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// The critical section serializing node state transitions during an epoch change.
///
/// Entered by the `EpochChangeStart` handler for the whole transition (node status changes,
/// shard changes, and recovery-task handling), by the lag-detection path when entering recovery
/// mode, and by the completions of the long-running tasks that modify node status — the node
/// recovery task (see `complete_recovery_once_shards_synced` in node_recovery.rs) and the
/// metadata-recovery task (see `sync_shards_task` in shard_sync.rs). This guarantees that a
/// completing task either observes a transition's full effect — for an epoch change, the
/// advanced recovery target together with the newly started shard syncs and the locked shards —
/// or completes entirely before it. In particular, a task takes and applies its completion
/// instruction inside one critical-section occupancy, so a superseding transition either
/// happens entirely before the completion or finds the instruction already revoked.
///
/// Lock ordering: the critical section must be entered *before* acquiring the storage shard map
/// lock. The recovery task's completion reads shard statuses (which takes the shard map read
/// lock) while inside the critical section, so entering the critical section while holding the
/// shard map lock deadlocks with it.
#[derive(Debug, Default)]
pub(crate) struct EpochChangeCriticalSection(tokio::sync::Mutex<()>);

impl EpochChangeCriticalSection {
    /// Enters the critical section, waiting until any other occupant has left it.
    pub(crate) async fn enter(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.0.lock().await
    }
}

/// Publishes the startup epoch synchronization goal, derived from persisted state and the
/// fetched committees, and re-mints the commit-protocol state — the `epoch_sync_done`
/// attestation token and the completion instructions — that the previous run held in
/// memory. Together with the reconciler's first pass (which rebuilds the sync work from
/// persisted shard statuses), this makes startup just another goal publication: there is
/// no dedicated restart path.
///
/// Must be called before the sync services are spawned.
pub(super) async fn publish_startup_goal(
    inner: &Arc<StorageNodeInner>,
    shard_sync_handler: &ShardSyncHandler,
    node_recovery_handler: &NodeRecoveryHandler,
) -> anyhow::Result<()> {
    let committees = inner.committee_service.active_committees();
    let public_key = inner.public_key();
    let in_current_committee = committees.current_committee().contains(public_key);
    let in_previous_committee = committees
        .previous_committee()
        .is_some_and(|committee| committee.contains(public_key));
    let membership =
        goal::Membership::from_committee_presence(in_current_committee, in_previous_committee);
    let node_status = inner.storage.node_status()?;
    let owned_shards = inner.owned_shards_at_latest_epoch();
    let epoch = committees.epoch();

    match &node_status {
        NodeStatus::RecoveryInProgress(target_epoch) => {
            // The node restarted while recovering: node recovery owns the completion.
            node_recovery_handler.set_completion_instruction(CompletionInstruction::new(
                NodeStatus::Active,
                Some(EpochSyncDoneToken::new_for_epoch(*target_epoch)),
            ));
        }
        status if membership.is_member() && !status.is_catching_up() => {
            if node_status == NodeStatus::RecoverMetadata {
                shard_sync_handler.set_metadata_recovery_completion(CompletionInstruction::new(
                    NodeStatus::Active,
                    None,
                ));
            }
            // Register the attestation with every owned shard that is not yet `Active` —
            // the work the reconciler rebuilds. With nothing pending (and no outstanding
            // metadata recovery), the attestation for this epoch was already sent before
            // the restart, or is re-sent when the epoch-change event is re-processed.
            let mut pending_shards = Vec::new();
            for shard in &owned_shards {
                let is_active = match inner.storage.shard_storage(*shard).await {
                    Some(shard_storage) => {
                        matches!(shard_storage.status().await, Ok(ShardStatus::Active))
                    }
                    None => false,
                };
                if !is_active {
                    pending_shards.push(*shard);
                }
            }
            if !pending_shards.is_empty() || node_status == NodeStatus::RecoverMetadata {
                shard_sync_handler.set_epoch_sync_done_token(
                    EpochSyncDoneToken::new_for_epoch(epoch),
                    pending_shards,
                );
            }
        }
        _ => {}
    }

    inner.publish_epoch_sync_goal(EpochSyncGoal {
        // Both generations are assigned by the publisher.
        generation: 0,
        sync_baseline_generation: 0,
        epoch,
        catching_up: node_status.is_catching_up(),
        membership,
        owned_shards,
        shards_to_fill: None,
    });
    Ok(())
}

/// The action to take when the node transitions to a new committee.
#[derive(Debug)]
pub enum BeginCommitteeChangeAction {
    /// The node should execute the epoch change.
    ExecuteEpochChange,
    /// The node should skip the epoch change.
    SkipEpochChange,
    /// The node should enter recovery mode.
    EnterRecoveryMode,
}

impl StorageNode {
    #[tracing::instrument(skip_all)]
    pub(super) async fn process_epoch_change_event(
        &self,
        blob_event_processor: &BlobEventProcessor,
        event_handle: EventHandle,
        epoch_change_event: EpochChangeEvent,
    ) -> anyhow::Result<()> {
        let _scope = monitored_scope::monitored_scope("ProcessEvent::EpochChangeEvent");

        // Make sure we get the latest contract data from the RPC node.
        self.inner.contract_service.flush_cache().await;

        // Log the event reception with appropriate level
        match &epoch_change_event {
            EpochChangeEvent::ShardsReceived(_) => {
                tracing::debug!(
                    ?epoch_change_event,
                    "{} event received",
                    epoch_change_event.name()
                );
            }
            _ => {
                tracing::info!(
                    ?epoch_change_event,
                    "{} event received",
                    epoch_change_event.name()
                );
            }
        }

        match epoch_change_event {
            EpochChangeEvent::EpochParametersSelected(event) => {
                let _scope = monitored_scope::monitored_scope(
                    "ProcessEvent::EpochChangeEvent::EpochParametersSelected",
                );
                self.wait_for_epoch_state(event.next_epoch.saturating_sub(1), |state| {
                    matches!(state, EpochState::NextParamsSelected(_))
                })
                .await?;
                self.handle_epoch_parameters_selected(event);
                event_handle.mark_as_complete();
            }
            EpochChangeEvent::EpochChangeStart(event) => {
                let _scope = monitored_scope::monitored_scope(
                    "ProcessEvent::EpochChangeEvent::EpochChangeStart",
                );
                self.wait_for_epoch_state(event.epoch, |_| true).await?;
                fail_point_async!("epoch_change_start_entry");
                self.process_epoch_change_start_event(blob_event_processor, event_handle, &event)
                    .await?;
            }
            EpochChangeEvent::EpochChangeDone(event) => {
                let _scope = monitored_scope::monitored_scope(
                    "ProcessEvent::EpochChangeEvent::EpochChangeDone",
                );
                self.wait_for_epoch_state(event.epoch, |state| {
                    matches!(
                        state,
                        EpochState::EpochChangeDone(_) | EpochState::NextParamsSelected(_)
                    )
                })
                .await?;
                self.process_epoch_change_done_event(&event).await?;
                event_handle.mark_as_complete();
            }
            EpochChangeEvent::ShardsReceived(_) => {
                let _scope = monitored_scope::monitored_scope(
                    "ProcessEvent::EpochChangeEvent::ShardsReceived",
                );
                event_handle.mark_as_complete();
            }
            EpochChangeEvent::ShardRecoveryStart(_) => {
                let _scope = monitored_scope::monitored_scope(
                    "ProcessEvent::EpochChangeEvent::ShardRecoveryStart",
                );
                event_handle.mark_as_complete();
            }
        }
        Ok(())
    }

    /// Repeatedly checks until the current Sui epoch state matches the expectation.
    ///
    /// Returns `Ok(())` if the current epoch is equal to the `expected_epoch` and the
    /// `state_matches` function returns `true` or the current epoch is greater than the
    /// `expected_epoch` (irrespective of the state).
    #[cfg(not(any(test, msim)))]
    async fn wait_for_epoch_state(
        &self,
        expected_epoch: Epoch,
        state_matches: impl Fn(&EpochState) -> bool,
    ) -> anyhow::Result<()> {
        let config = &self.inner.epoch_state_consistency_config;
        let deadline = Instant::now() + config.timeout;
        while Instant::now() < deadline {
            self.inner.contract_service.flush_cache().await;
            let Ok((epoch, state)) = self.inner.contract_service.get_epoch_and_state().await else {
                tracing::warn!("failed to get current epoch and state");
                continue;
            };
            if epoch == expected_epoch && state_matches(&state) || epoch > expected_epoch {
                return Ok(());
            }
            tracing::debug!(
                expected_epoch,
                current_epoch = epoch,
                current_state = ?state,
                "waiting for expected epoch state",
            );
            sleep(config.poll_interval).await;
        }
        bail!("timed out after waiting for expected epoch state")
    }

    #[cfg(any(test, msim))]
    #[allow(clippy::unused_async)]
    async fn wait_for_epoch_state(
        &self,
        _expected_epoch: Epoch,
        _state_matches: impl Fn(&EpochState) -> bool,
    ) -> anyhow::Result<()> {
        tracing::info!("waiting for epoch state is not supported in tests, skipping");
        Ok(())
    }

    /// Handles the epoch parameters selected event.
    ///
    /// This function cancels the scheduled voting end and initiates the epoch change.
    /// It also schedules the process subsidies and marks the event as complete.
    #[tracing::instrument(skip_all)]
    fn handle_epoch_parameters_selected(
        &self,
        event: walrus_sdk::sui::types::EpochParametersSelected,
    ) {
        self.epoch_change_driver
            .cancel_scheduled_voting_end(event.next_epoch);
        self.epoch_change_driver.schedule_initiate_epoch_change(
            NonZero::new(event.next_epoch).expect("the next epoch is always non-zero"),
        );
        self.epoch_change_driver.schedule_process_subsidies();
    }

    #[tracing::instrument(skip_all)]
    async fn process_epoch_change_start_event(
        &self,
        blob_event_processor: &BlobEventProcessor,
        event_handle: EventHandle,
        event: &EpochChangeStart,
    ) -> anyhow::Result<()> {
        // There shouldn't be an epoch change event for the genesis epoch.
        assert!(event.epoch != GENESIS_EPOCH);

        // Fire a warning if the foreground portion of the handler is still running after the
        // threshold, and keep warning every threshold interval for as long as it runs. The task is
        // aborted when `_warn_guard` is dropped on any return path, so warnings only reach the log
        // while we are actually exceeding the budget.
        let warn_handle = tokio::spawn({
            let epoch = event.epoch;
            let start = Instant::now();
            async move {
                loop {
                    tokio::time::sleep(EPOCH_CHANGE_START_SLOW_THRESHOLD).await;
                    tracing::warn!(
                        walrus.epoch = epoch,
                        threshold_secs = EPOCH_CHANGE_START_SLOW_THRESHOLD.as_secs_f64(),
                        elapsed_secs = start.elapsed().as_secs_f64(),
                        "processing epoch change start is taking longer than expected",
                    );
                }
            }
        });
        let _warn_guard = AbortOnDrop(warn_handle);

        // Irrespective of whether we are in this epoch, we can cancel any scheduled calls to change
        // to or end voting for the epoch identified by the event, as we're already in that epoch.
        self.epoch_change_driver
            .cancel_scheduled_voting_end(event.epoch);
        self.epoch_change_driver
            .cancel_scheduled_epoch_change_initiation(event.epoch);

        // Here we need to wait for the previous shard removal to finish so that for the case
        // where same shard is moved in again, we don't have shard removal and move-in running
        // concurrently.
        //
        // Note that we expect this call to finish quickly because removing RocksDb column
        // families is supposed to be fast, and we have an entire epoch duration to do so. By
        // the time next epoch starts, the shard removal task should have completed.
        self.start_epoch_change_finisher
            .wait_until_previous_task_done()
            .await;

        // Before processing the epoch change start event, we need to wait for all the events in
        // the current epoch to be processed (note that this does not include waiting for all
        // pending blob syncs to finish). This is to make sure that the node is in a consistent
        // state before processing the epoch change start event.
        blob_event_processor
            .get_pending_event_counter()
            .wait_for_all_events_to_be_processed()
            .await;

        if let Some(c) = self.config_synchronizer.as_ref() {
            c.sync_node_params().await?;
        }

        // Run GC phase 1 (blob info cleanup) before the rest of the epoch-change work.
        // Phase 1 only iterates global blob-info CFs against `event.epoch`; it does not depend
        // on the upcoming committee or shard transitions. Running it first means a phase-1
        // error stops processing before any committee/shard state changes, and shard removal
        // (spawned by `execute_epoch_change` as part of the finisher task) cannot contend with
        // phase 1's disk traffic on the same RocksDB instance.
        self.start_garbage_collection_task(event.epoch).await?;

        // Compute this before the handle is moved into `execute_epoch_change`, whose finisher marks
        // the event complete in the background: checking afterwards could misclassify first-time
        // processing as reprocessing.
        let event_index = event_handle.index();
        let node_is_reprocessing_events =
            self.inner.storage.get_latest_handled_event_index()? >= event_index;

        // Serialize only when enabled, not reprocessing, and not catching up (a catching-up node's
        // blob info tables are not at the clean cross-node boundary). The node-status DB lookup
        // runs only after the other two checks short-circuit.
        let should_serialize = self.inner.blob_info_snapshot_config.enabled
            && !node_is_reprocessing_events
            && !self.inner.storage.node_status()?.is_catching_up();

        // Serialize after GC phase 1 has settled the tables and before `execute_epoch_change`
        // spawns the finisher that marks the event complete (so a crash before completion replays
        // and re-creates it). Errors are logged and counted, never failing epoch processing.
        //
        // TODO(WAL-1250): this only writes the snapshot to local disk. Publishing and certifying it
        // on-chain (encode, store the node's own slivers, attest, track to certified) is future
        // work.
        if should_serialize
            && let Err(error) = blob_info_snapshot_writer::serialize_snapshot_at_epoch_boundary(
                self.inner.clone(),
                event.epoch,
                // Mirror what the node persists after completing this event: the
                // `EpochChangeStart`'s id, and its index + 1 as the next index to process.
                EventStreamCursor::new(Some(event_handle.event_id()), event_index + 1),
            )
            .await
        {
            self.inner.metrics.blob_info_snapshot_error_total.inc();
            tracing::warn!(
                ?error,
                walrus.epoch = event.epoch,
                "failed to serialize the blob info snapshot in-process at the epoch boundary"
            );
        }

        // Enter the epoch-change critical section (see [`EpochChangeCriticalSection`]; it must
        // be entered before the shard map lock), then lock the read access to the shard map
        // until all the new shards are created.
        let critical_section_guard = self.inner.epoch_change_critical_section.enter().await;
        let shard_map_lock = self.inner.storage.lock_shards().await;

        // Now the general tasks around epoch change are done. Next, entering epoch change logic
        // to bring the node state to the next epoch. `execute_epoch_change` ends by spawning
        // the finisher task (shard removal + `epoch_sync_done` + `mark_as_complete`), so the
        // finisher is guaranteed to fire only after phase 1 succeeded.
        self.execute_epoch_change(event_handle, event, shard_map_lock, critical_section_guard)
            .await?;

        // Update the latest event epoch to the new epoch. Now, blob syncs will use this epoch to
        // check for shard ownership.
        self.inner
            .latest_event_epoch_sender
            .send(Some(event.epoch))?;

        // Schedule post-epoch-change subsidies to distribute usage-independent subsidies
        // for the epoch that just ended.
        self.epoch_change_driver
            .schedule_post_epoch_change_subsidies();

        // Schedule the storage node consistency check after garbage collection has settled the
        // aggregate blob info table. The iterator's `is_certified` filter relies on counters
        // that GC decrements for newly-expired deletable and pooled blobs, so the digest
        // depends on whether GC has run. Taking the snapshot after GC keeps the digest
        // deterministic across nodes and across replay of `EpochChangeStart` after a crash.
        //
        // Skipped when:
        // - consistency check is disabled
        // - node is reprocessing events (blob info table should not be affected by future
        //   events)
        if self.inner.consistency_check_config.enable_consistency_check
            && !node_is_reprocessing_events
            && let Err(err) = consistency_check::schedule_background_consistency_check(
                self.inner.clone(),
                self.blob_sync_handler.clone(),
                event.epoch,
            )
            .await
        {
            tracing::warn!(
                ?err,
                walrus.epoch = event.epoch,
                "failed to schedule background blob info consistency check"
            );
        }

        Ok(())
    }

    /// Storage node execution of the epoch change start event, to bring the node state to the next
    /// epoch.
    ///
    /// `critical_section_guard` is the [`EpochChangeCriticalSection`] guard, entered by the
    /// caller before the shard map lock; the recovery-related paths below hold it across their
    /// node status transitions.
    async fn execute_epoch_change(
        &self,
        event_handle: EventHandle,
        event: &EpochChangeStart,
        shard_map_lock: StorageShardLock,
        critical_section_guard: tokio::sync::MutexGuard<'_, ()>,
    ) -> anyhow::Result<()> {
        if self.inner.storage.node_status()?.is_catching_up() {
            self.execute_epoch_change_while_catching_up(
                event_handle,
                event,
                shard_map_lock,
                critical_section_guard,
            )
            .await?;
        } else {
            match self.begin_committee_change(event.epoch).await? {
                BeginCommitteeChangeAction::ExecuteEpochChange => {
                    self.execute_epoch_change_when_node_is_in_sync(
                        event_handle,
                        event,
                        shard_map_lock,
                        critical_section_guard,
                    )
                    .await?;
                }
                BeginCommitteeChangeAction::SkipEpochChange => {
                    event_handle.mark_as_complete();
                    return Ok(());
                }
                BeginCommitteeChangeAction::EnterRecoveryMode => {
                    tracing::info!("storage node entering recovery mode during epoch change start");
                    sui_macros::fail_point!("fail-point-enter-recovery-mode");

                    self.enter_recovery_mode_in_critical_section().await?;

                    self.execute_epoch_change_while_catching_up(
                        event_handle,
                        event,
                        shard_map_lock,
                        critical_section_guard,
                    )
                    .await?;
                }
            };
        }

        Ok(())
    }

    /// Processes the epoch change start event while the node is in
    /// [`RecoveryCatchUp`][NodeStatus::RecoveryCatchUp] or
    /// [`RecoveryCatchUpWithIncompleteHistory`][NodeStatus::RecoveryCatchUpWithIncompleteHistory]
    /// state.
    async fn execute_epoch_change_while_catching_up(
        &self,
        event_handle: EventHandle,
        event: &EpochChangeStart,
        shard_map_lock: StorageShardLock,
        critical_section_guard: tokio::sync::MutexGuard<'_, ()>,
    ) -> anyhow::Result<()> {
        self.inner
            .committee_service
            .begin_committee_change_to_latest_committee()
            .await?;

        // For blobs that are expired in the new epoch, sends a notification to all the tasks
        // that may be affected by the blob expiration.
        self.inner
            .blob_retirement_notifier
            .epoch_change_notify_all_pending_blob_retirement(self.inner.clone())?;

        if event.epoch < self.inner.current_committee_epoch() {
            // We have not caught up to the latest epoch yet, so we can skip the event.
            event_handle.mark_as_complete();
            return Ok(());
        }

        tracing::info!(walrus.epoch = %event.epoch, "catching-up node reaches the current epoch");

        let active_committees = self.inner.committee_service.active_committees();
        if !active_committees
            .current_committee()
            .contains(self.inner.public_key())
        {
            tracing::info!("node is not in the current committee, set node status to 'Standby'");
            self.inner.set_node_status(NodeStatus::Standby)?;
            // A standby node makes no epoch-sync claim and runs no status-changing
            // long-running tasks: invalidate any unconsumed token or instruction.
            self.shard_sync_handler.clear_epoch_sync_done_token();
            self.shard_sync_handler.clear_metadata_recovery_completion();
            self.node_recovery_handler.clear_completion_instruction();
            self.inner.publish_epoch_sync_goal(EpochSyncGoal {
                // Both generations are assigned by the publisher.
                generation: 0,
                sync_baseline_generation: 0,
                epoch: event.epoch,
                catching_up: false,
                membership: goal::Membership::NotMember,
                owned_shards: Vec::new(),
                shards_to_fill: None,
            });
            event_handle.mark_as_complete();
            return Ok(());
        }

        if !active_committees
            .previous_committee()
            .is_some_and(|c| c.contains(self.inner.public_key()))
        {
            tracing::info!("node just became a new committee member, process shard changes");
            // This node just became a new committee member. Process shard changes as a new
            // committee member; this path performs no recovery-related status transitions, so
            // the status guard is not needed.
            drop(critical_section_guard);
            self.process_shard_changes_in_new_epoch_while_node_is_in_sync(
                event_handle,
                event,
                true,
                shard_map_lock,
            )
            .await?;
        } else {
            tracing::info!("start node recovery to catch up to the latest epoch");
            // This node is a past and current committee member. Start node recovery to catch up
            // to the latest epoch.
            self.process_shard_changes_in_new_epoch_and_start_node_recovery(
                event_handle,
                event,
                shard_map_lock,
                critical_section_guard,
            )
            .await?;
        }

        Ok(())
    }

    /// Executes the epoch change logic when the node is up-to-date with the epoch and event
    /// processing.
    async fn execute_epoch_change_when_node_is_in_sync(
        &self,
        event_handle: EventHandle,
        event: &EpochChangeStart,
        shard_map_lock: StorageShardLock,
        critical_section_guard: tokio::sync::MutexGuard<'_, ()>,
    ) -> anyhow::Result<()> {
        // For blobs that are expired in the new epoch, sends a notification to all the tasks
        // that may be affected by the blob expiration.
        self.inner
            .blob_retirement_notifier
            .epoch_change_notify_all_pending_blob_retirement(self.inner.clone())?;

        // Cancel all blob syncs for blobs that are expired in the *current epoch*.
        self.blob_sync_handler
            .cancel_all_expired_syncs_and_mark_events_completed()
            .await?;

        let is_in_current_committee = self
            .inner
            .committee_service
            .active_committees()
            .current_committee()
            .contains(self.inner.public_key());
        let is_new_node_joining_committee =
            self.inner.storage.node_status()? == NodeStatus::Standby && is_in_current_committee;

        if !is_in_current_committee {
            // The reason we set the node status to Standby here is that the node is not in the
            // current committee, and therefore from this epoch, it won't sync any blob
            // metadata. In the case it becomes committee member again, it needs to sync blob
            // metadata again.
            self.inner.set_node_status(NodeStatus::Standby)?;
            // A standby node makes no epoch-sync claim and runs no status-changing
            // long-running tasks: invalidate any unconsumed token or instruction.
            self.shard_sync_handler.clear_epoch_sync_done_token();
            self.shard_sync_handler.clear_metadata_recovery_completion();
            self.node_recovery_handler.clear_completion_instruction();
        }

        if is_new_node_joining_committee {
            tracing::info!(
                "node just became a committee member; changing status from 'Standby' to 'Active' \
                and processing shard changes"
            );
        }

        if let NodeStatus::RecoveryInProgress(recovering_epoch) =
            self.inner.storage.node_status()?
        {
            // If the node is already in recovery mode, we advance the recovery target to the
            // latest epoch, so that the node always recovers to the latest epoch. Since the node
            // is up-to-date with events, newly gained shards are synced from their previous
            // owners instead of being filled by blob recovery, and the running recovery task
            // keeps its progress instead of being restarted.
            tracing::info!(
                "node is currently recovering to epoch {recovering_epoch}, advancing the \
                recovery target to the latest epoch {}",
                event.epoch
            );
            self.process_shard_changes_in_new_epoch_while_recovering(
                event_handle,
                event,
                shard_map_lock,
                critical_section_guard,
            )
            .await
        } else {
            // This path performs no recovery-related status transitions, so the status guard is
            // not needed.
            drop(critical_section_guard);
            self.process_shard_changes_in_new_epoch_while_node_is_in_sync(
                event_handle,
                event,
                is_new_node_joining_committee,
                shard_map_lock,
            )
            .await
        }
    }

    /// Processes the shard changes in the new epoch and starts the node recovery process.
    ///
    /// As all functions that are passed an [`EventHandle`], this is responsible for marking the
    /// event as completed.
    async fn process_shard_changes_in_new_epoch_and_start_node_recovery(
        &self,
        event_handle: EventHandle,
        event: &EpochChangeStart,
        shard_map_lock: StorageShardLock,
        critical_section_guard: tokio::sync::MutexGuard<'_, ()>,
    ) -> anyhow::Result<()> {
        // A recovery run from before the node started catching up may still be in flight. Such
        // a run only scanned blobs certified before its own start epoch, and blob certified
        // events were skipped while catching up, so it must not complete the recovery target
        // written below. Its completion attempt parks at the epoch-change critical section
        // (held by the caller), where it finds itself superseded: the goal published below
        // carries a newer generation than the one the run is bound to.
        self.inner
            .set_node_status(NodeStatus::RecoveryInProgress(event.epoch))?;

        // The recovery task owns the `epoch_sync_done` attestation: authorize its completion
        // with an instruction bundling the status flip to `Active` with the attestation token,
        // so the two cannot diverge. Minted inside the critical section, atomically with the
        // target write above; any token held by shard sync is invalidated.
        self.shard_sync_handler.clear_epoch_sync_done_token();
        self.node_recovery_handler
            .set_completion_instruction(CompletionInstruction::new(
                NodeStatus::Active,
                Some(EpochSyncDoneToken::new_for_epoch(event.epoch)),
            ));

        let public_key = self.inner.public_key();
        let storage = &self.inner.storage;
        let committees = self.inner.committee_service.active_committees();
        let shard_diff_calculator =
            ShardDiffCalculator::new(&committees, public_key, shard_map_lock.existing_shards());

        // Since the node is doing a full recovery, its local shards may be out of sync with the
        // contract for multiple epochs. Here we need to make sure that all the shards that is
        // assigned to the node in the latest epoch are created.
        //
        // Note that the shard_map_lock will be unlocked after this function returns.
        self.inner
            .create_storage_for_shards_in_background(
                shard_diff_calculator.all_owned_shards().to_vec(),
                shard_map_lock,
            )
            .await?;

        // Given that the storage node is severely lagging, the node may contain shards in outdated
        // status. We need to set the status of all currently owned shards to `Active` despite
        // their current status. Node recovery will recover all the missing certified blobs in these
        // shards in a crash-tolerant manner.
        // Note that node recovery can only start if the event epoch matches the latest epoch.
        for shard in self.inner.owned_shards_at_latest_epoch() {
            storage
                .shard_storage(shard)
                .await
                .expect("we just create all storage, it must exist")
                .force_set_active_status()
                .await?;
        }

        // For shards that just moved out, we need to lock them to not store more data in them.
        for shard in shard_diff_calculator.shards_to_lock() {
            if let Some(shard_storage) = self.inner.storage.shard_storage(*shard).await {
                shard_storage
                    .lock_shard_for_epoch_change()
                    .await
                    .context("failed to lock shard")?;
            }
        }

        // Publish the goal for the new epoch. The permanent recovery service observes the
        // persisted `RecoveryInProgress` target (written above) and this publication, and
        // starts a recovery run bound to it: the run initiates blob sync for all certified
        // blobs tracked so far, after which the node has all the shards and blobs it should
        // have. Any previous run is superseded by the publication.
        self.inner.publish_epoch_sync_goal(EpochSyncGoal {
            // Both generations are assigned by the publisher.
            generation: 0,
            sync_baseline_generation: 0,
            epoch: event.epoch,
            catching_up: false,
            membership: goal::Membership::from_committee_presence(
                committees.current_committee().contains(public_key),
                committees
                    .previous_committee()
                    .is_some_and(|committee| committee.contains(public_key)),
            ),
            owned_shards: self.inner.owned_shards_at_latest_epoch(),
            shards_to_fill: None,
        });

        drop(critical_section_guard);

        // Last but not least, we need to remove any shards that are no longer owned by the node.
        let shards_to_remove = shard_diff_calculator.shards_to_remove();
        if !shards_to_remove.is_empty() {
            self.start_epoch_change_finisher
                .start_finish_epoch_change_tasks(
                    event_handle,
                    event,
                    shard_diff_calculator.shards_to_remove().to_vec(),
                    committees,
                    None,
                );
        } else {
            event_handle.mark_as_complete();
        }

        Ok(())
    }

    /// Processes the shard changes in the new epoch while node recovery is already in progress.
    ///
    /// In contrast to [`Self::process_shard_changes_in_new_epoch_and_start_node_recovery`], which
    /// handles a node that has lost track of the previous epoch's shard assignment, the node here
    /// is up-to-date with events, so newly gained shards are filled using shard sync from their
    /// previous owners instead of per-blob recovery. The running node recovery task is not
    /// restarted: it waits for these shard syncs to finish before recovering blobs, and attests
    /// epoch sync done for the advanced recovery target once both are complete.
    ///
    /// As all functions that are passed an [`EventHandle`], this is responsible for marking the
    /// event as completed.
    async fn process_shard_changes_in_new_epoch_while_recovering(
        &self,
        event_handle: EventHandle,
        event: &EpochChangeStart,
        shard_map_lock: StorageShardLock,
        critical_section_guard: tokio::sync::MutexGuard<'_, ()>,
    ) -> anyhow::Result<()> {
        // Advancing the recovery target, publishing the goal for the gained shards' syncs, and
        // locking the shards that moved away all happen inside the epoch-change critical
        // section (the guard is acquired by the caller, before the shard map lock): a
        // completing recovery run either observes the full transition, or completes entirely
        // before it (in which case the publication below supersedes nothing and the recovery
        // service starts a fresh run toward the advanced target).

        // Advance the recovery target so that the recovery task attests epoch sync done for the
        // latest epoch; a stale attestation would be dropped by the contract service.
        self.inner
            .set_node_status(NodeStatus::RecoveryInProgress(event.epoch))?;

        // Replace the recovery task's completion instruction with one for the advanced target:
        // the token for the old epoch is discarded with the replaced instruction. Minted inside
        // the critical section, atomically with the target write above.
        self.shard_sync_handler.clear_epoch_sync_done_token();
        self.node_recovery_handler
            .set_completion_instruction(CompletionInstruction::new(
                NodeStatus::Active,
                Some(EpochSyncDoneToken::new_for_epoch(event.epoch)),
            ));

        sui_macros::fail_point!("fail_point_shard_changes_in_new_epoch_while_recovering");

        let public_key = self.inner.public_key();
        let committees = self.inner.committee_service.active_committees();
        let shard_diff_calculator =
            ShardDiffCalculator::new(&committees, public_key, shard_map_lock.existing_shards());

        let shards_gained = shard_diff_calculator.gained_shards_from_prev_epoch();
        tracing::info!(
            ?shards_gained,
            "processing shard changes in new epoch while node recovery is in progress"
        );

        // Note that the shard_map_lock will be unlocked after this function returns.
        self.create_new_shards(shard_map_lock, shards_gained, &committees, false)
            .await?;

        // For shards that just moved out, we need to lock them to not store more data in them.
        for shard_id in shard_diff_calculator.shards_to_lock() {
            let Some(shard_storage) = self.inner.storage.shard_storage(*shard_id).await else {
                tracing::info!("skipping lost shard during epoch change as it is not stored");
                continue;
            };
            tracing::info!(
                walrus.shard_index = %shard_id,
                epoch = event.epoch,
                "locking shard for epoch change"
            );
            shard_storage
                .lock_shard_for_epoch_change()
                .await
                .context("failed to lock shard")?;
        }

        // Publish the goal for the advanced target: the shard-sync reconciler reacts by
        // starting the syncs for the gained shards, and the recovery service supersedes its
        // running recovery run with one bound to the advanced target (per-blob progress is
        // persisted, so only the scan is repeated). The run waits for the shard syncs to
        // finish before recovering blobs, and attests epoch sync done on completion via its
        // completion instruction.
        self.inner.publish_epoch_sync_goal(EpochSyncGoal {
            // Both generations are assigned by the publisher.
            generation: 0,
            sync_baseline_generation: 0,
            epoch: event.epoch,
            catching_up: false,
            membership: goal::Membership::from_committee_presence(
                committees.current_committee().contains(public_key),
                committees
                    .previous_committee()
                    .is_some_and(|committee| committee.contains(public_key)),
            ),
            owned_shards: self.inner.owned_shards_at_latest_epoch(),
            shards_to_fill: (!shards_gained.is_empty()).then(|| NewShards {
                shards: shards_gained.to_vec(),
                fill: ShardFill::ShardSync,
            }),
        });

        drop(critical_section_guard);

        // The recovery service is in charge of attesting epoch sync done, so the finisher is
        // never handed the token on this path.
        let shards_to_remove = shard_diff_calculator.shards_to_remove();
        if !shards_to_remove.is_empty() {
            self.start_epoch_change_finisher
                .start_finish_epoch_change_tasks(
                    event_handle,
                    event,
                    shards_to_remove.to_vec(),
                    committees,
                    None,
                );
        } else {
            event_handle.mark_as_complete();
        }

        Ok(())
    }

    /// Initiates a committee transition to a new epoch. Upon the return of this function, the
    /// latest committee on chain is updated to the new node.
    ///
    /// Returns the action to execute epoch change based on the result of committee service,
    /// including possible actions to enter recovery mode due to the node being severely lagging.
    #[tracing::instrument(skip_all)]
    async fn begin_committee_change(
        &self,
        epoch: Epoch,
    ) -> Result<BeginCommitteeChangeAction, BeginCommitteeChangeError> {
        match self
            .inner
            .committee_service
            .begin_committee_change(epoch)
            .await
        {
            Ok(()) => {
                tracing::info!(
                    walrus.epoch = epoch,
                    "successfully started a transition to a new epoch"
                );
                Ok(BeginCommitteeChangeAction::ExecuteEpochChange)
            }
            Err(BeginCommitteeChangeError::EpochIsTheSameAsCurrent) => {
                tracing::info!(
                    walrus.epoch = epoch,
                    "epoch change event was for the epoch we already fetched the committee info, \
                    directly executing epoch change"
                );
                Ok(BeginCommitteeChangeAction::ExecuteEpochChange)
            }
            Err(BeginCommitteeChangeError::ChangeAlreadyInProgress) => {
                // TODO(WAL-479): can this condition actually happen? It seems that the only case
                // this could happen is when the node calls begin_committee_change() multiple times
                // on the same epoch in the same life time of the storage node. This is not expected
                // and indicates software bug (convert this to debug assertion?).
                tracing::info!(
                    walrus.epoch = epoch,
                    committee_epoch = self.inner.committee_service.get_epoch(),
                    "epoch change is already in progress, do not need to re-execute epoch change"
                );
                Ok(BeginCommitteeChangeAction::SkipEpochChange)
            }
            Err(BeginCommitteeChangeError::EpochIsLess {
                latest_epoch,
                requested_epoch,
            }) => {
                debug_assert!(requested_epoch < latest_epoch);
                // We are processing a backlog of events. Since the committee service has a
                // more recent committee. In this situation, we have already lost the information
                // and the shard assignment of the previous epoch relative to `event.epoch`, the
                // node cannot execute the epoch change. Therefore, the node needs to enter recovery
                // mode to catch up to the latest epoch as quickly as possible.
                tracing::warn!(
                    ?latest_epoch,
                    ?requested_epoch,
                    "epoch change requested for an older epoch than the latest epoch, this means \
                    the node is severely lagging behind, and will enter recovery mode"
                );
                Ok(BeginCommitteeChangeAction::EnterRecoveryMode)
            }
            Err(error) => {
                tracing::error!(?error, "failed to initiate a transition to the new epoch");
                Err(error)
            }
        }
    }

    /// Processes all the shard changes in the new epoch, and finishes the epoch change.
    #[tracing::instrument(skip_all)]
    async fn process_shard_changes_in_new_epoch_while_node_is_in_sync(
        &self,
        event_handle: EventHandle,
        event: &EpochChangeStart,
        new_node_joining_committee: bool,
        shard_map_lock: StorageShardLock,
    ) -> anyhow::Result<()> {
        let public_key = self.inner.public_key();
        let storage = &self.inner.storage;
        let committees = self.inner.committee_service.active_committees();
        assert!(event.epoch <= committees.epoch());

        let shard_diff_calculator =
            ShardDiffCalculator::new(&committees, public_key, shard_map_lock.existing_shards());

        if cfg!(msim) {
            // In simtest, print out the shard migration information for easier debugging.
            tracing::info!("EpochChangeStart shard diffs: {:?}", shard_diff_calculator);
        }

        let shards_gained = shard_diff_calculator.gained_shards_from_prev_epoch();

        self.create_new_shards(
            shard_map_lock,
            shards_gained,
            &committees,
            new_node_joining_committee,
        )
        .await?;

        for shard_id in shard_diff_calculator.shards_to_lock() {
            let Some(shard_storage) = storage.shard_storage(*shard_id).await else {
                tracing::info!("skipping lost shard during epoch change as it is not stored");
                continue;
            };
            tracing::info!(
                walrus.shard_index = %shard_id,
                epoch = event.epoch,
                "locking shard for epoch change"
            );
            shard_storage
                .lock_shard_for_epoch_change()
                .await
                .context("failed to lock shard")?;
        }

        // Route the `epoch_sync_done` attestation: mint the token for the new epoch and
        // register it with the shards whose syncs must complete before it may be consumed —
        // the sync completion that finishes the registration attests. Routed after the lost
        // shards are locked (an attestation must never fire while the node still accepts
        // writes for shards it lost) and before the goal is published below (a sync started
        // by the reconciler must find its registered token). With no shards to sync, the
        // finisher attests directly (it skips the attestation if the node is not in the
        // committee).
        let token = EpochSyncDoneToken::new_for_epoch(event.epoch);
        self.node_recovery_handler.clear_completion_instruction();
        let mut finisher_attestation = None;
        if shards_gained.is_empty() {
            self.shard_sync_handler.clear_epoch_sync_done_token();
            finisher_attestation = Some(token);
        } else {
            self.shard_sync_handler
                .set_epoch_sync_done_token(token, shards_gained.to_vec());
        }

        // Publish the goal for this transition: the shard-sync reconciler reacts by starting
        // the syncs for the gained shards.
        self.inner.publish_epoch_sync_goal(EpochSyncGoal {
            // Both generations are assigned by the publisher.
            generation: 0,
            sync_baseline_generation: 0,
            epoch: event.epoch,
            catching_up: false,
            membership: goal::Membership::from_committee_presence(
                committees.current_committee().contains(public_key),
                committees
                    .previous_committee()
                    .is_some_and(|committee| committee.contains(public_key)),
            ),
            owned_shards: shard_diff_calculator.all_owned_shards().to_vec(),
            shards_to_fill: (!shards_gained.is_empty()).then(|| NewShards {
                shards: shards_gained.to_vec(),
                fill: ShardFill::ShardSync,
            }),
        });

        self.start_epoch_change_finisher
            .start_finish_epoch_change_tasks(
                event_handle,
                event,
                shard_diff_calculator.shards_to_remove().to_vec(),
                committees,
                finisher_attestation,
            );

        Ok(())
    }

    /// Creates the shards that are newly assigned to the node. Their contents are brought up
    /// to date by the shard-sync reconciler once the goal for this transition is published.
    /// Note that the shard_map_lock will be unlocked after this function returns.
    async fn create_new_shards(
        &self,
        shard_map_lock: StorageShardLock,
        shards_gained: &[ShardIndex],
        committees: &ActiveCommittees,
        new_node_joining_committee: bool,
    ) -> anyhow::Result<()> {
        let public_key = self.inner.public_key();
        if !shards_gained.is_empty() {
            assert!(committees.current_committee().contains(public_key));

            self.inner
                .create_storage_for_shards_in_background(shards_gained.to_vec(), shard_map_lock)
                .await?;

            if new_node_joining_committee {
                // Set node status to RecoverMetadata to sync metadata for the new shards.
                // Note that this must be set before marking the event as complete, so that
                // node crashing before setting the status will always be setting the status
                // again when re-processing the EpochChangeStart event.
                //
                // It's also important to set RecoverMetadata status after creating storage for
                // the new shards. Restarting seeing RecoverMetadata status will assume all the
                // shards are created.
                self.inner.set_node_status(NodeStatus::RecoverMetadata)?;
                // Authorize the metadata-recovery task to flip the node to `Active` when it
                // finishes. The instruction must be in place before the shard syncs start below
                // (a quickly finishing task must not miss it); a later transition that
                // supersedes the metadata recovery (dropping out of the committee, entering
                // recovery mode) revokes it by clearing the slot.
                self.shard_sync_handler.set_metadata_recovery_completion(
                    CompletionInstruction::new(NodeStatus::Active, None),
                );
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn process_epoch_change_done_event(&self, event: &EpochChangeDone) -> anyhow::Result<()> {
        match self
            .inner
            .committee_service
            .end_committee_change(event.epoch)
        {
            Ok(()) => tracing::info!(
                walrus.epoch = event.epoch,
                "successfully ended the transition to the new epoch"
            ),
            // This likely means that the committee was fetched (for example on startup) and we
            // are not processing the event that would have notified us that the epoch was
            // changing.
            Err(EndCommitteeChangeError::EpochChangeAlreadyDone) => tracing::info!(
                walrus.epoch = event.epoch,
                "the committee had already transitioned to the new epoch"
            ),
            Err(EndCommitteeChangeError::ProvidedEpochIsInThePast { .. }) => {
                // We are ending a change to an epoch that we have already advanced beyond. This is
                // likely due to processing a backlog of events and can be ignored.
                tracing::debug!(
                    walrus.epoch = event.epoch,
                    "skipping epoch change event that is in the past"
                );
                return Ok(());
            }
            Err(error @ EndCommitteeChangeError::ProvidedEpochIsInTheFuture { .. }) => {
                tracing::error!(
                    ?error,
                    "our committee service is lagging behind the events being processed, which \
                    should not happen"
                );
                return Err(error.into());
            }
        }

        self.epoch_change_driver.schedule_voting_end(
            NonZero::new(event.epoch + 1).expect("incremented value is non-zero"),
        );

        Ok(())
    }

    /// Enters recovery mode from outside the epoch-change flow (the lag-detection path).
    /// This function should only be called when the node is lagging behind.
    ///
    /// The transition runs inside the epoch-change critical section, so that revoking the
    /// long-running tasks' completion instructions is atomic with the status change: a task
    /// that has already taken its instruction (inside the critical section) applies it before
    /// this transition runs, and a task that has not finds the slot empty afterwards.
    pub(super) async fn enter_recovery_mode(&self) -> anyhow::Result<()> {
        let _critical_section_guard = self.inner.epoch_change_critical_section.enter().await;
        self.enter_recovery_mode_in_critical_section().await
    }

    /// Enters recovery mode. The caller must hold the epoch-change critical section (the
    /// epoch-change flow enters it for the whole transition; other callers use
    /// [`Self::enter_recovery_mode`]).
    async fn enter_recovery_mode_in_critical_section(&self) -> anyhow::Result<()> {
        self.inner.set_node_status(NodeStatus::RecoveryCatchUp)?;

        // Entering recovery supersedes the pending completions of long-running tasks: neither a
        // still-running metadata recovery nor a still-running node recovery task may transition
        // the node away from `RecoveryCatchUp` when it finishes.
        self.shard_sync_handler.quiesce_all_syncs().await;
        self.shard_sync_handler.clear_epoch_sync_done_token();
        self.shard_sync_handler.clear_metadata_recovery_completion();
        self.node_recovery_handler.clear_completion_instruction();

        // While catching up, the node's view of its shard assignment is not authoritative:
        // publish a catching-up goal so the sync services hold off on new work until the node
        // reaches the latest epoch.
        self.inner.publish_epoch_sync_goal(EpochSyncGoal {
            // Both generations are assigned by the publisher.
            generation: 0,
            sync_baseline_generation: 0,
            epoch: self.inner.current_committee_epoch(),
            catching_up: true,
            membership: goal::Membership::NotMember,
            owned_shards: Vec::new(),
            shards_to_fill: None,
        });

        // Now the node is entering recovery mode, we need to cancel all the blob syncs
        // that are in progress, since the node is lagging behind, and we don't have
        // any information about the shards that the node should own.
        //
        // The node now will try to only process blob info upon receiving a blob event
        // and blob recovery will be triggered when the node is in the latest epoch.
        self.blob_sync_handler
            .cancel_all_syncs_and_mark_events_completed()
            .await?;

        Ok(())
    }
}
