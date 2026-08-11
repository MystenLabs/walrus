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
use completion::BackgroundSyncTaskCompletionInstruction;
use goal::EpochChangeSyncAndRecoveryInfo;

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

        // Now the general tasks around epoch change are done. Next, entering epoch change logic
        // to bring the node state to the next epoch. `execute_epoch_change` ends by spawning
        // the finisher task (shard removal + `epoch_sync_done` + `mark_as_complete`), so the
        // finisher is guaranteed to fire only after phase 1 succeeded.
        self.epoch_change_executor
            .execute_epoch_change(event_handle, event)
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

    /// Enters recovery mode. This function should only be called when the node is lagging
    /// behind.
    pub(super) async fn enter_recovery_mode(&self) -> anyhow::Result<()> {
        self.epoch_change_executor.enter_recovery_mode().await
    }
}

/// Executes the storage node's epoch-change transitions.
///
/// The executor owns the epoch-change orchestration — reconciling the committee state
/// ([`Self::reconcile_committee_for_epoch_change`]), planning ([`plan::plan_epoch_change`]),
/// and applying the plan ([`Self::apply_epoch_change_plan`]) — and holds exactly the handles
/// the transition needs. The `EpochChangeStart` handler's front matter (waiting for pending
/// events, garbage collection, snapshots) stays with [`StorageNode`], which delegates the
/// transition itself to this type.
#[derive(Debug, Clone)]
pub(crate) struct EpochChangeExecutor {
    inner: Arc<StorageNodeInner>,
    blob_sync_handler: Arc<BlobSyncHandler>,
    shard_sync_handler: Arc<ShardSyncHandler>,
    node_recovery_handler: Arc<NodeRecoveryHandler>,
    start_epoch_change_finisher: Arc<StartEpochChangeFinisher>,
}

impl EpochChangeExecutor {
    pub(super) fn new(
        inner: Arc<StorageNodeInner>,
        blob_sync_handler: Arc<BlobSyncHandler>,
        shard_sync_handler: Arc<ShardSyncHandler>,
        node_recovery_handler: Arc<NodeRecoveryHandler>,
        start_epoch_change_finisher: Arc<StartEpochChangeFinisher>,
    ) -> Self {
        Self {
            inner,
            blob_sync_handler,
            shard_sync_handler,
            node_recovery_handler,
            start_epoch_change_finisher,
        }
    }

    /// Publishes the startup sync-and-recovery info, derived from persisted state and the
    /// fetched committees, and re-mints the commit-protocol state — the `epoch_sync_done`
    /// attestation token and the completion instructions — that the previous run held in
    /// memory. Together with the reconciler's first pass (which rebuilds the sync work from
    /// persisted shard statuses), this makes startup just another info publication: there is
    /// no dedicated restart path.
    ///
    /// Must be called before the sync services are spawned.
    pub(super) async fn publish_startup_sync_and_recovery_info(&self) -> anyhow::Result<()> {
        let committees = self.inner.committee_service.active_committees();
        let public_key = self.inner.public_key();
        let in_current_committee = committees.current_committee().contains(public_key);
        let in_previous_committee = committees
            .previous_committee()
            .is_some_and(|committee| committee.contains(public_key));
        let membership = goal::MembershipAtEpochChange::from_committee_presence(
            in_current_committee,
            in_previous_committee,
        );
        let node_status = self.inner.storage.node_status()?;
        let owned_shards = self.inner.owned_shards_at_latest_epoch();
        let epoch = committees.epoch();

        match &node_status {
            NodeStatus::RecoveryInProgress(target_epoch) if membership.is_member() => {
                // The node restarted while recovering: node recovery owns the completion. Only
                // minted while the node is still a committee member: a node that was dropped
                // while recovering must not complete to `Active` (with zero owned shards the
                // run would complete trivially and send a stale attestation); without an
                // instruction the run finishes without touching the node status, and the
                // replayed epoch change that dropped the node moves it to `Standby`.
                self.node_recovery_handler.set_completion_instruction(
                    BackgroundSyncTaskCompletionInstruction::new(
                        NodeStatus::Active,
                        Some(EpochSyncDoneToken::new_for_epoch(*target_epoch)),
                    ),
                );
            }
            status if membership.is_member() && !status.is_catching_up() => {
                if node_status == NodeStatus::RecoverMetadata {
                    self.shard_sync_handler.set_metadata_recovery_completion(
                        BackgroundSyncTaskCompletionInstruction::new(NodeStatus::Active, None),
                    );
                }
                // Register the attestation with every owned shard that is not yet `Active` —
                // the work the reconciler rebuilds. With nothing pending (and no outstanding
                // metadata recovery), the attestation for this epoch was already sent before
                // the restart, or is re-sent when the epoch-change event is re-processed.
                let mut pending_shards = Vec::new();
                for shard in &owned_shards {
                    let is_active = match self.inner.storage.shard_storage(*shard).await {
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
                    self.shard_sync_handler.set_epoch_sync_done_token(
                        EpochSyncDoneToken::new_for_epoch(epoch),
                        pending_shards,
                    );
                }
            }
            _ => {}
        }

        self.inner.publish_epoch_change_sync_and_recovery_info(
            EpochChangeSyncAndRecoveryInfo::new(
                epoch,
                node_status.is_catching_up(),
                membership,
                owned_shards,
                None,
            ),
        );
        Ok(())
    }

    /// Storage node execution of the epoch change start event, to bring the node state to the next
    /// epoch.
    ///
    /// This runs in three phases:
    ///
    /// 1. *Reconcile* ([`Self::reconcile_committee_for_epoch_change`]): bring the in-memory
    ///    committee state to the event's (or the latest) epoch and determine the route the node
    ///    takes through the epoch change.
    /// 2. *Plan* ([`plan::plan_epoch_change`]): a pure function of the reconciled facts that
    ///    decides everything the node must do: the status transition, the shard changes, the
    ///    recovery action, and which component attests `epoch_sync_done`.
    /// 3. *Apply* ([`Self::apply_epoch_change_plan`]): execute the plan in one linear pass.
    ///
    /// The whole transition runs inside the [`EpochChangeCriticalSection`], entered here before
    /// the shard map lock; the apply step holds the guard across the transition and releases it
    /// before the completion hand-off.
    pub(super) async fn execute_epoch_change(
        &self,
        event_handle: EventHandle,
        event: &EpochChangeStart,
    ) -> anyhow::Result<()> {
        // Enter the epoch-change critical section (see [`EpochChangeCriticalSection`]; it must
        // be entered before the shard map lock), then lock the read access to the shard map
        // until all the new shards are created.
        let critical_section_guard = self.inner.epoch_change_critical_section.enter().await;
        let shard_map_lock = self.inner.storage.lock_shards().await;

        let node_status_at_beginning_of_epoch_change =
            self.reconcile_committee_for_epoch_change(event).await?;

        // Clean up work that is pending on blobs no longer certified in the new epoch: notify
        // all the tasks that may be waiting on such blobs, and cancel their in-progress blob
        // syncs (marking the corresponding events as completed). This happens at every epoch
        // change, regardless of the route: retirement is a fact about the new epoch, not about
        // the route the node takes. It must run after reconciliation, because the certification
        // check compares each blob's end epoch against the committee service's epoch, which
        // reconciliation just advanced (on the already-in-progress route, the committee had
        // already transitioned). On entering recovery mode, reconciliation has additionally
        // cancelled *all* blob syncs, making the expired-sync cancellation here a no-op.
        self.notify_pending_blob_retirements()?;
        self.blob_sync_handler
            .cancel_all_expired_syncs_and_mark_events_completed()
            .await?;

        let committees = self.inner.committee_service.active_committees();
        let public_key = self.inner.public_key();
        let shard_diff_calculator =
            ShardDiffCalculator::new(&committees, public_key, shard_map_lock.existing_shards());
        if cfg!(msim) {
            // In simtest, print out the shard migration information for easier debugging.
            tracing::info!("EpochChangeStart shard diffs: {:?}", shard_diff_calculator);
        }

        let inputs = plan::PlanInputs {
            event_epoch: event.epoch,
            committee_epoch: committees.epoch(),
            node_status_at_beginning_of_epoch_change,
            node_status: self.inner.storage.node_status()?,
            in_current_committee: committees.current_committee().contains(public_key),
            in_previous_committee: committees
                .previous_committee()
                .is_some_and(|committee| committee.contains(public_key)),
            has_ongoing_shard_syncs: self.shard_sync_handler.has_sync_in_progress(),
            shards: plan::ShardDiff {
                gained: shard_diff_calculator
                    .gained_shards_from_prev_epoch()
                    .to_vec(),
                lost: shard_diff_calculator.shards_to_lock().to_vec(),
                removed: shard_diff_calculator.shards_to_remove().to_vec(),
                all_owned: shard_diff_calculator.all_owned_shards().to_vec(),
            },
        };
        let epoch_change_plan = plan::plan_epoch_change(&inputs);
        tracing::info!(
            walrus.epoch = event.epoch,
            plan = ?epoch_change_plan,
            "planned epoch change"
        );
        tracing::error!(
            walrus.epoch = event.epoch,
            inputs = ?inputs,
            plan = ?epoch_change_plan,
            "DIAG planned epoch change"
        );

        self.apply_epoch_change_plan(
            event_handle,
            event,
            epoch_change_plan,
            committees,
            shard_map_lock,
            critical_section_guard,
        )
        .await
    }

    /// Brings the in-memory committee state to the event's (or the latest) epoch and returns the
    /// route the node takes through the epoch change.
    ///
    /// This is the only phase of the epoch change that performs I/O to establish facts. Its only
    /// side effects besides the committee transition itself are those of entering recovery mode
    /// on a severely lagging node (cancelling *all* blob syncs).
    async fn reconcile_committee_for_epoch_change(
        &self,
        event: &EpochChangeStart,
    ) -> anyhow::Result<plan::NodeStatusAtBeginningOfEpochChange> {
        if self.inner.storage.node_status()?.is_catching_up() {
            self.inner
                .committee_service
                .begin_committee_change_to_latest_committee()
                .await?;
            return Ok(plan::NodeStatusAtBeginningOfEpochChange::CatchingUp);
        }

        match self.begin_committee_change(event.epoch).await? {
            BeginCommitteeChangeAction::ExecuteEpochChange => {
                Ok(plan::NodeStatusAtBeginningOfEpochChange::InSync)
            }
            BeginCommitteeChangeAction::SkipEpochChange => {
                Ok(plan::NodeStatusAtBeginningOfEpochChange::AlreadyInProgress)
            }
            BeginCommitteeChangeAction::EnterRecoveryMode => {
                tracing::info!("storage node entering recovery mode during epoch change start");
                sui_macros::fail_point!("fail-point-enter-recovery-mode");

                self.enter_recovery_mode_in_critical_section().await?;

                self.inner
                    .committee_service
                    .begin_committee_change_to_latest_committee()
                    .await?;
                Ok(plan::NodeStatusAtBeginningOfEpochChange::CatchingUp)
            }
        }
    }

    /// For blobs that are expired in the new epoch, sends a notification to all the tasks that
    /// may be affected by the blob expiration.
    fn notify_pending_blob_retirements(&self) -> anyhow::Result<()> {
        self.inner
            .blob_retirement_notifier
            .epoch_change_notify_all_pending_blob_retirement(self.inner.clone())
    }

    /// Applies an [`plan::EpochChangePlan`] in one linear pass.
    ///
    /// This function contains no decisions of its own: all branching on the node's situation
    /// lives in [`plan::plan_epoch_change`]. The plan is applied in four sections, in this
    /// order:
    ///
    /// 1. [node status changes][Self::apply_node_status_changes] and
    /// 2. [shard management][Self::apply_shard_changes] — both inside the epoch-change
    ///    critical section, so the whole transition is atomic with respect to the completions
    ///    of the long-running sync services — and, after leaving the critical section,
    /// 3. [completion hand-off][Self::hand_off_completion].
    ///
    /// The recovery service is not controlled from here: it reconciles on its own toward the
    /// published goal and the persisted recovery target.
    ///
    /// As all functions that are passed an [`EventHandle`], this is responsible for marking the
    /// event as completed.
    async fn apply_epoch_change_plan(
        &self,
        event_handle: EventHandle,
        event: &EpochChangeStart,
        epoch_change_plan: plan::EpochChangePlan,
        committees: ActiveCommittees,
        shard_map_lock: StorageShardLock,
        critical_section_guard: tokio::sync::MutexGuard<'_, ()>,
    ) -> anyhow::Result<()> {
        let execution_info = match epoch_change_plan {
            plan::EpochChangePlan::Skip(reason) => {
                tracing::info!(
                    walrus.epoch = event.epoch,
                    ?reason,
                    "skipping epoch change processing"
                );
                event_handle.mark_as_complete();
                return Ok(());
            }
            plan::EpochChangePlan::MoveToStandby => {
                tracing::info!(
                    "node is not in the current committee, set node status to 'Standby'"
                );
                self.inner.set_node_status(NodeStatus::Standby)?;
                // A standby node makes no epoch-sync claim and runs no sync work: quiesce it
                // and invalidate any unconsumed token or instruction.
                self.shard_sync_handler.quiesce_all_syncs().await;
                self.shard_sync_handler.clear_epoch_sync_done_token();
                self.shard_sync_handler.clear_metadata_recovery_completion();
                self.node_recovery_handler.clear_completion_instruction();
                self.inner.publish_epoch_change_sync_and_recovery_info(
                    EpochChangeSyncAndRecoveryInfo::new(
                        event.epoch,
                        false,
                        goal::MembershipAtEpochChange::NotMember,
                        Vec::new(),
                        None,
                    ),
                );
                event_handle.mark_as_complete();
                return Ok(());
            }
            plan::EpochChangePlan::Apply(execution_info) => execution_info,
        };

        // Sections 1-2 run inside the epoch-change critical section (entered by the caller).
        // The recovery service is not controlled from here: it watches the published info (and
        // the persisted recovery target) and reconciles on its own.
        self.apply_node_status_changes(&execution_info, event)?;
        let finisher_attestation = self
            .apply_shard_changes(&execution_info, &committees, shard_map_lock, event)
            .await?;

        // End of the critical section: the node's status, shards, and recovery task are
        // consistent with the new epoch; a parked recovery-task completion may now proceed and
        // will observe the full transition.
        drop(critical_section_guard);

        self.hand_off_completion(
            event_handle,
            event,
            &execution_info,
            committees,
            finisher_attestation,
        );

        Ok(())
    }

    /// Section 1 of applying an epoch-change plan: node status changes.
    ///
    /// Performs the status transitions that must precede shard creation (`Standby` and the
    /// `RecoveryInProgress` target).
    ///
    /// The `RecoverMetadata` transition is the one status write that does *not* happen here: it
    /// must be ordered between shard storage creation and the start of the shard syncs, and is
    /// therefore performed by [`Self::apply_shard_changes`] — as is the routing of the
    /// `epoch_sync_done` attestation, which must be ordered after the lost shards are locked.
    fn apply_node_status_changes(
        &self,
        execution_info: &plan::EpochChangeExecutionInfo,
        event: &EpochChangeStart,
    ) -> anyhow::Result<()> {
        // Status transitions that must precede shard creation. Advancing the recovery target,
        // starting the shard syncs for gained shards, and locking the shards that moved away
        // happen inside the critical section, so they are atomic with respect to the recovery
        // task's completion (which enters the same critical section): a completing task either
        // observes the advanced target together with the new shard syncs and the locked shards,
        // or completes entirely before this transition (detected above and handled by the
        // recovery action). In particular, completion must not attest epoch sync done before
        // the lost shards are locked, as the node would still accept slivers for shards it no
        // longer owns.
        match execution_info.status {
            Some(plan::StatusTransition::Standby) => {
                // The node is not in the current committee, and therefore from this epoch on it
                // won't sync any blob metadata. In the case it becomes a committee member again,
                // it needs to sync blob metadata again.
                tracing::info!(
                    "node is not in the current committee, set node status to 'Standby'"
                );
                self.inner.set_node_status(NodeStatus::Standby)?;
                // A dropout supersedes the pending completions of both long-running tasks:
                // neither a still-running metadata recovery nor a still-running node recovery
                // may transition the node away from `Standby` when it finishes. Both are
                // revoked here, atomically with the status write inside the critical section:
                // the attestation routing in `apply_shard_changes` also clears the recovery
                // instruction, but an error on the way there (locking a lost shard, creating
                // storage) would otherwise leave it armed for a stale completion — an illegal
                // `Standby` -> `Active` write.
                self.shard_sync_handler.clear_metadata_recovery_completion();
                self.node_recovery_handler.clear_completion_instruction();
            }
            Some(plan::StatusTransition::RecoveryInProgress) => {
                // TODO(WAL-1273): writing `RecoveryInProgress` before the shard storage is
                // created (section 2) makes the exited-catch-up transition non-replayable: a
                // crash (or an error-driven event retry) in between leaves shards that were
                // assigned during the skipped catch-up backlog without storage, and the
                // replayed event — routed on the now-persisted `RecoveryInProgress` — creates
                // only the previous-to-current committee diff, so node recovery waits for the
                // missing storage forever. Fix by moving this write after storage creation and
                // force-activation, mirroring `RecoverMetadata`'s "status implies storage
                // exists" ordering; a crash then replays through the catch-up route, which
                // re-plans the full recovery. Until fixed, the remedy for a stuck node is to
                // stop it and rewrite the persisted node status to `RecoveryCatchUp`: the next
                // epoch change then re-plans the full recovery, which creates storage for all
                // owned shards while keeping the existing shard data.
                tracing::info!(
                    walrus.epoch = event.epoch,
                    "setting the node recovery target to the event's epoch"
                );
                self.inner
                    .set_node_status(NodeStatus::RecoveryInProgress(event.epoch))?;
            }
            Some(plan::StatusTransition::RecoverMetadata) | None => {}
        }
        if matches!(
            execution_info.status,
            Some(plan::StatusTransition::RecoveryInProgress)
        ) {
            sui_macros::fail_point!("fail_point_shard_changes_in_new_epoch_while_recovering");
        }

        Ok(())
    }

    /// Section 2 of applying an epoch-change plan: shard management.
    ///
    /// Creates storage for the newly assigned shards (consuming the shard map lock), brings
    /// them up to date as planned (force-`Active` for recovery-filled shards; shard syncs are
    /// started for sync-filled shards), and locks the shards that moved to other nodes.
    ///
    /// This section also performs the `RecoverMetadata` status write, as an exception to the
    /// "status changes happen in section 1" rule: the write must be ordered after shard storage
    /// creation (a restart observing `RecoverMetadata` assumes all shard storage exists) and
    /// before the shard syncs start. It likewise routes the `epoch_sync_done` attestation,
    /// whose ordering is load-bearing in both directions: after the lost shards are locked (so
    /// no attestation — including the fast path for an already-active gained shard on event
    /// replay — can fire while the node still accepts writes for shards it lost), and before
    /// the shard syncs start (so a completing sync cannot miss its token).
    ///
    /// Returns the attestation token to hand to the finisher, if the finisher is the
    /// attestation owner (or if shard sync's pending set turned out to be already empty).
    async fn apply_shard_changes(
        &self,
        execution_info: &plan::EpochChangeExecutionInfo,
        committees: &ActiveCommittees,
        shard_map_lock: StorageShardLock,
        event: &EpochChangeStart,
    ) -> anyhow::Result<Option<EpochSyncDoneToken>> {
        // Create storage for the newly assigned shards. Note that the shard map lock is
        // released when creation completes (or here, if there are no new shards).
        if let Some(new_shards) = &execution_info.new_shards {
            assert!(
                committees
                    .current_committee()
                    .contains(self.inner.public_key())
            );
            self.inner
                .create_storage_for_shards_in_background(new_shards.shards.clone(), shard_map_lock)
                .await?;
        } else {
            drop(shard_map_lock);
        }

        // Force-set the new shards to `Active` when they are filled via node recovery
        // (full-recovery path). The node's local shards may be in outdated statuses from
        // multiple epochs ago; node recovery will recover all the missing certified blobs in
        // these shards in a crash-tolerant manner.
        if let Some(new_shards) = &execution_info.new_shards
            && new_shards.fill == plan::ShardFill::ForceActive
        {
            for shard in &new_shards.shards {
                self.inner
                    .storage
                    .shard_storage(*shard)
                    .await
                    .expect("we just create all storage, it must exist")
                    .force_set_active_status()
                    .await?;
            }
        }

        // Set `RecoverMetadata` for a node that newly joined the committee. This must be set
        // before marking the event as complete, so that a node crashing before setting the
        // status will always set it again when re-processing the `EpochChangeStart` event. It
        // must also be set after creating storage for the new shards: a restart observing
        // `RecoverMetadata` assumes all the shards are created.
        if matches!(
            execution_info.status,
            Some(plan::StatusTransition::RecoverMetadata)
        ) {
            tracing::info!(
                "node just became a new committee member; recovering blob metadata before \
                syncing shards"
            );
            self.inner.set_node_status(NodeStatus::RecoverMetadata)?;
            // Authorize the metadata-recovery task to flip the node to `Active` when it
            // finishes. The instruction must be in place before the shard syncs start below (a
            // quickly finishing task must not miss it); a later transition that supersedes the
            // metadata recovery (dropping out of the committee, entering recovery mode) revokes
            // it by clearing the slot.
            self.shard_sync_handler.set_metadata_recovery_completion(
                BackgroundSyncTaskCompletionInstruction::new(NodeStatus::Active, None),
            );
        }

        // Quiesce all sync work, still inside the critical section: after this point no
        // sync task from an earlier epoch change exists (`quiesce_all_syncs` aborts the
        // tasks and awaits their termination, and no new sync work can start while this
        // executor occupies the critical section — the reconciler enters it for every
        // pass). This must precede the shard locking below: a sync-driving task from an
        // earlier pass derives its shard list from a pre-change committee view, so it can
        // consider a moved-away shard owned and, through `start_new_shard_sync` (which
        // skips only `Active` shards), overwrite a just-written `LockedToMove` status with
        // `ActiveSync` — silently re-opening the shard for writes for the rest of the
        // epoch. Quiescing first also keeps the attestation's pending shard syncs recorded
        // below from being satisfied or consumed by stale work. The reconciler rebuilds the
        // still-needed syncs from persisted progress once the info published below becomes
        // visible; at most the in-flight batches are repeated, at the epoch-change cadence.
        self.shard_sync_handler.quiesce_all_syncs().await;

        // Lock the shards that moved out, so that they do not accept any more writes. This
        // must precede the attestation routing and the sync starts below: an attestation may
        // fire as soon as the token is placed (for example, via the already-active fast path
        // when re-processing the event after a crash), and it must never fire while the node
        // still accepts writes for shards it no longer owns.
        for shard_id in &execution_info.lock {
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

        // Route the `epoch_sync_done` attestation: mint the token for the new epoch and hand
        // it to the owner named in the plan, invalidating any token or instruction held by
        // the other components. This runs inside the critical section (for recovery-owned
        // attestations, the instruction placement is thereby atomic with the target
        // advancement in section 1), after the lost shards are locked, and before any shard
        // sync is started. The recovery task receives a completion instruction bundling the
        // status transition it must perform on completion with the attestation, so the two
        // cannot diverge.
        let token = EpochSyncDoneToken::new_for_epoch(event.epoch);
        let mut finisher_attestation = None;
        match execution_info.sync_done_owner {
            plan::EpochSyncDoneAttestationOwner::StartEpochChangeFinisher => {
                self.shard_sync_handler.clear_epoch_sync_done_token();
                self.node_recovery_handler.clear_completion_instruction();
                finisher_attestation = Some(token);
            }
            plan::EpochSyncDoneAttestationOwner::ShardSync => {
                self.node_recovery_handler.clear_completion_instruction();
                // Record the token's pending shard syncs: every owned shard whose persisted
                // status is not yet `Active` — exactly the work the reconciler rebuilds. With
                // all sync work quiesced above, the pending set cannot race stale completions.
                // An empty pending set means the epoch-sync claim already holds (for example,
                // when re-processing the event after a crash): the finisher attests directly.
                let mut pending_shards = Vec::new();
                for shard in &execution_info.owned_shards {
                    let is_active = match self.inner.storage.shard_storage(*shard).await {
                        Some(shard_storage) => {
                            matches!(shard_storage.status().await, Ok(ShardStatus::Active))
                        }
                        None => false,
                    };
                    if !is_active {
                        pending_shards.push(*shard);
                    }
                }
                if pending_shards.is_empty()
                    && self.inner.storage.node_status()? != NodeStatus::RecoverMetadata
                {
                    finisher_attestation = Some(token);
                } else {
                    // Registered even when no shard is pending while metadata recovery is
                    // outstanding: the metadata task's completion consumes the token then.
                    self.shard_sync_handler
                        .set_epoch_sync_done_token(token, pending_shards);
                }
            }
            plan::EpochSyncDoneAttestationOwner::RecoveryTask => {
                self.shard_sync_handler.clear_epoch_sync_done_token();
                self.node_recovery_handler.set_completion_instruction(
                    BackgroundSyncTaskCompletionInstruction::new(NodeStatus::Active, Some(token)),
                );
            }
        }

        // Publish the sync-and-recovery info for this transition. Published inside the
        // critical section, after the lost shards are locked (services acting on the info may
        // ultimately attest, which must not happen while the node still accepts writes for
        // shards it lost) and after the attestation is routed above (a sync started by the
        // reconciler must find its token and pending set). The shard-sync reconciler reacts
        // to the info by starting the syncs; the executor no longer starts them directly.
        // Record the event epoch before publishing: the shard-sync driver bounds its work by
        // the event epoch, and a sync started by the reconciler for this publication must use
        // the new epoch as its bound (the node has processed every event before this one, so
        // its certified-blob list is complete up to here).
        let _ = self.inner.latest_event_epoch_sender.send(Some(event.epoch));
        self.inner.publish_epoch_change_sync_and_recovery_info(
            EpochChangeSyncAndRecoveryInfo::new(
                event.epoch,
                false,
                execution_info.membership,
                execution_info.owned_shards.clone(),
                execution_info.new_shards.clone(),
            ),
        );

        Ok(finisher_attestation)
    }

    /// Section 4 of applying an epoch-change plan: completion hand-off.
    ///
    /// The finisher removes old shards in the background, and — if it holds the attestation
    /// token — attests `epoch_sync_done` for the new epoch before marking the event as
    /// complete. When another component owns the attestation and there is nothing to remove,
    /// the event is completed directly.
    fn hand_off_completion(
        &self,
        event_handle: EventHandle,
        event: &EpochChangeStart,
        execution_info: &plan::EpochChangeExecutionInfo,
        committees: ActiveCommittees,
        finisher_attestation: Option<EpochSyncDoneToken>,
    ) {
        let complete_directly = matches!(
            execution_info.sync_done_owner,
            plan::EpochSyncDoneAttestationOwner::RecoveryTask
        ) && execution_info.remove.is_empty();
        if complete_directly {
            event_handle.mark_as_complete();
        } else {
            self.start_epoch_change_finisher
                .start_finish_epoch_change_tasks(
                    event_handle,
                    event,
                    execution_info.remove.clone(),
                    committees,
                    finisher_attestation,
                );
        }
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
        // publish a catching-up info so the sync services hold off on new work until the node
        // reaches the latest epoch.
        self.inner.publish_epoch_change_sync_and_recovery_info(
            EpochChangeSyncAndRecoveryInfo::new(
                self.inner.current_committee_epoch(),
                true,
                goal::MembershipAtEpochChange::NotMember,
                Vec::new(),
                None,
            ),
        );

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

impl StorageNode {
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
}
