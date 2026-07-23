// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Epoch-change orchestration for the storage node.
//!
//! This module contains the processing of [`EpochChangeEvent`]s: reacting to
//! `EpochChangeStart` and `EpochChangeDone`, transitioning the committee, computing and applying
//! shard changes, and coordinating node recovery and the `epoch_sync_done` attestation.

use super::*;

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

        // During epoch change, we need to lock the read access to shard map until all the new
        // shards are created.
        //
        // Lock ordering: the recovery status mutex must be acquired before the shard map lock.
        // The recovery task's completion attempt reads shard statuses while holding the status
        // mutex, so acquiring the status mutex after the shard map lock (as the recovery-related
        // epoch-change paths do) would deadlock with it.
        let recovery_status_guard = self.node_recovery_handler.lock_status().await;
        let shard_map_lock = self.inner.storage.lock_shards().await;

        // Now the general tasks around epoch change are done. Next, entering epoch change logic
        // to bring the node state to the next epoch. `execute_epoch_change` ends by spawning
        // the finisher task (shard removal + `epoch_sync_done` + `mark_as_complete`), so the
        // finisher is guaranteed to fire only after phase 1 succeeded.
        self.execute_epoch_change(event_handle, event, shard_map_lock, recovery_status_guard)
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
    /// `status_guard` is the recovery status mutex guard, acquired by the caller before the shard
    /// map lock (see the lock ordering comment at the acquisition site); the recovery-related
    /// paths below hold it across their node status transitions.
    async fn execute_epoch_change(
        &self,
        event_handle: EventHandle,
        event: &EpochChangeStart,
        shard_map_lock: StorageShardLock,
        status_guard: tokio::sync::MutexGuard<'_, ()>,
    ) -> anyhow::Result<()> {
        if self.inner.storage.node_status()?.is_catching_up() {
            self.execute_epoch_change_while_catching_up(
                event_handle,
                event,
                shard_map_lock,
                status_guard,
            )
            .await?;
        } else {
            match self.begin_committee_change(event.epoch).await? {
                BeginCommitteeChangeAction::ExecuteEpochChange => {
                    self.execute_epoch_change_when_node_is_in_sync(
                        event_handle,
                        event,
                        shard_map_lock,
                        status_guard,
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

                    self.enter_recovery_mode().await?;

                    self.execute_epoch_change_while_catching_up(
                        event_handle,
                        event,
                        shard_map_lock,
                        status_guard,
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
        status_guard: tokio::sync::MutexGuard<'_, ()>,
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
            drop(status_guard);
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
                status_guard,
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
        status_guard: tokio::sync::MutexGuard<'_, ()>,
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
                status_guard,
            )
            .await
        } else {
            // This path performs no recovery-related status transitions, so the status guard is
            // not needed.
            drop(status_guard);
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
        status_guard: tokio::sync::MutexGuard<'_, ()>,
    ) -> anyhow::Result<()> {
        // A recovery task from before the node started catching up may still be running. Such a
        // task only scanned blobs certified before its own start epoch, and blob certified
        // events were skipped while catching up, so it must not complete the recovery target
        // written below. The status guard (acquired by the caller, before the shard map lock)
        // keeps any completion attempt of that task parked on the mutex, and aborting the task
        // here — before the new target is written — guarantees that no stale task survives to
        // observe it, even if a later step in this function fails and returns before reaching
        // `start_node_recovery` (which would otherwise perform the abort).
        self.node_recovery_handler.abort_recovery_task().await;

        self.inner
            .set_node_status(NodeStatus::RecoveryInProgress(event.epoch))?;

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

        // Initiate blob sync for all certified blobs we've tracked so far. After this is done,
        // the node will be in a state where it has all the shards and blobs that it should have.
        self.node_recovery_handler
            .start_node_recovery(event.epoch)
            .await?;

        drop(status_guard);

        // Last but not least, we need to remove any shards that are no longer owned by the node.
        let shards_to_remove = shard_diff_calculator.shards_to_remove();
        if !shards_to_remove.is_empty() {
            self.start_epoch_change_finisher
                .start_finish_epoch_change_tasks(
                    event_handle,
                    event,
                    shard_diff_calculator.shards_to_remove().to_vec(),
                    committees,
                    true,
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
        status_guard: tokio::sync::MutexGuard<'_, ()>,
    ) -> anyhow::Result<()> {
        // Advancing the recovery target, starting the shard syncs for gained shards, and locking
        // the shards that moved away must be atomic with respect to the recovery task's
        // completion, which runs under the same mutex (the guard is acquired by the caller,
        // before the shard map lock): a completing task either observes the advanced target
        // together with the new shard syncs and the locked shards, or completes entirely before
        // this transition (detected below via the node status, in which case a new task is
        // started). In particular, completion must not attest epoch sync done before the lost
        // shards are locked, as the node would still accept slivers for shards it no longer
        // owns.

        // If the running recovery task completed concurrently (after this event handler decided
        // to take the recovering path), it has flipped the node status away from
        // RecoveryInProgress; its completion no longer covers this epoch change.
        let recovery_task_completed_concurrently = !matches!(
            self.inner.storage.node_status()?,
            NodeStatus::RecoveryInProgress(_)
        );

        // Advance the recovery target so that the recovery task attests epoch sync done for the
        // latest epoch; a stale attestation would be dropped by the contract service.
        self.inner
            .set_node_status(NodeStatus::RecoveryInProgress(event.epoch))?;

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
        self.create_new_shards_and_start_sync(shard_map_lock, shards_gained, &committees, false)
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

        drop(status_guard);

        // The recovery task keeps running across epoch changes: it waits for the shard syncs
        // started above to finish before recovering blobs, and attests epoch sync done for the
        // advanced target on completion. A new task is only started when none is running.
        self.node_recovery_handler
            .ensure_recovery_task_running(event.epoch, recovery_task_completed_concurrently)
            .await?;

        // The recovery task is in charge of attesting epoch sync done, so the finisher is always
        // told that there are ongoing shard syncs.
        let shards_to_remove = shard_diff_calculator.shards_to_remove();
        if !shards_to_remove.is_empty() {
            self.start_epoch_change_finisher
                .start_finish_epoch_change_tasks(
                    event_handle,
                    event,
                    shards_to_remove.to_vec(),
                    committees,
                    true,
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
        self.create_new_shards_and_start_sync(
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

        self.start_epoch_change_finisher
            .start_finish_epoch_change_tasks(
                event_handle,
                event,
                shard_diff_calculator.shards_to_remove().to_vec(),
                committees,
                !shards_gained.is_empty(),
            );

        Ok(())
    }

    /// Creates the shards that are newly assigned to the node and starts the sync for them.
    /// Note that the shard_map_lock will be unlocked after this function returns.
    async fn create_new_shards_and_start_sync(
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
            }
            self.shard_sync_handler
                .start_sync_shards(shards_gained.to_vec())
                .await?;
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

    /// Enters recovery mode.
    /// This function should only be called when the node is lagging behind.
    pub(super) async fn enter_recovery_mode(&self) -> anyhow::Result<()> {
        self.inner.set_node_status(NodeStatus::RecoveryCatchUp)?;

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
