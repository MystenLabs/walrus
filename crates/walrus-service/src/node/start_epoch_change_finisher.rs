// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use sui_macros::fail_point_async;
use typed_store::TypedStoreError;
use walrus_core::ShardIndex;
use walrus_sdk::active_committees::ActiveCommittees;
use walrus_sui::types::EpochChangeStart;
use walrus_utils::backoff::{self, ExponentialBackoff};

use super::{StorageNodeInner, system_events::EventHandle};
use crate::node::system_events::CompletableHandle;

/// Args for the finisher task captured by
/// [`StartEpochChangeFinisher::schedule_finish_epoch_change_tasks`] and consumed by
/// [`StartEpochChangeFinisher::launch_pending_finisher_task`].
#[derive(Debug)]
struct PendingFinisherTask {
    event_handle: EventHandle,
    event: EpochChangeStart,
    shards: Vec<ShardIndex>,
    committees: ActiveCommittees,
    ongoing_shard_sync: bool,
}

#[derive(Debug, Clone)]
pub struct StartEpochChangeFinisher {
    node: Arc<StorageNodeInner>,

    // There can be at most one background finisher task at a time.
    task_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,

    // Args stashed by `schedule_finish_epoch_change_tasks`, consumed by
    // `launch_pending_finisher_task` once GC phase 1 has completed.
    pending: Arc<Mutex<Option<PendingFinisherTask>>>,
}

impl StartEpochChangeFinisher {
    pub fn new(node: Arc<StorageNodeInner>) -> Self {
        Self {
            node,
            task_handle: Arc::new(Mutex::new(None)),
            pending: Arc::new(Mutex::new(None)),
        }
    }

    /// Stashes args for the finisher task. The task itself is spawned by a subsequent
    /// call to [`launch_pending_finisher_task`].
    ///
    /// The task does three things: signal `epoch_sync_done` to the contract (when this
    /// node didn't gain new shards), call `remove_storage_for_shards` to discard shards
    /// no longer owned by this node, and mark the `EpochChangeStart` event as complete.
    ///
    /// The schedule/launch split exists because of the second step. Removing a shard
    /// means `drop_cf` against five column families and unlinking the SST/blob files
    /// behind them. On HDD-backed storage this saturates the disk for minutes per shard,
    /// and if it ran concurrently with GC phase 1's reads on the same RocksDB it blocked
    /// the event loop for tens of minutes at epoch boundaries (see Apr 21 2026 incident:
    /// ML1 phase 1 took ~31 min while removing 3 shards). Deferring the spawn until
    /// after phase 1 keeps the disk uncontended during phase 1.
    pub fn schedule_finish_epoch_change_tasks(
        &self,
        event_handle: EventHandle,
        event: &EpochChangeStart,
        shards: Vec<ShardIndex>,
        committees: ActiveCommittees,
        ongoing_shard_sync: bool,
    ) {
        let mut pending = self.pending.lock().expect("mutex should not be poisoned");
        if pending.is_some() {
            tracing::warn!(
                "discarding previously scheduled finisher task; reachable only if the \
                outer epoch-change handler errored between schedule and launch"
            );
        }
        *pending = Some(PendingFinisherTask {
            event_handle,
            event: event.clone(),
            shards,
            committees,
            ongoing_shard_sync,
        });
    }

    /// Spawns the finisher task previously stashed by
    /// [`schedule_finish_epoch_change_tasks`]. No-op if nothing is pending.
    pub fn launch_pending_finisher_task(&self) {
        let Some(task) = self
            .pending
            .lock()
            .expect("mutex should not be poisoned")
            .take()
        else {
            return;
        };

        let self_clone = self.clone();

        let mut locked_task_handle = self
            .task_handle
            .lock()
            .expect("mutex should not be poisoned");
        assert!(locked_task_handle.is_none());

        let handle = tokio::spawn(async move {
            let backoff = ExponentialBackoff::new_with_seed(
                Duration::from_secs(10),
                Duration::from_mins(5),
                // Since this function is in charge of marking the event as completed, we have to
                // keep retrying until success. Otherwise, the event process is blocked anyway.
                None,
                // Seed the backoff with the shard index.
                task.shards.first().unwrap_or(&ShardIndex(0)).as_u64(),
            );

            fail_point_async!("blocking_finishing_epoch_change_start");

            if let Err(error) = backoff::retry(backoff, || async {
                if !task.ongoing_shard_sync {
                    self_clone
                        .epoch_sync_done(&task.committees, &task.event)
                        .await;
                }
                self_clone
                    .remove_storage_for_shards(task.event.clone(), &task.shards)
                    .await?;
                anyhow::Ok(())
            })
            .await
            {
                // This should never happen as we don't have a max retry count.
                tracing::error!(
                    walrus.epoch = %task.event.epoch,
                    ?error,
                    "failed to finish epoch change start tasks",
                );
            }

            task.event_handle.mark_as_complete();
            self_clone
                .task_handle
                .lock()
                .expect("take lock should not fail")
                .take();
        });

        *locked_task_handle = Some(handle);
    }

    /// Signals that the epoch sync is done if the node is in the current committee and no shards.
    async fn epoch_sync_done(&self, committees: &ActiveCommittees, event: &EpochChangeStart) {
        let is_node_in_committee = committees
            .current_committee()
            .contains(self.node.public_key());
        if is_node_in_committee && committees.epoch() == event.epoch {
            // We are in the current committee, but no shards were gained. Directly signal that
            // the epoch sync is done.
            tracing::info!("no shards gained, so signalling that epoch sync is done");
            self.node
                .contract_service
                .epoch_sync_done(event.epoch, self.node.node_capability())
                .await;
            tracing::info!("epoch sync done signaled");
        } else {
            // Since we just refreshed the committee after receiving the event, the committees'
            // epoch must be at least the event's epoch.
            assert!(committees.epoch() >= event.epoch);
            tracing::info!(
                "skip sending epoch sync done event. \
                    node in committee: {}, committee epoch: {}, event epoch: {}",
                is_node_in_committee,
                committees.epoch(),
                event.epoch
            );
        }
    }

    async fn remove_storage_for_shards(
        &self,
        event: EpochChangeStart,
        shards: &[ShardIndex],
    ) -> Result<(), TypedStoreError> {
        for shard_index in shards {
            tracing::info!(walrus.shard_index = %shard_index, "start removing shard from storage");
            self.node
                .storage
                .remove_storage_for_shards(&[*shard_index])
                .await
                .map_err(|error| {
                    tracing::error!(
                        walrus.epoch = %event.epoch,
                        walrus.shard_index = %shard_index,
                        ?error,
                        "failed to remove storage for shard",
                    );
                    error
                })?;
            tracing::info!(walrus.shard_index = %shard_index, "removing shard storage successful");
        }

        Ok(())
    }

    /// Waits for the previous epoch transition's finisher task to complete.
    ///
    /// Drains `pending` first: if a prior attempt errored after
    /// `schedule_finish_epoch_change_tasks` but before
    /// `launch_pending_finisher_task` (e.g. `latest_event_epoch_sender.send` or
    /// phase 1 errored), the args would still be sitting in `pending`. Launch
    /// them now so they enter `task_handle` and are awaited below, instead of
    /// being silently skipped.
    pub async fn wait_until_previous_task_done(&self) {
        self.launch_pending_finisher_task();

        let existing_handle = self
            .task_handle
            .lock()
            .expect("grab lock should not fail")
            .take();
        if let Some(handle) = existing_handle {
            handle.await.expect("task should not have panicked");
        }
    }
}
