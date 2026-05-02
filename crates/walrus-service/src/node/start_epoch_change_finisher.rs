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

#[derive(Debug, Clone)]
pub struct StartEpochChangeFinisher {
    node: Arc<StorageNodeInner>,

    // There can be at most one background start epoch change finisher task at a time.
    task_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl StartEpochChangeFinisher {
    pub fn new(node: Arc<StorageNodeInner>) -> Self {
        Self {
            node,
            task_handle: Arc::new(Mutex::new(None)),
        }
    }

    /// Spawns the background finisher task that does any post-phase-1 work for an epoch
    /// change and marks the `EpochChangeStart` event complete when done.
    ///
    /// The task does, in order:
    /// 1. `remove_storage_for_shards` for `shards` (skipped if empty), with retry on each
    ///    `drop_cf` failure.
    /// 2. Awaits `epoch_sync_done_handle` if any, so the contract signal lands before the
    ///    event is marked complete (it could already be done — `await`ing a completed
    ///    JoinHandle is a no-op).
    /// 3. `mark_as_complete` on the event handle.
    ///
    /// `epoch_sync_done` itself runs in a *separate* spawned task (see
    /// [`Self::spawn_epoch_sync_done`]) which the caller should fire before GC phase 1.
    /// That separation prevents the contract RPC's unbounded retry from blocking phase 1.
    pub fn start_finish_epoch_change_tasks(
        &self,
        event_handle: EventHandle,
        event: &EpochChangeStart,
        shards: Vec<ShardIndex>,
        epoch_sync_done_handle: Option<tokio::task::JoinHandle<()>>,
    ) {
        let self_clone = self.clone();
        let event_clone = event.clone();

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
                shards.first().unwrap_or(&ShardIndex(0)).as_u64(),
            );

            fail_point_async!("blocking_finishing_epoch_change_start");

            if !shards.is_empty()
                && let Err(error) = backoff::retry(backoff, || async {
                    self_clone
                        .remove_storage_for_shards(event_clone.clone(), &shards.clone())
                        .await?;
                    anyhow::Ok(())
                })
                .await
            {
                // This should never happen as we don't have a max retry count.
                tracing::error!(
                    walrus.epoch = %event_clone.epoch,
                    ?error,
                    "failed to remove shard storage at epoch change",
                );
            }

            // Wait for the in-flight `epoch_sync_done` (if one was spawned) to complete
            // before marking the event done. This keeps the on-chain "sync done" signal
            // and the local "event handled" marker in the same order they would be in main.
            if let Some(handle) = epoch_sync_done_handle {
                let _ = handle.await;
            }

            event_handle.mark_as_complete();
            self_clone
                .task_handle
                .lock()
                .expect("take lock should not fail")
                .take();
        });

        *locked_task_handle = Some(handle);
    }

    /// Spawns the contract `epoch_sync_done` RPC as a background task and returns its
    /// `JoinHandle`. Used by the caller to fire the signal *before* GC phase 1 without
    /// blocking on the contract service's unbounded retry loop. The returned handle is
    /// later awaited by [`Self::start_finish_epoch_change_tasks`] so the event is not
    /// marked complete until the contract signal has actually landed.
    pub(super) fn spawn_epoch_sync_done(
        &self,
        committees: ActiveCommittees,
        event: EpochChangeStart,
    ) -> tokio::task::JoinHandle<()> {
        let self_clone = self.clone();
        tokio::spawn(async move {
            self_clone.epoch_sync_done(&committees, &event).await;
        })
    }

    /// Signals that the epoch sync is done if the node is in the current committee and no shards.
    async fn epoch_sync_done(
        &self,
        committees: &ActiveCommittees,
        event: &EpochChangeStart,
    ) {
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

    /// Wait until the previous shard remove task is done.
    pub async fn wait_until_previous_task_done(&self) {
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
