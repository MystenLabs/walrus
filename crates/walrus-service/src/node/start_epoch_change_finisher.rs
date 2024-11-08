// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use sui_macros::fail_point_async;
use typed_store::TypedStoreError;
use walrus_core::ShardIndex;
use walrus_sui::types::EpochChangeStart;

use super::StorageNodeInner;
use crate::{
    common::active_committees::ActiveCommittees,
    utils::{self, ExponentialBackoff},
};

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

    /// Starts background tasks to finish the epoch change.
    /// This includes:
    ///    - Sending epoch sync done if there is no newly scheduled shard syncs.
    ///    - Removing no longer owned storage for shards.
    ///    - Marking the event as completed.
    pub fn start_finish_epoch_change_tasks(
        &self,
        event_element: usize,
        event: &EpochChangeStart,
        shards: Vec<ShardIndex>,
        committees: ActiveCommittees,
        ongoing_shard_sync: bool,
    ) {
        let self_clone = self.clone();
        let event_clone = event.clone();
        let committees_clone = committees.clone();

        let mut locked_task_handle = self.task_handle.lock().unwrap();
        assert!(locked_task_handle.is_none());

        let handle = tokio::spawn(async move {
            let backoff = ExponentialBackoff::new_with_seed(
                Duration::from_secs(10),
                Duration::from_secs(300),
                // Since this function is in charge of mark the event completed, we have to keep
                // retrying until success. Otherwise, the event process is blocked anyway.
                None,
                // Seed the backoff with the shard index.
                shards.first().unwrap_or(&ShardIndex(0)).as_u64(),
            );

            fail_point_async!("blocking_finishing_epoch_change_start");

            let result = utils::retry(backoff, || async {
                if !ongoing_shard_sync {
                    self_clone
                        .epoch_sync_done(&committees_clone, &event_clone)
                        .await;
                }
                self_clone
                    .remove_storage_for_shards(event_clone.clone(), &shards.clone())
                    .await?;
                self_clone
                    .node
                    .mark_event_completed(event_element, &event_clone.event_id)
            })
            .await;

            if let Err(err) = result {
                tracing::error!(
                    epoch = %event_clone.epoch,
                    error = %err,
                    "failed to finish epoch change start tasks",
                );
            }

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
                .epoch_sync_done(event.epoch)
                .await;
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
        for shard in shards {
            tracing::info!(shard = %shard, "start removing shard from storage");
            self.node
                .storage
                .remove_storage_for_shards(&[*shard])
                .map_err(|err| {
                    tracing::error!(
                        epoch = %event.epoch,
                        shard = %shard,
                        error = %err,
                        "failed to remove storage for shard",
                    );
                    err
                })?;
            tracing::info!(shard = %shard, "removing shard storage successful");
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
