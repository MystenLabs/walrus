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

use super::{system_events::EventHandle, StorageNodeInner};
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

    /// Starts background tasks to finish the epoch change.
    ///
    /// This includes the following:
    /// - Sending epoch sync done if there is no newly scheduled shard syncs.
    /// - Removing no longer owned storage for shards.
    /// - Marking the event as completed.
    pub fn start_finish_epoch_change_tasks(
        &self,
        event_handle: EventHandle,
        event: &EpochChangeStart,
        shards: Vec<ShardIndex>,
        committees: ActiveCommittees,
        ongoing_shard_sync: bool,
    ) {
        let self_clone = self.clone();
        let event_clone = event.clone();

        let mut locked_task_handle = self.task_handle.lock().unwrap();
        assert!(locked_task_handle.is_none());

        let handle = tokio::spawn(async move {
            let backoff = ExponentialBackoff::new_with_seed(
                Duration::from_secs(10),
                Duration::from_secs(300),
                // Since this function is in charge of marking the event as completed, we have to
                // keep retrying until success. Otherwise, the event process is blocked anyway.
                None,
                // Seed the backoff with the shard index.
                shards.first().unwrap_or(&ShardIndex(0)).as_u64(),
            );

            fail_point_async!("blocking_finishing_epoch_change_start");

            if let Err(error) = backoff::retry(backoff, || async {
                if !ongoing_shard_sync {
                    self_clone.epoch_sync_done(&committees, &event_clone).await;
                }
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
                    "failed to finish epoch change start tasks",
                );
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
                        epoch = %event.epoch,
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
