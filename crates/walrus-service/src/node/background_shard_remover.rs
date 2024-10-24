// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use walrus_core::ShardIndex;
use walrus_sui::types::EpochChangeStart;

use super::StorageNodeInner;
use crate::utils::{self, ExponentialBackoff};

#[derive(Debug, Clone)]
pub struct BackgroundShardRemover {
    node: Arc<StorageNodeInner>,

    // There can be at most one background shard removal task at a time.
    task_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl BackgroundShardRemover {
    pub fn new(node: Arc<StorageNodeInner>) -> Self {
        Self {
            node,
            task_handle: Arc::new(Mutex::new(None)),
        }
    }

    /// Start a task to remove storage for the given shards.
    /// Mark the event as completed after the removal is successful.
    pub fn start_remove_storage_for_shards(
        &self,
        event_element: usize,
        event: &EpochChangeStart,
        shards: Vec<ShardIndex>,
    ) {
        let remover_clone = self.clone();
        let event_clone = event.clone();

        let mut locked_task_handle = self.task_handle.lock().unwrap();
        assert!(locked_task_handle.is_none());

        let handle = tokio::spawn(async move {
            let backoff = ExponentialBackoff::new_with_seed(
                Duration::from_secs(10),
                Duration::from_secs(300),
                Some(10),
                shards[0].as_u64(), // Seed the backoff with the shard index.
            );

            let result = utils::retry(backoff, || async {
                remover_clone
                    .remove_storage_for_shards(event_element, event_clone.clone(), &shards.clone())
                    .await
            })
            .await;

            if let Err(err) = result {
                tracing::error!(
                    epoch = %event_clone.epoch,
                    error = %err,
                    "failed to remove storage for shards",
                );
            }
            remover_clone
                .task_handle
                .lock()
                .expect("take lock should not fail")
                .take();
        });

        *locked_task_handle = Some(handle);
    }

    async fn remove_storage_for_shards(
        &self,
        event_element: usize,
        event: EpochChangeStart,
        shards: &[ShardIndex],
    ) -> anyhow::Result<()> {
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

        self.node
            .mark_event_completed(event_element, &event.event_id)?;

        Ok(())
    }

    /// Wait until the previous shard remove task is done.
    pub async fn wait_until_previous_shard_remove_task_done(&self) {
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
