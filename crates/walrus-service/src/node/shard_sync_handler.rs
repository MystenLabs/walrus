// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};

use tokio::sync::Mutex;
use walrus_core::ShardIndex;

use super::{
    storage::{ShardStatus, ShardStorage},
    StorageNodeInner,
};
use crate::node::errors::ShardNotAssigned;

#[derive(Debug, Clone)]
pub struct ShardSyncHandler {
    node: Arc<StorageNodeInner>,
    shard_sync_in_progress: Arc<Mutex<HashMap<ShardIndex, tokio::task::JoinHandle<()>>>>,
}

impl ShardSyncHandler {
    pub fn new(node: Arc<StorageNodeInner>) -> Self {
        Self {
            node,
            shard_sync_in_progress: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn restart_syncs(&self) -> Result<(), anyhow::Error> {
        for shard_index in self.node.storage.shards() {
            let shard_storage = self.node.storage.shard_storage(shard_index).unwrap();

            if shard_storage.status()? != ShardStatus::ActiveSync {
                continue;
            }

            self.start_shard_sync_impl(shard_storage.clone()).await;
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn start_new_shard_sync(&self, shard_index: ShardIndex) -> Result<(), anyhow::Error> {
        match self.node.storage.shard_storage(shard_index) {
            Some(shard_storage) => {
                if shard_storage.status()? == ShardStatus::None {
                    return Ok(());
                }
                self.start_shard_sync_impl(shard_storage.clone()).await;
                Ok(())
            }
            None => {
                tracing::error!(
                    "Shard index: {} is not assigned to this node. Cannot start shard sync.",
                    shard_index
                );
                Err(ShardNotAssigned(shard_index, self.node.current_epoch()).into())
            }
        }
    }

    async fn start_shard_sync_impl(&self, shard_storage: Arc<ShardStorage>) {
        // TODO: This needs to be the previous epoch, once storage node has a notion of multiple epochs.
        let epoch_to_sync = self.node.current_epoch();
        tracing::info!(
            "Syncing shard index {} to the end of epoch {}",
            shard_storage.id(),
            epoch_to_sync
        );

        // TODO: implement rate limiting for shard syncs.
        let mut shard_sync_in_progress = self.shard_sync_in_progress.lock().await;
        if let Entry::Vacant(entry) = shard_sync_in_progress.entry(shard_storage.id()) {
            let node_clone = self.node.clone();
            let shard_sync_handler_clone = self.clone();
            let shard_sync_task = tokio::spawn(async move {
                let shard_index = shard_storage.id();
                let sync_result = shard_storage
                    .start_sync_shard_to_epoch(epoch_to_sync, node_clone)
                    .await;

                if let Err(err) = sync_result {
                    tracing::error!(
                        "Failed to sync shard index: {} to epoch: {}. Error: {}",
                        shard_index,
                        epoch_to_sync,
                        err
                    );
                } else {
                    tracing::info!(
                        "Successfully synced shard index: {} to epoch: {}",
                        shard_index,
                        epoch_to_sync
                    );
                }

                shard_sync_handler_clone
                    .shard_sync_in_progress
                    .lock()
                    .await
                    .remove(&shard_index);
            });
            entry.insert(shard_sync_task);
        } else {
            tracing::info!(
                "Shard index: {} is already being synced. Skipping starting new sync task.",
                shard_storage.id()
            );
        }
    }
}
