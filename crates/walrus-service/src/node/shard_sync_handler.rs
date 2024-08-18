// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};

use tokio::sync::Mutex;
use walrus_core::ShardIndex;

use super::StorageNodeInner;

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

    pub async fn start_shard_sync(&self, sync_shard_index: ShardIndex, restarting: bool) {
        tracing::info!(
            "Syncing shard index: {} to epoch: {}",
            sync_shard_index,
            self.node.current_epoch()
        );

        let mut shard_sync_in_progress = self.shard_sync_in_progress.lock().await;

        if let Entry::Vacant(entry) = shard_sync_in_progress.entry(sync_shard_index) {
            // TODO: once the contract supports multiple epochs, this needs to be updated to the previous epoch.
            let epoch = self.node.current_epoch();
            let node_clone = self.node.clone();
            let handler_clone = self.clone();
            let sync_handle = tokio::spawn(async move {
                let sync_result = node_clone
                    .clone()
                    .storage
                    .sync_shard_to_epoch(epoch, sync_shard_index, node_clone, restarting)
                    .await;

                if let Err(err) = sync_result {
                    tracing::error!(
                        "Failed to sync shard index: {} to epoch: {}. Error: {}",
                        sync_shard_index,
                        epoch,
                        err
                    );
                } else {
                    tracing::info!(
                        "Successfully synced shard index: {} to epoch: {}",
                        sync_shard_index,
                        epoch
                    );
                }

                handler_clone
                    .shard_sync_in_progress
                    .lock()
                    .await
                    .remove(&sync_shard_index);
            });
            entry.insert(sync_handle);
        } else {
            tracing::info!(
                "Shard index: {} is already being synced. Skipping.",
                sync_shard_index
            );
        }
    }
}
