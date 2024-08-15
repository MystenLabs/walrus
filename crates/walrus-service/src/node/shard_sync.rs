// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use tokio::sync::mpsc::Receiver;
use walrus_core::ShardIndex;

use super::StorageNodeInner;

/// Background process for handling syncing shards.
pub async fn shard_sync_handler(
    node: Arc<StorageNodeInner>,
    mut shard_sync_receiver: Receiver<ShardIndex>,
) {
    // TODO: apply rate limiting.
    while let Some(sync_shard_index) = shard_sync_receiver.recv().await {
        let Some(shard_storage) = node.storage.shard_storage(sync_shard_index).cloned() else {
            tracing::warn!(
                "Failed to get shard storage for shard index: {}",
                sync_shard_index
            );
            continue;
        };

        tracing::info!(
            "Syncing shard index: {} to epoch: {}",
            sync_shard_index,
            node.current_epoch()
        );

        // TODO: once the contract supports multiple epochs, this needs to be updated to the previous epoch.
        let epoch = node.current_epoch();
        let node_clone = node.clone();
        tokio::spawn(async move {
            let sync_result = shard_storage.sync_shard_to_epoch(node_clone).await;
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
        });
    }
}
