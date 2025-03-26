// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Background consistency check for the storage node.

#[cfg(msim)]
use std::{collections::HashMap, sync::Mutex};
use std::{hash::Hasher, sync::Arc};

use anyhow::Result;
#[cfg(msim)]
use sui_types::base_types::ObjectID;
use tokio::sync::oneshot;
use walrus_core::Epoch;

use super::{storage::blob_info::BlobInfoIterator, StorageNodeInner};

/// Schedule a background task to compute the hash of the list of certified blobs at the
/// beginning of the epoch. This is used to detect inconsistencies in the blob info table
/// between the nodes.
pub(super) async fn schedule_background_consistency_check(
    node: Arc<StorageNodeInner>,
    epoch: Epoch,
) -> Result<()> {
    let node = node.clone();
    let (tx, rx) = oneshot::channel();

    // Create a background thread which takes the ownership of the iterator and process it.
    tokio::task::spawn_blocking(move || {
        let _scope =
            mysten_metrics::monitored_scope("EpochChange::background_blob_info_consistency_check");

        // Create a blob info iterator that takes the current blob info table as the snapshot.
        let blob_info_iterator = node.storage.certified_blob_info_iter_before_epoch(epoch);
        let _ = tx.send(());

        compose_certified_blob_list_digest(node.clone(), blob_info_iterator, epoch);
    });

    // We need to make sure that the function returns only after the blob info iterator is
    // created, so that it can operate on a consistent snapshot of the blob info table.
    // Otherwise, processing future events may cause inconsistency in different nodes.
    rx.await.map_err(|err| {
        anyhow::anyhow!(
            "background task failed to create iterator. Error: {:?}",
            err
        )
    })?;
    Ok(())
}

/// Compose the digest of the certified blob list.
fn compose_certified_blob_list_digest(
    node: Arc<StorageNodeInner>,
    blob_info_iter: BlobInfoIterator,
    epoch: Epoch,
) {
    // xxhash is not a cryptographic hash function, but it is fast, has good collision
    // resistance, and is consistent across all platforms.
    let mut hasher = twox_hash::XxHash64::with_seed(epoch as u64);
    for item in blob_info_iter {
        match item {
            Ok(blob_info) => {
                hasher.write(blob_info.0.as_ref());
            }
            Err(error) => {
                // Upon error, we can terminate the task and return immediately since
                // we no longer can get a consistent view of the blob info table.
                tracing::warn!(?error, "error when processing blob info");
                node.metrics.blob_info_consistency_check_error.inc();
                return;
            }
        }
    }

    let value = hasher.finish();

    tracing::info!(?epoch, certified_blob_hash = ?value,
            "background blob info consistency check finished");
    walrus_utils::with_label!(
        node.metrics.blob_info_consistency_check,
        (epoch % 100).to_string()
    )
    .set(value as i64);

    sui_macros::fail_point_arg!("storage_node_certified_blob_digest", |digest_map: Arc<
        Mutex<HashMap<Epoch, HashMap<ObjectID, u64>>>,
    >| {
        digest_map
            .lock()
            .expect("failed to lock the digest map")
            .entry(epoch)
            .or_insert_with(|| HashMap::new())
            .insert(node.node_capability, value);
    });
}
