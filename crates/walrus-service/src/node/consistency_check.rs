// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Background consistency check for the storage node.

#[cfg(msim)]
use std::{collections::HashMap, sync::Mutex};
use std::{collections::HashSet, hash::Hasher, sync::Arc};

use anyhow::{Context, Result};
use prometheus::IntCounterVec;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
#[cfg(msim)]
use sui_types::base_types::ObjectID;
use tokio::sync::oneshot;
use typed_store::TypedStoreError;
use walrus_core::{BlobId, Epoch};

use super::{
    StorageNodeInner,
    blob_sync::BlobSyncHandler,
    storage::blob_info::{
        BlobInfoIter,
        BlobInfoIterator,
        CertifiedBlobInfoApi,
        PerObjectBlobInfoIterator,
    },
};

/// Configuration for the consistency check.
#[serde_as]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct StorageNodeConsistencyCheckConfig {
    /// Enable the consistency check for the blob info table.
    pub enable_consistency_check: bool,
    /// Enable the sliver data existence check.
    pub enable_sliver_data_existence_check: bool,
}

impl Default for StorageNodeConsistencyCheckConfig {
    fn default() -> Self {
        Self {
            enable_consistency_check: true,
            enable_sliver_data_existence_check: false,
        }
    }
}

struct BlobConsistencyCheckResult {
    blob_list_digest: u64,
    total_synced_scanned: u64,
    total_fully_stored: u64,
    existence_check_error: u64,
}

/// Schedule a background task to compute the hash of the list of certified blobs at the
/// beginning of the epoch. This is used to detect inconsistencies in the blob info table
/// between the nodes.
pub(super) async fn schedule_background_consistency_check(
    node: Arc<StorageNodeInner>,
    blob_sync_handler: Arc<BlobSyncHandler>,
    epoch: Epoch,
) -> Result<()> {
    let node = node.clone();
    let blob_sync_handler = blob_sync_handler.clone();
    let (tx, rx) = oneshot::channel();

    // Create a background thread which takes the ownership of the iterator and process it.
    tokio::task::spawn_blocking(move || {
        let _scope =
            mysten_metrics::monitored_scope("EpochChange::background_blob_info_consistency_check");

        // Create a blob info iterator that takes the current blob info table as the snapshot.
        let blob_info_iterator = node.storage.certified_blob_info_iter_before_epoch(epoch);

        // Create a per-object blob info iterator that takes the current blob info table as the
        // snapshot.
        let per_object_blob_info_iterator = node
            .storage
            .certified_per_object_blob_info_iter_before_epoch(epoch);
        let _ = tx.send(());

        let blobs_being_synced = blob_sync_handler.blob_sync_in_progress();

        // Right now, the computing the two digests are sequential, given that scanning blob info
        // table is quick. We may consider parallelizing them in the future.
        certified_blob_consistency_check(
            node.clone(),
            blob_info_iterator,
            epoch,
            &blobs_being_synced,
        );
        compose_certified_object_blob_list_digest(
            node.clone(),
            per_object_blob_info_iterator,
            epoch,
        );
    });

    // We need to make sure that the function returns only after the blob info iterator is
    // created, so that it can operate on a consistent snapshot of the blob info table.
    // Otherwise, processing future events may cause inconsistency in different nodes.
    rx.await
        .context("background task failed to create iterator")?;
    Ok(())
}

fn get_epoch_bucket(epoch: Epoch) -> String {
    (epoch % 100).to_string()
}

fn handle_existence_check_result(
    result: Result<bool, anyhow::Error>,
    total_fully_stored: &mut u64,
    existence_check_error: &mut u64,
    blob_id: &BlobId,
) {
    match result {
        Ok(true) => *total_fully_stored += 1,
        Ok(false) => {}
        Err(error) => {
            *existence_check_error += 1;
            tracing::warn!(?error, blob_id=%blob_id, "error when checking sliver data existence");
        }
    }
}

/// Compose the digest of the blob list returned by the iterator.
/// `scan_counter` keeps track of the number of blobs scanned.
fn compose_blob_list_digest_and_check_sliver_data_existence(
    node: &StorageNodeInner,
    blob_info_iter: BlobInfoIterator<'_>,
    epoch: Epoch,
    scan_counter: &IntCounterVec,
    blobs_being_synced: &HashSet<BlobId>,
) -> Result<BlobConsistencyCheckResult, TypedStoreError> {
    #[cfg(not(msim))]
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to create tokio runtime");

    let epoch_bucket = get_epoch_bucket(epoch);
    let enable_sliver_data_existence_check = node
        .consistency_check_config
        .enable_sliver_data_existence_check;

    // xxhash is not a cryptographic hash function, but it is fast, has good collision
    // resistance, and is consistent across all platforms.
    let mut hasher = twox_hash::XxHash64::with_seed(epoch as u64);
    let mut total_synced_scanned = 0;
    let mut total_fully_stored = 0;
    let mut existence_check_error = 0;
    for item in blob_info_iter {
        match item {
            Ok(blob_info) => {
                hasher.write(blob_info.0.as_ref());

                if enable_sliver_data_existence_check && !blobs_being_synced.contains(&blob_info.0)
                {
                    total_synced_scanned += 1;

                    #[cfg(msim)]
                    {
                        handle_existence_check_result(
                            futures::executor::block_on(
                                node.is_stored_at_all_shards_at_latest_epoch(&blob_info.0),
                            ),
                            &mut total_fully_stored,
                            &mut existence_check_error,
                            &blob_info.0,
                        );
                    }

                    #[cfg(not(msim))]
                    {
                        handle_existence_check_result(
                            rt.block_on(node.is_stored_at_all_shards_at_latest_epoch(&blob_info.0)),
                            &mut total_fully_stored,
                            &mut existence_check_error,
                            &blob_info.0,
                        );
                    }
                }

                walrus_utils::with_label!(scan_counter, epoch_bucket).inc();
            }
            Err(error) => {
                // Upon error, we can terminate the task and return immediately since
                // we no longer can get a consistent view of the blob info table.
                return Err(error);
            }
        }
    }

    Ok(BlobConsistencyCheckResult {
        blob_list_digest: hasher.finish(),
        total_synced_scanned,
        total_fully_stored,
        existence_check_error,
    })
}

/// Compose the digest of the certified blob list.
fn certified_blob_consistency_check(
    node: Arc<StorageNodeInner>,
    blob_info_iter: BlobInfoIterator<'_>,
    epoch: Epoch,
    blobs_being_synced: &HashSet<BlobId>,
) {
    let _scope = mysten_metrics::monitored_scope(
        "EpochChange::background_certified_blob_info_consistency_check",
    );
    let epoch_bucket = get_epoch_bucket(epoch);

    match compose_blob_list_digest_and_check_sliver_data_existence(
        node.as_ref(),
        blob_info_iter,
        epoch,
        &node.metrics.blob_info_consistency_check_certified_scanned,
        blobs_being_synced,
    ) {
        Ok(BlobConsistencyCheckResult {
            blob_list_digest,
            total_synced_scanned,
            total_fully_stored,
            existence_check_error,
        }) => {
            tracing::info!(?epoch, certified_blob_hash = ?blob_list_digest,
                ?total_synced_scanned,
                ?total_fully_stored,
                ?existence_check_error,
            "background blob info consistency check finished");
            walrus_utils::with_label!(node.metrics.blob_info_consistency_check, epoch_bucket)
                .set(blob_list_digest as i64);

            if total_synced_scanned > 0 {
                let total_stored_percentage =
                    total_fully_stored as f64 / total_synced_scanned as f64;
                walrus_utils::with_label!(
                    node.metrics.blob_data_consistency_fully_stored_ratio,
                    epoch_bucket
                )
                .set(total_stored_percentage);
                walrus_utils::with_label!(
                    node.metrics.blob_data_consistency_check_existence_error,
                    epoch_bucket
                )
                .inc_by(existence_check_error);

                sui_macros::fail_point_arg!(
                    "storage_node_certified_blob_existence_check",
                    |digest_map: Arc<Mutex<HashMap<Epoch, HashMap<ObjectID, f64>>>>| {
                        digest_map
                            .lock()
                            .expect("failed to lock the digest map")
                            .entry(epoch)
                            .or_insert_with(|| HashMap::new())
                            .insert(node.node_capability, total_stored_percentage);
                    }
                );
            }

            // No-op out side of simtest.
            sui_macros::fail_point_arg!("storage_node_certified_blob_digest", |digest_map: Arc<
                Mutex<HashMap<Epoch, HashMap<ObjectID, u64>>>,
            >| {
                digest_map
                    .lock()
                    .expect("failed to lock the digest map")
                    .entry(epoch)
                    .or_insert_with(|| HashMap::new())
                    .insert(node.node_capability, blob_list_digest);
            });
        }
        Err(error) => {
            tracing::warn!(?error, "error when processing blob info");
            node.metrics.blob_info_consistency_check_error.inc();
        }
    };
}

/// Compose the digest of the blob list returned by the iterator.
/// `scan_counter` keeps track of the number of blobs scanned.
fn compose_blob_object_list_digest(
    blob_info_iter: PerObjectBlobInfoIterator,
    epoch: Epoch,
    scan_counter: &IntCounterVec,
) -> Result<u64, TypedStoreError> {
    let epoch_bucket = get_epoch_bucket(epoch);

    // xxhash is not a cryptographic hash function, but it is fast, has good collision
    // resistance, and is consistent across all platforms.
    let mut hasher = twox_hash::XxHash64::with_seed(epoch as u64);
    for item in blob_info_iter {
        match item {
            Ok(blob_info) => {
                hasher.write(blob_info.0.as_ref());
                walrus_utils::with_label!(scan_counter, epoch_bucket).inc();
            }
            Err(error) => {
                // Upon error, we can terminate the task and return immediately since
                // we no longer can get a consistent view of the blob info table.
                return Err(error);
            }
        }
    }

    Ok(hasher.finish())
}

/// Compose the digest of the certified object blob list.
fn compose_certified_object_blob_list_digest(
    node: Arc<StorageNodeInner>,
    per_object_blob_info_iter: PerObjectBlobInfoIterator,
    epoch: Epoch,
) {
    let _scope = mysten_metrics::monitored_scope(
        "EpochChange::background_certified_blob_object_info_consistency_check",
    );
    let epoch_bucket = get_epoch_bucket(epoch);

    let blob_object_list_digest = match compose_blob_object_list_digest(
        per_object_blob_info_iter,
        epoch,
        &node
            .metrics
            .per_object_blob_info_consistency_check_certified_scanned,
    ) {
        Ok(value) => value,
        Err(error) => {
            tracing::warn!(?error, "error when processing per object blob info");
            node.metrics
                .per_object_blob_info_consistency_check_error
                .inc();
            return;
        }
    };

    tracing::info!(?epoch, certified_blob_hash = ?blob_object_list_digest,
            "background per-object blob info consistency check finished");
    walrus_utils::with_label!(
        node.metrics.per_object_blob_info_consistency_check,
        epoch_bucket
    )
    .set(blob_object_list_digest as i64);

    // No-op out side of simtest.
    sui_macros::fail_point_arg!(
        "storage_node_certified_blob_object_digest",
        |digest_map: Arc<Mutex<HashMap<Epoch, HashMap<ObjectID, u64>>>>| {
            digest_map
                .lock()
                .expect("failed to lock the digest map")
                .entry(epoch)
                .or_insert_with(|| HashMap::new())
                .insert(node.node_capability, blob_object_list_digest);
        }
    );
}
