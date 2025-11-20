// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Background consistency check for the storage node.

#[cfg(msim)]
use std::{collections::HashMap, sync::Mutex};
use std::{collections::HashSet, hash::Hasher, sync::Arc};

use anyhow::{Context, Result};
use prometheus::IntCounterVec;
use rand::{Rng, SeedableRng, rngs::StdRng};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
#[cfg(msim)]
use sui_types::base_types::ObjectID;
use tokio::sync::oneshot;
use typed_store::TypedStoreError;
use walrus_core::{BlobId, Epoch};
use walrus_utils::metrics::monitored_scope;

use super::{
    NodeStatus, StorageNodeInner,
    blob_sync::BlobSyncHandler,
    storage::blob_info::{BlobInfoIterator, PerObjectBlobInfoIterator},
};

/// Configuration for the consistency check.
#[serde_as]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct StorageNodeConsistencyCheckConfig {
    /// Enable the consistency check for the blob info table.
    pub enable_consistency_check: bool,
    /// Enable an internal invariant check for the blob info tables.
    pub enable_blob_info_invariants_check: bool,
    /// Enable the sliver data existence check.
    pub enable_sliver_data_existence_check: bool,
    /// The sample rate of the sliver data existence check. Value is between 0 and 100.
    #[serde(deserialize_with = "deserialize_data_existence_check_sample_rate_percentage")]
    pub sliver_data_existence_check_sample_rate_percentage: u64,
}

impl Default for StorageNodeConsistencyCheckConfig {
    fn default() -> Self {
        Self {
            enable_consistency_check: true,
            enable_blob_info_invariants_check: false,
            enable_sliver_data_existence_check: false,
            sliver_data_existence_check_sample_rate_percentage: 100,
        }
    }
}

impl StorageNodeConsistencyCheckConfig {
    /// Returns a default configuration for testing.
    pub fn default_for_test() -> Self {
        Self {
            enable_blob_info_invariants_check: true,
            enable_sliver_data_existence_check: true,
            ..Self::default()
        }
    }
}

fn deserialize_data_existence_check_sample_rate_percentage<'de, D>(
    deserializer: D,
) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = u64::deserialize(deserializer)?;
    if value > 100 {
        Err(serde::de::Error::custom(
            "Percentage must be between 0 and 100",
        ))
    } else {
        Ok(value)
    }
}

/// Helper struct to store the result of the blob consistency check.
struct BlobConsistencyCheckResult {
    /// The hash of the blob list.
    blob_list_digest: u64,
    /// The number of fully synced blobs scanned. This does not include blobs that are certified
    /// but not yet fully synced locally.
    total_synced_scanned: u64,
    /// Among blobs that are fully synced, the number of blobs that are fully stored.
    total_fully_stored: u64,
    /// The number of errors when checking the existence of the sliver data.
    existence_check_error: u64,
}

/// Schedule a background task to compute the hash of the list of certified blobs at the
/// beginning of the epoch, and conduct blob sliver data existence check. This is used to detect
/// inconsistencies in the blob info table between the nodes.
pub(super) async fn schedule_background_consistency_check(
    node: Arc<StorageNodeInner>,
    blob_sync_handler: Arc<BlobSyncHandler>,
    epoch: Epoch,
) -> Result<()> {
    let node_status = node.storage.node_status()?;
    let node = node.clone();
    let blob_sync_handler = blob_sync_handler.clone();
    let (tx, rx) = oneshot::channel();

    // Create a background thread which takes the ownership of the iterator and process it.
    tokio::task::spawn_blocking(move || {
        let _scope =
            monitored_scope::monitored_scope("EpochChange::background_blob_info_consistency_check");

        // Create a blob info iterator that takes the current blob info table as the snapshot.
        let blob_info_iterator = node.storage.certified_blob_info_iter_before_epoch(epoch);

        // Create a per-object blob info iterator that takes the current blob info table as the
        // snapshot.
        let per_object_blob_info_iterator = node
            .storage
            .certified_per_object_blob_info_iter_before_epoch(epoch);

        // Unblock event processing.
        let _ = tx.send(());

        // Get the list of blobs that are being synced at the moment. We will skip the existence
        // check for these blobs since they are not yet fully synced locally.
        let blobs_not_yet_fully_synced = blob_sync_handler.blob_sync_in_progress();

        // Right now, the computing the two digests are sequential, given that scanning blob info
        // table is quick. We may consider parallelizing them in the future.
        certified_blob_consistency_check(
            node.clone(),
            blob_info_iterator,
            epoch,
            node_status,
            &blobs_not_yet_fully_synced,
        );

        compose_certified_object_blob_list_digest(
            node.clone(),
            per_object_blob_info_iterator,
            epoch,
        );

        if node
            .consistency_check_config
            .enable_blob_info_invariants_check
        {
            node.storage.blob_info_invariants_check();
        }
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
    is_fully_stored_result: Result<bool, anyhow::Error>, // True means blobs are fully stored.
    total_fully_stored: &mut u64,
    existence_check_error: &mut u64,
    blob_id: &BlobId,
) {
    match is_fully_stored_result {
        Ok(true) => *total_fully_stored += 1,
        Ok(false) => {}
        Err(error) => {
            *existence_check_error += 1;
            tracing::warn!(?error, blob_id=%blob_id, "error when checking sliver data existence");
        }
    }
}

/// Compose the digest of the blob list returned by the iterator, and check the existence of the
/// sliver data of the blobs that are fully synced.
///
/// `scan_counter` keeps track of the number of blobs scanned.
fn compose_blob_list_digest_and_check_sliver_data_existence(
    node: &StorageNodeInner,
    blob_info_iter: BlobInfoIterator<'_>,
    epoch: Epoch,
    node_status: NodeStatus,
    blobs_not_yet_fully_synced: &HashSet<BlobId>,
    scan_counter: &IntCounterVec,
) -> Result<BlobConsistencyCheckResult, TypedStoreError> {
    // Create a new tokio runtime for the async task to check blob existence.
    #[cfg(not(msim))]
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("failed to create tokio runtime");

    let epoch_bucket = get_epoch_bucket(epoch);

    // Using Epoch as the seed for the RNG to make the sampled blob selection deterministic for the
    // same epoch.
    let mut rng = StdRng::seed_from_u64(u64::from(epoch));

    // For data existence check, we should only check it if the node is in Active state. Otherwise,
    // the node may not be fully synced with the latest epoch.
    let mut enable_sliver_data_existence_check = node
        .consistency_check_config
        .enable_sliver_data_existence_check
        && node_status == NodeStatus::Active;

    // xxhash is not a cryptographic hash function, but it is fast, has good collision
    // resistance, and is consistent across all platforms.
    let mut hasher = twox_hash::XxHash64::with_seed(u64::from(epoch));
    let mut total_synced_scanned = 0;
    let mut total_fully_stored = 0;
    let mut existence_check_error = 0;
    for item in blob_info_iter {
        match item {
            Ok(blob_info) => {
                hasher.write(blob_info.0.as_ref());

                // Check if the committee onchain has moved forward. If so, we cannot perform the
                // sliver data existence check because the sliver data may not be fully stored
                // in `epoch` based on the blobs' certified status in committee epoch.
                if enable_sliver_data_existence_check && node.current_committee_epoch() > epoch {
                    enable_sliver_data_existence_check = false;
                    total_synced_scanned = 0;
                    total_fully_stored = 0;
                    tracing::info!(
                        event_epoch = epoch,
                        committee_epoch = node.current_committee_epoch(),
                        "current committee epoch is not the same as the event epoch, skipping \
                        sliver data existence check"
                    );
                }

                if enable_sliver_data_existence_check
                    && !blobs_not_yet_fully_synced.contains(&blob_info.0)
                    && rng.gen_range(0..100)
                        < node
                            .consistency_check_config
                            .sliver_data_existence_check_sample_rate_percentage
                {
                    total_synced_scanned += 1;

                    #[cfg(msim)]
                    let total_fully_stored_before = total_fully_stored;

                    #[cfg(not(msim))]
                    {
                        handle_existence_check_result(
                            // Note that we are running the async function in a new reusable Runtime
                            // created at the beginning of the task. Note that performance of this
                            // task is not a concern, and we should limit the resource this task
                            // can use.
                            //
                            // TODO(zhewu): ideally, we should create iterators over the sliver
                            // column families, and perform sequential scan along with
                            // BlobInfoIterator to conduct more efficient existence check. This
                            // requires the SafeIterator to support seek() functionality first.
                            rt.block_on(node.is_stored_at_all_active_shards(&blob_info.0)),
                            &mut total_fully_stored,
                            &mut existence_check_error,
                            &blob_info.0,
                        );
                    }

                    // Unfortunately, msim does not support tokio::Runtime::block_on, so we need to
                    // use less ideal method to execute the async function that checks blob
                    // data existence. Note that simtest is mostly for correctness verification, so
                    // the performance is not a concern.
                    #[cfg(msim)]
                    {
                        handle_existence_check_result(
                            futures::executor::block_on(
                                node.is_stored_at_all_active_shards(&blob_info.0),
                            ),
                            &mut total_fully_stored,
                            &mut existence_check_error,
                            &blob_info.0,
                        );
                    }

                    #[cfg(msim)]
                    if total_fully_stored_before == total_fully_stored {
                        // This helps to detect which certified blobs are not fully stored.
                        tracing::debug!(
                            blob_id=%blob_info.0,
                            "sliver data consistency check: blob not fully stored"
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

    // Before we return, we need to check if the committee onchain has moved forward. If so, we
    // need to reset the sliver data existence check counters, since the sliver data may not be
    // fully stored in `epoch` based on the blobs' certified status in committee epoch.
    if enable_sliver_data_existence_check && node.current_committee_epoch() > epoch {
        total_synced_scanned = 0;
        total_fully_stored = 0;
        tracing::info!(
            event_epoch = epoch,
            committee_epoch = node.current_committee_epoch(),
            "current committee epoch is not the same as the event epoch, reset sliver data \
            existence check counters"
        );
    }

    Ok(BlobConsistencyCheckResult {
        blob_list_digest: hasher.finish(),
        total_synced_scanned,
        total_fully_stored,
        existence_check_error,
    })
}

/// Compose the digest of the certified blob list, and check the existence of the sliver data of
/// the blobs that are fully synced.
fn certified_blob_consistency_check(
    node: Arc<StorageNodeInner>,
    blob_info_iter: BlobInfoIterator<'_>,
    epoch: Epoch,
    node_status: NodeStatus,
    blobs_not_yet_fully_synced: &HashSet<BlobId>,
) {
    let _scope = monitored_scope::monitored_scope(
        "EpochChange::background_certified_blob_info_consistency_check",
    );
    let epoch_bucket = get_epoch_bucket(epoch);

    match compose_blob_list_digest_and_check_sliver_data_existence(
        node.as_ref(),
        blob_info_iter,
        epoch,
        node_status,
        blobs_not_yet_fully_synced,
        &node.metrics.blob_info_consistency_check_certified_scanned,
    ) {
        Ok(BlobConsistencyCheckResult {
            blob_list_digest,
            total_synced_scanned,
            total_fully_stored,
            existence_check_error,
        }) => {
            tracing::info!(
                epoch,
                certified_blob_hash = blob_list_digest,
                total_synced_scanned,
                total_fully_stored,
                existence_check_error,
                "background blob info consistency check finished"
            );

            #[allow(clippy::cast_possible_wrap)] // wrapping is fine here
            walrus_utils::with_label!(node.metrics.blob_info_consistency_check, epoch_bucket)
                .set(blob_list_digest as i64);

            if total_synced_scanned > 0 {
                let total_stored_percentage =
                    total_fully_stored as f64 / total_synced_scanned as f64;
                walrus_utils::with_label!(
                    node.metrics.node_blob_data_fully_stored_ratio,
                    epoch_bucket
                )
                .set(total_stored_percentage);
                walrus_utils::with_label!(
                    node.metrics
                        .node_blob_data_consistency_check_existence_error,
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
    let mut hasher = twox_hash::XxHash64::with_seed(u64::from(epoch));
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
    let _scope = monitored_scope::monitored_scope(
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

    tracing::info!(
        epoch,
        certified_blob_hash = blob_object_list_digest,
        "background per-object blob info consistency check finished"
    );
    #[allow(clippy::cast_possible_wrap)] // wrapping is fine here
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
