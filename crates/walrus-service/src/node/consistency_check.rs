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

#[cfg(msim)]
use super::storage::ShardStorage;
use super::{
    NodeStatus,
    StorageNodeInner,
    blob_sync::BlobSyncHandler,
    storage::blob_info::{
        BlobInfoIterator,
        PerObjectBlobInfo,
        PerObjectBlobInfoIterator,
        PerObjectPooledBlobInfo,
        PerObjectPooledBlobInfoIterator,
    },
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
            enable_sliver_data_existence_check: true,
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
///
/// This must be called after garbage collection's blob-info cleanup has completed for `epoch`.
/// The aggregate-blob-info iterator's `is_certified` filter relies on counters that GC
/// decrements for newly-expired deletable and pooled blobs (see `BlobInfoV2::is_certified`),
/// so the digest depends on whether GC has run. Taking the snapshot post-GC keeps the digest
/// deterministic across nodes and across replay of `EpochChangeStart` after a crash.
pub(super) async fn schedule_background_consistency_check(
    node: Arc<StorageNodeInner>,
    blob_sync_handler: Arc<BlobSyncHandler>,
    epoch: Epoch,
) -> Result<()> {
    let node_status = node.storage.node_status()?;
    let node = node.clone();
    let blob_sync_handler = blob_sync_handler.clone();
    let (tx, rx) = oneshot::channel();

    // Snapshot the active shard storages once, here in async context, so that the existence check
    // does not need to take the shard-map read lock from inside the `spawn_blocking` closure.
    // Acquiring that lock there (through `futures::executor::block_on`) deadlocks the
    // single-threaded simulator against a pending shard-removal writer.
    #[cfg(msim)]
    let active_shard_storages = node.active_shard_storages_snapshot().await;

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

        // Create a per-object pooled blob info iterator that takes the current blob info table as
        // the snapshot.
        let per_object_pooled_blob_info_iterator = node
            .storage
            .certified_per_object_pooled_blob_info_iter_before_epoch(epoch);

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
            #[cfg(msim)]
            &active_shard_storages,
        );

        compose_certified_object_blob_list_digest(
            node.clone(),
            per_object_blob_info_iterator,
            epoch,
        );

        compose_certified_pooled_object_blob_list_digest(
            node.clone(),
            per_object_pooled_blob_info_iterator,
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

/// Number of distinct epoch buckets used for consistency-check Prometheus labels.
///
/// Metrics like `walrus_blob_info_consistency_check{epoch="N"}` use `epoch % epoch_bucket_count()`
/// as the label so that Prometheus label cardinality stays bounded. A bucket is reused (and its
/// old value overwritten) every `epoch_bucket_count()` epochs.
///
/// Antithesis runs override this via `WALRUS_EPOCH_BUCKET_COUNT` to prevent the cross-node
/// observer from seeing bucket reuse as a spurious cross-node divergence on long test runs.
fn epoch_bucket_count() -> u32 {
    static VALUE: std::sync::OnceLock<u32> = std::sync::OnceLock::new();
    *VALUE.get_or_init(|| {
        std::env::var("WALRUS_EPOCH_BUCKET_COUNT")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|&v| v > 0)
            .unwrap_or(10)
    })
}

pub(super) fn get_epoch_bucket(epoch: Epoch) -> String {
    (epoch % epoch_bucket_count()).to_string()
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
    #[cfg(msim)] active_shard_storages: &[Arc<ShardStorage>],
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
                            // TODO(WAL-1240): ideally, we should create iterators over the sliver
                            // column families, and perform sequential scan along with
                            // BlobInfoIterator to conduct more efficient existence check. This
                            // requires the SafeIterator to support seek() functionality first.
                            rt.block_on(node.is_stored_at_all_active_shards(&blob_info.0)),
                            &mut total_fully_stored,
                            &mut existence_check_error,
                            &blob_info.0,
                        );
                    }

                    // msim runs the whole consistency check inside a `spawn_blocking` closure that,
                    // unlike production, executes on the single simulator task runner rather than a
                    // real blocking thread. Driving an async existence check here with
                    // `futures::executor::block_on` would park that runner while the future waits
                    // for the shard-map read lock, deadlocking against a pending shard-removal
                    // writer that needs the same runner to proceed. Instead we check the
                    // pre-captured `active_shard_storages` snapshot synchronously, so no lock is
                    // acquired and the runner is never parked. Note that simtest is mostly for
                    // correctness verification, so performance is not a concern.
                    #[cfg(msim)]
                    {
                        handle_existence_check_result(
                            node.is_stored_at_active_shards_snapshot(
                                &blob_info.0,
                                active_shard_storages,
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
    #[cfg(msim)] active_shard_storages: &[Arc<ShardStorage>],
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
        #[cfg(msim)]
        active_shard_storages,
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

/// Compose the digest of the object list returned by the iterator.
///
/// Works for any per-object table keyed by `ObjectID` (the regular and the pooled per-object blob
/// info tables). When `extra_hash_input` is `Some`, the bytes it returns for each value are folded
/// into the digest in addition to the key; this lets callers include value fields (such as the
/// storage pool ID for pooled blobs) that must also be consistent across nodes. Callers that only
/// need the key to be consistent pass `None`. `scan_counter` keeps track of the number of objects
/// scanned.
fn compose_blob_object_list_digest<B, T, E, F, I>(
    blob_info_iter: I,
    epoch: Epoch,
    scan_counter: &IntCounterVec,
    extra_hash_input: Option<F>,
) -> Result<u64, TypedStoreError>
where
    B: AsRef<[u8]>,
    E: AsRef<[u8]>,
    F: Fn(&T) -> E,
    I: Iterator<Item = Result<(B, T), TypedStoreError>>,
{
    let epoch_bucket = get_epoch_bucket(epoch);

    // xxhash is not a cryptographic hash function, but it is fast, has good collision
    // resistance, and is consistent across all platforms.
    let mut hasher = twox_hash::XxHash64::with_seed(u64::from(epoch));
    for item in blob_info_iter {
        match item {
            Ok(blob_info) => {
                hasher.write(blob_info.0.as_ref());
                if let Some(extra_hash_input) = &extra_hash_input {
                    hasher.write(extra_hash_input(&blob_info.1).as_ref());
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
        // The regular per-object table only needs the object key to be consistent across nodes,
        // so no extra per-value bytes are folded into the digest. This keeps the digest identical
        // to before this function was generalized, preserving cross-node compatibility with nodes
        // running the pre-generalization code.
        None::<fn(&PerObjectBlobInfo) -> [u8; 0]>,
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

/// Compose the digest of the certified pooled object blob list.
fn compose_certified_pooled_object_blob_list_digest(
    node: Arc<StorageNodeInner>,
    per_object_pooled_blob_info_iter: PerObjectPooledBlobInfoIterator,
    epoch: Epoch,
) {
    let _scope = monitored_scope::monitored_scope(
        "EpochChange::background_certified_pooled_blob_object_info_consistency_check",
    );
    let epoch_bucket = get_epoch_bucket(epoch);

    let blob_object_list_digest = match compose_blob_object_list_digest(
        per_object_pooled_blob_info_iter,
        epoch,
        &node
            .metrics
            .per_object_pooled_blob_info_consistency_check_certified_scanned,
        // Pooled blobs additionally fold the storage pool ID into the digest so that a node
        // disagreeing on which pool a blob object belongs to is detected as an inconsistency.
        Some(|info: &PerObjectPooledBlobInfo| info.storage_pool_id()),
    ) {
        Ok(value) => value,
        Err(error) => {
            tracing::warn!(?error, "error when processing per object pooled blob info");
            node.metrics
                .per_object_pooled_blob_info_consistency_check_error
                .inc();
            return;
        }
    };

    tracing::info!(
        epoch,
        certified_blob_hash = blob_object_list_digest,
        "background per-object pooled blob info consistency check finished"
    );
    #[allow(clippy::cast_possible_wrap)] // wrapping is fine here
    walrus_utils::with_label!(
        node.metrics.per_object_pooled_blob_info_consistency_check,
        epoch_bucket
    )
    .set(blob_object_list_digest as i64);

    // No-op out side of simtest.
    sui_macros::fail_point_arg!(
        "storage_node_certified_pooled_blob_object_digest",
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

#[cfg(test)]
mod tests {
    use prometheus::{IntCounterVec, Opts};
    use sui_types::base_types::ObjectID;
    use typed_store::TypedStoreError;
    use walrus_core::Epoch;

    use super::compose_blob_object_list_digest;
    use crate::node::storage::blob_info::PerObjectPooledBlobInfo;

    fn test_scan_counter() -> IntCounterVec {
        IntCounterVec::new(
            Opts::new("test_scan_counter", "scan counter for tests"),
            &["epoch"],
        )
        .expect("failed to create test scan counter")
    }

    /// Computes the pooled per-object digest the same way the consistency check does, folding in
    /// the storage pool ID of each entry.
    fn pooled_digest(entries: Vec<(ObjectID, PerObjectPooledBlobInfo)>, epoch: Epoch) -> u64 {
        let counter = test_scan_counter();
        compose_blob_object_list_digest(
            entries.into_iter().map(Ok::<_, TypedStoreError>),
            epoch,
            &counter,
            Some(|info: &PerObjectPooledBlobInfo| info.storage_pool_id()),
        )
        .expect("digest computation should succeed")
    }

    /// Two entries with the same object key but different storage pool IDs must produce different
    /// digests, so that a node disagreeing on a blob object's pool is detected as a cross-node
    /// inconsistency. Reusing the same pool ID reproduces the same digest.
    #[test]
    fn pooled_digest_folds_in_storage_pool_id() {
        let object_id = ObjectID::new([7; 32]);
        let blob_id = walrus_core::test_utils::blob_id_from_u64(1);
        let epoch: Epoch = 5;
        let entry = |pool_id| {
            vec![(
                object_id,
                PerObjectPooledBlobInfo::new_for_test(blob_id, 2, pool_id),
            )]
        };

        let digest_pool_a = pooled_digest(entry(ObjectID::new([1; 32])), epoch);
        let digest_pool_b = pooled_digest(entry(ObjectID::new([2; 32])), epoch);

        assert_ne!(
            digest_pool_a, digest_pool_b,
            "digest must differ when an entry's storage pool ID differs"
        );
        assert_eq!(
            digest_pool_a,
            pooled_digest(entry(ObjectID::new([1; 32])), epoch),
            "digest must match for identical object key and storage pool ID"
        );
    }
}
