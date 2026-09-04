// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Epoch-boundary in-process serialization of blob info snapshots.
//!
//! When enabled, this module serializes the three blob-info column families in-process at the
//! post-GC-phase-1 epoch boundary and removes the previous epoch's snapshot, keeping at most one.
//! The size and content digest are reported through metrics and a log line for cross-node
//! comparison. Once the node's shard ownership has moved to the new epoch, the snapshot is
//! encoded to report its blob ID and, when certification is enabled, stored and attested on chain
//! (see [`publish_snapshot_after_epoch_change`]).

#[cfg(msim)]
use std::{collections::HashMap, sync::Mutex};
use std::{
    fs,
    hash::Hasher as _,
    io::{BufWriter, Read as _, Write as _},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use futures::future::try_join_all;
use serde::{Deserialize, Serialize};
#[cfg(msim)]
use sui_types::base_types::ObjectID;
use twox_hash::XxHash64;
#[cfg(msim)]
use walrus_core::BlobId;
use walrus_core::{
    DEFAULT_ENCODING,
    Epoch,
    Sliver,
    SliverPairIndex,
    encoding::{EncodingFactory as _, SliverPair},
    metadata::VerifiedBlobMetadataWithId,
};
use walrus_sui::client::BlobObjectMetadata;

use super::{
    StorageNodeInner,
    errors::StoreSliverError,
    storage::{
        SnapshotPublication,
        SnapshotPublicationState,
        blob_info_snapshot::{SnapshotHeader, SnapshotStats},
    },
};
use crate::event::events::EventStreamCursor;

/// Configuration for the blob info snapshot writer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct BlobInfoSnapshotWriterConfig {
    /// Whether to serialize a blob info snapshot at each epoch boundary.
    /// Defaults to `true`; set it explicitly to `false` to disable.
    ///
    /// When enabled, the node serializes the three blob-info column families in-process at the
    /// post-GC-phase-1 boundary and reports the serialization duration, size, and digest, then
    /// encodes the snapshot to report its blob ID. Note that disabling this flag leaves the
    /// last snapshot file on disk until it is removed manually.
    pub enabled: bool,
    /// Whether to certify the serialized snapshot on chain.
    ///
    /// The snapshot is encoded at every epoch boundary regardless, to report its blob ID.
    /// When this is enabled (together with `enabled`), the node additionally stores its own
    /// shards' slivers and attests the snapshot blob through the system contract, synchronously
    /// once the epoch change has been applied locally. Has no effect if `enabled` is false.
    pub certify: bool,
}

impl Default for BlobInfoSnapshotWriterConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            certify: false,
        }
    }
}

/// Bound on the background chain read that reports the epoch of the latest certified snapshot.
/// The read is best-effort and runs off the epoch-change path; the bound keeps a stuck full node
/// from holding the task open indefinitely.
const CHAIN_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Number of epoch buckets used as the label of `blob_info_snapshot_blob_id`.
///
/// The label is `epoch % SNAPSHOT_EPOCH_BUCKET_COUNT`, which bounds the distinct series a node
/// ever creates. Bucketing per se is not safe: it is safe here only because the metric is reset
/// before each write, so a node exports exactly one bucket, the one of its latest snapshot.
///
/// Two nodes therefore share a bucket only when their epochs are congruent modulo this count,
/// which means either the same epoch, a correct comparison, or a divergence of exactly a
/// multiple of it, a false mismatch. That takes a node running for this many epochs without
/// writing a snapshot, and it heals as soon as the node writes one again.
///
/// Without the reset, a node would keep every bucket it has ever written, every shared bucket
/// would compare values from epochs a multiple apart, and recovering the node would not clear
/// them until it had run for a further `SNAPSHOT_EPOCH_BUCKET_COUNT` epochs. Do not export more
/// than the current bucket without raising this count accordingly.
const SNAPSHOT_EPOCH_BUCKET_COUNT: Epoch = 100;

/// Returns the directory under which the writer keeps its snapshots.
pub fn snapshot_base_dir(storage_path: &Path) -> PathBuf {
    storage_path.join("blob_info_snapshots")
}

fn snapshot_file_path(base_dir: &Path, epoch: Epoch) -> PathBuf {
    base_dir.join(format!("snapshot_epoch_{epoch}.bin"))
}

/// Parses the epoch out of a (possibly temporary) snapshot file name.
fn snapshot_file_epoch(file_name: &str) -> Option<Epoch> {
    file_name
        .strip_suffix(".tmp")
        .unwrap_or(file_name)
        .strip_prefix("snapshot_epoch_")?
        .strip_suffix(".bin")?
        .parse()
        .ok()
}

fn saturating_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// Fsyncs a directory so that a preceding rename or create within it survives a crash.
fn sync_dir(dir: &Path) -> std::io::Result<()> {
    fs::File::open(dir)?.sync_all()
}

/// Computes the xxhash64 (seed 0) of a file's full contents, read in streaming chunks.
///
/// The digest is a separate pass over the finished file, not teed inline while writing. It is
/// observability only: logged and compared across nodes, not stored in the file.
fn hash_file(path: &Path) -> std::io::Result<u64> {
    let mut file = fs::File::open(path)?;
    // seed 0 is part of the digest contract: readers recompute the hash with it.
    let mut hasher = XxHash64::with_seed(0);
    let mut buffer = [0u8; 1 << 16];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.write(&buffer[..read]);
    }
    Ok(hasher.finish())
}

/// Serializes the three blob info column families in-process at the epoch boundary, reports the
/// duration, size, and digest, and removes older snapshot files.
///
/// Must be called at the post-GC-phase-1 point while event processing is blocked, and before
/// `execute_epoch_change` spawns the finisher that marks the event complete: the durable file
/// must exist before the boundary can stop being replayed, so a crash never skips a snapshot.
/// `event_cursor` is the position of the `EpochChangeStart` being processed. Everything derived
/// from the file (encoding, storing, attesting) happens in
/// [`publish_snapshot_after_epoch_change`].
pub(super) async fn serialize_snapshot_at_epoch_boundary(
    node: &Arc<StorageNodeInner>,
    epoch: Epoch,
    event_cursor: EventStreamCursor,
) -> Result<()> {
    let base_dir = node.blob_info_snapshot_dir.clone();
    fs::create_dir_all(&base_dir)?;

    let final_path = snapshot_file_path(&base_dir, epoch);
    if final_path.exists() {
        // Already created, e.g., because the epoch change event is being reprocessed after a
        // restart. Still drop any older snapshots so that at most one remains.
        tracing::debug!(walrus.epoch = epoch, "blob info snapshot already exists");
        remove_snapshot_files_matching(&base_dir, |snapshot_epoch| snapshot_epoch != epoch);
        return Ok(());
    }
    write_snapshot_file(node, epoch, event_cursor, &base_dir, &final_path).await
}

/// Encodes the durable snapshot of `epoch` to report its blob ID and, when certification is
/// enabled, stores this node's slivers and attests it on chain, reporting errors through the log
/// and metrics without failing the epoch change.
///
/// Must be called after `execute_epoch_change` has applied the epoch change locally, i.e., after
/// the committee service has advanced to `epoch` and the storage holds this node's shards for
/// that epoch. The contract tallies attestations by the new committee's shard weights and readers
/// route the certified blob by the new committee's shard assignment, so the slivers must be stored
/// under that assignment: storing them at the boundary, under the outgoing assignment, would leave
/// them where the new epoch's shard sync deliberately does not look (it skips blobs certified in
/// the epoch it is syncing, which are expected to have been written to their new owners
/// directly). Runs whether the file was just written or already existed (a replayed boundary).
/// Deliberately synchronous, like the serialization, to measure the full inline cost.
///
/// TODO(WAL-1252): resume an interrupted publication here once snapshots are published and
/// certified.
pub(super) async fn publish_snapshot_after_epoch_change(
    node: &Arc<StorageNodeInner>,
    epoch: Epoch,
) {
    let final_path = snapshot_file_path(&node.blob_info_snapshot_dir, epoch);
    let Some((sliver_pairs, verified_metadata)) = encode_snapshot(node, epoch, &final_path).await
    else {
        return;
    };
    if node.blob_info_snapshot_config.certify {
        certify_snapshot(node, epoch, &sliver_pairs, &verified_metadata).await;
    }
}

/// Serializes the snapshot to `final_path` durably (write, fsync, rename, fsync dir), removes
/// older snapshot files, and reports the duration, size, and digest.
async fn write_snapshot_file(
    node: &Arc<StorageNodeInner>,
    epoch: Epoch,
    event_cursor: EventStreamCursor,
    base_dir: &Path,
    final_path: &Path,
) -> Result<()> {
    let tmp_path = base_dir.join(format!("snapshot_epoch_{epoch}.bin.tmp"));
    if tmp_path.exists() {
        fs::remove_file(&tmp_path)?;
    }

    let start = Instant::now();
    let storage_node = node.clone();
    let serialize_tmp_path = tmp_path.clone();
    let (stats, digest, size_bytes) =
        tokio::task::spawn_blocking(move || -> Result<(SnapshotStats, u64, u64)> {
            // A snapshot is taken right after processing the `EpochChangeStart` event, which always
            // has an event id, so the cursor's event id is never the genesis `None`.
            let event_id = event_cursor.event_id.expect(
                "snapshot is taken after the EpochChangeStart event, which has an event id",
            );
            let header = SnapshotHeader::new(epoch, event_id, event_cursor.element_index);
            let file = fs::File::create(&serialize_tmp_path)?;
            let mut buf_writer = BufWriter::with_capacity(1 << 20, file);
            let stats = storage_node
                .storage
                .write_blob_info_snapshot(&header, &mut buf_writer)?;
            buf_writer.flush()?;
            buf_writer
                .into_inner()
                .context("failed to flush the snapshot file")?
                .sync_all()?;
            // Compute the digest as a separate pass over the finished file, decoupled from writing.
            let digest = hash_file(&serialize_tmp_path)?;
            let size_bytes = fs::metadata(&serialize_tmp_path)?.len();
            Ok((stats, digest, size_bytes))
        })
        .await
        .context("snapshot serialization task panicked")??;
    fs::rename(&tmp_path, final_path)?;
    // Fsync the directory so the rename is durable: the `sync_all` above flushes the file
    // contents, but not the parent directory entry that the rename created.
    sync_dir(base_dir)?;
    // Only now that the new snapshot is durably in place, remove older epochs' snapshots so that
    // at most one remains. Deleting them after the rename (rather than before writing) means a
    // write or rename failure leaves the previous snapshot intact instead of dropping it
    // prematurely.
    remove_snapshot_files_matching(base_dir, |snapshot_epoch| snapshot_epoch != epoch);
    let elapsed = start.elapsed();

    node.metrics
        .blob_info_snapshot_serialize_duration_seconds
        .set(elapsed.as_secs_f64());
    node.metrics
        .blob_info_snapshot_size_bytes
        .set(saturating_i64(size_bytes));
    // Expose the digest per epoch so an off-node observer can compare it across nodes; the label is
    // bucketed like the consistency-check hashes to keep Prometheus cardinality bounded.
    #[allow(clippy::cast_possible_wrap)] // reinterpreting the hash bits as i64 is fine
    walrus_utils::with_label!(
        node.metrics.per_object_blob_info_snapshot_digest,
        super::consistency_check::get_epoch_bucket(epoch)
    )
    .set(digest as i64);
    let digest_hex = format!("{digest:016x}");
    tracing::info!(
        walrus.epoch = epoch,
        ?elapsed,
        size_bytes,
        per_object = stats.per_object_count,
        per_object_pooled = stats.per_object_pooled_count,
        storage_pool = stats.storage_pool_count,
        digest = %digest_hex,
        path = %final_path.display(),
        "serialized blob info snapshot in-process"
    );

    // No-op outside of simtest.
    sui_macros::fail_point_arg!(
        "storage_node_blob_info_snapshot_digest",
        |digest_map: Arc<Mutex<HashMap<Epoch, HashMap<ObjectID, u64>>>>| {
            digest_map
                .lock()
                .expect("failed to lock the digest map")
                .entry(epoch)
                .or_default()
                .insert(node.node_capability, digest);
        }
    );
    Ok(())
}

/// Certifies the snapshot on chain, reporting errors through the log and metrics without
/// failing the epoch change.
async fn certify_snapshot(
    node: &Arc<StorageNodeInner>,
    epoch: Epoch,
    sliver_pairs: &[SliverPair],
    verified_metadata: &VerifiedBlobMetadataWithId,
) {
    if let Err(error) = try_certify_snapshot(node, epoch, sliver_pairs, verified_metadata).await {
        // TODO(WAL-1342): benign contract aborts (a late attestation, a committee change, a
        // replayed boundary) are counted as errors here; classify them as the event blob writer
        // does.
        node.metrics.blob_info_snapshot_certify_error_total.inc();
        tracing::warn!(
            ?error,
            walrus.epoch = epoch,
            "failed to certify the blob info snapshot"
        );
    }
}

/// Stores this node's slivers and the blob metadata, then attests the snapshot blob through the
/// system contract.
async fn try_certify_snapshot(
    node: &Arc<StorageNodeInner>,
    epoch: Epoch,
    sliver_pairs: &[SliverPair],
    verified_metadata: &VerifiedBlobMetadataWithId,
) -> Result<()> {
    // Only committee members can certify (the contract enforces membership), so skip the
    // storage work and the doomed transaction locally, mirroring the event blob writer. This
    // runs after the epoch change has been applied locally, so the committee service reports
    // the committee of `epoch`: a node joining the committee at `epoch` attests and stores
    // its new shards' slivers, and a node leaving at `epoch` skips, matching what the contract
    // would decide. Note that a node processing this boundary late (after the chain moved past
    // `epoch`) cannot be detected locally, since the committee service tracks the node's own
    // processed position; the contract rejects such an attestation with `EInvalidIdEpoch`, and
    // the catching-up and reprocessing cases never reach this code (see `should_serialize` at
    // the call site in `epoch_change.rs`).
    if !node
        .committee_service
        .active_committees()
        .current_committee()
        .contains(node.public_key())
    {
        tracing::debug!(
            walrus.epoch = epoch,
            "node is not in the committee; skipping blob info snapshot certification"
        );
        return Ok(());
    }

    // Record the publication before the first write, so that whatever gets stored is tracked
    // and reconciled at the next epoch boundary even if the store or the attestation fails
    // partway (see `reconcile_previous_publication`). This overwrites the single publication
    // record, which is safe because the boundary handler reconciles the previous publication
    // before publishing and fails the epoch change if that reconciliation errors.
    let blob_id = *verified_metadata.blob_id();
    let record = SnapshotPublication::new(epoch, blob_id);
    node.storage()
        .set_snapshot_publication(&record)
        .context("failed to record the snapshot publication")?;

    // TODO(WAL-1340): until the blob-info entry exists, garbage collection can delete these
    // bytes if a stale entry for the same blob ID is left by another registration; make it
    // honor the publication record.
    let store_start = Instant::now();
    node.storage()
        .put_verified_metadata_without_blob_info(verified_metadata)
        .context("failed to store the snapshot blob metadata")?;
    store_own_slivers(node, verified_metadata, sliver_pairs).await?;
    let store_elapsed = store_start.elapsed();
    node.storage()
        .set_snapshot_publication(&record.with_state(SnapshotPublicationState::Stored))
        .context("failed to record the snapshot publication")?;
    // No-op outside of simtest.
    sui_macros::fail_point_arg!(
        "storage_node_blob_info_snapshot_stored",
        |stored_map: Arc<Mutex<HashMap<Epoch, HashMap<ObjectID, BlobId>>>>| {
            stored_map
                .lock()
                .expect("failed to lock the stored map")
                .entry(epoch)
                .or_default()
                .insert(node.node_capability, blob_id);
        }
    );

    let certify_start = Instant::now();
    let blob_metadata: BlobObjectMetadata = verified_metadata
        .try_into()
        .context("failed to convert the snapshot blob metadata")?;
    node.contract_service
        .certify_snapshot_blob(blob_metadata, epoch, node.node_capability())
        .await?;
    let certify_elapsed = certify_start.elapsed();
    node.storage()
        .set_snapshot_publication(&record.with_state(SnapshotPublicationState::Attested))
        .context("failed to record the snapshot publication")?;
    node.metrics
        .blob_info_snapshot_certify_duration_seconds
        .set(certify_elapsed.as_secs_f64());

    tracing::info!(
        walrus.epoch = epoch,
        walrus.blob_id = %blob_id,
        ?store_elapsed,
        ?certify_elapsed,
        "attested blob info snapshot on chain"
    );
    Ok(())
}

/// Reports the epoch of the latest blob info snapshot certified on chain through the
/// `blob_info_snapshot_last_certified_epoch` gauge, so that an alert can detect a network that
/// stops certifying (the distance to the current epoch grows). Every node reports it, whether or
/// not it certifies, since it is a property of the network rather than of the node. A failed read
/// is logged and the gauge keeps its previous value; a contract that predates certification
/// reads as no certification. Runs in a background task at the epoch boundary, since only the
/// gauge depends on the result.
pub(super) async fn report_last_certified_snapshot_epoch(node: &Arc<StorageNodeInner>) {
    match tokio::time::timeout(
        CHAIN_READ_TIMEOUT,
        node.contract_service.last_certified_snapshot_blob(),
    )
    .await
    {
        Ok(Ok(Some(certified))) => node
            .metrics
            .blob_info_snapshot_last_certified_epoch
            .set(i64::from(certified.epoch)),
        Ok(Ok(None)) => {}
        Ok(Err(error)) => tracing::warn!(
            ?error,
            "failed to read the latest certified blob info snapshot"
        ),
        Err(_) => tracing::warn!(
            timeout = ?CHAIN_READ_TIMEOUT,
            "timed out reading the latest certified blob info snapshot"
        ),
    }
}

/// Reconciles the publication of the previous epoch's snapshot at the boundary of
/// `current_epoch`, before this epoch's snapshot is produced.
///
/// Whether the previous snapshot certified is decided locally: a certification during epoch E
/// emits `BlobCertified` before `EpochChangeStart(E + 1)` in the checkpoint-ordered event
/// stream, and the boundary handler drains all blob events before calling this, so a blob-info
/// entry for the snapshot's blob ID exists if and only if it certified. This relies on the
/// contract's lower bound of two epochs on the snapshot lifetime: garbage collection phase 1
/// runs before this and expires blobs whose storage ends at `E + 1`, which a one-epoch snapshot
/// certified in E would, so its entry would be gone before it is checked. A snapshot that did not
/// certify never will (the contract only accepts the current epoch), and its metadata and
/// slivers have no blob-info entry, so garbage collection would never find them: they are
/// deleted here. Why a snapshot did not certify (no quorum, or a divergence of this node's tables
/// from the network's) is not classified here yet; fleet-wide detection comes from comparing the
/// `blob_info_snapshot_blob_id` gauges across nodes (see `TODO(WAL-1341)` below).
///
/// Storage errors are returned to the caller, which fails the epoch change; the boundary is then
/// replayed on restart and this function runs again. It is idempotent: the record is cleared only
/// after the stored data is deleted, and deleting already-deleted data is a no-op.
pub(super) async fn reconcile_previous_publication(
    node: &Arc<StorageNodeInner>,
    current_epoch: Epoch,
) -> Result<()> {
    let Some(record) = node
        .storage()
        .snapshot_publication()
        .context("failed to read the snapshot publication record")?
    else {
        return Ok(());
    };
    let epoch = record.epoch();
    if epoch >= current_epoch {
        // The current epoch's publication (a replayed boundary) or, defensively, a newer one.
        return Ok(());
    }
    let blob_id = record.blob_id();
    // Lets a simtest crash the node here, where a storage error in the lookup below would stop
    // it, to exercise the replay of this boundary. No-op outside of simtest.
    sui_macros::fail_point_async!("storage_node_blob_info_snapshot_reconcile");
    if node.is_blob_certified(&blob_id)? {
        // Certified: from here on the blob is ordinary certified data owned by garbage
        // collection. The metadata was stored before the blob had a blob-info entry, so mark
        // it stored now, as for event blobs, provided it is still there; the node's own slivers
        // were stored the same way and are found by the regular existence checks.
        if node
            .storage()
            .get_metadata(&blob_id)
            .context("failed to read the snapshot blob metadata")?
            .is_some()
        {
            node.storage()
                .update_blob_info_with_metadata(&blob_id)
                .context("failed to mark the certified snapshot's metadata as stored")?;
        }
        node.storage()
            .clear_snapshot_publication()
            .context("failed to clear the snapshot publication record")?;
        // No-op outside of simtest.
        sui_macros::fail_point_arg!(
            "storage_node_blob_info_snapshot_reconciled",
            |reconciled_map: Arc<Mutex<HashMap<Epoch, HashMap<ObjectID, bool>>>>| {
                reconciled_map
                    .lock()
                    .expect("failed to lock the reconciled map")
                    .entry(epoch)
                    .or_default()
                    .insert(node.node_capability, true);
            }
        );
        return Ok(());
    }

    // Never certified as a snapshot. Delete whatever was stored for it, unless a blob-info
    // entry exists for the blob ID: the same content can be registered by anyone, and an entry
    // means the regular lifecycle owns the bytes (garbage collection deletes them once nothing
    // registers the blob), whereas without an entry nothing else would ever find them.
    if node
        .storage()
        .get_blob_info(&blob_id)
        .context("failed to read the snapshot blob info")?
        .is_none()
    {
        node.storage
            .delete_blob_data(&blob_id)
            .await
            .context("failed to delete the uncertified snapshot blob data")?;
    }
    node.metrics
        .blob_info_snapshot_uncertified_cleanup_total
        .inc();
    node.storage()
        .clear_snapshot_publication()
        .context("failed to clear the snapshot publication record")?;
    // No-op outside of simtest.
    sui_macros::fail_point_arg!(
        "storage_node_blob_info_snapshot_reconciled",
        |reconciled_map: Arc<Mutex<HashMap<Epoch, HashMap<ObjectID, bool>>>>| {
            reconciled_map
                .lock()
                .expect("failed to lock the reconciled map")
                .entry(epoch)
                .or_default()
                .insert(node.node_capability, false);
        }
    );

    // TODO(WAL-1341): classify why the snapshot did not certify (no quorum, or a divergence
    // from the snapshot the network certified) from the on-chain history, and expose it through
    // metrics; acting on a divergence is part of the recovery milestone (WAL-1252).
    tracing::warn!(
        walrus.epoch = epoch,
        walrus.blob_id = %blob_id,
        state = ?record.state(),
        "the blob info snapshot was not certified; its stored data is cleaned up"
    );
    Ok(())
}

/// Stores the slivers of the shards assigned to this node in the current committee.
///
/// Only those sliver pairs are touched: the others belong to shards this node never looks up,
/// including shards whose storage is being removed in the background at this boundary. A shard
/// that is assigned but not yet owned by this node (still being synced) is skipped, as in the
/// event blob writer.
async fn store_own_slivers(
    node: &Arc<StorageNodeInner>,
    verified_metadata: &VerifiedBlobMetadataWithId,
    sliver_pairs: &[SliverPair],
) -> Result<()> {
    let n_shards = node.encoding_config().n_shards();
    let own_shards = node
        .committee_service
        .active_committees()
        .current_committee()
        .shards_for_node_public_key(node.public_key())
        .to_vec();
    let own_pairs: Vec<&SliverPair> = sliver_pairs
        .iter()
        .filter(|pair| {
            own_shards.contains(
                &pair
                    .index()
                    .to_shard_index(n_shards, verified_metadata.blob_id()),
            )
        })
        .collect();
    let metadata = Arc::new(verified_metadata.clone());
    store_slivers_of_type(node, &metadata, &own_pairs, |pair| {
        Sliver::Primary(pair.primary.clone())
    })
    .await?;
    store_slivers_of_type(node, &metadata, &own_pairs, |pair| {
        Sliver::Secondary(pair.secondary.clone())
    })
    .await
}

async fn store_slivers_of_type(
    node: &Arc<StorageNodeInner>,
    metadata: &Arc<VerifiedBlobMetadataWithId>,
    sliver_pairs: &[&SliverPair],
    sliver_of_pair: impl Fn(&SliverPair) -> Sliver,
) -> Result<()> {
    try_join_all(sliver_pairs.iter().map(|&sliver_pair| {
        let metadata = metadata.clone();
        let sliver = sliver_of_pair(sliver_pair);
        let index: SliverPairIndex = sliver_pair.index();
        async move {
            match node.store_sliver_unchecked(metadata, index, sliver).await {
                Err(StoreSliverError::ShardNotAssigned(_)) | Ok(_) => Ok(()),
                Err(error) => Err(error),
            }
        }
    }))
    .await
    .context("failed to store the snapshot blob slivers")?;
    Ok(())
}

/// Encodes the snapshot into a Walrus blob and reports its blob ID, logging errors and counting
/// them in metrics without failing the epoch change.
///
/// Returns `None` when the encoding failed, in which case certification cannot proceed either.
async fn encode_snapshot(
    node: &Arc<StorageNodeInner>,
    epoch: Epoch,
    snapshot_path: &Path,
) -> Option<(Vec<SliverPair>, VerifiedBlobMetadataWithId)> {
    match try_encode_snapshot(node, epoch, snapshot_path).await {
        Ok(encoded) => Some(encoded),
        Err(error) => {
            node.metrics.blob_info_snapshot_encode_error_total.inc();
            tracing::warn!(
                ?error,
                walrus.epoch = epoch,
                "failed to encode the blob info snapshot"
            );
            None
        }
    }
}

/// Encodes the snapshot file, reports its blob ID for cross-node comparison, and returns the
/// sliver pairs so that certification can store and attest them.
async fn try_encode_snapshot(
    node: &Arc<StorageNodeInner>,
    epoch: Epoch,
    snapshot_path: &Path,
) -> Result<(Vec<SliverPair>, VerifiedBlobMetadataWithId)> {
    let encoding_config = node.encoding_config().get_for_type(DEFAULT_ENCODING);
    // Encoding is inherent to the protocol, so every valid system can encode. Committees of
    // fewer than four shards cannot: no shard may be faulty, so the encoding is left without
    // recovery symbols, and encoding panics.
    // TODO(WAL-1270): reject such committees where the system is defined, and drop this.
    debug_assert!(
        encoding_config.n_shards().get() >= 4,
        "the system must be able to encode"
    );

    let encode_start = Instant::now();
    let path = snapshot_path.to_path_buf();
    let (sliver_pairs, verified_metadata) = tokio::task::spawn_blocking(move || {
        let content = fs::read(path)?;
        // Lets a simtest make one node's snapshot blob differ from the other nodes', to exercise
        // the divergence detection, without changing the snapshot file on disk.
        #[cfg(msim)]
        let content = {
            let mut diverge = false;
            sui_macros::fail_point_if!("storage_node_blob_info_snapshot_diverge", || {
                diverge = true;
            });
            let mut content = content;
            if diverge {
                content.push(0);
            }
            content
        };
        encoding_config
            .encode_with_metadata(content)
            .map_err(anyhow::Error::from)
    })
    .await
    .context("snapshot encoding task panicked")?
    .context("failed to encode the blob info snapshot")?;
    let encode_elapsed = encode_start.elapsed();
    node.metrics
        .blob_info_snapshot_encode_duration_seconds
        .set(encode_elapsed.as_secs_f64());

    let blob_id = *verified_metadata.blob_id();
    let blob_id_prefix = u64::from_be_bytes(
        blob_id.as_ref()[..8]
            .try_into()
            .expect("blob id has at least 8 bytes"),
    );
    // Reset before setting, so that only the bucket of the latest snapshot is exported. The
    // bucket bound depends on this; see `SNAPSHOT_EPOCH_BUCKET_COUNT`.
    node.metrics.blob_info_snapshot_blob_id.reset();
    #[allow(clippy::cast_possible_wrap)] // reinterpreting the bits as i64 is fine
    walrus_utils::with_label!(
        node.metrics.blob_info_snapshot_blob_id,
        (epoch % SNAPSHOT_EPOCH_BUCKET_COUNT).to_string()
    )
    .set(blob_id_prefix as i64);
    tracing::info!(
        walrus.epoch = epoch,
        walrus.blob_id = %blob_id,
        ?encode_elapsed,
        "encoded blob info snapshot"
    );

    // No-op outside of simtest.
    sui_macros::fail_point_arg!(
        "storage_node_blob_info_snapshot_blob_id",
        |blob_id_map: Arc<Mutex<HashMap<Epoch, HashMap<ObjectID, BlobId>>>>| {
            blob_id_map
                .lock()
                .expect("failed to lock the blob id map")
                .entry(epoch)
                .or_default()
                .insert(node.node_capability, blob_id);
        }
    );
    Ok((sliver_pairs, verified_metadata))
}

/// Removes all snapshot files (including temporary ones) whose epoch matches `should_remove`.
fn remove_snapshot_files_matching(base_dir: &Path, should_remove: impl Fn(Epoch) -> bool) {
    let Ok(entries) = fs::read_dir(base_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if snapshot_file_epoch(&name).is_some_and(&should_remove)
            && let Err(error) = fs::remove_file(entry.path())
        {
            tracing::warn!(
                ?error,
                path = %entry.path().display(),
                "failed to remove blob info snapshot file"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn config_default_is_enabled() {
        // Default is enabled, and an omitted field falls back to it; certification is opt-in.
        assert!(BlobInfoSnapshotWriterConfig::default().enabled);
        assert!(!BlobInfoSnapshotWriterConfig::default().certify);
        let empty: BlobInfoSnapshotWriterConfig =
            serde_yaml::from_str("{}\n").expect("config should deserialize");
        assert!(empty.enabled);
        assert!(!empty.certify);
        // An explicit `enabled: false` still disables it.
        let disabled: BlobInfoSnapshotWriterConfig =
            serde_yaml::from_str("enabled: false\n").expect("config should deserialize");
        assert!(!disabled.enabled);
        assert!(!disabled.certify);
    }

    #[test]
    fn snapshot_file_epoch_parses_file_names() {
        assert_eq!(snapshot_file_epoch("snapshot_epoch_7.bin"), Some(7));
        assert_eq!(snapshot_file_epoch("snapshot_epoch_7.bin.tmp"), Some(7));
        assert_eq!(snapshot_file_epoch("unrelated"), None);
        assert_eq!(snapshot_file_epoch("snapshot_epoch_x.bin"), None);
    }

    #[test]
    fn keep_latest_removes_other_epochs() -> Result<()> {
        let dir = tempdir()?;
        let base = dir.path();
        fs::write(snapshot_file_path(base, 3), b"old")?;
        fs::write(snapshot_file_path(base, 4), b"keep")?;
        fs::write(base.join("snapshot_epoch_4.bin.tmp"), b"tmp")?;
        fs::write(base.join("unrelated.file"), b"keep me")?;

        remove_snapshot_files_matching(base, |epoch| epoch != 4);

        assert!(!snapshot_file_path(base, 3).exists());
        assert!(snapshot_file_path(base, 4).exists());
        // The temporary file for epoch 4 is kept by this filter; the serialization's atomic
        // rename replaces it otherwise.
        assert!(base.join("snapshot_epoch_4.bin.tmp").exists());
        assert!(base.join("unrelated.file").exists());
        Ok(())
    }
}
