// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Epoch-boundary in-process serialization of blob info snapshots.
//!
//! When enabled, this module serializes the three blob-info column families in-process at the
//! post-GC-phase-1 epoch boundary and removes the previous epoch's snapshot, keeping at most one.
//! The size and content digest are reported through metrics and a log line for cross-node
//! comparison.

use std::{
    fs,
    hash::Hasher as _,
    io::{BufWriter, Read as _, Write as _},
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use twox_hash::XxHash64;
use walrus_core::Epoch;

use super::{
    StorageNodeInner,
    storage::blob_info_snapshot::{SnapshotHeader, SnapshotStats},
};
use crate::event::events::EventStreamCursor;

/// Configuration for the blob info snapshot writer.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct BlobInfoSnapshotWriterConfig {
    /// Whether to serialize a blob info snapshot at each epoch boundary.
    ///
    /// When enabled, the node serializes the three blob-info column families in-process at the
    /// post-GC-phase-1 boundary and reports the serialization duration, size, and digest. Note
    /// that disabling this flag leaves the last snapshot file on disk until it is removed
    /// manually.
    pub enabled: bool,
}

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
/// Must be called at the post-GC-phase-1 point while event processing is blocked. `event_cursor`
/// is the position of the `EpochChangeStart` being processed.
pub(super) async fn serialize_snapshot_at_epoch_boundary(
    node: Arc<StorageNodeInner>,
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
    let tmp_path = base_dir.join(format!("snapshot_epoch_{epoch}.bin.tmp"));
    if tmp_path.exists() {
        fs::remove_file(&tmp_path)?;
    }

    let start = Instant::now();
    let storage_node = node.clone();
    let serialize_tmp_path = tmp_path.clone();
    let (stats, digest, size_bytes) =
        tokio::task::spawn_blocking(move || -> Result<(SnapshotStats, u64, u64)> {
            let header = SnapshotHeader::new(epoch, event_cursor);
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
    fs::rename(&tmp_path, &final_path)?;
    // Fsync the directory so the rename is durable: the `sync_all` above flushes the file
    // contents, but not the parent directory entry that the rename created.
    sync_dir(&base_dir)?;
    // Only now that the new snapshot is durably in place, remove older epochs' snapshots so that
    // at most one remains. Deleting them after the rename (rather than before writing) means a
    // write or rename failure leaves the previous snapshot intact instead of dropping it
    // prematurely.
    remove_snapshot_files_matching(&base_dir, |snapshot_epoch| snapshot_epoch != epoch);
    let elapsed = start.elapsed();

    node.metrics
        .blob_info_snapshot_serialize_duration_seconds
        .set(elapsed.as_secs_f64());
    node.metrics
        .blob_info_snapshot_size_bytes
        .set(saturating_i64(size_bytes));
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
    Ok(())
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
    fn config_default_is_disabled() {
        assert!(!BlobInfoSnapshotWriterConfig::default().enabled);
        let parsed: BlobInfoSnapshotWriterConfig =
            serde_yaml::from_str("enabled: true\n").expect("config should deserialize");
        assert!(parsed.enabled);
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
