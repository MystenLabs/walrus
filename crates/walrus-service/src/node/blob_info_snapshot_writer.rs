// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Epoch-boundary database checkpoints for blob info snapshot verification.
//!
//! At the epoch boundary, directly after garbage-collection phase 1 and before any further
//! events are processed, the blob info tables are identical across all honest nodes. When
//! enabled, this module creates a RocksDB checkpoint of the database at exactly that point
//! and removes the previous epoch's checkpoint, so that at most one checkpoint exists at a
//! time.
//!
//! The node itself does not serialize anything: operators serialize and compare the
//! checkpoints offline by running
//! `walrus-node db-tool bench-blob-info-snapshot --db-path <checkpoint>` on each node and
//! comparing the reported snapshot digests for the same epoch. The checkpoint captures the
//! deterministic post-GC-phase-1 state, so the digests must be identical across nodes.
//!
//! This module deliberately does not reuse [`super::db_checkpoint::DbCheckpointManager`]:
//! that subsystem implements wall-clock-periodic backups with retry-on-failure semantics and
//! recommends a separate physical disk. All three properties are wrong here: a retried
//! checkpoint would capture a non-boundary state (events have resumed), a cross-filesystem
//! checkpoint degrades from hard links to a full copy, and the schedule must be the epoch
//! boundary, not an interval. Only the underlying RocksDB checkpoint primitive is shared.

use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use walrus_core::Epoch;

use super::StorageNodeInner;

/// Configuration for the blob info snapshot checkpoint writer.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct BlobInfoSnapshotWriterConfig {
    /// Whether to create a database checkpoint at each epoch boundary.
    ///
    /// The checkpoint directory lives under the storage path and must therefore be on the
    /// same filesystem as the database, so creation uses hard links. Note that disabling
    /// this flag leaves the last checkpoint on disk until it is removed manually.
    pub enabled: bool,
}

/// Returns the directory under which the writer keeps its checkpoints.
pub fn snapshot_base_dir(storage_path: &Path) -> PathBuf {
    storage_path.join("blob_info_snapshots")
}

fn checkpoint_dir_path(base_dir: &Path, epoch: Epoch) -> PathBuf {
    base_dir.join(format!("checkpoint_epoch_{epoch}"))
}

/// Parses the epoch out of a (possibly temporary) checkpoint directory name.
fn checkpoint_epoch(file_name: &str) -> Option<Epoch> {
    file_name
        .strip_suffix(".tmp")
        .unwrap_or(file_name)
        .strip_prefix("checkpoint_epoch_")?
        .parse()
        .ok()
}

/// Creates the database checkpoint for `epoch` at the epoch boundary and removes all older
/// checkpoints.
///
/// Must be called after GC phase 1 has completed for `epoch` and before any further events
/// are processed, so that the checkpoint captures the deterministic post-GC state. The
/// checkpoint creation (memtable flush plus hard-linking every SST file) blocks event
/// processing; its duration is reported through the
/// `blob_info_snapshot_checkpoint_duration_seconds` metric.
pub(super) async fn create_checkpoint_at_epoch_boundary(
    node: Arc<StorageNodeInner>,
    epoch: Epoch,
) -> Result<()> {
    let base_dir = node.blob_info_snapshot_dir.clone();
    fs::create_dir_all(&base_dir)?;
    remove_checkpoints_matching(&base_dir, |checkpoint_epoch| checkpoint_epoch != epoch);

    let final_path = checkpoint_dir_path(&base_dir, epoch);
    if final_path.exists() {
        // Already created, e.g., because the epoch change event is being reprocessed after a
        // restart.
        tracing::debug!(
            walrus.epoch = epoch,
            "blob info snapshot checkpoint already exists"
        );
        return Ok(());
    }
    let tmp_path = base_dir.join(format!("checkpoint_epoch_{epoch}.tmp"));
    if tmp_path.exists() {
        fs::remove_dir_all(&tmp_path)?;
    }

    let start = Instant::now();
    let storage_node = node.clone();
    let checkpoint_tmp_path = tmp_path.clone();
    tokio::task::spawn_blocking(move || {
        storage_node
            .storage
            .checkpoint_database(&checkpoint_tmp_path)
    })
    .await
    .context("checkpoint creation task panicked")?
    .context("failed to create the blob info snapshot checkpoint")?;
    fs::rename(&tmp_path, &final_path)?;
    let elapsed = start.elapsed();

    node.metrics
        .blob_info_snapshot_checkpoint_duration_seconds
        .set(elapsed.as_secs_f64());
    tracing::info!(
        walrus.epoch = epoch,
        ?elapsed,
        path = %final_path.display(),
        "created blob info snapshot checkpoint"
    );
    Ok(())
}

/// Finishes the checkpoint cleanup at startup, in case the node crashed before or during the
/// epoch-boundary cleanup: removes temporary checkpoints and keeps only the latest one.
///
/// Without this, a stale checkpoint would survive until the next epoch boundary and keep
/// hard links to SST files that the live database has long deleted, pinning their disk space
/// for up to a full epoch.
pub(super) fn spawn_startup_cleanup(node: Arc<StorageNodeInner>) {
    if !node.blob_info_snapshot_config.enabled {
        return;
    }
    tokio::spawn(async move {
        let base_dir = node.blob_info_snapshot_dir.clone();
        if base_dir.exists() {
            remove_stale_checkpoints(&base_dir);
        }
    });
}

/// Removes all temporary checkpoints and all checkpoints older than the latest one.
fn remove_stale_checkpoints(base_dir: &Path) {
    let latest_epoch = checkpoint_entries(base_dir)
        .filter(|(name, _)| !name.ends_with(".tmp"))
        .filter_map(|(name, _)| checkpoint_epoch(&name))
        .max();
    for (name, path) in checkpoint_entries(base_dir) {
        let is_stale = name.ends_with(".tmp")
            || checkpoint_epoch(&name).is_some_and(|epoch| Some(epoch) < latest_epoch);
        if is_stale && let Err(error) = fs::remove_dir_all(&path) {
            tracing::warn!(
                ?error,
                path = %path.display(),
                "failed to remove stale blob info snapshot checkpoint"
            );
        }
    }
}

/// Removes all checkpoints (including temporary ones) whose epoch matches `should_remove`.
fn remove_checkpoints_matching(base_dir: &Path, should_remove: impl Fn(Epoch) -> bool) {
    for (name, path) in checkpoint_entries(base_dir) {
        if checkpoint_epoch(&name).is_some_and(&should_remove)
            && let Err(error) = fs::remove_dir_all(&path)
        {
            tracing::warn!(
                ?error,
                path = %path.display(),
                "failed to remove blob info snapshot checkpoint"
            );
        }
    }
}

/// Returns the names and paths of all checkpoint directories under `base_dir`.
fn checkpoint_entries(base_dir: &Path) -> impl Iterator<Item = (String, PathBuf)> {
    fs::read_dir(base_dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| {
            let name = entry.file_name().to_str()?.to_string();
            checkpoint_epoch(&name)?;
            Some((name, entry.path()))
        })
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
    fn checkpoint_epoch_parses_directory_names() {
        assert_eq!(checkpoint_epoch("checkpoint_epoch_7"), Some(7));
        assert_eq!(checkpoint_epoch("checkpoint_epoch_7.tmp"), Some(7));
        assert_eq!(checkpoint_epoch("unrelated"), None);
        assert_eq!(checkpoint_epoch("checkpoint_epoch_x"), None);
    }

    #[test]
    fn boundary_cleanup_removes_other_epochs() -> Result<()> {
        let dir = tempdir()?;
        let base = dir.path();
        fs::create_dir(checkpoint_dir_path(base, 3))?;
        fs::create_dir(checkpoint_dir_path(base, 4))?;
        fs::create_dir(base.join("checkpoint_epoch_4.tmp"))?;
        fs::write(base.join("unrelated.file"), b"keep me")?;

        remove_checkpoints_matching(base, |epoch| epoch != 4);

        assert!(!checkpoint_dir_path(base, 3).exists());
        assert!(checkpoint_dir_path(base, 4).exists());
        // The temporary directory for epoch 4 is also kept by this filter; it is replaced by
        // the checkpoint creation itself and removed by the startup cleanup otherwise.
        assert!(base.join("checkpoint_epoch_4.tmp").exists());
        assert!(base.join("unrelated.file").exists());
        Ok(())
    }

    #[test]
    fn startup_cleanup_keeps_only_the_latest_checkpoint() -> Result<()> {
        let dir = tempdir()?;
        let base = dir.path();
        fs::create_dir(checkpoint_dir_path(base, 3))?;
        fs::create_dir(checkpoint_dir_path(base, 5))?;
        fs::create_dir(base.join("checkpoint_epoch_6.tmp"))?;
        fs::write(base.join("unrelated.file"), b"keep me")?;

        remove_stale_checkpoints(base);

        assert!(!checkpoint_dir_path(base, 3).exists());
        assert!(checkpoint_dir_path(base, 5).exists());
        assert!(!base.join("checkpoint_epoch_6.tmp").exists());
        assert!(base.join("unrelated.file").exists());
        Ok(())
    }
}
