// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Epoch reconciliation bookkeeping. Events mark IDs; a worker computes final lifetimes.
//! All tables share the Walrus RocksDB, so dirty markers commit with reference metadata.

use std::sync::Arc;

use rocksdb::Options;
use serde::{Deserialize, Serialize};
use strata_index::port::IndexDb;
use sui_types::base_types::ObjectID;
use typed_store::{
    Map,
    TypedStoreError,
    rocks::{DBBatch, DBMap, ReadWriteOptions, RocksDB},
};
use walrus_core::{BlobId, Epoch};

use super::DatabaseTableOptionsFactory;

mod coordination;
mod database;

pub(super) const DIRTY_BLOBS_CF: &str = "strata_dirty_blobs";
pub(super) const DIRTY_POOLS_CF: &str = "strata_dirty_pools";
pub(super) const LIFETIMES_CF: &str = "strata_lifetimes";
pub(super) const EPOCH_CF: &str = "strata_reconcile_epoch";
pub(super) const REQUESTED_EPOCH_CF: &str = "strata_requested_epoch";
pub(super) const POOL_REFERENCES_CF: &str = "strata_pool_references";

/// Presence means lifetime reconciliation is pending. A put clears only `delete`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct DirtyBlob {
    pub event_index: u64,
    pub delete: bool,
}

/// A candidate selected from a durable metadata snapshot; live state is checked under its lock.
#[derive(Debug, Clone)]
pub(super) struct ReconcileBlob {
    pub id: BlobId,
    pub end_epoch: Epoch,
    pub dirty: Option<DirtyBlob>,
}

pub(super) struct ReconcileSnapshot {
    pub blobs: Vec<ReconcileBlob>,
    pub pools: Vec<(ObjectID, u64)>,
}

/// Crash-recovery checkpoint for the background reconciler, persisted in Walrus RocksDB.
///
/// A pass moves from `Applying(epoch)` to `Advancing(epoch)` to `Complete(epoch)`, syncing
/// each checkpoint before proceeding. This tracks reconciliation, not Walrus's current epoch:
/// events and puts can continue while the worker finishes an older pass.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(super) enum EpochProgress {
    /// Blob lifetime updates and deletes have started but may be incomplete.
    /// Recovery rescans the metadata and resumes reconciliation for this epoch.
    Applying(Epoch),
    /// The blob updates selected for this pass are durable. Recovery skips the scan and
    /// finishes Strata's epoch advancement (when deletion is enabled), sync and bookkeeping
    /// cleanup. The clock may already have advanced before a crash; this phase can be retried.
    Advancing(Epoch),
    /// The pass, including Strata sync and bookkeeping cleanup, finished durably.
    /// The worker waits unless a newer epoch has been requested.
    Complete(Epoch),
}

#[derive(Debug)]
pub(super) struct StrataLifecycle {
    pub blobs: DBMap<BlobId, DirtyBlob>,
    pub pools: DBMap<ObjectID, u64>,
    // A conservative lifetime used by puts; reconciliation refreshes it from all live references.
    pub lifetimes: DBMap<BlobId, Epoch>,
    // Blob/object -> pool, maintained with the authoritative reference rows. Puts can resolve
    // pool extensions without waiting for reconciliation or scanning unrelated references.
    pub pool_references: DBMap<(BlobId, ObjectID), ObjectID>,
    // Checkpoint of the active or most recently completed reconciliation pass.
    pub progress: DBMap<(), EpochProgress>,
    // Latest requested target; it can move ahead while an older pass is still running.
    pub requested_epoch: DBMap<(), Epoch>,
    pub wake: tokio::sync::Notify,
    pub db: Arc<dyn IndexDb>,
    pub pass: Arc<tokio::sync::Mutex<()>>,
    coordination: Arc<coordination::Coordination>,
}

pub(super) fn options(factory: &DatabaseTableOptionsFactory) -> Vec<(&'static str, Options)> {
    [
        DIRTY_BLOBS_CF,
        DIRTY_POOLS_CF,
        LIFETIMES_CF,
        POOL_REFERENCES_CF,
        EPOCH_CF,
        REQUESTED_EPOCH_CF,
    ]
    .into_iter()
    .map(|name| (name, factory.standard()))
    .collect()
}

pub(super) fn database(database: &Arc<RocksDB>) -> Arc<dyn IndexDb> {
    Arc::new(database::Database(Arc::clone(database)))
}

impl StrataLifecycle {
    pub fn reopen(database: &Arc<RocksDB>) -> Result<Self, TypedStoreError> {
        Ok(Self {
            blobs: DBMap::reopen(
                database,
                Some(DIRTY_BLOBS_CF),
                &ReadWriteOptions::default(),
                false,
            )?,
            pools: DBMap::reopen(
                database,
                Some(DIRTY_POOLS_CF),
                &ReadWriteOptions::default(),
                false,
            )?,
            lifetimes: DBMap::reopen(
                database,
                Some(LIFETIMES_CF),
                &ReadWriteOptions::default(),
                false,
            )?,
            progress: DBMap::reopen(
                database,
                Some(EPOCH_CF),
                &ReadWriteOptions::default(),
                false,
            )?,
            pool_references: DBMap::reopen(
                database,
                Some(POOL_REFERENCES_CF),
                &ReadWriteOptions::default(),
                false,
            )?,
            requested_epoch: DBMap::reopen(
                database,
                Some(REQUESTED_EPOCH_CF),
                &ReadWriteOptions::default(),
                false,
            )?,
            wake: tokio::sync::Notify::new(),
            db: self::database(database),
            pass: Arc::default(),
            coordination: Arc::default(),
        })
    }

    pub fn mark_blob(
        &self,
        batch: &mut DBBatch,
        id: BlobId,
        event_index: u64,
        end_epoch: Option<Epoch>,
        deletion: bool,
        registration: bool,
    ) -> Result<(), TypedStoreError> {
        let previous = self.blobs.get(&id)?;
        batch.insert_batch(
            &self.blobs,
            [(
                id,
                DirtyBlob {
                    event_index,
                    delete: deletion || (!registration && previous.is_some_and(|row| row.delete)),
                },
            )],
        )?;
        if let Some(end_epoch) = end_epoch {
            let end_epoch = end_epoch.max(self.lifetimes.get(&id)?.unwrap_or_default());
            batch.insert_batch(&self.lifetimes, [(id, end_epoch)])?;
        }
        Ok(())
    }

    pub fn is_empty(&self) -> Result<bool, TypedStoreError> {
        Ok(self.blobs.safe_iter()?.next().transpose()?.is_none()
            && self.pools.safe_iter()?.next().transpose()?.is_none()
            && self.lifetimes.safe_iter()?.next().transpose()?.is_none()
            && self
                .pool_references
                .safe_iter()?
                .next()
                .transpose()?
                .is_none())
    }
}

pub(super) fn error(error: impl std::fmt::Display) -> TypedStoreError {
    TypedStoreError::TaskError(format!("Strata lifecycle: {error}"))
}

/// Declare after the lock: any uncertain write must halt admission before unlocking.
pub(super) struct HaltOnIncompleteWrite {
    lifecycle: Arc<StrataLifecycle>,
    complete: bool,
}

impl HaltOnIncompleteWrite {
    pub fn new(lifecycle: Arc<StrataLifecycle>) -> Self {
        Self {
            lifecycle,
            complete: false,
        }
    }
    pub fn complete(&mut self) {
        self.complete = true;
    }
}

impl Drop for HaltOnIncompleteWrite {
    fn drop(&mut self) {
        if !self.complete {
            self.lifecycle
                .halt("write did not complete; reopen and recover".into());
        }
    }
}
