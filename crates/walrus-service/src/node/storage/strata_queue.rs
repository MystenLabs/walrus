// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Walrus's adapter for Strata-owned per-blob pending operations.
//!
//! Queue rows live in the node's control database, so pending operations and Walrus metadata
//! commit in one batch. Strata owns the schema, merge semantics and snapshot-before-sync API.
//! This milestone does not connect event producers, run a worker, or change sliver puts.
//!
//! Walrus remains responsible for reference checks, pool fan-out, aggregate lifetimes, shared
//! blob locks and expected shard generations. Source event IDs are opaque queue provenance.
//! Registration cancels pending ordinary deletes; an old in-flight put must not cancel them.

use std::sync::Arc;

use rocksdb::Options;
use serde::{Deserialize, Serialize};
use strata_queue::{
    EPOCH_BARRIERS_CF,
    EpochBarrier,
    LAST_REVISION_CF,
    MERGE_OPERATOR_NAME,
    PENDING_BLOBS_CF,
    PendingBlobOps,
    PendingQueue,
    QueueSnapshot,
    QueueStorage,
    QueueWrite,
    Revision,
    merge_pending,
};
use sui_types::event::EventID;
use typed_store::{
    Map,
    TypedStoreError,
    rocks::{DBBatch, DBMap, ReadWriteOptions, RocksDB, RocksDBSnapshot},
    traits::SeekableIterator,
};

use super::DatabaseTableOptionsFactory;

pub(crate) type StrataQueue = PendingQueue<WalrusQueueStorage>;

/// Application provenance, encoded as opaque bytes in generic queue commands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StrataSourceEvent {
    pub event_index: u64,
    pub event_id: EventID,
}

/// A foreground put waits for this blob's lifetime initialization, not a global applied maximum.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StrataRegistrationDependency {
    pub event: StrataSourceEvent,
    pub revision: Revision,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum QueueError {
    #[error(transparent)]
    Storage(#[from] TypedStoreError),
    #[error(transparent)]
    Queue(#[from] strata_queue::Error),
    #[error(transparent)]
    RocksDb(#[from] rocksdb::Error),
}

type Result<T> = std::result::Result<T, QueueError>;

pub(super) fn options(factory: &DatabaseTableOptionsFactory) -> [(&'static str, Options); 3] {
    let mut blobs = factory.standard();
    blobs.set_merge_operator(
        MERGE_OPERATOR_NAME,
        |_, existing, operands| merge_pending(existing, operands).ok(),
        // Cancellation and acknowledgement must be evaluated with the base row present.
        |_, _, _| None,
    );
    [
        (PENDING_BLOBS_CF, blobs),
        (EPOCH_BARRIERS_CF, factory.standard()),
        (LAST_REVISION_CF, factory.standard()),
    ]
}

pub(super) fn reopen(database: &Arc<RocksDB>) -> Result<StrataQueue> {
    let options = ReadWriteOptions::default();
    Ok(PendingQueue::new(WalrusQueueStorage {
        database: Arc::clone(database),
        blobs: DBMap::reopen(database, Some(PENDING_BLOBS_CF), &options, false)?,
        barriers: DBMap::reopen(database, Some(EPOCH_BARRIERS_CF), &options, false)?,
        last_revision: DBMap::reopen(database, Some(LAST_REVISION_CF), &options, false)?,
    }))
}

#[derive(Debug)]
pub(crate) struct WalrusQueueStorage {
    database: Arc<RocksDB>,
    blobs: DBMap<Vec<u8>, PendingBlobOps>,
    barriers: DBMap<Revision, EpochBarrier>,
    last_revision: DBMap<(), Revision>,
}

impl QueueStorage for WalrusQueueStorage {
    type Error = QueueError;
    type Write = WalrusQueueWrite;
    type Snapshot<'a> = WalrusQueueSnapshot<'a>;

    fn last_revision(&self) -> Result<Revision> {
        Ok(self.last_revision.get(&())?.unwrap_or_default())
    }

    fn batch(&self) -> Self::Write {
        WalrusQueueWrite {
            batch: self.blobs.batch(),
            blobs: self.blobs.clone(),
            barriers: self.barriers.clone(),
            last_revision: self.last_revision.clone(),
        }
    }

    fn commit(&self, write: Self::Write) -> Result<()> {
        Ok(write.batch.write()?)
    }

    fn snapshot(&self) -> Result<Self::Snapshot<'_>> {
        Ok(WalrusQueueSnapshot {
            storage: self,
            snapshot: self.database.snapshot(),
        })
    }

    fn sync(&self) -> Result<()> {
        Ok(self.database.sync_wal()?)
    }
}

/// Only stage application metadata in this batch; the generic queue owns its commit.
pub(crate) struct WalrusQueueWrite {
    batch: DBBatch,
    blobs: DBMap<Vec<u8>, PendingBlobOps>,
    barriers: DBMap<Revision, EpochBarrier>,
    last_revision: DBMap<(), Revision>,
}

impl QueueWrite for WalrusQueueWrite {
    type Error = QueueError;
    type Metadata = DBBatch;

    fn metadata(&mut self) -> &mut DBBatch {
        &mut self.batch
    }

    fn merge_blob(&mut self, key: &[u8], operand: &[u8]) -> Result<()> {
        self.batch
            .partial_merge_batch(&self.blobs, [(key.to_vec(), operand)])?;
        Ok(())
    }

    fn put_barrier(&mut self, revision: Revision, barrier: &EpochBarrier) -> Result<()> {
        self.batch
            .insert_batch(&self.barriers, [(revision, barrier)])?;
        Ok(())
    }

    fn set_last_revision(&mut self, revision: Revision) -> Result<()> {
        self.batch
            .insert_batch(&self.last_revision, [((), revision)])?;
        Ok(())
    }
}

pub(crate) struct WalrusQueueSnapshot<'a> {
    storage: &'a WalrusQueueStorage,
    snapshot: RocksDBSnapshot<'a>,
}

impl QueueSnapshot for WalrusQueueSnapshot<'_> {
    type Error = QueueError;

    fn blobs(&self, after: Option<&[u8]>, limit: usize) -> Result<Vec<(Vec<u8>, PendingBlobOps)>> {
        let mut iter = self.storage.blobs.safe_iter_with_snapshot(&self.snapshot)?;
        if let Some(after) = after {
            iter.seek(&after.to_vec())?;
        }
        Ok(iter
            .filter(|row| !matches!(row, Ok((key, _)) if Some(key.as_slice()) == after))
            .take(limit)
            .collect::<std::result::Result<_, _>>()?)
    }

    fn barriers(
        &self,
        after: Option<Revision>,
        limit: usize,
    ) -> Result<Vec<(Revision, EpochBarrier)>> {
        let mut iter = self
            .storage
            .barriers
            .safe_iter_with_snapshot(&self.snapshot)?;
        if let Some(after) = after {
            iter.seek(&after)?;
        }
        Ok(iter
            .filter(|row| !matches!(row, Ok((revision, _)) if Some(*revision) == after))
            .take(limit)
            .collect::<std::result::Result<_, _>>()?)
    }
}

#[cfg(test)]
mod tests;
