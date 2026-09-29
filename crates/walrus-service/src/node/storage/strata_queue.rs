// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Persistent commands for the Strata lifecycle worker.
//!
//! Queue entries, their sequence allocation, and Walrus metadata are committed in one RocksDB
//! write. This module does not apply commands to Strata or acknowledge their durability. A future
//! worker must capture a committed prefix, sync RocksDB, and only then apply that prefix.
//!
//! There is one queue handle per open [`super::Storage`]; clones share the producer lock. Both
//! batch and transaction producers hold it from sequence allocation through commit, so a later
//! sequence cannot commit ahead of an earlier one. Event handlers must still call these helpers
//! in event order: an event ID is an identity, not a sortable queue position.

use std::{
    ops::Bound::{Excluded, Included, Unbounded},
    sync::{Arc, Mutex},
};

use rocksdb::{OptimisticTransactionDB, Options, Transaction};
use serde::{Deserialize, Serialize};
use sui_types::{base_types::ObjectID, event::EventID};
use typed_store::{
    Map,
    TypedStoreError,
    rocks::{
        DBBatch,
        DBMap,
        ReadWriteOptions,
        RocksDB,
        be_fix_int_ser,
        errors::{typed_store_err_from_bcs_err, typed_store_err_from_rocks_err},
    },
};
use walrus_core::{BlobId, Epoch, ShardIndex};

use super::{DatabaseTableOptionsFactory, constants};

/// A queue position, distinct from both a source event index and a Strata LSN.
/// Zero denotes the empty prefix; committed commands start at one.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct StrataQueueSequence(pub u64);

impl StrataQueueSequence {
    fn next(self) -> Result<Self, TypedStoreError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or_else(|| TypedStoreError::TaskError("Strata queue sequence exhausted".to_owned()))
    }
}

/// The source event and its position in Walrus's event stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StrataSourceEvent {
    pub event_index: u64,
    pub event_id: EventID,
}

/// The lifetime initialization that a foreground put must wait for before writing to Strata.
/// The put's own durability wait can cover this preceding command as well.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StrataRegistrationDependency {
    pub event: StrataSourceEvent,
    pub sequence: StrataQueueSequence,
}

/// The shard incarnation a queued mutation is allowed to affect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StrataShardGeneration {
    pub shard: ShardIndex,
    pub generation: u64,
}

/// Invalidation is permanent; ordinary unreferenced-blob deletion can be cancelled by registration.
// Persisted inside V1 records: do not reorder variants or change their fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum StrataDeletionReason {
    Unreferenced { checked_at_epoch: Epoch },
    Invalidated,
}

/// Lifecycle work, never foreground payload bytes. Blob operations cover both sliver keys.
// Persisted inside V1 records: introduce a new record version for incompatible changes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum StrataOperation {
    /// Initialize a first or subsequent registration using a lifetime safe for all live references.
    RegisterBlob { blob_id: BlobId, end_epoch: Epoch },
    /// Extend the blob's lifetime, accounting for all live references.
    ExtendBlob { blob_id: BlobId, end_epoch: Epoch },
    /// All affected blobs must be updated before the worker advances past this command.
    ExtendPool {
        storage_pool_id: ObjectID,
        end_epoch: Epoch,
    },
    /// Delete only the recorded shard generations, after revalidating the intent
    /// under the blob lock.
    DeleteBlob {
        blob_id: BlobId,
        shards: Vec<StrataShardGeneration>,
        reason: StrataDeletionReason,
    },
    /// Advance to an absolute epoch; replay must not increment it again.
    AdvanceEpoch { epoch: Epoch },
}

/// A versioned on-disk command. New versions must preserve decoding of earlier records.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum StrataQueueEntry {
    V1(StrataQueueEntryV1),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StrataQueueEntryV1 {
    /// Periodic garbage collection need not have an associated on-chain event.
    pub source_event: Option<StrataSourceEvent>,
    pub operation: StrataOperation,
}

/// Queue tables in the same RocksDB database as Walrus control state.
#[derive(Debug, Clone)]
pub(crate) struct StrataQueue {
    database: Arc<RocksDB>,
    entries: DBMap<StrataQueueSequence, StrataQueueEntry>,
    last_sequence: DBMap<(), StrataQueueSequence>,
    producer_lock: Arc<Mutex<()>>,
}

impl StrataQueue {
    pub(super) fn options(factory: &DatabaseTableOptionsFactory) -> [(&'static str, Options); 2] {
        [
            (constants::strata_queue_cf_name(), factory.standard()),
            (
                constants::strata_queue_sequence_cf_name(),
                factory.standard(),
            ),
        ]
    }

    /// Open once during Storage initialization; use clones for additional producers.
    pub(super) fn reopen(database: &Arc<RocksDB>) -> Result<Self, TypedStoreError> {
        Ok(Self {
            database: Arc::clone(database),
            entries: DBMap::reopen(
                database,
                Some(constants::strata_queue_cf_name()),
                &ReadWriteOptions::default(),
                false,
            )?,
            last_sequence: DBMap::reopen(
                database,
                Some(constants::strata_queue_sequence_cf_name()),
                &ReadWriteOptions::default(),
                false,
            )?,
            producer_lock: Arc::default(),
        })
    }

    /// The committed high-water mark, even after old entries are removed. Not a durability promise.
    pub fn last_committed_sequence(&self) -> Result<StrataQueueSequence, TypedStoreError> {
        Ok(self.last_sequence.get(&())?.unwrap_or_default())
    }

    /// Read a bounded page after `after`, up to and including a captured committed prefix.
    /// Visibility alone does not authorize applying these entries to Strata:
    /// RocksDB must be synced.
    pub fn scan(
        &self,
        after: Option<StrataQueueSequence>,
        through: StrataQueueSequence,
        limit: usize,
    ) -> Result<Vec<(StrataQueueSequence, StrataQueueEntry)>, TypedStoreError> {
        if limit == 0 || after.is_some_and(|sequence| sequence >= through) {
            return Ok(Vec::new());
        }
        self.entries
            .safe_range_iter((after.map_or(Unbounded, Excluded), Included(through)))?
            .take(limit)
            .collect()
    }

    /// Commit queued commands and metadata together on either RocksDB engine.
    /// An error from `update` discards the entire batch, including sequence allocation.
    ///
    /// The callback must only stage metadata through `write.metadata()`, propagate staging errors,
    /// and must not reenter the queue. Success means committed, not synced. A commit error can be
    /// uncertain; callers must stop and recover rather than assuming it rolled back.
    pub fn write_batch<T>(
        &self,
        update: impl FnOnce(&mut StrataQueueBatch<'_>) -> Result<T, TypedStoreError>,
    ) -> Result<T, TypedStoreError> {
        let _guard = self
            .producer_lock
            .lock()
            .expect("queue producer mutex poisoned");
        let mut write = StrataQueueBatch {
            queue: self,
            batch: self.entries.batch(),
            last_sequence: self.last_committed_sequence()?,
        };
        let result = update(&mut write)?;
        write.batch.write()?;
        Ok(result)
    }

    /// Commit metadata checks/changes and queued commands in one optimistic transaction.
    /// Conflicts roll back both metadata and queue allocation; retry the whole callback.
    /// Other commit errors can be uncertain and require stopping and recovery.
    ///
    /// The callback has the same staging and non-reentrancy requirements as `write_batch`.
    /// Standard RocksDB cannot support this helper; use `write_batch` when no conflict checks are
    /// needed. The transaction is created under the producer lock so its snapshot is not stale
    /// relative to an earlier queue commit.
    pub fn write_transaction<T>(
        &self,
        update: impl FnOnce(&mut StrataQueueTransaction<'_>) -> Result<T, TypedStoreError>,
    ) -> Result<T, TypedStoreError> {
        let _guard = self
            .producer_lock
            .lock()
            .expect("queue producer mutex poisoned");
        let handle = self.database.as_optimistic().ok_or_else(|| {
            TypedStoreError::TaskError(
                "Strata queue transaction requires optimistic RocksDB".into(),
            )
        })?;
        let mut write = StrataQueueTransaction {
            queue: self,
            transaction: handle.transaction(),
            last_sequence: self.last_committed_sequence()?,
        };
        let result = update(&mut write)?;
        write
            .transaction
            .commit()
            .map_err(typed_store_err_from_rocks_err)?;
        Ok(result)
    }
}

/// A batch owned by `StrataQueue::write_batch`; the caller cannot commit it separately.
pub(crate) struct StrataQueueBatch<'a> {
    queue: &'a StrataQueue,
    batch: DBBatch,
    last_sequence: StrataQueueSequence,
}

impl StrataQueueBatch<'_> {
    pub fn metadata(&mut self) -> &mut DBBatch {
        &mut self.batch
    }

    pub fn enqueue(
        &mut self,
        entry: &StrataQueueEntry,
    ) -> Result<StrataQueueSequence, TypedStoreError> {
        let sequence = self.last_sequence.next()?;
        self.batch
            .insert_batch(&self.queue.entries, [(sequence, entry)])?;
        self.batch
            .insert_batch(&self.queue.last_sequence, [((), sequence)])?;
        self.last_sequence = sequence;
        Ok(sequence)
    }
}

/// A transaction owned by `StrataQueue::write_transaction`.
pub(crate) struct StrataQueueTransaction<'a> {
    queue: &'a StrataQueue,
    transaction: Transaction<'a, OptimisticTransactionDB>,
    last_sequence: StrataQueueSequence,
}

impl StrataQueueTransaction<'_> {
    pub fn metadata(&self) -> &Transaction<'_, OptimisticTransactionDB> {
        &self.transaction
    }

    pub fn enqueue(
        &mut self,
        entry: &StrataQueueEntry,
    ) -> Result<StrataQueueSequence, TypedStoreError> {
        let sequence = self.last_sequence.next()?;
        let entry_bytes = bcs::to_bytes(entry).map_err(typed_store_err_from_bcs_err)?;
        let sequence_bytes = bcs::to_bytes(&sequence).map_err(typed_store_err_from_bcs_err)?;
        self.transaction
            .put_cf(
                &self.queue.entries.cf()?,
                be_fix_int_ser(&sequence)?,
                entry_bytes,
            )
            .map_err(typed_store_err_from_rocks_err)?;
        self.transaction
            .put_cf(
                &self.queue.last_sequence.cf()?,
                be_fix_int_ser(&())?,
                sequence_bytes,
            )
            .map_err(typed_store_err_from_rocks_err)?;
        self.last_sequence = sequence;
        Ok(sequence)
    }
}

#[cfg(test)]
mod tests;
