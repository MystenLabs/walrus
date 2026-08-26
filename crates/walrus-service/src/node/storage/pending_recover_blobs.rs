// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Durable table of blobs whose recovery is pending.
//!
//! A record is inserted when a certify event needs a blob sync, before the event is marked as
//! complete. This way, a restart cannot lose the fact that the blob still needs recovery, and
//! the event cursor does not have to wait for the recovery to finish. Records are deleted when
//! the blob is recovered or retired (deleted, invalidated, or expired).

use std::sync::{
    Arc,
    Mutex,
    atomic::{AtomicU64, Ordering},
};

use rocksdb::Options;
use serde::{Deserialize, Serialize};
use typed_store::{
    Map,
    TypedStoreError,
    rocks::{DBMap, ReadWriteOptions, RocksDB},
};
use walrus_core::{BlobId, Epoch};

use super::{DatabaseTableOptionsFactory, constants::pending_recover_blobs_cf_name};

/// A record of a blob whose recovery is pending.
// Important: this enum is committed to database. Only extend it with new variants.
#[derive(Eq, PartialEq, Debug, Clone, Deserialize, Serialize)]
pub(crate) enum PendingRecoverBlob {
    V1(PendingRecoverBlobV1),
}

impl PendingRecoverBlob {
    /// Creates a `V1` pending-recovery record.
    pub fn new(event_index: u64, certified_epoch: Epoch) -> Self {
        Self::V1(PendingRecoverBlobV1 {
            event_index,
            certified_epoch,
        })
    }

    /// The index of the `BlobCertified` event that required the recovery.
    pub fn event_index(&self) -> u64 {
        match self {
            PendingRecoverBlob::V1(v1) => v1.event_index,
        }
    }

    /// The epoch in which the blob was certified, used to route recovery requests to the correct
    /// committee.
    pub fn certified_epoch(&self) -> Epoch {
        match self {
            PendingRecoverBlob::V1(v1) => v1.certified_epoch,
        }
    }
}

#[derive(Eq, PartialEq, Debug, Clone, Deserialize, Serialize)]
pub(crate) struct PendingRecoverBlobV1 {
    event_index: u64,
    certified_epoch: Epoch,
}

#[derive(Debug, Clone)]
pub(super) struct PendingRecoverBlobsTable {
    inner: DBMap<BlobId, PendingRecoverBlob>,
    // Serializes inserts and deletes so that the cached count stays exact.
    mutation_lock: Arc<Mutex<()>>,
    // Cached number of records for cheap metric and health reads.
    count: Arc<AtomicU64>,
}

impl PendingRecoverBlobsTable {
    pub fn reopen(database: &Arc<RocksDB>) -> Result<Self, TypedStoreError> {
        let inner: DBMap<BlobId, PendingRecoverBlob> = DBMap::reopen(
            database,
            Some(pending_recover_blobs_cf_name()),
            &ReadWriteOptions::default(),
            false,
        )?;

        // Count with error propagation: an iterator that keeps yielding a read error would
        // otherwise be counted forever and hang the open.
        // TODO(WAL-1322): this iterates the whole table (including
        // RocksDB tombstones) and delays the storage open when the table is large; use a
        // cheaper way to initialize the count.
        let mut count: u64 = 0;
        for entry in inner.safe_iter()? {
            entry?;
            count += 1;
        }

        Ok(Self {
            inner,
            mutation_lock: Arc::default(),
            count: Arc::new(AtomicU64::new(count)),
        })
    }

    pub fn options(db_table_opts_factory: &DatabaseTableOptionsFactory) -> (&'static str, Options) {
        (
            pending_recover_blobs_cf_name(),
            db_table_opts_factory.pending_recover_blobs(),
        )
    }

    /// Inserts or overwrites the pending-recovery record for `blob_id` and returns the number of
    /// records in the table.
    pub fn insert(
        &self,
        blob_id: &BlobId,
        record: &PendingRecoverBlob,
    ) -> Result<u64, TypedStoreError> {
        let _guard = self
            .mutation_lock
            .lock()
            .expect("mutex should not be poisoned");
        let existed = self.inner.contains_key(blob_id)?;
        self.inner.insert(blob_id, record)?;
        if !existed {
            self.count.fetch_add(1, Ordering::SeqCst);
        }
        Ok(self.count.load(Ordering::SeqCst))
    }

    /// Deletes the pending-recovery record for `blob_id`, if any, and returns the number of
    /// records remaining in the table.
    pub fn delete(&self, blob_id: &BlobId) -> Result<u64, TypedStoreError> {
        let _guard = self
            .mutation_lock
            .lock()
            .expect("mutex should not be poisoned");
        if self.inner.contains_key(blob_id)? {
            self.inner.remove(blob_id)?;
            self.count.fetch_sub(1, Ordering::SeqCst);
        }
        Ok(self.count.load(Ordering::SeqCst))
    }

    /// Returns all pending-recovery records.
    // TODO(WAL-1322): this materializes the whole table in memory
    // (tens of MB per million records); replace with bounded chunked iteration with a resume
    // cursor so large backlogs are processed with capped memory.
    // TODO(WAL-1324): the insert-then-delete churn of this table
    // leaves tombstones that slow scans until compaction; consider a periodic or post-drain
    // manual compaction of this column family.
    pub fn scan_all(&self) -> Result<Vec<(BlobId, PendingRecoverBlob)>, TypedStoreError> {
        self.inner.safe_iter()?.collect()
    }

    /// Returns the number of pending-recovery records.
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use walrus_core::{ShardIndex, test_utils::random_blob_id};
    use walrus_test_utils::Result as TestResult;

    use super::*;
    use crate::test_utils::empty_storage_with_shards;

    #[tokio::test]
    async fn insert_delete_and_count() -> TestResult {
        let storage = empty_storage_with_shards(&[ShardIndex(0)]).await;
        let storage = storage.as_ref();

        let blob_id_1 = random_blob_id();
        let blob_id_2 = random_blob_id();

        assert_eq!(storage.pending_recover_blob_count(), 0);
        assert_eq!(storage.insert_pending_recover_blob(&blob_id_1, 7, 2)?, 1);
        // Overwriting an existing record does not change the count.
        assert_eq!(storage.insert_pending_recover_blob(&blob_id_1, 9, 2)?, 1);
        assert_eq!(storage.insert_pending_recover_blob(&blob_id_2, 11, 3)?, 2);

        let mut records = storage.scan_pending_recover_blobs()?;
        records.sort_by_key(|(_, record)| record.event_index());
        assert_eq!(
            records,
            vec![
                (blob_id_1, PendingRecoverBlob::new(9, 2)),
                (blob_id_2, PendingRecoverBlob::new(11, 3)),
            ]
        );

        assert_eq!(storage.delete_pending_recover_blob(&blob_id_1)?, 1);
        // Deleting a non-existent record is a no-op.
        assert_eq!(storage.delete_pending_recover_blob(&blob_id_1)?, 1);
        assert_eq!(storage.delete_pending_recover_blob(&blob_id_2)?, 0);
        assert!(storage.scan_pending_recover_blobs()?.is_empty());
        assert_eq!(storage.pending_recover_blob_count(), 0);

        Ok(())
    }
}
