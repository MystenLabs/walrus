// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Strata's existing byte-level storage interface over Walrus's RocksDB handle.
//! This adapter contains no queue logic and preserves both supported database engines.

use std::sync::Arc;

use rocksdb::{BoundColumnFamily, Options, ReadOptions, WriteOptions};
use strata_index::{
    Error,
    Result,
    port::{IndexDb, IndexSnapshot, IndexWriteBatch, RowCursor},
};
use typed_store::rocks::{RocksDB, RocksDBBatch, RocksDBRawIter};

#[derive(Debug, Clone)]
pub(super) struct Database(pub Arc<RocksDB>);

impl Database {
    fn cf(&self, name: &str) -> Result<Arc<BoundColumnFamily<'_>>> {
        self.0
            .cf_handle(name)
            .ok_or_else(|| Error::RocksDb(format!("column family {name} is not open")))
    }
}

fn error(error: rocksdb::Error) -> Error {
    Error::RocksDb(error.into_string())
}

impl IndexDb for Database {
    fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self
            .0
            .get_pinned_cf_opt(&self.cf(cf)?, key, &ReadOptions::default())
            .map_err(error)?
            .map(|value| value.to_vec()))
    }

    fn contains_key(&self, cf: &str, key: &[u8]) -> Result<bool> {
        Ok(self.get(cf, key)?.is_some())
    }

    fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.0
            .put_cf(&self.cf(cf)?, key, value, &WriteOptions::default())
            .map_err(error)
    }

    fn delete(&self, cf: &str, key: &[u8]) -> Result<()> {
        self.0
            .delete_cf(&self.cf(cf)?, key, &WriteOptions::default())
            .map_err(error)
    }

    fn scan<'a>(&'a self, cf: &str) -> Result<Box<dyn RowCursor + 'a>> {
        let iter = self
            .0
            .raw_iterator_cf(&self.cf(cf)?, ReadOptions::default());
        Ok(Box::new(Cursor {
            iter,
            started: false,
        }))
    }

    fn snapshot(&self) -> Result<Box<dyn IndexSnapshot + '_>> {
        let view = match self.0.as_ref() {
            RocksDB::DB(db) => View::Standard(db.underlying.snapshot()),
            RocksDB::OptimisticTransactionDB(db) => View::Optimistic(db.underlying.snapshot()),
        };
        Ok(Box::new(Snapshot { db: self, view }))
    }

    fn write_batch(&self) -> Box<dyn IndexWriteBatch> {
        let batch = match self.0.as_ref() {
            RocksDB::DB(_) => RocksDBBatch::DB(rocksdb::WriteBatch::default()),
            RocksDB::OptimisticTransactionDB(_) => {
                RocksDBBatch::OptimisticTransactionDB(rocksdb::WriteBatchWithTransaction::default())
            }
        };
        Box::new(Batch {
            db: self.clone(),
            batch,
        })
    }

    fn cf_exists(&self, cf: &str) -> bool {
        self.0.cf_handle(cf).is_some()
    }

    fn create_cf(&self, cf: &str, options: &Options) -> Result<()> {
        self.0.create_cf(cf, options).map_err(error)
    }

    fn flush_wal(&self, sync: bool) -> Result<()> {
        match self.0.as_ref() {
            RocksDB::DB(db) => db.underlying.flush_wal(sync),
            RocksDB::OptimisticTransactionDB(db) => db.underlying.flush_wal(sync),
        }
        .map_err(error)
    }
}

enum View<'a> {
    Standard(
        rocksdb::SnapshotWithThreadMode<'a, rocksdb::DBWithThreadMode<rocksdb::MultiThreaded>>,
    ),
    Optimistic(
        rocksdb::SnapshotWithThreadMode<
            'a,
            rocksdb::OptimisticTransactionDB<rocksdb::MultiThreaded>,
        >,
    ),
}

struct Snapshot<'a> {
    db: &'a Database,
    view: View<'a>,
}

impl IndexSnapshot for Snapshot<'_> {
    fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let handle = self.db.cf(cf)?;
        match &self.view {
            View::Standard(snapshot) => snapshot.get_cf(&handle, key),
            View::Optimistic(snapshot) => snapshot.get_cf(&handle, key),
        }
        .map_err(error)
    }

    fn scan<'a>(&'a self, cf: &str) -> Result<Box<dyn RowCursor + 'a>> {
        let handle = self.db.cf(cf)?;
        let iter = match &self.view {
            View::Standard(snapshot) => RocksDBRawIter::DB(snapshot.raw_iterator_cf(&handle)),
            View::Optimistic(snapshot) => {
                RocksDBRawIter::OptimisticTransactionDB(snapshot.raw_iterator_cf(&handle))
            }
        };
        Ok(Box::new(Cursor {
            iter,
            started: false,
        }))
    }
}

struct Cursor<'a> {
    iter: RocksDBRawIter<'a>,
    started: bool,
}

impl RowCursor for Cursor<'_> {
    fn next_row(&mut self) -> Result<bool> {
        if self.started {
            self.iter.next();
        } else {
            self.iter.seek_to_first();
            self.started = true;
        }
        if self.iter.valid() {
            return Ok(true);
        }
        self.iter.status().map_err(error)?;
        Ok(false)
    }

    fn row(&self) -> (&[u8], &[u8]) {
        (
            self.iter.key().expect("cursor must be positioned on a row"),
            self.iter
                .value()
                .expect("cursor must be positioned on a row"),
        )
    }
}

struct Batch {
    db: Database,
    batch: RocksDBBatch,
}

impl IndexWriteBatch for Batch {
    fn put(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.batch.put_cf(&self.db.cf(cf)?, key, value);
        Ok(())
    }

    fn delete(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        self.batch.delete_cf(&self.db.cf(cf)?, key);
        Ok(())
    }

    fn merge(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.batch.merge_cf(&self.db.cf(cf)?, key, value);
        Ok(())
    }

    fn size_in_bytes(&self) -> usize {
        match &self.batch {
            RocksDBBatch::DB(batch) => batch.size_in_bytes(),
            RocksDBBatch::OptimisticTransactionDB(batch) => batch.size_in_bytes(),
        }
    }

    fn write(self: Box<Self>, sync: bool) -> Result<()> {
        let mut options = WriteOptions::default();
        options.set_sync(sync);
        self.db
            .0
            .write(self.batch, &options)
            .map_err(|error| Error::RocksDb(error.to_string()))
    }
}
