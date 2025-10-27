// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Error types and utilities for RocksDB operations.
pub mod errors;

/// Safe iterator utilities for RocksDB.
pub(crate) mod safe_iter;

use std::{
    borrow::Borrow,
    collections::HashSet,
    env,
    fmt,
    marker::PhantomData,
    ops::{Bound, RangeBounds},
    path::{Path, PathBuf},
    sync::Arc,
};

use bincode::Options;
use rocksdb::{
    AsColumnFamilyRef,
    BlockBasedOptions,
    BottommostLevelCompaction,
    CStrLike,
    Cache,
    ColumnFamilyDescriptor,
    CompactOptions,
    DBPinnableSlice,
    DBWithThreadMode,
    Error,
    IngestExternalFileOptions,
    LiveFile,
    MultiThreaded,
    OptimisticTransactionDB,
    OptimisticTransactionOptions,
    ReadOptions,
    SstFileWriter,
    Transaction,
    WriteBatch,
    WriteBatchWithTransaction,
    WriteOptions,
    backup::BackupEngine,
    checkpoint::Checkpoint,
};
use serde::{Serialize, de::DeserializeOwned};
use tap::TapFallible;
use tokio::sync::oneshot;

use crate::{
    TypedStoreError,
    metrics::{DBMetrics, RocksDBPerfContext, SamplingInterval},
    rocks::{
        errors::{
            typed_store_err_from_bcs_err,
            typed_store_err_from_bincode_err,
            typed_store_err_from_rocks_err,
        },
        safe_iter::{IterContext, SafeIter, SafeRevIter},
    },
    traits::{Map, TableSummary},
};

// Write buffer size per RocksDB instance can be set via the env var below.
// If the env var is not set, use the default value in MiB.
const ENV_VAR_DB_WRITE_BUFFER_SIZE: &str = "DB_WRITE_BUFFER_SIZE_MB";
const DEFAULT_DB_WRITE_BUFFER_SIZE: usize = 1024;

// Write ahead log size per RocksDB instance can be set via the env var below.
// If the env var is not set, use the default value in MiB.
const ENV_VAR_DB_WAL_SIZE: &str = "DB_WAL_SIZE_MB";
const DEFAULT_DB_WAL_SIZE: usize = 1024;

const ENV_VAR_DB_PARALLELISM: &str = "DB_PARALLELISM";

#[cfg(test)]
mod tests;

/// Repeatedly attempt an Optimistic Transaction until it succeeds.
/// This will loop forever until the transaction succeeds.
#[macro_export]
macro_rules! retry_transaction {
    ($transaction:expr) => {
        retry_transaction!($transaction, Some(20))
    };

    (
        $transaction:expr,
        $max_retries:expr
        $(,)?

    ) => {{
        use rand::{
            distributions::{Distribution, Uniform},
            rngs::ThreadRng,
        };
        use tokio::time::{Duration, sleep};

        let mut retries = 0;
        let max_retries = $max_retries;
        loop {
            let status = $transaction;
            match status {
                Err(TypedStoreError::RetryableTransactionError) => {
                    retries += 1;
                    let metrics = $crate::metrics::DBMetrics::get();
                    metrics.op_metrics.rocksdb_optimistic_tx_retries_total.inc();
                    // Randomized delay to help racing transactions get out of each other's way.
                    let delay = {
                        let mut rng = ThreadRng::default();
                        Duration::from_millis(Uniform::new(0, 50).sample(&mut rng))
                    };
                    if let Some(max_retries) = max_retries {
                        if retries > max_retries {
                            tracing::error!(?max_retries, "max retries exceeded");
                            break status;
                        }
                    }
                    if retries > 10 {
                        // TODO: monitoring needed?
                        tracing::error!(?delay, ?retries, "excessive transaction retries...");
                    } else {
                        tracing::info!(
                            ?delay,
                            ?retries,
                            "transaction write conflict detected, sleeping"
                        );
                    }
                    sleep(delay).await;
                }
                _ => {
                    // Observe per-transaction retries in a histogram (0 if no retries).
                    let metrics = $crate::metrics::DBMetrics::get();
                    metrics
                        .op_metrics
                        .rocksdb_optimistic_tx_retries_per_tx
                        .observe(f64::from(retries));
                    break status;
                }
            }
        }
    }};
}

/// Retry a transaction forever.
#[macro_export]
macro_rules! retry_transaction_forever {
    ($transaction:expr) => {
        $crate::retry_transaction!($transaction, None)
    };
}

/// Engine-specific behavior for RocksDB wrappers.
pub trait DbBehavior {
    /// Human-readable engine label used in Debug output.
    const ENGINE_LABEL: &'static str;
    /// Optional cancellation of background work on drop.
    fn cancel_on_drop(db: &Self);
}

impl DbBehavior for rocksdb::DBWithThreadMode<MultiThreaded> {
    const ENGINE_LABEL: &'static str = "RocksDB";
    fn cancel_on_drop(db: &Self) {
        db.cancel_all_background_work(/* wait */ true);
    }
}

impl DbBehavior for rocksdb::OptimisticTransactionDB<MultiThreaded> {
    const ENGINE_LABEL: &'static str = "OptimisticTransactionDB";
    fn cancel_on_drop(db: &Self) {
        db.cancel_all_background_work(/* wait */ true);
    }
}

/// A generic wrapper around RocksDB engines with common metadata and behavior.
pub struct DBWrapper<T: DbBehavior> {
    /// The underlying rocksdb database.
    pub underlying: T,
    /// The metric configuration.
    pub metric_conf: MetricConf,
    /// The path of the database.
    pub db_path: PathBuf,
    /// The database options.
    pub db_options: rocksdb::Options,
}

impl<T: DbBehavior> fmt::Debug for DBWrapper<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {{ db_path: {:?} }}", T::ENGINE_LABEL, self.db_path)
    }
}

impl<T: DbBehavior> Drop for DBWrapper<T> {
    fn drop(&mut self) {
        T::cancel_on_drop(&self.underlying);
        DBMetrics::get().decrement_num_active_dbs(&self.metric_conf.db_name);
    }
}

impl<T: DbBehavior> DBWrapper<T> {
    fn new(
        underlying: T,
        metric_conf: MetricConf,
        db_path: PathBuf,
        db_options: rocksdb::Options,
    ) -> Self {
        DBMetrics::get().increment_num_active_dbs(&metric_conf.db_name);
        Self {
            underlying,
            metric_conf,
            db_path,
            db_options,
        }
    }
}

/// Backwards-compatible alias for the standard `rocksdb::DB` wrapper.
pub type DBWithThreadModeWrapper = DBWrapper<rocksdb::DBWithThreadMode<MultiThreaded>>;
/// Backwards-compatible alias for the `rocksdb::OptimisticTransactionDB` wrapper.
pub type OptimisticTransactionDBWrapper =
    DBWrapper<rocksdb::OptimisticTransactionDB<MultiThreaded>>;

#[derive(Debug)]
/// Wrapper around RocksDB engines used by Walrus.
pub enum RocksDB {
    /// Standard RocksDB engine.
    DB(DBWithThreadModeWrapper),
    /// RocksDB optimistic transaction engine.
    OptimisticTransactionDB(OptimisticTransactionDBWrapper),
}

/// A deliberate handle for optimistic-transaction-only APIs.
#[derive(Debug, Copy, Clone)]
pub struct OptimisticHandle<'a> {
    pub(crate) inner: &'a OptimisticTransactionDBWrapper,
}

impl<'a> OptimisticHandle<'a> {
    /// Create a new transaction without a snapshot.
    /// Consistency: No snapshot; guarantees keys haven't changed since first write/get_for_update.
    pub fn transaction_without_snapshot(
        &self,
    ) -> Transaction<'a, rocksdb::OptimisticTransactionDB> {
        self.inner.underlying.transaction()
    }

    /// Create a new transaction with a snapshot for repeatable reads.
    pub fn transaction(&self) -> Transaction<'a, rocksdb::OptimisticTransactionDB> {
        let mut tx_opts = OptimisticTransactionOptions::new();
        tx_opts.set_snapshot(true);
        self.inner
            .underlying
            .transaction_opt(&WriteOptions::default(), &tx_opts)
    }
}

/// Handle for range-delete operations only valid for standard RocksDB engine.
#[derive(Debug, Copy, Clone)]
pub struct RangeDeleteHandle<'a> {
    pub(crate) inner: &'a DBWithThreadModeWrapper,
}

impl<'a> RangeDeleteHandle<'a> {
    /// Schedule a range delete tombstone from `from` (inclusive) to `to` (exclusive).
    pub fn delete_range_cf<K: AsRef<[u8]>>(
        &self,
        cf: &impl AsColumnFamilyRef,
        from: K,
        to: K,
    ) -> Result<(), rocksdb::Error> {
        self.inner.underlying.delete_range_cf(cf, from, to)
    }
}

macro_rules! delegate_call {
    ($self:ident.$method:ident($($args:ident),*)) => {
        match $self {
            Self::DB(d) => d.underlying.$method($($args),*),
            Self::OptimisticTransactionDB(d) => d.underlying.$method($($args),*),
        }
    };
    ($self:ident.$field:ident) => {
        match $self {
            Self::DB(d) => &d.$field,
            Self::OptimisticTransactionDB(d) => &d.$field,
        }
    }

}

macro_rules! delegate_pair {
    ($self:ident, $batch:ident, |$d:ident, $b:ident| $db_expr:expr,
        |$td:ident, $tb:ident| $txn_expr:expr) => {
        match ($self, $batch) {
            (RocksDB::DB($d), RocksDBBatch::DB($b)) => $db_expr,
            (RocksDB::OptimisticTransactionDB($td), RocksDBBatch::OptimisticTransactionDB($tb)) => {
                $txn_expr
            }
            _ => Err(TypedStoreError::RocksDBError(
                "using invalid batch type for the database".into(),
            )),
        }
    };
}

impl RocksDB {
    /// Returns an optimistic-transaction handle if the DB is using the optimistic engine.
    /// This allows invoking transaction APIs only when supported.
    pub fn as_optimistic(&self) -> Option<OptimisticHandle<'_>> {
        match self {
            RocksDB::OptimisticTransactionDB(db) => Some(OptimisticHandle { inner: db }),
            RocksDB::DB(_) => None,
        }
    }

    /// Returns a handle that allows range-delete operations (only for standard RocksDB engine).
    pub fn as_range_delete(&self) -> Option<RangeDeleteHandle<'_>> {
        match self {
            RocksDB::DB(db) => Some(RangeDeleteHandle { inner: db }),
            RocksDB::OptimisticTransactionDB(_) => None,
        }
    }

    /// Returns the configured `rocksdb::Options` for this database instance.
    pub fn db_options(&self) -> &rocksdb::Options {
        delegate_call!(self.db_options)
    }

    /// Get a value from the database.
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>, rocksdb::Error> {
        delegate_call!(self.get(key))
    }

    /// Get multiple values from the database.
    pub fn multi_get_cf<'a, 'b: 'a, K, I, W>(
        &'a self,
        keys: I,
        readopts: &ReadOptions,
    ) -> Vec<Result<Option<Vec<u8>>, rocksdb::Error>>
    where
        K: AsRef<[u8]>,
        I: IntoIterator<Item = (&'b W, K)>,
        W: 'b + AsColumnFamilyRef,
    {
        delegate_call!(self.multi_get_cf_opt(keys, readopts))
    }

    /// Get multiple values from a specific column family.
    pub fn batched_multi_get_cf_opt<'a, K, I>(
        &self,
        cf: &impl AsColumnFamilyRef,
        keys: I,
        sorted_input: bool,
        readopts: &ReadOptions,
    ) -> Vec<Result<Option<DBPinnableSlice<'_>>, Error>>
    where
        K: AsRef<[u8]> + 'a + ?Sized,
        I: IntoIterator<Item = &'a K>,
    {
        delegate_call!(self.batched_multi_get_cf_opt(cf, keys, sorted_input, readopts))
    }

    /// Get a property value from a specific column family.
    pub fn property_int_value_cf(
        &self,
        cf: &impl AsColumnFamilyRef,
        name: impl CStrLike,
    ) -> Result<Option<u64>, rocksdb::Error> {
        delegate_call!(self.property_int_value_cf(cf, name))
    }

    /// Get a pinned value from a specific column family.
    pub fn get_pinned_cf_opt<K: AsRef<[u8]>>(
        &self,
        cf: &impl AsColumnFamilyRef,
        key: K,
        readopts: &ReadOptions,
    ) -> Result<Option<DBPinnableSlice<'_>>, rocksdb::Error> {
        delegate_call!(self.get_pinned_cf_opt(cf, key, readopts))
    }

    /// Get a column family handle by name.
    pub fn cf_handle(&self, name: &str) -> Option<Arc<rocksdb::BoundColumnFamily<'_>>> {
        delegate_call!(self.cf_handle(name))
    }

    /// Create a new column family.
    pub fn create_cf<N: AsRef<str>>(
        &self,
        name: N,
        opts: &rocksdb::Options,
    ) -> Result<(), rocksdb::Error> {
        delegate_call!(self.create_cf(name, opts))
    }

    /// Drop a column family.
    pub fn drop_cf(&self, name: &str) -> Result<(), rocksdb::Error> {
        delegate_call!(self.drop_cf(name))
    }

    /// Delete files in a range.
    #[allow(dead_code)]
    pub fn delete_file_in_range<K: AsRef<[u8]>>(
        &self,
        cf: &impl AsColumnFamilyRef,
        from: K,
        to: K,
    ) -> Result<(), rocksdb::Error> {
        delegate_call!(self.delete_file_in_range_cf(cf, from, to))
    }

    /// Delete a value from a specific column family.
    pub fn delete_cf<K: AsRef<[u8]>>(
        &self,
        cf: &impl AsColumnFamilyRef,
        key: K,
        writeopts: &WriteOptions,
    ) -> Result<(), rocksdb::Error> {
        sui_macros::fail_point!("delete-cf-before");
        let ret = delegate_call!(self.delete_cf_opt(cf, key, writeopts));
        sui_macros::fail_point!("delete-cf-after");
        #[allow(clippy::let_and_return)]
        ret
    }

    /// Get the path of the database.
    pub fn path(&self) -> &Path {
        delegate_call!(self.path())
    }

    /// Put a value into a specific column family.
    pub fn put_cf<K, V>(
        &self,
        cf: &impl AsColumnFamilyRef,
        key: K,
        value: V,
        writeopts: &WriteOptions,
    ) -> Result<(), rocksdb::Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        sui_macros::fail_point!("put-cf-before");
        let ret = delegate_call!(self.put_cf_opt(cf, key, value, writeopts));
        sui_macros::fail_point!("put-cf-after");
        #[allow(clippy::let_and_return)]
        ret
    }

    /// Check if a key may exist in a specific column family.
    pub fn key_may_exist_cf<K: AsRef<[u8]>>(
        &self,
        cf: &impl AsColumnFamilyRef,
        key: K,
        readopts: &ReadOptions,
    ) -> bool {
        delegate_call!(self.key_may_exist_cf_opt(cf, key, readopts))
    }

    /// Try to catch up with the primary.
    pub fn try_catch_up_with_primary(&self) -> Result<(), rocksdb::Error> {
        delegate_call!(self.try_catch_up_with_primary())
    }

    /// Write a batch of operations to the database.
    pub fn write(
        &self,
        batch: RocksDBBatch,
        writeopts: &WriteOptions,
    ) -> Result<(), TypedStoreError> {
        sui_macros::fail_point!("batch-write-before");
        delegate_pair!(
            self,
            batch,
            |d, b| {
                d.underlying
                    .write_opt(b, writeopts)
                    .map_err(typed_store_err_from_rocks_err)
            },
            |d, b| {
                d.underlying
                    .write_opt(b, writeopts)
                    .map_err(typed_store_err_from_rocks_err)
            }
        )
    }

    /// Get a raw iterator for a specific column family.
    pub fn raw_iterator_cf<'a: 'b, 'b>(
        &'a self,
        cf_handle: &impl AsColumnFamilyRef,
        readopts: ReadOptions,
    ) -> RocksDBRawIter<'b> {
        match self {
            Self::DB(d) => {
                RocksDBRawIter::DB(d.underlying.raw_iterator_cf_opt(cf_handle, readopts))
            }
            Self::OptimisticTransactionDB(d) => RocksDBRawIter::OptimisticTransactionDB(
                d.underlying.raw_iterator_cf_opt(cf_handle, readopts),
            ),
        }
    }

    /// Compact a range of values in a specific column family.
    pub fn compact_range_cf<K: AsRef<[u8]>>(
        &self,
        cf: &impl AsColumnFamilyRef,
        start: Option<K>,
        end: Option<K>,
    ) {
        delegate_call!(self.compact_range_cf(cf, start, end))
    }

    /// Compact a range of values in a specific column family to the bottom.
    /// of the LSM tree.
    pub fn compact_range_to_bottom<K: AsRef<[u8]>>(
        &self,
        cf: &impl AsColumnFamilyRef,
        start: Option<K>,
        end: Option<K>,
    ) {
        let opt = &mut CompactOptions::default();
        opt.set_bottommost_level_compaction(BottommostLevelCompaction::ForceOptimized);
        delegate_call!(self.compact_range_cf_opt(cf, start, end, opt))
    }

    /// Ingest external SST files into a specific column family. Ingesting into an existing
    /// column family is allowed. If any ingested keys already exist in the target CF,
    /// RocksDB will, by default, assign a global sequence number to the ingested entries so
    /// that they are treated as the latest versions. Reads will then observe the ingested
    /// values for those conflicting keys. Behavior can be tuned via `IngestExternalFileOptions`
    /// (e.g., file move vs copy, and other ingestion modes provided by RocksDB).
    /// Note: Files must reside on the same filesystem for move/link optimizations.
    pub fn ingest_external_file_cf<P: AsRef<Path>>(
        &self,
        cf: &impl AsColumnFamilyRef,
        paths: &[P],
        opts: &IngestExternalFileOptions,
    ) -> Result<(), rocksdb::Error> {
        let v: Vec<&Path> = paths.iter().map(|p| p.as_ref()).collect();
        delegate_call!(self.ingest_external_file_cf_opts(cf, opts, v))
    }

    /// Flush the database
    #[allow(dead_code)]
    pub fn flush(&self) -> Result<(), TypedStoreError> {
        delegate_call!(self.flush()).map_err(|e| TypedStoreError::RocksDBError(e.into_string()))
    }

    /// Create a checkpoint of the database.
    pub fn checkpoint(&self, path: &Path) -> Result<(), TypedStoreError> {
        let checkpoint = self.new_checkpoint()?;
        checkpoint
            .create_checkpoint(path)
            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))
    }

    /// Create a new backup of the database.
    pub fn create_new_backup_flush(
        &self,
        engine: &mut BackupEngine,
        flush_before_backup: bool,
    ) -> Result<(), TypedStoreError> {
        match self {
            Self::DB(d) => engine
                .create_new_backup_flush(&d.underlying, flush_before_backup)
                .map_err(typed_store_err_from_rocks_err),
            Self::OptimisticTransactionDB(d) => engine
                .create_new_backup_flush(&d.underlying, flush_before_backup)
                .map_err(typed_store_err_from_rocks_err),
        }
    }

    /// Flush a specific column family.
    pub fn flush_cf(&self, cf: &impl AsColumnFamilyRef) -> Result<(), rocksdb::Error> {
        delegate_call!(self.flush_cf(cf))
    }

    /// Set options for a specific column family.
    #[allow(dead_code)]
    pub fn set_options_cf(
        &self,
        cf: &impl AsColumnFamilyRef,
        opts: &[(&str, &str)],
    ) -> Result<(), rocksdb::Error> {
        delegate_call!(self.set_options_cf(cf, opts))
    }

    /// Set options for the database.
    pub fn set_options(&self, opts: &[(&str, &str)]) -> Result<(), rocksdb::Error> {
        delegate_call!(self.set_options(opts))
    }

    /// Get the sampling interval for the database.
    pub fn get_sampling_interval(&self) -> SamplingInterval {
        delegate_call!(self.metric_conf)
            .read_sample_interval
            .new_from_self()
    }

    /// Get the sampling interval for multi-get operations.
    pub fn multiget_sampling_interval(&self) -> SamplingInterval {
        delegate_call!(self.metric_conf)
            .read_sample_interval
            .new_from_self()
    }

    /// Get the sampling interval for write operations.
    pub fn write_sampling_interval(&self) -> SamplingInterval {
        delegate_call!(self.metric_conf)
            .write_sample_interval
            .new_from_self()
    }

    /// Get the sampling interval for iterator operations.
    pub fn iter_sampling_interval(&self) -> SamplingInterval {
        delegate_call!(self.metric_conf)
            .iter_sample_interval
            .new_from_self()
    }

    /// Get the name of the database.
    pub fn db_name(&self) -> String {
        let name = delegate_call!(self.metric_conf).db_name.clone();
        if name.is_empty() {
            self.default_db_name()
        } else {
            name.clone()
        }
    }

    /// Get the default name of the database.
    fn default_db_name(&self) -> String {
        self.path()
            .file_name()
            .and_then(|f| f.to_str())
            .unwrap_or("unknown")
            .to_string()
    }

    /// Get the live files in the database.
    #[allow(dead_code)]
    pub fn live_files(&self) -> Result<Vec<LiveFile>, Error> {
        delegate_call!(self.live_files())
    }

    /// Create a new batch for the database.
    pub fn make_batch(&self) -> RocksDBBatch {
        match self {
            RocksDB::DB(_) => RocksDBBatch::DB(WriteBatch::default()),
            RocksDB::OptimisticTransactionDB(_) => {
                RocksDBBatch::OptimisticTransactionDB(WriteBatchWithTransaction::<true>::default())
            }
        }
    }

    fn new_checkpoint(&self) -> Result<Checkpoint<'_>, TypedStoreError> {
        match self {
            RocksDB::DB(d) => {
                Checkpoint::new(&d.underlying).map_err(typed_store_err_from_rocks_err)
            }
            RocksDB::OptimisticTransactionDB(d) => {
                Checkpoint::new(&d.underlying).map_err(typed_store_err_from_rocks_err)
            }
        }
    }
}

/// A batch of write operations for RocksDB, covering both standard and optimistic transaction DBs.
pub enum RocksDBBatch {
    /// A write batch for a standard `rocksdb::DB`.
    DB(rocksdb::WriteBatch),
    /// A write batch for an `rocksdb::OptimisticTransactionDB`.
    OptimisticTransactionDB(rocksdb::WriteBatchWithTransaction<true>),
}

impl fmt::Debug for RocksDBBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DB(_) => write!(f, "RocksDBBatch::DB"),
            Self::OptimisticTransactionDB(_) => write!(f, "RocksDBBatch::OptimisticTransactionDB"),
        }
    }
}

macro_rules! delegate_batch_call {
    ($self:ident.$method:ident($($args:ident),*)) => {
        match $self {
            Self::DB(b) => b.$method($($args),*),
            Self::OptimisticTransactionDB(b) => b.$method($($args),*),
        }
    }
}

impl RocksDBBatch {
    fn size_in_bytes(&self) -> usize {
        delegate_batch_call!(self.size_in_bytes())
    }

    /// Delete a key from the given column family within this batch.
    pub fn delete_cf<K: AsRef<[u8]>>(&mut self, cf: &impl AsColumnFamilyRef, key: K) {
        delegate_batch_call!(self.delete_cf(cf, key))
    }

    /// Insert a key-value pair into the given column family within this batch.
    pub fn put_cf<K, V>(&mut self, cf: &impl AsColumnFamilyRef, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        delegate_batch_call!(self.put_cf(cf, key, value))
    }

    /// Merge a value with the existing value for a key within the given column family.
    pub fn merge_cf<K, V>(&mut self, cf: &impl AsColumnFamilyRef, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        delegate_batch_call!(self.merge_cf(cf, key, value))
    }

    /// Schedule a range delete tombstone from `from` (inclusive) to `to` (exclusive).
    pub fn delete_range_cf<K: AsRef<[u8]>>(
        &mut self,
        cf: &impl AsColumnFamilyRef,
        from: K,
        to: K,
        _cap: &RangeDeleteHandle<'_>,
    ) -> Result<(), TypedStoreError> {
        match self {
            Self::DB(batch) => {
                batch.delete_range_cf(cf, from, to);
                Ok(())
            }
            Self::OptimisticTransactionDB(_) => {
                tracing::warn!("delete_range_cf is not supported for OptimisticTransactionDB");
                Err(TypedStoreError::RocksDBError(
                    "delete_range_cf is not supported for OptimisticTransactionDB".into(),
                ))
            }
        }
    }
}

/// A configuration for metrics.
#[derive(Debug, Default)]
pub struct MetricConf {
    /// The name of the database.
    pub db_name: String,
    /// The sampling interval for read operations.
    pub read_sample_interval: SamplingInterval,
    /// The sampling interval for write operations.
    pub write_sample_interval: SamplingInterval,
    /// The sampling interval for iterator operations.
    pub iter_sample_interval: SamplingInterval,
}

/// A configuration for metrics.
impl MetricConf {
    /// Create a new metric configuration.
    pub fn new(db_name: &str) -> Self {
        if db_name.is_empty() {
            tracing::error!("a meaningful DB name should be used for metrics reporting")
        }
        Self {
            db_name: db_name.to_string(),
            read_sample_interval: SamplingInterval::default(),
            write_sample_interval: SamplingInterval::default(),
            iter_sample_interval: SamplingInterval::default(),
        }
    }

    /// Set the sampling interval for the database.
    pub fn with_sampling(self, read_interval: SamplingInterval) -> Self {
        Self {
            db_name: self.db_name,
            read_sample_interval: read_interval,
            write_sample_interval: SamplingInterval::default(),
            iter_sample_interval: SamplingInterval::default(),
        }
    }
}

/// An interface to a rocksDB database, keyed by a columnfamily.
#[derive(Clone, Debug)]
pub struct DBMap<K, V> {
    /// The rocksDB database.
    pub rocksdb: Arc<RocksDB>,
    /// The phantom data.
    _phantom: PhantomData<fn(K) -> V>,
    /// The rocksDB ColumnFamily under which the map is stored.
    cf: String,
    // The class of the column family.
    cf_class: String,
    /// The read-write options.
    pub opts: ReadWriteOptions,
    /// The metrics for the database.
    db_metrics: Arc<DBMetrics>,
    /// The sampling interval for the database.
    get_sample_interval: SamplingInterval,
    /// The sampling interval for multi-get operations.
    multiget_sample_interval: SamplingInterval,
    /// The sampling interval for write operations.
    write_sample_interval: SamplingInterval,
    /// The sampling interval for iterator operations.
    iter_sample_interval: SamplingInterval,
    /// The cancel handle for the metrics task.
    _metrics_task_cancel_handle: Arc<oneshot::Sender<()>>,
}

unsafe impl<K: Send, V: Send> Send for DBMap<K, V> {}

impl<K, V> DBMap<K, V> {
    pub(crate) fn new(
        db: Arc<RocksDB>,
        opts: &ReadWriteOptions,
        opt_cf: &str,
        opt_cf_class: &str,
        is_deprecated: bool,
    ) -> Self {
        let db_metrics = DBMetrics::get();
        let cf = opt_cf.to_string();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        if !is_deprecated {
            crate::metrics::spawn_db_metrics_reporter(
                db.clone(),
                vec![cf.clone()],
                db_metrics.clone(),
                receiver,
            );
        }
        DBMap {
            rocksdb: db.clone(),
            opts: opts.clone(),
            _phantom: PhantomData,
            cf: opt_cf.to_string(),
            cf_class: opt_cf_class.to_string(),
            db_metrics: db_metrics.clone(),
            _metrics_task_cancel_handle: Arc::new(sender),
            get_sample_interval: db.get_sampling_interval(),
            multiget_sample_interval: db.multiget_sampling_interval(),
            write_sample_interval: db.write_sampling_interval(),
            iter_sample_interval: db.iter_sampling_interval(),
        }
    }

    /// Opens a database from a path, with specific options and an optional column family.
    ///
    /// This database is used to perform operations on single column family, and parametrizes.
    /// all operations in `DBBatch` when writing across column families.
    #[tracing::instrument(
        level="debug",
        skip_all,
        fields(path = ?path.as_ref(),
        cf = ?opt_cf),
        err
    )]
    pub fn open<P: AsRef<Path>>(
        path: P,
        metric_conf: MetricConf,
        db_options: Option<rocksdb::Options>,
        opt_cf: Option<&str>,
        opt_cf_class: Option<&str>,
        rw_options: &ReadWriteOptions,
    ) -> Result<Self, TypedStoreError> {
        let cf_key = opt_cf.unwrap_or(rocksdb::DEFAULT_COLUMN_FAMILY_NAME);
        let cf_class = opt_cf_class.unwrap_or(cf_key);
        let cfs = vec![cf_key];
        let rocksdb = open_cf(path, db_options, metric_conf, &cfs)?;
        Ok(DBMap::new(rocksdb, rw_options, cf_key, cf_class, false))
    }

    /// Reopens an open database as a typed map operating under a specific column family.
    /// if no column family is passed, the default column family is used.
    ///
    /// ```
    ///    use typed_store::rocks::*;
    ///    use typed_store::metrics::DBMetrics;
    ///    use tempfile::tempdir;
    ///    use prometheus::Registry;
    ///    use std::sync::Arc;
    ///    use core::fmt::Error;
    ///    #[tokio::main]
    ///    async fn main() -> Result<(), Error> {
    ///    /// Open the DB with all needed column families first.
    ///    let rocks = open_cf(
    ///     tempdir().unwrap(),
    ///     None,
    ///     MetricConf::default(),
    ///     &["First_CF", "Second_CF"],
    /// ).unwrap();
    ///    /// Attach the column families to specific maps.
    ///    let db_cf_1 = DBMap::<u32,u32>::reopen(
    ///     &rocks,
    ///     Some("First_CF"),
    ///     &ReadWriteOptions::default(),
    ///     false,
    /// ).expect("Failed to open storage");
    ///    let db_cf_2 = DBMap::<u32,u32>::reopen(
    ///     &rocks,
    ///     Some("Second_CF"),
    ///     &ReadWriteOptions::default(),
    ///     false,
    /// ).expect("Failed to open storage");
    ///    Ok(())
    ///    }
    /// ```
    #[tracing::instrument(level = "debug", skip(db), err)]
    pub fn reopen(
        db: &Arc<RocksDB>,
        opt_cf: Option<&str>,
        rw_options: &ReadWriteOptions,
        is_deprecated: bool,
    ) -> Result<Self, TypedStoreError> {
        Self::reopen_with_class(db, opt_cf, None, rw_options, is_deprecated)
    }

    /// Reopens an open database as a typed map operating under a specific column family.
    /// if no column family is passed, the default column family is used.
    /// Allows specifying a separate column family class for metrics reporting.
    ///
    /// ```
    ///    use typed_store::rocks::*;
    ///    use typed_store::metrics::DBMetrics;
    ///    use tempfile::tempdir;
    ///    use prometheus::Registry;
    ///    use std::sync::Arc;
    ///    use core::fmt::Error;
    ///    #[tokio::main]
    ///    async fn main() -> Result<(), Error> {
    ///    /// Open the DB with all needed column families first.
    ///    let rocks = open_cf(
    ///     tempdir().unwrap(),
    ///     None,
    ///     MetricConf::default(),
    ///     &["First_CF", "Second_CF"],
    /// ).unwrap();
    ///    /// Attach the column families to specific maps.
    ///    let db_cf_1 = DBMap::<u32,u32>::reopen_with_class(
    ///     &rocks,
    ///     Some("First_CF"),
    ///     Some("FirstClass"),
    ///     &ReadWriteOptions::default(),
    ///     false,
    /// ).expect("Failed to open storage");
    ///    let db_cf_2 = DBMap::<u32,u32>::reopen_with_class(
    ///     &rocks,
    ///     Some("Second_CF"),
    ///     Some("SecondClass"),
    ///     &ReadWriteOptions::default(),
    ///     false,
    /// ).expect("Failed to open storage");
    ///    Ok(())
    ///    }
    /// ```
    #[tracing::instrument(level = "debug", skip(db), err)]
    pub fn reopen_with_class(
        db: &Arc<RocksDB>,
        opt_cf: Option<&str>,
        opt_cf_class: Option<&str>,
        rw_options: &ReadWriteOptions,
        is_deprecated: bool,
    ) -> Result<Self, TypedStoreError> {
        let cf_key = opt_cf
            .unwrap_or(rocksdb::DEFAULT_COLUMN_FAMILY_NAME)
            .to_owned();

        let cf_class = opt_cf_class.unwrap_or(&cf_key).to_owned();

        db.cf_handle(&cf_key)
            .ok_or_else(|| TypedStoreError::UnregisteredColumn(cf_key.clone()))?;

        Ok(DBMap::new(
            db.clone(),
            rw_options,
            &cf_key,
            &cf_class,
            is_deprecated,
        ))
    }

    /// Get the column family name.
    pub fn cf_name(&self) -> &str {
        &self.cf
    }

    /// Create a new batch associated with a DB reference.
    pub fn batch(&self) -> DBBatch {
        let batch = match *self.rocksdb {
            RocksDB::DB(_) => RocksDBBatch::DB(WriteBatch::default()),
            RocksDB::OptimisticTransactionDB(_) => {
                RocksDBBatch::OptimisticTransactionDB(WriteBatchWithTransaction::<true>::default())
            }
        };
        DBBatch::new(
            &self.rocksdb,
            batch,
            self.opts.writeopts(),
            &self.db_metrics,
            &self.write_sample_interval,
        )
    }

    /// Compact a range of keys in a specific column family.
    pub fn compact_range<J: Serialize>(&self, start: &J, end: &J) -> Result<(), TypedStoreError> {
        let from_buf = be_fix_int_ser(start)?;
        let to_buf = be_fix_int_ser(end)?;
        self.rocksdb
            .compact_range_cf(&self.cf()?, Some(from_buf), Some(to_buf));
        Ok(())
    }

    /// Compact a range of keys in a specific column family.
    pub fn compact_range_to_bottom<J: Serialize>(
        &self,
        start: &J,
        end: &J,
    ) -> Result<(), TypedStoreError> {
        let from_buf = be_fix_int_ser(start)?;
        let to_buf = be_fix_int_ser(end)?;
        self.rocksdb
            .compact_range_to_bottom(&self.cf()?, Some(from_buf), Some(to_buf));
        Ok(())
    }

    /// Get the column family.
    pub fn cf(&self) -> Result<Arc<rocksdb::BoundColumnFamily<'_>>, TypedStoreError> {
        self.rocksdb
            .cf_handle(&self.cf)
            .ok_or_else(|| TypedStoreError::UnregisteredColumn(self.cf.clone()))
    }

    /// Flush the column family.
    pub fn flush(&self) -> Result<(), TypedStoreError> {
        self.rocksdb
            .flush_cf(&self.cf()?)
            .map_err(|e| TypedStoreError::RocksDBError(e.into_string()))
    }

    /// Ingest external SST files into this map's column family.
    /// Ingesting into an existing column family is allowed. If any ingested keys
    /// already exist in the target CF, RocksDB will, by default, assign a global
    /// sequence number to the ingested entries so that they are treated as the
    /// latest versions. Reads will then observe the ingested values for those
    /// conflicting keys. Behavior can be tuned via `IngestExternalFileOptions`
    /// (e.g., file move vs copy, and other ingestion modes provided by RocksDB).
    pub fn ingest_external_file<P: AsRef<Path>>(
        &self,
        paths: &[P],
        opts: &IngestExternalFileOptions,
    ) -> Result<(), TypedStoreError> {
        self.rocksdb
            .ingest_external_file_cf(&self.cf()?, paths, opts)
            .map_err(typed_store_err_from_rocks_err)
    }

    /// Returns a vector of raw values corresponding to the keys provided.
    fn multi_get_pinned<J>(
        &self,
        keys: impl IntoIterator<Item = J>,
    ) -> Result<Vec<Option<DBPinnableSlice<'_>>>, TypedStoreError>
    where
        J: Borrow<K>,
        K: Serialize,
    {
        let _timer = self
            .db_metrics
            .op_metrics
            .rocksdb_multiget_latency_seconds
            .with_label_values(&[&self.cf_class])
            .start_timer();
        let perf_ctx = if self.multiget_sample_interval.sample() {
            Some(RocksDBPerfContext)
        } else {
            None
        };
        let keys_bytes: Result<Vec<_>, _> = keys
            .into_iter()
            .map(|k| be_fix_int_ser(k.borrow()))
            .collect();
        let keys_bytes = keys_bytes?;
        let keys_refs = keys_bytes.iter().collect::<Vec<&Vec<u8>>>();
        let results: Result<Vec<_>, TypedStoreError> = self
            .rocksdb
            .batched_multi_get_cf_opt(
                &self.cf()?,
                keys_refs,
                /*sorted_keys=*/ false,
                &self.opts.readopts(),
            )
            .into_iter()
            .map(|r| r.map_err(|e| TypedStoreError::RocksDBError(e.into_string())))
            .collect();
        let entries = results?;
        let entry_size = entries
            .iter()
            .flatten()
            .map(|entry| entry.len())
            .sum::<usize>();
        self.db_metrics
            .op_metrics
            .rocksdb_multiget_bytes
            .with_label_values(&[&self.cf_class])
            .observe(entry_size as f64);
        if perf_ctx.is_some() {
            self.db_metrics
                .read_perf_ctx_metrics
                .report_metrics(&self.cf);
        }
        Ok(entries)
    }

    /// Create a checkpoint of the database.
    pub fn checkpoint_db(&self, path: &Path) -> Result<(), TypedStoreError> {
        self.rocksdb.checkpoint(path)
    }

    /// Get a summary of the table.
    pub fn table_summary(&self) -> eyre::Result<TableSummary>
    where
        K: Serialize + DeserializeOwned,
        V: Serialize + DeserializeOwned,
    {
        let mut num_keys = 0;
        let mut key_bytes_total = 0;
        let mut value_bytes_total = 0;
        let mut key_hist = hdrhistogram::Histogram::<u64>::new_with_max(100000, 2)
            .expect("the function parameters are valid");
        let mut value_hist = hdrhistogram::Histogram::<u64>::new_with_max(100000, 2)
            .expect("the function parameters are valid");
        for item in self.safe_iter()? {
            let (key, value) = item?;
            num_keys += 1;
            let key_len = be_fix_int_ser(key.borrow())?.len();
            let value_len = bcs::to_bytes(value.borrow())?.len();
            key_bytes_total += key_len;
            value_bytes_total += value_len;
            key_hist.record(key_len as u64)?;
            value_hist.record(value_len as u64)?;
        }
        Ok(TableSummary {
            num_keys,
            key_bytes_total,
            value_bytes_total,
            key_hist,
            value_hist,
        })
    }

    // Creates metrics and context for tracking an iterator usage and performance.
    fn create_iter_context(&self) -> IterContext {
        let timer = self
            .db_metrics
            .op_metrics
            .rocksdb_iter_latency_seconds
            .with_label_values(&[&self.cf_class])
            .start_timer();
        let bytes_scanned = self
            .db_metrics
            .op_metrics
            .rocksdb_iter_bytes
            .with_label_values(&[&self.cf_class]);
        let key_bytes_scanned = self
            .db_metrics
            .op_metrics
            .rocksdb_iter_key_bytes
            .with_label_values(&[&self.cf_class]);
        let value_bytes_scanned = self
            .db_metrics
            .op_metrics
            .rocksdb_iter_value_bytes
            .with_label_values(&[&self.cf_class]);
        let keys_scanned = self
            .db_metrics
            .op_metrics
            .rocksdb_iter_keys
            .with_label_values(&[&self.cf_class]);
        let perf_ctx = if self.iter_sample_interval.sample() {
            Some(RocksDBPerfContext)
        } else {
            None
        };
        IterContext {
            _timer: Some(timer),
            iter_bytes: Some(bytes_scanned),
            key_bytes_scanned: Some(key_bytes_scanned),
            value_bytes_scanned: Some(value_bytes_scanned),
            keys_scanned: Some(keys_scanned),
            _perf_ctx: perf_ctx,
        }
    }

    /// Creates a RocksDB read option with specified lower and upper bounds.
    ///
    /// Lower bound is inclusive, and upper bound is exclusive.
    fn create_read_options_with_bounds(
        &self,
        lower_bound: Option<K>,
        upper_bound: Option<K>,
    ) -> ReadOptions
    where
        K: Serialize,
    {
        let mut readopts = self.opts.readopts();
        if let Some(lower_bound) = lower_bound {
            let key_buf = be_fix_int_ser(&lower_bound).expect("serialization must not fail");
            readopts.set_iterate_lower_bound(key_buf);
        }
        if let Some(upper_bound) = upper_bound {
            let key_buf = be_fix_int_ser(&upper_bound).expect("serialization must not fail");
            readopts.set_iterate_upper_bound(key_buf);
        }
        readopts
    }

    /// Creates a safe reversed iterator with optional bounds.
    /// Upper bound is included.
    pub fn reversed_safe_iter_with_bounds(
        &self,
        lower_bound: Option<K>,
        upper_bound: Option<K>,
    ) -> Result<SafeRevIter<'_, K, V>, TypedStoreError>
    where
        K: Serialize + DeserializeOwned,
        V: Serialize + DeserializeOwned,
    {
        let upper_bound_key = upper_bound.as_ref().map(|k| be_fix_int_ser(&k));
        let readopts = self.create_read_options_with_range((
            lower_bound
                .as_ref()
                .map(Bound::Included)
                .unwrap_or(Bound::Unbounded),
            upper_bound
                .as_ref()
                .map(Bound::Included)
                .unwrap_or(Bound::Unbounded),
        ));

        let db_iter = self.rocksdb.raw_iterator_cf(&self.cf()?, readopts);
        let iter_context = self.create_iter_context();
        let iter = SafeIter::new(
            self.cf.clone(),
            db_iter,
            iter_context,
            Some(self.db_metrics.clone()),
        );
        Ok(SafeRevIter::new(iter, upper_bound_key.transpose()?))
    }

    // Creates a RocksDB read option with lower and upper bounds set corresponding to `range`.
    fn create_read_options_with_range(&self, range: impl RangeBounds<K>) -> ReadOptions
    where
        K: Serialize,
    {
        let mut readopts = self.opts.readopts();

        let lower_bound = range.start_bound();
        let upper_bound = range.end_bound();

        match lower_bound {
            Bound::Included(lower_bound) => {
                // Rocksdb lower bound is inclusive by default so nothing to do.
                let key_buf = be_fix_int_ser(&lower_bound).expect("serialization must not fail");
                readopts.set_iterate_lower_bound(key_buf);
            }
            Bound::Excluded(lower_bound) => {
                let mut key_buf =
                    be_fix_int_ser(&lower_bound).expect("serialization must not fail");

                // Since we want exclusive, we need to increment the key to exclude the previous.
                big_endian_saturating_add_one(&mut key_buf);
                readopts.set_iterate_lower_bound(key_buf);
            }
            Bound::Unbounded => (),
        };

        match upper_bound {
            Bound::Included(upper_bound) => {
                let mut key_buf =
                    be_fix_int_ser(&upper_bound).expect("serialization must not fail");

                // If the key is already at the limit, there's nowhere else to go,
                // so no upper bound.
                if !is_max(&key_buf) {
                    // Since we want exclusive, we need to increment the key to
                    // get the upper bound.
                    big_endian_saturating_add_one(&mut key_buf);
                    readopts.set_iterate_upper_bound(key_buf);
                }
            }
            Bound::Excluded(upper_bound) => {
                // Rocksdb upper bound is inclusive by default so nothing to do.
                let key_buf = be_fix_int_ser(&upper_bound).expect("serialization must not fail");
                readopts.set_iterate_upper_bound(key_buf);
            }
            Bound::Unbounded => (),
        };

        readopts
    }
}

/// Provides a mutable struct to form a collection of database write operations, and execute them.
///
/// Batching write and delete operations is faster than performing them one by one and ensures.
/// their atomicity, ie. they are all written or none is.
/// This is also true of operations across column families in the same database.
///
/// Serializations / Deserialization, and naming of column families is performed by passing.
/// a DBMap<K,V>.
/// with each operation.
///
/// ```
/// use typed_store::rocks::*;
/// use tempfile::tempdir;
/// use typed_store::Map;
/// use typed_store::metrics::DBMetrics;
/// use typed_store::rocks::MetricConf;
/// use prometheus::Registry;
/// use core::fmt::Error;
/// use std::sync::Arc;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Error> {
/// let rocks = open_cf(tempfile::tempdir().unwrap(), None,
///     MetricConf::default(),
///     &["First_CF", "Second_CF"],
/// )
/// .unwrap();
///
/// let db_cf_1 = DBMap::reopen(&rocks, Some("First_CF"), &ReadWriteOptions::default(), false)
///     .expect("Failed to open storage");
/// let keys_vals_1 = (1..100).map(|i| (i, i.to_string()));
///
/// let db_cf_2 = DBMap::reopen(&rocks, Some("Second_CF"), &ReadWriteOptions::default(), false)
///     .expect("Failed to open storage");
/// let keys_vals_2 = (1000..1100).map(|i| (i, i.to_string()));
///
/// let mut batch = db_cf_1.batch();
/// batch
///     .insert_batch(&db_cf_1, keys_vals_1.clone())
///     .expect("Failed to batch insert")
///     .insert_batch(&db_cf_2, keys_vals_2.clone())
///     .expect("Failed to batch insert");
///
/// let _ = batch.write().expect("Failed to execute batch");
/// for (k, v) in keys_vals_1 {
///     let val = db_cf_1.get(&k).expect("Failed to get inserted key");
///     assert_eq!(Some(v), val);
/// }
///
/// for (k, v) in keys_vals_2 {
///     let val = db_cf_2.get(&k).expect("Failed to get inserted key");
///     assert_eq!(Some(v), val);
/// }
/// Ok(())
/// }
/// ```
///
pub struct DBBatch {
    /// The rocksDB database.
    rocksdb: Arc<RocksDB>,
    /// The batch of write operations.
    batch: RocksDBBatch,
    /// The write options.
    opts: WriteOptions,
    /// The metrics for the database.
    db_metrics: Arc<DBMetrics>,
    /// The sampling interval for write operations.
    write_sample_interval: SamplingInterval,
}

impl fmt::Debug for DBBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DBBatch")
    }
}

impl DBBatch {
    /// Create a new batch associated with a DB reference.
    ///
    /// Use `open_cf` to get the DB reference or an existing open database.
    pub fn new(
        dbref: &Arc<RocksDB>,
        batch: RocksDBBatch,
        opts: WriteOptions,
        db_metrics: &Arc<DBMetrics>,
        write_sample_interval: &SamplingInterval,
    ) -> Self {
        DBBatch {
            rocksdb: dbref.clone(),
            batch,
            opts,
            db_metrics: db_metrics.clone(),
            write_sample_interval: write_sample_interval.clone(),
        }
    }

    /// Consume the batch and write its operations to the database.
    #[tracing::instrument(level = "trace", skip_all, err)]
    pub fn write(self) -> Result<(), TypedStoreError> {
        let db_name = self.rocksdb.db_name();
        let timer = self
            .db_metrics
            .op_metrics
            .rocksdb_batch_commit_latency_seconds
            .with_label_values(&[&db_name])
            .start_timer();
        let batch_size = self.batch.size_in_bytes();

        let perf_ctx = if self.write_sample_interval.sample() {
            Some(RocksDBPerfContext)
        } else {
            None
        };
        self.rocksdb.write(self.batch, &self.opts)?;
        self.db_metrics
            .op_metrics
            .rocksdb_batch_commit_bytes
            .with_label_values(&[&db_name])
            .observe(batch_size as f64);

        if perf_ctx.is_some() {
            self.db_metrics
                .write_perf_ctx_metrics
                .report_metrics(&db_name);
        }
        let elapsed = timer.stop_and_record();
        if elapsed > 1.0 {
            tracing::warn!(?elapsed, ?db_name, "very slow batch write");
            self.db_metrics
                .op_metrics
                .rocksdb_very_slow_batch_writes_count
                .with_label_values(&[&db_name])
                .inc();
            self.db_metrics
                .op_metrics
                .rocksdb_very_slow_batch_writes_duration_ms
                .with_label_values(&[&db_name])
                .inc_by((elapsed * 1000.0) as u64);
        }
        Ok(())
    }

    /// Get the size of the batch in bytes.
    pub fn size_in_bytes(&self) -> usize {
        self.batch.size_in_bytes()
    }
}

impl DBBatch {
    /// Delete a batch of keys.
    pub fn delete_batch<J: Borrow<K>, K: Serialize, V>(
        &mut self,
        db: &DBMap<K, V>,
        purged_vals: impl IntoIterator<Item = J>,
    ) -> Result<(), TypedStoreError> {
        if !Arc::ptr_eq(&db.rocksdb, &self.rocksdb) {
            return Err(TypedStoreError::CrossDBBatch);
        }

        purged_vals
            .into_iter()
            .try_for_each::<_, Result<_, TypedStoreError>>(|k| {
                let k_buf = be_fix_int_ser(k.borrow())?;
                self.batch.delete_cf(&db.cf()?, k_buf);

                Ok(())
            })?;
        Ok(())
    }

    /// Deletes a range of keys between `from` (inclusive) and `to` (non-inclusive)
    /// by writing a range delete tombstone in the db map.
    /// If the DBMap is configured with ignore_range_deletions set to false,
    /// the effect of this write will be visible immediately i.e. you won't.
    /// see old values when you do a lookup or scan. But if it is configured.
    /// with ignore_range_deletions set to true, the old value are visible until.
    /// compaction actually deletes them which will happen sometime after. By.
    /// default ignore_range_deletions is set to true on a DBMap (unless it is.
    /// overridden in the config), so please use this function with caution.
    pub fn schedule_delete_range<K: Serialize, V>(
        &mut self,
        db: &DBMap<K, V>,
        from: &K,
        to: &K,
        cap: &RangeDeleteHandle<'_>,
    ) -> Result<(), TypedStoreError> {
        if !Arc::ptr_eq(&db.rocksdb, &self.rocksdb) {
            return Err(TypedStoreError::CrossDBBatch);
        }

        let from_buf = be_fix_int_ser(from)?;
        let to_buf = be_fix_int_ser(to)?;

        self.batch
            .delete_range_cf(&db.cf()?, from_buf, to_buf, cap)?;
        Ok(())
    }

    /// inserts a range of (key, value) pairs given as an iterator.
    pub fn insert_batch<J: Borrow<K>, K: Serialize, U: Borrow<V>, V: Serialize>(
        &mut self,
        db: &DBMap<K, V>,
        new_vals: impl IntoIterator<Item = (J, U)>,
    ) -> Result<&mut Self, TypedStoreError> {
        if !Arc::ptr_eq(&db.rocksdb, &self.rocksdb) {
            return Err(TypedStoreError::CrossDBBatch);
        }
        let mut key_total = 0usize;
        let mut value_total = 0usize;
        new_vals
            .into_iter()
            .try_for_each::<_, Result<_, TypedStoreError>>(|(k, v)| {
                let k_buf = be_fix_int_ser(k.borrow())?;
                let v_buf = bcs::to_bytes(v.borrow()).map_err(typed_store_err_from_bcs_err)?;
                key_total += k_buf.len();
                value_total += v_buf.len();
                self.batch.put_cf(&db.cf()?, k_buf, v_buf);
                Ok(())
            })?;
        self.db_metrics
            .op_metrics
            .rocksdb_batch_put_key_bytes
            .with_label_values(&[&db.cf_class])
            .observe(key_total as f64);
        self.db_metrics
            .op_metrics
            .rocksdb_batch_put_value_bytes
            .with_label_values(&[&db.cf_class])
            .observe(value_total as f64);
        Ok(self)
    }

    /// Inserts a range of (key, value) pairs given as an iterator.
    pub fn partial_merge_batch<J: Borrow<K>, K: Serialize, V: Serialize, B: AsRef<[u8]>>(
        &mut self,
        db: &DBMap<K, V>,
        new_vals: impl IntoIterator<Item = (J, B)>,
    ) -> Result<&mut Self, TypedStoreError> {
        if !Arc::ptr_eq(&db.rocksdb, &self.rocksdb) {
            return Err(TypedStoreError::CrossDBBatch);
        }
        new_vals
            .into_iter()
            .try_for_each::<_, Result<_, TypedStoreError>>(|(k, v)| {
                let k_buf = be_fix_int_ser(k.borrow())?;
                self.batch.merge_cf(&db.cf()?, k_buf, v);
                Ok(())
            })?;
        Ok(self)
    }
}

macro_rules! delegate_iter_call {
    ($self:ident.$method:ident($($args:ident),*)) => {
        match $self {
            Self::DB(db) => db.$method($($args),*),
            Self::OptimisticTransactionDB(db) => db.$method($($args),*),
        }
    }
}

/// The raw iterator for the rocksdb.
pub enum RocksDBRawIter<'a> {
    /// Raw iterator variant for a standard `rocksdb::DB`.
    DB(rocksdb::DBRawIteratorWithThreadMode<'a, DBWithThreadMode<MultiThreaded>>),
    /// Raw iterator variant for an `rocksdb::OptimisticTransactionDB`.
    OptimisticTransactionDB(
        rocksdb::DBRawIteratorWithThreadMode<'a, OptimisticTransactionDB<MultiThreaded>>,
    ),
}

impl<'a> fmt::Debug for RocksDBRawIter<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DB(_) => write!(f, "RocksDBRawIter::DB(..)"),
            Self::OptimisticTransactionDB(_) => {
                write!(f, "RocksDBRawIter::OptimisticTransactionDB(..)")
            }
        }
    }
}

impl<'a> RocksDBRawIter<'a> {
    /// Returns whether the iterator points to a valid entry.
    pub fn valid(&self) -> bool {
        delegate_iter_call!(self.valid())
    }
    /// Returns the current key, if any.
    pub fn key(&self) -> Option<&[u8]> {
        delegate_iter_call!(self.key())
    }
    /// Returns the current value, if any.
    pub fn value(&self) -> Option<&[u8]> {
        delegate_iter_call!(self.value())
    }
    /// Advances the iterator to the next entry.
    pub fn next(&mut self) {
        delegate_iter_call!(self.next())
    }
    /// Moves the iterator to the previous entry.
    pub fn prev(&mut self) {
        delegate_iter_call!(self.prev())
    }
    /// Seeks the iterator to the first entry at or after the given key.
    pub fn seek<K: AsRef<[u8]>>(&mut self, key: K) {
        delegate_iter_call!(self.seek(key))
    }
    /// Seeks the iterator to the last entry.
    pub fn seek_to_last(&mut self) {
        delegate_iter_call!(self.seek_to_last())
    }
    /// Seeks the iterator to the first entry.
    pub fn seek_to_first(&mut self) {
        delegate_iter_call!(self.seek_to_first())
    }
    /// Seeks the iterator to the last key that is less than or equal to the given key.
    pub fn seek_for_prev<K: AsRef<[u8]>>(&mut self, key: K) {
        delegate_iter_call!(self.seek_for_prev(key))
    }
    /// Returns the iterator status.
    pub fn status(&self) -> Result<(), rocksdb::Error> {
        delegate_iter_call!(self.status())
    }
}

impl<'a, K, V> Map<'a, K, V> for DBMap<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    type Error = TypedStoreError;
    type SafeIterator = SafeIter<'a, K, V>;

    #[tracing::instrument(level = "trace", skip_all, err)]
    fn contains_key(&self, key: &K) -> Result<bool, TypedStoreError> {
        let start = std::time::Instant::now();
        let key_buf = be_fix_int_ser(key)?;
        // [`rocksdb::DBWithThreadMode::key_may_exist_cf`] can have false positives,
        // but no false negatives. We use it to short-circuit the absent case.
        let readopts = self.opts.readopts();
        let may_exist = self
            .rocksdb
            .key_may_exist_cf(&self.cf()?, &key_buf, &readopts);
        if may_exist {
            self.db_metrics
                .op_metrics
                .rocksdb_bloom_filter_may_exist_true_total
                .with_label_values(&[&self.cf_class])
                .inc();
        }
        let found = if may_exist {
            let pinned = self
                .rocksdb
                .get_pinned_cf_opt(&self.cf()?, &key_buf, &readopts)
                .map_err(typed_store_err_from_rocks_err)?;
            if pinned.is_none() {
                self.db_metrics
                    .op_metrics
                    .rocksdb_bloom_filter_false_positive_total
                    .with_label_values(&[&self.cf_class])
                    .inc();
            }
            pinned.is_some()
        } else {
            false
        };
        self.db_metrics
            .op_metrics
            .rocksdb_contains_key_latency_seconds
            .with_label_values(&[&self.cf_class, &found.to_string()])
            .observe(start.elapsed().as_secs_f64());
        Ok(found)
    }

    #[tracing::instrument(level = "trace", skip_all, err)]
    fn multi_contains_keys<J>(
        &self,
        keys: impl IntoIterator<Item = J>,
    ) -> Result<Vec<bool>, Self::Error>
    where
        J: Borrow<K>,
    {
        let values = self.multi_get_pinned(keys)?;
        Ok(values.into_iter().map(|v| v.is_some()).collect())
    }

    #[tracing::instrument(level = "trace", skip_all, err)]
    fn get(&self, key: &K) -> Result<Option<V>, TypedStoreError> {
        let start = std::time::Instant::now();
        let perf_ctx = if self.get_sample_interval.sample() {
            Some(RocksDBPerfContext)
        } else {
            None
        };
        let key_buf = be_fix_int_ser(key)?;
        let res = self
            .rocksdb
            .get_pinned_cf_opt(&self.cf()?, &key_buf, &self.opts.readopts())
            .map_err(typed_store_err_from_rocks_err)?;
        let found = res.is_some();
        self.db_metrics
            .op_metrics
            .rocksdb_get_latency_seconds
            .with_label_values(&[&self.cf_class, &found.to_string()])
            .observe(start.elapsed().as_secs_f64());
        self.db_metrics
            .op_metrics
            .rocksdb_get_key_bytes
            .with_label_values(&[&self.cf_class])
            .observe(key_buf.len() as f64);
        self.db_metrics
            .op_metrics
            .rocksdb_get_bytes
            .with_label_values(&[&self.cf_class])
            .observe(key_buf.len() as f64 + res.as_ref().map_or(0.0, |v| v.len() as f64));
        self.db_metrics
            .op_metrics
            .rocksdb_get_value_bytes
            .with_label_values(&[&self.cf_class])
            .observe(res.as_ref().map_or(0.0, |v| v.len() as f64));
        if perf_ctx.is_some() {
            self.db_metrics
                .read_perf_ctx_metrics
                .report_metrics(&self.cf);
        }
        match res {
            Some(data) => Ok(Some(
                bcs::from_bytes(&data).map_err(typed_store_err_from_bcs_err)?,
            )),
            None => Ok(None),
        }
    }

    #[tracing::instrument(level = "trace", skip_all, err)]
    fn insert(&self, key: &K, value: &V) -> Result<(), TypedStoreError> {
        let timer = self
            .db_metrics
            .op_metrics
            .rocksdb_put_latency_seconds
            .with_label_values(&[&self.cf_class])
            .start_timer();
        let perf_ctx = if self.write_sample_interval.sample() {
            Some(RocksDBPerfContext)
        } else {
            None
        };
        let key_buf = be_fix_int_ser(key)?;
        let value_buf = bcs::to_bytes(value).map_err(typed_store_err_from_bcs_err)?;
        self.db_metrics
            .op_metrics
            .rocksdb_put_key_bytes
            .with_label_values(&[&self.cf_class])
            .observe(key_buf.len() as f64);
        self.db_metrics
            .op_metrics
            .rocksdb_put_value_bytes
            .with_label_values(&[&self.cf_class])
            .observe(value_buf.len() as f64);
        self.db_metrics
            .op_metrics
            .rocksdb_put_bytes
            .with_label_values(&[&self.cf_class])
            .observe(key_buf.len() as f64 + value_buf.len() as f64);
        if perf_ctx.is_some() {
            self.db_metrics
                .write_perf_ctx_metrics
                .report_metrics(&self.cf);
        }
        self.rocksdb
            .put_cf(&self.cf()?, &key_buf, &value_buf, &self.opts.writeopts())
            .map_err(typed_store_err_from_rocks_err)?;

        let elapsed = timer.stop_and_record();
        if elapsed > 1.0 {
            tracing::warn!(?elapsed, cf = ?self.cf, "very slow insert");
            self.db_metrics
                .op_metrics
                .rocksdb_very_slow_puts_count
                .with_label_values(&[&self.cf_class])
                .inc();
            self.db_metrics
                .op_metrics
                .rocksdb_very_slow_puts_duration_ms
                .with_label_values(&[&self.cf_class])
                .inc_by((elapsed * 1000.0) as u64);
        }

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, err)]
    fn remove(&self, key: &K) -> Result<(), TypedStoreError> {
        let _timer = self
            .db_metrics
            .op_metrics
            .rocksdb_delete_latency_seconds
            .with_label_values(&[&self.cf_class])
            .start_timer();
        let perf_ctx = if self.write_sample_interval.sample() {
            Some(RocksDBPerfContext)
        } else {
            None
        };
        let key_buf = be_fix_int_ser(key)?;
        self.rocksdb
            .delete_cf(&self.cf()?, key_buf, &self.opts.writeopts())
            .map_err(typed_store_err_from_rocks_err)?;
        self.db_metrics
            .op_metrics
            .rocksdb_deletes
            .with_label_values(&[&self.cf_class])
            .inc();
        if perf_ctx.is_some() {
            self.db_metrics
                .write_perf_ctx_metrics
                .report_metrics(&self.cf);
        }
        Ok(())
    }

    /// This method first drops the existing column family and then creates a new one.
    /// with the same name. The two operations are not atomic and hence it is possible.
    /// to get into a race condition where the column family has been dropped but new.
    /// one is not created yet.
    #[tracing::instrument(level = "trace", skip_all, err)]
    fn unsafe_clear(&self) -> Result<(), TypedStoreError> {
        let _ = self.rocksdb.drop_cf(&self.cf);
        self.rocksdb
            .create_cf(self.cf.clone(), &default_db_options().options)
            .map_err(typed_store_err_from_rocks_err)?;
        Ok(())
    }

    /// Writes a range delete tombstone to delete all entries in the db map.
    /// If the DBMap is configured with ignore_range_deletions set to false,
    /// the effect of this write will be visible immediately i.e. you won't.
    /// see old values when you do a lookup or scan. But if it is configured.
    /// with ignore_range_deletions set to true, the old value are visible until.
    /// compaction actually deletes them which will happen sometime after. By.
    /// default ignore_range_deletions is set to true on a DBMap (unless it is.
    /// overridden in the config), so please use this function with caution.
    #[tracing::instrument(level = "trace", skip_all, err)]
    fn schedule_delete_all(&self) -> Result<(), TypedStoreError> {
        if let Some(cap) = self.rocksdb.as_range_delete() {
            let first_key = self.safe_iter()?.next().transpose()?.map(|(k, _v)| k);
            let last_key = self
                .reversed_safe_iter_with_bounds(None, None)?
                .next()
                .transpose()?
                .map(|(k, _v)| k);
            if let Some((first_key, last_key)) = first_key.zip(last_key) {
                let mut batch = self.batch();
                batch.schedule_delete_range(self, &first_key, &last_key, &cap)?;
                batch.write()?;
            }
            Ok(())
        } else {
            // Fallback to per-key deletes for engines that lack range delete.
            self.delete_all_individually()
        }
    }

    fn is_empty(&self) -> bool {
        self.safe_iter()
            .expect("safe_iter should not fail")
            .next()
            .is_none()
    }

    /// Deletes all entries by iterating keys and issuing per-key deletes (batched), for engines
    /// that do not support range deletes (e.g., OptimisticTransactionDB).
    /// This is less efficient than range deletes but safe and explicit.
    fn delete_all_individually(&self) -> Result<(), TypedStoreError>
    where
        K: Serialize + DeserializeOwned,
        V: Serialize + DeserializeOwned,
    {
        const BATCH_SIZE: usize = 10_000;
        let iter = self.safe_iter()?;
        let mut keys: Vec<K> = Vec::with_capacity(BATCH_SIZE);
        for item in iter {
            let (k, _v) = item?;
            keys.push(k);
            if keys.len() >= BATCH_SIZE {
                self.multi_remove(keys.drain(..))?;
            }
        }
        if !keys.is_empty() {
            self.multi_remove(keys.drain(..))?;
        }
        Ok(())
    }

    fn safe_iter(&'a self) -> Result<Self::SafeIterator, TypedStoreError> {
        let db_iter = self
            .rocksdb
            .raw_iterator_cf(&self.cf()?, self.opts.readopts());
        let iter_context = self.create_iter_context();
        Ok(SafeIter::new(
            self.cf.clone(),
            db_iter,
            iter_context,
            Some(self.db_metrics.clone()),
        ))
    }

    fn safe_iter_with_bounds(
        &'a self,
        lower_bound: Option<K>,
        upper_bound: Option<K>,
    ) -> Result<Self::SafeIterator, TypedStoreError> {
        let readopts = self.create_read_options_with_bounds(lower_bound, upper_bound);
        let db_iter = self.rocksdb.raw_iterator_cf(&self.cf()?, readopts);
        let iter_context = self.create_iter_context();
        Ok(SafeIter::new(
            self.cf.clone(),
            db_iter,
            iter_context,
            Some(self.db_metrics.clone()),
        ))
    }

    fn safe_range_iter(
        &'a self,
        range: impl RangeBounds<K>,
    ) -> Result<Self::SafeIterator, TypedStoreError> {
        let readopts = self.create_read_options_with_range(range);
        let db_iter = self.rocksdb.raw_iterator_cf(&self.cf()?, readopts);
        let iter_context = self.create_iter_context();
        Ok(SafeIter::new(
            self.cf.clone(),
            db_iter,
            iter_context,
            Some(self.db_metrics.clone()),
        ))
    }

    /// Returns a vector of values corresponding to the keys provided.
    #[tracing::instrument(level = "trace", skip_all, err)]
    fn multi_get<J>(
        &self,
        keys: impl IntoIterator<Item = J>,
    ) -> Result<Vec<Option<V>>, TypedStoreError>
    where
        J: Borrow<K>,
    {
        let results = self.multi_get_pinned(keys)?;
        let values_parsed: Result<Vec<_>, TypedStoreError> = results
            .into_iter()
            .map(|value_byte| match value_byte {
                Some(data) => Ok(Some(
                    bcs::from_bytes(&data).map_err(typed_store_err_from_bcs_err)?,
                )),
                None => Ok(None),
            })
            .collect();

        values_parsed
    }

    /// Convenience method for batch insertion.
    #[tracing::instrument(level = "trace", skip_all, err)]
    fn multi_insert<J, U>(
        &self,
        key_val_pairs: impl IntoIterator<Item = (J, U)>,
    ) -> Result<(), Self::Error>
    where
        J: Borrow<K>,
        U: Borrow<V>,
    {
        let mut batch = self.batch();
        batch.insert_batch(self, key_val_pairs)?;
        batch.write()
    }

    /// Convenience method for batch removal.
    #[tracing::instrument(level = "trace", skip_all, err)]
    fn multi_remove<J>(&self, keys: impl IntoIterator<Item = J>) -> Result<(), Self::Error>
    where
        J: Borrow<K>,
    {
        let mut batch = self.batch();
        batch.delete_batch(self, keys)?;
        batch.write()
    }

    /// Try to catch up with primary when running as secondary.
    #[tracing::instrument(level = "trace", skip_all, err)]
    fn try_catch_up_with_primary(&self) -> Result<(), Self::Error> {
        self.rocksdb
            .try_catch_up_with_primary()
            .map_err(typed_store_err_from_rocks_err)
    }
}

/// Read a size from an environment variable.
pub fn read_size_from_env(var_name: &str) -> Option<usize> {
    env::var(var_name)
        .ok()?
        .parse::<usize>()
        .tap_err(|e| {
            tracing::warn!(
                "Env var {} does not contain valid usize integer: {}",
                var_name,
                e
            )
        })
        .ok()
}

/// The read-write options.
#[derive(Clone, Debug)]
pub struct ReadWriteOptions {
    /// Whether to ignore range deletions.
    pub ignore_range_deletions: bool,
    // Whether to sync to disk on every write.
    sync_to_disk: bool,
}

impl ReadWriteOptions {
    /// The read options.
    pub fn readopts(&self) -> ReadOptions {
        let mut readopts = ReadOptions::default();
        readopts.set_ignore_range_deletions(self.ignore_range_deletions);
        readopts
    }

    /// The write options.
    pub fn writeopts(&self) -> WriteOptions {
        let mut opts = WriteOptions::default();
        opts.set_sync(self.sync_to_disk);
        opts
    }

    /// Set the ignore range deletions.
    pub fn set_ignore_range_deletions(mut self, ignore: bool) -> Self {
        self.ignore_range_deletions = ignore;
        self
    }
}

impl Default for ReadWriteOptions {
    fn default() -> Self {
        Self {
            ignore_range_deletions: true,
            sync_to_disk: std::env::var("SUI_DB_SYNC_TO_DISK").is_ok_and(|v| v != "0"),
        }
    }
}

/// The rocksdb options.
#[derive(Default, Clone)]
pub struct DBOptions {
    /// The rocksdb options.
    pub options: rocksdb::Options,
    /// The read-write options.
    pub rw_options: ReadWriteOptions,
}

impl fmt::Debug for DBOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DBOptions")
    }
}

/// Creates a default RocksDB option, to be used when RocksDB option is unspecified.
pub fn default_db_options() -> DBOptions {
    let mut opt = rocksdb::Options::default();

    // One common issue when running tests on Mac is that the default ulimit is too low,
    // leading to I/O errors such as "Too many open files". Raising fdlimit to bypass it.
    if let Ok(outcome) = fdlimit::raise_fd_limit() {
        let limit = match outcome {
            fdlimit::Outcome::LimitRaised { to, .. } => to,
            fdlimit::Outcome::Unsupported => {
                tracing::warn!("failed to raise fdlimit, defaulting to 1024");
                1024
            }
        };
        // on windows raise_fd_limit return None.
        opt.set_max_open_files((limit / 8) as i32);
    }

    // The table cache is locked for updates and this determines the number.
    // of shards, ie 2^10. Increase in case of lock contentions.
    opt.set_table_cache_num_shard_bits(10);

    // LSM compression settings.
    opt.set_compression_type(rocksdb::DBCompressionType::Lz4);
    opt.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);
    opt.set_bottommost_zstd_max_train_bytes(1024 * 1024, true);

    // Sui uses multiple RocksDB in a node, so total sizes of write buffers and WAL can be higher.
    // than the limits below.
    //
    // RocksDB also exposes the option to configure total write buffer size across.
    // multiple instances. But the write buffer flush policy (flushing the buffer.
    // receiving the next write) may not work well. So sticking to per-db write buffer.
    // size limit for now.
    //
    // The environment variables are only meant to be emergency overrides.
    // They may go away in future.
    // It is preferable to update the default value, or override the option in code.
    opt.set_db_write_buffer_size(
        read_size_from_env(ENV_VAR_DB_WRITE_BUFFER_SIZE).unwrap_or(DEFAULT_DB_WRITE_BUFFER_SIZE)
            * 1024
            * 1024,
    );
    opt.set_max_total_wal_size(
        read_size_from_env(ENV_VAR_DB_WAL_SIZE).unwrap_or(DEFAULT_DB_WAL_SIZE) as u64 * 1024 * 1024,
    );

    // Num threads for compactions and memtable flushes.
    opt.increase_parallelism(read_size_from_env(ENV_VAR_DB_PARALLELISM).unwrap_or(8) as i32);

    opt.set_enable_pipelined_write(true);

    // Increase block size to 16KiB.
    // https://github.com/EighteenZi/rocksdb_wiki/blob/master/.
    // Memory-usage-in-RocksDB.md#indexes-and-filter-blocks
    opt.set_block_based_table_factory(&get_block_options(
        // Use a dedicated 128MB block cache for the default column family.
        &Cache::new_lru_cache(128 << 20),
        Some(16 << 10),
        Some(true),
    ));

    // Set memtable bloomfilter.
    opt.set_memtable_prefix_bloom_ratio(0.02);

    DBOptions {
        options: opt,
        rw_options: ReadWriteOptions::default(),
    }
}

/// Get the block options.
///
/// Note that this function requires the caller to provide a block cache. If each column family
/// uses different block cache, the caller should create a block cache for each column family.
pub fn get_block_options(
    block_cache: &Cache,
    block_size_bytes: Option<usize>,
    pin_l0_filter_and_index_blocks_in_block_cache: Option<bool>,
) -> BlockBasedOptions {
    // https://github.com/facebook/rocksdb/blob/.
    // 11cb6af6e5009c51794641905ca40ce5beec7fee/options/options.cc#L611-L621.
    let mut block_options = BlockBasedOptions::default();
    // Overrides block size.
    if let Some(block_size_bytes) = block_size_bytes {
        block_options.set_block_size(block_size_bytes);
    }
    // Configure a block cache.
    block_options.set_block_cache(block_cache);
    block_options.set_cache_index_and_filter_blocks(true);
    // Set a bloomfilter with 1% false positive rate.
    block_options.set_bloom_filter(10.0, false);
    if let Some(pin_l0_filter_and_index_blocks_in_block_cache) =
        pin_l0_filter_and_index_blocks_in_block_cache
    {
        // From https://github.com/EighteenZi/rocksdb_wiki/blob/master/.
        // Block-Cache.md#caching-index-and-filter-blocks.
        block_options.set_pin_l0_filter_and_index_blocks_in_cache(
            pin_l0_filter_and_index_blocks_in_block_cache,
        );
    }
    block_options
}

/// Opens a database with options, and a number of column families that are created.
/// if they do not exist.
#[tracing::instrument(level="debug", skip_all, fields(path = ?path.as_ref(), cf = ?opt_cfs), err)]
pub fn open_cf<P: AsRef<Path>>(
    path: P,
    db_options: Option<rocksdb::Options>,
    metric_conf: MetricConf,
    opt_cfs: &[&str],
) -> Result<Arc<RocksDB>, TypedStoreError> {
    let options = db_options.unwrap_or_else(|| default_db_options().options);
    let column_descriptors: Vec<_> = opt_cfs
        .iter()
        .map(|name| (*name, options.clone()))
        .collect();
    open_cf_opts(
        path,
        Some(options.clone()),
        metric_conf,
        &column_descriptors[..],
    )
}

fn prepare_db_options(db_options: Option<rocksdb::Options>) -> rocksdb::Options {
    // Customize database options.
    let mut options = db_options.unwrap_or_else(|| default_db_options().options);
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    options
}

/// Opens a database with options, and a number of column families with individual options that.
/// are created if they do not exist.
#[tracing::instrument(level="debug", skip_all, fields(path = ?path.as_ref()), err)]
pub fn open_cf_opts<P: AsRef<Path>>(
    path: P,
    db_options: Option<rocksdb::Options>,
    metric_conf: MetricConf,
    opt_cfs: &[(&str, rocksdb::Options)],
) -> Result<Arc<RocksDB>, TypedStoreError> {
    let path = path.as_ref();
    // In the simulator, we intercept the wall clock in the test thread only. This causes problems.
    // because rocksdb uses the simulated clock when creating its background threads, but then.
    // those threads see the real wall clock (because they are not the test thread), which causes.
    // rocksdb to panic. The `nondeterministic` macro evaluates expressions in new threads, which.
    // resolves the issue.
    //
    // This is a no-op in non-simulator builds.

    let cfs = populate_missing_cfs(opt_cfs, path).map_err(typed_store_err_from_rocks_err)?;
    sui_macros::nondeterministic!({
        let options = prepare_db_options(db_options);
        let rocksdb = {
            rocksdb::DBWithThreadMode::<MultiThreaded>::open_cf_descriptors(
                &options,
                path,
                cfs.into_iter()
                    .map(|(name, opts)| ColumnFamilyDescriptor::new(name, opts)),
            )
            .map_err(typed_store_err_from_rocks_err)?
        };
        Ok(Arc::new(RocksDB::DB(DBWithThreadModeWrapper::new(
            rocksdb,
            metric_conf,
            PathBuf::from(path),
            options,
        ))))
    })
}

/// Opens an OptimisticTransactionDB with options, and a number of column families with.
/// individual options that are created if they do not exist.
#[tracing::instrument(level="debug", skip_all, fields(path = ?path.as_ref()), err)]
pub fn open_cf_opts_optimistic<P: AsRef<Path>>(
    path: P,
    db_options: Option<rocksdb::Options>,
    metric_conf: MetricConf,
    opt_cfs: &[(&str, rocksdb::Options)],
) -> Result<Arc<RocksDB>, TypedStoreError> {
    let path = path.as_ref();
    let cfs = populate_missing_cfs(opt_cfs, path).map_err(typed_store_err_from_rocks_err)?;
    sui_macros::nondeterministic!({
        let options = prepare_db_options(db_options);
        rocksdb::OptimisticTransactionDB::open_cf_descriptors(
            &options,
            path,
            cfs.into_iter()
                .map(|(name, opts)| ColumnFamilyDescriptor::new(name, opts)),
        )
        .map(|db| {
            Arc::new(RocksDB::OptimisticTransactionDB(
                OptimisticTransactionDBWrapper::new(db, metric_conf, PathBuf::from(path), options),
            ))
        })
        .map_err(typed_store_err_from_rocks_err)
    })
}

/// Opens an OptimisticTransactionDB with options, and a number of column families that are created.
/// if they do not exist. Uses the same options for each provided column family name.
#[tracing::instrument(level="debug", skip_all, fields(path = ?path.as_ref(), cf = ?opt_cfs), err)]
pub fn open_cf_optimistic<P: AsRef<Path>>(
    path: P,
    db_options: Option<rocksdb::Options>,
    metric_conf: MetricConf,
    opt_cfs: &[&str],
) -> Result<Arc<RocksDB>, TypedStoreError> {
    let options = db_options.unwrap_or_else(|| default_db_options().options);
    let column_descriptors: Vec<_> = opt_cfs
        .iter()
        .map(|name| (*name, options.clone()))
        .collect();
    let db = open_cf_opts_optimistic(path, Some(options), metric_conf, &column_descriptors[..])?;
    Ok(db)
}

/// TODO: Good description of why we're doing this :
/// RocksDB stores keys in BE and has a seek operator.
/// on iterators, see `https://github.com/facebook/rocksdb/wiki/Iterator#introduction`
#[inline]
pub fn be_fix_int_ser<S>(t: &S) -> Result<Vec<u8>, TypedStoreError>
where
    S: ?Sized + serde::Serialize,
{
    bincode::DefaultOptions::new()
        .with_big_endian()
        .with_fixint_encoding()
        .serialize(t)
        .map_err(typed_store_err_from_bincode_err)
}

/// The type of database access.
/// Safe drop the database.
pub fn safe_drop_db(path: PathBuf) -> Result<(), rocksdb::Error> {
    rocksdb::DB::destroy(&rocksdb::Options::default(), path)
}

/// Populate missing column families.
fn populate_missing_cfs(
    input_cfs: &[(&str, rocksdb::Options)],
    path: &Path,
) -> Result<Vec<(String, rocksdb::Options)>, rocksdb::Error> {
    let mut cfs = vec![];
    let input_cf_index: HashSet<_> = input_cfs.iter().map(|(name, _)| *name).collect();
    let existing_cfs =
        rocksdb::DBWithThreadMode::<MultiThreaded>::list_cf(&rocksdb::Options::default(), path)
            .ok()
            .unwrap_or_default();

    for cf_name in existing_cfs {
        if !input_cf_index.contains(&cf_name[..]) {
            cfs.push((cf_name, rocksdb::Options::default()));
        }
    }
    cfs.extend(
        input_cfs
            .iter()
            .map(|(name, opts)| (name.to_string(), (*opts).clone())),
    );
    Ok(cfs)
}

/// Given a vec<u8>, find the value which is one more than the vector.
/// if the vector was a big endian number.
/// If the vector is already minimum, don't change it.
fn big_endian_saturating_add_one(v: &mut [u8]) {
    if is_max(v) {
        return;
    }
    for i in (0..v.len()).rev() {
        if v[i] == u8::MAX {
            v[i] = 0;
        } else {
            v[i] += 1;
            break;
        }
    }
}

/// Check if all the bytes in the vector are 0xFF.
fn is_max(v: &[u8]) -> bool {
    v.iter().all(|&x| x == u8::MAX)
}

/// Build an SST file from raw key value bytes.
/// NOTE: Keys need to be pre-sorted and no duplicates are allowed or else ingester will
/// reject the file.
pub fn build_sst_from_kv<P, K, V, I>(sst_path: P, entries: I) -> Result<(), TypedStoreError>
where
    P: AsRef<Path>,
    K: Borrow<[u8]>,
    V: Borrow<[u8]>,
    I: IntoIterator<Item = (K, V)>,
{
    let entries_vec: Vec<(Vec<u8>, Vec<u8>)> = entries
        .into_iter()
        .map(|(k, v)| (k.borrow().to_vec(), v.borrow().to_vec()))
        .collect();
    let writer_opts = rocksdb::Options::default();
    let mut writer = SstFileWriter::create(&writer_opts);
    writer
        .open(sst_path.as_ref())
        .map_err(typed_store_err_from_rocks_err)?;
    for (k, v) in &entries_vec {
        writer.put(k, v).map_err(typed_store_err_from_rocks_err)?;
    }
    writer.finish().map_err(typed_store_err_from_rocks_err)?;
    Ok(())
}

/// Build an SST file from typed key value pairs using typed-store serialization.
/// Keys are serialized with big-endian fixed-int encoding and values use BCS.
pub fn build_sst_from_typed<P: AsRef<Path>, K, V, I>(
    sst_path: P,
    entries: I,
) -> Result<(), TypedStoreError>
where
    K: Serialize,
    V: Serialize,
    I: IntoIterator<Item = (K, V)>,
{
    let mut kv_bytes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for (k, v) in entries.into_iter() {
        let key = be_fix_int_ser(&k)?;
        let val = bcs::to_bytes(&v).map_err(typed_store_err_from_bcs_err)?;
        kv_bytes.push((key, val));
    }
    build_sst_from_kv(sst_path, kv_bytes)
}

/// Options to control buffered SST ingestion.
#[derive(Debug, Clone)]
pub struct SstIngestOptions {
    /// Flush when the number of buffered entries reaches this threshold.
    pub max_entries: usize,
    /// Optional subdirectory under the RocksDB path where SST files are written.
    /// If `None`, defaults to `sst_ingest/<column_family_name>`.
    pub dir_name: Option<String>,
    /// If false, keys will be serialized and sorted before building the SST.
    /// If true, the buffer assumes keys are appended in sorted order.
    pub assume_sorted: bool,
}

impl Default for SstIngestOptions {
    fn default() -> Self {
        Self {
            max_entries: 100_000,
            dir_name: None,
            assume_sorted: true,
        }
    }
}

fn sst_current_nanos() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u128::from(d.as_secs()) * 1_000_000_000u128 + u128::from(d.subsec_nanos()))
        .unwrap_or(0)
}

/// A typed buffer that writes entries to SST files and ingests them into RocksDB upon flush.
#[derive(Debug)]
pub struct SstIngestBuffer<K, V> {
    rocksdb: Arc<RocksDB>,
    cf_name: String,
    options: SstIngestOptions,
    buffer: Vec<(K, V)>,
}

impl<K, V> SstIngestBuffer<K, V>
where
    K: serde::Serialize,
    V: serde::Serialize,
{
    /// Create a new buffer associated with a specific map.
    pub fn new(map: &DBMap<K, V>, options: SstIngestOptions) -> Result<Self, TypedStoreError> {
        let buffer = Self {
            rocksdb: map.rocksdb.clone(),
            cf_name: map.cf_name().to_string(),
            options,
            buffer: Vec::new(),
        };
        buffer.cleanup_ingest_dir_with_self()?;
        Ok(buffer)
    }

    /// Remove the SST ingest directory used by this buffer's configuration.
    pub fn cleanup_ingest_dir_with_self(&self) -> Result<(), TypedStoreError> {
        let base_dir = if let Some(dir) = &self.options.dir_name {
            self.rocksdb.path().join(dir)
        } else {
            self.rocksdb.path().join("sst_ingest").join(&self.cf_name)
        };

        if !std::path::Path::new(&base_dir).exists() {
            return Ok(());
        }

        std::fs::remove_dir_all(&base_dir).map_err(|e| TypedStoreError::RocksDBError(e.to_string()))
    }

    /// Push a single entry. Flushes automatically when threshold is reached.
    pub fn push(&mut self, key: K, value: V) -> Result<(), TypedStoreError> {
        self.buffer.push((key, value));
        Ok(())
    }

    /// Push multiple entries. May flush multiple times if needed.
    pub fn push_iter<I>(&mut self, iter: I) -> Result<(), TypedStoreError>
    where
        I: IntoIterator<Item = (K, V)>,
    {
        for (k, v) in iter {
            self.push(k, v)?;
        }
        Ok(())
    }

    /// Force a flush of the current buffer. No-op if buffer is empty.
    pub fn flush(&mut self) -> Result<(), TypedStoreError> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let entries: Vec<(K, V)> = std::mem::take(&mut self.buffer);

        let base_dir = if let Some(dir) = &self.options.dir_name {
            self.rocksdb.path().join(dir)
        } else {
            self.rocksdb.path().join("sst_ingest").join(&self.cf_name)
        };

        if let Err(e) = std::fs::create_dir_all(&base_dir) {
            return Err(TypedStoreError::RocksDBError(e.to_string()));
        }
        let sst_path = base_dir.join(format!("ingest-{}.sst", sst_current_nanos()));

        if self.options.assume_sorted {
            build_sst_from_typed(&sst_path, entries)?;
        } else {
            let mut kv_bytes: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(entries.len());
            for (k, v) in entries.into_iter() {
                let key = be_fix_int_ser(&k)?;
                let val = bcs::to_bytes(&v).map_err(typed_store_err_from_bcs_err)?;
                kv_bytes.push((key, val));
            }
            kv_bytes.sort_by(|a, b| a.0.cmp(&b.0));
            build_sst_from_kv(&sst_path, kv_bytes)?;
        }

        let mut ingest_opts = rocksdb::IngestExternalFileOptions::default();
        ingest_opts.set_move_files(true);
        let cf = self
            .rocksdb
            .cf_handle(&self.cf_name)
            .ok_or_else(|| TypedStoreError::UnregisteredColumn(self.cf_name.clone()))?;
        self.rocksdb
            .ingest_external_file_cf(&cf, &[sst_path], &ingest_opts)
            .map_err(|e| TypedStoreError::RocksDBError(e.into_string()))
    }

    /// Flush remaining entries and consume the buffer.
    pub fn close(mut self) -> Result<(), TypedStoreError> {
        self.flush()
    }

    /// Get the number of entries in the buffer.
    pub fn size(&self) -> usize {
        self.buffer.len()
    }

    /// Clear any buffered entries without flushing them.
    /// Intended for abort/restart scenarios where the producer will re-fetch entries.
    pub fn clear(&mut self) -> Result<(), TypedStoreError> {
        self.buffer.clear();
        self.cleanup_ingest_dir_with_self()?;
        Ok(())
    }
}

#[allow(clippy::assign_op_pattern)]
#[allow(clippy::manual_div_ceil)]
#[test]
fn test_helpers() {
    let v = vec![];
    assert!(is_max(&v));

    fn check_add(v: Vec<u8>) {
        let mut v = v;
        let num = Num32::from_big_endian(&v);
        big_endian_saturating_add_one(&mut v);
        assert!(num + 1 == Num32::from_big_endian(&v));
    }

    uint::construct_uint! {
        // 32 byte number.
        struct Num32(4);
    }

    let mut v = vec![255; 32];
    big_endian_saturating_add_one(&mut v);
    assert!(Num32::MAX == Num32::from_big_endian(&v));

    check_add(vec![1; 32]);
    check_add(vec![6; 32]);
    check_add(vec![254; 32]);

    // TBD: More tests coming with randomized arrays.
}

#[test]
// This test demonstrates that bincode serialization does NOT preserve string ordering.
fn test_bincode_string_ordering_not_preserved() {
    // Two strings where "aa" < "b" in natural string ordering.
    let str1 = "aa";
    let str2 = "b";

    // Natural string ordering.
    assert!(
        str1 < str2,
        "In natural ordering, 'aa' should be less than 'b'"
    );

    // Serialize using bincode.
    let serialized1 = be_fix_int_ser(&str1).unwrap();
    let serialized2 = be_fix_int_ser(&str2).unwrap();

    // Bincode serialization reverses the order.
    // This is because bincode prefixes strings with their length:
    // - "aa" becomes [0, 0, 0, 0, 0, 0, 0, 2, 97, 97] (length=2, then 'a', 'a').
    // - "b"  becomes [0, 0, 0, 0, 0, 0, 0, 1, 98]     (length=1, then 'b').
    // Since 1 < 2 in the length prefix, "b" comes before "aa" when serialized.
    assert!(
        serialized1 > serialized2,
        "Bincode serialization reverses the order: serialized 'b' < serialized 'aa'"
    );

    // Another example with same length strings - these DO preserve order.
    let str3 = "abc";
    let str4 = "def";
    let serialized3 = be_fix_int_ser(&str3).unwrap();
    let serialized4 = be_fix_int_ser(&str4).unwrap();

    assert!(str3 < str4);
    assert!(
        serialized3 < serialized4,
        "Same-length strings preserve order"
    );

    // Another example with different length strings.
    let str5 = "z"; // Single 'z'.
    let str6 = "aaa"; // Three 'a's.
    let serialized5 = be_fix_int_ser(&str5).unwrap();
    let serialized6 = be_fix_int_ser(&str6).unwrap();

    assert!(str5 > str6, "In natural ordering, 'aaa' < 'z'");
    assert!(
        serialized5 < serialized6,
        "But serialized 'z' < serialized 'aaa' due to length prefix"
    );
}

#[cfg(test)]
mod sst_tests {
    use prometheus::Registry;
    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn sst_ingest_accepts_sorted_unique_keys() {
        crate::metrics::DBMetrics::init(&Registry::default());

        let dir = tempdir().unwrap();
        let mut db_options = rocksdb::Options::default();
        db_options.create_missing_column_families(true);
        db_options.create_if_missing(true);
        db_options.set_enable_blob_files(true);
        db_options.set_min_blob_size(1);
        let rocks = open_cf(
            dir.path(),
            Some(db_options),
            MetricConf::default(),
            &["cf1"],
        )
        .unwrap();

        let dbmap =
            DBMap::<u32, u32>::reopen(&rocks, Some("cf1"), &ReadWriteOptions::default(), false)
                .expect("open cf1");

        let entries: Vec<(u32, u32)> = (1..=1000).map(|k| (k, k * 2)).collect();

        let sst_dir = dbmap.rocksdb.path().join("sst_tests");
        std::fs::create_dir_all(&sst_dir).unwrap();
        let sst_path = sst_dir.join("sorted_unique.sst");
        let mut writer_opts = rocksdb::Options::default();
        writer_opts.set_enable_blob_files(true);
        writer_opts.set_min_blob_size(1);
        let mut writer = SstFileWriter::create(&writer_opts);
        writer.open(&sst_path).unwrap();
        for (k, v) in &entries {
            let kb = be_fix_int_ser(k).unwrap();
            let vb = bcs::to_bytes(v).unwrap();
            writer.put(&kb, &vb).unwrap();
        }
        writer.finish().unwrap();

        let mut ingest_opts = IngestExternalFileOptions::default();
        ingest_opts.set_move_files(true);
        dbmap
            .ingest_external_file(std::slice::from_ref(&sst_path), &ingest_opts)
            .expect("ingest should succeed");

        dbmap.flush().expect("flush should succeed");
        dbmap
            .compact_range_to_bottom(&0u32, &1000u32)
            .expect("compact should succeed");

        assert_eq!(dbmap.get(&1).unwrap(), Some(2));
        assert_eq!(dbmap.get(&500).unwrap(), Some(1000));
        assert_eq!(dbmap.get(&1000).unwrap(), Some(2000));
    }

    #[tokio::test]
    async fn sst_writer_rejects_out_of_order_keys() {
        let dir = tempdir().unwrap();
        let sst_path = dir.path().join("bad_order.sst");
        let writer_opts = rocksdb::Options::default();
        let mut writer = SstFileWriter::create(&writer_opts);
        writer.open(&sst_path).unwrap();
        let k2 = be_fix_int_ser(&2u32).unwrap();
        let k1 = be_fix_int_ser(&1u32).unwrap();
        let v = bcs::to_bytes(&0u32).unwrap();
        writer.put(&k2, &v).unwrap();
        let err = writer.put(&k1, &v).err();
        assert!(err.is_some());
        writer.finish().unwrap();
    }

    #[tokio::test]
    async fn sst_ingest_buffer_flush_assume_sorted_true() {
        crate::metrics::DBMetrics::init(&Registry::default());

        let dir = tempdir().unwrap();
        let mut db_options = rocksdb::Options::default();
        db_options.create_missing_column_families(true);
        db_options.create_if_missing(true);
        let rocks = open_cf(
            dir.path(),
            Some(db_options),
            MetricConf::default(),
            &["cf1"],
        )
        .unwrap();

        let dbmap =
            DBMap::<u32, u32>::reopen(&rocks, Some("cf1"), &ReadWriteOptions::default(), false)
                .expect("open cf1");

        let options = SstIngestOptions {
            max_entries: 10,
            dir_name: Some("sst_buffer_cf1".to_string()),
            assume_sorted: true,
        };

        let mut buf = SstIngestBuffer::new(&dbmap, options).expect("create buffer");
        for k in 1..=5u32 {
            buf.push(k, k * 10).unwrap();
        }
        assert_eq!(buf.size(), 5);
        buf.flush().expect("flush should succeed");
        assert_eq!(buf.size(), 0);

        dbmap.flush().expect("flush should succeed");
        dbmap
            .compact_range_to_bottom(&0u32, &10u32)
            .expect("compact should succeed");

        assert_eq!(dbmap.get(&1).unwrap(), Some(10));
        assert_eq!(dbmap.get(&3).unwrap(), Some(30));
        assert_eq!(dbmap.get(&5).unwrap(), Some(50));
    }

    #[tokio::test]
    async fn sst_ingest_buffer_flush_assume_sorted_false() {
        crate::metrics::DBMetrics::init(&Registry::default());

        // Create temp DB with a single CF
        let dir = tempdir().unwrap();
        let mut db_options = rocksdb::Options::default();
        db_options.create_missing_column_families(true);
        db_options.create_if_missing(true);
        let rocks = open_cf(
            dir.path(),
            Some(db_options),
            MetricConf::default(),
            &["cf1"],
        )
        .unwrap();

        let dbmap =
            DBMap::<u32, u32>::reopen(&rocks, Some("cf1"), &ReadWriteOptions::default(), false)
                .expect("open cf1");

        let options = SstIngestOptions {
            max_entries: 10,
            dir_name: None,
            assume_sorted: false,
        };

        let mut buf = SstIngestBuffer::new(&dbmap, options).expect("create buffer");

        // Intentionally push in unsorted order
        for (k, v) in [(5u32, 50u32), (1, 10), (3, 30), (2, 20), (4, 40)] {
            buf.push(k, v).unwrap();
        }
        buf.flush().expect("flush should succeed");

        dbmap.flush().expect("flush should succeed");
        dbmap
            .compact_range_to_bottom(&0u32, &10u32)
            .expect("compact should succeed");

        assert_eq!(dbmap.get(&1).unwrap(), Some(10));
        assert_eq!(dbmap.get(&2).unwrap(), Some(20));
        assert_eq!(dbmap.get(&3).unwrap(), Some(30));
        assert_eq!(dbmap.get(&4).unwrap(), Some(40));
        assert_eq!(dbmap.get(&5).unwrap(), Some(50));
    }

    #[tokio::test]
    async fn sst_ingest_two_flushes_with_overlapping_keys() {
        crate::metrics::DBMetrics::init(&Registry::default());

        let dir = tempdir().unwrap();
        let mut db_options = rocksdb::Options::default();
        db_options.create_missing_column_families(true);
        db_options.create_if_missing(true);
        db_options.set_enable_blob_files(true);
        db_options.set_min_blob_size(1);
        let rocks = open_cf(
            dir.path(),
            Some(db_options),
            MetricConf::default(),
            &["cf1"],
        )
        .unwrap();

        let dbmap =
            DBMap::<u32, u32>::reopen(&rocks, Some("cf1"), &ReadWriteOptions::default(), false)
                .expect("open cf1");

        let options = SstIngestOptions {
            max_entries: 10,
            dir_name: None,
            assume_sorted: true,
        };

        // First flush: keys 1..=5
        let mut buf = SstIngestBuffer::new(&dbmap, options.clone()).expect("create buffer");
        for k in 1..=5u32 {
            buf.push(k, k * 10).unwrap();
        }
        buf.flush().expect("first flush should succeed");

        // Second flush: overlapping keys 3..=7
        let mut buf2 = SstIngestBuffer::new(&dbmap, options).expect("create buffer");
        for k in 3..=7u32 {
            buf2.push(k, k * 100).unwrap();
        }
        buf2.flush().expect("second flush should succeed");

        dbmap.flush().expect("flush should succeed");
        dbmap
            .compact_range_to_bottom(&1u32, &7u32)
            .expect("compact should succeed");

        assert_eq!(dbmap.get(&1).unwrap(), Some(10));
        assert_eq!(dbmap.get(&2).unwrap(), Some(20));
        assert_eq!(dbmap.get(&3).unwrap(), Some(300));
        assert_eq!(dbmap.get(&4).unwrap(), Some(400));
        assert_eq!(dbmap.get(&5).unwrap(), Some(500));
        assert_eq!(dbmap.get(&6).unwrap(), Some(600));
        assert_eq!(dbmap.get(&7).unwrap(), Some(700));
    }
}
