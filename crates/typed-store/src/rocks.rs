// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Error types and utilities for RocksDB operations
pub mod errors;

/// Safe iterator utilities for RocksDB
pub(crate) mod safe_iter;

use std::{
    borrow::Borrow,
    collections::HashSet,
    env,
    ffi::CStr,
    fmt,
    marker::PhantomData,
    ops::{Bound, RangeBounds},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use bincode::Options;
use prometheus::{Histogram, HistogramTimer};
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
    LiveFile,
    MultiThreaded,
    ReadOptions,
    WriteBatch,
    WriteOptions,
    checkpoint::Checkpoint,
    properties,
    properties::num_files_at_level,
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
        safe_iter::{SafeIter, SafeRevIter},
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

// TODO: remove this after Rust rocksdb has the TOTAL_BLOB_FILES_SIZE property built-in.
const ROCKSDB_PROPERTY_TOTAL_BLOB_FILES_SIZE: &CStr =
    unsafe { CStr::from_bytes_with_nul_unchecked("rocksdb.total-blob-file-size\0".as_bytes()) };

#[cfg(test)]
mod tests;

#[derive(Debug)]
/// The rocksdb database
pub struct RocksDB {
    /// The underlying rocksdb database
    pub underlying: rocksdb::DBWithThreadMode<MultiThreaded>,
    /// The metric configuration
    pub metric_conf: MetricConf,
    /// The path of the database
    pub db_path: PathBuf,
}

impl Drop for RocksDB {
    fn drop(&mut self) {
        self.underlying.cancel_all_background_work(/* wait */ true);
        DBMetrics::get().decrement_num_active_dbs(&self.metric_conf.db_name);
    }
}

impl RocksDB {
    fn new(
        underlying: rocksdb::DBWithThreadMode<MultiThreaded>,
        metric_conf: MetricConf,
        db_path: PathBuf,
    ) -> Self {
        DBMetrics::get().increment_num_active_dbs(&metric_conf.db_name);
        Self {
            underlying,
            metric_conf,
            db_path,
        }
    }
}

impl RocksDB {
    /// Get a value from the database
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>, rocksdb::Error> {
        self.underlying.get(key)
    }

    /// Get multiple values from the database
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
        self.underlying.multi_get_cf_opt(keys, readopts)
    }

    /// Get multiple values from a specific column family
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
        self.underlying
            .batched_multi_get_cf_opt(cf, keys, sorted_input, readopts)
    }

    /// Get a property value from a specific column family
    pub fn property_int_value_cf(
        &self,
        cf: &impl AsColumnFamilyRef,
        name: impl CStrLike,
    ) -> Result<Option<u64>, rocksdb::Error> {
        self.underlying.property_int_value_cf(cf, name)
    }

    /// Get a pinned value from a specific column family
    pub fn get_pinned_cf_opt<K: AsRef<[u8]>>(
        &self,
        cf: &impl AsColumnFamilyRef,
        key: K,
        readopts: &ReadOptions,
    ) -> Result<Option<DBPinnableSlice<'_>>, rocksdb::Error> {
        self.underlying.get_pinned_cf_opt(cf, key, readopts)
    }

    /// Get a column family handle by name
    pub fn cf_handle(&self, name: &str) -> Option<Arc<rocksdb::BoundColumnFamily<'_>>> {
        self.underlying.cf_handle(name)
    }

    /// Create a new column family
    pub fn create_cf<N: AsRef<str>>(
        &self,
        name: N,
        opts: &rocksdb::Options,
    ) -> Result<(), rocksdb::Error> {
        self.underlying.create_cf(name, opts)
    }

    /// Drop a column family
    pub fn drop_cf(&self, name: &str) -> Result<(), rocksdb::Error> {
        self.underlying.drop_cf(name)
    }

    /// Delete files in a range
    pub fn delete_file_in_range<K: AsRef<[u8]>>(
        &self,
        cf: &impl AsColumnFamilyRef,
        from: K,
        to: K,
    ) -> Result<(), rocksdb::Error> {
        self.underlying.delete_file_in_range_cf(cf, from, to)
    }

    /// Delete a value from a specific column family
    pub fn delete_cf<K: AsRef<[u8]>>(
        &self,
        cf: &impl AsColumnFamilyRef,
        key: K,
        writeopts: &WriteOptions,
    ) -> Result<(), rocksdb::Error> {
        sui_macros::fail_point!("delete-cf-before");
        let ret = self.underlying.delete_cf_opt(cf, key, writeopts);
        sui_macros::fail_point!("delete-cf-after");
        #[allow(clippy::let_and_return)]
        ret
    }

    /// Get the path of the database
    pub fn path(&self) -> &Path {
        self.underlying.path()
    }

    /// Put a value into a specific column family
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
        let ret = self.underlying.put_cf_opt(cf, key, value, writeopts);
        sui_macros::fail_point!("put-cf-after");
        #[allow(clippy::let_and_return)]
        ret
    }

    /// Check if a key may exist in a specific column family
    pub fn key_may_exist_cf<K: AsRef<[u8]>>(
        &self,
        cf: &impl AsColumnFamilyRef,
        key: K,
        readopts: &ReadOptions,
    ) -> bool {
        self.underlying.key_may_exist_cf_opt(cf, key, readopts)
    }

    /// Try to catch up with the primary
    pub fn try_catch_up_with_primary(&self) -> Result<(), rocksdb::Error> {
        self.underlying.try_catch_up_with_primary()
    }

    /// Write a batch of operations to the database
    pub fn write(
        &self,
        batch: rocksdb::WriteBatch,
        writeopts: &WriteOptions,
    ) -> Result<(), TypedStoreError> {
        sui_macros::fail_point!("batch-write-before");
        self.underlying
            .write_opt(batch, writeopts)
            .map_err(typed_store_err_from_rocks_err)?;
        sui_macros::fail_point!("batch-write-after");
        #[allow(clippy::let_and_return)]
        Ok(())
    }

    /// Get a raw iterator for a specific column family
    pub fn raw_iterator_cf<'a: 'b, 'b>(
        &'a self,
        cf_handle: &impl AsColumnFamilyRef,
        readopts: ReadOptions,
    ) -> RocksDBRawIter<'b> {
        self.underlying.raw_iterator_cf_opt(cf_handle, readopts)
    }

    /// Compact a range of values in a specific column family
    pub fn compact_range_cf<K: AsRef<[u8]>>(
        &self,
        cf: &impl AsColumnFamilyRef,
        start: Option<K>,
        end: Option<K>,
    ) {
        self.underlying.compact_range_cf(cf, start, end)
    }

    /// Compact a range of values in a specific column family to the bottom
    /// of the LSM tree
    pub fn compact_range_to_bottom<K: AsRef<[u8]>>(
        &self,
        cf: &impl AsColumnFamilyRef,
        start: Option<K>,
        end: Option<K>,
    ) {
        let opt = &mut CompactOptions::default();
        opt.set_bottommost_level_compaction(BottommostLevelCompaction::ForceOptimized);
        self.underlying.compact_range_cf_opt(cf, start, end, opt)
    }

    /// Flush the database
    pub fn flush(&self) -> Result<(), TypedStoreError> {
        self.underlying
            .flush()
            .map_err(|e| TypedStoreError::RocksDBError(e.into_string()))
    }

    /// Create a checkpoint of the database
    pub fn checkpoint(&self, path: &Path) -> Result<(), TypedStoreError> {
        let checkpoint =
            Checkpoint::new(&self.underlying).map_err(typed_store_err_from_rocks_err)?;
        checkpoint
            .create_checkpoint(path)
            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
        Ok(())
    }

    /// Flush a specific column family
    pub fn flush_cf(&self, cf: &impl AsColumnFamilyRef) -> Result<(), rocksdb::Error> {
        self.underlying.flush_cf(cf)
    }

    /// Set options for a specific column family
    pub fn set_options_cf(
        &self,
        cf: &impl AsColumnFamilyRef,
        opts: &[(&str, &str)],
    ) -> Result<(), rocksdb::Error> {
        self.underlying.set_options_cf(cf, opts)
    }

    /// Get the sampling interval for the database
    pub fn get_sampling_interval(&self) -> SamplingInterval {
        self.metric_conf.read_sample_interval.new_from_self()
    }

    /// Get the sampling interval for multi-get operations
    pub fn multiget_sampling_interval(&self) -> SamplingInterval {
        self.metric_conf.read_sample_interval.new_from_self()
    }

    /// Get the sampling interval for write operations
    pub fn write_sampling_interval(&self) -> SamplingInterval {
        self.metric_conf.write_sample_interval.new_from_self()
    }

    /// Get the sampling interval for iterator operations
    pub fn iter_sampling_interval(&self) -> SamplingInterval {
        self.metric_conf.iter_sample_interval.new_from_self()
    }

    /// Get the name of the database
    pub fn db_name(&self) -> String {
        let name = &self.metric_conf.db_name;
        if name.is_empty() {
            self.default_db_name()
        } else {
            name.clone()
        }
    }

    /// Get the default name of the database
    fn default_db_name(&self) -> String {
        self.path()
            .file_name()
            .and_then(|f| f.to_str())
            .unwrap_or("unknown")
            .to_string()
    }

    /// Get the live files in the database
    pub fn live_files(&self) -> Result<Vec<LiveFile>, Error> {
        self.underlying.live_files()
    }
}

/// A configuration for metrics
#[derive(Debug, Default)]
pub struct MetricConf {
    /// The name of the database
    pub db_name: String,
    /// The sampling interval for read operations
    pub read_sample_interval: SamplingInterval,
    /// The sampling interval for write operations
    pub write_sample_interval: SamplingInterval,
    /// The sampling interval for iterator operations
    pub iter_sample_interval: SamplingInterval,
}

/// A configuration for metrics
impl MetricConf {
    /// Create a new metric configuration
    pub fn new(db_name: &str) -> Self {
        if db_name.is_empty() {
            tracing::error!("A meaningful db name should be used for metrics reporting.")
        }
        Self {
            db_name: db_name.to_string(),
            read_sample_interval: SamplingInterval::default(),
            write_sample_interval: SamplingInterval::default(),
            iter_sample_interval: SamplingInterval::default(),
        }
    }

    /// Set the sampling interval for the database
    pub fn with_sampling(self, read_interval: SamplingInterval) -> Self {
        Self {
            db_name: self.db_name,
            read_sample_interval: read_interval,
            write_sample_interval: SamplingInterval::default(),
            iter_sample_interval: SamplingInterval::default(),
        }
    }
}
const CF_METRICS_REPORT_PERIOD_SECS: u64 = 30;
const METRICS_ERROR: i64 = -1;

/// An interface to a rocksDB database, keyed by a columnfamily
#[derive(Clone, Debug)]
pub struct DBMap<K, V> {
    /// The rocksDB database
    pub rocksdb: Arc<RocksDB>,
    /// The phantom data
    _phantom: PhantomData<fn(K) -> V>,
    /// The rocksDB ColumnFamily under which the map is stored
    cf: String,
    /// The read-write options
    pub opts: ReadWriteOptions,
    /// The metrics for the database
    db_metrics: Arc<DBMetrics>,
    /// The sampling interval for the database
    get_sample_interval: SamplingInterval,
    /// The sampling interval for multi-get operations
    multiget_sample_interval: SamplingInterval,
    /// The sampling interval for write operations
    write_sample_interval: SamplingInterval,
    /// The sampling interval for iterator operations
    iter_sample_interval: SamplingInterval,
    /// The cancel handle for the metrics task
    _metrics_task_cancel_handle: Arc<oneshot::Sender<()>>,
}

unsafe impl<K: Send, V: Send> Send for DBMap<K, V> {}

impl<K, V> DBMap<K, V> {
    pub(crate) fn new(
        db: Arc<RocksDB>,
        opts: &ReadWriteOptions,
        opt_cf: &str,
        is_deprecated: bool,
    ) -> Self {
        let db_cloned = db.clone();
        let db_metrics = DBMetrics::get();
        let db_metrics_cloned = db_metrics.clone();
        let cf = opt_cf.to_string();
        let (sender, mut recv) = tokio::sync::oneshot::channel();
        if !is_deprecated {
            tokio::task::spawn(async move {
                let mut interval =
                    tokio::time::interval(Duration::from_secs(CF_METRICS_REPORT_PERIOD_SECS));
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            let db = db_cloned.clone();
                            let cf = cf.clone();
                            let db_metrics = db_metrics.clone();
                            if let Err(e) = tokio::task::spawn_blocking(move || {
                                Self::report_metrics(&db, &cf, &db_metrics);
                            }).await {
                                tracing::error!("Failed to log metrics with error: {}", e);
                            }
                        }
                        _ = &mut recv => break,
                    }
                }
                tracing::debug!("Returning the cf metric logging task for DBMap: {}", &cf);
            });
        }
        DBMap {
            rocksdb: db.clone(),
            opts: opts.clone(),
            _phantom: PhantomData,
            cf: opt_cf.to_string(),
            db_metrics: db_metrics_cloned,
            _metrics_task_cancel_handle: Arc::new(sender),
            get_sample_interval: db.get_sampling_interval(),
            multiget_sample_interval: db.multiget_sampling_interval(),
            write_sample_interval: db.write_sampling_interval(),
            iter_sample_interval: db.iter_sampling_interval(),
        }
    }

    /// Opens a database from a path, with specific options and an optional column family.
    ///
    /// This database is used to perform operations on single column family, and parametrizes
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
        rw_options: &ReadWriteOptions,
    ) -> Result<Self, TypedStoreError> {
        let cf_key = opt_cf.unwrap_or(rocksdb::DEFAULT_COLUMN_FAMILY_NAME);
        let cfs = vec![cf_key];
        let rocksdb = open_cf(path, db_options, metric_conf, &cfs)?;
        Ok(DBMap::new(rocksdb, rw_options, cf_key, false))
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
    ///     false
    /// ).expect("Failed to open storage");
    ///    let db_cf_2 = DBMap::<u32,u32>::reopen(
    ///     &rocks,
    ///     Some("Second_CF"),
    ///     &ReadWriteOptions::default(),
    ///     false
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
        let cf_key = opt_cf
            .unwrap_or(rocksdb::DEFAULT_COLUMN_FAMILY_NAME)
            .to_owned();

        db.cf_handle(&cf_key)
            .ok_or_else(|| TypedStoreError::UnregisteredColumn(cf_key.clone()))?;

        Ok(DBMap::new(db.clone(), rw_options, &cf_key, is_deprecated))
    }

    /// Get the column family name
    pub fn cf_name(&self) -> &str {
        &self.cf
    }

    /// Create a new batch associated with a DB reference.
    pub fn batch(&self) -> DBBatch {
        let batch = WriteBatch::default();
        DBBatch::new(
            &self.rocksdb,
            batch,
            self.opts.writeopts(),
            &self.db_metrics,
            &self.write_sample_interval,
        )
    }

    /// Compact a range of keys in a specific column family
    pub fn compact_range<J: Serialize>(&self, start: &J, end: &J) -> Result<(), TypedStoreError> {
        let from_buf = be_fix_int_ser(start)?;
        let to_buf = be_fix_int_ser(end)?;
        self.rocksdb
            .compact_range_cf(&self.cf()?, Some(from_buf), Some(to_buf));
        Ok(())
    }

    /// Compact a range of keys in a specific column family
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

    /// Get the column family
    pub fn cf(&self) -> Result<Arc<rocksdb::BoundColumnFamily<'_>>, TypedStoreError> {
        self.rocksdb
            .cf_handle(&self.cf)
            .ok_or_else(|| TypedStoreError::UnregisteredColumn(self.cf.clone()))
    }

    /// Flush the column family
    pub fn flush(&self) -> Result<(), TypedStoreError> {
        self.rocksdb
            .flush_cf(&self.cf()?)
            .map_err(|e| TypedStoreError::RocksDBError(e.into_string()))
    }

    fn get_int_property(
        rocksdb: &RocksDB,
        cf: &impl AsColumnFamilyRef,
        property_name: &std::ffi::CStr,
    ) -> Result<i64, TypedStoreError> {
        match rocksdb.property_int_value_cf(cf, property_name) {
            Ok(Some(value)) => Ok(value.min(i64::MAX as u64).try_into().unwrap_or_default()),
            Ok(None) => Ok(0),
            Err(e) => Err(TypedStoreError::RocksDBError(e.into_string())),
        }
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
            .with_label_values(&[&self.cf])
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
            .with_label_values(&[&self.cf])
            .observe(entry_size as f64);
        if perf_ctx.is_some() {
            self.db_metrics
                .read_perf_ctx_metrics
                .report_metrics(&self.cf);
        }
        Ok(entries)
    }

    fn report_metrics(rocksdb: &Arc<RocksDB>, cf_name: &str, db_metrics: &Arc<DBMetrics>) {
        let Some(cf) = rocksdb.cf_handle(cf_name) else {
            tracing::warn!(
                "unable to report metrics for cf {cf_name:?} in db {:?}",
                rocksdb.db_name()
            );
            return;
        };

        db_metrics
            .cf_metrics
            .rocksdb_total_sst_files_size
            .with_label_values(&[cf_name])
            .set(
                Self::get_int_property(rocksdb, &cf, properties::TOTAL_SST_FILES_SIZE)
                    .unwrap_or(METRICS_ERROR),
            );
        db_metrics
            .cf_metrics
            .rocksdb_total_blob_files_size
            .with_label_values(&[cf_name])
            .set(
                Self::get_int_property(rocksdb, &cf, ROCKSDB_PROPERTY_TOTAL_BLOB_FILES_SIZE)
                    .unwrap_or(METRICS_ERROR),
            );
        // 7 is the default number of levels in RocksDB. If we ever change the number
        // of levels using `set_num_levels`, we need to update here as well. Note that
        // there isn't an API to query the DB to get the number of levels (yet).
        let total_num_files: i64 = (0..=6)
            .map(|level| {
                Self::get_int_property(rocksdb, &cf, &num_files_at_level(level))
                    .unwrap_or(METRICS_ERROR)
            })
            .sum();
        db_metrics
            .cf_metrics
            .rocksdb_total_num_files
            .with_label_values(&[cf_name])
            .set(total_num_files);
        db_metrics
            .cf_metrics
            .rocksdb_num_level0_files
            .with_label_values(&[cf_name])
            .set(
                Self::get_int_property(rocksdb, &cf, &num_files_at_level(0))
                    .unwrap_or(METRICS_ERROR),
            );
        db_metrics
            .cf_metrics
            .rocksdb_current_size_active_mem_tables
            .with_label_values(&[cf_name])
            .set(
                Self::get_int_property(rocksdb, &cf, properties::CUR_SIZE_ACTIVE_MEM_TABLE)
                    .unwrap_or(METRICS_ERROR),
            );
        db_metrics
            .cf_metrics
            .rocksdb_size_all_mem_tables
            .with_label_values(&[cf_name])
            .set(
                Self::get_int_property(rocksdb, &cf, properties::SIZE_ALL_MEM_TABLES)
                    .unwrap_or(METRICS_ERROR),
            );
        db_metrics
            .cf_metrics
            .rocksdb_num_snapshots
            .with_label_values(&[cf_name])
            .set(
                Self::get_int_property(rocksdb, &cf, properties::NUM_SNAPSHOTS)
                    .unwrap_or(METRICS_ERROR),
            );
        db_metrics
            .cf_metrics
            .rocksdb_oldest_snapshot_time
            .with_label_values(&[cf_name])
            .set(
                Self::get_int_property(rocksdb, &cf, properties::OLDEST_SNAPSHOT_TIME)
                    .unwrap_or(METRICS_ERROR),
            );
        db_metrics
            .cf_metrics
            .rocksdb_actual_delayed_write_rate
            .with_label_values(&[cf_name])
            .set(
                Self::get_int_property(rocksdb, &cf, properties::ACTUAL_DELAYED_WRITE_RATE)
                    .unwrap_or(METRICS_ERROR),
            );
        db_metrics
            .cf_metrics
            .rocksdb_is_write_stopped
            .with_label_values(&[cf_name])
            .set(
                Self::get_int_property(rocksdb, &cf, properties::IS_WRITE_STOPPED)
                    .unwrap_or(METRICS_ERROR),
            );
        db_metrics
            .cf_metrics
            .rocksdb_block_cache_capacity
            .with_label_values(&[cf_name])
            .set(
                Self::get_int_property(rocksdb, &cf, properties::BLOCK_CACHE_CAPACITY)
                    .unwrap_or(METRICS_ERROR),
            );
        db_metrics
            .cf_metrics
            .rocksdb_block_cache_usage
            .with_label_values(&[cf_name])
            .set(
                Self::get_int_property(rocksdb, &cf, properties::BLOCK_CACHE_USAGE)
                    .unwrap_or(METRICS_ERROR),
            );
        db_metrics
            .cf_metrics
            .rocksdb_block_cache_pinned_usage
            .with_label_values(&[cf_name])
            .set(
                Self::get_int_property(rocksdb, &cf, properties::BLOCK_CACHE_PINNED_USAGE)
                    .unwrap_or(METRICS_ERROR),
            );
        db_metrics
            .cf_metrics
            .rocksdb_estimate_table_readers_mem
            .with_label_values(&[cf_name])
            .set(
                Self::get_int_property(rocksdb, &cf, properties::ESTIMATE_TABLE_READERS_MEM)
                    .unwrap_or(METRICS_ERROR),
            );
        db_metrics
            .cf_metrics
            .rocksdb_estimated_num_keys
            .with_label_values(&[cf_name])
            .set(
                Self::get_int_property(rocksdb, &cf, properties::ESTIMATE_NUM_KEYS)
                    .unwrap_or(METRICS_ERROR),
            );
        db_metrics
            .cf_metrics
            .rocksdb_num_immutable_mem_tables
            .with_label_values(&[cf_name])
            .set(
                Self::get_int_property(rocksdb, &cf, properties::NUM_IMMUTABLE_MEM_TABLE)
                    .unwrap_or(METRICS_ERROR),
            );
        db_metrics
            .cf_metrics
            .rocksdb_mem_table_flush_pending
            .with_label_values(&[cf_name])
            .set(
                Self::get_int_property(rocksdb, &cf, properties::MEM_TABLE_FLUSH_PENDING)
                    .unwrap_or(METRICS_ERROR),
            );
        db_metrics
            .cf_metrics
            .rocksdb_compaction_pending
            .with_label_values(&[cf_name])
            .set(
                Self::get_int_property(rocksdb, &cf, properties::COMPACTION_PENDING)
                    .unwrap_or(METRICS_ERROR),
            );
        db_metrics
            .cf_metrics
            .rocksdb_estimate_pending_compaction_bytes
            .with_label_values(&[cf_name])
            .set(
                Self::get_int_property(rocksdb, &cf, properties::ESTIMATE_PENDING_COMPACTION_BYTES)
                    .unwrap_or(METRICS_ERROR),
            );
        db_metrics
            .cf_metrics
            .rocksdb_num_running_compactions
            .with_label_values(&[cf_name])
            .set(
                Self::get_int_property(rocksdb, &cf, properties::NUM_RUNNING_COMPACTIONS)
                    .unwrap_or(METRICS_ERROR),
            );
        db_metrics
            .cf_metrics
            .rocksdb_num_running_flushes
            .with_label_values(&[cf_name])
            .set(
                Self::get_int_property(rocksdb, &cf, properties::NUM_RUNNING_FLUSHES)
                    .unwrap_or(METRICS_ERROR),
            );
        db_metrics
            .cf_metrics
            .rocksdb_estimate_oldest_key_time
            .with_label_values(&[cf_name])
            .set(
                Self::get_int_property(rocksdb, &cf, properties::ESTIMATE_OLDEST_KEY_TIME)
                    .unwrap_or(METRICS_ERROR),
            );
        db_metrics
            .cf_metrics
            .rocksdb_background_errors
            .with_label_values(&[cf_name])
            .set(
                Self::get_int_property(rocksdb, &cf, properties::BACKGROUND_ERRORS)
                    .unwrap_or(METRICS_ERROR),
            );
        db_metrics
            .cf_metrics
            .rocksdb_base_level
            .with_label_values(&[cf_name])
            .set(
                Self::get_int_property(rocksdb, &cf, properties::BASE_LEVEL)
                    .unwrap_or(METRICS_ERROR),
            );
    }

    /// Create a checkpoint of the database
    pub fn checkpoint_db(&self, path: &Path) -> Result<(), TypedStoreError> {
        self.rocksdb.checkpoint(path)
    }

    /// Get a summary of the table
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
    fn create_iter_context(
        &self,
    ) -> (
        Option<HistogramTimer>,
        Option<Histogram>,
        Option<Histogram>,
        Option<RocksDBPerfContext>,
    ) {
        let timer = self
            .db_metrics
            .op_metrics
            .rocksdb_iter_latency_seconds
            .with_label_values(&[&self.cf])
            .start_timer();
        let bytes_scanned = self
            .db_metrics
            .op_metrics
            .rocksdb_iter_bytes
            .with_label_values(&[&self.cf]);
        let keys_scanned = self
            .db_metrics
            .op_metrics
            .rocksdb_iter_keys
            .with_label_values(&[&self.cf]);
        let perf_ctx = if self.iter_sample_interval.sample() {
            Some(RocksDBPerfContext)
        } else {
            None
        };
        (
            Some(timer),
            Some(bytes_scanned),
            Some(keys_scanned),
            perf_ctx,
        )
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
            let key_buf = be_fix_int_ser(&lower_bound).unwrap();
            readopts.set_iterate_lower_bound(key_buf);
        }
        if let Some(upper_bound) = upper_bound {
            let key_buf = be_fix_int_ser(&upper_bound).unwrap();
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
        let (_timer, bytes_scanned, keys_scanned, _perf_ctx) = self.create_iter_context();
        let iter = SafeIter::new(
            self.cf.clone(),
            db_iter,
            _timer,
            _perf_ctx,
            bytes_scanned,
            keys_scanned,
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
                // Rocksdb lower bound is inclusive by default so nothing to do
                let key_buf = be_fix_int_ser(&lower_bound).expect("Serialization must not fail");
                readopts.set_iterate_lower_bound(key_buf);
            }
            Bound::Excluded(lower_bound) => {
                let mut key_buf =
                    be_fix_int_ser(&lower_bound).expect("Serialization must not fail");

                // Since we want exclusive, we need to increment the key to exclude the previous
                big_endian_saturating_add_one(&mut key_buf);
                readopts.set_iterate_lower_bound(key_buf);
            }
            Bound::Unbounded => (),
        };

        match upper_bound {
            Bound::Included(upper_bound) => {
                let mut key_buf =
                    be_fix_int_ser(&upper_bound).expect("Serialization must not fail");

                // If the key is already at the limit, there's nowhere else to go, so no upper bound
                if !is_max(&key_buf) {
                    // Since we want exclusive, we need to increment the key to get the upper bound
                    big_endian_saturating_add_one(&mut key_buf);
                    readopts.set_iterate_upper_bound(key_buf);
                }
            }
            Bound::Excluded(upper_bound) => {
                // Rocksdb upper bound is inclusive by default so nothing to do
                let key_buf = be_fix_int_ser(&upper_bound).expect("Serialization must not fail");
                readopts.set_iterate_upper_bound(key_buf);
            }
            Bound::Unbounded => (),
        };

        readopts
    }
}

/// Provides a mutable struct to form a collection of database write operations, and execute them.
///
/// Batching write and delete operations is faster than performing them one by one and ensures
/// their atomicity, ie. they are all written or none is.
/// This is also true of operations across column families in the same database.
///
/// Serializations / Deserialization, and naming of column families is performed by passing
/// a DBMap<K,V>
/// with each operation.
///
/// ```
/// use typed_store::rocks::*;
/// use tempfile::tempdir;
/// use typed_store::Map;
/// use typed_store::metrics::DBMetrics;
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
    /// The rocksDB database
    rocksdb: Arc<RocksDB>,
    /// The batch of write operations
    batch: rocksdb::WriteBatch,
    /// The write options
    opts: WriteOptions,
    /// The metrics for the database
    db_metrics: Arc<DBMetrics>,
    /// The sampling interval for write operations
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
        batch: rocksdb::WriteBatch,
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

    /// Consume the batch and write its operations to the database
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

    /// Get the size of the batch in bytes
    pub fn size_in_bytes(&self) -> usize {
        self.batch.size_in_bytes()
    }
}

impl DBBatch {
    /// Delete a batch of keys
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
    /// by writing a range delete tombstone in the db map
    /// If the DBMap is configured with ignore_range_deletions set to false,
    /// the effect of this write will be visible immediately i.e. you won't
    /// see old values when you do a lookup or scan. But if it is configured
    /// with ignore_range_deletions set to true, the old value are visible until
    /// compaction actually deletes them which will happen sometime after. By
    /// default ignore_range_deletions is set to true on a DBMap (unless it is
    /// overridden in the config), so please use this function with caution
    pub fn schedule_delete_range<K: Serialize, V>(
        &mut self,
        db: &DBMap<K, V>,
        from: &K,
        to: &K,
    ) -> Result<(), TypedStoreError> {
        if !Arc::ptr_eq(&db.rocksdb, &self.rocksdb) {
            return Err(TypedStoreError::CrossDBBatch);
        }

        let from_buf = be_fix_int_ser(from)?;
        let to_buf = be_fix_int_ser(to)?;

        self.batch.delete_range_cf(&db.cf()?, from_buf, to_buf);
        Ok(())
    }

    /// inserts a range of (key, value) pairs given as an iterator
    pub fn insert_batch<J: Borrow<K>, K: Serialize, U: Borrow<V>, V: Serialize>(
        &mut self,
        db: &DBMap<K, V>,
        new_vals: impl IntoIterator<Item = (J, U)>,
    ) -> Result<&mut Self, TypedStoreError> {
        if !Arc::ptr_eq(&db.rocksdb, &self.rocksdb) {
            return Err(TypedStoreError::CrossDBBatch);
        }
        let mut total = 0usize;
        new_vals
            .into_iter()
            .try_for_each::<_, Result<_, TypedStoreError>>(|(k, v)| {
                let k_buf = be_fix_int_ser(k.borrow())?;
                let v_buf = bcs::to_bytes(v.borrow()).map_err(typed_store_err_from_bcs_err)?;
                total += k_buf.len() + v_buf.len();
                self.batch.put_cf(&db.cf()?, k_buf, v_buf);
                Ok(())
            })?;
        self.db_metrics
            .op_metrics
            .rocksdb_batch_put_bytes
            .with_label_values(&[&db.cf])
            .observe(total as f64);
        Ok(self)
    }

    /// Inserts a range of (key, value) pairs given as an iterator
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

/// The raw iterator for the rocksdb
pub type RocksDBRawIter<'a> =
    rocksdb::DBRawIteratorWithThreadMode<'a, DBWithThreadMode<MultiThreaded>>;

impl<'a, K, V> Map<'a, K, V> for DBMap<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    type Error = TypedStoreError;
    type SafeIterator = SafeIter<'a, K, V>;

    #[tracing::instrument(level = "trace", skip_all, err)]
    fn contains_key(&self, key: &K) -> Result<bool, TypedStoreError> {
        let key_buf = be_fix_int_ser(key)?;
        // [`rocksdb::DBWithThreadMode::key_may_exist_cf`] can have false positives,
        // but no false negatives. We use it to short-circuit the absent case
        let readopts = self.opts.readopts();
        Ok(self
            .rocksdb
            .key_may_exist_cf(&self.cf()?, &key_buf, &readopts)
            && self
                .rocksdb
                .get_pinned_cf_opt(&self.cf()?, &key_buf, &readopts)
                .map_err(typed_store_err_from_rocks_err)?
                .is_some())
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
        let _timer = self
            .db_metrics
            .op_metrics
            .rocksdb_get_latency_seconds
            .with_label_values(&[&self.cf])
            .start_timer();
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
        self.db_metrics
            .op_metrics
            .rocksdb_get_bytes
            .with_label_values(&[&self.cf])
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
            .with_label_values(&[&self.cf])
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
            .rocksdb_put_bytes
            .with_label_values(&[&self.cf])
            .observe((key_buf.len() + value_buf.len()) as f64);
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
                .with_label_values(&[&self.cf])
                .inc();
            self.db_metrics
                .op_metrics
                .rocksdb_very_slow_puts_duration_ms
                .with_label_values(&[&self.cf])
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
            .with_label_values(&[&self.cf])
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
            .with_label_values(&[&self.cf])
            .inc();
        if perf_ctx.is_some() {
            self.db_metrics
                .write_perf_ctx_metrics
                .report_metrics(&self.cf);
        }
        Ok(())
    }

    /// This method first drops the existing column family and then creates a new one
    /// with the same name. The two operations are not atomic and hence it is possible
    /// to get into a race condition where the column family has been dropped but new
    /// one is not created yet
    #[tracing::instrument(level = "trace", skip_all, err)]
    fn unsafe_clear(&self) -> Result<(), TypedStoreError> {
        let _ = self.rocksdb.drop_cf(&self.cf);
        self.rocksdb
            .create_cf(self.cf.clone(), &default_db_options().options)
            .map_err(typed_store_err_from_rocks_err)?;
        Ok(())
    }

    /// Writes a range delete tombstone to delete all entries in the db map
    /// If the DBMap is configured with ignore_range_deletions set to false,
    /// the effect of this write will be visible immediately i.e. you won't
    /// see old values when you do a lookup or scan. But if it is configured
    /// with ignore_range_deletions set to true, the old value are visible until
    /// compaction actually deletes them which will happen sometime after. By
    /// default ignore_range_deletions is set to true on a DBMap (unless it is
    /// overridden in the config), so please use this function with caution
    #[tracing::instrument(level = "trace", skip_all, err)]
    fn schedule_delete_all(&self) -> Result<(), TypedStoreError> {
        let first_key = self.safe_iter()?.next().transpose()?.map(|(k, _v)| k);
        let last_key = self
            .reversed_safe_iter_with_bounds(None, None)?
            .next()
            .transpose()?
            .map(|(k, _v)| k);
        if let Some((first_key, last_key)) = first_key.zip(last_key) {
            let mut batch = self.batch();
            batch.schedule_delete_range(self, &first_key, &last_key)?;
            batch.write()?;
        }
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.safe_iter()
            .expect("safe_iter should not fail")
            .next()
            .is_none()
    }

    fn safe_iter(&'a self) -> Result<Self::SafeIterator, TypedStoreError> {
        let db_iter = self
            .rocksdb
            .raw_iterator_cf(&self.cf()?, self.opts.readopts());
        let (_timer, bytes_scanned, keys_scanned, _perf_ctx) = self.create_iter_context();
        Ok(SafeIter::new(
            self.cf.clone(),
            db_iter,
            _timer,
            _perf_ctx,
            bytes_scanned,
            keys_scanned,
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
        let (_timer, bytes_scanned, keys_scanned, _perf_ctx) = self.create_iter_context();
        Ok(SafeIter::new(
            self.cf.clone(),
            db_iter,
            _timer,
            _perf_ctx,
            bytes_scanned,
            keys_scanned,
            Some(self.db_metrics.clone()),
        ))
    }

    fn safe_range_iter(
        &'a self,
        range: impl RangeBounds<K>,
    ) -> Result<Self::SafeIterator, TypedStoreError> {
        let readopts = self.create_read_options_with_range(range);
        let db_iter = self.rocksdb.raw_iterator_cf(&self.cf()?, readopts);
        let (_timer, bytes_scanned, keys_scanned, _perf_ctx) = self.create_iter_context();
        Ok(SafeIter::new(
            self.cf.clone(),
            db_iter,
            _timer,
            _perf_ctx,
            bytes_scanned,
            keys_scanned,
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

    /// Convenience method for batch insertion
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

    /// Convenience method for batch removal
    #[tracing::instrument(level = "trace", skip_all, err)]
    fn multi_remove<J>(&self, keys: impl IntoIterator<Item = J>) -> Result<(), Self::Error>
    where
        J: Borrow<K>,
    {
        let mut batch = self.batch();
        batch.delete_batch(self, keys)?;
        batch.write()
    }

    /// Try to catch up with primary when running as secondary
    #[tracing::instrument(level = "trace", skip_all, err)]
    fn try_catch_up_with_primary(&self) -> Result<(), Self::Error> {
        self.rocksdb
            .try_catch_up_with_primary()
            .map_err(typed_store_err_from_rocks_err)
    }
}

/// Read a size from an environment variable
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

/// The read-write options
#[derive(Clone, Debug)]
pub struct ReadWriteOptions {
    /// Whether to ignore range deletions
    pub ignore_range_deletions: bool,
    // Whether to sync to disk on every write.
    sync_to_disk: bool,
}

impl ReadWriteOptions {
    /// The read options
    pub fn readopts(&self) -> ReadOptions {
        let mut readopts = ReadOptions::default();
        readopts.set_ignore_range_deletions(self.ignore_range_deletions);
        readopts
    }

    /// The write options
    pub fn writeopts(&self) -> WriteOptions {
        let mut opts = WriteOptions::default();
        opts.set_sync(self.sync_to_disk);
        opts
    }

    /// Set the ignore range deletions
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

/// The rocksdb options
#[derive(Default, Clone)]
pub struct DBOptions {
    /// The rocksdb options
    pub options: rocksdb::Options,
    /// The read-write options
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
                tracing::warn!("Failed to raise fdlimit, defaulting to 1024");
                1024
            }
        };
        // on windows raise_fd_limit return None
        opt.set_max_open_files((limit / 8) as i32);
    }

    // The table cache is locked for updates and this determines the number
    // of shards, ie 2^10. Increase in case of lock contentions.
    opt.set_table_cache_num_shard_bits(10);

    // LSM compression settings
    opt.set_compression_type(rocksdb::DBCompressionType::Lz4);
    opt.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);
    opt.set_bottommost_zstd_max_train_bytes(1024 * 1024, true);

    // Sui uses multiple RocksDB in a node, so total sizes of write buffers and WAL can be higher
    // than the limits below.
    //
    // RocksDB also exposes the option to configure total write buffer size across
    // multiple instances. But the write buffer flush policy (flushing the buffer
    // receiving the next write) may not work well. So sticking to per-db write buffer
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
    // https://github.com/EighteenZi/rocksdb_wiki/blob/master/
    // Memory-usage-in-RocksDB.md#indexes-and-filter-blocks
    opt.set_block_based_table_factory(&get_block_options(128 << 20, Some(16 << 10), Some(true)));

    // Set memtable bloomfilter.
    opt.set_memtable_prefix_bloom_ratio(0.02);

    DBOptions {
        options: opt,
        rw_options: ReadWriteOptions::default(),
    }
}

/// Get the block options
pub fn get_block_options(
    block_cache_size_bytes: usize,
    block_size_bytes: Option<usize>,
    pin_l0_filter_and_index_blocks_in_block_cache: Option<bool>,
) -> BlockBasedOptions {
    // https://github.com/facebook/rocksdb/blob/
    // 11cb6af6e5009c51794641905ca40ce5beec7fee/options/options.cc#L611-L621
    let mut block_options = BlockBasedOptions::default();
    // Overrides block size.
    if let Some(block_size_bytes) = block_size_bytes {
        block_options.set_block_size(block_size_bytes);
    }
    // Configure a block cache.
    block_options.set_block_cache(&Cache::new_lru_cache(block_cache_size_bytes));
    block_options.set_cache_index_and_filter_blocks(true);
    // Set a bloomfilter with 1% false positive rate.
    block_options.set_bloom_filter(10.0, false);
    if let Some(pin_l0_filter_and_index_blocks_in_block_cache) =
        pin_l0_filter_and_index_blocks_in_block_cache
    {
        // From https://github.com/EighteenZi/rocksdb_wiki/blob/master/
        // Block-Cache.md#caching-index-and-filter-blocks
        block_options.set_pin_l0_filter_and_index_blocks_in_cache(
            pin_l0_filter_and_index_blocks_in_block_cache,
        );
    }
    block_options
}

/// Opens a database with options, and a number of column families that are created
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
    // Customize database options
    let mut options = db_options.unwrap_or_else(|| default_db_options().options);
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    options
}

/// Opens a database with options, and a number of column families with individual options that
/// are created if they do not exist.
#[tracing::instrument(level="debug", skip_all, fields(path = ?path.as_ref()), err)]
pub fn open_cf_opts<P: AsRef<Path>>(
    path: P,
    db_options: Option<rocksdb::Options>,
    metric_conf: MetricConf,
    opt_cfs: &[(&str, rocksdb::Options)],
) -> Result<Arc<RocksDB>, TypedStoreError> {
    let path = path.as_ref();
    // In the simulator, we intercept the wall clock in the test thread only. This causes problems
    // because rocksdb uses the simulated clock when creating its background threads, but then
    // those threads see the real wall clock (because they are not the test thread), which causes
    // rocksdb to panic. The `nondeterministic` macro evaluates expressions in new threads, which
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
        Ok(Arc::new(RocksDB::new(
            rocksdb,
            metric_conf,
            PathBuf::from(path),
        )))
    })
}

/// TODO: Good description of why we're doing this :
/// RocksDB stores keys in BE and has a seek operator
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

/// The type of database access
/// Safe drop the database
pub fn safe_drop_db(path: PathBuf) -> Result<(), rocksdb::Error> {
    rocksdb::DB::destroy(&rocksdb::Options::default(), path)
}

/// Populate missing column families
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

/// Given a vec<u8>, find the value which is one more than the vector
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

/// Check if all the bytes in the vector are 0xFF
fn is_max(v: &[u8]) -> bool {
    v.iter().all(|&x| x == u8::MAX)
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
        // 32 byte number
        struct Num32(4);
    }

    let mut v = vec![255; 32];
    big_endian_saturating_add_one(&mut v);
    assert!(Num32::MAX == Num32::from_big_endian(&v));

    check_add(vec![1; 32]);
    check_add(vec![6; 32]);
    check_add(vec![254; 32]);

    // TBD: More tests coming with randomized arrays
}
