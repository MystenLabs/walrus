// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    cell::RefCell,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use once_cell::sync::OnceCell;
use prometheus::{
    HistogramVec,
    IntCounterVec,
    IntGaugeVec,
    Registry,
    register_histogram_vec_with_registry,
    register_int_counter_vec_with_registry,
    register_int_gauge_vec_with_registry,
};
use rocksdb::{PerfContext, PerfMetric, PerfStatsLevel, perf::set_perf_stats};
use tap::TapFallible;

thread_local! {
    static PER_THREAD_ROCKS_PERF_CONTEXT: std::cell::RefCell<rocksdb::PerfContext> =
        RefCell::new(PerfContext::default());
}

const LATENCY_SEC_BUCKETS: &[f64] = &[
    0.00001, 0.00005, // 10 mcs, 50 mcs
    0.0001, 0.0002, 0.0003, 0.0004, 0.0005, // 100..500 mcs
    0.001, 0.002, 0.003, 0.004, 0.005, // 1..5ms
    0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1., 2.5, 5., 10.,
];

#[derive(Debug, Clone)]
/// A struct for sampling based on number of operations or duration.
/// Sampling happens if the duration expires and after number of operations
pub struct SamplingInterval {
    /// Sample once every time duration
    pub once_every_duration: Duration,
    /// Sample once every number of operations
    pub after_num_ops: u64,
    /// Counter for keeping track of previous sample
    pub counter: Arc<AtomicU64>,
}

impl Default for SamplingInterval {
    fn default() -> Self {
        // Enabled with 60 second interval
        SamplingInterval::new(Duration::from_secs(60), 0)
    }
}

impl SamplingInterval {
    /// Create a new sampling interval
    pub fn new(once_every_duration: Duration, after_num_ops: u64) -> Self {
        let counter = Arc::new(AtomicU64::new(1));
        if !once_every_duration.is_zero() {
            let counter = counter.clone();
            tokio::task::spawn(async move {
                loop {
                    if counter.load(Ordering::SeqCst) > after_num_ops {
                        counter.store(0, Ordering::SeqCst);
                    }
                    tokio::time::sleep(once_every_duration).await;
                }
            });
        }
        SamplingInterval {
            once_every_duration,
            after_num_ops,
            counter,
        }
    }
    /// Create a new sampling interval from the current one
    pub fn new_from_self(&self) -> SamplingInterval {
        SamplingInterval::new(self.once_every_duration, self.after_num_ops)
    }
    /// Sample the metrics
    pub fn sample(&self) -> bool {
        if self.once_every_duration.is_zero() {
            self.counter.fetch_add(1, Ordering::Relaxed) % (self.after_num_ops + 1) == 0
        } else {
            self.counter.fetch_add(1, Ordering::Relaxed) == 0
        }
    }
}

#[derive(Debug)]
/// The metrics for the column families
pub struct ColumnFamilyMetrics {
    /// The storage size occupied by the sst files in the column family
    pub rocksdb_total_sst_files_size: IntGaugeVec,
    /// The storage size occupied by the blob files in the column family
    pub rocksdb_total_blob_files_size: IntGaugeVec,
    /// Total number of files used in the column family
    pub rocksdb_total_num_files: IntGaugeVec,
    /// Number of level 0 files in the column family
    pub rocksdb_num_level0_files: IntGaugeVec,
    /// The current approximate size of active memtable (bytes).
    pub rocksdb_current_size_active_mem_tables: IntGaugeVec,
    /// The memory size occupied by the column family's in-memory buffer
    pub rocksdb_size_all_mem_tables: IntGaugeVec,
    /// Number of snapshots held for the column family
    pub rocksdb_num_snapshots: IntGaugeVec,
    /// Unit timestamp of the oldest unreleased snapshot
    pub rocksdb_oldest_snapshot_time: IntGaugeVec,
    /// The current actual delayed write rate. 0 means no delay
    pub rocksdb_actual_delayed_write_rate: IntGaugeVec,
    /// Flag indicating if writes are stopped (1) or not (0) for this column family
    pub rocksdb_is_write_stopped: IntGaugeVec,
    /// The block cache capacity of the column family.
    pub rocksdb_block_cache_capacity: IntGaugeVec,
    /// The memory size used by the column family in the block cache.
    pub rocksdb_block_cache_usage: IntGaugeVec,
    /// The memory size used by pinned entries in block cache
    pub rocksdb_block_cache_pinned_usage: IntGaugeVec,
    /// The estimated memory size used for reading SST tables in this column
    /// family such as filters and index blocks. Note that this number does not
    /// include the memory used in block cache.
    pub rocksdb_estimate_table_readers_mem: IntGaugeVec,
    /// The number of immutable memtables that have not yet been flushed.
    pub rocksdb_num_immutable_mem_tables: IntGaugeVec,
    /// A 1 or 0 flag indicating whether a memtable flush is pending.
    pub rocksdb_mem_table_flush_pending: IntGaugeVec,
    /// A 1 or 0 flag indicating whether a compaction job is pending.
    pub rocksdb_compaction_pending: IntGaugeVec,
    /// Estimated total number of bytes compaction needs to rewrite to get all levels down
    /// to under target size. Not valid for other compactions than level-based.
    pub rocksdb_estimate_pending_compaction_bytes: IntGaugeVec,
    /// The number of compactions that are currently running for the column family.
    pub rocksdb_num_running_compactions: IntGaugeVec,
    /// The number of flushes that are currently running for the column family.
    pub rocksdb_num_running_flushes: IntGaugeVec,
    /// Estimated oldest key timestamp in the DB
    pub rocksdb_estimate_oldest_key_time: IntGaugeVec,
    /// The accumulated number of RocksDB background errors.
    pub rocksdb_background_errors: IntGaugeVec,
    /// The estimated number of keys in the table
    pub rocksdb_estimated_num_keys: IntGaugeVec,
    /// The number of level to which L0 data will be compacted.
    pub rocksdb_base_level: IntGaugeVec,
}

impl ColumnFamilyMetrics {
    pub(crate) fn new(registry: &Registry) -> Self {
        ColumnFamilyMetrics {
            rocksdb_total_sst_files_size: register_int_gauge_vec_with_registry!(
                "rocksdb_total_sst_files_size",
                "The storage size occupied by the sst files in the column family",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            rocksdb_total_blob_files_size: register_int_gauge_vec_with_registry!(
                "rocksdb_total_blob_files_size",
                "The storage size occupied by the blob files in the column family",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            rocksdb_total_num_files: register_int_gauge_vec_with_registry!(
                "rocksdb_total_num_files",
                "Total number of files used in the column family",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            rocksdb_num_level0_files: register_int_gauge_vec_with_registry!(
                "rocksdb_num_level0_files",
                "Number of level 0 files in the column family",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            rocksdb_current_size_active_mem_tables: register_int_gauge_vec_with_registry!(
                "rocksdb_current_size_active_mem_tables",
                "The current approximate size of active memtable (bytes).",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            rocksdb_size_all_mem_tables: register_int_gauge_vec_with_registry!(
                "rocksdb_size_all_mem_tables",
                "The memory size occupied by the column family's in-memory buffer",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            rocksdb_num_snapshots: register_int_gauge_vec_with_registry!(
                "rocksdb_num_snapshots",
                "Number of snapshots held for the column family",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            rocksdb_oldest_snapshot_time: register_int_gauge_vec_with_registry!(
                "rocksdb_oldest_snapshot_time",
                "Unit timestamp of the oldest unreleased snapshot",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            rocksdb_actual_delayed_write_rate: register_int_gauge_vec_with_registry!(
                "rocksdb_actual_delayed_write_rate",
                "The current actual delayed write rate. 0 means no delay",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            rocksdb_is_write_stopped: register_int_gauge_vec_with_registry!(
                "rocksdb_is_write_stopped",
                "Flag indicating if writes are stopped (1) or not (0) for this column family",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            rocksdb_block_cache_capacity: register_int_gauge_vec_with_registry!(
                "rocksdb_block_cache_capacity",
                "The block cache capacity of the column family.",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            rocksdb_block_cache_usage: register_int_gauge_vec_with_registry!(
                "rocksdb_block_cache_usage",
                "The memory size used by the column family in the block cache.",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            rocksdb_block_cache_pinned_usage: register_int_gauge_vec_with_registry!(
                "rocksdb_block_cache_pinned_usage",
                "Memory size used by pinned entries in block cache",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            rocksdb_estimate_table_readers_mem: register_int_gauge_vec_with_registry!(
                "rocksdb_estimate_table_readers_mem",
                "The estimated memory size used for reading SST tables in this column
                family such as filters and index blocks. Note that this number does not
                include the memory used in block cache.",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            rocksdb_num_immutable_mem_tables: register_int_gauge_vec_with_registry!(
                "rocksdb_num_immutable_mem_tables",
                "The number of immutable memtables that have not yet been flushed.",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            rocksdb_mem_table_flush_pending: register_int_gauge_vec_with_registry!(
                "rocksdb_mem_table_flush_pending",
                "A 1 or 0 flag indicating whether a memtable flush is pending.
                If this number is 1, it means a memtable is waiting for being flushed,
                but there might be too many L0 files that prevents it from being flushed.",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            rocksdb_compaction_pending: register_int_gauge_vec_with_registry!(
                "rocksdb_compaction_pending",
                "A 1 or 0 flag indicating whether a compaction job is pending.
                If this number is 1, it means some part of the column family requires
                compaction in order to maintain shape of LSM tree, but the compaction
                is pending because the desired compaction job is either waiting for
                other dependent compactions to be finished or waiting for an available
                compaction thread.",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            rocksdb_estimate_pending_compaction_bytes: register_int_gauge_vec_with_registry!(
                "rocksdb_estimate_pending_compaction_bytes",
                "Estimated total number of bytes compaction needs to rewrite to get all levels down
                to under target size. Not valid for other compactions than level-based.",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            rocksdb_num_running_compactions: register_int_gauge_vec_with_registry!(
                "rocksdb_num_running_compactions",
                "The number of compactions that are currently running for the column family.",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            rocksdb_num_running_flushes: register_int_gauge_vec_with_registry!(
                "rocksdb_num_running_flushes",
                "The number of flushes that are currently running for the column family.",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            rocksdb_estimate_oldest_key_time: register_int_gauge_vec_with_registry!(
                "rocksdb_estimate_oldest_key_time",
                "Estimation of the oldest key timestamp in the DB. Only available
                for FIFO compaction with compaction_options_fifo.allow_compaction = false.",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            rocksdb_estimated_num_keys: register_int_gauge_vec_with_registry!(
                "rocksdb_estimated_num_keys",
                "The estimated number of keys in the table",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            rocksdb_background_errors: register_int_gauge_vec_with_registry!(
                "rocksdb_background_errors",
                "The accumulated number of RocksDB background errors.",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            rocksdb_base_level: register_int_gauge_vec_with_registry!(
                "rocksdb_base_level",
                "The number of level to which L0 data will be compacted.",
                &["cf_name"],
                registry,
            )
            .unwrap(),
        }
    }
}

#[derive(Debug)]
/// The metrics for the operations
pub struct OperationMetrics {
    /// Rocksdb iter latency in seconds
    pub rocksdb_iter_latency_seconds: HistogramVec,
    /// Rocksdb iter key bytes
    pub rocksdb_iter_key_bytes: HistogramVec,
    /// Rocksdb iter value bytes
    pub rocksdb_iter_value_bytes: HistogramVec,
    /// Rocksdb iter size in bytes
    pub rocksdb_iter_bytes: HistogramVec,
    /// Rocksdb iter num keys
    pub rocksdb_iter_keys: HistogramVec,
    /// Rocksdb get latency in seconds
    pub rocksdb_get_latency_seconds: HistogramVec,
    /// Rocksdb get bytes
    pub rocksdb_get_bytes: HistogramVec,
    /// Rocksdb get key bytes
    pub rocksdb_get_key_bytes: HistogramVec,
    /// Rocksdb get value bytes
    pub rocksdb_get_value_bytes: HistogramVec,
    /// Rocksdb contains_key latency in seconds
    pub rocksdb_contains_key_latency_seconds: HistogramVec,
    /// Number of times key_may_exist_cf returned true per CF (potential positives)
    pub rocksdb_bloom_filter_may_exist_true_total: IntCounterVec,
    /// Number of times key_may_exist_cf returned true but get
    /// found nothing (false positives) per CF
    pub rocksdb_bloom_filter_false_positive_total: IntCounterVec,
    /// Rocksdb multiget latency in seconds
    pub rocksdb_multiget_latency_seconds: HistogramVec,
    /// Rocksdb multiget bytes
    pub rocksdb_multiget_bytes: HistogramVec,
    /// Rocksdb put latency in seconds
    pub rocksdb_put_latency_seconds: HistogramVec,
    /// Rocksdb put bytes
    pub rocksdb_put_bytes: HistogramVec,
    /// Rocksdb put key bytes
    pub rocksdb_put_key_bytes: HistogramVec,
    /// Rocksdb put value bytes
    pub rocksdb_put_value_bytes: HistogramVec,
    /// Rocksdb batch put bytes
    pub rocksdb_batch_put_bytes: HistogramVec,
    /// Rocksdb batch put key bytes
    pub rocksdb_batch_put_key_bytes: HistogramVec,
    /// Rocksdb batch put value bytes
    pub rocksdb_batch_put_value_bytes: HistogramVec,
    /// Rocksdb delete latency in seconds
    pub rocksdb_delete_latency_seconds: HistogramVec,
    /// Rocksdb delete calls
    pub rocksdb_deletes: IntCounterVec,
    /// Rocksdb schema batch commit latency in seconds
    pub rocksdb_batch_commit_latency_seconds: HistogramVec,
    /// Rocksdb schema batch commit size in bytes
    pub rocksdb_batch_commit_bytes: HistogramVec,
    /// Number of active db handles
    pub rocksdb_num_active_db_handles: IntGaugeVec,
    /// Number of batch writes that took more than 1 second
    pub rocksdb_very_slow_batch_writes_count: IntCounterVec,
    /// Total duration of batch writes that took more than 1 second
    pub rocksdb_very_slow_batch_writes_duration_ms: IntCounterVec,
    /// Number of puts that took more than 1 second
    pub rocksdb_very_slow_puts_count: IntCounterVec,
    /// Total duration of puts that took more than 1 second
    pub rocksdb_very_slow_puts_duration_ms: IntCounterVec,
    /// Bloom filter ticker counters
    /// Note: These are only available upon turning on statistics in the rocksdb options
    /// using `opt.enable_statistics();` and with lowest setting
    /// `opt.set_statistics_level(StatsLevel::ExceptHistogramOrTimers);`
    /// Number of times bloom filter has avoided file reads (negatives)
    pub rocksdb_bloom_filter_useful_total: IntGaugeVec,
    /// Number of times bloom FullFilter has not avoided the reads
    pub rocksdb_bloom_filter_full_positive_total: IntGaugeVec,
    /// Number of times bloom FullFilter has not avoided the reads and data actually exist
    pub rocksdb_bloom_filter_full_true_positive_total: IntGaugeVec,
    /// Number of times prefix filter was queried
    pub rocksdb_bloom_filter_prefix_checked_total: IntGaugeVec,
    /// Number of times prefix filter returned false so prevented accessing data+index blocks
    pub rocksdb_bloom_filter_prefix_useful_total: IntGaugeVec,
    /// Number of times prefix filter found a key matching the point query
    pub rocksdb_bloom_filter_prefix_true_positive_total: IntGaugeVec,
    /// Number of bytes the compaction algorithm copied
    pub rocksdb_compaction_num_bytes_written: IntGaugeVec,
    /// Number of bytes read by the compaction algorithm
    pub rocksdb_compaction_num_bytes_read: IntGaugeVec,
    /// Number of bytes read
    pub rocksdb_num_bytes_read: IntGaugeVec,
    /// Number of bytes written
    pub rocksdb_num_bytes_written: IntGaugeVec,
}

impl OperationMetrics {
    pub(crate) fn new(registry: &Registry) -> Self {
        let bucket_vec = prometheus::exponential_buckets(1.0, 4.0, 15)
            .unwrap()
            .to_vec();

        OperationMetrics {
            rocksdb_iter_latency_seconds: register_histogram_vec_with_registry!(
                "rocksdb_iter_latency_seconds",
                "Rocksdb iter latency in seconds",
                &["cf_name"],
                LATENCY_SEC_BUCKETS.to_vec(),
                registry,
            )
            .unwrap(),
            rocksdb_iter_key_bytes: register_histogram_vec_with_registry!(
                "rocksdb_iter_key_bytes",
                "Rocksdb iter key size in bytes",
                &["cf_name"],
                bucket_vec.clone(),
                registry,
            )
            .unwrap(),
            rocksdb_iter_bytes: register_histogram_vec_with_registry!(
                "rocksdb_iter_bytes",
                "Rocksdb iter size in bytes",
                &["cf_name"],
                bucket_vec.clone(),
                registry,
            )
            .unwrap(),
            rocksdb_iter_value_bytes: register_histogram_vec_with_registry!(
                "rocksdb_iter_value_bytes",
                "Rocksdb iter value size in bytes",
                &["cf_name"],
                bucket_vec.clone(),
                registry,
            )
            .unwrap(),
            rocksdb_iter_keys: register_histogram_vec_with_registry!(
                "rocksdb_iter_keys",
                "Rocksdb iter num keys",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            rocksdb_get_latency_seconds: register_histogram_vec_with_registry!(
                "rocksdb_get_latency_seconds",
                "Rocksdb get latency in seconds. `found` label is true if key was found",
                &["cf_name", "found"],
                LATENCY_SEC_BUCKETS.to_vec(),
                registry,
            )
            .unwrap(),
            rocksdb_get_bytes: register_histogram_vec_with_registry!(
                "rocksdb_get_bytes",
                "Rocksdb get call returned data size in bytes",
                &["cf_name"],
                bucket_vec.clone(),
                registry,
            )
            .unwrap(),
            rocksdb_get_key_bytes: register_histogram_vec_with_registry!(
                "rocksdb_get_key_bytes",
                "Rocksdb get call key size in bytes",
                &["cf_name"],
                bucket_vec.clone(),
                registry
            )
            .unwrap(),
            rocksdb_get_value_bytes: register_histogram_vec_with_registry!(
                "rocksdb_get_value_bytes",
                "Rocksdb get call returned value size in bytes",
                &["cf_name"],
                bucket_vec.clone(),
                registry
            )
            .unwrap(),
            rocksdb_contains_key_latency_seconds: register_histogram_vec_with_registry!(
                "rocksdb_contains_key_latency_seconds",
                "Rocksdb contains_key latency in seconds. `found` label is true if key was found",
                &["cf_name", "found"],
                LATENCY_SEC_BUCKETS.to_vec(),
                registry
            )
            .unwrap(),
            rocksdb_bloom_filter_may_exist_true_total: register_int_counter_vec_with_registry!(
                "rocksdb_bloom_filter_may_exist_true_total",
                "Number of times key_may_exist_cf returned true (potential positives)",
                &["cf_name"],
                registry
            )
            .unwrap(),
            rocksdb_bloom_filter_false_positive_total: register_int_counter_vec_with_registry!(
                "rocksdb_bloom_filter_false_positive_total",
                "Number of false positives where key_may_exist_cf \
                returned true but get found nothing",
                &["cf_name"],
                registry
            )
            .unwrap(),
            rocksdb_multiget_latency_seconds: register_histogram_vec_with_registry!(
                "rocksdb_multiget_latency_seconds",
                "Rocksdb multiget latency in seconds",
                &["cf_name"],
                LATENCY_SEC_BUCKETS.to_vec(),
                registry,
            )
            .unwrap(),
            rocksdb_multiget_bytes: register_histogram_vec_with_registry!(
                "rocksdb_multiget_bytes",
                "Rocksdb multiget call returned data size in bytes",
                &["cf_name"],
                bucket_vec.clone(),
                registry,
            )
            .unwrap(),
            rocksdb_put_latency_seconds: register_histogram_vec_with_registry!(
                "rocksdb_put_latency_seconds",
                "Rocksdb put latency in seconds",
                &["cf_name"],
                LATENCY_SEC_BUCKETS.to_vec(),
                registry,
            )
            .unwrap(),
            rocksdb_put_bytes: register_histogram_vec_with_registry!(
                "rocksdb_put_bytes",
                "Rocksdb put call returned data size in bytes",
                &["cf_name"],
                bucket_vec.clone(),
                registry,
            )
            .unwrap(),
            rocksdb_put_key_bytes: register_histogram_vec_with_registry!(
                "rocksdb_put_key_bytes",
                "Rocksdb put call key size in bytes",
                &["cf_name"],
                bucket_vec.clone(),
                registry,
            )
            .unwrap(),
            rocksdb_put_value_bytes: register_histogram_vec_with_registry!(
                "rocksdb_put_value_bytes",
                "Rocksdb put call value size in bytes",
                &["cf_name"],
                bucket_vec.clone(),
                registry,
            )
            .unwrap(),
            rocksdb_batch_put_bytes: register_histogram_vec_with_registry!(
                "rocksdb_batch_put_bytes",
                "Rocksdb batch put call puts data size in bytes",
                &["cf_name"],
                bucket_vec.clone(),
                registry,
            )
            .unwrap(),
            rocksdb_batch_put_key_bytes: register_histogram_vec_with_registry!(
                "rocksdb_batch_put_key_bytes",
                "Rocksdb batch put call puts key data size in bytes",
                &["cf_name"],
                bucket_vec.clone(),
                registry,
            )
            .unwrap(),
            rocksdb_batch_put_value_bytes: register_histogram_vec_with_registry!(
                "rocksdb_batch_put_value_bytes",
                "Rocksdb batch put call puts value data size in bytes",
                &["cf_name"],
                bucket_vec.clone(),
                registry,
            )
            .unwrap(),
            rocksdb_delete_latency_seconds: register_histogram_vec_with_registry!(
                "rocksdb_delete_latency_seconds",
                "Rocksdb delete latency in seconds",
                &["cf_name"],
                LATENCY_SEC_BUCKETS.to_vec(),
                registry,
            )
            .unwrap(),
            rocksdb_deletes: register_int_counter_vec_with_registry!(
                "rocksdb_deletes",
                "Rocksdb delete calls",
                &["cf_name"],
                registry
            )
            .unwrap(),
            rocksdb_batch_commit_latency_seconds: register_histogram_vec_with_registry!(
                "rocksdb_write_batch_commit_latency_seconds",
                "Rocksdb schema batch commit latency in seconds",
                &["db_name"],
                LATENCY_SEC_BUCKETS.to_vec(),
                registry,
            )
            .unwrap(),
            rocksdb_batch_commit_bytes: register_histogram_vec_with_registry!(
                "rocksdb_batch_commit_bytes",
                "Rocksdb schema batch commit size in bytes",
                &["db_name"],
                bucket_vec,
                registry,
            )
            .unwrap(),
            rocksdb_num_active_db_handles: register_int_gauge_vec_with_registry!(
                "rocksdb_num_active_db_handles",
                "Number of active db handles",
                &["db_name"],
                registry,
            )
            .unwrap(),
            rocksdb_very_slow_batch_writes_count: register_int_counter_vec_with_registry!(
                "rocksdb_num_very_slow_batch_writes",
                "Number of batch writes that took more than 1 second",
                &["db_name"],
                registry,
            )
            .unwrap(),
            rocksdb_very_slow_batch_writes_duration_ms: register_int_counter_vec_with_registry!(
                "rocksdb_very_slow_batch_writes_duration",
                "Total duration of batch writes that took more than 1 second",
                &["db_name"],
                registry,
            )
            .unwrap(),
            rocksdb_very_slow_puts_count: register_int_counter_vec_with_registry!(
                "rocksdb_num_very_slow_puts",
                "Number of puts that took more than 1 second",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            rocksdb_very_slow_puts_duration_ms: register_int_counter_vec_with_registry!(
                "rocksdb_very_slow_puts_duration",
                "Total duration of puts that took more than 1 second",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            rocksdb_bloom_filter_useful_total: register_int_gauge_vec_with_registry!(
                "rocksdb_bloom_filter_useful_total",
                "Number of times bloom filter has avoided file reads (negatives)",
                &["db_name"],
                registry,
            )
            .unwrap(),
            rocksdb_bloom_filter_full_positive_total: register_int_gauge_vec_with_registry!(
                "rocksdb_bloom_filter_full_positive_total",
                "Number of times bloom FullFilter has not avoided the reads",
                &["db_name"],
                registry,
            )
            .unwrap(),
            rocksdb_bloom_filter_full_true_positive_total: register_int_gauge_vec_with_registry!(
                "rocksdb_bloom_filter_full_true_positive_total",
                "Number of times bloom FullFilter hasn't avoided reads and data actually exist",
                &["db_name"],
                registry,
            )
            .unwrap(),
            rocksdb_bloom_filter_prefix_checked_total: register_int_gauge_vec_with_registry!(
                "rocksdb_bloom_filter_prefix_checked_total",
                "Number of times prefix filter was queried",
                &["db_name"],
                registry,
            )
            .unwrap(),
            rocksdb_bloom_filter_prefix_useful_total: register_int_gauge_vec_with_registry!(
                "rocksdb_bloom_filter_prefix_useful_total",
                "Number of times prefix filter returned false to prevent data+index block",
                &["db_name"],
                registry,
            )
            .unwrap(),
            rocksdb_bloom_filter_prefix_true_positive_total: register_int_gauge_vec_with_registry!(
                "rocksdb_bloom_filter_prefix_true_positive_total",
                "Number of times prefix filter found a key matching the point query",
                &["db_name"],
                registry,
            )
            .unwrap(),
            rocksdb_compaction_num_bytes_written: register_int_gauge_vec_with_registry!(
                "rocksdb_compaction_num_bytes_written",
                "Number of bytes the compaction algorithm copied",
                &["db_name"],
                registry,
            )
            .unwrap(),
            rocksdb_compaction_num_bytes_read: register_int_gauge_vec_with_registry!(
                "rocksdb_compaction_num_bytes_read",
                "Number of bytes read by the compaction algorithm",
                &["db_name"],
                registry,
            )
            .unwrap(),
            rocksdb_num_bytes_read: register_int_gauge_vec_with_registry!(
                "rocksdb_num_bytes_read",
                "Number of bytes read by the db",
                &["db_name"],
                registry,
            )
            .unwrap(),
            rocksdb_num_bytes_written: register_int_gauge_vec_with_registry!(
                "rocksdb_num_bytes_written",
                "Number of bytes written by the db",
                &["db_name"],
                registry,
            )
            .unwrap(),
        }
    }
}

/// The performance context for RocksDB
#[derive(Debug)]
pub struct RocksDBPerfContext;

impl Default for RocksDBPerfContext {
    fn default() -> Self {
        set_perf_stats(PerfStatsLevel::EnableTime);
        PER_THREAD_ROCKS_PERF_CONTEXT.with(|perf_context| {
            perf_context.borrow_mut().reset();
        });
        RocksDBPerfContext {}
    }
}

impl Drop for RocksDBPerfContext {
    fn drop(&mut self) {
        set_perf_stats(PerfStatsLevel::Disable);
    }
}

#[derive(Debug)]
/// The metrics for the read performance
pub struct ReadPerfContextMetrics {
    /// Helps us figure out whether too many comparisons in binary search can be a problem,
    /// especially when a more expensive comparator is used.
    pub user_key_comparison_count: IntCounterVec,
    /// Tells us how many times we read data blocks from block cache
    pub block_cache_hit_count: IntCounterVec,
    /// Tells us how many times we have to read blocks from the file system
    pub block_read_count: IntCounterVec,
    /// Total bytes read from the file system
    pub block_read_byte: IntCounterVec,
    /// Total nanos spent on block reads
    pub block_read_nanos: IntCounterVec,
    /// Total nanos spent on block checksum
    pub block_checksum_nanos: IntCounterVec,
    /// Total nanos spent on block decompression
    pub block_decompress_nanos: IntCounterVec,
    /// Total bytes for values returned by Get
    pub get_read_bytes: IntCounterVec,
    /// Total bytes for values returned by MultiGet
    pub multiget_read_bytes: IntCounterVec,
    /// Total nanos spent on getting snapshot
    pub get_snapshot_nanos: IntCounterVec,
    /// Total nanos spent on reading data from memtable
    pub get_from_memtable_nanos: IntCounterVec,
    /// Total number of memtables queried
    pub get_from_memtable_count: IntCounterVec,
    /// Total nanos spent after Get() finds a key
    pub get_post_process_nanos: IntCounterVec,
    /// Total nanos spent reading from output files
    pub get_from_output_files_nanos: IntCounterVec,
    /// Total nanos spent on acquiring db mutex
    pub db_mutex_lock_nanos: IntCounterVec,
    /// Total nanos spent waiting with a condition variable created with DB Mutex
    pub db_condition_wait_nanos: IntCounterVec,
    /// Total nanos spent on merge operator
    pub merge_operator_nanos: IntCounterVec,
    /// Total nanos spent on reading index block from block cache or SST file
    pub read_index_block_nanos: IntCounterVec,
    /// Total nanos spent on reading filter block from block cache or SST file
    pub read_filter_block_nanos: IntCounterVec,
    /// Total nanos spent on creating data block iterator
    pub new_table_block_iter_nanos: IntCounterVec,
    /// Total nanos spent on seeking a key in data/index blocks
    pub block_seek_nanos: IntCounterVec,
    /// Total nanos spent on finding or creating a table reader
    pub find_table_nanos: IntCounterVec,
    /// Total number of mem table bloom hits
    pub bloom_memtable_hit_count: IntCounterVec,
    /// Total number of mem table bloom misses
    pub bloom_memtable_miss_count: IntCounterVec,
    /// Total number of SST table bloom hits
    pub bloom_sst_hit_count: IntCounterVec,
    /// Total number of SST table bloom misses
    pub bloom_sst_miss_count: IntCounterVec,
    /// Total nanos spent waiting on key locks in transaction lock manager
    pub key_lock_wait_time: IntCounterVec,
    /// Total number of times acquiring a lock was blocked by another transaction
    pub key_lock_wait_count: IntCounterVec,
    /// Total number of deleted keys skipped during iteration
    pub internal_delete_skipped_count: IntCounterVec,
    /// Total number of internal keys skipped during iteration
    pub internal_skipped_count: IntCounterVec,
}

impl ReadPerfContextMetrics {
    pub(crate) fn new(registry: &Registry) -> Self {
        ReadPerfContextMetrics {
            user_key_comparison_count: register_int_counter_vec_with_registry!(
                "user_key_comparison_count",
                "Number of comparisons in binary search",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            block_cache_hit_count: register_int_counter_vec_with_registry!(
                "block_cache_hit_count",
                "Number of block cache hits",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            block_read_count: register_int_counter_vec_with_registry!(
                "block_read_count",
                "Number of blocks read from filesystem (cache miss or disabled)",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            block_read_byte: register_int_counter_vec_with_registry!(
                "block_read_byte",
                "Total bytes read from filesystem, including index and bloom filter blocks",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            block_read_nanos: register_int_counter_vec_with_registry!(
                "block_read_nanos",
                "Total nanos spent on block reads",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            block_checksum_nanos: register_int_counter_vec_with_registry!(
                "block_checksum_nanos",
                "Total nanos spent on verifying block checksum",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            block_decompress_nanos: register_int_counter_vec_with_registry!(
                "block_decompress_nanos",
                "Total nanos spent on decompressing a block",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            get_read_bytes: register_int_counter_vec_with_registry!(
                "get_read_bytes",
                "Total bytes for values returned by Get",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            multiget_read_bytes: register_int_counter_vec_with_registry!(
                "multiget_read_bytes",
                "Total bytes for values returned by MultiGet.",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            get_snapshot_nanos: register_int_counter_vec_with_registry!(
                "get_snapshot_nanos",
                "Time spent in getting snapshot.",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            get_from_memtable_nanos: register_int_counter_vec_with_registry!(
                "get_from_memtable_nanos",
                "Time spent on reading data from memtable.",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            get_from_memtable_count: register_int_counter_vec_with_registry!(
                "get_from_memtable_count",
                "Number of memtables queried",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            get_post_process_nanos: register_int_counter_vec_with_registry!(
                "get_post_process_nanos",
                "Total nanos spent after Get() finds a key",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            get_from_output_files_nanos: register_int_counter_vec_with_registry!(
                "get_from_output_files_nanos",
                "Total nanos reading from output files",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            db_mutex_lock_nanos: register_int_counter_vec_with_registry!(
                "db_mutex_lock_nanos",
                "Time spent on acquiring db mutex",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            db_condition_wait_nanos: register_int_counter_vec_with_registry!(
                "db_condition_wait_nanos",
                "Time spent waiting with a condition variable created with DB Mutex.",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            merge_operator_nanos: register_int_counter_vec_with_registry!(
                "merge_operator_nanos",
                "Time spent on merge operator.",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            read_index_block_nanos: register_int_counter_vec_with_registry!(
                "read_index_block_nanos",
                "Time spent on reading index block from block cache or SST file",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            read_filter_block_nanos: register_int_counter_vec_with_registry!(
                "read_filter_block_nanos",
                "Time spent on reading filter block from block cache or SST file",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            new_table_block_iter_nanos: register_int_counter_vec_with_registry!(
                "new_table_block_iter_nanos",
                "Time spent on creating data block iterator",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            block_seek_nanos: register_int_counter_vec_with_registry!(
                "block_seek_nanos",
                "Time spent on seeking a key in data/index blocks",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            find_table_nanos: register_int_counter_vec_with_registry!(
                "find_table_nanos",
                "Time spent on finding or creating a table reader",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            bloom_memtable_hit_count: register_int_counter_vec_with_registry!(
                "bloom_memtable_hit_count",
                "Total number of mem table bloom hits",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            bloom_memtable_miss_count: register_int_counter_vec_with_registry!(
                "bloom_memtable_miss_count",
                "Total number of mem table bloom misses",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            bloom_sst_hit_count: register_int_counter_vec_with_registry!(
                "bloom_sst_hit_count",
                "Total number of SST table bloom hits",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            bloom_sst_miss_count: register_int_counter_vec_with_registry!(
                "bloom_sst_miss_count",
                "Total number of SST table bloom misses",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            key_lock_wait_time: register_int_counter_vec_with_registry!(
                "key_lock_wait_time",
                "Time spent waiting on key locks in transaction lock manager",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            key_lock_wait_count: register_int_counter_vec_with_registry!(
                "key_lock_wait_count",
                "Number of times acquiring a lock was blocked by another transaction",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            internal_delete_skipped_count: register_int_counter_vec_with_registry!(
                "internal_delete_skipped_count",
                "Total number of deleted keys skipped during iteration",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            internal_skipped_count: register_int_counter_vec_with_registry!(
                "internal_skipped_count",
                "Total number of internal keys skipped during iteration",
                &["cf_name"],
                registry,
            )
            .unwrap(),
        }
    }

    /// Report the metrics for the read performance
    pub fn report_metrics(&self, cf_name: &str) {
        PER_THREAD_ROCKS_PERF_CONTEXT.with(|perf_context_cell| {
            set_perf_stats(PerfStatsLevel::Disable);
            let perf_context = perf_context_cell.borrow();
            self.user_key_comparison_count
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::UserKeyComparisonCount));
            self.block_cache_hit_count
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::BlockCacheHitCount));
            self.block_read_count
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::BlockReadCount));
            self.block_read_byte
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::BlockReadByte));
            self.block_read_nanos
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::BlockReadTime));
            self.block_checksum_nanos
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::BlockChecksumTime));
            self.block_decompress_nanos
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::BlockDecompressTime));
            self.get_read_bytes
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::GetReadBytes));
            self.multiget_read_bytes
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::MultigetReadBytes));
            self.get_snapshot_nanos
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::GetSnapshotTime));
            self.get_from_memtable_nanos
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::GetFromMemtableTime));
            self.get_from_memtable_count
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::GetFromMemtableCount));
            self.get_post_process_nanos
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::GetPostProcessTime));
            self.get_from_output_files_nanos
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::GetFromOutputFilesTime));
            self.db_mutex_lock_nanos
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::DbMutexLockNanos));
            self.db_condition_wait_nanos
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::DbConditionWaitNanos));
            self.merge_operator_nanos
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::MergeOperatorTimeNanos));
            self.read_index_block_nanos
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::ReadIndexBlockNanos));
            self.read_filter_block_nanos
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::ReadFilterBlockNanos));
            self.new_table_block_iter_nanos
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::NewTableBlockIterNanos));
            self.block_seek_nanos
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::BlockSeekNanos));
            self.find_table_nanos
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::FindTableNanos));
            self.bloom_memtable_hit_count
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::BloomMemtableHitCount));
            self.bloom_memtable_miss_count
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::BloomMemtableMissCount));
            self.bloom_sst_hit_count
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::BloomSstHitCount));
            self.bloom_sst_miss_count
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::BloomSstMissCount));
            self.key_lock_wait_time
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::KeyLockWaitTime));
            self.key_lock_wait_count
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::KeyLockWaitCount));
            self.internal_delete_skipped_count
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::InternalDeleteSkippedCount));
            self.internal_skipped_count
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::InternalKeySkippedCount));
        });
    }
}

#[derive(Debug)]
/// The metrics for the write performance
pub struct WritePerfContextMetrics {
    /// Total nanos spent on writing to WAL
    pub write_wal_nanos: IntCounterVec,
    /// Total nanos spent on writing to memtable
    pub write_memtable_nanos: IntCounterVec,
    /// Total nanos spent on delaying or throttling write
    pub write_delay_nanos: IntCounterVec,
    /// Total nanos spent on writing a record, excluding the above four things
    pub write_pre_and_post_process_nanos: IntCounterVec,
    /// Total nanos spent on acquiring db mutex
    pub write_db_mutex_lock_nanos: IntCounterVec,
    /// Total nanos spent waiting with a condition variable created with DB Mutex
    pub write_db_condition_wait_nanos: IntCounterVec,
    /// Total nanos spent waiting on key locks in transaction lock manager
    pub write_key_lock_wait_nanos: IntCounterVec,
    /// Total number of times acquiring a lock was blocked by another transaction
    pub write_key_lock_wait_count: IntCounterVec,
}

impl WritePerfContextMetrics {
    pub(crate) fn new(registry: &Registry) -> Self {
        WritePerfContextMetrics {
            write_wal_nanos: register_int_counter_vec_with_registry!(
                "write_wal_nanos",
                "Total nanos spent on writing to WAL",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            write_memtable_nanos: register_int_counter_vec_with_registry!(
                "write_memtable_nanos",
                "Total nanos spent on writing to memtable",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            write_delay_nanos: register_int_counter_vec_with_registry!(
                "write_delay_nanos",
                "Total nanos spent on delaying or throttling write",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            write_pre_and_post_process_nanos: register_int_counter_vec_with_registry!(
                "write_pre_and_post_process_nanos",
                "Total nanos spent on writing a record, excluding the above four things",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            write_db_mutex_lock_nanos: register_int_counter_vec_with_registry!(
                "write_db_mutex_lock_nanos",
                "Time spent on acquiring db mutex",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            write_db_condition_wait_nanos: register_int_counter_vec_with_registry!(
                "write_db_condition_wait_nanos",
                "Time spent waiting with a condition variable created with DB Mutex.",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            write_key_lock_wait_nanos: register_int_counter_vec_with_registry!(
                "write_key_lock_wait_time",
                "Time spent waiting on key locks in transaction lock manager",
                &["cf_name"],
                registry,
            )
            .unwrap(),
            write_key_lock_wait_count: register_int_counter_vec_with_registry!(
                "write_key_lock_wait_count",
                "Number of times acquiring a lock was blocked by another transaction",
                &["cf_name"],
                registry,
            )
            .unwrap(),
        }
    }
    /// Report the metrics for the write performance
    pub fn report_metrics(&self, db_name: &str) {
        PER_THREAD_ROCKS_PERF_CONTEXT.with(|perf_context_cell| {
            set_perf_stats(PerfStatsLevel::Disable);
            let perf_context = perf_context_cell.borrow();
            self.write_wal_nanos
                .with_label_values(&[db_name])
                .inc_by(perf_context.metric(PerfMetric::WriteWalTime));
            self.write_memtable_nanos
                .with_label_values(&[db_name])
                .inc_by(perf_context.metric(PerfMetric::WriteMemtableTime));
            self.write_delay_nanos
                .with_label_values(&[db_name])
                .inc_by(perf_context.metric(PerfMetric::WriteDelayTime));
            self.write_pre_and_post_process_nanos
                .with_label_values(&[db_name])
                .inc_by(perf_context.metric(PerfMetric::WritePreAndPostProcessTime));
            self.write_db_mutex_lock_nanos
                .with_label_values(&[db_name])
                .inc_by(perf_context.metric(PerfMetric::DbMutexLockNanos));
            self.write_db_condition_wait_nanos
                .with_label_values(&[db_name])
                .inc_by(perf_context.metric(PerfMetric::DbConditionWaitNanos));
            self.write_key_lock_wait_nanos
                .with_label_values(&[db_name])
                .inc_by(perf_context.metric(PerfMetric::KeyLockWaitTime));
            self.write_key_lock_wait_count
                .with_label_values(&[db_name])
                .inc_by(perf_context.metric(PerfMetric::KeyLockWaitCount));
        });
    }
}

#[derive(Debug)]
/// The metrics for the database
pub struct DBMetrics {
    /// The metrics for the operations
    pub op_metrics: OperationMetrics,
    /// The metrics for the column families
    pub cf_metrics: ColumnFamilyMetrics,
    /// The metrics for the read performance
    pub read_perf_ctx_metrics: ReadPerfContextMetrics,
    /// The metrics for the write performance
    pub write_perf_ctx_metrics: WritePerfContextMetrics,
}

static ONCE: OnceCell<Arc<DBMetrics>> = OnceCell::new();

impl DBMetrics {
    fn new(registry: &Registry) -> Self {
        DBMetrics {
            op_metrics: OperationMetrics::new(registry),
            cf_metrics: ColumnFamilyMetrics::new(registry),
            read_perf_ctx_metrics: ReadPerfContextMetrics::new(registry),
            write_perf_ctx_metrics: WritePerfContextMetrics::new(registry),
        }
    }
    /// Initialize the DBMetrics instance
    pub fn init(registry: &Registry) -> &'static Arc<DBMetrics> {
        // Initialize this before creating any instance of DBMap
        // TODO: Remove static initialization because this basically means we can
        // only ever initialize db metrics once with a registry whereas
        // in the code we might want to initialize it with different
        // registries. The problem is underlying metrics cannot be re-initialized
        // or prometheus complains. We essentially need to pass in DBMetrics
        // everywhere we create DBMap as the right fix
        let _ = ONCE
            .set(Arc::new(DBMetrics::new(registry)))
            // this happens many times during tests
            .tap_err(|_| tracing::warn!("DBMetrics registry overwritten"));
        ONCE.get().unwrap()
    }
    /// Increment the number of active databases
    pub fn increment_num_active_dbs(&self, db_name: &str) {
        self.op_metrics
            .rocksdb_num_active_db_handles
            .with_label_values(&[db_name])
            .inc();
    }
    /// Decrement the number of active databases
    pub fn decrement_num_active_dbs(&self, db_name: &str) {
        self.op_metrics
            .rocksdb_num_active_db_handles
            .with_label_values(&[db_name])
            .dec();
    }
    /// Get the DBMetrics instance
    pub fn get() -> &'static Arc<DBMetrics> {
        ONCE.get()
            .unwrap_or_else(|| DBMetrics::init(prometheus::default_registry()))
    }
}
