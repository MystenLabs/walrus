// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use core::fmt;

use rocksdb::{Cache, DBCompressionType, Options, statistics::StatsLevel};
use serde::{Deserialize, Serialize};
use typed_store::rocks::get_block_options;

/// Configuration for shared block cache mode.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct SharedBlockCacheConfig {
    /// Total size of the shared block cache in bytes.
    pub cache_size: usize,
}

/// Options for configuring a column family.
/// One option object can be mapped to a specific RocksDB column family option used to create and
/// open a RocksDB column family.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
#[serde(default)]
pub struct DatabaseTableOptions {
    /// Set it to true to enable key-value separation.
    #[serde(skip_serializing_if = "Option::is_none")]
    enable_blob_files: Option<bool>,
    /// Values at or above this threshold will be written to blob files during flush or compaction.
    #[serde(skip_serializing_if = "Option::is_none")]
    min_blob_size: Option<u64>,
    /// The size limit for blob files.
    #[serde(skip_serializing_if = "Option::is_none")]
    blob_file_size: Option<u64>,
    /// The compression type to use for blob files.
    /// All blobs in the same file are compressed using the same algorithm.
    #[serde(skip_serializing_if = "Option::is_none")]
    blob_compression_type: Option<String>,
    /// Set this to true to make BlobDB actively relocate valid blobs from the oldest
    /// blob files as they are encountered during compaction.
    #[serde(skip_serializing_if = "Option::is_none")]
    enable_blob_garbage_collection: Option<bool>,
    /// The cutoff that the GC logic uses to determine which blob files should be considered "old."
    #[serde(skip_serializing_if = "Option::is_none")]
    blob_garbage_collection_age_cutoff: Option<f64>,
    /// If the ratio of garbage in the oldest blob files exceeds this threshold, targeted
    /// compactions are scheduled in order to force garbage collecting the blob files in question,
    /// assuming they are all eligible based on the value of blob_garbage_collection_age_cutoff
    /// above. This can help reduce space amplification in the case of skewed workloads where the
    /// affected files would not otherwise be picked up for compaction.
    #[serde(skip_serializing_if = "Option::is_none")]
    blob_garbage_collection_force_threshold: Option<f64>,
    /// When set, BlobDB will prefetch data from blob files in chunks of the configured size during
    /// compaction.
    #[serde(skip_serializing_if = "Option::is_none")]
    blob_compaction_read_ahead_size: Option<u64>,
    /// Size of the write buffer in bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    write_buffer_size: Option<usize>,
    /// The target file size for level-1 files in bytes.
    /// Per https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide, this is recommended to be
    /// at least the same as write_buffer_size.
    #[serde(skip_serializing_if = "Option::is_none")]
    target_file_size_base: Option<u64>,
    /// The maximum total data size for level 1 in bytes.
    /// Per https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide, this is recommended to be
    /// 10 times of target_file_size_base.
    #[serde(skip_serializing_if = "Option::is_none")]
    max_bytes_for_level_base: Option<u64>,
    /// Block cache size in bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    block_cache_size: Option<usize>,
    /// Block size in bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    block_size: Option<usize>,
    /// Pin l0 filter and index blocks in block cache.
    #[serde(skip_serializing_if = "Option::is_none")]
    pin_l0_filter_and_index_blocks_in_block_cache: Option<bool>,
    /// The soft pending compaction bytes limit. When pending compaction bytes exceed this limit,
    /// write rate will be throttled.
    #[serde(skip_serializing_if = "Option::is_none")]
    soft_pending_compaction_bytes_limit: Option<usize>,
    /// The hard pending compaction bytes limit. When pending compaction bytes exceed this limit,
    /// write will be stopped.
    #[serde(skip_serializing_if = "Option::is_none")]
    hard_pending_compaction_bytes_limit: Option<usize>,
}

/// DatabaseTableOptions specifies 4 generally column family options for different tables to adopt.
/// They are the basic template for each column family type, and can be overridden in
/// DatabaseConfig.
impl DatabaseTableOptions {
    /// The standard options are applied to most of the column families.
    fn standard() -> Self {
        Self {
            enable_blob_files: Some(false),
            min_blob_size: Some(0),
            blob_file_size: Some(0),
            blob_compression_type: Some("none".to_string()),
            enable_blob_garbage_collection: Some(false),
            blob_garbage_collection_age_cutoff: Some(0.0),
            blob_garbage_collection_force_threshold: Some(0.0),
            blob_compaction_read_ahead_size: Some(0),
            write_buffer_size: Some(64 << 20),         // 64 MB,
            target_file_size_base: Some(64 << 20),     // 64 MB,
            max_bytes_for_level_base: Some(512 << 20), // 512 MB,
            block_cache_size: Some(256 << 20),         // 256 MB,
            block_size: Some(64 << 10),                // 64 KiB,
            pin_l0_filter_and_index_blocks_in_block_cache: Some(true),
            soft_pending_compaction_bytes_limit: None,
            hard_pending_compaction_bytes_limit: None,
        }
    }

    /// The optimized_for_blobs options are applied to column families that store blobs.
    fn optimized_for_blobs() -> Self {
        // - Use blob mode
        // - Large block cache size
        Self {
            enable_blob_files: Some(true),
            min_blob_size: Some(1 << 20),
            blob_file_size: Some(1 << 28),
            blob_compression_type: Some("zstd".to_string()),
            enable_blob_garbage_collection: Some(true),
            blob_garbage_collection_age_cutoff: Some(0.5),
            blob_garbage_collection_force_threshold: Some(0.5),
            blob_compaction_read_ahead_size: Some(10 << 20),
            write_buffer_size: Some(256 << 20),      // 256 MB,
            target_file_size_base: Some(256 << 20),  // 256 MB,
            max_bytes_for_level_base: Some(2 << 30), // 2 GB,
            block_cache_size: Some(1 << 30),         // 1 GB,
            block_size: Some(64 << 10),              // 64 KiB,
            ..Default::default()
        }
        .inherit_from(Self::standard())
    }

    /// Used by metadata column family.
    /// Metadata column family by far is the most frequently accessed column family and also
    /// stores the most data.
    fn metadata() -> Self {
        Self {
            write_buffer_size: Some(512 << 20),      // 512 MB,
            target_file_size_base: Some(512 << 20),  // 512 MB,
            max_bytes_for_level_base: Some(5 << 30), // 5 GB,
            block_cache_size: Some(512 << 20),       // 512 MB,
            block_size: Some(64 << 10),              // 64 KiB,
            soft_pending_compaction_bytes_limit: None,
            // TODO(WAL-840): decide whether we want to keep this option even after all the nodes
            // applied RocksDB 0.22.0, or apply it to all column families.
            hard_pending_compaction_bytes_limit: Some(0), // Disable write stall.
            ..Default::default()
        }
        .inherit_from(Self::standard())
    }

    /// Used by blob_info and per_object_blob_info column families.
    fn blob_info_template() -> Self {
        Self {
            block_cache_size: Some(512 << 20),
            ..Default::default()
        }
        .inherit_from(Self::standard())
    }

    /// Inherit from the default options. If a field is None, use the value from the
    /// default_override.
    pub fn inherit_from(&self, default_override: DatabaseTableOptions) -> DatabaseTableOptions {
        // for all fields that are None, use the value from the default_override
        DatabaseTableOptions {
            enable_blob_files: self
                .enable_blob_files
                .or(default_override.enable_blob_files),
            min_blob_size: self.min_blob_size.or(default_override.min_blob_size),
            blob_file_size: self.blob_file_size.or(default_override.blob_file_size),
            blob_compression_type: self
                .blob_compression_type
                .as_ref()
                .cloned()
                .or(default_override.blob_compression_type),
            enable_blob_garbage_collection: self
                .enable_blob_garbage_collection
                .or(default_override.enable_blob_garbage_collection),
            blob_garbage_collection_age_cutoff: self
                .blob_garbage_collection_age_cutoff
                .or(default_override.blob_garbage_collection_age_cutoff),
            blob_garbage_collection_force_threshold: self
                .blob_garbage_collection_force_threshold
                .or(default_override.blob_garbage_collection_force_threshold),
            blob_compaction_read_ahead_size: self
                .blob_compaction_read_ahead_size
                .or(default_override.blob_compaction_read_ahead_size),
            write_buffer_size: self
                .write_buffer_size
                .or(default_override.write_buffer_size),
            target_file_size_base: self
                .target_file_size_base
                .or(default_override.target_file_size_base),
            max_bytes_for_level_base: self
                .max_bytes_for_level_base
                .or(default_override.max_bytes_for_level_base),
            block_cache_size: self.block_cache_size.or(default_override.block_cache_size),
            block_size: self.block_size.or(default_override.block_size),
            pin_l0_filter_and_index_blocks_in_block_cache: self
                .pin_l0_filter_and_index_blocks_in_block_cache
                .or(default_override.pin_l0_filter_and_index_blocks_in_block_cache),
            soft_pending_compaction_bytes_limit: self
                .soft_pending_compaction_bytes_limit
                .or(default_override.soft_pending_compaction_bytes_limit),
            hard_pending_compaction_bytes_limit: self
                .hard_pending_compaction_bytes_limit
                .or(default_override.hard_pending_compaction_bytes_limit),
        }
    }
}

/// RocksDB options applied to the overall database.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct GlobalDatabaseOptions {
    /// The maximum number of open files
    pub max_open_files: Option<i32>,
    /// The maximum total size for all WALs in bytes.
    pub max_total_wal_size: Option<u64>,
    /// The number of log files to keep.
    pub keep_log_file_num: Option<usize>,
    /// Below two options are only control the behavior of archived WALs.
    /// The TTL for the WAL in seconds.
    pub wal_ttl_seconds: Option<u64>,
    /// The size limit for the WAL in MB.
    pub wal_size_limit_mb: Option<u64>,
    /// Whether to enable statistics.
    pub enable_statistics: bool,
}

impl Default for GlobalDatabaseOptions {
    fn default() -> Self {
        Self {
            max_open_files: Some(512_000),
            max_total_wal_size: Some(10 * 1024 * 1024 * 1024), // 10 GB,
            keep_log_file_num: Some(50),
            wal_ttl_seconds: Some(60 * 60 * 24 * 2), // 2 days,
            wal_size_limit_mb: Some(10 * 1024),      // 10 GB,
            enable_statistics: false,
        }
    }
}

impl From<&GlobalDatabaseOptions> for Options {
    fn from(value: &GlobalDatabaseOptions) -> Self {
        let mut options = Options::default();

        if let Some(max_files) = value.max_open_files {
            options.set_max_open_files(max_files);
        }

        if let Some(max_total_wal_size) = value.max_total_wal_size {
            options.set_max_total_wal_size(max_total_wal_size);
        }

        if let Some(keep_log_file_num) = value.keep_log_file_num {
            options.set_keep_log_file_num(keep_log_file_num);
        }

        if let Some(wal_ttl_seconds) = value.wal_ttl_seconds {
            options.set_wal_ttl_seconds(wal_ttl_seconds);
        }

        if let Some(wal_size_limit_mb) = value.wal_size_limit_mb {
            options.set_wal_size_limit_mb(wal_size_limit_mb);
        }

        if value.enable_statistics {
            options.enable_statistics();
            options.set_statistics_level(StatsLevel::ExceptHistogramOrTimers);
        }

        options
    }
}

/// Database configuration for Walrus storage nodes.
///
/// There are 4 template options whose default values are specified in the templates in
/// DatabaseTableOptions.
///  - standard
///  - optimized_for_blobs
///  - blob_info_template
///  - metadata
///
/// Any partial options specified in the DatabaseConfig will override the corresponding option in
/// the template options. If not specified in the DatabaseConfig, the template options will be
/// inherited.
///
/// Each column family option is inherited from one of the templates. If any option is specified in
/// the DatabaseConfig, that option will override the template option. Any option not specified in
/// the DatabaseConfig will be inherited from the template option.
#[serde_with::serde_as]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(default)]
pub struct DatabaseConfig {
    /// Global database options. Applied when opening/creating the database.
    pub(super) global: GlobalDatabaseOptions,

    /// Below options overrides the template options.
    ///
    /// Default database table options used by most column families.
    pub(super) standard: DatabaseTableOptions,
    /// Database table options applied to sliver column families.
    pub(super) optimized_for_blobs: DatabaseTableOptions,
    /// Default database table options used by blob info tables.
    pub(super) blob_info_template: DatabaseTableOptions,
    /// Default database table options used by metadata column family.
    pub(super) metadata: DatabaseTableOptions,

    /// Below options overrides specific column families' options.
    ///
    /// Node status database options.
    pub(super) node_status: Option<DatabaseTableOptions>,
    /// Blob info database options.
    pub(super) blob_info: Option<DatabaseTableOptions>,
    /// Per object blob info database options.
    pub(super) per_object_blob_info: Option<DatabaseTableOptions>,
    /// Event cursor database options.
    pub(super) event_cursor: Option<DatabaseTableOptions>,
    /// Shard database options.
    pub(super) shard: Option<DatabaseTableOptions>,
    /// Shard status database options.
    pub(super) shard_status: Option<DatabaseTableOptions>,
    /// Shard sync progress database options.
    pub(super) shard_sync_progress: Option<DatabaseTableOptions>,
    /// Pending recover slivers database options.
    pub(super) pending_recover_slivers: Option<DatabaseTableOptions>,
    /// Event blob writer certified options.
    pub(super) certified: Option<DatabaseTableOptions>,
    /// Event blob writer pending options.
    pub(super) pending: Option<DatabaseTableOptions>,
    /// Event blob writer attested options.
    pub(super) attested: Option<DatabaseTableOptions>,
    /// Event blob writer failed to attest options.
    pub(super) failed_to_attest: Option<DatabaseTableOptions>,
    /// Checkpoint store database options.
    pub(super) checkpoint_store: Option<DatabaseTableOptions>,
    /// Walrus package store database options.
    pub(super) walrus_package_store: Option<DatabaseTableOptions>,
    /// Committee store database options.
    pub(super) committee_store: Option<DatabaseTableOptions>,
    /// Event store database options.
    pub(super) event_store: Option<DatabaseTableOptions>,
    /// Init state store database options.
    pub(super) init_state: Option<DatabaseTableOptions>,

    /// Shared block cache configuration. When present, all column families share a single block
    /// cache. When None (default), each column family has its own block cache.
    pub(super) shared_block_cache_config: Option<SharedBlockCacheConfig>,
}

impl DatabaseConfig {
    /// Returns the global database options.
    pub fn global(&self) -> GlobalDatabaseOptions {
        self.global.clone()
    }

    fn standard(&self) -> DatabaseTableOptions {
        self.standard
            .clone()
            .inherit_from(DatabaseTableOptions::standard())
    }

    fn optimized_for_blobs(&self) -> DatabaseTableOptions {
        self.optimized_for_blobs
            .clone()
            .inherit_from(DatabaseTableOptions::optimized_for_blobs())
    }

    fn blob_info_template(&self) -> DatabaseTableOptions {
        self.blob_info_template
            .clone()
            .inherit_from(DatabaseTableOptions::blob_info_template())
    }

    /// Uses the template if the options are None, otherwise inherits from the template to set all
    /// the None fields in `options`.
    fn inherit_from_or_use_template(
        options: &Option<DatabaseTableOptions>,
        template: DatabaseTableOptions,
    ) -> DatabaseTableOptions {
        match options {
            Some(options) => options.inherit_from(template),
            None => template,
        }
    }

    /// Returns the node status database option.
    pub fn node_status(&self) -> DatabaseTableOptions {
        Self::inherit_from_or_use_template(&self.node_status, self.standard())
    }

    /// Returns the metadata database option.
    pub fn metadata(&self) -> DatabaseTableOptions {
        self.metadata
            .clone()
            .inherit_from(DatabaseTableOptions::metadata())
    }

    /// Returns the blob info database option.
    pub fn blob_info(&self) -> DatabaseTableOptions {
        Self::inherit_from_or_use_template(&self.blob_info, self.blob_info_template())
    }

    /// Returns the per object blob info database option.
    pub fn per_object_blob_info(&self) -> DatabaseTableOptions {
        Self::inherit_from_or_use_template(&self.per_object_blob_info, self.blob_info_template())
    }

    /// Returns the event cursor database option.
    pub fn event_cursor(&self) -> DatabaseTableOptions {
        Self::inherit_from_or_use_template(&self.event_cursor, self.standard())
    }

    /// Returns the shard database option.
    pub fn shard(&self) -> DatabaseTableOptions {
        Self::inherit_from_or_use_template(&self.shard, self.optimized_for_blobs())
    }

    /// Returns the shard status database option.
    pub fn shard_status(&self) -> DatabaseTableOptions {
        Self::inherit_from_or_use_template(&self.shard_status, self.standard())
    }

    /// Returns the shard sync progress database option.
    pub fn shard_sync_progress(&self) -> DatabaseTableOptions {
        Self::inherit_from_or_use_template(&self.shard_sync_progress, self.standard())
    }

    /// Returns the pending recover slivers database option.
    pub fn pending_recover_slivers(&self) -> DatabaseTableOptions {
        Self::inherit_from_or_use_template(&self.pending_recover_slivers, self.standard())
    }

    /// Returns the event blob writer certified database option.
    pub fn certified(&self) -> DatabaseTableOptions {
        Self::inherit_from_or_use_template(&self.certified, self.standard())
    }

    /// Returns the event blob writer pending database option.
    pub fn pending(&self) -> DatabaseTableOptions {
        Self::inherit_from_or_use_template(&self.pending, self.standard())
    }

    /// Returns the event blob writer attested database option.
    pub fn attested(&self) -> DatabaseTableOptions {
        Self::inherit_from_or_use_template(&self.attested, self.standard())
    }

    /// Returns the event blob writer failed to attest database option.
    pub fn failed_to_attest(&self) -> DatabaseTableOptions {
        Self::inherit_from_or_use_template(&self.failed_to_attest, self.standard())
    }

    /// Returns the checkpoint store database option.
    pub fn checkpoint_store(&self) -> DatabaseTableOptions {
        Self::inherit_from_or_use_template(&self.checkpoint_store, self.standard())
    }

    /// Returns the walrus package store database option.
    pub fn walrus_package_store(&self) -> DatabaseTableOptions {
        Self::inherit_from_or_use_template(&self.walrus_package_store, self.standard())
    }

    /// Returns the committee store database option.
    pub fn committee_store(&self) -> DatabaseTableOptions {
        Self::inherit_from_or_use_template(&self.committee_store, self.standard())
    }

    /// Returns the event store database option.
    pub fn event_store(&self) -> DatabaseTableOptions {
        Self::inherit_from_or_use_template(&self.event_store, self.standard())
    }

    /// Returns the init state store database option.
    pub fn init_state(&self) -> DatabaseTableOptions {
        Self::inherit_from_or_use_template(&self.init_state, self.standard())
    }

    /// Returns true if shared block cache mode is enabled.
    pub fn use_shared_block_cache(&self) -> bool {
        self.shared_block_cache_config.is_some()
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            global: GlobalDatabaseOptions::default(),
            standard: DatabaseTableOptions::standard(),
            optimized_for_blobs: DatabaseTableOptions::optimized_for_blobs(),
            blob_info_template: DatabaseTableOptions::blob_info_template(),
            metadata: DatabaseTableOptions::metadata(),
            node_status: None,
            blob_info: None,
            per_object_blob_info: None,
            event_cursor: None,
            shard: None,
            shard_status: None,
            shard_sync_progress: None,
            pending_recover_slivers: None,
            certified: None,
            pending: None,
            attested: None,
            failed_to_attest: None,
            checkpoint_store: None,
            walrus_package_store: None,
            committee_store: None,
            event_store: None,
            init_state: None,
            shared_block_cache_config: None,
        }
    }
}

/// A factory for creating RocksDB Options from DatabaseTableOptions with a shared block cache.
/// This ensures all column families share the same block cache instance.
#[derive(Clone)]
pub struct DatabaseTableOptionsFactory {
    /// The shared block cache instance for all column families.
    block_cache: Option<Cache>,
    /// The database configuration.
    config: DatabaseConfig,
}

impl DatabaseTableOptionsFactory {
    /// Creates a new DatabaseTableOptionsFactory with a shared block cache.
    pub fn new(config: DatabaseConfig) -> Self {
        let block_cache =
            config
                .shared_block_cache_config
                .as_ref()
                .map(|shared_block_cache_config| {
                    Cache::new_lru_cache(shared_block_cache_config.cache_size)
                });

        Self {
            block_cache,
            config,
        }
    }

    /// Converts a DatabaseTableOptions to a RocksDB Options object with the shared block cache.
    fn to_options(&self, table_options: &DatabaseTableOptions) -> Options {
        let mut options = Options::default();
        if let Some(enable_blob_files) = table_options.enable_blob_files {
            options.set_enable_blob_files(enable_blob_files);
        }
        if let Some(min_blob_size) = table_options.min_blob_size {
            options.set_min_blob_size(min_blob_size);
        }
        if let Some(blob_file_size) = table_options.blob_file_size {
            options.set_blob_file_size(blob_file_size);
        }
        if let Some(blob_compression_type) = &table_options.blob_compression_type {
            let compression_type = match blob_compression_type.as_str() {
                "none" => DBCompressionType::None,
                "snappy" => DBCompressionType::Snappy,
                "zlib" => DBCompressionType::Zlib,
                "lz4" => DBCompressionType::Lz4,
                "lz4hc" => DBCompressionType::Lz4hc,
                "zstd" => DBCompressionType::Zstd,
                _ => DBCompressionType::None,
            };
            options.set_blob_compression_type(compression_type);
        }
        if let Some(enable_blob_garbage_collection) = table_options.enable_blob_garbage_collection {
            options.set_enable_blob_gc(enable_blob_garbage_collection);
        }
        if let Some(blob_garbage_collection_age_cutoff) =
            table_options.blob_garbage_collection_age_cutoff
        {
            options.set_blob_gc_age_cutoff(blob_garbage_collection_age_cutoff);
        }
        if let Some(blob_garbage_collection_force_threshold) =
            table_options.blob_garbage_collection_force_threshold
        {
            options.set_blob_gc_force_threshold(blob_garbage_collection_force_threshold);
        }
        if let Some(blob_compaction_read_ahead_size) = table_options.blob_compaction_read_ahead_size
        {
            options.set_blob_compaction_readahead_size(blob_compaction_read_ahead_size);
        }
        if let Some(write_buffer_size) = table_options.write_buffer_size {
            options.set_write_buffer_size(write_buffer_size);
        }
        if let Some(target_file_size_base) = table_options.target_file_size_base {
            options.set_target_file_size_base(target_file_size_base);
        }
        if let Some(max_bytes_for_level_base) = table_options.max_bytes_for_level_base {
            options.set_max_bytes_for_level_base(max_bytes_for_level_base);
        }
        if let Some(block_cache_size) = table_options.block_cache_size {
            let block_cache = if let Some(block_cache) = &self.block_cache {
                block_cache.clone()
            } else {
                Cache::new_lru_cache(block_cache_size)
            };
            let block_based_options = get_block_options(
                &block_cache,
                table_options.block_size,
                table_options.pin_l0_filter_and_index_blocks_in_block_cache,
            );
            options.set_block_based_table_factory(&block_based_options);
        }
        if let Some(soft_pending_compaction_bytes_limit) =
            table_options.soft_pending_compaction_bytes_limit
        {
            options.set_soft_pending_compaction_bytes_limit(soft_pending_compaction_bytes_limit);
        }
        if let Some(hard_pending_compaction_bytes_limit) =
            table_options.hard_pending_compaction_bytes_limit
        {
            options.set_hard_pending_compaction_bytes_limit(hard_pending_compaction_bytes_limit);
        }
        options
    }

    /// Returns the global database options.
    pub fn global(&self) -> GlobalDatabaseOptions {
        self.config.global()
    }

    /// Returns the standard (default) database option.
    pub fn standard(&self) -> Options {
        self.to_options(&self.config.standard())
    }

    /// Returns the node status database option with shared cache.
    pub fn node_status(&self) -> Options {
        self.to_options(&self.config.node_status())
    }

    /// Returns the metadata database option with shared cache.
    pub fn metadata(&self) -> Options {
        self.to_options(&self.config.metadata())
    }

    /// Returns the blob info database option with shared cache.
    pub fn blob_info(&self) -> Options {
        self.to_options(&self.config.blob_info())
    }

    /// Returns the per object blob info database option with shared cache.
    pub fn per_object_blob_info(&self) -> Options {
        self.to_options(&self.config.per_object_blob_info())
    }

    /// Returns the event cursor database option with shared cache.
    pub fn event_cursor(&self) -> Options {
        self.to_options(&self.config.event_cursor())
    }

    /// Returns the shard database option with shared cache.
    pub fn shard(&self) -> Options {
        self.to_options(&self.config.shard())
    }

    /// Returns the shard status database option with shared cache.
    pub fn shard_status(&self) -> Options {
        self.to_options(&self.config.shard_status())
    }

    /// Returns the shard sync progress database option with shared cache.
    pub fn shard_sync_progress(&self) -> Options {
        self.to_options(&self.config.shard_sync_progress())
    }

    /// Returns the pending recover slivers database option with shared cache.
    pub fn pending_recover_slivers(&self) -> Options {
        self.to_options(&self.config.pending_recover_slivers())
    }

    /// Returns the event blob writer certified database option with shared cache.
    pub fn certified(&self) -> Options {
        self.to_options(&self.config.certified())
    }

    /// Returns the event blob writer pending database option with shared cache.
    pub fn pending(&self) -> Options {
        self.to_options(&self.config.pending())
    }

    /// Returns the event blob writer attested database option with shared cache.
    pub fn attested(&self) -> Options {
        self.to_options(&self.config.attested())
    }

    /// Returns the event blob writer failed to attest database option with shared cache.
    pub fn failed_to_attest(&self) -> Options {
        self.to_options(&self.config.failed_to_attest())
    }

    /// Returns the checkpoint store database option with shared cache.
    pub fn checkpoint_store(&self) -> Options {
        self.to_options(&self.config.checkpoint_store())
    }

    /// Returns the walrus package store database option with shared cache.
    pub fn walrus_package_store(&self) -> Options {
        self.to_options(&self.config.walrus_package_store())
    }

    /// Returns the committee store database option with shared cache.
    pub fn committee_store(&self) -> Options {
        self.to_options(&self.config.committee_store())
    }

    /// Returns the event store database option with shared cache.
    pub fn event_store(&self) -> Options {
        self.to_options(&self.config.event_store())
    }

    /// Returns the init state store database option with shared cache.
    pub fn init_state(&self) -> Options {
        self.to_options(&self.config.init_state())
    }
}

impl fmt::Debug for DatabaseTableOptionsFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DatabaseTableOptionsFactory")?;
        // write debug info for the config
        write!(f, "config: {:?}", self.config)?;
        // write debug info for the block cache
        write!(f, "block_cache set: {:?}", self.block_cache.is_some())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use indoc::indoc;
    use walrus_test_utils::Result as TestResult;

    use super::*;

    #[test]
    fn test_optimized_for_blobs_database_config() -> TestResult {
        let yaml = indoc! {"
            standard:
                enable_blob_files: false
                min_blob_size: 123
                blob_file_size: 2000
            optimized_for_blobs:
                enable_blob_files: true
                min_blob_size: 0
                blob_file_size: 1000
            shard:
                blob_garbage_collection_force_threshold: 0.5
        "};

        let config: DatabaseConfig = serde_yaml::from_str(yaml)?;

        let shard_options = config.shard();
        // shard inherits from optimized_for_blobs.
        assert_eq!(
            shard_options,
            DatabaseTableOptions {
                blob_garbage_collection_force_threshold: Some(0.5),
                ..config.optimized_for_blobs()
            }
        );

        let optimized_for_blobs_options = config.optimized_for_blobs();
        // optimized_for_blobs specific override.
        assert_eq!(
            optimized_for_blobs_options,
            DatabaseTableOptions {
                enable_blob_files: Some(true),
                min_blob_size: Some(0),
                blob_file_size: Some(1000),
                ..DatabaseTableOptions::optimized_for_blobs()
            }
        );

        Ok(())
    }

    #[test]
    fn test_blob_info_database_config() -> TestResult {
        let yaml = indoc! {"
            standard:
                enable_blob_garbage_collection: true
            blob_info_template:
                block_cache_size: 2000000000
            blob_info:
                block_cache_size: 1000000000
        "};

        let config: DatabaseConfig = serde_yaml::from_str(yaml)?;

        let blob_info_options = config.blob_info();
        assert_eq!(
            blob_info_options,
            DatabaseTableOptions {
                enable_blob_garbage_collection: Some(false),
                block_cache_size: Some(1000000000),
                ..DatabaseTableOptions::blob_info_template()
            }
        );
        let per_object_blob_info_options = config.per_object_blob_info();
        assert_eq!(
            per_object_blob_info_options,
            DatabaseTableOptions {
                enable_blob_garbage_collection: Some(false),
                block_cache_size: Some(2000000000),
                ..DatabaseTableOptions::blob_info_template()
            }
        );

        Ok(())
    }

    #[test]
    fn test_optimized_for_blobs_inherits_from_standard() -> TestResult {
        let optimized_for_blobs = DatabaseTableOptions::optimized_for_blobs();
        let standard = DatabaseTableOptions::standard();

        // Fields that use ..Default::default() in optimized_for_blobs should inherit from standard
        assert_eq!(
            optimized_for_blobs.pin_l0_filter_and_index_blocks_in_block_cache,
            standard.pin_l0_filter_and_index_blocks_in_block_cache
        );
        assert_eq!(
            optimized_for_blobs.soft_pending_compaction_bytes_limit,
            standard.soft_pending_compaction_bytes_limit
        );
        assert_eq!(
            optimized_for_blobs.hard_pending_compaction_bytes_limit,
            standard.hard_pending_compaction_bytes_limit
        );

        Ok(())
    }

    #[test]
    fn test_metadata_inherits_from_standard() -> TestResult {
        let metadata = DatabaseTableOptions::metadata();
        let standard = DatabaseTableOptions::standard();

        // Fields that use ..Default::default() in metadata should inherit from standard
        assert_eq!(metadata.enable_blob_files, standard.enable_blob_files);
        assert_eq!(metadata.min_blob_size, standard.min_blob_size);
        assert_eq!(metadata.blob_file_size, standard.blob_file_size);
        assert_eq!(
            metadata.blob_compression_type,
            standard.blob_compression_type
        );
        assert_eq!(
            metadata.enable_blob_garbage_collection,
            standard.enable_blob_garbage_collection
        );
        assert_eq!(
            metadata.blob_garbage_collection_age_cutoff,
            standard.blob_garbage_collection_age_cutoff
        );
        assert_eq!(
            metadata.blob_garbage_collection_force_threshold,
            standard.blob_garbage_collection_force_threshold
        );
        assert_eq!(
            metadata.blob_compaction_read_ahead_size,
            standard.blob_compaction_read_ahead_size
        );
        assert_eq!(
            metadata.pin_l0_filter_and_index_blocks_in_block_cache,
            standard.pin_l0_filter_and_index_blocks_in_block_cache
        );

        Ok(())
    }

    #[test]
    fn test_blob_info_template_inherits_from_standard() -> TestResult {
        let blob_info_template = DatabaseTableOptions::blob_info_template();
        let standard = DatabaseTableOptions::standard();

        // Only block_cache_size is explicitly set in blob_info_template, should be different from
        // standard
        assert_ne!(
            blob_info_template.block_cache_size,
            standard.block_cache_size
        );

        // All other fields that use ..Default::default() in blob_info_template should inherit from
        // standard
        assert_eq!(
            blob_info_template.enable_blob_files,
            standard.enable_blob_files
        );
        assert_eq!(blob_info_template.min_blob_size, standard.min_blob_size);
        assert_eq!(blob_info_template.blob_file_size, standard.blob_file_size);
        assert_eq!(
            blob_info_template.blob_compression_type,
            standard.blob_compression_type
        );
        assert_eq!(
            blob_info_template.enable_blob_garbage_collection,
            standard.enable_blob_garbage_collection
        );
        assert_eq!(
            blob_info_template.blob_garbage_collection_age_cutoff,
            standard.blob_garbage_collection_age_cutoff
        );
        assert_eq!(
            blob_info_template.blob_garbage_collection_force_threshold,
            standard.blob_garbage_collection_force_threshold
        );
        assert_eq!(
            blob_info_template.blob_compaction_read_ahead_size,
            standard.blob_compaction_read_ahead_size
        );
        assert_eq!(
            blob_info_template.write_buffer_size,
            standard.write_buffer_size
        );
        assert_eq!(
            blob_info_template.target_file_size_base,
            standard.target_file_size_base
        );
        assert_eq!(
            blob_info_template.max_bytes_for_level_base,
            standard.max_bytes_for_level_base
        );
        assert_eq!(blob_info_template.block_size, standard.block_size);
        assert_eq!(
            blob_info_template.pin_l0_filter_and_index_blocks_in_block_cache,
            standard.pin_l0_filter_and_index_blocks_in_block_cache
        );
        assert_eq!(
            blob_info_template.soft_pending_compaction_bytes_limit,
            standard.soft_pending_compaction_bytes_limit
        );
        assert_eq!(
            blob_info_template.hard_pending_compaction_bytes_limit,
            standard.hard_pending_compaction_bytes_limit
        );

        Ok(())
    }

    // Tests inheritance logic.
    #[test]
    fn test_database_config_inheritance() -> TestResult {
        // Test 1: Default configuration (no overrides)
        let default_config = DatabaseConfig::default();

        // Verify static method inheritance still works
        assert_eq!(default_config.standard(), DatabaseTableOptions::standard());
        assert_eq!(
            default_config.optimized_for_blobs(),
            DatabaseTableOptions::optimized_for_blobs()
        );
        assert_eq!(default_config.metadata(), DatabaseTableOptions::metadata());
        assert_eq!(
            default_config.blob_info_template(),
            DatabaseTableOptions::blob_info_template()
        );

        // Verify optional fields with static defaults
        assert_eq!(
            default_config.blob_info(),
            DatabaseTableOptions::blob_info_template()
        );
        assert_eq!(
            default_config.per_object_blob_info(),
            DatabaseTableOptions::blob_info_template()
        );

        // Verify optional fields with instance defaults (inherit from standard)
        assert_eq!(default_config.node_status(), default_config.standard());
        assert_eq!(default_config.event_cursor(), default_config.standard());
        assert_eq!(default_config.certified(), default_config.standard());

        // Verify shard inherits from optimized_for_blobs
        assert_eq!(default_config.shard(), default_config.optimized_for_blobs());

        // Test 2: Partial configuration from YAML
        let yaml = indoc! {"
            standard:
                write_buffer_size: 128000000
                block_cache_size: 500000000
            optimized_for_blobs:
                enable_blob_files: false
                write_buffer_size: 512000000
            metadata:
                write_buffer_size: 256000000
            node_status:
                block_cache_size: 100000000
            shard:
                blob_file_size: 2000000000
        "};

        let config: DatabaseConfig = serde_yaml::from_str(yaml)?;

        // Test standard field inheritance (partial config should inherit from
        // DatabaseTableOptions::standard())
        let standard = config.standard();
        assert_eq!(standard.write_buffer_size, Some(128000000)); // overridden
        assert_eq!(standard.block_cache_size, Some(500000000)); // overridden
        // inherited from DatabaseTableOptions::standard()
        assert_eq!(standard.target_file_size_base, Some(64 << 20));

        // Test optimized_for_blobs field inheritance
        let optimized = config.optimized_for_blobs();
        assert_eq!(optimized.enable_blob_files, Some(false)); // overridden
        assert_eq!(optimized.write_buffer_size, Some(512000000)); // overridden
        // inherited from DatabaseTableOptions::optimized_for_blobs()
        assert_eq!(optimized.min_blob_size, Some(1 << 20));
        assert_eq!(optimized.blob_compression_type, Some("zstd".to_string())); // inherited

        // Test metadata field inheritance (should inherit from DatabaseTableOptions::metadata())
        let metadata = config.metadata();
        assert_eq!(metadata.write_buffer_size, Some(256000000)); // overridden
        // inherited from DatabaseTableOptions::metadata()
        assert_eq!(metadata.hard_pending_compaction_bytes_limit, Some(0));

        // Test optional field inheritance from instance default (node_status inherits from
        // standard)
        let node_status = config.node_status();
        assert_eq!(node_status.block_cache_size, Some(100000000)); // overridden
        // inherited from config.standard()
        assert_eq!(node_status.write_buffer_size, Some(128000000));
        assert_eq!(node_status.target_file_size_base, Some(64 << 20)); // inherited through standard

        // Test optional field inheritance from instance default (shard inherits from
        // optimized_for_blobs)
        let shard = config.shard();
        assert_eq!(shard.blob_file_size, Some(2000000000)); // overridden
        // inherited from config.optimized_for_blobs()
        assert_eq!(shard.enable_blob_files, Some(false));
        // inherited from config.optimized_for_blobs()
        assert_eq!(shard.write_buffer_size, Some(512000000));

        // Test fields with no config should use their respective defaults
        assert_eq!(config.event_cursor(), config.standard());
        assert_eq!(config.certified(), config.standard());
        assert_eq!(
            config.blob_info(),
            DatabaseTableOptions::blob_info_template()
        );

        Ok(())
    }

    // Tests config overrides apply to the correct fields.
    #[test]
    fn test_database_config_overrides() -> TestResult {
        let yaml = indoc! {"
            standard:
                blob_compression_type: test_standard
            optimized_for_blobs:
                blob_compression_type: test_optimized_for_blobs
            blob_info_template:
                blob_compression_type: test_blob_info_template
            blob_info:
                blob_compression_type: test_blob_info
            metadata:
                blob_compression_type: test_metadata
            node_status:
                blob_compression_type: test_node_status
        "};

        let config: DatabaseConfig = serde_yaml::from_str(yaml)?;

        // standard specific override.
        assert_eq!(
            config.standard().blob_compression_type,
            Some("test_standard".to_string())
        );
        // optimized_for_blobs specific override.
        assert_eq!(
            config.optimized_for_blobs().blob_compression_type,
            Some("test_optimized_for_blobs".to_string())
        );
        // blob_info specific override.
        assert_eq!(
            config.blob_info().blob_compression_type,
            Some("test_blob_info".to_string())
        );
        // per_object_blob_info inherits from blob_info_template.
        assert_eq!(
            config.per_object_blob_info().blob_compression_type,
            Some("test_blob_info_template".to_string())
        );
        // metadata inherits from standard.
        assert_eq!(
            config.metadata().blob_compression_type,
            Some("test_metadata".to_string())
        );
        // node_status inherits from standard.
        assert_eq!(
            config.node_status().blob_compression_type,
            Some("test_node_status".to_string())
        );
        // event_cursor inherits from standard.
        assert_eq!(
            config.event_cursor().blob_compression_type,
            Some("test_standard".to_string())
        );
        // shard inherits from optimized_for_blobs.
        assert_eq!(
            config.shard().blob_compression_type,
            Some("test_optimized_for_blobs".to_string())
        );

        Ok(())
    }
}
