// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use rocksdb::{DBCompressionType, Options};
use serde::{Deserialize, Serialize};
use typed_store::rocks::get_block_options;

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

// DatabaseTableOptions specifies 4 generally column family options for different tables to adopt.
// They are the basic template for each column family type, and can be overridden in DatabaseConfig
// before building column family options in DatabaseTableOptionsFactory.
impl DatabaseTableOptions {
    /// Used by all tables except for slivers, metadata, and blob_info column families.
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

    /// Used by sliver column families.
    fn optimized_for_blobs_template() -> Self {
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
            pin_l0_filter_and_index_blocks_in_block_cache: Some(true),
            soft_pending_compaction_bytes_limit: None,
            hard_pending_compaction_bytes_limit: None,
        }
    }

    /// Used by metadata column family.
    /// Metadata column family by far is the most frequently accessed column family and also
    /// stores the most data.
    fn metadata_template() -> Self {
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

    /// Inherit from the `default_override` options. If a field in `self` is None, use the value
    /// from the `default_override`.
    pub fn inherit_from(&self, default_override: DatabaseTableOptions) -> DatabaseTableOptions {
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

    /// Converts the DatabaseTableOptions to a RocksDB Options object.
    pub fn to_options(&self) -> Options {
        let mut options = Options::default();
        if let Some(enable_blob_files) = self.enable_blob_files {
            options.set_enable_blob_files(enable_blob_files);
        }
        if let Some(min_blob_size) = self.min_blob_size {
            options.set_min_blob_size(min_blob_size);
        }
        if let Some(blob_file_size) = self.blob_file_size {
            options.set_blob_file_size(blob_file_size);
        }
        if let Some(blob_compression_type) = &self.blob_compression_type {
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
        if let Some(enable_blob_garbage_collection) = self.enable_blob_garbage_collection {
            options.set_enable_blob_gc(enable_blob_garbage_collection);
        }
        if let Some(blob_garbage_collection_age_cutoff) = self.blob_garbage_collection_age_cutoff {
            options.set_blob_gc_age_cutoff(blob_garbage_collection_age_cutoff);
        }
        if let Some(blob_garbage_collection_force_threshold) =
            self.blob_garbage_collection_force_threshold
        {
            options.set_blob_gc_force_threshold(blob_garbage_collection_force_threshold);
        }
        if let Some(blob_compaction_read_ahead_size) = self.blob_compaction_read_ahead_size {
            options.set_blob_compaction_readahead_size(blob_compaction_read_ahead_size);
        }
        if let Some(write_buffer_size) = self.write_buffer_size {
            options.set_write_buffer_size(write_buffer_size);
        }
        if let Some(target_file_size_base) = self.target_file_size_base {
            options.set_target_file_size_base(target_file_size_base);
        }
        if let Some(max_bytes_for_level_base) = self.max_bytes_for_level_base {
            options.set_max_bytes_for_level_base(max_bytes_for_level_base);
        }
        if let Some(block_cache_size) = self.block_cache_size {
            let block_based_options = get_block_options(
                block_cache_size,
                self.block_size,
                self.pin_l0_filter_and_index_blocks_in_block_cache,
            );
            options.set_block_based_table_factory(&block_based_options);
        }
        if let Some(soft_pending_compaction_bytes_limit) = self.soft_pending_compaction_bytes_limit
        {
            options.set_soft_pending_compaction_bytes_limit(soft_pending_compaction_bytes_limit);
        }
        if let Some(hard_pending_compaction_bytes_limit) = self.hard_pending_compaction_bytes_limit
        {
            options.set_hard_pending_compaction_bytes_limit(hard_pending_compaction_bytes_limit);
        }
        options
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
}

impl Default for GlobalDatabaseOptions {
    fn default() -> Self {
        Self {
            max_open_files: Some(512_000),
            max_total_wal_size: Some(10 * 1024 * 1024 * 1024), // 10 GB,
            keep_log_file_num: Some(50),
            wal_ttl_seconds: Some(60 * 60 * 24 * 2), // 2 days,
            wal_size_limit_mb: Some(10 * 1024),      // 10 GB,
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

        options
    }
}

/// Database configuration for Walrus storage nodes. Note that any populated fields in this struct
/// will only override the same field in the default options in `DatabaseTableOptionsFactory`.
///
/// The `standard` options are applied to all tables except for slivers and metadata. The
/// `optimized_for_blobs` options are applied to sliver tables. The `metadata` options are applied
/// to metadata tables. The `blob_info` options are applied to blob info tables.
///
/// Options for all individual tables can be set as well through the `node_status`, `metadata`,
/// `blob_info`, `per_object_blob_info`, `event_cursor`, `shard`, `shard_status`,
/// `shard_sync_progress`, and `pending_recover_slivers` fields.
///
/// For any `DatabaseTableOptions`, they have a default options to inherit from. For any partial
/// options, unset fields will inherit from the default options.
///
/// Note that when changing `standard` or `optimized_for_blobs` options, the changes will affect any
/// existing column families that inherit from them.
///
/// Example 1: if `block_cache_size` is set in `standard` field, it will only override the
/// `block_cache_size` in the default `standard` options in `DatabaseTableOptionsFactory`. All
/// other fields in `standard` will inherit from the default `standard` options. All the column
/// families who is based on `standard` will inherit from the overridden `standard` options.
///
/// Example 2: if `block_cache_size` is set in `node_status` field, it will only override the
/// `block_cache_size` in the default `node_status` options in `DatabaseTableOptionsFactory`. All
/// other fields in `node_status` will inherit from the default `node_status` options.
#[serde_with::serde_as]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(default)]
pub struct DatabaseConfig {
    /// Global database options.
    pub(super) global: GlobalDatabaseOptions,

    /// Below are the column family option templates. Any specific column family can adopt one of
    /// these templates.
    ///
    /// Default database table options used by all tables except for slivers, metadata, and blob
    /// info.
    pub(super) standard: DatabaseTableOptions,
    /// Database table options applied to sliver tables.
    pub(super) optimized_for_blobs: DatabaseTableOptions,
    /// Default metadata database table options.
    pub(super) metadata_template: DatabaseTableOptions,
    /// Default blob info database table options.
    pub(super) blob_info_template: DatabaseTableOptions,

    /// Below are the column family option overrides specific to a single column family.
    ///
    /// Node status database options.
    pub(super) node_status: Option<DatabaseTableOptions>,
    /// Metadata database options.
    pub(super) metadata: Option<DatabaseTableOptions>,
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
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            global: GlobalDatabaseOptions::default(),
            standard: DatabaseTableOptions::standard(),
            optimized_for_blobs: DatabaseTableOptions::optimized_for_blobs_template(),
            metadata_template: DatabaseTableOptions::metadata_template(),
            blob_info_template: DatabaseTableOptions::blob_info_template(),
            node_status: None,
            metadata: None,
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
        }
    }
}

/// Database table options factory for the Walrus node.
///
/// This factory is used to create a `DatabaseTableOptions` object for each column family based on
/// the `DatabaseConfig` and the default options in `DatabaseTableOptionsFactory`.
#[derive(Debug, Clone, PartialEq)]
pub struct DatabaseTableOptionsFactory {
    /// Holds the database config from the node config. Any config specifically set in this struct
    /// will only override the same field of the corresponding column family's option.
    database_config: DatabaseConfig,
    /// Holds the default options for the `standard` column family option.
    default_standard_options: DatabaseTableOptions,
    /// Holds the default options for the `optimized_for_blobs` column family option.
    default_optimized_for_blobs_options: DatabaseTableOptions,
    /// Holds the default options for the `metadata` column family option.
    default_metadata_options: DatabaseTableOptions,
    /// Holds the default options for the `blob_info` column family option.
    default_blob_info_options: DatabaseTableOptions,
}

impl DatabaseTableOptionsFactory {
    /// Creates a new `DatabaseTableOptionsFactory` from a `DatabaseConfig`.
    pub fn new(database_config: DatabaseConfig) -> Self {
        // Set the default options for the 4 template options.
        let default_standard_options = Self::get_table_options_with_inheritance(
            &database_config.standard,
            DatabaseTableOptions::standard(),
        );
        let default_optimized_for_blobs_options = Self::get_table_options_with_inheritance(
            &database_config.optimized_for_blobs,
            DatabaseTableOptions::optimized_for_blobs_template(),
        );
        let default_metadata_options = Self::get_table_options_with_inheritance(
            &database_config.metadata_template,
            DatabaseTableOptions::metadata_template(),
        );
        let default_blob_info_options = Self::get_table_options_with_inheritance(
            &database_config.blob_info_template,
            DatabaseTableOptions::blob_info_template(),
        );

        Self {
            database_config,
            default_standard_options,
            default_optimized_for_blobs_options,
            default_metadata_options,
            default_blob_info_options,
        }
    }

    /// Returns the global database options for creating a RocksDB instance.
    pub(crate) fn global_db_options(&self) -> GlobalDatabaseOptions {
        self.database_config.global.clone()
    }

    /// Helper to get a non-optional DatabaseTableOptions field with inheritance from a static
    /// default.
    fn get_table_options_with_inheritance(
        db_config_option: &DatabaseTableOptions,
        default_option: DatabaseTableOptions,
    ) -> DatabaseTableOptions {
        db_config_option.clone().inherit_from(default_option)
    }

    /// Helper to get an optional DatabaseTableOptions field with inheritance from a default.
    fn get_optional_table_options_with_inheritance(
        db_config_option: &Option<DatabaseTableOptions>,
        default_option: DatabaseTableOptions,
    ) -> DatabaseTableOptions {
        db_config_option
            .clone()
            .map(|options| options.inherit_from(default_option.clone()))
            .unwrap_or(default_option)
    }

    /// Helper to get an optional DatabaseTableOptions field with inheritance from the default
    /// `standard` options.
    fn get_optional_table_options_with_inheritance_from_standard(
        &self,
        db_config_option: &Option<DatabaseTableOptions>,
    ) -> DatabaseTableOptions {
        Self::get_optional_table_options_with_inheritance(
            db_config_option,
            self.default_standard_options.clone(),
        )
    }

    /// Returns the default standard database option. Can be used as the default option for all
    /// low footprint, low traffic column families.
    pub(crate) fn standard(&self) -> DatabaseTableOptions {
        self.default_standard_options.clone()
    }

    /// Returns the metadata database option inherited from the default
    /// DatabaseTableOptions::metadata().
    pub(crate) fn metadata(&self) -> DatabaseTableOptions {
        Self::get_optional_table_options_with_inheritance(
            &self.database_config.metadata,
            self.default_metadata_options.clone(),
        )
    }

    /// Returns the blob info database option inherited from the default
    /// DatabaseTableOptions::blob_info().
    pub(crate) fn blob_info(&self) -> DatabaseTableOptions {
        Self::get_optional_table_options_with_inheritance(
            &self.database_config.blob_info,
            self.default_blob_info_options.clone(),
        )
    }

    /// Returns the per object blob info database option inherited from the default
    /// DatabaseTableOptions::blob_info().
    pub(crate) fn per_object_blob_info(&self) -> DatabaseTableOptions {
        Self::get_optional_table_options_with_inheritance(
            &self.database_config.per_object_blob_info,
            self.default_blob_info_options.clone(),
        )
    }

    /// Returns the shard database option inherited from Self::optimized_for_blobs().
    pub(crate) fn shard(&self) -> DatabaseTableOptions {
        Self::get_optional_table_options_with_inheritance(
            &self.database_config.shard,
            self.default_optimized_for_blobs_options.clone(),
        )
    }

    /// Returns the node status database option inherited from Self::standard().
    pub(crate) fn node_status(&self) -> DatabaseTableOptions {
        self.get_optional_table_options_with_inheritance_from_standard(
            &self.database_config.node_status,
        )
    }

    /// Returns the event cursor database option inherited from Self::standard().
    pub(crate) fn event_cursor(&self) -> DatabaseTableOptions {
        self.get_optional_table_options_with_inheritance_from_standard(
            &self.database_config.event_cursor,
        )
    }

    /// Returns the shard status database option inherited from Self::standard().
    pub(crate) fn shard_status(&self) -> DatabaseTableOptions {
        self.get_optional_table_options_with_inheritance_from_standard(
            &self.database_config.shard_status,
        )
    }

    /// Returns the shard sync progress database option inherited from Self::standard().
    pub(crate) fn shard_sync_progress(&self) -> DatabaseTableOptions {
        self.get_optional_table_options_with_inheritance_from_standard(
            &self.database_config.shard_sync_progress,
        )
    }

    /// Returns the pending recover slivers database option inherited from Self::standard().
    pub(crate) fn pending_recover_slivers(&self) -> DatabaseTableOptions {
        self.get_optional_table_options_with_inheritance_from_standard(
            &self.database_config.pending_recover_slivers,
        )
    }

    /// Returns the event blob writer certified database option inherited from Self::standard().
    pub fn certified(&self) -> DatabaseTableOptions {
        self.get_optional_table_options_with_inheritance_from_standard(
            &self.database_config.certified,
        )
    }

    /// Returns the event blob writer pending database option inherited from Self::standard().
    pub fn pending(&self) -> DatabaseTableOptions {
        self.get_optional_table_options_with_inheritance_from_standard(
            &self.database_config.pending,
        )
    }

    /// Returns the event blob writer attested database option inherited from Self::standard().
    pub fn attested(&self) -> DatabaseTableOptions {
        self.get_optional_table_options_with_inheritance_from_standard(
            &self.database_config.attested,
        )
    }

    /// Returns the event blob writer failed to attest database option inherited from
    /// Self::standard().
    pub fn failed_to_attest(&self) -> DatabaseTableOptions {
        self.get_optional_table_options_with_inheritance_from_standard(
            &self.database_config.failed_to_attest,
        )
    }

    /// Returns the checkpoint store database option inherited from Self::standard().
    pub(crate) fn checkpoint_store(&self) -> DatabaseTableOptions {
        self.get_optional_table_options_with_inheritance_from_standard(
            &self.database_config.checkpoint_store,
        )
    }

    /// Returns the walrus package store database option inherited from Self::standard().
    pub(crate) fn walrus_package_store(&self) -> DatabaseTableOptions {
        self.get_optional_table_options_with_inheritance_from_standard(
            &self.database_config.walrus_package_store,
        )
    }

    /// Returns the committee store database option inherited from Self::standard().
    pub(crate) fn committee_store(&self) -> DatabaseTableOptions {
        self.get_optional_table_options_with_inheritance_from_standard(
            &self.database_config.committee_store,
        )
    }

    /// Returns the event store database option inherited from Self::standard().
    pub(crate) fn event_store(&self) -> DatabaseTableOptions {
        self.get_optional_table_options_with_inheritance_from_standard(
            &self.database_config.event_store,
        )
    }

    /// Returns the init state store database option inherited from Self::standard().
    pub(crate) fn init_state(&self) -> DatabaseTableOptions {
        self.get_optional_table_options_with_inheritance_from_standard(
            &self.database_config.init_state,
        )
    }
}

impl Default for DatabaseTableOptionsFactory {
    fn default() -> Self {
        Self::new(DatabaseConfig::default())
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
                blob_compression_type: none
                enable_blob_garbage_collection: false
            optimized_for_blobs:
                enable_blob_files: true
                min_blob_size: 0
                blob_file_size: 1000
            shard:
                blob_garbage_collection_force_threshold: 0.5
        "};

        let config: DatabaseConfig = serde_yaml::from_str(yaml)?;
        let db_table_options_factory = DatabaseTableOptionsFactory::new(config.clone());

        let shard_options = db_table_options_factory.shard();
        assert_eq!(
            shard_options,
            DatabaseTableOptions {
                blob_garbage_collection_force_threshold: Some(0.5),
                ..db_table_options_factory.default_optimized_for_blobs_options
            }
        );

        let optimized_for_blobs_options = config.optimized_for_blobs;
        assert_eq!(
            optimized_for_blobs_options,
            DatabaseTableOptions {
                enable_blob_files: Some(true),
                min_blob_size: Some(0),
                blob_file_size: Some(1000),
                ..Default::default()
            }
        );

        Ok(())
    }

    #[test]
    fn test_blob_info_database_config() -> TestResult {
        let yaml = indoc! {"
            blob_info:
                block_cache_size: 1000000000
        "};

        let config: DatabaseConfig = serde_yaml::from_str(yaml)?;
        let db_table_options_factory = DatabaseTableOptionsFactory::new(config);

        let blob_info_options = db_table_options_factory.blob_info();
        assert_eq!(
            blob_info_options,
            DatabaseTableOptions {
                block_cache_size: Some(1000000000),
                ..DatabaseTableOptions::standard()
            }
        );

        Ok(())
    }

    #[test]
    fn test_database_config_inheritance() -> TestResult {
        // Test 1: Default configuration (no overrides)
        let default_config = DatabaseConfig::default();
        let db_table_options_factory = DatabaseTableOptionsFactory::new(default_config);

        // Verify static method inheritance still works
        assert_eq!(
            db_table_options_factory.default_standard_options,
            DatabaseTableOptions::standard()
        );
        assert_eq!(
            db_table_options_factory.default_optimized_for_blobs_options,
            DatabaseTableOptions::optimized_for_blobs_template()
        );
        assert_eq!(
            db_table_options_factory.default_metadata_options,
            DatabaseTableOptions::metadata_template()
        );

        // Verify optional fields with static defaults
        assert_eq!(
            db_table_options_factory.blob_info(),
            DatabaseTableOptions::blob_info_template()
        );
        assert_eq!(
            db_table_options_factory.per_object_blob_info(),
            DatabaseTableOptions::blob_info_template()
        );

        // Verify optional fields with instance defaults (inherit from standard)
        assert_eq!(
            db_table_options_factory.node_status(),
            db_table_options_factory.default_standard_options
        );
        assert_eq!(
            db_table_options_factory.event_cursor(),
            db_table_options_factory.default_standard_options
        );
        assert_eq!(
            db_table_options_factory.certified(),
            db_table_options_factory.default_standard_options
        );

        // Verify shard inherits from optimized_for_blobs
        assert_eq!(
            db_table_options_factory.shard(),
            db_table_options_factory.default_optimized_for_blobs_options
        );

        // Test 2: Partial configuration from YAML
        let yaml = indoc! {"
            standard:
                write_buffer_size: 128000000
                block_cache_size: 500000000
            optimized_for_blobs:
                enable_blob_files: false
                write_buffer_size: 512000000
            metadata_template:
                write_buffer_size: 256000000
            node_status:
                block_cache_size: 100000000
            shard:
                blob_file_size: 2000000000
        "};

        let config: DatabaseConfig = serde_yaml::from_str(yaml)?;
        let db_table_options_factory = DatabaseTableOptionsFactory::new(config);

        // Test standard field inheritance (partial config should inherit from
        // DatabaseTableOptions::standard())
        let standard = db_table_options_factory.default_standard_options.clone();
        assert_eq!(standard.write_buffer_size, Some(128000000)); // overridden
        assert_eq!(standard.block_cache_size, Some(500000000)); // overridden
        // inherited from DatabaseTableOptions::standard()
        assert_eq!(standard.target_file_size_base, Some(64 << 20));

        // Test optimized_for_blobs field inheritance
        let optimized = db_table_options_factory
            .default_optimized_for_blobs_options
            .clone();
        assert_eq!(optimized.enable_blob_files, Some(false)); // overridden
        assert_eq!(optimized.write_buffer_size, Some(512000000)); // overridden
        // inherited from DatabaseTableOptions::optimized_for_blobs()
        assert_eq!(optimized.min_blob_size, Some(1 << 20));
        assert_eq!(optimized.blob_compression_type, Some("zstd".to_string())); // inherited

        // Test metadata field inheritance (should inherit from DatabaseTableOptions::metadata())
        let metadata = db_table_options_factory.default_metadata_options.clone();
        assert_eq!(metadata.write_buffer_size, Some(256000000)); // overridden
        // inherited from DatabaseTableOptions::metadata()
        assert_eq!(metadata.hard_pending_compaction_bytes_limit, Some(0));

        // Test optional field inheritance from instance default (node_status inherits from
        // standard)
        let node_status = db_table_options_factory.node_status();
        assert_eq!(node_status.block_cache_size, Some(100000000)); // overridden
        // inherited from config.standard()
        assert_eq!(node_status.write_buffer_size, Some(128000000));
        assert_eq!(node_status.target_file_size_base, Some(64 << 20)); // inherited through standard

        // Test optional field inheritance from instance default (shard inherits from
        // optimized_for_blobs)
        let shard = db_table_options_factory.shard();
        assert_eq!(shard.blob_file_size, Some(2000000000)); // overridden
        // inherited from config.optimized_for_blobs()
        assert_eq!(shard.enable_blob_files, Some(false));
        // inherited from config.optimized_for_blobs()
        assert_eq!(shard.write_buffer_size, Some(512000000));

        // Test fields with no config should use their respective defaults
        assert_eq!(
            db_table_options_factory.event_cursor(),
            db_table_options_factory.default_standard_options.clone()
        );
        assert_eq!(
            db_table_options_factory.certified(),
            db_table_options_factory.default_standard_options.clone()
        );
        assert_eq!(
            db_table_options_factory.blob_info(),
            DatabaseTableOptions::blob_info_template()
        );

        Ok(())
    }
}
