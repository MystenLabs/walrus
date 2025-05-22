// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use rocksdb::{BlockBasedOptions, Cache, DBCompressionType, Options};
use serde::{Deserialize, Serialize};

/// Options for configuring a column family.
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
    #[serde(skip_serializing_if = "Option::is_none")]
    target_file_size_base: Option<u64>,
    /// The maximum total data size for level 1 in bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    max_bytes_for_level_base: Option<u64>,
    /// Block cache size in bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    block_cache_size: Option<u64>,
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

impl DatabaseTableOptions {
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
            write_buffer_size: Some(64 << 20),
            target_file_size_base: Some(64 << 20),
            max_bytes_for_level_base: Some(512 << 20),
            block_cache_size: Some(64 << 20),
            pin_l0_filter_and_index_blocks_in_block_cache: Some(true),
            soft_pending_compaction_bytes_limit: None,
            hard_pending_compaction_bytes_limit: None,
        }
    }

    fn optimized_for_blobs() -> Self {
        Self {
            enable_blob_files: Some(true),
            min_blob_size: Some(1 << 20),
            blob_file_size: Some(1 << 28),
            blob_compression_type: Some("zstd".to_string()),
            enable_blob_garbage_collection: Some(true),
            blob_garbage_collection_age_cutoff: Some(0.5),
            blob_garbage_collection_force_threshold: Some(0.5),
            blob_compaction_read_ahead_size: Some(10 << 20),
            write_buffer_size: Some(256 << 20),
            target_file_size_base: Some(64 << 20),
            max_bytes_for_level_base: Some(512 << 20),
            block_cache_size: Some(256 << 20),
            pin_l0_filter_and_index_blocks_in_block_cache: Some(true),
            soft_pending_compaction_bytes_limit: None,
            hard_pending_compaction_bytes_limit: None,
        }
    }

    fn blob_info() -> Self {
        Self {
            block_cache_size: Some(512 << 20),
            ..Default::default()
        }
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
            let mut block_based_options = BlockBasedOptions::default();
            block_based_options.set_block_cache(&Cache::new_lru_cache(block_cache_size as usize));
            if let Some(pin_l0_filter_and_index_blocks_in_block_cache) =
                self.pin_l0_filter_and_index_blocks_in_block_cache
            {
                block_based_options.set_pin_l0_filter_and_index_blocks_in_cache(
                    pin_l0_filter_and_index_blocks_in_block_cache,
                );
            }
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
}

impl Default for GlobalDatabaseOptions {
    fn default() -> Self {
        Self {
            max_open_files: Some(512_000),
        }
    }
}

impl From<&GlobalDatabaseOptions> for Options {
    fn from(value: &GlobalDatabaseOptions) -> Self {
        let mut options = Options::default();

        if let Some(max_files) = value.max_open_files {
            options.set_max_open_files(max_files);
        }

        options
    }
}

/// Database configuration for Walrus storage nodes.
///
/// The `standard` options are applied to all tables except for slivers and metadata. The
/// `optimized_for_blobs` options are applied to sliver and metadata tables.
///
/// Options for all individual tables can be set as well through the `node_status`, `metadata`,
/// `blob_info`, `per_object_blob_info`, `event_cursor`, `shard`, `shard_status`,
/// `shard_sync_progress`, and `pending_recover_slivers` fields.
///
/// **Warning:** Note that the configuration is currently not properly hierarchical. For example, if
/// the `metadata` options are defined, they are *not* merged with the `optimized_for_blobs` or
/// `standard` options. Any options that should not be `None` need to be set explicitly, even if
/// they are equal to those from the `standard` or `optimized_for_blobs` options.
#[serde_with::serde_as]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(default)]
pub struct DatabaseConfig {
    /// Global database options.
    pub(super) global: GlobalDatabaseOptions,
    /// Default database table options used by all tables except for slivers and metadata.
    pub(super) standard: DatabaseTableOptions,
    /// Database table options applied to sliver and metadata tables.
    pub(super) optimized_for_blobs: DatabaseTableOptions,
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

impl DatabaseConfig {
    /// Returns the global database options.
    pub fn global(&self) -> GlobalDatabaseOptions {
        self.global.clone()
    }

    /// Returns the node status database option.
    pub fn node_status(&self) -> DatabaseTableOptions {
        self.node_status
            .clone()
            .map(|options| options.inherit_from(self.standard.clone()))
            .unwrap_or_else(|| self.standard.clone())
    }

    /// Returns the metadata database option.
    pub fn metadata(&self) -> DatabaseTableOptions {
        let mut metadata_cf_options = self
            .metadata
            .clone()
            .map(|options| options.inherit_from(self.optimized_for_blobs.clone()))
            .unwrap_or_else(|| self.optimized_for_blobs.clone());
        // Disable write throttling for metadata column family.
        metadata_cf_options.soft_pending_compaction_bytes_limit = Some(256 * 1024 * 1024 * 1024); // 256GB
        metadata_cf_options.hard_pending_compaction_bytes_limit = Some(0); // Disable write stall.
        metadata_cf_options
    }

    /// Returns the blob info database option.
    pub fn blob_info(&self) -> DatabaseTableOptions {
        self.blob_info
            .clone()
            .map(|options| options.inherit_from(self.standard.clone()))
            .unwrap_or_else(|| self.standard.clone())
    }

    /// Returns the per object blob info database option.
    pub fn per_object_blob_info(&self) -> DatabaseTableOptions {
        self.per_object_blob_info
            .clone()
            .map(|options| options.inherit_from(self.standard.clone()))
            .unwrap_or_else(|| self.standard.clone())
    }

    /// Returns the event cursor database option.
    pub fn event_cursor(&self) -> DatabaseTableOptions {
        self.event_cursor
            .clone()
            .map(|options| options.inherit_from(self.standard.clone()))
            .unwrap_or_else(|| self.standard.clone())
    }

    /// Returns the shard database option.
    pub fn shard(&self) -> DatabaseTableOptions {
        self.shard
            .clone()
            .map(|options| options.inherit_from(self.optimized_for_blobs.clone()))
            .unwrap_or_else(|| self.optimized_for_blobs.clone())
    }

    /// Returns the shard status database option.
    pub fn shard_status(&self) -> DatabaseTableOptions {
        self.shard_status
            .clone()
            .map(|options| options.inherit_from(self.standard.clone()))
            .unwrap_or_else(|| self.standard.clone())
    }

    /// Returns the shard sync progress database option.
    pub fn shard_sync_progress(&self) -> DatabaseTableOptions {
        self.shard_sync_progress
            .clone()
            .map(|options| options.inherit_from(self.standard.clone()))
            .unwrap_or_else(|| self.standard.clone())
    }

    /// Returns the pending recover slivers database option.
    pub fn pending_recover_slivers(&self) -> DatabaseTableOptions {
        self.pending_recover_slivers
            .clone()
            .map(|options| options.inherit_from(self.standard.clone()))
            .unwrap_or_else(|| self.standard.clone())
    }

    /// Returns the event blob writer certified database option.
    pub fn certified(&self) -> DatabaseTableOptions {
        self.certified
            .clone()
            .unwrap_or_else(|| self.standard.clone())
    }

    /// Returns the event blob writer pending database option.
    pub fn pending(&self) -> DatabaseTableOptions {
        self.pending
            .clone()
            .unwrap_or_else(|| self.standard.clone())
    }

    /// Returns the event blob writer attested database option.
    pub fn attested(&self) -> DatabaseTableOptions {
        self.attested
            .clone()
            .unwrap_or_else(|| self.standard.clone())
    }

    /// Returns the event blob writer failed to attest database option.
    pub fn failed_to_attest(&self) -> DatabaseTableOptions {
        self.failed_to_attest
            .clone()
            .unwrap_or_else(|| self.standard.clone())
    }

    /// Returns the checkpoint store database option.
    pub fn checkpoint_store(&self) -> DatabaseTableOptions {
        self.checkpoint_store
            .clone()
            .unwrap_or_else(|| self.standard.clone())
    }

    /// Returns the walrus package store database option.
    pub fn walrus_package_store(&self) -> DatabaseTableOptions {
        self.walrus_package_store
            .clone()
            .unwrap_or_else(|| self.standard.clone())
    }

    /// Returns the committee store database option.
    pub fn committee_store(&self) -> DatabaseTableOptions {
        self.committee_store
            .clone()
            .unwrap_or_else(|| self.standard.clone())
    }

    /// Returns the event store database option.
    pub fn event_store(&self) -> DatabaseTableOptions {
        self.event_store
            .clone()
            .unwrap_or_else(|| self.standard.clone())
    }

    /// Returns the init state store database option.
    pub fn init_state(&self) -> DatabaseTableOptions {
        self.init_state
            .clone()
            .unwrap_or_else(|| self.standard.clone())
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            global: GlobalDatabaseOptions::default(),
            standard: DatabaseTableOptions::standard(),
            optimized_for_blobs: DatabaseTableOptions::optimized_for_blobs(),
            node_status: None,
            metadata: None,
            blob_info: Some(DatabaseTableOptions::blob_info()),
            per_object_blob_info: Some(DatabaseTableOptions::blob_info()),
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

#[cfg(test)]
mod tests {
    use indoc::indoc;
    use walrus_test_utils::Result as TestResult;

    use super::*;

    #[test]
    fn test_optimized_for_blobs_database_config() -> TestResult {
        let yaml = indoc! {"
            default:
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

        let shard_options = config.shard();
        assert_eq!(
            shard_options,
            DatabaseTableOptions {
                blob_garbage_collection_force_threshold: Some(0.5),
                ..config.optimized_for_blobs.clone()
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

        let blob_info_options = config.blob_info();
        assert_eq!(
            blob_info_options,
            DatabaseTableOptions {
                block_cache_size: Some(1000000000),
                ..DatabaseTableOptions::standard()
            }
        );

        Ok(())
    }
}
