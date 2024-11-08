// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use rocksdb::{DBCompressionType, Options};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

/// Options for configuring a column family.
#[serde_with::serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DatabaseTableOptions {
    // Set it to true to enable key-value separation.
    enable_blob_files: Option<bool>,
    // Values at or above this threshold will be written
    // to blob files during flush or compaction
    min_blob_size: Option<u64>,
    // The size limit for blob files.
    blob_file_size: Option<u64>,
    // The compression type to use for blob files.
    // All blobs in the same file are compressed using the same algorithm.
    blob_compression_type: Option<String>,
    // Set this to true to make BlobDB actively relocate valid blobs from the oldest
    // blob files as they are encountered during compaction
    enable_blob_garbage_collection: Option<bool>,
    // The cutoff that the GC logic uses to determine which blob files should be considered “old.”
    blob_garbage_collection_age_cutoff: Option<f64>,
    // If the ratio of garbage in the oldest blob files exceeds this threshold, targeted compactions
    // are scheduled in order to force garbage collecting the blob files in question, assuming they
    // are all eligible based on the value of blob_garbage_collection_age_cutoff above. This can
    // help reduce space amplification in the case of skewed workloads where the affected files
    // would not otherwise be picked up for compaction.
    blob_garbage_collection_force_threshold: Option<f64>,
    // When set, BlobDB will prefetch data from blob files in chunks of the configured size during
    // compaction
    blob_compaction_read_ahead_size: Option<u64>,
    // Size of the write buffer in bytes
    write_buffer_size: Option<usize>,
    // The target file size for level-1 files in bytes
    target_file_size_base: Option<u64>,
    // The maximum total data size for level 1 in bytes
    max_bytes_for_level_base: Option<u64>,
}

impl Default for DatabaseTableOptions {
    fn default() -> Self {
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
        }
    }
}

impl DatabaseTableOptions {
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
            target_file_size_base: Some(4 << 20),
            max_bytes_for_level_base: Some(32 << 20),
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

        options
    }
}

/// RocksDB options applied to the overall database.
#[serde_with::serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, Deserialize, Serialize)]
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
#[serde_with::serde_as]
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct DatabaseConfig {
    pub(super) global: GlobalDatabaseOptions,
    pub(super) node_status: DatabaseTableOptions,
    pub(super) metadata: DatabaseTableOptions,
    pub(super) blob_info: DatabaseTableOptions,
    pub(super) event_cursor: DatabaseTableOptions,
    pub(super) shard: DatabaseTableOptions,
    pub(super) shard_status: DatabaseTableOptions,
    pub(super) shard_sync_progress: DatabaseTableOptions,
    pub(super) pending_recover_slivers: DatabaseTableOptions,
}

impl DatabaseConfig {
    /// Returns the shard configuration.
    pub fn shard(&self) -> &DatabaseTableOptions {
        &self.shard
    }

    /// Returns the shard status database option.
    pub fn shard_status(&self) -> &DatabaseTableOptions {
        &self.shard_status
    }

    /// Returns the shard sync progress database option.
    pub fn shard_sync_progress(&self) -> &DatabaseTableOptions {
        &self.shard_sync_progress
    }

    /// Returns the pending recover slivers database option.
    pub fn pending_recover_slivers(&self) -> &DatabaseTableOptions {
        &self.pending_recover_slivers
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            global: GlobalDatabaseOptions::default(),
            node_status: DatabaseTableOptions::default(),
            metadata: DatabaseTableOptions::optimized_for_blobs(),
            blob_info: DatabaseTableOptions::default(),
            event_cursor: DatabaseTableOptions::default(),
            shard: DatabaseTableOptions::optimized_for_blobs(),
            shard_status: DatabaseTableOptions::default(),
            shard_sync_progress: DatabaseTableOptions::default(),
            pending_recover_slivers: DatabaseTableOptions::default(),
        }
    }
}
