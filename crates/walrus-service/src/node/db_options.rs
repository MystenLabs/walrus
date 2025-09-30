// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Database options module for providing consistent column family options across the codebase.
//!
//! This module exports functions that return the proper RocksDB options for each column family,
//! including any custom merge operators. This ensures that databases can be opened consistently
//! both in production code and in debugging tools like db_tools.

use std::path::Path;

use anyhow::Result;
use rocksdb::{DB, Options};

use super::DatabaseConfig;

/// Represents a column family with its name and options.
#[derive(Debug, Clone)]
pub enum DbColumnFamily {
    // Main storage column families.
    /// Node status column family.
    NodeStatus,
    /// Metadata column family.
    Metadata,
    /// Aggregate blob info column family.
    AggregateBlobInfo,
    /// Per-object blob info column family.
    PerObjectBlobInfo,
    /// Event cursor column family.
    EventCursor,
    /// Primary sliver column family for a specific shard.
    PrimarySliver(u16),
    /// Secondary sliver column family for a specific shard.
    SecondarySliver(u16),
    /// Shard status column family for a specific shard.
    ShardStatus(u16),
    /// Shard sync progress column family for a specific shard.
    ShardSyncProgress(u16),
    /// Pending recover slivers column family for a specific shard.
    PendingRecoverSlivers(u16),

    // Event processor column families.
    /// Event store column family.
    EventStore,
    /// Init state column family.
    InitState,

    // Event blob writer column families.
    /// Certified event blob column family.
    Certified,
    /// Attested event blob column family.
    Attested,
    /// Pending event blob column family.
    Pending,
    /// Failed to attest event blob column family.
    FailedToAttest,

    // Unknown/custom column family.
    /// Custom column family with a given name.
    Custom(String),
}

impl DbColumnFamily {
    /// Create a DbColumnFamily from a column family name string.
    pub fn from_name(name: &str) -> Self {
        use crate::{
            event::event_processor::db::constants as event_processor_constants,
            node::{
                event_blob_writer::{
                    attested_cf_name,
                    certified_cf_name,
                    failed_to_attest_cf_name,
                    pending_cf_name,
                },
                storage::constants::{
                    aggregate_blob_info_cf_name,
                    event_cursor_cf_name,
                    metadata_cf_name,
                    node_status_cf_name,
                    per_object_blob_info_cf_name,
                },
            },
        };

        // Check for known column family names.
        if name == node_status_cf_name() {
            Self::NodeStatus
        } else if name == metadata_cf_name() {
            Self::Metadata
        } else if name == aggregate_blob_info_cf_name() {
            Self::AggregateBlobInfo
        } else if name == per_object_blob_info_cf_name() {
            Self::PerObjectBlobInfo
        } else if name == event_cursor_cf_name() {
            Self::EventCursor
        } else if name == event_processor_constants::EVENT_STORE {
            Self::EventStore
        } else if name == event_processor_constants::INIT_STATE {
            Self::InitState
        } else if name == certified_cf_name() {
            Self::Certified
        } else if name == attested_cf_name() {
            Self::Attested
        } else if name == pending_cf_name() {
            Self::Pending
        } else if name == failed_to_attest_cf_name() {
            Self::FailedToAttest
        } else if let Some(shard_str) = name.strip_prefix("primary_sliver_") {
            if let Ok(shard) = shard_str.parse::<u16>() {
                Self::PrimarySliver(shard)
            } else {
                Self::Custom(name.to_string())
            }
        } else if let Some(shard_str) = name.strip_prefix("secondary_sliver_") {
            if let Ok(shard) = shard_str.parse::<u16>() {
                Self::SecondarySliver(shard)
            } else {
                Self::Custom(name.to_string())
            }
        } else if let Some(shard_str) = name.strip_prefix("shard_status_") {
            if let Ok(shard) = shard_str.parse::<u16>() {
                Self::ShardStatus(shard)
            } else {
                Self::Custom(name.to_string())
            }
        } else if let Some(shard_str) = name.strip_prefix("shard_sync_progress_") {
            if let Ok(shard) = shard_str.parse::<u16>() {
                Self::ShardSyncProgress(shard)
            } else {
                Self::Custom(name.to_string())
            }
        } else if let Some(shard_str) = name.strip_prefix("pending_recover_slivers_") {
            if let Ok(shard) = shard_str.parse::<u16>() {
                Self::PendingRecoverSlivers(shard)
            } else {
                Self::Custom(name.to_string())
            }
        } else {
            Self::Custom(name.to_string())
        }
    }

    /// Get the name of this column family.
    pub fn name(&self) -> String {
        use walrus_core::ShardIndex;

        use crate::{
            event::event_processor::db::constants as event_processor_constants,
            node::{
                event_blob_writer::{
                    attested_cf_name,
                    certified_cf_name,
                    failed_to_attest_cf_name,
                    pending_cf_name,
                },
                storage::constants::{
                    aggregate_blob_info_cf_name,
                    event_cursor_cf_name,
                    metadata_cf_name,
                    node_status_cf_name,
                    pending_recover_slivers_column_family_name,
                    per_object_blob_info_cf_name,
                    primary_slivers_column_family_name,
                    secondary_slivers_column_family_name,
                    shard_status_column_family_name,
                    shard_sync_progress_column_family_name,
                },
            },
        };

        match self {
            Self::NodeStatus => node_status_cf_name().to_string(),
            Self::Metadata => metadata_cf_name().to_string(),
            Self::AggregateBlobInfo => aggregate_blob_info_cf_name().to_string(),
            Self::PerObjectBlobInfo => per_object_blob_info_cf_name().to_string(),
            Self::EventCursor => event_cursor_cf_name().to_string(),
            Self::PrimarySliver(shard) => primary_slivers_column_family_name(ShardIndex(*shard)),
            Self::SecondarySliver(shard) => {
                secondary_slivers_column_family_name(ShardIndex(*shard))
            }
            Self::ShardStatus(shard) => shard_status_column_family_name(ShardIndex(*shard)),
            Self::ShardSyncProgress(shard) => {
                shard_sync_progress_column_family_name(ShardIndex(*shard))
            }
            Self::PendingRecoverSlivers(shard) => {
                pending_recover_slivers_column_family_name(ShardIndex(*shard))
            }
            Self::EventStore => event_processor_constants::EVENT_STORE.to_string(),
            Self::InitState => event_processor_constants::INIT_STATE.to_string(),
            Self::Certified => certified_cf_name().to_string(),
            Self::Attested => attested_cf_name().to_string(),
            Self::Pending => pending_cf_name().to_string(),
            Self::FailedToAttest => failed_to_attest_cf_name().to_string(),
            Self::Custom(name) => name.clone(),
        }
    }

    /// Get the RocksDB options for this column family, including any merge operators.
    pub fn options(&self, db_config: &DatabaseConfig) -> Options {
        use crate::node::storage::{
            blob_info::{blob_info_cf_options, per_object_blob_info_cf_options},
            event_cursor_table_options,
            metadata_options,
            node_status_options,
        };

        match self {
            Self::NodeStatus => node_status_options(db_config),
            Self::Metadata => metadata_options(db_config),
            Self::AggregateBlobInfo => blob_info_cf_options(db_config),
            Self::PerObjectBlobInfo => per_object_blob_info_cf_options(db_config),
            Self::EventCursor => {
                // Use the EventCursorTable::options which includes the proper merge operator.
                let (_, opts) = event_cursor_table_options(db_config);
                opts
            }
            Self::PrimarySliver(_) | Self::SecondarySliver(_) | Self::PendingRecoverSlivers(_) => {
                // Sliver column families use the same shard options.
                db_config.shard().to_options()
            }
            Self::ShardStatus(_) | Self::ShardSyncProgress(_) => {
                // These don't have special options defined.
                let mut opts = Options::default();
                opts.set_allow_mmap_reads(true);
                opts
            }
            Self::EventStore | Self::InitState => {
                // Event processor CFs don't have special merge operators.
                let mut opts = Options::default();
                opts.set_allow_mmap_reads(true);
                opts
            }
            Self::Certified | Self::Attested | Self::Pending | Self::FailedToAttest => {
                // Event blob writer CFs don't have special merge operators.
                let mut opts = Options::default();
                opts.set_allow_mmap_reads(true);
                opts
            }
            Self::Custom(_) => {
                // Default options for unknown column families.
                let mut opts = Options::default();
                opts.set_allow_mmap_reads(true);
                opts
            }
        }
    }
}

/// Open database with only specific column families in read-only mode.
pub fn open_db_cf_readonly(
    db_path: &Path,
    cf_names: &[&str],
    db_config: &DatabaseConfig,
) -> Result<DB> {
    let mut db_opts = Options::default();

    // Apply common options for read-only access.
    db_opts.set_allow_mmap_reads(true);

    // Don't create if missing for read-only operations.
    db_opts.create_if_missing(false);
    db_opts.create_missing_column_families(false);

    // Try to get existing column families.
    let existing_cfs = DB::list_cf(&db_opts, db_path)?;

    // Only open the requested column families that exist.
    let mut cfs_to_open = Vec::new();
    for cf_name in cf_names {
        if existing_cfs.contains(&cf_name.to_string()) {
            // Use the DbColumnFamily enum to get the proper options.
            let cf = DbColumnFamily::from_name(cf_name);
            let cf_opts = cf.options(db_config);
            cfs_to_open.push((*cf_name, cf_opts));
        }
    }

    // Open the database with only the requested CFs in read-only mode.
    if cfs_to_open.is_empty() {
        // If no requested CFs exist, open without column families.
        DB::open_for_read_only(&db_opts, db_path, false)
            .map_err(|e| anyhow::anyhow!("failed to open database: {}", e))
    } else {
        // We need to use open_cf_descriptors_read_only to pass the column family options
        // with their merge operators for proper read behavior.
        let cf_descriptors: Vec<rocksdb::ColumnFamilyDescriptor> = cfs_to_open
            .into_iter()
            .map(|(name, opts)| rocksdb::ColumnFamilyDescriptor::new(name, opts))
            .collect();

        DB::open_cf_descriptors_read_only(&db_opts, db_path, cf_descriptors, false)
            .map_err(|e| anyhow::anyhow!("failed to open database: {}", e))
    }
}
