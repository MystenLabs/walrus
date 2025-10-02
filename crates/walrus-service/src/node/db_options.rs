// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Database options module for providing consistent column family options across the codebase.
//!
//! This module exports functions that return the proper RocksDB options for each column family,
//! including any custom merge operators. This ensures that databases can be opened consistently
//! both in production code and in debugging tools like db_tools.

use std::path::Path;

use anyhow::Result;
use clap::ValueEnum;
use rocksdb::{DB, Options};
use serde::{Deserialize, Serialize};

use super::DatabaseConfig;

/// Database type selector for db_tools commands.
#[derive(Debug, Clone, Copy, ValueEnum, Serialize, Deserialize, PartialEq, Eq)]
pub enum DbType {
    /// Main storage database.
    Main,
    /// Event processor database.
    EventProcessor,
    /// Event blob writer database.
    EventBlobWriter,
}

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
    /// Parse the base column family name to extract shard ID.
    ///
    /// Expected format: "shard-{id}"
    /// Returns the shard ID if the format matches, None otherwise.
    fn parse_base_column_family_name(name: &str) -> Option<u16> {
        name.strip_prefix("shard-")?.parse::<u16>().ok()
    }

    /// Parse a primary slivers column family name.
    ///
    /// Expected format: "shard-{id}/primary-slivers"
    fn parse_primary_slivers(name: &str) -> Option<Self> {
        let (base, suffix) = name.split_once('/')?;
        if suffix != "primary-slivers" {
            return None;
        }
        let shard_id = Self::parse_base_column_family_name(base)?;
        Some(Self::PrimarySliver(shard_id))
    }

    /// Parse a secondary slivers column family name.
    ///
    /// Expected format: "shard-{id}/secondary-slivers"
    fn parse_secondary_slivers(name: &str) -> Option<Self> {
        let (base, suffix) = name.split_once('/')?;
        if suffix != "secondary-slivers" {
            return None;
        }
        let shard_id = Self::parse_base_column_family_name(base)?;
        Some(Self::SecondarySliver(shard_id))
    }

    /// Parse a shard status column family name.
    ///
    /// Expected format: "shard-{id}/status"
    fn parse_shard_status(name: &str) -> Option<Self> {
        let (base, suffix) = name.split_once('/')?;
        if suffix != "status" {
            return None;
        }
        let shard_id = Self::parse_base_column_family_name(base)?;
        Some(Self::ShardStatus(shard_id))
    }

    /// Parse a shard sync progress column family name.
    ///
    /// Expected format: "shard-{id}/sync-progress"
    fn parse_shard_sync_progress(name: &str) -> Option<Self> {
        let (base, suffix) = name.split_once('/')?;
        if suffix != "sync-progress" {
            return None;
        }
        let shard_id = Self::parse_base_column_family_name(base)?;
        Some(Self::ShardSyncProgress(shard_id))
    }

    /// Parse a pending recover slivers column family name.
    ///
    /// Expected format: "shard-{id}/pending-recover-slivers"
    fn parse_pending_recover_slivers(name: &str) -> Option<Self> {
        let (base, suffix) = name.split_once('/')?;
        if suffix != "pending-recover-slivers" {
            return None;
        }
        let shard_id = Self::parse_base_column_family_name(base)?;
        Some(Self::PendingRecoverSlivers(shard_id))
    }

    /// Parse any shard-based column family name.
    ///
    /// Expected format: "shard-{id}/{type}"
    /// Returns None if the name doesn't match any known shard column family format.
    fn parse_shard_column_family(name: &str) -> Option<Self> {
        // Try each specific parser.
        Self::parse_primary_slivers(name)
            .or_else(|| Self::parse_secondary_slivers(name))
            .or_else(|| Self::parse_shard_status(name))
            .or_else(|| Self::parse_shard_sync_progress(name))
            .or_else(|| Self::parse_pending_recover_slivers(name))
    }

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
        } else if let Some(cf) = Self::parse_shard_column_family(name) {
            cf
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

/// Get all possible column families for a specific database type.
/// This function returns ALL expected column families for the given type.
pub fn get_all_possible_column_families(db_type: DbType, db_path: &Path) -> Vec<DbColumnFamily> {
    use crate::{
        event::event_processor::db::constants as event_processor_constants,
        node::storage::ShardStorage,
    };

    let mut all_cfs = Vec::new();

    match db_type {
        DbType::Main => {
            // Add all main storage column families.
            all_cfs.push(DbColumnFamily::NodeStatus);
            all_cfs.push(DbColumnFamily::Metadata);
            all_cfs.push(DbColumnFamily::AggregateBlobInfo);
            all_cfs.push(DbColumnFamily::PerObjectBlobInfo);
            all_cfs.push(DbColumnFamily::EventCursor);

            // Get existing shard IDs from the database.
            let mut db_opts = Options::default();
            db_opts.create_missing_column_families(true);
            let existing_shards_ids = ShardStorage::existing_cf_shards_ids(db_path, &db_opts);

            for shard_id in existing_shards_ids {
                all_cfs.push(DbColumnFamily::PrimarySliver(shard_id.0));
                all_cfs.push(DbColumnFamily::SecondarySliver(shard_id.0));
                all_cfs.push(DbColumnFamily::ShardStatus(shard_id.0));
                all_cfs.push(DbColumnFamily::ShardSyncProgress(shard_id.0));
                all_cfs.push(DbColumnFamily::PendingRecoverSlivers(shard_id.0));
            }
        }
        DbType::EventProcessor => {
            // Add all event processor column families.
            all_cfs.push(DbColumnFamily::EventStore);
            all_cfs.push(DbColumnFamily::InitState);
            // Also add other event processor CFs.
            all_cfs.push(DbColumnFamily::Custom(
                event_processor_constants::CHECKPOINT_STORE.to_string(),
            ));
            all_cfs.push(DbColumnFamily::Custom(
                event_processor_constants::COMMITTEE_STORE.to_string(),
            ));
            all_cfs.push(DbColumnFamily::Custom(
                event_processor_constants::WALRUS_PACKAGE_STORE.to_string(),
            ));
        }
        DbType::EventBlobWriter => {
            // Add all event blob writer column families.
            all_cfs.push(DbColumnFamily::Certified);
            all_cfs.push(DbColumnFamily::Attested);
            all_cfs.push(DbColumnFamily::Pending);
            all_cfs.push(DbColumnFamily::FailedToAttest);
        }
    }

    all_cfs
}

/// Open database with write access and all column families with proper options.
/// This will create missing column families with the correct options.
pub fn open_db_for_write(
    db_path: &Path,
    db_type: DbType,
    db_config: &DatabaseConfig,
) -> Result<DB> {
    let mut db_opts = Options::default();
    db_opts.create_if_missing(false);
    db_opts.create_missing_column_families(true);

    // Get all possible column families for this database type.
    let all_cfs = get_all_possible_column_families(db_type, db_path);

    // Prepare column family descriptors with proper options.
    // Include ALL column families, not just existing ones.
    let mut cf_descriptors = Vec::new();
    for cf in all_cfs {
        cf_descriptors.push(rocksdb::ColumnFamilyDescriptor::new(
            cf.name(),
            cf.options(db_config),
        ));
    }

    // Always include default column family if not already present.
    if !cf_descriptors.iter().any(|cf| cf.name() == "default") {
        cf_descriptors.push(rocksdb::ColumnFamilyDescriptor::new(
            "default",
            Options::default(),
        ));
    }

    DB::open_cf_descriptors(&db_opts, db_path, cf_descriptors)
        .map_err(|e| anyhow::anyhow!("failed to open database for write: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_base_column_family_name() {
        // Valid base names.
        assert_eq!(
            DbColumnFamily::parse_base_column_family_name("shard-0"),
            Some(0)
        );
        assert_eq!(
            DbColumnFamily::parse_base_column_family_name("shard-42"),
            Some(42)
        );
        assert_eq!(
            DbColumnFamily::parse_base_column_family_name("shard-999"),
            Some(999)
        );
        assert_eq!(
            DbColumnFamily::parse_base_column_family_name("shard-65535"),
            Some(65535)
        );

        // Invalid base names.
        assert_eq!(
            DbColumnFamily::parse_base_column_family_name("shard-"),
            None
        );
        assert_eq!(
            DbColumnFamily::parse_base_column_family_name("shard-abc"),
            None
        );
        assert_eq!(
            DbColumnFamily::parse_base_column_family_name("not-a-shard"),
            None
        );
        assert_eq!(DbColumnFamily::parse_base_column_family_name("shard"), None);
        assert_eq!(DbColumnFamily::parse_base_column_family_name(""), None);
    }

    #[test]
    fn test_parse_individual_shard_column_families() {
        // Test parse_primary_slivers.
        assert!(matches!(
            DbColumnFamily::parse_primary_slivers("shard-42/primary-slivers"),
            Some(DbColumnFamily::PrimarySliver(42))
        ));
        assert!(DbColumnFamily::parse_primary_slivers("shard-42/secondary-slivers").is_none());
        assert!(DbColumnFamily::parse_primary_slivers("shard-42").is_none());

        // Test parse_secondary_slivers.
        assert!(matches!(
            DbColumnFamily::parse_secondary_slivers("shard-100/secondary-slivers"),
            Some(DbColumnFamily::SecondarySliver(100))
        ));
        assert!(DbColumnFamily::parse_secondary_slivers("shard-100/primary-slivers").is_none());

        // Test parse_shard_status.
        assert!(matches!(
            DbColumnFamily::parse_shard_status("shard-0/status"),
            Some(DbColumnFamily::ShardStatus(0))
        ));
        assert!(DbColumnFamily::parse_shard_status("shard-0/wrong-status").is_none());

        // Test parse_shard_sync_progress.
        assert!(matches!(
            DbColumnFamily::parse_shard_sync_progress("shard-999/sync-progress"),
            Some(DbColumnFamily::ShardSyncProgress(999))
        ));
        assert!(DbColumnFamily::parse_shard_sync_progress("shard-999/status").is_none());

        // Test parse_pending_recover_slivers.
        assert!(matches!(
            DbColumnFamily::parse_pending_recover_slivers("shard-1/pending-recover-slivers"),
            Some(DbColumnFamily::PendingRecoverSlivers(1))
        ));
        assert!(DbColumnFamily::parse_pending_recover_slivers("shard-1/pending").is_none());
    }

    #[test]
    fn test_db_column_family_name_construction_and_parsing() {
        // Test simple column families.
        let node_status = DbColumnFamily::NodeStatus;
        assert_eq!(node_status.name(), "node_status");
        assert!(matches!(
            DbColumnFamily::from_name("node_status"),
            DbColumnFamily::NodeStatus
        ));

        let metadata = DbColumnFamily::Metadata;
        assert_eq!(metadata.name(), "metadata");
        assert!(matches!(
            DbColumnFamily::from_name("metadata"),
            DbColumnFamily::Metadata
        ));

        let aggregate_blob = DbColumnFamily::AggregateBlobInfo;
        assert_eq!(aggregate_blob.name(), "aggregate_blob_info");
        assert!(matches!(
            DbColumnFamily::from_name("aggregate_blob_info"),
            DbColumnFamily::AggregateBlobInfo
        ));

        let per_object_blob = DbColumnFamily::PerObjectBlobInfo;
        assert_eq!(per_object_blob.name(), "per_object_blob_info");
        assert!(matches!(
            DbColumnFamily::from_name("per_object_blob_info"),
            DbColumnFamily::PerObjectBlobInfo
        ));

        let event_cursor = DbColumnFamily::EventCursor;
        assert_eq!(event_cursor.name(), "event_cursor");
        assert!(matches!(
            DbColumnFamily::from_name("event_cursor"),
            DbColumnFamily::EventCursor
        ));

        // Test shard-based column families.
        let shard_id = 42u16;

        let primary_sliver = DbColumnFamily::PrimarySliver(shard_id);
        let primary_name = primary_sliver.name();
        assert_eq!(primary_name, "shard-42/primary-slivers");
        if let DbColumnFamily::PrimarySliver(parsed_shard) =
            DbColumnFamily::from_name(&primary_name)
        {
            assert_eq!(parsed_shard, shard_id);
        } else {
            panic!("Failed to parse primary sliver column family name");
        }

        let secondary_sliver = DbColumnFamily::SecondarySliver(shard_id);
        let secondary_name = secondary_sliver.name();
        assert_eq!(secondary_name, "shard-42/secondary-slivers");
        if let DbColumnFamily::SecondarySliver(parsed_shard) =
            DbColumnFamily::from_name(&secondary_name)
        {
            assert_eq!(parsed_shard, shard_id);
        } else {
            panic!("Failed to parse secondary sliver column family name");
        }

        let shard_status = DbColumnFamily::ShardStatus(shard_id);
        let status_name = shard_status.name();
        assert_eq!(status_name, "shard-42/status");
        if let DbColumnFamily::ShardStatus(parsed_shard) = DbColumnFamily::from_name(&status_name) {
            assert_eq!(parsed_shard, shard_id);
        } else {
            panic!("Failed to parse shard status column family name");
        }

        let shard_sync = DbColumnFamily::ShardSyncProgress(shard_id);
        let sync_name = shard_sync.name();
        assert_eq!(sync_name, "shard-42/sync-progress");
        if let DbColumnFamily::ShardSyncProgress(parsed_shard) =
            DbColumnFamily::from_name(&sync_name)
        {
            assert_eq!(parsed_shard, shard_id);
        } else {
            panic!("Failed to parse shard sync progress column family name");
        }

        let pending_recover = DbColumnFamily::PendingRecoverSlivers(shard_id);
        let pending_name = pending_recover.name();
        assert_eq!(pending_name, "shard-42/pending-recover-slivers");
        if let DbColumnFamily::PendingRecoverSlivers(parsed_shard) =
            DbColumnFamily::from_name(&pending_name)
        {
            assert_eq!(parsed_shard, shard_id);
        } else {
            panic!("Failed to parse pending recover slivers column family name");
        }

        // Test with different shard IDs.
        let shard_0 = DbColumnFamily::PrimarySliver(0);
        assert_eq!(shard_0.name(), "shard-0/primary-slivers");

        let shard_999 = DbColumnFamily::SecondarySliver(999);
        assert_eq!(shard_999.name(), "shard-999/secondary-slivers");

        // Test event processor column families.
        let event_store = DbColumnFamily::EventStore;
        assert_eq!(event_store.name(), "event_store");
        assert!(matches!(
            DbColumnFamily::from_name("event_store"),
            DbColumnFamily::EventStore
        ));

        let init_state = DbColumnFamily::InitState;
        assert_eq!(init_state.name(), "init_state");
        assert!(matches!(
            DbColumnFamily::from_name("init_state"),
            DbColumnFamily::InitState
        ));

        // Test event blob writer column families.
        let certified = DbColumnFamily::Certified;
        assert_eq!(certified.name(), "certified_blob_store");
        assert!(matches!(
            DbColumnFamily::from_name("certified_blob_store"),
            DbColumnFamily::Certified
        ));

        let attested = DbColumnFamily::Attested;
        assert_eq!(attested.name(), "attested_blob_store");
        assert!(matches!(
            DbColumnFamily::from_name("attested_blob_store"),
            DbColumnFamily::Attested
        ));

        let pending = DbColumnFamily::Pending;
        assert_eq!(pending.name(), "pending_blob_store");
        assert!(matches!(
            DbColumnFamily::from_name("pending_blob_store"),
            DbColumnFamily::Pending
        ));

        let failed = DbColumnFamily::FailedToAttest;
        assert_eq!(failed.name(), "failed_to_attest_blob_store");
        assert!(matches!(
            DbColumnFamily::from_name("failed_to_attest_blob_store"),
            DbColumnFamily::FailedToAttest
        ));

        // Test unknown/custom column family.
        let custom_cf = DbColumnFamily::from_name("unknown_column_family");
        assert!(
            matches!(custom_cf, DbColumnFamily::Custom(name) if name == "unknown_column_family")
        );

        // Test malformed shard column families.
        let malformed1 = DbColumnFamily::from_name("shard-abc/primary-slivers");
        assert!(matches!(malformed1, DbColumnFamily::Custom(_)));

        let malformed2 = DbColumnFamily::from_name("shard-42/unknown-type");
        assert!(matches!(malformed2, DbColumnFamily::Custom(_)));

        let malformed3 = DbColumnFamily::from_name("shard-42");
        assert!(matches!(malformed3, DbColumnFamily::Custom(_)));
    }

    #[test]
    fn test_db_column_family_roundtrip() {
        // Test that all variants can roundtrip through name() and from_name().
        let test_cases = vec![
            DbColumnFamily::NodeStatus,
            DbColumnFamily::Metadata,
            DbColumnFamily::AggregateBlobInfo,
            DbColumnFamily::PerObjectBlobInfo,
            DbColumnFamily::EventCursor,
            DbColumnFamily::PrimarySliver(100),
            DbColumnFamily::SecondarySliver(200),
            DbColumnFamily::ShardStatus(300),
            DbColumnFamily::ShardSyncProgress(400),
            DbColumnFamily::PendingRecoverSlivers(500),
            DbColumnFamily::EventStore,
            DbColumnFamily::InitState,
            DbColumnFamily::Certified,
            DbColumnFamily::Attested,
            DbColumnFamily::Pending,
            DbColumnFamily::FailedToAttest,
        ];

        for original in test_cases {
            let name = original.name();
            let parsed = DbColumnFamily::from_name(&name);

            // For comparison, we need to handle the fact that both produce the same type.
            match (&original, &parsed) {
                (DbColumnFamily::NodeStatus, DbColumnFamily::NodeStatus) => {}
                (DbColumnFamily::Metadata, DbColumnFamily::Metadata) => {}
                (DbColumnFamily::AggregateBlobInfo, DbColumnFamily::AggregateBlobInfo) => {}
                (DbColumnFamily::PerObjectBlobInfo, DbColumnFamily::PerObjectBlobInfo) => {}
                (DbColumnFamily::EventCursor, DbColumnFamily::EventCursor) => {}
                (DbColumnFamily::PrimarySliver(a), DbColumnFamily::PrimarySliver(b)) => {
                    assert_eq!(a, b, "PrimarySliver shard mismatch");
                }
                (DbColumnFamily::SecondarySliver(a), DbColumnFamily::SecondarySliver(b)) => {
                    assert_eq!(a, b, "SecondarySliver shard mismatch");
                }
                (DbColumnFamily::ShardStatus(a), DbColumnFamily::ShardStatus(b)) => {
                    assert_eq!(a, b, "ShardStatus shard mismatch");
                }
                (DbColumnFamily::ShardSyncProgress(a), DbColumnFamily::ShardSyncProgress(b)) => {
                    assert_eq!(a, b, "ShardSyncProgress shard mismatch");
                }
                (
                    DbColumnFamily::PendingRecoverSlivers(a),
                    DbColumnFamily::PendingRecoverSlivers(b),
                ) => {
                    assert_eq!(a, b, "PendingRecoverSlivers shard mismatch");
                }
                (DbColumnFamily::EventStore, DbColumnFamily::EventStore) => {}
                (DbColumnFamily::InitState, DbColumnFamily::InitState) => {}
                (DbColumnFamily::Certified, DbColumnFamily::Certified) => {}
                (DbColumnFamily::Attested, DbColumnFamily::Attested) => {}
                (DbColumnFamily::Pending, DbColumnFamily::Pending) => {}
                (DbColumnFamily::FailedToAttest, DbColumnFamily::FailedToAttest) => {}
                _ => panic!(
                    "Roundtrip failed for {:?}: got {:?} after parsing '{}'",
                    original, parsed, name
                ),
            }
        }
    }
}
