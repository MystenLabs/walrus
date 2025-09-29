// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Type-safe column family definitions for database operations.

#![allow(missing_docs)]
#![allow(missing_debug_implementations)]

use std::path::Path;

use anyhow::Result;
use rocksdb::{DB, Options};

use super::{
    DatabaseConfig,
    event_blob_writer::{
        attested_cf_name,
        certified_cf_name,
        failed_to_attest_cf_name,
        pending_cf_name,
    },
    storage::{
        ShardStorage,
        blob_info::{blob_info_cf_options, per_object_blob_info_cf_options},
        constants::{
            aggregate_blob_info_cf_name,
            metadata_cf_name,
            node_status_cf_name,
            pending_recover_slivers_column_family_name,
            per_object_blob_info_cf_name,
            primary_slivers_column_family_name,
            secondary_slivers_column_family_name,
            shard_status_column_family_name,
            shard_sync_progress_column_family_name,
        },
        event_cursor_table_options,
        metadata_options,
        node_status_options,
        pending_recover_slivers_column_family_options,
        primary_slivers_column_family_options,
        secondary_slivers_column_family_options,
        shard_status_column_family_options,
        shard_sync_progress_column_family_options,
    },
};
use crate::event::event_processor::db::constants as event_processor_constants;

/// Column family definition with name and options.
#[derive(Clone)]
pub struct ColumnFamilyDef {
    pub name: String,
    pub options: Options,
}

impl ColumnFamilyDef {
    pub fn new(name: impl Into<String>, options: Options) -> Self {
        Self {
            name: name.into(),
            options,
        }
    }
}

/// Database type with its column family definitions.
pub enum DatabaseDef {
    /// Main storage database with all its column families.
    MainStorage {
        path: PathBuf,
        config: DatabaseConfig,
    },
    /// Event processor database with all its column families.
    EventProcessor {
        path: PathBuf,
        config: DatabaseConfig,
    },
    /// Event blob writer database with all its column families.
    EventBlobWriter {
        path: PathBuf,
        config: DatabaseConfig,
    },
}

use std::path::PathBuf;

impl DatabaseDef {
    /// Get all column family definitions for this database.
    pub fn get_column_families(&self) -> Result<Vec<ColumnFamilyDef>> {
        match self {
            Self::MainStorage { path, config } => {
                let mut db_opts = Options::from(&config.global());
                db_opts.create_missing_column_families(true);

                // Get existing shard IDs if any.
                let existing_shards_ids = ShardStorage::existing_cf_shards_ids(path, &db_opts);

                let mut cfs = vec![
                    ColumnFamilyDef::new(node_status_cf_name(), node_status_options(config)),
                    ColumnFamilyDef::new(metadata_cf_name(), metadata_options(config)),
                    ColumnFamilyDef::new(
                        aggregate_blob_info_cf_name(),
                        blob_info_cf_options(config),
                    ),
                    ColumnFamilyDef::new(
                        per_object_blob_info_cf_name(),
                        per_object_blob_info_cf_options(config),
                    ),
                ];

                // Add event cursor with its merge operator.
                let (event_cursor_cf_name, event_cursor_options) =
                    event_cursor_table_options(config);
                cfs.push(ColumnFamilyDef::new(
                    event_cursor_cf_name,
                    event_cursor_options,
                ));

                // Add shard-specific column families.
                for shard_id in existing_shards_ids {
                    cfs.push(ColumnFamilyDef::new(
                        primary_slivers_column_family_name(shard_id),
                        primary_slivers_column_family_options(config),
                    ));
                    cfs.push(ColumnFamilyDef::new(
                        secondary_slivers_column_family_name(shard_id),
                        secondary_slivers_column_family_options(config),
                    ));
                    cfs.push(ColumnFamilyDef::new(
                        shard_status_column_family_name(shard_id),
                        shard_status_column_family_options(config),
                    ));
                    cfs.push(ColumnFamilyDef::new(
                        shard_sync_progress_column_family_name(shard_id),
                        shard_sync_progress_column_family_options(config),
                    ));
                    cfs.push(ColumnFamilyDef::new(
                        pending_recover_slivers_column_family_name(shard_id),
                        pending_recover_slivers_column_family_options(config),
                    ));
                }

                Ok(cfs)
            }
            Self::EventProcessor { config, .. } => Ok(vec![
                ColumnFamilyDef::new(
                    event_processor_constants::CHECKPOINT_STORE,
                    config.checkpoint_store().to_options(),
                ),
                ColumnFamilyDef::new(
                    event_processor_constants::WALRUS_PACKAGE_STORE,
                    config.walrus_package_store().to_options(),
                ),
                ColumnFamilyDef::new(
                    event_processor_constants::COMMITTEE_STORE,
                    config.committee_store().to_options(),
                ),
                ColumnFamilyDef::new(
                    event_processor_constants::EVENT_STORE,
                    config.event_store().to_options(),
                ),
                ColumnFamilyDef::new(
                    event_processor_constants::INIT_STATE,
                    config.init_state().to_options(),
                ),
            ]),
            Self::EventBlobWriter { config, .. } => Ok(vec![
                ColumnFamilyDef::new(pending_cf_name(), config.pending().to_options()),
                ColumnFamilyDef::new(attested_cf_name(), config.attested().to_options()),
                ColumnFamilyDef::new(certified_cf_name(), config.certified().to_options()),
                ColumnFamilyDef::new(
                    failed_to_attest_cf_name(),
                    config.failed_to_attest().to_options(),
                ),
            ]),
        }
    }

    /// Open this database in read-only mode.
    pub fn open_readonly(&self) -> Result<DB> {
        let (path, cf_defs) = match self {
            Self::MainStorage { path, .. }
            | Self::EventProcessor { path, .. }
            | Self::EventBlobWriter { path, .. } => (path.as_path(), self.get_column_families()?),
        };

        open_db_with_cfs_readonly(path, cf_defs)
    }
}

/// Helper function to open a database with given column family definitions.
fn open_db_with_cfs_readonly(db_path: &Path, cf_defs: Vec<ColumnFamilyDef>) -> Result<DB> {
    // First, list existing column families.
    let existing_cfs =
        DB::list_cf(&Options::default(), db_path).unwrap_or_else(|_| vec!["default".to_string()]);

    // Filter to only include CFs that exist.
    let cfs_to_open: Vec<ColumnFamilyDef> = cf_defs
        .into_iter()
        .filter(|def| existing_cfs.iter().any(|cf| cf == &def.name))
        .collect();

    if cfs_to_open.is_empty() {
        // Open without column families if none exist.
        DB::open_for_read_only(&Options::default(), db_path, false)
            .map_err(|e| anyhow::anyhow!("Failed to open database: {}", e))
    } else {
        // Convert to the format expected by RocksDB.
        let cfs_with_refs: Vec<(&str, Options)> = cfs_to_open
            .iter()
            .map(|def| (def.name.as_str(), def.options.clone()))
            .collect();

        DB::open_cf_with_opts_for_read_only(&Options::default(), db_path, cfs_with_refs, false)
            .map_err(|e| anyhow::anyhow!("Failed to open database: {}", e))
    }
}
