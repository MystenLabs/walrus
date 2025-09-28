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

/// Type of database to open
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbType {
    /// Main storage database (default)
    Main,
    /// Event processor database
    EventProcessor,
    /// Event blob writer database
    EventBlobWriter,
}

impl DbType {
    /// Get the database type from a path by checking for specific subdirectories
    pub fn from_path(db_path: &Path) -> Self {
        if db_path.ends_with("event_processor") || db_path.ends_with("event_processor/db") {
            DbType::EventProcessor
        } else if db_path.ends_with("event_blob_writer")
            || db_path.ends_with("event_blob_writer/db")
        {
            DbType::EventBlobWriter
        } else {
            DbType::Main
        }
    }
}

/// Get column family options for the main storage database
pub fn get_main_db_cf_options(
    db_config: &DatabaseConfig,
    db_path: &Path,
) -> Result<Vec<(String, Options)>> {
    let mut db_opts = Options::from(&db_config.global());
    db_opts.create_missing_column_families(true);

    // Get existing shard IDs if any
    let existing_shards_ids = ShardStorage::existing_cf_shards_ids(db_path, &db_opts);

    let mut cf_options = vec![
        (
            node_status_cf_name().to_string(),
            node_status_options(db_config),
        ),
        (metadata_cf_name().to_string(), metadata_options(db_config)),
        (
            aggregate_blob_info_cf_name().to_string(),
            blob_info_cf_options(db_config),
        ),
        (
            per_object_blob_info_cf_name().to_string(),
            per_object_blob_info_cf_options(db_config),
        ),
    ];

    // Add event cursor with its merge operator
    let (event_cursor_cf_name, event_cursor_options) = event_cursor_table_options(db_config);
    cf_options.push((event_cursor_cf_name.to_string(), event_cursor_options));

    // Add shard-specific column families
    for shard_id in existing_shards_ids {
        cf_options.push((
            primary_slivers_column_family_name(shard_id),
            primary_slivers_column_family_options(db_config),
        ));
        cf_options.push((
            secondary_slivers_column_family_name(shard_id),
            secondary_slivers_column_family_options(db_config),
        ));
        cf_options.push((
            shard_status_column_family_name(shard_id),
            shard_status_column_family_options(db_config),
        ));
        cf_options.push((
            shard_sync_progress_column_family_name(shard_id),
            shard_sync_progress_column_family_options(db_config),
        ));
        cf_options.push((
            pending_recover_slivers_column_family_name(shard_id),
            pending_recover_slivers_column_family_options(db_config),
        ));
    }

    Ok(cf_options)
}

/// Get column family options for the event processor database
pub fn get_event_processor_db_cf_options(db_config: &DatabaseConfig) -> Vec<(String, Options)> {
    vec![
        (
            event_processor_constants::CHECKPOINT_STORE.to_string(),
            db_config.checkpoint_store().to_options(),
        ),
        (
            event_processor_constants::WALRUS_PACKAGE_STORE.to_string(),
            db_config.walrus_package_store().to_options(),
        ),
        (
            event_processor_constants::COMMITTEE_STORE.to_string(),
            db_config.committee_store().to_options(),
        ),
        (
            event_processor_constants::EVENT_STORE.to_string(),
            db_config.event_store().to_options(),
        ),
        (
            event_processor_constants::INIT_STATE.to_string(),
            db_config.init_state().to_options(),
        ),
    ]
}

/// Get column family options for the event blob writer database
pub fn get_event_blob_writer_db_cf_options(db_config: &DatabaseConfig) -> Vec<(String, Options)> {
    vec![
        (
            pending_cf_name().to_string(),
            db_config.pending().to_options(),
        ),
        (
            attested_cf_name().to_string(),
            db_config.attested().to_options(),
        ),
        (
            certified_cf_name().to_string(),
            db_config.certified().to_options(),
        ),
        (
            failed_to_attest_cf_name().to_string(),
            db_config.failed_to_attest().to_options(),
        ),
    ]
}

/// Open main storage database in read-only mode with proper options
pub fn open_main_db_readonly(db_path: &Path, db_config: &DatabaseConfig) -> Result<DB> {
    let cf_options = get_main_db_cf_options(db_config, db_path)?;

    // First, list existing column families
    let existing_cfs =
        DB::list_cf(&Options::default(), db_path).unwrap_or_else(|_| vec!["default".to_string()]);

    // Filter to only include CFs that exist
    let cfs_to_open: Vec<(String, Options)> = cf_options
        .into_iter()
        .filter(|(name, _)| existing_cfs.iter().any(|cf| cf == name))
        .collect();

    if cfs_to_open.is_empty() {
        // Open without column families if none exist
        DB::open_for_read_only(&Options::default(), db_path, false)
            .map_err(|e| anyhow::anyhow!("Failed to open database: {}", e))
    } else {
        // Open with the filtered column families and their options
        DB::open_cf_with_opts_for_read_only(&Options::default(), db_path, cfs_to_open, false)
            .map_err(|e| anyhow::anyhow!("Failed to open database: {}", e))
    }
}

/// Open event processor database in read-only mode with proper options
pub fn open_event_processor_db_readonly(db_path: &Path, db_config: &DatabaseConfig) -> Result<DB> {
    let cf_options = get_event_processor_db_cf_options(db_config);

    // First, list existing column families
    let existing_cfs =
        DB::list_cf(&Options::default(), db_path).unwrap_or_else(|_| vec!["default".to_string()]);

    // Filter to only include CFs that exist
    let cfs_to_open: Vec<(String, Options)> = cf_options
        .into_iter()
        .filter(|(name, _)| existing_cfs.iter().any(|cf| cf == name))
        .collect();

    if cfs_to_open.is_empty() {
        DB::open_for_read_only(&Options::default(), db_path, false)
            .map_err(|e| anyhow::anyhow!("Failed to open database: {}", e))
    } else {
        // Convert to the format expected by RocksDB (with references)
        let cfs_with_refs: Vec<(&str, Options)> = cfs_to_open
            .iter()
            .map(|(name, opts)| (name.as_str(), opts.clone()))
            .collect();

        DB::open_cf_with_opts_for_read_only(&Options::default(), db_path, cfs_with_refs, false)
            .map_err(|e| anyhow::anyhow!("Failed to open database: {}", e))
    }
}

/// Open event blob writer database in read-only mode with proper options
pub fn open_event_blob_writer_db_readonly(
    db_path: &Path,
    db_config: &DatabaseConfig,
) -> Result<DB> {
    let cf_options = get_event_blob_writer_db_cf_options(db_config);

    // First, list existing column families
    let existing_cfs =
        DB::list_cf(&Options::default(), db_path).unwrap_or_else(|_| vec!["default".to_string()]);

    // Filter to only include CFs that exist
    let cfs_to_open: Vec<(String, Options)> = cf_options
        .into_iter()
        .filter(|(name, _)| existing_cfs.iter().any(|cf| cf == name))
        .collect();

    if cfs_to_open.is_empty() {
        DB::open_for_read_only(&Options::default(), db_path, false)
            .map_err(|e| anyhow::anyhow!("Failed to open database: {}", e))
    } else {
        // Convert to the format expected by RocksDB (with references)
        let cfs_with_refs: Vec<(&str, Options)> = cfs_to_open
            .iter()
            .map(|(name, opts)| (name.as_str(), opts.clone()))
            .collect();

        DB::open_cf_with_opts_for_read_only(&Options::default(), db_path, cfs_with_refs, false)
            .map_err(|e| anyhow::anyhow!("Failed to open database: {}", e))
    }
}

/// Open database in read-only mode based on the database type
pub fn open_db_readonly(db_path: &Path, db_type: DbType, db_config: &DatabaseConfig) -> Result<DB> {
    match db_type {
        DbType::Main => open_main_db_readonly(db_path, db_config),
        DbType::EventProcessor => open_event_processor_db_readonly(db_path, db_config),
        DbType::EventBlobWriter => open_event_blob_writer_db_readonly(db_path, db_config),
    }
}
