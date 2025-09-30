// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Database options module for providing consistent column family options across the codebase.
//!
//! This module exports functions that return the proper RocksDB options for each column family,
//! including any custom merge operators. This ensures that databases can be opened consistently
//! both in production code and in debugging tools like db_tools.

use std::path::{Path, PathBuf};

use anyhow::Result;
use rocksdb::DB;

use super::{DatabaseConfig, db_column_family::DatabaseDef};

/// Type of database to open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbType {
    /// Main storage database.
    Main,
    /// Event processor database.
    EventProcessor,
    /// Event blob writer database.
    EventBlobWriter,
}

impl DbType {
    /// Get the database type from a path by checking for specific subdirectories.
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

/// Open database in read-only mode based on the database type.
pub fn open_db_readonly(db_path: &Path, db_type: DbType, db_config: &DatabaseConfig) -> Result<DB> {
    let db_def = match db_type {
        DbType::Main => DatabaseDef::MainStorage {
            path: PathBuf::from(db_path),
            config: db_config.clone(),
        },
        DbType::EventProcessor => DatabaseDef::EventProcessor {
            path: PathBuf::from(db_path),
            config: db_config.clone(),
        },
        DbType::EventBlobWriter => DatabaseDef::EventBlobWriter {
            path: PathBuf::from(db_path),
            config: db_config.clone(),
        },
    };

    db_def.open_readonly()
}

/// Open main storage database in read-only mode with proper options.
pub fn open_main_db_readonly(db_path: &Path, db_config: &DatabaseConfig) -> Result<DB> {
    open_db_readonly(db_path, DbType::Main, db_config)
}

/// Open event processor database in read-only mode with proper options.
pub fn open_event_processor_db_readonly(db_path: &Path, db_config: &DatabaseConfig) -> Result<DB> {
    open_db_readonly(db_path, DbType::EventProcessor, db_config)
}

/// Open event blob writer database in read-only mode with proper options.
pub fn open_event_blob_writer_db_readonly(
    db_path: &Path,
    db_config: &DatabaseConfig,
) -> Result<DB> {
    open_db_readonly(db_path, DbType::EventBlobWriter, db_config)
}

/// Open database with only specific column families in read-only mode.
pub fn open_db_cf_readonly(
    db_path: &Path,
    cf_names: &[&str],
    _db_config: &DatabaseConfig,
) -> Result<DB> {
    let mut db_opts = rocksdb::Options::default();

    // Apply common options for read-only access
    db_opts.set_max_open_files(100);
    db_opts.set_allow_mmap_reads(true);

    // Don't create if missing for read-only operations
    db_opts.create_if_missing(false);
    db_opts.create_missing_column_families(false);

    // Try to get existing column families
    let existing_cfs = DB::list_cf(&db_opts, db_path)?;

    // Only open the requested column families that exist
    let mut cfs_to_open = Vec::new();
    for cf_name in cf_names {
        if existing_cfs.contains(&cf_name.to_string()) {
            let mut cf_opts = rocksdb::Options::default();
            // Apply basic CF options for read-only
            cf_opts.set_allow_mmap_reads(true);
            cfs_to_open.push((*cf_name, cf_opts));
        }
    }

    // Open the database with only the requested CFs in read-only mode
    if cfs_to_open.is_empty() {
        // If no requested CFs exist, open without column families
        DB::open_for_read_only(&db_opts, db_path, false)
            .map_err(|e| anyhow::anyhow!("Failed to open database: {}", e))
    } else {
        // Open with only the requested column families
        let cf_names: Vec<&str> = cfs_to_open.iter().map(|(name, _)| *name).collect();
        DB::open_cf_for_read_only(&db_opts, db_path, cf_names, false)
            .map_err(|e| anyhow::anyhow!("Failed to open database: {}", e))
    }
}
