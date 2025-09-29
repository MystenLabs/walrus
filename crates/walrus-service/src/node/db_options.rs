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
