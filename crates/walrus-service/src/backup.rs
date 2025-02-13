// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Walrus blob backup service.

mod config;
pub use config::{BackupConfig, BACKUP_BLOB_ARCHIVE_SUBDIR};

#[cfg(feature = "backup")]
mod models;

#[cfg(feature = "backup")]
mod schema;

#[cfg(feature = "backup")]
mod service;

#[cfg(feature = "backup")]
mod metrics;

#[cfg(feature = "backup")]
pub use service::{
    run_backup_database_migrations,
    start_backup_fetcher,
    start_backup_orchestrator,
    VERSION,
};
