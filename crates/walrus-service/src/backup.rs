// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Walrus blob backup service.
//!
//! The backup service runs alongside the Walrus network. It should not be relied upon during
//! typical network operation. It is an extra layer of protection to assist recovery in the event of
//! an unforeseen systemic failure.
//!
//! The backup service is comprised of three major components, each with different roles.
//!
//!   - Orchestrator: The orchestrator consumes Sui events, filters them down to Walrus blob
//!     lifetime-related events, and then schedules the blobs for backup by writing to the
//!     `blob_state` table in the backup database. The orchestrator will upsert blobs into the table
//!     with a `'waiting'` state, which signals to the backup fetcher that the blob is ready to be
//!     archived.
//!   - Fetcher: Many fetchers run simultaneously, pulling `'waiting'` blobs from the backup
//!     database and archiving them in the backup storage. The fetcher will update the `blob_state`
//!     table with the `'archived'` state once the blob has been successfully archived.
//!   - Garbage Collector: The garbage collector runs continuously, detecting and deleting expired
//!     blobs. The garbage collector will update the `blob_state` table with the `'deleted'` state
//!     once the blob has been successfully deleted.
//!
//! The `blob_state` table is the primary table used by the backup service. The available states are
//!
//!   - `'waiting'`  - the blob is ready to be fetched from the Walrus network and then archived in
//!     backup storage. Note that there is a race condition between archiving and expiration. This
//!     is acceptable as the garbage collector will clean up any expired blobs, and once a blob is
//!     expired, it need not be archived anyway.
//!   - `'archived'` - the blob has been archived in the backup storage.
//!   - `'deleted'`  - the blob has been deleted from the backup storage.
//!
//! Note that the Postgres `blob_state` table is currently an overly conservative approximation to
//! the actual RocksDB `blob_info` table which is maintained by Walrus storage nodes. Blobs in the
//! backup archive might live longer than necessary. This is acceptable as the backup service is a
//! last resort and does not need to be as efficient as the storage nodes themselves.
//!
mod config;
pub use config::{BACKUP_BLOB_ARCHIVE_SUBDIR, BackupConfig};

#[cfg(feature = "backup")]
mod garbage_collector;

#[cfg(feature = "backup")]
mod models;

#[cfg(feature = "backup")]
mod schema;

#[cfg(feature = "backup")]
mod service;

#[cfg(feature = "backup")]
mod metrics;

#[cfg(feature = "backup")]
pub use self::{
    garbage_collector::start_backup_garbage_collector,
    service::{
        VERSION,
        run_backup_database_migrations,
        start_backup_fetcher,
        start_backup_orchestrator,
    },
};
