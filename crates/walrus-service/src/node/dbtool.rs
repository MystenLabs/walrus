// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Tools for inspecting and maintaining the RocksDB database.

use std::{collections::BTreeMap, path::PathBuf};

use anyhow::Result;
use bincode::Options;
use clap::Subcommand;
use rocksdb::{DB, DBRecoveryMode, Options as RocksdbOptions, ReadOptions};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use sui_types::base_types::ObjectID;
use typed_store::rocks::be_fix_int_ser;
use walrus_core::{
    BlobId, Epoch, ShardIndex,
    metadata::{BlobMetadata, BlobMetadataApi},
};

use super::DatabaseTableOptionsFactory;
use crate::{
    event::{
        event_processor::db::constants::{self as event_processor_constants},
        events::{InitState, PositionedStreamEvent},
    },
    node::{
        DatabaseConfig,
        event_blob_writer::{
            AttestedEventBlobMetadata, CertifiedEventBlobMetadata, FailedToAttestEventBlobMetadata,
            PendingEventBlobMetadata, attested_cf_name, certified_cf_name,
            failed_to_attest_cf_name, pending_cf_name,
        },
        storage::{
            PrimarySliverData, SecondarySliverData,
            blob_info::{
                BlobInfo, CertifiedBlobInfoApi, PerObjectBlobInfo, blob_info_cf_options,
                per_object_blob_info_cf_options, per_object_pooled_blob_info_cf_options,
            },
            constants::{
                aggregate_blob_info_cf_name, event_cursor_cf_name, event_index_cf_name,
                garbage_collector_table_cf_name, metadata_cf_name, node_status_cf_name,
                per_object_blob_info_cf_name, per_object_pooled_blob_info_cf_name,
                primary_slivers_column_family_name, secondary_slivers_column_family_name,
            },
        },
    },
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "kebab-case")]
/// WAL recovery policy used by the read-only recovery probe.
pub enum ProbeWalRecoveryMode {
    /// Tolerate incomplete records only at the end of the WAL.
    TolerateCorruptedTailRecords,
    /// Require strict consistency and fail on any corruption.
    AbsoluteConsistency,
    /// Recover to the last consistent point before corruption.
    PointInTime,
    /// Skip corrupted records and continue replaying later valid records.
    SkipAnyCorruptedRecord,
}

impl ProbeWalRecoveryMode {
    fn as_rocksdb(self) -> DBRecoveryMode {
        match self {
            Self::TolerateCorruptedTailRecords => DBRecoveryMode::TolerateCorruptedTailRecords,
            Self::AbsoluteConsistency => DBRecoveryMode::AbsoluteConsistency,
            Self::PointInTime => DBRecoveryMode::PointInTime,
            Self::SkipAnyCorruptedRecord => DBRecoveryMode::SkipAnyCorruptedRecord,
        }
    }
}

impl std::fmt::Display for ProbeWalRecoveryMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::TolerateCorruptedTailRecords => "tolerate-corrupted-tail-records",
            Self::AbsoluteConsistency => "absolute-consistency",
            Self::PointInTime => "point-in-time",
            Self::SkipAnyCorruptedRecord => "skip-any-corrupted-record",
        };
        f.write_str(name)
    }
}

/// Database inspection and maintenance tools.
#[derive(Subcommand, Debug, Clone, Serialize, Deserialize)]
#[serde_as]
#[command(rename_all = "kebab-case")]
pub enum DbToolCommands {
    /// Repair a corrupted RocksDB database due to non-clean shutdowns.
    RepairDb {
        /// Path to the RocksDB database directory.
        #[arg(long)]
        db_path: PathBuf,
    },

    /// Scan events from the event_store table in RocksDB.
    ScanEvents {
        /// Path to the RocksDB database directory.
        #[arg(long)]
        db_path: PathBuf,
        /// Start index of the events to scan.
        #[arg(long)]
        start_event_index: u64,
        /// Number of events to scan.
        #[arg(long, default_value = "1")]
        count: usize,
    },

    /// Read blob info from the RocksDB database.
    ReadBlobInfo {
        /// Path to the RocksDB database directory.
        #[arg(long)]
        db_path: PathBuf,
        /// Start blob ID in URL-safe base64 format (no padding).
        #[arg(long)]
        #[serde_as(as = "Option<DisplayFromStr>")]
        start_blob_id: Option<BlobId>,
        /// Number of entries to scan.
        #[arg(long, default_value = "1")]
        count: usize,
    },

    /// Read object blob info from the RocksDB database.
    ReadObjectBlobInfo {
        /// Path to the RocksDB database directory.
        #[arg(long)]
        db_path: PathBuf,
        /// Start object ID to read.
        #[arg(long)]
        #[serde_as(as = "Option<DisplayFromStr>")]
        start_object_id: Option<ObjectID>,
        /// Count of objects to read.
        #[arg(long, default_value = "1")]
        count: usize,
    },

    /// Count the number of certified blobs in the RocksDB database.
    CountCertifiedBlobs {
        /// Path to the RocksDB database directory.
        #[arg(long)]
        db_path: PathBuf,
        /// Epoch the blobs are in certified status.
        #[arg(long)]
        epoch: Epoch,
    },

    /// Drop a column family from the RocksDB database. This can only be called when the storage
    /// node is stopped.
    DropColumnFamilies {
        /// Path to the RocksDB database directory.
        #[arg(long)]
        db_path: PathBuf,
        /// Column families to drop.
        #[arg(num_args = 1..)]
        column_family_names: Vec<String>,
    },

    /// List all column families in the RocksDB database.
    ListColumnFamilies {
        /// Path to the RocksDB database directory.
        #[arg(long)]
        db_path: PathBuf,
    },

    /// Open the RocksDB database read-only with a selectable WAL recovery mode and report what
    /// data is visible without persisting any recovery result.
    ProbeRecovery {
        /// Path to the RocksDB database directory.
        #[arg(long)]
        db_path: PathBuf,
        /// WAL recovery mode to use for the read-only probe.
        #[arg(long, value_enum, default_value_t = ProbeWalRecoveryMode::PointInTime)]
        wal_recovery_mode: ProbeWalRecoveryMode,
        /// Count entries by iterating each reported column family instead of using
        /// `rocksdb.estimate-num-keys`.
        #[arg(long, default_value_t = false)]
        exact_counts: bool,
    },

    /// Open the RocksDB database read-write with a selectable WAL recovery mode and persist the
    /// recovery result without starting the full node.
    RecoverDb {
        /// Path to the RocksDB database directory.
        #[arg(long)]
        db_path: PathBuf,
        /// WAL recovery mode to use for the writable recovery.
        #[arg(long, value_enum, default_value_t = ProbeWalRecoveryMode::PointInTime)]
        wal_recovery_mode: ProbeWalRecoveryMode,
        /// Count entries by iterating each reported column family instead of using
        /// `rocksdb.estimate-num-keys`.
        #[arg(long, default_value_t = false)]
        exact_counts: bool,
    },

    /// Scan blob metadata from the RocksDB database.
    ReadBlobMetadata {
        /// Path to the RocksDB database directory.
        #[arg(long)]
        db_path: PathBuf,
        /// Start blob ID in URL-safe base64 format (no padding).
        #[arg(long)]
        #[serde_as(as = "Option<DisplayFromStr>")]
        start_blob_id: Option<BlobId>,
        /// Number of entries to scan.
        #[arg(long, default_value = "1")]
        count: usize,
        /// Output size only.
        #[arg(long, default_value = "false")]
        output_size_only: bool,
    },

    /// Read primary slivers from the RocksDB database.
    ReadPrimarySlivers {
        /// Path to the RocksDB database directory.
        #[arg(long)]
        db_path: PathBuf,
        /// Start blob ID in URL-safe base64 format (no padding).
        #[arg(long)]
        #[serde_as(as = "Option<DisplayFromStr>")]
        start_blob_id: Option<BlobId>,
        /// Number of entries to scan.
        #[arg(long, default_value = "1")]
        count: usize,
        /// Shard index to read from.
        #[arg(long)]
        shard_index: u16,
    },

    /// Read secondary slivers from the RocksDB database.
    ReadSecondarySlivers {
        /// Path to the RocksDB database directory.
        #[arg(long)]
        db_path: PathBuf,
        /// Start blob ID in URL-safe base64 format (no padding).
        #[arg(long)]
        #[serde_as(as = "Option<DisplayFromStr>")]
        start_blob_id: Option<BlobId>,
        /// Number of entries to scan.
        #[arg(long, default_value = "1")]
        count: usize,
        /// Shard index to read from.
        #[arg(long)]
        shard_index: u16,
    },

    /// Read event blob writer metadata from the RocksDB database.
    EventBlobWriter {
        /// Path to the RocksDB database directory.
        #[arg(long)]
        db_path: PathBuf,
        /// Commands to read event blob writer metadata.
        #[command(subcommand)]
        command: EventBlobWriterCommands,
    },

    /// Read event processor metadata from the RocksDB database.
    EventProcessor {
        /// Path to the RocksDB database directory.
        #[arg(long)]
        db_path: PathBuf,
        /// Commands to read event processor metadata.
        #[command(subcommand)]
        command: EventProcessorCommands,
    },
}

/// Commands for reading event blob writer metadata.
#[derive(Subcommand, Debug, Clone, Serialize, Deserialize)]
#[serde_as]
#[command(rename_all = "kebab-case")]
pub enum EventBlobWriterCommands {
    /// Read certified event blob metadata.
    ReadCertified,

    /// Read attested event blob metadata.
    ReadAttested,

    /// Read pending event blob metadata.
    ReadPending {
        /// Start sequence number.
        #[arg(long)]
        start_seq: Option<u64>,
        /// Number of entries to scan.
        #[arg(long, default_value = "1")]
        count: usize,
    },

    /// Read failed-to-attest event blob metadata.
    ReadFailedToAttest,
}

/// Commands for reading event processor metadata.
#[derive(Subcommand, Debug, Clone, Serialize, Deserialize)]
#[serde_as]
#[command(rename_all = "kebab-case")]
pub enum EventProcessorCommands {
    /// Read event processor metadata.
    ReadInitState,
}

impl DbToolCommands {
    /// Execute the database tool command.
    pub fn execute(self) -> Result<()> {
        match self {
            Self::RepairDb { db_path } => repair_db(db_path),
            Self::ScanEvents {
                db_path,
                start_event_index,
                count,
            } => scan_events(db_path, start_event_index, count),
            Self::ReadBlobInfo {
                db_path,
                start_blob_id,
                count,
            } => read_blob_info(db_path, start_blob_id, count),
            Self::ReadObjectBlobInfo {
                db_path,
                start_object_id,
                count,
            } => read_object_blob_info(db_path, start_object_id, count),
            Self::CountCertifiedBlobs { db_path, epoch } => count_certified_blobs(db_path, epoch),
            Self::DropColumnFamilies {
                db_path,
                column_family_names,
            } => drop_column_families(db_path, column_family_names),
            Self::ListColumnFamilies { db_path } => list_column_families(db_path),
            Self::ProbeRecovery {
                db_path,
                wal_recovery_mode,
                exact_counts,
            } => probe_recovery(db_path, wal_recovery_mode, exact_counts),
            Self::RecoverDb {
                db_path,
                wal_recovery_mode,
                exact_counts,
            } => recover_db(db_path, wal_recovery_mode, exact_counts),
            Self::ReadBlobMetadata {
                db_path,
                start_blob_id,
                count,
                output_size_only,
            } => read_blob_metadata(db_path, start_blob_id, count, output_size_only),
            Self::ReadPrimarySlivers {
                db_path,
                start_blob_id,
                count,
                shard_index,
            } => read_primary_slivers(db_path, start_blob_id, count, shard_index),
            Self::ReadSecondarySlivers {
                db_path,
                start_blob_id,
                count,
                shard_index,
            } => read_secondary_slivers(db_path, start_blob_id, count, shard_index),
            Self::EventBlobWriter { db_path, command } => match command {
                EventBlobWriterCommands::ReadCertified => read_certified_event_blobs(db_path),
                EventBlobWriterCommands::ReadAttested => read_attested_event_blobs(db_path),
                EventBlobWriterCommands::ReadPending { start_seq, count } => {
                    read_pending_event_blobs(db_path, start_seq, count)
                }
                EventBlobWriterCommands::ReadFailedToAttest => {
                    read_failed_to_attest_event_blobs(db_path)
                }
            },
            Self::EventProcessor { db_path, command } => match command {
                EventProcessorCommands::ReadInitState => read_event_processor_init_state(db_path),
            },
        }
    }
}

fn repair_db(db_path: PathBuf) -> Result<()> {
    let mut opts = RocksdbOptions::default();
    opts.create_if_missing(true);
    opts.set_max_open_files(512_000);
    DB::repair(&opts, db_path).map_err(Into::into)
}

fn scan_events(db_path: PathBuf, start_event_index: u64, count: usize) -> Result<()> {
    println!("Scanning events from event index {start_event_index}");
    let db = DB::open_cf_with_opts_for_read_only(
        &RocksdbOptions::default(),
        db_path,
        [(
            event_processor_constants::EVENT_STORE,
            DatabaseTableOptionsFactory::new(DatabaseConfig::default(), false).event_store(),
        )],
        false,
    )?;
    let cf = db
        .cf_handle(event_processor_constants::EVENT_STORE)
        .expect("event store column family should exist");

    let iter = db.iterator_cf(
        &cf,
        rocksdb::IteratorMode::From(
            &be_fix_int_ser(&start_event_index)?,
            rocksdb::Direction::Forward,
        ),
    );

    let config = bincode::DefaultOptions::new()
        .with_big_endian()
        .with_fixint_encoding();
    let mut scan_count = 0;
    for event in iter {
        let (key, value) = event?;
        let event_index: u64 = config.deserialize(&key)?;
        let event: PositionedStreamEvent = bcs::from_bytes(&value)?;
        println!("Event index: {event_index}. Event: {event:?}");

        scan_count += 1;
        if scan_count >= count {
            break;
        }
    }

    Ok(())
}

fn read_blob_info(db_path: PathBuf, start_blob_id: Option<BlobId>, count: usize) -> Result<()> {
    let blob_info_options = blob_info_cf_options(&DatabaseTableOptionsFactory::new(
        DatabaseConfig::default(),
        false,
    ));
    let db = DB::open_cf_with_opts_for_read_only(
        &RocksdbOptions::default(),
        db_path,
        [(aggregate_blob_info_cf_name(), blob_info_options)],
        false,
    )?;

    let cf = db
        .cf_handle(aggregate_blob_info_cf_name())
        .expect("aggregate blob info column family should exist");

    let iter = if let Some(blob_id) = start_blob_id {
        db.iterator_cf(
            &cf,
            rocksdb::IteratorMode::From(&be_fix_int_ser(&blob_id)?, rocksdb::Direction::Forward),
        )
    } else {
        db.iterator_cf(&cf, rocksdb::IteratorMode::Start)
    };

    for result in iter.take(count) {
        match result {
            Ok((key, value)) => {
                let blob_id: BlobId = bcs::from_bytes(&key)?;
                let blob_info: BlobInfo = bcs::from_bytes(&value)?;
                println!("Blob ID: {blob_id}, BlobInfo: {blob_info:?}");
            }
            Err(e) => {
                println!("Error: {e:?}");
                return Err(e.into());
            }
        }
    }

    Ok(())
}

fn read_object_blob_info(
    db_path: PathBuf,
    start_object_id: Option<ObjectID>,
    count: usize,
) -> Result<()> {
    let per_object_blob_info_options = per_object_blob_info_cf_options(
        &DatabaseTableOptionsFactory::new(DatabaseConfig::default(), false),
    );
    let db = DB::open_cf_with_opts_for_read_only(
        &RocksdbOptions::default(),
        db_path,
        [(per_object_blob_info_cf_name(), per_object_blob_info_options)],
        false,
    )?;

    let cf = db
        .cf_handle(per_object_blob_info_cf_name())
        .expect("per-object blob info column family should exist");

    let iter = if let Some(object_id) = start_object_id {
        db.iterator_cf(
            &cf,
            rocksdb::IteratorMode::From(&be_fix_int_ser(&object_id)?, rocksdb::Direction::Forward),
        )
    } else {
        db.iterator_cf(&cf, rocksdb::IteratorMode::Start)
    };

    for result in iter.take(count) {
        match result {
            Ok((key, value)) => {
                let object_id: ObjectID = bcs::from_bytes(&key)?;
                let blob_info: PerObjectBlobInfo = bcs::from_bytes(&value)?;
                println!("Object ID: {object_id}, PerObjectBlobInfo: {blob_info:?}");
            }
            Err(e) => {
                println!("Error: {e:?}");
                return Err(e.into());
            }
        }
    }

    Ok(())
}

fn count_certified_blobs(db_path: PathBuf, epoch: Epoch) -> Result<()> {
    let blob_info_options = blob_info_cf_options(&DatabaseTableOptionsFactory::new(
        DatabaseConfig::default(),
        false,
    ));
    let db = DB::open_cf_with_opts_for_read_only(
        &RocksdbOptions::default(),
        db_path,
        [(aggregate_blob_info_cf_name(), blob_info_options)],
        false,
    )?;

    let cf = db
        .cf_handle(aggregate_blob_info_cf_name())
        .expect("aggregate blob info column family should exist");

    // Scan all the blob info and count the certified ones
    let iter = db.iterator_cf(&cf, rocksdb::IteratorMode::Start);

    let mut certified_count = 0;
    let mut scan_count = 0;
    for blob_info_raw in iter {
        let (_key, value) = blob_info_raw?;
        let blob_info: BlobInfo = bcs::from_bytes(&value)?;
        if blob_info.is_certified(epoch) {
            certified_count += 1;
        }

        scan_count += 1;
        if scan_count % 10000 == 0 {
            println!("Scanned {scan_count} blobs. Found {certified_count} certified blobs");
        }
    }

    println!("Number of certified blobs: {certified_count}. Scanned {scan_count} blobs");
    Ok(())
}

/// Drop a column family from the RocksDB database.
fn drop_column_families(db_path: PathBuf, column_family_names: Vec<String>) -> Result<()> {
    let column_families = DB::list_cf(&RocksdbOptions::default(), &db_path)?;
    let db_kind = detect_probe_db_kind(&column_families);

    let mut db_opts = RocksdbOptions::default();
    db_opts.set_max_open_files(512_000);

    let db = DB::open_cf_with_opts(&db_opts, &db_path, cf_options(&column_families, db_kind))
        .inspect_err(|_| {
            println!(
                "failed to open database; \
            make sure to stop the storage node before attempting to drop column families"
            )
        })?;

    for column_family_name in column_family_names {
        println!("Dropping column family: {column_family_name}");
        match db.drop_cf(column_family_name.as_str()) {
            Ok(()) => println!("Success."),
            Err(e) => println!("Failed to drop column family: {e:?}"),
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProbeDbKind {
    Storage,
    EventProcessor,
    Unknown,
}

fn list_column_families(db_path: PathBuf) -> Result<()> {
    let result = rocksdb::DB::list_cf(&RocksdbOptions::default(), db_path);
    if let Ok(column_families) = result {
        println!("Column families: {column_families:?}");
    } else {
        println!("Failed to get column families: {result:?}");
    }

    Ok(())
}

#[derive(Debug, Default)]
struct StorageShardProbeStats {
    primary: Option<u64>,
    secondary: Option<u64>,
    pending_recover: Option<u64>,
    status: Option<u64>,
    sync_progress: Option<u64>,
}

fn probe_recovery(
    db_path: PathBuf,
    wal_recovery_mode: ProbeWalRecoveryMode,
    exact_counts: bool,
) -> Result<()> {
    let (column_families, db_kind, db_opts) = prepare_recovery_open(&db_path, wal_recovery_mode)?;
    let cf_options = cf_options(&column_families, db_kind);
    let db = DB::open_cf_with_opts_for_read_only(&db_opts, &db_path, cf_options, false)?;

    println!("Opened DB read-only at {}", db_path.display());
    println!("WAL recovery mode: {wal_recovery_mode}");
    println!("Probe is non-mutating: read-only open does not persist recovery state.");
    println!(
        "Count mode: {}",
        if exact_counts {
            "exact iteration"
        } else {
            "rocksdb.estimate-num-keys"
        }
    );
    println!("Detected DB kind: {}", probe_db_kind_name(db_kind));
    println!("Column families discovered: {}", column_families.len());

    match db_kind {
        ProbeDbKind::Storage => report_storage_probe(&db, &column_families, exact_counts)?,
        ProbeDbKind::EventProcessor => report_generic_probe(
            &db,
            &column_families,
            exact_counts,
            Some(&event_processor_headings()),
        )?,
        ProbeDbKind::Unknown => report_generic_probe(&db, &column_families, exact_counts, None)?,
    }

    Ok(())
}

fn recover_db(
    db_path: PathBuf,
    wal_recovery_mode: ProbeWalRecoveryMode,
    exact_counts: bool,
) -> Result<()> {
    let (column_families, db_kind, db_opts) = prepare_recovery_open(&db_path, wal_recovery_mode)?;
    let cf_options = cf_options(&column_families, db_kind);
    let db = DB::open_cf_with_opts(&db_opts, &db_path, cf_options).inspect_err(|_| {
        println!(concat!(
            "failed to open database for recovery; ",
            "make sure the storage node is stopped before running recover-db",
        ))
    })?;

    println!("Opened DB read-write at {}", db_path.display());
    println!("WAL recovery mode: {wal_recovery_mode}");
    println!("Recovery is mutating: successful open persists the recovered DB state.");
    println!(
        "Count mode: {}",
        if exact_counts {
            "exact iteration"
        } else {
            "rocksdb.estimate-num-keys"
        }
    );
    println!("Detected DB kind: {}", probe_db_kind_name(db_kind));
    println!("Column families discovered: {}", column_families.len());

    match db_kind {
        ProbeDbKind::Storage => report_storage_probe(&db, &column_families, exact_counts)?,
        ProbeDbKind::EventProcessor => report_generic_probe(
            &db,
            &column_families,
            exact_counts,
            Some(&event_processor_headings()),
        )?,
        ProbeDbKind::Unknown => report_generic_probe(&db, &column_families, exact_counts, None)?,
    }

    drop(db);
    println!();
    println!("Recovery committed successfully.");

    Ok(())
}

fn prepare_recovery_open(
    db_path: &std::path::Path,
    wal_recovery_mode: ProbeWalRecoveryMode,
) -> Result<(Vec<String>, ProbeDbKind, RocksdbOptions)> {
    let column_families = DB::list_cf(&RocksdbOptions::default(), db_path)?;
    let db_kind = detect_probe_db_kind(&column_families);

    let mut db_opts = RocksdbOptions::default();
    db_opts.set_max_open_files(512_000);
    db_opts.set_wal_recovery_mode(wal_recovery_mode.as_rocksdb());

    Ok((column_families, db_kind, db_opts))
}
fn detect_probe_db_kind(column_families: &[String]) -> ProbeDbKind {
    if column_families.iter().any(|cf| {
        cf == aggregate_blob_info_cf_name()
            || cf == per_object_blob_info_cf_name()
            || cf == metadata_cf_name()
            || cf.starts_with("shard-")
    }) {
        ProbeDbKind::Storage
    } else if column_families.iter().any(|cf| {
        cf == event_processor_constants::CHECKPOINT_STORE
            || cf == event_processor_constants::WALRUS_PACKAGE_STORE
            || cf == event_processor_constants::COMMITTEE_STORE
            || cf == event_processor_constants::EVENT_STORE
            || cf == event_processor_constants::INIT_STATE
    }) {
        ProbeDbKind::EventProcessor
    } else {
        ProbeDbKind::Unknown
    }
}

fn probe_db_kind_name(db_kind: ProbeDbKind) -> &'static str {
    match db_kind {
        ProbeDbKind::Storage => "storage",
        ProbeDbKind::EventProcessor => "event-processor",
        ProbeDbKind::Unknown => "unknown",
    }
}
fn cf_options(column_families: &[String], db_kind: ProbeDbKind) -> Vec<(&str, RocksdbOptions)> {
    let db_config = DatabaseConfig::default();
    let factory = DatabaseTableOptionsFactory::new(db_config, db_kind == ProbeDbKind::Storage);
    column_families
        .iter()
        .map(|cf_name| {
            (
                cf_name.as_str(),
                cf_options_for_name(cf_name, db_kind, &factory),
            )
        })
        .collect()
}

fn cf_options_for_name(
    cf_name: &str,
    db_kind: ProbeDbKind,
    factory: &DatabaseTableOptionsFactory,
) -> RocksdbOptions {
    match db_kind {
        ProbeDbKind::Storage => {
            if let Some((_, suffix)) = parse_shard_cf_name(cf_name) {
                return match suffix {
                    "primary-slivers" | "secondary-slivers" => factory.shard(),
                    "status" => factory.shard_status(),
                    "sync-progress" => factory.shard_sync_progress(),
                    "pending-recover-slivers" => factory.pending_recover_slivers(),
                    _ => RocksdbOptions::default(),
                };
            }

            match cf_name {
                name if name == aggregate_blob_info_cf_name() => blob_info_cf_options(factory),
                name if name == per_object_blob_info_cf_name() => {
                    per_object_blob_info_cf_options(factory)
                }
                name if name == per_object_pooled_blob_info_cf_name() => {
                    per_object_pooled_blob_info_cf_options(factory)
                }
                name if name == event_index_cf_name() => factory.standard(),
                name if name == metadata_cf_name() => factory.metadata(),
                name if name == node_status_cf_name() => factory.node_status(),
                name if name == event_cursor_cf_name() => factory.event_cursor(),
                name if name == garbage_collector_table_cf_name() => factory.garbage_collector(),
                _ => RocksdbOptions::default(),
            }
        }
        ProbeDbKind::EventProcessor => match cf_name {
            name if name == event_processor_constants::CHECKPOINT_STORE => {
                factory.checkpoint_store()
            }
            name if name == event_processor_constants::WALRUS_PACKAGE_STORE => {
                factory.walrus_package_store()
            }
            name if name == event_processor_constants::COMMITTEE_STORE => factory.committee_store(),
            name if name == event_processor_constants::EVENT_STORE => factory.event_store(),
            name if name == event_processor_constants::INIT_STATE => factory.init_state(),
            _ => RocksdbOptions::default(),
        },
        ProbeDbKind::Unknown => RocksdbOptions::default(),
    }
}

fn report_storage_probe(db: &DB, column_families: &[String], exact_counts: bool) -> Result<()> {
    println!();
    println!("Top-level column families:");
    for cf_name in [
        aggregate_blob_info_cf_name(),
        per_object_blob_info_cf_name(),
        per_object_pooled_blob_info_cf_name(),
        metadata_cf_name(),
        node_status_cf_name(),
        event_cursor_cf_name(),
        event_index_cf_name(),
        garbage_collector_table_cf_name(),
    ] {
        if column_families.iter().any(|name| name == cf_name) {
            let count = count_cf_entries(db, cf_name, exact_counts)?;
            println!("  {cf_name}: {}", format_probe_count(count));
        }
    }

    let mut shard_stats = BTreeMap::<u16, StorageShardProbeStats>::new();
    for cf_name in column_families {
        let Some((shard_index, suffix)) = parse_shard_cf_name(cf_name) else {
            continue;
        };
        let count = count_cf_entries(db, cf_name, exact_counts)?;
        let entry = shard_stats.entry(shard_index).or_default();
        match suffix {
            "primary-slivers" => entry.primary = count,
            "secondary-slivers" => entry.secondary = count,
            "pending-recover-slivers" => entry.pending_recover = count,
            "status" => entry.status = count,
            "sync-progress" => entry.sync_progress = count,
            _ => {}
        }
    }

    println!();
    println!("Per-shard sliver visibility:");
    let mut total_primary = 0_u64;
    let mut total_secondary = 0_u64;
    let mut total_pending = 0_u64;
    for (shard_index, stats) in &shard_stats {
        total_primary += stats.primary.unwrap_or(0);
        total_secondary += stats.secondary.unwrap_or(0);
        total_pending += stats.pending_recover.unwrap_or(0);
        println!(
            concat!(
                "  shard-{}: primary={}, secondary={}, ",
                "pending-recover={}, status={}, sync-progress={}",
            ),
            shard_index,
            format_probe_count(stats.primary),
            format_probe_count(stats.secondary),
            format_probe_count(stats.pending_recover),
            format_probe_count(stats.status),
            format_probe_count(stats.sync_progress),
        );
    }

    println!();
    println!(
        "Totals: primary={}, secondary={}, pending-recover={}",
        total_primary, total_secondary, total_pending
    );

    Ok(())
}

fn event_processor_headings() -> Vec<&'static str> {
    vec![
        event_processor_constants::CHECKPOINT_STORE,
        event_processor_constants::WALRUS_PACKAGE_STORE,
        event_processor_constants::COMMITTEE_STORE,
        event_processor_constants::EVENT_STORE,
        event_processor_constants::INIT_STATE,
    ]
}

fn report_generic_probe(
    db: &DB,
    column_families: &[String],
    exact_counts: bool,
    preferred_order: Option<&[&str]>,
) -> Result<()> {
    println!();
    println!("Column family visibility:");

    if let Some(preferred_order) = preferred_order {
        for cf_name in preferred_order {
            if column_families.iter().any(|name| name == cf_name) {
                let count = count_cf_entries(db, cf_name, exact_counts)?;
                println!("  {cf_name}: {}", format_probe_count(count));
            }
        }
    }

    for cf_name in column_families {
        if preferred_order.is_some_and(|ordered| ordered.iter().any(|name| *name == cf_name)) {
            continue;
        }
        let count = count_cf_entries(db, cf_name, exact_counts)?;
        println!("  {cf_name}: {}", format_probe_count(count));
    }

    Ok(())
}
fn parse_shard_cf_name(cf_name: &str) -> Option<(u16, &str)> {
    let (shard_name, suffix) = cf_name.split_once('/')?;
    let shard_index = shard_name.strip_prefix("shard-")?.parse().ok()?;
    Some((shard_index, suffix))
}

fn count_cf_entries(db: &DB, cf_name: &str, exact_counts: bool) -> Result<Option<u64>> {
    let Some(cf) = db.cf_handle(cf_name) else {
        return Ok(None);
    };

    if !exact_counts {
        return db
            .property_int_value_cf(&cf, "rocksdb.estimate-num-keys")
            .map_err(Into::into);
    }

    let mut count = 0_u64;
    for entry in db.iterator_cf(&cf, rocksdb::IteratorMode::Start) {
        let _ = entry?;
        count += 1;
    }
    Ok(Some(count))
}

fn format_probe_count(count: Option<u64>) -> String {
    count
        .map(|value| value.to_string())
        .unwrap_or_else(|| "n/a".to_string())
}
fn read_blob_metadata(
    db_path: PathBuf,
    start_blob_id: Option<BlobId>,
    count: usize,
    output_size_only: bool,
) -> Result<()> {
    let db = DB::open_cf_with_opts_for_read_only(
        &RocksdbOptions::default(),
        db_path,
        [(
            metadata_cf_name(),
            DatabaseTableOptionsFactory::new(DatabaseConfig::default(), false).metadata(),
        )],
        false,
    )?;

    let Some(cf) = db.cf_handle(metadata_cf_name()) else {
        println!("Metadata column family not found");
        return Ok(());
    };

    let iter = if let Some(blob_id) = start_blob_id {
        db.iterator_cf(
            &cf,
            rocksdb::IteratorMode::From(&be_fix_int_ser(&blob_id)?, rocksdb::Direction::Forward),
        )
    } else {
        db.iterator_cf(&cf, rocksdb::IteratorMode::Start)
    };

    for result in iter.take(count) {
        match result {
            Ok((key, value)) => {
                let blob_id: BlobId = bcs::from_bytes(&key)?;
                let metadata: BlobMetadata = bcs::from_bytes(&value)?;
                if output_size_only {
                    println!(
                        "Blob ID: {}, unencoded size: {}",
                        blob_id,
                        metadata.unencoded_length()
                    );
                } else {
                    println!("Blob ID: {blob_id}, Metadata: {metadata:?}");
                }
            }
            Err(e) => {
                println!("Error: {e:?}");
                return Err(e.into());
            }
        }
    }

    Ok(())
}

fn read_primary_slivers(
    db_path: PathBuf,
    start_blob_id: Option<BlobId>,
    count: usize,
    shard_index: u16,
) -> Result<()> {
    let shard_index = ShardIndex::from(shard_index);
    let db = DB::open_cf_with_opts_for_read_only(
        &RocksdbOptions::default(),
        db_path,
        [(
            primary_slivers_column_family_name(shard_index),
            DatabaseTableOptionsFactory::new(DatabaseConfig::default(), false).shard(),
        )],
        false,
    )?;

    let Some(cf) = db.cf_handle(&primary_slivers_column_family_name(shard_index)) else {
        println!("Primary slivers column family not found for shard {shard_index}");
        return Ok(());
    };

    let iter = if let Some(blob_id) = start_blob_id {
        db.iterator_cf(
            &cf,
            rocksdb::IteratorMode::From(&be_fix_int_ser(&blob_id)?, rocksdb::Direction::Forward),
        )
    } else {
        db.iterator_cf(&cf, rocksdb::IteratorMode::Start)
    };

    for result in iter.take(count) {
        match result {
            Ok((key, value)) => {
                let blob_id: BlobId = bcs::from_bytes(&key)?;
                let sliver: PrimarySliverData = bcs::from_bytes(&value)?;
                println!("Blob ID: {blob_id}, Primary Sliver: {sliver:?}");
            }
            Err(e) => {
                println!("Error: {e:?}");
                return Err(e.into());
            }
        }
    }

    Ok(())
}

fn read_secondary_slivers(
    db_path: PathBuf,
    start_blob_id: Option<BlobId>,
    count: usize,
    shard_index: u16,
) -> Result<()> {
    let shard_index = ShardIndex::from(shard_index);
    let db = DB::open_cf_with_opts_for_read_only(
        &RocksdbOptions::default(),
        db_path,
        [(
            secondary_slivers_column_family_name(shard_index),
            DatabaseTableOptionsFactory::new(DatabaseConfig::default(), false).shard(),
        )],
        false,
    )?;

    let Some(cf) = db.cf_handle(&secondary_slivers_column_family_name(shard_index)) else {
        println!("Secondary slivers column family not found for shard {shard_index}");
        return Ok(());
    };

    let iter = if let Some(blob_id) = start_blob_id {
        db.iterator_cf(
            &cf,
            rocksdb::IteratorMode::From(&be_fix_int_ser(&blob_id)?, rocksdb::Direction::Forward),
        )
    } else {
        db.iterator_cf(&cf, rocksdb::IteratorMode::Start)
    };

    for result in iter.take(count) {
        match result {
            Ok((key, value)) => {
                let blob_id: BlobId = bcs::from_bytes(&key)?;
                let sliver: SecondarySliverData = bcs::from_bytes(&value)?;
                println!("Blob ID: {blob_id}, Secondary Sliver: {sliver:?}");
            }
            Err(e) => {
                println!("Error: {e:?}");
                return Err(e.into());
            }
        }
    }

    Ok(())
}

fn read_event_processor_init_state(db_path: PathBuf) -> Result<()> {
    let db = DB::open_cf_with_opts_for_read_only(
        &RocksdbOptions::default(),
        db_path,
        [(
            event_processor_constants::INIT_STATE,
            DatabaseTableOptionsFactory::new(DatabaseConfig::default(), false).init_state(),
        )],
        false,
    )?;

    let Some(cf) = db.cf_handle(event_processor_constants::INIT_STATE) else {
        println!("Event processor init state column family not found");
        return Ok(());
    };

    let iter = db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
    let config = bincode::DefaultOptions::new()
        .with_big_endian()
        .with_fixint_encoding();
    for result in iter {
        let (key, value) = result?;
        let init_state: InitState = bcs::from_bytes(&value)?;
        let key: u64 = config.deserialize(&key)?;
        println!("Key: {key}, Init state: {init_state:?}");
    }

    Ok(())
}

fn read_certified_event_blobs(db_path: PathBuf) -> Result<()> {
    let db = DB::open_cf_with_opts_for_read_only(
        &RocksdbOptions::default(),
        db_path,
        [(
            certified_cf_name(),
            DatabaseTableOptionsFactory::new(DatabaseConfig::default(), false).certified(),
        )],
        false,
    )?;

    let Some(cf) = db.cf_handle(certified_cf_name()) else {
        println!("Certified event blobs column family not found");
        return Ok(());
    };
    let key_buf = be_fix_int_ser("".as_bytes())?;
    let res = db.get_pinned_cf_opt(&cf, &key_buf, &ReadOptions::default())?;
    match res {
        Some(data) => {
            let metadata: CertifiedEventBlobMetadata = bcs::from_bytes(&data)?;
            println!("Certified Event Blob Metadata: {metadata:?}");
        }
        None => println!("Certified event blob not found"),
    }

    Ok(())
}

fn read_attested_event_blobs(db_path: PathBuf) -> Result<()> {
    let db = DB::open_cf_with_opts_for_read_only(
        &RocksdbOptions::default(),
        db_path,
        [(
            attested_cf_name(),
            DatabaseTableOptionsFactory::new(DatabaseConfig::default(), false).attested(),
        )],
        false,
    )?;

    let Some(cf) = db.cf_handle(attested_cf_name()) else {
        println!("Attested event blobs column family not found");
        return Ok(());
    };

    let key_buf = be_fix_int_ser("".as_bytes())?;
    let res = db.get_pinned_cf_opt(&cf, &key_buf, &ReadOptions::default())?;
    match res {
        Some(data) => {
            let metadata: AttestedEventBlobMetadata = bcs::from_bytes(&data)?;
            println!("Attested Event Blob Metadata: {metadata:?}");
        }
        None => println!("Attested event blob not found"),
    }

    Ok(())
}

fn read_pending_event_blobs(db_path: PathBuf, start_seq: Option<u64>, count: usize) -> Result<()> {
    let db = DB::open_cf_with_opts_for_read_only(
        &RocksdbOptions::default(),
        db_path,
        [(
            pending_cf_name(),
            DatabaseTableOptionsFactory::new(DatabaseConfig::default(), false).pending(),
        )],
        false,
    )?;

    let Some(cf) = db.cf_handle(pending_cf_name()) else {
        println!("Pending event blobs column family not found");
        return Ok(());
    };

    let iter = if let Some(seq) = start_seq {
        db.iterator_cf(
            &cf,
            rocksdb::IteratorMode::From(&be_fix_int_ser(&seq)?, rocksdb::Direction::Forward),
        )
    } else {
        db.iterator_cf(&cf, rocksdb::IteratorMode::Start)
    };

    for result in iter.take(count) {
        match result {
            Ok((key, value)) => {
                let seq: u64 = bcs::from_bytes(&key)?;
                let metadata: PendingEventBlobMetadata = bcs::from_bytes(&value)?;
                println!("Sequence: {seq}, Pending Event Blob Metadata: {metadata:?}");
            }
            Err(e) => {
                println!("Error: {e:?}");
                return Err(e.into());
            }
        }
    }

    Ok(())
}

fn read_failed_to_attest_event_blobs(db_path: PathBuf) -> Result<()> {
    let db = DB::open_cf_with_opts_for_read_only(
        &RocksdbOptions::default(),
        db_path,
        [(
            failed_to_attest_cf_name(),
            DatabaseTableOptionsFactory::new(DatabaseConfig::default(), false).failed_to_attest(),
        )],
        false,
    )?;

    let Some(cf) = db.cf_handle(failed_to_attest_cf_name()) else {
        println!("Failed-to-attest event blobs column family not found");
        return Ok(());
    };

    let key_buf = be_fix_int_ser("".as_bytes())?;
    let res = db.get_pinned_cf_opt(&cf, &key_buf, &ReadOptions::default())?;
    match res {
        Some(data) => {
            let metadata: FailedToAttestEventBlobMetadata = bcs::from_bytes(&data)?;
            println!("Failed-to-attest Event Blob Metadata: {metadata:?}");
        }
        None => println!("Failed-to-attest event blob not found"),
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        ProbeDbKind, detect_probe_db_kind, event_processor_constants, parse_shard_cf_name,
    };
    use crate::node::storage::constants::{aggregate_blob_info_cf_name, metadata_cf_name};

    #[test]
    fn detect_storage_probe_db() {
        let column_families = vec![
            "default".to_string(),
            aggregate_blob_info_cf_name().to_string(),
            metadata_cf_name().to_string(),
            "shard-7/primary-slivers".to_string(),
        ];

        assert_eq!(detect_probe_db_kind(&column_families), ProbeDbKind::Storage);
    }

    #[test]
    fn detect_event_processor_probe_db() {
        let column_families = vec![
            "default".to_string(),
            event_processor_constants::CHECKPOINT_STORE.to_string(),
            event_processor_constants::EVENT_STORE.to_string(),
        ];

        assert_eq!(
            detect_probe_db_kind(&column_families),
            ProbeDbKind::EventProcessor
        );
    }

    #[test]
    fn parse_storage_shard_column_family_name() {
        assert_eq!(
            parse_shard_cf_name("shard-12/pending-recover-slivers"),
            Some((12, "pending-recover-slivers"))
        );
        assert_eq!(parse_shard_cf_name("metadata"), None);
        assert_eq!(parse_shard_cf_name("shard-x/primary-slivers"), None);
    }
}
