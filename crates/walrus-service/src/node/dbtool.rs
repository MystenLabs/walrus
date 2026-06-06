// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Tools for inspecting and maintaining the RocksDB database.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    thread::sleep,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use bincode::Options;
use clap::{Subcommand, ValueEnum};
use rocksdb::{
    BottommostLevelCompaction,
    ColumnFamilyDescriptor,
    CompactOptions,
    DB,
    DBRecoveryMode,
    Options as RocksdbOptions,
    ReadOptions,
    WaitForCompactOptions,
    properties,
};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use sui_types::base_types::ObjectID;
use typed_store::rocks::be_fix_int_ser;
use walrus_core::{
    BlobId,
    Epoch,
    ShardIndex,
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
            AttestedEventBlobMetadata,
            CertifiedEventBlobMetadata,
            FailedToAttestEventBlobMetadata,
            PendingEventBlobMetadata,
            attested_cf_name,
            certified_cf_name,
            failed_to_attest_cf_name,
            pending_cf_name,
        },
        storage::{
            PrimarySliverData,
            SecondarySliverData,
            blob_info::{
                BlobInfo,
                CertifiedBlobInfoApi,
                PerObjectBlobInfo,
                blob_info_cf_options,
                per_object_blob_info_cf_options,
                per_object_pooled_blob_info_cf_options,
                storage_pool_info_cf_options,
            },
            constants::{
                aggregate_blob_info_cf_name,
                event_cursor_cf_name,
                event_index_cf_name,
                garbage_collector_table_cf_name,
                metadata_cf_name,
                node_status_cf_name,
                per_object_blob_info_cf_name,
                per_object_pooled_blob_info_cf_name,
                primary_slivers_column_family_name,
                secondary_slivers_column_family_name,
                storage_pool_info_cf_name,
            },
            event_cursor_cf_options,
        },
    },
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ValueEnum)]
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

    /// Compact all column families in a RocksDB database.
    CompactDb {
        /// Path to the RocksDB database directory.
        #[arg(long)]
        db_path: PathBuf,
        /// Whether to compact the whole DB manually or only drain current compaction debt.
        #[arg(long, value_enum, default_value_t = CompactDbMode::Full)]
        mode: CompactDbMode,
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

    /// Map logged RocksDB SST corruption to the affected live column family.
    DetectCorruptColumnFamilies {
        /// Path to the RocksDB database directory.
        #[arg(long)]
        db_path: PathBuf,
        /// WAL recovery mode to use for the read-only open.
        #[arg(long, value_enum, default_value_t = ProbeWalRecoveryMode::PointInTime)]
        wal_recovery_mode: ProbeWalRecoveryMode,
        /// Path to RocksDB LOG files. Defaults to `db_path`.
        #[arg(long)]
        log_dir: Option<PathBuf>,
    },

    /// Open the RocksDB database read-only with a selectable WAL recovery mode and report what
    /// data is visible without persisting any recovery result.
    #[command(hide = true)]
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
    #[command(hide = true)]
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

/// Compaction mode for the `compact-db` command.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, ValueEnum)]
pub enum CompactDbMode {
    /// Rewrite each column family with a full-range manual compaction.
    #[default]
    Full,
    /// Reopen the DB and wait for automatic compaction to clear current compaction debt.
    Drain,
}

impl DbToolCommands {
    /// Execute the database tool command.
    pub fn execute(self) -> Result<()> {
        match self {
            Self::RepairDb { db_path } => repair_db(db_path),
            Self::CompactDb { db_path, mode } => compact_db(db_path, mode),
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
            Self::DetectCorruptColumnFamilies {
                db_path,
                wal_recovery_mode,
                log_dir,
            } => detect_corrupt_column_families(db_path, wal_recovery_mode, log_dir),
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

fn repair_db(_db_path: PathBuf) -> Result<()> {
    // Reference implementation disabled:
    // let mut opts = RocksdbOptions::default();
    // opts.create_if_missing(true);
    // opts.set_max_open_files(512_000);
    // DB::repair(&opts, db_path).map_err(Into::into)

    println!(
        "WARNING: The repair-db command is currently disabled. \
        Please contact the Walrus team for assistance with database recovery."
    );
    Ok(())
}

fn compact_db(db_path: PathBuf, mode: CompactDbMode) -> Result<()> {
    let column_families = DB::list_cf(&RocksdbOptions::default(), &db_path)?;
    if column_families.is_empty() {
        println!("No column families found in database");
        return Ok(());
    }

    // Pick per-column-family options from the same table-option families the node uses for the
    // detected DB shape. This lets the tool reopen storage, event-blob-writer, and
    // event-processor DBs with the expected RocksDB options while still tolerating unknown CFs.
    let db_kind = detect_probe_db_kind(&column_families);
    let db_table_opts_factory = db_table_options_factory_for_kind(db_kind);
    let mut db_opts = RocksdbOptions::from(&db_table_opts_factory.global());
    db_opts.create_if_missing(false);
    db_opts.create_missing_column_families(false);
    let cf_descriptors = cf_options(&column_families, db_kind, &db_table_opts_factory)
        .into_iter()
        .map(|(cf_name, options)| ColumnFamilyDescriptor::new(cf_name, options))
        .collect::<Vec<_>>();

    let db = DB::open_cf_descriptors(&db_opts, &db_path, cf_descriptors).inspect_err(|_| {
        println!(
            "failed to open database; \
            make sure to stop the storage node before attempting to compact the database"
        )
    })?;

    match mode {
        CompactDbMode::Full => compact_db_full(&db, &column_families),
        CompactDbMode::Drain => compact_db_drain(&db, &column_families),
    }
}

fn compact_db_full(db: &DB, column_families: &[String]) -> Result<()> {
    let mut compact_options = CompactOptions::default();
    // Run a full offline compaction: optimize the bottommost level rewrite, do not interleave it
    // with background compactions, and wait for the flush/compaction work to finish before exit.
    compact_options.set_bottommost_level_compaction(BottommostLevelCompaction::ForceOptimized);
    compact_options.set_exclusive_manual_compaction(true);

    for column_family_name in column_families {
        let cf = db
            .cf_handle(column_family_name)
            .with_context(|| format!("column family `{column_family_name}` should exist"))?;
        println!("Compacting column family: {column_family_name}");
        db.compact_range_cf_opt(&cf, None::<&[u8]>, None::<&[u8]>, &compact_options);
    }

    let mut wait_options = WaitForCompactOptions::default();
    wait_options.set_flush(true);
    db.wait_for_compact(&wait_options)?;

    println!(
        "Finished compacting {} column families",
        column_families.len()
    );
    Ok(())
}

const DRAIN_COMPACTION_POLL_INTERVAL: Duration = Duration::from_secs(1);
const DRAIN_COMPACTION_STALL_LIMIT: usize = 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CompactionProgress {
    pending_compaction_bytes: u64,
    pending_compactions: u64,
    running_compactions: u64,
    running_flushes: u64,
    background_errors: u64,
}

fn compact_db_drain(db: &DB, column_families: &[String]) -> Result<()> {
    for column_family_name in column_families {
        let cf = db
            .cf_handle(column_family_name)
            .with_context(|| format!("column family `{column_family_name}` should exist"))?;
        db.set_options_cf(&cf, &[("disable_auto_compactions", "false")])?;
    }

    let mut stalled_polls = 0;
    let mut previous_progress = None;
    loop {
        let progress = compaction_progress(db, column_families)?;
        if progress.background_errors > 0 {
            bail!(
                concat!(
                    "RocksDB reported {} background compaction/flush errors ",
                    "while draining compaction debt"
                ),
                progress.background_errors
            );
        }

        if previous_progress != Some(progress) {
            println!(
                concat!(
                    "Compaction debt: {} bytes pending, {} pending compactions, ",
                    "{} running compactions, {} running flushes"
                ),
                progress.pending_compaction_bytes,
                progress.pending_compactions,
                progress.running_compactions,
                progress.running_flushes,
            );
        }

        if progress.pending_compaction_bytes == 0
            && progress.pending_compactions == 0
            && progress.running_compactions == 0
            && progress.running_flushes == 0
        {
            println!(
                "Finished draining compaction debt for {} column families",
                column_families.len()
            );
            return Ok(());
        }

        if progress.running_compactions == 0
            && progress.running_flushes == 0
            && progress.pending_compactions == 0
        {
            stalled_polls += 1;
            if stalled_polls >= DRAIN_COMPACTION_STALL_LIMIT {
                bail!(
                    concat!(
                        "compaction debt is still {} bytes, but RocksDB is no ",
                        "longer scheduling background work; try `--mode full` ",
                        "instead"
                    ),
                    progress.pending_compaction_bytes
                );
            }
        } else {
            stalled_polls = 0;
        }

        previous_progress = Some(progress);
        sleep(DRAIN_COMPACTION_POLL_INTERVAL);
    }
}

fn compaction_progress(db: &DB, column_families: &[String]) -> Result<CompactionProgress> {
    let mut pending_compaction_bytes: u64 = 0;
    let mut pending_compactions: u64 = 0;

    for column_family_name in column_families {
        let cf = db
            .cf_handle(column_family_name)
            .with_context(|| format!("column family `{column_family_name}` should exist"))?;

        pending_compaction_bytes = pending_compaction_bytes.saturating_add(
            db.property_int_value_cf(&cf, properties::ESTIMATE_PENDING_COMPACTION_BYTES)?
                .unwrap_or(0),
        );
        pending_compactions = pending_compactions.saturating_add(
            db.property_int_value_cf(&cf, properties::COMPACTION_PENDING)?
                .unwrap_or(0),
        );
    }

    Ok(CompactionProgress {
        pending_compaction_bytes,
        pending_compactions,
        running_compactions: db
            .property_int_value(properties::NUM_RUNNING_COMPACTIONS)?
            .unwrap_or(0),
        running_flushes: db
            .property_int_value(properties::NUM_RUNNING_FLUSHES)?
            .unwrap_or(0),
        background_errors: db
            .property_int_value(properties::BACKGROUND_ERRORS)?
            .unwrap_or(0),
    })
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
    let db_table_opts_factory = db_table_options_factory_for_kind(db_kind);

    let mut db_opts = RocksdbOptions::default();
    db_opts.set_max_open_files(512_000);

    let db = DB::open_cf_with_opts(
        &db_opts,
        &db_path,
        cf_options(&column_families, db_kind, &db_table_opts_factory),
    )
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
    EventBlobWriter,
    EventProcessor,
    Unknown,
}

#[derive(Debug, Default)]
struct StorageShardProbeStats {
    primary: Option<u64>,
    secondary: Option<u64>,
    pending_recover: Option<u64>,
    status: Option<u64>,
    sync_progress: Option<u64>,
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

fn detect_corrupt_column_families(
    db_path: PathBuf,
    wal_recovery_mode: ProbeWalRecoveryMode,
    log_dir: Option<PathBuf>,
) -> Result<()> {
    let log_dir = log_dir.unwrap_or_else(|| db_path.clone());
    let logged_sst_probe = collect_logged_corrupt_ssts(&log_dir)?;

    report_logged_sst_probe(&log_dir, &logged_sst_probe);
    if logged_sst_probe.sst_file_names.is_empty() {
        return Ok(());
    }

    let (column_families, db_kind, db_opts, db_table_opts_factory) =
        prepare_recovery_open(&db_path, wal_recovery_mode).with_context(|| {
            format!(
                "failed to prepare read-only DB open at {}; cannot map logged SST corruption to \
                column families",
                db_path.display()
            )
        })?;
    let db = DB::open_cf_with_opts_for_read_only(
        &db_opts,
        &db_path,
        cf_options(&column_families, db_kind, &db_table_opts_factory),
        false,
    )
    .with_context(|| {
        format!(
            "failed to open DB read-only at {}; cannot map logged SST corruption to column \
            families",
            db_path.display()
        )
    })?;

    println!("Opened DB read-only at {}", db_path.display());
    println!("WAL recovery mode: {wal_recovery_mode}");
    println!("Detected DB kind: {}", probe_db_kind_name(db_kind));
    println!("Column families discovered: {}", column_families.len());
    println!("Probe is non-mutating: it only reads RocksDB LOG files and live-file metadata.");
    println!();

    let mappings = map_sst_files_to_column_families(&db, &logged_sst_probe.sst_file_names)
        .context("failed to map logged corrupted SSTs to live column families")?;
    if report_logged_sst_mappings(&mappings) {
        bail!("detected suspect column families from logged SST corruption");
    }

    bail!("found logged SST corruption but could not map it to a live column family")
}

#[derive(Debug, Default)]
struct LoggedSstProbe {
    log_files_scanned: usize,
    sst_file_names: BTreeSet<String>,
}

#[derive(Debug)]
struct LoggedSstMapping {
    sst_file_name: String,
    column_family_name: Option<String>,
    level: Option<i32>,
    size: Option<usize>,
}

fn collect_logged_corrupt_ssts(log_dir: &Path) -> Result<LoggedSstProbe> {
    let log_files = rocksdb_log_files(log_dir)?;
    let mut probe = LoggedSstProbe {
        log_files_scanned: log_files.len(),
        ..Default::default()
    };

    for log_file in log_files {
        let file = File::open(&log_file)
            .with_context(|| format!("failed to open RocksDB LOG file `{}`", log_file.display()))?;
        for line in BufReader::new(file).lines() {
            let line = line.with_context(|| {
                format!("failed to read RocksDB LOG file `{}`", log_file.display())
            })?;
            probe
                .sst_file_names
                .extend(extract_corrupt_sst_file_names(&line));
        }
    }

    Ok(probe)
}

fn rocksdb_log_files(log_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut log_files = Vec::new();
    for entry in std::fs::read_dir(log_dir).with_context(|| {
        format!(
            "failed to read RocksDB LOG directory `{}`",
            log_dir.display()
        )
    })? {
        let entry = entry.with_context(|| {
            format!(
                "failed to read entry in LOG directory `{}`",
                log_dir.display()
            )
        })?;
        let file_type = entry.file_type().with_context(|| {
            format!(
                "failed to read file type for LOG directory entry `{}`",
                entry.path().display()
            )
        })?;
        if !file_type.is_file() {
            continue;
        }

        let Some(file_name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if file_name == "LOG" || file_name.starts_with("LOG.old.") || file_name.starts_with("LOG.")
        {
            log_files.push(entry.path());
        }
    }
    log_files.sort();
    Ok(log_files)
}

fn extract_corrupt_sst_file_names(line: &str) -> Vec<String> {
    let lower_line = line.to_ascii_lowercase();
    if !lower_line.contains("corrupt") && !lower_line.contains("checksum mismatch") {
        return Vec::new();
    }

    let mut sst_file_names = BTreeSet::new();
    let mut search_from = 0;
    while let Some(relative_sst_position) = line[search_from..].find(".sst") {
        let sst_end = search_from + relative_sst_position + ".sst".len();
        let mut path_start = search_from + relative_sst_position;
        while path_start > 0 {
            let previous = line.as_bytes()[path_start - 1] as char;
            if previous.is_ascii_whitespace()
                || matches!(previous, '"' | '\'' | '`' | '(' | '[' | '<' | '=')
            {
                break;
            }
            path_start -= 1;
        }

        if let Some(sst_file_name) = normalize_sst_file_name(&line[path_start..sst_end]) {
            sst_file_names.insert(sst_file_name);
        }
        search_from = sst_end;
    }

    sst_file_names.into_iter().collect()
}

fn normalize_sst_file_name(value: &str) -> Option<String> {
    let trimmed = value.trim_matches(|character: char| {
        matches!(
            character,
            '"' | '\'' | '`' | '(' | ')' | '[' | ']' | '<' | '>' | ',' | ';' | ':'
        )
    });
    let file_name = Path::new(trimmed).file_name()?.to_str()?;
    file_name.ends_with(".sst").then(|| file_name.to_owned())
}

fn map_sst_files_to_column_families(
    db: &DB,
    sst_file_names: &BTreeSet<String>,
) -> Result<Vec<LoggedSstMapping>> {
    let mut live_files_by_name = BTreeMap::new();
    for live_file in db.live_files()? {
        if let Some(sst_file_name) = normalize_sst_file_name(&live_file.name) {
            live_files_by_name.insert(
                sst_file_name,
                (
                    live_file.column_family_name,
                    live_file.level,
                    live_file.size,
                ),
            );
        }
    }

    Ok(sst_file_names
        .iter()
        .map(|sst_file_name| {
            let live_file = live_files_by_name.get(sst_file_name);
            LoggedSstMapping {
                sst_file_name: sst_file_name.clone(),
                column_family_name: live_file.map(|(cf_name, _, _)| cf_name.clone()),
                level: live_file.map(|(_, level, _)| *level),
                size: live_file.map(|(_, _, size)| *size),
            }
        })
        .collect())
}

fn report_logged_sst_probe(log_dir: &Path, probe: &LoggedSstProbe) {
    println!(
        "Fast path: scanned {} RocksDB LOG file(s) in {}",
        probe.log_files_scanned,
        log_dir.display()
    );

    if probe.sst_file_names.is_empty() {
        println!("Fast path: no corruption-related SST references found in RocksDB LOG files.");
    } else {
        println!(
            "Fast path: found {} corruption-related SST reference(s): {}",
            probe.sst_file_names.len(),
            probe
                .sst_file_names
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    println!();
}

fn report_logged_sst_mappings(mappings: &[LoggedSstMapping]) -> bool {
    let mut mapped_by_cf = BTreeMap::<&str, Vec<&LoggedSstMapping>>::new();
    let mut unmapped_ssts = Vec::new();
    for mapping in mappings {
        if let Some(column_family_name) = &mapping.column_family_name {
            mapped_by_cf
                .entry(column_family_name.as_str())
                .or_default()
                .push(mapping);
        } else {
            unmapped_ssts.push(mapping.sst_file_name.as_str());
        }
    }

    if !mapped_by_cf.is_empty() {
        println!("Suspect column families from logged SST corruption:");
        for (column_family_name, mappings) in mapped_by_cf {
            let sst_details = mappings
                .iter()
                .map(|mapping| {
                    format!(
                        "{} (level={}, size={} bytes)",
                        mapping.sst_file_name,
                        format_optional_value(mapping.level),
                        format_optional_value(mapping.size)
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            println!("  {column_family_name}: {sst_details}");
        }
        println!();
    }

    if !unmapped_ssts.is_empty() {
        println!(
            "Logged corrupted SSTs not found among current live files: {}",
            unmapped_ssts.join(", ")
        );
        println!();
    }

    mappings
        .iter()
        .any(|mapping| mapping.column_family_name.is_some())
}

fn format_optional_value<T: std::fmt::Display>(value: Option<T>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "n/a".to_string())
}

fn probe_recovery(
    db_path: PathBuf,
    wal_recovery_mode: ProbeWalRecoveryMode,
    exact_counts: bool,
) -> Result<()> {
    let (column_families, db_kind, db_opts, db_table_opts_factory) =
        prepare_recovery_open(&db_path, wal_recovery_mode)?;
    let db = DB::open_cf_with_opts_for_read_only(
        &db_opts,
        &db_path,
        cf_options(&column_families, db_kind, &db_table_opts_factory),
        false,
    )?;

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
        ProbeDbKind::EventBlobWriter => report_generic_probe(
            &db,
            &column_families,
            exact_counts,
            Some(&event_blob_writer_headings()),
        )?,
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
    let (column_families, db_kind, db_opts, db_table_opts_factory) =
        prepare_recovery_open(&db_path, wal_recovery_mode)?;
    let db = DB::open_cf_with_opts(
        &db_opts,
        &db_path,
        cf_options(&column_families, db_kind, &db_table_opts_factory),
    )
    .inspect_err(|_| {
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
        ProbeDbKind::EventBlobWriter => report_generic_probe(
            &db,
            &column_families,
            exact_counts,
            Some(&event_blob_writer_headings()),
        )?,
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
) -> Result<(
    Vec<String>,
    ProbeDbKind,
    RocksdbOptions,
    DatabaseTableOptionsFactory,
)> {
    let column_families = DB::list_cf(&RocksdbOptions::default(), db_path)?;
    let db_kind = detect_probe_db_kind(&column_families);
    let db_table_opts_factory = db_table_options_factory_for_kind(db_kind);
    let mut db_opts = RocksdbOptions::from(&db_table_opts_factory.global());
    db_opts.create_if_missing(false);
    db_opts.create_missing_column_families(false);
    db_opts.set_wal_recovery_mode(wal_recovery_mode.as_rocksdb());

    Ok((column_families, db_kind, db_opts, db_table_opts_factory))
}

fn probe_db_kind_name(db_kind: ProbeDbKind) -> &'static str {
    match db_kind {
        ProbeDbKind::Storage => "storage",
        ProbeDbKind::EventBlobWriter => "event-blob-writer",
        ProbeDbKind::EventProcessor => "event-processor",
        ProbeDbKind::Unknown => "unknown",
    }
}

fn report_storage_probe(db: &DB, column_families: &[String], exact_counts: bool) -> Result<()> {
    println!();
    println!("Top-level column families:");
    for cf_name in [
        aggregate_blob_info_cf_name(),
        per_object_blob_info_cf_name(),
        per_object_pooled_blob_info_cf_name(),
        storage_pool_info_cf_name(),
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

fn event_blob_writer_headings() -> Vec<&'static str> {
    vec![
        certified_cf_name(),
        attested_cf_name(),
        pending_cf_name(),
        failed_to_attest_cf_name(),
    ]
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

fn detect_probe_db_kind(column_families: &[String]) -> ProbeDbKind {
    if column_families.iter().any(|cf| {
        cf == aggregate_blob_info_cf_name()
            || cf == per_object_blob_info_cf_name()
            || cf == per_object_pooled_blob_info_cf_name()
            || cf == storage_pool_info_cf_name()
            || cf == metadata_cf_name()
            || cf.starts_with("shard-")
    }) {
        ProbeDbKind::Storage
    } else if column_families.iter().any(|cf| {
        cf == certified_cf_name()
            || cf == attested_cf_name()
            || cf == pending_cf_name()
            || cf == failed_to_attest_cf_name()
    }) {
        ProbeDbKind::EventBlobWriter
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

// db-tool reuses the compiled-in DatabaseConfig defaults for the detected DB kind. Storage DBs
// get the shared shard-cache wiring; other DB kinds use their non-storage option families.
fn db_table_options_factory_for_kind(db_kind: ProbeDbKind) -> DatabaseTableOptionsFactory {
    DatabaseTableOptionsFactory::new(DatabaseConfig::default(), db_kind == ProbeDbKind::Storage)
}

fn cf_options<'a>(
    column_families: &'a [String],
    db_kind: ProbeDbKind,
    factory: &DatabaseTableOptionsFactory,
) -> Vec<(&'a str, RocksdbOptions)> {
    column_families
        .iter()
        .map(|cf_name| {
            (
                cf_name.as_str(),
                cf_options_for_name(cf_name, db_kind, factory),
            )
        })
        .collect()
}

fn cf_options_for_name(
    cf_name: &str,
    db_kind: ProbeDbKind,
    factory: &DatabaseTableOptionsFactory,
) -> RocksdbOptions {
    if cf_name == "default" {
        return factory.standard();
    }

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
                name if name == storage_pool_info_cf_name() => {
                    storage_pool_info_cf_options(factory)
                }
                name if name == event_index_cf_name() => factory.standard(),
                name if name == metadata_cf_name() => factory.metadata(),
                name if name == node_status_cf_name() => factory.node_status(),
                name if name == event_cursor_cf_name() => event_cursor_cf_options(factory),
                name if name == garbage_collector_table_cf_name() => factory.garbage_collector(),
                _ => RocksdbOptions::default(),
            }
        }
        ProbeDbKind::EventBlobWriter => match cf_name {
            name if name == certified_cf_name() => factory.certified(),
            name if name == attested_cf_name() => factory.attested(),
            name if name == pending_cf_name() => factory.pending(),
            name if name == failed_to_attest_cf_name() => factory.failed_to_attest(),
            _ => RocksdbOptions::default(),
        },
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

fn parse_shard_cf_name(cf_name: &str) -> Option<(u16, &str)> {
    let (shard_name, suffix) = cf_name.split_once('/')?;
    let shard_index = shard_name.strip_prefix("shard-")?.parse().ok()?;
    Some((shard_index, suffix))
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
    use clap::{CommandFactory, Parser};
    use tempfile::tempdir;

    use super::*;

    #[derive(Parser)]
    #[command(rename_all = "kebab-case")]
    struct DbToolArgs {
        #[command(subcommand)]
        command: DbToolCommands,
    }

    #[test]
    fn compact_db_handles_known_and_unknown_column_families() -> Result<()> {
        let db_dir = tempdir()?;
        let db_table_opts_factory =
            DatabaseTableOptionsFactory::new(DatabaseConfig::default(), true);
        let mut db_opts = RocksdbOptions::from(&db_table_opts_factory.global());
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        let db = DB::open_cf_descriptors(
            &db_opts,
            db_dir.path(),
            vec![
                ColumnFamilyDescriptor::new(
                    aggregate_blob_info_cf_name(),
                    blob_info_cf_options(&db_table_opts_factory),
                ),
                ColumnFamilyDescriptor::new(metadata_cf_name(), db_table_opts_factory.metadata()),
                ColumnFamilyDescriptor::new(
                    primary_slivers_column_family_name(ShardIndex(0)),
                    db_table_opts_factory.shard(),
                ),
                ColumnFamilyDescriptor::new("custom_cf", RocksdbOptions::default()),
            ],
        )?;

        let metadata_cf = db
            .cf_handle(metadata_cf_name())
            .expect("metadata column family should exist");
        db.put_cf(&metadata_cf, b"key", b"value")?;
        drop(metadata_cf);
        drop(db);

        compact_db(db_dir.path().to_path_buf(), CompactDbMode::Full)
    }

    #[test]
    fn compact_db_handles_event_blob_writer_column_families() -> Result<()> {
        let db_dir = tempdir()?;
        let db_table_opts_factory =
            DatabaseTableOptionsFactory::new(DatabaseConfig::default(), false);
        let mut db_opts = RocksdbOptions::from(&db_table_opts_factory.global());
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        let db = DB::open_cf_descriptors(
            &db_opts,
            db_dir.path(),
            vec![
                ColumnFamilyDescriptor::new(certified_cf_name(), db_table_opts_factory.certified()),
                ColumnFamilyDescriptor::new(attested_cf_name(), db_table_opts_factory.attested()),
                ColumnFamilyDescriptor::new(pending_cf_name(), db_table_opts_factory.pending()),
                ColumnFamilyDescriptor::new(
                    failed_to_attest_cf_name(),
                    db_table_opts_factory.failed_to_attest(),
                ),
                ColumnFamilyDescriptor::new("custom_cf", RocksdbOptions::default()),
            ],
        )?;

        drop(db);

        compact_db(db_dir.path().to_path_buf(), CompactDbMode::Full)
    }

    #[test]
    fn compact_db_drain_exits_for_idle_storage_db() -> Result<()> {
        let db_dir = tempdir()?;
        let db_table_opts_factory =
            DatabaseTableOptionsFactory::new(DatabaseConfig::default(), true);
        let mut db_opts = RocksdbOptions::from(&db_table_opts_factory.global());
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        let db = DB::open_cf_descriptors(
            &db_opts,
            db_dir.path(),
            vec![
                ColumnFamilyDescriptor::new(
                    aggregate_blob_info_cf_name(),
                    blob_info_cf_options(&db_table_opts_factory),
                ),
                ColumnFamilyDescriptor::new(metadata_cf_name(), db_table_opts_factory.metadata()),
                ColumnFamilyDescriptor::new("custom_cf", RocksdbOptions::default()),
            ],
        )?;

        drop(db);

        compact_db(db_dir.path().to_path_buf(), CompactDbMode::Drain)
    }

    #[test]
    fn detect_corrupt_column_families_succeeds_for_clean_storage_db() -> Result<()> {
        let db_dir = tempdir()?;
        let db_table_opts_factory =
            DatabaseTableOptionsFactory::new(DatabaseConfig::default(), true);
        let mut db_opts = RocksdbOptions::from(&db_table_opts_factory.global());
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        let primary_cf_name = primary_slivers_column_family_name(ShardIndex(0));
        let db = DB::open_cf_descriptors(
            &db_opts,
            db_dir.path(),
            vec![
                ColumnFamilyDescriptor::new(
                    aggregate_blob_info_cf_name(),
                    blob_info_cf_options(&db_table_opts_factory),
                ),
                ColumnFamilyDescriptor::new(metadata_cf_name(), db_table_opts_factory.metadata()),
                ColumnFamilyDescriptor::new(primary_cf_name.clone(), db_table_opts_factory.shard()),
                ColumnFamilyDescriptor::new("custom_cf", RocksdbOptions::default()),
            ],
        )?;

        let metadata_cf = db
            .cf_handle(metadata_cf_name())
            .expect("metadata column family should exist");
        db.put_cf(&metadata_cf, b"metadata-key", b"metadata-value")?;
        let primary_cf = db
            .cf_handle(&primary_cf_name)
            .expect("primary slivers column family should exist");
        db.put_cf(&primary_cf, b"blob-key", b"sliver-value")?;
        drop(metadata_cf);
        drop(primary_cf);
        drop(db);

        detect_corrupt_column_families(
            db_dir.path().to_path_buf(),
            ProbeWalRecoveryMode::PointInTime,
            None,
        )
    }

    #[test]
    fn extracts_corrupt_sst_references_from_log_lines() -> Result<()> {
        let log_dir = tempdir()?;
        std::fs::write(
            log_dir.path().join("LOG"),
            concat!(
                "ordinary log line, but mentions /opt/walrus/db/111111.sst\n",
                "rocksdb error: Corruption: block checksum mismatch: stored = 1, computed = 2 ",
                "in /opt/walrus/db/486117.sst offset 332208453 size 720421\n",
                "Corruption: checksum mismatch in file=000123.sst offset 10\n",
            ),
        )?;

        let probe = collect_logged_corrupt_ssts(log_dir.path())?;

        assert_eq!(probe.log_files_scanned, 1);
        assert_eq!(
            probe.sst_file_names,
            ["000123.sst".to_string(), "486117.sst".to_string()]
                .into_iter()
                .collect()
        );
        Ok(())
    }

    #[test]
    fn maps_logged_sst_to_live_column_family() -> Result<()> {
        let db_dir = tempdir()?;
        let db_table_opts_factory =
            DatabaseTableOptionsFactory::new(DatabaseConfig::default(), true);
        let mut db_opts = RocksdbOptions::from(&db_table_opts_factory.global());
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        let primary_cf_name = primary_slivers_column_family_name(ShardIndex(0));
        let db = DB::open_cf_descriptors(
            &db_opts,
            db_dir.path(),
            vec![
                ColumnFamilyDescriptor::new(metadata_cf_name(), db_table_opts_factory.metadata()),
                ColumnFamilyDescriptor::new(primary_cf_name.clone(), db_table_opts_factory.shard()),
            ],
        )?;

        let primary_cf = db
            .cf_handle(&primary_cf_name)
            .expect("primary slivers column family should exist");
        db.put_cf(&primary_cf, b"blob-key", b"sliver-value")?;
        db.flush_cf(&primary_cf)?;

        let primary_sst_name = db
            .live_files()?
            .into_iter()
            .find(|live_file| live_file.column_family_name == primary_cf_name)
            .and_then(|live_file| normalize_sst_file_name(&live_file.name))
            .expect("flushed primary column family should have a live SST");
        let mappings =
            map_sst_files_to_column_families(&db, &[primary_sst_name].into_iter().collect())?;

        assert_eq!(mappings.len(), 1);
        assert_eq!(
            mappings[0].column_family_name.as_deref(),
            Some(primary_cf_name.as_str())
        );
        Ok(())
    }

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

    #[test]
    fn recovery_commands_are_hidden_from_help() {
        let mut command = DbToolArgs::command();
        let help = command.render_long_help().to_string();

        assert!(!help.contains("probe-recovery"));
        assert!(!help.contains("recover-db"));
    }

    #[test]
    fn detect_corrupt_column_families_command_parses() {
        let args = DbToolArgs::try_parse_from([
            "db-tool",
            "detect-corrupt-column-families",
            "--db-path",
            "/tmp/testdb",
            "--wal-recovery-mode",
            "absolute-consistency",
            "--log-dir",
            "/tmp/rocksdb-logs",
        ])
        .expect("detect-corrupt-column-families command should parse");

        assert!(matches!(
            args.command,
            DbToolCommands::DetectCorruptColumnFamilies {
                db_path,
                wal_recovery_mode: ProbeWalRecoveryMode::AbsoluteConsistency,
                log_dir: Some(log_dir),
            } if db_path == std::path::PathBuf::from("/tmp/testdb")
                && log_dir == std::path::PathBuf::from("/tmp/rocksdb-logs")
        ));
    }

    #[test]
    fn hidden_recovery_commands_still_parse() {
        let probe =
            DbToolArgs::try_parse_from(["db-tool", "probe-recovery", "--db-path", "/tmp/testdb"])
                .expect("hidden probe-recovery command should still parse");
        assert!(matches!(
            probe.command,
            DbToolCommands::ProbeRecovery { .. }
        ));

        let recover =
            DbToolArgs::try_parse_from(["db-tool", "recover-db", "--db-path", "/tmp/testdb"])
                .expect("hidden recover-db command should still parse");
        assert!(matches!(recover.command, DbToolCommands::RecoverDb { .. }));
    }
}
