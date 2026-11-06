// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Tools for inspecting and maintaining the RocksDB database.

use std::{
    collections::{BTreeMap, BTreeSet},
    io::Cursor,
    num::NonZeroU16,
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
    FlushOptions,
    Options as RocksdbOptions,
    ReadOptions,
    WaitForCompactOptions,
    WriteBatch,
    properties,
};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use sui_types::base_types::ObjectID;
use typed_store::rocks::{MetricConf, be_fix_int_ser};
use walrus_core::{
    BlobId,
    DEFAULT_ENCODING,
    Epoch,
    ShardIndex,
    encoding::{EncodingConfig, EncodingFactory as _},
    metadata::{BlobMetadata, BlobMetadataApi},
};
use walrus_utils::metrics::Registry;

use super::DatabaseTableOptionsFactory;
use crate::{
    event::{
        event_processor::db::constants::{self as event_processor_constants},
        events::{InitState, PositionedStreamEvent},
    },
    node::{
        DatabaseConfig,
        blob_info_snapshot_writer::snapshot_base_dir,
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
            PendingRecoverBlob,
            PrimarySliverData,
            SecondarySliverData,
            Storage,
            blob_info::{
                BlobInfo,
                CertifiedBlobInfoApi,
                PerObjectBlobInfo,
                blob_info_cf_options,
                per_object_blob_info_cf_options,
                per_object_pooled_blob_info_cf_options,
                storage_pool_info_cf_options,
            },
            blob_info_snapshot,
            constants::{
                aggregate_blob_info_cf_name,
                blob_info_snapshot_publication_cf_name,
                event_cursor_cf_name,
                event_index_cf_name,
                garbage_collector_table_cf_name,
                metadata_cf_name,
                node_status_cf_name,
                pending_recover_blobs_cf_name,
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

/// A mapping from an input RocksDB column family to an output Walrus DB column family.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnFamilyMapping {
    input: String,
    output: String,
}

fn parse_column_family_mapping(value: &str) -> std::result::Result<ColumnFamilyMapping, String> {
    let (input, output) = value
        .split_once('=')
        .ok_or_else(|| "expected mapping in the form input_cf=output_cf".to_string())?;
    let input = input.trim();
    let output = output.trim();
    if input.is_empty() || output.is_empty() {
        return Err("column family mapping cannot contain an empty side".to_string());
    }

    Ok(ColumnFamilyMapping {
        input: input.to_owned(),
        output: output.to_owned(),
    })
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

    /// Read pending-recovery records from the RocksDB database.
    ReadPendingRecoverBlobs {
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

    /// Wipe a stopped node's state except its blob data, so that it starts as a brand-new node
    /// that loads the latest certified blob info snapshot while keeping its metadata and slivers.
    ///
    /// Opens the node's database first, which fails while the node runs, takes a checkpoint of
    /// it in `<storage_path>/wiped-<timestamp>/db-checkpoint/` (hard links to the database's
    /// files, so it takes little space until the node rewrites them), then moves the event blob
    /// writer's state (`event_blob_writer/`) and the local snapshot files
    /// (`blob_info_snapshots/`) into the same `wiped-<timestamp>/` directory (inside the storage
    /// path, so that the move stays on one filesystem when the storage path is a mount point;
    /// the node ignores the directory), drops every column family except `metadata`, the
    /// per-shard ones (`shard-*`), the snapshot publication record, and `aggregate_blob_info`,
    /// and reduces the latter to the entries of the blobs with stored metadata: the node's
    /// record of the blobs it holds data for, from which the snapshot load keeps them known to
    /// garbage collection.
    ///
    /// The event processor's database (`events/`) is kept, so that the node neither downloads
    /// event blobs nor tails the checkpoints since the latest certified event blob again. A
    /// store that starts after the first event, as on a node that joined through event blobs,
    /// makes the next start load the snapshot; one that reaches the first event, as on a node
    /// that ran since genesis, makes it replay every event instead, which takes hours but keeps
    /// the full history. The command prints which of the two the node will do.
    ///
    /// `<storage_path>/wipe-in-progress` exists from the first change to the last, and the node
    /// refuses to start while it does: if a run is interrupted, run the command again, which
    /// repeats what is left and removes the file. `restore-node-state` reverts the wipe from the
    /// `wiped-<timestamp>` directory; delete the directory once the node is back in service,
    /// which also frees the space of the files only the checkpoint still links.
    WipeNodeState {
        /// Path to the node's storage directory (`storage_path` in its configuration).
        #[arg(long)]
        db_path: PathBuf,
        /// Move the event processor's database (`events/`) aside as well. The node then
        /// downloads the event blobs since the oldest unexpired one before it loads the
        /// snapshot. For a store that is suspect, or that reaches the first event on a network
        /// whose early event blobs expired, where the snapshot route is faster than a replay.
        #[arg(long)]
        wipe_event_store: bool,
        /// Do not take the checkpoint of the database; `restore-node-state` then cannot revert
        /// the wipe.
        #[arg(long)]
        skip_checkpoint: bool,
    },

    /// Revert a `wipe-node-state` run on a stopped node from its `wiped-<timestamp>` directory.
    ///
    /// Moves the directories the wipe moved aside back into the storage path (one the node
    /// created since goes to `<wiped_dir>/replaced-<timestamp>/`) and copies every column family
    /// the wipe dropped or reduced back from the checkpoint the wipe took; fails if the wipe ran
    /// with `--skip-checkpoint`. The metadata and per-shard tables are not restored: the wipe
    /// left them as they were, and a node that ran since the wipe deleted from them only the
    /// data of blobs that garbage collection deletes on every node. The node refuses to start
    /// while the restore runs, as during a wipe; if a run is interrupted, run the command again.
    RestoreNodeState {
        /// Path to the node's storage directory (`storage_path` in its configuration).
        #[arg(long)]
        db_path: PathBuf,
        /// The `wiped-<timestamp>` directory of the wipe to revert.
        #[arg(long)]
        wiped_dir: PathBuf,
    },

    /// Replace column families in a Walrus RocksDB database by copying them from another RocksDB
    /// database. This can only be called when the storage node is stopped.
    RestoreColumnFamilies {
        /// Path to the Walrus RocksDB database directory to mutate.
        #[arg(long)]
        db_path: PathBuf,
        /// Path to the input RocksDB database directory to copy from.
        #[arg(long)]
        input_db_path: PathBuf,
        /// Mapping from input column family to output column family, in the form
        /// `input_cf=output_cf`. Can be passed multiple times.
        #[arg(long = "cf-mapping", value_parser = parse_column_family_mapping, num_args = 1..)]
        cf_mappings: Vec<ColumnFamilyMapping>,
    },

    /// List all column families in the RocksDB database.
    ListColumnFamilies {
        /// Path to the RocksDB database directory.
        #[arg(long)]
        db_path: PathBuf,
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

    /// Decode a blob info snapshot file, print its header and per-section entry counts, and
    /// re-encode it with the node's writer, asserting a byte-identical round-trip.
    DecodeBlobInfoSnapshot {
        /// Path to the snapshot file, as written by the node or read back as a blob.
        #[arg(long)]
        input: PathBuf,
    },

    /// Check that a blob info snapshot file has the given blob ID.
    ///
    /// Encodes the file the way a node does when it publishes its snapshot (default encoding, the
    /// network's shard count) and compares the resulting blob ID with `--blob-id`, for example a
    /// certified snapshot's blob ID from the System object. Fails on a mismatch. Run it before
    /// loading a file that did not come from the read path, which already verifies the blob ID.
    VerifyBlobInfoSnapshot {
        /// Path to the snapshot file.
        #[arg(long)]
        input: PathBuf,
        /// The expected blob ID, in URL-safe base64 format (no padding).
        #[arg(long)]
        #[serde_as(as = "DisplayFromStr")]
        blob_id: BlobId,
        /// The number of shards of the network the snapshot was published on.
        #[arg(long)]
        n_shards: NonZeroU16,
    },

    /// Replace a stopped node's blob info tables with the contents of a blob info snapshot.
    ///
    /// Clears and refills `per_object_blob_info`, `per_object_pooled_blob_info`, and
    /// `storage_pool_info`, rebuilds `aggregate_blob_info` from them and from the entries of the
    /// blobs with stored metadata (a blob among them without live objects keeps an entry, so
    /// that garbage collection deletes its data), moves the event cursor to the snapshot's
    /// boundary event, sets the garbage collector's progress to the snapshot's epoch, and sets
    /// the node status to `RecoveryCatchUp`, all in one atomic write. On its next start, the
    /// node replays events from the snapshot's epoch boundary and then recovers any missing data
    /// through the regular recovery path. Metadata and slivers are not touched, except those of
    /// the blobs a format-2 snapshot marks invalid, which are deleted together with their
    /// pending-recovery records. The node must be stopped; back up the database first.
    LoadBlobInfoSnapshot {
        /// Path to the RocksDB database directory; must exist.
        #[arg(long)]
        db_path: PathBuf,
        /// Path to the snapshot file, as written by the node or read back as a blob.
        #[arg(long)]
        input: PathBuf,
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
            Self::ReadPendingRecoverBlobs {
                db_path,
                start_blob_id,
                count,
            } => read_pending_recover_blobs(db_path, start_blob_id, count),
            Self::CountCertifiedBlobs { db_path, epoch } => count_certified_blobs(db_path, epoch),
            Self::DropColumnFamilies {
                db_path,
                column_family_names,
            } => drop_column_families(db_path, column_family_names),
            Self::WipeNodeState {
                db_path,
                wipe_event_store,
                skip_checkpoint,
            } => wipe_node_state(db_path, wipe_event_store, skip_checkpoint),
            Self::RestoreNodeState { db_path, wiped_dir } => restore_node_state(db_path, wiped_dir),
            Self::RestoreColumnFamilies {
                db_path,
                input_db_path,
                cf_mappings,
            } => restore_column_families(db_path, input_db_path, cf_mappings),
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
            Self::DecodeBlobInfoSnapshot { input } => decode_blob_info_snapshot(input),
            Self::VerifyBlobInfoSnapshot {
                input,
                blob_id,
                n_shards,
            } => verify_blob_info_snapshot(input, blob_id, n_shards),
            Self::LoadBlobInfoSnapshot { db_path, input } => {
                load_blob_info_snapshot(db_path, input)
            }
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

fn read_pending_recover_blobs(
    db_path: PathBuf,
    start_blob_id: Option<BlobId>,
    count: usize,
) -> Result<()> {
    let pending_recover_blobs_options =
        DatabaseTableOptionsFactory::new(DatabaseConfig::default(), false).pending_recover_blobs();
    let db = DB::open_cf_with_opts_for_read_only(
        &RocksdbOptions::default(),
        db_path,
        [(
            pending_recover_blobs_cf_name(),
            pending_recover_blobs_options,
        )],
        false,
    )?;

    let cf = db
        .cf_handle(pending_recover_blobs_cf_name())
        .expect("pending recover blobs column family should exist");

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
                let record: PendingRecoverBlob = bcs::from_bytes(&value)?;
                println!("Blob ID: {blob_id}, PendingRecoverBlob: {record:?}");
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
    let db = open_database_with_column_families(&db_path, &column_families)?;
    drop_column_families_of(&db, column_family_names)
}

/// Opens the database at `db_path` with the given column families, to change its column
/// families. Opening takes the database's lock, which a running node holds, so this fails while
/// the node runs.
fn open_database_with_column_families(db_path: &Path, column_families: &[String]) -> Result<DB> {
    let db_kind = detect_probe_db_kind(column_families);
    let db_table_opts_factory = db_table_options_factory_for_kind(db_kind);

    let mut db_opts = RocksdbOptions::default();
    db_opts.set_max_open_files(512_000);

    println!(
        "Opening the database; replaying its write-ahead log flushes every column family and can \
        take minutes on a node with many shards (progress in {}/LOG)",
        db_path.display()
    );
    let db = DB::open_cf_with_opts(
        &db_opts,
        db_path,
        cf_options(column_families, db_kind, &db_table_opts_factory),
    )
    .inspect_err(|_| {
        println!(
            "failed to open database; \
            make sure to stop the storage node before changing its column families"
        )
    })?;
    Ok(db)
}

/// Drops the given column families of an open database, every one of them before failing on
/// those that could not be dropped.
fn drop_column_families_of(db: &DB, column_family_names: Vec<String>) -> Result<()> {
    let mut failed = Vec::new();
    for column_family_name in column_family_names {
        println!("Dropping column family: {column_family_name}");
        match db.drop_cf(column_family_name.as_str()) {
            Ok(()) => println!("Success."),
            Err(error) => {
                println!("Failed to drop column family: {error:?}");
                failed.push(column_family_name);
            }
        }
    }
    anyhow::ensure!(
        failed.is_empty(),
        "failed to drop the column families {failed:?}"
    );
    Ok(())
}

/// Whether [`wipe_node_state`] keeps a column family: the blob data, in the metadata table and
/// the per-shard tables (which `ShardStorage` names `shard-<index>/...`), and the node's records
/// of it, in the aggregate blob info table (reduced to the entries of the blobs with stored
/// metadata) and the snapshot publication record.
fn kept_by_wipe(column_family_name: &str) -> bool {
    column_family_name == metadata_cf_name()
        || column_family_name.starts_with("shard-")
        || column_family_name == aggregate_blob_info_cf_name()
        || column_family_name == blob_info_snapshot_publication_cf_name()
}

/// Entries per write while [`wipe_node_state`] reduces the aggregate blob info table.
const WIPE_WRITE_BATCH_SIZE: usize = 10_000;

/// `<directory>/<prefix>-<timestamp>`, or the first of `<directory>/<prefix>-<timestamp>-2`,
/// `-3`, ... that does not exist, when a run in the same second created it already.
fn unused_directory(directory: &Path, prefix: &str, timestamp: u64) -> PathBuf {
    let path = directory.join(format!("{prefix}-{timestamp}"));
    if !path.exists() {
        return path;
    }
    let mut suffix = 2;
    loop {
        let path = directory.join(format!("{prefix}-{timestamp}-{suffix}"));
        if !path.exists() {
            return path;
        }
        suffix += 1;
    }
}

/// The file `wipe-node-state` and `restore-node-state` keep in the storage path from their first
/// change to their last, naming the command and its `wiped-<timestamp>` directory. A node
/// refuses to start while it exists (see [`ensure_no_interrupted_wipe`]), so an interrupted run
/// is finished by running the command again rather than by starting the node on a half-wiped
/// state, in which an event replay would count the kept blob info entries again.
pub const WIPE_IN_PROGRESS_FILE_NAME: &str = "wipe-in-progress";

/// The command names an in-progress marker records.
const WIPE_COMMAND: &str = "wipe-node-state";
const RESTORE_COMMAND: &str = "restore-node-state";

/// What an in-progress marker records: the command that wrote it and the `wiped-<timestamp>`
/// directory it works with, so that a run that finishes an interrupted one uses the same.
#[derive(Debug, PartialEq, Eq)]
struct InProgressMarker {
    command: String,
    wiped_dir: PathBuf,
}

/// Writes the in-progress marker of `command` working with `wiped_dir` and makes it durable, so
/// that it outlives a power loss as the changes after it do.
fn write_in_progress_marker(
    storage_path: &Path,
    command: &str,
    wiped_dir: &Path,
    timestamp: u64,
) -> Result<PathBuf> {
    use std::io::Write as _;

    let marker = storage_path.join(WIPE_IN_PROGRESS_FILE_NAME);
    let mut file = std::fs::File::create(&marker)
        .with_context(|| format!("failed to write {}", marker.display()))?;
    file.write_all(
        format!(
            "command: {command}\ndirectory: {}\nstarted at {timestamp} (seconds since the Unix \
            epoch) and not finished; run `db-tool {command}` again to finish it, which removes \
            this file\n",
            wiped_dir.display()
        )
        .as_bytes(),
    )?;
    file.sync_all()?;
    sync_directory(storage_path)?;
    println!(
        "Wrote {}; the node refuses to start while it exists",
        marker.display()
    );
    Ok(marker)
}

/// Reads the in-progress marker in `storage_path`, if any; fails on a file that is not one.
fn read_in_progress_marker(storage_path: &Path) -> Result<Option<InProgressMarker>> {
    let marker = storage_path.join(WIPE_IN_PROGRESS_FILE_NAME);
    let content = match std::fs::read_to_string(&marker) {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| format!("failed to read {}", marker.display()));
        }
    };
    let mut lines = content.lines();
    let mut parse = || {
        let command = lines.next()?.strip_prefix("command: ")?;
        let directory = lines.next()?.strip_prefix("directory: ")?;
        Some(InProgressMarker {
            command: command.to_string(),
            wiped_dir: PathBuf::from(directory),
        })
    };
    parse().map(Some).with_context(|| {
        format!(
            "{} is not a marker db-tool wrote; remove it if no db-tool run on the storage path \
            is in progress",
            marker.display()
        )
    })
}

/// Makes the entries of the directory at `path` durable: a rename or a file creation is not
/// until the directory is synced.
fn sync_directory(path: &Path) -> Result<()> {
    std::fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .with_context(|| format!("failed to sync the directory {}", path.display()))
}

/// Whether `a` and `b` name the same directory, as given or once resolved.
fn same_directory(a: &Path, b: &Path) -> bool {
    a == b
        || matches!(
            (std::fs::canonicalize(a), std::fs::canonicalize(b)),
            (Ok(a), Ok(b)) if a == b
        )
}

/// Fails if a `wipe-node-state` or `restore-node-state` run on the storage at `storage_path` was
/// interrupted, so that the node does not start on a half-wiped state; see
/// [`WIPE_IN_PROGRESS_FILE_NAME`]. The node calls this while holding the database lock, which
/// both commands hold whenever they write the file, so the check cannot pass just before a run
/// starts.
pub fn ensure_no_interrupted_wipe(storage_path: &Path) -> Result<()> {
    let marker = storage_path.join(WIPE_IN_PROGRESS_FILE_NAME);
    if !marker.exists() {
        return Ok(());
    }
    let command = read_in_progress_marker(storage_path)
        .ok()
        .flatten()
        .map_or_else(
            || format!("{WIPE_COMMAND} or {RESTORE_COMMAND}"),
            |interrupted| interrupted.command,
        );
    anyhow::bail!(
        "a db-tool {command} run on {} was interrupted ({} exists); run the command again to \
        finish it before starting the node",
        storage_path.display(),
        marker.display()
    )
}

/// The start of the event store at `events_path`: the index of its first event and whether an
/// init state is recorded at it, which is what makes a brand-new node's start load a snapshot
/// instead of replaying every event (see `blob_info_snapshot_bootstrap`); `None` for an empty
/// store.
fn kept_event_store_start(events_path: &Path) -> Result<Option<(u64, bool)>> {
    let table_options = DatabaseTableOptionsFactory::new(DatabaseConfig::default(), false);
    let db = DB::open_cf_with_opts_for_read_only(
        &RocksdbOptions::default(),
        events_path,
        [
            (
                event_processor_constants::EVENT_STORE,
                table_options.event_store(),
            ),
            (
                event_processor_constants::INIT_STATE,
                table_options.init_state(),
            ),
        ],
        false,
    )?;
    let event_store = db
        .cf_handle(event_processor_constants::EVENT_STORE)
        .context("the event store column family should exist")?;
    let Some(first) = db
        .iterator_cf(&event_store, rocksdb::IteratorMode::Start)
        .next()
    else {
        return Ok(None);
    };
    let (key, _) = first?;
    let init_state = db
        .cf_handle(event_processor_constants::INIT_STATE)
        .context("the init state column family should exist")?;
    let has_init_state = db.get_cf(&init_state, &key)?.is_some();
    let first_index: u64 = bincode::DefaultOptions::new()
        .with_big_endian()
        .with_fixint_encoding()
        .deserialize(&key)?;
    Ok(Some((first_index, has_init_state)))
}

/// Wipes a stopped node's state except its blob data and its records of it; see the command's
/// documentation. Every step is safe to repeat, so a run finishes what an interrupted one left.
fn wipe_node_state(db_path: PathBuf, wipe_event_store: bool, skip_checkpoint: bool) -> Result<()> {
    let column_families = DB::list_cf(&RocksdbOptions::default(), &db_path)
        .context("failed to list the column families; is this the node's storage path?")?;
    let (kept, dropped): (Vec<_>, Vec<_>) = column_families
        .iter()
        .filter(|name| *name != "default")
        .cloned()
        .partition(|name| kept_by_wipe(name));

    // Opening the database takes its lock, which a running node holds, so nothing below touches
    // the state of a running node.
    let db = open_database_with_column_families(&db_path, &column_families)?;

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system time is before the Unix epoch")?
        .as_secs();
    let marker = db_path.join(WIPE_IN_PROGRESS_FILE_NAME);

    // A run that finishes an interrupted one works with the directory the marker records, where
    // the checkpoint taken before the first change already is, and takes none of the half-wiped
    // state. The checkpoint is the before-image `restore-node-state` reverts the wipe from.
    let aside_dir = match read_in_progress_marker(&db_path)? {
        Some(interrupted) if interrupted.command == WIPE_COMMAND => {
            println!(
                "Finishing the interrupted run recorded in {}, which works with {}",
                marker.display(),
                interrupted.wiped_dir.display()
            );
            interrupted.wiped_dir
        }
        Some(interrupted) => anyhow::bail!(
            "a db-tool {} run on {} was interrupted ({} exists); run that command again to \
            finish it before wiping",
            interrupted.command,
            db_path.display(),
            marker.display()
        ),
        None => {
            // The aside directory lives inside the storage path: a rename never crosses a
            // filesystem boundary when the storage path is a mount point, and RocksDB ignores
            // the extra directory.
            let aside_dir = unused_directory(&db_path, "wiped", timestamp);
            if skip_checkpoint {
                println!("Skipping the checkpoint of the database; the wipe cannot be reverted");
            } else {
                let checkpoint_dir = aside_dir.join(WIPE_CHECKPOINT_DIR_NAME);
                std::fs::create_dir_all(&aside_dir)?;
                println!(
                    "Taking a checkpoint of the database in {}",
                    checkpoint_dir.display()
                );
                rocksdb::checkpoint::Checkpoint::new(&db)?
                    .create_checkpoint(&checkpoint_dir)
                    .with_context(|| {
                        format!(
                            "failed to take a checkpoint of the database in {}",
                            checkpoint_dir.display()
                        )
                    })?;
                // The checkpoint is renamed into place last; the rename is durable once its
                // directory is synced.
                sync_directory(&aside_dir)?;
            }
            // The marker keeps the node from starting until the last step is done.
            write_in_progress_marker(&db_path, WIPE_COMMAND, &aside_dir, timestamp)?;
            aside_dir
        }
    };

    // Move the directories before dropping the column families, so that a failed move leaves
    // the node's database untouched.
    // The event processor's database (`EventProcessorRuntime`), the event blob writer's state
    // (`EventBlobWriterFactory`), and the local snapshot files.
    let mut subdirectories = vec![
        db_path.join("event_blob_writer"),
        snapshot_base_dir(&db_path),
    ];
    let events = db_path.join("events");
    if wipe_event_store {
        subdirectories.push(events.clone());
    } else {
        println!(
            "Keeping the event processor's database: {}",
            events.display()
        );
    }
    for directory in subdirectories.iter().filter(|directory| directory.exists()) {
        std::fs::create_dir_all(&aside_dir)?;
        let target = aside_dir.join(
            directory
                .file_name()
                .context("the subdirectory has no final component")?,
        );
        std::fs::rename(directory, &target).with_context(|| {
            format!(
                "failed to move {} to {}",
                directory.display(),
                target.display()
            )
        })?;
        println!("Moved {} to {}", directory.display(), target.display());
    }

    println!("Keeping column families: {kept:?}");
    drop_column_families_of(&db, dropped)?;
    // The storage below opens the database itself.
    drop(db);

    // The aggregate blob info table stays as the node's record of the blobs it holds data for,
    // reduced to that record so that whatever rebuilds the blob info next can start from it:
    // the snapshot load keeps those blobs known to garbage collection, and an event replay from
    // the first event merges onto them. Opening the storage recreates the dropped column
    // families empty, as the node's own start would.
    let (kept_entries, deleted_entries) = reduce_aggregate_blob_info(&db_path)?;
    println!(
        "Reduced the aggregate blob info table to the {kept_entries} entries of the blobs with \
        stored metadata ({deleted_entries} other entries deleted)"
    );

    std::fs::remove_file(&marker)
        .with_context(|| format!("failed to remove {}", marker.display()))?;
    sync_directory(&db_path)?;
    let next_start = if wipe_event_store || !events.exists() {
        "On its next start the node downloads the event blobs, loads the latest certified blob \
        info snapshot, and replays the events since its boundary."
            .to_string()
    } else {
        match kept_event_store_start(&events) {
            Ok(Some((first_index, true))) if first_index > 0 => format!(
                "On its next start the node loads the latest certified blob info snapshot and \
                replays the events since its boundary; its event store starts at event \
                {first_index}."
            ),
            Ok(Some((first_index, _))) => format!(
                "On its next start the node replays every event from the first one instead of \
                loading a snapshot, as its event store starts at event {first_index} without an \
                init state."
            ),
            Ok(None) => "On its next start the node downloads the event blobs, loads the latest \
                certified blob info snapshot, and replays the events since its boundary; its \
                event store is empty."
                .to_string(),
            Err(error) => format!(
                "Could not inspect the event store ({error:#}). On its next start the node loads \
                the latest certified blob info snapshot if the store starts after the first \
                event, or replays every event if it reaches it."
            ),
        }
    };
    println!(
        "Done. {next_start} The node then recovers any blob data it does not hold, and garbage \
        collection deletes the data of the blobs it does not list."
    );
    if aside_dir.join(WIPE_CHECKPOINT_DIR_NAME).is_dir() {
        println!(
            "`restore-node-state --db-path {} --wiped-dir {}` reverts the wipe.",
            db_path.display(),
            aside_dir.display()
        );
    }
    if aside_dir.exists() {
        println!(
            "Delete {} once the node is back in service.",
            aside_dir.display()
        );
    }
    Ok(())
}

/// The checkpoint of the database that [`wipe_node_state`] takes in its `wiped-<timestamp>`
/// directory before its first change, from which [`restore_node_state`] reverts the wipe.
pub const WIPE_CHECKPOINT_DIR_NAME: &str = "db-checkpoint";

/// Reverts a wipe on a stopped node from its `wiped-<timestamp>` directory; see the command's
/// documentation. Every step is safe to repeat, so a run finishes what an interrupted one left.
fn restore_node_state(db_path: PathBuf, wiped_dir: PathBuf) -> Result<()> {
    let checkpoint_dir = wiped_dir.join(WIPE_CHECKPOINT_DIR_NAME);
    anyhow::ensure!(
        checkpoint_dir.is_dir(),
        "{} holds no checkpoint of the database ({} does not exist): the wipe ran with \
        --skip-checkpoint, so its tables cannot be restored",
        wiped_dir.display(),
        checkpoint_dir.display()
    );
    let checkpoint_column_families = DB::list_cf(&RocksdbOptions::default(), &checkpoint_dir)
        .with_context(|| {
            format!(
                "failed to list the column families of the checkpoint {}",
                checkpoint_dir.display()
            )
        })?;
    let column_families = DB::list_cf(&RocksdbOptions::default(), &db_path)
        .context("failed to list the column families; is this the node's storage path?")?;

    // Opening the database takes its lock, which a running node holds, so nothing below touches
    // the state of a running node. The handle is released before the copy, which opens the
    // database itself; the marker keeps a node from starting in between.
    let db = open_database_with_column_families(&db_path, &column_families)?;

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system time is before the Unix epoch")?
        .as_secs();
    // An interrupted wipe is reverted, and an interrupted restore finished, with its own
    // directory; another one would leave what that run moved aside where it is.
    if let Some(interrupted) = read_in_progress_marker(&db_path)?
        && !same_directory(&interrupted.wiped_dir, &wiped_dir)
    {
        anyhow::bail!(
            "a db-tool {} run on {} working with {} was interrupted; restore from that directory \
            first",
            interrupted.command,
            db_path.display(),
            interrupted.wiped_dir.display()
        );
    }
    let marker = write_in_progress_marker(&db_path, RESTORE_COMMAND, &wiped_dir, timestamp)?;

    // The directories the wipe moved aside go back; one the node created since goes aside.
    let replaced_dir = unused_directory(&wiped_dir, "replaced", timestamp);
    for target in [
        db_path.join("event_blob_writer"),
        snapshot_base_dir(&db_path),
        db_path.join("events"),
    ] {
        let name = target
            .file_name()
            .context("the subdirectory has no final component")?;
        let source = wiped_dir.join(name);
        if !source.exists() {
            continue;
        }
        if target.exists() {
            std::fs::create_dir_all(&replaced_dir)?;
            let replaced = replaced_dir.join(name);
            std::fs::rename(&target, &replaced).with_context(|| {
                format!(
                    "failed to move {} to {}",
                    target.display(),
                    replaced.display()
                )
            })?;
            println!("Moved {} to {}", target.display(), replaced.display());
        }
        std::fs::rename(&source, &target).with_context(|| {
            format!(
                "failed to move {} back to {}",
                source.display(),
                target.display()
            )
        })?;
        println!("Moved {} back to {}", source.display(), target.display());
    }
    // The copy below opens the database itself.
    drop(db);

    // The tables the wipe dropped or reduced; the rest it left as they were.
    let cf_mappings = checkpoint_column_families
        .iter()
        .filter(|name| {
            *name != "default" && (!kept_by_wipe(name) || *name == aggregate_blob_info_cf_name())
        })
        .map(|name| ColumnFamilyMapping {
            input: name.clone(),
            output: name.clone(),
        })
        .collect();
    restore_column_families(db_path.clone(), checkpoint_dir, cf_mappings)?;

    std::fs::remove_file(&marker)
        .with_context(|| format!("failed to remove {}", marker.display()))?;
    sync_directory(&db_path)?;
    println!(
        "Done. The node starts where the wipe found it. Delete {} once it is back in service.",
        wiped_dir.display()
    );
    Ok(())
}

/// Reduces the aggregate blob info table of the database at `db_path` to the entries of the
/// blobs with stored metadata; see `Storage::reduce_aggregate_blob_info_to_stored_metadata`.
fn reduce_aggregate_blob_info(db_path: &Path) -> Result<(usize, usize)> {
    // Opening the storage starts database metrics tasks, which need a runtime.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let _runtime_guard = runtime.enter();
    let storage = Storage::open(
        db_path,
        DatabaseConfig::default(),
        MetricConf::default(),
        Registry::default(),
    )
    .with_context(|| format!("failed to open the database at {}", db_path.display()))?;
    Ok(storage.reduce_aggregate_blob_info_to_stored_metadata(WIPE_WRITE_BATCH_SIZE)?)
}

const RESTORE_CF_WRITE_BATCH_SIZE: usize = 10_000;

fn restore_column_families(
    db_path: PathBuf,
    input_db_path: PathBuf,
    cf_mappings: Vec<ColumnFamilyMapping>,
) -> Result<()> {
    let db_path = canonicalize_db_path(&db_path, "output Walrus DB")?;
    let input_db_path = canonicalize_db_path(&input_db_path, "input DB")?;
    if db_path == input_db_path {
        bail!("input DB path and output Walrus DB path must be different");
    }

    validate_column_family_mappings(&cf_mappings)?;

    let input_column_families = DB::list_cf(&RocksdbOptions::default(), &input_db_path)
        .with_context(|| {
            format!(
                "failed to list column families in input DB `{}`",
                input_db_path.display()
            )
        })?;
    validate_input_column_families_exist(&cf_mappings, &input_column_families)?;

    let output_column_families =
        DB::list_cf(&RocksdbOptions::default(), &db_path).with_context(|| {
            format!(
                "failed to list column families in output Walrus DB `{}`",
                db_path.display()
            )
        })?;
    let output_db_kind = detect_probe_db_kind(&output_column_families);
    let output_db_table_opts_factory = db_table_options_factory_for_kind(output_db_kind);

    let input_db_kind = detect_probe_db_kind(&input_column_families);
    let input_db_table_opts_factory = db_table_options_factory_for_kind(input_db_kind);
    let mut input_db_opts = RocksdbOptions::from(&input_db_table_opts_factory.global());
    input_db_opts.create_if_missing(false);
    input_db_opts.create_missing_column_families(false);

    let input_db = DB::open_cf_with_opts_for_read_only(
        &input_db_opts,
        &input_db_path,
        input_cf_options(
            &input_column_families,
            input_db_kind,
            &input_db_table_opts_factory,
            output_db_kind,
            &output_db_table_opts_factory,
            &cf_mappings,
        ),
        false,
    )
    .with_context(|| format!("failed to open input DB `{}`", input_db_path.display()))?;

    let mut output_db_opts = RocksdbOptions::from(&output_db_table_opts_factory.global());
    output_db_opts.create_if_missing(false);
    output_db_opts.create_missing_column_families(false);

    let output_db = DB::open_cf_with_opts(
        &output_db_opts,
        &db_path,
        cf_options(
            &output_column_families,
            output_db_kind,
            &output_db_table_opts_factory,
        ),
    )
    .inspect_err(|_| {
        println!(
            "failed to open output Walrus DB; \
            make sure to stop the storage node before attempting to restore column families"
        )
    })
    .with_context(|| format!("failed to open output Walrus DB `{}`", db_path.display()))?;

    println!(
        "Restoring {} column family mapping(s) from {} into {}",
        cf_mappings.len(),
        input_db_path.display(),
        db_path.display()
    );

    let mut total_entries = 0_u64;
    for mapping in cf_mappings {
        let copied = restore_column_family(
            &input_db,
            &output_db,
            &mapping,
            output_db_kind,
            &output_db_table_opts_factory,
        )?;
        total_entries += copied;
    }

    println!("Finished restoring column families; copied {total_entries} entries total");
    Ok(())
}

fn canonicalize_db_path(db_path: &Path, label: &str) -> Result<PathBuf> {
    std::fs::canonicalize(db_path).with_context(|| {
        format!(
            "failed to canonicalize {label} path `{}`",
            db_path.display()
        )
    })
}

fn validate_column_family_mappings(cf_mappings: &[ColumnFamilyMapping]) -> Result<()> {
    if cf_mappings.is_empty() {
        bail!("at least one column family mapping is required");
    }

    let mut input_column_families = BTreeSet::new();
    let mut output_column_families = BTreeSet::new();
    for mapping in cf_mappings {
        if !input_column_families.insert(mapping.input.as_str()) {
            bail!(
                "input column family `{}` is listed more than once",
                mapping.input
            );
        }
        if !output_column_families.insert(mapping.output.as_str()) {
            bail!(
                "output column family `{}` is listed more than once",
                mapping.output
            );
        }
    }

    Ok(())
}

fn validate_input_column_families_exist(
    cf_mappings: &[ColumnFamilyMapping],
    input_column_families: &[String],
) -> Result<()> {
    let input_column_family_set = input_column_families
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();

    for mapping in cf_mappings {
        if !input_column_family_set.contains(mapping.input.as_str()) {
            bail!(
                "input column family `{}` does not exist in input DB",
                mapping.input
            );
        }
    }

    Ok(())
}

fn restore_column_family(
    input_db: &DB,
    output_db: &DB,
    mapping: &ColumnFamilyMapping,
    output_db_kind: ProbeDbKind,
    output_db_table_opts_factory: &DatabaseTableOptionsFactory,
) -> Result<u64> {
    let input_cf = input_db
        .cf_handle(&mapping.input)
        .with_context(|| format!("input column family `{}` should exist", mapping.input))?;

    if output_db.cf_handle(&mapping.output).is_some() {
        println!("Dropping output column family: {}", mapping.output);
        output_db
            .drop_cf(&mapping.output)
            .with_context(|| format!("failed to drop output column family `{}`", mapping.output))?;
    } else {
        println!(
            "Output column family {} does not exist; creating it",
            mapping.output
        );
    }

    let output_cf_options = cf_options_for_name(
        &mapping.output,
        output_db_kind,
        output_db_table_opts_factory,
    );
    output_db
        .create_cf(&mapping.output, &output_cf_options)
        .with_context(|| format!("failed to create output column family `{}`", mapping.output))?;
    let output_cf = output_db
        .cf_handle(&mapping.output)
        .with_context(|| format!("output column family `{}` should exist", mapping.output))?;

    println!(
        "Copying input column family {} to output column family {}",
        mapping.input, mapping.output
    );

    let mut copied = 0_u64;
    let mut batch = WriteBatch::default();
    for entry in input_db.iterator_cf(&input_cf, rocksdb::IteratorMode::Start) {
        let (key, value) = entry.with_context(|| {
            format!(
                "failed while reading input column family `{}`",
                mapping.input
            )
        })?;
        batch.put_cf(&output_cf, key, value);
        copied += 1;

        if batch.len() >= RESTORE_CF_WRITE_BATCH_SIZE {
            output_db.write(batch).with_context(|| {
                format!("failed to write output column family `{}`", mapping.output)
            })?;
            batch = WriteBatch::default();
            println!("Copied {copied} entries into {}", mapping.output);
        }
    }

    if !batch.is_empty() {
        output_db.write(batch).with_context(|| {
            format!("failed to write output column family `{}`", mapping.output)
        })?;
    }

    output_db
        .flush_cf_opt(&output_cf, &FlushOptions::default())
        .with_context(|| format!("failed to flush output column family `{}`", mapping.output))?;

    println!(
        "Restored output column family {} from input column family {}; copied {} entries",
        mapping.output, mapping.input, copied
    );

    Ok(copied)
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
        pending_recover_blobs_cf_name(),
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

fn input_cf_options<'a>(
    input_column_families: &'a [String],
    input_db_kind: ProbeDbKind,
    input_factory: &DatabaseTableOptionsFactory,
    output_db_kind: ProbeDbKind,
    output_factory: &DatabaseTableOptionsFactory,
    cf_mappings: &[ColumnFamilyMapping],
) -> Vec<(&'a str, RocksdbOptions)> {
    let output_by_input = cf_mappings
        .iter()
        .map(|mapping| (mapping.input.as_str(), mapping.output.as_str()))
        .collect::<BTreeMap<_, _>>();

    input_column_families
        .iter()
        .map(|cf_name| {
            let options = output_by_input
                .get(cf_name.as_str())
                .map(|output_cf_name| {
                    cf_options_for_name(output_cf_name, output_db_kind, output_factory)
                })
                .unwrap_or_else(|| cf_options_for_name(cf_name, input_db_kind, input_factory));
            (cf_name.as_str(), options)
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
                name if name == pending_recover_blobs_cf_name() => factory.pending_recover_blobs(),
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

/// Decodes a snapshot file, prints its header and entry counts, re-encodes it with the writer, and
/// asserts a byte-identical round-trip.
fn decode_blob_info_snapshot(input: PathBuf) -> Result<()> {
    let bytes = std::fs::read(&input)
        .with_context(|| format!("failed to read snapshot file {}", input.display()))?;
    let snapshot = blob_info_snapshot::read_snapshot(&bytes)?;

    // Re-encode in the input's own format, so that a version-1 file stays a version-1 file.
    let mut cursor = Cursor::new(Vec::with_capacity(bytes.len()));
    snapshot.write(&mut cursor)?;
    let reencoded = cursor.into_inner();

    let header = &snapshot.header;
    let event_id = header.event_cursor.event_id();
    println!("Blob info snapshot ({})", input.display());
    println!("  format version:   {}", snapshot.format.version());
    println!("  epoch in header:  {}", header.epoch);
    println!(
        "  event cursor:     tx {} seq {}, next event index {}",
        event_id.tx_digest,
        event_id.event_seq,
        header.event_cursor.next_event_index()
    );
    println!(
        "  decoded entries:  per_object {}, pooled {}, pool {}, invalid {}",
        snapshot.per_object.len(),
        snapshot.per_object_pooled.len(),
        snapshot.storage_pools.len(),
        invalid_blobs_summary(&snapshot),
    );
    println!("  input size:       {} bytes", bytes.len());
    println!("  re-encoded size:  {} bytes", reencoded.len());
    if reencoded == bytes {
        println!("  ROUND-TRIP OK: decode -> re-encode is byte-identical");
        Ok(())
    } else {
        bail!(
            "round-trip mismatch: re-encoded {} bytes differ from input {} bytes",
            reencoded.len(),
            bytes.len()
        )
    }
}

/// Describes a decoded snapshot's invalidated blob IDs: their count, or that the format omits them.
fn invalid_blobs_summary(snapshot: &blob_info_snapshot::DecodedSnapshot) -> String {
    match &snapshot.invalid_blobs {
        Some(invalid_blobs) => invalid_blobs.len().to_string(),
        None => format!(
            "omitted (format version {} carries no invalidation history)",
            snapshot.format.version()
        ),
    }
}

/// Encodes a snapshot file as a node does when publishing it and compares the blob ID.
fn verify_blob_info_snapshot(input: PathBuf, blob_id: BlobId, n_shards: NonZeroU16) -> Result<()> {
    // With fewer than four shards no shard may be faulty, the encoding has no recovery symbols,
    // and encoding panics.
    if n_shards.get() < 4 {
        bail!("the number of shards must be at least 4, got {n_shards}");
    }
    let bytes = std::fs::read(&input)
        .with_context(|| format!("failed to read snapshot file {}", input.display()))?;
    let metadata = EncodingConfig::new(n_shards)
        .get_for_type(DEFAULT_ENCODING)
        .compute_metadata(&bytes)
        .context("failed to encode the snapshot file")?;
    let computed = *metadata.blob_id();

    println!("Blob info snapshot ({})", input.display());
    println!("  size:              {} bytes", bytes.len());
    println!("  expected blob ID:  {blob_id}");
    println!("  computed blob ID:  {computed} ({n_shards} shards)");
    if computed != blob_id {
        bail!(
            "blob ID mismatch: the file does not have the expected blob ID (a wrong --n-shards \
            also changes the computed blob ID)"
        );
    }
    println!("  VERIFIED: the file has the expected blob ID");
    Ok(())
}

/// Loads a blob info snapshot file into the database of a stopped node.
fn load_blob_info_snapshot(db_path: PathBuf, input: PathBuf) -> Result<()> {
    if !db_path.is_dir() {
        bail!("database directory {} does not exist", db_path.display());
    }
    let bytes = std::fs::read(&input)
        .with_context(|| format!("failed to read snapshot file {}", input.display()))?;
    let snapshot = blob_info_snapshot::read_snapshot(&bytes)?;

    // Opening the storage starts database metrics tasks, which need a runtime.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let _runtime_guard = runtime.enter();
    let storage = Storage::open(
        &db_path,
        DatabaseConfig::default(),
        MetricConf::default(),
        Registry::default(),
    )
    .with_context(|| format!("failed to open the database at {}", db_path.display()))?;
    let aggregate_count = storage.load_blob_info_snapshot(&snapshot)?;

    let header = &snapshot.header;
    println!("Loaded blob info snapshot ({})", input.display());
    println!("  format version:      {}", snapshot.format.version());
    println!("  epoch in header:     {}", header.epoch);
    println!(
        "  loaded entries:      per_object {}, pooled {}, pool {}",
        snapshot.per_object.len(),
        snapshot.per_object_pooled.len(),
        snapshot.storage_pools.len()
    );
    match &snapshot.invalid_blobs {
        Some(invalid_blobs) => println!(
            "  invalid blobs:       {} marked; their metadata, slivers, and pending-recovery \
            records are deleted",
            invalid_blobs.len()
        ),
        None => println!(
            "  invalid blobs:       invalidation history omitted (format version {}); blobs \
            invalidated before the boundary are not marked until a later event for them",
            snapshot.format.version()
        ),
    }
    println!("  rebuilt aggregate:   {aggregate_count} entries");
    println!(
        "  event cursor:        next event index {} (the boundary event)",
        header.event_cursor.next_event_index().saturating_sub(1)
    );
    println!("  node status:         RecoveryCatchUp");
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

    /// The wipe keeps the metadata, the slivers, and the aggregate table reduced to the entries
    /// of the blobs with stored metadata, as untracked blobs; the other entries and the event
    /// cursor are gone.
    #[test]
    #[cfg_attr(msim, ignore)]
    fn wipe_node_state_keeps_the_stored_blobs_recorded() -> Result<()> {
        use tokio::runtime::Runtime;
        use walrus_core::{
            SliverType,
            metadata::VerifiedBlobMetadataWithId,
            test_utils::{blob_id_from_u64, verified_blob_metadata},
        };
        use walrus_sui::{test_utils::EventForTesting, types::BlobRegistered};

        use crate::node::storage::{
            blob_info::BlobInfoApi,
            tests::{SHARD_INDEX, WhichSlivers, populated_storage},
        };

        let (stored, unstored) = (blob_id_from_u64(1), blob_id_from_u64(2));
        // Its own runtime, so that the tasks typed_store spawns are dropped with it and free the
        // storage lock.
        let directory = Runtime::new()?.block_on(async {
            let storage = populated_storage(&[(SHARD_INDEX, vec![(stored, WhichSlivers::Both)])])
                .await
                .map_err(|error| anyhow::anyhow!(error))?;
            for (index, blob_id) in [stored, unstored].into_iter().enumerate() {
                storage.inner.update_blob_info(
                    u64::try_from(index)?,
                    &BlobRegistered::for_testing(blob_id).into(),
                )?;
            }
            storage
                .inner
                .put_verified_metadata(&VerifiedBlobMetadataWithId::new_verified_unchecked(
                    stored,
                    verified_blob_metadata().metadata().clone(),
                ))
                .await?;
            Result::<_, anyhow::Error>::Ok(storage.temp_dir)
        })?;
        let events = directory.path().join("events");
        std::fs::create_dir(&events)?;
        std::fs::write(events.join("CURRENT"), "")?;

        wipe_node_state(directory.path().to_path_buf(), false, false)?;
        ensure_no_interrupted_wipe(directory.path())?;
        // A second run, as after an interruption, finds nothing left to do and leaves the same
        // state.
        wipe_node_state(directory.path().to_path_buf(), false, false)?;
        ensure_no_interrupted_wipe(directory.path())?;
        assert!(
            events.join("CURRENT").exists(),
            "the event store is kept unless asked otherwise"
        );

        Runtime::new()?.block_on(async {
            let storage = Storage::open(
                directory.path(),
                DatabaseConfig::default(),
                MetricConf::default(),
                Registry::default(),
            )?;
            let entry = storage
                .get_blob_info(&stored)?
                .expect("the blob with stored metadata keeps an entry");
            assert!(entry.is_metadata_stored());
            assert!(
                entry.can_blob_info_be_deleted(1),
                "the entry has no objects"
            );
            assert!(storage.get_blob_info(&unstored)?.is_none());
            assert!(storage.get_metadata(&stored)?.is_some());
            assert!(storage.get_event_cursor_and_next_index()?.is_none());
            let shard = storage
                .shard_storage(SHARD_INDEX)
                .await
                .expect("the shard is kept");
            assert!(shard.get_sliver(&stored, SliverType::Primary)?.is_some());
            Result::<(), anyhow::Error>::Ok(())
        })?;
        Ok(())
    }

    /// The wipe opens the database before it touches anything, so against a running node it
    /// fails without moving a directory or leaving the marker that keeps the node from starting.
    #[test]
    #[cfg_attr(msim, ignore)]
    fn wipe_node_state_refuses_a_running_node_before_touching_anything() -> Result<()> {
        use tokio::runtime::Runtime;

        use crate::node::storage::tests::{SHARD_INDEX, populated_storage};

        // The runtime and the storage are dropped explicitly: together they hold the database
        // lock, as a running node does.
        let runtime = Runtime::new()?;
        let storage = runtime
            .block_on(populated_storage(&[(SHARD_INDEX, vec![])]))
            .map_err(|error| anyhow::anyhow!(error))?;
        let (storage, directory) = (storage.inner, storage.temp_dir);
        let events = directory.path().join("events");
        std::fs::create_dir(&events)?;
        std::fs::write(events.join("CURRENT"), "")?;
        let marker = directory.path().join(WIPE_IN_PROGRESS_FILE_NAME);
        let aside_dirs = || -> Result<Vec<String>> {
            Ok(std::fs::read_dir(directory.path())?
                .filter_map(|entry| Some(entry.ok()?.file_name().to_string_lossy().into_owned()))
                .filter(|name| name.starts_with("wiped-"))
                .collect())
        };

        assert!(wipe_node_state(directory.path().to_path_buf(), true, false).is_err());
        assert!(events.join("CURRENT").exists());
        assert!(!marker.exists());
        assert!(aside_dirs()?.is_empty());
        ensure_no_interrupted_wipe(directory.path())?;

        drop(storage);
        drop(runtime);
        wipe_node_state(directory.path().to_path_buf(), true, false)?;
        assert!(!events.exists());
        assert!(!marker.exists());
        assert_eq!(aside_dirs()?.len(), 1);
        Ok(())
    }

    /// A node does not start on the state an interrupted wipe left, nor on a marker it cannot
    /// read.
    #[test]
    fn interrupted_wipe_keeps_the_node_from_starting() -> Result<()> {
        let directory = tempdir()?;
        ensure_no_interrupted_wipe(directory.path())?;
        let wiped_dir = directory.path().join("wiped-1");
        let marker = write_in_progress_marker(directory.path(), WIPE_COMMAND, &wiped_dir, 1)?;
        assert_eq!(
            read_in_progress_marker(directory.path())?,
            Some(InProgressMarker {
                command: WIPE_COMMAND.to_string(),
                wiped_dir,
            })
        );
        let error = ensure_no_interrupted_wipe(directory.path())
            .expect_err("the marker keeps the node from starting");
        assert!(error.to_string().contains("wipe-node-state"), "{error:#}");

        std::fs::write(&marker, "")?;
        assert!(read_in_progress_marker(directory.path()).is_err());
        let error = ensure_no_interrupted_wipe(directory.path())
            .expect_err("an unreadable marker keeps the node from starting too");
        assert!(error.to_string().contains("wipe-node-state"), "{error:#}");
        Ok(())
    }

    /// A run that finishes an interrupted wipe moves into the directory the marker records,
    /// where the checkpoint is, and takes no checkpoint of the half-wiped state.
    #[test]
    #[cfg_attr(msim, ignore)]
    fn wipe_node_state_resumes_into_the_recorded_directory() -> Result<()> {
        use tokio::runtime::Runtime;

        use crate::node::storage::tests::{SHARD_INDEX, populated_storage};

        let directory = Runtime::new()?.block_on(async {
            let storage = populated_storage(&[(SHARD_INDEX, vec![])])
                .await
                .map_err(|error| anyhow::anyhow!(error))?;
            Result::<_, anyhow::Error>::Ok(storage.temp_dir)
        })?;
        let events = directory.path().join("events");
        std::fs::create_dir(&events)?;
        std::fs::write(events.join("CURRENT"), "")?;
        // The interrupted run moved the writer's state aside and died before the rest.
        let wiped_dir = directory.path().join("wiped-1");
        std::fs::create_dir_all(wiped_dir.join("event_blob_writer"))?;
        write_in_progress_marker(directory.path(), WIPE_COMMAND, &wiped_dir, 1)?;

        wipe_node_state(directory.path().to_path_buf(), true, false)?;
        ensure_no_interrupted_wipe(directory.path())?;
        assert!(wiped_dir.join("events").join("CURRENT").exists());
        assert!(!wiped_dir.join(WIPE_CHECKPOINT_DIR_NAME).exists());
        assert_eq!(
            subdirectories_starting_with(directory.path(), "wiped-")?,
            vec![wiped_dir]
        );
        Ok(())
    }

    /// An interrupted restore is finished with its own directory before anything else runs.
    #[test]
    #[cfg_attr(msim, ignore)]
    fn interrupted_restore_blocks_other_runs() -> Result<()> {
        use tokio::runtime::Runtime;

        use crate::node::storage::tests::{SHARD_INDEX, populated_storage};

        let directory = Runtime::new()?.block_on(async {
            let storage = populated_storage(&[(SHARD_INDEX, vec![])])
                .await
                .map_err(|error| anyhow::anyhow!(error))?;
            Result::<_, anyhow::Error>::Ok(storage.temp_dir)
        })?;
        wipe_node_state(directory.path().to_path_buf(), true, false)?;
        let wiped_dirs = subdirectories_starting_with(directory.path(), "wiped-")?;
        assert_eq!(wiped_dirs.len(), 1);
        let other_dir = directory.path().join("wiped-other");
        let marker = write_in_progress_marker(directory.path(), RESTORE_COMMAND, &other_dir, 1)?;

        let error = wipe_node_state(directory.path().to_path_buf(), true, false)
            .expect_err("a wipe does not run over an interrupted restore");
        assert!(
            error.to_string().contains("restore-node-state"),
            "{error:#}"
        );
        let error = ensure_no_interrupted_wipe(directory.path())
            .expect_err("the marker keeps the node from starting");
        assert!(
            error.to_string().contains("restore-node-state"),
            "{error:#}"
        );
        let error = restore_node_state(directory.path().to_path_buf(), wiped_dirs[0].clone())
            .expect_err("a restore from another directory does not run either");
        assert!(error.to_string().contains("wiped-other"), "{error:#}");

        std::fs::remove_file(marker)?;
        restore_node_state(directory.path().to_path_buf(), wiped_dirs[0].clone())?;
        ensure_no_interrupted_wipe(directory.path())
    }

    /// The subdirectories of `directory` whose names start with `prefix`.
    fn subdirectories_starting_with(directory: &Path, prefix: &str) -> Result<Vec<PathBuf>> {
        Ok(std::fs::read_dir(directory)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_name().to_string_lossy().starts_with(prefix))
            .map(|entry| entry.path())
            .collect())
    }

    /// `restore-node-state` puts the node back where the wipe found it, from the checkpoint and
    /// the moved directories, and moves aside what the node created since.
    #[test]
    #[cfg_attr(msim, ignore)]
    fn restore_node_state_reverts_a_wipe() -> Result<()> {
        use tokio::runtime::Runtime;
        use walrus_core::test_utils::blob_id_from_u64;
        use walrus_sui::{test_utils::EventForTesting, types::BlobRegistered};

        use crate::node::storage::tests::{SHARD_INDEX, populated_storage};

        let blob_id = blob_id_from_u64(1);
        let (directory, before) = Runtime::new()?.block_on(async {
            let storage = populated_storage(&[(SHARD_INDEX, vec![])])
                .await
                .map_err(|error| anyhow::anyhow!(error))?;
            storage
                .inner
                .update_blob_info(0, &BlobRegistered::for_testing(blob_id).into())?;
            let before = storage.inner.get_blob_info(&blob_id)?;
            Result::<_, anyhow::Error>::Ok((storage.temp_dir, before))
        })?;
        assert!(before.is_some());
        let events = directory.path().join("events");
        std::fs::create_dir(&events)?;
        std::fs::write(events.join("CURRENT"), "")?;

        wipe_node_state(directory.path().to_path_buf(), true, false)?;
        let wiped_dirs = subdirectories_starting_with(directory.path(), "wiped-")?;
        assert_eq!(wiped_dirs.len(), 1);
        let wiped_dir = wiped_dirs[0].clone();
        assert!(wiped_dir.join(WIPE_CHECKPOINT_DIR_NAME).is_dir());
        // The node ran since and created a fresh event store.
        std::fs::create_dir(&events)?;
        std::fs::write(events.join("NEW"), "")?;
        // The wipe itself may have been interrupted: its marker names the same directory.
        write_in_progress_marker(directory.path(), WIPE_COMMAND, &wiped_dir, 1)?;

        restore_node_state(directory.path().to_path_buf(), wiped_dir.clone())?;
        ensure_no_interrupted_wipe(directory.path())?;
        // A second run, as after an interruption, finds nothing left to move and leaves the
        // same state.
        restore_node_state(directory.path().to_path_buf(), wiped_dir.clone())?;
        ensure_no_interrupted_wipe(directory.path())?;

        assert!(events.join("CURRENT").exists());
        assert!(!events.join("NEW").exists());
        let replaced_dirs = subdirectories_starting_with(&wiped_dir, "replaced-")?;
        assert_eq!(replaced_dirs.len(), 1);
        assert!(replaced_dirs[0].join("events").join("NEW").exists());

        Runtime::new()?.block_on(async {
            let storage = Storage::open(
                directory.path(),
                DatabaseConfig::default(),
                MetricConf::default(),
                Registry::default(),
            )?;
            assert_eq!(storage.get_blob_info(&blob_id)?, before);
            Result::<(), anyhow::Error>::Ok(())
        })?;
        Ok(())
    }

    /// A wipe that skipped the checkpoint cannot be reverted.
    #[test]
    #[cfg_attr(msim, ignore)]
    fn restore_node_state_needs_the_checkpoint() -> Result<()> {
        use tokio::runtime::Runtime;

        use crate::node::storage::tests::{SHARD_INDEX, populated_storage};

        let directory = Runtime::new()?.block_on(async {
            let storage = populated_storage(&[(SHARD_INDEX, vec![])])
                .await
                .map_err(|error| anyhow::anyhow!(error))?;
            Result::<_, anyhow::Error>::Ok(storage.temp_dir)
        })?;
        let events = directory.path().join("events");
        std::fs::create_dir(&events)?;
        std::fs::write(events.join("CURRENT"), "")?;

        wipe_node_state(directory.path().to_path_buf(), true, true)?;
        let wiped_dirs = subdirectories_starting_with(directory.path(), "wiped-")?;
        assert_eq!(wiped_dirs.len(), 1);
        assert!(!wiped_dirs[0].join(WIPE_CHECKPOINT_DIR_NAME).exists());

        let error = restore_node_state(directory.path().to_path_buf(), wiped_dirs[0].clone())
            .expect_err("the restore needs the checkpoint");
        assert!(error.to_string().contains("skip-checkpoint"), "{error:#}");
        assert!(wiped_dirs[0].join("events").join("CURRENT").exists());
        Ok(())
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
    fn restore_column_families_replaces_output_column_family() -> Result<()> {
        let input_db_dir = tempdir()?;
        let output_db_dir = tempdir()?;
        let db_table_opts_factory =
            DatabaseTableOptionsFactory::new(DatabaseConfig::default(), true);
        let mut db_opts = RocksdbOptions::from(&db_table_opts_factory.global());
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        let input_cf_name = "repaired-primary-slivers";
        let output_cf_name = primary_slivers_column_family_name(ShardIndex(7));

        let input_db = DB::open_cf_descriptors(
            &db_opts,
            input_db_dir.path(),
            vec![ColumnFamilyDescriptor::new(
                input_cf_name,
                db_table_opts_factory.shard(),
            )],
        )?;
        let input_cf = input_db
            .cf_handle(input_cf_name)
            .expect("input column family should exist");
        let recovered_large_value = vec![42; (1 << 20) + 1];
        input_db.put_cf(&input_cf, b"blob-1", b"recovered-1")?;
        input_db.put_cf(&input_cf, b"blob-2", &recovered_large_value)?;
        input_db.flush_cf(&input_cf)?;
        drop(input_cf);
        drop(input_db);

        let output_db = DB::open_cf_descriptors(
            &db_opts,
            output_db_dir.path(),
            vec![ColumnFamilyDescriptor::new(
                output_cf_name.clone(),
                db_table_opts_factory.shard(),
            )],
        )?;
        let output_cf = output_db
            .cf_handle(&output_cf_name)
            .expect("output column family should exist");
        output_db.put_cf(&output_cf, b"blob-1", b"corrupted")?;
        output_db.put_cf(&output_cf, b"stale", b"stale")?;
        drop(output_cf);
        drop(output_db);

        restore_column_families(
            output_db_dir.path().to_path_buf(),
            input_db_dir.path().to_path_buf(),
            vec![ColumnFamilyMapping {
                input: input_cf_name.to_owned(),
                output: output_cf_name.clone(),
            }],
        )?;

        let output_db = DB::open_cf_with_opts_for_read_only(
            &RocksdbOptions::default(),
            output_db_dir.path(),
            [(output_cf_name.as_str(), db_table_opts_factory.shard())],
            false,
        )?;
        let output_cf = output_db
            .cf_handle(&output_cf_name)
            .expect("output column family should exist after restore");

        assert_eq!(
            output_db.get_cf(&output_cf, b"blob-1")?,
            Some(b"recovered-1".to_vec())
        );
        assert_eq!(
            output_db.get_cf(&output_cf, b"blob-2")?,
            Some(recovered_large_value)
        );
        assert_eq!(output_db.get_cf(&output_cf, b"stale")?, None);

        Ok(())
    }

    #[test]
    fn restore_column_families_command_parses_mapping() {
        let args = DbToolArgs::try_parse_from([
            "db-tool",
            "restore-column-families",
            "--db-path",
            "/tmp/walrus-db",
            "--input-db-path",
            "/tmp/repaired-db",
            "--cf-mapping",
            "source_cf=shard-7/primary-slivers",
        ])
        .expect("restore-column-families command should parse");

        let DbToolCommands::RestoreColumnFamilies {
            db_path,
            input_db_path,
            cf_mappings,
        } = args.command
        else {
            panic!("expected restore-column-families command");
        };

        assert_eq!(db_path, PathBuf::from("/tmp/walrus-db"));
        assert_eq!(input_db_path, PathBuf::from("/tmp/repaired-db"));
        assert_eq!(
            cf_mappings,
            vec![ColumnFamilyMapping {
                input: "source_cf".to_owned(),
                output: "shard-7/primary-slivers".to_owned(),
            }]
        );
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
