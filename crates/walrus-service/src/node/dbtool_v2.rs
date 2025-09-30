// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Tools for inspecting and maintaining the RocksDB database (V2 with proper db options).

use std::path::{Path, PathBuf};

use anyhow::Result;
use bincode::Options as BincodeOptions;
use clap::{Subcommand, ValueEnum};
use rocksdb::{DB, Options, Options as RocksdbOptions, ReadOptions};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use sui_types::base_types::ObjectID;
use walrus_core::{
    BlobId,
    Epoch,
    ShardIndex,
    metadata::{BlobMetadata, BlobMetadataApi},
};

use crate::{
    event::{
        event_processor::db::constants::{self as event_processor_constants},
        events::{InitState, PositionedStreamEvent},
    },
    node::{
        DatabaseConfig,
        db_options::{get_all_possible_column_families, open_db_cf_readonly, open_db_for_write},
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
            blob_info::{BlobInfo, CertifiedBlobInfoApi, PerObjectBlobInfo},
            constants::{
                aggregate_blob_info_cf_name,
                metadata_cf_name,
                per_object_blob_info_cf_name,
                primary_slivers_column_family_name,
                secondary_slivers_column_family_name,
            },
        },
    },
};

/// Database type selector for db_tools commands (V2 - no defaults).
#[derive(Debug, Clone, Copy, ValueEnum, Serialize, Deserialize)]
pub enum DbTypeArg {
    /// Main storage database.
    Main,
    /// Event processor database.
    EventProcessor,
    /// Event blob writer database.
    EventBlobWriter,
}

/// Database inspection and maintenance tools (V2).
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
        /// Database type (main, event-processor, event-blob-writer) - MANDATORY.
        #[arg(long)]
        db_type: DbTypeArg,
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
        /// Database type (main, event-processor, event-blob-writer) - MANDATORY.
        #[arg(long)]
        db_type: DbTypeArg,
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
        /// Database type (main, event-processor, event-blob-writer) - MANDATORY.
        #[arg(long)]
        db_type: DbTypeArg,
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
        /// Database type (main, event-processor, event-blob-writer) - MANDATORY.
        #[arg(long)]
        db_type: DbTypeArg,
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

    /// Scan blob metadata from the RocksDB database.
    ReadBlobMetadata {
        /// Path to the RocksDB database directory.
        #[arg(long)]
        db_path: PathBuf,
        /// Database type (main, event-processor, event-blob-writer) - MANDATORY.
        #[arg(long)]
        db_type: DbTypeArg,
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
        /// Database type (main, event-processor, event-blob-writer) - MANDATORY.
        #[arg(long)]
        db_type: DbTypeArg,
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
        /// Database type (main, event-processor, event-blob-writer) - MANDATORY.
        #[arg(long)]
        db_type: DbTypeArg,
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
        /// Database type (main, event-processor, event-blob-writer) - MANDATORY.
        #[arg(long)]
        db_type: DbTypeArg,
        /// Commands to read event blob writer metadata.
        #[command(subcommand)]
        command: EventBlobWriterCommands,
    },

    /// Read event processor metadata from the RocksDB database.
    EventProcessor {
        /// Path to the RocksDB database directory.
        #[arg(long)]
        db_path: PathBuf,
        /// Database type (main, event-processor, event-blob-writer) - MANDATORY.
        #[arg(long)]
        db_type: DbTypeArg,
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
        // Create a default database config for db_tools
        let db_config = DatabaseConfig::default();

        match self {
            Self::RepairDb { db_path } => repair_db(db_path),
            Self::ScanEvents {
                db_path,
                db_type,
                start_event_index,
                count,
            } => scan_events(&db_path, db_type, &db_config, start_event_index, count),
            Self::ReadBlobInfo {
                db_path,
                db_type,
                start_blob_id,
                count,
            } => read_blob_info(&db_path, db_type, &db_config, start_blob_id, count),
            Self::ReadObjectBlobInfo {
                db_path,
                db_type,
                start_object_id,
                count,
            } => read_object_blob_info(&db_path, db_type, &db_config, start_object_id, count),
            Self::CountCertifiedBlobs {
                db_path,
                db_type,
                epoch,
            } => count_certified_blobs(&db_path, db_type, &db_config, epoch),
            Self::DropColumnFamilies {
                db_path,
                column_family_names,
            } => drop_column_families(db_path, column_family_names),
            Self::ListColumnFamilies { db_path } => list_column_families(db_path),
            Self::ReadBlobMetadata {
                db_path,
                db_type,
                start_blob_id,
                count,
                output_size_only,
            } => read_blob_metadata(
                &db_path,
                db_type,
                &db_config,
                start_blob_id,
                count,
                output_size_only,
            ),
            Self::ReadPrimarySlivers {
                db_path,
                db_type,
                start_blob_id,
                count,
                shard_index,
            } => read_primary_slivers(
                &db_path,
                db_type,
                &db_config,
                start_blob_id,
                count,
                ShardIndex(shard_index),
            ),
            Self::ReadSecondarySlivers {
                db_path,
                db_type,
                start_blob_id,
                count,
                shard_index,
            } => read_secondary_slivers(
                &db_path,
                db_type,
                &db_config,
                start_blob_id,
                count,
                ShardIndex(shard_index),
            ),
            Self::EventBlobWriter {
                db_path,
                db_type,
                command,
            } => event_blob_writer(&db_path, db_type, &db_config, command),
            Self::EventProcessor {
                db_path,
                db_type,
                command,
            } => event_processor(&db_path, db_type, &db_config, command),
        }
    }
}

fn repair_db(db_path: PathBuf) -> Result<()> {
    println!("Repairing RocksDB at path: {:?}", db_path);

    // Create a default database config for repairs.
    let db_config = DatabaseConfig::default();

    // Get all possible column families for this database.
    let all_cfs = get_all_possible_column_families(&db_path, &db_config);

    // Prepare options for repair.
    let mut opts = RocksdbOptions::default();
    opts.create_if_missing(false);
    opts.create_missing_column_families(true);

    // Collect column family names and options for repair.
    let cf_names: Vec<String> = all_cfs.iter().map(|cf| cf.name()).collect();

    println!("Detected database type based on existing column families.");
    println!("Will repair with {} column families:", cf_names.len());
    for cf_name in &cf_names {
        println!("  - {}", cf_name);
    }

    // Perform the repair operation.
    // Note: DB::repair doesn't take column family options, but we'll open the DB
    // after repair with proper options to ensure everything is consistent.
    DB::repair(&opts, &db_path)?;

    println!("Basic repair completed. Opening database with proper column family options...");

    // Open the database with all column families and proper options to ensure consistency.
    let _db = open_db_for_write(&db_path, &db_config)?;

    println!("Database opened successfully with proper column family options.");
    println!("Repair completed successfully!");
    Ok(())
}

fn scan_events(
    db_path: &Path,
    db_type: DbTypeArg,
    db_config: &DatabaseConfig,
    start_event_index: u64,
    count: usize,
) -> Result<()> {
    // Sanity check: scan_events should only be used with EventProcessor DB
    if !matches!(db_type, DbTypeArg::EventProcessor) {
        return Err(anyhow::anyhow!(
            "scan-events command requires --db-type event-processor"
        ));
    }

    // Only open the event_store column family
    let db = open_db_cf_readonly(
        db_path,
        &[event_processor_constants::EVENT_STORE],
        db_config,
    )?;
    let cf = db
        .cf_handle(event_processor_constants::EVENT_STORE)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Column family {} not found",
                event_processor_constants::EVENT_STORE
            )
        })?;

    let mut iter = db.raw_iterator_cf(&cf);
    iter.seek(start_event_index.to_be_bytes());

    let mut events_scanned = 0;
    while iter.valid() && events_scanned < count {
        if let (Some(key), Some(value)) = (iter.key(), iter.value()) {
            let event_index = u64::from_be_bytes(
                key.try_into()
                    .map_err(|_| anyhow::anyhow!("Invalid key format"))?,
            );
            let event: PositionedStreamEvent = bincode::DefaultOptions::new().deserialize(value)?;
            println!("Event index: {}", event_index);
            println!("Event: {:#?}", event);
            println!();
            events_scanned += 1;
        }
        iter.next();
    }
    println!("Scanned {} events", events_scanned);
    Ok(())
}

fn read_blob_info(
    db_path: &Path,
    db_type: DbTypeArg,
    db_config: &DatabaseConfig,
    start_blob_id: Option<BlobId>,
    count: usize,
) -> Result<()> {
    // Sanity check: blob info is only in Main DB
    if !matches!(db_type, DbTypeArg::Main) {
        return Err(anyhow::anyhow!(
            "read-blob-info command requires --db-type main"
        ));
    }

    // Only open the aggregate_blob_info column family
    let db = open_db_cf_readonly(db_path, &[aggregate_blob_info_cf_name()], db_config)?;
    let cf = db.cf_handle(aggregate_blob_info_cf_name()).ok_or_else(|| {
        anyhow::anyhow!("Column family {} not found", aggregate_blob_info_cf_name())
    })?;

    let mut iter = db.raw_iterator_cf(&cf);
    if let Some(blob_id) = start_blob_id {
        iter.seek(blob_id.0);
    } else {
        iter.seek_to_first();
    }

    let mut entries_read = 0;
    while iter.valid() && entries_read < count {
        if let (Some(key), Some(value)) = (iter.key(), iter.value())
            && let Ok(blob_id) = BlobId::try_from(key)
        {
            let blob_info: BlobInfo = bincode::DefaultOptions::new().deserialize(value)?;
            println!("Blob ID: {}", blob_id);
            println!("Blob info: {:#?}", blob_info);
            println!();
            entries_read += 1;
        }
        iter.next();
    }
    println!("Read {} entries", entries_read);
    Ok(())
}

fn read_object_blob_info(
    db_path: &Path,
    db_type: DbTypeArg,
    db_config: &DatabaseConfig,
    start_object_id: Option<ObjectID>,
    count: usize,
) -> Result<()> {
    // Sanity check: object blob info is only in Main DB
    if !matches!(db_type, DbTypeArg::Main) {
        return Err(anyhow::anyhow!(
            "read-object-blob-info command requires --db-type main"
        ));
    }

    // Only open the per_object_blob_info column family
    let db = open_db_cf_readonly(db_path, &[per_object_blob_info_cf_name()], db_config)?;
    let cf = db
        .cf_handle(per_object_blob_info_cf_name())
        .ok_or_else(|| {
            anyhow::anyhow!("Column family {} not found", per_object_blob_info_cf_name())
        })?;

    let mut iter = db.raw_iterator_cf(&cf);
    if let Some(object_id) = start_object_id {
        iter.seek(object_id.to_vec());
    } else {
        iter.seek_to_first();
    }

    let mut entries_read = 0;
    while iter.valid() && entries_read < count {
        if let (Some(key), Some(value)) = (iter.key(), iter.value()) {
            let object_id = ObjectID::from_bytes(key)?;
            let per_object_blob_info: PerObjectBlobInfo =
                bincode::DefaultOptions::new().deserialize(value)?;
            println!("Object ID: {}", object_id);
            println!("Per object blob info: {:#?}", per_object_blob_info);
            println!();
            entries_read += 1;
        }
        iter.next();
    }
    println!("Read {} entries", entries_read);
    Ok(())
}

fn count_certified_blobs(
    db_path: &Path,
    db_type: DbTypeArg,
    db_config: &DatabaseConfig,
    epoch: Epoch,
) -> Result<()> {
    // Sanity check: blob info is only in Main DB
    if !matches!(db_type, DbTypeArg::Main) {
        return Err(anyhow::anyhow!(
            "count-certified-blobs command requires --db-type main"
        ));
    }

    // Only open the aggregate_blob_info column family
    let db = open_db_cf_readonly(db_path, &[aggregate_blob_info_cf_name()], db_config)?;
    let cf = db.cf_handle(aggregate_blob_info_cf_name()).ok_or_else(|| {
        anyhow::anyhow!("Column family {} not found", aggregate_blob_info_cf_name())
    })?;

    let mut iter = db.raw_iterator_cf(&cf);
    iter.seek_to_first();

    let mut certified_count = 0;
    let mut total_count = 0;

    while iter.valid() {
        if let (Some(_key), Some(value)) = (iter.key(), iter.value()) {
            let blob_info: BlobInfo = bincode::DefaultOptions::new().deserialize(value)?;
            total_count += 1;
            if blob_info.initial_certified_epoch() == Some(epoch) {
                certified_count += 1;
            }
        }
        iter.next();
    }

    println!(
        "Found {} certified blobs in epoch {} (out of {} total blobs)",
        certified_count, epoch, total_count
    );
    Ok(())
}

fn drop_column_families(db_path: PathBuf, column_family_names: Vec<String>) -> Result<()> {
    println!(
        "Preparing to drop column families from database at: {:?}",
        db_path
    );

    // Create a default database config.
    let db_config = DatabaseConfig::default();

    // Validate that the column families exist before attempting to drop them.
    let existing_cfs = DB::list_cf(&Options::default(), &db_path)?;
    let mut cfs_to_drop = Vec::new();
    let mut not_found = Vec::new();

    for cf_name in &column_family_names {
        if existing_cfs.contains(cf_name) {
            cfs_to_drop.push(cf_name.clone());
        } else {
            not_found.push(cf_name.clone());
        }
    }

    if !not_found.is_empty() {
        println!("Warning: The following column families were not found:");
        for cf in &not_found {
            println!("  - {}", cf);
        }
    }

    if cfs_to_drop.is_empty() {
        println!("No column families to drop.");
        return Ok(());
    }

    println!("Column families to drop:");
    for cf in &cfs_to_drop {
        println!("  - {}", cf);
    }

    // Open the database with write access and all column families with proper options.
    let db = open_db_for_write(&db_path, &db_config)?;

    // Drop the specified column families.
    for cf_name in cfs_to_drop {
        match db.drop_cf(&cf_name) {
            Ok(_) => println!("Successfully dropped column family: {}", cf_name),
            Err(e) => {
                // Log the error but continue with other drops.
                eprintln!("Failed to drop column family '{}': {}", cf_name, e);
            }
        }
    }

    println!("Column family drop operation completed!");
    Ok(())
}

fn list_column_families(db_path: PathBuf) -> Result<()> {
    let opts = RocksdbOptions::default();
    let cfs = DB::list_cf(&opts, &db_path)?;
    println!("Column families in database at {:?}:", db_path);
    for cf in cfs {
        println!("  {}", cf);
    }
    Ok(())
}

fn read_blob_metadata(
    db_path: &Path,
    db_type: DbTypeArg,
    db_config: &DatabaseConfig,
    start_blob_id: Option<BlobId>,
    count: usize,
    output_size_only: bool,
) -> Result<()> {
    // Sanity check: metadata is only in Main DB
    if !matches!(db_type, DbTypeArg::Main) {
        return Err(anyhow::anyhow!(
            "read-blob-metadata command requires --db-type main"
        ));
    }

    // Only open the metadata column family
    let db = open_db_cf_readonly(db_path, &[metadata_cf_name()], db_config)?;
    let cf = db
        .cf_handle(metadata_cf_name())
        .ok_or_else(|| anyhow::anyhow!("Column family {} not found", metadata_cf_name()))?;

    let mut iter = db.raw_iterator_cf(&cf);
    if let Some(blob_id) = start_blob_id {
        iter.seek(blob_id.0);
    } else {
        iter.seek_to_first();
    }

    let mut entries_read = 0;
    while iter.valid() && entries_read < count {
        if let (Some(key), Some(value)) = (iter.key(), iter.value())
            && let Ok(blob_id) = BlobId::try_from(key)
        {
            let metadata: BlobMetadata = bincode::DefaultOptions::new().deserialize(value)?;
            if output_size_only {
                println!("Blob ID: {}, Size: {:?}", blob_id, metadata.encoded_size());
            } else {
                println!("Blob ID: {}", blob_id);
                println!("Metadata: {:#?}", metadata);
                println!();
            }
            entries_read += 1;
        }
        iter.next();
    }
    println!("Read {} entries", entries_read);
    Ok(())
}

fn read_primary_slivers(
    db_path: &Path,
    db_type: DbTypeArg,
    db_config: &DatabaseConfig,
    start_blob_id: Option<BlobId>,
    count: usize,
    shard_index: ShardIndex,
) -> Result<()> {
    // Sanity check: slivers are only in Main DB
    if !matches!(db_type, DbTypeArg::Main) {
        return Err(anyhow::anyhow!(
            "read-primary-slivers command requires --db-type main"
        ));
    }

    let cf_name = primary_slivers_column_family_name(shard_index);
    // Only open the specific primary sliver column family
    let db = open_db_cf_readonly(db_path, &[&cf_name], db_config)?;
    let cf = db
        .cf_handle(&cf_name)
        .ok_or_else(|| anyhow::anyhow!("Column family {} not found", cf_name))?;

    let mut read_opts = ReadOptions::default();
    let start_key = start_blob_id.as_ref().map(|id| id.0.to_vec());
    if let Some(ref key) = start_key {
        read_opts.set_iterate_lower_bound(&key[..]);
    }

    let mut iter = db.raw_iterator_cf_opt(&cf, read_opts);
    iter.seek_to_first();

    let mut entries_read = 0;
    while iter.valid() && entries_read < count {
        if let (Some(key), Some(value)) = (iter.key(), iter.value()) {
            let blob_id = BlobId::try_from(key)?;
            let sliver_data: PrimarySliverData = bcs::from_bytes(value)?;
            println!("Blob ID: {}", blob_id);
            match sliver_data {
                PrimarySliverData::V1(sliver) => {
                    println!("Primary sliver symbols: {} bytes", sliver.symbols.len());
                }
            }
            println!();
            entries_read += 1;
        }
        iter.next();
    }
    println!("Read {} entries from shard {}", entries_read, shard_index.0);
    Ok(())
}

fn read_secondary_slivers(
    db_path: &Path,
    db_type: DbTypeArg,
    db_config: &DatabaseConfig,
    start_blob_id: Option<BlobId>,
    count: usize,
    shard_index: ShardIndex,
) -> Result<()> {
    // Sanity check: slivers are only in Main DB
    if !matches!(db_type, DbTypeArg::Main) {
        return Err(anyhow::anyhow!(
            "read-secondary-slivers command requires --db-type main"
        ));
    }

    let cf_name = secondary_slivers_column_family_name(shard_index);
    // Only open the specific secondary sliver column family
    let db = open_db_cf_readonly(db_path, &[&cf_name], db_config)?;
    let cf = db
        .cf_handle(&cf_name)
        .ok_or_else(|| anyhow::anyhow!("Column family {} not found", cf_name))?;

    let mut read_opts = ReadOptions::default();
    let start_key = start_blob_id.as_ref().map(|id| id.0.to_vec());
    if let Some(ref key) = start_key {
        read_opts.set_iterate_lower_bound(&key[..]);
    }

    let mut iter = db.raw_iterator_cf_opt(&cf, read_opts);
    iter.seek_to_first();

    let mut entries_read = 0;
    while iter.valid() && entries_read < count {
        if let (Some(key), Some(value)) = (iter.key(), iter.value()) {
            let blob_id = BlobId::try_from(key)?;
            let sliver_data: SecondarySliverData = bcs::from_bytes(value)?;
            println!("Blob ID: {}", blob_id);
            match sliver_data {
                SecondarySliverData::V1(sliver) => {
                    println!("Secondary sliver symbols: {} bytes", sliver.symbols.len());
                }
            }
            println!();
            entries_read += 1;
        }
        iter.next();
    }
    println!("Read {} entries from shard {}", entries_read, shard_index.0);
    Ok(())
}

fn event_blob_writer(
    db_path: &Path,
    db_type: DbTypeArg,
    db_config: &DatabaseConfig,
    command: EventBlobWriterCommands,
) -> Result<()> {
    // Sanity check: event blob writer commands require EventBlobWriter DB
    if !matches!(db_type, DbTypeArg::EventBlobWriter) {
        return Err(anyhow::anyhow!(
            "event-blob-writer command requires --db-type event-blob-writer"
        ));
    }

    // Determine which column family we need based on the command
    let cf_name = match &command {
        EventBlobWriterCommands::ReadCertified => certified_cf_name(),
        EventBlobWriterCommands::ReadAttested => attested_cf_name(),
        EventBlobWriterCommands::ReadPending { .. } => pending_cf_name(),
        EventBlobWriterCommands::ReadFailedToAttest => failed_to_attest_cf_name(),
    };

    // Only open the specific column family we need
    let db = open_db_cf_readonly(db_path, &[cf_name], db_config)?;

    match command {
        EventBlobWriterCommands::ReadCertified => {
            let cf = db.cf_handle(certified_cf_name()).ok_or_else(|| {
                anyhow::anyhow!("Column family {} not found", certified_cf_name())
            })?;

            let mut iter = db.raw_iterator_cf(&cf);
            iter.seek_to_first();

            let mut entries_read = 0;
            while iter.valid() {
                if let (Some(key), Some(value)) = (iter.key(), iter.value()) {
                    let blob_id = BlobId::try_from(key)?;
                    let metadata: CertifiedEventBlobMetadata =
                        bincode::DefaultOptions::new().deserialize(value)?;
                    println!("Blob ID: {}", blob_id);
                    println!("Certified metadata: {:#?}", metadata);
                    println!();
                    entries_read += 1;
                }
                iter.next();
            }
            println!("Read {} certified entries", entries_read);
        }
        EventBlobWriterCommands::ReadAttested => {
            let cf = db
                .cf_handle(attested_cf_name())
                .ok_or_else(|| anyhow::anyhow!("Column family {} not found", attested_cf_name()))?;

            let mut iter = db.raw_iterator_cf(&cf);
            iter.seek_to_first();

            let mut entries_read = 0;
            while iter.valid() {
                if let (Some(key), Some(value)) = (iter.key(), iter.value()) {
                    let blob_id = BlobId::try_from(key)?;
                    let metadata: AttestedEventBlobMetadata =
                        bincode::DefaultOptions::new().deserialize(value)?;
                    println!("Blob ID: {}", blob_id);
                    println!("Attested metadata: {:#?}", metadata);
                    println!();
                    entries_read += 1;
                }
                iter.next();
            }
            println!("Read {} attested entries", entries_read);
        }
        EventBlobWriterCommands::ReadPending { start_seq, count } => {
            let cf = db
                .cf_handle(pending_cf_name())
                .ok_or_else(|| anyhow::anyhow!("Column family {} not found", pending_cf_name()))?;

            let mut iter = db.raw_iterator_cf(&cf);
            if let Some(seq) = start_seq {
                iter.seek(seq.to_be_bytes());
            } else {
                iter.seek_to_first();
            }

            let mut entries_read = 0;
            while iter.valid() && entries_read < count {
                if let (Some(key), Some(value)) = (iter.key(), iter.value()) {
                    let seq = u64::from_be_bytes(
                        key.try_into()
                            .map_err(|_| anyhow::anyhow!("Invalid key format"))?,
                    );
                    let metadata: PendingEventBlobMetadata =
                        bincode::DefaultOptions::new().deserialize(value)?;
                    println!("Sequence: {}", seq);
                    println!("Pending metadata: {:#?}", metadata);
                    println!();
                    entries_read += 1;
                }
                iter.next();
            }
            println!("Read {} pending entries", entries_read);
        }
        EventBlobWriterCommands::ReadFailedToAttest => {
            let cf = db.cf_handle(failed_to_attest_cf_name()).ok_or_else(|| {
                anyhow::anyhow!("Column family {} not found", failed_to_attest_cf_name())
            })?;

            let mut iter = db.raw_iterator_cf(&cf);
            iter.seek_to_first();

            let mut entries_read = 0;
            while iter.valid() {
                if let (Some(key), Some(value)) = (iter.key(), iter.value()) {
                    let blob_id = BlobId::try_from(key)?;
                    let metadata: FailedToAttestEventBlobMetadata =
                        bincode::DefaultOptions::new().deserialize(value)?;
                    println!("Blob ID: {}", blob_id);
                    println!("Failed to attest metadata: {:#?}", metadata);
                    println!();
                    entries_read += 1;
                }
                iter.next();
            }
            println!("Read {} failed-to-attest entries", entries_read);
        }
    }
    Ok(())
}

fn event_processor(
    db_path: &Path,
    db_type: DbTypeArg,
    db_config: &DatabaseConfig,
    command: EventProcessorCommands,
) -> Result<()> {
    // Sanity check: event processor commands require EventProcessor DB
    if !matches!(db_type, DbTypeArg::EventProcessor) {
        return Err(anyhow::anyhow!(
            "event-processor command requires --db-type event-processor"
        ));
    }

    // Determine which column family we need based on the command
    let cf_name = match &command {
        EventProcessorCommands::ReadInitState => event_processor_constants::INIT_STATE,
    };

    // Only open the specific column family we need
    let db = open_db_cf_readonly(db_path, &[cf_name], db_config)?;

    match command {
        EventProcessorCommands::ReadInitState => {
            let cf = db
                .cf_handle(event_processor_constants::INIT_STATE)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Column family {} not found",
                        event_processor_constants::INIT_STATE
                    )
                })?;

            let mut iter = db.raw_iterator_cf(&cf);
            iter.seek_to_first();

            if iter.valid() {
                if let Some(value) = iter.value() {
                    let init_state: InitState =
                        bincode::DefaultOptions::new().deserialize(value)?;
                    println!("Init state: {:#?}", init_state);
                } else {
                    println!("No init state found");
                }
            } else {
                println!("No init state found");
            }
        }
    }
    Ok(())
}
