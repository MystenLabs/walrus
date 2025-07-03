// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Tools for inspecting and maintaining the RocksDB database.

use std::path::PathBuf;

use anyhow::Result;
use bincode::Options;
use clap::Subcommand;
use rocksdb::{DB, Options as RocksdbOptions, ReadOptions};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use sui_types::base_types::ObjectID;
use typed_store::rocks::be_fix_int_ser;
use walrus_core::{
    BlobId, Epoch, ShardIndex,
    metadata::{BlobMetadata, BlobMetadataApi},
};

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
                per_object_blob_info_cf_options,
            },
            constants::{
                aggregate_blob_info_cf_name, metadata_cf_name, per_object_blob_info_cf_name,
                primary_slivers_column_family_name, secondary_slivers_column_family_name,
            },
            metadata_options, primary_slivers_column_family_options,
            secondary_slivers_column_family_options,
        },
    },
};

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
    println!("Scanning events from event index {}", start_event_index);
    let opts = RocksdbOptions::default();
    let db = DB::open_cf_for_read_only(
        &opts,
        db_path,
        [event_processor_constants::EVENT_STORE],
        false,
    )?;
    let cf = db
        .cf_handle(event_processor_constants::EVENT_STORE)
        .expect("Event store column family should exist");

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
        println!("Event index: {}. Event: {:?}", event_index, event);

        scan_count += 1;
        if scan_count >= count {
            break;
        }
    }

    Ok(())
}

fn read_blob_info(db_path: PathBuf, start_blob_id: Option<BlobId>, count: usize) -> Result<()> {
    let blob_info_options = blob_info_cf_options(&DatabaseConfig::default());
    let db = DB::open_cf_with_opts_for_read_only(
        &RocksdbOptions::default(),
        db_path,
        [(aggregate_blob_info_cf_name(), blob_info_options)],
        false,
    )?;

    let cf = db
        .cf_handle(aggregate_blob_info_cf_name())
        .expect("Aggregate blob info column family should exist");

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
                println!("Blob ID: {}, BlobInfo: {:?}", blob_id, blob_info);
            }
            Err(e) => {
                println!("Error: {:?}", e);
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
    let per_object_blob_info_options = per_object_blob_info_cf_options(&DatabaseConfig::default());
    let db = DB::open_cf_with_opts_for_read_only(
        &RocksdbOptions::default(),
        db_path,
        [(per_object_blob_info_cf_name(), per_object_blob_info_options)],
        false,
    )?;

    let cf = db
        .cf_handle(per_object_blob_info_cf_name())
        .expect("PerObjectBlobInfo column family should exist");

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
                println!(
                    "Object ID: {}, PerObjectBlobInfo: {:?}",
                    object_id, blob_info
                );
            }
            Err(e) => {
                println!("Error: {:?}", e);
                return Err(e.into());
            }
        }
    }

    Ok(())
}

fn count_certified_blobs(db_path: PathBuf, epoch: Epoch) -> Result<()> {
    let blob_info_options = blob_info_cf_options(&DatabaseConfig::default());
    let db = DB::open_cf_with_opts_for_read_only(
        &RocksdbOptions::default(),
        db_path,
        [(aggregate_blob_info_cf_name(), blob_info_options)],
        false,
    )?;

    let cf = db
        .cf_handle(aggregate_blob_info_cf_name())
        .expect("Aggregate blob info column family should exist");

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
            println!(
                "Scanned {} blobs. Found {} certified blobs",
                scan_count, certified_count
            );
        }
    }

    println!(
        "Number of certified blobs: {}. Scanned {} blobs",
        certified_count, scan_count
    );
    Ok(())
}

/// Drop a column family from the RocksDB database.
fn drop_column_families(db_path: PathBuf, column_family_names: Vec<String>) -> Result<()> {
    let db = DB::open_cf(
        &RocksdbOptions::default(),
        &db_path,
        &DB::list_cf(&RocksdbOptions::default(), &db_path)?,
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

fn list_column_families(db_path: PathBuf) -> Result<()> {
    let result = rocksdb::DB::list_cf(&RocksdbOptions::default(), db_path);
    if let Ok(column_families) = result {
        println!("Column families: {:?}", column_families);
    } else {
        println!("Failed to get column families: {:?}", result);
    }

    Ok(())
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
            metadata_options(&DatabaseConfig::default()),
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
                    println!("Blob ID: {}, Metadata: {:?}", blob_id, metadata);
                }
            }
            Err(e) => {
                println!("Error: {:?}", e);
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
            primary_slivers_column_family_options(&DatabaseConfig::default()),
        )],
        false,
    )?;

    let Some(cf) = db.cf_handle(&primary_slivers_column_family_name(shard_index)) else {
        println!(
            "Primary slivers column family not found for shard {}",
            shard_index
        );
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
                println!("Blob ID: {}, Primary Sliver: {:?}", blob_id, sliver);
            }
            Err(e) => {
                println!("Error: {:?}", e);
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
            secondary_slivers_column_family_options(&DatabaseConfig::default()),
        )],
        false,
    )?;

    let Some(cf) = db.cf_handle(&secondary_slivers_column_family_name(shard_index)) else {
        println!(
            "Secondary slivers column family not found for shard {}",
            shard_index
        );
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
                println!("Blob ID: {}, Secondary Sliver: {:?}", blob_id, sliver);
            }
            Err(e) => {
                println!("Error: {:?}", e);
                return Err(e.into());
            }
        }
    }

    Ok(())
}

fn read_event_processor_init_state(db_path: PathBuf) -> Result<()> {
    let db = DB::open_cf_for_read_only(
        &RocksdbOptions::default(),
        db_path,
        [event_processor_constants::INIT_STATE],
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
        println!("Key: {}, Init state: {:?}", key, init_state);
    }

    Ok(())
}

fn read_certified_event_blobs(db_path: PathBuf) -> Result<()> {
    let db = DB::open_cf_for_read_only(
        &RocksdbOptions::default(),
        db_path,
        [certified_cf_name()],
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
            println!("Certified Event Blob Metadata: {:?}", metadata);
        }
        None => println!("Certified event blob not found"),
    }

    Ok(())
}

fn read_attested_event_blobs(db_path: PathBuf) -> Result<()> {
    let db = DB::open_cf_for_read_only(
        &RocksdbOptions::default(),
        db_path,
        [attested_cf_name()],
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
            println!("Attested Event Blob Metadata: {:?}", metadata);
        }
        None => println!("Attested event blob not found"),
    }

    Ok(())
}

fn read_pending_event_blobs(db_path: PathBuf, start_seq: Option<u64>, count: usize) -> Result<()> {
    let db = DB::open_cf_for_read_only(
        &RocksdbOptions::default(),
        db_path,
        [pending_cf_name()],
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
                println!(
                    "Sequence: {}, Pending Event Blob Metadata: {:?}",
                    seq, metadata
                );
            }
            Err(e) => {
                println!("Error: {:?}", e);
                return Err(e.into());
            }
        }
    }

    Ok(())
}

fn read_failed_to_attest_event_blobs(db_path: PathBuf) -> Result<()> {
    let db = DB::open_cf_for_read_only(
        &RocksdbOptions::default(),
        db_path,
        [failed_to_attest_cf_name()],
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
            println!("Failed-to-attest Event Blob Metadata: {:?}", metadata);
        }
        None => println!("Failed-to-attest event blob not found"),
    }

    Ok(())
}
