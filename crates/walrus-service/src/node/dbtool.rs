// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Tools for inspecting and maintaining the RocksDB database.

use std::path::PathBuf;

use anyhow::Result;
use bincode::Options;
use clap::Subcommand;
use rocksdb::{Options as RocksdbOptions, ReadOptions, DB};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use sui_types::base_types::ObjectID;
use typed_store::rocks::be_fix_int_ser;
use walrus_core::{BlobId, BlobMetadata, Epoch, ShardIndex};

use crate::node::{
    events::{
        event_blob_writer::{
            attested_cf_name,
            certified_cf_name,
            failed_to_attest_cf_name,
            pending_cf_name,
            AttestedEventBlobMetadata,
            CertifiedEventBlobMetadata,
            FailedToAttestEventBlobMetadata,
            PendingEventBlobMetadata,
        },
        event_processor::event_store_cf_name,
        PositionedStreamEvent,
    },
    storage::{
        blob_info::{
            blob_info_cf_options,
            per_object_blob_info_cf_options,
            BlobInfo,
            BlobInfoApi,
            PerObjectBlobInfo,
        },
        constants::{
            aggregate_blob_info_cf_name,
            metadata_cf_name,
            per_object_blob_info_cf_name,
            primary_slivers_column_family_name,
            secondary_slivers_column_family_name,
        },
        metadata_options,
        primary_slivers_column_family_options,
        secondary_slivers_column_family_options,
        PrimarySliverData,
        SecondarySliverData,
    },
    DatabaseConfig,
};

/// Database inspection and maintenance tools.
#[derive(Subcommand, Debug, Clone, Serialize, Deserialize)]
#[serde_as]
#[clap(rename_all = "kebab-case")]
pub enum DbToolCommands {
    /// Repair a corrupted RocksDB database due to non-clean shutdowns.
    RepairDb {
        /// Path to the RocksDB database directory.
        #[clap(long)]
        db_path: PathBuf,
    },

    /// Scan events from the event_store table in RocksDB.
    ScanEvents {
        /// Path to the RocksDB database directory.
        #[clap(long)]
        db_path: PathBuf,
        /// Start index of the events to scan.
        #[clap(long)]
        start_event_index: u64,
        /// Number of events to scan.
        #[clap(long, default_value = "1")]
        count: u64,
    },

    /// Read blob info from the RocksDB database.
    ReadBlobInfo {
        /// Path to the RocksDB database directory.
        #[clap(long)]
        db_path: PathBuf,
        /// Start blob ID in URL-safe base64 format (no padding).
        #[clap(long)]
        #[serde_as(as = "Option<DisplayFromStr>")]
        start_blob_id: Option<BlobId>,
        /// Number of entries to scan.
        #[clap(long, default_value = "1")]
        count: u64,
    },

    /// Read object blob info from the RocksDB database.
    ReadObjectBlobInfo {
        /// Path to the RocksDB database directory.
        #[clap(long)]
        db_path: PathBuf,
        /// Start object ID to read.
        #[clap(long)]
        #[serde_as(as = "Option<DisplayFromStr>")]
        start_object_id: Option<ObjectID>,
        /// Count of objects to read.
        #[clap(long, default_value = "1")]
        count: u64,
    },

    /// Count the number of certified blobs in the RocksDB database.
    CountCertifiedBlobs {
        /// Path to the RocksDB database directory.
        #[clap(long)]
        db_path: PathBuf,
        /// Epoch the blobs are in certified status.
        #[clap(long)]
        epoch: Epoch,
    },

    /// Drop a column family from the RocksDB database.
    DropColumnFamily {
        /// Path to the RocksDB database directory.
        #[clap(long)]
        db_path: PathBuf,
        /// Column family to drop.
        column_family_name: String,
    },

    /// List all column families in the RocksDB database.
    ListColumnFamilies {
        /// Path to the RocksDB database directory.
        #[clap(long)]
        db_path: PathBuf,
    },

    /// Scan blob metadata from the RocksDB database.
    ReadBlobMetadata {
        /// Path to the RocksDB database directory.
        #[clap(long)]
        db_path: PathBuf,
        /// Start blob ID in URL-safe base64 format (no padding).
        #[clap(long)]
        #[serde_as(as = "Option<DisplayFromStr>")]
        start_blob_id: Option<BlobId>,
        /// Number of entries to scan.
        #[clap(long, default_value = "1")]
        count: u64,
    },

    /// Read primary slivers from the RocksDB database.
    ReadPrimarySlivers {
        /// Path to the RocksDB database directory.
        #[clap(long)]
        db_path: PathBuf,
        /// Start blob ID in URL-safe base64 format (no padding).
        #[clap(long)]
        #[serde_as(as = "Option<DisplayFromStr>")]
        start_blob_id: Option<BlobId>,
        /// Number of entries to scan.
        #[clap(long, default_value = "1")]
        count: u64,
        /// Shard index to read from.
        #[clap(long)]
        shard_index: u16,
    },

    /// Read secondary slivers from the RocksDB database.
    ReadSecondarySlivers {
        /// Path to the RocksDB database directory.
        #[clap(long)]
        db_path: PathBuf,
        /// Start blob ID in URL-safe base64 format (no padding).
        #[clap(long)]
        #[serde_as(as = "Option<DisplayFromStr>")]
        start_blob_id: Option<BlobId>,
        /// Number of entries to scan.
        #[clap(long, default_value = "1")]
        count: u64,
        /// Shard index to read from.
        #[clap(long)]
        shard_index: u16,
    },

    /// Read event blob writer metadata from the RocksDB database.
    EventBlobWriter {
        /// Path to the RocksDB database directory.
        #[clap(long)]
        db_path: PathBuf,
        /// Commands to read event blob writer metadata.
        #[command(subcommand)]
        command: EventBlobWriterCommands,
    },
}

/// Commands for reading event blob writer metadata.
#[derive(Subcommand, Debug, Clone, Serialize, Deserialize)]
#[serde_as]
#[clap(rename_all = "kebab-case")]
pub enum EventBlobWriterCommands {
    /// Read certified event blob metadata.
    ReadCertified,

    /// Read attested event blob metadata.
    ReadAttested,

    /// Read pending event blob metadata.
    ReadPending {
        /// Start sequence number.
        #[clap(long)]
        start_seq: Option<u64>,
        /// Number of entries to scan.
        #[clap(long, default_value = "1")]
        count: u64,
    },

    /// Read failed-to-attest event blob metadata.
    ReadFailedToAttest,
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
            Self::DropColumnFamily {
                db_path,
                column_family_name,
            } => drop_column_family(db_path, column_family_name),
            Self::ListColumnFamilies { db_path } => list_column_families(db_path),
            Self::ReadBlobMetadata {
                db_path,
                start_blob_id,
                count,
            } => read_blob_metadata(db_path, start_blob_id, count),
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
        }
    }
}

fn repair_db(db_path: PathBuf) -> Result<()> {
    let mut opts = RocksdbOptions::default();
    opts.create_if_missing(true);
    opts.set_max_open_files(512_000);
    DB::repair(&opts, db_path).map_err(Into::into)
}

fn scan_events(db_path: PathBuf, start_event_index: u64, count: u64) -> Result<()> {
    println!("Scanning events from event index {}", start_event_index);
    let opts = RocksdbOptions::default();
    let db = DB::open_cf_for_read_only(&opts, db_path, [event_store_cf_name()], false)?;
    let cf = db
        .cf_handle(event_store_cf_name())
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

fn read_blob_info(db_path: PathBuf, start_blob_id: Option<BlobId>, count: u64) -> Result<()> {
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

    for result in iter.take(count as usize) {
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
    count: u64,
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

    for result in iter.take(count as usize) {
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
fn drop_column_family(db_path: PathBuf, column_family_name: String) -> Result<()> {
    let db = DB::open(&RocksdbOptions::default(), db_path)?;

    let result = db.drop_cf(column_family_name.as_str());
    println!("Dropped column family: {:?}", result);

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

fn read_blob_metadata(db_path: PathBuf, start_blob_id: Option<BlobId>, count: u64) -> Result<()> {
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

    for result in iter.take(count as usize) {
        match result {
            Ok((key, value)) => {
                let blob_id: BlobId = bcs::from_bytes(&key)?;
                let metadata: BlobMetadata = bcs::from_bytes(&value)?;
                println!("Blob ID: {}, Metadata: {:?}", blob_id, metadata);
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
    count: u64,
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

    for result in iter.take(count as usize) {
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
    count: u64,
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

    for result in iter.take(count as usize) {
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

fn read_pending_event_blobs(db_path: PathBuf, start_seq: Option<u64>, count: u64) -> Result<()> {
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

    for result in iter.take(count as usize) {
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
