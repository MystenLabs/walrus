// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tools for inspecting and maintaining the RocksDB database.

use std::path::PathBuf;

use anyhow::Result;
use bincode::Options;
use clap::Subcommand;
use rocksdb::{Options as RocksdbOptions, DB};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use sui_types::base_types::ObjectID;
use typed_store::rocks::be_fix_int_ser;
use walrus_core::{BlobId, Epoch};

use super::events::PositionedStreamEvent;
use crate::node::{
    events::event_processor::EVENT_STORE,
    storage::blob_info::{
        blob_info_cf_options,
        per_object_blob_info_cf_options,
        BlobInfo,
        BlobInfoApi,
        PerObjectBlobInfo,
        AGGREGATE_BLOB_INFO_COLUMN_FAMILY_NAME,
        PER_OBJECT_BLOB_INFO_COLUMN_FAMILY_NAME,
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
        /// Blob ID to read.
        #[serde_as(as = "DisplayFromStr")]
        blob_id: BlobId,
    },

    /// Read object blob info from the RocksDB database.
    ReadObjectBlobInfo {
        /// Path to the RocksDB database directory.
        #[clap(long)]
        db_path: PathBuf,
        /// Object ID to read.
        object_id: ObjectID,
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
            Self::ReadBlobInfo { db_path, blob_id } => read_blob_info(db_path, blob_id),
            Self::ReadObjectBlobInfo { db_path, object_id } => {
                read_object_blob_info(db_path, object_id)
            }
            Self::CountCertifiedBlobs { db_path, epoch } => count_certified_blobs(db_path, epoch),
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
    let db = DB::open_cf_for_read_only(&opts, db_path, [EVENT_STORE], false)?;
    let cf = db
        .cf_handle(EVENT_STORE)
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

fn read_blob_info(db_path: PathBuf, blob_id: BlobId) -> Result<()> {
    let blob_info_options = blob_info_cf_options(&DatabaseConfig::default());
    let db = DB::open_cf_with_opts_for_read_only(
        &RocksdbOptions::default(),
        db_path,
        [(AGGREGATE_BLOB_INFO_COLUMN_FAMILY_NAME, blob_info_options)],
        false,
    )?;

    let cf = db
        .cf_handle(AGGREGATE_BLOB_INFO_COLUMN_FAMILY_NAME)
        .expect("Aggregate blob info column family should exist");

    println!("Reading blob info for Blob ID: {}", blob_id);
    if let Some(blob_info) = db.get_cf(&cf, &be_fix_int_ser(&blob_id)?)? {
        let blob_info: BlobInfo = bcs::from_bytes(&blob_info)?;
        println!("BlobInfo: {:?}", blob_info);
    } else {
        println!("BlobInfo not found");
    }

    Ok(())
}

fn read_object_blob_info(db_path: PathBuf, object_id: ObjectID) -> Result<()> {
    let per_object_blob_info_options = per_object_blob_info_cf_options(&DatabaseConfig::default());
    let db = DB::open_cf_with_opts_for_read_only(
        &RocksdbOptions::default(),
        db_path,
        [(
            PER_OBJECT_BLOB_INFO_COLUMN_FAMILY_NAME,
            per_object_blob_info_options,
        )],
        false,
    )?;

    let cf = db
        .cf_handle(PER_OBJECT_BLOB_INFO_COLUMN_FAMILY_NAME)
        .expect("PerObjectBlobInfo column family should exist");
    println!("Reading object blob info for Object ID: {:?}", object_id);

    if let Some(blob_info) = db.get_cf(&cf, &be_fix_int_ser(&object_id)?)? {
        let blob_info: PerObjectBlobInfo = bcs::from_bytes(&blob_info)?;
        println!("PerObjectBlobInfo: {:?}", blob_info);
    } else {
        println!("PerObjectBlobInfo not found");
    }

    Ok(())
}

fn count_certified_blobs(db_path: PathBuf, epoch: Epoch) -> Result<()> {
    let blob_info_options = blob_info_cf_options(&DatabaseConfig::default());
    let db = DB::open_cf_with_opts_for_read_only(
        &RocksdbOptions::default(),
        db_path,
        [(AGGREGATE_BLOB_INFO_COLUMN_FAMILY_NAME, blob_info_options)],
        false,
    )?;

    let cf = db
        .cf_handle(AGGREGATE_BLOB_INFO_COLUMN_FAMILY_NAME)
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
