// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Database module for storing indexer event processor data.

use std::{path::Path, sync::Arc};

use anyhow::Result;
use sui_types::messages_checkpoint::{CheckpointSequenceNumber, TrustedCheckpoint};
use typed_store::{
    Map,
    TypedStoreError,
    rocks::{DBMap, MetricConf, ReadWriteOptions, RocksDB, open_cf_opts},
};

use crate::IndexOperation;

/// Constants used by the indexer event processor.
pub(crate) mod constants {
    /// The name of the checkpoint store.
    pub const CHECKPOINT_STORE: &str = "indexer_checkpoint_store";

    /// The name of the processed events store.
    pub const PROCESSED_EVENTS_STORE: &str = "indexer_processed_events_store";
}

/// Index event for storage in the event processor.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexerProcessedEvent {
    /// The checkpoint sequence number this event came from
    pub checkpoint: CheckpointSequenceNumber,
    /// The actual index operation
    pub operation: IndexOperation,
}

/// Stores for the indexer event processor.
#[derive(Clone, Debug)]
pub struct IndexerEventProcessorStores {
    /// The checkpoint store to track processing progress.
    pub checkpoint_store: DBMap<(), TrustedCheckpoint>,
    /// The processed events store for tracking what we've processed.
    pub processed_events_store: DBMap<u64, IndexerProcessedEvent>,
}

impl IndexerEventProcessorStores {
    /// Creates a new store manager.
    pub fn new(db_path: &Path) -> Result<Self> {
        let metric_conf = MetricConf::new("indexer_event_processor");
        let database = Self::initialize_database(db_path, metric_conf)?;
        let stores = Self::open_stores(&database)?;
        Ok(stores)
    }

    /// Initializes the database for event processing.
    pub fn initialize_database(
        storage_path: &Path,
        metric_conf: MetricConf,
    ) -> Result<Arc<RocksDB>> {
        let mut db_opts = rocksdb::Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let db = open_cf_opts(
            storage_path,
            Some(db_opts),
            metric_conf,
            &[
                (constants::CHECKPOINT_STORE, rocksdb::Options::default()),
                (
                    constants::PROCESSED_EVENTS_STORE,
                    rocksdb::Options::default(),
                ),
            ],
        )?;
        Ok(db)
    }

    /// Opens all the stores.
    fn open_stores(database: &Arc<RocksDB>) -> Result<Self> {
        let read_write_options = ReadWriteOptions::default();

        let checkpoint_store = DBMap::reopen(
            database,
            Some(constants::CHECKPOINT_STORE),
            &read_write_options,
            false,
        )?;

        let processed_events_store = DBMap::reopen(
            database,
            Some(constants::PROCESSED_EVENTS_STORE),
            &read_write_options,
            false,
        )?;

        Ok(IndexerEventProcessorStores {
            checkpoint_store,
            processed_events_store,
        })
    }

    /// Clears all stores. Used for testing and when starting from scratch.
    pub fn clear_stores(&self) -> Result<(), TypedStoreError> {
        // For simplicity, we'll iterate and delete instead of using batch operations
        // This is less efficient but avoids the missing keys() method issue

        // Clear checkpoint store (should only have one entry with key ())
        if self.checkpoint_store.contains_key(&())? {
            self.checkpoint_store.remove(&())?;
        }

        // Clear processed events store by iterating over all entries
        let keys_to_delete: Vec<u64> = self
            .processed_events_store
            .safe_iter_with_bounds(None, None)?
            .map(|result| result.map(|(k, _)| k))
            .collect::<Result<Vec<_>, _>>()?;

        for key in keys_to_delete {
            self.processed_events_store.remove(&key)?;
        }

        Ok(())
    }
}
