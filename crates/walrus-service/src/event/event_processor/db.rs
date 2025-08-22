// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Database module for storing event processor data.
use std::{path::Path, sync::Arc};

use anyhow::Result;
use sui_types::{
    base_types::ObjectID,
    committee::Committee,
    messages_checkpoint::TrustedCheckpoint,
    object::Object,
};
use typed_store::{
    Map,
    TypedStoreError,
    rocks::{self, DBMap, MetricConf, ReadWriteOptions, RocksDB},
};

use crate::{
    event::events::{InitState, PositionedStreamEvent},
    node::{DatabaseConfig, DatabaseTableOptionsFactory},
};

/// Constants used by the event processor.
pub(crate) mod constants {
    /// The name of the checkpoint store.
    pub const CHECKPOINT_STORE: &str = "checkpoint_store";

    /// The name of the Walrus package store.
    pub const WALRUS_PACKAGE_STORE: &str = "walrus_package_store";

    /// The name of the committee store.
    pub const COMMITTEE_STORE: &str = "committee_store";

    /// The name of the event store.
    pub const EVENT_STORE: &str = "event_store";

    /// Event blob state to consider before the first event is processed.
    pub const INIT_STATE: &str = "init_state";
}

/// Stores for the event processor.
#[derive(Clone, Debug)]
pub struct EventProcessorStores {
    /// The checkpoint store.
    pub checkpoint_store: DBMap<(), TrustedCheckpoint>,
    /// The Walrus package store.
    pub walrus_package_store: DBMap<ObjectID, Object>,
    /// The committee store.
    pub committee_store: DBMap<(), Committee>,
    /// The event store.
    pub event_store: DBMap<u64, PositionedStreamEvent>,
    /// The init state store. Key is the event index
    /// of the first event in an event blob.
    pub init_state: DBMap<u64, InitState>,
}

impl EventProcessorStores {
    /// Creates a new store manager.
    pub fn new(db_config: &DatabaseConfig, db_path: &Path) -> Result<Self> {
        let metric_conf = MetricConf::new("event_processor");
        let database = Self::initialize_database(db_path, db_config, metric_conf)?;
        let stores = Self::open_stores(&database)?;
        Ok(stores)
    }

    /// Initializes the database for event processing.
    pub fn initialize_database(
        storage_path: &Path,
        db_config: &DatabaseConfig,
        metric_conf: MetricConf,
    ) -> Result<Arc<RocksDB>> {
        let mut db_opts = rocksdb::Options::from(&db_config.global());
        let db_table_opts_factory = DatabaseTableOptionsFactory::new(db_config.clone(), false);
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);
        let db = rocks::open_cf_opts(
            storage_path,
            Some(db_opts),
            metric_conf,
            &[
                (
                    constants::CHECKPOINT_STORE,
                    db_table_opts_factory.checkpoint_store(),
                ),
                (
                    constants::WALRUS_PACKAGE_STORE,
                    db_table_opts_factory.walrus_package_store(),
                ),
                (
                    constants::COMMITTEE_STORE,
                    db_table_opts_factory.committee_store(),
                ),
                (constants::EVENT_STORE, db_table_opts_factory.event_store()),
                (constants::INIT_STATE, db_table_opts_factory.init_state()),
            ],
        )?;

        Ok(db)
    }

    /// Opens the stores for event processing.
    pub fn open_stores(database: &Arc<RocksDB>) -> Result<EventProcessorStores> {
        let checkpoint_store = DBMap::reopen(
            database,
            Some(constants::CHECKPOINT_STORE),
            &ReadWriteOptions::default(),
            false,
        )?;
        let walrus_package_store = DBMap::reopen(
            database,
            Some(constants::WALRUS_PACKAGE_STORE),
            &ReadWriteOptions::default(),
            false,
        )?;
        let committee_store = DBMap::reopen(
            database,
            Some(constants::COMMITTEE_STORE),
            &ReadWriteOptions::default(),
            false,
        )?;
        let event_store = DBMap::reopen(
            database,
            Some(constants::EVENT_STORE),
            &ReadWriteOptions::default(),
            false,
        )?;
        let init_state = DBMap::reopen(
            database,
            Some(constants::INIT_STATE),
            &ReadWriteOptions::default(),
            false,
        )?;

        Ok(EventProcessorStores {
            checkpoint_store,
            walrus_package_store,
            committee_store,
            event_store,
            init_state,
        })
    }

    /// Clears all stores by scheduling deletion of all entries.
    pub fn clear_stores(&self) -> Result<(), TypedStoreError> {
        self.committee_store.schedule_delete_all()?;
        self.event_store.schedule_delete_all()?;
        self.walrus_package_store.schedule_delete_all()?;
        Ok(())
    }
}
