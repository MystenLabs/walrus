// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use move_core_types::annotated_value::{MoveDatatypeLayout, MoveTypeLayout};
use rocksdb::Options;
use serde::{Deserialize, Serialize};
use sui_config::genesis::Genesis;
use sui_package_resolver::{error::Error as PackageResolverError, Package, PackageStore, Resolver};
use sui_rest_api::Client;
use sui_sdk::rpc_types::SuiEvent;
use sui_storage::verify_checkpoint_with_committee;
use sui_types::{
    base_types::ObjectID,
    committee::Committee,
    full_checkpoint_content::CheckpointData,
    messages_checkpoint::{CheckpointSequenceNumber, TrustedCheckpoint, VerifiedCheckpoint},
    object::Object,
};
use tokio::time::{sleep, Instant};
use tokio_util::sync::CancellationToken;
use typed_store::{
    rocks,
    rocks::{errors::typed_store_err_from_rocks_err, DBMap, MetricConf, ReadWriteOptions},
    Map,
};

use crate::EventConfig;

#[allow(dead_code)]
const CHECKPOINT_STORE: &str = "checkpoint_store";
#[allow(dead_code)]
const WALRUS_PACKAGE_STORE: &str = "walrus_package_store";
#[allow(dead_code)]
const COMMITTEE_STORE: &str = "committee_store";
#[allow(dead_code)]
const EVENT_STORE: &str = "event_store";
const MAX_TIMEOUT: Duration = Duration::from_secs(60);
const RETRY_DELAY: Duration = Duration::from_secs(5);

#[derive(Eq, PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct CheckpointOrderedEventID {
    sequence: CheckpointSequenceNumber,
    counter: u64,
}

impl CheckpointOrderedEventID {
    fn new(sequence: CheckpointSequenceNumber, counter: u64) -> Self {
        CheckpointOrderedEventID { sequence, counter }
    }
}

#[derive(Eq, PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct CheckpointSuiEvent {
    pub serialized_event: Vec<u8>,
    pub checkpoint_sequence_number: CheckpointOrderedEventID,
}

#[derive(Clone)]
pub struct CheckpointProcessor {
    /// Full node REST client.
    client: Client,
    /// The address of the Walrus system package.
    system_pkg_id: ObjectID,
    /// Event to read next.
    event_store_next_cursor: CheckpointOrderedEventID,
    /// Store which only stores the latest checkpoint.
    checkpoint_store: DBMap<(), TrustedCheckpoint>,
    /// Store which only stores the latest Walrus package.
    walrus_package_store: DBMap<(), Object>,
    /// Store which only stores the latest Sui committee.
    committee_store: DBMap<(), Committee>,
    /// Store which only stores all uncommitted events.
    event_store: DBMap<CheckpointOrderedEventID, CheckpointSuiEvent>,
}
#[allow(dead_code)]
impl CheckpointProcessor {
    async fn poll(&mut self, timeout: Duration) -> Result<Vec<CheckpointSuiEvent>> {
        let mut events = vec![];
        let mut iter = self.event_store.unbounded_iter();
        iter = iter.skip_to(&self.event_store_next_cursor)?;
        let start_time = std::time::Instant::now();
        while start_time.elapsed() < timeout {
            if let Some((checkpoint_event_id, event)) = iter.next() {
                events.push(event.clone());
                self.event_store_next_cursor = CheckpointOrderedEventID::new(
                    checkpoint_event_id.sequence,
                    checkpoint_event_id.counter.saturating_add(1),
                );
            } else {
                break;
            }
        }
        Ok(events)
    }

    async fn commit(&mut self, committed_event_id: CheckpointOrderedEventID) -> Result<()> {
        let iter = self.event_store.unbounded_iter();
        let rev_iter = iter.skip_to(&committed_event_id)?.reverse();
        let mut write_batch = self.event_store.batch();
        for (event_id, _) in rev_iter {
            write_batch
                .delete_batch(&self.event_store, std::iter::once(event_id))
                .map_err(|e| anyhow!("Failed to remove event from event store: {}", e))?;
        }
        write_batch
            .write()
            .map_err(|e| anyhow!("Failed to remove event from event store: {}", e))?;
        Ok(())
    }

    pub async fn start(&self) -> Result<CancellationToken> {
        let cancellation_token = CancellationToken::new();
        tokio::task::spawn(Self::start_tailing_checkpoints(
            self.clone(),
            cancellation_token.clone(),
        ));
        Ok(cancellation_token)
    }

    async fn get_full_checkpoint(
        &self,
        sequence_number: CheckpointSequenceNumber,
    ) -> Result<CheckpointData> {
        let start_time = Instant::now();

        loop {
            match self.client.get_full_checkpoint(sequence_number).await {
                Ok(checkpoint) => return Ok(checkpoint),
                Err(inner) => {
                    let elapsed = start_time.elapsed();
                    if elapsed >= MAX_TIMEOUT {
                        return Err(inner);
                    }
                    let delay = RETRY_DELAY.min(MAX_TIMEOUT - elapsed);
                    sleep(delay).await;
                }
            }
        }
    }

    async fn start_tailing_checkpoints(self, cancel_token: CancellationToken) -> Result<()> {
        while !cancel_token.is_cancelled() {
            let Some(prev_checkpoint) = self.checkpoint_store.get(&())? else {
                bail!("No checkpoint found in the checkpoint store");
            };
            let next_checkpoint = prev_checkpoint.inner().sequence_number().saturating_add(1);
            let Ok(checkpoint) = self.get_full_checkpoint(next_checkpoint).await else {
                // TODO:
                // 1. Read walrus events from event blobs s we've fallen behind the first unpruned
                // checkpoint on the fullnode or the fullnode doesn't have the new checkpoint yet
                // 2. Reset the committee and previous checkpoint using a light client
                // based approach so we can continue processing events from full node
                // 3. Reset the walrus package store with the latest walrus package object from fullnode
                bail!("Failed to get checkpoint {}", next_checkpoint);
            };
            let Some(committee) = self.committee_store.get(&())? else {
                bail!("No committee found in the committee store");
            };
            let verified_checkpoint = verify_checkpoint_with_committee(
                Arc::new(committee.clone()),
                &VerifiedCheckpoint::new_from_verified(prev_checkpoint.into_inner()),
                checkpoint.checkpoint_summary.clone(),
            )
            .map_err(|checkpoint| {
                anyhow!(
                    "Failed to verify sui checkpoint: {}",
                    checkpoint.sequence_number
                )
            })?;
            let resolver = Resolver::new(self.clone());
            let mut write_batch = self.event_store.batch();
            for tx in checkpoint.transactions.into_iter() {
                for object in tx.output_objects.into_iter() {
                    if object.id() == self.system_pkg_id {
                        write_batch
                            .insert_batch(&self.walrus_package_store, std::iter::once(((), object)))
                            .map_err(|e| {
                                anyhow!("Failed to insert object into walrus package store: {}", e)
                            })?;
                    }
                }
                let tx_events = tx.events.unwrap();
                for (seq, tx_event) in tx_events
                    .data
                    .into_iter()
                    .filter(|event| event.package_id == self.system_pkg_id)
                    .enumerate()
                {
                    let move_type_layout = resolver
                        .type_layout(move_core_types::language_storage::TypeTag::Struct(
                            Box::new(tx_event.type_.clone()),
                        ))
                        .await?;
                    let move_datatype_layout = match move_type_layout {
                        MoveTypeLayout::Struct(s) => Some(MoveDatatypeLayout::Struct(s)),
                        MoveTypeLayout::Enum(e) => Some(MoveDatatypeLayout::Enum(e)),
                        _ => None,
                    }
                    .ok_or(anyhow!("Failed to get move datatype layout"))?;
                    let sui_event = SuiEvent::try_from(
                        tx_event,
                        *tx.transaction.digest(),
                        seq as u64,
                        None,
                        move_datatype_layout,
                    )?;
                    let checkpoint_event_id = CheckpointOrderedEventID::new(
                        *checkpoint.checkpoint_summary.sequence_number(),
                        seq as u64,
                    );
                    let checkpoint_event = CheckpointSuiEvent {
                        serialized_event: serde_json::to_vec(&sui_event)?,
                        checkpoint_sequence_number: checkpoint_event_id.clone(),
                    };
                    write_batch
                        .insert_batch(
                            &self.event_store,
                            std::iter::once((checkpoint_event_id, checkpoint_event)),
                        )
                        .map_err(|e| anyhow!("Failed to insert event into event store: {}", e))?;
                }
            }
            if let Some(end_of_epoch_data) = &checkpoint.checkpoint_summary.end_of_epoch_data {
                let next_committee = end_of_epoch_data
                    .next_epoch_committee
                    .iter()
                    .cloned()
                    .collect();
                let committee = Committee::new(
                    checkpoint.checkpoint_summary.epoch().saturating_add(1),
                    next_committee,
                );
                write_batch
                    .insert_batch(&self.committee_store, std::iter::once(((), committee)))
                    .map_err(|e| {
                        anyhow!("Failed to insert committee into committee store: {}", e)
                    })?;
            }
            write_batch
                .insert_batch(
                    &self.checkpoint_store,
                    std::iter::once(((), verified_checkpoint.serializable_ref())),
                )
                .map_err(|e| anyhow!("Failed to insert checkpoint into checkpoint store: {}", e))?;
            write_batch.write()?;
        }
        Ok(())
    }

    pub async fn new_for_testing() -> Result<Self, anyhow::Error> {
        let metric_conf = MetricConf::default();
        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);
        let dir: PathBuf = tempfile::tempdir()
            .expect("Failed to open temporary directory")
            .into_path();
        let database = rocks::open_cf_opts(
            dir,
            Some(db_opts),
            metric_conf,
            &[
                (CHECKPOINT_STORE, rocksdb::Options::default()),
                (WALRUS_PACKAGE_STORE, rocksdb::Options::default()),
                (COMMITTEE_STORE, rocksdb::Options::default()),
                (EVENT_STORE, rocksdb::Options::default()),
            ],
        )?;
        if database.cf_handle(CHECKPOINT_STORE).is_none() {
            database
                .create_cf(CHECKPOINT_STORE, &rocksdb::Options::default())
                .map_err(typed_store_err_from_rocks_err)?;
        }
        if database.cf_handle(WALRUS_PACKAGE_STORE).is_none() {
            database
                .create_cf(WALRUS_PACKAGE_STORE, &rocksdb::Options::default())
                .map_err(typed_store_err_from_rocks_err)?;
        }
        if database.cf_handle(COMMITTEE_STORE).is_none() {
            database
                .create_cf(COMMITTEE_STORE, &rocksdb::Options::default())
                .map_err(typed_store_err_from_rocks_err)?;
        }
        if database.cf_handle(EVENT_STORE).is_none() {
            database
                .create_cf(EVENT_STORE, &rocksdb::Options::default())
                .map_err(typed_store_err_from_rocks_err)?;
        }
        let checkpoint_store = DBMap::reopen(
            &database,
            Some(CHECKPOINT_STORE),
            &ReadWriteOptions::default(),
            false,
        )?;
        let walrus_package_store = DBMap::reopen(
            &database,
            Some(WALRUS_PACKAGE_STORE),
            &ReadWriteOptions::default(),
            false,
        )?;
        let committee_store = DBMap::reopen(
            &database,
            Some(CHECKPOINT_STORE),
            &ReadWriteOptions::default(),
            false,
        )?;
        let event_store = DBMap::<CheckpointOrderedEventID, CheckpointSuiEvent>::reopen(
            &database,
            Some(EVENT_STORE),
            &ReadWriteOptions::default(),
            false,
        )?;
        Ok(CheckpointProcessor {
            client: Client::new("http://localhost:8080"),
            walrus_package_store,
            checkpoint_store,
            committee_store,
            event_store,
            system_pkg_id: ObjectID::random(),
            event_store_next_cursor: CheckpointOrderedEventID::new(0, 0),
        })
    }

    pub async fn new(config: EventConfig) -> Result<Self, anyhow::Error> {
        // return a new CheckpointProcessor
        let client = Client::new(config.rest_url);
        let metric_conf = MetricConf::default();
        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);
        let database = rocks::open_cf_opts(
            config.path,
            Some(db_opts),
            metric_conf,
            &[
                (CHECKPOINT_STORE, rocksdb::Options::default()),
                (WALRUS_PACKAGE_STORE, rocksdb::Options::default()),
                (COMMITTEE_STORE, rocksdb::Options::default()),
                (EVENT_STORE, rocksdb::Options::default()),
            ],
        )?;
        if database.cf_handle(CHECKPOINT_STORE).is_none() {
            database
                .create_cf(CHECKPOINT_STORE, &rocksdb::Options::default())
                .map_err(typed_store_err_from_rocks_err)?;
        }
        if database.cf_handle(WALRUS_PACKAGE_STORE).is_none() {
            database
                .create_cf(WALRUS_PACKAGE_STORE, &rocksdb::Options::default())
                .map_err(typed_store_err_from_rocks_err)?;
        }
        if database.cf_handle(COMMITTEE_STORE).is_none() {
            database
                .create_cf(COMMITTEE_STORE, &rocksdb::Options::default())
                .map_err(typed_store_err_from_rocks_err)?;
        }
        if database.cf_handle(EVENT_STORE).is_none() {
            database
                .create_cf(EVENT_STORE, &rocksdb::Options::default())
                .map_err(typed_store_err_from_rocks_err)?;
        }
        let checkpoint_store = DBMap::reopen(
            &database,
            Some(CHECKPOINT_STORE),
            &ReadWriteOptions::default(),
            false,
        )?;
        let walrus_package_store = DBMap::reopen(
            &database,
            Some(WALRUS_PACKAGE_STORE),
            &ReadWriteOptions::default(),
            false,
        )?;
        let committee_store = DBMap::reopen(
            &database,
            Some(CHECKPOINT_STORE),
            &ReadWriteOptions::default(),
            false,
        )?;
        let event_store = DBMap::reopen(
            &database,
            Some(EVENT_STORE),
            &ReadWriteOptions::default(),
            false,
        )?;
        if checkpoint_store.is_empty() {
            // This is a fresh start as there is no prev disk state
            committee_store.schedule_delete_all()?;
            event_store.schedule_delete_all()?;
            walrus_package_store.schedule_delete_all()?;
            let genesis = Genesis::load(config.sui_genesis_path)?;
            let genesis_committee = genesis.committee()?;
            let checkpoint = genesis.checkpoint();
            committee_store.insert(&(), &genesis_committee)?;
            checkpoint_store.insert(&(), checkpoint.serializable_ref())?;
        }
        let event_iter = event_store.unbounded_iter();
        let event_store_next_cursor = event_iter
            .seek_to_first()
            .next()
            .map(|item| item.0)
            .unwrap_or(CheckpointOrderedEventID::new(0, 0));
        let checkpoint_processor = CheckpointProcessor {
            client,
            walrus_package_store,
            checkpoint_store,
            committee_store,
            event_store,
            system_pkg_id: config.system_pkg_id,
            event_store_next_cursor,
        };
        Ok(checkpoint_processor)
    }
}

#[async_trait]
impl PackageStore for CheckpointProcessor {
    async fn fetch(
        &self,
        id: move_core_types::account_address::AccountAddress,
    ) -> sui_package_resolver::Result<Arc<Package>> {
        let Some(walrus_system_pkg) =
            self.walrus_package_store.get(&()).map_err(|store_error| {
                PackageResolverError::Store {
                    store: "RocksDB",
                    source: Arc::new(store_error),
                }
            })?
        else {
            return Err(PackageResolverError::PackageNotFound(id));
        };
        Ok(Arc::new(Package::read_from_object(&walrus_system_pkg)?))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use sui_sdk::rpc_types::SuiEvent;
    use typed_store::Map;

    use crate::checkpoint_processor::{
        CheckpointOrderedEventID,
        CheckpointProcessor,
        CheckpointSuiEvent,
    };

    #[tokio::test]
    async fn test_poll() {
        let mut processor = CheckpointProcessor::new_for_testing().await.unwrap();
        // add 100 events to the event store
        let mut expected_events = vec![];
        for i in 0..100 {
            let event = CheckpointSuiEvent {
                serialized_event: serde_json::to_vec(&SuiEvent::random_for_testing()).unwrap(),
                checkpoint_sequence_number: CheckpointOrderedEventID::new(0, i),
            };
            expected_events.push(event.clone());
            let key: CheckpointOrderedEventID = CheckpointOrderedEventID::new(0, i);
            processor.event_store.insert(&key, &event).unwrap();
        }

        // poll events
        let events = processor.poll(Duration::from_secs(100)).await.unwrap();
        assert_eq!(events.len(), 100);
        // assert events are the same
        for (expected, actual) in expected_events.iter().zip(events.iter()) {
            assert_eq!(expected, actual);
        }
    }

    #[tokio::test]
    async fn test_multiple_poll() {
        let mut processor = CheckpointProcessor::new_for_testing().await.unwrap();
        // add 100 events to the event store
        let mut expected_events1 = vec![];
        for i in 0..100 {
            let event = CheckpointSuiEvent {
                serialized_event: serde_json::to_vec(&SuiEvent::random_for_testing()).unwrap(),
                checkpoint_sequence_number: CheckpointOrderedEventID::new(0, i),
            };
            expected_events1.push(event.clone());
            let key: CheckpointOrderedEventID = CheckpointOrderedEventID::new(0, i);
            processor.event_store.insert(&key, &event).unwrap();
        }

        // poll events
        let events = processor.poll(Duration::from_secs(100)).await.unwrap();
        assert_eq!(events.len(), 100);
        // assert events are the same
        for (expected, actual) in expected_events1.iter().zip(events.iter()) {
            assert_eq!(expected, actual);
        }
        let mut expected_events2 = vec![];
        for i in 100..200 {
            let event = CheckpointSuiEvent {
                serialized_event: serde_json::to_vec(&SuiEvent::random_for_testing()).unwrap(),
                checkpoint_sequence_number: CheckpointOrderedEventID::new(0, i),
            };
            expected_events2.push(event.clone());
            let key: CheckpointOrderedEventID = CheckpointOrderedEventID::new(0, i);
            processor.event_store.insert(&key, &event).unwrap();
        }

        let events = processor.poll(Duration::from_secs(100)).await.unwrap();
        assert_eq!(events.len(), 100);
        // assert events are the same
        for (expected, actual) in expected_events2.iter().zip(events.iter()) {
            assert_eq!(expected, actual);
        }
    }

    #[tokio::test]
    async fn test_commit() {
        let mut processor = CheckpointProcessor::new_for_testing().await.unwrap();
        // add 100 events to the event store
        let mut expected_events = vec![];
        for i in 0..100 {
            let event = CheckpointSuiEvent {
                serialized_event: serde_json::to_vec(&SuiEvent::random_for_testing()).unwrap(),
                checkpoint_sequence_number: CheckpointOrderedEventID::new(0, i),
            };
            expected_events.push(event.clone());
            let key: CheckpointOrderedEventID = CheckpointOrderedEventID::new(0, i);
            processor.event_store.insert(&key, &event).unwrap();
        }
        // commit first 10 events
        let committed_event_id = CheckpointOrderedEventID::new(0, 9);
        processor.commit(committed_event_id.clone()).await.unwrap();

        // poll events
        let events = processor.poll(Duration::from_secs(100)).await.unwrap();
        assert_eq!(events.len(), 90);
        // assert events are the same
        for (expected, actual) in expected_events.iter().skip(10).zip(events.iter()) {
            assert_eq!(expected, actual);
        }
    }
}
