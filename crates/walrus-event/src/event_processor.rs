// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fmt::Debug, path::Path, sync::Arc, time::Duration};

use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use move_core_types::{
    account_address::AccountAddress,
    annotated_value::{MoveDatatypeLayout, MoveTypeLayout},
};
use prometheus::{
    register_int_counter_with_registry,
    register_int_gauge_with_registry,
    IntCounter,
    IntGauge,
    Registry,
};
use rocksdb::Options;
use sui_package_resolver::{
    error::Error as PackageResolverError,
    Package,
    PackageStore,
    PackageStoreWithLruCache,
    Resolver,
};
use sui_rest_api::Client;
use sui_sdk::{rpc_types::SuiEvent, SuiClientBuilder};
use sui_storage::verify_checkpoint_with_committee;
use sui_types::{
    base_types::ObjectID,
    committee::Committee,
    messages_checkpoint::{TrustedCheckpoint, VerifiedCheckpoint},
    object::Object,
    SYSTEM_PACKAGE_ADDRESSES,
};
use tokio::{
    join,
    select,
    sync::Mutex,
    time::{sleep, Instant},
};
use tokio_util::sync::CancellationToken;
use typed_store::{
    rocks,
    rocks::{errors::typed_store_err_from_rocks_err, DBMap, MetricConf, ReadWriteOptions},
    Map,
    TypedStoreError,
};
use walrus_sui::types::ContractEvent;

use crate::{
    ensure_experimental_rest_endpoint_exists,
    get_bootstrap_committee_and_checkpoint,
    handle_checkpoint_error,
    EventProcessorConfig,
    EventSequenceNumber,
    IndexedStreamElement,
};

/// The name of the checkpoint store.
#[allow(dead_code)]
const CHECKPOINT_STORE: &str = "checkpoint_store";
/// The name of the Walrus package store.
#[allow(dead_code)]
const WALRUS_PACKAGE_STORE: &str = "walrus_package_store";
/// The name of the committee store.
#[allow(dead_code)]
const COMMITTEE_STORE: &str = "committee_store";
/// The name of the event store.
#[allow(dead_code)]
const EVENT_STORE: &str = "event_store";
/// The maximum time without successfully reading a checkpoint
/// before stopping event processor
const MAX_TIMEOUT: Duration = Duration::from_secs(60);
/// The delay between retries when polling the full node on failure
/// to read a checkpoint.
const RETRY_DELAY: Duration = Duration::from_millis(250);

pub(crate) type PackageCache = PackageStoreWithLruCache<LocalDBPackageStore>;

/// Store which keeps package objects in a local rocksdb store. It is expected that this store is
/// kept updated with latest version of package objects while iterating over checkpoints. If the
// local db is missing (or gets deleted), packages are fetched from a full node and local store is
// updated
#[derive(Clone)]
pub struct LocalDBPackageStore {
    /// The table which stores the package objects.
    package_store_table: DBMap<ObjectID, Object>,
    /// The full node REST client.
    fallback_client: Client,
}

/// Metrics for the event processor.
#[derive(Clone)]
pub struct EventProcessorMetrics {
    /// The latest downloaded full checkpoint.
    pub latest_downloaded_checkpoint: IntGauge,
    /// The number of checkpoints downloaded. Useful for computing the download rate.
    pub total_downloaded_checkpoints: IntCounter,
}

#[derive(Clone)]
pub struct EventProcessor {
    /// Full node REST client.
    pub client: Client,
    /// Event polling interval.
    pub event_polling_interval: Duration,
    /// The address of the Walrus system package.
    pub system_pkg_id: ObjectID,
    /// Event index before which events are pruned.
    pub event_store_commit_index: Arc<Mutex<u64>>,
    /// Event store pruning interval.
    pub pruning_interval: Duration,
    /// Store which only stores the latest checkpoint.
    pub checkpoint_store: DBMap<(), TrustedCheckpoint>,
    /// Store which only stores the latest Walrus package.
    pub walrus_package_store: DBMap<ObjectID, Object>,
    /// Store which only stores the latest Sui committee.
    pub committee_store: DBMap<(), Committee>,
    /// Store which only stores all event stream elements.
    pub event_store: DBMap<u64, IndexedStreamElement>,
    /// Package resolver.
    pub package_resolver: Arc<Resolver<PackageCache>>,
    /// Event processor metrics.
    pub metrics: EventProcessorMetrics,
}

impl EventProcessorMetrics {
    pub fn new(registry: &Registry) -> Self {
        Self {
            latest_downloaded_checkpoint: register_int_gauge_with_registry!(
                "event_processor_latest_downloaded_checkpoint",
                "Latest downloaded full checkpoint",
                registry,
            )
            .expect("this is a valid metrics registration"),
            total_downloaded_checkpoints: register_int_counter_with_registry!(
                "event_processor_total_downloaded_checkpoints",
                "Total number of checkpoints downloaded",
                registry,
            )
            .expect("this is a valid metrics registration"),
        }
    }
}

impl Debug for EventProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventProcessor")
            .field("system_pkg_id", &self.system_pkg_id)
            .field("checkpoint_store", &self.checkpoint_store)
            .field("walrus_package_store", &self.walrus_package_store)
            .field("committee_store", &self.committee_store)
            .field("event_store", &self.event_store)
            .finish()
    }
}

#[allow(dead_code)]
impl EventProcessor {
    pub async fn poll(&self, from: u64) -> Result<Vec<IndexedStreamElement>> {
        let mut elements = vec![];
        let mut iter = self.event_store.unbounded_iter();
        iter = iter.skip_to(&from)?;
        for (_, event) in iter {
            elements.push(event.clone());
        }
        Ok(elements)
    }

    pub async fn start(&self, cancellation_token: CancellationToken) -> Result<(), anyhow::Error> {
        let pruning_task = self.start_pruning_events(cancellation_token.clone());
        let tailing_task = self.start_tailing_checkpoints(cancellation_token.clone());
        let (pruning_result, tailing_result) = join!(pruning_task, tailing_task);

        match (pruning_result, tailing_result) {
            (Ok(_), Ok(_)) => Ok(()),
            (Err(e), _) | (_, Err(e)) => Err(e),
        }
    }

    fn update_package_store(&self, objects: &[Object]) -> Result<(), TypedStoreError> {
        let mut write_batch = self.walrus_package_store.batch();
        for object in objects.iter() {
            // Update the package store with the given object.We want to track not just the
            // walrus package but all its transitive dependencies as well. While it is possible
            // to get the transitive dependencies for a package, it is more efficient to just
            // track all packages as we are not expecting a large number of packages.
            if object.is_package() {
                write_batch.insert_batch(
                    &self.walrus_package_store,
                    std::iter::once((object.id(), object)),
                )?;
            }
        }
        write_batch.write()?;
        Ok(())
    }

    /// Starts a periodic pruning process for events in the event store. This method will run until
    /// the cancellation token is cancelled.
    pub async fn start_pruning_events(&self, cancel_token: CancellationToken) -> Result<()> {
        loop {
            select! {
                _ = sleep(self.pruning_interval) => {
                    let commit_index = *self.event_store_commit_index.lock().await;
                    if commit_index == 0 {
                        continue;
                    }
                    let mut write_batch = self.event_store.batch();
                    write_batch.schedule_delete_range(&self.event_store, &0, &commit_index)?;
                    write_batch.write()?;
                    // This will prune the event store by deleting all the sst files relevant to the
                    // events before the commit index
                    self.event_store.delete_file_in_range(&0, &commit_index)?;
                }
                _ = cancel_token.cancelled() => {
                    return Ok(());
                },
            }
        }
    }

    /// Tails the full node for new checkpoints and processes them. This method will run until the
    /// cancellation token is cancelled. If the checkpoint processor falls behind the full node, it
    /// will read events from the event blobs so it can catch up.
    pub async fn start_tailing_checkpoints(&self, cancel_token: CancellationToken) -> Result<()> {
        let mut next_event_index = self
            .event_store
            .unbounded_iter()
            .skip_to_last()
            .next()
            .map(|(k, _)| k + 1)
            .unwrap_or(0);
        let mut start = Instant::now();
        while !cancel_token.is_cancelled() {
            let Some(prev_checkpoint) = self.checkpoint_store.get(&())? else {
                bail!("No checkpoint found in the checkpoint store");
            };
            let next_checkpoint = prev_checkpoint.inner().sequence_number().saturating_add(1);
            let result = self.client.get_full_checkpoint(next_checkpoint).await;
            let Ok(checkpoint) = result else {
                sleep(RETRY_DELAY).await;
                if start.elapsed() > MAX_TIMEOUT {
                    bail!(
                        "Failed to read checkpoint from full node: {}",
                        result.err().unwrap()
                    );
                } else {
                    handle_checkpoint_error(result.err(), next_checkpoint);
                    continue;
                }
            };
            start = Instant::now();
            self.metrics
                .latest_downloaded_checkpoint
                .set(next_checkpoint as i64);
            self.metrics.total_downloaded_checkpoints.inc();
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
            let mut write_batch = self.event_store.batch();
            let mut counter = 0;
            for tx in checkpoint.transactions.into_iter() {
                self.update_package_store(&tx.output_objects)
                    .map_err(|e| anyhow!("Failed to update walrus package store: {}", e))?;
                let tx_events = tx.events.unwrap_or_default();
                for (seq, tx_event) in tx_events
                    .data
                    .into_iter()
                    .filter(|event| event.package_id == self.system_pkg_id)
                    .enumerate()
                {
                    let move_type_layout = self
                        .package_resolver
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
                    let contract_event: ContractEvent = sui_event.try_into()?;
                    let event_sequence_number = EventSequenceNumber::new(
                        *checkpoint.checkpoint_summary.sequence_number(),
                        counter,
                    );
                    let walrus_event =
                        IndexedStreamElement::new(contract_event, event_sequence_number.clone());
                    write_batch
                        .insert_batch(
                            &self.event_store,
                            std::iter::once((next_event_index, walrus_event)),
                        )
                        .map_err(|e| anyhow!("Failed to insert event into event store: {}", e))?;
                    counter += 1;
                    next_event_index += 1;
                }
            }
            let end_of_checkpoint = IndexedStreamElement::new_checkpoint_boundary(
                checkpoint.checkpoint_summary.sequence_number,
                counter,
            );
            write_batch.insert_batch(
                &self.event_store,
                std::iter::once((next_event_index, end_of_checkpoint)),
            )?;
            next_event_index += 1;
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
                self.package_resolver
                    .package_store()
                    .evict(SYSTEM_PACKAGE_ADDRESSES.iter().copied());
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

    /// Creates a new checkpoint processor with the given configuration. The processor will use the
    /// given configuration to connect to the full node and the checkpoint store. If the checkpoint
    /// store is not found, it will be created. If the checkpoint store is found, the processor will
    /// resume from the last checkpoint.
    pub async fn new(
        config: &EventProcessorConfig,
        rpc_address: String,
        system_pkg_id: ObjectID,
        event_polling_interval: Duration,
        db_path: &Path,
        registry: &Registry,
    ) -> Result<Self, anyhow::Error> {
        // return a new CheckpointProcessor
        let client = Client::new(&config.rest_url);

        // Fail with an error if experimental rest endpoint does not exist
        // as we need it to get the full checkpoint
        ensure_experimental_rest_endpoint_exists(client.clone()).await?;

        let metric_conf = MetricConf::default();
        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);
        let database = rocks::open_cf_opts(
            db_path,
            Some(db_opts),
            metric_conf,
            &[
                (CHECKPOINT_STORE, Options::default()),
                (WALRUS_PACKAGE_STORE, Options::default()),
                (COMMITTEE_STORE, Options::default()),
                (EVENT_STORE, Options::default()),
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
            Some(COMMITTEE_STORE),
            &ReadWriteOptions::default(),
            false,
        )?;
        let event_store = DBMap::reopen(
            &database,
            Some(EVENT_STORE),
            &ReadWriteOptions::default().set_ignore_range_deletions(true),
            false,
        )?;

        let package_store = LocalDBPackageStore::new(walrus_package_store.clone(), client.clone());

        let event_processor = EventProcessor {
            client,
            walrus_package_store,
            checkpoint_store,
            committee_store,
            event_store,
            system_pkg_id,
            event_polling_interval,
            event_store_commit_index: Arc::new(Mutex::new(0)),
            pruning_interval: config.pruning_interval,
            package_resolver: Arc::new(Resolver::new(PackageCache::new(package_store))),
            metrics: EventProcessorMetrics::new(registry),
        };

        if event_processor.checkpoint_store.is_empty() {
            // This is a fresh start as there is no prev disk state
            event_processor.committee_store.schedule_delete_all()?;
            event_processor.event_store.schedule_delete_all()?;
            event_processor.walrus_package_store.schedule_delete_all()?;
            let sui_client = SuiClientBuilder::default().build(rpc_address).await?;
            let (committee, verified_checkpoint) = get_bootstrap_committee_and_checkpoint(
                &sui_client,
                event_processor.client.clone(),
                event_processor.system_pkg_id,
            )
            .await?;
            event_processor.committee_store.insert(&(), &committee)?;
            event_processor
                .checkpoint_store
                .insert(&(), verified_checkpoint.serializable_ref())?;
        }

        Ok(event_processor)
    }
}

impl LocalDBPackageStore {
    pub fn new(package_store_table: DBMap<ObjectID, Object>, client: Client) -> Self {
        Self {
            package_store_table,
            fallback_client: client,
        }
    }

    pub fn update(&self, object: &Object) -> Result<()> {
        if object.is_package() {
            let mut write_batch = self.package_store_table.batch();
            write_batch.insert_batch(
                &self.package_store_table,
                std::iter::once((object.id(), object)),
            )?;
            write_batch.write()?;
        }
        Ok(())
    }

    pub async fn get(&self, id: AccountAddress) -> Result<Object, PackageResolverError> {
        let object = if let Some(object) = self
            .package_store_table
            .get(&ObjectID::from(id))
            .map_err(|store_error| PackageResolverError::Store {
                store: "RocksDB",
                error: store_error.to_string(),
            })? {
            object
        } else {
            let object = self
                .fallback_client
                .get_object(ObjectID::from(id))
                .await
                .map_err(|_| PackageResolverError::PackageNotFound(id))?;
            self.update(&object)
                .map_err(|store_error| PackageResolverError::Store {
                    store: "RocksDB",
                    error: store_error.to_string(),
                })?;
            object
        };
        Ok(object)
    }
}

#[async_trait]
impl PackageStore for LocalDBPackageStore {
    async fn fetch(&self, id: AccountAddress) -> sui_package_resolver::Result<Arc<Package>> {
        let object = self.get(id).await?;
        Ok(Arc::new(Package::read_from_object(&object)?))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::OnceLock;

    use sui_types::messages_checkpoint::CheckpointSequenceNumber;
    use tokio::sync::Mutex;
    use walrus_core::BlobId;
    use walrus_sui::{test_utils::EventForTesting, types::BlobCertified};

    use super::*;

    // Prevent tests running simultaneously to avoid interferences or race conditions.
    fn global_test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(Mutex::default)
    }

    async fn new_event_processor_for_testing() -> Result<EventProcessor, anyhow::Error> {
        let metric_conf = MetricConf::default();
        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);
        let root_dir_path = tempfile::tempdir()
            .expect("Failed to open temporary directory")
            .into_path();
        let database = {
            let _lock = global_test_lock().lock().await;
            rocks::open_cf_opts(
                root_dir_path.as_path(),
                Some(db_opts),
                metric_conf,
                &[
                    (CHECKPOINT_STORE, Options::default()),
                    (WALRUS_PACKAGE_STORE, Options::default()),
                    (COMMITTEE_STORE, Options::default()),
                    (EVENT_STORE, Options::default()),
                ],
            )?
        };
        if database.cf_handle(CHECKPOINT_STORE).is_none() {
            database
                .create_cf(CHECKPOINT_STORE, &Options::default())
                .map_err(typed_store_err_from_rocks_err)?;
        }
        if database.cf_handle(WALRUS_PACKAGE_STORE).is_none() {
            database
                .create_cf(WALRUS_PACKAGE_STORE, &Options::default())
                .map_err(typed_store_err_from_rocks_err)?;
        }
        if database.cf_handle(COMMITTEE_STORE).is_none() {
            database
                .create_cf(COMMITTEE_STORE, &Options::default())
                .map_err(typed_store_err_from_rocks_err)?;
        }
        if database.cf_handle(EVENT_STORE).is_none() {
            database
                .create_cf(EVENT_STORE, &Options::default())
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
            Some(COMMITTEE_STORE),
            &ReadWriteOptions::default(),
            false,
        )?;
        let event_store = DBMap::<u64, IndexedStreamElement>::reopen(
            &database,
            Some(EVENT_STORE),
            &ReadWriteOptions::default(),
            false,
        )?;
        let client = Client::new("http://localhost:8080");
        let package_store = LocalDBPackageStore::new(walrus_package_store.clone(), client.clone());
        Ok(EventProcessor {
            client,
            walrus_package_store,
            checkpoint_store,
            committee_store,
            event_store,
            system_pkg_id: ObjectID::random(),
            event_store_commit_index: Arc::new(Mutex::new(0)),
            pruning_interval: Duration::from_secs(10),
            event_polling_interval: Duration::from_secs(1),
            package_resolver: Arc::new(Resolver::new(PackageCache::new(package_store))),
            metrics: EventProcessorMetrics::new(&Registry::default()),
        })
    }

    fn default_event_for_testing(
        checkpoint_sequence_number: CheckpointSequenceNumber,
        counter: u64,
    ) -> IndexedStreamElement {
        IndexedStreamElement::new(
            BlobCertified::for_testing(BlobId([7; 32])).into(),
            EventSequenceNumber::new(checkpoint_sequence_number, counter),
        )
    }
    #[tokio::test]
    async fn test_poll() {
        let processor = new_event_processor_for_testing().await.unwrap();
        // add 100 events to the event store
        let mut expected_events = vec![];
        for i in 0..100 {
            let event = default_event_for_testing(0, i);
            expected_events.push(event.clone());
            processor.event_store.insert(&i, &event).unwrap();
        }

        // poll events from beginning
        let events = processor.poll(0).await.unwrap();
        assert_eq!(events.len(), 100);
        // assert events are the same
        for (expected, actual) in expected_events.iter().zip(events.iter()) {
            assert_eq!(expected, actual);
        }
    }

    #[tokio::test]
    async fn test_multiple_poll() {
        let processor = new_event_processor_for_testing().await.unwrap();
        // add 100 events to the event store
        let mut expected_events1 = vec![];
        for i in 0..100 {
            let event = default_event_for_testing(0, i);
            expected_events1.push(event.clone());
            processor.event_store.insert(&i, &event).unwrap();
        }

        // poll events
        let events = processor.poll(0).await.unwrap();
        assert_eq!(events.len(), 100);
        // assert events are the same
        for (expected, actual) in expected_events1.iter().zip(events.iter()) {
            assert_eq!(expected, actual);
        }
        let mut expected_events2 = vec![];
        for i in 100..200 {
            let event = default_event_for_testing(0, i);
            expected_events2.push(event.clone());
            processor.event_store.insert(&i, &event).unwrap();
        }
        let events = processor.poll(100).await.unwrap();
        assert_eq!(events.len(), 100);
        // assert events are the same
        for (expected, actual) in expected_events2.iter().zip(events.iter()) {
            assert_eq!(expected, actual);
        }
    }
}
