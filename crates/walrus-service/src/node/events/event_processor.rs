// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Event processor for processing events from the full node.

use std::{
    collections::HashMap,
    fmt::Debug,
    fs,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use bincode::Options as _;
use checkpoint_downloader::ParallelCheckpointDownloader;
use chrono::Utc;
use futures_util::future::try_join_all;
use move_core_types::{
    account_address::AccountAddress,
    annotated_value::{MoveDatatypeLayout, MoveTypeLayout},
};
use prometheus::{IntCounter, IntCounterVec, IntGauge, Registry};
use rocksdb::Options;
use sui_package_resolver::{
    error::Error as PackageResolverError,
    Package,
    PackageStore,
    PackageStoreWithLruCache,
    Resolver,
};
use sui_sdk::{
    rpc_types::{SuiEvent, SuiObjectDataOptions, SuiTransactionBlockResponseOptions},
    SuiClientBuilder,
};
use sui_storage::verify_checkpoint_with_committee;
use sui_types::{
    base_types::ObjectID,
    committee::Committee,
    effects::TransactionEffectsAPI,
    full_checkpoint_content::CheckpointData,
    message_envelope::Message,
    messages_checkpoint::{TrustedCheckpoint, VerifiedCheckpoint},
    object::{Data, Object},
    sui_serde::BigInt,
    SYSTEM_PACKAGE_ADDRESSES,
};
use tokio::{
    select,
    sync::{Mutex, RwLock},
    time::sleep,
};
use tokio_util::sync::CancellationToken;
use typed_store::{
    rocks,
    rocks::{errors::typed_store_err_from_rocks_err, DBMap, MetricConf, ReadWriteOptions, RocksDB},
    Map,
    TypedStoreError,
};
use walrus_core::{ensure, BlobId};
use walrus_sui::{
    client::{
        retry_client::{RetriableRpcClient, RetriableSuiClient},
        rpc_config::RpcFallbackConfig,
    },
    types::ContractEvent,
};
use walrus_utils::backoff::ExponentialBackoffConfig;

use crate::{
    node::events::{
        ensure_experimental_rest_endpoint_exists,
        event_blob::EventBlob,
        CheckpointEventPosition,
        EventProcessorConfig,
        IndexedStreamEvent,
        InitState,
        PositionedStreamEvent,
        StreamEventWithInitState,
    },
    utils::collect_event_blobs_for_catchup,
};

/// The name of the checkpoint store.
const CHECKPOINT_STORE: &str = "checkpoint_store";
/// The name of the Walrus package store.
const WALRUS_PACKAGE_STORE: &str = "walrus_package_store";
/// The name of the committee store.
const COMMITTEE_STORE: &str = "committee_store";
/// The name of the event store.
const EVENT_STORE: &str = "event_store";
/// Event blob state to consider before the first event is processed.
const INIT_STATE: &str = "init_state";
/// Max events per stream poll
const MAX_EVENTS_PER_POLL: usize = 1000;

pub(crate) type PackageCache = PackageStoreWithLruCache<LocalDBPackageStore>;

pub(crate) fn event_store_cf_name() -> &'static str {
    EVENT_STORE
}

/// Store which keeps package objects in a local rocksdb store. It is expected that this store is
/// kept updated with latest version of package objects while iterating over checkpoints. If the
// local db is missing (or gets deleted), packages are fetched from a full node and local store is
// updated
#[derive(Clone)]
pub struct LocalDBPackageStore {
    /// The table which stores the package objects.
    package_store_table: DBMap<ObjectID, Object>,
    /// The full node REST client.
    fallback_client: RetriableRpcClient,
    /// Cache for original package ids.
    original_id_cache: Arc<RwLock<HashMap<AccountAddress, ObjectID>>>,
}

walrus_utils::metrics::define_metric_set! {
    #[namespace = "walrus"]
    /// Metrics for the event processor.
    pub struct EventProcessorMetrics {
        #[help = "Latest downloaded full checkpoint"]
        event_processor_latest_downloaded_checkpoint: IntGauge[],
        #[help = "The number of checkpoints downloaded. Useful for computing the download rate"]
        event_processor_total_downloaded_checkpoints: IntCounter[],
        #[help = "The number of event blobs fetched with their source"]
        event_processor_event_blob_fetched: IntCounterVec["blob_source"],
    }
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

/// Event processor for processing checkpoint and extract Walrus events from the full node.
#[derive(Clone)]
pub struct EventProcessor {
    /// Full node REST client.
    pub client: RetriableRpcClient,
    /// Event polling interval.
    pub event_polling_interval: Duration,
    /// The address of the Walrus system package.
    pub system_pkg_id: ObjectID,
    /// Event index before which events are pruned.
    pub event_store_commit_index: Arc<Mutex<u64>>,
    /// Event store pruning interval.
    pub pruning_interval: Duration,
    /// Store which only stores the latest checkpoint.
    pub stores: EventProcessorStores,
    /// Package resolver.
    pub package_resolver: Arc<Resolver<PackageCache>>,
    /// Event processor metrics.
    pub metrics: EventProcessorMetrics,
    /// Pipelined checkpoint downloader.
    pub checkpoint_downloader: ParallelCheckpointDownloader,
    /// Local package store.
    pub package_store: LocalDBPackageStore,
}

impl Debug for LocalDBPackageStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalDBPackageStore")
            .field("package_store_table", &self.package_store_table)
            .finish()
    }
}

impl Debug for EventProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventProcessor")
            .field("system_pkg_id", &self.system_pkg_id)
            .field("checkpoint_store", &self.stores.checkpoint_store)
            .field("walrus_package_store", &self.stores.walrus_package_store)
            .field("committee_store", &self.stores.committee_store)
            .field("event_store", &self.stores.event_store)
            .finish()
    }
}

/// Struct to group system-related parameters.
#[derive(Debug, Clone)]
pub struct SystemConfig {
    /// The package ID of the system package.
    pub system_pkg_id: ObjectID,
    /// The object ID of the system object.
    pub system_object_id: ObjectID,
    /// The object ID of the staking object.
    pub staking_object_id: ObjectID,
}

impl SystemConfig {
    /// Creates a new instance of the system configuration.
    pub fn new(
        system_pkg_id: ObjectID,
        system_object_id: ObjectID,
        staking_object_id: ObjectID,
    ) -> Self {
        Self {
            system_pkg_id,
            system_object_id,
            staking_object_id,
        }
    }
}

/// Struct to group general configuration parameters.
#[derive(Debug)]
pub struct EventProcessorRuntimeConfig {
    /// The address of the RPC server.
    pub rpc_address: String,
    /// The event polling interval.
    pub event_polling_interval: Duration,
    /// The path to the database.
    pub db_path: PathBuf,
    /// The path to the rpc fallback config.
    pub rpc_fallback_config: Option<RpcFallbackConfig>,
}

/// Struct to group client-related parameters.
pub struct SuiClientSet {
    /// Sui client.
    pub sui_client: RetriableSuiClient,
    /// Rest client for the full node.
    client: RetriableRpcClient,
}

impl SuiClientSet {
    /// Creates a new instance of the Sui client set.
    pub fn new(sui_client: RetriableSuiClient, client: RetriableRpcClient) -> Self {
        Self { sui_client, client }
    }
}
impl Debug for SuiClientSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientConfig").finish()
    }
}

impl EventProcessor {
    /// Returns the initialization state for the given event index. If the event index is not found,
    /// it will return `None`. This method is used to recover the state of the event blob writer.
    pub fn get_init_state(&self, from: u64) -> Result<Option<InitState>> {
        let res = self.stores.init_state.get(&from)?;
        Ok(res)
    }

    /// Polls the event store for new events starting from the given sequence number.
    pub fn poll(&self, from: u64) -> Result<Vec<PositionedStreamEvent>, TypedStoreError> {
        self.stores
            .event_store
            .safe_iter_with_bounds(Some(from), None)
            .take(MAX_EVENTS_PER_POLL)
            .map(|result| result.map(|(_, event)| event))
            .collect()
    }

    /// Polls the event store for the next event starting from the given sequence number,
    /// and returns the event along with any InitState that exists at that index.
    pub fn poll_next(&self, from: u64) -> Result<Option<StreamEventWithInitState>> {
        let mut iter = self
            .stores
            .event_store
            .safe_iter_with_bounds(Some(from), None);
        let Some(result) = iter.next() else {
            return Ok(None);
        };
        let (index, event) = result?;
        let init_state = self.get_init_state(index)?;
        let event_with_cursor = StreamEventWithInitState::new(event, init_state);
        Ok(Some(event_with_cursor))
    }

    /// Starts the event processor. This method will run until the cancellation token is cancelled.
    pub async fn start(&self, cancellation_token: CancellationToken) -> Result<(), anyhow::Error> {
        tracing::info!("Starting event processor");
        let pruning_task = self.start_pruning_events(cancellation_token.clone());
        let tailing_task = self.start_tailing_checkpoints(cancellation_token.clone());
        select! {
            pruning_result = pruning_task => {
                cancellation_token.cancel();
                pruning_result
            }
            tailing_result = tailing_task => {
                cancellation_token.cancel();
                tailing_result
            }
        }
    }

    /// Updates the package store with the given objects.
    fn update_package_store(&self, objects: &[Object]) -> Result<(), TypedStoreError> {
        let mut write_batch = self.stores.walrus_package_store.batch();
        for object in objects {
            // Update the package store with the given object.We want to track not just the
            // walrus package but all its transitive dependencies as well. While it is possible
            // to get the transitive dependencies for a package, it is more efficient to just
            // track all packages as we are not expecting a large number of packages.
            if !object.is_package() {
                continue;
            }
            write_batch.insert_batch(
                &self.stores.walrus_package_store,
                std::iter::once((object.id(), object)),
            )?;
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
                    let mut write_batch = self.stores.event_store.batch();
                    write_batch.schedule_delete_range(&self.stores.event_store, &0, &commit_index)?;
                    write_batch.schedule_delete_range(&self.stores.init_state, &0, &commit_index)?;
                    write_batch.write()?;

                    // This will prune the event store by deleting all the sst files relevant to the
                    // events before the commit index
                    let start = bincode::DefaultOptions::new()
                        .with_big_endian()
                        .with_fixint_encoding()
                        .serialize(&0)?;
                    let end = bincode::DefaultOptions::new()
                        .with_big_endian()
                        .with_fixint_encoding()
                        .serialize(&commit_index)?;
                    self.stores.event_store.rocksdb.delete_file_in_range(
                        &self.stores.event_store.cf(),
                        &start,
                        &end,
                    )?;
                }
                _ = cancel_token.cancelled() => {
                    return Ok(());
                },
            }
        }
    }

    /// Verifies the given checkpoint with the given previous checkpoint. This method will verify
    /// that the checkpoint summary matches the content and that the checkpoint contents match the
    /// transactions.
    pub fn verify_checkpoint(
        &self,
        checkpoint: &CheckpointData,
        prev_checkpoint: VerifiedCheckpoint,
    ) -> Result<VerifiedCheckpoint> {
        let Some(committee) = self.stores.committee_store.get(&())? else {
            bail!("No committee found in the committee store");
        };

        let verified_checkpoint = verify_checkpoint_with_committee(
            Arc::new(committee.clone()),
            &prev_checkpoint,
            checkpoint.checkpoint_summary.clone(),
        )
        .map_err(|checkpoint| {
            anyhow!(
                "Failed to verify sui checkpoint: {}",
                checkpoint.sequence_number
            )
        })?;

        // Verify that checkpoint summary matches the content
        if verified_checkpoint.content_digest != *checkpoint.checkpoint_contents.digest() {
            bail!("Checkpoint summary does not match the content");
        }

        // Verify that the checkpoint contents match the transactions
        for (digests, transaction) in checkpoint
            .checkpoint_contents
            .iter()
            .zip(checkpoint.transactions.iter())
        {
            if *transaction.transaction.digest() != digests.transaction {
                bail!("Transaction digest does not match");
            }

            if transaction.effects.digest() != digests.effects {
                bail!("Effects digest does not match");
            }

            if transaction.effects.events_digest().is_some() != transaction.events.is_some() {
                bail!("Events digest and events are inconsistent");
            }

            if let Some((events_digest, events)) = transaction
                .effects
                .events_digest()
                .zip(transaction.events.as_ref())
            {
                if *events_digest != events.digest() {
                    bail!("Events digest does not match");
                }
            }
        }

        Ok(verified_checkpoint)
    }

    /// Tails the full node for new checkpoints and processes them. This method will run until the
    /// cancellation token is cancelled. If the checkpoint processor falls behind the full node, it
    /// will read events from the event blobs so it can catch up.
    pub async fn start_tailing_checkpoints(&self, cancel_token: CancellationToken) -> Result<()> {
        let mut next_event_index = self
            .stores
            .event_store
            .reversed_safe_iter_with_bounds(None, None)?
            .next()
            .transpose()?
            .map(|(k, _)| k + 1)
            .unwrap_or(0);
        let Some(prev_checkpoint) = self.stores.checkpoint_store.get(&())? else {
            bail!("No checkpoint found in the checkpoint store");
        };
        let mut next_checkpoint = prev_checkpoint.inner().sequence_number().saturating_add(1);
        let mut prev_verified_checkpoint =
            VerifiedCheckpoint::new_from_verified(prev_checkpoint.into_inner());
        let mut rx = self
            .checkpoint_downloader
            .start(next_checkpoint, cancel_token);

        // TODO(WAL-667): remove special case
        let on_public_testnet = self
            .package_store
            .get_original_package_id(self.system_pkg_id.into())
            .await?
            == ObjectID::from_str(
                "0x795ddbc26b8cfff2551f45e198b87fc19473f2df50f995376b924ac80e56f88b",
            )?;
        let march_7_7_pm_utc =
            chrono::DateTime::parse_from_rfc3339("2025-03-07T19:00:00+00:00")?.to_utc();

        while let Some(entry) = rx.recv().await {
            let Ok(checkpoint) = entry.result else {
                let error = entry.result.err().unwrap_or(anyhow!("unknown error"));
                tracing::error!(
                    ?error,
                    "failed to download checkpoint {}",
                    entry.sequence_number,
                );
                bail!("failed to download checkpoint: {}", entry.sequence_number);
            };
            ensure!(
                *checkpoint.checkpoint_summary.sequence_number() == next_checkpoint,
                "received out-of-order checkpoint: expected {}, got {}",
                next_checkpoint,
                checkpoint.checkpoint_summary.sequence_number()
            );
            self.metrics
                .event_processor_latest_downloaded_checkpoint
                .set(next_checkpoint as i64);
            self.metrics
                .event_processor_total_downloaded_checkpoints
                .inc();
            let verified_checkpoint =
                self.verify_checkpoint(&checkpoint, prev_verified_checkpoint)?;
            let mut write_batch = self.stores.event_store.batch();
            let mut counter = 0;
            // TODO(WAL-667): remove special case
            let checkpoint_datetime =
                chrono::DateTime::<Utc>::from(verified_checkpoint.timestamp());
            let before_march_7_7pm_utc = checkpoint_datetime < march_7_7_pm_utc;
            for tx in checkpoint.transactions.into_iter() {
                self.update_package_store(&tx.output_objects)
                    .map_err(|e| anyhow!("Failed to update walrus package store: {}", e))?;
                let tx_events = tx.events.unwrap_or_default();
                let original_package_ids: Vec<ObjectID> =
                    try_join_all(tx_events.data.iter().map(|event| {
                        // TODO(WAL-667): remove special case
                        let pkg_address = if on_public_testnet && before_march_7_7pm_utc {
                            event.package_id.into()
                        } else {
                            event.type_.address
                        };
                        self.package_store.get_original_package_id(pkg_address)
                    }))
                    .await?;
                for (seq, tx_event) in tx_events
                    .data
                    .into_iter()
                    .zip(original_package_ids)
                    // Filter out events that are not from the Walrus system package.
                    .filter(|(_, original_id)| *original_id == self.system_pkg_id)
                    .map(|(event, _)| event)
                    .enumerate()
                {
                    tracing::trace!(?tx_event, "event received");
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
                    let event_sequence_number = CheckpointEventPosition::new(
                        *checkpoint.checkpoint_summary.sequence_number(),
                        counter,
                    );
                    let walrus_event =
                        PositionedStreamEvent::new(contract_event, event_sequence_number);
                    write_batch
                        .insert_batch(
                            &self.stores.event_store,
                            std::iter::once((next_event_index, walrus_event)),
                        )
                        .map_err(|e| anyhow!("Failed to insert event into event store: {}", e))?;
                    counter += 1;
                    next_event_index += 1;
                }
            }
            let end_of_checkpoint = PositionedStreamEvent::new_checkpoint_boundary(
                checkpoint.checkpoint_summary.sequence_number,
                counter,
            );
            write_batch.insert_batch(
                &self.stores.event_store,
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
                    .insert_batch(
                        &self.stores.committee_store,
                        std::iter::once(((), committee)),
                    )
                    .map_err(|e| {
                        anyhow!("Failed to insert committee into committee store: {}", e)
                    })?;
                self.package_resolver
                    .package_store()
                    .evict(SYSTEM_PACKAGE_ADDRESSES.iter().copied());
            }
            write_batch
                .insert_batch(
                    &self.stores.checkpoint_store,
                    std::iter::once(((), verified_checkpoint.serializable_ref())),
                )
                .map_err(|e| anyhow!("Failed to insert checkpoint into checkpoint store: {}", e))?;
            write_batch.write()?;
            prev_verified_checkpoint = verified_checkpoint;
            next_checkpoint += 1;
        }
        Ok(())
    }

    /// Clears all stores by scheduling deletion of all entries.
    fn clear_stores(&self) -> Result<(), TypedStoreError> {
        self.stores.committee_store.schedule_delete_all()?;
        self.stores.event_store.schedule_delete_all()?;
        self.stores.walrus_package_store.schedule_delete_all()?;
        Ok(())
    }

    /// Creates a new checkpoint processor with the given configuration. The processor will use the
    /// given configuration to connect to the full node and the checkpoint store. If the checkpoint
    /// store is not found, it will be created. If the checkpoint store is found, the processor will
    /// resume from the last checkpoint.
    pub async fn new(
        config: &EventProcessorConfig,
        runtime_config: EventProcessorRuntimeConfig,
        system_config: SystemConfig,
        registry: &Registry,
    ) -> Result<Self, anyhow::Error> {
        let retry_client = Self::create_and_validate_client(
            &runtime_config.rpc_address,
            config.checkpoint_request_timeout,
            runtime_config.rpc_fallback_config.as_ref(),
        )
        .await?;
        let database = Self::initialize_database(&runtime_config)?;
        let stores = Self::open_stores(&database)?;
        let package_store =
            LocalDBPackageStore::new(stores.walrus_package_store.clone(), retry_client.clone());
        let original_system_package_id = package_store
            .get_original_package_id(system_config.system_pkg_id.into())
            .await?;
        let checkpoint_downloader = ParallelCheckpointDownloader::new(
            retry_client.clone(),
            stores.checkpoint_store.clone(),
            config.adaptive_downloader_config.clone(),
            registry,
        )?;
        let metrics = EventProcessorMetrics::new(registry);
        let event_processor = EventProcessor {
            client: retry_client.clone(),
            stores: stores.clone(),
            system_pkg_id: original_system_package_id,
            event_polling_interval: runtime_config.event_polling_interval,
            event_store_commit_index: Arc::new(Mutex::new(0)),
            pruning_interval: config.pruning_interval,
            package_resolver: Arc::new(Resolver::new(PackageCache::new(package_store.clone()))),
            metrics,
            checkpoint_downloader,
            package_store,
        };

        if event_processor.stores.checkpoint_store.is_empty() {
            event_processor.clear_stores()?;
        }

        let current_checkpoint = event_processor
            .stores
            .checkpoint_store
            .get(&())?
            .map(|t| *t.inner().sequence_number())
            .unwrap_or(0);

        let latest_checkpoint = retry_client.get_latest_checkpoint_summary().await?;
        if current_checkpoint > latest_checkpoint.sequence_number {
            tracing::error!(
                current_checkpoint,
                ?latest_checkpoint,
                "Current store has a checkpoint that is greater than latest network checkpoint! \
                    This is especially likely when a node is restarted running against a newer \
                    localnet, testnet or devnet network."
            );
            return Err(anyhow!("Invalid checkpoint state"));
        }
        let current_lag = latest_checkpoint.sequence_number - current_checkpoint;

        let url = runtime_config.rpc_address.clone();
        let sui_client = SuiClientBuilder::default()
            .build(&url)
            .await
            .context(format!("cannot connect to Sui RPC node at {url}"))?;
        let retriable_sui_client =
            RetriableSuiClient::new(sui_client.clone(), ExponentialBackoffConfig::default());
        if current_lag > config.event_stream_catchup_min_checkpoint_lag {
            let clients = SuiClientSet {
                sui_client: retriable_sui_client.clone(),
                client: retry_client.clone(),
            };
            let recovery_path = runtime_config.db_path.join("recovery");
            if let Err(error) = Self::catchup_using_event_blobs(
                clients,
                system_config.clone(),
                stores.clone(),
                &recovery_path,
                Some(&event_processor.metrics),
            )
            .await
            {
                tracing::error!(?error, "failed to catch up using event blobs");
            } else {
                tracing::info!("successfully caught up using event blobs");
            }
        }

        if event_processor.stores.checkpoint_store.is_empty() {
            let (committee, verified_checkpoint) = Self::get_bootstrap_committee_and_checkpoint(
                retriable_sui_client.clone(),
                retry_client.clone(),
                event_processor.system_pkg_id,
            )
            .await?;
            event_processor
                .stores
                .committee_store
                .insert(&(), &committee)?;
            event_processor
                .stores
                .checkpoint_store
                .insert(&(), verified_checkpoint.serializable_ref())?;
        }

        Ok(event_processor)
    }

    async fn create_and_validate_client(
        rest_url: &str,
        request_timeout: Duration,
        rpc_fallback_config: Option<&RpcFallbackConfig>,
    ) -> Result<RetriableRpcClient, anyhow::Error> {
        let client = sui_rpc_api::Client::new(rest_url)?;
        // Ensure the experimental REST endpoint exists
        ensure_experimental_rest_endpoint_exists(client.clone()).await?;
        let retriable_client = RetriableRpcClient::new(
            client,
            request_timeout,
            ExponentialBackoffConfig::default(),
            rpc_fallback_config.cloned(),
        );
        Ok(retriable_client)
    }

    /// Initializes the database for the event processor.
    pub fn initialize_database(
        processor_config: &EventProcessorRuntimeConfig,
    ) -> Result<Arc<RocksDB>> {
        let metric_conf = MetricConf::default();
        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);
        let database = rocks::open_cf_opts(
            processor_config.db_path.clone(),
            Some(db_opts),
            metric_conf,
            &[
                (CHECKPOINT_STORE, Options::default()),
                (WALRUS_PACKAGE_STORE, Options::default()),
                (COMMITTEE_STORE, Options::default()),
                (EVENT_STORE, Options::default()),
                (INIT_STATE, Options::default()),
            ],
        )?;

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
        if database.cf_handle(INIT_STATE).is_none() {
            database
                .create_cf(INIT_STATE, &Options::default())
                .map_err(typed_store_err_from_rocks_err)?;
        }
        Ok(database)
    }

    /// Opens the stores for the event processor.
    pub fn open_stores(database: &Arc<RocksDB>) -> Result<EventProcessorStores, anyhow::Error> {
        let checkpoint_store = DBMap::reopen(
            database,
            Some(CHECKPOINT_STORE),
            &ReadWriteOptions::default(),
            false,
        )?;
        let walrus_package_store = DBMap::reopen(
            database,
            Some(WALRUS_PACKAGE_STORE),
            &ReadWriteOptions::default(),
            false,
        )?;
        let committee_store = DBMap::reopen(
            database,
            Some(COMMITTEE_STORE),
            &ReadWriteOptions::default(),
            false,
        )?;
        let event_store = DBMap::reopen(
            database,
            Some(EVENT_STORE),
            &ReadWriteOptions::default().set_ignore_range_deletions(true),
            false,
        )?;
        let init_state = DBMap::reopen(
            database,
            Some(INIT_STATE),
            &ReadWriteOptions::default().set_ignore_range_deletions(true),
            false,
        )?;

        let event_processor_stores = EventProcessorStores {
            checkpoint_store,
            walrus_package_store,
            committee_store,
            event_store,
            init_state,
        };

        Ok(event_processor_stores)
    }

    /// Gets the initial committee and checkpoint information by:
    /// 1. Fetching the system package object
    /// 2. Getting its previous transaction
    /// 3. Using that transaction's checkpoint to get the committee and checkpoint data
    ///
    /// Returns a tuple containing:
    /// - The committee for the current or next epoch
    /// - The verified checkpoint containing the system package deployment
    pub async fn get_bootstrap_committee_and_checkpoint(
        sui_client: RetriableSuiClient,
        client: RetriableRpcClient,
        system_pkg_id: ObjectID,
    ) -> Result<(Committee, VerifiedCheckpoint)> {
        let object_options = SuiObjectDataOptions::new()
            .with_bcs()
            .with_type()
            .with_previous_transaction();
        let object = sui_client
            .get_object_with_options(system_pkg_id, object_options)
            .await?;
        let txn_options = SuiTransactionBlockResponseOptions::new();
        let txn_digest = object
            .data
            .ok_or(anyhow!("No object data"))?
            .previous_transaction
            .ok_or(anyhow!("No transaction data"))?;
        let txn = sui_client
            .get_transaction_with_options(txn_digest, txn_options)
            .await?;
        let checkpoint_data = client
            .get_full_checkpoint(txn.checkpoint.ok_or(anyhow!("No checkpoint data"))?)
            .await?;
        let epoch = checkpoint_data.checkpoint_summary.epoch;
        let checkpoint_summary = checkpoint_data.checkpoint_summary.clone();
        let committee = if let Some(end_of_epoch_data) = &checkpoint_summary.end_of_epoch_data {
            let next_committee = end_of_epoch_data
                .next_epoch_committee
                .iter()
                .cloned()
                .collect();
            Committee::new(epoch + 1, next_committee)
        } else {
            let committee_info = sui_client
                .get_committee_info(Some(BigInt::from(epoch)))
                .await?;
            Committee::new(
                committee_info.epoch,
                committee_info.validators.into_iter().collect(),
            )
        };
        let verified_checkpoint = VerifiedCheckpoint::new_unchecked(checkpoint_summary);
        Ok((committee, verified_checkpoint))
    }

    /// Catch up the local event store using certified event blobs stored on Walrus nodes.
    ///
    /// This function performs the following steps:
    /// 1. Initializes Sui and Walrus clients for network communication.
    /// 2. Retrieves the last certified event blob from the network.
    /// 3. Iteratively fetches event blobs backwards from the latest, storing relevant ones locally:
    ///    - Stops when it reaches a blob containing events earlier than the local store's next
    ///      checkpoint.
    ///    - Temporarily stores relevant blobs in a local directory.
    /// 4. Processes stored blobs in reverse order (oldest to newest):
    ///    - Extracts events and inserts them into the local event database.
    ///    - Skips events that are already present in the local store.
    ///    - Updates checkpoints and committee information.
    ///    - Maintains initialization state for continuity.
    ///
    /// This catch-up mechanism ensures that it never introduces any gaps in stored events (i.e., if
    /// the last stored event index in local store is `N`, the catch-up will only store events
    /// starting from `N+1`). If however, the local store is empty, the catch-up will store all
    /// events from the earliest available event blob (in which case the first stored event index
    /// could be greater than `0`).
    pub async fn catchup_using_event_blobs(
        clients: SuiClientSet,
        system_objects: SystemConfig,
        stores: EventProcessorStores,
        recovery_path: &Path,
        metrics: Option<&EventProcessorMetrics>,
    ) -> Result<()> {
        tracing::info!("Starting event catchup using event blobs");
        let next_checkpoint = stores
            .checkpoint_store
            .reversed_safe_iter_with_bounds(None, None)?
            .next()
            .transpose()?
            .map(|(_, checkpoint)| checkpoint.inner().sequence_number + 1);

        if !recovery_path.exists() {
            fs::create_dir_all(recovery_path)?;
        }

        let blobs = collect_event_blobs_for_catchup(
            clients.sui_client.clone(),
            system_objects.staking_object_id,
            system_objects.system_object_id,
            next_checkpoint,
            recovery_path,
            metrics,
        )
        .await?;

        let next_event_index = stores
            .event_store
            .reversed_safe_iter_with_bounds(None, None)?
            .next()
            .transpose()?
            .map(|(i, _)| i + 1);

        Self::process_event_blobs(blobs, &stores, recovery_path, &clients, next_event_index)
            .await?;

        Ok(())
    }

    async fn process_event_blobs(
        blobs: Vec<BlobId>,
        stores: &EventProcessorStores,
        recovery_path: &Path,
        clients: &SuiClientSet,
        next_event_index: Option<u64>,
    ) -> Result<()> {
        tracing::info!("starting to process event blobs");

        let mut num_events_recovered = 0;
        let mut next_event_index = next_event_index;
        for blob_id in blobs.iter().rev() {
            let blob_path = recovery_path.join(blob_id.to_string());
            let buf = std::fs::read(&blob_path)?;
            let event_blob = EventBlob::new(&buf)?;
            let prev_blob_id = event_blob.prev_blob_id();
            let prev_event_id = event_blob.prev_event_id();
            let epoch = event_blob.epoch();
            let last_checkpoint = event_blob.end_checkpoint_sequence_number();

            let (first_event, events) = Self::collect_relevant_events(event_blob, next_event_index);

            if events.is_empty() {
                // We break (rather than continue) because empty events indicates we've hit our
                // first "gap" in the sequence, and all future blobs will also have gaps. Here's
                // why:
                // We process blobs from oldest to newest (in chronological order)
                // For each blob, we only collect events that maintain a continuous sequence (no
                // gaps) with what's already in our database. If our last stored event in the DB has
                // index N, we only accept events starting at index N+1.
                // If a blob returns empty events, it means none of its events could maintain this
                // continuous sequence - there's a gap between our DB's last event and this blob's
                // first event. Since we're going forward in time, all future blobs will have even
                // larger gaps so there's no point in processing them.
                //
                // For example:
                // If our DB's last event has index 100
                // And we find a blob with events [200,201,202], it will return empty events
                // All future blobs will have indices > 200, making gaps even larger
                // So we can safely break the loop
                tracing::info!(
                    event_blob_id = %blob_id,
                    next_event_index = ?next_event_index,
                    "no relevant events found in event blob; breaking the loop"
                );
                break;
            }

            num_events_recovered += events.len();

            let first_event_index = first_event.expect("Event list is not empty").index;
            let last_event_index = events.last().expect("Event list is not empty").index;

            let mut batch = stores.event_store.batch();
            batch.insert_batch(
                &stores.event_store,
                events
                    .iter()
                    .map(|event| (event.index, event.element.clone())),
            )?;

            let checkpoint_summary = clients
                .client
                .get_checkpoint_summary(last_checkpoint)
                .await?;
            let verified_checkpoint = VerifiedCheckpoint::new_unchecked(checkpoint_summary.clone());
            batch.insert_batch(
                &stores.checkpoint_store,
                [((), verified_checkpoint.serializable_ref())],
            )?;

            let next_committee =
                if let Some(end_of_epoch_data) = &checkpoint_summary.end_of_epoch_data {
                    Committee::new(
                        checkpoint_summary.epoch + 1,
                        end_of_epoch_data
                            .next_epoch_committee
                            .iter()
                            .cloned()
                            .collect(),
                    )
                } else {
                    let committee_info = clients
                        .sui_client
                        .governance_api()
                        .get_committee_info(Some(BigInt::from(checkpoint_summary.epoch)))
                        .await?;
                    Committee::new(
                        committee_info.epoch,
                        committee_info.validators.into_iter().collect(),
                    )
                };

            batch.insert_batch(
                &stores.committee_store,
                std::iter::once(((), next_committee)),
            )?;

            let state = InitState::new(prev_blob_id, prev_event_id, first_event_index, epoch);
            batch.insert_batch(
                &stores.init_state,
                std::iter::once((first_event_index, state)),
            )?;

            batch.write()?;
            fs::remove_file(blob_path)?;
            next_event_index = Some(last_event_index + 1);
            tracing::info!(
                "processed event blob {} with {} events, last event index: {}",
                blob_id,
                events.len(),
                last_event_index
            );
        }
        tracing::info!("recovered {} events from event blobs", num_events_recovered);
        Ok(())
    }

    /// Processes an event blob and returns relevant events that maintain a continuous sequence with
    /// the local store.
    ///
    /// Returns a tuple containing:
    /// - The first event in the blob (regardless of relevance)
    /// - A vector of relevant events paired with their indices
    ///
    /// Events are considered relevant if they either:
    /// - Start at the next expected index (when next_event_index is Some)
    /// - Or all events in the blob (when next_event_index is None)
    ///
    /// The function stops collecting events as soon as it encounters a gap in the sequence.
    fn collect_relevant_events(
        event_blob: EventBlob,
        next_event_index: Option<u64>,
    ) -> (Option<IndexedStreamEvent>, Vec<IndexedStreamEvent>) {
        let mut iterator = event_blob.peekable();
        let first_event = iterator.peek().cloned();
        let relevant_events: Vec<IndexedStreamEvent> = iterator
            .skip_while(|event| next_event_index.is_some_and(|index| event.index < index))
            .scan(next_event_index, |state, event| match state {
                Some(expected_index) if event.index == *expected_index => {
                    *state = Some(*expected_index + 1);
                    Some(event)
                }
                None => Some(event),
                _ => None,
            })
            .collect();
        (first_event, relevant_events)
    }
}

impl LocalDBPackageStore {
    /// Creates a new instance of the local package store.
    pub fn new(package_store_table: DBMap<ObjectID, Object>, client: RetriableRpcClient) -> Self {
        Self {
            package_store_table,
            fallback_client: client,
            original_id_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Updates the package store with the given object.
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

    /// Gets the object with the given id. If the object is not found in the local store, it will be
    /// fetched from the full node.
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

    /// Gets the original package id for the given package id.
    pub async fn get_original_package_id(&self, id: AccountAddress) -> Result<ObjectID> {
        if let Some(&original_id) = self.original_id_cache.read().await.get(&id) {
            return Ok(original_id);
        }

        let object = self.get(id).await?;
        let Data::Package(package) = &object.data else {
            return Err(anyhow!("Object is not a package"));
        };

        let original_id = package.original_package_id();

        self.original_id_cache.write().await.insert(id, original_id);

        Ok(original_id)
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

    use checkpoint_downloader::AdaptiveDownloaderConfig;
    use sui_types::messages_checkpoint::CheckpointSequenceNumber;
    use tokio::sync::Mutex;
    use walrus_core::BlobId;
    use walrus_sui::{test_utils::EventForTesting, types::BlobCertified};
    use walrus_utils::tests::global_test_lock;

    use super::*;

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
                    (INIT_STATE, Options::default()),
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
        let event_store = DBMap::<u64, PositionedStreamEvent>::reopen(
            &database,
            Some(EVENT_STORE),
            &ReadWriteOptions::default(),
            false,
        )?;
        let init_state = DBMap::<u64, InitState>::reopen(
            &database,
            Some(INIT_STATE),
            &ReadWriteOptions::default(),
            false,
        )?;
        let client = sui_rpc_api::Client::new("http://localhost:8080")?;
        let retry_client = RetriableRpcClient::new(
            client.clone(),
            Duration::from_secs(5),
            ExponentialBackoffConfig::default(),
            None,
        );
        let package_store =
            LocalDBPackageStore::new(walrus_package_store.clone(), retry_client.clone());
        let checkpoint_downloader = ParallelCheckpointDownloader::new(
            retry_client.clone(),
            checkpoint_store.clone(),
            AdaptiveDownloaderConfig::default(),
            &Registry::default(),
        )?;
        let stores = EventProcessorStores {
            checkpoint_store,
            walrus_package_store,
            committee_store,
            event_store,
            init_state,
        };
        Ok(EventProcessor {
            client: retry_client,
            stores,
            system_pkg_id: ObjectID::random(),
            event_store_commit_index: Arc::new(Mutex::new(0)),
            pruning_interval: Duration::from_secs(10),
            event_polling_interval: Duration::from_secs(1),
            package_resolver: Arc::new(Resolver::new(PackageCache::new(package_store.clone()))),
            metrics: EventProcessorMetrics::new(&Registry::default()),
            checkpoint_downloader,
            package_store,
        })
    }

    fn default_event_for_testing(
        checkpoint_sequence_number: CheckpointSequenceNumber,
        counter: u64,
    ) -> PositionedStreamEvent {
        PositionedStreamEvent::new(
            BlobCertified::for_testing(BlobId([7; 32])).into(),
            CheckpointEventPosition::new(checkpoint_sequence_number, counter),
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
            processor.stores.event_store.insert(&i, &event).unwrap();
        }

        // poll events from beginning
        let events = processor.poll(0).unwrap();
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
            processor.stores.event_store.insert(&i, &event).unwrap();
        }

        // poll events
        let events = processor.poll(0).unwrap();
        assert_eq!(events.len(), 100);
        // assert events are the same
        for (expected, actual) in expected_events1.iter().zip(events.iter()) {
            assert_eq!(expected, actual);
        }
        let mut expected_events2 = vec![];
        for i in 100..200 {
            let event = default_event_for_testing(0, i);
            expected_events2.push(event.clone());
            processor.stores.event_store.insert(&i, &event).unwrap();
        }
        let events = processor.poll(100).unwrap();
        assert_eq!(events.len(), 100);
        // assert events are the same
        for (expected, actual) in expected_events2.iter().zip(events.iter()) {
            assert_eq!(expected, actual);
        }
    }

    #[test]
    fn test_collect_relevant_events() {
        // Helper function to create dummy events
        fn create_event(index: u64) -> IndexedStreamEvent {
            IndexedStreamEvent {
                index,
                element: PositionedStreamEvent::new_checkpoint_boundary(index, 0),
            }
        }

        // Create a mock EventBlob
        let events = vec![
            create_event(0),
            create_event(1),
            create_event(2),
            create_event(3),
            create_event(4),
        ];
        let buf = EventBlob::from_events(0, BlobId::ZERO, None, 2, 10, events.iter()).unwrap();
        let event_blob = EventBlob::new(&buf).unwrap();

        // Test case 1: Collect all events (next_event_index = None)
        let (_first_event, result) = EventProcessor::collect_relevant_events(event_blob, None);
        assert_eq!(result.len(), 5);
        assert_eq!(result[0].index, 0);
        assert_eq!(result[4].index, 4);

        // Test case 2: Collect events starting from index 2
        let buf = EventBlob::from_events(0, BlobId::ZERO, None, 2, 10, events.iter()).unwrap();
        let event_blob = EventBlob::new(&buf).unwrap();
        let (_first_event, result) = EventProcessor::collect_relevant_events(event_blob, Some(2));
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].index, 2);
        assert_eq!(result[2].index, 4);

        // Test case 3: Collect events starting from an index beyond the blob's range
        let buf = EventBlob::from_events(0, BlobId::ZERO, None, 2, 10, events.iter()).unwrap();
        let event_blob = EventBlob::new(&buf).unwrap();
        let (_first_event, result) = EventProcessor::collect_relevant_events(event_blob, Some(10));
        assert!(result.is_empty());
    }
}
