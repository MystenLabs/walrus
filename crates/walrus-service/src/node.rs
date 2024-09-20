// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Walrus storage node.

use std::{future::Future, num::NonZeroU16, path::PathBuf, sync::Arc};

use anyhow::{anyhow, bail, Context};
use fastcrypto::traits::KeyPair;
use futures::{StreamExt, TryFutureExt};
use futures_util::stream;
use prometheus::Registry;
use serde::Serialize;
use shard_sync::ShardSyncHandler;
use sui_types::{digests::TransactionDigest, event::EventID};
use tokio::{select, time::Instant};
use tokio_util::sync::CancellationToken;
use tracing::{field, Instrument};
use typed_store::{rocks::MetricConf, DBMetrics, TypedStoreError};
use walrus_core::{
    encoding::{EncodingAxis, EncodingConfig, RecoverySymbolError},
    ensure,
    keys::ProtocolKeyPair,
    merkle::MerkleProof,
    messages::{
        Confirmation,
        InvalidBlobIdAttestation,
        InvalidBlobIdMsg,
        ProtocolMessage,
        SignedMessage,
        SignedSyncShardRequest,
        StorageConfirmation,
        SyncShardResponse,
    },
    metadata::{UnverifiedBlobMetadataWithId, VerifiedBlobMetadataWithId},
    BlobId,
    Epoch,
    InconsistencyProof,
    PublicKey,
    RecoverySymbol,
    ShardIndex,
    Sliver,
    SliverPairIndex,
    SliverType,
};
use walrus_event::event_processor::EventProcessor;
use walrus_sdk::api::{BlobStatus, ServiceHealthInfo, StoredOnNodeStatus};
use walrus_sui::{
    client::SuiReadClient,
    types::{BlobCertified, BlobEvent, ContractEvent, EpochChangeEvent, InvalidBlobId},
};

use self::{
    blob_sync::BlobSyncHandler,
    committee::{CommitteeService, CommitteeServiceFactory, SuiCommitteeServiceFactory},
    config::{StorageNodeConfig, SuiConfig},
    contract_service::{SuiSystemContractService, SystemContractService},
    errors::IndexOutOfRange,
    metrics::{NodeMetricSet, TelemetryLabel as _, STATUS_PENDING, STATUS_PERSISTED},
    storage::{blob_info::BlobInfoApi as _, EventProgress, ShardStorage},
};

pub mod committee;
pub mod config;
pub mod contract_service;
pub mod server;
pub mod system_events;

pub(crate) mod metrics;

mod blob_sync;
mod shard_sync;

pub(crate) mod errors;
use errors::{
    BlobStatusError,
    ComputeStorageConfirmationError,
    InconsistencyProofError,
    InvalidEpochError,
    RetrieveMetadataError,
    RetrieveSliverError,
    RetrieveSymbolError,
    ShardNotAssigned,
    StoreMetadataError,
    StoreSliverError,
    SyncShardServiceError,
};

mod storage;
pub use storage::{DatabaseConfig, Storage};
use walrus_event::{EventStreamCursor, EventStreamElement};

use crate::node::{
    storage::event_blob_writer::EventBlobWriter,
    system_events::{EventManager, SuiSystemEventProvider},
};

/// Trait for all functionality offered by a storage node.
pub trait ServiceState {
    /// Retrieves the metadata associated with a blob.
    fn retrieve_metadata(
        &self,
        blob_id: &BlobId,
    ) -> Result<VerifiedBlobMetadataWithId, RetrieveMetadataError>;

    /// Stores the metadata associated with a blob.
    ///
    /// Returns true if the metadata was newly stored, false if it was already present.
    fn store_metadata(
        &self,
        metadata: UnverifiedBlobMetadataWithId,
    ) -> Result<bool, StoreMetadataError>;

    /// Returns whether the metadata is stored in the shard.
    fn metadata_status(
        &self,
        blob_id: &BlobId,
    ) -> Result<StoredOnNodeStatus, RetrieveMetadataError>;

    /// Retrieves a primary or secondary sliver for a blob for a shard held by this storage node.
    fn retrieve_sliver(
        &self,
        blob_id: &BlobId,
        sliver_pair_index: SliverPairIndex,
        sliver_type: SliverType,
    ) -> Result<Sliver, RetrieveSliverError>;

    /// Stores the primary or secondary encoding for a blob for a shard held by this storage node.
    fn store_sliver(
        &self,
        blob_id: &BlobId,
        sliver_pair_index: SliverPairIndex,
        sliver: &Sliver,
    ) -> Result<bool, StoreSliverError>;

    /// Retrieves a signed confirmation over the identifiers of the shards storing their respective
    /// sliver-pairs for their BlobIds.
    fn compute_storage_confirmation(
        &self,
        blob_id: &BlobId,
    ) -> impl Future<Output = Result<StorageConfirmation, ComputeStorageConfirmationError>> + Send;

    /// Verifies an inconsistency proof and provides a signed attestation for it, if valid.
    fn verify_inconsistency_proof(
        &self,
        blob_id: &BlobId,
        inconsistency_proof: InconsistencyProof,
    ) -> impl Future<Output = Result<InvalidBlobIdAttestation, InconsistencyProofError>> + Send;

    /// Retrieves a recovery symbol for a shard held by this storage node.
    ///
    /// The function creates the recovery symbol for the sliver of type `sliver_type` and of sliver
    /// pair index `target_pair_index`, starting from the sliver of the orthogonal sliver type and
    /// index `sliver_pair_index`.
    ///
    /// Returns the recovery symbol for the requested sliver.
    fn retrieve_recovery_symbol(
        &self,
        blob_id: &BlobId,
        sliver_pair_index: SliverPairIndex,
        sliver_type: SliverType,
        target_pair_index: SliverPairIndex,
    ) -> Result<RecoverySymbol<MerkleProof>, RetrieveSymbolError>;

    /// Retrieves the blob status for the given `blob_id`.
    fn blob_status(&self, blob_id: &BlobId) -> Result<BlobStatus, BlobStatusError>;

    /// Returns the number of shards the node is currently operating with.
    fn n_shards(&self) -> NonZeroU16;

    /// Returns the node health information of this ServiceState.
    fn health_info(&self) -> ServiceHealthInfo;

    /// Returns whether the sliver is stored in the shard.
    fn sliver_status<A: EncodingAxis>(
        &self,
        blob_id: &BlobId,
        sliver_pair_index: SliverPairIndex,
    ) -> Result<StoredOnNodeStatus, RetrieveSliverError>;

    /// Returns the shard data with the provided signed request and the public key of the sender.
    fn sync_shard(
        &self,
        public_key: PublicKey,
        signed_request: SignedSyncShardRequest,
    ) -> Result<SyncShardResponse, SyncShardServiceError>;
}

/// Builder to construct a [`StorageNode`].
#[derive(Debug, Default)]
pub struct StorageNodeBuilder {
    storage: Option<Storage>,
    event_manager: Option<Box<dyn EventManager>>,
    committee_service_factory: Option<Box<dyn CommitteeServiceFactory>>,
    contract_service: Option<Box<dyn SystemContractService>>,
}

impl StorageNodeBuilder {
    /// Creates a new builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the underlying storage for the node, instead of constructing one from the config.
    pub fn with_storage(mut self, storage: Storage) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Sets the [`EventManager`] to be used with the node.
    pub fn with_system_event_manager(mut self, event_manager: Box<dyn EventManager>) -> Self {
        self.event_manager = Some(event_manager);
        self
    }

    /// Sets the [`SystemContractService`] to be used with the node.
    pub fn with_system_contract_service(
        mut self,
        contract_service: Box<dyn SystemContractService>,
    ) -> Self {
        self.contract_service = Some(contract_service);
        self
    }

    /// Sets the [`CommitteeServiceFactory`] used with the node.
    pub fn with_committee_service_factory(
        mut self,
        factory: Box<dyn CommitteeServiceFactory>,
    ) -> Self {
        self.committee_service_factory = Some(factory);
        self
    }

    /// Consumes the builder and constructs a new [`StorageNode`].
    ///
    /// The constructed storage node will use dependent services provided to the builder, otherwise,
    /// it will construct a new underlying storage and [`EventManager`] from
    /// parameters in the config.
    ///
    /// # Panics
    ///
    /// Panics if `config.sui` is `None` and no [`EventManager`], no
    /// [`CommitteeServiceFactory`], or no [`SystemContractService`] was configured with
    /// their respective functions
    /// ([`with_system_event_manager()`][Self::with_system_event_manager],
    /// [`with_committee_service_factory()`][Self::with_committee_service_factory],
    /// [`with_system_contract_service()`][Self::with_system_contract_service]); or if the
    /// `config.protocol_key_pair` has not yet been loaded into memory.
    pub async fn build(
        self,
        config: &StorageNodeConfig,
        metrics_registry: Registry,
    ) -> Result<StorageNode, anyhow::Error> {
        DBMetrics::init(&metrics_registry);

        let protocol_key_pair = config
            .protocol_key_pair
            .get()
            .expect("protocol key pair must already be loaded")
            .clone();

        let sui_config_and_client =
            if self.event_manager.is_none() || self.committee_service_factory.is_none() {
                let sui_config = config.sui.as_ref().expect(
                    "either a Sui config or an event provider and committee service \
                            factory must be specified",
                );
                Some((create_read_client(sui_config).await?, sui_config))
            } else {
                None
            };

        let event_manager: Box<dyn EventManager> = if let Some(event_manager) = self.event_manager {
            event_manager
        } else {
            let (read_client, sui_config) = sui_config_and_client
                .as_ref()
                .expect("this is always created if self.event_manager.is_none()");
            let event_manager: Box<dyn EventManager> = match &config.event_processor_config {
                Some(event_processor_config) => Box::new(
                    EventProcessor::new(
                        event_processor_config,
                        read_client.get_system_package_id(),
                        sui_config.event_polling_interval,
                        &config.storage_path,
                    )
                    .await?,
                ),
                None => Box::new(SuiSystemEventProvider::new(
                    read_client.clone(),
                    sui_config.event_polling_interval,
                )),
            };
            event_manager
        };

        let contract_service = match self.contract_service {
            None => Box::new(
                SuiSystemContractService::from_config(
                    config.sui.as_ref().expect("Sui config must be provided"),
                )
                .await?,
            ),
            Some(service) => service,
        };

        let committee_service_factory = self.committee_service_factory.unwrap_or_else(|| {
            let (read_client, _) = sui_config_and_client
                .expect("this is always created if self.committee_service_factory.is_none()");
            Box::new(SuiCommitteeServiceFactory::new(
                read_client,
                config.blob_recovery.committee_service_config.clone(),
            ))
        });

        StorageNode::new(
            config,
            protocol_key_pair,
            event_manager,
            committee_service_factory,
            contract_service,
            &metrics_registry,
            self.storage,
        )
        .await
    }
}

async fn create_read_client(sui_config: &SuiConfig) -> Result<SuiReadClient, anyhow::Error> {
    let SuiConfig {
        rpc,
        system_object,
        staking_object,
        ..
    } = sui_config;

    let client = SuiReadClient::new_for_rpc(&rpc, *system_object, *staking_object).await?;

    Ok(client)
}

/// A Walrus storage node, responsible for 1 or more shards on Walrus.
#[derive(Debug)]
pub struct StorageNode {
    inner: Arc<StorageNodeInner>,
    blob_sync_handler: BlobSyncHandler,
    _shard_sync_handler: ShardSyncHandler,
}

/// The internal state of a Walrus storage node.
#[derive(Debug)]

pub struct StorageNodeInner {
    protocol_key_pair: ProtocolKeyPair,
    storage: Storage,
    encoding_config: Arc<EncodingConfig>,
    event_manager: Box<dyn EventManager>,
    contract_service: Arc<dyn SystemContractService>,
    committee_service: Arc<dyn CommitteeService>,
    _committee_service_factory: Box<dyn CommitteeServiceFactory>,
    start_time: Instant,
    db_root_dir_path: PathBuf,
    metrics: NodeMetricSet,
}

impl StorageNode {
    async fn new(
        config: &StorageNodeConfig,
        key_pair: ProtocolKeyPair,
        event_manager: Box<dyn EventManager>,
        committee_service_factory: Box<dyn CommitteeServiceFactory>,
        contract_service: Box<dyn SystemContractService>,
        registry: &Registry,
        pre_created_storage: Option<Storage>, // For testing purposes. TODO(#703): remove.
    ) -> Result<Self, anyhow::Error> {
        let start_time = Instant::now();
        let committee_service = committee_service_factory
            .new_for_epoch(Some(key_pair.as_ref().public()))
            .await
            .context("unable to construct a committee service for the storage node")?;

        let db_config = config.db_config.clone().unwrap_or_default();
        let mut storage = if let Some(storage) = pre_created_storage {
            storage
        } else {
            Storage::open(
                config.storage_path.as_path(),
                db_config,
                MetricConf::new("storage"),
            )?
        };

        let encoding_config = Arc::new(EncodingConfig::new(committee_service.get_shard_count()));

        let committee = committee_service.committee();
        let managed_shards = committee.shards_for_node_public_key(key_pair.as_ref().public());
        if managed_shards.is_empty() {
            tracing::info!(epoch = committee.epoch, "node does not manage any shards");
        }

        for shard in managed_shards {
            storage
                .create_storage_for_shard(*shard)
                .with_context(|| format!("unable to initialize storage for shard {}", shard))?;
        }

        let inner = Arc::new(StorageNodeInner {
            protocol_key_pair: key_pair,
            storage,
            event_manager,
            encoding_config,
            contract_service: contract_service.into(),
            committee_service: committee_service.into(),
            _committee_service_factory: committee_service_factory,
            metrics: NodeMetricSet::new(registry),
            start_time,
            db_root_dir_path: config.storage_path.clone(),
        });

        inner.init_gauges()?;

        let blob_sync_handler = BlobSyncHandler::new(
            inner.clone(),
            config.blob_recovery.max_concurrent_blob_syncs,
        );

        let shard_sync_handler =
            ShardSyncHandler::new(inner.clone(), config.shard_sync_config.clone());
        // Upon restart, resume any ongoing blob syncs if there is any.
        shard_sync_handler.restart_syncs().await?;

        Ok(StorageNode {
            inner,
            blob_sync_handler,
            _shard_sync_handler: shard_sync_handler,
        })
    }

    /// Creates a new [`StorageNodeBuilder`] for constructing a `StorageNode`.
    pub fn builder() -> StorageNodeBuilder {
        StorageNodeBuilder::default()
    }

    /// Run the walrus-node logic until cancelled using the provided cancellation token.
    pub async fn run(&self, cancel_token: CancellationToken) -> anyhow::Result<()> {
        select! {
            result = self.process_events() => match result {
                Ok(()) => unreachable!("process_events should never return successfully"),
                Err(err) => return Err(err),
            },
            _ = cancel_token.cancelled() => {
                self.blob_sync_handler.cancel_all().await?;
            },
        }
        Ok(())
    }

    /// Returns the shards currently owned by the storage node.
    pub fn shards(&self) -> impl ExactSizeIterator<Item = ShardIndex> + '_ {
        self.inner.storage.shards()
    }

    async fn process_events(&self) -> anyhow::Result<()> {
        let storage = &self.inner.storage;
        let mut event_blob_writer =
            EventBlobWriter::new(&self.inner.db_root_dir_path, self.inner.clone())?;
        let from_event_id = storage.get_event_cursor()?.map(|(_, cursor)| cursor);
        let from_element_index = storage.get_sequentially_processed_event_count()?;
        let event_cursor = EventStreamCursor::new(from_event_id, from_element_index);
        let event_stream = Box::into_pin(self.inner.event_manager.events(event_cursor).await?);
        let next_index: usize = from_element_index.try_into().expect("64-bit architecture");
        let index_stream = stream::iter(next_index..);
        let mut indexed_element_stream = index_stream.zip(event_stream);
        while let Some((element_index, stream_element)) = indexed_element_stream.next().await {
            let span = tracing::info_span!(
                parent: None,
                "blob_store receive",
                "otel.kind" = "CONSUMER",
                "otel.status_code" = field::Empty,
                "otel.status_message" = field::Empty,
                "messaging.operation.type" = "receive",
                "messaging.system" = "sui",
                "messaging.destination.name" = "blob_store",
                "messaging.client.id" = %self.inner.public_key(),
                "walrus.event.index" = element_index,
                "walrus.event.tx_digest" = ?stream_element.element.event_id().map(|c| c.tx_digest),
                "walrus.event.checkpoint_seq" = ?stream_element.global_sequence_number
                    .checkpoint_sequence_number,
                "walrus.event.kind" = stream_element.element.label(),
                "walrus.blob_id" = ?stream_element.element.blob_id(),
                "error.type" = field::Empty,
            );
            let cloned_stream_element = stream_element.clone();
            async move {
                let _timer_guard = &self
                    .inner
                    .metrics
                    .event_process_duration_seconds
                    .with_label_values(&[stream_element.element.label()])
                    .start_timer();
                if let Some(blob_event) = stream_element.element.blob_event() {
                    storage.update_blob_info(blob_event)?;
                }
                match stream_element.element {
                    EventStreamElement::ContractEvent(ContractEvent::BlobEvent(
                        BlobEvent::Certified(event),
                    )) => {
                        self.process_blob_certified_event(element_index, event)
                            .await?;
                    }
                    EventStreamElement::ContractEvent(ContractEvent::BlobEvent(
                        BlobEvent::InvalidBlobID(event),
                    )) => {
                        self.process_blob_invalid_event(element_index, event)
                            .await?;
                    }
                    EventStreamElement::ContractEvent(ContractEvent::BlobEvent(
                        BlobEvent::Registered(event),
                    )) => {
                        self.inner
                            .mark_event_completed(element_index, &event.event_id)?;
                    }
                    EventStreamElement::ContractEvent(ContractEvent::EpochChangeEvent(
                        EpochChangeEvent::EpochParametersSelected(event),
                    )) => {
                        tracing::info!("EpochParametersSelected event received: {:?}", event,);
                        self.inner
                            .mark_event_completed(element_index, &event.event_id)?;
                    }
                    EventStreamElement::ContractEvent(ContractEvent::EpochChangeEvent(
                        EpochChangeEvent::EpochChangeStart(event),
                    )) => {
                        tracing::info!("EpochChangeStart event received: {:?}", event);
                        self.inner
                            .mark_event_completed(element_index, &event.event_id)?;
                    }
                    EventStreamElement::ContractEvent(ContractEvent::EpochChangeEvent(
                        EpochChangeEvent::EpochChangeDone(event),
                    )) => {
                        tracing::info!("EpochChangeDone event received: {:?}", event);
                        self.inner
                            .mark_event_completed(element_index, &event.event_id)?;
                    }
                    EventStreamElement::CheckpointBoundary => {
                        self.inner.mark_element_at_index(element_index)?;
                    }
                }
                Ok::<(), anyhow::Error>(())
            }
            .inspect_err(|err| {
                let span = tracing::Span::current();
                span.record("otel.status_code", "error");
                span.record("otel.status_message", field::display(err));
            })
            .instrument(span)
            .await?;
            event_blob_writer
                .write(cloned_stream_element, element_index as u64)
                .await?;
            self.inner
                .event_manager
                .drop_events_before(EventStreamCursor::new(
                    None,
                    self.get_last_committed_event_index(&event_blob_writer)?,
                ))
                .await?;
        }

        bail!("event stream for blob events stopped")
    }

    #[tracing::instrument(skip_all)]
    fn get_last_committed_event_index(
        &self,
        event_blob_writer: &EventBlobWriter,
    ) -> anyhow::Result<u64> {
        let storage = &self.inner.storage;
        let last_committed_sequencer_index: Option<u64> =
            storage.get_event_cursor()?.map(|(index, _)| index);
        let last_committed_event_blob_writer_index: Option<u64> =
            event_blob_writer.latest_committed_event_index();
        // if both are some, return minimum of the two otherwise return 0
        Ok(last_committed_sequencer_index
            .zip(last_committed_event_blob_writer_index)
            .map(|(sequencer_index, blob_writer_index)| {
                u64::min(sequencer_index, blob_writer_index)
            })
            .unwrap_or(0))
    }

    #[tracing::instrument(skip_all)]
    async fn process_blob_certified_event(
        &self,
        event_index: usize,
        event: BlobCertified,
    ) -> anyhow::Result<()> {
        let start = tokio::time::Instant::now();
        let histogram_set = self.inner.metrics.recover_blob_duration_seconds.clone();

        if self.inner.storage.is_stored_at_all_shards(&event.blob_id)?
            || self.inner.storage.is_invalid(&event.blob_id)?
        {
            self.inner
                .mark_event_completed(event_index, &event.event_id)?;

            metrics::with_label!(histogram_set, metrics::STATUS_SKIPPED)
                .observe(start.elapsed().as_secs_f64());

            return Ok(());
        }

        // Slivers and (possibly) metadata are not stored, so initiate blob sync.
        // TODO(kwuest): Handle epoch change. (#405)
        self.blob_sync_handler
            .start_sync(event, event_index, start)
            .await?;

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn process_blob_invalid_event(
        &self,
        event_index: usize,
        event: InvalidBlobId,
    ) -> anyhow::Result<()> {
        if let Some((cancelled_event_index, cancelled_event_id)) =
            self.blob_sync_handler.cancel_sync(&event.blob_id).await?
        {
            // Advance the event cursor with the event of the cancelled sync. Since the blob is
            // invalid the associated blob certified event is completed without a sync.
            //
            // Race condition is avoided here by the fact that process_blob_invalid_event is not
            // run concurrently with any logic for processing events from the stream, so a
            // cancelled event cannot be restarted.
            self.inner
                .mark_event_completed(cancelled_event_index, &cancelled_event_id)?;
        }
        self.inner.storage.delete_blob(&event.blob_id)?;
        self.inner
            .mark_event_completed(event_index, &event.event_id)?;
        Ok(())
    }
}

impl StorageNodeInner {
    fn current_epoch(&self) -> Epoch {
        self.committee_service.get_epoch()
    }

    fn check_index(&self, index: SliverPairIndex) -> Result<(), IndexOutOfRange> {
        if index.get() < self.n_shards().get() {
            Ok(())
        } else {
            Err(IndexOutOfRange {
                index: index.get(),
                max: self.n_shards().get(),
            })
        }
    }

    fn get_shard_for_sliver_pair(
        &self,
        sliver_pair_index: SliverPairIndex,
        blob_id: &BlobId,
    ) -> Result<&Arc<ShardStorage>, ShardNotAssigned> {
        let shard_index =
            sliver_pair_index.to_shard_index(self.encoding_config.n_shards(), blob_id);
        self.storage
            .shard_storage(shard_index)
            .ok_or(ShardNotAssigned(shard_index, self.current_epoch()))
    }

    fn init_gauges(&self) -> Result<(), TypedStoreError> {
        let persisted = self.storage.get_sequentially_processed_event_count()?;

        metrics::with_label!(self.metrics.event_cursor_progress, "persisted").set(persisted);

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    fn mark_event_completed(
        &self,
        event_index: usize,
        cursor: &EventID,
    ) -> Result<(), TypedStoreError> {
        let EventProgress { persisted, pending } = self
            .storage
            .maybe_advance_event_cursor(event_index, cursor)?;

        let event_cursor_progress = &self.metrics.event_cursor_progress;
        metrics::with_label!(event_cursor_progress, STATUS_PERSISTED).add(persisted);
        metrics::with_label!(event_cursor_progress, STATUS_PENDING).set(pending);

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    fn mark_element_at_index(&self, element_index: usize) -> Result<(), TypedStoreError> {
        let event_id = EventID::from((TransactionDigest::random(), 0));
        self.mark_event_completed(element_index, &event_id)?;
        Ok(())
    }

    fn public_key(&self) -> &PublicKey {
        self.protocol_key_pair.as_ref().public()
    }
}

impl ServiceState for StorageNode {
    fn retrieve_metadata(
        &self,
        blob_id: &BlobId,
    ) -> Result<VerifiedBlobMetadataWithId, RetrieveMetadataError> {
        self.inner.retrieve_metadata(blob_id)
    }

    fn store_metadata(
        &self,
        metadata: UnverifiedBlobMetadataWithId,
    ) -> Result<bool, StoreMetadataError> {
        self.inner.store_metadata(metadata)
    }

    fn metadata_status(
        &self,
        blob_id: &BlobId,
    ) -> Result<StoredOnNodeStatus, RetrieveMetadataError> {
        self.inner.metadata_status(blob_id)
    }

    fn retrieve_sliver(
        &self,
        blob_id: &BlobId,
        sliver_pair_index: SliverPairIndex,
        sliver_type: SliverType,
    ) -> Result<Sliver, RetrieveSliverError> {
        self.inner
            .retrieve_sliver(blob_id, sliver_pair_index, sliver_type)
    }

    fn store_sliver(
        &self,
        blob_id: &BlobId,
        sliver_pair_index: SliverPairIndex,
        sliver: &Sliver,
    ) -> Result<bool, StoreSliverError> {
        self.inner.store_sliver(blob_id, sliver_pair_index, sliver)
    }

    fn compute_storage_confirmation(
        &self,
        blob_id: &BlobId,
    ) -> impl Future<Output = Result<StorageConfirmation, ComputeStorageConfirmationError>> + Send
    {
        self.inner.compute_storage_confirmation(blob_id)
    }

    fn verify_inconsistency_proof(
        &self,
        blob_id: &BlobId,
        inconsistency_proof: InconsistencyProof,
    ) -> impl Future<Output = Result<InvalidBlobIdAttestation, InconsistencyProofError>> + Send
    {
        self.inner
            .verify_inconsistency_proof(blob_id, inconsistency_proof)
    }

    fn retrieve_recovery_symbol(
        &self,
        blob_id: &BlobId,
        sliver_pair_index: SliverPairIndex,
        sliver_type: SliverType,
        target_pair_index: SliverPairIndex,
    ) -> Result<RecoverySymbol<MerkleProof>, RetrieveSymbolError> {
        self.inner.retrieve_recovery_symbol(
            blob_id,
            sliver_pair_index,
            sliver_type,
            target_pair_index,
        )
    }

    fn blob_status(&self, blob_id: &BlobId) -> Result<BlobStatus, BlobStatusError> {
        self.inner.blob_status(blob_id)
    }

    fn n_shards(&self) -> NonZeroU16 {
        self.inner.n_shards()
    }

    fn health_info(&self) -> ServiceHealthInfo {
        self.inner.health_info()
    }

    fn sliver_status<A: EncodingAxis>(
        &self,
        blob_id: &BlobId,
        sliver_pair_index: SliverPairIndex,
    ) -> Result<StoredOnNodeStatus, RetrieveSliverError> {
        self.inner.sliver_status::<A>(blob_id, sliver_pair_index)
    }

    fn sync_shard(
        &self,
        public_key: PublicKey,
        signed_request: SignedSyncShardRequest,
    ) -> Result<SyncShardResponse, SyncShardServiceError> {
        self.inner.sync_shard(public_key, signed_request)
    }
}

impl ServiceState for StorageNodeInner {
    fn retrieve_metadata(
        &self,
        blob_id: &BlobId,
    ) -> Result<VerifiedBlobMetadataWithId, RetrieveMetadataError> {
        self.storage
            .get_metadata(blob_id)
            .context("database error when retrieving metadata")?
            .ok_or(RetrieveMetadataError::Unavailable)
            .inspect(|_| self.metrics.metadata_retrieved_total.inc())
    }

    fn store_metadata(
        &self,
        metadata: UnverifiedBlobMetadataWithId,
    ) -> Result<bool, StoreMetadataError> {
        let Some(blob_info) = self
            .storage
            .get_blob_info(metadata.blob_id())
            .context("could not retrieve blob info")?
        else {
            return Err(StoreMetadataError::NotCurrentlyRegistered);
        };

        if let Some(event) = blob_info.invalidation_event() {
            return Err(StoreMetadataError::InvalidBlob(event));
        }

        if blob_info.is_metadata_stored() {
            return Ok(false);
        }

        let verified_metadata_with_id = metadata.verify(&self.encoding_config)?;
        self.storage
            .put_verified_metadata(&verified_metadata_with_id)
            .context("unable to store metadata")?;

        self.metrics.metadata_stored_total.inc();

        Ok(true)
    }

    fn metadata_status(
        &self,
        blob_id: &BlobId,
    ) -> Result<StoredOnNodeStatus, RetrieveMetadataError> {
        match self.storage.has_metadata(blob_id) {
            Ok(true) => Ok(StoredOnNodeStatus::Stored),
            Ok(false) => Ok(StoredOnNodeStatus::Nonexistent),
            Err(err) => Err(RetrieveMetadataError::Internal(err.into())),
        }
    }

    fn retrieve_sliver(
        &self,
        blob_id: &BlobId,
        sliver_pair_index: SliverPairIndex,
        sliver_type: SliverType,
    ) -> Result<Sliver, RetrieveSliverError> {
        self.check_index(sliver_pair_index)?;

        let shard_storage = self.get_shard_for_sliver_pair(sliver_pair_index, blob_id)?;

        shard_storage
            .get_sliver(blob_id, sliver_type)
            .context("unable to retrieve sliver")?
            .ok_or(RetrieveSliverError::Unavailable)
            .inspect(|sliver| {
                metrics::with_label!(self.metrics.slivers_retrieved_total, sliver.r#type()).inc();
            })
    }

    fn store_sliver(
        &self,
        blob_id: &BlobId,
        sliver_pair_index: SliverPairIndex,
        sliver: &Sliver,
    ) -> Result<bool, StoreSliverError> {
        self.check_index(sliver_pair_index)?;

        let shard_storage = self.get_shard_for_sliver_pair(sliver_pair_index, blob_id)?;

        let shard_status = shard_storage
            .status()
            .context("Unable to retrieve shard status")?;

        if !shard_status.is_owned_by_node() {
            return Err(ShardNotAssigned(shard_storage.id(), self.current_epoch()).into());
        }

        if shard_storage
            .is_sliver_type_stored(blob_id, sliver.r#type())
            .context("database error when checking sliver existence")?
        {
            return Ok(false);
        }

        // Ensure we already received metadata for this sliver.
        let metadata = self
            .storage
            .get_metadata(blob_id)
            .context("database error when storing sliver")?
            .ok_or(StoreSliverError::MissingMetadata)?;

        sliver.verify(&self.encoding_config, metadata.as_ref())?;

        // Finally store the sliver in the appropriate shard storage.
        shard_storage
            .put_sliver(blob_id, sliver)
            .context("unable to store sliver")?;

        metrics::with_label!(self.metrics.slivers_stored_total, sliver.r#type()).inc();

        Ok(true)
    }

    async fn compute_storage_confirmation(
        &self,
        blob_id: &BlobId,
    ) -> Result<StorageConfirmation, ComputeStorageConfirmationError> {
        ensure!(
            self.storage
                .is_stored_at_all_shards(blob_id)
                .context("database error when storage status")?,
            ComputeStorageConfirmationError::NotFullyStored,
        );

        let confirmation = Confirmation::new(self.current_epoch(), *blob_id);
        let signed = sign_message(confirmation, self.protocol_key_pair.clone()).await?;

        self.metrics.storage_confirmations_issued_total.inc();

        Ok(StorageConfirmation::Signed(signed))
    }

    fn blob_status(&self, blob_id: &BlobId) -> Result<BlobStatus, BlobStatusError> {
        Ok(self
            .storage
            .get_blob_info(blob_id)
            .context("could not retrieve blob info")?
            .map(BlobStatus::from)
            .unwrap_or_default())
    }

    async fn verify_inconsistency_proof(
        &self,
        blob_id: &BlobId,
        inconsistency_proof: InconsistencyProof,
    ) -> Result<InvalidBlobIdAttestation, InconsistencyProofError> {
        let metadata = self.retrieve_metadata(blob_id)?;

        inconsistency_proof.verify(metadata.as_ref(), &self.encoding_config)?;

        let message = InvalidBlobIdMsg::new(self.current_epoch(), blob_id.to_owned());
        Ok(sign_message(message, self.protocol_key_pair.clone()).await?)
    }

    fn retrieve_recovery_symbol(
        &self,
        blob_id: &BlobId,
        sliver_pair_index: SliverPairIndex,
        sliver_type: SliverType,
        target_pair_index: SliverPairIndex,
    ) -> Result<RecoverySymbol<MerkleProof>, RetrieveSymbolError> {
        // Before touching the database, verify that the target_pair_index is possibly valid, and
        // not out of range. Checking the sliver_pair_index is done by retrieve_sliver.
        self.check_index(target_pair_index)?;

        let sliver = self.retrieve_sliver(blob_id, sliver_pair_index, sliver_type.orthogonal())?;

        let symbol_result = match sliver {
            Sliver::Primary(inner) => inner
                .recovery_symbol_for_sliver(target_pair_index, &self.encoding_config)
                .map(RecoverySymbol::Secondary),
            Sliver::Secondary(inner) => inner
                .recovery_symbol_for_sliver(target_pair_index, &self.encoding_config)
                .map(RecoverySymbol::Primary),
        };

        symbol_result.map_err(|error| match error {
            RecoverySymbolError::IndexTooLarge => {
                panic!("index validity must be checked above")
            }
            RecoverySymbolError::EncodeError(error) => {
                RetrieveSymbolError::Internal(anyhow!(error))
            }
        })
    }

    fn n_shards(&self) -> NonZeroU16 {
        self.encoding_config.n_shards()
    }

    fn health_info(&self) -> ServiceHealthInfo {
        ServiceHealthInfo {
            uptime: self.start_time.elapsed(),
            epoch: self.current_epoch(),
            public_key: self.public_key().clone(),
        }
    }

    fn sliver_status<A: EncodingAxis>(
        &self,
        blob_id: &BlobId,
        sliver_pair_index: SliverPairIndex,
    ) -> Result<StoredOnNodeStatus, RetrieveSliverError> {
        match self
            .get_shard_for_sliver_pair(sliver_pair_index, blob_id)?
            .is_sliver_stored::<A>(blob_id)
        {
            Ok(true) => Ok(StoredOnNodeStatus::Stored),
            Ok(false) => Ok(StoredOnNodeStatus::Nonexistent),
            Err(err) => Err(RetrieveSliverError::Internal(err.into())),
        }
    }

    fn sync_shard(
        &self,
        public_key: PublicKey,
        signed_request: SignedSyncShardRequest,
    ) -> Result<SyncShardResponse, SyncShardServiceError> {
        if !self.committee_service.is_walrus_storage_node(&public_key) {
            return Err(SyncShardServiceError::Unauthorized);
        }

        let sync_shard_msg = signed_request.verify_signature_and_get_message(&public_key)?;
        let request = sync_shard_msg.as_ref().contents();

        tracing::debug!("Sync shard request received: {:?}", request);

        // If the epoch of the requester should not be older than the current epoch of the node.
        // In a normal scenario, a storage node will never fetch shards from a future epoch.
        if request.epoch() != self.current_epoch() {
            return Err(InvalidEpochError {
                request_epoch: request.epoch(),
                server_epoch: self.current_epoch(),
            }
            .into());
        }

        self.storage
            .handle_sync_shard_request(request, self.current_epoch())
    }
}

#[tracing::instrument(skip_all, err)]
async fn sign_message<T, I>(
    message: T,
    signer: ProtocolKeyPair,
) -> Result<SignedMessage<T>, anyhow::Error>
where
    T: AsRef<ProtocolMessage<I>> + Serialize + Send + Sync + 'static,
{
    let signed = tokio::task::spawn_blocking(move || signer.sign_message(&message))
        .await
        .with_context(|| {
            format!(
                "unexpected error while signing a {}",
                std::any::type_name::<T>()
            )
        })?;

    Ok(signed)
}

#[cfg(test)]
mod tests {

    use std::{sync::OnceLock, time::Duration};

    use storage::{
        tests::{populated_storage, WhichSlivers, BLOB_ID, OTHER_SHARD_INDEX, SHARD_INDEX},
        ShardStatus,
    };
    use tokio::sync::{broadcast::Sender, Mutex};
    use walrus_core::{
        encoding::{Primary, Secondary, SliverData, SliverPair},
        messages::{SyncShardMsg, SyncShardRequest},
        test_utils::generate_config_metadata_and_valid_recovery_symbols,
    };
    use walrus_sdk::client::Client;
    use walrus_sui::{test_utils::EventForTesting, types::BlobRegistered};
    use walrus_test_utils::{async_param_test, Result as TestResult, WithTempDir};

    use super::*;
    use crate::test_utils::{StorageNodeHandle, TestCluster};

    const TIMEOUT: Duration = Duration::from_secs(1);
    const OTHER_BLOB_ID: BlobId = BlobId([247; 32]);
    const BLOB: &[u8] = &[
        0, 1, 255, 0, 2, 254, 0, 3, 253, 0, 4, 252, 0, 5, 251, 0, 6, 250, 0, 7, 249, 0, 8, 248,
    ];

    async fn storage_node_with_storage(storage: WithTempDir<Storage>) -> StorageNodeHandle {
        StorageNodeHandle::builder()
            .with_storage(storage)
            .build()
            .await
            .expect("storage node creation in setup should not fail")
    }

    mod get_storage_confirmation {
        use fastcrypto::traits::VerifyingKey;

        use super::*;

        #[tokio::test]
        async fn errs_if_no_shards_store_pairs() -> TestResult {
            let storage_node = storage_node_with_storage(populated_storage(&[(
                SHARD_INDEX,
                vec![
                    (BLOB_ID, WhichSlivers::Primary),
                    (OTHER_BLOB_ID, WhichSlivers::Both),
                ],
            )])?)
            .await;

            let err = storage_node
                .as_ref()
                .compute_storage_confirmation(&BLOB_ID)
                .await
                .expect_err("should fail");

            assert!(matches!(
                err,
                ComputeStorageConfirmationError::NotFullyStored
            ));

            Ok(())
        }

        #[tokio::test]
        async fn returns_confirmation_over_nodes_storing_the_pair() -> TestResult {
            let storage_node = storage_node_with_storage(populated_storage(&[
                (SHARD_INDEX, vec![(BLOB_ID, WhichSlivers::Both)]),
                (OTHER_SHARD_INDEX, vec![(BLOB_ID, WhichSlivers::Both)]),
            ])?)
            .await;

            let confirmation = storage_node
                .as_ref()
                .compute_storage_confirmation(&BLOB_ID)
                .await?;

            let StorageConfirmation::Signed(signed) = confirmation;

            storage_node
                .as_ref()
                .inner
                .protocol_key_pair
                .as_ref()
                .public()
                .verify(&signed.serialized_message, &signed.signature)
                .expect("message should be verifiable");

            let confirmation: Confirmation =
                bcs::from_bytes(&signed.serialized_message).expect("message should be decodable");

            assert_eq!(
                confirmation.as_ref().epoch(),
                storage_node.as_ref().inner.current_epoch()
            );
            assert_eq!(*confirmation.as_ref().contents(), BLOB_ID);

            Ok(())
        }
    }

    #[tokio::test]
    async fn services_slivers_for_shards_managed_according_to_committee() -> TestResult {
        let shard_for_node = ShardIndex(0);
        let node = StorageNodeHandle::builder()
            .with_system_event_provider(vec![BlobRegistered::for_testing(BLOB_ID).into()])
            .with_shard_assignment(&[shard_for_node])
            .with_node_started(true)
            .build()
            .await?;
        let n_shards = node.as_ref().inner.committee_service.get_shard_count();
        let sliver_pair_index = shard_for_node.to_pair_index(n_shards, &BLOB_ID);

        let result =
            node.as_ref()
                .retrieve_sliver(&BLOB_ID, sliver_pair_index, SliverType::Primary);

        assert!(matches!(result, Err(RetrieveSliverError::Unavailable)));

        Ok(())
    }

    #[tokio::test]
    async fn deletes_blob_data_on_invalid_blob_event() -> TestResult {
        let events = Sender::new(48);
        let node = StorageNodeHandle::builder()
            .with_storage(populated_storage(&[
                (SHARD_INDEX, vec![(BLOB_ID, WhichSlivers::Both)]),
                (OTHER_SHARD_INDEX, vec![(BLOB_ID, WhichSlivers::Both)]),
            ])?)
            .with_system_event_provider(events.clone())
            .with_node_started(true)
            .build()
            .await?;
        let storage = &node.as_ref().inner.storage;

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(storage.is_stored_at_all_shards(&BLOB_ID)?);
        events.send(InvalidBlobId::for_testing(BLOB_ID).into())?;

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!storage.is_stored_at_all_shards(&BLOB_ID)?);
        Ok(())
    }

    #[tokio::test]
    async fn returns_correct_blob_status() -> TestResult {
        let blob_event = BlobRegistered::for_testing(BLOB_ID);
        let node = StorageNodeHandle::builder()
            .with_system_event_provider(vec![blob_event.clone().into()])
            .with_shard_assignment(&[ShardIndex(0)])
            .with_node_started(true)
            .build()
            .await?;

        // Wait to make sure the event is received.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let BlobStatus::Permanent {
            end_epoch,
            status_event,
            is_certified,
            ..
        } = node.as_ref().blob_status(&BLOB_ID)?
        else {
            panic!("got nonexistent blob status")
        };

        assert!(!is_certified);
        assert_eq!(status_event, blob_event.event_id);
        assert_eq!(end_epoch, blob_event.end_epoch);

        Ok(())
    }

    #[tokio::test]
    async fn returns_correct_sliver_status() -> TestResult {
        let storage_node = storage_node_with_storage(populated_storage(&[
            (SHARD_INDEX, vec![(BLOB_ID, WhichSlivers::Both)]),
            (OTHER_SHARD_INDEX, vec![(BLOB_ID, WhichSlivers::Primary)]),
        ])?)
        .await;

        let pair_index =
            SHARD_INDEX.to_pair_index(storage_node.as_ref().inner.n_shards(), &BLOB_ID);
        let other_pair_index =
            OTHER_SHARD_INDEX.to_pair_index(storage_node.as_ref().inner.n_shards(), &BLOB_ID);

        check_sliver_status::<Primary>(&storage_node, pair_index, StoredOnNodeStatus::Stored)?;
        check_sliver_status::<Secondary>(&storage_node, pair_index, StoredOnNodeStatus::Stored)?;
        check_sliver_status::<Primary>(
            &storage_node,
            other_pair_index,
            StoredOnNodeStatus::Stored,
        )?;
        check_sliver_status::<Secondary>(
            &storage_node,
            other_pair_index,
            StoredOnNodeStatus::Nonexistent,
        )?;
        Ok(())
    }
    fn check_sliver_status<A: EncodingAxis>(
        storage_node: &StorageNodeHandle,
        pair_index: SliverPairIndex,
        expected: StoredOnNodeStatus,
    ) -> TestResult {
        let effective = storage_node
            .as_ref()
            .inner
            .sliver_status::<A>(&BLOB_ID, pair_index)?;
        assert_eq!(effective, expected);
        Ok(())
    }

    #[tokio::test]
    async fn returns_correct_metadata_status() -> TestResult {
        let (_ec, metadata, _idx, _rs) = generate_config_metadata_and_valid_recovery_symbols()?;
        let storage_node = set_up_node_with_metadata(metadata.clone().into_unverified()).await?;

        let metadata_status = storage_node
            .as_ref()
            .inner
            .metadata_status(metadata.blob_id())?;
        assert_eq!(metadata_status, StoredOnNodeStatus::Stored);
        Ok(())
    }

    #[tokio::test]
    async fn errs_for_empty_blob_status() -> TestResult {
        let node = StorageNodeHandle::builder()
            .with_system_event_provider(vec![])
            .with_shard_assignment(&[ShardIndex(0)])
            .with_node_started(true)
            .build()
            .await?;

        assert!(matches!(
            node.as_ref().blob_status(&BLOB_ID),
            Ok(BlobStatus::Nonexistent)
        ));

        Ok(())
    }

    async fn set_up_node_with_metadata(
        metadata: UnverifiedBlobMetadataWithId,
    ) -> anyhow::Result<StorageNodeHandle> {
        let blob_id = metadata.blob_id().to_owned();

        let shards = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9].map(ShardIndex::new);

        // create a storage node with a registered event for the blob id
        let node = StorageNodeHandle::builder()
            .with_system_event_provider(vec![BlobRegistered::for_testing(blob_id).into()])
            .with_shard_assignment(&shards)
            .with_node_started(true)
            .build()
            .await?;

        // make sure that the event is received by the node
        tokio::time::sleep(Duration::from_millis(50)).await;

        // store the metadata in the storage node
        node.as_ref().store_metadata(metadata)?;

        Ok(node)
    }

    mod inconsistency_proof {

        use fastcrypto::traits::VerifyingKey;
        use walrus_core::{
            inconsistency::PrimaryInconsistencyProof,
            merkle::Node,
            test_utils::generate_config_metadata_and_valid_recovery_symbols,
        };

        use super::*;

        #[tokio::test]
        async fn returns_err_for_invalid_proof() -> TestResult {
            let (_encoding_config, metadata, index, recovery_symbols) =
                generate_config_metadata_and_valid_recovery_symbols()?;

            // create invalid inconsistency proof
            let inconsistency_proof = InconsistencyProof::Primary(PrimaryInconsistencyProof::new(
                index,
                recovery_symbols,
            ));

            let blob_id = metadata.blob_id().to_owned();
            let node = set_up_node_with_metadata(metadata.into_unverified()).await?;

            let verification_result = node
                .as_ref()
                .verify_inconsistency_proof(&blob_id, inconsistency_proof)
                .await;

            // The sliver should be recoverable, i.e. the proof is invalid.
            assert!(verification_result.is_err());

            Ok(())
        }

        #[tokio::test]
        async fn returns_attestation_for_valid_proof() -> TestResult {
            let (_encoding_config, metadata, index, recovery_symbols) =
                generate_config_metadata_and_valid_recovery_symbols()?;

            // Change metadata
            let mut metadata = metadata.metadata().to_owned();
            metadata.hashes[0].primary_hash = Node::Digest([0; 32]);
            let blob_id = BlobId::from_sliver_pair_metadata(&metadata);
            let metadata = UnverifiedBlobMetadataWithId::new(blob_id, metadata);

            // create valid inconsistency proof
            let inconsistency_proof = InconsistencyProof::Primary(PrimaryInconsistencyProof::new(
                index,
                recovery_symbols,
            ));

            let node = set_up_node_with_metadata(metadata).await?;

            let attestation = node
                .as_ref()
                .verify_inconsistency_proof(&blob_id, inconsistency_proof)
                .await?;

            // The proof should be valid and we should receive a valid signature
            node.as_ref()
                .inner
                .protocol_key_pair
                .as_ref()
                .public()
                .verify(&attestation.serialized_message, &attestation.signature)?;

            let invalid_blob_msg: InvalidBlobIdMsg =
                bcs::from_bytes(&attestation.serialized_message)
                    .expect("message should be decodable");

            assert_eq!(
                invalid_blob_msg.as_ref().epoch(),
                node.as_ref().inner.current_epoch()
            );
            assert_eq!(*invalid_blob_msg.as_ref().contents(), blob_id);

            Ok(())
        }
    }

    #[derive(Debug)]
    struct EncodedBlob {
        pub config: EncodingConfig,
        pub pairs: Vec<SliverPair>,
        pub metadata: VerifiedBlobMetadataWithId,
    }

    impl EncodedBlob {
        fn new(blob: &[u8], config: EncodingConfig) -> EncodedBlob {
            let (pairs, metadata) = config
                .get_blob_encoder(blob)
                .expect("must be able to get encoder")
                .encode_with_metadata();

            EncodedBlob {
                pairs,
                metadata,
                config,
            }
        }

        fn blob_id(&self) -> &BlobId {
            self.metadata.blob_id()
        }

        fn assigned_sliver_pair(&self, shard: ShardIndex) -> &SliverPair {
            let pair_index = shard.to_pair_index(self.config.n_shards(), self.blob_id());
            self.pairs
                .iter()
                .find(|pair| pair.index() == pair_index)
                .expect("shard must be assigned at least 1 sliver")
        }
    }

    async fn store_at_shards<F>(
        blob: &EncodedBlob,
        cluster: &TestCluster,
        mut store_at_shard: F,
    ) -> TestResult
    where
        F: FnMut(&ShardIndex, SliverType) -> bool,
    {
        let nodes_and_shards: Vec<_> = cluster
            .nodes
            .iter()
            .flat_map(|node| std::iter::repeat(node).zip(node.storage_node.shards()))
            .collect();

        let mut metadata_stored = vec![];

        for (node, shard) in nodes_and_shards {
            if !metadata_stored.contains(&&node.public_key)
                && (store_at_shard(&shard, SliverType::Primary)
                    || store_at_shard(&shard, SliverType::Secondary))
            {
                node.client.store_metadata(&blob.metadata).await?;
                metadata_stored.push(&node.public_key);
            }

            let sliver_pair = blob.assigned_sliver_pair(shard);

            if store_at_shard(&shard, SliverType::Primary) {
                node.client
                    .store_sliver(blob.blob_id(), sliver_pair.index(), &sliver_pair.primary)
                    .await?;
            }

            if store_at_shard(&shard, SliverType::Secondary) {
                node.client
                    .store_sliver(blob.blob_id(), sliver_pair.index(), &sliver_pair.secondary)
                    .await?;
            }
        }

        Ok(())
    }

    // Prevent tests running simultaneously to avoid interferences or race conditions.
    fn global_test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(Mutex::default)
    }

    async fn cluster_with_partially_stored_blob<'a, F>(
        assignment: &[&[u16]],
        blob: &'a [u8],
        store_at_shard: F,
    ) -> TestResult<(TestCluster, Sender<ContractEvent>, EncodedBlob)>
    where
        F: FnMut(&ShardIndex, SliverType) -> bool,
    {
        let events = Sender::new(48);

        let cluster = {
            // Lock to avoid race conditions.
            let _lock = global_test_lock().lock().await;
            TestCluster::builder()
                .with_shard_assignment(assignment)
                .with_system_event_providers(events.clone())
                .build()
                .await?
        };

        let config = cluster.encoding_config();
        let blob_details = EncodedBlob::new(blob, config);

        events.send(BlobRegistered::for_testing(*blob_details.blob_id()).into())?;
        store_at_shards(&blob_details, &cluster, store_at_shard).await?;

        Ok((cluster, events, blob_details))
    }

    // Creates a test cluster with custom initial epoch and blobs that are already certified.
    async fn cluster_with_initial_epoch_and_certified_blob<'a>(
        assignment: &[&[u16]],
        blobs: &[&'a [u8]],
        initial_epoch: Epoch,
    ) -> TestResult<(TestCluster, Sender<ContractEvent>, Vec<EncodedBlob>)> {
        let events = Sender::new(48);

        let cluster = {
            // Lock to avoid race conditions.
            let _lock = global_test_lock().lock().await;
            TestCluster::builder()
                .with_shard_assignment(assignment)
                .with_system_event_providers(events.clone())
                .with_initial_epoch(initial_epoch)
                .build()
                .await?
        };

        let config = cluster.encoding_config();
        let mut details = Vec::new();
        for blob in blobs {
            let blob_details = EncodedBlob::new(blob, config.clone());
            // Note: register and certify the blob are always using epoch 0.
            events.send(BlobRegistered::for_testing(*blob_details.blob_id()).into())?;
            store_at_shards(&blob_details, &cluster, |_, _| true).await?;
            events.send(BlobCertified::for_testing(*blob_details.blob_id()).into())?;
            details.push(blob_details);
        }

        Ok((cluster, events, details))
    }

    /// Creates a test cluster with custom initial epoch and blobs that are partially stored
    /// in shard 0.
    ///
    /// The function is created for testing shard syncing/recovery. So for blobs that are
    /// not stored in shard 0, it also won't receive a certified event.
    async fn cluster_with_partially_stored_blobs_in_shard_0<'a, F>(
        assignment: &[&[u16]],
        blobs: &[&'a [u8]],
        initial_epoch: Epoch,
        mut blob_index_store_at_shard_0: F,
    ) -> TestResult<(TestCluster, Vec<EncodedBlob>)>
    where
        F: FnMut(usize) -> bool,
    {
        // Node 0 must contain shard 0.
        assert!(assignment[0].contains(&0));

        // Create event providers for each node.
        let node_0_events = Sender::new(48);
        let all_other_node_events = Sender::new(48);
        let event_providers = vec![node_0_events.clone(); 1]
            .into_iter()
            .chain(vec![all_other_node_events.clone(); assignment.len() - 1].into_iter())
            .collect::<Vec<_>>();

        let cluster = {
            // Lock to avoid race conditions.
            let _lock = global_test_lock().lock().await;
            TestCluster::builder()
                .with_shard_assignment(assignment)
                .with_individual_system_event_providers(&event_providers)
                .with_initial_epoch(initial_epoch)
                .build()
                .await?
        };

        let config = cluster.encoding_config();
        let mut details = Vec::new();
        for (i, blob) in blobs.iter().enumerate() {
            let blob_details = EncodedBlob::new(blob, config.clone());
            // Note: register and certify the blob are always using epoch 0.
            node_0_events.send(BlobRegistered::for_testing(*blob_details.blob_id()).into())?;
            all_other_node_events
                .send(BlobRegistered::for_testing(*blob_details.blob_id()).into())?;

            if blob_index_store_at_shard_0(i) {
                store_at_shards(&blob_details, &cluster, |_, _| true).await?;
                node_0_events.send(BlobCertified::for_testing(*blob_details.blob_id()).into())?;
            } else {
                // Don't certify the blob if it's not stored in shard 0.
                store_at_shards(&blob_details, &cluster, |shard_index, _| {
                    shard_index != &ShardIndex(0)
                })
                .await?;
            }

            all_other_node_events
                .send(BlobCertified::for_testing(*blob_details.blob_id()).into())?;
            details.push(blob_details);
        }

        Ok((cluster, details))
    }

    #[tokio::test]
    async fn retrieves_metadata_from_other_nodes_on_certified_blob_event() -> TestResult {
        let shards: &[&[u16]] = &[&[1], &[0, 2, 3, 4]];

        let (cluster, events, blob) =
            cluster_with_partially_stored_blob(shards, BLOB, |shard, _| shard.get() != 1).await?;

        let node_client = cluster.client(0);

        node_client
            .get_metadata(blob.blob_id())
            .await
            .expect_err("metadata should not yet be available");

        events.send(BlobCertified::for_testing(*blob.blob_id()).into())?;

        let synced_metadata = retry_until_success_or_timeout(TIMEOUT, || {
            node_client.get_and_verify_metadata(blob.blob_id(), &blob.config)
        })
        .await
        .expect("metadata should be available at some point after being certified");

        assert_eq!(synced_metadata, blob.metadata);

        Ok(())
    }

    async_param_test! {
        recovers_sliver_from_other_nodes_on_certified_blob_event -> TestResult: [
            primary: (SliverType::Primary),
            secondary: (SliverType::Secondary),
        ]
    }
    async fn recovers_sliver_from_other_nodes_on_certified_blob_event(
        sliver_type: SliverType,
    ) -> TestResult {
        let shards: &[&[u16]] = &[&[1], &[0, 2, 3, 4, 5, 6]];
        let test_shard = ShardIndex(1);

        let (cluster, events, blob) =
            cluster_with_partially_stored_blob(shards, BLOB, |&shard, _| shard != test_shard)
                .await?;
        let node_client = cluster.client(0);

        let pair_to_sync = blob.assigned_sliver_pair(test_shard);

        node_client
            .get_sliver_by_type(blob.blob_id(), pair_to_sync.index(), sliver_type)
            .await
            .expect_err("sliver should not yet be available");

        events.send(BlobCertified::for_testing(*blob.blob_id()).into())?;

        let synced_sliver = retry_until_success_or_timeout(TIMEOUT, || {
            node_client.get_sliver_by_type(blob.blob_id(), pair_to_sync.index(), sliver_type)
        })
        .await
        .expect("sliver should be available at some point after being certified");

        let expected: Sliver = match sliver_type {
            SliverType::Primary => pair_to_sync.primary.clone().into(),
            SliverType::Secondary => pair_to_sync.secondary.clone().into(),
        };
        assert_eq!(synced_sliver, expected);

        Ok(())
    }

    #[tokio::test]
    async fn recovers_slivers_for_multiple_shards_from_other_nodes() -> TestResult {
        let shards: &[&[u16]] = &[&[1, 6], &[0, 2, 3, 4, 5]];
        let own_shards = [ShardIndex(1), ShardIndex(6)];

        let (cluster, events, blob) =
            cluster_with_partially_stored_blob(shards, BLOB, |shard, _| {
                !own_shards.contains(shard)
            })
            .await?;
        let node_client = cluster.client(0);

        events.send(BlobCertified::for_testing(*blob.blob_id()).into())?;

        for shard in own_shards {
            let synced_sliver_pair =
                expect_sliver_pair_stored_before_timeout(&blob, node_client, shard, TIMEOUT).await;
            let expected = blob.assigned_sliver_pair(shard);

            assert_eq!(
                synced_sliver_pair, *expected,
                "invalid sliver pair for {shard}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn recovers_sliver_from_own_shards() -> TestResult {
        let shards: &[&[u16]] = &[&[0, 1, 2, 3, 4, 5], &[6]];
        let shard_under_test = ShardIndex(0);

        // Store with all except the shard under test.
        let (cluster, events, blob) =
            cluster_with_partially_stored_blob(shards, BLOB, |&shard, _| shard != shard_under_test)
                .await?;
        let node_client = cluster.client(0);

        events.send(BlobCertified::for_testing(*blob.blob_id()).into())?;

        let synced_sliver_pair =
            expect_sliver_pair_stored_before_timeout(&blob, node_client, shard_under_test, TIMEOUT)
                .await;
        let expected = blob.assigned_sliver_pair(shard_under_test);

        assert_eq!(synced_sliver_pair, *expected,);

        Ok(())
    }

    async_param_test! {
        recovers_sliver_from_only_symbols_of_one_type -> TestResult: [
            primary: (SliverType::Primary),
            secondary: (SliverType::Secondary),
        ]
    }
    async fn recovers_sliver_from_only_symbols_of_one_type(
        sliver_type_to_store: SliverType,
    ) -> TestResult {
        let shards: &[&[u16]] = &[&[0], &[1, 2, 3, 4, 5, 6]];

        // Store only slivers of type `sliver_type_to_store`.
        let (cluster, events, blob) =
            cluster_with_partially_stored_blob(shards, BLOB, |_, sliver_type| {
                sliver_type == sliver_type_to_store
            })
            .await?;

        events.send(BlobCertified::for_testing(*blob.blob_id()).into())?;

        for (node_index, shards) in shards.iter().enumerate() {
            let node_client = cluster.client(node_index);

            for shard in shards.iter() {
                let expected = blob.assigned_sliver_pair(shard.into());
                let synced = expect_sliver_pair_stored_before_timeout(
                    &blob,
                    node_client,
                    shard.into(),
                    TIMEOUT,
                )
                .await;

                assert_eq!(synced, *expected,);
            }
        }

        Ok(())
    }

    #[tokio::test(start_paused = false)]
    async fn recovers_sliver_from_a_small_set() -> TestResult {
        let shards: &[&[u16]] = &[&[0], &(1..=6).collect::<Vec<_>>()];
        let store_secondary_at: Vec<_> = ShardIndex::range(0..5).collect();

        // Store only a few secondary slivers.
        let (cluster, events, blob) =
            cluster_with_partially_stored_blob(shards, BLOB, |shard, sliver_type| {
                sliver_type == SliverType::Secondary && store_secondary_at.contains(shard)
            })
            .await?;

        events.send(BlobCertified::for_testing(*blob.blob_id()).into())?;

        for (node_index, shards) in shards.iter().enumerate() {
            let node_client = cluster.client(node_index);

            for shard in shards.iter() {
                let expected = blob.assigned_sliver_pair(shard.into());
                let synced = expect_sliver_pair_stored_before_timeout(
                    &blob,
                    node_client,
                    shard.into(),
                    Duration::from_secs(10),
                )
                .await;

                assert_eq!(synced, *expected,);
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn does_not_advance_cursor_past_incomplete_blobs() -> TestResult {
        let shards: &[&[u16]] = &[&[1, 6], &[0, 2, 3, 4, 5]];
        let own_shards = [ShardIndex(1), ShardIndex(6)];

        let blob1 = (0..80u8).collect::<Vec<_>>();
        let blob2 = (80..160u8).collect::<Vec<_>>();
        let blob3 = (160..255u8).collect::<Vec<_>>();

        let store_at_other_node_fn = |shard: &ShardIndex, _| !own_shards.contains(shard);
        let (cluster, events, blob1_details) =
            cluster_with_partially_stored_blob(shards, &blob1, store_at_other_node_fn).await?;
        events.send(BlobCertified::for_testing(*blob1_details.blob_id()).into())?;

        let node_client = cluster.client(0);
        let config = &blob1_details.config;

        // Send events that some unobserved blob has been certified.
        let blob2_details = EncodedBlob::new(&blob2, config.clone());
        let blob2_registered_event = BlobRegistered::for_testing(*blob2_details.blob_id());
        events.send(blob2_registered_event.clone().into())?;

        // The node should not be able to advance past the following event.
        events.send(BlobCertified::for_testing(*blob2_details.blob_id()).into())?;

        // Register and store the second blob
        let blob3_details = EncodedBlob::new(&blob3, config.clone());
        events.send(BlobRegistered::for_testing(*blob3_details.blob_id()).into())?;
        store_at_shards(&blob3_details, &cluster, store_at_other_node_fn).await?;
        events.send(BlobCertified::for_testing(*blob3_details.blob_id()).into())?;

        // All shards for blobs 1 and 3 should be synced by the node.
        for blob_details in [blob1_details, blob3_details] {
            for shard in own_shards {
                let synced_sliver_pair = expect_sliver_pair_stored_before_timeout(
                    &blob_details,
                    node_client,
                    shard,
                    TIMEOUT,
                )
                .await;
                let expected = blob_details.assigned_sliver_pair(shard);

                assert_eq!(
                    synced_sliver_pair, *expected,
                    "invalid sliver pair for {shard}"
                );
            }
        }

        // The cursor should not have moved beyond that of blob2 registration, since blob2 is yet
        // to be synced.
        let latest_cursor = cluster.nodes[0]
            .storage_node
            .inner
            .storage
            .get_event_cursor()?
            .map(|(_, cursor)| cursor);
        assert_eq!(latest_cursor, Some(blob2_registered_event.event_id));

        Ok(())
    }

    async fn expect_sliver_pair_stored_before_timeout(
        blob: &EncodedBlob,
        node_client: &Client,
        shard: ShardIndex,
        timeout: Duration,
    ) -> SliverPair {
        let (primary, secondary) = tokio::join!(
            expect_sliver_stored_before_timeout::<Primary>(blob, node_client, shard, timeout,),
            expect_sliver_stored_before_timeout::<Secondary>(blob, node_client, shard, timeout,)
        );

        SliverPair { primary, secondary }
    }

    async fn expect_sliver_stored_before_timeout<A: EncodingAxis>(
        blob: &EncodedBlob,
        node_client: &Client,
        shard: ShardIndex,
        timeout: Duration,
    ) -> SliverData<A> {
        retry_until_success_or_timeout(timeout, || {
            let pair_to_sync = blob.assigned_sliver_pair(shard);
            node_client.get_sliver::<A>(blob.blob_id(), pair_to_sync.index())
        })
        .await
        .expect("sliver should be available at some point after being certified")
    }

    /// Retries until success or a timeout, returning the last result.
    async fn retry_until_success_or_timeout<F, Fut, T, E>(
        duration: Duration,
        mut func_to_retry: F,
    ) -> Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let mut last_result = None;

        let _ = tokio::time::timeout(duration, async {
            loop {
                last_result = Some(func_to_retry().await);
                if last_result.as_ref().unwrap().is_ok() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;

        last_result.expect("function to have completed at least once")
    }

    #[tokio::test]
    async fn skip_storing_metadata_if_already_stored() -> TestResult {
        let (cluster, _, blob) =
            cluster_with_partially_stored_blob(&[&[0]], BLOB, |_, _| true).await?;

        let is_newly_stored = cluster.nodes[0]
            .storage_node
            .store_metadata(blob.metadata.into_unverified())?;

        assert!(!is_newly_stored);

        Ok(())
    }

    #[tokio::test]
    async fn skip_storing_sliver_if_already_stored() -> TestResult {
        let (cluster, _, blob) =
            cluster_with_partially_stored_blob(&[&[0]], BLOB, |_, _| true).await?;

        let assigned_sliver_pair = blob.assigned_sliver_pair(ShardIndex(0));
        let is_newly_stored = cluster.nodes[0].storage_node.store_sliver(
            blob.blob_id(),
            assigned_sliver_pair.index(),
            &Sliver::Primary(assigned_sliver_pair.primary.clone()),
        )?;

        assert!(!is_newly_stored);

        Ok(())
    }

    // Tests the basic `sync_shard` API.
    #[tokio::test]
    async fn sync_shard_node_api_success() -> TestResult {
        let (cluster, _, blob_detail) =
            cluster_with_initial_epoch_and_certified_blob(&[&[0], &[1]], &[BLOB], 2).await?;

        let blob_id = *blob_detail[0].blob_id();

        // Tests successful sync shard operation.
        let status = cluster.nodes[0]
            .client
            .sync_shard::<Primary>(
                ShardIndex(0),
                blob_id,
                10,
                2,
                &cluster.nodes[0].as_ref().inner.protocol_key_pair,
            )
            .await;
        assert!(status.is_ok(), "Unexpected sync shard error: {:?}", status);

        let SyncShardResponse::V1(response) = status.unwrap();
        assert_eq!(response.len(), 1);
        assert_eq!(response[0].0, blob_id);
        assert_eq!(
            response[0].1,
            Sliver::Primary(
                cluster.nodes[0]
                    .storage_node
                    .inner
                    .storage
                    .shard_storage(ShardIndex(0))
                    .unwrap()
                    .get_primary_sliver(&blob_id)
                    .unwrap()
                    .unwrap()
            )
        );

        Ok(())
    }

    // Tests that the `sync_shard` API does not return blobs certified after the requested epoch.
    #[tokio::test]
    async fn sync_shard_do_not_send_certified_after_requested_epoch() -> TestResult {
        // Note that the blobs are certified in epoch 0.
        let (cluster, _, blob_detail) =
            cluster_with_initial_epoch_and_certified_blob(&[&[0], &[1]], &[BLOB], 1).await?;

        let blob_id = *blob_detail[0].blob_id();

        let status = cluster.nodes[0]
            .client
            .sync_shard::<Primary>(
                ShardIndex(0),
                blob_id,
                10,
                1,
                &cluster.nodes[0].as_ref().inner.protocol_key_pair,
            )
            .await;
        assert!(status.is_ok(), "Unexpected sync shard error: {:?}", status);

        let SyncShardResponse::V1(response) = status.unwrap();
        assert_eq!(response.len(), 0);

        Ok(())
    }

    // Tests unauthorized sync shard operation (requester is not a storage node in Walrus).
    #[tokio::test]
    async fn sync_shard_node_api_unauthorized_error() -> TestResult {
        let (cluster, _, _) =
            cluster_with_initial_epoch_and_certified_blob(&[&[0], &[1]], &[BLOB], 1).await?;

        let response: Result<SyncShardResponse, walrus_sdk::error::NodeError> = cluster.nodes[0]
            .client
            .sync_shard::<Primary>(ShardIndex(0), BLOB_ID, 10, 0, &ProtocolKeyPair::generate())
            .await;
        assert!(matches!(
            response,
            Err(err) if err.to_string().contains(
                            "The client is not authorized to perform sync shard operation"
                        )
        ));

        Ok(())
    }

    // Tests signed SyncShardRequest verification error.
    #[tokio::test]
    async fn sync_shard_node_api_request_verification_error() -> TestResult {
        let (cluster, _, _) =
            cluster_with_initial_epoch_and_certified_blob(&[&[0], &[1]], &[BLOB], 1).await?;

        let request = SyncShardRequest::new(ShardIndex(0), SliverType::Primary, BLOB_ID, 10, 1);
        let sync_shard_msg = SyncShardMsg::new(1, request);
        let signed_request = cluster.nodes[0]
            .as_ref()
            .inner
            .protocol_key_pair
            .sign_message(&sync_shard_msg);

        let result = cluster.nodes[0].storage_node.sync_shard(
            cluster.nodes[1]
                .as_ref()
                .inner
                .protocol_key_pair
                .0
                .public()
                .clone(),
            signed_request,
        );
        assert!(matches!(
            result,
            Err(SyncShardServiceError::MessageVerificationError(..))
        ));

        Ok(())
    }

    // Tests SyncShardRequest with wrong epoch.
    async_param_test! {
        sync_shard_node_api_invalid_epoch -> TestResult: [
            too_old: (10, 1, "Invalid epoch. Client epoch: 1. Server epoch: 10"),
            too_new: (10, 11, "Invalid epoch. Client epoch: 11. Server epoch: 10"),
        ]
    }
    async fn sync_shard_node_api_invalid_epoch(
        cluster_epoch: Epoch,
        requester_epoch: Epoch,
        error_message: &str,
    ) -> TestResult {
        // Creates a cluster with initial epoch set to 10.
        let (cluster, _, blob_detail) =
            cluster_with_initial_epoch_and_certified_blob(&[&[0], &[1]], &[BLOB], cluster_epoch)
                .await?;

        // Requests a shard from epoch 0.
        let status = cluster.nodes[0]
            .client
            .sync_shard::<Primary>(
                ShardIndex(0),
                *blob_detail[0].blob_id(),
                10,
                requester_epoch,
                &cluster.nodes[0].as_ref().inner.protocol_key_pair,
            )
            .await;

        assert!(matches!(
            status,
            Err(err) if err.service_error().is_some() &&
                err.to_string().contains(
                    error_message
                )
        ));

        Ok(())
    }

    #[tokio::test]
    async fn can_read_locked_shard() -> TestResult {
        let (cluster, _, blob) =
            cluster_with_partially_stored_blob(&[&[0]], BLOB, |_, _| true).await?;

        cluster.nodes[0]
            .storage_node
            .inner
            .storage
            .shard_storage(ShardIndex(0))
            .unwrap()
            .lock_shard_for_epoch_change()
            .expect("Lock shard failed.");

        let sliver = cluster.nodes[0]
            .storage_node
            .retrieve_sliver(blob.blob_id(), SliverPairIndex(0), SliverType::Primary)
            .expect("Sliver retrieval failed.");

        assert_eq!(
            blob.assigned_sliver_pair(ShardIndex(0)).primary,
            sliver.try_into().expect("Sliver conversion failed.")
        );

        Ok(())
    }

    #[tokio::test]
    async fn reject_writes_if_shard_is_locked_in_node() -> TestResult {
        let (cluster, _, blob) =
            cluster_with_partially_stored_blob(&[&[0]], BLOB, |_, _| true).await?;

        cluster.nodes[0]
            .storage_node
            .inner
            .storage
            .shard_storage(ShardIndex(0))
            .unwrap()
            .lock_shard_for_epoch_change()
            .expect("Lock shard failed.");

        let assigned_sliver_pair = blob.assigned_sliver_pair(ShardIndex(0));
        assert!(matches!(
            cluster.nodes[0].storage_node.store_sliver(
                blob.blob_id(),
                assigned_sliver_pair.index(),
                &Sliver::Primary(assigned_sliver_pair.primary.clone()),
            ),
            Err(StoreSliverError::ShardNotAssigned(..))
        ));

        Ok(())
    }

    #[tokio::test]
    async fn compute_storage_confirmation_ignore_locked_shard() -> TestResult {
        let (cluster, _, blob) =
            cluster_with_partially_stored_blob(&[&[0, 1, 2]], BLOB, |index, _| index.get() != 0)
                .await?;

        assert!(matches!(
            cluster.nodes[0]
                .storage_node
                .compute_storage_confirmation(blob.blob_id())
                .await,
            Err(ComputeStorageConfirmationError::NotFullyStored)
        ));

        cluster.nodes[0]
            .storage_node
            .inner
            .storage
            .shard_storage(ShardIndex(0))
            .unwrap()
            .lock_shard_for_epoch_change()
            .expect("Lock shard failed.");

        assert!(cluster.nodes[0]
            .storage_node
            .compute_storage_confirmation(blob.blob_id())
            .await
            .is_ok());

        Ok(())
    }

    // The common setup for shard sync tests.
    //   - Initial cluster with 2 nodes. Shard 0 in node 0 and shard 1 in node 1.
    //   - 23 blobs created and certified in node 0.
    //   - Create a new shard in node 1 with shard index 0 to test sync.
    async fn setup_cluster_for_shard_sync_tests(
    ) -> TestResult<(TestCluster, Vec<EncodedBlob>, Arc<ShardStorage>)> {
        let blobs: Vec<[u8; 32]> = (1..24).map(|i| [i; 32]).collect();
        let blobs: Vec<_> = blobs.iter().map(|b| &b[..]).collect();
        let (cluster, _, blob_details) =
            cluster_with_initial_epoch_and_certified_blob(&[&[0], &[1]], &blobs, 2).await?;

        // Makes storage inner mutable so that we can manually add another shard to node 1.
        let node_inner = unsafe {
            &mut *(Arc::as_ptr(&cluster.nodes[1].storage_node.inner) as *mut StorageNodeInner)
        };
        node_inner.storage.create_storage_for_shard(ShardIndex(0))?;
        let shard_storage_dst = node_inner.storage.shard_storage(ShardIndex(0)).unwrap();
        shard_storage_dst.update_status_in_test(ShardStatus::None)?;

        Ok((cluster, blob_details, shard_storage_dst.clone()))
    }

    // Checks that all primary and secondary slivers match the original encoding of the blobs.
    fn check_all_blobs_are_synced(
        blob_details: &[EncodedBlob],
        shard_storage_dst: &ShardStorage,
    ) -> anyhow::Result<()> {
        blob_details.iter().try_for_each(|details| {
            let blob_id = *details.blob_id();
            let Sliver::Primary(dst_primary) = shard_storage_dst
                .get_sliver(&blob_id, SliverType::Primary)
                .unwrap()
                .unwrap()
            else {
                panic!("Must get primary sliver");
            };
            let Sliver::Secondary(dst_secondary) = shard_storage_dst
                .get_sliver(&blob_id, SliverType::Secondary)
                .unwrap()
                .unwrap()
            else {
                panic!("Must get secondary sliver");
            };

            assert_eq!(
                details.assigned_sliver_pair(ShardIndex(0)),
                &SliverPair {
                    primary: dst_primary,
                    secondary: dst_secondary,
                }
            );
            Ok(())
        })
    }

    async fn wait_for_shard_in_active_state(shard_storage: &ShardStorage) -> TestResult {
        // Waits for the shard to be synced.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let status = shard_storage.status().unwrap();
                if status == ShardStatus::Active {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await?;

        Ok(())
    }

    // Tests shard transfer only using shard sync functionality.
    #[tokio::test]
    async fn sync_shard_complete_transfer() -> TestResult {
        telemetry_subscribers::init_for_testing();

        let (cluster, blob_details, shard_storage_dst) =
            setup_cluster_for_shard_sync_tests().await?;

        let shard_storage_src = cluster.nodes[0]
            .storage_node
            .inner
            .storage
            .shard_storage(ShardIndex(0))
            .unwrap();

        assert_eq!(blob_details.len(), 23);
        assert_eq!(shard_storage_src.sliver_count(SliverType::Primary), 23);
        assert_eq!(shard_storage_src.sliver_count(SliverType::Secondary), 23);
        assert_eq!(shard_storage_dst.sliver_count(SliverType::Primary), 0);
        assert_eq!(shard_storage_dst.sliver_count(SliverType::Secondary), 0);

        // Starts the shard syncing process.
        cluster.nodes[1]
            .storage_node
            ._shard_sync_handler
            .start_new_shard_sync(ShardIndex(0))
            .await?;

        // Waits for the shard to be synced.
        wait_for_shard_in_active_state(&shard_storage_dst).await?;

        assert_eq!(shard_storage_dst.sliver_count(SliverType::Primary), 23);
        assert_eq!(shard_storage_dst.sliver_count(SliverType::Secondary), 23);

        assert_eq!(blob_details.len(), 23);

        // Checks that the shard is completely migrated.
        check_all_blobs_are_synced(&blob_details, &shard_storage_dst)?;

        Ok(())
    }

    async fn setup_shard_recovery_test_cluster<F>(
        blob_index_store_at_shard_0: F,
    ) -> TestResult<(TestCluster, Vec<EncodedBlob>)>
    where
        F: FnMut(usize) -> bool,
    {
        let blobs: Vec<[u8; 32]> = (1..24).map(|i| [i; 32]).collect();
        let blobs: Vec<_> = blobs.iter().map(|b| &b[..]).collect();
        let (cluster, blob_details) = cluster_with_partially_stored_blobs_in_shard_0(
            &[&[0], &[1, 2, 3, 4], &[5, 6, 7, 8, 9]],
            &blobs,
            2,
            blob_index_store_at_shard_0,
        )
        .await?;

        Ok((cluster, blob_details))
    }

    // Tests shard transfer completely using shard recovery functionality.
    #[tokio::test]
    async fn sync_shard_shard_recovery() -> TestResult {
        telemetry_subscribers::init_for_testing();

        let (cluster, blob_details) = setup_shard_recovery_test_cluster(|_| false).await?;

        // Make sure that all blobs are not certified in node 0.
        for blob_detail in blob_details.iter() {
            let blob_info = cluster.nodes[0]
                .storage_node
                .inner
                .storage
                .get_blob_info(blob_detail.blob_id());
            assert!(matches!(
                blob_info.unwrap().unwrap().to_blob_status(),
                BlobStatus::Permanent {
                    is_certified: false,
                    ..
                }
            ));
        }

        let node_inner = unsafe {
            &mut *(Arc::as_ptr(&cluster.nodes[1].storage_node.inner) as *mut StorageNodeInner)
        };
        node_inner.storage.create_storage_for_shard(ShardIndex(0))?;
        let shard_storage_dst = node_inner.storage.shard_storage(ShardIndex(0)).unwrap();
        shard_storage_dst.update_status_in_test(ShardStatus::None)?;

        cluster.nodes[1]
            .storage_node
            ._shard_sync_handler
            .start_new_shard_sync(ShardIndex(0))
            .await?;
        wait_for_shard_in_active_state(shard_storage_dst).await?;
        check_all_blobs_are_synced(&blob_details, shard_storage_dst)?;

        Ok(())
    }

    // Tests shard transfer partially using shard recovery functionality and partially using shard
    // sync.
    // This test also tests that no missing blobs after sync completion.
    #[tokio::test]
    async fn sync_shard_partial_recovery() -> TestResult {
        telemetry_subscribers::init_for_testing();

        let skip_stored_blob_index: [usize; 12] = [3, 4, 5, 9, 10, 11, 15, 18, 19, 20, 21, 22];
        let (cluster, blob_details) = setup_shard_recovery_test_cluster(|blob_index| {
            !skip_stored_blob_index.contains(&blob_index)
        })
        .await?;

        // Make sure that blobs in `sync_shard_partial_recovery` are not certified in node 0.
        for i in skip_stored_blob_index {
            let blob_info = cluster.nodes[0]
                .storage_node
                .inner
                .storage
                .get_blob_info(blob_details[i].blob_id());
            assert!(matches!(
                blob_info.unwrap().unwrap().to_blob_status(),
                BlobStatus::Permanent {
                    is_certified: false,
                    ..
                }
            ));
        }

        let node_inner = unsafe {
            &mut *(Arc::as_ptr(&cluster.nodes[1].storage_node.inner) as *mut StorageNodeInner)
        };
        node_inner.storage.create_storage_for_shard(ShardIndex(0))?;
        let shard_storage_dst = node_inner.storage.shard_storage(ShardIndex(0)).unwrap();
        shard_storage_dst.update_status_in_test(ShardStatus::None)?;

        cluster.nodes[1]
            .storage_node
            ._shard_sync_handler
            .start_new_shard_sync(ShardIndex(0))
            .await?;
        wait_for_shard_in_active_state(shard_storage_dst).await?;
        check_all_blobs_are_synced(&blob_details, shard_storage_dst)?;

        Ok(())
    }

    #[cfg(msim)]
    mod failure_injection_tests {
        use sui_macros::{
            clear_fail_point,
            register_fail_point_arg,
            register_fail_point_if,
            sim_test,
        };
        use walrus_test_utils::simtest_param_test;

        use super::*;

        async fn wait_until_no_sync_tasks(shard_sync_handler: &ShardSyncHandler) -> TestResult {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if shard_sync_handler.current_sync_task_count().await == 0 {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
            .await?;
            Ok(())
        }

        // Tests that shard sync can be resumed from a specific progress point.
        // `break_index` is the index of the blob to break the sync process.
        // Note that currently, each sync batch contains 10 blobs. So testing various interesting
        // places to break the sync process.
        // TODO(#705): make shard sync parameters configurable.
        simtest_param_test! {
            simtest_sync_shard_start_from_progress -> TestResult: [
                primary1: (1, SliverType::Primary),
                primary5: (5, SliverType::Primary),
                primary10: (10, SliverType::Primary),
                primary11: (11, SliverType::Primary),
                primary15: (15, SliverType::Primary),
                primary23: (23, SliverType::Primary),
                secondary1: (1, SliverType::Secondary),
                secondary5: (5, SliverType::Secondary),
                secondary10: (10, SliverType::Secondary),
                secondary11: (11, SliverType::Secondary),
                secondary15: (15, SliverType::Secondary),
                secondary23: (23, SliverType::Secondary),
            ]
        }
        async fn simtest_sync_shard_start_from_progress(
            break_index: u64,
            sliver_type: SliverType,
        ) -> TestResult {
            telemetry_subscribers::init_for_testing();

            let (cluster, blob_details, shard_storage_dst) =
                setup_cluster_for_shard_sync_tests().await?;

            register_fail_point_arg(
                "fail_point_fetch_sliver",
                move || -> Option<(SliverType, u64)> { Some((sliver_type, break_index)) },
            );

            // Starts the shard syncing process in the new shard, which will fail at the specified
            // break index.
            cluster.nodes[1]
                .storage_node
                ._shard_sync_handler
                .start_new_shard_sync(ShardIndex(0))
                .await?;

            // Waits for the shard sync process to stop.
            wait_until_no_sync_tasks(&cluster.nodes[1].storage_node._shard_sync_handler).await?;

            // Check that shard sync process is not finished.
            let shard_storage_src = cluster.nodes[0]
                .storage_node
                .inner
                .storage
                .shard_storage(ShardIndex(0))
                .unwrap();
            assert!(
                shard_storage_dst.sliver_count(SliverType::Primary)
                    < shard_storage_src.sliver_count(SliverType::Primary)
                    || shard_storage_dst.sliver_count(SliverType::Secondary)
                        < shard_storage_src.sliver_count(SliverType::Secondary)
            );

            clear_fail_point("fail_point_fetch_sliver");

            // restart the shard syncing process, to simulate a reboot.
            cluster.nodes[1]
                .storage_node
                ._shard_sync_handler
                .restart_syncs()
                .await?;

            // Waits for the shard to be synced.
            wait_until_no_sync_tasks(&cluster.nodes[1].storage_node._shard_sync_handler).await?;

            // Checks that the shard is completely migrated.
            check_all_blobs_are_synced(&blob_details, &shard_storage_dst)?;

            Ok(())
        }

        // Tests that there is a discrepancy between the source and destination shards in terms
        // of certified blobs. If the source doesn't return any blobs, the destination should
        // finish the sync process.
        #[sim_test]
        async fn simtest_sync_shard_src_return_empty() -> TestResult {
            telemetry_subscribers::init_for_testing();

            let (cluster, _blob_details, _shard_storage_dst) =
                setup_cluster_for_shard_sync_tests().await?;

            register_fail_point_if("fail_point_sync_shard_return_empty", || true);

            // Starts the shard syncing process in the new shard, which will fail at the specified
            // break index.
            cluster.nodes[1]
                .storage_node
                ._shard_sync_handler
                .start_new_shard_sync(ShardIndex(0))
                .await?;

            // Waits for the shard sync process to stop.
            wait_until_no_sync_tasks(&cluster.nodes[1].storage_node._shard_sync_handler).await?;

            Ok(())
        }

        // Tests crash recovery of shard transfer partially using shard recovery functionality
        // and partially using shard sync.
        simtest_param_test! {
            simtest_sync_shard_shard_recovery_restart -> TestResult: [
                primary1: (1, SliverType::Primary, false),
                primary5: (5, SliverType::Primary, false),
                primary10: (10, SliverType::Primary, false),
                secondary1: (1, SliverType::Secondary, false),
                secondary5: (5, SliverType::Secondary, false),
                secondary10: (10, SliverType::Secondary, false),
                restart_after_recovery: (10, SliverType::Secondary, true),
            ]
        }
        async fn simtest_sync_shard_shard_recovery_restart(
            break_index: u64,
            sliver_type: SliverType,
            restart_after_recovery: bool,
        ) -> TestResult {
            telemetry_subscribers::init_for_testing();

            register_fail_point_if("fail_point_after_start_recovery", move || {
                restart_after_recovery
            });
            if !restart_after_recovery {
                register_fail_point_arg(
                    "fail_point_fetch_sliver",
                    move || -> Option<(SliverType, u64)> { Some((sliver_type, break_index)) },
                );
            }

            let skip_stored_blob_index: [usize; 12] = [3, 4, 5, 9, 10, 11, 15, 18, 19, 20, 21, 22];
            let (cluster, blob_details) = setup_shard_recovery_test_cluster(|blob_index| {
                !skip_stored_blob_index.contains(&blob_index)
            })
            .await?;

            let node_inner = unsafe {
                &mut *(Arc::as_ptr(&cluster.nodes[1].storage_node.inner) as *mut StorageNodeInner)
            };
            node_inner.storage.create_storage_for_shard(ShardIndex(0))?;
            let shard_storage_dst = node_inner.storage.shard_storage(ShardIndex(0)).unwrap();
            shard_storage_dst.update_status_in_test(ShardStatus::None)?;

            cluster.nodes[1]
                .storage_node
                ._shard_sync_handler
                .start_new_shard_sync(ShardIndex(0))
                .await?;
            // Waits for the shard sync process to stop.
            wait_until_no_sync_tasks(&cluster.nodes[1].storage_node._shard_sync_handler).await?;

            // Check that shard sync process is not finished.
            if !restart_after_recovery {
                let shard_storage_src = cluster.nodes[0]
                    .storage_node
                    .inner
                    .storage
                    .shard_storage(ShardIndex(0))
                    .unwrap();
                assert!(
                    shard_storage_dst.sliver_count(SliverType::Primary)
                        < shard_storage_src.sliver_count(SliverType::Primary)
                        || shard_storage_dst.sliver_count(SliverType::Secondary)
                            < shard_storage_src.sliver_count(SliverType::Secondary)
                );
            }

            clear_fail_point("fail_point_after_start_recovery");
            if !restart_after_recovery {
                clear_fail_point("fail_point_fetch_sliver");
            }

            // restart the shard syncing process, to simulate a reboot.
            cluster.nodes[1]
                .storage_node
                ._shard_sync_handler
                .restart_syncs()
                .await?;

            wait_for_shard_in_active_state(shard_storage_dst).await?;
            check_all_blobs_are_synced(&blob_details, shard_storage_dst)?;

            Ok(())
        }
    }
}
