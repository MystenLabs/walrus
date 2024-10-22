// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Walrus storage node.

use std::{
    future::Future,
    num::{NonZero, NonZeroU16},
    pin::Pin,
    sync::Arc,
};

use anyhow::{anyhow, bail, Context};
use committee::{BeginCommitteeChangeError, EndCommitteeChangeError};
use config::EventProviderConfig;
use epoch_change_driver::EpochChangeDriver;
use fastcrypto::traits::KeyPair;
use futures::{Stream, StreamExt, TryFutureExt};
use futures_util::stream;
use prometheus::Registry;
use rand::{rngs::StdRng, thread_rng, Rng, SeedableRng};
use serde::Serialize;
use sui_types::{digests::TransactionDigest, event::EventID};
use tokio::{select, sync::watch, time::Instant};
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
use walrus_event::{event_processor::EventProcessor, EventProcessorConfig, IndexedStreamElement};
use walrus_sdk::api::{
    BlobStatus,
    ServiceHealthInfo,
    ShardHealthInfo,
    ShardStatus as ApiShardStatus,
    ShardStatusDetail,
    ShardStatusSummary,
    StoredOnNodeStatus,
};
use walrus_sui::{
    client::SuiReadClient,
    types::{
        BlobCertified,
        BlobDeleted,
        BlobEvent,
        ContractEvent,
        EpochChangeDone,
        EpochChangeEvent,
        EpochChangeStart,
        InvalidBlobId,
        GENESIS_EPOCH,
    },
};

use self::{
    blob_sync::BlobSyncHandler,
    committee::{CommitteeService, NodeCommitteeService},
    config::{StorageNodeConfig, SuiConfig},
    contract_service::{SuiSystemContractService, SystemContractService},
    errors::IndexOutOfRange,
    metrics::{NodeMetricSet, TelemetryLabel as _, STATUS_PENDING, STATUS_PERSISTED},
    shard_sync::ShardSyncHandler,
    storage::{blob_info::BlobInfoApi as _, EventProgress, ShardStatus, ShardStorage},
};
pub mod committee;
pub mod config;
pub mod contract_service;
pub mod server;
pub mod system_events;

pub(crate) mod metrics;

mod blob_sync;
mod epoch_change_driver;
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

use crate::{
    common::utils::ShardDiff,
    node::system_events::{EventManager, SuiSystemEventProvider},
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
    fn health_info(&self, detailed: bool) -> ServiceHealthInfo;

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
    committee_service: Option<Box<dyn CommitteeService>>,
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

    /// Sets the [`CommitteeService`] used with the node.
    pub fn with_committee_service(mut self, service: Box<dyn CommitteeService>) -> Self {
        self.committee_service = Some(service);
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
    /// [`CommitteeService`], or no [`SystemContractService`] was configured with
    /// their respective functions
    /// ([`with_system_event_manager()`][Self::with_system_event_manager],
    /// [`with_committee_service()`][Self::with_committee_service],
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
            if self.event_manager.is_none() || self.committee_service.is_none() {
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

            match &config.event_provider_config {
                EventProviderConfig::CheckpointBasedEventProcessor(event_processor_config) => {
                    let event_processor_config =
                        event_processor_config.clone().unwrap_or_else(|| {
                            EventProcessorConfig::new_with_default_pruning_interval(
                                sui_config.rpc.clone(),
                            )
                        });

                    Box::new(
                        EventProcessor::new(
                            &event_processor_config,
                            sui_config.rpc.clone(),
                            read_client.get_system_package_id(),
                            sui_config.event_polling_interval,
                            &config.storage_path.join("events"),
                            &metrics_registry,
                        )
                        .await?,
                    )
                }
                EventProviderConfig::LegacyEventProvider => Box::new(SuiSystemEventProvider::new(
                    read_client.clone(),
                    sui_config.event_polling_interval,
                )),
            }
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

        let committee_service = if let Some(committee_service) = self.committee_service {
            committee_service
        } else {
            let (read_client, _) = sui_config_and_client
                .expect("this is always created if self.committee_service_factory.is_none()");
            let service = NodeCommitteeService::builder()
                .local_identity(protocol_key_pair.public().clone())
                .config(config.blob_recovery.committee_service_config.clone())
                .metrics_registry(&metrics_registry)
                .build(read_client)
                .await?;
            Box::new(service)
        };

        StorageNode::new(
            config,
            protocol_key_pair,
            event_manager,
            committee_service,
            contract_service,
            &metrics_registry,
            self.storage,
        )
        .await
    }
}

async fn create_read_client(sui_config: &SuiConfig) -> Result<SuiReadClient, anyhow::Error> {
    Ok(sui_config.new_read_client().await?)
}

/// A Walrus storage node, responsible for 1 or more shards on Walrus.
#[derive(Debug)]
pub struct StorageNode {
    inner: Arc<StorageNodeInner>,
    blob_sync_handler: BlobSyncHandler,
    shard_sync_handler: ShardSyncHandler,
    epoch_change_driver: EpochChangeDriver,
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
    start_time: Instant,
    metrics: NodeMetricSet,
    current_epoch: watch::Sender<Epoch>,
}

impl StorageNode {
    async fn new(
        config: &StorageNodeConfig,
        key_pair: ProtocolKeyPair,
        event_manager: Box<dyn EventManager>,
        committee_service: Box<dyn CommitteeService>,
        contract_service: Box<dyn SystemContractService>,
        registry: &Registry,
        pre_created_storage: Option<Storage>, // For testing purposes. TODO(#703): remove.
    ) -> Result<Self, anyhow::Error> {
        let start_time = Instant::now();
        let encoding_config = committee_service.encoding_config().clone();

        let storage = if let Some(storage) = pre_created_storage {
            storage
        } else {
            Storage::open(
                config.storage_path.as_path(),
                config.db_config.clone().unwrap_or_default(),
                MetricConf::new("storage"),
            )?
        };
        tracing::info!("successfully opened the node database");

        let contract_service: Arc<dyn SystemContractService> = Arc::from(contract_service);
        let inner = Arc::new(StorageNodeInner {
            protocol_key_pair: key_pair,
            storage,
            event_manager,
            encoding_config,
            contract_service: contract_service.clone(),
            current_epoch: watch::Sender::new(committee_service.get_epoch()),
            committee_service: committee_service.into(),
            metrics: NodeMetricSet::new(registry),
            start_time,
        });

        inner.init_gauges()?;

        let blob_sync_handler = BlobSyncHandler::new(
            inner.clone(),
            config.blob_recovery.max_concurrent_blob_syncs,
            config.blob_recovery.max_concurrent_sliver_syncs,
        );

        let shard_sync_handler =
            ShardSyncHandler::new(inner.clone(), config.shard_sync_config.clone());
        // Upon restart, resume any ongoing blob syncs if there is any.
        shard_sync_handler.restart_syncs().await?;

        let system_parameters = contract_service.fixed_system_parameters().await?;
        let epoch_change_driver = EpochChangeDriver::new(
            system_parameters,
            contract_service,
            StdRng::seed_from_u64(thread_rng().gen()),
        );

        Ok(StorageNode {
            inner,
            blob_sync_handler,
            shard_sync_handler,
            epoch_change_driver,
        })
    }

    /// Creates a new [`StorageNodeBuilder`] for constructing a `StorageNode`.
    pub fn builder() -> StorageNodeBuilder {
        StorageNodeBuilder::default()
    }

    /// Run the walrus-node logic until cancelled using the provided cancellation token.
    pub async fn run(&self, cancel_token: CancellationToken) -> anyhow::Result<()> {
        if let Err(error) = self
            .epoch_change_driver
            .schedule_relevant_calls_for_current_epoch()
            .await
        {
            // We only warn here, as this fails during tests.
            tracing::warn!(?error, "unable to schedule epoch calls on startup")
        };

        select! {
            () = self.epoch_change_driver.run() => {
                unreachable!("epoch change driver never completes");
            },
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
    pub fn shards(&self) -> Vec<ShardIndex> {
        self.inner.storage.shards()
    }

    /// Wait for the storage node to be in at least the provided epoch.
    ///
    /// Returns the epoch to which the storage node arrived, which may be later than the requested
    /// epoch.
    pub async fn wait_for_epoch(&self, epoch: Epoch) -> Epoch {
        let mut receiver = self.inner.current_epoch.subscribe();
        let epoch_ref = receiver
            .wait_for(|current_epoch| *current_epoch >= epoch)
            .await
            .expect("current_epoch channel cannot be dropped while holding a ref to self");
        *epoch_ref
    }

    /// Continues the event stream from the last committed event.
    async fn continue_event_stream(
        &self,
    ) -> anyhow::Result<(
        Pin<Box<dyn Stream<Item = IndexedStreamElement> + Send + Sync + '_>>,
        usize,
    )> {
        let storage = &self.inner.storage;
        let (from_event_id, next_event_index) = storage
            .get_event_cursor_and_next_index()?
            .map_or((None, 0), |(cursor, index)| (Some(cursor), index));
        let event_cursor = EventStreamCursor::new(from_event_id, next_event_index);

        Ok((
            Box::into_pin(self.inner.event_manager.events(event_cursor).await?),
            next_event_index.try_into().expect("64-bit architecture"),
        ))
    }

    async fn process_events(&self) -> anyhow::Result<()> {
        let (event_stream, next_event_index) = self.continue_event_stream().await?;

        let index_stream = stream::iter(next_event_index..);
        let mut maybe_epoch_at_start = Some(self.inner.committee_service.get_epoch());

        let mut indexed_element_stream = index_stream.zip(event_stream);
        // Important: Events must be handled consecutively and in order to prevent (intermittent)
        // invariant violations and interference between different events. See, for example,
        // `Self::cancel_sync_and_mark_certified_event_completed`.
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

            if let Some(epoch_at_start) = maybe_epoch_at_start {
                if let EventStreamElement::ContractEvent(ref event) = stream_element.element {
                    tracing::debug!("checking the first contract event if we're severely lagging");
                    // Clear the starting epoch, so that we never make this check again.
                    maybe_epoch_at_start = None;

                    // if event.event_epoch() < starting_epoch - 1
                    if event.event_epoch() + 1 < epoch_at_start {
                        tracing::warn!(
                            "the current epoch ({}) is too far ahead of the event epoch: {}",
                            epoch_at_start,
                            event.event_epoch()
                        );
                    }
                }
            }

            self.process_event(element_index, stream_element)
                .inspect_err(|err| {
                    let span = tracing::Span::current();
                    span.record("otel.status_code", "error");
                    span.record("otel.status_message", field::display(err));
                })
                .instrument(span)
                .await?;
        }

        bail!("event stream for blob events stopped")
    }

    #[tracing::instrument(skip_all)]
    async fn process_event(
        &self,
        element_index: usize,
        stream_element: IndexedStreamElement,
    ) -> anyhow::Result<()> {
        let _timer_guard = &self
            .inner
            .metrics
            .event_process_duration_seconds
            .with_label_values(&[stream_element.element.label()])
            .start_timer();
        match stream_element.element {
            EventStreamElement::ContractEvent(ContractEvent::BlobEvent(blob_event)) => {
                self.process_blob_event(element_index, blob_event).await?;
            }
            EventStreamElement::ContractEvent(ContractEvent::EpochChangeEvent(
                epoch_change_event,
            )) => {
                self.process_epoch_change_event(element_index, epoch_change_event)
                    .await?;
            }
            EventStreamElement::CheckpointBoundary => {
                self.inner.mark_element_at_index(element_index)?;
            }
        }
        Ok::<(), anyhow::Error>(())
    }

    #[tracing::instrument(skip_all)]
    async fn process_blob_event(
        &self,
        element_index: usize,
        blob_event: BlobEvent,
    ) -> anyhow::Result<()> {
        self.inner
            .storage
            .update_blob_info(element_index, &blob_event)?;
        match blob_event {
            BlobEvent::Registered(event) => {
                tracing::debug!("BlobRegistered event received: {:?}", event);
                self.inner
                    .mark_event_completed(element_index, &event.event_id)?;
            }
            BlobEvent::Certified(event) => {
                tracing::debug!("BlobCertified event received: {:?}", event);
                self.process_blob_certified_event(element_index, event)
                    .await?;
            }
            BlobEvent::Deleted(event) => {
                tracing::debug!("BlobDeleted event received: {:?}", event);
                self.process_blob_deleted_event(element_index, event)
                    .await?;
            }
            BlobEvent::InvalidBlobID(event) => {
                tracing::debug!("BlobInvalid event received: {:?}", event);
                self.process_blob_invalid_event(element_index, event)
                    .await?;
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn process_epoch_change_event(
        &self,
        element_index: usize,
        epoch_change_event: EpochChangeEvent,
    ) -> anyhow::Result<()> {
        match epoch_change_event {
            EpochChangeEvent::EpochParametersSelected(event) => {
                tracing::info!("EpochParametersSelected event received: {:?}", event);
                self.epoch_change_driver
                    .cancel_scheduled_voting_end(event.next_epoch);
                self.epoch_change_driver.schedule_initiate_epoch_change(
                    NonZero::new(event.next_epoch).expect("the next epoch is always non-zero"),
                );
                self.inner
                    .mark_event_completed(element_index, &event.event_id)?;
            }
            EpochChangeEvent::EpochChangeStart(event) => {
                tracing::info!("EpochChangeStart event received: {:?}", event);
                self.process_epoch_change_start_event(&event).await?;
                self.inner
                    .mark_event_completed(element_index, &event.event_id)?;
            }
            EpochChangeEvent::EpochChangeDone(event) => {
                tracing::info!("EpochChangeDone event received: {:?}", event);
                self.process_epoch_change_done_event(&event).await?;
                self.inner
                    .mark_event_completed(element_index, &event.event_id)?;
            }
            EpochChangeEvent::ShardsReceived(event) => {
                tracing::info!("ShardsReceived event received: {:?}", event);
                self.inner
                    .mark_event_completed(element_index, &event.event_id)?;
            }
            EpochChangeEvent::ShardRecoveryStart(event) => {
                tracing::info!("ShardRecoveryStart event received: {:?}", event);
                self.inner
                    .mark_event_completed(element_index, &event.event_id)?;
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    fn next_event_index(&self) -> anyhow::Result<u64> {
        Ok(self
            .inner
            .storage
            .get_event_cursor_and_next_index()?
            .map_or(0, |(_, index)| index))
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
            || self.inner.current_epoch() >= event.end_epoch
        {
            self.inner
                .mark_event_completed(event_index, &event.event_id)?;

            metrics::with_label!(histogram_set, metrics::STATUS_SKIPPED)
                .observe(start.elapsed().as_secs_f64());

            return Ok(());
        }

        // Slivers and (possibly) metadata are not stored, so initiate blob sync.
        self.blob_sync_handler
            .start_sync(event, event_index, start)
            .await?;

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn process_blob_deleted_event(
        &self,
        event_index: usize,
        event: BlobDeleted,
    ) -> anyhow::Result<()> {
        let blob_id = event.blob_id;

        if let Some(blob_info) = self.inner.storage.get_blob_info(&blob_id)? {
            if !blob_info.is_certified(self.inner.current_epoch()) {
                self.cancel_sync_and_mark_certified_event_completed(&blob_id)
                    .await?;
            }
            // Note that this function is called *after* the blob info has already been updated with
            // the event. So it can happen that the only registered blob was deleted and the blob is
            // now no longer registered.
            if !blob_info.is_registered(self.inner.current_epoch()) {
                self.inner.storage.delete_blob(&event.blob_id, true)?;
            }
        } else {
            tracing::warn!(%blob_id, "handling `BlobDeleted` event for untracked blob");
        }

        self.inner
            .mark_event_completed(event_index, &event.event_id)?;

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn process_blob_invalid_event(
        &self,
        event_index: usize,
        event: InvalidBlobId,
    ) -> anyhow::Result<()> {
        self.cancel_sync_and_mark_certified_event_completed(&event.blob_id)
            .await?;
        self.inner.storage.delete_blob(&event.blob_id, false)?;
        self.inner
            .mark_event_completed(event_index, &event.event_id)?;
        Ok(())
    }

    /// Cancels any existing blob syncs for the provided `blob_id` and marks the corresponding
    /// events as completed.
    ///
    /// To avoid interference with later events (e.g., cancelling a sync initiated by a later event
    /// or immediately restarting the sync that is cancelled here), this function should be called
    /// before any later events are being handled.
    async fn cancel_sync_and_mark_certified_event_completed(
        &self,
        blob_id: &BlobId,
    ) -> anyhow::Result<()> {
        if let Some((cancelled_event_index, cancelled_event_id)) =
            self.blob_sync_handler.cancel_sync(blob_id).await?
        {
            // Advance the event cursor with the event of the cancelled sync. Since the blob is
            // invalid the associated blob certified event is completed without a sync.
            self.inner
                .mark_event_completed(cancelled_event_index, &cancelled_event_id)?;
        }
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn process_epoch_change_start_event(
        &self,
        event: &EpochChangeStart,
    ) -> anyhow::Result<()> {
        // Irrespective of whether we are in this epoch, we can cancel any scheduled calls to change
        // to or end voting for the epoch identified by the event, as we're already in that epoch.
        self.epoch_change_driver
            .cancel_scheduled_voting_end(event.epoch);
        self.epoch_change_driver
            .cancel_scheduled_epoch_change_initiation(event.epoch);

        if !self.begin_committee_change(event.epoch).await? {
            return Ok(());
        }

        let public_key = self.inner.public_key();
        let storage = &self.inner.storage;
        let committees = self.inner.committee_service.active_committees();
        let shard_diff = ShardDiff::diff_previous(&committees, public_key);

        for shard_id in &shard_diff.lost {
            let Some(shard_storage) = storage.shard_storage(*shard_id) else {
                tracing::debug!("skipping lost shard during epoch change as it is not stored");
                continue;
            };
            // TODO: remove shard at the next EpochChangeStart event.
            //       eventually, we can remove the shard upon receiving ShardsReceived events.
            tracing::debug!(walrus.shard_index = %shard_id, "locking shard for epoch change");
            shard_storage
                .lock_shard_for_epoch_change()
                .context("failed to lock shard")?;
        }

        if shard_diff.gained.is_empty() {
            let is_node_in_committee = committees.current_committee().contains(public_key);
            if is_node_in_committee && committees.epoch() == event.epoch {
                // We are in the current committee, but no shards were gained. Directly signal that
                // the epoch sync is done.
                tracing::info!("no shards gained, so signalling that epoch sync is done");
                self.inner
                    .contract_service
                    .epoch_sync_done(event.epoch)
                    .await;
            } else {
                // Since we just refreshed the committee after receiving the event, the committees'
                // epoch must be at least the event's epoch.
                assert!(committees.epoch() >= event.epoch);
                tracing::info!(
                    "skip sending epoch sync done event. \
                    node in committee: {}, committee epoch: {}, event epoch: {}",
                    is_node_in_committee,
                    committees.epoch(),
                    event.epoch
                );
            }
        } else {
            assert!(committees.current_committee().contains(public_key));
            self.inner
                .create_storage_for_shards_in_background(shard_diff.gained.clone())
                .await?;

            // There shouldn't be an epoch change event for the genesis epoch.
            assert!(event.epoch != GENESIS_EPOCH);
            for shard in &shard_diff.gained {
                self.shard_sync_handler.start_new_shard_sync(*shard).await?;
            }
        }

        Ok(())
    }

    /// Initiates a committee transition to a new epoch.
    ///
    /// Returns `true` if epoch change event has started or was sufficiently recent such
    /// that it should be handled.
    #[tracing::instrument(skip_all)]
    async fn begin_committee_change(
        &self,
        epoch: Epoch,
    ) -> Result<bool, BeginCommitteeChangeError> {
        match self
            .inner
            .committee_service
            .begin_committee_change(epoch)
            .await
        {
            Ok(()) => {
                tracing::debug!(
                    walrus.epoch = epoch,
                    "successfully started a transition to a new epoch"
                );
                self.inner.current_epoch.send_replace(epoch);
                Ok(true)
            }
            Err(BeginCommitteeChangeError::EpochIsTheSameAsCurrent) => {
                tracing::debug!(
                    walrus.epoch = epoch,
                    "epoch change event was for the epoch we are currently in, not skipping"
                );
                Ok(true)
            }
            Err(BeginCommitteeChangeError::ChangeAlreadyInProgress)
            | Err(BeginCommitteeChangeError::EpochIsLess { .. }) => {
                // We are likely processing a backlog of events. Since the committee service has a
                // more recent committee or has already had the current committee marked as
                // transitioning, our shards have also already been configured for the more
                // recent committee and there is actual nothing to do.
                tracing::debug!(
                    walrus.epoch = epoch,
                    "skipping epoch change start event for an older epoch"
                );
                Ok(false)
            }
            Err(error) => {
                tracing::error!(?error, "failed to initiate a transition to the new epoch");
                Err(error)
            }
        }
    }

    #[tracing::instrument(skip_all)]
    async fn process_epoch_change_done_event(&self, event: &EpochChangeDone) -> anyhow::Result<()> {
        match self
            .inner
            .committee_service
            .end_committee_change(event.epoch)
        {
            Ok(()) => tracing::debug!(
                walrus.epoch = event.epoch,
                "successfully ended the transition to the new epoch"
            ),
            // This likely means that the committee was fetched (for example on startup) and we
            // are not processing the event that would have notified us that the epoch was
            // changing.
            Err(EndCommitteeChangeError::EpochChangeAlreadyDone) => tracing::debug!(
                walrus.epoch = event.epoch,
                "the committee had already transitioned to the new epoch"
            ),
            Err(EndCommitteeChangeError::ProvidedEpochIsInThePast { .. }) => {
                // We are ending a change to an epoch that we have already advanced beyond. This is
                // likely due to processing a backlog of events and can be ignored.
                tracing::debug!(
                    walrus.epoch = event.epoch,
                    "skipping epoch change event that is in the past"
                );
                return Ok(());
            }
            Err(error @ EndCommitteeChangeError::ProvidedEpochIsInTheFuture { .. }) => {
                tracing::error!(
                    ?error,
                    "our committee service is lagging behind the events being processed which \
                    should not happen"
                );
                return Err(error.into());
            }
        }

        self.epoch_change_driver.schedule_voting_end(
            NonZero::new(event.epoch + 1).expect("incremented value is non-zero"),
        );

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
    ) -> Result<Arc<ShardStorage>, ShardNotAssigned> {
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

    fn shard_health_status(
        &self,
        detailed: bool,
    ) -> (ShardStatusSummary, Option<ShardStatusDetail>) {
        // NOTE: It is possible that the committee or shards change between this and the next call.
        // As this is for admin consumption, this is not considered a problem.
        let mut shard_statuses = self.storage.try_list_shard_status().unwrap_or_default();
        let committee = self.committee_service.committee();
        let owned_shards = committee.shards_for_node_public_key(self.public_key());
        let mut summary = ShardStatusSummary::default();

        let mut detail = detailed.then(|| {
            let mut detail = ShardStatusDetail::default();
            detail.owned.reserve_exact(owned_shards.len());
            detail
        });

        // Record the status for the owned shards.
        for &shard in owned_shards {
            // Consume statuses, so that we are left with shards that are not owned.
            let status = shard_statuses
                .remove(&shard)
                .flatten()
                .map_or(ApiShardStatus::Unknown, api_status_from_shard_status);

            increment_shard_summary(&mut summary, status, true);
            if let Some(ref mut detail) = detail {
                detail.owned.push(ShardHealthInfo { shard, status });
            }
        }

        // Record the status for the unowned shards.
        for (shard, status) in shard_statuses {
            let status = status.map_or(ApiShardStatus::Unknown, api_status_from_shard_status);
            increment_shard_summary(&mut summary, status, false);
            if let Some(ref mut detail) = detail {
                detail.other.push(ShardHealthInfo { shard, status });
            }
        }

        // Sort the result by the shard index.
        if let Some(ref mut detail) = detail {
            detail.owned.sort_by_key(|info| info.shard);
            detail.other.sort_by_key(|info| info.shard);
        }

        (summary, detail)
    }

    async fn create_storage_for_shards_in_background(
        self: &Arc<Self>,
        new_shards: Vec<ShardIndex>,
    ) -> Result<(), anyhow::Error> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.storage.create_storage_for_shards(&new_shards))
            .in_current_span()
            .await??;
        Ok(())
    }

    fn is_blob_registered(&self, blob_id: &BlobId) -> Result<bool, anyhow::Error> {
        Ok(self
            .storage
            .get_blob_info(blob_id)
            .context("could not retrieve blob info")?
            .is_some_and(|blob_info| blob_info.is_registered(self.current_epoch())))
    }

    fn is_blob_certified(&self, blob_id: &BlobId) -> Result<bool, anyhow::Error> {
        Ok(self
            .storage
            .get_blob_info(blob_id)
            .context("could not retrieve blob info")?
            .is_some_and(|blob_info| blob_info.is_certified(self.current_epoch())))
    }
}

fn api_status_from_shard_status(status: ShardStatus) -> ApiShardStatus {
    match status {
        ShardStatus::None => ApiShardStatus::Unknown,
        ShardStatus::Active => ApiShardStatus::Ready,
        ShardStatus::ActiveSync => ApiShardStatus::InTransfer,
        ShardStatus::ActiveRecover => ApiShardStatus::InRecovery,
        ShardStatus::LockedToMove => ApiShardStatus::ReadOnly,
    }
}

fn increment_shard_summary(
    summary: &mut ShardStatusSummary,
    status: ApiShardStatus,
    is_owned: bool,
) {
    if !is_owned {
        if ApiShardStatus::ReadOnly == status {
            summary.read_only += 1;
        }
        return;
    }

    debug_assert!(is_owned);
    summary.owned += 1;
    match status {
        ApiShardStatus::Unknown => summary.unknown += 1,
        ApiShardStatus::Ready => summary.ready += 1,
        ApiShardStatus::InTransfer => summary.in_transfer += 1,
        ApiShardStatus::InRecovery => summary.in_recovery += 1,
        // We do not expect owned shards to be read-only.
        _ => (),
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

    fn health_info(&self, detailed: bool) -> ServiceHealthInfo {
        self.inner.health_info(detailed)
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
        ensure!(
            self.is_blob_registered(blob_id)?,
            RetrieveMetadataError::Unavailable,
        );

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

        ensure!(
            blob_info.is_registered(self.current_epoch()),
            StoreMetadataError::NotCurrentlyRegistered,
        );

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

        ensure!(
            self.is_blob_certified(blob_id)?,
            RetrieveSliverError::Unavailable,
        );

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

        ensure!(
            self.is_blob_registered(blob_id)?,
            StoreSliverError::NotCurrentlyRegistered,
        );

        // Ensure we have received the blob metadata.
        let metadata = self
            .storage
            .get_metadata(blob_id)
            .context("database error when storing sliver")?
            .ok_or(StoreSliverError::MissingMetadata)?;

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
            self.is_blob_registered(blob_id)?,
            ComputeStorageConfirmationError::NotCurrentlyRegistered,
        );
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

    fn health_info(&self, detailed: bool) -> ServiceHealthInfo {
        let (shard_summary, shard_detail) = self.shard_health_status(detailed);
        ServiceHealthInfo {
            uptime: self.start_time.elapsed(),
            epoch: self.current_epoch(),
            public_key: self.public_key().clone(),
            shard_detail,
            shard_summary,
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

    use chrono::Utc;
    use contract_service::MockSystemContractService;
    use storage::{
        tests::{populated_storage, WhichSlivers, BLOB_ID, OTHER_SHARD_INDEX, SHARD_INDEX},
        ShardStatus,
    };
    use sui_types::base_types::ObjectID;
    use system_events::SystemEventProvider;
    use tokio::sync::{broadcast::Sender, Mutex};
    use walrus_core::{
        encoding::{Primary, Secondary, SliverData, SliverPair},
        messages::{SyncShardMsg, SyncShardRequest},
        test_utils::generate_config_metadata_and_valid_recovery_symbols,
    };
    use walrus_sdk::client::Client;
    use walrus_sui::{
        client::FixedSystemParameters,
        test_utils::{event_id_for_testing, EventForTesting},
        types::{move_structs::EpochState, BlobRegistered},
    };
    use walrus_test_utils::{async_param_test, Result as TestResult, WithTempDir};

    use super::*;
    use crate::test_utils::{StorageNodeHandle, StorageNodeHandleTrait, TestCluster};

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

    async fn storage_node_with_storage_and_events<U>(
        storage: WithTempDir<Storage>,
        events: U,
    ) -> StorageNodeHandle
    where
        U: SystemEventProvider + Into<Box<U>> + 'static,
    {
        StorageNodeHandle::builder()
            .with_storage(storage)
            .with_system_event_provider(events)
            .with_node_started(true)
            .build()
            .await
            .expect("storage node creation in setup should not fail")
    }

    mod get_storage_confirmation {
        use fastcrypto::traits::VerifyingKey;

        use super::*;

        #[tokio::test]
        async fn errs_if_blob_is_not_registered() -> TestResult {
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
                ComputeStorageConfirmationError::NotCurrentlyRegistered
            ));

            Ok(())
        }

        #[tokio::test]
        async fn errs_if_not_all_slivers_stored() -> TestResult {
            let storage_node = storage_node_with_storage_and_events(
                populated_storage(&[(
                    SHARD_INDEX,
                    vec![
                        (BLOB_ID, WhichSlivers::Primary),
                        (OTHER_BLOB_ID, WhichSlivers::Both),
                    ],
                )])?,
                vec![BlobRegistered::for_testing(BLOB_ID).into()],
            )
            .await;

            let err = retry_until_success_or_timeout(TIMEOUT, || async {
                match storage_node
                    .as_ref()
                    .compute_storage_confirmation(&BLOB_ID)
                    .await
                {
                    Err(ComputeStorageConfirmationError::NotCurrentlyRegistered) => Err(()),
                    result => Ok(result),
                }
            })
            .await
            .expect("retry should eventually return something besides 'NotCurrentlyRegistered'")
            .expect_err("should fail");

            assert!(matches!(
                err,
                ComputeStorageConfirmationError::NotFullyStored,
            ));

            Ok(())
        }

        #[tokio::test]
        async fn returns_confirmation_over_nodes_storing_the_pair() -> TestResult {
            let storage_node = storage_node_with_storage_and_events(
                populated_storage(&[(
                    SHARD_INDEX,
                    vec![
                        (BLOB_ID, WhichSlivers::Both),
                        (OTHER_BLOB_ID, WhichSlivers::Both),
                    ],
                )])?,
                vec![BlobRegistered::for_testing(BLOB_ID).into()],
            )
            .await;

            let confirmation = retry_until_success_or_timeout(TIMEOUT, || {
                storage_node.as_ref().compute_storage_confirmation(&BLOB_ID)
            })
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
            .with_system_event_provider(vec![
                ContractEvent::EpochChangeEvent(EpochChangeEvent::EpochChangeStart(
                    EpochChangeStart {
                        epoch: 1,
                        event_id: event_id_for_testing(),
                    },
                )),
                BlobRegistered::for_testing(BLOB_ID).into(),
            ])
            .with_shard_assignment(&[shard_for_node])
            .with_node_started(true)
            .with_rest_api_started(true)
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

    async_param_test! {
        deletes_blob_data_on_event -> TestResult: [
            invalid_blob_event_registered: (InvalidBlobId::for_testing(BLOB_ID).into(), false),
            blob_deleted_event_registered: (
                BlobDeleted{was_certified: false, ..BlobDeleted::for_testing(BLOB_ID)}.into(),
                false
            ),
            invalid_blob_event_certified: (InvalidBlobId::for_testing(BLOB_ID).into(), true),
            blob_deleted_event_certified: (BlobDeleted::for_testing(BLOB_ID).into(), true),
        ]
    }
    async fn deletes_blob_data_on_event(event: BlobEvent, is_certified: bool) -> TestResult {
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
        events.send(
            BlobRegistered {
                deletable: true,
                ..BlobRegistered::for_testing(BLOB_ID)
            }
            .into(),
        )?;
        if is_certified {
            events.send(
                BlobCertified {
                    deletable: true,
                    ..BlobCertified::for_testing(BLOB_ID)
                }
                .into(),
            )?;
        }

        events.send(event.into())?;

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
            .flat_map(|node| std::iter::repeat(node).zip(node.storage_node().shards()))
            .collect();

        let mut metadata_stored = vec![];

        for (node, shard) in nodes_and_shards {
            if !metadata_stored.contains(&node.public_key())
                && (store_at_shard(&shard, SliverType::Primary)
                    || store_at_shard(&shard, SliverType::Secondary))
            {
                node.client().store_metadata(&blob.metadata).await?;
                metadata_stored.push(node.public_key());
            }

            let sliver_pair = blob.assigned_sliver_pair(shard);

            if store_at_shard(&shard, SliverType::Primary) {
                node.client()
                    .store_sliver(blob.blob_id(), sliver_pair.index(), &sliver_pair.primary)
                    .await?;
            }

            if store_at_shard(&shard, SliverType::Secondary) {
                node.client()
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

    async fn cluster_at_epoch1_without_blobs(
        assignment: &[&[u16]],
    ) -> TestResult<(TestCluster, Sender<ContractEvent>)> {
        let events = Sender::new(48);

        let cluster = {
            // Lock to avoid race conditions.
            let _lock = global_test_lock().lock().await;
            TestCluster::<StorageNodeHandle>::builder()
                .with_shard_assignment(assignment)
                .with_system_event_providers(events.clone())
                .build()
                .await?
        };

        Ok((cluster, events))
    }

    async fn cluster_with_partially_stored_blob<'a, F>(
        assignment: &[&[u16]],
        blob: &'a [u8],
        store_at_shard: F,
    ) -> TestResult<(TestCluster, Sender<ContractEvent>, EncodedBlob)>
    where
        F: FnMut(&ShardIndex, SliverType) -> bool,
    {
        let (cluster, events) = cluster_at_epoch1_without_blobs(assignment).await?;

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
        let (cluster, events) = cluster_at_epoch1_without_blobs(assignment).await?;

        let config = cluster.encoding_config();
        let mut details = Vec::new();

        // Add the blobs at epoch 1, the epoch at which the cluster starts.
        for blob in blobs {
            let blob_details = EncodedBlob::new(blob, config.clone());
            // Note: register and certify the blob are always using epoch 0.
            events.send(BlobRegistered::for_testing(*blob_details.blob_id()).into())?;
            store_at_shards(&blob_details, &cluster, |_, _| true).await?;
            events.send(BlobCertified::for_testing(*blob_details.blob_id()).into())?;
            details.push(blob_details);
        }

        advance_cluster_to_epoch(&cluster, &[&events], initial_epoch).await?;

        Ok((cluster, events, details))
    }

    async fn advance_cluster_to_epoch(
        cluster: &TestCluster,
        events: &[&Sender<ContractEvent>],
        epoch: Epoch,
    ) -> TestResult {
        let lookup_service_handle = cluster.lookup_service_handle.clone().unwrap();

        for epoch in lookup_service_handle.epoch() + 1..epoch + 1 {
            let new_epoch = lookup_service_handle.advance_epoch();
            assert_eq!(new_epoch, epoch);
            for event_queue in events {
                event_queue.send(ContractEvent::EpochChangeEvent(
                    EpochChangeEvent::EpochChangeStart(EpochChangeStart {
                        epoch,
                        event_id: walrus_sui::test_utils::event_id_for_testing(),
                    }),
                ))?;
                event_queue.send(ContractEvent::EpochChangeEvent(
                    EpochChangeEvent::EpochChangeDone(EpochChangeDone {
                        epoch,
                        event_id: walrus_sui::test_utils::event_id_for_testing(),
                    }),
                ))?;
            }
            cluster.wait_for_nodes_to_reach_epoch(epoch).await;
        }

        Ok(())
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
            TestCluster::<StorageNodeHandle>::builder()
                .with_shard_assignment(assignment)
                .with_individual_system_event_providers(&event_providers)
                .build()
                .await?
        };

        let config = cluster.encoding_config();
        let mut details = Vec::new();
        for (i, blob) in blobs.iter().enumerate() {
            let blob_details = EncodedBlob::new(blob, config.clone());
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

        advance_cluster_to_epoch(
            &cluster,
            &[&node_0_events, &all_other_node_events],
            initial_epoch,
        )
        .await?;

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
    async fn does_not_start_blob_sync_for_already_expired_blob() -> TestResult {
        let shards: &[&[u16]] = &[&[1], &[0, 2, 3, 4]];

        let (cluster, events) = cluster_at_epoch1_without_blobs(shards).await?;
        let node = &cluster.nodes[0];

        // Register and certify an already expired blob.
        let object_id = ObjectID::random();
        let event_id = event_id_for_testing();
        events.send(
            BlobRegistered {
                epoch: 1,
                blob_id: BLOB_ID,
                end_epoch: 1,
                deletable: false,
                object_id,
                event_id,
                size: 0,
                encoding_type: walrus_core::EncodingType::RedStuff,
            }
            .into(),
        )?;
        events.send(
            BlobCertified {
                epoch: 1,
                blob_id: BLOB_ID,
                end_epoch: 1,
                deletable: false,
                object_id,
                is_extension: false,
                event_id,
            }
            .into(),
        )?;

        // Make sure the node actually saw and started processing the event.
        retry_until_success_or_timeout(TIMEOUT, || async {
            node.storage_node
                .inner
                .storage
                .get_blob_info(&BLOB_ID)?
                .ok_or(anyhow!("blob info not updated"))
        })
        .await?;

        assert_eq!(node.storage_node.blob_sync_handler.cancel_all().await?, 0);

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
                .instrument(tracing::info_span!("test-inners"))
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
            .get_event_cursor_and_next_index()?
            .map(|(cursor, _)| cursor);
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
            too_old: (3, 1, "Invalid epoch. Client epoch: 1. Server epoch: 3"),
            too_new: (3, 4, "Invalid epoch. Client epoch: 4. Server epoch: 3"),
        ]
    }
    async fn sync_shard_node_api_invalid_epoch(
        cluster_epoch: Epoch,
        requester_epoch: Epoch,
        error_message: &str,
    ) -> TestResult {
        // Creates a cluster with initial epoch set to 3.
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
        let (cluster, events, blob) =
            cluster_with_partially_stored_blob(&[&[0]], BLOB, |_, _| true).await?;

        events.send(BlobCertified::for_testing(*blob.blob_id()).into())?;

        cluster.nodes[0]
            .storage_node
            .inner
            .storage
            .shard_storage(ShardIndex(0))
            .unwrap()
            .lock_shard_for_epoch_change()
            .expect("Lock shard failed.");

        let sliver = retry_until_success_or_timeout(TIMEOUT, || async {
            cluster.nodes[0].storage_node.retrieve_sliver(
                blob.blob_id(),
                SliverPairIndex(0),
                SliverType::Primary,
            )
        })
        .await
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
        node_inner
            .storage
            .create_storage_for_shards(&[ShardIndex(0)])?;
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
            .shard_sync_handler
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
        node_inner
            .storage
            .create_storage_for_shards(&[ShardIndex(0)])?;
        let shard_storage_dst = node_inner.storage.shard_storage(ShardIndex(0)).unwrap();
        shard_storage_dst.update_status_in_test(ShardStatus::None)?;

        cluster.nodes[1]
            .storage_node
            .shard_sync_handler
            .start_new_shard_sync(ShardIndex(0))
            .await?;
        wait_for_shard_in_active_state(shard_storage_dst.as_ref()).await?;
        check_all_blobs_are_synced(&blob_details, shard_storage_dst.as_ref())?;

        Ok(())
    }

    // Tests shard transfer partially using shard recovery functionality and partially using shard
    // sync.
    // This test also tests that no missing blobs after sync completion.
    #[tokio::test]
    async fn sync_shard_partial_recovery() -> TestResult {
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
        node_inner
            .storage
            .create_storage_for_shards(&[ShardIndex(0)])?;
        let shard_storage_dst = node_inner.storage.shard_storage(ShardIndex(0)).unwrap();
        shard_storage_dst.update_status_in_test(ShardStatus::None)?;

        cluster.nodes[1]
            .storage_node
            .shard_sync_handler
            .start_new_shard_sync(ShardIndex(0))
            .await?;
        wait_for_shard_in_active_state(shard_storage_dst.as_ref()).await?;
        check_all_blobs_are_synced(&blob_details, shard_storage_dst.as_ref())?;

        Ok(())
    }

    #[cfg(msim)]
    mod failure_injection_tests {
        use sui_macros::{clear_fail_point, register_fail_point_arg, register_fail_point_if};
        use walrus_proc_macros::walrus_simtest;
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
            sync_shard_start_from_progress -> TestResult: [
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
        async fn sync_shard_start_from_progress(
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
                .shard_sync_handler
                .start_new_shard_sync(ShardIndex(0))
                .await?;

            // Waits for the shard sync process to stop.
            wait_until_no_sync_tasks(&cluster.nodes[1].storage_node.shard_sync_handler).await?;

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
                .shard_sync_handler
                .restart_syncs()
                .await?;

            // Waits for the shard to be synced.
            wait_until_no_sync_tasks(&cluster.nodes[1].storage_node.shard_sync_handler).await?;

            // Checks that the shard is completely migrated.
            check_all_blobs_are_synced(&blob_details, &shard_storage_dst)?;

            Ok(())
        }

        // Tests that there is a discrepancy between the source and destination shards in terms
        // of certified blobs. If the source doesn't return any blobs, the destination should
        // finish the sync process.
        #[walrus_simtest]
        async fn sync_shard_src_return_empty() -> TestResult {
            telemetry_subscribers::init_for_testing();

            let (cluster, _blob_details, _shard_storage_dst) =
                setup_cluster_for_shard_sync_tests().await?;

            register_fail_point_if("fail_point_sync_shard_return_empty", || true);

            // Starts the shard syncing process in the new shard, which will fail at the specified
            // break index.
            cluster.nodes[1]
                .storage_node
                .shard_sync_handler
                .start_new_shard_sync(ShardIndex(0))
                .await?;

            // Waits for the shard sync process to stop.
            wait_until_no_sync_tasks(&cluster.nodes[1].storage_node.shard_sync_handler).await?;

            Ok(())
        }

        // Tests crash recovery of shard transfer partially using shard recovery functionality
        // and partially using shard sync.
        simtest_param_test! {
            sync_shard_shard_recovery_restart -> TestResult: [
                primary1: (1, SliverType::Primary, false),
                primary5: (5, SliverType::Primary, false),
                primary10: (10, SliverType::Primary, false),
                secondary1: (1, SliverType::Secondary, false),
                secondary5: (5, SliverType::Secondary, false),
                secondary10: (10, SliverType::Secondary, false),
                restart_after_recovery: (10, SliverType::Secondary, true),
            ]
        }
        async fn sync_shard_shard_recovery_restart(
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
            node_inner
                .storage
                .create_storage_for_shards(&[ShardIndex(0)])?;
            let shard_storage_dst = node_inner.storage.shard_storage(ShardIndex(0)).unwrap();
            shard_storage_dst.update_status_in_test(ShardStatus::None)?;

            cluster.nodes[1]
                .storage_node
                .shard_sync_handler
                .start_new_shard_sync(ShardIndex(0))
                .await?;
            // Waits for the shard sync process to stop.
            wait_until_no_sync_tasks(&cluster.nodes[1].storage_node.shard_sync_handler).await?;

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
                .shard_sync_handler
                .restart_syncs()
                .await?;

            wait_for_shard_in_active_state(shard_storage_dst.as_ref()).await?;
            check_all_blobs_are_synced(&blob_details, shard_storage_dst.as_ref())?;

            Ok(())
        }
    }

    // Waits until the storage node processes the specified number of events.
    async fn wait_until_events_processed(
        node: &StorageNodeHandle,
        processed_event_count: u64,
    ) -> anyhow::Result<()> {
        retry_until_success_or_timeout(Duration::from_secs(10), || async {
            if node
                .storage_node
                .inner
                .storage
                .get_sequentially_processed_event_count()?
                >= processed_event_count
            {
                Ok(())
            } else {
                bail!("not enough events processed")
            }
        })
        .await
    }

    #[tokio::test]
    async fn shard_initialization_in_epoch_one() -> TestResult {
        let node = StorageNodeHandle::builder()
            .with_system_event_provider(vec![ContractEvent::EpochChangeEvent(
                EpochChangeEvent::EpochChangeStart(EpochChangeStart {
                    epoch: 1,
                    event_id: event_id_for_testing(),
                }),
            )])
            .with_shard_assignment(&[ShardIndex(0), ShardIndex(27)])
            .with_node_started(true)
            .with_rest_api_started(true)
            .build()
            .await?;

        wait_until_events_processed(&node, 1).await?;

        assert_eq!(
            node.as_ref()
                .inner
                .storage
                .shard_storage(ShardIndex(0))
                .expect("Shard storage should be created")
                .status()
                .unwrap(),
            ShardStatus::Active
        );

        assert!(node
            .as_ref()
            .inner
            .storage
            .shard_storage(ShardIndex(1))
            .is_none());

        assert_eq!(
            node.as_ref()
                .inner
                .storage
                .shard_storage(ShardIndex(27))
                .expect("Shard storage should be created")
                .status()
                .unwrap(),
            ShardStatus::Active
        );
        Ok(())
    }

    async_param_test! {
        test_update_blob_info_is_idempotent -> TestResult: [
            empty: (&[], &[]),
            repeated_register_and_certify: (
                &[],
                &[
                    BlobRegistered::for_testing(BLOB_ID).into(),
                    BlobCertified::for_testing(BLOB_ID).into(),
                ]
            ),
            repeated_certify: (
                &[BlobRegistered::for_testing(BLOB_ID).into()],
                &[BlobCertified::for_testing(BLOB_ID).into()]
            ),
        ]
    }
    async fn test_update_blob_info_is_idempotent(
        setup_events: &[BlobEvent],
        repeated_events: &[BlobEvent],
    ) -> TestResult {
        let node = StorageNodeHandle::builder()
            .with_system_event_provider(vec![])
            .with_shard_assignment(&[ShardIndex(0)])
            .with_node_started(true)
            .build()
            .await?;
        let count_setup_events = setup_events.len();
        for (index, event) in setup_events
            .iter()
            .chain(repeated_events.iter())
            .enumerate()
        {
            node.storage_node
                .inner
                .storage
                .update_blob_info(index, event)?;
        }
        let intermediate_blob_info = node.storage_node.inner.storage.get_blob_info(&BLOB_ID)?;

        for (index, event) in repeated_events.iter().enumerate() {
            node.storage_node
                .inner
                .storage
                .update_blob_info(index + count_setup_events, event)?;
        }
        assert_eq!(
            intermediate_blob_info,
            node.storage_node.inner.storage.get_blob_info(&BLOB_ID)?
        );
        Ok(())
    }

    async_param_test! {
        test_no_epoch_sync_done_transaction -> TestResult: [
            not_committee_member: (None, &[]),
            outdated_epoch: (Some(2), &[ShardIndex(0)]),
        ]
    }
    async fn test_no_epoch_sync_done_transaction(
        initial_epoch: Option<Epoch>,
        shard_assignment: &[ShardIndex],
    ) -> TestResult {
        let mut contract_service = MockSystemContractService::new();
        contract_service.expect_epoch_sync_done().never();
        contract_service
            .expect_fixed_system_parameters()
            .returning(|| {
                Ok(FixedSystemParameters {
                    epoch_duration: Duration::from_secs(600),
                    epoch_zero_end: Utc::now() + Duration::from_secs(60),
                })
            });
        contract_service
            .expect_get_epoch_and_state()
            .returning(move || Ok((0, EpochState::EpochChangeDone(Utc::now()))));
        let node = StorageNodeHandle::builder()
            .with_system_event_provider(vec![ContractEvent::EpochChangeEvent(
                EpochChangeEvent::EpochChangeStart(EpochChangeStart {
                    epoch: 1,
                    event_id: event_id_for_testing(),
                }),
            )])
            .with_shard_assignment(shard_assignment)
            .with_system_contract_service(Box::new(contract_service))
            .with_node_started(true)
            .with_initial_epoch(initial_epoch)
            .build()
            .await?;

        wait_until_events_processed(&node, 1).await?;

        Ok(())
    }
}
