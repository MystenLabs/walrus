// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Walrus storage node.

use std::{
    future::Future,
    num::{NonZero, NonZeroU16},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
};

use anyhow::{Context, anyhow, bail};
use blob_event_processor::BlobEventProcessor;
use blob_retirement_notifier::BlobRetirementNotifier;
use committee::{BeginCommitteeChangeError, EndCommitteeChangeError};
use consistency_check::StorageNodeConsistencyCheckConfig;
use epoch_change_driver::EpochChangeDriver;
use errors::{ListSymbolsError, Unavailable};
use events::{CheckpointEventPosition, event_blob_writer::EventBlobWriter};
use fastcrypto::traits::KeyPair;
use futures::{
    FutureExt as _,
    Stream,
    StreamExt,
    TryFutureExt as _,
    stream::{self, FuturesOrdered},
};
use itertools::Either;
use node_recovery::NodeRecoveryHandler;
use rand::{Rng, SeedableRng, rngs::StdRng, thread_rng};
use recovery_symbol_service::{RecoverySymbolRequest, RecoverySymbolService};
use serde::Serialize;
use start_epoch_change_finisher::StartEpochChangeFinisher;
pub use storage::{DatabaseConfig, NodeStatus, Storage};
use storage::{StorageShardLock, blob_info::PerObjectBlobInfoApi};
#[cfg(msim)]
use sui_macros::fail_point_if;
use sui_macros::{fail_point_arg, fail_point_async};
use sui_types::{base_types::ObjectID, event::EventID};
use system_events::{CompletableHandle, EVENT_ID_FOR_CHECKPOINT_EVENTS, EventHandle};
use thread_pool::{BoundedThreadPool, ThreadPoolBuilder};
use tokio::{select, sync::watch, time::Instant};
use tokio_metrics::TaskMonitor;
use tokio_util::sync::CancellationToken;
use tower::{Service, ServiceExt};
use tracing::{Instrument as _, Span, field};
use typed_store::{TypedStoreError, rocks::MetricConf};
use walrus_core::{
    BlobId,
    Epoch,
    InconsistencyProof,
    PublicKey,
    ShardIndex,
    Sliver,
    SliverPairIndex,
    SliverType,
    SymbolId,
    encoding::{
        EncodingAxis,
        EncodingConfig,
        GeneralRecoverySymbol,
        Primary,
        RecoverySymbolError,
        Secondary,
    },
    ensure,
    keys::ProtocolKeyPair,
    messages::{
        BlobPersistenceType,
        Confirmation,
        InvalidBlobIdAttestation,
        InvalidBlobIdMsg,
        ProtocolMessage,
        SignedMessage,
        SignedSyncShardRequest,
        StorageConfirmation,
        SyncShardResponse,
    },
    metadata::{
        BlobMetadataApi as _,
        BlobMetadataWithId,
        UnverifiedBlobMetadataWithId,
        VerifiedBlobMetadataWithId,
    },
};
use walrus_sdk::{
    active_committees::ActiveCommittees,
    blocklist::Blocklist,
    config::combine_rpc_urls,
    sui::{
        client::SuiReadClient,
        types::{
            BlobEvent,
            ContractEvent,
            EpochChangeDone,
            EpochChangeEvent,
            EpochChangeStart,
            GENESIS_EPOCH,
            PackageEvent,
        },
    },
};
use walrus_storage_node_client::{
    RecoverySymbolsFilter,
    SymbolIdFilter,
    api::{
        BlobStatus,
        ServiceHealthInfo,
        ShardHealthInfo,
        ShardStatus as ApiShardStatus,
        ShardStatusDetail,
        ShardStatusSummary,
        StoredOnNodeStatus,
    },
};
use walrus_utils::metrics::{Registry, TaskMonitorFamily, monitored_scope};

use self::{
    blob_sync::BlobSyncHandler,
    committee::{CommitteeService, NodeCommitteeService},
    config::StorageNodeConfig,
    contract_service::{SuiSystemContractService, SystemContractService},
    db_checkpoint::DbCheckpointManager,
    errors::{
        BlobStatusError,
        ComputeStorageConfirmationError,
        InconsistencyProofError,
        IndexOutOfRange,
        InvalidEpochError,
        RetrieveMetadataError,
        RetrieveSliverError,
        RetrieveSymbolError,
        ShardNotAssigned,
        StoreMetadataError,
        StoreSliverError,
        SyncNodeConfigError,
        SyncShardServiceError,
    },
    events::{
        EventProcessorConfig,
        EventStreamCursor,
        EventStreamElement,
        PositionedStreamEvent,
        event_blob_writer::EventBlobWriterFactory,
        event_processor::{EventProcessor, EventProcessorRuntimeConfig, SystemConfig},
    },
    metrics::{NodeMetricSet, STATUS_PENDING, STATUS_PERSISTED, TelemetryLabel as _},
    shard_sync::ShardSyncHandler,
    storage::{
        ShardStatus,
        ShardStorage,
        blob_info::{BlobInfoApi, CertifiedBlobInfoApi},
    },
    system_events::{EventManager, SuiSystemEventProvider},
};
use crate::{
    common::{config::SuiConfig, utils::should_reposition_cursor},
    utils::ShardDiffCalculator,
};

pub(crate) mod db_checkpoint;

pub mod committee;
pub mod config;
pub(crate) mod consistency_check;
pub mod contract_service;
pub mod dbtool;
pub mod events;
pub mod server;
pub mod system_events;

pub(crate) mod metrics;

mod blob_event_processor;
mod blob_retirement_notifier;
mod blob_sync;
mod epoch_change_driver;
mod node_recovery;
mod recovery_symbol_service;
mod shard_sync;
mod start_epoch_change_finisher;
mod thread_pool;

pub(crate) mod errors;
mod storage;

mod config_synchronizer;
pub use config_synchronizer::{ConfigLoader, ConfigSynchronizer, StorageNodeConfigLoader};

// The number of events are predonimently by the checkpoints, as we don't expect all checkpoints
// contain Walrus events. 20K events per recording is roughly 1 recording per 1.5 hours.
const NUM_EVENTS_PER_DIGEST_RECORDING: u64 = 20_000;
const NUM_DIGEST_BUCKETS: u64 = 10;
const CHECKPOINT_EVENT_POSITION_SCALE: u64 = 100;

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
    ) -> impl Future<Output = Result<bool, StoreMetadataError>> + Send;

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
    ) -> impl Future<Output = Result<Sliver, RetrieveSliverError>> + Send;

    /// Stores the primary or secondary encoding for a blob for a shard held by this storage node.
    fn store_sliver(
        &self,
        blob_id: BlobId,
        sliver_pair_index: SliverPairIndex,
        sliver: Sliver,
    ) -> impl Future<Output = Result<bool, StoreSliverError>> + Send;

    /// Retrieves a signed confirmation over the identifiers of the shards storing their respective
    /// sliver-pairs for their BlobIds.
    fn compute_storage_confirmation(
        &self,
        blob_id: &BlobId,
        blob_persistence_type: &BlobPersistenceType,
    ) -> impl Future<Output = Result<StorageConfirmation, ComputeStorageConfirmationError>> + Send;

    /// Verifies an inconsistency proof and provides a signed attestation for it, if valid.
    fn verify_inconsistency_proof(
        &self,
        blob_id: &BlobId,
        inconsistency_proof: InconsistencyProof,
    ) -> impl Future<Output = Result<InvalidBlobIdAttestation, InconsistencyProofError>> + Send;

    /// Retrieves a recovery symbol from a shard held by this storage node.
    ///
    /// Returns a recovery symbol for the identified symbol, if it can be constructed from the
    /// slivers stored with the storage node.
    fn retrieve_recovery_symbol(
        &self,
        blob_id: &BlobId,
        symbol_id: SymbolId,
        sliver_type: Option<SliverType>,
    ) -> impl Future<Output = Result<GeneralRecoverySymbol, RetrieveSymbolError>> + Send;

    /// Retrieves multiple recovery symbols.
    ///
    /// Attempts to retrieve multiple recovery symbols, skipping any failures that occur. Returns an
    /// error if none of the requested symbols can be retrieved.
    fn retrieve_multiple_recovery_symbols(
        &self,
        blob_id: &BlobId,
        filter: RecoverySymbolsFilter,
    ) -> impl Future<Output = Result<Vec<GeneralRecoverySymbol>, ListSymbolsError>> + Send;

    /// Retrieves the blob status for the given `blob_id`.
    fn blob_status(&self, blob_id: &BlobId) -> Result<BlobStatus, BlobStatusError>;

    /// Returns the number of shards the node is currently operating with.
    fn n_shards(&self) -> NonZeroU16;

    /// Returns the node health information of this ServiceState.
    fn health_info(&self, detailed: bool) -> impl Future<Output = ServiceHealthInfo> + Send;

    /// Returns whether the sliver is stored in the shard.
    fn sliver_status<A: EncodingAxis>(
        &self,
        blob_id: &BlobId,
        sliver_pair_index: SliverPairIndex,
    ) -> impl Future<Output = Result<StoredOnNodeStatus, RetrieveSliverError>> + Send;

    /// Returns the shard data with the provided signed request and the public key of the sender.
    fn sync_shard(
        &self,
        public_key: PublicKey,
        signed_request: SignedSyncShardRequest,
    ) -> impl Future<Output = Result<SyncShardResponse, SyncShardServiceError>> + Send;
}

/// Builder to construct a [`StorageNode`].
#[derive(Debug, Default)]
pub struct StorageNodeBuilder {
    storage: Option<Storage>,
    event_manager: Option<Box<dyn EventManager>>,
    committee_service: Option<Arc<dyn CommitteeService>>,
    contract_service: Option<Arc<dyn SystemContractService>>,
    num_checkpoints_per_blob: Option<u32>,
    config_loader: Option<Arc<dyn ConfigLoader>>,
}

impl StorageNodeBuilder {
    /// Creates a new builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the config loader for the node.
    pub fn with_config_loader(mut self, config_loader: Option<Arc<dyn ConfigLoader>>) -> Self {
        self.config_loader = config_loader;
        self
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
        contract_service: Arc<dyn SystemContractService>,
    ) -> Self {
        self.contract_service = Some(contract_service);
        self
    }

    /// Sets the number of checkpoints to use per event blob.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn with_num_checkpoints_per_blob(mut self, num_checkpoints_per_blob: u32) -> Self {
        self.num_checkpoints_per_blob = Some(num_checkpoints_per_blob);
        self
    }

    /// Sets the [`CommitteeService`] used with the node.
    pub fn with_committee_service(mut self, service: Arc<dyn CommitteeService>) -> Self {
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

            if config.use_legacy_event_provider {
                Box::new(SuiSystemEventProvider::new(
                    read_client.clone(),
                    sui_config.event_polling_interval,
                ))
            } else {
                let rpc_addresses =
                    combine_rpc_urls(&sui_config.rpc, &sui_config.additional_rpc_endpoints);
                let processor_config = EventProcessorRuntimeConfig {
                    rpc_addresses,
                    event_polling_interval: sui_config.event_polling_interval,
                    db_path: config.storage_path.join("events"),
                    rpc_fallback_config: sui_config.rpc_fallback_config.clone(),
                    db_config: config.db_config.clone(),
                };
                let system_config = SystemConfig {
                    system_pkg_id: read_client.get_system_package_id(),
                    system_object_id: sui_config.contract_config.system_object,
                    staking_object_id: sui_config.contract_config.staking_object,
                };
                Box::new(
                    EventProcessor::new(
                        &config.event_processor_config,
                        processor_config,
                        system_config,
                        &metrics_registry,
                    )
                    .await?,
                )
            }
        };

        let committee_service: Arc<dyn CommitteeService> =
            if let Some(service) = self.committee_service {
                service
            } else {
                let (read_client, _) = sui_config_and_client
                    .expect("this is always created if self.committee_service_factory.is_none()");
                let service = NodeCommitteeService::builder()
                    .local_identity(protocol_key_pair.public().clone())
                    .config(config.blob_recovery.committee_service_config.clone())
                    .metrics_registry(&metrics_registry)
                    .build(read_client)
                    .await?;
                Arc::new(service)
            };

        let contract_service: Arc<dyn SystemContractService> = if let Some(service) =
            self.contract_service
        {
            service
        } else {
            Arc::new(
                SuiSystemContractService::builder()
                    .metrics_registry(metrics_registry.clone())
                    .balance_check_frequency(config.balance_check.interval)
                    .balance_check_warning_threshold(config.balance_check.warning_threshold_mist)
                    .build_from_config(
                        config.sui.as_ref().expect("Sui config must be provided"),
                        committee_service.clone(),
                    )
                    .await?,
            )
        };

        let node_params = NodeParameters {
            pre_created_storage: self.storage,
            num_checkpoints_per_blob: self.num_checkpoints_per_blob,
        };

        StorageNode::new(
            config,
            event_manager,
            committee_service,
            contract_service,
            &metrics_registry,
            self.config_loader,
            node_params,
        )
        .await
    }
}

pub(crate) async fn create_read_client(
    sui_config: &SuiConfig,
) -> Result<SuiReadClient, anyhow::Error> {
    Ok(sui_config.new_read_client().await?)
}

/// A Walrus storage node, responsible for 1 or more shards on Walrus.
#[derive(Debug)]
pub struct StorageNode {
    inner: Arc<StorageNodeInner>,
    blob_sync_handler: Arc<BlobSyncHandler>,
    shard_sync_handler: ShardSyncHandler,
    epoch_change_driver: EpochChangeDriver,
    start_epoch_change_finisher: StartEpochChangeFinisher,
    node_recovery_handler: NodeRecoveryHandler,
    blob_event_processor: BlobEventProcessor,
    event_blob_writer_factory: Option<EventBlobWriterFactory>,
    config_synchronizer: Option<Arc<ConfigSynchronizer>>,
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
    is_shutting_down: AtomicBool,
    blocklist: Arc<Blocklist>,
    node_capability: ObjectID,
    blob_retirement_notifier: Arc<BlobRetirementNotifier>,
    symbol_service: RecoverySymbolService,
    thread_pool: BoundedThreadPool,
    registry: Registry,
    latest_event_epoch: AtomicU32, // The epoch of the latest event processed by the node.
    consistency_check_config: StorageNodeConsistencyCheckConfig,
    checkpoint_manager: Option<Arc<DbCheckpointManager>>,
}

/// Parameters for configuring and initializing a node.
///
/// This struct contains optional configuration parameters that can be used
/// to customize the behavior of a node during its creation or runtime.
#[derive(Debug, Default)]
pub struct NodeParameters {
    // For testing purposes. TODO(#703): remove.
    pre_created_storage: Option<Storage>,
    // Number of checkpoints per blob to use when creating event blobs.
    // If not provided, the default value will be used.
    num_checkpoints_per_blob: Option<u32>,
}

/// The action to take when the node transitions to a new committee.
#[derive(Debug)]
pub enum BeginCommitteeChangeAction {
    /// The node should execute the epoch change.
    ExecuteEpochChange,
    /// The node should skip the epoch change.
    SkipEpochChange,
    /// The node should enter recovery mode.
    EnterRecoveryMode,
}

impl StorageNode {
    async fn new(
        config: &StorageNodeConfig,
        event_manager: Box<dyn EventManager>,
        committee_service: Arc<dyn CommitteeService>,
        contract_service: Arc<dyn SystemContractService>,
        registry: &Registry,
        config_loader: Option<Arc<dyn ConfigLoader>>,
        node_params: NodeParameters,
    ) -> Result<Self, anyhow::Error> {
        let start_time = Instant::now();
        let metrics = NodeMetricSet::new(registry);

        let node_capability = contract_service
            .get_node_capability_object(config.storage_node_cap)
            .await?;

        tracing::info!(
            walrus.node.node_id = %node_capability.node_id.to_hex_uncompressed(),
            walrus.node.capability_id = %node_capability.id.to_hex_uncompressed(),
            "selected storage node capability object"
        );
        walrus_utils::with_label!(
            metrics.node_id,
            node_capability.node_id.to_hex_uncompressed()
        )
        .set(1);

        let config_synchronizer =
            config
                .config_synchronizer
                .enabled
                .then_some(Arc::new(ConfigSynchronizer::new(
                    contract_service.clone(),
                    committee_service.clone(),
                    config.config_synchronizer.interval,
                    node_capability.id,
                    config_loader,
                )));

        contract_service
            .sync_node_params(config, node_capability.id)
            .await
            .or_else(|e| match e {
                SyncNodeConfigError::ProtocolKeyPairRotationRequired => Err(e),
                _ => {
                    tracing::warn!(error = ?e, "failed to sync node params");
                    Ok(())
                }
            })?;

        let encoding_config = committee_service.encoding_config().clone();

        let storage = if let Some(storage) = node_params.pre_created_storage {
            storage
        } else {
            Storage::open(
                config.storage_path.as_path(),
                config.db_config.clone(),
                MetricConf::new("storage"),
                registry.clone(),
            )?
        };
        tracing::info!("successfully opened the node database");

        let thread_pool = ThreadPoolBuilder::default()
            .max_concurrent(config.thread_pool.max_concurrent_tasks)
            .metrics_registry(registry.clone())
            .build_bounded();
        let blocklist: Arc<Blocklist> = Arc::new(Blocklist::new(&config.blocklist_path)?);
        let checkpoint_manager = match DbCheckpointManager::new(
            storage.get_db(),
            config.checkpoint_config.clone(),
        )
        .await
        {
            Ok(manager) => Some(Arc::new(manager)),
            Err(e) => {
                tracing::warn!(?e, "Failed to initialize checkpoint manager");
                None
            }
        };
        let inner = Arc::new(StorageNodeInner {
            protocol_key_pair: config
                .protocol_key_pair
                .get()
                .expect("protocol key pair must already be loaded")
                .clone(),
            storage,
            event_manager,
            contract_service: contract_service.clone(),
            current_epoch: watch::Sender::new(committee_service.get_epoch()),
            committee_service,
            metrics,
            start_time,
            is_shutting_down: false.into(),
            blocklist: blocklist.clone(),
            node_capability: node_capability.id,
            blob_retirement_notifier: Arc::new(BlobRetirementNotifier::new()),
            symbol_service: RecoverySymbolService::new(
                config.blob_recovery.max_proof_cache_elements,
                encoding_config.clone(),
                thread_pool.clone(),
                registry,
            ),
            thread_pool,
            encoding_config,
            registry: registry.clone(),
            latest_event_epoch: AtomicU32::new(0),
            consistency_check_config: config.consistency_check.clone(),
            checkpoint_manager,
        });

        blocklist.start_refresh_task();

        inner.init_gauges()?;

        let blob_sync_handler = Arc::new(BlobSyncHandler::new(
            inner.clone(),
            config.blob_recovery.max_concurrent_blob_syncs,
            config.blob_recovery.max_concurrent_sliver_syncs,
        ));

        let shard_sync_handler =
            ShardSyncHandler::new(inner.clone(), config.shard_sync_config.clone());
        // Upon restart, resume any ongoing blob syncs if there is any.
        shard_sync_handler.restart_syncs().await?;

        let system_parameters = contract_service.fixed_system_parameters().await?;
        let epoch_change_driver = EpochChangeDriver::new(
            system_parameters,
            contract_service.clone(),
            StdRng::seed_from_u64(thread_rng().r#gen()),
        );

        let start_epoch_change_finisher = StartEpochChangeFinisher::new(inner.clone());

        let node_recovery_handler = NodeRecoveryHandler::new(
            inner.clone(),
            blob_sync_handler.clone(),
            config.node_recovery_config.clone(),
        );
        node_recovery_handler.restart_recovery().await?;

        let blob_event_processor = BlobEventProcessor::new(
            inner.clone(),
            blob_sync_handler.clone(),
            config.node_blob_event_processor_config.num_workers,
        );

        tracing::debug!(
            "num_checkpoints_per_blob for event blobs: {:?}",
            node_params.num_checkpoints_per_blob
        );

        let last_certified_event_blob = contract_service.last_certified_event_blob().await?;
        let event_blob_writer_factory = if !config.disable_event_blob_writer {
            Some(EventBlobWriterFactory::new(
                &config.storage_path,
                &config.db_config,
                inner.clone(),
                registry,
                node_params.num_checkpoints_per_blob,
                last_certified_event_blob,
                config.num_uncertified_blob_threshold,
            )?)
        } else {
            None
        };

        Ok(StorageNode {
            inner,
            blob_sync_handler,
            shard_sync_handler,
            epoch_change_driver,
            start_epoch_change_finisher,
            node_recovery_handler,
            blob_event_processor,
            event_blob_writer_factory,
            config_synchronizer,
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
                Err(err) => {
                    tracing::error!("event processing terminated with an error: {:?}", err);
                    return Err(err);
                }
            },
            _ = cancel_token.cancelled() => {
                if let Some(checkpoint_manager) = self.checkpoint_manager() {
                    checkpoint_manager.shutdown();
                }
                self.inner.shut_down();
                self.blob_sync_handler.cancel_all().await?;
            },
            blob_sync_result = self.blob_sync_handler.spawn_task_monitor() => {
                match blob_sync_result {
                    Ok(()) => unreachable!("blob sync task monitor never returns"),
                    Err(e) => {
                        if e.is_panic() {
                            std::panic::resume_unwind(e.into_panic());
                        }
                        return Err(e.into());
                    },
                }
            },
            config_synchronizer_result = async {
                if let Some(c) = self.config_synchronizer.as_ref() {
                    c.run().await
                } else {
                    // Never complete if no config synchronizer
                    std::future::pending().await
                }
            } => {
                tracing::info!("config monitor task ended");
                match config_synchronizer_result {
                    Ok(()) => unreachable!("config monitor never returns"),
                    Err(e) => return Err(e.into()),
                }
            }
        }

        Ok(())
    }

    /// Returns the checkpoint manager for the node.
    pub fn checkpoint_manager(&self) -> Option<Arc<DbCheckpointManager>> {
        self.inner.checkpoint_manager.clone()
    }

    /// Returns the shards which the node currently manages in its storage.
    ///
    /// This neither considers the current shard assignment from the Walrus contracts nor the status
    /// of the local shard storage.
    pub async fn existing_shards(&self) -> Vec<ShardIndex> {
        self.inner.storage.existing_shards().await
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
        event_blob_writer_cursor: EventStreamCursor,
        storage_node_cursor: EventStreamCursor,
        event_blob_writer: &mut Option<EventBlobWriter>,
    ) -> anyhow::Result<(
        Pin<Box<dyn Stream<Item = PositionedStreamEvent> + Send + Sync + '_>>,
        u64,
    )> {
        let event_cursor = std::cmp::min(storage_node_cursor, event_blob_writer_cursor);
        let event_stream = self.inner.event_manager.events(event_cursor).await?;
        let event_index = event_cursor.element_index;

        let init_state = self.inner.event_manager.init_state(event_cursor).await?;
        let Some(init_state) = init_state else {
            return Ok((Pin::from(event_stream), event_index));
        };

        let actual_event_index = init_state.event_cursor.element_index;

        let storage_index = storage_node_cursor.element_index;
        let mut storage_node_cursor_repositioned = false;
        if should_reposition_cursor(storage_index, actual_event_index) {
            tracing::info!(
                "Repositioning storage node cursor from {} to {}",
                storage_index,
                actual_event_index
            );
            self.inner.reposition_event_cursor(
                init_state
                    .event_cursor
                    .event_id
                    .unwrap_or(EVENT_ID_FOR_CHECKPOINT_EVENTS),
                actual_event_index,
            )?;
            storage_node_cursor_repositioned = true;
        }

        let mut event_blob_writer_repositioned = false;
        if let Some(writer) = event_blob_writer {
            let event_blob_writer_index = event_blob_writer_cursor.element_index;
            if should_reposition_cursor(event_blob_writer_index, actual_event_index) {
                tracing::info!(
                    "Repositioning event blob writer cursor from {} to {}",
                    event_blob_writer_index,
                    actual_event_index
                );
                writer.update(init_state).await?;
                event_blob_writer_repositioned = true;
            }
        }

        if !storage_node_cursor_repositioned && !event_blob_writer_repositioned {
            ensure!(
                event_index == actual_event_index,
                "event stream out of sync"
            );
        }

        Ok((Pin::from(event_stream), actual_event_index))
    }

    async fn storage_node_cursor(&self) -> anyhow::Result<EventStreamCursor> {
        let storage = &self.inner.storage;
        let (from_event_id, next_event_index) = storage
            .get_event_cursor_and_next_index()?
            .map_or((None, 0), |e| (Some(e.event_id()), e.next_event_index()));
        Ok(EventStreamCursor::new(from_event_id, next_event_index))
    }

    #[cfg(not(msim))]
    async fn get_storage_node_cursor(&self) -> anyhow::Result<EventStreamCursor> {
        self.storage_node_cursor().await
    }

    // A version of `get_storage_node_cursor` that can be used in simtest which can control the
    // initial cursor.
    #[cfg(msim)]
    async fn get_storage_node_cursor(&self) -> anyhow::Result<EventStreamCursor> {
        let mut cursor = self.storage_node_cursor().await?;
        fail_point_arg!(
            "storage_node_initial_cursor",
            |update_cursor: EventStreamCursor| {
                tracing::info!("updating storage node cursor to {:?}", update_cursor);
                cursor = update_cursor;
            }
        );
        Ok(cursor)
    }

    async fn process_events(&self) -> anyhow::Result<()> {
        let writer_cursor = match self.event_blob_writer_factory {
            Some(ref factory) => factory.event_cursor().unwrap_or_default(),
            None => EventStreamCursor::new(None, u64::MAX),
        };
        let storage_node_cursor = self.get_storage_node_cursor().await?;

        let mut event_blob_writer = match &self.event_blob_writer_factory {
            Some(factory) => Some(factory.create().await?),
            None => None,
        };
        let (event_stream, next_event_index) = self
            .continue_event_stream(writer_cursor, storage_node_cursor, &mut event_blob_writer)
            .await?;

        let index_stream = stream::iter(next_event_index..);
        let mut maybe_epoch_at_start = Some(self.inner.committee_service.get_epoch());

        let mut indexed_element_stream = index_stream.zip(event_stream);
        let task_monitors = TaskMonitorFamily::<&'static str>::new(self.inner.registry.clone());
        let stream_poll_monitor = task_monitors
            .get_or_insert_with_task_name(&"event_stream_poll", || {
                "poll_indexed_element_stream".to_string()
            });

        // Important: Events must be handled consecutively and in order to prevent (intermittent)
        // invariant violations and interference between different events.
        while let Some((element_index, stream_element)) =
            TaskMonitor::instrument(&stream_poll_monitor, async {
                indexed_element_stream.next().await
            })
            .await
        {
            let event_label: &'static str = stream_element.element.label();
            let monitor = task_monitors.get_or_insert_with_task_name(&event_label, || {
                format!("process_event {}", event_label)
            });

            let task = async {
                fail_point_arg!("event_processing_epoch_check", |epoch: Epoch| {
                    tracing::info!("updating epoch check to {:?}", epoch);
                    maybe_epoch_at_start = Some(epoch);
                });

                let should_write = element_index >= writer_cursor.element_index;
                let should_process = element_index >= storage_node_cursor.element_index;
                ensure!(should_write || should_process, "event stream out of sync");

                if should_process {
                    sui_macros::fail_point!("process-event-before");
                    self.process_event(
                        stream_element.clone(),
                        element_index,
                        &mut maybe_epoch_at_start,
                    )
                    .await?;
                    sui_macros::fail_point!("process-event-after");
                }

                if should_write {
                    if let Some(writer) = &mut event_blob_writer {
                        sui_macros::fail_point!("write-event-before");
                        writer.write(stream_element.clone(), element_index).await?;
                        sui_macros::fail_point!("write-event-after");
                    }
                }

                anyhow::Result::<()>::Ok(())
            };

            TaskMonitor::instrument(&monitor, task).await?;
        }

        bail!("event stream for blob events stopped")
    }

    /// Process an event.
    ///
    /// When `maybe_epoch_at_start` is provided, it indicates the node has not processed any events
    /// yet, and this function needs to check if the node is severely lagging behind.
    #[tracing::instrument(skip_all)]
    async fn process_event(
        &self,
        stream_element: PositionedStreamEvent,
        element_index: u64,
        maybe_epoch_at_start: &mut Option<Epoch>,
    ) -> anyhow::Result<()> {
        monitored_scope::monitored_scope("ProcessEvent");
        let node_status = self.inner.storage.node_status()?;
        let span = tracing::info_span!(
            parent: &Span::current(),
            "blob_store receive",
            "otel.kind" = "CONSUMER",
            "otel.status_code" = field::Empty,
            "otel.status_message" = field::Empty,
            "messaging.operation.type" = "receive",
            "messaging.system" = "sui",
            "messaging.destination.name" = "blob_store",
            "messaging.client.id" = %self.inner.public_key(),
            "walrus.event.index" = element_index,
            "walrus.event.tx_digest" = ?stream_element.element.event_id()
                .map(|c| c.tx_digest),
            "walrus.event.checkpoint_seq" = ?stream_element.checkpoint_event_position
                .checkpoint_sequence_number,
            "walrus.event.kind" = stream_element.element.label(),
            "walrus.blob_id" = ?stream_element.element.blob_id(),
            "walrus.node_status" = %node_status,
            "error.type" = field::Empty,
        );

        if maybe_epoch_at_start.is_some() {
            if let EventStreamElement::ContractEvent(ref event) = stream_element.element {
                self.check_if_node_lagging_and_enter_recovery_mode(
                    event,
                    node_status,
                    maybe_epoch_at_start,
                )?;
            }
        }

        // Ignore the error here since this is a best effort operation, and we don't
        // want any error from it to stop the node.
        if let Err(error) =
            self.maybe_record_event_source(element_index, &stream_element.checkpoint_event_position)
        {
            tracing::warn!(?error, "failed to record event source");
        }

        let event_handle = EventHandle::new(
            element_index,
            stream_element.element.event_id(),
            self.inner.clone(),
        );
        self.process_event_impl(event_handle, stream_element.clone())
            .inspect_err(|err| {
                let span = tracing::Span::current();
                span.record("otel.status_code", "error");
                span.record("otel.status_message", field::display(err));
            })
            .instrument(span)
            .await
    }

    /// Checks if the node is severely lagging behind.
    ///
    /// If so, the node will enter RecoveryCatchUp mode, and try to catch up with events until
    /// the latest epoch as fast as possible.
    fn check_if_node_lagging_and_enter_recovery_mode(
        &self,
        event: &ContractEvent,
        node_status: NodeStatus,
        maybe_epoch_at_start: &mut Option<Epoch>,
    ) -> anyhow::Result<()> {
        let Some(epoch_at_start) = *maybe_epoch_at_start else {
            return Ok(());
        };

        // For blob extension events, the epoch is the event's original
        // certified epoch, and not the current epoch. Skip node lagging check
        // for blob extension events.
        if event.is_blob_extension() {
            return Ok(());
        }

        // Update initial latest event epoch. This is the first event the
        // node processes.
        self.inner
            .latest_event_epoch
            .store(event.event_epoch(), Ordering::SeqCst);

        tracing::debug!("checking the first contract event if we're severely lagging");

        // Clear the starting epoch, so that we won't make this check again in the current run.
        *maybe_epoch_at_start = None;

        // Checks if the node is severely lagging behind.
        if node_status != NodeStatus::RecoveryCatchUp && event.event_epoch() + 1 < epoch_at_start {
            tracing::warn!(
                "the current epoch ({}) is far ahead of the event epoch ({}); \
                                node entering recovery mode",
                epoch_at_start,
                event.event_epoch()
            );
            self.inner.set_node_status(NodeStatus::RecoveryCatchUp)?;
        }

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn process_event_impl(
        &self,
        event_handle: EventHandle,
        stream_element: PositionedStreamEvent,
    ) -> anyhow::Result<()> {
        monitored_scope::monitored_scope("ProcessEvent::Impl");
        let _timer_guard = walrus_utils::with_label!(
            self.inner.metrics.event_process_duration_seconds,
            stream_element.element.label()
        )
        .start_timer();
        fail_point_async!("before-process-event-impl");
        match stream_element.element {
            EventStreamElement::ContractEvent(ContractEvent::BlobEvent(blob_event)) => {
                self.process_blob_event(event_handle, blob_event).await?;
            }
            EventStreamElement::ContractEvent(ContractEvent::EpochChangeEvent(
                epoch_change_event,
            )) => {
                self.process_epoch_change_event(event_handle, epoch_change_event)
                    .await?;
            }
            EventStreamElement::ContractEvent(ContractEvent::PackageEvent(package_event)) => {
                self.process_package_event(event_handle, package_event)
                    .await?;
            }
            EventStreamElement::ContractEvent(ContractEvent::DenyListEvent(_event)) => {
                // TODO: Implement DenyListEvent handling (WAL-424)
                event_handle.mark_as_complete();
            }
            EventStreamElement::CheckpointBoundary => {
                event_handle.mark_as_complete();
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn process_blob_event(
        &self,
        event_handle: EventHandle,
        blob_event: BlobEvent,
    ) -> anyhow::Result<()> {
        monitored_scope::monitored_scope("ProcessEvent::BlobEvent");
        tracing::debug!(?blob_event, "{} event received", blob_event.name());
        self.blob_event_processor
            .process_event(event_handle, blob_event)
            .await?;
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn process_epoch_change_event(
        &self,
        event_handle: EventHandle,
        epoch_change_event: EpochChangeEvent,
    ) -> anyhow::Result<()> {
        monitored_scope::monitored_scope("ProcessEvent::EpochChangeEvent");
        match epoch_change_event {
            EpochChangeEvent::ShardsReceived(_) => {
                tracing::debug!(
                    ?epoch_change_event,
                    "{} event received",
                    epoch_change_event.name()
                );
            }
            _ => {
                tracing::info!(
                    ?epoch_change_event,
                    "{} event received",
                    epoch_change_event.name()
                );
            }
        }
        match epoch_change_event {
            EpochChangeEvent::EpochParametersSelected(event) => {
                monitored_scope::monitored_scope(
                    "ProcessEvent::EpochChangeEvent::EpochParametersSelected",
                );
                self.epoch_change_driver
                    .cancel_scheduled_voting_end(event.next_epoch);
                self.epoch_change_driver.schedule_initiate_epoch_change(
                    NonZero::new(event.next_epoch).expect("the next epoch is always non-zero"),
                );
                event_handle.mark_as_complete();
            }
            EpochChangeEvent::EpochChangeStart(event) => {
                monitored_scope::monitored_scope(
                    "ProcessEvent::EpochChangeEvent::EpochChangeStart",
                );
                fail_point_async!("epoch_change_start_entry");
                self.process_epoch_change_start_event(event_handle, &event)
                    .await?;
            }
            EpochChangeEvent::EpochChangeDone(event) => {
                monitored_scope::monitored_scope("ProcessEvent::EpochChangeEvent::EpochChangeDone");
                self.process_epoch_change_done_event(&event).await?;
                event_handle.mark_as_complete();
            }
            EpochChangeEvent::ShardsReceived(_) => {
                monitored_scope::monitored_scope("ProcessEvent::EpochChangeEvent::ShardsReceived");
                event_handle.mark_as_complete();
            }
            EpochChangeEvent::ShardRecoveryStart(_) => {
                monitored_scope::monitored_scope(
                    "ProcessEvent::EpochChangeEvent::ShardRecoveryStart",
                );
                event_handle.mark_as_complete();
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn process_package_event(
        &self,
        event_handle: EventHandle,
        package_event: PackageEvent,
    ) -> anyhow::Result<()> {
        monitored_scope::monitored_scope("ProcessEvent::PackageEvent");
        tracing::info!(?package_event, "{} event received", package_event.name());
        match package_event {
            PackageEvent::ContractUpgraded(_event) => {
                self.inner
                    .contract_service
                    .refresh_contract_package()
                    .await?;
                event_handle.mark_as_complete();
            }
            PackageEvent::ContractUpgradeProposed(_) => {
                event_handle.mark_as_complete();
            }
            PackageEvent::ContractUpgradeQuorumReached(_) => {
                event_handle.mark_as_complete();
            }
            _ => bail!("unknown package event type: {:?}", package_event),
        }
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    fn next_event_index(&self) -> anyhow::Result<u64> {
        Ok(self
            .inner
            .storage
            .get_event_cursor_and_next_index()?
            .map_or(0, |e| e.next_event_index()))
    }

    #[tracing::instrument(skip_all)]
    async fn process_epoch_change_start_event(
        &self,
        event_handle: EventHandle,
        event: &EpochChangeStart,
    ) -> anyhow::Result<()> {
        // There shouldn't be an epoch change event for the genesis epoch.
        assert!(event.epoch != GENESIS_EPOCH);

        if let Some(c) = self.config_synchronizer.as_ref() {
            c.sync_node_params().await?;
        }

        // Irrespective of whether we are in this epoch, we can cancel any scheduled calls to change
        // to or end voting for the epoch identified by the event, as we're already in that epoch.
        self.epoch_change_driver
            .cancel_scheduled_voting_end(event.epoch);
        self.epoch_change_driver
            .cancel_scheduled_epoch_change_initiation(event.epoch);

        // Here we need to wait for the previous shard removal to finish so that for the case
        // where same shard is moved in again, we don't have shard removal and move-in running
        // concurrently.
        //
        // Note that we expect this call to finish quickly because removing RocksDb column
        // families is supposed to be fast, and we have an entire epoch duration to do so. By
        // the time next epoch starts, the shard removal task should have completed.
        self.start_epoch_change_finisher
            .wait_until_previous_task_done()
            .await;

        if self.inner.consistency_check_config.enable_consistency_check {
            if let Err(err) = consistency_check::schedule_background_consistency_check(
                self.inner.clone(),
                self.blob_sync_handler.clone(),
                event.epoch,
            )
            .await
            {
                tracing::warn!(
                    ?err,
                    epoch = %event.epoch,
                    "failed to schedule background blob info consistency check"
                );
            }
        }

        // During epoch change, we need to lock the read access to shard map until all the new
        // shards are created.
        let shard_map_lock = self.inner.storage.lock_shards().await;

        // Now the general tasks around epoch change are done. Next, entering epoch change logic
        // to bring the node state to the next epoch.
        self.execute_epoch_change(event_handle, event, shard_map_lock)
            .await?;

        // Update the latest event epoch to the new epoch. Now, blob syncs will use this epoch to
        // check for shard ownership.
        self.inner
            .latest_event_epoch
            .store(event.epoch, Ordering::SeqCst);

        Ok(())
    }

    /// Storage node execution of the epoch change start event, to bring the node state to the next
    /// epoch.
    async fn execute_epoch_change(
        &self,
        event_handle: EventHandle,
        event: &EpochChangeStart,
        shard_map_lock: StorageShardLock,
    ) -> anyhow::Result<()> {
        if self.inner.storage.node_status()? == NodeStatus::RecoveryCatchUp {
            self.execute_epoch_change_while_catching_up(event_handle, event, shard_map_lock)
                .await?;
        } else {
            match self.begin_committee_change(event.epoch).await? {
                BeginCommitteeChangeAction::ExecuteEpochChange => {
                    self.execute_epoch_change_when_node_is_in_sync(
                        event_handle,
                        event,
                        shard_map_lock,
                    )
                    .await?;
                }
                BeginCommitteeChangeAction::SkipEpochChange => {
                    event_handle.mark_as_complete();
                    return Ok(());
                }
                BeginCommitteeChangeAction::EnterRecoveryMode => {
                    tracing::info!("storage node entering recovery mode during epoch change start");
                    sui_macros::fail_point!("fail-point-enter-recovery-mode");
                    self.inner.set_node_status(NodeStatus::RecoveryCatchUp)?;
                    self.execute_epoch_change_while_catching_up(
                        event_handle,
                        event,
                        shard_map_lock,
                    )
                    .await?;
                }
            };
        }

        Ok(())
    }

    /// Executes the epoch change logic while the node is in
    /// [`RecoveryCatchUp`][NodeStatus::RecoveryCatchUp] mode.
    async fn execute_epoch_change_while_catching_up(
        &self,
        event_handle: EventHandle,
        event: &EpochChangeStart,
        shard_map_lock: StorageShardLock,
    ) -> anyhow::Result<()> {
        self.inner
            .committee_service
            .begin_committee_change_to_latest_committee()
            .await?;

        // For blobs that are expired in the new epoch, sends a notification to all the tasks
        // that may be affected by the blob expiration.
        self.inner
            .blob_retirement_notifier
            .epoch_change_notify_all_pending_blob_retirement(self.inner.clone())?;

        if event.epoch < self.inner.current_epoch() {
            // We have not caught up to the latest epoch yet, so we can skip the event.
            event_handle.mark_as_complete();
            return Ok(());
        }

        tracing::info!(
            epoch = %event.epoch,
            "processing event during node RecoveryCatchUp reaches the latest epoch"
        );

        let active_committees = self.inner.committee_service.active_committees();
        if !active_committees
            .current_committee()
            .contains(self.inner.public_key())
        {
            tracing::info!("node is not in the current committee, set node status to 'Standby'");
            self.inner.set_node_status(NodeStatus::Standby)?;
            event_handle.mark_as_complete();
            return Ok(());
        }

        if !active_committees
            .previous_committee()
            .is_some_and(|c| c.contains(self.inner.public_key()))
        {
            tracing::info!("node just became a new committee member, process shard changes");
            // This node just became a new committee member. Process shard changes as a new
            // committee member.
            self.process_shard_changes_in_new_epoch(event_handle, event, true, shard_map_lock)
                .await?;
        } else {
            tracing::info!("start node recovery to catch up to the latest epoch");
            // This node is a past and current committee member. Start node recovery to catch up
            // to the latest epoch.
            self.start_node_recovery(event_handle, event, shard_map_lock)
                .await?;
        }

        Ok(())
    }

    /// Executes the epoch change logic when the node is up-to-date with the epoch and event
    /// processing.
    async fn execute_epoch_change_when_node_is_in_sync(
        &self,
        event_handle: EventHandle,
        event: &EpochChangeStart,
        shard_map_lock: StorageShardLock,
    ) -> anyhow::Result<()> {
        // For blobs that are expired in the new epoch, sends a notification to all the tasks
        // that may be affected by the blob expiration.
        self.inner
            .blob_retirement_notifier
            .epoch_change_notify_all_pending_blob_retirement(self.inner.clone())?;

        // Cancel all blob syncs for blobs that are expired in the *current epoch*.
        self.blob_sync_handler
            .cancel_all_expired_syncs_and_mark_events_completed()
            .await?;

        let is_in_current_committee = self
            .inner
            .committee_service
            .active_committees()
            .current_committee()
            .contains(self.inner.public_key());
        let is_new_node_joining_committee =
            self.inner.storage.node_status()? == NodeStatus::Standby && is_in_current_committee;

        if !is_in_current_committee {
            // The reason we set the node status to Standby here is that the node is not in the
            // current committee, and therefore from this epoch, it won't sync any blob
            // metadata. In the case it becomes committee member again, it needs to sync blob
            // metadata again.
            self.inner.set_node_status(NodeStatus::Standby)?;
        }

        if is_new_node_joining_committee {
            tracing::info!(
                "node just became a committee member; changing status from 'Standby' to 'Active' \
                and processing shard changes"
            );
        }

        self.process_shard_changes_in_new_epoch(
            event_handle,
            event,
            is_new_node_joining_committee,
            shard_map_lock,
        )
        .await
    }

    /// Starts the node recovery process.
    ///
    /// As all functions that are passed an [`EventHandle`], this is responsible for marking the
    /// event as completed.
    async fn start_node_recovery(
        &self,
        event_handle: EventHandle,
        event: &EpochChangeStart,
        shard_map_lock: StorageShardLock,
    ) -> anyhow::Result<()> {
        self.inner
            .set_node_status(NodeStatus::RecoveryInProgress(event.epoch))?;

        let public_key = self.inner.public_key();
        let storage = &self.inner.storage;
        let committees = self.inner.committee_service.active_committees();
        let shard_diff_calculator =
            ShardDiffCalculator::new(&committees, public_key, shard_map_lock.existing_shards());

        // Since the node is doing a full recovery, its local shards may be out of sync with the
        // contract for multiple epochs. Here we need to make sure that all the shards that is
        // assigned to the node in the latest epoch are created.
        //
        // Note that the shard_map_lock will be unlocked after this function returns.
        self.inner
            .create_storage_for_shards_in_background(
                shard_diff_calculator.all_owned_shards().to_vec(),
                shard_map_lock,
            )
            .await?;

        // Given that the storage node is severely lagging, the node may contain shards in outdated
        // status. We need to set the status of all currently owned shards to `Active` despite
        // their current status. Node recovery will recover all the missing certified blobs in these
        // shards in a crash-tolerant manner.
        // Note that node recovery can only start if the event epoch matches the latest epoch.
        for shard in self.inner.owned_shards_at_latest_epoch() {
            storage
                .shard_storage(shard)
                .await
                .expect("we just create all storage, it must exist")
                .set_active_status()?;
        }

        // For shards that just moved out, we need to lock them to not store more data in them.
        for shard in shard_diff_calculator.shards_to_lock() {
            if let Some(shard_storage) = self.inner.storage.shard_storage(*shard).await {
                shard_storage
                    .lock_shard_for_epoch_change()
                    .context("failed to lock shard")?;
            }
        }

        // Initiate blob sync for all certified blobs we've tracked so far. After this is done,
        // the node will be in a state where it has all the shards and blobs that it should have.
        self.node_recovery_handler
            .start_node_recovery(event.epoch)
            .await?;

        // Last but not least, we need to remove any shards that are no longer owned by the node.
        let shards_to_remove = shard_diff_calculator.shards_to_remove();
        if !shards_to_remove.is_empty() {
            self.start_epoch_change_finisher
                .start_finish_epoch_change_tasks(
                    event_handle,
                    event,
                    shard_diff_calculator.shards_to_remove().to_vec(),
                    committees,
                    true,
                );
        } else {
            event_handle.mark_as_complete();
        }

        Ok(())
    }

    /// Initiates a committee transition to a new epoch. Upon the return of this function, the
    /// latest committee on chain is updated to the new node.
    ///
    /// Returns the action to execute epoch change based on the result of committee service,
    /// including possible actions to enter recovery mode due to the node being severely lagging.
    #[tracing::instrument(skip_all)]
    async fn begin_committee_change(
        &self,
        epoch: Epoch,
    ) -> Result<BeginCommitteeChangeAction, BeginCommitteeChangeError> {
        match self
            .inner
            .committee_service
            .begin_committee_change(epoch)
            .await
        {
            Ok(()) => {
                tracing::info!(
                    walrus.epoch = epoch,
                    "successfully started a transition to a new epoch"
                );
                self.inner.current_epoch.send_replace(epoch);
                Ok(BeginCommitteeChangeAction::ExecuteEpochChange)
            }
            Err(BeginCommitteeChangeError::EpochIsTheSameAsCurrent) => {
                tracing::info!(
                    walrus.epoch = epoch,
                    "epoch change event was for the epoch we already fetched the committee info, \
                    directly executing epoch change"
                );
                Ok(BeginCommitteeChangeAction::ExecuteEpochChange)
            }
            Err(BeginCommitteeChangeError::ChangeAlreadyInProgress) => {
                // TODO(WAL-479): can this condition actually happen? It seems that the only case
                // this could happen is when the node calls begin_committee_change() multiple times
                // on the same epoch in the same life time of the storage node. This is not expected
                // and indicates software bug (convert this to debug assertion?).
                tracing::info!(
                    walrus.epoch = epoch,
                    committee_epoch = self.inner.committee_service.get_epoch(),
                    "epoch change is already in progress, do not need to re-execute epoch change"
                );
                Ok(BeginCommitteeChangeAction::SkipEpochChange)
            }
            Err(BeginCommitteeChangeError::EpochIsLess {
                latest_epoch,
                requested_epoch,
            }) => {
                debug_assert!(requested_epoch < latest_epoch);
                // We are processing a backlog of events. Since the committee service has a
                // more recent committee. In this situation, we have already lost the information
                // and the shard assignment of the previous epoch relative to `event.epoch`, the
                // node cannot execute the epoch change. Therefore, the node needs to enter recovery
                // mode to catch up to the latest epoch as quickly as possible.
                tracing::warn!(
                    ?latest_epoch,
                    ?requested_epoch,
                    "epoch change requested for an older epoch than the latest epoch, this means \
                    the node is severely lagging behind, and will enter recovery mode"
                );
                Ok(BeginCommitteeChangeAction::EnterRecoveryMode)
            }
            Err(error) => {
                tracing::error!(?error, "failed to initiate a transition to the new epoch");
                Err(error)
            }
        }
    }

    /// Processes all the shard changes in the new epoch.
    #[tracing::instrument(skip_all)]
    async fn process_shard_changes_in_new_epoch(
        &self,
        event_handle: EventHandle,
        event: &EpochChangeStart,
        new_node_joining_committee: bool,
        shard_map_lock: StorageShardLock,
    ) -> anyhow::Result<()> {
        let public_key = self.inner.public_key();
        let storage = &self.inner.storage;
        let committees = self.inner.committee_service.active_committees();
        assert!(event.epoch <= committees.epoch());

        let shard_diff_calculator =
            ShardDiffCalculator::new(&committees, public_key, shard_map_lock.existing_shards());

        let shards_gained = shard_diff_calculator.gained_shards_from_prev_epoch();
        self.create_new_shards_and_start_sync(
            shard_map_lock,
            shards_gained,
            &committees,
            new_node_joining_committee,
        )
        .await?;

        for shard_id in shard_diff_calculator.shards_to_lock() {
            let Some(shard_storage) = storage.shard_storage(*shard_id).await else {
                tracing::info!("skipping lost shard during epoch change as it is not stored");
                continue;
            };
            tracing::info!(walrus.shard_index = %shard_id, "locking shard for epoch change");
            shard_storage
                .lock_shard_for_epoch_change()
                .context("failed to lock shard")?;
        }

        self.start_epoch_change_finisher
            .start_finish_epoch_change_tasks(
                event_handle,
                event,
                shard_diff_calculator.shards_to_remove().to_vec(),
                committees,
                !shards_gained.is_empty(),
            );

        Ok(())
    }

    /// Creates the shards that are newly assigned to the node and starts the sync for them.
    /// Note that the shard_map_lock will be unlocked after this function returns.
    async fn create_new_shards_and_start_sync(
        &self,
        shard_map_lock: StorageShardLock,
        shards_gained: &[ShardIndex],
        committees: &ActiveCommittees,
        new_node_joining_committee: bool,
    ) -> anyhow::Result<()> {
        let public_key = self.inner.public_key();
        if !shards_gained.is_empty() {
            assert!(committees.current_committee().contains(public_key));

            self.inner
                .create_storage_for_shards_in_background(shards_gained.to_vec(), shard_map_lock)
                .await?;

            if new_node_joining_committee {
                // Set node status to RecoverMetadata to sync metadata for the new shards.
                // Note that this must be set before marking the event as complete, so that
                // node crashing before setting the status will always be setting the status
                // again when re-processing the EpochChangeStart event.
                //
                // It's also important to set RecoverMetadata status after creating storage for
                // the new shards. Restarting seeing RecoverMetadata status will assume all the
                // shards are created.
                self.inner.set_node_status(NodeStatus::RecoverMetadata)?;
            }
            self.shard_sync_handler
                .start_sync_shards(shards_gained.to_vec(), new_node_joining_committee)
                .await?;
        }

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn process_epoch_change_done_event(&self, event: &EpochChangeDone) -> anyhow::Result<()> {
        match self
            .inner
            .committee_service
            .end_committee_change(event.epoch)
        {
            Ok(()) => tracing::info!(
                walrus.epoch = event.epoch,
                "successfully ended the transition to the new epoch"
            ),
            // This likely means that the committee was fetched (for example on startup) and we
            // are not processing the event that would have notified us that the epoch was
            // changing.
            Err(EndCommitteeChangeError::EpochChangeAlreadyDone) => tracing::info!(
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
                    "our committee service is lagging behind the events being processed, which \
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

    /// Storage node periodically records an event digest to check consistency of processed events.
    ///
    /// Every `NUM_EVENTS_PER_DIGEST_RECORDING`, we record the source of the event in metrics
    /// `process_event_digest` by bucket. The use of bucket is to store the recent bucket size
    /// number of recordings for better observability.
    ///
    /// We only record events in storage node that is sufficiently up-to-date. This means that the
    /// node is either in Active state or RecoveryInProgress state.
    ///
    /// The idea is that for most recent recordings, two nodes in the same bucket should record
    /// exact same event source. If there is a discrepancy, it means that these two nodes do
    /// not have the same event history. Once a divergence is detected, we can use the db tool
    /// to observe the event store to further analyze the issue.
    fn maybe_record_event_source(
        &self,
        event_index: u64,
        event_source: &CheckpointEventPosition,
    ) -> Result<(), TypedStoreError> {
        // Only record every Nth event.
        // `NUM_EVENTS_PER_DIGEST_RECORDING` is chosen in a way that a node produces a recording
        // every few hours.
        if event_index % NUM_EVENTS_PER_DIGEST_RECORDING != 0 {
            return Ok(());
        }

        // Only record digests for active or recovering nodes
        let node_status = self.inner.storage.node_status()?;
        if !matches!(
            node_status,
            NodeStatus::Active | NodeStatus::RecoveryInProgress(_)
        ) {
            return Ok(());
        }

        let bucket = (event_index / NUM_EVENTS_PER_DIGEST_RECORDING) % NUM_DIGEST_BUCKETS;
        debug_assert!(bucket < NUM_DIGEST_BUCKETS);

        // The event source is the combination of checkpoint sequence number and counter.
        // We scale the checkpoint sequence number by `CHECKPOINT_EVENT_POSITION_SCALE` to add
        // event counter in the checkpoint in the event source as well.
        let event_source = event_source
            .checkpoint_sequence_number
            .checked_mul(CHECKPOINT_EVENT_POSITION_SCALE)
            .unwrap_or(0)
            + event_source.counter;

        #[allow(clippy::cast_possible_wrap)] // wrapping is fine here
        walrus_utils::with_label!(
            self.inner
                .metrics
                .periodic_event_source_for_deterministic_events,
            bucket.to_string()
        )
        .set(event_source as i64);

        Ok(())
    }

    pub(crate) fn inner(&self) -> &Arc<StorageNodeInner> {
        &self.inner
    }

    /// Test utility to get the shards that are live on the node.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn existing_shards_live(&self) -> Vec<ShardIndex> {
        self.inner.storage.existing_shards_live().await
    }
}

impl StorageNodeInner {
    pub(crate) fn encoding_config(&self) -> &EncodingConfig {
        &self.encoding_config
    }

    /// Returns the node capability object ID.
    pub fn node_capability(&self) -> ObjectID {
        self.node_capability
    }

    /// Returns the shards that are owned by the node at the latest epoch in the committee info
    /// fetched from the chain.
    pub(crate) fn owned_shards_at_latest_epoch(&self) -> Vec<ShardIndex> {
        self.committee_service
            .active_committees()
            .current_committee()
            .shards_for_node_public_key(self.public_key())
            .to_vec()
    }

    /// Returns the shards that are owned by the node at the given epoch. Since the committee
    /// only contains the shard assignment for the current and previous epoch, this function
    /// returns an error if the given epoch is not the current or previous epoch.
    pub(crate) fn owned_shards_at_epoch(&self, epoch: Epoch) -> anyhow::Result<Vec<ShardIndex>> {
        let latest_epoch = self.committee_service.get_epoch();

        if latest_epoch == epoch + 1 {
            return self
                .committee_service
                .active_committees()
                .previous_committee()
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "previous committee is not set when checking shard assignment at epoch {}",
                        epoch
                    )
                })
                .map(|committee| {
                    committee
                        .shards_for_node_public_key(self.public_key())
                        .to_vec()
                });
        }

        if latest_epoch == epoch {
            return Ok(self
                .committee_service
                .active_committees()
                .current_committee()
                .shards_for_node_public_key(self.public_key())
                .to_vec());
        }

        anyhow::bail!("unknown epoch {} when checking shard assignment", epoch);
    }

    #[tracing::instrument(skip_all, fields(epoch))]
    async fn is_stored_at_all_shards_impl(
        &self,
        blob_id: &BlobId,
        epoch: Option<Epoch>,
    ) -> anyhow::Result<bool> {
        let shards = match epoch {
            Some(e) => self.owned_shards_at_epoch(e)?,
            None => self.owned_shards_at_latest_epoch(),
        };

        self.is_stored_at_specific_shards(blob_id, &shards).await
    }

    #[tracing::instrument(skip_all)]
    async fn is_stored_at_specific_shards(
        &self,
        blob_id: &BlobId,
        shards: &[ShardIndex],
    ) -> anyhow::Result<bool> {
        for shard in shards {
            match self.storage.is_stored_at_shard(blob_id, *shard).await {
                Ok(false) => return Ok(false),
                Ok(true) => continue,
                Err(error) => {
                    tracing::warn!(?error, "failed to check if blob is stored at shard");
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    /// Returns true if the blob is stored at all shards at the latest epoch.
    pub(crate) async fn is_stored_at_all_shards_at_latest_epoch(
        &self,
        blob_id: &BlobId,
    ) -> anyhow::Result<bool> {
        self.is_stored_at_all_shards_impl(blob_id, None).await
    }

    /// Returns true if the blob is stored at all shards at the given epoch.
    /// Note that since shard assignment is only available for the current and previous epoch,
    /// this function will return false if the given epoch is not the current or previous epoch.
    pub(crate) async fn is_stored_at_all_shards_at_epoch(
        &self,
        blob_id: &BlobId,
        epoch: Epoch,
    ) -> anyhow::Result<bool> {
        self.is_stored_at_all_shards_impl(blob_id, Some(epoch))
            .await
    }

    /// Returns true if the blob is stored at all shards that are in Active state.
    pub(crate) async fn is_stored_at_all_active_shards(
        &self,
        blob_id: &BlobId,
    ) -> anyhow::Result<bool> {
        let shards = self
            .storage
            .existing_shard_storages()
            .await
            .iter()
            .filter_map(|shard_storage| {
                shard_storage
                    .status()
                    .is_ok_and(|status| status == ShardStatus::Active)
                    .then_some(shard_storage.id())
            })
            .collect::<Vec<_>>();

        self.is_stored_at_specific_shards(blob_id, &shards).await
    }

    pub(crate) fn storage(&self) -> &Storage {
        &self.storage
    }

    /// Recovers the blob metadata from the committee service.
    pub(crate) async fn get_or_recover_blob_metadata(
        &self,
        blob_id: &BlobId,
        certified_epoch: Epoch,
    ) -> Result<BlobMetadataWithId<true>, TypedStoreError> {
        tracing::debug!(%blob_id, "check blob metadata existence");

        if let Some(metadata) = self.storage.get_metadata(blob_id)? {
            tracing::debug!(%blob_id, "not syncing metadata: already stored");
            return Ok(metadata);
        }

        tracing::debug!(%blob_id, "syncing metadata");
        let metadata = self
            .committee_service
            .get_and_verify_metadata(*blob_id, certified_epoch)
            .await;

        self.storage.put_verified_metadata(&metadata).await?;
        tracing::debug!(%blob_id, "metadata successfully synced");
        Ok(metadata)
    }

    fn current_event_epoch(&self) -> Epoch {
        self.latest_event_epoch.load(Ordering::SeqCst)
    }

    fn current_epoch(&self) -> Epoch {
        self.committee_service.get_epoch()
    }

    fn check_index<T>(&self, index: T) -> Result<(), IndexOutOfRange>
    where
        T: Into<u16>,
    {
        let index: u16 = index.into();

        if index < self.n_shards().get() {
            Ok(())
        } else {
            Err(IndexOutOfRange {
                index,
                max: self.n_shards().get(),
            })
        }
    }

    fn reposition_event_cursor(
        &self,
        event_id: EventID,
        event_index: u64,
    ) -> Result<(), TypedStoreError> {
        self.storage.reposition_event_cursor(event_index, event_id)
    }

    fn is_blocked(&self, blob_id: &BlobId) -> bool {
        self.blocklist.is_blocked(blob_id)
    }

    async fn get_shard_for_sliver_pair(
        &self,
        sliver_pair_index: SliverPairIndex,
        blob_id: &BlobId,
    ) -> Result<Arc<ShardStorage>, ShardNotAssigned> {
        let shard_index =
            sliver_pair_index.to_shard_index(self.encoding_config.n_shards(), blob_id);
        self.storage
            .shard_storage(shard_index)
            .await
            .ok_or(ShardNotAssigned(shard_index, self.current_epoch()))
    }

    fn init_gauges(&self) -> Result<(), TypedStoreError> {
        let persisted = self.storage.get_sequentially_processed_event_count()?;
        let node_status = self.storage.node_status()?;

        walrus_utils::with_label!(self.metrics.event_cursor_progress, "persisted").set(persisted);
        self.metrics.current_node_status.set(node_status.to_i64());

        Ok(())
    }

    fn public_key(&self) -> &PublicKey {
        self.protocol_key_pair.as_ref().public()
    }

    async fn shard_health_status(
        &self,
        detailed: bool,
    ) -> (ShardStatusSummary, Option<ShardStatusDetail>) {
        // NOTE: It is possible that the committee or shards change between this and the next call.
        // As this is for admin consumption, this is not considered a problem.
        let mut shard_statuses = self.storage.list_shard_status().await;
        let owned_shards = self.owned_shards_at_latest_epoch();
        let mut summary = ShardStatusSummary::default();

        let mut detail = detailed.then(|| {
            let mut detail = ShardStatusDetail::default();
            detail.owned.reserve_exact(owned_shards.len());
            detail
        });

        // Record the status for the owned shards.
        for shard in owned_shards {
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

    pub(crate) async fn store_sliver_unchecked(
        &self,
        metadata: VerifiedBlobMetadataWithId,
        sliver_pair_index: SliverPairIndex,
        sliver: Sliver,
    ) -> Result<bool, StoreSliverError> {
        let shard_storage = self
            .get_shard_for_sliver_pair(sliver_pair_index, metadata.blob_id())
            .await?;

        let shard_status = shard_storage
            .status()
            .context("Unable to retrieve shard status")?;

        if !shard_status.is_owned_by_node() {
            return Err(ShardNotAssigned(shard_storage.id(), self.current_epoch()).into());
        }

        if shard_storage
            .is_sliver_type_stored(metadata.blob_id(), sliver.r#type())
            .context("database error when checking sliver existence")?
        {
            return Ok(false);
        }

        let encoding_config = self.encoding_config.clone();
        let result = self
            .thread_pool
            .clone()
            .oneshot(move || {
                sliver.verify(&encoding_config, metadata.as_ref())?;
                Result::<_, StoreSliverError>::Ok((metadata, sliver))
            })
            .await;
        let (metadata, sliver) = thread_pool::unwrap_or_resume_panic(result)?;
        let sliver_type = sliver.r#type();

        // Finally store the sliver in the appropriate shard storage.
        shard_storage
            .put_sliver(*metadata.blob_id(), sliver)
            .await
            .context("unable to store sliver")?;

        walrus_utils::with_label!(self.metrics.slivers_stored_total, sliver_type).inc();

        Ok(true)
    }

    async fn create_storage_for_shards_in_background(
        self: &Arc<Self>,
        new_shards: Vec<ShardIndex>,
        shard_map_lock: StorageShardLock,
    ) -> Result<(), anyhow::Error> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || async move {
            this.storage
                .create_storage_for_shards_locked(shard_map_lock, &new_shards)
                .await
        })
        .in_current_span()
        .await?
        .await?;
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

    /// Sets the status of the node.
    pub fn set_node_status(&self, status: NodeStatus) -> Result<(), TypedStoreError> {
        self.metrics.current_node_status.set(status.to_i64());
        self.storage.set_node_status(status)
    }

    fn shut_down(&self) {
        self.is_shutting_down.store(true, Ordering::SeqCst)
    }

    fn is_shutting_down(&self) -> bool {
        self.is_shutting_down.load(Ordering::SeqCst)
    }

    async fn try_retrieve_recovery_symbol(
        &self,
        blob_id: &BlobId,
        sliver_pair_index: SliverPairIndex,
        target_sliver_type: SliverType,
        target_pair_index: SliverPairIndex,
    ) -> Result<GeneralRecoverySymbol, RetrieveSymbolError> {
        // Claim a worker for performing the expansion necessary to get the symbol.
        let mut worker = match self.symbol_service.clone().ready_oneshot().now_or_never() {
            Some(result) => result.expect("polling the symbol service is infallible"),
            None => return Err(Unavailable.into()),
        };

        let sliver = self
            .retrieve_sliver(blob_id, sliver_pair_index, target_sliver_type.orthogonal())
            .await?;
        let convert_error = |error| match error {
            RecoverySymbolError::IndexTooLarge => {
                panic!("index validity must be checked above")
            }
            RecoverySymbolError::EncodeError(error) => {
                RetrieveSymbolError::Internal(anyhow!(error))
            }
        };
        let metadata = self
            .storage
            .get_metadata(blob_id)
            .context("could not retrieve blob metadata")?
            .ok_or_else(|| {
                RetrieveSymbolError::Internal(anyhow!("metadata not found for blob {:?}", blob_id))
            })?;

        let request = RecoverySymbolRequest {
            blob_id: *metadata.blob_id(),
            source_sliver: sliver,
            target_pair_index,
            encoding_type: metadata.metadata().encoding_type(),
        };

        worker.call(request).map_err(convert_error).await
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
        ApiShardStatus::Unknown => summary.owned_shard_status.unknown += 1,
        ApiShardStatus::Ready => summary.owned_shard_status.ready += 1,
        ApiShardStatus::InTransfer => summary.owned_shard_status.in_transfer += 1,
        ApiShardStatus::InRecovery => summary.owned_shard_status.in_recovery += 1,
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

    async fn store_metadata(
        &self,
        metadata: UnverifiedBlobMetadataWithId,
    ) -> Result<bool, StoreMetadataError> {
        self.inner.store_metadata(metadata).await
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
    ) -> impl Future<Output = Result<Sliver, RetrieveSliverError>> + Send {
        self.inner
            .retrieve_sliver(blob_id, sliver_pair_index, sliver_type)
    }

    fn store_sliver(
        &self,
        blob_id: BlobId,
        sliver_pair_index: SliverPairIndex,
        sliver: Sliver,
    ) -> impl Future<Output = Result<bool, StoreSliverError>> + Send {
        self.inner.store_sliver(blob_id, sliver_pair_index, sliver)
    }

    fn compute_storage_confirmation(
        &self,
        blob_id: &BlobId,
        blob_persistence_type: &BlobPersistenceType,
    ) -> impl Future<Output = Result<StorageConfirmation, ComputeStorageConfirmationError>> + Send
    {
        self.inner
            .compute_storage_confirmation(blob_id, blob_persistence_type)
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
        symbol_id: SymbolId,
        sliver_type: Option<SliverType>,
    ) -> impl Future<Output = Result<GeneralRecoverySymbol, RetrieveSymbolError>> + Send {
        self.inner
            .retrieve_recovery_symbol(blob_id, symbol_id, sliver_type)
    }

    fn retrieve_multiple_recovery_symbols(
        &self,
        blob_id: &BlobId,
        filter: RecoverySymbolsFilter,
    ) -> impl Future<Output = Result<Vec<GeneralRecoverySymbol>, ListSymbolsError>> + Send {
        self.inner
            .retrieve_multiple_recovery_symbols(blob_id, filter)
    }

    fn blob_status(&self, blob_id: &BlobId) -> Result<BlobStatus, BlobStatusError> {
        self.inner.blob_status(blob_id)
    }

    fn n_shards(&self) -> NonZeroU16 {
        self.inner.n_shards()
    }

    fn health_info(&self, detailed: bool) -> impl Future<Output = ServiceHealthInfo> + Send {
        self.inner.health_info(detailed)
    }

    fn sliver_status<A: EncodingAxis>(
        &self,
        blob_id: &BlobId,
        sliver_pair_index: SliverPairIndex,
    ) -> impl Future<Output = Result<StoredOnNodeStatus, RetrieveSliverError>> + Send {
        self.inner.sliver_status::<A>(blob_id, sliver_pair_index)
    }

    fn sync_shard(
        &self,
        public_key: PublicKey,
        signed_request: SignedSyncShardRequest,
    ) -> impl Future<Output = Result<SyncShardResponse, SyncShardServiceError>> + Send {
        self.inner.sync_shard(public_key, signed_request)
    }
}

impl ServiceState for StorageNodeInner {
    fn retrieve_metadata(
        &self,
        blob_id: &BlobId,
    ) -> Result<VerifiedBlobMetadataWithId, RetrieveMetadataError> {
        #[cfg(msim)]
        {
            // Register a fail point to inject an unavailable error.
            let mut return_unavailable = false;
            fail_point_if!("get_metadata_return_unavailable", || {
                return_unavailable = true;
            });
            if return_unavailable {
                return Err(RetrieveMetadataError::Unavailable);
            }
        }

        ensure!(!self.is_blocked(blob_id), RetrieveMetadataError::Forbidden);

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

    async fn store_metadata(
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

        // Check if encoding type is supported
        let encoding_type = metadata.metadata().encoding_type();
        if !encoding_type.is_supported() {
            return Err(StoreMetadataError::UnsupportedEncodingType(encoding_type));
        }

        let encoding_config = self.encoding_config.clone();
        let verified_metadata_with_id = self
            .thread_pool
            .clone()
            .oneshot(move || metadata.verify(&encoding_config))
            .map(thread_pool::unwrap_or_resume_panic)
            .await?;

        self.storage
            .put_verified_metadata(&verified_metadata_with_id)
            .await
            .context("unable to store metadata")?;

        self.metrics
            .uploaded_metadata_unencoded_blob_bytes
            .observe(verified_metadata_with_id.as_ref().unencoded_length() as f64);
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

    async fn retrieve_sliver(
        &self,
        blob_id: &BlobId,
        sliver_pair_index: SliverPairIndex,
        sliver_type: SliverType,
    ) -> Result<Sliver, RetrieveSliverError> {
        self.check_index(sliver_pair_index)?;

        ensure!(!self.is_blocked(blob_id), RetrieveSliverError::Forbidden);

        ensure!(
            self.is_blob_registered(blob_id)?,
            RetrieveSliverError::Unavailable,
        );

        let shard_storage = self
            .get_shard_for_sliver_pair(sliver_pair_index, blob_id)
            .await?;

        shard_storage
            .get_sliver(blob_id, sliver_type)
            .context("unable to retrieve sliver")?
            .ok_or(RetrieveSliverError::Unavailable)
            .inspect(|sliver| {
                walrus_utils::with_label!(self.metrics.slivers_retrieved_total, sliver.r#type())
                    .inc();
            })
    }

    async fn store_sliver(
        &self,
        blob_id: BlobId,
        sliver_pair_index: SliverPairIndex,
        sliver: Sliver,
    ) -> Result<bool, StoreSliverError> {
        self.check_index(sliver_pair_index)?;

        ensure!(
            self.is_blob_registered(&blob_id)?,
            StoreSliverError::NotCurrentlyRegistered,
        );

        // Get metadata first to check encoding type.
        let metadata = self
            .storage
            .get_metadata(&blob_id)
            .context("database error when storing sliver")?
            .ok_or(StoreSliverError::MissingMetadata)?;

        // Check if encoding type is supported
        let encoding_type = metadata.metadata().encoding_type();
        if !encoding_type.is_supported() {
            return Err(StoreSliverError::UnsupportedEncodingType(encoding_type));
        }

        self.store_sliver_unchecked(metadata, sliver_pair_index, sliver)
            .await
    }

    async fn compute_storage_confirmation(
        &self,
        blob_id: &BlobId,
        blob_persistence_type: &BlobPersistenceType,
    ) -> Result<StorageConfirmation, ComputeStorageConfirmationError> {
        ensure!(
            self.is_blob_registered(blob_id)?,
            ComputeStorageConfirmationError::NotCurrentlyRegistered,
        );

        // Storage confirmation must use the last shard assignment, even though the node hasn't
        // processed to the latest epoch yet. This is because if the onchain committee has moved
        // on to the new epoch, confirmation from the old epoch is not longer valid.
        ensure!(
            self.is_stored_at_all_shards_at_latest_epoch(blob_id)
                .await
                .context("database error when checkingstorage status")?,
            ComputeStorageConfirmationError::NotFullyStored,
        );

        if let BlobPersistenceType::Deletable { object_id } = blob_persistence_type {
            let per_object_info = self
                .storage
                .get_per_object_info(&object_id.into())
                .context("database error when checking per object info")?
                .ok_or(ComputeStorageConfirmationError::NotCurrentlyRegistered)?;
            ensure!(
                per_object_info.is_registered(self.current_epoch()),
                ComputeStorageConfirmationError::NotCurrentlyRegistered,
            );
        }

        let confirmation =
            Confirmation::new(self.current_epoch(), *blob_id, *blob_persistence_type);
        let signed = sign_message(confirmation, self.protocol_key_pair.clone()).await?;

        self.metrics.storage_confirmations_issued_total.inc();

        Ok(StorageConfirmation::Signed(signed))
    }

    fn blob_status(&self, blob_id: &BlobId) -> Result<BlobStatus, BlobStatusError> {
        Ok(self
            .storage
            .get_blob_info(blob_id)
            .context("could not retrieve blob info")?
            .map(|blob_info| blob_info.to_blob_status(self.current_epoch()))
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

    #[tracing::instrument(skip(self))]
    async fn retrieve_recovery_symbol(
        &self,
        blob_id: &BlobId,
        symbol_id: SymbolId,
        sliver_type: Option<SliverType>,
    ) -> Result<GeneralRecoverySymbol, RetrieveSymbolError> {
        let n_shards = self.n_shards();

        let primary_index = symbol_id.primary_sliver_index();
        self.check_index(primary_index)?;
        let primary_pair_index = primary_index.to_pair_index::<Primary>(n_shards);

        let secondary_index = symbol_id.secondary_sliver_index();
        self.check_index(secondary_index)?;
        let secondary_pair_index = secondary_index.to_pair_index::<Secondary>(n_shards);

        let owned_shards = self.owned_shards_at_latest_epoch();

        // In the event that neither of the slivers are assigned to this shard use this error,
        // otherwise it is overwritten.
        let mut final_error = RetrieveSymbolError::SymbolNotPresentAtShards;

        for (source_pair_index, target_sliver_pair, target_sliver_type) in [
            (
                primary_pair_index,
                secondary_pair_index,
                SliverType::Secondary,
            ),
            (
                secondary_pair_index,
                primary_pair_index,
                SliverType::Primary,
            ),
        ] {
            if sliver_type.is_some() && sliver_type != Some(target_sliver_type) {
                // Respect the caller specified sliver type.
                continue;
            }

            let required_shard = &source_pair_index.to_shard_index(n_shards, blob_id);
            if !owned_shards.contains(required_shard) {
                // This node does not manage the shard owning the source pair.
                continue;
            }

            match self
                .try_retrieve_recovery_symbol(
                    blob_id,
                    source_pair_index,
                    target_sliver_type,
                    target_sliver_pair,
                )
                .await
            {
                Ok(symbol) => return Ok(symbol),
                Err(error) => final_error = error,
            }
        }

        Err(final_error)
    }

    #[tracing::instrument(skip_all)]
    async fn retrieve_multiple_recovery_symbols(
        &self,
        blob_id: &BlobId,
        filter: RecoverySymbolsFilter,
    ) -> Result<Vec<GeneralRecoverySymbol>, ListSymbolsError> {
        let n_shards = self.n_shards();

        let symbol_id_iter =
            match filter.id_filter() {
                SymbolIdFilter::Ids(symbol_ids) => Either::Left(symbol_ids.iter().copied()),

                SymbolIdFilter::Recovers {
                    target_sliver: target,
                    target_type,
                } => Either::Right(self.owned_shards_at_latest_epoch().into_iter().map(
                    |shard_id| {
                        let pair_stored = shard_id.to_pair_index(n_shards, blob_id);
                        match *target_type {
                            SliverType::Primary => SymbolId::new(
                                *target,
                                pair_stored.to_sliver_index::<Secondary>(n_shards),
                            ),
                            SliverType::Secondary => SymbolId::new(
                                pair_stored.to_sliver_index::<Primary>(n_shards),
                                *target,
                            ),
                        }
                    },
                )),
            };

        let mut output = vec![];
        let mut last_error = ListSymbolsError::NoSymbolsSpecified;

        // If a specific proof axis is requested, then specify the target-type to the retrieve
        // function, otherwise, specify only the symbol IDs.
        let target_type_from_proof = filter.proof_axis().map(|axis| axis.orthogonal());

        // We use FuturesOrdered to keep the results in the same order as the requests.
        let mut symbols: FuturesOrdered<_> = symbol_id_iter
            .map(|symbol_id| {
                self.retrieve_recovery_symbol(blob_id, symbol_id, target_type_from_proof)
                    .map(move |result| (symbol_id, result))
            })
            .collect();

        while let Some((symbol_id, result)) = symbols.next().await {
            match result {
                Ok(symbol) => output.push(symbol),

                // Callers may request symbols that are not stored with this shard, or
                // completely invalid symbols. These are ignored unless there are no successes.
                Err(error) => {
                    tracing::debug!(%error, %symbol_id, "failed to get requested symbol");
                    last_error = error.into();
                }
            }
        }

        if output.is_empty() {
            Err(last_error)
        } else {
            Ok(output)
        }
    }

    fn n_shards(&self) -> NonZeroU16 {
        self.encoding_config.n_shards()
    }

    async fn health_info(&self, detailed: bool) -> ServiceHealthInfo {
        let (shard_summary, shard_detail) = self.shard_health_status(detailed).await;

        // Get the latest checkpoint sequence number directly from the event manager.
        let latest_checkpoint_sequence_number =
            self.event_manager.latest_checkpoint_sequence_number();

        ServiceHealthInfo {
            uptime: self.start_time.elapsed(),
            epoch: self.current_epoch(),
            public_key: self.public_key().clone(),
            node_status: self
                .storage
                .node_status()
                .expect("fetching node status should not fail")
                .to_string(),
            event_progress: self
                .storage
                .get_event_cursor_progress()
                .expect("get cursor progress should not fail")
                .into(),
            shard_detail,
            shard_summary,
            latest_checkpoint_sequence_number,
        }
    }

    async fn sliver_status<A: EncodingAxis>(
        &self,
        blob_id: &BlobId,
        sliver_pair_index: SliverPairIndex,
    ) -> Result<StoredOnNodeStatus, RetrieveSliverError> {
        match self
            .get_shard_for_sliver_pair(sliver_pair_index, blob_id)
            .await?
            .is_sliver_stored::<A>(blob_id)
        {
            Ok(true) => Ok(StoredOnNodeStatus::Stored),
            Ok(false) => Ok(StoredOnNodeStatus::Nonexistent),
            Err(err) => Err(RetrieveSliverError::Internal(err.into())),
        }
    }

    async fn sync_shard(
        &self,
        public_key: PublicKey,
        signed_request: SignedSyncShardRequest,
    ) -> Result<SyncShardResponse, SyncShardServiceError> {
        if !self.committee_service.is_walrus_storage_node(&public_key) {
            return Err(SyncShardServiceError::Unauthorized);
        }

        let sync_shard_msg = signed_request.verify_signature_and_get_message(&public_key)?;
        let request = sync_shard_msg.as_ref().contents();

        tracing::debug!(?request, "sync shard request received");

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
            .await
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
    use config::ShardSyncConfig;
    use contract_service::MockSystemContractService;
    use storage::{
        ShardStatus,
        tests::{BLOB_ID, OTHER_SHARD_INDEX, SHARD_INDEX, WhichSlivers, populated_storage},
    };
    use sui_types::base_types::ObjectID;
    use system_events::SystemEventProvider;
    use tokio::sync::{Mutex, broadcast::Sender};
    use walrus_core::{
        DEFAULT_ENCODING,
        encoding::{EncodingConfigTrait as _, Primary, Secondary, SliverData, SliverPair},
        messages::{SyncShardMsg, SyncShardRequest},
        test_utils::generate_config_metadata_and_valid_recovery_symbols,
    };
    use walrus_proc_macros::walrus_simtest;
    use walrus_storage_node_client::{StorageNodeClient, api::errors::STORAGE_NODE_ERROR_DOMAIN};
    use walrus_sui::{
        client::FixedSystemParameters,
        test_utils::{EventForTesting, event_id_for_testing},
        types::{
            BlobCertified,
            BlobDeleted,
            BlobRegistered,
            InvalidBlobId,
            StorageNodeCap,
            move_structs::EpochState,
        },
    };
    use walrus_test_utils::{Result as TestResult, WithTempDir, async_param_test};

    use super::*;
    use crate::test_utils::{StorageNodeHandle, StorageNodeHandleTrait, TestCluster};

    const TIMEOUT: Duration = Duration::from_secs(1);
    const OTHER_BLOB_ID: BlobId = BlobId([247; 32]);
    const BLOB: &[u8] = &[
        0, 1, 255, 0, 2, 254, 0, 3, 253, 0, 4, 252, 0, 5, 251, 0, 6, 250, 0, 7, 249, 0, 8, 248,
    ];

    struct ShardStorageSet {
        pub shard_storage: Vec<Arc<ShardStorage>>,
    }

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
            let storage_node = storage_node_with_storage(
                populated_storage(&[(
                    SHARD_INDEX,
                    vec![
                        (BLOB_ID, WhichSlivers::Primary),
                        (OTHER_BLOB_ID, WhichSlivers::Both),
                    ],
                )])
                .await?,
            )
            .await;

            let err = storage_node
                .as_ref()
                .compute_storage_confirmation(&BLOB_ID, &BlobPersistenceType::Permanent)
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
                )])
                .await?,
                vec![BlobRegistered::for_testing(BLOB_ID).into()],
            )
            .await;

            let err = retry_until_success_or_timeout(TIMEOUT, || async {
                match storage_node
                    .as_ref()
                    .compute_storage_confirmation(&BLOB_ID, &BlobPersistenceType::Permanent)
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
                )])
                .await?,
                vec![BlobRegistered::for_testing(BLOB_ID).into()],
            )
            .await;

            let confirmation = retry_until_success_or_timeout(TIMEOUT, || {
                storage_node
                    .as_ref()
                    .compute_storage_confirmation(&BLOB_ID, &BlobPersistenceType::Permanent)
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

            assert_eq!(confirmation.as_ref().contents().blob_id, BLOB_ID);
            assert_eq!(
                confirmation.as_ref().contents().blob_type,
                BlobPersistenceType::Permanent
            );

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

        let result = node
            .as_ref()
            .retrieve_sliver(&BLOB_ID, sliver_pair_index, SliverType::Primary)
            .await;

        assert!(matches!(result, Err(RetrieveSliverError::Unavailable)));

        Ok(())
    }

    // Test that `is_stored_at_all_shards` uses the committee assignment to determine if the blob
    // is stored at all shards.
    async_param_test! {
        is_stored_at_all_shards_uses_committee_assignment -> TestResult: [
            shard_not_assigned_in_committee: (&[ShardIndex(0)], &[ShardIndex(1)], false),
            shard_assigned_in_committee: (&[ShardIndex(0)], &[ShardIndex(0), ShardIndex(1)], true),
        ]
    }
    async fn is_stored_at_all_shards_uses_committee_assignment(
        shard_assignment: &[ShardIndex],
        shards_in_storage: &[ShardIndex],
        is_stored_at_all_shards: bool,
    ) -> TestResult {
        let node = StorageNodeHandle::builder()
            .with_shard_assignment(shard_assignment)
            .with_storage(
                populated_storage(
                    shards_in_storage
                        .iter()
                        .map(|shard| (*shard, vec![(BLOB_ID, WhichSlivers::Both)]))
                        .collect::<Vec<_>>()
                        .as_slice(),
                )
                .await?,
            )
            .with_system_event_provider(vec![])
            .with_node_started(true)
            .build()
            .await?;

        assert_eq!(
            node.storage_node
                .inner
                .is_stored_at_all_shards_at_latest_epoch(&BLOB_ID)
                .await
                .expect("error checking is stord at all shards"),
            is_stored_at_all_shards
        );

        Ok(())
    }

    async_param_test! {
        deletes_blob_data_on_event -> TestResult: [
            invalid_blob_event_registered: (InvalidBlobId::for_testing(BLOB_ID).into(), false),
            invalid_blob_event_certified: (InvalidBlobId::for_testing(BLOB_ID).into(), true),
            // TODO (WAL-201): Uncomment the following tests as soon as we actually delete blob
            // data.
            // blob_deleted_event_registered: (
            //     BlobDeleted{was_certified: false, ..BlobDeleted::for_testing(BLOB_ID)}.into(),
            //     false
            // ),
            // blob_deleted_event_certified: (BlobDeleted::for_testing(BLOB_ID).into(), true),
        ]
    }
    async fn deletes_blob_data_on_event(event: BlobEvent, is_certified: bool) -> TestResult {
        let events = Sender::new(48);
        let node = StorageNodeHandle::builder()
            .with_storage(
                populated_storage(&[
                    (SHARD_INDEX, vec![(BLOB_ID, WhichSlivers::Both)]),
                    (OTHER_SHARD_INDEX, vec![(BLOB_ID, WhichSlivers::Both)]),
                ])
                .await?,
            )
            .with_system_event_provider(events.clone())
            .with_node_started(true)
            .build()
            .await?;
        let inner = node.as_ref().inner.clone();

        tokio::time::sleep(Duration::from_millis(50)).await;

        assert!(
            inner
                .is_stored_at_all_shards_at_latest_epoch(&BLOB_ID)
                .await?,
        );
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
        assert!(
            !inner
                .is_stored_at_all_shards_at_latest_epoch(&BLOB_ID)
                .await?
        );
        Ok(())
    }

    async_param_test! {
        correctly_handles_blob_deletions_with_concurrent_instances -> TestResult: [
            same_epoch: (1),
            later_epoch: (2),
        ]
    }
    async fn correctly_handles_blob_deletions_with_concurrent_instances(
        current_epoch: Epoch,
    ) -> TestResult {
        let (cluster, events) = cluster_at_epoch1_without_blobs(&[&[0]], None).await?;
        advance_cluster_to_epoch(&cluster, &[&events], current_epoch).await?;

        let node = &cluster.nodes[0];
        println!("{}", node.storage_node.inner.current_epoch());

        let blob_events: Vec<BlobEvent> = vec![
            BlobRegistered {
                deletable: true,
                end_epoch: 2,
                ..BlobRegistered::for_testing(BLOB_ID)
            }
            .into(),
            BlobCertified {
                deletable: true,
                end_epoch: 2,
                ..BlobCertified::for_testing(BLOB_ID)
            }
            .into(),
            BlobDeleted {
                end_epoch: 2,
                ..BlobDeleted::for_testing(BLOB_ID)
            }
            .into(),
        ];

        // Send each event twice. This corresponds to registering and certifying two `Blob`
        // instances with the same blob ID, and then deleting both.
        for event in blob_events {
            events.send(event.clone().into())?;
            events.send(event.into())?;
        }

        wait_until_events_processed(node, 6).await?;

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
        let storage_node = storage_node_with_storage(
            populated_storage(&[
                (SHARD_INDEX, vec![(BLOB_ID, WhichSlivers::Both)]),
                (OTHER_SHARD_INDEX, vec![(BLOB_ID, WhichSlivers::Primary)]),
            ])
            .await?,
        )
        .await;

        let pair_index =
            SHARD_INDEX.to_pair_index(storage_node.as_ref().inner.n_shards(), &BLOB_ID);
        let other_pair_index =
            OTHER_SHARD_INDEX.to_pair_index(storage_node.as_ref().inner.n_shards(), &BLOB_ID);

        check_sliver_status::<Primary>(&storage_node, pair_index, StoredOnNodeStatus::Stored)
            .await?;
        check_sliver_status::<Secondary>(&storage_node, pair_index, StoredOnNodeStatus::Stored)
            .await?;
        check_sliver_status::<Primary>(&storage_node, other_pair_index, StoredOnNodeStatus::Stored)
            .await?;
        check_sliver_status::<Secondary>(
            &storage_node,
            other_pair_index,
            StoredOnNodeStatus::Nonexistent,
        )
        .await?;
        Ok(())
    }
    async fn check_sliver_status<A: EncodingAxis>(
        storage_node: &StorageNodeHandle,
        pair_index: SliverPairIndex,
        expected: StoredOnNodeStatus,
    ) -> TestResult {
        let effective = storage_node
            .as_ref()
            .inner
            .sliver_status::<A>(&BLOB_ID, pair_index)
            .await?;
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
        node.as_ref().store_metadata(metadata).await?;

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
            metadata.mut_inner().hashes[0].primary_hash = Node::Digest([0; 32]);
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
                .get_for_type(DEFAULT_ENCODING)
                .encode_with_metadata(blob)
                .expect("must be able to get encoder");

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
        let mut nodes_and_shards = Vec::new();
        for node in cluster.nodes.iter() {
            let existing_shards = node.storage_node().existing_shards().await;
            nodes_and_shards.extend(std::iter::repeat(node).zip(existing_shards));
        }

        let mut metadata_stored = vec![];

        for (node, shard) in nodes_and_shards {
            if !metadata_stored.contains(&node.public_key())
                && (store_at_shard(&shard, SliverType::Primary)
                    || store_at_shard(&shard, SliverType::Secondary))
            {
                retry_until_success_or_timeout(TIMEOUT, || {
                    node.client().store_metadata(&blob.metadata)
                })
                .await?;
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
        shard_sync_config: Option<ShardSyncConfig>,
    ) -> TestResult<(TestCluster, Sender<ContractEvent>)> {
        let events = Sender::new(48);

        let cluster = {
            // Lock to avoid race conditions.
            let _lock = global_test_lock().lock().await;
            let mut builder = TestCluster::<StorageNodeHandle>::builder()
                .with_shard_assignment(assignment)
                .with_system_event_providers(events.clone());
            if let Some(shard_sync_config) = shard_sync_config {
                builder = builder.with_shard_sync_config(shard_sync_config);
            }
            builder.build().await?
        };

        Ok((cluster, events))
    }

    async fn cluster_with_partially_stored_blob<F>(
        assignment: &[&[u16]],
        blob: &[u8],
        store_at_shard: F,
    ) -> TestResult<(TestCluster, Sender<ContractEvent>, EncodedBlob)>
    where
        F: FnMut(&ShardIndex, SliverType) -> bool,
    {
        let (cluster, events) = cluster_at_epoch1_without_blobs(assignment, None).await?;

        let config = cluster.encoding_config();
        let blob_details = EncodedBlob::new(blob, config);

        events.send(BlobRegistered::for_testing(*blob_details.blob_id()).into())?;
        store_at_shards(&blob_details, &cluster, store_at_shard).await?;

        Ok((cluster, events, blob_details))
    }

    // Creates a test cluster with custom initial epoch and blobs that are already certified.
    async fn cluster_with_initial_epoch_and_certified_blob(
        assignment: &[&[u16]],
        blobs: &[&[u8]],
        initial_epoch: Epoch,
        shard_sync_config: Option<ShardSyncConfig>,
    ) -> TestResult<(TestCluster, Sender<ContractEvent>, Vec<EncodedBlob>)> {
        let (cluster, events) =
            cluster_at_epoch1_without_blobs(assignment, shard_sync_config).await?;

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

    /// A struct that contains the event senders for each node in the cluster.
    #[allow(unused)]
    struct ClusterEventSenders {
        /// The event sender for node 0.
        node_0_events: Sender<ContractEvent>,
        /// The event sender for all other nodes.
        all_other_node_events: Sender<ContractEvent>,
    }

    /// Creates a test cluster with custom initial epoch and blobs that are partially stored
    /// in shard 0.
    ///
    /// The function is created for testing shard syncing/recovery. So for blobs that are
    /// not stored in shard 0, it also won't receive a certified event.
    ///
    /// The function also takes custom function to determine the end epoch of a blob, and whether
    /// the blob should be deletable.
    async fn cluster_with_partially_stored_blobs_in_shard_0<F, G, H>(
        assignment: &[&[u16]],
        blobs: &[&[u8]],
        initial_epoch: Epoch,
        mut blob_index_store_at_shard_0: F,
        mut blob_index_to_end_epoch: G,
        mut blob_index_to_deletable: H,
    ) -> TestResult<(TestCluster, Vec<EncodedBlob>, ClusterEventSenders)>
    where
        F: FnMut(usize) -> bool,
        G: FnMut(usize) -> Epoch,
        H: FnMut(usize) -> bool,
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
            let blob_end_epoch = blob_index_to_end_epoch(i);
            let deletable = blob_index_to_deletable(i);
            let blob_registration_event = BlobRegistered {
                deletable,
                end_epoch: blob_end_epoch,
                ..BlobRegistered::for_testing(*blob_details.blob_id())
            };
            node_0_events.send(blob_registration_event.clone().into())?;
            all_other_node_events.send(blob_registration_event.into())?;

            let blob_certified_event = BlobCertified {
                deletable,
                end_epoch: blob_end_epoch,
                ..BlobCertified::for_testing(*blob_details.blob_id())
            };
            if blob_index_store_at_shard_0(i) {
                store_at_shards(&blob_details, &cluster, |_, _| true).await?;
                node_0_events.send(blob_certified_event.clone().into())?;
            } else {
                // Don't certify the blob if it's not stored in shard 0.
                store_at_shards(&blob_details, &cluster, |shard_index, _| {
                    shard_index != &ShardIndex(0)
                })
                .await?;
            }

            all_other_node_events.send(blob_certified_event.into())?;
            details.push(blob_details);
        }

        advance_cluster_to_epoch(
            &cluster,
            &[&node_0_events, &all_other_node_events],
            initial_epoch,
        )
        .await?;

        Ok((
            cluster,
            details,
            ClusterEventSenders {
                node_0_events,
                all_other_node_events,
            },
        ))
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

        let (cluster, events) = cluster_at_epoch1_without_blobs(shards, None).await?;
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
                encoding_type: DEFAULT_ENCODING,
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

    // Tests that a panic thrown by a blob sync task is propagated to the node runtime.
    #[tokio::test]
    #[ignore = "ignore long-running test by default"]
    async fn blob_sync_panic_thrown() {
        let shards: &[&[u16]] = &[&[1], &[0, 2, 3, 4, 5, 6]];
        let test_shard = ShardIndex(1);

        let (mut cluster, _events, blob) =
            cluster_with_partially_stored_blob(shards, BLOB, |&shard, _| shard != test_shard)
                .await
                .unwrap();

        // Delete shard data to force a panic in the blob sync task.
        // Note that this only deletes the storage for the shard. Storage still has an entry for the
        // shard, so it thinks it still owns the shard.
        cluster.nodes[0]
            .storage_node
            .inner
            .storage
            .shard_storage(test_shard)
            .await
            .unwrap()
            .delete_shard_storage()
            .unwrap();

        // Start a sync to trigger the blob sync task.
        cluster.nodes[0]
            .storage_node
            .blob_sync_handler
            .start_sync(*blob.blob_id(), 1, None)
            .await
            .unwrap();

        // Wait for the node runtime to finish, and check that a panic was thrown.
        let result = cluster.nodes[0].node_runtime_handle.as_mut().unwrap().await;
        if let Err(e) = result {
            assert!(e.is_panic());
        } else {
            panic!("expected panic");
        }
    }

    #[walrus_simtest]
    async fn cancel_expired_blob_sync_upon_epoch_change() -> TestResult {
        telemetry_subscribers::init_for_testing();

        let shards: &[&[u16]] = &[&[1], &[0, 2, 3, 4]];

        let (cluster, events, blob) =
            cluster_with_partially_stored_blob(shards, BLOB, |shard, _| shard.get() != 1).await?;
        events.send(
            BlobCertified {
                epoch: 1,
                blob_id: *blob.blob_id(),
                end_epoch: 2,
                deletable: false,
                object_id: ObjectID::random(),
                is_extension: false,
                event_id: event_id_for_testing(),
            }
            .into(),
        )?;
        advance_cluster_to_epoch(&cluster, &[&events], 2).await?;

        // Node 1 which has the blob stored should finish processing 4 events: blob registered,
        // blob certified, epoch change start, epoch change done.
        wait_until_events_processed(&cluster.nodes[1], 4).await?;

        // Node 0 should also finish all events as blob syncs of expired blobs are cancelled on
        // epoch change.
        wait_until_events_processed(&cluster.nodes[0], 4).await?;

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
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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
                    // The nodes will now quickly return an "Unavailable" error when they are
                    // loaded, this means that we may the exponential backoff requiring more time.
                    TIMEOUT * 2,
                )
                .instrument(tracing::info_span!("test-inners"))
                .await;

                assert_eq!(synced, *expected,);
            }
        }

        Ok(())
    }

    #[tokio::test(start_paused = false)]
    #[ignore = "ignore long-running test by default"]
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
            .map(|e| e.event_id());
        assert_eq!(latest_cursor, Some(blob2_registered_event.event_id));

        Ok(())
    }

    async fn expect_sliver_pair_stored_before_timeout(
        blob: &EncodedBlob,
        node_client: &StorageNodeClient,
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
        node_client: &StorageNodeClient,
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
            cluster_with_partially_stored_blob(&[&[0, 1, 2, 3]], BLOB, |_, _| true).await?;

        let is_newly_stored = cluster.nodes[0]
            .storage_node
            .store_metadata(blob.metadata.into_unverified())
            .await?;

        assert!(!is_newly_stored);

        Ok(())
    }

    #[tokio::test]
    async fn skip_storing_sliver_if_already_stored() -> TestResult {
        let (cluster, _, blob) =
            cluster_with_partially_stored_blob(&[&[0, 1, 2, 3]], BLOB, |_, _| true).await?;

        let assigned_sliver_pair = blob.assigned_sliver_pair(ShardIndex(0));
        let is_newly_stored = cluster.nodes[0]
            .storage_node
            .store_sliver(
                *blob.blob_id(),
                assigned_sliver_pair.index(),
                Sliver::Primary(assigned_sliver_pair.primary.clone()),
            )
            .await?;

        assert!(!is_newly_stored);

        Ok(())
    }

    // Tests the basic `sync_shard` API.
    #[tokio::test]
    async fn sync_shard_node_api_success() -> TestResult {
        let (cluster, _, blob_detail) =
            cluster_with_initial_epoch_and_certified_blob(&[&[0, 1], &[2, 3]], &[BLOB], 2, None)
                .await?;

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
                    .await
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
            cluster_with_initial_epoch_and_certified_blob(&[&[0, 1], &[2, 3]], &[BLOB], 1, None)
                .await?;

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
            cluster_with_initial_epoch_and_certified_blob(&[&[0, 1], &[2, 3]], &[BLOB], 1, None)
                .await?;

        let error: walrus_storage_node_client::error::NodeError = cluster.nodes[0]
            .client
            .sync_shard::<Primary>(ShardIndex(0), BLOB_ID, 10, 0, &ProtocolKeyPair::generate())
            .await
            .expect_err("the request must fail");

        let status = error.status().expect("response has error status");
        assert_eq!(status.reason(), Some("REQUEST_UNAUTHORIZED"));
        assert_eq!(status.domain(), Some(STORAGE_NODE_ERROR_DOMAIN));

        Ok(())
    }

    // Tests signed SyncShardRequest verification error.
    #[tokio::test]
    async fn sync_shard_node_api_request_verification_error() -> TestResult {
        let (cluster, _, _) =
            cluster_with_initial_epoch_and_certified_blob(&[&[0, 1], &[2, 3]], &[BLOB], 1, None)
                .await?;

        let request = SyncShardRequest::new(ShardIndex(0), SliverType::Primary, BLOB_ID, 10, 1);
        let sync_shard_msg = SyncShardMsg::new(1, request);
        let signed_request = cluster.nodes[0]
            .as_ref()
            .inner
            .protocol_key_pair
            .sign_message(&sync_shard_msg);

        let result = cluster.nodes[0]
            .storage_node
            .sync_shard(
                cluster.nodes[1]
                    .as_ref()
                    .inner
                    .protocol_key_pair
                    .0
                    .public()
                    .clone(),
                signed_request,
            )
            .await;
        assert!(matches!(
            result,
            Err(SyncShardServiceError::MessageVerificationError(..))
        ));

        Ok(())
    }

    // Tests SyncShardRequest with wrong epoch.
    async_param_test! {
        sync_shard_node_api_invalid_epoch -> TestResult: [
            too_old: (3, 1),
            too_new: (3, 4),
        ]
    }
    async fn sync_shard_node_api_invalid_epoch(
        cluster_epoch: Epoch,
        requester_epoch: Epoch,
    ) -> TestResult {
        // Creates a cluster with initial epoch set to 3.
        let (cluster, _, blob_detail) = cluster_with_initial_epoch_and_certified_blob(
            &[&[0, 1], &[2, 3]],
            &[BLOB],
            cluster_epoch,
            None,
        )
        .await?;

        // Requests a shard from epoch 0.
        let error = cluster.nodes[0]
            .client
            .sync_shard::<Primary>(
                ShardIndex(0),
                *blob_detail[0].blob_id(),
                10,
                requester_epoch,
                &cluster.nodes[0].as_ref().inner.protocol_key_pair,
            )
            .await
            .expect_err("request should fail");
        let status = error.status().expect("response has an error status");
        let error_info = status.error_info().expect("response has error details");

        assert_eq!(error_info.domain(), STORAGE_NODE_ERROR_DOMAIN);
        assert_eq!(error_info.reason(), "INVALID_EPOCH");
        assert_eq!(Some(requester_epoch), error_info.field("request_epoch"));
        assert_eq!(Some(cluster_epoch), error_info.field("server_epoch"));

        Ok(())
    }

    #[tokio::test]
    async fn can_read_locked_shard() -> TestResult {
        let (cluster, events, blob) =
            cluster_with_partially_stored_blob(&[&[0, 1, 2, 3]], BLOB, |_, _| true).await?;

        events.send(BlobCertified::for_testing(*blob.blob_id()).into())?;

        cluster.nodes[0]
            .storage_node
            .inner
            .storage
            .shard_storage(ShardIndex(0))
            .await
            .unwrap()
            .lock_shard_for_epoch_change()
            .expect("Lock shard failed.");

        assert_eq!(
            blob.assigned_sliver_pair(ShardIndex(0)).index(),
            SliverPairIndex(3)
        );
        let sliver = retry_until_success_or_timeout(TIMEOUT, || async {
            cluster.nodes[0]
                .storage_node
                .retrieve_sliver(blob.blob_id(), SliverPairIndex(3), SliverType::Primary)
                .await
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
            cluster_with_partially_stored_blob(&[&[0, 1, 2, 3]], BLOB, |_, _| true).await?;

        cluster.nodes[0]
            .storage_node
            .inner
            .storage
            .shard_storage(ShardIndex(0))
            .await
            .unwrap()
            .lock_shard_for_epoch_change()
            .expect("Lock shard failed.");

        let assigned_sliver_pair = blob.assigned_sliver_pair(ShardIndex(0));
        assert!(matches!(
            cluster.nodes[0]
                .storage_node
                .store_sliver(
                    *blob.blob_id(),
                    assigned_sliver_pair.index(),
                    Sliver::Primary(assigned_sliver_pair.primary.clone()),
                )
                .await,
            Err(StoreSliverError::ShardNotAssigned(..))
        ));

        Ok(())
    }

    #[tokio::test]
    async fn compute_storage_confirmation_ignore_not_owned_shard() -> TestResult {
        let (cluster, _, blob) =
            cluster_with_partially_stored_blob(&[&[0, 1, 2, 3]], BLOB, |index, _| index.get() != 0)
                .await?;

        assert!(matches!(
            cluster.nodes[0]
                .storage_node
                .compute_storage_confirmation(blob.blob_id(), &BlobPersistenceType::Permanent)
                .await,
            Err(ComputeStorageConfirmationError::NotFullyStored)
        ));

        let lookup_service_handle = cluster
            .lookup_service_handle
            .as_ref()
            .expect("should contain lookup service");

        // Set up the committee in a way that shard 0 is removed from the first storage node in the
        // contract.
        let committees = lookup_service_handle.committees.lock().unwrap().clone();
        let mut next_committee = (**committees.current_committee()).clone();
        next_committee.epoch += 1;
        next_committee.members_mut()[0].shard_ids.remove(0);
        lookup_service_handle.set_next_epoch_committee(next_committee);

        assert_eq!(
            cluster
                .lookup_service_handle
                .as_ref()
                .expect("should contain lookup service")
                .advance_epoch(),
            2
        );

        cluster.nodes[0]
            .storage_node
            .inner
            .committee_service
            .begin_committee_change_to_latest_committee()
            .await
            .unwrap();

        assert!(
            cluster.nodes[0]
                .storage_node
                .compute_storage_confirmation(blob.blob_id(), &BlobPersistenceType::Permanent)
                .await
                .is_ok()
        );

        Ok(())
    }

    // The common setup for shard sync tests.
    // By default:
    //   - Initial cluster with 2 nodes. Shard 0 in node 0 and shard 1 in node 1.
    //   - 23 blobs created and certified in node 0.
    //   - Create a new shard in node 1 with shard index 0 to test sync.
    // If assignment is provided, it will be used to create the cluster, then all
    // shards in the first node will be created in the second node for sync.
    // If shard_sync_config is provided, it will be used to configure the shard sync.
    async fn setup_cluster_for_shard_sync_tests(
        assignment: Option<&[&[u16]]>,
        shard_sync_config: Option<ShardSyncConfig>,
    ) -> TestResult<(TestCluster, Vec<EncodedBlob>, Storage, Arc<ShardStorageSet>)> {
        let assignment = assignment.unwrap_or(&[&[0], &[1, 2, 3]]);
        let blobs: Vec<[u8; 32]> = (1..24).map(|i| [i; 32]).collect();
        let blobs: Vec<_> = blobs.iter().map(|b| &b[..]).collect();
        let (cluster, _, blob_details) =
            cluster_with_initial_epoch_and_certified_blob(assignment, &blobs, 2, shard_sync_config)
                .await?;

        // Makes storage inner mutable so that we can manually add another shard to node 1.
        let node_inner = unsafe {
            &mut *(Arc::as_ptr(&cluster.nodes[1].storage_node.inner) as *mut StorageNodeInner)
        };
        let shard_indices: Vec<_> = assignment[0].iter().map(|i| ShardIndex(*i)).collect();
        node_inner
            .storage
            .create_storage_for_shards(&shard_indices)
            .await?;
        let mut shard_storage = vec![];
        for shard_index in shard_indices {
            shard_storage.push(
                node_inner
                    .storage
                    .shard_storage(shard_index)
                    .await
                    .expect("shard storage should exist"),
            );
        }

        let shard_storage_set = ShardStorageSet { shard_storage };
        let shard_storage_set = Arc::new(shard_storage_set);

        for shard_storage in shard_storage_set.shard_storage.iter() {
            shard_storage.update_status_in_test(ShardStatus::None)?;
        }

        Ok((
            cluster,
            blob_details,
            node_inner.storage.clone(),
            shard_storage_set.clone(),
        ))
    }

    // Checks that all primary and secondary slivers match the original encoding of the blobs.
    // Checks that blobs in the skip list are not synced.
    fn check_all_blobs_are_synced(
        blob_details: &[EncodedBlob],
        storage_dst: &Storage,
        shard_storage_dst: &ShardStorage,
        skip_blob_indices: &[usize],
    ) -> anyhow::Result<()> {
        blob_details
            .iter()
            .enumerate()
            .try_for_each(|(i, details)| {
                let blob_id = *details.blob_id();

                // If the blob is in the skip list, it should not be present in the destination
                // shard storage.
                if skip_blob_indices.contains(&i) {
                    assert!(
                        shard_storage_dst
                            .get_sliver(&blob_id, SliverType::Primary)
                            .unwrap()
                            .is_none()
                    );
                    assert!(
                        shard_storage_dst
                            .get_sliver(&blob_id, SliverType::Secondary)
                            .unwrap()
                            .is_none()
                    );
                    return Ok(());
                }

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

                // Check that metadata is synced.
                assert_eq!(
                    details.metadata,
                    storage_dst.get_metadata(&blob_id).unwrap().unwrap(),
                );

                Ok(())
            })
    }

    async fn wait_for_shard_in_active_state(shard_storage: &ShardStorage) -> TestResult {
        // Waits for the shard to be synced.
        tokio::time::timeout(Duration::from_secs(15), async {
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

    async fn wait_for_shards_in_active_state(shard_storage_set: &ShardStorageSet) -> TestResult {
        // Waits for the shard to be synced.
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let mut all_active = true;
                for shard_storage in &shard_storage_set.shard_storage {
                    let status = shard_storage
                        .status()
                        .expect("Shard status should be present");
                    if status != ShardStatus::Active {
                        all_active = false;
                        break;
                    }
                }
                if all_active {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await?;

        Ok(())
    }

    // Tests shard transfer only using shard sync functionality.
    async_param_test! {
        sync_shard_complete_transfer -> TestResult: [
            only_sync_blob: (false),
            also_sync_metadata: (true),
        ]
    }
    async fn sync_shard_complete_transfer(
        wipe_metadata_before_transfer_in_dst: bool,
    ) -> TestResult {
        let assignment: &[&[u16]] = &[&[0, 1, 2], &[3]];
        let shard_sync_config: ShardSyncConfig = ShardSyncConfig {
            shard_sync_concurrency: rand::thread_rng().gen_range(1..=assignment.len()),
            ..Default::default()
        };
        let (cluster, blob_details, storage_dst, shard_storage_set) =
            setup_cluster_for_shard_sync_tests(Some(assignment), Some(shard_sync_config)).await?;

        let expected_shard_count = assignment[0].len();

        assert_eq!(shard_storage_set.shard_storage.len(), expected_shard_count);
        let shard_storage_dst = shard_storage_set.shard_storage[0].clone();
        if wipe_metadata_before_transfer_in_dst {
            storage_dst.clear_metadata_in_test()?;
            storage_dst.set_node_status(NodeStatus::RecoverMetadata)?;
        }

        let shard_storage_src = cluster.nodes[0]
            .storage_node
            .inner
            .storage
            .shard_storage(ShardIndex(0))
            .await
            .expect("shard storage should exist");

        assert_eq!(blob_details.len(), 23);
        assert_eq!(shard_storage_src.sliver_count(SliverType::Primary), Ok(23));
        assert_eq!(
            shard_storage_src.sliver_count(SliverType::Secondary),
            Ok(23)
        );
        assert_eq!(shard_storage_dst.sliver_count(SliverType::Primary), Ok(0));
        assert_eq!(shard_storage_dst.sliver_count(SliverType::Secondary), Ok(0));

        let shard_indices: Vec<_> = assignment[0].iter().map(|i| ShardIndex(*i)).collect();

        // Starts the shard syncing process.
        cluster.nodes[1]
            .storage_node
            .shard_sync_handler
            .start_sync_shards(shard_indices, wipe_metadata_before_transfer_in_dst)
            .await?;

        // Waits for the shard to be synced.
        wait_for_shards_in_active_state(&shard_storage_set).await?;

        assert_eq!(shard_storage_dst.sliver_count(SliverType::Primary), Ok(23));
        assert_eq!(
            shard_storage_dst.sliver_count(SliverType::Secondary),
            Ok(23)
        );

        assert_eq!(blob_details.len(), 23);

        // Checks that the shard is completely migrated.
        check_all_blobs_are_synced(&blob_details, &storage_dst, &shard_storage_dst, &[])?;

        Ok(())
    }

    /// Sets up a test cluster for shard recovery tests.
    async fn setup_shard_recovery_test_cluster_with_blob_count<F, G, H>(
        blob_count: u8,
        blob_index_store_at_shard_0: F,
        blob_index_to_end_epoch: G,
        blob_index_to_deletable: H,
    ) -> TestResult<(TestCluster, Vec<EncodedBlob>, ClusterEventSenders)>
    where
        F: FnMut(usize) -> bool,
        G: FnMut(usize) -> Epoch,
        H: FnMut(usize) -> bool,
    {
        let blobs: Vec<[u8; 32]> = (1..=blob_count).map(|i| [i; 32]).collect();
        let blobs: Vec<_> = blobs.iter().map(|b| &b[..]).collect();
        let (cluster, blob_details, event_senders) =
            cluster_with_partially_stored_blobs_in_shard_0(
                &[&[0], &[1, 2, 3, 4], &[5, 6, 7, 8, 9]],
                &blobs,
                2,
                blob_index_store_at_shard_0,
                blob_index_to_end_epoch,
                blob_index_to_deletable,
            )
            .await?;

        Ok((cluster, blob_details, event_senders))
    }

    async fn setup_shard_recovery_test_cluster<F, G, H>(
        blob_index_store_at_shard_0: F,
        blob_index_to_end_epoch: G,
        blob_index_to_deletable: H,
    ) -> TestResult<(TestCluster, Vec<EncodedBlob>, ClusterEventSenders)>
    where
        F: FnMut(usize) -> bool,
        G: FnMut(usize) -> Epoch,
        H: FnMut(usize) -> bool,
    {
        setup_shard_recovery_test_cluster_with_blob_count(
            23,
            blob_index_store_at_shard_0,
            blob_index_to_end_epoch,
            blob_index_to_deletable,
        )
        .await
    }

    // Tests shard transfer completely using shard recovery functionality.
    async_param_test! {
        sync_shard_shard_recovery -> TestResult: [
            only_sync_blob: (false),
            also_sync_metadata: (true),
        ]
    }
    async fn sync_shard_shard_recovery(wipe_metadata_before_transfer_in_dst: bool) -> TestResult {
        let (cluster, blob_details, _) =
            setup_shard_recovery_test_cluster(|_| false, |_| 42, |_| false).await?;

        // Make sure that all blobs are not certified in node 0.
        for blob_detail in blob_details.iter() {
            let blob_info = cluster.nodes[0]
                .storage_node
                .inner
                .storage
                .get_blob_info(blob_detail.blob_id());
            assert!(matches!(
                blob_info.unwrap().unwrap().to_blob_status(1),
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
            .create_storage_for_shards(&[ShardIndex(0)])
            .await?;
        let shard_storage_dst = node_inner
            .storage
            .shard_storage(ShardIndex(0))
            .await
            .unwrap();
        shard_storage_dst.update_status_in_test(ShardStatus::None)?;

        if wipe_metadata_before_transfer_in_dst {
            node_inner.storage.clear_metadata_in_test()?;
            node_inner.set_node_status(NodeStatus::RecoverMetadata)?;
        }

        cluster.nodes[1]
            .storage_node
            .shard_sync_handler
            .start_sync_shards(vec![ShardIndex(0)], wipe_metadata_before_transfer_in_dst)
            .await?;
        wait_for_shard_in_active_state(shard_storage_dst.as_ref()).await?;
        check_all_blobs_are_synced(
            &blob_details,
            &node_inner.storage.clone(),
            shard_storage_dst.as_ref(),
            &[],
        )?;

        Ok(())
    }

    // Tests shard transfer partially using shard recovery functionality and partially using shard
    // sync.
    // This test also tests that no missing blobs after sync completion.
    async_param_test! {
        sync_shard_partial_recovery -> TestResult: [
            only_sync_blob: (false),
            also_sync_metadata: (true),
        ]
    }
    async fn sync_shard_partial_recovery(wipe_metadata_before_transfer_in_dst: bool) -> TestResult {
        let skip_stored_blob_index: [usize; 12] = [3, 4, 5, 9, 10, 11, 15, 18, 19, 20, 21, 22];
        let (cluster, blob_details, _) = setup_shard_recovery_test_cluster(
            |blob_index| !skip_stored_blob_index.contains(&blob_index),
            |_| 42,
            |_| false,
        )
        .await?;

        // Make sure that blobs in `sync_shard_partial_recovery` are not certified in node 0.
        for i in skip_stored_blob_index {
            let blob_info = cluster.nodes[0]
                .storage_node
                .inner
                .storage
                .get_blob_info(blob_details[i].blob_id());
            assert!(matches!(
                blob_info.unwrap().unwrap().to_blob_status(1),
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
            .create_storage_for_shards(&[ShardIndex(0)])
            .await?;
        let shard_storage_dst = node_inner
            .storage
            .shard_storage(ShardIndex(0))
            .await
            .unwrap();
        shard_storage_dst.update_status_in_test(ShardStatus::None)?;

        if wipe_metadata_before_transfer_in_dst {
            node_inner.storage.clear_metadata_in_test()?;
            node_inner.set_node_status(NodeStatus::RecoverMetadata)?;
        }

        cluster.nodes[1]
            .storage_node
            .shard_sync_handler
            .start_sync_shards(vec![ShardIndex(0)], wipe_metadata_before_transfer_in_dst)
            .await?;
        wait_for_shard_in_active_state(shard_storage_dst.as_ref()).await?;
        check_all_blobs_are_synced(
            &blob_details,
            &node_inner.storage,
            shard_storage_dst.as_ref(),
            &[],
        )?;

        Ok(())
    }

    // TODO(WAL-872): move failure injection test to src/tests/. Currently there is no way to run seed-search
    // on these tests since there is no test target.
    #[cfg(msim)]
    mod failure_injection_tests {
        use sui_macros::{
            clear_fail_point,
            register_fail_point,
            register_fail_point_arg,
            register_fail_point_async,
            register_fail_point_if,
        };
        use tokio::sync::Notify;
        use walrus_proc_macros::walrus_simtest;
        use walrus_test_utils::simtest_param_test;

        use super::*;

        async fn wait_until_no_sync_tasks(shard_sync_handler: &ShardSyncHandler) -> TestResult {
            // Timeout needs to be longer than shard sync retry interval.
            tokio::time::timeout(Duration::from_secs(120), async {
                loop {
                    if shard_sync_handler.current_sync_task_count().await == 0
                        && shard_sync_handler.no_pending_recover_metadata().await
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
            .await
            .map_err(|_| anyhow::anyhow!("Timed out waiting for shard sync tasks to complete"))?;

            Ok(())
        }

        // Tests that shard sync can be resumed from a specific progress point.
        // `break_index` is the index of the blob to break the sync process.
        // Note that currently, each sync batch contains 10 blobs. So testing various interesting
        // places to break the sync process.
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
            let (cluster, blob_details, storage_dst, shard_storage_set) =
                setup_cluster_for_shard_sync_tests(None, None).await?;

            assert_eq!(shard_storage_set.shard_storage.len(), 1);
            let shard_storage_dst = shard_storage_set.shard_storage[0].clone();
            register_fail_point_arg(
                "fail_point_fetch_sliver",
                move || -> Option<(SliverType, u64, bool)> {
                    Some((sliver_type, break_index, false))
                },
            );

            // Skip retry loop in shard sync to simulate a reboot.
            register_fail_point_if("fail_point_shard_sync_no_retry", || true);

            // Starts the shard syncing process in the new shard, which will fail at the specified
            // break index.
            cluster.nodes[1]
                .storage_node
                .shard_sync_handler
                .start_sync_shards(vec![ShardIndex(0)], false)
                .await?;

            // Waits for the shard sync process to stop.
            wait_until_no_sync_tasks(&cluster.nodes[1].storage_node.shard_sync_handler).await?;

            // Check that shard sync process is not finished.
            let shard_storage_src = cluster.nodes[0]
                .storage_node
                .inner
                .storage
                .shard_storage(ShardIndex(0))
                .await
                .expect("shard storage should exist");
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
            check_all_blobs_are_synced(&blob_details, &storage_dst, &shard_storage_dst, &[])?;

            Ok(())
        }

        // Tests that restarting shard sync will retry shard transfer first, even though last sync
        // entered recovery mode.
        simtest_param_test! {
            sync_shard_restart_recover_retry_transfer -> TestResult: [
                primary5: (5, SliverType::Primary),
                primary15: (15, SliverType::Primary),
                secondary5: (5, SliverType::Secondary),
                secondary15: (15, SliverType::Secondary),
            ]
        }
        async fn sync_shard_restart_recover_retry_transfer(
            break_index: u64,
            sliver_type: SliverType,
        ) -> TestResult {
            let (cluster, blob_details, storage_dst, shard_storage_set) =
                setup_cluster_for_shard_sync_tests(None, None).await?;

            assert_eq!(shard_storage_set.shard_storage.len(), 1);
            let shard_storage_dst = shard_storage_set.shard_storage[0].clone();

            // Register two fail points here. `fail_point_fetch_sliver` will cause shard transfer
            // to fail in the middle, which makes node entering recover mode, and
            // `fail_point_after_start_recovery` will cause the shard recovery to fail, which
            // terminates the shard sync process. Upon restart, we should see that shards retry
            // shard transfer first.
            register_fail_point_arg(
                "fail_point_fetch_sliver",
                move || -> Option<(SliverType, u64, bool)> {
                    Some((sliver_type, break_index, false))
                },
            );
            register_fail_point_if("fail_point_after_start_recovery", || true);

            // Starts the shard syncing process in the new shard, which will fail at the specified
            // break index.
            cluster.nodes[1]
                .storage_node
                .shard_sync_handler
                .start_sync_shards(vec![ShardIndex(0)], false)
                .await?;

            // Waits for the shard sync process to stop.
            wait_until_no_sync_tasks(&cluster.nodes[1].storage_node.shard_sync_handler).await?;

            // Check that shard sync process is not finished.
            let shard_storage_src = cluster.nodes[0]
                .storage_node
                .inner
                .storage
                .shard_storage(ShardIndex(0))
                .await
                .expect("shard storage should exist");
            assert!(
                shard_storage_dst.sliver_count(SliverType::Primary)
                    < shard_storage_src.sliver_count(SliverType::Primary)
                    || shard_storage_dst.sliver_count(SliverType::Secondary)
                        < shard_storage_src.sliver_count(SliverType::Secondary)
            );

            // Register a fail point to check that the node will not enter recovery mode from this
            // point after restart.
            register_fail_point("fail_point_shard_sync_recover_blob", move || {
                panic!("shard sync should not enter recovery mode in this test");
            });
            clear_fail_point("fail_point_fetch_sliver");

            // restart the shard syncing process, to simulate a reboot.
            cluster.nodes[1]
                .storage_node
                .shard_sync_handler
                .clear_shard_sync_tasks()
                .await;
            cluster.nodes[1]
                .storage_node
                .shard_sync_handler
                .restart_syncs()
                .await?;

            // Waits for the shard to be synced.
            wait_until_no_sync_tasks(&cluster.nodes[1].storage_node.shard_sync_handler).await?;

            // Checks that the shard is completely migrated.
            check_all_blobs_are_synced(&blob_details, &storage_dst, &shard_storage_dst, &[])?;

            clear_fail_point("fail_point_shard_sync_recover_blob");
            clear_fail_point("fail_point_after_start_recovery");
            Ok(())
        }

        // Tests that shard sync's behavior when encountering repeated sync failures.
        // More specifically, it tests that
        //   - When the shard sync repeatedly encounters retriable error, but the shard sync is
        //     able to make progress, the node continue to use shard sync to sync the shard.
        //   - When the shard sync repeatedly encounters retriable error, and the shard sync is
        //     not able to make progress, the node will eventually enter recovery mode.
        simtest_param_test! {
            sync_shard_repeated_failures -> TestResult: [
                // In this test case, we inject a retriable error fetching every 4 blobs. Since
                // the size is larger than `sliver_count_per_sync_request`, shard sync will
                // continue to make progress.
                with_progress: (4, false),
                // In this test case, we inject a retriable error fetching every 1 blob. Since
                // the size is smaller than `sliver_count_per_sync_request`, shard sync will
                // not be able to make progress.
                without_progress: (1, true),
            ]
        }
        async fn sync_shard_repeated_failures(
            break_index: u64,
            must_use_recovery: bool,
        ) -> TestResult {
            let shard_sync_config = ShardSyncConfig {
                sliver_count_per_sync_request: 2,
                shard_sync_retry_min_backoff: Duration::from_secs(1),
                shard_sync_retry_max_backoff: Duration::from_secs(5),
                shard_sync_retry_switch_to_recovery_interval: Duration::from_secs(10),
                ..Default::default()
            };
            let (cluster, blob_details, storage_dst, shard_storage_set) =
                setup_cluster_for_shard_sync_tests(None, Some(shard_sync_config)).await?;

            assert_eq!(shard_storage_set.shard_storage.len(), 1);
            let shard_storage_dst = shard_storage_set.shard_storage[0].clone();

            // Simulate repeated retriable errors. break_index indicates how many blobs fetched
            // before generating the retriable error.
            register_fail_point_arg(
                "fail_point_fetch_sliver",
                move || -> Option<(SliverType, u64, bool)> {
                    Some((SliverType::Primary, break_index, true))
                },
            );

            let enter_recovery_mode = Arc::new(AtomicBool::new(false));
            let enter_recovery_mode_clone = enter_recovery_mode.clone();
            // Fail point to track if the shard sync enters recovery mode.
            register_fail_point("fail_point_shard_sync_recover_blob", move || {
                enter_recovery_mode_clone.store(true, Ordering::SeqCst);
            });

            // Starts the shard syncing process in the new shard, which will fail at the specified
            // break index.
            cluster.nodes[1]
                .storage_node
                .shard_sync_handler
                .start_sync_shards(vec![ShardIndex(0)], false)
                .await?;

            // Waits for the shard sync process to stop.
            wait_until_no_sync_tasks(&cluster.nodes[1].storage_node.shard_sync_handler).await?;

            // Checks that the shard is completely migrated.
            check_all_blobs_are_synced(&blob_details, &storage_dst, &shard_storage_dst, &[])?;

            if must_use_recovery {
                assert!(enter_recovery_mode.load(Ordering::SeqCst));
            } else {
                assert!(!enter_recovery_mode.load(Ordering::SeqCst));
            }

            clear_fail_point("fail_point_fetch_sliver");

            Ok(())
        }

        simtest_param_test! {
            sync_shard_src_abnormal_return -> TestResult: [
                // Tests that there is a discrepancy between the source and destination shards in
                // terms of certified blobs. If the source doesn't return any blobs, the destination
                // should finish the sync process.
                return_empty: ("fail_point_sync_shard_return_empty"),
                // Tests that when direct shard sync request fails, the shard sync process will be
                // retried using shard recovery.
                return_error: ("fail_point_sync_shard_return_error")
            ]
        }
        async fn sync_shard_src_abnormal_return(fail_point: &'static str) -> TestResult {
            let (cluster, _blob_details, storage_dst, shard_storage_set) =
                setup_cluster_for_shard_sync_tests(None, None).await?;

            assert_eq!(shard_storage_set.shard_storage.len(), 1);
            let shard_storage_dst = shard_storage_set.shard_storage[0].clone();
            register_fail_point_if(fail_point, || true);

            // Starts the shard syncing process in the new shard, which will return empty slivers.
            cluster.nodes[1]
                .storage_node
                .shard_sync_handler
                .start_sync_shards(vec![ShardIndex(0)], false)
                .await?;

            // Waits for the shard sync process to stop.
            wait_until_no_sync_tasks(&cluster.nodes[1].storage_node.shard_sync_handler).await?;
            check_all_blobs_are_synced(&_blob_details, &storage_dst, &shard_storage_dst, &[])?;

            Ok(())
        }

        // Tests that non-certified blobs are not synced during shard sync. And expired certified
        // blobs do not cause shard sync to enter recovery directly.
        #[walrus_simtest]
        async fn sync_shard_ignore_non_certified_blobs() -> TestResult {
            // Creates some regular blobs that will be synced.
            let blobs: Vec<[u8; 32]> = (9..13).map(|i| [i; 32]).collect();
            let blobs: Vec<_> = blobs.iter().map(|b| &b[..]).collect();

            // Creates some expired certified blobs that will not be synced.
            let blobs_expired: Vec<[u8; 32]> = (1..21).map(|i| [i; 32]).collect();
            let blobs_expired: Vec<_> = blobs_expired.iter().map(|b| &b[..]).collect();

            // Generates a cluster with two nodes and one shard each.
            let (cluster, events) =
                cluster_at_epoch1_without_blobs(&[&[0, 1], &[2, 3]], None).await?;

            // Uses fail point to track whether shard sync recovery is triggered.
            let shard_sync_recovery_triggered = Arc::new(AtomicBool::new(false));
            let trigger = shard_sync_recovery_triggered.clone();
            register_fail_point("fail_point_shard_sync_recovery", move || {
                trigger.store(true, Ordering::SeqCst)
            });

            // Certifies all the blobs and upload data.
            let mut details = Vec::new();
            {
                let config = cluster.encoding_config();

                for blob in blobs {
                    let blob_details = EncodedBlob::new(blob, config.clone());
                    // Note: register and certify the blob are always using epoch 0.
                    events.send(BlobRegistered::for_testing(*blob_details.blob_id()).into())?;
                    store_at_shards(&blob_details, &cluster, |_, _| true).await?;
                    events.send(BlobCertified::for_testing(*blob_details.blob_id()).into())?;
                    details.push(blob_details);
                }

                // These blobs will be expired at epoch 3.
                for blob in blobs_expired {
                    let blob_details = EncodedBlob::new(blob, config.clone());
                    events.send(
                        BlobRegistered {
                            end_epoch: 3,
                            ..BlobRegistered::for_testing(*blob_details.blob_id())
                        }
                        .into(),
                    )?;
                    store_at_shards(&blob_details, &cluster, |_, _| false).await?;
                    events.send(
                        BlobCertified {
                            end_epoch: 3,
                            ..BlobCertified::for_testing(*blob_details.blob_id())
                        }
                        .into(),
                    )?;
                }

                // Advance cluster to epoch 4.
                advance_cluster_to_epoch(&cluster, &[&events], 4).await?;
            }

            // Makes storage inner mutable so that we can manually add another shard to node 1.
            let node_inner = unsafe {
                &mut *(Arc::as_ptr(&cluster.nodes[1].storage_node.inner) as *mut StorageNodeInner)
            };
            node_inner
                .storage
                .create_storage_for_shards(&[ShardIndex(0)])
                .await?;
            let shard_storage_dst = node_inner
                .storage
                .shard_storage(ShardIndex(0))
                .await
                .expect("shard storage should exist");
            shard_storage_dst.update_status_in_test(ShardStatus::None)?;

            // Starts the shard syncing process in the new shard, which should only use happy path
            // shard sync to sync non-expired certified blobs.
            cluster.nodes[1]
                .storage_node
                .shard_sync_handler
                .start_sync_shards(vec![ShardIndex(0)], false)
                .await?;

            // Waits for the shard sync process to stop.
            wait_until_no_sync_tasks(&cluster.nodes[1].storage_node.shard_sync_handler).await?;

            // All blobs should be recovered in the new dst node.
            check_all_blobs_are_synced(&details, &node_inner.storage, &shard_storage_dst, &[])?;

            // Checks that shard sync recovery is not triggered.
            assert!(!shard_sync_recovery_triggered.load(Ordering::SeqCst));

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
            register_fail_point_if("fail_point_after_start_recovery", move || {
                restart_after_recovery
            });
            if !restart_after_recovery {
                register_fail_point_arg(
                    "fail_point_fetch_sliver",
                    move || -> Option<(SliverType, u64, bool)> {
                        Some((sliver_type, break_index, false))
                    },
                );
            }

            // Skip retry loop in shard sync to simulate a reboot.
            register_fail_point_if("fail_point_shard_sync_no_retry", || true);

            let skip_stored_blob_index: [usize; 12] = [3, 4, 5, 9, 10, 11, 15, 18, 19, 20, 21, 22];
            let (cluster, blob_details, _) = setup_shard_recovery_test_cluster(
                |blob_index| !skip_stored_blob_index.contains(&blob_index),
                |_| 42,
                |_| false,
            )
            .await?;

            let node_inner = unsafe {
                &mut *(Arc::as_ptr(&cluster.nodes[1].storage_node.inner) as *mut StorageNodeInner)
            };
            node_inner
                .storage
                .create_storage_for_shards(&[ShardIndex(0)])
                .await?;
            let shard_storage_dst = node_inner
                .storage
                .shard_storage(ShardIndex(0))
                .await
                .expect("shard storage should exist");
            shard_storage_dst.update_status_in_test(ShardStatus::None)?;

            cluster.nodes[1]
                .storage_node
                .shard_sync_handler
                .start_sync_shards(vec![ShardIndex(0)], false)
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
                    .await
                    .expect("shard storage should exist");
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
            check_all_blobs_are_synced(
                &blob_details,
                &node_inner.storage,
                shard_storage_dst.as_ref(),
                &[],
            )?;

            Ok(())
        }

        // Tests shard metadata recovery with failure injection.
        simtest_param_test! {
            sync_shard_recovery_metadata_restart -> TestResult: [
                fail_before_start_fetching: (true),
                fail_during_fetching: (false),
            ]
        }
        async fn sync_shard_recovery_metadata_restart(
            fail_before_start_fetching: bool,
        ) -> TestResult {
            let (cluster, blob_details, storage_dst, shard_storage_set) =
                setup_cluster_for_shard_sync_tests(None, None).await?;

            assert_eq!(shard_storage_set.shard_storage.len(), 1);
            let shard_storage_dst = shard_storage_set.shard_storage[0].clone();
            if fail_before_start_fetching {
                register_fail_point_if(
                    "fail_point_shard_sync_recovery_metadata_error_before_fetch",
                    || true,
                );
            } else {
                let total_blobs = blob_details.len() as u64;
                // Randomly pick a blob index to inject failure.
                // Note that the scan count starts from 1.
                let break_scan_count = rand::thread_rng().gen_range(1..=total_blobs);

                register_fail_point_arg(
                    "fail_point_shard_sync_recovery_metadata_error_during_fetch",
                    move || -> Option<u64> { Some(break_scan_count) },
                );
            }

            storage_dst
                .remove_storage_for_shards(&[ShardIndex(1)])
                .await?;
            storage_dst.clear_metadata_in_test()?;
            storage_dst.set_node_status(NodeStatus::RecoverMetadata)?;

            // Starts the shard syncing process in the new shard, which will fail at the specified
            // break index.
            cluster.nodes[1]
                .storage_node
                .shard_sync_handler
                .start_sync_shards(vec![ShardIndex(0)], true)
                .await?;

            // Waits for the shard sync process to stop.
            wait_until_no_sync_tasks(&cluster.nodes[1].storage_node.shard_sync_handler).await?;

            assert!(shard_storage_dst.status().unwrap() == ShardStatus::None);

            if fail_before_start_fetching {
                clear_fail_point("fail_point_shard_sync_recovery_metadata_error_before_fetch");
            } else {
                clear_fail_point("fail_point_shard_sync_recovery_metadata_error_during_fetch");
            }

            // restart the shard syncing process, to simulate a reboot.
            cluster.nodes[1]
                .storage_node
                .shard_sync_handler
                .restart_syncs()
                .await?;

            // Waits for the shard to be synced.
            wait_until_no_sync_tasks(&cluster.nodes[1].storage_node.shard_sync_handler).await?;

            // Checks that the shard is completely migrated.
            check_all_blobs_are_synced(&blob_details, &storage_dst, &shard_storage_dst, &[])?;

            Ok(())
        }

        #[walrus_simtest]
        async fn finish_epoch_change_start_should_not_block_event_processing() -> TestResult {
            telemetry_subscribers::init_for_testing();

            // It is important to only use one node in this test, so that no other node would
            // drive epoch change on chain, and send events to the nodes.
            let (cluster, events, _blob_detail) =
                cluster_with_initial_epoch_and_certified_blob(&[&[0, 1, 2, 3]], &[BLOB], 2, None)
                    .await?;
            cluster.nodes[0]
                .storage_node
                .start_epoch_change_finisher
                .wait_until_previous_task_done()
                .await;

            let processed_event_count_initial = &cluster.nodes[0]
                .storage_node
                .inner
                .storage
                .get_sequentially_processed_event_count()?;

            // Use fail point to block finishing epoch change start event.
            let unblock = Arc::new(Notify::new());
            let unblock_clone = unblock.clone();
            register_fail_point_async("blocking_finishing_epoch_change_start", move || {
                let unblock_clone = unblock_clone.clone();
                async move {
                    unblock_clone.notified().await;
                }
            });

            // Update mocked on chain committee to the new epoch.
            cluster
                .lookup_service_handle
                .clone()
                .unwrap()
                .advance_epoch();

            // Sends one epoch change start event which will be blocked finishing.
            events.send(ContractEvent::EpochChangeEvent(
                EpochChangeEvent::EpochChangeStart(EpochChangeStart {
                    epoch: 3,
                    event_id: walrus_sui::test_utils::event_id_for_testing(),
                }),
            ))?;

            // Register and certified a blob, and then check the blob should be certified in the
            // node indicating that the event processing is not blocked.
            assert_eq!(
                cluster.nodes[0]
                    .storage_node
                    .inner
                    .blob_status(&OTHER_BLOB_ID)
                    .expect("getting blob status should succeed"),
                BlobStatus::Nonexistent
            );

            // Must send the blob registered and certified events with the same epoch as the
            // epoch change start event.
            events.send(
                BlobRegistered {
                    epoch: 3,
                    ..BlobRegistered::for_testing(OTHER_BLOB_ID)
                }
                .into(),
            )?;
            events.send(
                BlobCertified {
                    epoch: 3,
                    ..BlobCertified::for_testing(OTHER_BLOB_ID)
                }
                .into(),
            )?;
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if cluster.nodes[0]
                        .storage_node
                        .inner
                        .is_blob_certified(&OTHER_BLOB_ID)
                        .expect("getting blob status should succeed")
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
            .await?;

            // Persist event count should remain the same as the beginning since we haven't
            // unblock epoch change start event.
            assert_eq!(
                processed_event_count_initial,
                &cluster.nodes[0]
                    .storage_node
                    .inner
                    .storage
                    .get_sequentially_processed_event_count()?
            );

            // Unblock the epoch change start event, and expect that processed event count should
            // make progress. Use `+2` instead of `+3` is because certify blob initiats a blob sync,
            // and sync we don't upload the blob data, so it won't get processed.
            // The point here is that the epoch change start event should be marked completed.
            unblock.notify_one();
            wait_until_events_processed(&cluster.nodes[0], processed_event_count_initial + 2)
                .await?;

            Ok(())
        }

        // Tests that storage node lag check is not affected by the blob writer cursor.
        #[walrus_simtest]
        async fn event_blob_cursor_should_not_affect_node_state() -> TestResult {
            telemetry_subscribers::init_for_testing();

            // Set the initial cursor to a high value to simulate a severe lag to
            // blob writer cursor.
            register_fail_point_arg(
                "storage_node_initial_cursor",
                || -> Option<EventStreamCursor> {
                    Some(EventStreamCursor::new(Some(event_id_for_testing()), 10000))
                },
            );

            // Set the epoch check to a high value to simulate a severe lag to epoch check.
            register_fail_point_arg("event_processing_epoch_check", || -> Option<Epoch> {
                Some(100)
            });

            // Create a cluster and send some events.
            let (cluster, events) = cluster_at_epoch1_without_blobs(&[&[0]], None).await?;
            events.send(BlobRegistered::for_testing(BLOB_ID).into())?;
            tokio::time::sleep(Duration::from_secs(10)).await;

            // Expect that the node is not in recovery catch up mode because the lag check should
            // not be triggered.
            assert_ne!(
                cluster.nodes[0].storage_node.inner.storage.node_status()?,
                NodeStatus::RecoveryCatchUp
            );

            Ok(())
        }

        // Tests shard recovery with expired, invalid, and deleted blobs.
        //
        // When `skip_blob_certification_at_recovery_beginning` is true, it simulates the case where
        // the shard recovery of the blob is already in progress, and then the blob becomes expired,
        // invalid, or deleted.
        //
        // Although both tests can run under `cargo nextest`, `check_certification_during_recovery`
        // only works when running in simtest, since it uses failpoints to skip initial blob
        // certification check.
        simtest_param_test! {
            shard_recovery_blob_not_recover_expired_invalid_deleted_blobs -> TestResult: [
                check_certification_at_beginning: (false),
                check_certification_during_recovery: (true),
            ]
        }
        async fn shard_recovery_blob_not_recover_expired_invalid_deleted_blobs(
            skip_blob_certification_at_recovery_beginning: bool,
        ) -> TestResult {
            register_fail_point_if(
                "shard_recovery_skip_initial_blob_certification_check",
                move || skip_blob_certification_at_recovery_beginning,
            );

            let skip_stored_blob_index: [usize; 12] = [3, 4, 5, 9, 10, 11, 15, 18, 19, 20, 21, 22];
            // Blob 9 is a deletable blob.
            let deletable_blob_index: [usize; 1] = [9];

            let (cluster, blob_details, event_senders) = setup_shard_recovery_test_cluster(
                |blob_index| !skip_stored_blob_index.contains(&blob_index),
                // Blob 3 expires at epoch 2, which is the current epoch when
                // `setup_shard_recovery_test_cluster` returns.
                |blob_index| if blob_index == 3 { 2 } else { 42 },
                |blob_index| deletable_blob_index.contains(&blob_index),
            )
            .await?;

            // Delete blob 9 and invalidate blob 19.
            event_senders
                .all_other_node_events
                .send(BlobDeleted::for_testing(*blob_details[9].blob_id()).into())?;

            event_senders
                .all_other_node_events
                .send(InvalidBlobId::for_testing(*blob_details[19].blob_id()).into())?;

            // Make sure that blobs in `skip_stored_blob_index` are not certified in node 0.
            for i in skip_stored_blob_index {
                let blob_info = cluster.nodes[0]
                    .storage_node
                    .inner
                    .storage
                    .get_blob_info(blob_details[i].blob_id());
                if deletable_blob_index.contains(&i) {
                    assert!(matches!(
                        blob_info.unwrap().unwrap().to_blob_status(1),
                        BlobStatus::Deletable {
                            deletable_counts: walrus_storage_node_client::api::DeletableCounts {
                                count_deletable_total: 1,
                                count_deletable_certified: 0,
                            },
                            ..
                        }
                    ));
                } else {
                    assert!(matches!(
                        blob_info.unwrap().unwrap().to_blob_status(1),
                        BlobStatus::Permanent {
                            is_certified: false,
                            ..
                        }
                    ));
                }
            }

            let node_inner = unsafe {
                &mut *(Arc::as_ptr(&cluster.nodes[1].storage_node.inner) as *mut StorageNodeInner)
            };
            node_inner
                .storage
                .create_storage_for_shards(&[ShardIndex(0)])
                .await?;
            let shard_storage_dst = node_inner
                .storage
                .shard_storage(ShardIndex(0))
                .await
                .expect("shard storage should exist");
            shard_storage_dst.update_status_in_test(ShardStatus::None)?;

            cluster.nodes[1]
                .storage_node
                .shard_sync_handler
                .start_sync_shards(vec![ShardIndex(0)], false)
                .await?;

            // Shard recovery should be completed, and all the data should be synced.
            wait_for_shard_in_active_state(shard_storage_dst.as_ref()).await?;
            check_all_blobs_are_synced(
                &blob_details,
                &node_inner.storage,
                shard_storage_dst.as_ref(),
                &[3, 9, 19],
            )?;

            clear_fail_point("shard_recovery_skip_initial_blob_certification_check");

            Ok(())
        }

        // Tests that blob metadata sync can be cancelled when the blob is expired, deleted, or
        //invalidated.
        #[walrus_simtest]
        async fn shard_recovery_cancel_metadata_sync_when_blob_expired_deleted_invalidated()
        -> TestResult {
            register_fail_point_if("get_metadata_return_unavailable", move || true);

            // The test creates 3 blobs:
            //  - Blob 0 expires at epoch 3.
            //  - Blob 1 is a deletable blob.
            //  - Blob 2 is an invalid blob.

            let deletable_blob_index: [usize; 1] = [1];
            let (cluster, blob_details, event_senders) =
                setup_shard_recovery_test_cluster_with_blob_count(
                    3,
                    |_blob_index| true,
                    // Blob 0 expires at epoch 3, which is the next epoch when
                    // `setup_shard_recovery_test_cluster` returns.
                    |blob_index| if blob_index == 0 { 3 } else { 42 },
                    |blob_index| deletable_blob_index.contains(&blob_index),
                )
                .await?;

            // Setup node 1 to sync recovery shard 0 from node 0.
            let node_inner = unsafe {
                &mut *(Arc::as_ptr(&cluster.nodes[1].storage_node.inner) as *mut StorageNodeInner)
            };
            node_inner
                .storage
                .create_storage_for_shards(&[ShardIndex(0)])
                .await?;
            node_inner.storage.clear_metadata_in_test()?;
            node_inner.set_node_status(NodeStatus::RecoverMetadata)?;

            let shard_storage_dst = node_inner
                .storage
                .shard_storage(ShardIndex(0))
                .await
                .expect("shard storage should exist");
            shard_storage_dst.update_status_in_test(ShardStatus::None)?;

            cluster.nodes[1]
                .storage_node
                .shard_sync_handler
                .start_sync_shards(vec![ShardIndex(0)], true)
                .await?;

            tokio::time::sleep(Duration::from_secs(1)).await;
            // After the sync starts, the node status should stay at `RecoverMetadata`.
            assert_eq!(
                node_inner.storage.node_status().unwrap(),
                NodeStatus::RecoverMetadata
            );
            // Setup complete, now we can start the test.

            let unblock = Arc::new(Notify::new());

            // Send blob deletion event for blob 1.
            {
                tracing::info!(
                    "send blob deletion event for blob {:?}",
                    blob_details[1].blob_id()
                );
                // Delete blob 1 and invalidate blob 2.
                event_senders
                    .all_other_node_events
                    .send(BlobDeleted::for_testing(*blob_details[1].blob_id()).into())?;
            }

            // Send invalid blob event for blob 2.
            {
                tracing::info!(
                    "send invalid blob evnt for blob {:?}",
                    blob_details[2].blob_id()
                );
                event_senders
                    .all_other_node_events
                    .send(InvalidBlobId::for_testing(*blob_details[2].blob_id()).into())?;
            }

            // Advance to epoch 3, so that blob 0 expires.
            {
                let unblock_clone = unblock.clone();
                register_fail_point_async("blocking_finishing_epoch_change_start", move || {
                    let unblock_clone = unblock_clone.clone();
                    async move {
                        unblock_clone.notified().await;
                    }
                });

                tracing::info!("advance to epoch 3");
                advance_cluster_to_epoch(
                    &cluster,
                    &[
                        &event_senders.node_0_events,
                        &event_senders.all_other_node_events,
                    ],
                    3,
                )
                .await?;
            }

            // Shard recovery should be completed, and all the data should be synced.
            wait_for_shard_in_active_state(shard_storage_dst.as_ref()).await?;

            // Cleanup the test environment.
            unblock.notify_one();
            clear_fail_point("get_metadata_return_unavailable");
            clear_fail_point("blocking_finishing_epoch_change_start");
            Ok(())
        }

        // Tests that blob events for the same blob are always processed in order. This is a
        // randomized test in a way that the order of events is random based on the random seed.
        // So single test run passing is not a guarantee to cover all the test cases.
        #[walrus_simtest]
        async fn no_out_of_order_blob_certify_and_delete_event_processing() -> TestResult {
            let shards: &[&[u16]] = &[&[1], &[0, 2, 3, 4, 5, 6]];
            let test_shard = ShardIndex(1);

            // Add delay between checking if a blob needs to recover and actual starting the
            // recover process. This window is where other events can break event processing order.
            register_fail_point_async("fail_point_process_blob_certified_event", || async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
            });

            let (cluster, events) = cluster_at_epoch1_without_blobs(shards, None).await?;

            // Randomly generated blob.
            let random_blob = walrus_test_utils::random_data_list(10, 1);

            let config = cluster.encoding_config();
            let blob = EncodedBlob::new(&random_blob[0], config);

            let object_id = ObjectID::random();

            // Do not store the sliver in the first node.
            events.send(
                BlobRegistered {
                    deletable: true,
                    object_id: object_id.clone(),
                    ..BlobRegistered::for_testing(*blob.blob_id())
                }
                .into(),
            )?;
            store_at_shards(&blob, &cluster, |&shard, _| shard != test_shard).await?;

            let node_client = cluster.client(0);
            let pair_to_sync = blob.assigned_sliver_pair(test_shard);

            node_client
                .get_sliver_by_type(blob.blob_id(), pair_to_sync.index(), SliverType::Primary)
                .await
                .expect_err("sliver should not yet be available");

            // Sends certified event followed by delete event.
            events.send(
                BlobCertified {
                    deletable: true,
                    object_id: object_id.clone(),
                    ..BlobCertified::for_testing(*blob.blob_id()).into()
                }
                .into(),
            )?;
            tokio::time::sleep(Duration::from_secs(1)).await;
            events.send(
                BlobDeleted {
                    object_id: object_id.clone(),
                    ..BlobDeleted::for_testing(*blob.blob_id())
                }
                .into(),
            )?;

            // Wait for the blob sync to complete.
            tokio::time::sleep(Duration::from_secs(10)).await;

            // There shouldn't be any blob sync in progress.
            assert!(
                cluster.nodes[0]
                    .storage_node
                    .blob_sync_handler
                    .blob_sync_in_progress()
                    .is_empty()
            );

            clear_fail_point("fail_point_process_blob_certified_event");
            Ok(())
        }
    }

    /// Waits until the storage node processes the specified number of events.
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
                .await
                .expect("Shard storage should be created")
                .status()
                .unwrap(),
            ShardStatus::Active
        );

        assert!(
            node.as_ref()
                .inner
                .storage
                .shard_storage(ShardIndex(1))
                .await
                .is_none()
        );

        assert_eq!(
            node.as_ref()
                .inner
                .storage
                .shard_storage(ShardIndex(27))
                .await
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
        let count_setup_events = setup_events.len() as u64;
        for (index, event) in setup_events
            .iter()
            .chain(repeated_events.iter())
            .enumerate()
        {
            node.storage_node
                .inner
                .storage
                .update_blob_info(index as u64, event)?;
        }
        let intermediate_blob_info = node.storage_node.inner.storage.get_blob_info(&BLOB_ID)?;

        for (index, event) in repeated_events.iter().enumerate() {
            node.storage_node
                .inner
                .storage
                .update_blob_info(index as u64 + count_setup_events, event)?;
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
        contract_service
            .expect_sync_node_params()
            .returning(|_config, _node_cap_id| Ok(()));
        contract_service.expect_epoch_sync_done().never();
        contract_service
            .expect_fixed_system_parameters()
            .returning(|| {
                Ok(FixedSystemParameters {
                    n_shards: NonZeroU16::new(1000).expect("1000 > 0"),
                    max_epochs_ahead: 200,
                    epoch_duration: Duration::from_secs(600),
                    epoch_zero_end: Utc::now() + Duration::from_secs(60),
                })
            });
        contract_service
            .expect_get_node_capability_object()
            .returning(|capability_object_id| {
                Ok(StorageNodeCap {
                    id: capability_object_id.unwrap_or(ObjectID::random()),
                    ..StorageNodeCap::new_for_testing()
                })
            });
        contract_service
            .expect_get_epoch_and_state()
            .returning(move || Ok((0, EpochState::EpochChangeDone(Utc::now()))));
        contract_service
            .expect_last_certified_event_blob()
            .returning(|| Ok(None));
        let node = StorageNodeHandle::builder()
            .with_system_event_provider(vec![ContractEvent::EpochChangeEvent(
                EpochChangeEvent::EpochChangeStart(EpochChangeStart {
                    epoch: 1,
                    event_id: event_id_for_testing(),
                }),
            )])
            .with_shard_assignment(shard_assignment)
            .with_system_contract_service(Arc::new(contract_service))
            .with_node_started(true)
            .with_initial_epoch(initial_epoch)
            .build()
            .await?;

        wait_until_events_processed(&node, 1).await?;

        Ok(())
    }

    async_param_test! {
        process_epoch_change_start_idempotent -> TestResult: [
            wait_for_shard_active: (true),
            do_not_wait_for_shard: (false),
        ]
    }
    async fn process_epoch_change_start_idempotent(wait_for_shard_active: bool) -> TestResult {
        let _ = tracing_subscriber::fmt::try_init();

        let (cluster, events, _blob_detail) =
            cluster_with_initial_epoch_and_certified_blob(&[&[0, 1], &[2, 3]], &[BLOB], 2, None)
                .await?;
        let lookup_service_handle = cluster
            .lookup_service_handle
            .as_ref()
            .expect("should contain lookup service");

        // Set up the committee in a way that shard 1 is moved to the second storage node, and
        // shard 2 is moved to the first storage node.
        let committees = lookup_service_handle.committees.lock().unwrap().clone();
        let mut next_committee = (**committees.current_committee()).clone();
        next_committee.epoch += 1;
        let moved_index_0 = next_committee.members_mut()[0].shard_ids.remove(1);
        let moved_index_1 = next_committee.members_mut()[1].shard_ids.remove(0);
        next_committee.members_mut()[1]
            .shard_ids
            .push(moved_index_0);
        next_committee.members_mut()[0]
            .shard_ids
            .push(moved_index_1);

        lookup_service_handle.set_next_epoch_committee(next_committee);

        assert_eq!(
            cluster
                .lookup_service_handle
                .as_ref()
                .expect("should contain lookup service")
                .advance_epoch(),
            3
        );

        let processed_event_count = &cluster.nodes[1]
            .storage_node
            .inner
            .storage
            .get_sequentially_processed_event_count()?;

        // Sends one epoch change start event.
        events.send(ContractEvent::EpochChangeEvent(
            EpochChangeEvent::EpochChangeStart(EpochChangeStart {
                epoch: 3,
                event_id: walrus_sui::test_utils::event_id_for_testing(),
            }),
        ))?;

        if wait_for_shard_active {
            wait_until_events_processed(&cluster.nodes[1], processed_event_count + 1).await?;
            wait_for_shard_in_active_state(
                &cluster.nodes[1]
                    .storage_node
                    .inner
                    .storage
                    .shard_storage(ShardIndex(1))
                    .await
                    .unwrap(),
            )
            .await?;
        }

        // Sends another epoch change start for the same event to simulate duplicate events.
        events.send(ContractEvent::EpochChangeEvent(
            EpochChangeEvent::EpochChangeStart(EpochChangeStart {
                epoch: 3,
                event_id: walrus_sui::test_utils::event_id_for_testing(),
            }),
        ))?;

        wait_until_events_processed(&cluster.nodes[1], processed_event_count + 2).await?;

        assert_eq!(
            cluster
                .lookup_service_handle
                .as_ref()
                .expect("should contain lookup service")
                .advance_epoch(),
            4
        );
        advance_cluster_to_epoch(&cluster, &[&events], 4).await?;

        Ok(())
    }

    async_param_test! {
        test_extend_blob_also_extends_registration -> TestResult: [
            permanent: (false),
            deletable: (true),
        ]
    }
    async fn test_extend_blob_also_extends_registration(deletable: bool) -> TestResult {
        let _ = tracing_subscriber::fmt::try_init();

        let (cluster, events, _blob_detail) =
            cluster_with_initial_epoch_and_certified_blob(&[&[0, 1, 2, 3]], &[], 1, None).await?;

        let blob_details = EncodedBlob::new(BLOB, cluster.encoding_config());
        events.send(
            BlobRegistered {
                end_epoch: 3,
                deletable,
                ..BlobRegistered::for_testing(*blob_details.blob_id())
            }
            .into(),
        )?;
        store_at_shards(&blob_details, &cluster, |_, _| true).await?;
        events.send(
            BlobCertified {
                end_epoch: 3,
                deletable,
                ..BlobCertified::for_testing(*blob_details.blob_id())
            }
            .into(),
        )?;

        events.send(
            BlobCertified {
                end_epoch: 6,
                is_extension: true,
                deletable,
                ..BlobCertified::for_testing(*blob_details.blob_id())
            }
            .into(),
        )?;

        advance_cluster_to_epoch(&cluster, &[&events], 5).await?;

        assert!(
            cluster.nodes[0]
                .storage_node
                .inner
                .is_blob_certified(blob_details.blob_id())?
        );

        assert!(
            cluster.nodes[0]
                .storage_node
                .inner
                .is_blob_registered(blob_details.blob_id())?
        );

        Ok(())
    }

    // Tests that blob extension is correctly handled after multiple epochs.
    #[tokio::test]
    async fn extend_blob_after_multiple_epochs() -> TestResult {
        let _ = tracing_subscriber::fmt::try_init();

        let (cluster, events, _blob_detail) =
            cluster_with_initial_epoch_and_certified_blob(&[&[0, 1, 2, 3]], &[], 1, None).await?;

        let blob_details = EncodedBlob::new(BLOB, cluster.encoding_config());

        tracing::info!("blob to be extended: {:?}", blob_details.blob_id());
        let object_id = ObjectID::random();
        events.send(
            BlobRegistered {
                end_epoch: 10,
                object_id,
                ..BlobRegistered::for_testing(*blob_details.blob_id())
            }
            .into(),
        )?;
        store_at_shards(&blob_details, &cluster, |_, _| true).await?;
        events.send(
            BlobCertified {
                end_epoch: 10,
                object_id,
                ..BlobCertified::for_testing(*blob_details.blob_id())
            }
            .into(),
        )?;

        // Advance to multiple epochs before extending the blob.
        advance_cluster_to_epoch(&cluster, &[&events], 4).await?;

        events.send(
            BlobCertified {
                end_epoch: 20,
                is_extension: true,
                object_id,
                ..BlobCertified::for_testing(*blob_details.blob_id())
            }
            .into(),
        )?;

        wait_until_events_processed(&cluster.nodes[0], 9).await?;
        Ok(())
    }
}
