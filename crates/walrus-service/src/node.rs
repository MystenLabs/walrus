// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Walrus storage node.

use std::{
    collections::{BTreeMap, hash_map::Entry},
    future::Future,
    num::{NonZero, NonZeroU16, NonZeroUsize},
    pin::Pin,
    sync::{
        Arc,
        Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, anyhow, bail};
use blob_event_processor::BlobEventProcessor;
use blob_retirement_notifier::BlobRetirementNotifier;
use chrono::Utc;
use committee::{BeginCommitteeChangeError, EndCommitteeChangeError};
use consistency_check::StorageNodeConsistencyCheckConfig;
use epoch_change_driver::EpochChangeDriver;
use errors::{ListSymbolsError, Unavailable};
use fastcrypto::traits::KeyPair;
use futures::{
    FutureExt as _,
    StreamExt,
    TryFutureExt as _,
    stream::{self, FuturesOrdered},
};
use itertools::Either;
use moka::{future::Cache, policy::EvictionPolicy};
use node_recovery::NodeRecoveryHandler;
use rand::{Rng, SeedableRng, rngs::StdRng, thread_rng};
use recovery_symbol_service::{RecoverySymbolRequest, RecoverySymbolService};
use serde::Serialize;
use start_epoch_change_finisher::StartEpochChangeFinisher;
pub use storage::{DatabaseConfig, DatabaseTableOptionsFactory, NodeStatus, Storage};
use storage::{StorageShardLock, blob_info::PerObjectBlobInfoApi};
#[cfg(msim)]
use sui_macros::fail_point_if;
use sui_macros::{fail_point_arg, fail_point_async};
use sui_types::{base_types::ObjectID, event::EventID};
use system_events::{CompletableHandle, EVENT_ID_FOR_CHECKPOINT_EVENTS, EventHandle};
use thread_pool::{BoundedThreadPool, ThreadPoolBuilder};
#[cfg(not(any(test, msim)))]
use tokio::time::sleep;
use tokio::{
    select,
    sync::{Notify, RwLock, watch},
    time::Instant,
};
use tokio_metrics::TaskMonitor;
use tokio_util::sync::CancellationToken;
use tower::{Service, ServiceExt};
use tracing::{Instrument as _, Span, field};
use typed_store::{TypedStoreError, rocks::MetricConf};
use walrus_core::{
    BlobId,
    EncodingType,
    Epoch,
    InconsistencyProof,
    PublicKey,
    ShardIndex,
    Sliver,
    SliverIndex,
    SliverPairIndex,
    SliverType,
    SymbolId,
    by_axis::{self, Axis},
    encoding::{
        DecodingSymbol,
        EitherDecodingSymbol,
        EncodingAxis,
        EncodingConfig,
        GeneralRecoverySymbol,
        Primary,
        RecoverySymbolError,
        Secondary,
        SliverData,
        source_symbols_for_n_shards,
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
            ProtocolEvent,
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
use walrus_sui::{client::FixedSystemParameters, types::move_structs::EpochState};
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
        SetRecoveryDeferralError,
        ShardNotAssigned,
        StoreMetadataError,
        StoreSliverError,
        SyncNodeConfigError,
        SyncShardServiceError,
    },
    event_blob_writer::EventBlobWriterFactory,
    metrics::{NodeMetricSet, STATUS_PENDING, STATUS_PERSISTED, TelemetryLabel as _},
    pending_metadata_cache::PendingMetadataCache,
    pending_sliver_cache::{PendingSliverCache, PendingSliverCacheError},
    registration_notifier::RegistrationNotifier,
    shard_sync::ShardSyncHandler,
    storage::{
        ShardStatus,
        ShardStorage,
        blob_info::{BlobInfoApi, CertifiedBlobInfoApi},
    },
    system_events::EventManager,
};
use crate::{
    common::config::SuiConfig,
    event::{
        event_blob_downloader::{EventBlobDownloader, LastCertifiedEventBlob},
        event_processor::{
            config::{EventProcessorRuntimeConfig, SystemConfig},
            processor::EventProcessor,
        },
        events::{
            CheckpointEventPosition,
            EventStreamCursor,
            EventStreamElement,
            EventStreamWithStartingIndices,
            InitState,
            PositionedStreamEvent,
        },
    },
    node::{
        blob_event_processor::pending_events::PendingEventCounter,
        config::LiveUploadDeferralConfig,
        event_blob_writer::EventBlobWriter,
        garbage_collector::GarbageCollector,
        wal_price_monitor::WalPriceMonitor,
    },
    utils::ShardDiffCalculator,
};

pub mod committee;
pub mod config;
pub mod contract_service;
pub mod dbtool;
pub mod event_blob_writer;
mod ref_counted_notify_map;
mod registration_notifier;
pub mod server;
pub mod system_events;

pub(crate) mod blob_event_processor;
pub(crate) mod consistency_check;
pub(crate) mod db_checkpoint;
pub(crate) mod errors;
pub(crate) mod metrics;

mod blob_retirement_notifier;
mod blob_sync;
mod config_synchronizer;
mod epoch_change_driver;
mod garbage_collector;
mod network_overrides;
mod node_recovery;
mod pending_metadata_cache;
mod pending_sliver_cache;
mod recovery_symbol_service;
mod shard_sync;
mod start_epoch_change_finisher;
pub(crate) mod storage;
mod thread_pool;
mod wal_price_monitor;

pub use config_synchronizer::{ConfigLoader, ConfigSynchronizer, StorageNodeConfigLoader};
pub use garbage_collector::GarbageCollectionConfig;

#[cfg(any(test, feature = "test-utils"))]
#[allow(
    missing_docs,
    clippy::unwrap_used,
    missing_debug_implementations,
    private_interfaces
)]
pub mod test_helpers;

// The number of events are dominated by the checkpoints, as we don't expect all checkpoints
// contain Walrus events. 20K events per recording is roughly 1 recording per 1.5 hours.
#[cfg(not(msim))]
const NUM_EVENTS_PER_DIGEST_RECORDING: u64 = 20_000;

// In simtest, we record event source for events to make event index consistency checking more
// accurate.
#[cfg(msim)]
const NUM_EVENTS_PER_DIGEST_RECORDING: u64 = 1;

const NUM_DIGEST_BUCKETS: u64 = 10;
const CHECKPOINT_EVENT_POSITION_SCALE: u64 = 100;

/// Indicates how the node should treat uploads.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum UploadIntent {
    /// Store immediately; reject if the blob is not yet registered.
    #[default]
    Immediate,
    /// Cache the upload until the blob registration is observed.
    Pending,
}

impl UploadIntent {
    /// Returns true if the upload should be cached until registration completes.
    pub const fn is_pending(self) -> bool {
        matches!(self, Self::Pending)
    }

    /// Builds an [`UploadIntent`] from the provided query flag.
    pub const fn from_pending_flag(pending: bool) -> Self {
        if pending {
            Self::Pending
        } else {
            Self::Immediate
        }
    }
}

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
        intent: UploadIntent,
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
    ) -> impl Future<Output = Result<Arc<Sliver>, RetrieveSliverError>> + Send;

    /// Stores the primary or secondary encoding for a blob for a shard held by this storage node.
    fn store_sliver(
        &self,
        blob_id: BlobId,
        sliver_pair_index: SliverPairIndex,
        sliver: Sliver,
        intent: UploadIntent,
    ) -> impl Future<Output = Result<bool, StoreSliverError>> + Send;

    /// Retrieves a signed confirmation over the identifiers of the shards storing their respective
    /// sliver-pairs for their BlobIds.
    fn compute_storage_confirmation(
        &self,
        blob_id: &BlobId,
        blob_persistence_type: &BlobPersistenceType,
    ) -> impl Future<Output = Result<StorageConfirmation, ComputeStorageConfirmationError>> + Send;

    /// Waits for the blob registration event to be observed by the node.
    ///
    /// Returns true if the blob is registered before the timeout elapses.
    fn wait_for_registration(
        &self,
        blob_id: &BlobId,
        timeout: Duration,
    ) -> impl Future<Output = bool> + Send;

    /// Verifies an inconsistency proof and provides a signed attestation for it, if valid.
    fn verify_inconsistency_proof(
        &self,
        blob_id: &BlobId,
        inconsistency_proof: InconsistencyProof,
    ) -> impl Future<Output = Result<InvalidBlobIdAttestation, InconsistencyProofError>> + Send;

    /// Retrieves multiple recovery symbols.
    ///
    /// Attempts to retrieve multiple recovery symbols, skipping any failures that occur. Returns an
    /// error if none of the requested symbols can be retrieved.
    fn retrieve_multiple_recovery_symbols(
        &self,
        blob_id: &BlobId,
        filter: RecoverySymbolsFilter,
    ) -> impl Future<Output = Result<Vec<GeneralRecoverySymbol>, ListSymbolsError>> + Send;

    /// Retrieves multiple decoding symbols.
    ///
    /// Attempts to retrieve multiple decoding symbols, skipping any failures that occur.
    /// Returns an error if none of the requested symbols can be retrieved.
    fn retrieve_multiple_decoding_symbols(
        &self,
        blob_id: &BlobId,
        target_slivers: Vec<SliverIndex>,
        target_type: SliverType,
    ) -> impl Future<
        Output = Result<BTreeMap<SliverIndex, Vec<EitherDecodingSymbol>>, ListSymbolsError>,
    > + Send;

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

    /// Sets a deferral window for recovery for the specified blob ID.
    fn set_recovery_deferral(
        &self,
        _blob_id: BlobId,
        _defer_for: std::time::Duration,
    ) -> impl Future<Output = Result<(), SetRecoveryDeferralError>> + Send {
        async { Err(SetRecoveryDeferralError::Unsupported) }
    }
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
        tracing::info!("building storage node with config: {:#?}", config);
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
                system_pkg_id: read_client.walrus_package_id(),
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
    pub(crate) inner: Arc<StorageNodeInner>,
    pub(crate) blob_sync_handler: Arc<BlobSyncHandler>,
    pub(crate) shard_sync_handler: ShardSyncHandler,
    epoch_change_driver: EpochChangeDriver,
    pub(crate) start_epoch_change_finisher: StartEpochChangeFinisher,
    num_blob_event_processors: NonZeroUsize,
    pending_event_counter: PendingEventCounter,
    node_recovery_handler: NodeRecoveryHandler,
    garbage_collector: GarbageCollector,
    event_blob_writer_factory: Option<EventBlobWriterFactory>,
    config_synchronizer: Option<Arc<ConfigSynchronizer>>,
    _wal_price_monitor: Option<Arc<WalPriceMonitor>>,
}

type RecoveryDeferralEntry = (
    std::time::Instant,
    std::sync::Arc<tokio_util::sync::CancellationToken>,
);
type RecoveryDeferralMap = std::collections::HashMap<BlobId, RecoveryDeferralEntry>;
type RecoveryDeferrals = std::sync::Arc<RwLock<RecoveryDeferralMap>>;
type SliverRefCacheKey = (BlobId, SliverPairIndex, SliverType);

/// The internal state of a Walrus storage node.
#[derive(Debug)]
pub struct StorageNodeInner {
    protocol_key_pair: ProtocolKeyPair,
    pub(crate) storage: Storage,
    system_parameters: FixedSystemParameters,
    pub(crate) encoding_config: Arc<EncodingConfig>,
    event_manager: Box<dyn EventManager>,
    contract_service: Arc<dyn SystemContractService>,
    committee_service: Arc<dyn CommitteeService>,
    start_time: Instant,
    metrics: Arc<NodeMetricSet>,
    is_shutting_down: AtomicBool,
    blocklist: Arc<Blocklist>,
    node_capability: ObjectID,
    blob_retirement_notifier: Arc<BlobRetirementNotifier>,
    registration_notifier: Arc<RegistrationNotifier>,
    symbol_service: RecoverySymbolService,
    thread_pool: BoundedThreadPool,
    registry: Registry,
    pub(crate) pending_metadata_cache: PendingMetadataCache,
    pub(crate) pending_sliver_cache: PendingSliverCache,
    // Below tokio watch channel holds the current event epoch that the node is processing.
    // Storage node is a state machine processing events, and in many places, we need to use
    // the current event epoch which can be lagging behind the latest Walrus epoch on chain.
    //
    // Receiver for watching the latest event epoch.
    latest_event_epoch_watcher: watch::Receiver<Option<Epoch>>,
    // Sender for updating the latest event epoch.
    latest_event_epoch_sender: watch::Sender<Option<Epoch>>,
    consistency_check_config: StorageNodeConsistencyCheckConfig,
    checkpoint_manager: Option<Arc<DbCheckpointManager>>,
    garbage_collection_config: GarbageCollectionConfig,
    recovery_deferrals: RecoveryDeferrals,
    recovery_deferral_notify: Arc<Notify>,
    recovery_deferral_cleanup_token: CancellationToken,
    live_upload_deferral_config: LiveUploadDeferralConfig,
    sliver_ref_cache: Cache<SliverRefCacheKey, Arc<RwLock<Weak<Sliver>>>>,
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
        let metrics = Arc::new(NodeMetricSet::new(registry));

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

        // Initialize WAL price monitor if enabled
        let (wal_price_monitor, current_wal_price) =
            if config.wal_price_monitor.enable_wal_price_monitor {
                let monitor = Arc::new(
                    WalPriceMonitor::start(config.wal_price_monitor.clone(), metrics.clone()).await,
                );
                let current_wal_price = monitor.get_current_price().await;
                (Some(monitor), current_wal_price)
            } else {
                (None, None)
            };

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
                    wal_price_monitor.clone(),
                    registry,
                )));

        contract_service
            .sync_node_params(config, node_capability.id, current_wal_price)
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

        // General thread pool: used for metadata verification and other high-priority CPU work.
        // Runs at the default OS scheduling priority (nice=0).
        let mut general_builder = ThreadPoolBuilder::default();
        general_builder
            .name("general")
            .thread_name("walrus-cpu-general")
            .max_concurrent(config.thread_pool.max_concurrent_general_tasks)
            .metrics_registry(registry.clone());
        let thread_pool = general_builder.build_bounded();

        // Recovery thread pool: used exclusively by RecoverySymbolService.
        // Worker threads are niced at startup so the OS scheduler always prefers general pool
        // threads over recovery threads when both have runnable work.
        let mut recovery_builder = ThreadPoolBuilder::default();
        recovery_builder
            .name("recovery")
            .thread_name("walrus-cpu-recovery")
            .max_concurrent(
                config
                    .thread_pool
                    .max_concurrent_recovery_tasks
                    .or(config.thread_pool.max_concurrent_general_tasks),
            )
            .nice_level(config.thread_pool.recovery_nice_level)
            .metrics_registry(registry.clone());

        let recovery_pool = recovery_builder.build_bounded();
        let blocklist: Arc<Blocklist> = Arc::new(Blocklist::new_with_metrics(
            &config.blocklist_path,
            Some(registry),
        )?);
        let pending_metadata_cache = PendingMetadataCache::new(
            config.pending_metadata_cache.max_cached_entries,
            config.pending_metadata_cache.cache_ttl,
            metrics.clone(),
        );
        let checkpoint_manager =
            match DbCheckpointManager::new(storage.get_db(), config.checkpoint_config.clone()) {
                Ok(manager) => Some(Arc::new(manager)),
                Err(error) => {
                    tracing::warn!(?error, "failed to initialize checkpoint manager");
                    None
                }
            };
        let system_parameters = contract_service.fixed_system_parameters();
        let (latest_event_epoch_sender, latest_event_epoch_watcher) = watch::channel(None);
        let inner = Arc::new(StorageNodeInner {
            protocol_key_pair: config
                .protocol_key_pair
                .get()
                .expect("protocol key pair must already be loaded")
                .clone(),
            storage,
            system_parameters,
            event_manager,
            contract_service: contract_service.clone(),
            committee_service,
            metrics: metrics.clone(),
            start_time,
            is_shutting_down: false.into(),
            blocklist: blocklist.clone(),
            node_capability: node_capability.id,
            blob_retirement_notifier: Arc::new(BlobRetirementNotifier::new(metrics.clone())),
            registration_notifier: Arc::new(RegistrationNotifier::new()),
            symbol_service: RecoverySymbolService::new(
                config.blob_recovery.max_proof_cache_elements,
                encoding_config.clone(),
                recovery_pool,
                registry,
            ),
            thread_pool,
            encoding_config,
            registry: registry.clone(),
            pending_metadata_cache,
            pending_sliver_cache: PendingSliverCache::new(
                config.pending_sliver_cache.max_cached_slivers,
                config.pending_sliver_cache.max_cached_bytes,
                config.pending_sliver_cache.max_cached_sliver_bytes,
                config.pending_sliver_cache.cache_ttl,
                metrics.clone(),
            ),
            latest_event_epoch_sender,
            latest_event_epoch_watcher,
            consistency_check_config: config.consistency_check.clone(),
            checkpoint_manager,
            garbage_collection_config: config.garbage_collection,
            recovery_deferrals: std::sync::Arc::new(RwLock::new(Default::default())),
            recovery_deferral_notify: Arc::new(Notify::new()),
            recovery_deferral_cleanup_token: CancellationToken::new(),
            live_upload_deferral_config: config.live_upload_deferral.clone(),
            sliver_ref_cache: Cache::builder()
                .name("sliver-refs")
                .eviction_policy(EvictionPolicy::lru())
                .max_capacity(config.sliver_reference_cache_max_entries)
                .build(),
        });

        blocklist.start_refresh_task();

        inner.init_gauges()?;
        inner.start_recovery_deferral_cleanup_task();

        let blob_sync_handler = Arc::new(BlobSyncHandler::new(
            inner.clone(),
            config.blob_recovery.max_concurrent_blob_syncs,
            config.blob_recovery.max_concurrent_sliver_syncs,
            config.blob_recovery.monitor_interval,
        ));

        let shard_sync_handler =
            ShardSyncHandler::new(inner.clone(), config.shard_sync_config.clone());
        // Upon restart, resume any ongoing blob syncs if there is any.
        shard_sync_handler.restart_syncs().await?;

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

        tracing::debug!(
            "num_checkpoints_per_blob for event blobs: {:?}",
            node_params.num_checkpoints_per_blob
        );
        let event_blob_downloader = Self::get_event_blob_downloader_from_config(config).await?;
        let mut last_certified_event_blob = None;
        if let Some(downloader) = event_blob_downloader {
            last_certified_event_blob = downloader
                .get_last_certified_event_blob()
                .await
                .ok()
                .flatten()
                .map(LastCertifiedEventBlob::EventBlobWithMetadata);
        }
        if last_certified_event_blob.is_none() {
            last_certified_event_blob = contract_service
                .last_certified_event_blob()
                .await
                .ok()
                .flatten()
                .map(LastCertifiedEventBlob::EventBlob);
        }
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

        let garbage_collector =
            GarbageCollector::new(config.garbage_collection, inner.clone(), metrics);

        Ok(StorageNode {
            inner,
            blob_sync_handler,
            shard_sync_handler,
            epoch_change_driver,
            start_epoch_change_finisher,
            node_recovery_handler,
            num_blob_event_processors: config.blob_event_processor_config.num_workers,
            pending_event_counter: PendingEventCounter::default(),
            garbage_collector,
            event_blob_writer_factory,
            config_synchronizer,
            _wal_price_monitor: wal_price_monitor,
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

        if let Err(error) = self.check_and_start_garbage_collection_on_startup().await {
            tracing::warn!(
                ?error,
                "failed to check and start garbage collection on startup"
            );
        }

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
                self.garbage_collector.abort().await;
            },
            blob_sync_result = BlobSyncHandler::spawn_task_monitor(
                self.blob_sync_handler.clone()
            ) => {
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

    /// Returns the pending event counter for blob events.
    pub fn get_pending_event_counter(&self) -> PendingEventCounter {
        self.pending_event_counter.clone()
    }

    /// Wait for the storage node to be in at least the provided epoch.
    ///
    /// Returns the epoch to which the storage node arrived, which may be later than the requested
    /// epoch.
    pub async fn wait_for_epoch(&self, epoch: Epoch) -> Epoch {
        self.inner
            .latest_event_epoch_watcher
            .clone()
            .wait_for(|current_epoch| {
                current_epoch.is_some_and(|current_epoch| current_epoch >= epoch)
            })
            .await
            .expect("current_epoch channel cannot be dropped while holding a ref to self")
            .expect("current_epoch should be some")
    }

    /// Continues the event stream from the last committed event.
    ///
    /// This function is used to continue the event stream from the last committed event. It also
    /// handles the special case of a fresh node starting with incomplete event history.
    async fn continue_event_stream(
        &self,
        event_blob_writer_cursor: EventStreamCursor,
        storage_node_cursor: EventStreamCursor,
        event_blob_writer: &mut Option<EventBlobWriter>,
    ) -> anyhow::Result<EventStreamWithStartingIndices<'_>> {
        let event_cursor = storage_node_cursor.min(event_blob_writer_cursor);
        let lowest_needed_event_index = event_cursor.element_index;
        let processing_starting_index = storage_node_cursor.element_index;
        let writer_starting_index = event_blob_writer_cursor.element_index;

        tracing::info!(
            "continue event stream initial cursors: event blob writer cursor: {:?}, \
            storage node cursor: {:?}",
            event_blob_writer_cursor,
            storage_node_cursor
        );

        let default_result = || async {
            Ok(EventStreamWithStartingIndices {
                event_stream: Pin::from(self.inner.event_manager.events(event_cursor).await?),
                processing_starting_index,
                writer_starting_index,
            })
        };

        let Some(init_state) = self.inner.event_manager.init_state(event_cursor).await? else {
            return default_result().await;
        };

        let first_available_event_index = init_state.event_cursor.element_index;
        if first_available_event_index <= lowest_needed_event_index {
            return default_result().await;
        }

        // If we reach this point, we have to reposition at least one of the event cursors.
        // Important: We need to make sure we start the event stream from the correct cursor.

        // Use the event cursor from the init state, as it is the earliest event that we can
        // process.
        let new_event_cursor = init_state.event_cursor;
        assert!(
            new_event_cursor > event_cursor,
            "the event cursor from the init state must be greater than the persisted cursor"
        );

        // Reposition the node event cursor if it is behind the first available event.
        self.reposition_event_cursor_if_starting_with_incomplete_history(
            storage_node_cursor.element_index,
            init_state.clone(),
        )?;

        // Reposition the event blob writer if it is behind the first available event.
        if let Some(writer) = event_blob_writer
            && writer_starting_index < first_available_event_index
        {
            tracing::info!(
                "repositioning event blob writer cursor from {} to {}",
                writer_starting_index,
                first_available_event_index
            );
            writer.update(init_state)?;
        }

        // Reset the starting indices to the first event that is available for processing and
        // writing.
        let new_starting_index = new_event_cursor.element_index;
        let processing_starting_index = processing_starting_index.max(new_starting_index);
        let writer_starting_index = writer_starting_index.max(new_starting_index);

        Ok(EventStreamWithStartingIndices {
            event_stream: Pin::from(self.inner.event_manager.events(new_event_cursor).await?),
            processing_starting_index,
            writer_starting_index,
        })
    }

    /// Checks if the node is a fresh node starting with incomplete event history, adjusts the event
    /// cursor and status if necessary, and returns the new event cursor.
    ///
    /// Normally, when an existing node restarts, the next event to be processed exists is available
    /// through checkpoints or the event blobs. In this case, no special handling is necessary, and
    /// the node picks up processing right where it left off.
    ///
    /// However, there's a special case during node bootstrapping (when starting with no previous
    /// state) and event processor bootstrapping itself from event blobs because older event blobs
    /// might have expired due to the MAX_EPOCHS_AHEAD limit.
    ///
    /// Let's say the earliest available event starts at index `N` (where `N > 0`) but the new node
    /// wants to start processing from index 0. In this scenario, we actually want
    /// to reposition the node's cursor to index `N`.
    ///
    /// We also need to set the node status to `RecoveryCatchUpWithIncompleteHistory` to indicate
    /// that the node is missing some early events.
    fn reposition_event_cursor_if_starting_with_incomplete_history(
        &self,
        next_event_index: u64,
        init_state: InitState,
    ) -> anyhow::Result<()> {
        let first_available_event_index = init_state.event_cursor.element_index;
        if next_event_index >= first_available_event_index {
            // Standard case: the node is not behind the first available event.
            return Ok(());
        }
        if next_event_index != 0 {
            // TODO(WAL-894): Implement recovery with incomplete event history for nodes that are
            // not new.
            unimplemented!(
                "the node is too far behind for normal recovery and recovery with incomplete event \
                history is only implemented for fresh nodes; \
                please wipe the DB and restart the node"
            );
        }

        #[cfg(msim)]
        sui_macros::fail_point!("fail_point_recovery_with_incomplete_history");

        // `first_complete_epoch` is the oldest epoch for which all events are guaranteed to be
        // still available through non-expired event blobs.
        let current_epoch = self.inner.committee_service.get_epoch();
        let first_complete_epoch =
            current_epoch + 1 - self.inner.system_parameters.max_epochs_ahead;

        assert!(
            // Note that the first event of the first non-expired event blob cannot be the
            // `EpochChangeStart` for the first complete epoch:
            //
            // Assume we have epoch `E` as the first complete epoch and that event blob `B` has the
            // `EpochChangeStart` for epoch `E` as its first event. This means the previous event
            // blob `B-1` cannot have been certified in epoch `E-1` as we would otherwise miss its
            // registration and certification events. This means that `B-1` must have been certified
            // in epoch `E` (or later). So we definitely have events for epoch `E-1` in our
            // incomplete history.
            init_state.epoch < first_complete_epoch,
            "inconsistent event-blob state: we do not have access to all events from epoch \
            {first_complete_epoch}",
        );

        tracing::info!(
            "repositioning node event cursor from {} to {} and setting node status to \
            RecoveryCatchUpWithIncompleteHistory (first complete epoch: {}, current epoch: {})",
            next_event_index,
            first_available_event_index,
            first_complete_epoch,
            current_epoch,
        );
        self.inner.reposition_event_cursor(
            init_state
                .event_cursor
                .event_id
                .unwrap_or(EVENT_ID_FOR_CHECKPOINT_EVENTS),
            first_available_event_index,
        )?;
        self.inner
            .set_node_status(NodeStatus::RecoveryCatchUpWithIncompleteHistory {
                first_complete_epoch,
                epoch_at_start: current_epoch,
            })?;

        // Remark: Given that we only reach this point when we start from the very beginning,
        // clearing the blob-info table may not actually be necessary. However, it also doesn't hurt
        // in that case.
        self.inner.storage.clear_blob_info_table()?;

        Ok(())
    }

    async fn get_event_blob_downloader_from_config(
        config: &StorageNodeConfig,
    ) -> anyhow::Result<Option<EventBlobDownloader>> {
        let sui_config: Option<SuiConfig> = config.sui.as_ref().cloned();
        let Some(sui_config) = sui_config else {
            return Ok(None);
        };
        let sui_read_client = sui_config.new_read_client().await?;
        let walrus_client = crate::common::utils::create_walrus_client_with_refresher(
            sui_config.contract_config.clone(),
            sui_read_client.clone(),
        )
        .await?;
        Ok(Some(EventBlobDownloader::new(
            walrus_client,
            sui_read_client,
        )))
    }

    fn storage_node_cursor(&self) -> anyhow::Result<EventStreamCursor> {
        let storage = &self.inner.storage;
        let (from_event_id, next_event_index) = storage
            .get_event_cursor_and_next_index()?
            .map_or((None, 0), |e| (Some(e.event_id()), e.next_event_index()));
        Ok(EventStreamCursor::new(from_event_id, next_event_index))
    }

    #[cfg(not(msim))]
    fn get_storage_node_cursor(&self) -> anyhow::Result<EventStreamCursor> {
        self.storage_node_cursor()
    }

    // A version of `get_storage_node_cursor` that can be used in simtest which can control the
    // initial cursor.
    #[cfg(msim)]
    fn get_storage_node_cursor(&self) -> anyhow::Result<EventStreamCursor> {
        let mut cursor = self.storage_node_cursor()?;
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
        let blob_event_processor = BlobEventProcessor::new(
            self.inner.clone(),
            self.blob_sync_handler.clone(),
            self.num_blob_event_processors,
            self.pending_event_counter.clone(),
        );

        let writer_cursor = match self.event_blob_writer_factory {
            Some(ref factory) => factory.event_cursor().unwrap_or_default(),
            None => EventStreamCursor::new(None, u64::MAX),
        };
        let storage_node_cursor = self.get_storage_node_cursor()?;

        let mut event_blob_writer = match &self.event_blob_writer_factory {
            Some(factory) => Some(factory.create().await?),
            None => None,
        };
        let EventStreamWithStartingIndices {
            event_stream,
            processing_starting_index,
            writer_starting_index,
        } = self
            .continue_event_stream(writer_cursor, storage_node_cursor, &mut event_blob_writer)
            .await?;
        tracing::info!(
            processing_starting_index,
            writer_starting_index,
            "starting to process events",
        );

        let next_event_index = processing_starting_index.min(writer_starting_index);
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
            self.inner
                .metrics
                .started_processing_event(stream_element.checkpoint_event_position);

            let event_label: &'static str = stream_element.element.label();
            let monitor = task_monitors.get_or_insert_with_task_name(&event_label, || {
                format!("process_event {event_label}")
            });

            let task = async {
                fail_point_arg!("event_processing_epoch_check", |epoch: Epoch| {
                    tracing::info!("updating epoch check to {:?}", epoch);
                    maybe_epoch_at_start = Some(epoch);
                });

                if element_index >= processing_starting_index {
                    sui_macros::fail_point!("process-event-before");
                    self.process_event(
                        &blob_event_processor,
                        stream_element.clone(),
                        element_index,
                        &mut maybe_epoch_at_start,
                    )
                    .await?;
                    sui_macros::fail_point!("process-event-after");
                }

                if element_index >= writer_starting_index
                    && let Some(writer) = &mut event_blob_writer
                {
                    sui_macros::fail_point!("write-event-before");
                    writer.write(stream_element.clone(), element_index).await?;
                    sui_macros::fail_point!("write-event-after");
                }

                anyhow::Result::<()>::Ok(())
            };

            TaskMonitor::instrument(&monitor, task).await?;

            self.inner
                .metrics
                .completed_processing_event(stream_element.checkpoint_event_position);
        }

        bail!("event stream for blob events stopped")
    }

    /// Returns `true` if the node is recovering with incomplete history and the event is not
    /// relevant to the recovery because it is before the first complete epoch of the recovery.
    fn should_skip_event_in_incomplete_history_before_first_complete_epoch(
        &self,
        stream_element: &PositionedStreamEvent,
    ) -> anyhow::Result<bool> {
        let NodeStatus::RecoveryCatchUpWithIncompleteHistory {
            first_complete_epoch,
            ..
        } = self.inner.storage.node_status()?
        else {
            return Ok(false);
        };

        // When current event epoch is not set, we are processing events before the first epoch
        // change start event, which should be excluded from processing.
        if let Some(current_event_epoch) = self.inner.try_get_current_event_epoch() {
            if current_event_epoch >= first_complete_epoch {
                return Ok(false);
            }

            if let EventStreamElement::ContractEvent(ContractEvent::EpochChangeEvent(
                EpochChangeEvent::EpochChangeStart(EpochChangeStart { epoch, .. }),
            )) = &stream_element.element
                && *epoch >= first_complete_epoch
            {
                // Processing the `EpochChangeStart` event for the first complete epoch will set
                // the `current_event_epoch` to `epoch`, such that we will take the previous
                // if-statement and return `false` for all future events.
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Process an event.
    ///
    /// When `maybe_epoch_at_start` is provided, it indicates the node has not processed any events
    /// yet, and this function needs to check if the node is severely lagging behind.
    #[tracing::instrument(skip_all)]
    async fn process_event(
        &self,
        blob_event_processor: &BlobEventProcessor,
        stream_element: PositionedStreamEvent,
        element_index: u64,
        maybe_epoch_at_start: &mut Option<Epoch>,
    ) -> anyhow::Result<()> {
        sui_macros::fail_point!("fail_point_process_event");
        let _scope = monitored_scope::monitored_scope("ProcessEvent");

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

        if maybe_epoch_at_start.is_some()
            && let EventStreamElement::ContractEvent(ref event) = stream_element.element
        {
            self.check_if_node_lagging_and_enter_recovery_mode(
                event,
                node_status,
                maybe_epoch_at_start,
            )
            .await?;
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
        if self
            .should_skip_event_in_incomplete_history_before_first_complete_epoch(&stream_element)?
        {
            tracing::debug!(
                "skipping event {} as it is before the first complete epoch",
                stream_element.element.label()
            );
            event_handle.mark_as_complete();
            return Ok(());
        }

        self.process_event_impl(blob_event_processor, event_handle, stream_element.clone())
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
    // TODO(WAL-895): We should simplify/improve the way we check if the node is lagging behind
    // after storing the latest event epoch in the DB.
    async fn check_if_node_lagging_and_enter_recovery_mode(
        &self,
        event: &ContractEvent,
        node_status: NodeStatus,
        maybe_epoch_at_start: &mut Option<Epoch>,
    ) -> anyhow::Result<()> {
        let Some(epoch_at_start) = *maybe_epoch_at_start else {
            return Ok(());
        };

        // Blob extensions do not contain their event emission epoch. So we use this to filter out
        // blob extensions events.
        let Some(first_new_event_epoch) = event.event_epoch() else {
            tracing::debug!(
                "no event epoch found for event; skipping checking if we're severely lagging"
            );
            return Ok(());
        };

        tracing::debug!(
            first_new_event_epoch,
            epoch_at_start,
            "checking the first contract event if we're severely lagging"
        );

        // Clear the starting epoch, so that we won't make this check again in the current run.
        *maybe_epoch_at_start = None;

        // Checks if the node is severely lagging behind.
        if !node_status.is_catching_up() && first_new_event_epoch + 1 < epoch_at_start {
            tracing::warn!(
                "the current epoch ({}) is far ahead of the event epoch ({}); \
                node entering recovery mode",
                epoch_at_start,
                first_new_event_epoch,
            );

            self.enter_recovery_mode().await?;
        }

        // Set the initial current event epoch. Note that if the first event is an epoch change
        // start event, we don't set it here since the epoch change execution will set it to the
        // new epoch. This helps prevent a race condition where the node enters recovery mode
        // before the epoch change execution, and the node may be recovering to an epoch that is
        // 2 or more epochs behind the latest epoch.
        if !matches!(
            event,
            ContractEvent::EpochChangeEvent(EpochChangeEvent::EpochChangeStart(_))
        ) {
            self.inner
                .latest_event_epoch_sender
                .send(Some(first_new_event_epoch))?;
        }

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn process_event_impl(
        &self,
        blob_event_processor: &BlobEventProcessor,
        event_handle: EventHandle,
        stream_element: PositionedStreamEvent,
    ) -> anyhow::Result<()> {
        let _scope = monitored_scope::monitored_scope("ProcessEvent::Impl");

        let _timer_guard = walrus_utils::with_label!(
            self.inner.metrics.event_process_duration_seconds,
            stream_element.element.label()
        )
        .start_timer();
        fail_point_async!("before-process-event-impl");
        let checkpoint_position = stream_element.checkpoint_event_position;
        match stream_element.element {
            EventStreamElement::ContractEvent(ContractEvent::BlobEvent(blob_event)) => {
                self.process_blob_event(
                    blob_event_processor,
                    event_handle,
                    blob_event,
                    checkpoint_position,
                )
                .await?;
            }
            EventStreamElement::ContractEvent(ContractEvent::EpochChangeEvent(
                epoch_change_event,
            )) => {
                self.process_epoch_change_event(
                    blob_event_processor,
                    event_handle,
                    epoch_change_event,
                )
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
            EventStreamElement::ContractEvent(ContractEvent::ProtocolEvent(
                ProtocolEvent::PricesUpdated(_),
            )) => {
                event_handle.mark_as_complete();
            }
            EventStreamElement::ContractEvent(ContractEvent::ProtocolEvent(event)) => {
                panic!(
                    "unexpected protocol version update: {:?}",
                    event.protocol_version()
                );
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
        blob_event_processor: &BlobEventProcessor,
        event_handle: EventHandle,
        blob_event: BlobEvent,
        checkpoint_position: CheckpointEventPosition,
    ) -> anyhow::Result<()> {
        let _scope = monitored_scope::monitored_scope("ProcessEvent::BlobEvent");

        tracing::debug!(?blob_event, "{} event received", blob_event.name());
        blob_event_processor
            .process_event(event_handle, blob_event, checkpoint_position)
            .await?;
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn process_epoch_change_event(
        &self,
        blob_event_processor: &BlobEventProcessor,
        event_handle: EventHandle,
        epoch_change_event: EpochChangeEvent,
    ) -> anyhow::Result<()> {
        let _scope = monitored_scope::monitored_scope("ProcessEvent::EpochChangeEvent");

        // Make sure we get the latest contract data from the RPC node.
        self.inner.contract_service.flush_cache().await;

        // Log the event reception with appropriate level
        match &epoch_change_event {
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
                let _scope = monitored_scope::monitored_scope(
                    "ProcessEvent::EpochChangeEvent::EpochParametersSelected",
                );
                self.wait_for_epoch_state(event.next_epoch.saturating_sub(1), |state| {
                    matches!(state, EpochState::NextParamsSelected(_))
                })
                .await?;
                self.handle_epoch_parameters_selected(event);
                event_handle.mark_as_complete();
            }
            EpochChangeEvent::EpochChangeStart(event) => {
                let _scope = monitored_scope::monitored_scope(
                    "ProcessEvent::EpochChangeEvent::EpochChangeStart",
                );
                self.wait_for_epoch_state(event.epoch, |_| true).await?;
                fail_point_async!("epoch_change_start_entry");
                self.process_epoch_change_start_event(blob_event_processor, event_handle, &event)
                    .await?;
            }
            EpochChangeEvent::EpochChangeDone(event) => {
                let _scope = monitored_scope::monitored_scope(
                    "ProcessEvent::EpochChangeEvent::EpochChangeDone",
                );
                self.wait_for_epoch_state(event.epoch, |state| {
                    matches!(
                        state,
                        EpochState::EpochChangeDone(_) | EpochState::NextParamsSelected(_)
                    )
                })
                .await?;
                self.process_epoch_change_done_event(&event).await?;
                event_handle.mark_as_complete();
            }
            EpochChangeEvent::ShardsReceived(_) => {
                let _scope = monitored_scope::monitored_scope(
                    "ProcessEvent::EpochChangeEvent::ShardsReceived",
                );
                event_handle.mark_as_complete();
            }
            EpochChangeEvent::ShardRecoveryStart(_) => {
                let _scope = monitored_scope::monitored_scope(
                    "ProcessEvent::EpochChangeEvent::ShardRecoveryStart",
                );
                event_handle.mark_as_complete();
            }
        }
        Ok(())
    }

    /// Repeatedly checks until the current Sui epoch state matches the expectation.
    ///
    /// Returns `Ok(())` if the current epoch is equal to the `expected_epoch` and the
    /// `state_matches` function returns `true` or the current epoch is greater than the
    /// `expected_epoch` (irrespective of the state).
    #[cfg(not(any(test, msim)))]
    async fn wait_for_epoch_state(
        &self,
        expected_epoch: Epoch,
        state_matches: impl Fn(&EpochState) -> bool,
    ) -> anyhow::Result<()> {
        const EPOCH_STATE_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
        const EPOCH_STATE_WAIT_SLEEP: Duration = Duration::from_millis(100);
        let deadline = Instant::now() + EPOCH_STATE_WAIT_TIMEOUT;
        while Instant::now() < deadline {
            self.inner.contract_service.flush_cache().await;
            let Ok((epoch, state)) = self.inner.contract_service.get_epoch_and_state().await else {
                tracing::warn!("failed to get current epoch and state");
                continue;
            };
            if epoch == expected_epoch && state_matches(&state) || epoch > expected_epoch {
                return Ok(());
            }
            tracing::debug!(
                expected_epoch,
                current_epoch = epoch,
                current_state = ?state,
                "waiting for expected epoch state",
            );
            sleep(EPOCH_STATE_WAIT_SLEEP).await;
        }
        bail!("timed out after waiting for expected epoch state")
    }

    #[cfg(any(test, msim))]
    #[allow(clippy::unused_async)]
    async fn wait_for_epoch_state(
        &self,
        _expected_epoch: Epoch,
        _state_matches: impl Fn(&EpochState) -> bool,
    ) -> anyhow::Result<()> {
        tracing::info!("waiting for epoch state is not supported in tests, skipping");
        Ok(())
    }

    /// Handles the epoch parameters selected event.
    ///
    /// This function cancels the scheduled voting end and initiates the epoch change.
    /// It also schedules the process subsidies and marks the event as complete.
    #[tracing::instrument(skip_all)]
    fn handle_epoch_parameters_selected(
        &self,
        event: walrus_sdk::sui::types::EpochParametersSelected,
    ) {
        self.epoch_change_driver
            .cancel_scheduled_voting_end(event.next_epoch);
        self.epoch_change_driver.schedule_initiate_epoch_change(
            NonZero::new(event.next_epoch).expect("the next epoch is always non-zero"),
        );
        self.epoch_change_driver.schedule_process_subsidies();
    }

    #[tracing::instrument(skip_all)]
    async fn process_package_event(
        &self,
        event_handle: EventHandle,
        package_event: PackageEvent,
    ) -> anyhow::Result<()> {
        let _scope = monitored_scope::monitored_scope("ProcessEvent::PackageEvent");

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
    async fn process_epoch_change_start_event(
        &self,
        blob_event_processor: &BlobEventProcessor,
        event_handle: EventHandle,
        event: &EpochChangeStart,
    ) -> anyhow::Result<()> {
        // There shouldn't be an epoch change event for the genesis epoch.
        assert!(event.epoch != GENESIS_EPOCH);

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

        // Before processing the epoch change start event, we need to wait for all the events in
        // the current epoch to be processed (note that this does not include waiting for all
        // pending blob syncs to finish). This is to make sure that the node is in a consistent
        // state before processing the epoch change start event.
        blob_event_processor
            .get_pending_event_counter()
            .wait_for_all_events_to_be_processed()
            .await;

        if let Some(c) = self.config_synchronizer.as_ref() {
            c.sync_node_params().await?;
        }

        // Start storage node consistency check if
        // - consistency check is enabled
        // - node is not reprocessing events (blob info table should not be affected by future
        //   events)
        let node_is_reprocessing_events =
            self.inner.storage.get_latest_handled_event_index()? >= event_handle.index();
        if self.inner.consistency_check_config.enable_consistency_check
            && !node_is_reprocessing_events
            && let Err(err) = consistency_check::schedule_background_consistency_check(
                self.inner.clone(),
                self.blob_sync_handler.clone(),
                event.epoch,
            )
            .await
        {
            tracing::warn!(
                ?err,
                walrus.epoch = event.epoch,
                "failed to schedule background blob info consistency check"
            );
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
            .latest_event_epoch_sender
            .send(Some(event.epoch))?;

        self.start_garbage_collection_task(event.epoch).await?;

        Ok(())
    }

    /// Starts a background task to perform database cleanup operations if the node is active.
    async fn start_garbage_collection_task(&self, epoch: Epoch) -> anyhow::Result<()> {
        // Try to get the epoch start time from the contract service. If the epoch state is not
        // available (e.g., in tests), use the current time as the epoch start.
        let epoch_start = self
            .inner
            .contract_service
            .get_epoch_and_state()
            .await
            .ok()
            .and_then(|(current_epoch, state)| {
                if current_epoch == epoch {
                    Some(state.earliest_start_of_current_epoch())
                } else {
                    None
                }
            })
            .unwrap_or_else(Utc::now);
        if let Err(error) = self
            .garbage_collector
            .start_garbage_collection_task(epoch, epoch_start)
            .await
        {
            tracing::error!(?error, epoch, "failed to start garbage-collection task");
        }
        Ok(())
    }

    /// Checks if garbage collection needs to be restarted on startup and starts it if needed.
    ///
    /// This method is intended to be run at startup to check if a garbage-collection task was
    /// started but not completed. If this is the case and we are still in the same epoch, it
    /// restarts the garbage-collection task.
    async fn check_and_start_garbage_collection_on_startup(&self) -> anyhow::Result<()> {
        let (last_started_epoch, last_completed_epoch) = self
            .inner
            .storage
            .garbage_collector_last_started_and_completed_epochs()?;

        // Set metrics based on DB entries during startup.
        self.inner
            .metrics
            .set_garbage_collection_last_started_epoch(last_started_epoch);
        self.inner
            .metrics
            .set_garbage_collection_last_completed_epoch(last_completed_epoch);
        if last_completed_epoch == last_started_epoch {
            tracing::debug!(
                last_started_epoch,
                last_completed_epoch,
                "previous garbage-collection task already completed; skipping restart"
            );
            return Ok(());
        }

        // TODO(WAL-1111): It is possible that blob-info cleanup is enabled but data deletion is
        // disabled, in which case we never store the `last_completed_epoch` in the DB and therefore
        // keep restarting the task. We could store the last completed epoch for the blob-info
        // cleanup in addition.

        let (current_epoch, _) = self.inner.contract_service.get_epoch_and_state().await?;

        if current_epoch != last_started_epoch {
            tracing::info!(
                last_started_epoch,
                current_epoch,
                "the Walrus epoch has changed since the last garbage-collection task was started; \
                skipping restart"
            );
            return Ok(());
        }

        tracing::info!(
            last_started_epoch,
            last_completed_epoch,
            current_epoch,
            "restarting unfinished garbage-collection task on startup"
        );
        self.start_garbage_collection_task(current_epoch).await
    }

    /// Storage node execution of the epoch change start event, to bring the node state to the next
    /// epoch.
    async fn execute_epoch_change(
        &self,
        event_handle: EventHandle,
        event: &EpochChangeStart,
        shard_map_lock: StorageShardLock,
    ) -> anyhow::Result<()> {
        if self.inner.storage.node_status()?.is_catching_up() {
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

                    self.enter_recovery_mode().await?;

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

    /// Processes the epoch change start event while the node is in
    /// [`RecoveryCatchUp`][NodeStatus::RecoveryCatchUp] or
    /// [`RecoveryCatchUpWithIncompleteHistory`][NodeStatus::RecoveryCatchUpWithIncompleteHistory]
    /// state.
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

        if event.epoch < self.inner.current_committee_epoch() {
            // We have not caught up to the latest epoch yet, so we can skip the event.
            event_handle.mark_as_complete();
            return Ok(());
        }

        tracing::info!(walrus.epoch = %event.epoch, "catching-up node reaches the current epoch");

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
            self.process_shard_changes_in_new_epoch_while_node_is_in_sync(
                event_handle,
                event,
                true,
                shard_map_lock,
            )
            .await?;
        } else {
            tracing::info!("start node recovery to catch up to the latest epoch");
            // This node is a past and current committee member. Start node recovery to catch up
            // to the latest epoch.
            self.process_shard_changes_in_new_epoch_and_start_node_recovery(
                event_handle,
                event,
                shard_map_lock,
            )
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

        if let NodeStatus::RecoveryInProgress(recovering_epoch) =
            self.inner.storage.node_status()?
        {
            // If the node is already in recovery mode, we need to restart node recovery to recover
            // to the latest epoch. This is to make sure that the node is always recovering to the
            // latest epoch.
            tracing::info!(
                "node is currently recovering to epoch {recovering_epoch}, restarting \
                node recovery to recover to the latest epoch {}",
                event.epoch
            );
            self.process_shard_changes_in_new_epoch_and_start_node_recovery(
                event_handle,
                event,
                shard_map_lock,
            )
            .await
        } else {
            self.process_shard_changes_in_new_epoch_while_node_is_in_sync(
                event_handle,
                event,
                is_new_node_joining_committee,
                shard_map_lock,
            )
            .await
        }
    }

    /// Processes the shard changes in the new epoch and starts the node recovery process.
    ///
    /// As all functions that are passed an [`EventHandle`], this is responsible for marking the
    /// event as completed.
    async fn process_shard_changes_in_new_epoch_and_start_node_recovery(
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
                .force_set_active_status()
                .await?;
        }

        // For shards that just moved out, we need to lock them to not store more data in them.
        for shard in shard_diff_calculator.shards_to_lock() {
            if let Some(shard_storage) = self.inner.storage.shard_storage(*shard).await {
                shard_storage
                    .lock_shard_for_epoch_change()
                    .await
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

    /// Processes all the shard changes in the new epoch, and finishes the epoch change.
    #[tracing::instrument(skip_all)]
    async fn process_shard_changes_in_new_epoch_while_node_is_in_sync(
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

        if cfg!(msim) {
            // In simtest, print out the shard migration information for easier debugging.
            tracing::info!("EpochChangeStart shard diffs: {:?}", shard_diff_calculator);
        }

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
            tracing::info!(
                walrus.shard_index = %shard_id,
                epoch = event.epoch,
                "locking shard for epoch change"
            );
            shard_storage
                .lock_shard_for_epoch_change()
                .await
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

        if !event_index.is_multiple_of(NUM_EVENTS_PER_DIGEST_RECORDING) {
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

        // Stores event source for event index consistency checking in simtest.
        sui_macros::fail_point_arg!(
            "storage_node_event_index_source",
            |event_source_map: Arc<
                std::sync::Mutex<
                    std::collections::HashMap<u64, std::collections::HashMap<ObjectID, u64>>,
                >,
            >| {
                event_source_map
                    .lock()
                    .expect("failed to lock the event source map")
                    .entry(event_index)
                    .or_insert_with(|| std::collections::HashMap::new())
                    .insert(self.inner.node_capability(), event_source);
            }
        );

        Ok(())
    }

    /// Enters recovery mode.
    /// This function should only be called when the node is lagging behind.
    async fn enter_recovery_mode(&self) -> anyhow::Result<()> {
        self.inner.set_node_status(NodeStatus::RecoveryCatchUp)?;

        // Now the node is entering recovery mode, we need to cancel all the blob syncs
        // that are in progress, since the node is lagging behind, and we don't have
        // any information about the shards that the node should own.
        //
        // The node now will try to only process blob info upon receiving a blob event
        // and blob recovery will be triggered when the node is in the latest epoch.
        self.blob_sync_handler
            .cancel_all_syncs_and_mark_events_completed()
            .await?;

        Ok(())
    }

    #[cfg(test)]
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
    /// Computes a deferral duration from the configured policy and a given unencoded size.
    pub fn deferral_for_unencoded_size(&self, size_bytes: u64) -> Option<std::time::Duration> {
        self.live_upload_deferral_config
            .deferral_for_size(size_bytes)
    }

    /// Returns the latest checkpoint sequence number known to the node.
    pub fn latest_checkpoint_sequence_number(&self) -> Option<u64> {
        self.event_manager.latest_checkpoint_sequence_number()
    }

    /// Applies a recovery deferral if the node determines the blob is part of an in-flight upload.
    #[tracing::instrument(skip(self))]
    pub async fn maybe_apply_live_upload_deferral(
        &self,
        blob_id: BlobId,
        checkpoint_sequence: u64,
    ) {
        if !self.live_upload_deferral_config.enabled {
            return;
        }

        // The checkpoint number here is the one reported by the blob event that triggered this
        // evaluation.
        let Some(latest_checkpoint) = self.latest_checkpoint_sequence_number() else {
            tracing::trace!("latest checkpoint unknown; skipping live-upload deferral");
            return;
        };

        if checkpoint_sequence > latest_checkpoint {
            // This can happen if the event stream pulls a checkpoint before we switch
            // to a new fullnode which is not fully caught up.
            tracing::trace!(
                checkpoint_sequence,
                latest_checkpoint,
                "checkpoint sequence ahead of latest; skipping live-upload deferral"
            );
            return;
        }

        let lag = latest_checkpoint.saturating_sub(checkpoint_sequence);
        if lag > self.live_upload_deferral_config.max_checkpoint_lag {
            tracing::trace!(
                lag,
                threshold = self.live_upload_deferral_config.max_checkpoint_lag,
                "checkpoint lag too high; skipping live-upload deferral"
            );
            return;
        }

        let deferral_from_size = match self.storage.get_metadata(&blob_id) {
            Ok(Some(metadata)) => {
                let size = metadata.as_ref().unencoded_length();
                self.deferral_for_unencoded_size(size).inspect(|&duration| {
                    tracing::trace!(size, ?duration, "applying size-based live-upload deferral");
                })
            }
            Ok(None) => {
                tracing::trace!(
                    "metadata not yet stored, falling back to default live-upload deferral"
                );
                None
            }
            Err(error) => {
                tracing::warn!(
                    ?error,
                    "database error while loading metadata while evaluating live-upload deferral"
                );
                return;
            }
        };

        let Some(duration) = deferral_from_size.or_else(|| {
            self.live_upload_deferral_config
                .fallback_deferral()
                .inspect(|duration| {
                    tracing::trace!(?duration, "applying fallback live-upload deferral");
                })
        }) else {
            tracing::trace!("no live-upload deferral configured for this blob");
            return;
        };

        if duration.is_zero() {
            tracing::trace!("configured deferral duration is zero; skipping live-upload deferral");
            return;
        }

        if let Err(error) = self.set_recovery_deferral(blob_id, duration).await {
            tracing::trace!(
                ?error,
                lag,
                latest_checkpoint,
                checkpoint_sequence,
                "skipping live-upload recovery deferral"
            );
            return;
        }

        tracing::debug!(
            ?duration,
            lag,
            latest_checkpoint,
            checkpoint_sequence,
            "applied live-upload recovery deferral"
        );
    }

    pub(crate) fn encoding_config(&self) -> &EncodingConfig {
        &self.encoding_config
    }

    /// Waits until the recovery deferral for the given blob ID expires
    /// and cancels the recovery deferral upon timeout.
    pub async fn wait_until_recovery_deferral_expires(
        &self,
        blob_id: &BlobId,
    ) -> anyhow::Result<Arc<tokio_util::sync::CancellationToken>> {
        let (until, token) = {
            let map = self.recovery_deferrals.read().await;
            if let Some((u, existing_token)) = map.get(blob_id).cloned() {
                (Some(u), existing_token)
            } else {
                let t = Arc::new(tokio_util::sync::CancellationToken::new());
                (None, t)
            }
        };

        let mut waiter_metric_incremented = false;
        if until.is_some() {
            self.metrics.recovery_deferral_waiters.inc();
            waiter_metric_incremented = true;
        } else {
            token.cancel();
        }

        token.cancelled().await;

        if waiter_metric_incremented {
            self.metrics.recovery_deferral_waiters.dec();
        }

        Ok(token)
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

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn check_does_not_store_metadata_or_slivers_for_blob(
        &self,
        blob_id: &BlobId,
    ) -> anyhow::Result<()> {
        // Only check shards that are owned by the node.
        for shard in self.owned_shards_at_latest_epoch() {
            if self
                .storage
                .is_stored_at_shard(blob_id, shard)
                .await
                .context("failed to check if blob is stored at shard {shard}")?
            {
                anyhow::bail!("blob {blob_id} is stored at shard {shard}");
            }
        }

        if self
            .storage
            .get_metadata(blob_id)
            .context("failed to get blob metadata")?
            .is_some()
        {
            anyhow::bail!("node stores metadata for blob {blob_id}");
        }

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn is_stored_at_specific_shards(
        &self,
        blob_id: &BlobId,
        shards: &[ShardIndex],
    ) -> anyhow::Result<bool> {
        for shard in shards {
            match self.storage.is_stored_at_shard(blob_id, *shard).await {
                Ok(false) => {
                    if cfg!(msim) {
                        // Extremely helpful for debugging consistency issue in simtest.
                        tracing::debug!(%blob_id, %shard, "blob not stored at shard");
                    }
                    return Ok(false);
                }
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
        let shard_storages = self.storage.existing_shard_storages().await;
        let mut shards = Vec::new();

        for shard_storage in shard_storages.iter() {
            if let Ok(status) = shard_storage.status().await
                && status == ShardStatus::Active
            {
                shards.push(shard_storage.id());
            }
        }

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

    /// Returns the latest event epoch.
    /// If the storage node hasn't processed the first event yet, this function will block until
    /// the first event is processed, so that the current event epoch is set.
    async fn current_event_epoch(&self) -> Result<Epoch, watch::error::RecvError> {
        let mut watcher = self.latest_event_epoch_watcher.clone();
        while watcher.borrow().is_none() {
            watcher.changed().await?;
        }
        Ok(watcher.borrow().expect("watcher should be set"))
    }

    /// Non-blocking version of `current_event_epoch`.
    fn try_get_current_event_epoch(&self) -> Option<Epoch> {
        *self.latest_event_epoch_watcher.borrow()
    }

    pub(crate) fn current_committee_epoch(&self) -> Epoch {
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
            .ok_or(ShardNotAssigned(
                shard_index,
                self.current_committee_epoch(),
            ))
    }

    fn init_gauges(&self) -> Result<(), TypedStoreError> {
        let persisted = self.storage.get_sequentially_processed_event_count()?;
        let node_status = self.storage.node_status()?;

        walrus_utils::with_label!(self.metrics.event_cursor_progress, "persisted").set(persisted);
        self.metrics.current_node_status.set(node_status.to_i64());
        self.metrics.recovery_deferrals_active.set(0);
        self.metrics.recovery_deferral_waiters.set(0);

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

    async fn verify_sliver_against_metadata(
        &self,
        metadata: Arc<VerifiedBlobMetadataWithId>,
        sliver: Sliver,
    ) -> Result<Sliver, StoreSliverError> {
        let encoding_config = self.encoding_config.clone();
        let metadata_for_verification = metadata.clone();
        let result = self
            .thread_pool
            .clone()
            .oneshot(move || {
                sliver.verify(
                    &encoding_config,
                    metadata_for_verification.as_ref().as_ref(),
                )?;
                Result::<_, StoreSliverError>::Ok(sliver)
            })
            .await;
        thread_pool::unwrap_or_resume_panic(result)
    }

    async fn prepare_sliver_for_storage(
        &self,
        metadata: Arc<VerifiedBlobMetadataWithId>,
        sliver_pair_index: SliverPairIndex,
        sliver: Sliver,
    ) -> Result<Option<(Arc<ShardStorage>, Sliver)>, StoreSliverError> {
        let shard_storage = self
            .get_shard_for_sliver_pair(sliver_pair_index, metadata.blob_id())
            .await?;

        let shard_status = shard_storage
            .status()
            .await
            .context("Unable to retrieve shard status")?;

        if !shard_status.is_owned_by_node() {
            return Err(
                ShardNotAssigned(shard_storage.id(), self.current_committee_epoch()).into(),
            );
        }

        if shard_storage
            .is_sliver_type_stored(metadata.blob_id(), sliver.r#type())
            .context("database error when checking sliver existence")?
        {
            return Ok(None);
        }

        let verified_sliver = self
            .verify_sliver_against_metadata(metadata, sliver)
            .await?;

        Ok(Some((shard_storage, verified_sliver)))
    }

    pub(crate) async fn store_sliver_unchecked(
        &self,
        metadata: Arc<VerifiedBlobMetadataWithId>,
        sliver_pair_index: SliverPairIndex,
        sliver: Sliver,
    ) -> Result<bool, StoreSliverError> {
        let Some((shard_storage, verified_sliver)) = self
            .prepare_sliver_for_storage(metadata.clone(), sliver_pair_index, sliver)
            .await?
        else {
            return Ok(false);
        };

        let sliver_type = verified_sliver.r#type();

        shard_storage
            .put_sliver(*metadata.blob_id(), verified_sliver)
            .await
            .context("unable to store sliver")?;

        walrus_utils::with_label!(self.metrics.slivers_stored_total, sliver_type).inc();

        Ok(true)
    }

    async fn persist_verified_metadata(
        &self,
        blob_id: &BlobId,
        verified: Arc<VerifiedBlobMetadataWithId>,
    ) -> Result<bool, StoreMetadataError> {
        if self
            .storage
            .has_metadata(blob_id)
            .context("could not check metadata existence")?
        {
            return Ok(false);
        }

        self.storage
            .put_verified_metadata(&verified)
            .await
            .context("unable to store metadata")
            .map_err(StoreMetadataError::Internal)?;

        self.pending_metadata_cache.remove(blob_id).await;

        self.metrics
            .uploaded_metadata_unencoded_blob_bytes
            .observe(verified.metadata().unencoded_length() as f64);
        self.metrics.metadata_stored_total.inc();

        Ok(true)
    }

    /// Resolves the metadata required to validate an incoming sliver.
    ///
    /// The returned tuple contains the metadata and a flag indicating whether that metadata has
    /// already been persisted to durable storage (`true`) or is currently buffered in-memory
    /// (`false`).
    async fn resolve_metadata_for_sliver(
        &self,
        blob_id: &BlobId,
        allow_pending: bool,
    ) -> Result<(Arc<VerifiedBlobMetadataWithId>, bool), StoreSliverError> {
        let persisted = self
            .storage
            .get_metadata(blob_id)
            .context("database error when storing sliver")?;
        if let Some(metadata) = persisted {
            return Ok((Arc::new(metadata), true));
        }

        let pending = self.pending_metadata_cache.get(blob_id).await;
        let is_registered = self.is_blob_registered(blob_id)?;

        match (pending, is_registered, allow_pending) {
            (Some(metadata), _, _) => Ok((metadata, false)),
            (None, true, _) | (None, _, true) => Err(StoreSliverError::MissingMetadata),
            (None, false, false) => Err(StoreSliverError::NotCurrentlyRegistered),
        }
    }

    pub(crate) async fn flush_pending_metadata(
        &self,
        blob_id: &BlobId,
    ) -> Result<(), StoreMetadataError> {
        if self
            .storage
            .has_metadata(blob_id)
            .context("could not check metadata existence")?
        {
            return Ok(());
        }

        if let Some(metadata) = self.pending_metadata_cache.remove(blob_id).await {
            self.storage
                .put_verified_metadata(&metadata)
                .await
                .context("unable to persist pending metadata")
                .map_err(StoreMetadataError::Internal)?;

            self.metrics
                .uploaded_metadata_unencoded_blob_bytes
                .observe(metadata.metadata().unencoded_length() as f64);
            self.metrics.metadata_stored_total.inc();
        }

        Ok(())
    }

    pub(crate) async fn flush_pending_slivers(
        &self,
        blob_id: &BlobId,
        metadata: Arc<VerifiedBlobMetadataWithId>,
    ) -> Result<(), StoreSliverError> {
        if !self.pending_sliver_cache.has_blob(blob_id).await {
            return Ok(());
        }

        let mut pending = self.pending_sliver_cache.drain(blob_id).await;
        if pending.is_empty() {
            return Ok(());
        }

        while let Some(cached) = pending.pop() {
            let sliver_for_storage = cached.sliver.clone();
            match self
                .store_sliver_unchecked(
                    metadata.clone(),
                    cached.sliver_pair_index,
                    sliver_for_storage,
                )
                .await
            {
                Ok(_) => continue,
                Err(error) => {
                    pending.push(cached);
                    self.pending_sliver_cache
                        .insert_many(*blob_id, pending)
                        .await;
                    return Err(error);
                }
            }
        }

        Ok(())
    }

    async fn flush_pending_caches(&self, blob_id: &BlobId) -> Result<(), PendingCacheError> {
        if let Err(error) = self.flush_pending_metadata(blob_id).await {
            match error {
                StoreMetadataError::NotCurrentlyRegistered => (),
                other => return Err(PendingCacheError::Metadata(other)),
            }
        }

        if self.pending_sliver_cache.has_blob(blob_id).await {
            let metadata = match self
                .storage
                .get_metadata(blob_id)
                .context("unable to fetch metadata while flushing pending slivers")
                .map_err(|error| PendingCacheError::Sliver(StoreSliverError::Internal(error)))?
            {
                Some(metadata) => Arc::new(metadata),
                None => return Err(PendingCacheError::Sliver(StoreSliverError::MissingMetadata)),
            };

            if let Err(error) = self.flush_pending_slivers(blob_id, metadata).await
                && !matches!(
                    error,
                    StoreSliverError::NotCurrentlyRegistered | StoreSliverError::MissingMetadata
                )
            {
                return Err(PendingCacheError::Sliver(error));
            }
        }

        Ok(())
    }

    pub(crate) async fn flush_pending_caches_with_logging(&self, blob_id: BlobId) {
        #[cfg(msim)]
        sui_macros::fail_point_async!("fail_point_flush_pending_caches_with_logging");

        if let Err(error) = self.flush_pending_caches(&blob_id).await {
            match error {
                PendingCacheError::Metadata(metadata_error) => {
                    tracing::warn!(
                        ?metadata_error,
                        blob_id = %blob_id,
                        "failed to flush pending metadata after blob registration"
                    );
                }
                PendingCacheError::Sliver(sliver_error) => {
                    tracing::warn!(
                        ?sliver_error,
                        blob_id = %blob_id,
                        "failed to flush pending slivers after blob registration"
                    );
                }
            }
        }
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
            .is_some_and(|blob_info| blob_info.is_registered(self.current_committee_epoch())))
    }

    fn notify_registration(&self, blob_id: &BlobId) {
        self.registration_notifier.notify_registered(blob_id);
    }

    async fn wait_for_registration_inner(&self, blob_id: &BlobId, timeout: Duration) -> bool {
        // TODO: For deletable blobs, confirmations are per-object (not per-blob). This wait is only
        // blob scoped, so it can return `true` due to registration of a different object that
        // references the same `BlobId`.
        if timeout.is_zero() {
            return self.is_blob_registered(blob_id).unwrap_or(false);
        }

        let notify = self.registration_notifier.acquire(blob_id);
        let notified = notify.notified();

        if self.is_blob_registered(blob_id).unwrap_or(false) {
            return true;
        }

        // Only long-poll if we've actually buffered any data for this blob. This prevents clients
        // from tying up the server by waiting on arbitrary blob IDs that the node hasn't seen.
        //
        // Note: this is best-effort; caches are bounded and time-based. If entries are evicted, we
        // may skip waiting even if the blob later becomes registered.
        if !self.pending_sliver_cache.has_blob(blob_id).await
            && self.pending_metadata_cache.get(blob_id).await.is_none()
        {
            tracing::debug!(
                %blob_id,
                "wait_for_registration: skipping wait because no pending data is buffered"
            );
            return false;
        }

        match tokio::time::timeout(timeout, notified).await {
            Ok(()) => self.is_blob_registered(blob_id).unwrap_or(false),
            Err(_) => false,
        }
    }

    pub(crate) fn is_blob_certified(&self, blob_id: &BlobId) -> Result<bool, anyhow::Error> {
        Ok(self
            .storage
            .get_blob_info(blob_id)
            .context("could not retrieve blob info")?
            .is_some_and(|blob_info| blob_info.is_certified(self.current_committee_epoch())))
    }

    /// Returns true if the blob is currently not certified.
    ///
    /// If an error occurs while retrieving the blob info, this returns `false`. The intention
    /// is that if this is used to cancel a blob sync or to delete blob data, we need to be *sure*
    /// that the blob is not certified.
    fn is_blob_not_certified(&self, blob_id: &BlobId) -> bool {
        if let Ok(false) = self.is_blob_certified(blob_id) {
            return true;
        }
        false
    }

    /// Sets the status of the node.
    pub fn set_node_status(&self, status: NodeStatus) -> Result<(), TypedStoreError> {
        self.metrics.current_node_status.set(status.to_i64());
        self.storage.set_node_status(status)
    }

    fn shut_down(&self) {
        self.is_shutting_down.store(true, Ordering::SeqCst);
        self.recovery_deferral_cleanup_token.cancel();
        self.recovery_deferral_notify.notify_waiters();
    }

    fn is_shutting_down(&self) -> bool {
        self.is_shutting_down.load(Ordering::SeqCst)
    }

    /// Common validation for blob operations
    fn validate_blob_access<E>(
        &self,
        blob_id: &BlobId,
        forbidden_error: E,
        unavailable_error: E,
    ) -> Result<(), E>
    where
        E: From<anyhow::Error>,
    {
        ensure!(!self.is_blocked(blob_id), forbidden_error);
        ensure!(self.is_blob_registered(blob_id)?, unavailable_error,);
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn retrieve_sliver_unchecked(
        &self,
        blob_id: &BlobId,
        sliver_pair_index: SliverPairIndex,
        sliver_type: SliverType,
    ) -> Result<Arc<Sliver>, RetrieveSliverError> {
        // Check the cache for the sliver before going to the database.
        let key = (*blob_id, sliver_pair_index, sliver_type);
        let cached_entry = self
            .sliver_ref_cache
            .get_with(key, async { Arc::new(RwLock::new(Weak::new())) })
            .await;

        if let Some(sliver) = cached_entry.read().await.upgrade() {
            return Ok(sliver);
        }

        // The sliver was not cached, or the cached weak is no longer usable, so we need to fetch
        // from the database. Before fetching, however, we need a write guard.
        let mut weak_sliver_guard = cached_entry.write().await;

        // It's possible that between us dropping the read guard and acquiring the write guard,
        // another writer has already replaced the Weak with one that is still alive, so attempt to
        // upgrade the pointer once more.
        if let Some(sliver) = weak_sliver_guard.upgrade() {
            return Ok(sliver);
        }

        // Okay, the weak that's stored really is not upgradeable, so access the database.
        let shard_storage = self
            .get_shard_for_sliver_pair(sliver_pair_index, blob_id)
            .await?;

        let blob_id = *blob_id;

        let result = tokio::task::spawn_blocking(move || {
            shard_storage
                .get_sliver(&blob_id, sliver_type)
                .context("unable to retrieve sliver")?
                .ok_or(RetrieveSliverError::Unavailable)
        })
        .await;

        match result {
            Ok(Err(err)) => Err(err),
            Ok(Ok(sliver)) => {
                walrus_utils::with_label!(self.metrics.slivers_retrieved_total, sliver.r#type())
                    .inc();
                let shared_sliver = Arc::new(sliver);
                *weak_sliver_guard = Arc::downgrade(&shared_sliver);

                Ok(shared_sliver)
            }
            Err(e) => {
                if e.is_panic() {
                    drop(weak_sliver_guard); // Because no need to panic while holding the guard.
                    std::panic::resume_unwind(e.into_panic());
                }
                Err(e)
                    .context("blocking thread failed to retrieve sliver")
                    .map_err(RetrieveSliverError::Internal)
            }
        }
    }

    /// Retrieve the recovery symbol without calling `validate_blob_access`.
    #[tracing::instrument(level = "debug", skip_all)]
    async fn retrieve_recovery_symbol_unchecked(
        &self,
        blob_id: &BlobId,
        symbol_id: SymbolId,
        sliver_type: Option<SliverType>,
        encoding_type: EncodingType,
        worker: RecoverySymbolService,
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

        let mut worker = Some(worker);

        for target_sliver_type in [SliverType::Secondary, SliverType::Primary]
            .into_iter()
            .filter(|target_type| sliver_type.is_none() || sliver_type == Some(*target_type))
        {
            let (source_pair_index, target_pair_index) = match target_sliver_type {
                Axis::Primary => (secondary_pair_index, primary_pair_index),
                Axis::Secondary => (primary_pair_index, secondary_pair_index),
            };

            let required_shard = &source_pair_index.to_shard_index(n_shards, blob_id);
            if !owned_shards.contains(required_shard) {
                // This node does not manage the shard owning the source pair.
                continue;
            }

            // Since the caller was granted a ready-worker, we ensure that both attempts are made,
            // and avoid failing the second attempt with Unavailable. Note that in most cases,
            // the node has only a single matching shard and so this loop and code only runs once.
            let wait_for_worker = async {
                match worker.take() {
                    Some(worker) => worker,
                    None => self.wait_for_ready_symbol_service().await,
                }
            };

            let (mut worker, sliver_result) = tokio::join!(
                wait_for_worker,
                self.retrieve_sliver_unchecked(
                    blob_id,
                    source_pair_index,
                    target_sliver_type.orthogonal(),
                )
            );

            let source_sliver = match sliver_result {
                Ok(sliver) => sliver,
                Err(err) => {
                    final_error = err.into();
                    continue;
                }
            };

            let request = RecoverySymbolRequest {
                blob_id: *blob_id,
                source_sliver,
                target_pair_index,
                encoding_type,
            };

            match worker.call(request).await {
                Ok(symbol) => return Ok(symbol),
                Err(RecoverySymbolError::IndexTooLarge) => {
                    panic!("index validity must be checked before calling this function")
                }
                Err(RecoverySymbolError::EncodeError(error)) => {
                    final_error = RetrieveSymbolError::Internal(anyhow!(error));
                }
            }
        }

        Err(final_error)
    }

    /// Extracts decoding symbols from a source sliver for multiple target slivers.
    /// Returns a map of target sliver indexes to decoding symbols.
    fn extract_decoding_symbols_for_target_sliver_into_output<T: EncodingAxis>(
        &self,
        sliver: &SliverData<T>,
        target_sliver_indexes: &[SliverIndex],
    ) -> Result<BTreeMap<SliverIndex, EitherDecodingSymbol>, ListSymbolsError>
    where
        EitherDecodingSymbol: From<DecodingSymbol<<T as EncodingAxis>::OrthogonalAxis>>,
    {
        let mut output = BTreeMap::new();
        for target_sliver_index in target_sliver_indexes {
            if usize::from(target_sliver_index.get()) >= sliver.symbols.len() {
                return Err(ListSymbolsError::RetrieveDecodingSymbolOutOfRange(format!(
                    "target sliver index out of range: {} < {}",
                    target_sliver_index.get(),
                    sliver.symbols.len(),
                )));
            }

            let symbol_bytes = sliver.symbols[target_sliver_index.as_usize()].to_vec();
            let sliver_index = sliver.index;
            let decoding_symbol: EitherDecodingSymbol =
                DecodingSymbol::<T::OrthogonalAxis>::new(sliver_index.get(), symbol_bytes).into();
            output.insert(*target_sliver_index, decoding_symbol);
        }
        Ok(output)
    }

    fn get_encoding_type_for_blob(
        &self,
        blob_id: &BlobId,
    ) -> Result<Option<EncodingType>, TypedStoreError> {
        match walrus_core::SUPPORTED_ENCODING_TYPES {
            &[EncodingType::RS2] => Ok(Some(EncodingType::RS2)),
            // If additional encoding types are used, then it becomes necessary to fetch the
            // encoding type from the database. If the metadata is otherwise available, then the
            // encoding type is available there and this function should not be used, otherwise, a
            // database call may be required.
            _ => self
                .storage
                .get_metadata(blob_id)
                .map(|option| option.map(|metadata| metadata.metadata().encoding_type())),
        }
    }

    fn symbol_ids_from_filter<'a>(
        &self,
        blob_id: BlobId,
        filter: &'a RecoverySymbolsFilter,
    ) -> impl Iterator<Item = SymbolId> + 'a {
        match filter.id_filter() {
            SymbolIdFilter::Ids(symbol_ids) => Either::Left(symbol_ids.iter().copied()),

            SymbolIdFilter::Recovers {
                target_sliver: target,
                target_type,
            } => {
                let n_shards = self.n_shards();
                let map_fn = move |shard_id: ShardIndex| {
                    let pair_stored = shard_id.to_pair_index(n_shards, &blob_id);
                    match *target_type {
                        SliverType::Primary => SymbolId::new(
                            *target,
                            pair_stored.to_sliver_index::<Secondary>(n_shards),
                        ),
                        SliverType::Secondary => {
                            SymbolId::new(pair_stored.to_sliver_index::<Primary>(n_shards), *target)
                        }
                    }
                };
                Either::Right(self.owned_shards_at_latest_epoch().into_iter().map(map_fn))
            }
        }
    }

    async fn wait_for_ready_symbol_service(&self) -> RecoverySymbolService {
        self.symbol_service
            .clone()
            .ready_oneshot()
            .await
            .expect("polling the symbol_service is infallible")
    }

    fn get_ready_symbol_service(&self) -> Result<RecoverySymbolService, RetrieveSymbolError> {
        self.wait_for_ready_symbol_service()
            .now_or_never()
            .ok_or_else(|| Unavailable.into())
    }

    /// Sets deferral for recovery for the blob for the specified duration (inherent method).
    pub async fn set_recovery_deferral(
        &self,
        blob_id: BlobId,
        defer_duration: std::time::Duration,
    ) -> Result<(), SetRecoveryDeferralError> {
        let is_registered = self.is_blob_registered(&blob_id).map_err(|error| {
            tracing::warn!(
                walrus.blob_id = %blob_id,
                ?error,
                "failed to check blob registration before scheduling recovery deferral"
            );
            SetRecoveryDeferralError::Internal(error)
        })?;
        if !is_registered {
            tracing::trace!(
                walrus.blob_id = %blob_id,
                "skipping recovery deferral; blob is not registered at this node"
            );
            return Err(SetRecoveryDeferralError::NotRegistered);
        }

        if self
            .is_stored_at_all_shards_at_latest_epoch(&blob_id)
            .await
            .map_err(|error| {
                tracing::warn!(
                    walrus.blob_id = %blob_id,
                    ?error,
                    "failed to check storage state before scheduling recovery deferral"
                );
                SetRecoveryDeferralError::Internal(error)
            })?
        {
            tracing::trace!(
                walrus.blob_id = %blob_id,
                "skipping recovery deferral; blob already stored on all shards"
            );
            return Err(SetRecoveryDeferralError::AlreadyStored);
        }

        let until = std::time::Instant::now() + defer_duration;
        let current_size = {
            let mut map = self.recovery_deferrals.write().await;
            match map.entry(blob_id) {
                Entry::Occupied(mut entry) => {
                    let (existing_until, _) = entry.get_mut();
                    if until > *existing_until {
                        tracing::trace!(
                            walrus.blob_id = %blob_id,
                            "extending recovery deferral deadline"
                        );
                        *existing_until = until;
                    } else {
                        tracing::trace!(
                            walrus.blob_id = %blob_id,
                            "existing recovery deferral already expires later"
                        );
                    }
                    None
                }
                Entry::Vacant(entry) => {
                    entry.insert((
                        until,
                        std::sync::Arc::new(tokio_util::sync::CancellationToken::new()),
                    ));
                    Some(map.len())
                }
            }
        };
        if let Some(size) = current_size {
            self.update_recovery_deferral_size_metric(size);
        }

        self.recovery_deferral_notify.notify_one();

        Ok(())
    }

    /// Removes any active recovery deferral for the specified blob.
    pub async fn clear_recovery_deferral(&self, blob_id: &BlobId) {
        let size = {
            let mut map = self.recovery_deferrals.write().await;
            if let Some((_, token)) = map.remove(blob_id) {
                token.cancel();
            }
            map.len()
        };
        self.update_recovery_deferral_size_metric(size);
        self.recovery_deferral_notify.notify_one();
    }

    fn update_recovery_deferral_size_metric(&self, size: usize) {
        // Avoid potential wrap by saturating at i64::MAX on extremely large sizes.
        let size_i64 = i64::try_from(size).unwrap_or(i64::MAX);
        self.metrics.recovery_deferrals_active.set(size_i64);
    }

    fn start_recovery_deferral_cleanup_task(self: &Arc<Self>) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            this.recovery_deferral_cleanup_task().await;
        });
    }

    async fn recovery_deferral_cleanup_task(self: Arc<Self>) {
        loop {
            if self.recovery_deferral_cleanup_token.is_cancelled() {
                break;
            }

            self.remove_expired_recovery_deferrals().await;

            let sleep_duration = match self.next_recovery_deferral_deadline().await {
                Some(deadline) => deadline.saturating_duration_since(std::time::Instant::now()),
                None => Duration::from_secs(60),
            };

            // Any newly scheduled deferral notifies the waiters below, forcing us to recompute the
            // minimum deadline so we never oversleep a shorter timeout.
            tokio::select! {
                _ = self.recovery_deferral_cleanup_token.cancelled() => break,
                _ = self.recovery_deferral_notify.notified() => continue,
                _ = tokio::time::sleep(sleep_duration) => {}
            }
        }

        self.cancel_all_recovery_deferrals().await;
    }

    async fn next_recovery_deferral_deadline(&self) -> Option<std::time::Instant> {
        let map = self.recovery_deferrals.read().await;
        map.values().map(|(until, _)| *until).min()
    }

    async fn remove_expired_recovery_deferrals(&self) {
        let now = std::time::Instant::now();
        let mut removed_any = false;
        let size = {
            let mut map = self.recovery_deferrals.write().await;
            map.retain(|_, (until, token)| {
                if *until <= now {
                    token.cancel();
                    removed_any = true;
                    false
                } else {
                    true
                }
            });
            map.len()
        };
        if removed_any {
            self.update_recovery_deferral_size_metric(size);
        }
    }

    async fn cancel_all_recovery_deferrals(&self) {
        let mut map = self.recovery_deferrals.write().await;
        for (_, (_, token)) in map.drain() {
            token.cancel();
        }
        self.update_recovery_deferral_size_metric(0);
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
        intent: UploadIntent,
    ) -> Result<bool, StoreMetadataError> {
        self.inner.store_metadata(metadata, intent).await
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
    ) -> impl Future<Output = Result<Arc<Sliver>, RetrieveSliverError>> + Send {
        self.inner
            .retrieve_sliver(blob_id, sliver_pair_index, sliver_type)
    }

    fn store_sliver(
        &self,
        blob_id: BlobId,
        sliver_pair_index: SliverPairIndex,
        sliver: Sliver,
        intent: UploadIntent,
    ) -> impl Future<Output = Result<bool, StoreSliverError>> + Send {
        self.inner
            .store_sliver(blob_id, sliver_pair_index, sliver, intent)
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

    fn wait_for_registration(
        &self,
        blob_id: &BlobId,
        timeout: Duration,
    ) -> impl Future<Output = bool> + Send {
        self.inner.wait_for_registration_inner(blob_id, timeout)
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

    fn retrieve_multiple_recovery_symbols(
        &self,
        blob_id: &BlobId,
        filter: RecoverySymbolsFilter,
    ) -> impl Future<Output = Result<Vec<GeneralRecoverySymbol>, ListSymbolsError>> + Send {
        self.inner
            .retrieve_multiple_recovery_symbols(blob_id, filter)
    }

    fn retrieve_multiple_decoding_symbols(
        &self,
        blob_id: &BlobId,
        target_slivers: Vec<SliverIndex>,
        target_type: SliverType,
    ) -> impl Future<
        Output = Result<BTreeMap<SliverIndex, Vec<EitherDecodingSymbol>>, ListSymbolsError>,
    > + Send {
        self.inner
            .retrieve_multiple_decoding_symbols(blob_id, target_slivers, target_type)
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

    fn set_recovery_deferral(
        &self,
        blob_id: BlobId,
        defer_duration: std::time::Duration,
    ) -> impl Future<Output = Result<(), SetRecoveryDeferralError>> + Send {
        self.inner.set_recovery_deferral(blob_id, defer_duration)
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

        self.validate_blob_access(
            blob_id,
            RetrieveMetadataError::Forbidden,
            RetrieveMetadataError::Unavailable,
        )?;

        self.storage
            .get_metadata(blob_id)
            .context("database error when retrieving metadata")?
            .ok_or(RetrieveMetadataError::Unavailable)
            .inspect(|_| self.metrics.metadata_retrieved_total.inc())
    }

    /// Stores metadata for a blob.
    ///
    /// Returns `Ok(true)` when the metadata was persisted to storage or cached for later
    /// persistence, and `Ok(false)` when an identical copy is already present.
    async fn store_metadata(
        &self,
        metadata: UnverifiedBlobMetadataWithId,
        intent: UploadIntent,
    ) -> Result<bool, StoreMetadataError> {
        let blob_id = *metadata.blob_id();

        let blob_info = self
            .storage
            .get_blob_info(&blob_id)
            .context("could not retrieve blob info")?;

        if let Some(event) = blob_info
            .as_ref()
            .and_then(|info| info.invalidation_event())
        {
            return Err(StoreMetadataError::InvalidBlob(event));
        }

        if self
            .storage
            .has_metadata(&blob_id)
            .context("could not check metadata existence")?
        {
            return Ok(false);
        }

        if self.pending_metadata_cache.contains(&blob_id).await {
            return Ok(false);
        }

        let encoding_type = metadata.metadata().encoding_type();
        ensure!(
            encoding_type.is_supported(),
            StoreMetadataError::UnsupportedEncodingType(encoding_type),
        );

        let encoding_config = self.encoding_config.clone();
        let verified = Arc::new(
            self.thread_pool
                .clone()
                .oneshot(move || metadata.verify(&encoding_config))
                .map(thread_pool::unwrap_or_resume_panic)
                .await?,
        );

        if blob_info
            .as_ref()
            .is_some_and(|info| info.is_registered(self.current_committee_epoch()))
        {
            return self.persist_verified_metadata(&blob_id, verified).await;
        }

        if !intent.is_pending() {
            tracing::debug!(
                %blob_id,
                ?intent,
                "store_metadata: blob not registered and pending not allowed"
            );
            return Err(StoreMetadataError::NotCurrentlyRegistered);
        }

        let inserted = self
            .pending_metadata_cache
            .insert(blob_id, verified.clone())
            .await
            .map_err(|_| StoreMetadataError::CacheSaturated)?;

        if !inserted {
            return Ok(false);
        }

        if self.is_blob_registered(&blob_id)? {
            // Read the blob info again because registration can race between the initial blob_info
            // check and adding to the cache. Flush immediately in that case so the pending metadata
            // doesn’t linger indefinitely.
            if let Err(error) = self.flush_pending_caches(&blob_id).await {
                return Err(match error {
                    PendingCacheError::Metadata(inner) => inner,
                    PendingCacheError::Sliver(inner) => map_sliver_error_to_metadata(inner),
                });
            }
        }

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
    ) -> Result<Arc<Sliver>, RetrieveSliverError> {
        self.check_index(sliver_pair_index)?;

        self.validate_blob_access(
            blob_id,
            RetrieveSliverError::Forbidden,
            RetrieveSliverError::Unavailable,
        )?;

        self.retrieve_sliver_unchecked(blob_id, sliver_pair_index, sliver_type)
            .await
    }

    async fn store_sliver(
        &self,
        blob_id: BlobId,
        sliver_pair_index: SliverPairIndex,
        sliver: Sliver,
        intent: UploadIntent,
    ) -> Result<bool, StoreSliverError> {
        self.check_index(sliver_pair_index)?;

        let n_shards = self.n_shards();
        let expected_pair_index = match sliver.r#type() {
            SliverType::Primary => sliver.sliver_index().to_pair_index::<Primary>(n_shards),
            SliverType::Secondary => sliver.sliver_index().to_pair_index::<Secondary>(n_shards),
        };
        ensure!(
            sliver_pair_index == expected_pair_index,
            StoreSliverError::SliverIndexMismatch {
                sliver_pair_index,
                sliver_index: sliver.sliver_index(),
            }
        );

        let (metadata_persisted, persisted) = self
            .resolve_metadata_for_sliver(&blob_id, intent.is_pending())
            .await?;

        let encoding_type = metadata_persisted.metadata().encoding_type();
        if !encoding_type.is_supported() {
            return Err(StoreSliverError::UnsupportedEncodingType(encoding_type));
        }
        tracing::debug!(
            %blob_id,
            ?sliver_pair_index,
            ?intent,
            ?metadata_persisted,
            "store_sliver: resolved metadata for sliver"
        );

        if persisted {
            // Metadata is already persisted, so the sliver can be written directly because
            // metadata is persisted after the blob is registered.
            return self
                .store_sliver_unchecked(metadata_persisted.clone(), sliver_pair_index, sliver)
                .await;
        }

        let Some((_, verified_sliver)) = self
            .prepare_sliver_for_storage(metadata_persisted.clone(), sliver_pair_index, sliver)
            .await?
        else {
            return Ok(false);
        };

        match self
            .pending_sliver_cache
            .insert(blob_id, sliver_pair_index, verified_sliver)
            .await
        {
            Ok(inserted) => {
                tracing::debug!(
                    %blob_id,
                    ?sliver_pair_index,
                    inserted,
                    "store_sliver: buffered sliver in pending cache"
                );
                if self.is_blob_registered(&blob_id)? {
                    // Registration may arrive between the initial registration check and the point
                    // where we enqueue the sliver. If it does, flush everything immediately so the
                    // data doesn’t stay buffered without another trigger.
                    if let Err(error) = self.flush_pending_caches(&blob_id).await {
                        return Err(match error {
                            PendingCacheError::Sliver(inner) => inner,
                            PendingCacheError::Metadata(inner) => {
                                map_metadata_error_to_sliver(inner)
                            }
                        });
                    }
                }

                Ok(inserted)
            }
            Err(PendingSliverCacheError::SliverTooLarge) => Err(StoreSliverError::CacheSaturated),
            Err(PendingSliverCacheError::Saturated) => Err(StoreSliverError::CacheSaturated),
        }
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

        if let Err(error) = self.flush_pending_caches(blob_id).await {
            return Err(map_flush_error_to_confirmation(error));
        }

        // Storage confirmation must use the last shard assignment, even though the node hasn't
        // processed to the latest epoch yet. This is because if the onchain committee has moved
        // on to the new epoch, confirmation from the old epoch is not longer valid.
        let fully_stored = self
            .is_stored_at_all_shards_at_latest_epoch(blob_id)
            .await
            .context("database error when checking storage status")?;
        ensure!(
            fully_stored,
            ComputeStorageConfirmationError::NotFullyStored,
        );

        if let BlobPersistenceType::Deletable { object_id } = blob_persistence_type {
            let per_object_info = self
                .storage
                .get_per_object_info(&object_id.into())
                .context("database error when checking per object info")?
                .ok_or(ComputeStorageConfirmationError::NotCurrentlyRegistered)?;
            ensure!(
                per_object_info.is_registered(self.current_committee_epoch()),
                ComputeStorageConfirmationError::NotCurrentlyRegistered,
            );
        }

        let confirmation = Confirmation::new(
            self.current_committee_epoch(),
            *blob_id,
            *blob_persistence_type,
        );
        let signed = sign_message(confirmation, self.protocol_key_pair.clone()).await?;

        self.metrics.storage_confirmations_issued_total.inc();

        Ok(StorageConfirmation::Signed(signed))
    }

    fn wait_for_registration(
        &self,
        blob_id: &BlobId,
        timeout: Duration,
    ) -> impl Future<Output = bool> + Send {
        self.wait_for_registration_inner(blob_id, timeout)
    }

    fn blob_status(&self, blob_id: &BlobId) -> Result<BlobStatus, BlobStatusError> {
        Ok(self
            .storage
            .get_blob_info(blob_id)
            .context("could not retrieve blob info")?
            .map(|blob_info| blob_info.to_blob_status(self.current_committee_epoch()))
            .unwrap_or_default())
    }

    async fn verify_inconsistency_proof(
        &self,
        blob_id: &BlobId,
        inconsistency_proof: InconsistencyProof,
    ) -> Result<InvalidBlobIdAttestation, InconsistencyProofError> {
        let metadata = self.retrieve_metadata(blob_id)?;

        inconsistency_proof.verify(metadata.as_ref(), &self.encoding_config)?;

        let message = InvalidBlobIdMsg::new(self.current_committee_epoch(), blob_id.to_owned());
        Ok(sign_message(message, self.protocol_key_pair.clone()).await?)
    }

    #[tracing::instrument(skip_all)]
    async fn retrieve_multiple_recovery_symbols(
        &self,
        blob_id: &BlobId,
        filter: RecoverySymbolsFilter,
    ) -> Result<Vec<GeneralRecoverySymbol>, ListSymbolsError> {
        // Begin by fetching a ready worker, as this gates all database reads based
        // on whether we have capacity to even serve the request.
        let mut worker = Some(self.get_ready_symbol_service()?);

        self.validate_blob_access(
            blob_id,
            RetrieveSliverError::Forbidden,
            RetrieveSliverError::Unavailable,
        )
        .map_err(RetrieveSymbolError::RetrieveSliver)?;

        let encoding_type = self
            .get_encoding_type_for_blob(blob_id)
            .context("could not retrieve blob encoding type")?
            .ok_or_else(|| anyhow!("encoding type unavailable for blob {:?}", blob_id))?;

        // If a specific proof axis is requested, then specify the target-type to the retrieve
        // function, otherwise, specify only the symbol IDs.
        let target_type_from_proof = filter.proof_axis().map(|axis| axis.orthogonal());

        // We use FuturesOrdered to keep the results in the same order as the requests.
        let mut symbols = FuturesOrdered::new();

        for symbol_id in self.symbol_ids_from_filter(*blob_id, &filter) {
            let owned_worker = worker.take();

            symbols.push_back(async move {
                // Since we acquired the service above, and began processing this list-request,
                // wait for workers for the subsequent symbols so that we make an attempt at
                // processing the entire request.
                let worker = match owned_worker {
                    Some(worker) => worker,
                    None => self.wait_for_ready_symbol_service().await,
                };

                self.retrieve_recovery_symbol_unchecked(
                    blob_id,
                    symbol_id,
                    target_type_from_proof,
                    encoding_type,
                    worker,
                )
                .map(move |result| (symbol_id, result))
                .await
            });
        }

        let mut output = vec![];
        let mut last_error = ListSymbolsError::NoSymbolsSpecified;

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

    async fn retrieve_multiple_decoding_symbols(
        &self,
        blob_id: &BlobId,
        target_sliver_indexes: Vec<SliverIndex>,
        target_type: SliverType,
    ) -> Result<BTreeMap<SliverIndex, Vec<EitherDecodingSymbol>>, ListSymbolsError> {
        self.validate_blob_access(
            blob_id,
            RetrieveSliverError::Forbidden,
            RetrieveSliverError::Unavailable,
        )
        .map_err(RetrieveSymbolError::RetrieveSliver)?;

        if target_sliver_indexes.is_empty() {
            return Err(ListSymbolsError::NoTargetSliversSpecified);
        }

        let mut output: BTreeMap<SliverIndex, Vec<EitherDecodingSymbol>> = BTreeMap::new();
        let n_shards = self.n_shards();
        let owned_shards = self.owned_shards_at_latest_epoch();

        // Makes sure that the target sliver indexes are all targeting source slivers, and not
        // derived slivers. This endpoint is meant to only serve symbols used to recover
        // source slivers.
        for target_sliver_index in target_sliver_indexes.iter() {
            let (source_primary_encoding_symbol_count, source_secondary_encoding_symbol_count) =
                source_symbols_for_n_shards(n_shards);
            let source_sliver_index_boundary = match target_type {
                Axis::Primary => source_primary_encoding_symbol_count,
                Axis::Secondary => source_secondary_encoding_symbol_count,
            };
            if target_sliver_index >= &source_sliver_index_boundary {
                return Err(ListSymbolsError::RetrieveDecodingSymbolOutOfRange(format!(
                    "target sliver index out of range, requested target sliver index {}, \
                        max source sliver index {}, requested sliver type {}",
                    target_sliver_index.get(),
                    source_sliver_index_boundary,
                    target_type
                )));
            }
        }

        for source_pair_index in owned_shards
            .iter()
            .map(|shard_index| shard_index.to_pair_index(n_shards, blob_id))
        {
            // TODO(WAL-1120): make fetching slivers parallel.
            let sliver_result = match self
                .retrieve_sliver_unchecked(blob_id, source_pair_index, target_type.orthogonal())
                .await
                .map_err(RetrieveSymbolError::RetrieveSliver)
            {
                Ok(sliver) => sliver,
                Err(error) => {
                    tracing::warn!(%error, ?blob_id, ?source_pair_index, ?target_type,
                            "retrieve decoding symbols failed to retrieve sliver");
                    continue;
                }
            };

            let extracted_symbols =
                by_axis::flat_map!(sliver_result.as_ref().as_ref(), |sliver| self
                    .extract_decoding_symbols_for_target_sliver_into_output(
                        sliver,
                        &target_sliver_indexes,
                    ))?;

            for (target_sliver_index, decoding_symbol) in extracted_symbols {
                output
                    .entry(target_sliver_index)
                    .or_default()
                    .push(decoding_symbol);
            }
        }

        Ok(output)
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
            epoch: self.current_committee_epoch(),
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
        let shard_storage = self
            .get_shard_for_sliver_pair(sliver_pair_index, blob_id)
            .await?;

        match shard_storage.is_sliver_stored::<A>(blob_id) {
            Ok(true) => Ok(StoredOnNodeStatus::Stored),
            Ok(false) => {
                if self
                    .pending_sliver_cache
                    .contains(blob_id, sliver_pair_index, A::sliver_type())
                    .await
                {
                    Ok(StoredOnNodeStatus::Buffered)
                } else {
                    Ok(StoredOnNodeStatus::Nonexistent)
                }
            }
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
        if request.epoch() != self.current_committee_epoch() {
            return Err(InvalidEpochError {
                request_epoch: request.epoch(),
                server_epoch: self.current_committee_epoch(),
            }
            .into());
        }

        self.storage
            .handle_sync_shard_request(request, self.current_committee_epoch())
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

#[derive(Debug)]
enum PendingCacheError {
    Metadata(StoreMetadataError),
    Sliver(StoreSliverError),
}

fn map_sliver_error_to_metadata(error: StoreSliverError) -> StoreMetadataError {
    match error {
        StoreSliverError::Internal(inner) => StoreMetadataError::Internal(inner),
        StoreSliverError::NotCurrentlyRegistered
        | StoreSliverError::MissingMetadata
        | StoreSliverError::CacheSaturated
        | StoreSliverError::SliverTooLarge => StoreMetadataError::NotCurrentlyRegistered,
        StoreSliverError::UnsupportedEncodingType(kind) => {
            StoreMetadataError::UnsupportedEncodingType(kind)
        }
        StoreSliverError::SliverOutOfRange(_)
        | StoreSliverError::InvalidSliver(_)
        | StoreSliverError::SliverIndexMismatch { .. } => {
            StoreMetadataError::Internal(anyhow!("sliver cache flush failed: {error:?}"))
        }
        StoreSliverError::ShardNotAssigned(inner) => StoreMetadataError::Internal(inner.into()),
    }
}

fn map_metadata_error_to_sliver(error: StoreMetadataError) -> StoreSliverError {
    match error {
        StoreMetadataError::NotCurrentlyRegistered => StoreSliverError::NotCurrentlyRegistered,
        StoreMetadataError::CacheSaturated => StoreSliverError::CacheSaturated,
        StoreMetadataError::UnsupportedEncodingType(kind) => {
            StoreSliverError::UnsupportedEncodingType(kind)
        }
        StoreMetadataError::InvalidMetadata(err) => StoreSliverError::Internal(anyhow!(err)),
        StoreMetadataError::InvalidBlob(event) => StoreSliverError::Internal(anyhow!(
            "metadata associated with event {event:?} was invalid"
        )),
        StoreMetadataError::Internal(inner) => StoreSliverError::Internal(inner),
    }
}

fn map_flush_error_to_confirmation(error: PendingCacheError) -> ComputeStorageConfirmationError {
    match error {
        PendingCacheError::Metadata(inner) => {
            ComputeStorageConfirmationError::Internal(inner.into())
        }
        PendingCacheError::Sliver(inner) => match inner {
            StoreSliverError::MissingMetadata => {
                ComputeStorageConfirmationError::NotCurrentlyRegistered
            }
            StoreSliverError::ShardNotAssigned(inner) => {
                ComputeStorageConfirmationError::Internal(inner.into())
            }
            other => ComputeStorageConfirmationError::Internal(other.into()),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use chrono::Utc;
    use config::ShardSyncConfig;
    use contract_service::MockSystemContractService;
    use storage::{
        ShardStatus,
        tests::{BLOB_ID, OTHER_SHARD_INDEX, SHARD_INDEX, WhichSlivers, populated_storage},
    };
    use sui_types::base_types::ObjectID;
    use system_events::SystemEventProvider;
    use tokio::sync::broadcast::Sender;
    use walrus_core::{
        DEFAULT_ENCODING,
        Sliver,
        SliverType,
        by_axis::ByAxis,
        encoding::{Primary, Secondary, SliverData},
        messages::{SyncShardMsg, SyncShardRequest},
        test_utils::random_blob_id,
    };
    use walrus_storage_node_client::api::errors::STORAGE_NODE_ERROR_DOMAIN;
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

    use super::{test_helpers::*, *};
    use crate::test_utils::{
        StorageNodeHandle,
        StorageNodeHandleTrait,
        retry_until_success_or_timeout,
    };

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
                storage_node.as_ref().inner.current_committee_epoch()
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
            blob_deleted_event_registered: (
                BlobDeleted{was_certified: false, ..BlobDeleted::for_testing(BLOB_ID)}.into(),
                false
            ),
            blob_deleted_event_certified: (BlobDeleted::for_testing(BLOB_ID).into(), true),
        ]
    }
    async fn deletes_blob_data_on_event(event: BlobEvent, is_certified: bool) -> TestResult {
        walrus_test_utils::init_tracing();
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

        inner
            .check_does_not_store_metadata_or_slivers_for_blob(&BLOB_ID)
            .await?;
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

    #[tokio::test]
    async fn reports_buffered_status_for_pending_slivers() -> TestResult {
        let (cluster, _) = cluster_at_epoch1_without_blobs(&[&[0, 1, 2, 3]], None).await?;
        let storage_node = cluster.nodes[0].storage_node.clone();
        let encoding_config = storage_node.as_ref().inner.encoding_config.as_ref().clone();
        let encoded = EncodedBlob::new(BLOB, encoding_config);
        let blob_id = *encoded.blob_id();
        let pair = encoded.assigned_sliver_pair(SHARD_INDEX);

        assert!(
            storage_node
                .as_ref()
                .store_metadata(
                    encoded.metadata.clone().into_unverified(),
                    UploadIntent::Pending
                )
                .await?
        );

        assert!(
            storage_node
                .as_ref()
                .store_sliver(
                    blob_id,
                    pair.index(),
                    Sliver::Primary(pair.primary.clone()),
                    UploadIntent::Pending,
                )
                .await?
        );

        let buffered_status = storage_node
            .as_ref()
            .inner
            .sliver_status::<Primary>(&blob_id, pair.index())
            .await?;
        assert_eq!(buffered_status, StoredOnNodeStatus::Buffered);

        // This unit test bypasses the event processor, so flush manually. The registration-driven
        // path is covered in `flush_after_registration`.
        storage_node
            .as_ref()
            .inner
            .storage
            .update_blob_info(0, &BlobRegistered::for_testing(blob_id).into())?;
        storage_node
            .as_ref()
            .inner
            .flush_pending_metadata(&blob_id)
            .await?;
        let persisted_metadata = Arc::new(
            storage_node
                .as_ref()
                .inner
                .storage
                .get_metadata(&blob_id)?
                .expect("metadata should be persisted"),
        );
        storage_node
            .as_ref()
            .inner
            .flush_pending_slivers(&blob_id, persisted_metadata)
            .await?;

        let stored_status = storage_node
            .as_ref()
            .inner
            .sliver_status::<Primary>(&blob_id, pair.index())
            .await?;
        assert_eq!(stored_status, StoredOnNodeStatus::Stored);
        Ok(())
    }

    #[tokio::test]
    async fn store_sliver_rejects_inconsistent_sliver_pair_index() -> TestResult {
        let (cluster, _) = cluster_at_epoch1_without_blobs(&[&[0, 1, 2, 3]], None).await?;
        let storage_node = cluster.nodes[0].storage_node.clone();
        let encoding_config = storage_node.as_ref().inner.encoding_config.as_ref().clone();
        let encoded = EncodedBlob::new(BLOB, encoding_config);
        let blob_id = *encoded.blob_id();
        let pair_0 = encoded.assigned_sliver_pair(SHARD_INDEX);
        let pair_1 = encoded.assigned_sliver_pair(OTHER_SHARD_INDEX);

        assert!(
            storage_node
                .as_ref()
                .store_metadata(
                    encoded.metadata.clone().into_unverified(),
                    UploadIntent::Pending
                )
                .await?
        );

        // Primary sliver from pair 0 has sliver index 0, so it must be stored with pair index 0.
        // Passing pair_1.index() is inconsistent and must be rejected.
        let err = storage_node
            .as_ref()
            .store_sliver(
                blob_id,
                pair_1.index(),
                Sliver::Primary(pair_0.primary.clone()),
                UploadIntent::Pending,
            )
            .await
            .expect_err("store_sliver must reject inconsistent sliver pair index");
        assert!(
            matches!(err, StoreSliverError::SliverIndexMismatch { .. }),
            "expected SliverIndexMismatch, got {err:?}"
        );

        // Secondary sliver from pair 0 has sliver index n_shards-1 (for pair index 0).
        // Passing pair_1.index() is inconsistent and must be rejected.
        let err = storage_node
            .as_ref()
            .store_sliver(
                blob_id,
                pair_1.index(),
                Sliver::Secondary(pair_0.secondary.clone()),
                UploadIntent::Pending,
            )
            .await
            .expect_err("store_sliver must reject inconsistent sliver pair index for secondary");
        assert!(
            matches!(err, StoreSliverError::SliverIndexMismatch { .. }),
            "expected SliverIndexMismatch, got {err:?}"
        );

        // Consistent indices must be accepted.
        for pair in [pair_0, pair_1] {
            storage_node
                .as_ref()
                .store_sliver(
                    blob_id,
                    pair.index(),
                    Sliver::Primary(pair.primary.clone()),
                    UploadIntent::Pending,
                )
                .await?;
        }

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
                .await
                .context("failed to verify inconsistency proof")?;

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
                node.as_ref().inner.current_committee_epoch()
            );
            assert_eq!(*invalid_blob_msg.as_ref().contents(), blob_id);

            Ok(())
        }
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
    #[ignore = "ignore long-running test by default"]
    async fn recovers_all_shards_for_multi_shard_node() -> TestResult {
        let shards: &[&[u16]] = &[&[0, 1], &[2, 3]];

        let (cluster, events, blob) =
            cluster_with_partially_stored_blob(shards, BLOB, |shard, _| shard.get() >= 2).await?;
        events.send(BlobCertified::for_testing(*blob.blob_id()).into())?;

        let node_client = cluster.client(0);
        for shard in [ShardIndex(0), ShardIndex(1)] {
            let synced_sliver_pair =
                expect_sliver_pair_stored_before_timeout(&blob, node_client, shard, TIMEOUT).await;
            assert_eq!(
                synced_sliver_pair,
                *blob.assigned_sliver_pair(shard),
                "invalid sliver pair for {shard}"
            );
        }

        Ok(())
    }

    async_param_test! {
        nodes_in_cluster_are_active -> TestResult: [
            single_node_single_shard: (&[&[0]]),
            single_node_multiple_shards: (&[&[0, 1, 2, 3, 4]]),
            two_nodes: (&[&[0, 1], &[2, 3, 4]]),
            four_nodes: (&[&[0, 1], &[2, 3], &[4, 5], &[6, 7]]),
        ]
    }
    async fn nodes_in_cluster_are_active(shard_assignment: &[&[u16]]) -> TestResult {
        let (cluster, _events) =
            cluster_at_epoch1_without_blobs_waiting_for_active_nodes(shard_assignment, None)
                .await?;
        for node in cluster.nodes {
            assert_eq!(
                node.storage_node().inner.storage.node_status()?,
                NodeStatus::Active
            )
        }
        Ok(())
    }

    #[tokio::test]
    async fn does_not_start_blob_sync_for_already_expired_blob() -> TestResult {
        walrus_test_utils::init_tracing();
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

        // Wait until the node has processed all events (2 epoch-change events + 2 blob events).
        wait_until_events_processed_exact(node, 4).await?;
        assert_eq!(node.storage_node.blob_sync_handler.cancel_all().await?, 0);

        Ok(())
    }

    // Tests that a panic thrown by a blob sync task is propagated to the node runtime.
    #[tokio::test]
    #[ignore = "ignore long-running test by default"]
    async fn blob_sync_panic_thrown() -> TestResult {
        let shards: &[&[u16]] = &[&[1], &[0, 2, 3, 4, 5, 6]];
        let test_shard = ShardIndex(1);

        let (mut cluster, events, blob) =
            cluster_with_partially_stored_blob(shards, BLOB, |&shard, _| shard != test_shard)
                .await?;

        // Start a sync to trigger the blob sync task.
        // Using an epoch number in the future will trigger a panic in reading the committee.
        cluster.nodes[0]
            .storage_node
            .blob_sync_handler
            .start_sync(*blob.blob_id(), 40, None)
            .await?;

        events.send(BlobCertified::for_testing(*blob.blob_id()).into())?;

        // Wait for the node runtime to finish, and check that a panic was thrown.
        match tokio::time::timeout(
            Duration::from_mins(2),
            cluster.nodes[0].node_runtime_handle.as_mut().unwrap(),
        )
        .await
        {
            Ok(Err(e)) => assert!(e.is_panic()),
            Err(_) => panic!("node didn't panic within timeout"),
            Ok(Ok(_)) => panic!("expected node to panic"),
        }

        Ok(())
    }

    // Tests that a blob sync is not started for a node in recovery catch up.
    #[tokio::test]
    async fn does_not_start_blob_sync_for_node_in_recovery_catch_up() -> TestResult {
        walrus_test_utils::init_tracing();
        let shards: &[&[u16]] = &[&[1], &[0, 2, 3, 4, 5, 6]];

        // Create a cluster at epoch 1 without any blobs.
        let (cluster, _events) = cluster_at_epoch1_without_blobs(shards, None).await?;

        cluster.wait_for_nodes_to_reach_epoch(1).await;

        // Set node 0 status to recovery catch up.
        cluster.nodes[0]
            .storage_node
            .inner
            .storage
            .set_node_status(NodeStatus::RecoveryCatchUp)?;

        // Start a sync for a random blob id. Since this blob does not exist, the sync will be
        // running indefinitely if not cancelled.
        let random_blob_id = random_blob_id();
        cluster.nodes[0]
            .storage_node
            .blob_sync_handler
            .start_sync(random_blob_id, 1, None)
            .await
            .unwrap();

        // Wait for the sync to be cancelled.
        retry_until_success_or_timeout(TIMEOUT, || async {
            let blob_sync_in_progress = cluster.nodes[0]
                .storage_node
                .blob_sync_handler
                .blob_sync_in_progress()
                .len();
            if blob_sync_in_progress == 0 {
                Ok(())
            } else {
                Err(anyhow!("{} blob syncs in progress", blob_sync_in_progress))
            }
        })
        .await?;

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
        walrus_test_utils::init_tracing();

        let shards: &[&[u16]] = &[&[1, 6], &[0, 2, 3, 4, 5]];
        let own_shards = [ShardIndex(1), ShardIndex(6)];

        let blob1 = (0..80u8).collect::<Vec<_>>();
        let blob2 = (80..160u8).collect::<Vec<_>>();
        let blob3 = (160..255u8).collect::<Vec<_>>();

        let store_at_other_node_fn = |shard: &ShardIndex, _| !own_shards.contains(shard);
        let (cluster, events, blob1_details) =
            cluster_with_partially_stored_blob(shards, &blob1, store_at_other_node_fn).await?;
        events.send(
            BlobCertified::for_testing_with_object_id(
                *blob1_details.blob_id(),
                blob1_details.object_id.unwrap(),
            )
            .into(),
        )?;

        let node_client = cluster.client(0);
        let config = &blob1_details.config;

        // Send events that some unobserved blob has been certified.
        let blob2_details = EncodedBlob::new(&blob2, config.clone());
        let blob2_registered_event =
            BlobRegistered::for_testing_with_random_object_id(*blob2_details.blob_id());
        events.send(blob2_registered_event.clone().into())?;
        let blob2_registered_event_id = blob2_registered_event.event_id;

        // The node should not be able to advance past the following event.
        events.send(
            blob2_registered_event
                .into_corresponding_certified_event_for_testing()
                .into(),
        )?;

        // Register and store the second blob
        let blob3_details = EncodedBlob::new(&blob3, config.clone());
        let blob3_registered_event =
            BlobRegistered::for_testing_with_random_object_id(*blob3_details.blob_id());
        events.send(blob3_registered_event.clone().into())?;
        store_at_shards(&blob3_details, &cluster, store_at_other_node_fn).await?;
        events.send(
            blob3_registered_event
                .into_corresponding_certified_event_for_testing()
                .into(),
        )?;

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
        assert_eq!(latest_cursor, Some(blob2_registered_event_id));

        Ok(())
    }

    #[tokio::test]
    async fn skip_storing_metadata_if_already_stored() -> TestResult {
        let (cluster, _, blob) =
            cluster_with_partially_stored_blob(&[&[0, 1, 2, 3]], BLOB, |_, _| true).await?;

        let is_newly_stored = cluster.nodes[0]
            .storage_node
            .store_metadata(blob.metadata.into_unverified(), UploadIntent::Immediate)
            .await?;

        assert!(!is_newly_stored);

        Ok(())
    }

    // Tests the basic `sync_shard` API.
    #[tokio::test]
    async fn sync_shard_node_api_success() -> TestResult {
        let (cluster, _, blob_detail) =
            cluster_with_initial_epoch_and_certified_blobs(&[&[0, 1], &[2, 3]], &[BLOB], 2, None)
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
        assert!(status.is_ok(), "Unexpected sync shard error: {status:?}");

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
            cluster_with_initial_epoch_and_certified_blobs(&[&[0, 1], &[2, 3]], &[BLOB], 1, None)
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
        assert!(status.is_ok(), "Unexpected sync shard error: {status:?}");

        let SyncShardResponse::V1(response) = status.unwrap();
        assert_eq!(response.len(), 0);

        Ok(())
    }

    // Tests unauthorized sync shard operation (requester is not a storage node in Walrus).
    #[tokio::test]
    async fn sync_shard_node_api_unauthorized_error() -> TestResult {
        let (cluster, _, _) =
            cluster_with_initial_epoch_and_certified_blobs(&[&[0, 1], &[2, 3]], &[BLOB], 1, None)
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
            cluster_with_initial_epoch_and_certified_blobs(&[&[0, 1], &[2, 3]], &[BLOB], 1, None)
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
        let (cluster, _, blob_detail) = cluster_with_initial_epoch_and_certified_blobs(
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
            .await
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
            Arc::unwrap_or_clone(sliver)
                .try_into()
                .expect("Sliver conversion failed.")
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
            .await
            .expect("Lock shard failed.");

        let assigned_sliver_pair = blob.assigned_sliver_pair(ShardIndex(0));
        assert!(matches!(
            cluster.nodes[0]
                .storage_node
                .store_sliver(
                    *blob.blob_id(),
                    assigned_sliver_pair.index(),
                    Sliver::Primary(assigned_sliver_pair.primary.clone()),
                    UploadIntent::Immediate,
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

        // Checks that the shard sync progress is reset.
        assert!(
            shard_storage_dst
                .get_last_synced_blob_id()
                .expect("getting last synced blob id should succeed")
                .is_none()
        );
        Ok(())
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
        shard_storage_dst
            .update_status_in_test(ShardStatus::None)
            .await?;

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

        // Checks that the shard sync progress is reset.
        assert!(
            shard_storage_dst
                .get_last_synced_blob_id()
                .expect("getting last synced blob id should succeed")
                .is_none()
        );

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
        shard_storage_dst
            .update_status_in_test(ShardStatus::None)
            .await?;

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

        // Checks that the shard sync progress is reset.
        assert!(
            shard_storage_dst
                .get_last_synced_blob_id()
                .expect("getting last synced blob id should succeed")
                .is_none()
        );

        Ok(())
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

        wait_until_events_processed_exact(&node, 1).await?;

        assert_eq!(
            node.as_ref()
                .inner
                .storage
                .shard_storage(ShardIndex(0))
                .await
                .expect("Shard storage should be created")
                .status()
                .await
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
                .await
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
            .returning(|_config, _node_cap_id, _wal_price| Ok(()));
        contract_service.expect_epoch_sync_done().never();
        contract_service
            .expect_fixed_system_parameters()
            .returning(|| FixedSystemParameters {
                n_shards: NonZeroU16::new(1000).expect("1000 > 0"),
                max_epochs_ahead: 200,
                epoch_duration: Duration::from_mins(10),
                epoch_zero_end: Utc::now() + Duration::from_mins(1),
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
        contract_service.expect_flush_cache().return_const(());
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

        wait_until_events_processed_exact(&node, 1).await?;

        Ok(())
    }

    async_param_test! {
        process_epoch_change_start_idempotent -> TestResult: [
            wait_for_shard_active: (true),
            do_not_wait_for_shard: (false),
        ]
    }
    async fn process_epoch_change_start_idempotent(wait_for_shard_active: bool) -> TestResult {
        walrus_test_utils::init_tracing();

        let (cluster, events, _blob_detail) =
            cluster_with_initial_epoch_and_certified_blobs(&[&[0, 1], &[2, 3]], &[BLOB], 2, None)
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
            wait_until_events_processed_exact(&cluster.nodes[1], processed_event_count + 1).await?;
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

        wait_until_events_processed_exact(&cluster.nodes[1], processed_event_count + 2).await?;

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
        walrus_test_utils::init_tracing();

        let (cluster, events, _blob_detail) =
            cluster_with_initial_epoch_and_certified_blobs(&[&[0, 1, 2, 3]], &[], 1, None).await?;

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
        walrus_test_utils::init_tracing();

        let (cluster, events, _blob_detail) =
            cluster_with_initial_epoch_and_certified_blobs(&[&[0, 1, 2, 3]], &[], 1, None).await?;

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

        wait_until_events_processed_exact(&cluster.nodes[0], 11).await?;
        Ok(())
    }

    // Tests that entering recovery mode cancels all existing blob syncs.
    #[tokio::test]
    async fn enter_recovery_mode_cancels_blob_syncs() -> TestResult {
        let shards: &[&[u16]] = &[&[1], &[0, 2, 3, 4, 5, 6]];

        // Create a cluster at epoch 1 without any blobs.
        let (cluster, _events) =
            cluster_at_epoch1_without_blobs_waiting_for_active_nodes(shards, None).await?;

        // Start syncs for multiple random blob ids. Since these blobs do not exist,
        // the syncs will run indefinitely if not cancelled.
        let blob_id_1 = random_blob_id();
        let blob_id_2 = random_blob_id();
        let blob_id_3 = random_blob_id();

        cluster.nodes[0]
            .storage_node
            .blob_sync_handler
            .start_sync(blob_id_1, 1, None)
            .await?;
        cluster.nodes[0]
            .storage_node
            .blob_sync_handler
            .start_sync(blob_id_2, 1, None)
            .await?;
        cluster.nodes[0]
            .storage_node
            .blob_sync_handler
            .start_sync(blob_id_3, 1, None)
            .await?;

        // Verify that 3 blob syncs are in progress.
        assert_eq!(
            cluster.nodes[0]
                .storage_node
                .blob_sync_handler
                .blob_sync_in_progress()
                .len(),
            3
        );

        // Enter recovery mode.
        cluster.nodes[0].storage_node.enter_recovery_mode().await?;

        // Verify that all blob syncs have been cancelled.
        assert_eq!(
            cluster.nodes[0]
                .storage_node
                .blob_sync_handler
                .blob_sync_in_progress()
                .len(),
            0
        );

        // Verify that the node status is set to RecoveryCatchUp.
        assert_eq!(
            cluster.nodes[0].storage_node.inner.storage.node_status()?,
            NodeStatus::RecoveryCatchUp
        );

        Ok(())
    }

    // Tests that `retrieve_multiple_decoding_symbols` correctly fetches decoding symbols
    // from storage nodes and successfully recovers the original sliver.
    //
    // This test:
    // 1. Fetches decoding symbols from multiple nodes for a target sliver
    // 2. Combines the symbols and reconstructs the sliver
    // 3. Verifies the recovered sliver matches the original
    async_param_test! {
        retrieve_multiple_decoding_symbols -> TestResult: [
            p0: (SliverIndex(0), SliverType::Primary),
            p1: (SliverIndex(1), SliverType::Primary),
            p2: (SliverIndex(2), SliverType::Primary),
            p3: (SliverIndex(3), SliverType::Primary),
            s0: (SliverIndex(0), SliverType::Secondary),
            s1: (SliverIndex(1), SliverType::Secondary),
            s2: (SliverIndex(2), SliverType::Secondary),
            s3: (SliverIndex(3), SliverType::Secondary),
            s4: (SliverIndex(4), SliverType::Secondary),
            s5: (SliverIndex(5), SliverType::Secondary),
            s6: (SliverIndex(6), SliverType::Secondary),
        ]
    }
    async fn retrieve_multiple_decoding_symbols(
        target_sliver_index: SliverIndex,
        target_type: SliverType,
    ) -> TestResult {
        walrus_test_utils::init_tracing();

        // blob_data is essentially a 30 bytes filled from 0 to 29 in each bytes.
        let blob_data: [u8; 30] = {
            let mut data = [0u8; 30];
            let mut i = 0;
            while i < 30 {
                data[i] = u8::try_from(i).expect("byte must be converted to u8");
                i += 1;
            }
            data
        };

        // Two nodes:
        // Node 0: [0, 2, 4 ,6 ,8]
        // Node 1: [1, 3, 5, 7, 9]
        let n_shards = 10;
        let shard_distribution: Vec<Vec<u16>> = (0..2)
            .map(|committee_idx| {
                (0..n_shards)
                    .filter(|shard_idx| shard_idx % 2 == committee_idx)
                    .collect()
            })
            .collect();
        let shard_refs: Vec<&[u16]> = shard_distribution.iter().map(|v| v.as_slice()).collect();

        let (cluster, _, blob_detail) =
            cluster_with_initial_epoch_and_certified_blobs(&shard_refs, &[&blob_data], 2, None)
                .await?;

        let blob_id = *blob_detail[0].blob_id();
        let n_shards_nonzero = NonZero::new(n_shards).unwrap();

        // Retrieve recovery symbols from both nodes
        let results_node_0 = cluster.nodes[0]
            .storage_node
            .retrieve_multiple_decoding_symbols(&blob_id, vec![target_sliver_index], target_type)
            .await?;
        let results_node_1 = cluster.nodes[1]
            .storage_node
            .retrieve_multiple_decoding_symbols(&blob_id, vec![target_sliver_index], target_type)
            .await?;

        // Combine all recovery symbols from both nodes and extract the appropriate type
        let all_results = results_node_0.into_iter().chain(results_node_1);

        match target_type {
            SliverType::Primary => {
                let recovery_symbols: Vec<_> = all_results
                    .flat_map(|(_, symbols)| symbols)
                    .map(|s| match s {
                        ByAxis::Primary(s) => s,
                        ByAxis::Secondary(_) => unreachable!("only primary symbols expected"),
                    })
                    .collect();

                let recovered_sliver = SliverData::try_recover_sliver_from_decoding_symbols(
                    recovery_symbols,
                    target_sliver_index,
                    blob_detail[0].metadata.metadata(),
                    &cluster.encoding_config(),
                )?;

                let target_pair_index =
                    target_sliver_index.to_pair_index::<Primary>(n_shards_nonzero);
                let expected_sliver = &blob_detail[0]
                    .pairs
                    .iter()
                    .find(|pair| pair.index() == target_pair_index)
                    .expect("sliver pair must exist")
                    .primary;

                assert_eq!(expected_sliver, &recovered_sliver);
            }
            SliverType::Secondary => {
                let recovery_symbols: Vec<_> = all_results
                    .flat_map(|(_, symbols)| symbols)
                    .map(|s| match s {
                        ByAxis::Primary(_) => unreachable!("only secondary symbols expected"),
                        ByAxis::Secondary(s) => s,
                    })
                    .collect();

                let recovered_sliver = SliverData::try_recover_sliver_from_decoding_symbols(
                    recovery_symbols,
                    target_sliver_index,
                    blob_detail[0].metadata.metadata(),
                    &cluster.encoding_config(),
                )?;

                let target_pair_index =
                    target_sliver_index.to_pair_index::<Secondary>(n_shards_nonzero);
                let expected_sliver = &blob_detail[0]
                    .pairs
                    .iter()
                    .find(|pair| pair.index() == target_pair_index)
                    .expect("sliver pair must exist")
                    .secondary;

                assert_eq!(expected_sliver, &recovered_sliver);
            }
        }

        Ok(())
    }

    // Tests that `retrieve_multiple_decoding_symbols` only returns symbols for source slivers.
    //
    // This test verifies that attempting to retrieve symbols for non-source (derived) sliver
    // indices returns an out-of-range error, ensuring the endpoint only serves symbols that
    // can reconstruct original slivers.
    #[tokio::test]
    async fn retrieve_multiple_decoding_symbols_only_support_source_slivers() -> TestResult {
        walrus_test_utils::init_tracing();

        // Two nodes:
        // Node 0: [0, 2, 4 ,6 ,8]
        // Node 1: [1, 3, 5, 7, 9]
        let n_shards = 10;
        let shard_distribution: Vec<Vec<u16>> = (0..2)
            .map(|committee_idx| {
                (0..n_shards)
                    .filter(|shard_idx| shard_idx % 2 == committee_idx)
                    .collect()
            })
            .collect();
        let shard_refs: Vec<&[u16]> = shard_distribution.iter().map(|v| v.as_slice()).collect();

        let (cluster, _, blob_detail) =
            cluster_with_initial_epoch_and_certified_blobs(&shard_refs, &[BLOB], 2, None).await?;

        let blob_id = blob_detail[0].blob_id();
        let storage_node = &cluster.nodes[0].storage_node;
        let (source_primary_symbol_count, source_secondary_symbol_count) =
            source_symbols_for_n_shards(NonZero::new(n_shards).unwrap());

        // Test that retrieving non-source primary slivers returns an error
        for target_sliver_index in source_primary_symbol_count.get()..=n_shards {
            let result = storage_node
                .retrieve_multiple_decoding_symbols(
                    blob_id,
                    vec![SliverIndex(target_sliver_index)],
                    SliverType::Primary,
                )
                .await;
            assert!(
                matches!(
                    result,
                    Err(ListSymbolsError::RetrieveDecodingSymbolOutOfRange(_))
                ),
                "Expected out-of-range error for primary sliver index {target_sliver_index}"
            );
        }

        // Test that retrieving non-source secondary slivers returns an error
        for target_sliver_index in source_secondary_symbol_count.get()..=n_shards {
            let result = storage_node
                .retrieve_multiple_decoding_symbols(
                    blob_id,
                    vec![SliverIndex(target_sliver_index)],
                    SliverType::Secondary,
                )
                .await;
            assert!(
                matches!(
                    result,
                    Err(ListSymbolsError::RetrieveDecodingSymbolOutOfRange(_))
                ),
                "Expected out-of-range error for secondary sliver index {target_sliver_index}"
            );
        }

        Ok(())
    }
}
