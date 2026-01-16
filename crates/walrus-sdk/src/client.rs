// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Low-level client for use when communicating directly with Walrus nodes.

use std::{
    collections::{HashMap, HashSet},
    fmt::{Debug, Display},
    marker::PhantomData,
    num::NonZeroU16,
    path::PathBuf,
    pin::{Pin, pin},
    sync::{
        Arc,
        OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use anyhow::{anyhow, bail};
use bimap::BiMap;
use futures::{
    Future,
    FutureExt,
    future::{Either, select},
};
use indicatif::MultiProgress;
use rand::{RngCore as _, rngs::ThreadRng};
use rayon::{iter::IntoParallelIterator, prelude::*};
use sui_types::base_types::ObjectID;
use tokio::{
    sync::{Mutex, Semaphore},
    task::JoinHandle,
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument as _, Level};
use walrus_core::{
    BlobId,
    DEFAULT_ENCODING,
    EncodingType,
    Epoch,
    ShardIndex,
    Sliver,
    SliverIndex,
    bft,
    encoding::{
        ConsistencyCheckType,
        DecodeError,
        EncodingAxis,
        EncodingConfig,
        EncodingConfigEnum,
        EncodingFactory as _,
        RequiredCount,
        SliverData,
        SliverPair,
    },
    ensure,
    messages::{BlobPersistenceType, ConfirmationCertificate, SignedStorageConfirmation},
    metadata::{BlobMetadataApi as _, VerifiedBlobMetadataWithId},
};
use walrus_storage_node_client::{UploadIntent, api::BlobStatus, error::NodeError};
use walrus_sui::{
    client::{CertifyAndExtendBlobResult, ExpirySelectionPolicy, ReadClient, SuiContractClient},
    types::{
        Blob,
        BlobEvent,
        StakedWal,
        move_structs::{BlobAttribute, BlobWithAttribute},
    },
};
use walrus_utils::{backoff::BackoffStrategy, metrics::Registry};

use crate::{
    active_committees::ActiveCommittees,
    client::{
        auto_tune::AutoTuneHandle,
        byte_range_read_client::ByteRangeReadClient,
        client_types::{
            BlobAwaitingUpload,
            BlobData,
            BlobPendingCertifyAndExtend,
            BlobWithStatus,
            EncodedBlob,
            RegisteredBlob,
            UnencodedBlob,
            WalrusStoreBlobFinished,
            WalrusStoreBlobMaybeFinished,
            WalrusStoreBlobUnfinished,
            WalrusStoreEncodedBlobApi as _,
        },
        communication::{NodeResult, NodeWriteCommunication, node::NodeIndex},
        quilt_client::QuiltClient,
        refresh::{CommitteesRefresherHandle, RequestKind, are_current_previous_different},
        resource::{PriceComputation, ResourceManager},
        responses::{BlobStoreResult, BlobStoreResultWithPath},
        upload_relay_client::UploadRelayClient,
    },
    config::CommunicationLimits,
    error::{ClientError, ClientErrorKind, ClientResult, StoreError},
    uploader::{DistributedUploader, RunOutput, TailHandling, UploaderEvent},
    utils::{
        self,
        CompletedReasonWeight,
        WeightedFutures,
        WeightedResult,
        styled_progress_bar,
        styled_spinner,
    },
};

pub mod byte_range_read_client;
pub mod client_types;
pub mod communication;
pub mod streaming;
pub use communication::NodeCommunicationFactory;
pub mod metrics;
pub mod quilt_client;
pub mod refresh;
pub mod resource;
pub mod responses;
pub mod store_args;
pub use store_args::{EncodingProgressEvent, StoreArgs};
pub mod upload_relay_client;

mod auto_tune;

pub use crate::{
    blocklist::Blocklist,
    config::{ClientCommunicationConfig, ClientConfig, default_configuration_paths},
};

/// The delay between retries when retrieving slivers.
#[allow(unused)]
const RETRIEVE_SLIVERS_RETRY_DELAY: Duration = Duration::from_millis(100);

/// Maximum number of re-upload rounds when retrying missing confirmations.
///
/// This is a best-effort retry to handle cases like cache eviction; callers may also be in a
/// degraded environment (e.g., crashed nodes), in which case retrying indefinitely would hang.
const MAX_MISSING_CONFIRMATION_RETRY_ROUNDS: usize = 2;

/// A set of slivers to be retrieved from Walrus.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SliverSelector<E: EncodingAxis> {
    indices_and_shards: BiMap<SliverIndex, ShardIndex>,
    _phantom: PhantomData<E>,
}

#[allow(unused)]
impl<E: EncodingAxis> SliverSelector<E> {
    /// Creates a new sliver selector.
    pub fn new(sliver_indices: &[SliverIndex], n_shards: NonZeroU16, blob_id: &BlobId) -> Self {
        let indices_and_shards = sliver_indices
            .iter()
            .map(|sliver_index| {
                let pair_index = sliver_index.to_pair_index::<E>(n_shards);
                let shard_index = pair_index.to_shard_index(n_shards, blob_id);
                (*sliver_index, shard_index)
            })
            .collect::<BiMap<_, _>>();

        Self {
            indices_and_shards,
            _phantom: PhantomData,
        }
    }

    /// Returns the number of slivers.
    pub fn len(&self) -> usize {
        self.indices_and_shards.len()
    }

    /// Returns true if no slivers are left.
    pub fn is_empty(&self) -> bool {
        self.indices_and_shards.is_empty()
    }

    /// Returns true if the shard contains one of the slivers.
    pub fn should_read_from_shard(&self, shard_index: &ShardIndex) -> bool {
        self.indices_and_shards.contains_right(shard_index)
    }

    /// Removes a sliver from the selector.
    ///
    /// Returns true if the sliver was removed.
    pub fn remove_sliver(&mut self, sliver_index: &SliverIndex) -> bool {
        self.indices_and_shards
            .remove_by_left(sliver_index)
            .is_some()
    }
}

type PendingUploadFuture<'a> =
    Pin<Box<dyn Future<Output = ClientResult<RunOutput<Vec<BlobId>, StoreError>>> + Send + 'a>>;

struct UploadOptions<'a> {
    tail_handling: TailHandling,
    target_nodes: Option<(Epoch, &'a [NodeIndex])>,
    upload_intent: UploadIntent,
    initial_completed_weight: Option<&'a HashMap<BlobId, usize>>,
    cancellation: Option<CancellationToken>,
}

struct SendBlobOptions<'a> {
    blob_persistence_type: &'a BlobPersistenceType,
    multi_pb: Option<&'a MultiProgress>,
    tail_handling: TailHandling,
    quorum_forwarder: Option<tokio::sync::mpsc::Sender<UploaderEvent>>,
    tail_handle_collector: Option<Arc<tokio::sync::Mutex<Vec<JoinHandle<()>>>>>,
    target_nodes: Option<(Epoch, &'a [NodeIndex])>,
    initial_completed_weight: Option<&'a HashMap<BlobId, usize>>,
}

/// Pending upload state carried through a single reserve+store execution.
#[derive(Debug, Default, Clone)]
struct PendingUploadContext {
    /// Nodes that reported successful pending uploads per blob.
    /// Nodes that did not report success (errors or missing results) are treated as missing.
    initial_success_nodes: Option<HashMap<BlobId, Vec<NodeIndex>>>,
}

struct PendingUploadHandle<'a> {
    cancel: CancellationToken,
    future: PendingUploadFuture<'a>,
}

struct ConfirmationCollection {
    confirmations: Vec<NodeResult<SignedStorageConfirmation, NodeError>>,
    certificate: ClientResult<ConfirmationCertificate>,
}

impl ConfirmationCollection {
    fn into_parts(
        self,
    ) -> (
        Vec<NodeResult<SignedStorageConfirmation, NodeError>>,
        ClientResult<ConfirmationCertificate>,
    ) {
        (self.confirmations, self.certificate)
    }
}

/// A client to communicate with Walrus shards and storage nodes.
#[derive(Debug, Clone)]
pub struct WalrusNodeClient<T> {
    config: ClientConfig,
    sui_client: T,
    communication_limits: CommunicationLimits,
    committees_handle: CommitteesRefresherHandle,
    // The `Arc` is used to share the encoding config with the `communication_factory` without
    // introducing lifetimes.
    encoding_config: Arc<EncodingConfig>,
    blocklist: Option<Blocklist>,
    communication_factory: NodeCommunicationFactory,
    max_blob_size: Option<u64>,
}

impl WalrusNodeClient<()> {
    /// Creates a new Walrus client without a Sui client.
    pub fn new(
        config: ClientConfig,
        committees_handle: CommitteesRefresherHandle,
    ) -> ClientResult<Self> {
        Self::new_inner(config, committees_handle, None, None)
    }

    /// Creates a new Walrus client without a Sui client.
    pub fn new_with_max_blob_size(
        config: ClientConfig,
        committees_handle: CommitteesRefresherHandle,
        max_blob_size: Option<u64>,
    ) -> ClientResult<Self> {
        Self::new_inner(config, committees_handle, None, max_blob_size)
    }

    /// Creates a new Walrus client without a Sui client, that records metrics to the provided
    /// registry.
    pub fn new_with_metrics(
        config: ClientConfig,
        committees_handle: CommitteesRefresherHandle,
        metrics_registry: Registry,
    ) -> ClientResult<Self> {
        Self::new_inner(config, committees_handle, Some(metrics_registry), None)
    }

    fn new_inner(
        config: ClientConfig,
        committees_handle: CommitteesRefresherHandle,
        metrics_registry: Option<Registry>,
        max_blob_size: Option<u64>,
    ) -> ClientResult<Self> {
        tracing::debug!(?config, "running client");

        let encoding_config = Arc::new(EncodingConfig::new(committees_handle.n_shards()));
        let communication_limits =
            CommunicationLimits::new(&config.communication_config, encoding_config.n_shards());

        Ok(Self {
            sui_client: (),
            encoding_config: encoding_config.clone(),
            communication_limits,
            committees_handle,
            blocklist: None,
            communication_factory: NodeCommunicationFactory::new(
                config.communication_config.clone(),
                encoding_config,
                metrics_registry,
            )?,
            config,
            max_blob_size,
        })
    }

    /// Converts `self` to a [`WalrusNodeClient<C>`] by adding the `sui_client`.
    pub fn with_client<C>(self, sui_client: C) -> WalrusNodeClient<C> {
        let Self {
            config,
            sui_client: _,
            committees_handle,
            encoding_config,
            communication_limits,
            blocklist,
            communication_factory: node_client_factory,
            max_blob_size,
        } = self;
        WalrusNodeClient::<C> {
            config,
            sui_client,
            committees_handle,
            encoding_config,
            communication_limits,
            blocklist,
            communication_factory: node_client_factory,
            max_blob_size,
        }
    }
}

impl<T: ReadClient> WalrusNodeClient<T> {
    /// Creates a new read client starting from a config file.
    pub fn new_read_client(
        config: ClientConfig,
        committees_handle: CommitteesRefresherHandle,
        sui_read_client: T,
    ) -> ClientResult<Self> {
        Ok(WalrusNodeClient::new(config, committees_handle)?.with_client(sui_read_client))
    }

    /// Creates a new read client starting from a config file with an optional maximum blob size.
    pub fn new_read_client_with_max_blob_size(
        config: ClientConfig,
        committees_handle: CommitteesRefresherHandle,
        sui_read_client: T,
        max_blob_size: Option<u64>,
    ) -> ClientResult<Self> {
        Ok(
            WalrusNodeClient::new_with_max_blob_size(config, committees_handle, max_blob_size)?
                .with_client(sui_read_client),
        )
    }

    /// Creates a new read client, and starts a committees refresher process in the background.
    ///
    /// This is useful when only one client is needed, and the refresher handle is not useful.
    pub async fn new_read_client_with_refresher(
        config: ClientConfig,
        sui_read_client: T,
    ) -> ClientResult<Self>
    where
        T: ReadClient + Clone + 'static,
    {
        let committees_handle = config
            .build_refresher_and_run(sui_read_client.clone())
            .await
            .map_err(|e| ClientError::from(ClientErrorKind::Other(e.into())))?;
        Ok(WalrusNodeClient::new(config, committees_handle)?.with_client(sui_read_client))
    }

    /// Sets the maximum blob size for the client for testing purposes.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn set_max_blob_size(&mut self, max_blob_size: Option<u64>) {
        self.max_blob_size = max_blob_size;
    }

    /// Reconstructs the blob by reading slivers from Walrus shards.
    ///
    /// The operation is retried if epoch it fails due to epoch change.
    pub async fn read_blob_retry_committees<U>(
        &self,
        blob_id: &BlobId,
        consistency_check: ConsistencyCheckType,
    ) -> ClientResult<Vec<u8>>
    where
        U: EncodingAxis,
        SliverData<U>: TryFrom<Sliver>,
    {
        self.retry_if_notified_epoch_change(|| {
            self.read_blob_with_consistency_check_type::<U>(blob_id, consistency_check)
        })
        .await
    }

    /// Reconstructs the blob by reading slivers from Walrus shards, performing the default
    /// consistency check.
    #[tracing::instrument(level = Level::ERROR, skip_all, fields(%blob_id))]
    pub async fn read_blob<U>(&self, blob_id: &BlobId) -> ClientResult<Vec<u8>>
    where
        U: EncodingAxis,
        SliverData<U>: TryFrom<Sliver>,
    {
        self.read_blob_internal::<U>(blob_id, None, ConsistencyCheckType::Default)
            .await
    }

    /// Reconstructs the blob by reading slivers from Walrus shards, performing the provided type of
    /// consistency check.
    #[tracing::instrument(level = Level::ERROR, skip_all, fields(%blob_id))]
    pub async fn read_blob_with_consistency_check_type<U>(
        &self,
        blob_id: &BlobId,
        consistency_check: ConsistencyCheckType,
    ) -> ClientResult<Vec<u8>>
    where
        U: EncodingAxis,
        SliverData<U>: TryFrom<Sliver>,
    {
        self.read_blob_internal(blob_id, None, consistency_check)
            .await
    }

    /// Reconstructs the blob by reading slivers from Walrus shards with the given status.
    #[tracing::instrument(level = Level::ERROR, skip_all, fields(%blob_id))]
    pub async fn read_blob_with_status<U>(
        &self,
        blob_id: &BlobId,
        blob_status: BlobStatus,
        consistency_check: ConsistencyCheckType,
    ) -> ClientResult<Vec<u8>>
    where
        U: EncodingAxis,
        SliverData<U>: TryFrom<Sliver>,
    {
        self.read_blob_internal(blob_id, Some(blob_status), consistency_check)
            .await
    }

    /// Tries to get the blob status if not provided.
    async fn try_get_blob_status(
        &self,
        blob_id: &BlobId,
        status: Option<BlobStatus>,
    ) -> ClientResult<BlobStatus> {
        let status = if let Some(status) = status {
            status
        } else {
            self.get_blob_status_with_retries(blob_id, &self.sui_client)
                .await?
        };

        if BlobStatus::Nonexistent == status {
            Err(ClientError::from(ClientErrorKind::BlobIdDoesNotExist))
        } else {
            Ok(status)
        }
    }

    /// If the status check fails with NoValidStatusReceived, continues with current epoch and
    /// known status.
    ///
    /// Otherwise, propagates the error.
    fn continue_on_no_valid_status_received(
        result: ClientResult<BlobStatus>,
        committees: &ActiveCommittees,
        known_status: Option<BlobStatus>,
    ) -> ClientResult<(Option<Epoch>, Option<BlobStatus>)> {
        match result {
            Ok(status) => Ok((status.initial_certified_epoch(), Some(status))),
            Err(e) if matches!(e.kind(), ClientErrorKind::NoValidStatusReceived) => {
                tracing::debug!(
                    "no valid status received; continuing with current epoch and known status"
                );
                Ok((Some(committees.epoch()), known_status))
            }
            Err(e) => Err(e),
        }
    }

    async fn get_blob_status_and_certified_epoch(
        &self,
        blob_id: &BlobId,
        known_status: Option<BlobStatus>,
    ) -> ClientResult<(Epoch, Option<BlobStatus>)> {
        let committees = self.get_committees().await?;
        let (epoch_to_be_read, blob_status) = if committees.is_change_in_progress() {
            Self::continue_on_no_valid_status_received(
                self.try_get_blob_status(blob_id, known_status).await,
                &committees,
                known_status,
            )?
        } else {
            // We are not during epoch change, we can read from the current epoch directly if we do
            // not have a blob status.
            (
                known_status
                    .map(|status| status.initial_certified_epoch())
                    .unwrap_or_else(|| Some(committees.epoch())),
                known_status,
            )
        };

        // Return an error if the blob is not registered.
        if matches!(
            blob_status,
            Some(BlobStatus::Nonexistent | BlobStatus::Invalid { .. })
        ) {
            return Err(ClientError::from(ClientErrorKind::BlobIdDoesNotExist));
        }

        // Read from the epoch of certification, or the current epoch if so far we have not been
        // able to get the certified epoch. let current_epoch = committees.epoch();
        let current_epoch = committees.epoch();
        let epoch_to_be_read = epoch_to_be_read.unwrap_or(current_epoch);

        // Return an error if the committee is behind.
        if epoch_to_be_read > current_epoch {
            return Err(ClientError::from(ClientErrorKind::BehindCurrentEpoch {
                client_epoch: current_epoch,
                // The epooch_to_be_read can be ahead of the current epoch only if it is a
                // certified epoch.
                certified_epoch: epoch_to_be_read,
            }));
        }

        Ok((epoch_to_be_read, blob_status))
    }

    /// Internal method to handle the common logic for reading blobs.
    async fn read_blob_internal<U>(
        &self,
        blob_id: &BlobId,
        blob_status: Option<BlobStatus>,
        consistency_check: ConsistencyCheckType,
    ) -> ClientResult<Vec<u8>>
    where
        U: EncodingAxis,
        SliverData<U>: TryFrom<Sliver>,
    {
        tracing::debug!("starting to read blob");

        self.check_blob_is_blocked(blob_id)?;

        let (certified_epoch, blob_status) = self
            .get_blob_status_and_certified_epoch(blob_id, blob_status)
            .await?;

        // Execute the status request and the metadata/sliver request concurrently.
        //
        // If the status request fails, the metadata/sliver request will be cancelled. If the status
        // request succeeds, the metadata/sliver request will be executed to completion. If we
        // already have a blob status, the `get_status_fn` immediately returns Ok.
        //
        // In the unlikely event that the status request takes longer than reading the
        // metadata/slivers, the status request will be dropped.
        match select(
            pin!(self.try_get_blob_status(blob_id, blob_status)),
            pin!(self.read_metadata_and_slivers_and_reconstruct_blob::<U>(
                certified_epoch,
                blob_id,
                consistency_check
            )),
        )
        .await
        {
            Either::Left((status_result, read_future)) => {
                Self::continue_on_no_valid_status_received(
                    status_result,
                    self.get_committees().await?.as_ref(),
                    blob_status,
                )?;
                read_future.await
            }
            Either::Right((read_result, _status_future)) => read_result,
        }
    }

    async fn read_metadata_and_slivers_and_reconstruct_blob<U>(
        &self,
        certified_epoch: Epoch,
        blob_id: &BlobId,
        consistency_check: ConsistencyCheckType,
    ) -> ClientResult<Vec<u8>>
    where
        U: EncodingAxis,
        SliverData<U>: TryFrom<Sliver>,
    {
        let metadata = self.retrieve_metadata(certified_epoch, blob_id).await?;
        self.check_read_data_size(metadata.metadata().unencoded_length())?;
        let blob = self
            .request_slivers_and_decode::<U>(certified_epoch, &metadata, consistency_check)
            .await?;

        Ok(blob)
    }

    /// Retries the given function if the client gets notified that the committees have changed.
    ///
    /// This function should not be used to retry function `func` that cannot be interrupted at
    /// arbitrary await points. Most importantly, functions that use the wallet to sign and execute
    /// transactions on Sui.
    async fn retry_if_notified_epoch_change<F, R, Fut>(&self, func: F) -> ClientResult<R>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = ClientResult<R>>,
    {
        let func_check_notify = || self.await_while_checking_notification(func());
        self.retry_if_error_epoch_change(func_check_notify).await
    }

    /// Retries the given function if the function returns with an error that may be related to
    /// epoch change.
    async fn retry_if_error_epoch_change<F, R, Fut>(&self, func: F) -> ClientResult<R>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = ClientResult<R>>,
    {
        let mut attempts = 0;

        let mut backoff = self
            .config
            .communication_config
            .committee_change_backoff
            .get_strategy(ThreadRng::default().next_u64());

        // Retry the given function N-1 times; if it does not succeed after N-1 times, then the
        // last try is made outside the loop.
        while let Some(delay) = backoff.next_delay() {
            match func().await {
                Ok(result) => return Ok(result),
                Err(error) => {
                    if error.may_be_caused_by_epoch_change() {
                        tracing::info!(
                            %error,
                            "operation aborted; maybe because of epoch change; retrying"
                        );
                        if !matches!(*error.kind(), ClientErrorKind::CommitteeChangeNotified) {
                            // If the error is CommitteeChangeNotified, we do not need to refresh
                            // the committees.
                            self.force_refresh_committees().await?;
                        }
                    } else {
                        tracing::debug!(%error, "operation failed; not retrying");
                        return Err(error);
                    }
                }
            };

            attempts += 1;
            tracing::debug!(
                ?attempts,
                ?delay,
                "a potential committee change detected; retrying after a delay",
            );
            tokio::time::sleep(delay).await;
        }

        // The last try.
        tracing::warn!(?attempts, "retries exhausted; conduct one last try");
        func().await
    }

    /// Fetch a blob with its object ID.
    pub async fn get_blob_by_object_id(
        &self,
        blob_object_id: &ObjectID,
    ) -> ClientResult<BlobWithAttribute> {
        self.sui_client
            .get_blob_by_object_id(blob_object_id)
            .await
            .map_err(|e| {
                if e.to_string()
                    .contains("response does not contain object data")
                {
                    ClientError::from(ClientErrorKind::BlobIdDoesNotExist)
                } else {
                    ClientError::other(e)
                }
            })
    }

    /// Executes the function while also awaiiting on the change notification.
    ///
    /// Returns a [`ClientErrorKind::CommitteeChangeNotified`] error if the client is notified that
    /// the committee has changed.
    async fn await_while_checking_notification<Fut, R>(&self, future: Fut) -> ClientResult<R>
    where
        Fut: Future<Output = ClientResult<R>>,
    {
        tokio::select! {
            _ = self.committees_handle.change_notified() => {
                Err(ClientError::from(ClientErrorKind::CommitteeChangeNotified))
            },
            result = future => result,
        }
    }

    async fn retrieve_slivers_retry_committees<E: EncodingAxis>(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        sliver_indices: &[SliverIndex],
        certified_epoch: Epoch,
        max_attempts: usize,
        timeout_duration: Duration,
    ) -> Result<Vec<SliverData<E>>, ClientError>
    where
        SliverData<E>: TryFrom<Sliver>,
    {
        self.retry_if_error_epoch_change(|| {
            self.retrieve_slivers_with_retry(
                metadata,
                sliver_indices,
                certified_epoch,
                max_attempts,
                timeout_duration,
            )
        })
        .await
    }

    /// Retrieves slivers with retry logic, only requesting missing slivers in subsequent attempts.
    ///
    /// Only unique slivers are retrieved, duplicates sliver indices are ignored.
    /// This function will keep retrying until all requested slivers are received or the maximum
    /// number of attempts is reached or the timeout is reached.
    ///
    /// Returned slivers may not be in the same order as the sliver_indices.
    async fn retrieve_slivers_with_retry<E: EncodingAxis>(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        sliver_indices: &[SliverIndex],
        certified_epoch: Epoch,
        max_attempts: usize,
        timeout_duration: Duration,
    ) -> Result<Vec<SliverData<E>>, ClientError>
    where
        SliverData<E>: TryFrom<walrus_core::Sliver>,
    {
        if metadata.metadata().unencoded_length() == 0 {
            return Ok(Vec::new());
        }

        // Get the sliver size for the given encoding type.
        let sliver_size = u64::from(
            self.encoding_config()
                .get_for_type(metadata.metadata().encoding_type())
                .sliver_size_for_blob::<E>(metadata.metadata().unencoded_length())
                .map_err(|e| ClientError::from(ClientErrorKind::Other(e.into())))?
                .get(),
        );
        let read_total_sliver_size = sliver_size
            .checked_mul(u64::try_from(sliver_indices.len()).unwrap_or(u64::MAX))
            .ok_or(ClientError::from(ClientErrorKind::BlobTooLarge(u64::MAX)))?;
        self.check_read_data_size(read_total_sliver_size)?;

        let mut sliver_selector =
            SliverSelector::<E>::new(sliver_indices, metadata.n_shards(), metadata.blob_id());
        let mut attempts = 0;
        let num_unique_slivers = sliver_selector.len();
        let mut all_slivers = Vec::with_capacity(num_unique_slivers);
        let start_time = Instant::now();
        let mut last_error: Option<ClientError> = None;

        while !sliver_selector.is_empty() {
            // Check if we've exceeded the timeout or the max retries.
            if start_time.elapsed() > timeout_duration {
                tracing::debug!(
                    "timeout reached after {} while retrieving slivers",
                    humantime::Duration::from(timeout_duration)
                );
                break;
            }
            if attempts > max_attempts {
                tracing::debug!("max attempts ({}) reached", max_attempts);
                break;
            }

            tracing::debug!(?sliver_selector, "retrieving slivers");
            match self
                .retrieve_slivers(
                    metadata,
                    &sliver_selector,
                    certified_epoch,
                    timeout_duration - start_time.elapsed(),
                )
                .await
            {
                Ok(new_slivers) => {
                    // Track which indices we've successfully retrieved.
                    for sliver in new_slivers {
                        sliver_selector.remove_sliver(&sliver.index);
                        all_slivers.push(sliver);
                    }
                }
                Err(error) => {
                    // TODO(WAL-685): if the error is not retriable, return the error immediately.
                    tracing::warn!(?error, "error retrieving slivers");
                    last_error = Some(error);
                }
            }

            attempts += 1;
            if all_slivers.len() != num_unique_slivers {
                tokio::time::sleep(RETRIEVE_SLIVERS_RETRY_DELAY).await;
            }
        }

        if all_slivers.len() != num_unique_slivers {
            Err(ClientError::from(ClientErrorKind::Other(
                format!(
                    "failed to retrieve some slivers ({}/{} successful): {:?}",
                    all_slivers.len(),
                    num_unique_slivers,
                    last_error
                )
                .into(),
            )))
        } else {
            Ok(all_slivers)
        }
    }

    /// Retrieves specific slivers from storage nodes based on their indices.
    #[tracing::instrument(level = Level::ERROR, skip_all)]
    async fn retrieve_slivers<E: EncodingAxis>(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        sliver_selector: &SliverSelector<E>,
        certified_epoch: Epoch,
        timeout_duration: Duration,
    ) -> ClientResult<Vec<SliverData<E>>>
    where
        SliverData<E>: TryFrom<Sliver>,
    {
        let blob_id = metadata.blob_id();
        tracing::info!("starting to retrieve slivers {:?}", sliver_selector);
        self.check_blob_is_blocked(blob_id)?;

        // Create a progress bar to track the progress of the sliver retrieval.
        let progress_bar: indicatif::ProgressBar =
            styled_progress_bar(sliver_selector.len() as u64);
        progress_bar.set_message(format!("requesting {} slivers", sliver_selector.len()));

        let committees = self.get_committees().await?;
        let comms = self
            .communication_factory
            .node_read_communications(&committees, certified_epoch)?;

        // Create requests to get all required slivers.
        let futures = comms.iter().flat_map(|n| {
            n.node
                .shard_ids
                .iter()
                .cloned()
                .filter(|&s| sliver_selector.should_read_from_shard(&s))
                .map(|s| {
                    n.retrieve_verified_sliver::<E>(metadata, s)
                        .instrument(n.span.clone())
                        .inspect({
                            let value = progress_bar.clone();
                            move |result| {
                                if result.is_ok() {
                                    value.inc(1);
                                }
                            }
                        })
                })
        });

        let mut requests = WeightedFutures::new(futures);

        // Execute all requests with appropriate concurrency limits.
        // NB: This is a "best effort" retrieval: we get as many slivers as possible within the
        // timeout. Partial results are returned to the caller, which has retry logic to fetch
        // missing slivers.
        let completed_reason = requests
            .execute_time(
                timeout_duration,
                self.communication_limits
                    .max_concurrent_sliver_reads_for_blob_size(
                        metadata.metadata().unencoded_length(),
                        &self.encoding_config,
                        metadata.metadata().encoding_type(),
                    ),
            )
            .await;

        tracing::debug!(%completed_reason, "finished executing sliver retrieval requests");

        progress_bar.finish_with_message("slivers received");

        let slivers = requests
            .take_results()
            .into_iter()
            .filter_map(|NodeResult { result, node, .. }| {
                result
                    .map_err(|error| {
                        tracing::debug!(%node, %error, "retrieving sliver failed");
                        error
                    })
                    .ok()
            })
            .collect::<Vec<_>>();

        Ok(slivers)
    }

    /// Encodes the blob and sends metadata and slivers to the selected nodes.
    ///
    /// The function optionally receives a blob ID as input, to check that the blob ID resulting
    /// from the encoding matches the expected blob ID. This operation is intended for backfills,
    /// and it will not request a certificate from the storage nodes.
    ///
    /// Returns a vector containing the results of the store operations on each node.
    pub async fn backfill_blob_to_nodes(
        &self,
        blob: Vec<u8>,
        node_ids: impl IntoIterator<Item = ObjectID>,
        encoding_type: EncodingType,
        expected_blob_id: Option<BlobId>,
    ) -> ClientResult<Vec<NodeResult<(), StoreError>>> {
        tracing::info!(
            ?expected_blob_id,
            blob_size = blob.len(),
            "attempting to backfill blob to nodes"
        );
        let committees = self.get_committees().await?;
        let (pairs, metadata) = self
            .encoding_config
            .get_for_type(encoding_type)
            .encode_with_metadata(blob)
            .map_err(ClientError::other)?;

        if let Some(expected) = expected_blob_id {
            ensure!(
                expected == *metadata.blob_id(),
                ClientError::store_blob_internal(format!(
                    "the expected blob ID ({}) does not match the encoded blob ID ({})",
                    expected,
                    metadata.blob_id()
                ))
            )
        }

        let mut pairs_per_node = self.pairs_per_node(metadata.blob_id(), &pairs, &committees);

        let (sliver_write_semaphore, auto_tune_handle) = self.build_sliver_write_throttle(
            metadata.metadata().unencoded_length(),
            metadata.metadata().encoding_type(),
        );
        let comms = self.communication_factory.node_write_communications_by_id(
            &committees,
            sliver_write_semaphore,
            auto_tune_handle,
            node_ids,
        )?;

        let store_operations: Vec<_> = comms
            .iter()
            .map(|nc| {
                nc.store_metadata_and_pairs_without_confirmation(
                    &metadata,
                    pairs_per_node
                        .remove(&nc.node_index)
                        .expect("there are shards for each node"),
                    UploadIntent::Immediate,
                )
            })
            .collect();

        // Await on all store operations concurrently.
        let results = futures::future::join_all(store_operations).await;

        Ok(results)
    }

    /// Checks if the data size is within the maximum allowed size.
    fn check_read_data_size(&self, data_size: u64) -> ClientResult<()> {
        if let Some(max_blob_size) = self.max_blob_size
            && data_size > max_blob_size
        {
            return Err(ClientError::from(ClientErrorKind::BlobTooLarge(
                max_blob_size,
            )));
        }
        Ok(())
    }
}

impl WalrusNodeClient<SuiContractClient> {
    /// Creates a new client starting from a config file.
    pub fn new_contract_client(
        config: ClientConfig,
        committees_handle: CommitteesRefresherHandle,
        sui_client: SuiContractClient,
    ) -> ClientResult<Self> {
        Ok(WalrusNodeClient::new(config, committees_handle)?.with_client(sui_client))
    }

    /// Creates a new client, and starts a committees refresher process in the background.
    ///
    /// This is useful when only one client is needed, and the refresher handle is not useful.
    pub async fn new_contract_client_with_refresher(
        config: ClientConfig,
        sui_client: SuiContractClient,
    ) -> ClientResult<Self> {
        let committees_handle = config
            .build_refresher_and_run(sui_client.read_client().clone())
            .await
            .map_err(|e| ClientError::from(ClientErrorKind::Other(e.into())))?;
        Ok(WalrusNodeClient::new(config, committees_handle)?.with_client(sui_client))
    }

    /// Stores the blobs on Walrus, reserving space or extending registered blobs, if necessary.
    ///
    /// Returns a [`ClientErrorKind::CommitteeChangeNotified`] error if, during the registration or
    /// store operations, the client is notified that the committee has changed.
    // TODO(WAL-600): This function is very long and should be split into smaller functions.
    #[tracing::instrument(level = Level::DEBUG, skip_all, fields(count = encoded_blobs.len()))]
    async fn reserve_and_store_encoded_blobs(
        &self,
        encoded_blobs: Vec<WalrusStoreBlobUnfinished<EncodedBlob>>,
        store_args: &StoreArgs,
    ) -> ClientResult<Vec<WalrusStoreBlobFinished>> {
        let blobs_count = encoded_blobs.len();
        if blobs_count == 0 {
            tracing::debug!("no blobs provided");
            return Ok(vec![]);
        }

        let store_args = self.pending_upload_store_args(store_args);

        tracing::info!(
            "writing {blobs_count} blob{} to Walrus",
            if blobs_count == 1 { "" } else { "s" }
        );
        let status_start_timer = Instant::now();
        let committees = self.get_committees().await?;
        tracing::info!(duration = ?status_start_timer.elapsed(), "finished getting committees");

        // Retrieve the blob status, checking if the committee has changed in the meantime.
        // This operation can be safely interrupted as it does not require a wallet.
        let encoded_blobs_with_status = self
            .await_while_checking_notification(self.get_blob_statuses(encoded_blobs))
            .await?;

        debug_assert_eq!(
            encoded_blobs_with_status.len(),
            blobs_count,
            "the number of blob statuses and the number of blobs to store must be the same",
        );
        let status_timer_duration = status_start_timer.elapsed();
        tracing::info!(
            duration = ?status_timer_duration,
            "retrieved blob statuses",
        );
        store_args.maybe_observe_checking_blob_status(status_timer_duration);

        let pending_blobs = self.pending_upload_candidates(&encoded_blobs_with_status, &store_args);

        let store_op_timer = Instant::now();
        let (registered_blobs, pending_upload_result) = self
            .register_with_pending_uploads(
                encoded_blobs_with_status,
                &store_args,
                &committees,
                &pending_blobs,
            )
            .await?;
        debug_assert_eq!(
            registered_blobs.len(),
            blobs_count,
            "the number of registered blobs and the number of blobs to store must be the same",
        );

        let store_op_duration = store_op_timer.elapsed();
        tracing::info!(duration = ?store_op_duration, "finished registering blobs");
        tracing::debug!(?registered_blobs);
        store_args.maybe_observe_store_operation(store_op_duration);

        let pending_context = self.apply_pending_upload_outcome(pending_upload_result);

        let (mut final_result, blobs_awaiting_upload, mut blobs_pending_certify_and_extend) =
            Self::partition_registered_blobs(registered_blobs, blobs_count);
        let num_to_be_certified = blobs_awaiting_upload.len();

        // Check if the committee has changed while registering the blobs.
        if are_current_previous_different(
            committees.as_ref(),
            self.get_committees().await?.as_ref(),
        ) {
            tracing::warn!("committees have changed while registering blobs");
            return Err(ClientError::from(ClientErrorKind::CommitteeChangeNotified));
        }

        // Get blob certificates for to_be_certified blobs.
        let mut blobs_with_certificates = Vec::with_capacity(blobs_awaiting_upload.len());
        if !blobs_awaiting_upload.is_empty() {
            let get_certificates_timer = Instant::now();
            // Get the blob certificates, possibly storing slivers, while checking if the committee
            // has changed in the meantime.
            // This operation can be safely interrupted as it does not require a wallet.
            blobs_with_certificates = self
                .await_while_checking_notification(self.get_all_blob_certificates(
                    blobs_awaiting_upload,
                    &store_args,
                    &pending_context,
                ))
                .await?;

            debug_assert_eq!(blobs_with_certificates.len(), num_to_be_certified);
            let get_certificates_duration = get_certificates_timer.elapsed();

            tracing::debug!(
                duration = ?get_certificates_duration,
                "fetched certificates for {} blobs",
                blobs_with_certificates.len()
            );
            store_args.maybe_observe_get_certificates(get_certificates_duration);
        }

        // Move completed blobs to final_result and keep only non-completed ones.
        let (to_be_certified, completed_blobs) =
            client_types::partition_unfinished_finished(blobs_with_certificates);
        final_result.extend(completed_blobs);
        blobs_pending_certify_and_extend.extend(to_be_certified);

        // Certify and extend the blobs on Sui.
        final_result.extend(
            self.certify_and_extend_blobs(blobs_pending_certify_and_extend, &store_args)
                .await?,
        );

        Ok(final_result)
    }

    fn pending_upload_store_args(&self, store_args: &StoreArgs) -> StoreArgs {
        let mut store_args = store_args.clone();
        if self.config.communication_config.pending_uploads_enabled {
            store_args.store_optimizations.optimistic_uploads = true;
        }
        store_args
    }

    fn pending_upload_candidates(
        &self,
        encoded_blobs_with_status: &[WalrusStoreBlobMaybeFinished<BlobWithStatus>],
        store_args: &StoreArgs,
    ) -> Vec<(VerifiedBlobMetadataWithId, Arc<Vec<SliverPair>>)> {
        let pending_upload_max_blob_bytes = self.pending_upload_max_blob_bytes();
        let pending_uploads_enabled = self.config.communication_config.pending_uploads_enabled;

        let pending: Vec<_> = encoded_blobs_with_status
            .iter()
            .filter_map(|blob| {
                blob.pending_upload_payload(
                    pending_uploads_enabled,
                    store_args,
                    pending_upload_max_blob_bytes,
                )
            })
            .collect();

        tracing::debug!(
            pending_candidates = pending.len(),
            pending_enabled = pending_uploads_enabled,
            pending_uploads = store_args.store_optimizations.pending_uploads_enabled(),
            max_blob_bytes = pending_upload_max_blob_bytes,
            "computed pending upload candidates",
        );

        pending
    }

    fn start_pending_uploads<'a>(
        &'a self,
        pending_blobs: &'a [(VerifiedBlobMetadataWithId, Arc<Vec<SliverPair>>)],
        store_args: &StoreArgs,
    ) -> Option<PendingUploadHandle<'a>> {
        if pending_blobs.is_empty() {
            return None;
        }

        // We don't need to receive the results of the pending uploads,
        // so we can ignore the receiver. The progress bar is only updated during the upload
        // with immediate intent later.
        tracing::info!(
            pending_blobs = pending_blobs.len(),
            "starting pending upload task"
        );
        let (tx, mut rx) = tokio::sync::mpsc::channel(pending_blobs.len().max(1));
        // Drain progress events so pending uploads don't emit SendError warnings.
        tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let cancel = CancellationToken::new();
        let future = Box::pin(self.distributed_upload_without_confirmation(
            pending_blobs,
            tx,
            UploadOptions {
                tail_handling: store_args.tail_handling,
                target_nodes: None,
                upload_intent: UploadIntent::Pending,
                initial_completed_weight: None,
                cancellation: Some(cancel.clone()),
            },
        ));

        Some(PendingUploadHandle { cancel, future })
    }

    async fn register_with_pending_uploads(
        &self,
        encoded_blobs_with_status: Vec<WalrusStoreBlobMaybeFinished<BlobWithStatus>>,
        store_args: &StoreArgs,
        committees: &ActiveCommittees,
        pending_blobs: &[(VerifiedBlobMetadataWithId, Arc<Vec<SliverPair>>)],
    ) -> ClientResult<(
        Vec<WalrusStoreBlobMaybeFinished<RegisteredBlob>>,
        Option<RunOutput<Vec<BlobId>, StoreError>>,
    )> {
        let mut pending_upload_handle = self.start_pending_uploads(pending_blobs, store_args);

        // Register blobs if they are not registered, and get the store operations.
        let resource_manager = self.resource_manager(committees);
        let registration_fut = resource_manager.register_walrus_store_blobs(
            encoded_blobs_with_status,
            store_args.epochs_ahead,
            store_args.persistence,
            store_args.store_optimizations,
        );

        if let Some(ref mut pending) = pending_upload_handle {
            tokio::pin!(registration_fut);
            let mut pending_upload_result = None;
            let registered_blobs = tokio::select! {
                reg = &mut registration_fut => {
                    // Cancel pending uploads once registration completes; if they already finished
                    // we'll still await their output below.
                    pending.cancel.cancel();
                    reg
                }
                pending_res = pending.future.as_mut() => {
                    // Pending uploads may finish first; record their output before awaiting
                    // registration.
                    pending_upload_result = Some(pending_res?);
                    registration_fut.await
                }
            }?;

            if pending_upload_result.is_none() {
                // Registration finished first; still await the pending task to capture any
                // completed work before or during cancellation.
                pending.cancel.cancel();
                match pending.future.as_mut().await {
                    Ok(res) => pending_upload_result = Some(res),
                    Err(err) => tracing::debug!(?err, "pending upload task failed or cancelled"),
                }
            }

            Ok((registered_blobs, pending_upload_result))
        } else {
            Ok((registration_fut.await?, None))
        }
    }

    fn apply_pending_upload_outcome(
        &self,
        mut pending_upload_result: Option<RunOutput<Vec<BlobId>, StoreError>>,
    ) -> PendingUploadContext {
        let mut context = PendingUploadContext::default();
        // Cancel any in-flight pending tail. Tail handle is returned during the upload with
        // immediate intent later.
        if let Some(ref mut pending_output) = pending_upload_result
            && let Some(handle) = pending_output.tail_handle.take()
        {
            handle.abort();
        }

        if let Some(pending_output) = pending_upload_result {
            let success_nodes = Self::successful_nodes_by_blob(&pending_output.results);
            if !success_nodes.is_empty() {
                context.initial_success_nodes = Some(success_nodes);
            }
        }

        context
    }

    fn partition_registered_blobs(
        registered_blobs: Vec<WalrusStoreBlobMaybeFinished<RegisteredBlob>>,
        blobs_count: usize,
    ) -> (
        Vec<WalrusStoreBlobFinished>,
        Vec<WalrusStoreBlobUnfinished<BlobAwaitingUpload>>,
        Vec<WalrusStoreBlobUnfinished<BlobPendingCertifyAndExtend>>,
    ) {
        // Classify the blobs into to_be_certified and to_be_extended, and move completed blobs to
        // final_result.
        let mut final_result: Vec<WalrusStoreBlobFinished> = Vec::with_capacity(blobs_count);
        let mut blobs_awaiting_upload = Vec::new();
        let mut blobs_pending_certify_and_extend = Vec::new();

        for registered_blob in registered_blobs {
            match registered_blob.try_finish() {
                Ok(blob) => final_result.push(blob),
                Err(blob) => match blob.map_either(|blob| blob.classify(), "classify") {
                    utils::Either::Left(blob_to_be_certified) => {
                        blobs_awaiting_upload.push(blob_to_be_certified)
                    }
                    utils::Either::Right(blob_to_be_extended) => {
                        blobs_pending_certify_and_extend.push(blob_to_be_extended)
                    }
                },
            }
        }
        let num_to_be_certified = blobs_awaiting_upload.len();
        debug_assert_eq!(
            num_to_be_certified + blobs_pending_certify_and_extend.len() + final_result.len(),
            blobs_count,
            "the sum of the number of blobs to certify, extend, and store must be the original \
            number of blobs"
        );

        (
            final_result,
            blobs_awaiting_upload,
            blobs_pending_certify_and_extend,
        )
    }

    /// Fetches the status of each blob.
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    async fn get_blob_statuses(
        &self,
        encoded_blobs: Vec<WalrusStoreBlobUnfinished<EncodedBlob>>,
    ) -> ClientResult<Vec<WalrusStoreBlobMaybeFinished<BlobWithStatus>>> {
        futures::future::try_join_all(encoded_blobs.into_iter().map(|encoded_blob| async move {
            let blob_id = encoded_blob.state.blob_id();
            if let Err(e) = self.check_blob_is_blocked(&blob_id) {
                return Ok(encoded_blob
                    .into_maybe_finished()
                    .fail_with(e, "check_blob_is_blocked"));
            }
            let status_result = self
                .get_blob_status_with_retries(&blob_id, &self.sui_client)
                .await;
            encoded_blob
                .into_maybe_finished()
                .map(|blob| blob.with_status(status_result), "get_blob_status")
        }))
        .await
    }

    /// Fetches the certificates for all the blobs, and returns a vector of
    /// WalrusStoreBlob::WithCertificate or WalrusStoreBlob::Error.
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    async fn get_all_blob_certificates(
        &self,
        blobs_to_be_certified: Vec<WalrusStoreBlobUnfinished<BlobAwaitingUpload>>,
        store_args: &StoreArgs,
        pending_context: &PendingUploadContext,
    ) -> ClientResult<Vec<WalrusStoreBlobMaybeFinished<BlobPendingCertifyAndExtend>>> {
        if blobs_to_be_certified.is_empty() {
            return Ok(vec![]);
        }

        let get_cert_timer = Instant::now();

        let multi_pb = Arc::new(MultiProgress::new());
        let blobs = futures::future::try_join_all(blobs_to_be_certified.into_iter().map(
            |blob_to_be_certified| {
                let multi_pb = Arc::clone(&multi_pb);
                async move {
                    self.get_certificate(
                        blob_to_be_certified,
                        multi_pb.as_ref(),
                        store_args,
                        pending_context,
                    )
                    .await
                }
            },
        ))
        .await?;

        if !walrus_utils::is_internal_run() {
            let certificate_count = blobs.iter().filter(|blob| !blob.is_finished()).count();
            tracing::info!(
                duration = ?get_cert_timer.elapsed(),
                "obtained {certificate_count} blob certificate{}",
                if certificate_count == 1 { "" } else { "s" },
            );
        }

        Ok(blobs)
    }

    async fn get_certificate(
        &self,
        blob_to_be_certified: WalrusStoreBlobUnfinished<BlobAwaitingUpload>,
        multi_pb: &MultiProgress,
        store_args: &StoreArgs,
        pending_context: &PendingUploadContext,
    ) -> ClientResult<WalrusStoreBlobMaybeFinished<BlobPendingCertifyAndExtend>> {
        let committees = self.get_committees().await?;

        let BlobAwaitingUpload {
            encoded_blob,
            status: blob_status,
            blob_object,
            operation,
            ..
        } = &blob_to_be_certified.state;

        let certificate_result = match blob_status.initial_certified_epoch() {
            Some(certified_epoch) if !committees.is_change_in_progress() => {
                // If the blob is already certified on chain and there is no committee change in
                // progress, all nodes already have the slivers.
                self.get_certificate_standalone(
                    &blob_object.blob_id,
                    certified_epoch,
                    &blob_object.blob_persistence_type(),
                )
                .await
            }
            _ => {
                // If the blob is not certified, we need to store the slivers. Also, during
                // epoch change we may need to store the slivers again for an already certified
                // blob, as the current committee may not have synced them yet.

                if (operation.is_registration() || operation.is_reuse_storage())
                    && !blob_status.is_registered()
                {
                    tracing::debug!(
                        delay=?self.config.communication_config.registration_delay,
                        "waiting to ensure that all storage nodes have seen the registration"
                    );
                    tokio::time::sleep(self.config.communication_config.registration_delay).await;
                }

                let certify_start_timer = Instant::now();
                let result: Result<_, ClientError> = match &encoded_blob.data {
                    BlobData::SliverPairs(sliver_pairs) => {
                        self.upload_and_collect_certificate(
                            &encoded_blob.metadata,
                            sliver_pairs.clone(),
                            &blob_object.blob_persistence_type(),
                            Some(multi_pb),
                            store_args,
                            pending_context,
                        )
                        .await
                    }
                    BlobData::BlobForUploadRelay(blob, upload_relay_client) => upload_relay_client
                        .send_blob_data_and_get_certificate_with_relay(
                            &self.sui_client,
                            blob,
                            blob_object.blob_id,
                            store_args.encoding_type,
                            blob_object.blob_persistence_type(),
                        )
                        .await
                        .map_err(|error| ClientErrorKind::UploadRelayError(error).into()),
                };

                let blob_size = blob_object.size;
                if !walrus_utils::is_internal_run() {
                    tracing::debug!(
                        blob_id = %encoded_blob.blob_id(),
                        duration = ?certify_start_timer.elapsed(),
                        blob_size,
                        "finished sending blob data and collecting certificate"
                    );
                }
                result
            }
        };
        blob_to_be_certified.with_certificate_result(certificate_result)
    }

    async fn certify_and_extend_blobs(
        &self,
        blobs_to_certify_and_extend: Vec<WalrusStoreBlobUnfinished<BlobPendingCertifyAndExtend>>,
        store_args: &StoreArgs,
    ) -> ClientResult<Vec<WalrusStoreBlobFinished>> {
        let blobs_count = blobs_to_certify_and_extend.len();
        if blobs_count == 0 {
            return Ok(vec![]);
        }

        let start = Instant::now();

        let certify_and_extend_parameters = blobs_to_certify_and_extend
            .iter()
            .map(|blob| blob.get_certify_and_extend_params())
            .collect::<Vec<_>>();

        let cert_and_extend_results = self
            .sui_client
            .certify_and_extend_blobs(&certify_and_extend_parameters, store_args.post_store)
            .await
            .map_err(|error| {
                tracing::warn!(
                    %error,
                    "failure occurred while certifying and extending blobs on Sui"
                );
                ClientError::from(ClientErrorKind::CertificationFailed(error))
            })?;

        let sui_cert_timer_duration = start.elapsed();
        tracing::info!(
            duration = ?sui_cert_timer_duration,
            "finished certifying and extending blobs on Sui",
        );
        store_args.maybe_observe_upload_certificate(sui_cert_timer_duration);

        // Build map from object ID to CertifyAndExtendBlobResult.
        let result_map: HashMap<ObjectID, CertifyAndExtendBlobResult> = cert_and_extend_results
            .into_iter()
            .map(|result| (result.blob_object_id, result))
            .collect();

        // Get price computation for completing blobs
        let price_computation = self.get_price_computation().await?;
        let results = blobs_to_certify_and_extend
            .into_iter()
            .map(|blob| {
                blob.map_infallible(
                    |blob| {
                        let certify_and_extend_result = result_map.get(&blob.blob_object.id);
                        blob.with_certify_and_extend_result(
                            certify_and_extend_result,
                            &price_computation,
                        )
                    },
                    "with_certify_and_extend_result",
                )
            })
            .collect();
        Ok(results)
    }

    /// Creates a resource manager for the client.
    pub fn resource_manager(&self, committees: &ActiveCommittees) -> ResourceManager<'_> {
        ResourceManager::new(&self.sui_client, committees.write_committee().epoch)
    }

    // Blob deletion

    /// Returns an iterator over the list of blobs that can be deleted, based on the blob ID.
    pub async fn deletable_blobs_by_id<'a>(
        &self,
        blob_id: &'a BlobId,
    ) -> ClientResult<impl Iterator<Item = Blob> + 'a> {
        let owned_blobs = self
            .sui_client
            .owned_blobs(None, ExpirySelectionPolicy::Valid);
        Ok(owned_blobs
            .await?
            .into_iter()
            .filter(|blob| blob.blob_id == *blob_id && blob.deletable))
    }

    #[tracing::instrument(skip_all, fields(blob_id))]
    /// Deletes all owned blobs that match the blob ID, and returns the number of deleted objects.
    pub async fn delete_owned_blob(&self, blob_id: &BlobId) -> ClientResult<usize> {
        let mut deleted = 0;
        for blob in self.deletable_blobs_by_id(blob_id).await? {
            self.delete_owned_blob_by_object(blob.id).await?;
            deleted += 1;
        }
        Ok(deleted)
    }

    /// Deletes the owned _deletable_ blob on Walrus, specified by Sui Object ID.
    pub async fn delete_owned_blob_by_object(&self, blob_object_id: ObjectID) -> ClientResult<()> {
        tracing::debug!(%blob_object_id, "deleting blob object");
        self.sui_client.delete_blob(blob_object_id).await?;
        Ok(())
    }

    /// For each entry in `node_ids_with_amounts`, stakes the amount of WAL specified by the
    /// second element of the pair with the node represented by the first element of the pair.
    pub async fn stake_with_node_pools(
        &self,
        node_ids_with_amounts: &[(ObjectID, u64)],
    ) -> ClientResult<Vec<StakedWal>> {
        let staked_wal = self
            .sui_client
            .stake_with_pools(node_ids_with_amounts)
            .await?;
        Ok(staked_wal)
    }

    /// Stakes the specified amount of WAL with the node represented by `node_id`.
    pub async fn stake_with_node_pool(&self, node_id: ObjectID, amount: u64) -> ClientResult<()> {
        self.stake_with_node_pools(&[(node_id, amount)]).await?;
        Ok(())
    }

    /// Exchanges the provided amount of SUI (in MIST) for WAL using the specified exchange.
    pub async fn exchange_sui_for_wal(
        &self,
        exchange_id: ObjectID,
        amount: u64,
    ) -> ClientResult<()> {
        Ok(self
            .sui_client
            .exchange_sui_for_wal(exchange_id, amount)
            .await?)
    }

    /// Returns the latest committees from the chain.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn get_latest_committees_in_test(&self) -> Result<ActiveCommittees, ClientError> {
        Ok(ActiveCommittees::from_committees_and_state(
            self.sui_client
                .get_committees_and_state()
                .await
                .map_err(ClientError::other)?,
        ))
    }
}

impl<T> WalrusNodeClient<T> {
    /// Adds a [`Blocklist`] to the client that will be checked when storing or reading blobs.
    ///
    /// This can be called again to replace the blocklist.
    pub fn with_blocklist(mut self, blocklist: Blocklist) -> Self {
        self.blocklist = Some(blocklist);
        self
    }

    /// Creates a blocklist from a path with metrics support.
    ///
    /// This is a convenience method for creating a blocklist with metrics when the client
    /// has access to a metrics registry.
    pub fn create_blocklist_with_metrics(
        path: &Option<PathBuf>,
        metrics_registry: Option<&Registry>,
    ) -> anyhow::Result<Blocklist> {
        Blocklist::new_with_metrics(path, metrics_registry)
    }

    /// Returns a [`QuiltClient`] for storing and retrieving quilts.
    pub fn quilt_client(&self) -> QuiltClient<'_, Self> {
        QuiltClient::new(self, self.config.quilt_client_config.clone())
    }

    /// Returns a [`ByteRangeReadClient`] for reading byte ranges from blobs.
    pub fn byte_range_read_client(&self) -> ByteRangeReadClient<'_, T> {
        ByteRangeReadClient::new(self, self.config.byte_range_read_client_config.clone())
    }

    fn build_sliver_write_throttle(
        &self,
        blob_size: u64,
        encoding_type: EncodingType,
    ) -> (Arc<Semaphore>, Option<AutoTuneHandle>) {
        let initial_permits = self
            .communication_limits
            .max_concurrent_sliver_writes_for_blob_size(
                blob_size,
                &self.encoding_config,
                encoding_type,
            );
        let auto_tune_handle =
            AutoTuneHandle::new(&self.communication_limits.auto_tune, initial_permits);
        if let Some(handle) = &auto_tune_handle {
            (handle.semaphore(), auto_tune_handle)
        } else {
            (Arc::new(Semaphore::new(initial_permits)), None)
        }
    }

    fn pending_upload_max_blob_bytes(&self) -> u64 {
        self.config
            .communication_config
            .optimistic_upload_max_blob_bytes
    }

    async fn process_tail_handle(
        &self,
        tail_handle: Option<JoinHandle<()>>,
        tail_handling: TailHandling,
        tail_handle_collector: Option<&Arc<tokio::sync::Mutex<Vec<JoinHandle<()>>>>>,
    ) {
        if let Some(handle) = tail_handle {
            if let Some(collector) = tail_handle_collector {
                collector.lock().await.push(handle);
            } else if matches!(tail_handling, TailHandling::Detached) {
                tokio::spawn(async move {
                    if let Err(err) = handle.await {
                        tracing::warn!(?err, "tail upload task failed");
                    }
                });
            } else if let Err(err) = handle.await {
                tracing::warn!(?err, "tail upload task failed");
            }
        }
    }

    /// Stores the already-encoded metadata and sliver pairs for a blob into Walrus, by sending
    /// sliver pairs to at least 2f+1 shards.
    ///
    /// Assumes the blob ID has already been registered, with an appropriate blob size.
    #[tracing::instrument(skip_all)]
    #[allow(clippy::too_many_arguments)]
    pub async fn send_blob_data_and_get_certificate(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        pairs: Arc<Vec<SliverPair>>,
        blob_persistence_type: &BlobPersistenceType,
        multi_pb: Option<&MultiProgress>,
        tail_handling: TailHandling,
        quorum_forwarder: Option<tokio::sync::mpsc::Sender<UploaderEvent>>,
        tail_handle_collector: Option<Arc<tokio::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>>,
        target_nodes: Option<(Epoch, &[NodeIndex])>,
        initial_completed_weight: Option<&HashMap<BlobId, usize>>,
    ) -> ClientResult<ConfirmationCertificate> {
        let send_options = SendBlobOptions {
            blob_persistence_type,
            multi_pb,
            tail_handling,
            quorum_forwarder,
            tail_handle_collector,
            target_nodes,
            initial_completed_weight,
        };

        let (_, certificate) = self
            .send_blob_data_and_collect_confirmations(metadata, pairs, send_options)
            .await?
            .into_parts();

        certificate
    }

    /// Uploads slivers (optionally to a target node subset) and then collects confirmations from
    /// the full write committee; missing confirmations from non-targeted nodes still count as
    /// missing and can trigger retries.
    async fn send_blob_data_and_collect_confirmations(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        pairs: Arc<Vec<SliverPair>>,
        options: SendBlobOptions<'_>,
    ) -> ClientResult<ConfirmationCollection> {
        let SendBlobOptions {
            blob_persistence_type,
            multi_pb,
            tail_handling,
            quorum_forwarder,
            tail_handle_collector,
            target_nodes,
            initial_completed_weight,
        } = options;
        tracing::info!(blob_id = %metadata.blob_id(), "starting to send data to storage nodes");
        let committees = self.get_committees().await?;

        let progress_bar = multi_pb.map(|multi_pb| {
            let pb = styled_progress_bar(bft::min_n_correct(committees.n_shards()).get().into());
            pb.set_message(format!("sending slivers ({})", metadata.blob_id()));
            multi_pb.add(pb)
        });

        let blobs = vec![(metadata.clone(), pairs)];
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(blobs.len().max(1));

        let mut upload_fut = Box::pin(self.distributed_upload_without_confirmation(
            &blobs,
            event_tx.clone(),
            UploadOptions {
                tail_handling,
                target_nodes,
                upload_intent: UploadIntent::Immediate,
                initial_completed_weight,
                cancellation: None,
            },
        ));

        let mut upload_results: Option<RunOutput<Vec<BlobId>, StoreError>> = None;

        while upload_results.is_none() {
            tokio::select! {
                biased;
                maybe_event = event_rx.recv() => {
                    if let Some(event) = maybe_event {
                        tracing::debug!(
                            blob_id = %metadata.blob_id(), ?event, "received uploader event");
                        if let Some(tx) = quorum_forwarder.as_ref() {
                            if let Err(err) = tx.send(event.clone()).await {
                                tracing::error!(
                                    blob_id = %metadata.blob_id(), ?event, ?err,
                                    "failed to forward uploader event");
                            }
                            tracing::debug!(
                                blob_id = %metadata.blob_id(), ?event, "forwarded uploader event");
                        }

                        match event {
                            UploaderEvent::BlobProgress {
                                completed_weight,
                                required_weight,
                                ..
                            } => {
                                if let Some(pb) = &progress_bar {
                                    pb.set_length(required_weight as u64);
                                    pb.set_position(std::cmp::min(
                                        completed_weight, required_weight) as u64);
                                }
                            }
                            UploaderEvent::BlobQuorumReached { .. } => {
                                tracing::debug!(
                                    blob_id = %metadata.blob_id(),
                                    "received blob quorum reached event");
                                if let Some(pb) = &progress_bar && !pb.is_finished() {
                                    pb.finish_with_message(
                                        format!("slivers sent ({})", metadata.blob_id()));
                                }
                            }
                        }
                    } else {
                        tracing::debug!(
                            blob_id = %metadata.blob_id(), "uploader event channel closed");
                    }
                }
                result = &mut upload_fut => {
                    let run_output = result?;
                    tracing::debug!(
                        blob_id = %metadata.blob_id(), results = run_output.results.len(),
                        "uploader run completed"
                    );
                    upload_results = Some(run_output);
                }
            }
        }

        if let Some(pb) = &progress_bar
            && !pb.is_finished()
        {
            pb.finish_with_message(format!("slivers sent ({})", metadata.blob_id()));
        }
        tracing::debug!(blob_id = %metadata.blob_id(), "all uploader events consumed");

        let upload_results = upload_results.expect("distributed upload must return results");

        tracing::debug!(
            blob_id = %metadata.blob_id(),
            tail_handle = upload_results.tail_handle.is_some(),
            "uploader run completed"
        );
        if let Some(handle) = upload_results.tail_handle {
            tracing::debug!(blob_id = %metadata.blob_id(), "received tail handle from uploader");
            if let Some(collector) = tail_handle_collector {
                collector.lock().await.push(handle);
                tracing::debug!(blob_id = %metadata.blob_id(), "queued tail handle for collector");
            } else if matches!(tail_handling, TailHandling::Detached) {
                tracing::debug!(blob_id = %metadata.blob_id(), "spawned detached tail handler");
                tokio::spawn(async move {
                    if let Err(err) = handle.await {
                        tracing::warn!(?err, "tail upload task failed");
                    }
                });
            } else {
                tracing::debug!(blob_id = %metadata.blob_id(), "awaiting tail handle inline");
                if let Err(err) = handle.await {
                    tracing::warn!(?err, "tail upload task failed");
                }
            }
        }

        self.collect_confirmation_attempt(
            metadata.blob_id(),
            committees.write_committee().epoch,
            blob_persistence_type,
            &committees,
        )
        .await
    }

    /// Uploads metadata and sliver pairs to the storage nodes without requesting confirmations.
    ///
    /// Returns the node-level results of the upload action and emits progress via the provided
    /// `event_sender`.
    async fn distributed_upload_without_confirmation(
        &self,
        blobs: &[(VerifiedBlobMetadataWithId, Arc<Vec<SliverPair>>)],
        event_sender: tokio::sync::mpsc::Sender<UploaderEvent>,
        options: UploadOptions<'_>,
    ) -> ClientResult<RunOutput<Vec<BlobId>, StoreError>> {
        let UploadOptions {
            tail_handling,
            target_nodes,
            upload_intent,
            initial_completed_weight,
            cancellation,
        } = options;
        if blobs.is_empty() {
            return Ok(RunOutput {
                results: Vec::new(),
                tail_handle: None,
            });
        }
        tracing::info!(
            blobs = blobs.len(),
            intent = ?upload_intent,
            target_nodes = target_nodes.as_ref().map(|(_, nodes)| nodes.len()).unwrap_or(0),
            cancellation = cancellation.is_some(),
            "starting distributed upload"
        );

        let committees = self.get_committees().await?;
        if let Some((target_epoch, _)) = target_nodes
            && committees.epoch() != target_epoch
        {
            return Err(ClientError::other(StoreError::TargetEpochMismatch {
                target_epoch,
                current_epoch: committees.epoch(),
            }));
        }

        let max_unencoded = blobs
            .iter()
            .map(|(metadata, _)| metadata.metadata().unencoded_length())
            .max()
            .unwrap_or(0);

        let encoding_type = blobs
            .first()
            .map(|(metadata, _)| metadata.metadata().encoding_type())
            .unwrap_or(DEFAULT_ENCODING);

        let (sliver_write_semaphore, auto_tune_handle) =
            self.build_sliver_write_throttle(max_unencoded, encoding_type);

        if auto_tune_handle.is_some() {
            tracing::info!("auto tune is enabled");
        } else {
            tracing::debug!("auto tune is disabled");
        }

        let comms = self.node_write_communications_for_upload(
            &committees,
            sliver_write_semaphore,
            auto_tune_handle,
            target_nodes.map(|(_, nodes)| nodes),
        )?;

        let sliver_write_extra_time = self
            .config
            .communication_config
            .sliver_write_extra_time
            .clone();

        let mut uploader = DistributedUploader::new(
            blobs,
            committees.clone(),
            comms,
            sliver_write_extra_time,
            initial_completed_weight,
        );

        let run_output = uploader
            .run_distributed_upload(
                move |node, work| async move {
                    tracing::debug!(
                        node = ?node.node_index,
                        work_items = work.len(),
                        intent = ?upload_intent,
                        "upload worker starting"
                    );
                    let mut stored = Vec::with_capacity(work.len());
                    for item in &work {
                        let response = node
                            .store_metadata_and_pairs_without_confirmation(
                                &item.metadata,
                                item.pair_indices.iter().map(|&i| &item.pairs[i]),
                                upload_intent,
                            )
                            .await;

                        match response.result {
                            Ok(()) => {
                                tracing::debug!(
                                    node = ?node.node_index,
                                    blob_id = %item.blob_id(),
                                    intent = ?upload_intent,
                                    "store without confirmation succeeded"
                                );
                                stored.push(*item.blob_id())
                            }
                            Err(err) => {
                                tracing::warn!(
                                    node = ?node.node_index,
                                    blob_id = %item.blob_id(),
                                    intent = ?upload_intent,
                                    ?err,
                                    "store without confirmation failed"
                                );
                                return NodeResult::new(
                                    response.committee_epoch,
                                    response.weight,
                                    response.node,
                                    Err(err),
                                );
                            }
                        }
                    }

                    NodeResult::new(
                        node.committee_epoch,
                        node.n_owned_shards().get().into(),
                        node.node_index,
                        Ok(stored),
                    )
                },
                event_sender,
                tail_handling,
                cancellation,
            )
            .await?;

        Ok(run_output)
    }

    /// Immediate upload path that seeds the uploader with any initial weight and retries missing
    /// confirmations with immediate intent.
    async fn upload_and_collect_certificate(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        pairs: Arc<Vec<SliverPair>>,
        blob_persistence_type: &BlobPersistenceType,
        multi_pb: Option<&MultiProgress>,
        store_args: &StoreArgs,
        pending_context: &PendingUploadContext,
    ) -> ClientResult<ConfirmationCertificate> {
        let blobs = vec![(metadata.clone(), pairs.clone())];
        let committees = self.get_committees().await?;
        let mut missing_nodes = None;
        let mut initial_completed_weight = None;
        if let Some(success_nodes) = pending_context
            .initial_success_nodes
            .as_ref()
            .and_then(|m| m.get(metadata.blob_id()))
        {
            // Nodes that did not report success are treated as missing.
            let success_set: HashSet<NodeIndex> = success_nodes.iter().copied().collect();
            let all_nodes = committees.write_committee().members().len();
            let missing: Vec<NodeIndex> = (0..all_nodes)
                .filter(|idx| !success_set.contains(idx))
                .collect();
            if !missing.is_empty() {
                missing_nodes = Some(missing);
            }

            let members = committees.write_committee().members();
            let weight: usize = success_nodes
                .iter()
                .map(|idx| members[*idx].shard_ids.len())
                .sum();
            if weight > 0 {
                initial_completed_weight = Some(HashMap::from([(*metadata.blob_id(), weight)]));
            }
        }
        let target_nodes = missing_nodes
            .as_ref()
            .map(|nodes| (committees.epoch(), nodes.as_slice()));

        let send_options = SendBlobOptions {
            blob_persistence_type,
            multi_pb,
            tail_handling: store_args.tail_handling,
            quorum_forwarder: store_args.quorum_event_tx.clone(),
            tail_handle_collector: store_args.tail_handle_collector.clone(),
            target_nodes,
            initial_completed_weight: initial_completed_weight.as_ref(),
        };

        let (confirmation_results, upload_result) = self
            .send_blob_data_and_collect_confirmations(metadata, pairs, send_options)
            .await?
            .into_parts();

        match upload_result {
            Ok(cert) => Ok(cert),
            Err(err) => {
                if !matches!(err.kind(), ClientErrorKind::NotEnoughConfirmations(_, _)) {
                    return Err(err);
                }

                let certified_epoch = committees.write_committee().epoch;

                // Pending slivers can be cache-evicted, so retry uploads to nodes that never
                // confirmed.
                self.retry_missing_nodes(
                    metadata,
                    blob_persistence_type,
                    certified_epoch,
                    &committees,
                    &blobs,
                    store_args,
                    confirmation_results,
                )
                .await
            }
        }
    }

    /// Retries the upload with immediate intent to the missing nodes until quorum is reached.
    #[allow(clippy::too_many_arguments)]
    async fn retry_missing_nodes(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        blob_persistence_type: &BlobPersistenceType,
        certified_epoch: Epoch,
        committees: &ActiveCommittees,
        blobs: &[(VerifiedBlobMetadataWithId, Arc<Vec<SliverPair>>)],
        store_args: &StoreArgs,
        confirmation_results: Vec<NodeResult<SignedStorageConfirmation, NodeError>>,
    ) -> ClientResult<ConfirmationCertificate> {
        let mut confirmation_results = confirmation_results;
        let mut attempts = 0usize;

        loop {
            let not_enough_confirmations_err =
                match self.confirmations_to_certificate_ref(&confirmation_results, committees) {
                    Ok(cert) => return Ok(cert),
                    Err(err) => {
                        if !matches!(err.kind(), ClientErrorKind::NotEnoughConfirmations(_, _)) {
                            return Err(err);
                        }
                        err
                    }
                };

            let mut confirmed_nodes = HashSet::new();
            let mut confirmed_weight = 0usize;
            for NodeResult {
                weight,
                node,
                result,
                ..
            } in &confirmation_results
            {
                if result.is_ok() && confirmed_nodes.insert(*node) {
                    confirmed_weight += weight;
                }
            }

            let all_nodes: HashSet<NodeIndex> =
                (0..committees.write_committee().members().len()).collect();
            let missing_nodes: Vec<NodeIndex> =
                all_nodes.difference(&confirmed_nodes).copied().collect();
            tracing::debug!(
                blob_id = %metadata.blob_id(),
                confirmed_weight,
                confirmed_nodes = confirmed_nodes.len(),
                missing_nodes = missing_nodes.len(),
                "retrying missing confirmations"
            );

            if missing_nodes.is_empty() {
                return self.confirmations_to_certificate(confirmation_results, committees);
            }

            if attempts >= MAX_MISSING_CONFIRMATION_RETRY_ROUNDS {
                tracing::warn!(
                    blob_id = %metadata.blob_id(),
                    confirmed_weight,
                    confirmed_nodes = confirmed_nodes.len(),
                    missing_nodes = missing_nodes.len(),
                    attempts,
                    "missing confirmation retries exhausted; giving up"
                );
                return Err(not_enough_confirmations_err);
            }
            attempts += 1;

            // Drain uploader progress events so bounded channels don't deadlock the retry path.
            let buffer = committees
                .write_committee()
                .members()
                .len()
                .saturating_mul(blobs.len().max(1))
                .saturating_add(1)
                .max(1);
            let (tx, mut rx) = tokio::sync::mpsc::channel(buffer);
            tokio::spawn(async move { while rx.recv().await.is_some() {} });

            let initial_weight = (confirmed_weight > 0)
                .then(|| HashMap::from([(*metadata.blob_id(), confirmed_weight)]));

            let retry_results = self
                .distributed_upload_without_confirmation(
                    blobs,
                    tx,
                    UploadOptions {
                        tail_handling: store_args.tail_handling,
                        target_nodes: Some((certified_epoch, &missing_nodes)),
                        upload_intent: UploadIntent::Immediate,
                        initial_completed_weight: initial_weight.as_ref(),
                        cancellation: None,
                    },
                )
                .await?;
            tracing::debug!(
                blob_id = %metadata.blob_id(),
                missing_nodes = missing_nodes.len(),
                retry_results = retry_results.results.len(),
                "retry upload completed"
            );

            self.process_tail_handle(
                retry_results.tail_handle,
                store_args.tail_handling,
                store_args.tail_handle_collector.as_ref(),
            )
            .await;

            confirmation_results = self
                .collect_confirmations(
                    metadata.blob_id(),
                    certified_epoch,
                    blob_persistence_type,
                    committees,
                )
                .await?;
        }
    }

    fn node_write_communications_for_upload(
        &self,
        committees: &ActiveCommittees,
        sliver_write_semaphore: Arc<Semaphore>,
        auto_tune_handle: Option<AutoTuneHandle>,
        target_nodes: Option<&[NodeIndex]>,
    ) -> ClientResult<Vec<NodeWriteCommunication>> {
        match target_nodes {
            Some(indices) => self
                .communication_factory
                .node_write_communications_by_index(
                    committees,
                    sliver_write_semaphore,
                    auto_tune_handle,
                    indices.iter().copied(),
                ),
            None => self.communication_factory.node_write_communications(
                committees,
                sliver_write_semaphore,
                auto_tune_handle,
            ),
        }
    }

    async fn collect_confirmations(
        &self,
        blob_id: &BlobId,
        certified_epoch: Epoch,
        blob_persistence_type: &BlobPersistenceType,
        committees: &ActiveCommittees,
    ) -> ClientResult<Vec<NodeResult<SignedStorageConfirmation, NodeError>>> {
        let comms = self
            .communication_factory
            .node_read_communications(committees, certified_epoch)?;

        let mut requests = WeightedFutures::new(comms.iter().map(|n| {
            n.get_confirmation_with_retries(blob_id, committees.epoch(), blob_persistence_type)
        }));

        let _ = requests
            .execute_weight(
                &|weight| committees.is_quorum(weight),
                self.communication_limits.max_concurrent_sliver_reads,
            )
            .await;

        Ok(requests.into_results())
    }

    async fn collect_confirmation_attempt(
        &self,
        blob_id: &BlobId,
        certified_epoch: Epoch,
        blob_persistence_type: &BlobPersistenceType,
        committees: &ActiveCommittees,
    ) -> ClientResult<ConfirmationCollection> {
        let confirmations = self
            .collect_confirmations(blob_id, certified_epoch, blob_persistence_type, committees)
            .await?;

        let certificate = self.confirmations_to_certificate_ref(&confirmations, committees);
        Ok(ConfirmationCollection {
            confirmations,
            certificate,
        })
    }

    /// Fetches confirmations for a blob from a quorum of nodes and returns the certificate.
    async fn get_certificate_standalone(
        &self,
        blob_id: &BlobId,
        certified_epoch: Epoch,
        blob_persistence_type: &BlobPersistenceType,
    ) -> ClientResult<ConfirmationCertificate> {
        let committees = self.get_committees().await?;
        let (_, certificate) = self
            .collect_confirmation_attempt(
                blob_id,
                certified_epoch,
                blob_persistence_type,
                &committees,
            )
            .await?
            .into_parts();

        certificate
    }

    /// Combines the received storage confirmations into a single certificate.
    ///
    /// This function _does not_ check that the received confirmations match the current epoch and
    /// blob ID, as it assumes that the storage confirmations were received through
    /// [`NodeCommunication::get_confirmation_with_retries`][gcwr], which internally verifies them
    /// to check the blob ID, epoch, and blob persistence type.
    ///
    /// [gcwr]: crate::client::communication::NodeCommunication::get_confirmation_with_retries
    fn confirmations_to_certificate<E: Display>(
        &self,
        confirmations: Vec<NodeResult<SignedStorageConfirmation, E>>,
        committees: &ActiveCommittees,
    ) -> ClientResult<ConfirmationCertificate> {
        self.confirmations_to_certificate_ref(&confirmations, committees)
    }

    fn confirmations_to_certificate_ref<E: Display>(
        &self,
        confirmations: &[NodeResult<SignedStorageConfirmation, E>],
        committees: &ActiveCommittees,
    ) -> ClientResult<ConfirmationCertificate> {
        let mut aggregate_weight = 0;
        let mut signers = Vec::with_capacity(confirmations.len());
        let mut signed_messages = Vec::with_capacity(confirmations.len());

        for confirmation in confirmations {
            match &confirmation.result {
                Ok(confirmation_msg) => {
                    aggregate_weight += confirmation.weight;
                    signed_messages.push(confirmation_msg.clone());
                    signers.push(
                        u16::try_from(confirmation.node)
                            .expect("the node index is computed from the vector of members"),
                    );
                }
                Err(error) => {
                    tracing::info!(confirmation.node, %error, "storing metadata and pairs failed")
                }
            }
        }

        ensure!(
            committees
                .write_committee()
                .is_at_least_min_n_correct(aggregate_weight),
            self.not_enough_confirmations_error(aggregate_weight, committees)
        );

        let cert =
            ConfirmationCertificate::from_signed_messages_and_indices(signed_messages, signers)
                .map_err(ClientError::other)?;
        Ok(cert)
    }

    fn not_enough_confirmations_error(
        &self,
        weight: usize,
        committees: &ActiveCommittees,
    ) -> ClientError {
        ClientErrorKind::NotEnoughConfirmations(weight, committees.min_n_correct()).into()
    }

    /// Returns per-blob successful nodes from pending upload results.
    /// Nodes that did not report success (errors or missing results) are treated as missing later.
    fn successful_nodes_by_blob(
        results: &[NodeResult<Vec<BlobId>, StoreError>],
    ) -> HashMap<BlobId, Vec<NodeIndex>> {
        let mut nodes_by_blob: HashMap<BlobId, HashSet<NodeIndex>> = HashMap::new();

        for res in results {
            if let Ok(blob_ids) = &res.result {
                for blob_id in blob_ids {
                    nodes_by_blob.entry(*blob_id).or_default().insert(res.node);
                }
            }
        }

        nodes_by_blob
            .into_iter()
            .map(|(blob_id, mut nodes)| {
                let mut nodes = nodes.drain().collect::<Vec<_>>();
                nodes.sort_unstable();
                (blob_id, nodes)
            })
            .collect()
    }

    /// Requests the slivers and decodes them into a blob.
    ///
    /// Returns a [`ClientError`] of kind [`ClientErrorKind::BlobIdDoesNotExist`] if it receives a
    /// quorum (at least 2f+1) of "not found" error status codes from the storage nodes.
    #[tracing::instrument(level = Level::ERROR, skip_all)]
    async fn request_slivers_and_decode<A>(
        &self,
        certified_epoch: Epoch,
        metadata: &VerifiedBlobMetadataWithId,
        consistency_check: ConsistencyCheckType,
    ) -> ClientResult<Vec<u8>>
    where
        A: EncodingAxis,
        SliverData<A>: TryFrom<Sliver>,
    {
        let committees = self.get_committees().await?;
        // Create a progress bar to track the progress of the sliver retrieval.
        let progress_bar: indicatif::ProgressBar = styled_progress_bar(
            self.encoding_config
                .get_for_type(metadata.metadata().encoding_type())
                .n_source_symbols::<A>()
                .get()
                .into(),
        );
        progress_bar.set_message("requesting slivers");

        let comms = self
            .communication_factory
            .node_read_communications(&committees, certified_epoch)?;
        // Create requests to get all slivers from all nodes.
        let verified_slivers_futures = comms.iter().flat_map(|n| {
            // NOTE: the cloned here is needed because otherwise the compiler complains about the
            // lifetimes of `s`.
            n.node.shard_ids.iter().cloned().map(|s| {
                n.retrieve_verified_sliver::<A>(metadata, s)
                    .instrument(n.span.clone())
                    // Increment the progress bar if the sliver is successfully retrieved.
                    .inspect({
                        let value = progress_bar.clone();
                        move |result| {
                            if result.is_ok() {
                                value.inc(1)
                            }
                        }
                    })
            })
        });
        // Get the first ~1/3 or ~2/3 of slivers directly, and decode with these.
        let mut requests = WeightedFutures::new(verified_slivers_futures);

        // Note: The following code may have to be changed if we add encodings that require a
        // variable number of slivers to reconstruct a blob.
        let RequiredCount::Exact(required_slivers) = self
            .encoding_config
            .get_for_type(metadata.metadata().encoding_type())
            .n_slivers_for_reconstruction::<A>();
        let completed_reason = requests
            .execute_weight(
                &|weight| weight >= required_slivers,
                self.communication_limits
                    .max_concurrent_sliver_reads_for_blob_size(
                        metadata.metadata().unencoded_length(),
                        &self.encoding_config,
                        metadata.metadata().encoding_type(),
                    ),
            )
            .await;
        progress_bar.finish_with_message("slivers received");

        match completed_reason {
            CompletedReasonWeight::ThresholdReached => {
                let verified_slivers = requests.take_inner_ok();
                assert!(
                    verified_slivers.len() >= required_slivers,
                    "we must have sufficient slivers if the threshold was reached"
                );
                match self
                    .encoding_config
                    .get_for_type(metadata.metadata().encoding_type())
                    .decode_and_verify(metadata, verified_slivers, consistency_check)
                {
                    Ok(blob) => Ok(blob),
                    Err(DecodeError::VerificationError) => Err(ClientErrorKind::InvalidBlob.into()),
                    Err(error) => {
                        panic!(
                            "unable to decode blob from a sufficient number of slivers;\n\
                            this should never happen; please report this as a bug;\n\
                            error: {error:?}"
                        );
                    }
                }
            }
            CompletedReasonWeight::FuturesConsumed(weight) => {
                assert!(
                    weight < required_slivers,
                    "the case where we have collected sufficient slivers is handled above"
                );
                let mut n_not_found = 0; // Counts the number of "not found" status codes received.
                let mut n_forbidden = 0; // Counts the number of "forbidden" status codes received.
                requests.take_results().into_iter().for_each(
                    |NodeResult { node, result, .. }| {
                        if let Err(error) = result {
                            tracing::debug!(%node, %error, "retrieving sliver failed");
                            if error.is_status_not_found() {
                                n_not_found += 1;
                            } else if error.is_blob_blocked() {
                                n_forbidden += 1;
                            }
                        }
                    },
                );

                if committees.is_quorum(n_not_found + n_forbidden) {
                    if n_not_found > n_forbidden {
                        Err(ClientErrorKind::BlobIdDoesNotExist.into())
                    } else {
                        Err(ClientErrorKind::BlobIdBlocked(*metadata.blob_id()).into())
                    }
                } else {
                    Err(ClientErrorKind::NotEnoughSlivers.into())
                }
            }
        }
    }

    /// Requests the metadata from storage nodes, and keeps the first reply that correctly verifies.
    ///
    /// At a high level:
    /// 1. The function requests a random subset of nodes amounting to at least a quorum (2f+1)
    ///    stake for the metadata.
    /// 1. If the function receives valid metadata for the blob, then it returns the metadata.
    /// 1. Otherwise:
    ///    1. If it received f+1 "not found" status responses, it can conclude that the blob ID was
    ///       not certified and returns an error of kind [`ClientErrorKind::BlobIdDoesNotExist`].
    ///    1. Otherwise, there is some major problem with the network and returns an error of kind
    ///       [`ClientErrorKind::NoMetadataReceived`].
    ///
    /// This procedure works because:
    /// 1. If the blob ID was never certified: Then at least f+1 of the 2f+1 nodes by stake that
    ///    were contacted are correct and have returned a "not found" status response.
    /// 1. If the blob ID was certified: Considering the worst possible case where it was certified
    ///    by 2f+1 stake, of which f was malicious, and the remaining f honest did not receive the
    ///    metadata and have yet to recover it. Then, by quorum intersection, in the 2f+1 that reply
    ///    to the client at least 1 is honest and has the metadata. This one node will provide it
    ///    and the client will know the blob exists.
    ///
    /// Note that if a faulty node returns _valid_ metadata for a blob ID that was however not
    /// certified yet, the client proceeds even if the blob ID was possibly not certified yet. This
    /// instance is not considered problematic, as the client will just continue to retrieving the
    /// slivers and fail there.
    ///
    /// The general problem in this latter case is the difficulty to distinguish correct nodes that
    /// have received the certification of the blob before the others, from malicious nodes that
    /// pretend the certification exists.
    pub async fn retrieve_metadata(
        &self,
        certified_epoch: Epoch,
        blob_id: &BlobId,
    ) -> ClientResult<VerifiedBlobMetadataWithId> {
        let committees = self.get_committees().await?;
        let comms = self
            .communication_factory
            .node_read_communications_quorum(&committees, certified_epoch)?;
        let futures = comms.iter().map(|n| {
            n.retrieve_verified_metadata(blob_id)
                .instrument(n.span.clone())
        });
        // Wait until the first request succeeds
        let mut requests = WeightedFutures::new(futures);
        let just_one = |weight| weight >= 1;
        let _ = requests
            .execute_weight(
                &just_one,
                self.communication_limits.max_concurrent_metadata_reads,
            )
            .await;

        let mut n_not_found = 0;
        let mut n_forbidden = 0;
        for NodeResult {
            weight,
            node,
            result,
            ..
        } in requests.into_results()
        {
            match result {
                Ok(metadata) => {
                    tracing::debug!(?node, "metadata received");
                    return Ok(metadata);
                }
                Err(error) => {
                    let res = {
                        if error.is_status_not_found() {
                            n_not_found += weight;
                        } else if error.is_blob_blocked() {
                            n_forbidden += weight;
                        }
                        committees.is_quorum(n_not_found + n_forbidden)
                    };
                    if res {
                        // Return appropriate error based on which response type was more common
                        return if n_not_found > n_forbidden {
                            // TODO(giac): now that we check that the blob is certified before
                            // starting to read, this error should not technically happen unless (1)
                            // the client was disconnected while reading, or (2) the bft threshold
                            // was exceeded.
                            Err(ClientErrorKind::BlobIdDoesNotExist.into())
                        } else {
                            Err(ClientErrorKind::BlobIdBlocked(*blob_id).into())
                        };
                    }
                }
            }
        }
        Err(ClientErrorKind::NoMetadataReceived.into())
    }

    /// Retries to get the verified blob status.
    ///
    /// Retries are implemented with backoff, until the fetch succeeds or the maximum number of
    /// retries is reached. If the maximum number of retries is reached, the function returns an
    /// error of kind [`ClientErrorKind::NoValidStatusReceived`].
    #[tracing::instrument(skip_all, fields(%blob_id), err(level = Level::WARN))]
    pub async fn get_blob_status_with_retries<U: ReadClient>(
        &self,
        blob_id: &BlobId,
        read_client: &U,
    ) -> ClientResult<BlobStatus> {
        // The backoff is both the interval between retries and the maximum duration of the retry.
        let backoff = self
            .config
            .backoff_config()
            .get_strategy(ThreadRng::default().next_u64());

        let mut peekable = backoff.peekable();

        while let Some(delay) = peekable.next() {
            let maybe_status = self
                .get_verified_blob_status(blob_id, read_client, delay)
                .await;

            match maybe_status {
                Ok(_) => {
                    return maybe_status;
                }
                Err(client_error)
                    if matches!(client_error.kind(), &ClientErrorKind::BlobIdDoesNotExist) =>
                {
                    return Err(client_error);
                }
                Err(_) => (),
            };

            if peekable.peek().is_some() {
                tracing::debug!(
                    ?delay,
                    latest_status = ?maybe_status,
                    "fetching blob status failed; retrying after delay",
                );
                tokio::time::sleep(delay).await;
            } else {
                tracing::warn!(
                    latest_status = ?maybe_status,
                    "fetching blob status failed; no more retries",
                );
            }
        }

        return Err(ClientErrorKind::NoValidStatusReceived.into());
    }

    /// Gets the blob status from multiple nodes and returns the latest status that can be verified.
    ///
    /// The nodes are selected such that at least one correct node is contacted. This function reads
    /// from the latest committee, because, during epoch change, it is the committee that will have
    /// the most up-to-date information on the old and newly certified blobs.
    #[tracing::instrument(skip_all, fields(%blob_id), err(level = Level::WARN))]
    pub async fn get_verified_blob_status<U: ReadClient>(
        &self,
        blob_id: &BlobId,
        read_client: &U,
        timeout: Duration,
    ) -> ClientResult<BlobStatus> {
        tracing::debug!(?timeout, "trying to get blob status");
        let committees = self.get_committees().await?;

        let comms = self
            .communication_factory
            .node_read_communications(&committees, committees.write_committee().epoch)?;
        let futures = comms
            .iter()
            .map(|n| n.get_blob_status(blob_id).instrument(n.span.clone()));
        let mut requests = WeightedFutures::new(futures);
        requests
            .execute_until(
                &|weight| committees.is_quorum(weight),
                timeout,
                self.communication_limits.max_concurrent_status_reads,
            )
            .await;

        // If 2f+1 nodes return a 404 status, we know the blob does not exist.
        let n_not_found = requests
            .inner_err()
            .iter()
            .filter(|(err, _)| err.is_status_not_found())
            .map(|(_, weight)| weight)
            .sum();

        if committees.is_quorum(n_not_found) {
            return Err(ClientErrorKind::BlobIdDoesNotExist.into());
        }

        // Check the received statuses.
        let statuses = requests.take_unique_results_with_aggregate_weight();
        tracing::debug!(?statuses, "received blob statuses from storage nodes");
        let mut statuses_list: Vec<_> = statuses.keys().copied().collect();

        // Going through statuses from later (invalid) to earlier (nonexistent), see implementation
        // of `Ord` and `PartialOrd` for `BlobStatus`.
        statuses_list.sort_unstable();
        for status in statuses_list.into_iter().rev() {
            if committees
                .write_committee()
                .is_above_validity(statuses[&status])
                || verify_blob_status_event(blob_id, status, read_client)
                    .await
                    .is_ok()
            {
                return Ok(status);
            }
        }

        Err(ClientErrorKind::NoValidStatusReceived.into())
    }

    /// Returns a [`ClientError`] with [`ClientErrorKind::BlobIdBlocked`] if the provided blob ID is
    /// contained in the blocklist.
    fn check_blob_is_blocked(&self, blob_id: &BlobId) -> ClientResult<()> {
        if let Some(blocklist) = &self.blocklist
            && blocklist.is_blocked(blob_id)
        {
            tracing::debug!(%blob_id, "encountered blocked blob ID");
            return Err(ClientErrorKind::BlobIdBlocked(*blob_id).into());
        }
        Ok(())
    }

    /// Returns the shards of the given node in the write committee.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn shards_of(
        &self,
        node_names: &[String],
        committees: &ActiveCommittees,
    ) -> Vec<ShardIndex> {
        committees
            .write_committee()
            .members()
            .iter()
            .filter(|node| node_names.contains(&node.name))
            .flat_map(|node| node.shard_ids.clone())
            .collect::<Vec<_>>()
    }

    /// Maps the sliver pairs to the node in the write committee that holds their shard.
    fn pairs_per_node<'a>(
        &'a self,
        blob_id: &'a BlobId,
        pairs: &'a [SliverPair],
        committees: &ActiveCommittees,
    ) -> HashMap<usize, Vec<&'a SliverPair>> {
        committees
            .write_committee()
            .members()
            .iter()
            .map(|node| {
                pairs
                    .iter()
                    .filter(|pair| {
                        node.shard_ids
                            .contains(&pair.index().to_shard_index(committees.n_shards(), blob_id))
                    })
                    .collect::<Vec<_>>()
            })
            .enumerate()
            .collect()
    }

    /// Returns a reference to the encoding config in use.
    pub fn encoding_config(&self) -> &EncodingConfig {
        &self.encoding_config
    }

    /// Returns the inner sui client.
    pub fn sui_client(&self) -> &T {
        &self.sui_client
    }

    /// Returns the inner sui client as mutable reference.
    pub fn sui_client_mut(&mut self) -> &mut T {
        &mut self.sui_client
    }

    /// Returns the config used by the client.
    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    /// Gets the current active committees and price computation from the cache.
    #[tracing::instrument(skip_all, level = Level::DEBUG)]
    pub async fn get_committees_and_price(
        &self,
    ) -> ClientResult<(Arc<ActiveCommittees>, PriceComputation)> {
        self.committees_handle
            .send_committees_and_price_request(RequestKind::Get)
            .await
            .map_err(ClientError::other)
    }

    /// Forces a refresh of the committees and price computation.
    pub async fn force_refresh_committees(
        &self,
    ) -> ClientResult<(Arc<ActiveCommittees>, PriceComputation)> {
        tracing::warn!("[force_refresh_committees] forcing committee refresh");
        let result = self
            .committees_handle
            .send_committees_and_price_request(RequestKind::Refresh)
            .await
            .map_err(|error| {
                tracing::warn!(?error, "[force_refresh_committees] refresh failed");
                ClientError::other(error)
            })?;
        tracing::info!("[force_refresh_committees] refresh succeeded");
        Ok(result)
    }

    /// Gets the current active committees from the cache.
    pub async fn get_committees(&self) -> ClientResult<Arc<ActiveCommittees>> {
        let (committees, _) = self.get_committees_and_price().await?;
        Ok(committees)
    }

    /// Gets the current price computation from the cache.
    pub async fn get_price_computation(&self) -> ClientResult<PriceComputation> {
        let (_, price_computation) = self.get_committees_and_price().await?;
        Ok(price_computation)
    }
}

/// A wrapper for a [`WalrusNodeClient`] that may be created in a background task.
// INV: either client is initialized xor the join handle is Some.
// INV: if the encoding_config is not initialized, the join handle is Some.
#[derive(Debug)]
pub struct WalrusNodeClientCreatedInBackground<T> {
    encoding_config: OnceLock<EncodingConfig>,
    /// Contains the result of the background task if was already completed. If it returned an
    /// error, the error is stored as a string.
    client: OnceLock<Result<WalrusNodeClient<T>, String>>,
    #[allow(clippy::type_complexity)]
    join_handle: Mutex<Option<JoinHandle<ClientResult<WalrusNodeClient<T>>>>>,
    start_time: Instant,
}

impl<T> WalrusNodeClientCreatedInBackground<T>
where
    T: Debug + Send + 'static,
{
    /// Creates a new [`WalrusNodeClientCreatedInBackground`] that will create the
    /// [`WalrusNodeClient`] in a background task using the provided future.
    pub fn new<F>(create_future: F, maybe_n_shards: Option<NonZeroU16>) -> Self
    where
        F: Future<Output = ClientResult<WalrusNodeClient<T>>> + Send + 'static,
    {
        let encoding_config = OnceLock::new();
        if let Some(n_shards) = maybe_n_shards {
            encoding_config
                .set(EncodingConfig::new(n_shards))
                .expect("cell is not initialized yet");
        }
        let join_handle = tokio::task::spawn(async move {
            tracing::debug!("starting to create the `WalrusNodeClient` in a background task");
            create_future.await
        });
        Self {
            encoding_config,
            client: OnceLock::new(),
            join_handle: Mutex::new(Some(join_handle)),
            start_time: Instant::now(),
        }
    }

    /// Returns a reference to the encoding config.
    pub async fn encoding_config(&self) -> ClientResult<&EncodingConfig> {
        if let Some(encoding_config) = self.encoding_config.get() {
            return Ok(encoding_config);
        }
        Ok(self.client().await?.encoding_config())
    }

    /// Returns a reference to the [`WalrusNodeClient`].
    ///
    /// If the [`WalrusNodeClient`] is not yet created, this will wait for the background task to
    /// complete, replace the [`WalrusNodeClientCreatedInBackground`] with the created
    /// [`WalrusNodeClient`] and return the [`WalrusNodeClient`].
    ///
    /// # Errors
    ///
    /// If the background task returns an error, this will be returned on the first call to this
    /// function or [`Self::into_client`]. On subsequent calls, the error will be returned wrapped
    /// into a [`ClientError`] with [`ClientErrorKind::ClientInitializationError`].
    // INV: after calling this function, the join handle is None and the client is initialized.
    #[tracing::instrument(
        name = "WalrusNodeClientCreatedInBackground::client",
        skip_all,
        level = Level::DEBUG,
    )]
    pub async fn client(&self) -> ClientResult<&WalrusNodeClient<T>> {
        if let Some(result) = self.client.get() {
            return Self::convert_result_ref(result);
        }
        let start = Instant::now();
        let result = if let Some(join_handle) = self.join_handle.lock().await.take() {
            match join_handle
                .await
                .expect("creating the WalrusNodeClient must not panic")
            {
                Ok(client) => self.client.get_or_init(|| Ok(client)),
                Err(e) => {
                    let _ = self.client.get_or_init(|| Err(e.to_string()));
                    return Err(e);
                }
            }
        } else {
            self.client
                .get()
                .expect("cell must have been initialized if the join handle is no longer set")
        };
        let client = Self::convert_result_ref(result)?;
        tracing::info!(
            total_duration = ?self.start_time.elapsed(),
            waiting_duration = ?start.elapsed(),
            "finished initializing the WalrusNodeClient"
        );
        Ok(client)
    }

    fn convert_result_ref(
        result: &Result<WalrusNodeClient<T>, String>,
    ) -> ClientResult<&WalrusNodeClient<T>> {
        result
            .as_ref()
            .map_err(|e| ClientErrorKind::ClientInitializationError(e.clone()).into())
    }

    /// Returns the [`WalrusNodeClient`].
    ///
    /// If the [`WalrusNodeClient`] is not yet created, this will wait for the background task to
    /// complete.
    ///
    /// # Errors
    ///
    /// If the client is not yet initialized and the background task returns an error, this will be
    /// returned on the first call to this function or [`Self::into_client`]. On subsequent calls,
    /// the error will be returned wrapped into a [`ClientError`] with
    /// [`ClientErrorKind::ClientInitializationError`].
    #[tracing::instrument(skip_all, level = Level::DEBUG)]
    pub async fn into_client(self) -> ClientResult<WalrusNodeClient<T>> {
        if let Some(client) = self.client.into_inner() {
            return client.map_err(|e| ClientErrorKind::ClientInitializationError(e).into());
        }
        let start = Instant::now();
        let join_handle = self
            .join_handle
            .lock()
            .await
            .take()
            .expect("join handle must be Some if the client is not initialized");
        let client = join_handle
            .await
            .expect("creating the WalrusNodeClient must not panic")?;
        tracing::info!(
            total_duration = ?self.start_time.elapsed(),
            waiting_duration = ?start.elapsed(),
            "finished initializing the WalrusNodeClient"
        );
        Ok(client)
    }
}

mod internal {
    use super::*;

    pub trait StoreBlobApiInternal: Sync {
        /// Returns the [`WalrusNodeClient`].
        fn client(
            &self,
        ) -> impl Future<Output = ClientResult<&WalrusNodeClient<SuiContractClient>>> + Send;

        /// Encodes the blobs, reserves & registers the space on chain, stores the slivers to the
        /// storage nodes, and certifies the blobs.
        ///
        /// Returns a vector of [`BlobStoreResult`]s, in the same order as the input blobs. The
        /// length of the output vector is the same as the input vector.
        fn reserve_and_store_blobs_inner(
            &self,
            walrus_store_blobs: Vec<WalrusStoreBlobMaybeFinished<UnencodedBlob>>,
            store_args: &StoreArgs,
            perform_retries: bool,
        ) -> impl Future<Output = ClientResult<Vec<BlobStoreResult>>> + Send {
            async move {
                let blobs_count = walrus_store_blobs.len();
                if blobs_count == 0 {
                    tracing::debug!("no blobs provided to store");
                    return Ok(vec![]);
                }
                let start = Instant::now();

                let upload_relay_client = store_args.upload_relay_client.clone();
                let encoding_event_tx = store_args.encoding_event_tx.clone();
                let maybe_encoded_blobs = tokio::task::spawn_blocking(move || {
                    encode_blobs(
                        walrus_store_blobs,
                        upload_relay_client,
                        encoding_event_tx.as_ref(),
                    )
                })
                .await
                .map_err(ClientError::other)??;
                let (encoded_blobs, mut results) =
                    client_types::partition_unfinished_finished(maybe_encoded_blobs);
                store_args.maybe_observe_encoding_latency(start.elapsed());

                let client = self.client().await?;

                if !encoded_blobs.is_empty() {
                    let store_results = if perform_retries {
                        client
                            .retry_if_error_epoch_change(|| {
                                client.reserve_and_store_encoded_blobs(
                                    encoded_blobs.clone(),
                                    store_args,
                                )
                            })
                            .await?
                    } else {
                        client
                            .reserve_and_store_encoded_blobs(encoded_blobs.clone(), store_args)
                            .await?
                    };
                    results.extend(store_results);
                }

                debug_assert_eq!(results.len(), blobs_count);
                // Make sure the output order is the same as the input order.
                results.sort_by_key(|blob| blob.common.identifier.to_string());

                Ok(results.into_iter().map(|blob| blob.state).collect())
            }
        }
    }
}

/// A trait containing functions to store blobs to Walrus.
pub trait StoreBlobsApi: internal::StoreBlobApiInternal + Sized {
    /// Returns the encoding config used by the client.
    fn encoding_config(&self) -> impl Future<Output = ClientResult<&EncodingConfig>> + Send;

    /// Returns the [`WalrusNodeClient`]. If the [`WalrusNodeClient`] is not yet created, this will
    /// wait for the background task to complete.
    fn client(
        &self,
    ) -> impl Future<Output = ClientResult<&WalrusNodeClient<SuiContractClient>>> + Send {
        internal::StoreBlobApiInternal::client(self)
    }

    /// Stores a list of blobs to Walrus, retrying if it fails because of epoch change.
    #[tracing::instrument(skip_all, fields(blob_id))]
    fn reserve_and_store_blobs_retry_committees(
        &self,
        blobs: Vec<Vec<u8>>,
        attributes: Vec<BlobAttribute>,
        store_args: &StoreArgs,
    ) -> impl Future<Output = ClientResult<Vec<BlobStoreResult>>> + Send {
        async {
            let walrus_store_blobs =
                WalrusStoreBlobMaybeFinished::unencoded_blobs_with_default_identifiers(
                    blobs,
                    attributes,
                    self.encoding_config()
                        .await?
                        .get_for_type(store_args.encoding_type),
                );

            self.reserve_and_store_blobs_inner(walrus_store_blobs, store_args, true)
                .await
        }
    }

    /// Stores a list of blobs to Walrus, retrying if it fails because of epoch change.
    /// Similar to `[Client::reserve_and_store_blobs_retry_committees]`, except the result
    /// includes the corresponding path for blob.
    #[tracing::instrument(skip_all, fields(blob_id))]
    fn reserve_and_store_blobs_retry_committees_with_path(
        &self,
        blobs_with_paths: Vec<(PathBuf, Vec<u8>)>,
        store_args: &StoreArgs,
    ) -> impl Future<Output = ClientResult<Vec<BlobStoreResultWithPath>>> + Send {
        async {
            // Not using Path as identifier because it's not unique.
            let (paths, blobs): (Vec<_>, Vec<_>) = blobs_with_paths.into_iter().unzip();
            let walrus_store_blobs =
                WalrusStoreBlobMaybeFinished::unencoded_blobs_with_default_identifiers(
                    blobs,
                    vec![],
                    self.encoding_config()
                        .await?
                        .get_for_type(store_args.encoding_type),
                );

            let completed_blobs = self
                .reserve_and_store_blobs_inner(walrus_store_blobs, store_args, true)
                .await?;

            Ok(completed_blobs
                .into_iter()
                .zip(paths.into_iter())
                .map(|(blob_store_result, path)| blob_store_result.with_path(path))
                .collect())
        }
    }

    /// Encodes the blob, reserves & registers the space on chain, and stores the slivers to the
    /// storage nodes. Finally, the function aggregates the storage confirmations and posts the
    /// [`ConfirmationCertificate`] on chain.
    #[tracing::instrument(skip_all, fields(blob_id))]
    fn reserve_and_store_blobs(
        &self,
        blobs: Vec<Vec<u8>>,
        store_args: &StoreArgs,
    ) -> impl Future<Output = ClientResult<Vec<BlobStoreResult>>> + Send {
        async {
            let walrus_store_blobs =
                WalrusStoreBlobMaybeFinished::unencoded_blobs_with_default_identifiers(
                    blobs,
                    vec![],
                    self.encoding_config()
                        .await?
                        .get_for_type(store_args.encoding_type),
                );
            self.reserve_and_store_blobs_inner(walrus_store_blobs, store_args, false)
                .await
        }
    }
}

impl internal::StoreBlobApiInternal for WalrusNodeClientCreatedInBackground<SuiContractClient> {
    async fn client(&self) -> ClientResult<&WalrusNodeClient<SuiContractClient>> {
        self.client().await
    }
}
impl StoreBlobsApi for WalrusNodeClientCreatedInBackground<SuiContractClient> {
    async fn encoding_config(&self) -> ClientResult<&EncodingConfig> {
        self.encoding_config()
            .await
            .map_err(|e| ClientError::store_blob_internal(e.to_string()))
    }
}

impl internal::StoreBlobApiInternal for WalrusNodeClient<SuiContractClient> {
    async fn client(&self) -> ClientResult<&WalrusNodeClient<SuiContractClient>> {
        Ok(self)
    }
}

impl StoreBlobsApi for WalrusNodeClient<SuiContractClient> {
    async fn encoding_config(&self) -> ClientResult<&EncodingConfig> {
        Ok(&self.encoding_config)
    }
}

/// Encodes multiple blobs.
///
/// Returns a list of WalrusStoreBlob as the encoded result. The return list
/// is in the same order as the input list.
/// A WalrusStoreBlob::Encoded is returned if the blob is encoded successfully.
/// A WalrusStoreBlob::Failed is returned if the blob fails to encode.
#[tracing::instrument(skip_all, fields(count = walrus_store_blobs.len()))]
pub fn encode_blobs(
    walrus_store_blobs: Vec<WalrusStoreBlobMaybeFinished<UnencodedBlob>>,
    upload_relay_client: Option<Arc<UploadRelayClient>>,
    encoding_event_tx: Option<&tokio::sync::mpsc::UnboundedSender<EncodingProgressEvent>>,
) -> ClientResult<Vec<WalrusStoreBlobMaybeFinished<EncodedBlob>>> {
    let total_blobs_count = walrus_store_blobs.len();
    if total_blobs_count == 0 {
        return Ok(Vec::new());
    }

    let multi_pb = Arc::new(MultiProgress::new());
    let parent = tracing::span::Span::current();
    let start = Instant::now();
    let encoding_event_tx = encoding_event_tx.cloned();
    if let Some(tx) = encoding_event_tx.as_ref()
        && let Err(error) = tx.send(EncodingProgressEvent::Started {
            total: total_blobs_count,
        })
    {
        tracing::warn!(%error, "failed to send encoding started event")
    }
    let completed_blobs_count = Arc::new(AtomicUsize::new(0));
    let show_spinner = encoding_event_tx.is_none();

    // Encode each blob into sliver pairs and metadata. Filters out failed blobs and continue.
    let blobs = walrus_store_blobs
        .into_par_iter()
        .map(|blob| {
            let _entered =
                tracing::info_span!(parent: parent.clone(), "encode_blobs__par_iter").entered();
            let encoding_config = blob.common.encoding_config;
            let encode_fn = |blob: UnencodedBlob| {
                encode_blob(
                    blob,
                    encoding_config,
                    multi_pb.as_ref(),
                    upload_relay_client.clone(),
                    show_spinner,
                )
            };
            let encode_result = blob.map(encode_fn, "encode");

            if let Some(tx) = encoding_event_tx.as_ref() {
                let finished_blobs_count = completed_blobs_count.fetch_add(1, Ordering::SeqCst) + 1;
                if let Err(err) = tx.send(EncodingProgressEvent::BlobCompleted {
                    completed: finished_blobs_count,
                    total: total_blobs_count,
                }) {
                    tracing::warn!(%err, "failed to send encoding blob completed event");
                }
            }
            encode_result
        })
        .collect();

    if let Some(tx) = encoding_event_tx
        && let Err(error) = tx.send(EncodingProgressEvent::Finished)
    {
        tracing::warn!(%error, "failed to send encoding finished event");
    }
    tracing::info!(
        duration = ?start.elapsed(),
        "finished blob encoding",
    );
    blobs
}

fn encode_blob(
    blob: UnencodedBlob,
    encoding_config: EncodingConfigEnum,
    multi_pb: &MultiProgress,
    upload_relay_client: Option<Arc<UploadRelayClient>>,
    show_spinner: bool,
) -> ClientResult<EncodedBlob> {
    let spinner = show_spinner.then(|| {
        let spinner = multi_pb.add(styled_spinner());
        spinner.set_message("encoding the blob");
        spinner
    });
    let encode_start_timer = Instant::now();

    let encoded_blob = blob.encode(encoding_config, upload_relay_client)?;

    tracing::debug!(
        ?encoded_blob,
        duration = ?encode_start_timer.elapsed().as_millis(),
        "blob encoded"
    );
    if let Some(spinner) = spinner {
        spinner.finish_with_message(format!("blob encoded; blob ID: {}", encoded_blob.blob_id()));
    }

    Ok(encoded_blob)
}

/// Verifies the [`BlobStatus`] using the on-chain event.
///
/// This only verifies the [`BlobStatus::Invalid`] and [`BlobStatus::Permanent`] variants and does
/// not check the quoted counts for deletable blobs.
#[tracing::instrument(skip(sui_read_client), err(level = Level::WARN))]
async fn verify_blob_status_event(
    blob_id: &BlobId,
    status: BlobStatus,
    sui_read_client: &impl ReadClient,
) -> Result<(), anyhow::Error> {
    let event = match status {
        BlobStatus::Invalid { event } => event,
        BlobStatus::Permanent { status_event, .. } => status_event,
        BlobStatus::Deletable { .. } => {
            bail!("deletable status cannot be verified with an on-chain event")
        }
        BlobStatus::Nonexistent => return Ok(()),
    };
    tracing::debug!(?event, "verifying blob status with on-chain event");

    let blob_event = sui_read_client.get_blob_event(event).await?;
    anyhow::ensure!(blob_id == &blob_event.blob_id(), "blob ID mismatch");

    match (status, blob_event) {
        (
            BlobStatus::Permanent {
                end_epoch,
                is_certified: false,
                ..
            },
            BlobEvent::Registered(event),
        ) => {
            anyhow::ensure!(end_epoch == event.end_epoch, "end epoch mismatch");
            event.blob_id
        }
        (
            BlobStatus::Permanent {
                end_epoch,
                is_certified: true,
                ..
            },
            BlobEvent::Certified(event),
        ) => {
            anyhow::ensure!(end_epoch == event.end_epoch, "end epoch mismatch");
            event.blob_id
        }
        (BlobStatus::Invalid { .. }, BlobEvent::InvalidBlobID(event)) => event.blob_id,
        (_, _) => Err(anyhow!("blob event does not match status"))?,
    };

    Ok(())
}
