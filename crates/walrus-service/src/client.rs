// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Client for the Walrus service.

use std::{collections::HashMap, fmt::Display, sync::Arc, time::Instant};

use anyhow::anyhow;
use communication::NodeCommunicationFactory;
use fastcrypto::{bls12381::min_pk::BLS12381AggregateSignature, traits::AggregateAuthenticator};
use futures::Future;
use metrics::ClientMetricSet;
use prometheus::Registry;
use resource::{PriceComputation, ResourceManager, StoreOp};
use sui_types::base_types::ObjectID;
use tokio::{
    sync::{RwLock, Semaphore},
    time::Duration,
};
use tracing::{Instrument, Level};
use walrus_core::{
    encoding::{BlobDecoder, EncodingAxis, EncodingConfig, SliverData, SliverPair},
    ensure,
    messages::{Confirmation, ConfirmationCertificate, SignedStorageConfirmation},
    metadata::VerifiedBlobMetadataWithId,
    BlobId,
    Epoch,
    EpochCount,
    Sliver,
};
use walrus_sdk::{api::BlobStatus, error::NodeError};
use walrus_sui::{
    client::{BlobPersistence, ContractClient, ReadClient},
    types::{Blob, BlobEvent, StakedWal},
};

use self::{
    communication::NodeResult,
    config::CommunicationLimits,
    responses::BlobStoreResult,
    utils::{CompletedReasonWeight, WeightedFutures},
};
use crate::{common::active_committees::ActiveCommittees, utils::ExponentialBackoff};

pub mod cli;
pub mod responses;

mod blocklist;
pub use blocklist::Blocklist;

mod communication;

mod config;
pub use config::{default_configuration_paths, ClientCommunicationConfig, Config};

mod daemon;
pub use daemon::ClientDaemon;

mod error;
pub use error::{ClientError, ClientErrorKind};

mod resource;

mod utils;
pub use utils::string_prefix;

mod metrics;

type ClientResult<T> = Result<T, ClientError>;

/// Represents how the store operation should be carried out by the client.
#[derive(Debug, Clone, Copy)]
pub enum StoreWhen {
    /// Store the blob always, without checking the status.
    Always,
    /// Check the status of the blob before storing it, and store it only if it is not already.
    NotStored,
}

impl StoreWhen {
    /// Returns `true` if the operation is [`Self::Always`].
    pub fn is_store_always(&self) -> bool {
        matches!(self, StoreWhen::Always)
    }

    /// Returns [`Self`] based on the value of a `force` flag.
    pub fn always(force: bool) -> Self {
        if force {
            Self::Always
        } else {
            Self::NotStored
        }
    }
}

/// A client to communicate with Walrus shards and storage nodes.
#[derive(Debug, Clone)]
pub struct Client<T> {
    config: Config,
    sui_client: T,
    committees: Arc<RwLock<ActiveCommittees>>,
    price_computation: PriceComputation,
    communication_limits: CommunicationLimits,
    // The `Arc` is used to share the encoding config with the `communication_factory` without
    // introducing lifetimes.
    encoding_config: Arc<EncodingConfig>,
    blocklist: Option<Blocklist>,
    communication_factory: NodeCommunicationFactory,
    metrics: Option<ClientMetricSet>,
}

impl Client<()> {
    /// Creates a new Walrus client without a Sui client.
    pub async fn new(config: Config, sui_read_client: &impl ReadClient) -> ClientResult<Self> {
        tracing::debug!(?config, "running client");

        let committees = ActiveCommittees::from_committees_and_state(
            sui_read_client
                .get_committees_and_state()
                .await
                .map_err(ClientError::other)?,
        );

        let (storage_price, write_price) = sui_read_client
            .storage_and_write_price_per_unit_size()
            .await?;
        let price_computation = PriceComputation::new(storage_price, write_price);

        let encoding_config = EncodingConfig::new(committees.n_shards());

        let committees = Arc::new(RwLock::new(committees));
        let communication_limits =
            CommunicationLimits::new(&config.communication_config, encoding_config.n_shards());

        let encoding_config = Arc::new(encoding_config);

        Ok(Self {
            sui_client: (),
            committees: committees.clone(),
            price_computation,
            encoding_config: encoding_config.clone(),
            communication_limits,
            blocklist: None,
            communication_factory: NodeCommunicationFactory::new(
                config.communication_config.clone(),
                encoding_config,
            ),
            config,
            metrics: None,
        })
    }

    /// Converts `self` to a [`Client::<T>`] by adding the `sui_client`.
    pub async fn with_client<C>(self, sui_client: C) -> Client<C> {
        let Self {
            config,
            sui_client: _,
            committees,
            price_computation,
            encoding_config,
            communication_limits,
            blocklist,
            communication_factory: node_client_factory,
            metrics,
        } = self;
        Client::<C> {
            config,
            sui_client,
            committees,
            price_computation,
            encoding_config,
            communication_limits,
            blocklist,
            communication_factory: node_client_factory,
            metrics,
        }
    }
}
impl<T> Client<T> {
    /// Sets the metric registry used by the client.
    pub fn set_metric_registry(&mut self, registry: &Registry) {
        let metrics = ClientMetricSet::new(registry);

        // Since the metrics have just been set, update them with the stored committee if possible.
        // We use try_read as this is called during the 'construction' phase and it's unlikely that
        // there is a write-lock necessitating the `.await`. Even if this fails, the daemon will
        // eventually refresh the committee and log the state.
        if let Ok(committees_guard) = self.committees.try_read() {
            metrics.current_epoch.set(committees_guard.epoch());
            metrics
                .current_epoch_state
                .set_from_committees(&committees_guard);
        }
        self.metrics = Some(metrics);
    }
}

impl<T: ReadClient> Client<T> {
    /// Creates a new read client starting from a config file.
    pub async fn new_read_client(config: Config, sui_read_client: T) -> ClientResult<Self> {
        Ok(Client::new(config, &sui_read_client)
            .await?
            .with_client(sui_read_client)
            .await)
    }

    /// Reconstructs the blob by reading slivers from Walrus shards.
    ///
    /// The operation is retried if epoch it fails due to epoch change.
    pub async fn read_blob_retry_epoch<U>(&self, blob_id: &BlobId) -> ClientResult<Vec<u8>>
    where
        U: EncodingAxis,
        SliverData<U>: TryFrom<Sliver>,
    {
        retry_if_epoch_change(self, || self.read_blob::<U>(blob_id)).await
    }

    /// Reconstructs the blob by reading slivers from Walrus shards.
    #[tracing::instrument(level = Level::ERROR, skip_all, fields(%blob_id))]
    pub async fn read_blob<U>(&self, blob_id: &BlobId) -> ClientResult<Vec<u8>>
    where
        U: EncodingAxis,
        SliverData<U>: TryFrom<Sliver>,
    {
        tracing::debug!("starting to read blob");
        self.check_blob_id(blob_id)?;

        let certified_epoch = if self.committees.read().await.is_change_in_progress() {
            tracing::info!("epoch change in progress, reading from initial certified epoch");
            self.get_blob_status_with_retries(blob_id, &self.sui_client)
                .await?
                .initial_certified_epoch()
                .ok_or_else(|| ClientError::from(ClientErrorKind::BlobIdDoesNotExist))?
        } else {
            // We are not during epoch change, we can read from the current epoch directly.
            self.committees.read().await.epoch()
        };

        // Return early if the committee is behind.
        if certified_epoch > self.committees.read().await.epoch() {
            return Err(ClientError::from(ClientErrorKind::BehindCurrentEpoch {
                client_epoch: self.committees.read().await.epoch(),
                certified_epoch,
            }));
        }

        self.read_metadata_and_slivers::<U>(certified_epoch, blob_id)
            .await
    }

    async fn read_metadata_and_slivers<U>(
        &self,
        certified_epoch: Epoch,
        blob_id: &BlobId,
    ) -> ClientResult<Vec<u8>>
    where
        U: EncodingAxis,
        SliverData<U>: TryFrom<Sliver>,
    {
        let metadata = self.retrieve_metadata(certified_epoch, blob_id).await?;
        self.request_slivers_and_decode::<U>(certified_epoch, &metadata)
            .await
    }

    /// Fetches again the current committees from chain.
    ///
    /// The provided `refresh_epoch` is the epoch at which the committee was when the function was
    /// called. Since may concurrent calls to this function may be made, we fist check if
    /// `self.committees` was already updated.
    ///
    /// This works because the `tokio::sync::RwLock` is write preferring, and all read locks are
    /// delayed as soon as the first write lock is scheduled.
    pub async fn refresh_committees(&self, refresh_epoch: Epoch) -> ClientResult<()> {
        if self.committees.read().await.epoch() > refresh_epoch {
            // Another thread has already updated the committees, no need call the chain and get
            // the write lock.
            return Ok(());
        }

        let new_committees = ActiveCommittees::from_committees_and_state(
            self.sui_client
                .get_committees_and_state()
                .await
                .map_err(ClientError::other)?,
        );

        // Acquire the guard across updating the metric.
        let mut committee_guard = self.committees.write().await;

        // Update the metrics to reflect the new committee.
        if let Some(metrics) = self.metrics.as_ref() {
            metrics.current_epoch.set(new_committees.epoch());
            metrics
                .current_epoch_state
                .set_from_committees(&new_committees);
        }

        *committee_guard = new_committees;

        Ok(())
    }
}

impl<T: ContractClient> Client<T> {
    /// Creates a new client starting from a config file.
    pub async fn new_contract_client(config: Config, sui_client: T) -> ClientResult<Self> {
        Ok(Client::new(config, sui_client.read_client())
            .await?
            .with_client(sui_client)
            .await)
    }

    /// Stores the blob to Walrus, retrying if it fails because of epoch change.
    #[tracing::instrument(skip_all, fields(blob_id))]
    pub async fn reserve_and_store_blob_retry_epoch(
        &self,
        blob: &[u8],
        epochs_ahead: EpochCount,
        store_when: StoreWhen,
        persistence: BlobPersistence,
    ) -> ClientResult<BlobStoreResult> {
        let (pairs, metadata) = self.encode_pairs_and_metadata(blob).await?;

        retry_if_epoch_change(self, || {
            self.reserve_and_store_encoded_blob(
                &pairs,
                &metadata,
                epochs_ahead,
                store_when,
                persistence,
            )
        })
        .await
    }

    /// Encodes the blob, reserves & registers the space on chain, and stores the slivers to the
    /// storage nodes. Finally, the function aggregates the storage confirmations and posts the
    /// [`ConfirmationCertificate`] on chain.
    #[tracing::instrument(skip_all, fields(blob_id))]
    pub async fn reserve_and_store_blob(
        &self,
        blob: &[u8],
        epochs_ahead: EpochCount,
        store_when: StoreWhen,
        persistence: BlobPersistence,
    ) -> ClientResult<BlobStoreResult> {
        let (pairs, metadata) = self.encode_pairs_and_metadata(blob).await?;

        self.reserve_and_store_encoded_blob(
            &pairs,
            &metadata,
            epochs_ahead,
            store_when,
            persistence,
        )
        .await
    }

    async fn encode_pairs_and_metadata(
        &self,
        blob: &[u8],
    ) -> ClientResult<(Vec<SliverPair>, VerifiedBlobMetadataWithId)> {
        let encode_start_timer = Instant::now();

        let (pairs, metadata) = self
            .encoding_config
            .get_blob_encoder(blob)
            .map_err(ClientError::other)?
            .encode_with_metadata();

        let encode_duration = encode_start_timer.elapsed();

        let pair = pairs.first().expect("the encoding produces sliver pairs");
        let symbol_size = pair.primary.symbols.symbol_size().get();
        tracing::debug!(
            symbol_size=%symbol_size,
            primary_sliver_size=%pair.primary.symbols.len() * usize::from(symbol_size),
            secondary_sliver_size=%pair.secondary.symbols.len() * usize::from(symbol_size),
            duration = %humantime::Duration::from(encode_duration),
            "computed sliver pairs and metadata"
        );

        Ok((pairs, metadata))
    }

    async fn reserve_and_store_encoded_blob(
        &self,
        pairs: &[SliverPair],
        metadata: &VerifiedBlobMetadataWithId,
        epochs_ahead: EpochCount,
        store_when: StoreWhen,
        persistence: BlobPersistence,
    ) -> ClientResult<BlobStoreResult> {
        let blob_id = *metadata.blob_id();
        self.check_blob_id(&blob_id)?;
        tracing::Span::current().record("blob_id", blob_id.to_string());

        let blob_status = self
            .get_blob_status_with_retries(&blob_id, &self.sui_client)
            .await?;

        let store_operation = self
            .resource_manager()
            .await
            .store_operation_for_blob(metadata, epochs_ahead, persistence, store_when, blob_status)
            .await?;

        let (mut blob_object, resource_operation) = match store_operation {
            StoreOp::NoOp(result) => return Ok(result),
            StoreOp::RegisterNew { blob, operation } => (blob, operation),
        };

        let committees = self.committees.read().await;

        let certificate = match blob_status.initial_certified_epoch() {
            Some(certified_epoch) if !committees.is_change_in_progress() => {
                // The blob is already certified on chain: the slivers are already available.
                // However, during epoch change we may need to store the slivers again, as the
                // current committee may not have synced them yet.
                self.get_certificate_standalone(&blob_id, certified_epoch)
                    .await?
            }
            _ => {
                let certify_start_timer = Instant::now();
                let result = self
                    .send_blob_data_and_get_certificate(metadata, pairs)
                    .await?;
                let certify_duration = certify_start_timer.elapsed();
                let blob_size = blob_object.size;
                tracing::info!(
                    duration =  %humantime::Duration::from(certify_duration),
                    blob_size = blob_size,
                    "finished sending blob data and collected certificate"
                );
                result
            }
        };

        self.sui_client
            .certify_blob(blob_object.clone(), &certificate)
            .await
            .map_err(|e| ClientError::from(ClientErrorKind::CertificationFailed(e)))?;
        blob_object.certified_epoch = Some(committees.write_committee().epoch);
        let cost = self.price_computation.operation_cost(&resource_operation);

        Ok(BlobStoreResult::NewlyCreated {
            blob_object,
            resource_operation,
            cost,
        })
    }

    /// Creates a resource manager for the client.
    pub async fn resource_manager(&self) -> ResourceManager<T> {
        ResourceManager::new(
            &self.sui_client,
            self.committees.read().await.write_committee().epoch,
        )
    }

    // Blob deletion

    /// Returns an iterator over the list of blobs that can be deleted, based on the blob ID.
    pub async fn deletable_blobs_by_id<'a>(
        &self,
        blob_id: &'a BlobId,
    ) -> ClientResult<impl Iterator<Item = Blob> + 'a> {
        Ok(self
            .sui_client
            .owned_blobs(false)
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

    /// Stakes the specified amount of WAL with the node represented by `node_id`.
    pub async fn stake_with_node_pool(
        &self,
        node_id: ObjectID,
        amount: u64,
    ) -> ClientResult<StakedWal> {
        let staked_wal = self.sui_client.stake_with_pool(amount, node_id).await?;
        Ok(staked_wal)
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
}

impl<T> Client<T> {
    /// Adds a [`Blocklist`] to the client that will be checked when storing or reading blobs.
    ///
    /// This can be called again to replace the blocklist.
    pub fn with_blocklist(mut self, blocklist: Blocklist) -> Self {
        self.blocklist = Some(blocklist);
        self
    }

    /// Stores the already-encoded metadata and sliver pairs for a blob into Walrus, by sending
    /// sliver pairs to at least 2f+1 shards.
    ///
    /// Assumes the blob ID has already been registered, with an appropriate blob size.
    #[tracing::instrument(skip_all)]
    pub async fn send_blob_data_and_get_certificate(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        pairs: &[SliverPair],
    ) -> ClientResult<ConfirmationCertificate> {
        tracing::info!("starting to send data to storage nodes");
        let mut pairs_per_node = self.pairs_per_node(metadata.blob_id(), pairs).await;
        let sliver_write_limit = self
            .communication_limits
            .max_concurrent_sliver_writes_for_blob_size(
                metadata.metadata().unencoded_length,
                &self.encoding_config,
            );
        tracing::debug!(
            communication_limits = sliver_write_limit,
            "establishing node communications"
        );

        let committees = self.committees.read().await;
        let comms = self
            .communication_factory
            .node_write_communications(&committees, Arc::new(Semaphore::new(sliver_write_limit)))?;

        let mut requests = WeightedFutures::new(comms.iter().map(|n| {
            n.store_metadata_and_pairs(
                metadata,
                pairs_per_node
                    .remove(&n.node_index)
                    .expect("there are shards for each node"),
            )
        }));
        let start = Instant::now();

        // We do not limit the number of concurrent futures awaited here, because the number of
        // connections is limited through a semaphore depending on the [`max_data_in_flight`][]
        if let CompletedReasonWeight::FuturesConsumed(weight) = requests
            .execute_weight(
                &|weight| {
                    committees
                        .write_committee()
                        .is_at_least_min_n_correct(weight)
                },
                committees.n_shards().get().into(),
            )
            .await
        {
            tracing::debug!(
                elapsed_time = ?start.elapsed(),
                executed_weight = weight,
                responses = ?requests.into_results(),
                "all futures consumed before reaching a threshold of successful responses"
            );
            return Err(self.not_enough_confirmations_error(weight).await);
        }
        tracing::debug!(
            elapsed_time = ?start.elapsed(), "stored metadata and slivers onto a quorum of nodes"
        );

        // Allow extra time for the client to store the slivers.
        let completed_reason = requests
            .execute_time(
                self.config
                    .communication_config
                    .sliver_write_extra_time
                    .extra_time(start.elapsed()),
                committees.n_shards().get().into(),
            )
            .await;
        tracing::debug!(
            elapsed_time = ?start.elapsed(),
            %completed_reason,
            "stored metadata and slivers onto additional nodes"
        );
        let results = requests.into_results();
        self.confirmations_to_certificate(metadata.blob_id(), results)
            .await
    }

    /// Fetches confirmations for a blob from a quorum of nodes and returns the certificate.
    async fn get_certificate_standalone(
        &self,
        blob_id: &BlobId,
        certified_epoch: Epoch,
    ) -> ClientResult<ConfirmationCertificate> {
        let committees = self.committees.read().await;
        let comms = self
            .communication_factory
            .node_read_communications(&committees, certified_epoch)?;

        let mut requests = WeightedFutures::new(
            comms
                .iter()
                .map(|n| n.get_confirmation_with_retries(blob_id, committees.epoch())),
        );

        requests
            .execute_weight(
                &|weight| committees.is_quorum(weight),
                self.communication_limits.max_concurrent_sliver_reads,
            )
            .await;
        let results = requests.into_results();
        self.confirmations_to_certificate(blob_id, results).await
    }

    /// Combines the received storage confirmations into a single certificate.
    ///
    /// This function _does not_ check that the received confirmations match the current epoch and
    /// blob ID, as it assumes that the storage confirmations were received through
    /// [`NodeCommunication::store_metadata_and_pairs`], which internally verifies it to check the
    /// blob ID and epoch.
    async fn confirmations_to_certificate<E: Display>(
        &self,
        blob_id: &BlobId,
        confirmations: Vec<NodeResult<SignedStorageConfirmation, E>>,
    ) -> ClientResult<ConfirmationCertificate> {
        let mut aggregate_weight = 0;
        let mut signers = Vec::with_capacity(confirmations.len());
        let mut valid_signatures = Vec::with_capacity(confirmations.len());
        for NodeResult(_, weight, node, result) in confirmations {
            match result {
                Ok(confirmation) => {
                    aggregate_weight += weight;
                    valid_signatures.push(confirmation.signature);
                    signers.push(
                        u16::try_from(node)
                            .expect("the node index is computed from the vector of members"),
                    );
                }
                Err(error) => tracing::warn!(node, %error, "storing metadata and pairs failed"),
            }
        }

        let committees = self.committees.read().await;
        ensure!(
            committees
                .write_committee()
                .is_at_least_min_n_correct(aggregate_weight),
            self.not_enough_confirmations_error(aggregate_weight).await
        );

        let aggregate =
            BLS12381AggregateSignature::aggregate(&valid_signatures).map_err(ClientError::other)?;
        let cert = ConfirmationCertificate::new(
            signers,
            bcs::to_bytes(&Confirmation::new(
                committees.write_committee().epoch,
                *blob_id,
            ))
            .expect("serialization should always succeed"),
            aggregate,
        );
        Ok(cert)
    }

    async fn not_enough_confirmations_error(&self, weight: usize) -> ClientError {
        ClientErrorKind::NotEnoughConfirmations(
            weight,
            self.committees
                .read()
                .await
                .write_committee()
                .min_n_correct(),
        )
        .into()
    }

    /// Requests the slivers and decodes them into a blob.
    ///
    /// Returns a [`ClientError`] of kind [`ClientErrorKind::BlobIdDoesNotExist`] if it receives a
    /// quorum (at least 2f+1) of "not found" error status codes from the storage nodes.
    #[tracing::instrument(level = Level::ERROR, skip_all)]
    async fn request_slivers_and_decode<U>(
        &self,
        certified_epoch: Epoch,
        metadata: &VerifiedBlobMetadataWithId,
    ) -> ClientResult<Vec<u8>>
    where
        U: EncodingAxis,
        SliverData<U>: TryFrom<Sliver>,
    {
        let committees = self.committees.read().await;

        let comms = self
            .communication_factory
            .node_read_communications(&committees, certified_epoch)?;
        // Create requests to get all slivers from all nodes.
        let futures = comms.iter().flat_map(|n| {
            // NOTE: the cloned here is needed because otherwise the compiler complains about the
            // lifetimes of `s`.
            n.node.shard_ids.iter().cloned().map(|s| {
                n.retrieve_verified_sliver::<U>(metadata, s)
                    .instrument(n.span.clone())
            })
        });
        let mut decoder = self
            .encoding_config
            .get_blob_decoder::<U>(metadata.metadata().unencoded_length)
            .map_err(ClientError::other)?;
        // Get the first ~1/3 or ~2/3 of slivers directly, and decode with these.
        let mut requests = WeightedFutures::new(futures);
        let enough_source_symbols =
            |weight| weight >= self.encoding_config.n_source_symbols::<U>().get().into();
        requests
            .execute_weight(
                &enough_source_symbols,
                self.communication_limits
                    .max_concurrent_sliver_reads_for_blob_size(
                        metadata.metadata().unencoded_length,
                        &self.encoding_config,
                    ),
            )
            .await;

        let mut n_not_found = 0; // Counts the number of "not found" status codes received.
        let slivers = requests
            .take_results()
            .into_iter()
            .filter_map(|NodeResult(_, _, node, result)| {
                result
                    .map_err(|error| {
                        tracing::warn!(%node, %error, "retrieving sliver failed");
                        if error.is_status_not_found() {
                            n_not_found += 1;
                        }
                    })
                    .ok()
            })
            .collect::<Vec<_>>();

        if committees.is_quorum(n_not_found) {
            return Err(ClientErrorKind::BlobIdDoesNotExist.into());
        }

        if let Some((blob, _meta)) = decoder
            .decode_and_verify(metadata.blob_id(), slivers)
            .map_err(ClientError::other)?
        {
            // We have enough to decode the blob.
            Ok(blob)
        } else {
            // We were not able to decode. Keep requesting slivers and try decoding as soon as every
            // new sliver is received.
            self.decode_sliver_by_sliver(&mut requests, &mut decoder, metadata, n_not_found)
                .await
        }
    }

    /// Decodes the blob of given blob ID by requesting slivers and trying to decode at each new
    /// sliver it receives.
    #[tracing::instrument(level = Level::ERROR, skip_all)]
    async fn decode_sliver_by_sliver<'a, I, Fut, U>(
        &self,
        requests: &mut WeightedFutures<I, Fut, NodeResult<SliverData<U>, NodeError>>,
        decoder: &mut BlobDecoder<'a, U>,
        metadata: &VerifiedBlobMetadataWithId,
        mut n_not_found: usize,
    ) -> ClientResult<Vec<u8>>
    where
        U: EncodingAxis,
        I: Iterator<Item = Fut>,
        Fut: Future<Output = NodeResult<SliverData<U>, NodeError>>,
    {
        while let Some(NodeResult(_, _, node, result)) = requests
            .next(
                self.communication_limits
                    .max_concurrent_sliver_reads_for_blob_size(
                        metadata.metadata().unencoded_length,
                        &self.encoding_config,
                    ),
            )
            .await
        {
            match result {
                Ok(sliver) => {
                    let result = decoder
                        .decode_and_verify(metadata.blob_id(), [sliver])
                        .map_err(ClientError::other)?;
                    if let Some((blob, _meta)) = result {
                        return Ok(blob);
                    }
                }
                Err(error) => {
                    tracing::warn!(%node, %error, "retrieving sliver failed");
                    if error.is_status_not_found() && {
                        n_not_found += 1;
                        self.committees.read().await.is_quorum(n_not_found)
                    } {
                        return Err(ClientErrorKind::BlobIdDoesNotExist.into());
                    }
                }
            }
        }
        // We have exhausted all the slivers but were not able to reconstruct the blob.
        Err(ClientErrorKind::NotEnoughSlivers.into())
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
        let committees = self.committees.read().await;
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
        requests
            .execute_weight(
                &just_one,
                self.communication_limits.max_concurrent_metadata_reads,
            )
            .await;

        let mut n_not_found = 0;
        for NodeResult(_, weight, node, result) in requests.into_results() {
            match result {
                Ok(metadata) => {
                    tracing::debug!(?node, "metadata received");
                    return Ok(metadata);
                }
                Err(error) => {
                    if error.is_status_not_found() && {
                        n_not_found += weight;
                        committees.is_quorum(n_not_found)
                    } {
                        // TODO(giac): now that we check that the blob is certified before starting
                        // to read, this error should not technically happen unless (1) the client
                        // was disconnected while reading, or (2) the bft threshold was exceeded.
                        return Err(ClientErrorKind::BlobIdDoesNotExist.into());
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
        let config = &self.config.communication_config.request_rate_config;
        let backoff = ExponentialBackoff::new_with_seed(
            config.min_backoff,
            config.max_backoff,
            config.max_retries,
            u64::from_le_bytes(
                blob_id.0[..8]
                    .try_into()
                    .expect("can always convert 8 bytes into u64"),
            ),
        );

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
                    return Err(client_error)
                }
                Err(_) => (),
            };

            if peekable.peek().is_some() {
                tracing::debug!(?delay, "fetching blob status failed; retrying after delay");
                tokio::time::sleep(delay).await;
            } else {
                tracing::warn!("fetching blob status failed; no more retries");
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
        let committees = self.committees.read().await;

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
            .filter(|err| err.is_status_not_found())
            .count();
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
    fn check_blob_id(&self, blob_id: &BlobId) -> ClientResult<()> {
        if let Some(blocklist) = &self.blocklist {
            if blocklist.is_blocked(blob_id) {
                tracing::debug!(%blob_id, "encountered blocked blob ID");
                return Err(ClientErrorKind::BlobIdBlocked(*blob_id).into());
            }
        }
        Ok(())
    }

    /// Maps the sliver pairs to the node in the write committee that holds their shard.
    async fn pairs_per_node<'a>(
        &'a self,
        blob_id: &'a BlobId,
        pairs: &'a [SliverPair],
    ) -> HashMap<usize, Vec<&SliverPair>> {
        let committees = self.committees.read().await;
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
                    .collect()
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
        BlobStatus::Nonexistent | BlobStatus::Deletable { .. } => return Ok(()),
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

/// Retries the given function if the error may be caused by an epoch change, after refreshing the
/// set of active committees.
async fn retry_if_epoch_change<T, F, R, Fut>(client: &Client<T>, func: F) -> ClientResult<R>
where
    T: ReadClient,
    F: Fn() -> Fut,
    Fut: Future<Output = ClientResult<R>>,
{
    let current_epoch = client.committees.read().await.epoch();
    let result = func().await;
    match result {
        Err(error) if error.may_be_caused_by_epoch_change() => {
            tracing::warn!(
                %error,
                "an error occurred during the current operation, \
                which may be caused by epoch change; refreshing the committees and retrying"
            );
            client.refresh_committees(current_epoch).await?;
            func().await
        }
        result => result,
    }
}
