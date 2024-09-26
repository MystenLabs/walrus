// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Client for the Walrus service.

use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{Arc, Mutex},
    time::Instant,
};

use anyhow::anyhow;
use fastcrypto::{bls12381::min_pk::BLS12381AggregateSignature, traits::AggregateAuthenticator};
use futures::Future;
use rand::{seq::SliceRandom, thread_rng};
use reqwest::Client as ReqwestClient;
use sui_types::base_types::ObjectID;
use tokio::{sync::Semaphore, time::Duration};
use tracing::{Instrument, Level};
use walrus_core::{
    encoding::{
        encoded_blob_length_for_n_shards,
        BlobDecoder,
        EncodingAxis,
        EncodingConfig,
        SliverData,
        SliverPair,
    },
    ensure,
    messages::{Confirmation, ConfirmationCertificate, SignedStorageConfirmation},
    metadata::VerifiedBlobMetadataWithId,
    BlobId,
    EpochCount,
    NetworkPublicKey,
    Sliver,
};
use walrus_sdk::{
    api::BlobStatus,
    client::{Client as StorageNodeClient, ClientBuilder as StorageNodeClientBuilder},
    error::{ClientBuildError, NodeError},
};
use walrus_sui::{
    client::{BlobPersistence, ContractClient, ReadClient},
    types::{Blob, BlobEvent, Committee, NetworkAddress, StorageNode},
    utils::storage_price_for_encoded_length,
};

use self::{
    communication::{NodeCommunication, NodeReadCommunication, NodeResult, NodeWriteCommunication},
    config::CommunicationLimits,
    error::StoreError,
    responses::BlobStoreResult,
    utils::{CompletedReasonWeight, WeightedFutures},
};
use crate::common::active_committees::ActiveCommittees;

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

mod utils;
pub use utils::string_prefix;

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
    committees: ActiveCommittees,
    storage_price_per_unit_size: u64,
    communication_limits: CommunicationLimits,
    encoding_config: EncodingConfig,
    blocklist: Option<Blocklist>,
    communication_factory: NodeCommunicationFactory,
}

impl Client<()> {
    /// Creates a new read client starting from a config file.
    pub async fn new_read_client(
        config: Config,
        sui_read_client: &impl ReadClient,
    ) -> ClientResult<Self> {
        tracing::debug!(?config, "running client");

        let committees = ActiveCommittees::from_committees_and_state(
            sui_read_client
                .get_committees_and_state()
                .await
                .map_err(ClientError::other)?,
        );

        let storage_price_per_unit_size = sui_read_client
            .storage_price_per_unit_size()
            .await
            .map_err(ClientError::other)?;

        let encoding_config = EncodingConfig::new(committees.n_shards());
        let communication_limits =
            CommunicationLimits::new(&config.communication_config, encoding_config.n_shards());

        Ok(Self {
            sui_client: (),
            committees,
            storage_price_per_unit_size,
            encoding_config,
            communication_limits,
            blocklist: None,
            communication_factory: NodeCommunicationFactory::new(
                config.communication_config.clone(),
            ),
            config,
        })
    }

    /// Converts `self` to a [`Client::<T>`] by adding the `sui_client`.
    pub async fn with_client<T: ContractClient>(self, sui_client: T) -> Client<T> {
        let Self {
            config,
            sui_client: _,
            committees,
            storage_price_per_unit_size,
            encoding_config,
            communication_limits,
            blocklist,
            communication_factory: node_client_factory,
        } = self;
        Client::<T> {
            config,
            sui_client,
            committees,
            storage_price_per_unit_size,
            encoding_config,
            communication_limits,
            blocklist,
            communication_factory: node_client_factory,
        }
    }
}

impl<T: ContractClient> Client<T> {
    /// Creates a new client starting from a config file.
    pub async fn new(config: Config, sui_client: T) -> ClientResult<Self> {
        Ok(Client::new_read_client(config, sui_client.read_client())
            .await?
            .with_client(sui_client)
            .await)
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
        let (pairs, metadata) = self
            .encoding_config
            .get_blob_encoder(blob)
            .map_err(ClientError::other)?
            .encode_with_metadata();
        let blob_id = *metadata.blob_id();
        self.check_blob_id(&blob_id)?;
        tracing::Span::current().record("blob_id", blob_id.to_string());
        let pair = pairs.first().expect("the encoding produces sliver pairs");
        let symbol_size = pair.primary.symbols.symbol_size().get();
        tracing::debug!(
            symbol_size=%symbol_size,
            primary_sliver_size=%pair.primary.symbols.len() * usize::from(symbol_size),
            secondary_sliver_size=%pair.secondary.symbols.len() * usize::from(symbol_size),
            "computed blob pairs and metadata"
        );

        // Return early if the blob is already certified or marked as invalid.
        if !store_when.is_store_always() {
            // Use short timeout as this is only an optimization.
            const STATUS_TIMEOUT: Duration = Duration::from_secs(5);

            match self
                .get_verified_blob_status(
                    metadata.blob_id(),
                    self.sui_client.read_client(),
                    STATUS_TIMEOUT,
                )
                .await?
            {
                BlobStatus::Permanent {
                    end_epoch,
                    is_certified: true,
                    status_event,
                    ..
                } => {
                    if end_epoch >= self.committees.current_committee().epoch + epochs_ahead {
                        tracing::debug!(end_epoch, "blob is already certified");
                        return Ok(BlobStoreResult::AlreadyCertified {
                            blob_id,
                            event: status_event,
                            end_epoch,
                        });
                    } else {
                        tracing::debug!(
                        end_epoch,
                        "blob is already certified but its lifetime is too short; creating new one"
                    );
                    }
                }
                BlobStatus::Invalid { event } => {
                    tracing::debug!("blob is marked as invalid");
                    return Ok(BlobStoreResult::MarkedInvalid { blob_id, event });
                }
                status => {
                    // We intentionally don't check for "registered" blobs here: even if the blob is
                    // already registered, we cannot certify it without access to the corresponding
                    // Sui object. The check to see if we own the registered-but-not-certified Blob
                    // object is done in `reserve_and_register_blob`.
                    tracing::debug!(
                        ?status,
                        "no corresponding permanent certified `Blob` object exists"
                    );
                }
            }
        }

        // Get an appropriate registered blob object.
        let mut blob = self
            .get_blob_registration(&metadata, epochs_ahead, persistence)
            .await?;

        // We do not need to wait explicitly as we anyway retry all requests to storage nodes.
        let certificate = self
            .send_blob_data_and_get_certificate(&metadata, &pairs)
            .await?;
        self.sui_client
            .certify_blob(blob.clone(), &certificate)
            .await
            .map_err(|e| ClientError::from(ClientErrorKind::CertificationFailed(e)))?;

        // TODO(mlegner): Make sure this works if the epoch changes. (#753)
        blob.certified_epoch = Some(self.committees.current_committee().epoch);

        let encoded_size =
            encoded_blob_length_for_n_shards(self.encoding_config.n_shards(), blob.size)
                .expect("must be valid as the store succeeded");
        // TODO(mlegner): Fix prices. (#820)
        let cost = storage_price_for_encoded_length(
            encoded_size,
            self.storage_price_per_unit_size,
            epochs_ahead,
        );
        Ok(BlobStoreResult::NewlyCreated {
            blob_object: blob,
            encoded_size,
            cost,
            deletable: persistence.is_deletable(),
        })
    }

    /// Returns a [`Blob`] registration object for the specified metadata and number of epochs.
    ///
    /// Tries to reuse existing blob registrations or storage resources if possible.
    /// Specifically:
    /// - First, it checks if the blob is registered and returns the corresponding [`Blob`];
    /// - otherwise, it checks if there is an appropriate storage resource (with sufficient space
    ///   and for a sufficient duration) that can be used to register the blob; or
    /// - if the above fails, it purchases a new storage resource and registers the blob.
    #[tracing::instrument(skip_all, err(level = Level::DEBUG))]
    pub async fn get_blob_registration(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        epochs_ahead: EpochCount,
        persistence: BlobPersistence,
    ) -> ClientResult<Blob> {
        let blob = if let Some(blob) = self
            .is_blob_registered_in_wallet(metadata.blob_id(), epochs_ahead, persistence)
            .await?
        {
            tracing::debug!(
                end_epoch=%blob.storage.end_epoch,
                "blob is already registered and valid; using the existing registration"
            );
            blob
        } else if let Some(storage_resource) = self
            .sui_client
            .owned_storage_for_size_and_epoch(
                metadata.metadata().encoded_size().ok_or_else(|| {
                    ClientError::other(ClientErrorKind::Other(
                        anyhow!(
                            "the provided metadata is invalid: could not compute the encoded size"
                        )
                        .into(),
                    ))
                })?,
                epochs_ahead + self.committees.current_committee().epoch,
            )
            .await?
        {
            tracing::debug!(
                storage_object=%storage_resource.id,
                "using an existing storage resource to register the blob"
            );
            self.sui_client
                .register_blob(
                    &storage_resource,
                    *metadata.blob_id(),
                    metadata.metadata().compute_root_hash().bytes(),
                    metadata.metadata().unencoded_length,
                    metadata.metadata().encoding_type,
                    persistence,
                )
                .await?
        } else {
            tracing::debug!(
                "the blob is not already registered or its lifetime is too short; creating new one"
            );
            self.sui_client
                .reserve_and_register_blob(epochs_ahead, metadata, persistence)
                .await?
        };
        Ok(blob)
    }

    /// Checks if the blob is registered by the active wallet for a sufficient duration.
    async fn is_blob_registered_in_wallet(
        &self,
        blob_id: &BlobId,
        epochs_ahead: EpochCount,
        persistence: BlobPersistence,
    ) -> ClientResult<Option<Blob>> {
        Ok(self
            .sui_client
            .owned_blobs(false)
            .await?
            .into_iter()
            .find(|blob| {
                blob.blob_id == *blob_id
                    && blob.storage.end_epoch
                        >= self.committees.current_committee().epoch + epochs_ahead
                    && blob.deletable == persistence.is_deletable()
            }))
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
        let mut pairs_per_node = self.pairs_per_node(metadata.blob_id(), pairs);
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
        let comms = self.node_write_communications(Arc::new(Semaphore::new(sliver_write_limit)))?;

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
                    self.committees
                        .current_committee()
                        .is_at_least_min_n_correct(weight)
                },
                self.committees.n_shards().get().into(),
            )
            .await
        {
            tracing::debug!(
                elapsed_time = ?start.elapsed(),
                executed_weight = weight,
                responses = ?requests.into_results(),
                "all futures consumed before reaching a threshold of successful responses"
            );
            return Err(self.not_enough_confirmations_error(weight));
        }
        tracing::debug!(
            elapsed_time = ?start.elapsed(), "stored metadata and slivers onto a quorum of nodes"
        );

        // Add 10% of the execution time, plus 100 ms. This gives the client time to collect more
        // storage confirmations.
        let completed_reason = requests
            .execute_time(
                start.elapsed() / 10 + Duration::from_millis(100),
                self.committees.n_shards().get().into(),
            )
            .await;
        tracing::debug!(
            elapsed_time = ?start.elapsed(),
            %completed_reason,
            "stored metadata and slivers onto additional nodes"
        );
        let results = requests.into_results();
        self.confirmations_to_certificate(metadata.blob_id(), results)
    }

    /// Combines the received storage confirmations into a single certificate.
    ///
    /// This function _does not_ check that the received confirmations match the current epoch and
    /// blob ID, as it assumes that the storage confirmations were received through
    /// [`NodeCommunication::store_metadata_and_pairs`], which internally verifies it to check the
    /// blob ID and epoch.
    fn confirmations_to_certificate(
        &self,
        blob_id: &BlobId,
        confirmations: Vec<NodeResult<SignedStorageConfirmation, StoreError>>,
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
        ensure!(
            self.committees
                .current_committee()
                .is_at_least_min_n_correct(aggregate_weight),
            self.not_enough_confirmations_error(aggregate_weight)
        );

        let aggregate =
            BLS12381AggregateSignature::aggregate(&valid_signatures).map_err(ClientError::other)?;
        let cert = ConfirmationCertificate::new(
            signers,
            bcs::to_bytes(&Confirmation::new(
                self.committees.current_committee().epoch,
                *blob_id,
            ))
            .expect("serialization should always succeed"),
            aggregate,
        );
        Ok(cert)
    }

    fn not_enough_confirmations_error(&self, weight: usize) -> ClientError {
        ClientErrorKind::NotEnoughConfirmations(
            weight,
            self.committees.current_committee().min_n_correct(),
        )
        .into()
    }

    /// Reconstructs the blob by reading slivers from Walrus shards.
    #[tracing::instrument(level = Level::ERROR, skip_all, fields (blob_id = %blob_id))]
    pub async fn read_blob<U>(&self, blob_id: &BlobId) -> ClientResult<Vec<u8>>
    where
        U: EncodingAxis,
        SliverData<U>: TryFrom<Sliver>,
    {
        tracing::debug!("starting to read blob");
        self.check_blob_id(blob_id)?;
        let metadata = self.retrieve_metadata(blob_id).await?;
        self.request_slivers_and_decode::<U>(&metadata).await
    }

    /// Requests the slivers and decodes them into a blob.
    ///
    /// Returns a [`ClientError`] of kind [`ClientErrorKind::BlobIdDoesNotExist`] if it receives a
    /// quorum (at least 2f+1) of "not found" error status codes from the storage nodes.
    #[tracing::instrument(level = Level::ERROR, skip_all)]
    async fn request_slivers_and_decode<U>(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
    ) -> ClientResult<Vec<u8>>
    where
        U: EncodingAxis,
        SliverData<U>: TryFrom<Sliver>,
    {
        let comms = self.node_read_communications()?;
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

        if self.committees.current_committee().is_quorum(n_not_found) {
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
                        self.committees.current_committee().is_quorum(n_not_found)
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
        blob_id: &BlobId,
    ) -> ClientResult<VerifiedBlobMetadataWithId> {
        let comms = self.node_communications_quorum();
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
                        self.committees.current_committee().is_quorum(n_not_found)
                    } {
                        return Err(ClientErrorKind::BlobIdDoesNotExist.into());
                    }
                }
            }
        }
        Err(ClientErrorKind::NoMetadataReceived.into())
    }

    /// Gets the blob status from multiple nodes and returns the latest status that can be verified.
    ///
    /// The nodes are selected such that at least one correct node is contacted.
    pub async fn get_verified_blob_status<U: ReadClient>(
        &self,
        blob_id: &BlobId,
        read_client: &U,
        timeout: Duration,
    ) -> ClientResult<BlobStatus> {
        let comms = self.node_read_communications()?;
        let futures = comms
            .iter()
            .map(|n| n.get_blob_status(blob_id).instrument(n.span.clone()));
        let mut requests = WeightedFutures::new(futures);
        requests
            .execute_until(
                &|weight| self.committees.current_committee().is_quorum(weight),
                timeout,
                self.communication_limits.max_concurrent_status_reads,
            )
            .await;

        let statuses = requests.take_unique_results_with_aggregate_weight();
        tracing::debug!(?statuses, "received blob statuses from storage nodes");
        let mut statuses_list: Vec<_> = statuses.keys().copied().collect();

        // Going through statuses from later (invalid) to earlier (nonexistent), see implementation
        // of `Ord` and `PartialOrd` for `BlobStatus`.
        statuses_list.sort_unstable();
        for status in statuses_list.into_iter().rev() {
            if self
                .committees
                .current_committee()
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

    fn node_communications<'a, W>(
        &'a self,
        constructor: impl Fn(usize) -> Result<Option<NodeCommunication<'a, W>>, ClientBuildError>,
    ) -> ClientResult<Vec<NodeCommunication<'a, W>>> {
        let mut comms: Vec<_> = (0..self.committees.current_committee().n_members())
            .map(|i| (i, constructor(i)))
            .collect();

        if comms.iter().all(|(_, result)| result.is_err()) {
            let Some((_, Err(sample_error))) = comms.pop() else {
                unreachable!("`all()` guarantees at least 1 result and all results are errors");
            };
            return Err(ClientErrorKind::AllConnectionsFailed(sample_error).into());
        }

        let mut comms: Vec<_> = comms
            .into_iter()
            .filter_map(|(index, result)| match result {
                Ok(maybe_communication) => maybe_communication,
                Err(err) => {
                    tracing::warn!(
                        node=index, %err, "unable to establish any connection to a storage node"
                    );
                    None
                }
            })
            .collect();
        comms.shuffle(&mut thread_rng());

        Ok(comms)
    }

    /// Returns a vector of [`NodeWriteCommunication`] objects representing nodes in random order.
    fn node_write_communications(
        &self,
        sliver_write_limit: Arc<Semaphore>,
    ) -> ClientResult<Vec<NodeWriteCommunication>> {
        self.node_communications(|index| {
            self.communication_factory.create_write_communication(
                index,
                self.committees.current_committee(),
                &self.encoding_config,
                sliver_write_limit.clone(),
            )
        })
    }

    /// Returns a vector of [`NodeReadCommunication`] objects representing nodes in random order.
    fn node_read_communications(&self) -> ClientResult<Vec<NodeReadCommunication>> {
        self.node_communications(|index| {
            self.communication_factory.create_read_communication(
                index,
                self.committees.current_committee(),
                &self.encoding_config,
            )
        })
    }

    /// Returns a vector of [`NodeCommunication`] objects the total weight of which fulfills the
    /// threshold function.
    ///
    /// The set and order of nodes included in the communication is randomized.
    ///
    /// # Errors
    ///
    /// Returns a [`ClientError`] with [`ClientErrorKind::Other`] if the threshold function is not
    /// fulfilled after considering all storage nodes.
    fn node_communications_threshold(
        &self,
        threshold_fn: impl Fn(usize) -> bool,
    ) -> ClientResult<Vec<NodeCommunication>> {
        let mut random_indices: Vec<_> =
            (0..self.committees.current_committee().members().len()).collect();
        random_indices.shuffle(&mut thread_rng());
        let mut random_indices = random_indices.into_iter();
        let mut weight = 0;
        let mut comms = vec![];

        loop {
            if threshold_fn(weight) {
                break Ok(comms);
            }
            let Some(index) = random_indices.next() else {
                break Err(ClientErrorKind::Other(
                    anyhow!("unable to create sufficient NodeCommunications").into(),
                )
                .into());
            };
            weight += self.committees.current_committee().members()[index]
                .shard_ids
                .len();

            // Since we are attempting this in a loop, we will retry until we have a threshold of
            // successfully constructed clients (no error and with shards).
            if let Ok(Some(comm)) = self.communication_factory.create_read_communication(
                index,
                self.committees.current_committee(),
                &self.encoding_config,
            ) {
                comms.push(comm);
            }
        }
    }

    /// Returns a vector of [`NodeCommunication`] objects, the weight of which is at least a quorum.
    fn node_communications_quorum(&self) -> Vec<NodeCommunication> {
        self.node_communications_threshold(|weight| {
            self.committees.current_committee().is_quorum(weight)
        })
        .expect("the threshold is below the total number of shards")
    }

    /// Maps the sliver pairs to the node that holds their shard.
    fn pairs_per_node<'a>(
        &'a self,
        blob_id: &'a BlobId,
        pairs: &'a [SliverPair],
    ) -> HashMap<usize, impl Iterator<Item = &SliverPair>> {
        self.committees
            .current_committee()
            .members()
            .iter()
            .map(|node| {
                pairs.iter().filter(|pair| {
                    node.shard_ids.contains(
                        &pair
                            .index()
                            .to_shard_index(self.committees.n_shards(), blob_id),
                    )
                })
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

#[derive(Clone, Debug)]
struct NodeCommunicationFactory {
    config: ClientCommunicationConfig,
    client_cache: Arc<Mutex<HashMap<(NetworkAddress, NetworkPublicKey), StorageNodeClient>>>,
}

impl NodeCommunicationFactory {
    fn new(config: ClientCommunicationConfig) -> Self {
        Self {
            config,
            client_cache: Default::default(),
        }
    }

    /// Builds a [`NodeReadCommunication`] object for the identified storage node within the
    /// committee.
    ///
    /// Returns `None` if the node has no shards.
    ///
    /// # Panics
    ///
    /// Panics if the index is out of range of the committee members.
    fn create_read_communication<'a>(
        &self,
        index: usize,
        committee: &'a Committee,
        encoding_config: &'a EncodingConfig,
    ) -> Result<Option<NodeReadCommunication<'a>>, ClientBuildError> {
        let node = &committee.members()[index];
        let client = self.create_client(node)?;

        Ok(NodeReadCommunication::new(
            index,
            committee.epoch,
            client,
            node,
            encoding_config,
            self.config.request_rate_config.clone(),
        ))
    }

    /// Builds a [`NodeWriteCommunication`] object for the given storage node.
    ///
    /// Returns `None` if the node has no shards.
    fn create_write_communication<'a>(
        &self,
        index: usize,
        committee: &'a Committee,
        encoding_config: &'a EncodingConfig,
        sliver_write_limit: Arc<Semaphore>,
    ) -> Result<Option<NodeWriteCommunication<'a>>, ClientBuildError> {
        let maybe_node_communication = self
            .create_read_communication(index, committee, encoding_config)?
            .map(|nc| nc.with_write_limits(sliver_write_limit));
        Ok(maybe_node_communication)
    }

    fn create_client(&self, node: &StorageNode) -> Result<StorageNodeClient, ClientBuildError> {
        let node_client_id = (
            node.network_address.clone(),
            node.network_public_key.clone(),
        );
        let mut cache = self.client_cache.lock().unwrap();

        match cache.entry(node_client_id) {
            Entry::Occupied(occupied) => Ok(occupied.get().clone()),
            Entry::Vacant(vacant) => {
                let reqwest_builder = self.config.reqwest_config.apply(ReqwestClient::builder());
                let client = StorageNodeClientBuilder::from_reqwest(reqwest_builder)
                    .authenticate_with_public_key(node.network_public_key.clone())
                    .build(&node.network_address.host, node.network_address.port)?;
                Ok(vacant.insert(client).clone())
            }
        }
    }
}
