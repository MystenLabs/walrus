// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Client for the Walrus service.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
};

use anyhow::anyhow;
use fastcrypto::{bls12381::min_pk::BLS12381AggregateSignature, traits::AggregateAuthenticator};
use futures::Future;
use rand::{seq::SliceRandom, thread_rng};
use reqwest::{Client as ReqwestClient, ClientBuilder};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use sui_types::event::EventID;
use tokio::{
    sync::Semaphore,
    time::{sleep, Duration},
};
use tracing::{Instrument, Level};
use walrus_core::{
    encoding::{BlobDecoder, EncodingAxis, EncodingConfig, Sliver, SliverPair},
    ensure,
    messages::{Confirmation, ConfirmationCertificate, SignedStorageConfirmation},
    metadata::VerifiedBlobMetadataWithId,
    BlobId,
    Epoch,
    Sliver as SliverEnum,
};
use walrus_sdk::{
    api::{BlobCertificationStatus, BlobStatus},
    error::NodeError,
};
use walrus_sui::{
    client::{ContractClient, ReadClient},
    types::{Blob, BlobEvent, Committee, StorageNode},
};

mod blocklist;
pub use blocklist::Blocklist;
mod communication;
mod config;
pub use config::{default_configuration_paths, ClientCommunicationConfig, Config};
mod error;
mod utils;

use communication::{NodeCommunication, NodeResult};
use error::StoreError;
pub use utils::string_prefix;
use utils::WeightedFutures;

use self::config::default;
pub use self::error::{ClientError, ClientErrorKind};
use crate::client::utils::CompletedReasonWeight;

/// A client to communicate with Walrus shards and storage nodes.
#[derive(Debug, Clone)]
pub struct Client<T> {
    reqwest_client: ReqwestClient,
    config: Config,
    sui_client: T,
    // INV: committee.n_shards > 0
    committee: Committee,
    // The maximum number of nodes to contact in parallel when writing.
    max_concurrent_writes: usize,
    // The maximum number of shards to contact in parallel when reading slivers.
    max_concurrent_sliver_reads: usize,
    // The maximum number of nodes to contact in parallel when reading metadata.
    max_concurrent_metadata_reads: usize,
    // The maximum number of nodes to contact in parallel when getting the blob status.
    max_concurrent_status_reads: usize,
    encoding_config: EncodingConfig,
    global_write_limit: Arc<Semaphore>,
    blocklist: Option<Blocklist>,
}

impl Client<()> {
    /// Creates a new read client starting from a config file.
    pub async fn new_read_client(
        config: Config,
        sui_read_client: &impl ReadClient,
    ) -> Result<Self, ClientError> {
        tracing::debug!(?config, "running client");
        let reqwest_client = Self::build_reqwest_client(&config)?;

        // Get the committee, and check that there is at least one shard per node.
        let committee = sui_read_client
            .current_committee()
            .await
            .map_err(ClientError::other)?;
        for node in committee.members() {
            ensure!(
                !node.shard_ids.is_empty(),
                ClientErrorKind::InvalidConfig.into(),
            );
        }

        let encoding_config = EncodingConfig::new(committee.n_shards());
        // Try to store on n-f nodes concurrently, as the work to store is never wasted.
        let max_concurrent_writes = config
            .communication_config
            .max_concurrent_writes
            .unwrap_or(default::max_concurrent_writes(committee.n_shards()));
        // Read n-2f slivers concurrently to avoid wasted work on the storage nodes.
        let max_concurrent_sliver_reads = config
            .communication_config
            .max_concurrent_sliver_reads
            .unwrap_or(default::max_concurrent_sliver_reads(committee.n_shards()));
        let max_concurrent_metadata_reads =
            config.communication_config.max_concurrent_metadata_reads;
        let max_concurrent_status_reads = config
            .communication_config
            .max_concurrent_status_reads
            .unwrap_or(default::max_concurrent_status_reads(committee.n_shards()));
        let global_write_limit = Arc::new(Semaphore::new(max_concurrent_writes));
        Ok(Self {
            config,
            reqwest_client,
            sui_client: (),
            committee,
            encoding_config,
            max_concurrent_writes,
            max_concurrent_sliver_reads,
            max_concurrent_metadata_reads,
            max_concurrent_status_reads,
            global_write_limit,
            blocklist: None,
        })
    }

    /// Converts `self` to a [`Client::<T>`] by adding the `sui_client`.
    pub async fn with_client<T: ContractClient>(self, sui_client: T) -> Client<T> {
        let Self {
            reqwest_client,
            config,
            sui_client: _,
            committee,
            max_concurrent_writes,
            max_concurrent_sliver_reads,
            max_concurrent_metadata_reads,
            max_concurrent_status_reads,
            encoding_config,
            global_write_limit: global_connection_limit,
            blocklist,
        } = self;
        Client::<T> {
            reqwest_client,
            config,
            sui_client,
            committee,
            max_concurrent_writes,
            max_concurrent_sliver_reads,
            max_concurrent_metadata_reads,
            max_concurrent_status_reads,
            encoding_config,
            global_write_limit: global_connection_limit,
            blocklist,
        }
    }
}

impl<T: ContractClient> Client<T> {
    /// Creates a new client starting from a config file.
    pub async fn new(config: Config, sui_client: T) -> Result<Self, ClientError> {
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
        epochs_ahead: u64,
        force: bool,
    ) -> Result<BlobStoreResult, ClientError> {
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
        if !force {
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
                BlobStatus::Existent {
                    end_epoch,
                    status: BlobCertificationStatus::Certified,
                    status_event,
                } => {
                    if end_epoch >= self.committee.epoch + epochs_ahead {
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
                BlobStatus::Existent {
                    status: BlobCertificationStatus::Invalid,
                    status_event,
                    ..
                } => {
                    tracing::debug!("blob is marked as invalid");
                    return Ok(BlobStoreResult::MarkedInvalid {
                        blob_id,
                        event: status_event,
                    });
                }
                status => {
                    // We intentionally don't check for "registered" blobs here: even if the blob is
                    // already registered, we cannot certify it without access to the corresponding
                    // Sui object.
                    tracing::debug!(?status, "blob is not certified, creating it");
                }
            }
        }

        // Reserve space for the blob.
        let blob_sui_object = self
            .reserve_blob(&metadata, blob.len(), epochs_ahead)
            .await?;

        // We need to wait to be sure that the storage nodes received the registration event.
        sleep(Duration::from_secs(1)).await;

        let certificate = self.store_metadata_and_pairs(&metadata, &pairs).await?;
        let blob = self
            .sui_client
            .certify_blob(blob_sui_object, &certificate)
            .await
            .map_err(|e| ClientError::from(ClientErrorKind::CertificationFailed(e)))?;
        Ok(BlobStoreResult::NewlyCreated(blob))
    }

    /// Reserves the space for the blob on chain.
    #[tracing::instrument(skip_all, err(level = Level::DEBUG))]
    pub async fn reserve_blob(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        unencoded_size: usize,
        epochs_ahead: u64,
    ) -> Result<Blob, ClientError> {
        let encoded_size = self
            .encoding_config
            .encoded_blob_length_from_usize(unencoded_size)
            .expect("valid for metadata created from the same config");
        tracing::debug!(encoded_size, "starting to reserve storage for blob");

        let root_hash = metadata.metadata().compute_root_hash();
        let storage_resource = self
            .sui_client
            .reserve_space(encoded_size, epochs_ahead)
            .await
            .map_err(ClientError::other)?;
        self.sui_client
            .register_blob(
                &storage_resource,
                *metadata.blob_id(),
                root_hash.bytes(),
                unencoded_size
                    .try_into()
                    .expect("conversion implicitly checked above"),
                metadata.metadata().encoding_type,
            )
            .await
            .map_err(ClientError::other)
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
    pub async fn store_metadata_and_pairs(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        pairs: &[SliverPair],
    ) -> Result<ConfirmationCertificate, ClientError> {
        let mut pairs_per_node = self.pairs_per_node(metadata.blob_id(), pairs);
        let comms = self.node_communications();
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
        // connections is already limited by the `global_write_limit` semaphore.
        if let CompletedReasonWeight::FuturesConsumed(weight) = requests
            .execute_weight(
                &|weight| self.quorum_check(weight),
                self.committee.n_shards().get().into(),
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
                self.committee.n_shards().get().into(),
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
    ) -> Result<ConfirmationCertificate, ClientError> {
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
            self.quorum_check(aggregate_weight),
            self.not_enough_confirmations_error(aggregate_weight)
        );

        let aggregate =
            BLS12381AggregateSignature::aggregate(&valid_signatures).map_err(ClientError::other)?;
        let cert = ConfirmationCertificate::new(
            signers,
            bcs::to_bytes(&Confirmation::new(self.committee.epoch, *blob_id))
                .expect("serialization should always succeed"),
            aggregate,
        );
        Ok(cert)
    }

    fn quorum_check(&self, weight: usize) -> bool {
        self.committee.is_at_least_min_n_correct(weight)
    }

    fn not_enough_confirmations_error(&self, weight: usize) -> ClientError {
        ClientErrorKind::NotEnoughConfirmations(weight, self.committee.min_n_correct()).into()
    }

    /// Reconstructs the blob by reading slivers from Walrus shards.
    #[tracing::instrument(level = Level::ERROR, skip_all, fields (blob_id = %blob_id))]
    pub async fn read_blob<U>(&self, blob_id: &BlobId) -> Result<Vec<u8>, ClientError>
    where
        U: EncodingAxis,
        Sliver<U>: TryFrom<SliverEnum>,
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
    ) -> Result<Vec<u8>, ClientError>
    where
        U: EncodingAxis,
        Sliver<U>: TryFrom<SliverEnum>,
    {
        // TODO(giac): optimize by reading first from the shards that have the systematic part of
        // the encoding. Currently the read order is randomized.
        let comms = self.node_communications();
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
            .execute_weight(&enough_source_symbols, self.max_concurrent_sliver_reads)
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

        if self.committee.is_quorum(n_not_found) {
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
            self.decode_sliver_by_sliver(
                &mut requests,
                &mut decoder,
                metadata.blob_id(),
                n_not_found,
            )
            .await
        }
    }

    /// Decodes the blob of given blob ID by requesting slivers and trying to decode at each new
    /// sliver it receives.
    #[tracing::instrument(level = Level::ERROR, skip_all)]
    async fn decode_sliver_by_sliver<'a, I, Fut, U>(
        &self,
        requests: &mut WeightedFutures<I, Fut, NodeResult<Sliver<U>, NodeError>>,
        decoder: &mut BlobDecoder<'a, U>,
        blob_id: &BlobId,
        mut n_not_found: usize,
    ) -> Result<Vec<u8>, ClientError>
    where
        U: EncodingAxis,
        I: Iterator<Item = Fut>,
        Fut: Future<Output = NodeResult<Sliver<U>, NodeError>>,
    {
        while let Some(NodeResult(_, _, node, result)) =
            requests.next(self.max_concurrent_sliver_reads).await
        {
            match result {
                Ok(sliver) => {
                    let result = decoder
                        .decode_and_verify(blob_id, [sliver])
                        .map_err(ClientError::other)?;
                    if let Some((blob, _meta)) = result {
                        return Ok(blob);
                    }
                }
                Err(error) => {
                    tracing::warn!(%node, %error, "retrieving sliver failed");
                    if error.is_status_not_found() && {
                        n_not_found += 1;
                        self.committee.is_quorum(n_not_found)
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
    ) -> Result<VerifiedBlobMetadataWithId, ClientError> {
        let comms = self.node_communications_quorum();
        let futures = comms.iter().map(|n| {
            n.retrieve_verified_metadata(blob_id)
                .instrument(n.span.clone())
        });
        // Wait until the first request succeeds
        let mut requests = WeightedFutures::new(futures);
        let just_one = |weight| weight >= 1;
        requests
            .execute_weight(&just_one, self.max_concurrent_metadata_reads)
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
                        self.committee.is_quorum(n_not_found)
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
    ) -> Result<BlobStatus, ClientError> {
        let comms = self.node_communications_with_correct_node();
        let futures = comms
            .iter()
            .map(|n| n.get_blob_status(blob_id).instrument(n.span.clone()));
        let mut requests = WeightedFutures::new(futures);
        requests
            .execute_time(timeout, self.max_concurrent_status_reads)
            .await;

        let mut statuses: Vec<_> =
            // Using `HashSet` to get unique status responses.
            HashSet::<BlobStatus>::from_iter(requests.take_inner_ok().into_iter())
                .into_iter()
                .collect();

        // Going through statuses from later (invalid) to earlier (nonexistent), see implementation
        // of `Ord` and `PartialOrd` for `BlobStatus`.
        statuses.sort_unstable();
        tracing::debug!(?statuses, "received blob statuses from storage nodes");
        for status in statuses.into_iter().rev() {
            if verify_blob_status(blob_id, status, read_client)
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
    fn check_blob_id(&self, blob_id: &BlobId) -> Result<(), ClientError> {
        if let Some(blocklist) = &self.blocklist {
            if blocklist.is_blocked(blob_id) {
                tracing::debug!(%blob_id, "encountered blocked blob ID");
                return Err(ClientErrorKind::BlobIdBlocked(*blob_id).into());
            }
        }
        Ok(())
    }

    /// Builds a [`NodeCommunication`] object for the given storage node.
    ///
    /// Returns `None` if the node has no shards.
    fn new_node_communication<'a>(
        &'a self,
        index: usize,
        node: &'a StorageNode,
    ) -> Option<NodeCommunication> {
        NodeCommunication::new(
            index,
            self.committee.epoch,
            &self.reqwest_client,
            node,
            &self.encoding_config,
            self.config.communication_config.request_rate_config.clone(),
            self.global_write_limit.clone(),
        )
    }

    /// Returns a vector of [`NodeCommunication`] objects representing nodes in random order.
    fn node_communications(&self) -> Vec<NodeCommunication> {
        let mut comms: Vec<_> = self
            .committee
            .members()
            .iter()
            .enumerate()
            .filter_map(|(index, node)| self.new_node_communication(index, node))
            .collect();
        comms.shuffle(&mut thread_rng());
        comms
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
    ) -> Result<Vec<NodeCommunication>, ClientError> {
        let mut random_indices: Vec<_> = (0..self.committee.members().len()).collect();
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
            weight += self.committee.members()[index].shard_ids.len();
            if let Some(comm) = self.new_node_communication(index, &self.committee.members()[index])
            {
                comms.push(comm);
            }
        }
    }

    /// Returns a vector of [`NodeCommunication`] objects, the weight of which is at least a quorum.
    fn node_communications_quorum(&self) -> Vec<NodeCommunication> {
        self.node_communications_threshold(|weight| self.committee.is_quorum(weight))
            .expect("the threshold is below the total number of shards")
    }

    /// Returns a vector of [`NodeCommunication`] objects which contains at least one correct node.
    fn node_communications_with_correct_node(&self) -> Vec<NodeCommunication> {
        self.node_communications_threshold(|weight| self.committee.is_above_validity(weight))
            .expect("the threshold is below the total number of shards")
    }

    /// Maps the sliver pairs to the node that holds their shard.
    fn pairs_per_node<'a>(
        &'a self,
        blob_id: &'a BlobId,
        pairs: &'a [SliverPair],
    ) -> HashMap<usize, impl Iterator<Item = &SliverPair>> {
        self.committee
            .members()
            .iter()
            .map(|node| {
                pairs.iter().filter(|pair| {
                    node.shard_ids.contains(
                        &pair
                            .index()
                            .to_shard_index(self.committee.n_shards(), blob_id),
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

    fn build_reqwest_client(config: &Config) -> Result<ReqwestClient, ClientError> {
        config
            .communication_config
            .reqwest_config
            .apply(ClientBuilder::new())
            .build()
            .map_err(ClientError::other)
    }

    /// Resets the reqwest client inside the Walrus client.
    ///
    /// Useful to ensure that the client cannot communicate with storage nodes through connections
    /// that are being kept alive.
    #[cfg(feature = "test-utils")]
    pub fn reset_reqwest_client(&mut self) -> Result<(), ClientError> {
        self.reqwest_client = Self::build_reqwest_client(&self.config)?;
        Ok(())
    }
}

/// Result when attempting to store a blob.
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum BlobStoreResult {
    /// The blob already exists within Walrus, was certified, and is stored for at least the
    /// intended duration.
    AlreadyCertified {
        /// The blob ID.
        #[serde_as(as = "DisplayFromStr")]
        blob_id: BlobId,
        /// The event where the blob was certified.
        event: EventID,
        /// The epoch until which the blob is stored (exclusive).
        end_epoch: Epoch,
    },
    /// The blob was newly created; this contains the newly created Sui object associated with the
    /// blob.
    NewlyCreated(Blob),
    /// The blob is known to Walrus but was marked as invalid.
    ///
    /// This indicates a bug within the client, the storage nodes, or more than a third malicious
    /// storage nodes.
    MarkedInvalid {
        /// The blob ID.
        #[serde_as(as = "DisplayFromStr")]
        blob_id: BlobId,
        /// The event where the blob was marked as invalid.
        event: EventID,
    },
}

impl BlobStoreResult {
    /// Returns the blob ID.
    pub fn blob_id(&self) -> &BlobId {
        match self {
            Self::AlreadyCertified { blob_id, .. } => blob_id,
            Self::MarkedInvalid { blob_id, .. } => blob_id,
            Self::NewlyCreated(Blob { blob_id, .. }) => blob_id,
        }
    }
}

#[tracing::instrument(skip(sui_read_client), err(level = Level::WARN))]
async fn verify_blob_status(
    blob_id: &BlobId,
    status: BlobStatus,
    sui_read_client: &impl ReadClient,
) -> Result<(), anyhow::Error> {
    let BlobStatus::Existent {
        end_epoch,
        status,
        status_event,
    } = status
    else {
        return Ok(());
    };
    tracing::debug!("verifying blob status with on-chain event");
    let blob_event = sui_read_client.get_blob_event(status_event).await?;

    let event_blob_id = match (status, blob_event) {
        (BlobCertificationStatus::Registered, BlobEvent::Registered(event)) => {
            anyhow::ensure!(end_epoch == event.end_epoch, "end epoch mismatch");
            event.blob_id
        }
        (BlobCertificationStatus::Certified, BlobEvent::Certified(event)) => {
            anyhow::ensure!(end_epoch == event.end_epoch, "end epoch mismatch");
            event.blob_id
        }
        (BlobCertificationStatus::Invalid, BlobEvent::InvalidBlobID(event)) => event.blob_id,
        (_, _) => Err(anyhow!("blob event does not match status"))?,
    };
    anyhow::ensure!(blob_id == &event_blob_id, "blob ID mismatch");

    Ok(())
}
