// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Client for the Walrus service.

use std::{collections::HashMap, time::Instant};

use fastcrypto::{bls12381::min_pk::BLS12381AggregateSignature, traits::AggregateAuthenticator};
use futures::Future;
use reqwest::{Client as ReqwestClient, ClientBuilder};
use tokio::time::{sleep, Duration};
use tracing::Instrument;
use walrus_core::{
    bft,
    encoding::{BlobDecoder, EncodingAxis, EncodingConfig, Sliver, SliverPair},
    ensure,
    messages::{Confirmation, ConfirmationCertificate, SignedStorageConfirmation},
    metadata::VerifiedBlobMetadataWithId,
    BlobId,
    Sliver as SliverEnum,
};
use walrus_sdk::error::NodeError;
use walrus_sui::{
    client::{ContractClient, ReadClient},
    types::{Blob, Committee, StorageNode},
};

mod communication;
mod config;
pub use config::{default_configuration_paths, ClientCommunicationConfig, Config};
mod error;
mod utils;

use communication::{NodeCommunication, NodeResult};
use error::StoreError;
use utils::WeightedFutures;

use self::config::default;
pub use self::error::{ClientError, ClientErrorKind};

/// A client to communicate with Walrus shards and storage nodes.
#[derive(Debug, Clone)]
pub struct Client<T> {
    reqwest_client: ReqwestClient,
    sui_client: T,
    // INV: committee.n_shards > 0
    committee: Committee,
    // The maximum number of nodes to contact in parallel when writing.
    concurrent_writes: usize,
    // The maximum number of shards to contact in parallel when reading slivers.
    concurrent_sliver_reads: usize,
    // The maximum number of shards to contact in parallel when reading metadata.
    concurrent_metadata_reads: usize,
    encoding_config: EncodingConfig,
}

impl Client<()> {
    /// Creates a new read client starting from a config file.
    pub async fn new_read_client(
        config: Config,
        sui_read_client: &impl ReadClient,
    ) -> Result<Self, ClientError> {
        tracing::debug!(?config, "running client");
        let reqwest_client = config
            .communication_config
            .reqwest_config
            .apply(ClientBuilder::new())
            .build()
            .map_err(ClientError::other)?;
        let committee = sui_read_client
            .current_committee()
            .await
            .map_err(ClientError::other)?;
        let encoding_config = EncodingConfig::new(committee.n_shards());
        // Try to store on n-f nodes concurrently, as the work to store is never wasted.
        let concurrent_writes = config
            .communication_config
            .concurrent_writes
            .unwrap_or(default::concurrent_writes(committee.n_shards()));
        // Read n-2f slivers concurrently to avoid wasted work on the storage nodes.
        let concurrent_sliver_reads = config
            .communication_config
            .concurrent_writes
            .unwrap_or(default::concurrent_sliver_reads(committee.n_shards()));
        Ok(Self {
            reqwest_client,
            sui_client: (),
            committee,
            concurrent_writes,
            encoding_config,
            concurrent_sliver_reads,
            concurrent_metadata_reads: config.communication_config.concurrent_metadata_reads,
        })
    }

    /// Converts `self` to a [`Client::<T>`] by adding the `sui_client`.
    pub async fn with_client<T: ContractClient>(self, sui_client: T) -> Client<T> {
        let Self {
            reqwest_client,
            sui_client: _,
            committee,
            concurrent_writes,
            concurrent_sliver_reads,
            concurrent_metadata_reads,
            encoding_config,
        } = self;
        Client::<T> {
            reqwest_client,
            sui_client,
            committee,
            concurrent_writes,
            concurrent_sliver_reads,
            concurrent_metadata_reads,
            encoding_config,
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
    #[tracing::instrument(skip_all, fields(blob_id_prefix))]
    pub async fn reserve_and_store_blob(
        &self,
        blob: &[u8],
        epochs_ahead: u64,
    ) -> Result<Blob, ClientError> {
        let (pairs, metadata) = self
            .encoding_config
            .get_blob_encoder(blob)
            .map_err(ClientError::other)?
            .encode_with_metadata();
        tracing::Span::current().record("blob_id_prefix", string_prefix(metadata.blob_id()));
        let encoded_length = self
            .encoding_config
            .encoded_blob_length_from_usize(blob.len())
            .expect("valid for metadata created from the same config");
        tracing::debug!(blob_id = %metadata.blob_id(), ?encoded_length,
                        "computed blob pairs and metadata");

        // Get the root hash of the blob.
        let root_hash = metadata.metadata().compute_root_hash();

        let storage_resource = self
            .sui_client
            .reserve_space(encoded_length, epochs_ahead)
            .await
            .map_err(ClientError::other)?;
        let blob_sui_object = self
            .sui_client
            .register_blob(
                &storage_resource,
                *metadata.blob_id(),
                root_hash.bytes(),
                blob.len()
                    .try_into()
                    .expect("conversion implicitly checked above"),
                metadata.metadata().encoding_type,
            )
            .await
            .map_err(ClientError::other)?;

        // We need to wait to be sure that the storage nodes received the registration event.
        sleep(Duration::from_secs(1)).await;

        let certificate = self.store_metadata_and_pairs(&metadata, pairs).await?;
        self.sui_client
            .certify_blob(&blob_sui_object, &certificate)
            .await
            .map_err(ClientError::other)
    }
}

impl<T> Client<T> {
    /// Stores the already-encoded metadata and sliver pairs for a blob into Walrus, by sending
    /// sliver pairs to at least 2f+1 shards.
    ///
    /// Assumes the blob ID has already been registered, with an appropriate blob size.
    #[tracing::instrument(skip_all)]
    pub async fn store_metadata_and_pairs(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        pairs: Vec<SliverPair>,
    ) -> Result<ConfirmationCertificate, ClientError> {
        let pairs_per_node = self.pairs_per_node(metadata.blob_id(), pairs);
        let comms = self.node_communications();
        let mut requests = WeightedFutures::new(
            comms
                .iter()
                .zip(pairs_per_node.into_iter())
                .map(|(n, p)| n.store_metadata_and_pairs(metadata, p)),
        );
        let start = Instant::now();
        let quorum_check = |weight| self.committee.is_quorum(weight);
        requests
            .execute_weight(&quorum_check, self.concurrent_writes)
            .await;
        tracing::debug!(
            elapsed_time=?start.elapsed(), "stored metadata and slivers onto a quorum of nodes"
        );
        // Double the execution time, with a minimum of 100 ms. This gives the client time to
        // collect more storage confirmations.
        let completed_reason = requests
            .execute_time(
                start.elapsed() + Duration::from_millis(100),
                self.concurrent_writes,
            )
            .await;
        tracing::debug!(
            elapsed_time=?start.elapsed(),
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
    /// `NodeCommunication::store_metadata_and_pairs`, which internally verifies it to check the
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
                Err(err) => tracing::error!(?node, ?err, "storing metadata and pairs failed"),
            }
        }

        ensure!(
            self.committee.is_quorum(aggregate_weight),
            ClientErrorKind::NotEnoughConfirmations(
                aggregate_weight,
                bft::min_n_correct(self.committee.n_shards()).get().into()
            )
            .into()
        );
        let aggregate =
            BLS12381AggregateSignature::aggregate(&valid_signatures).map_err(ClientError::other)?;
        let cert = ConfirmationCertificate {
            signers,
            confirmation: bcs::to_bytes(&Confirmation::new(self.committee.epoch, *blob_id))
                .expect("serialization should always succeed"),
            signature: aggregate,
        };
        Ok(cert)
    }

    /// Reconstructs the blob by reading slivers from Walrus shards.
    #[tracing::instrument(skip_all, fields(blob_id_prefix=string_prefix(blob_id)))]
    pub async fn read_blob<U>(&self, blob_id: &BlobId) -> Result<Vec<u8>, ClientError>
    where
        U: EncodingAxis,
        Sliver<U>: TryFrom<SliverEnum>,
    {
        tracing::debug!(%blob_id, "starting to read blob");
        let metadata = self.retrieve_metadata(blob_id).await?;
        self.request_slivers_and_decode::<U>(&metadata).await
    }

    async fn request_slivers_and_decode<U>(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
    ) -> Result<Vec<u8>, ClientError>
    where
        U: EncodingAxis,
        Sliver<U>: TryFrom<SliverEnum>,
    {
        // TODO(giac): optimize by reading first from the shards that have the systematic part of
        // the encoding.
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
            .execute_weight(&enough_source_symbols, self.concurrent_sliver_reads)
            .await;

        let slivers = requests
            .take_results()
            .into_iter()
            .filter_map(|NodeResult(_, _, node, result)| {
                result
                    .map_err(|err| {
                        tracing::error!(?node, ?err, "retrieving sliver failed");
                    })
                    .ok()
            })
            .collect::<Vec<_>>();

        if let Some((blob, _meta)) = decoder
            .decode_and_verify(metadata.blob_id(), slivers)
            .map_err(ClientError::other)?
        {
            // We have enough to decode the blob.
            Ok(blob)
        } else {
            // We were not able to decode. Keep requesting slivers and try decoding as soon as every
            // new sliver is received.
            self.decode_sliver_by_sliver(&mut requests, &mut decoder, metadata.blob_id())
                .await
        }
    }

    /// Decodes the blob of given blob ID by requesting slivers and trying to decode at each new
    /// sliver it receives.
    #[tracing::instrument(skip_all)]
    async fn decode_sliver_by_sliver<'a, I, Fut, U>(
        &self,
        requests: &mut WeightedFutures<I, Fut, NodeResult<Sliver<U>, NodeError>>,
        decoder: &mut BlobDecoder<'a, U>,
        blob_id: &BlobId,
    ) -> Result<Vec<u8>, ClientError>
    where
        U: EncodingAxis,
        I: Iterator<Item = Fut>,
        Fut: Future<Output = NodeResult<Sliver<U>, NodeError>>,
    {
        while let Some(NodeResult(_, _, node, result)) =
            requests.next(self.concurrent_sliver_reads).await
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
                Err(err) => {
                    tracing::error!(?node, ?err, "retrieving sliver failed");
                }
            }
        }
        // We have exhausted all the slivers but were not able to reconstruct the blob.
        Err(ClientErrorKind::NotEnoughSlivers.into())
    }

    /// Requests the metadata from all storage nodes, and keeps the first that is correctly verified
    /// against the blob ID.
    pub async fn retrieve_metadata(
        &self,
        blob_id: &BlobId,
    ) -> Result<VerifiedBlobMetadataWithId, ClientError> {
        let comms = self.node_communications();
        let futures = comms.iter().map(|n| {
            n.retrieve_verified_metadata(blob_id)
                .instrument(n.span.clone())
        });
        // Wait until the first request succeeds
        let mut requests = WeightedFutures::new(futures);
        let just_one = |weight| weight >= 1;
        requests
            .execute_weight(&just_one, self.concurrent_metadata_reads)
            .await;
        let metadata = requests
            .take_inner_ok()
            .pop()
            .ok_or(ClientErrorKind::NoMetadataReceived)?;
        Ok(metadata)
    }

    /// Builds a [`NodeCommunication`] object for the given storage node.
    fn new_node_communication<'a>(
        &'a self,
        index: usize,
        node: &'a StorageNode,
    ) -> NodeCommunication {
        NodeCommunication::new(
            index,
            self.committee.epoch,
            &self.reqwest_client,
            node,
            &self.encoding_config,
        )
    }

    fn node_communications(&self) -> Vec<NodeCommunication> {
        self.committee
            .members()
            .iter()
            .enumerate()
            .map(|(index, node)| self.new_node_communication(index, node))
            .collect()
    }

    /// Maps the sliver pairs to the node that holds their shard.
    fn pairs_per_node(&self, blob_id: &BlobId, pairs: Vec<SliverPair>) -> Vec<Vec<SliverPair>> {
        let mut pairs_per_node = Vec::with_capacity(self.committee.members().len());
        pairs_per_node.extend(
            self.committee
                .members()
                .iter()
                .map(|n| Vec::with_capacity(n.shard_ids.len())),
        );
        let shard_to_node = self
            .committee
            .members()
            .iter()
            .enumerate()
            .flat_map(|(index, m)| m.shard_ids.iter().map(move |s| (*s, index)))
            .collect::<HashMap<_, _>>();
        pairs.into_iter().for_each(|p| {
            pairs_per_node
                [shard_to_node[&p.index().to_shard_index(self.committee.n_shards(), blob_id)]]
                .push(p)
        });
        pairs_per_node
    }
}

pub(crate) fn string_prefix<T: ToString>(s: &T) -> String {
    let mut string = s.to_string();
    string.truncate(8);
    format!("{}...", string)
}
