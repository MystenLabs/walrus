// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, time::Instant};

use anyhow::{anyhow, Result};
use fastcrypto::{bls12381::min_pk::BLS12381AggregateSignature, traits::AggregateAuthenticator};
use futures::Future;
use reqwest::{Client as ReqwestClient, ClientBuilder};
use tokio::time::{sleep, Duration};
use tracing::Instrument;
use walrus_core::{
    encoding::{BlobDecoder, EncodingAxis, EncodingConfig, Sliver, SliverPair},
    ensure,
    messages::{Confirmation, ConfirmationCertificate},
    metadata::VerifiedBlobMetadataWithId,
    BlobId,
    SignedStorageConfirmation,
    Sliver as SliverEnum,
};
use walrus_sui::{
    client::{ContractClient, ReadClient},
    types::{Blob, Committee, StorageNode},
};

mod communication;
mod config;
pub use config::{default_configuration_paths, Config};
mod error;
mod utils;

use communication::{NodeCommunication, NodeResult};
use error::{SliverRetrieveError, StoreError};
use utils::WeightedFutures;

/// A client to communicate with Walrus shards and storage nodes.
#[derive(Debug)]
pub struct Client<T> {
    reqwest_client: ReqwestClient,
    sui_client: T,
    // INV: committee.total_weight > 0
    committee: Committee,
    concurrent_requests: usize,
    encoding_config: EncodingConfig,
}

impl Client<()> {
    /// Creates a new read client starting from a config file.
    pub async fn new_read_client(
        config: Config,
        sui_read_client: &impl ReadClient,
    ) -> Result<Self> {
        let reqwest_client = ClientBuilder::new()
            .timeout(config.connection_timeout)
            .build()?;
        let committee = sui_read_client.current_committee().await?;
        let encoding_config = EncodingConfig::new(committee.n_shards());

        Ok(Self {
            reqwest_client,
            sui_client: (),
            committee,
            concurrent_requests: config.concurrent_requests,
            encoding_config,
        })
    }

    /// Converts `self` to a [`Client::<T>`] by adding the `sui_client`.
    pub async fn with_client<T: ContractClient>(self, sui_client: T) -> Client<T> {
        let Self {
            reqwest_client,
            sui_client: _,
            committee,
            concurrent_requests,
            encoding_config,
        } = self;
        Client::<T> {
            reqwest_client,
            sui_client,
            committee,
            concurrent_requests,
            encoding_config,
        }
    }
}

impl<T: ContractClient> Client<T> {
    /// Creates a new client starting from a config file.
    pub async fn new(config: Config, sui_client: T) -> Result<Self> {
        Ok(Client::new_read_client(config, sui_client.read_client())
            .await?
            .with_client(sui_client)
            .await)
    }

    /// Encodes the blob, reserves & registers the space on chain, and stores the slivers to the
    /// storage nodes. Finally, the function aggregates the storage confirmations and posts the
    /// [`ConfirmationCertificate`] on chain.
    #[tracing::instrument(skip_all, fields(blob_id_prefix))]
    pub async fn reserve_and_store_blob(&self, blob: &[u8], epochs_ahead: u64) -> Result<Blob> {
        let (pairs, metadata) = self
            .encoding_config
            .get_blob_encoder(blob)?
            .encode_with_metadata();
        tracing::Span::current().record("blob_id_prefix", truncate_blob_id(metadata.blob_id()));
        let encoded_length = self
            .encoding_config
            .encoded_blob_length(blob.len())
            .expect("valid for metadata created from the same config");
        tracing::debug!(blob_id = %metadata.blob_id(), ?encoded_length,
                        "computed blob pairs and metadata");

        // Get the root hash of the blob.
        let root_hash = metadata.metadata().compute_root_hash();

        let storage_resource = self
            .sui_client
            .reserve_space(encoded_length, epochs_ahead)
            .await?;
        let blob_sui_object = self
            .sui_client
            .register_blob(
                &storage_resource,
                *metadata.blob_id(),
                root_hash.bytes(),
                blob.len() as u64,
                metadata.metadata().encoding_type,
            )
            .await?;

        // We need to wait to be sure that the storage nodes received the registration event.
        sleep(Duration::from_secs(1)).await;

        let certificate = self.store_metadata_and_pairs(&metadata, pairs).await?;
        self.sui_client
            .certify_blob(&blob_sui_object, &certificate)
            .await
            .map_err(|e| anyhow!("blob certification failed: {e}"))
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
    ) -> Result<ConfirmationCertificate> {
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
            .execute_weight(&quorum_check, self.concurrent_requests)
            .await;
        // Double the execution time, with a minimum of 100 ms. This gives the client time to
        // collect more storage confirmations.
        requests
            .execute_time(
                start.elapsed() + Duration::from_millis(100),
                self.concurrent_requests,
            )
            .await;
        let results = requests.into_results();
        self.confirmations_to_certificate(metadata.blob_id(), results)
    }

    /// Combines the received storage confirmations into a single certificate.
    ///
    /// This function _does not_ check that the received confirmations match the current epoch and
    /// blob ID, as it assumes that the storage confirmations were received through
    /// `NodeCommunication::store_metadata_and_pairs`, which internally uses `verify_confirmation`
    /// to check blob ID and epoch.
    fn confirmations_to_certificate(
        &self,
        blob_id: &BlobId,
        confirmations: Vec<NodeResult<SignedStorageConfirmation, StoreError>>,
    ) -> Result<ConfirmationCertificate> {
        let mut total_weight = 0;
        let mut signers = Vec::with_capacity(confirmations.len());
        let mut valid_signatures = Vec::with_capacity(confirmations.len());
        for NodeResult(_, weight, node, result) in confirmations {
            match result {
                Ok(confirmation) => {
                    total_weight += weight;
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
            self.committee.is_quorum(total_weight),
            "not enough confirmations for the blob id were retrieved"
        );
        let aggregate = BLS12381AggregateSignature::aggregate(&valid_signatures)?;
        let cert = ConfirmationCertificate {
            signers,
            confirmation: bcs::to_bytes(&Confirmation::new(self.committee.epoch, *blob_id))
                .expect("serialization should always succeed"),
            signature: aggregate,
        };
        Ok(cert)
    }

    /// Reconstructs the blob by reading slivers from Walrus shards.
    #[tracing::instrument(skip_all, fields(blob_id_prefix))]
    pub async fn read_blob<U>(&self, blob_id: &BlobId) -> Result<Vec<u8>>
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
    ) -> Result<Vec<u8>>
    where
        U: EncodingAxis,
        Sliver<U>: TryFrom<SliverEnum>,
    {
        // TODO(giac): optimize by reading first from the shards that have the systematic part of
        // the encoding.
        let comms = self.node_communications();
        // Create requests to get all slivers from all nodes.
        let futures = comms.iter().flat_map(|n| {
            n.node.shard_ids.iter().map(|s| {
                n.retrieve_verified_sliver::<U>(metadata, *s)
                    .instrument(n.span.clone())
            })
        });
        let mut decoder = self
            .encoding_config
            .get_blob_decoder::<U>(metadata.metadata().unencoded_length.try_into()?)?;
        // Get the first ~1/3 or ~2/3 of slivers directly, and decode with these.
        let mut requests = WeightedFutures::new(futures);
        let enough_source_symbols =
            |weight| weight >= self.encoding_config.n_source_symbols::<U>().get().into();
        requests
            .execute_weight(&enough_source_symbols, self.concurrent_requests)
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

        if let Some((blob, _meta)) = decoder.decode_and_verify(metadata.blob_id(), slivers)? {
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
        requests: &mut WeightedFutures<I, Fut, NodeResult<Sliver<U>, SliverRetrieveError>>,
        decoder: &mut BlobDecoder<'a, U>,
        blob_id: &BlobId,
    ) -> Result<Vec<u8>>
    where
        U: EncodingAxis,
        I: Iterator<Item = Fut>,
        Fut: Future<Output = NodeResult<Sliver<U>, SliverRetrieveError>>,
    {
        while let Some(NodeResult(_, _, node, result)) =
            requests.next(self.concurrent_requests).await
        {
            match result {
                Ok(sliver) => {
                    let result = decoder.decode_and_verify(blob_id, [sliver])?;
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
        Err(anyhow!(
            "not enough slivers were received to reconstruct the blob"
        ))
    }

    /// Requests the metadata from all storage nodes, and keeps the first that is correctly verified
    /// against the blob ID.
    pub async fn retrieve_metadata(&self, blob_id: &BlobId) -> Result<VerifiedBlobMetadataWithId> {
        let comms = self.node_communications();
        let futures = comms.iter().map(|n| {
            n.retrieve_verified_metadata(blob_id)
                .instrument(n.span.clone())
        });
        // Wait until the first request succeeds
        let mut requests = WeightedFutures::new(futures);
        let just_one = |weight| weight >= 1;
        requests
            .execute_weight(&just_one, self.concurrent_requests)
            .await;
        let metadata = requests.take_inner_ok().pop().ok_or(anyhow!(
            "could not retrieve the metadata from the storage nodes"
        ))?;
        Ok(metadata)
    }

    /// Builds a [`NodeCommunication`] object for the given storage node.
    fn new_node_communication<'a>(
        &'a self,
        idx: usize,
        node: &'a StorageNode,
    ) -> NodeCommunication {
        NodeCommunication::new(
            idx,
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
            .map(|(idx, node)| self.new_node_communication(idx, node))
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
            .flat_map(|(idx, m)| m.shard_ids.iter().map(move |s| (*s, idx)))
            .collect::<HashMap<_, _>>();
        pairs.into_iter().for_each(|p| {
            pairs_per_node
                [shard_to_node[&p.index().to_shard_index(self.committee.n_shards(), blob_id)]]
                .push(p)
        });
        pairs_per_node
    }
}

/// Returns the 8 characters of the blob ID.
fn truncate_blob_id(blob_id: &BlobId) -> String {
    let mut blob_id_string = blob_id.to_string();
    blob_id_string.truncate(8);
    blob_id_string
}
