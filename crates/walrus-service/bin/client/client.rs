// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    net::{SocketAddr, ToSocketAddrs},
    time::Instant,
};

use anyhow::{anyhow, Context, Result};
use fastcrypto::{
    hash::Blake2b256,
    traits::{ToFromBytes, VerifyingKey},
};
use futures::{stream::FuturesUnordered, Future, Stream};
use reqwest::Client as ReqwestClient;
use tokio::time::Duration;
use walrus_core::{
    encoding::{get_encoding_config, BlobDecoder, EncodingAxis, Sliver, SliverPair},
    messages::Confirmation,
    metadata::{UnverifiedBlobMetadataWithId, VerifiedBlobMetadataWithId},
    BlobId,
    Epoch,
    PublicKey,
    ShardIndex,
    SignedStorageConfirmation,
    Sliver as SliverEnum,
    SliverType,
};
use walrus_service::{
    mapping::{pair_index_for_shard, shard_index_for_pair},
    server::{METADATA_ENDPOINT, SLIVER_ENDPOINT, STORAGE_CONFIRMATION_ENDPOINT},
};
use walrus_sui::types::{Committee, StorageNode};

use crate::{
    cli::Config,
    utils::{unwrap_response, WeightedFutures, WeightedResult},
};

/// A client to communicate with Walrus shards and storage nodes.
pub struct Client {
    client: ReqwestClient,
    committee: Committee,
}

/// The number of concurrent requests that the client performs towards storage nodes.
// TODO(giac): add to the configuration.
const CONCURRENT_REQUESTS: usize = 10;

impl Client {
    /// Creates a new client starting from a config file.
    // TODO(giac): Remove once fetching the configuration from the chain is available.
    pub fn new(config: Config) -> Self {
        Self {
            client: ReqwestClient::new(),
            committee: config.committee,
        }
    }

    /// Encodes and stores a blob into Walrus by sending sliver pairs to at least 2f+1 shards.
    pub async fn store_blob(&self, blob: Vec<u8>) -> Result<Vec<SignedStorageConfirmation>> {
        let (pairs, metadata) = get_encoding_config()
            .get_blob_encoder(&blob)?
            .encode_with_metadata();
        let pairs_per_node = self.pairs_per_node(metadata.blob_id(), pairs);
        let comms = self.node_communications();
        let mut requests = WeightedFutures::new(
            comms
                .iter()
                .zip(pairs_per_node.into_iter())
                .map(|(n, p)| n.store_metadata_and_pairs(&metadata, p)),
        );
        let start = Instant::now();
        requests
            .execute_weight(self.committee.quorum_threshold(), CONCURRENT_REQUESTS)
            .await;
        // Double the execution time, with a minimum of 100 ms. This gives the client time to
        // collect more storage confirmations.
        requests
            .execute_time(
                start.elapsed() + Duration::from_millis(100),
                CONCURRENT_REQUESTS,
            )
            .await;
        Ok(requests.into_results())
    }

    /// Reconstructs the blob by reading slivers from Walrus shards.
    pub async fn read_blob<T: EncodingAxis>(&self, blob_id: &BlobId) -> Result<Vec<u8>> {
        let metadata = self.retrieve_metadata(blob_id).await?;
        self.request_slivers_and_decode::<T>(&metadata).await
    }

    async fn request_slivers_and_decode<T: EncodingAxis>(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
    ) -> Result<Vec<u8>> {
        // TODO(giac): optimize by reading first from the shards that have the systematic part of
        // the encoding.
        let comms = self.node_communications();
        // Create requests to get all slivers from all nodes.
        let futures = comms.iter().flat_map(|n| {
            n.node
                .shard_ids
                .iter()
                .map(|s| n.retrieve_verified_sliver::<T>(metadata, ShardIndex(*s)))
        });
        let mut decoder = get_encoding_config()
            .get_blob_decoder::<T>(metadata.metadata().unencoded_length.try_into()?)?;
        // Get the first ~1/3 or ~2/3 of slivers directly, and decode with these.
        let mut requests = WeightedFutures::new(futures);
        requests
            .execute_weight(
                get_encoding_config().n_source_symbols::<T>().into(),
                CONCURRENT_REQUESTS,
            )
            .await;

        let slivers = requests.empty_results();

        if let Some((blob, _meta)) = decoder.decode_and_verify(metadata.blob_id(), slivers)? {
            // We have enough to decode the blob.
            Ok(blob)
        } else {
            // We were not able to decode. Keep requesting slivers and try decoding as soon as every
            // new sliver is received.
            Self::decode_sliver_by_sliver(&mut requests, &mut decoder, metadata.blob_id()).await
        }
    }

    /// Decodes the blob of given blob ID by requesting slivers and trying to decode at each new
    /// sliver it receives.
    async fn decode_sliver_by_sliver<'a, I, Fut, T: EncodingAxis>(
        requests: &mut WeightedFutures<I, Fut, Sliver<T>>,
        decoder: &mut BlobDecoder<'a, T>,
        blob_id: &BlobId,
    ) -> Result<Vec<u8>>
    where
        I: Iterator<Item = Fut>,
        Fut: Future<Output = WeightedResult<Sliver<T>>>,
        FuturesUnordered<Fut>: Stream<Item = WeightedResult<Sliver<T>>>,
    {
        while let Some(sliver) = requests.execute_next(CONCURRENT_REQUESTS).await {
            let result = decoder.decode_and_verify(blob_id, [sliver])?;
            if let Some((blob, _meta)) = result {
                return Ok(blob);
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
        let futures = comms.iter().map(|n| n.retrieve_verified_metadata(blob_id));
        // Wait until the first request succeeds
        let mut requests = WeightedFutures::new(futures);
        requests.execute_weight(1, CONCURRENT_REQUESTS).await;
        let metadata = requests.into_results().pop().ok_or(anyhow!(
            "could not retrieve the metadata from the storage nodes"
        ))?;
        Ok(metadata)
    }

    /// Builds a [`NodeCommunication`] object for the given storage node.
    fn new_node_communication<'a>(&'a self, node: &'a StorageNode) -> NodeCommunication {
        NodeCommunication::new(
            self.committee.epoch,
            &self.client,
            node,
            self.committee.total_weight,
        )
    }

    fn node_communications(&self) -> Vec<NodeCommunication> {
        self.committee
            .members
            .iter()
            .map(|n| self.new_node_communication(n))
            .collect()
    }

    /// Maps the sliver pairs to the node that holds their shard.
    fn pairs_per_node(&self, blob_id: &BlobId, pairs: Vec<SliverPair>) -> Vec<Vec<SliverPair>> {
        let mut pairs_per_node = vec![vec![]; self.committee.members.len()];
        let shard_to_node = self
            .committee
            .members
            .iter()
            .enumerate()
            .flat_map(|(idx, m)| m.shard_ids.iter().map(move |s| (ShardIndex(*s), idx)))
            .collect::<HashMap<_, _>>();
        pairs.into_iter().for_each(|p| {
            pairs_per_node[shard_to_node
                [&shard_index_for_pair(p.index() as u16, self.committee.total_weight, blob_id)]]
                .push(p)
        });
        pairs_per_node
    }
}

struct NodeCommunication<'a> {
    epoch: Epoch,
    client: &'a ReqwestClient,
    node: &'a StorageNode,
    total_weight: usize,
}

impl<'a> NodeCommunication<'a> {
    pub fn new(
        epoch: Epoch,
        client: &'a ReqwestClient,
        node: &'a StorageNode,
        total_weight: usize,
    ) -> Self {
        Self {
            epoch,
            client,
            node,
            total_weight,
        }
    }

    // Read operations.

    /// Requests the metadata for a blob ID from the node.
    async fn retrieve_verified_metadata(
        &self,
        blob_id: &BlobId,
    ) -> WeightedResult<VerifiedBlobMetadataWithId> {
        let response = self
            .client
            .get(self.metadata_endpoint(blob_id))
            .send()
            .await?;

        let metadata = unwrap_response::<UnverifiedBlobMetadataWithId>(response)
            .await?
            .verify(self.total_weight.try_into()?)
            .context("blob metadata verification failed")?;
        Ok((self.node.shard_ids.len(), metadata))
    }

    /// Requests the storage confirmation from the node.
    async fn retrieve_confirmation(&self, blob_id: &BlobId) -> Result<SignedStorageConfirmation> {
        let response = self
            .client
            .get(self.storage_confirmation_endpoint(blob_id))
            .send()
            .await?;
        let confirmation = unwrap_response::<SignedStorageConfirmation>(response).await?;
        self.verify_confirmation(blob_id, &confirmation)?;
        Ok(confirmation)
    }

    async fn retrieve_verified_sliver<T: EncodingAxis>(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        shard_idx: ShardIndex,
    ) -> WeightedResult<Sliver<T>> {
        let sliver = self.retrieve_sliver::<T>(metadata, shard_idx).await?;
        anyhow::ensure!(
            self.verify_sliver(metadata, &sliver, shard_idx)?,
            "the sliver hash does not match the metadata for the blob id"
        );
        // Each sliver is in this case requested individually, so the weight is 1.
        Ok((1, sliver))
    }

    /// Requests a sliver from a shard.
    async fn retrieve_sliver<T: EncodingAxis>(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        shard_idx: ShardIndex,
    ) -> Result<Sliver<T>> {
        let response = self
            .client
            .get(self.sliver_endpoint(
                metadata.blob_id(),
                pair_index_for_shard(shard_idx, self.total_weight, metadata.blob_id()) as u32,
                SliverType::for_encoding::<T>(),
            ))
            .send()
            .await?;
        unwrap_response(response).await
    }

    /// Checks that the provided sliver matches the corresponding hash in the metadata.
    fn verify_sliver<T: EncodingAxis>(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        sliver: &Sliver<T>,
        shard_idx: ShardIndex,
    ) -> Result<bool> {
        let pair_metadata = metadata
            .metadata()
            .hashes
            .get(pair_index_for_shard(
                shard_idx,
                self.total_weight,
                metadata.blob_id(),
            ))
            .ok_or(anyhow!("missing hashes for the sliver"))?;
        Ok(sliver.get_merkle_root::<Blake2b256>()? == *pair_metadata.hash::<T>())
    }

    // Write operations.

    /// Stores metadata and sliver pairs on a node, and requests a storage confirmation.
    ///
    /// Returns a [`WeightedResult`], where the weight is the number of shards for which the storage
    /// confirmation was issued.
    async fn store_metadata_and_pairs(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        pairs: Vec<SliverPair>,
    ) -> WeightedResult<SignedStorageConfirmation> {
        // TODO(giac): add error handling and retries.
        self.store_metadata(metadata).await?;
        self.store_pairs(metadata.blob_id(), pairs).await?;
        let confirmation = self.retrieve_confirmation(metadata.blob_id()).await?;
        self.verify_confirmation(metadata.blob_id(), &confirmation)?;
        Ok((self.node.shard_ids.len(), confirmation))
    }

    /// Stores the metadata on the node.
    async fn store_metadata(&self, metadata: &VerifiedBlobMetadataWithId) -> Result<()> {
        let response = self
            .client
            .put(self.metadata_endpoint(metadata.blob_id()))
            .json(metadata)
            .send()
            .await?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(anyhow!("failed to store metadata on node {:?}", self.node))
        }
    }

    /// Stores the sliver pairs on the node _sequentially_.
    async fn store_pairs(&self, blob_id: &BlobId, pairs: Vec<SliverPair>) -> Result<()> {
        for pair in pairs.into_iter() {
            let pair_index = pair.index();
            let SliverPair { primary, secondary } = pair;
            self.store_sliver(blob_id, &SliverEnum::Primary(primary), pair_index)
                .await?;
            self.store_sliver(blob_id, &SliverEnum::Secondary(secondary), pair_index)
                .await?;
        }
        Ok(())
    }

    /// Stores a sliver on a node.
    async fn store_sliver(
        &self,
        blob_id: &BlobId,
        sliver: &SliverEnum,
        pair_index: u32,
    ) -> Result<()> {
        let response = self
            .client
            .put(self.sliver_endpoint(blob_id, pair_index, sliver.r#type()))
            .json(sliver)
            .send()
            .await?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(anyhow!("failed to store metadata on node {:?}", self.node))
        }
    }

    // Verification flows.

    /// Converts the public key of the node.
    // TODO(giac): remove when we change the key in `StorageNode`.
    fn public_key(&self) -> Result<PublicKey> {
        Ok(PublicKey::from_bytes(&self.node.public_key)?)
    }

    /// Checks the signature and the contents of a storage confirmation.
    fn verify_confirmation(
        &self,
        blob_id: &BlobId,
        confirmation: &SignedStorageConfirmation,
    ) -> Result<()> {
        let deserialized: Confirmation = bcs::from_bytes(&confirmation.confirmation)?;
        anyhow::ensure!(
            // TODO(giac): when the chain integration is added, ensure that the Epoch checks are
            // consistent and do not cause problems at epoch change.
            self.epoch == deserialized.epoch && *blob_id == deserialized.blob_id,
            "the epoch or the blob ID in the storage confirmation are mismatched"
        );
        Ok(self
            .public_key()?
            .verify(&confirmation.confirmation, &confirmation.signature)?)
    }

    // Endpoints.

    /// Returns the URL of the storage confirmation endpoint.
    fn storage_confirmation_endpoint(&self, blob_id: &BlobId) -> String {
        Self::request_url(&self.address(), &Self::storage_confirmation_path(blob_id))
    }

    /// Returns the URL of the metadata endpoint.
    fn metadata_endpoint(&self, blob_id: &BlobId) -> String {
        Self::request_url(&self.address(), &Self::metadata_path(blob_id))
    }

    /// Returns the URL of the primary/secondary sliver endpoint.
    fn sliver_endpoint(
        &self,
        blob_id: &BlobId,
        pair_index: u32,
        sliver_type: SliverType,
    ) -> String {
        Self::request_url(
            &self.address(),
            &Self::sliver_path(blob_id, pair_index, sliver_type),
        )
    }

    fn storage_confirmation_path(blob_id: &BlobId) -> String {
        STORAGE_CONFIRMATION_ENDPOINT.replace(":blobId", &blob_id.to_string())
    }

    fn metadata_path(blob_id: &BlobId) -> String {
        METADATA_ENDPOINT.replace(":blobId", &blob_id.to_string())
    }

    fn sliver_path(blob_id: &BlobId, pair_index: u32, sliver_type: SliverType) -> String {
        SLIVER_ENDPOINT
            .replace(":blobId", &blob_id.to_string())
            .replace(":sliverPairIdx", &pair_index.to_string())
            .replace(":sliverType", &sliver_type.to_string())
    }

    fn request_url(addr: &SocketAddr, path: &str) -> String {
        format!("http://{}{}", addr, path)
    }

    fn address(&self) -> SocketAddr {
        self.node
            .network_address
            .to_socket_addrs()
            // TODO(giac): Change this when the `StorageNode` type is changed.
            .expect("the node addresses could not be parsed")
            .next()
            .expect("there must be one node address")
    }
}
