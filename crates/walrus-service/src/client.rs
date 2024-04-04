// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, time::Instant};

use anyhow::{anyhow, Result};
use futures::{stream::FuturesUnordered, Future, Stream};
use reqwest::{Client as ReqwestClient, ClientBuilder};
use tokio::time::{timeout, Duration};
use tokio_stream::StreamExt;
use walrus_core::{
    encoding::{BlobDecoder, EncodingAxis, EncodingConfig, Sliver, SliverPair},
    metadata::VerifiedBlobMetadataWithId,
    BlobId,
    SignedStorageConfirmation,
    Sliver as SliverEnum,
};
use walrus_sui::types::{Committee, StorageNode};

use crate::mapping::shard_index_for_pair;

mod communication;
mod config;
mod error;
mod utils;

pub use self::config::Config;
use self::{
    communication::NodeCommunication,
    error::{NodeError, NodeResult},
    utils::push_futures,
};

/// A client to communicate with Walrus shards and storage nodes.
#[derive(Debug)]
pub struct Client {
    client: ReqwestClient,
    committee: Committee,
    concurrent_store_requests: usize,
    concurrent_metadata_requests: usize,
    concurrent_sliver_read_requests: usize,
}

impl Client {
    /// Creates a new client starting from a config file.
    // TODO(giac): Remove once fetching the configuration from the chain is available.
    pub fn new(config: Config) -> Result<Self> {
        let client = ClientBuilder::new()
            .timeout(config.connection_timeout)
            .build()?;
        let encoding_config = config.encoding_config();
        Ok(Self {
            client,
            committee: config.committee,
            concurrent_store_requests: config.concurrent_store_requests,
            concurrent_metadata_requests: config.concurrent_metadata_requests,
            concurrent_sliver_read_requests: config.concurrent_sliver_read_requests,
        })
    }

    /// Encodes and stores a blob into Walrus by sending sliver pairs to at least 2f+1 shards.
    pub async fn store_blob(
        &self,
        blob: &[u8],
    ) -> Result<(VerifiedBlobMetadataWithId, Vec<SignedStorageConfirmation>)> {
        let (pairs, metadata) = self
            .encoding_config
            .get_blob_encoder(blob)?
            .encode_with_metadata();
        let pairs_per_node = self.pairs_per_node(metadata.blob_id(), pairs);
        let comms = self.node_communications();
        let mut futures = comms
            .iter()
            .zip(pairs_per_node.into_iter())
            .map(|(n, p)| n.store_metadata_and_pairs(&metadata, p));
        let mut requests = FuturesUnordered::new();
        push_futures(self.concurrent_store_requests, &mut futures, &mut requests);

        let mut confirmations = Vec::with_capacity(self.committee.total_weight);
        let mut confirmation_weight = 0;
        let start = Instant::now();
        while let Some((epoch, node, result)) = requests.next().await {
            match result {
                Ok(confirmation) => {
                    tracing::info!(
                        "storage confirmation obtained from node {:?} for epoch {:?}",
                        node,
                        epoch,
                    );
                    confirmation_weight += node.shard_ids.len();
                    confirmations.push(confirmation);
                    if self.committee.is_quorum(confirmation_weight) {
                        break;
                    }
                }
                Err(e) => {
                    tracing::info!(
                        "storage confirmation could not be obtained from \
                                    node {node:?} for epoch {epoch} with error: {e:?}"
                    );
                }
            }
            if let Some(future) = futures.next() {
                requests.push(future);
            }
        }

        // Double the execution time, with a minimum of 100 ms. This gives the client time to
        // collect more storage confirmations.
        let to_wait = start.elapsed() + Duration::from_millis(100);
        let _ = timeout(to_wait, async {
            while let Some((epoch, node, result)) = requests.next().await {
                match result {
                    Ok(confirmation) => {
                        tracing::info!(
                            "storage confirmation obtained from node {:?} for epoch {:?}",
                            node,
                            epoch,
                        );
                        confirmations.push(confirmation);
                    }
                    Err(e) => {
                        tracing::info!(
                            "storage confirmation could not be obtained from \
                                        node {node:?} for epoch {epoch} with error: {e:?}"
                        );
                    }
                }
                if let Some(future) = futures.next() {
                    requests.push(future);
                }
            }
        })
        .await;
        drop(requests);
        Ok((metadata, confirmations))
    }

    /// Reconstructs the blob by reading slivers from Walrus shards.
    pub async fn read_blob<T>(&self, blob_id: &BlobId) -> Result<Vec<u8>>
    where
        T: EncodingAxis,
        Sliver<T>: TryFrom<SliverEnum>,
    {
        let metadata = self.retrieve_metadata(blob_id).await?;
        self.request_slivers_and_decode::<T>(&metadata).await
    }

    async fn request_slivers_and_decode<T>(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
    ) -> Result<Vec<u8>>
    where
        T: EncodingAxis,
        Sliver<T>: TryFrom<SliverEnum>,
    {
        // TODO(giac): optimize by reading first from the shards that have the systematic part of
        // the encoding.
        let comms = self.node_communications();
        // Create requests to get all slivers from all nodes.
        let mut futures = comms.iter().flat_map(|n| {
            n.node
                .shard_ids
                .iter()
                .map(|s| n.retrieve_verified_sliver::<T>(metadata, *s))
        });
        let mut decoder =
            config.get_blob_decoder::<T>(metadata.metadata().unencoded_length.try_into()?)?;
        let mut requests = FuturesUnordered::new();
        push_futures(
            self.concurrent_sliver_read_requests,
            &mut futures,
            &mut requests,
        );

        let mut slivers = Vec::with_capacity(self.committee.total_weight);
        // Get the first ~1/3 or ~2/3 of slivers directly, and decode with these.
        while let Some((epoch, node, result)) = requests.next().await {
            match result {
                Ok(sliver) => {
                    tracing::info!("sliver obtained from node {:?} for epoch {:?}", node, epoch,);
                    slivers.push(sliver);
                    if self.meets_encoding_threshold::<T>(slivers.len()) {
                        break;
                    }
                }
                Err(e) => {
                    tracing::info!(
                        "sliver could not be obtained from \
                                    node {node:?} for epoch {epoch} with error: {e:?}"
                    );
                }
            }
            if let Some(future) = futures.next() {
                requests.push(future);
            }
        }
        if let Some((blob, _meta)) = decoder.decode_and_verify(metadata.blob_id(), slivers)? {
            // We have enough to decode the blob.
            Ok(blob)
        } else {
            // We were not able to decode. Keep requesting slivers and try decoding as soon as every
            // new sliver is received.
            self.decode_sliver_by_sliver(
                &mut requests,
                &mut futures,
                &mut decoder,
                metadata.blob_id(),
            )
            .await
        }
    }

    /// Decodes the blob of given blob ID by requesting slivers and trying to decode at each new
    /// sliver it receives.
    async fn decode_sliver_by_sliver<'a, I, Fut, T>(
        &self,
        requests: &mut FuturesUnordered<Fut>,
        futures: &mut I,
        decoder: &mut BlobDecoder<'a, T>,
        blob_id: &BlobId,
    ) -> Result<Vec<u8>>
    where
        T: EncodingAxis,
        I: Iterator<Item = Fut>,
        Fut: Future<Output = NodeResult<Sliver<T>, NodeError>>,
        FuturesUnordered<Fut>: Stream<Item = NodeResult<Sliver<T>, NodeError>>,
    {
        while let Some((epoch, node, result)) = requests.next().await {
            match result {
                Ok(sliver) => {
                    tracing::info!("sliver obtained from node {:?} for epoch {:?}", node, epoch,);
                    let result = decoder.decode_and_verify(blob_id, [sliver])?;
                    if let Some((blob, _meta)) = result {
                        return Ok(blob);
                    }
                }
                Err(e) => {
                    tracing::info!(
                        "sliver could not be obtained from \
                                    node {node:?} for epoch {epoch} with error: {e:?}"
                    );
                }
            }
            if let Some(future) = futures.next() {
                requests.push(future);
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
        let mut futures = comms.iter().map(|n| n.retrieve_verified_metadata(blob_id));
        let mut requests = FuturesUnordered::new();
        push_futures(
            self.concurrent_metadata_requests,
            &mut futures,
            &mut requests,
        );
        while let Some((epoch, node, result)) = requests.next().await {
            if let Ok(metadata) = result {
                tracing::info!(
                    "metadata obtained from node {:?} for blob id {:?} and epoch {:?}",
                    node,
                    blob_id,
                    epoch
                );
                return Ok(metadata);
            } else {
                tracing::info!(
                    "metadata could not be obtained from node {node:?} for blob id \
                                {blob_id} and epoch {epoch} with error: {result:?}"
                );
                if let Some(future) = futures.next() {
                    requests.push(future);
                }
            }
        }
        Err(anyhow!("could not retrieve the metadata from any node"))
    }

    /// Builds a [`NodeCommunication`] object for the given storage node.
    fn new_node_communication<'a>(&'a self, node: &'a StorageNode) -> NodeCommunication {
        NodeCommunication::new(
            self.committee.epoch,
            &self.client,
            node,
            self.committee.total_weight,
            &self.encoding_config,
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
        let mut pairs_per_node = Vec::with_capacity(self.committee.members.len());
        pairs_per_node.extend(
            self.committee
                .members
                .iter()
                .map(|n| Vec::with_capacity(n.shard_ids.len())),
        );
        let shard_to_node = self
            .committee
            .members
            .iter()
            .enumerate()
            .flat_map(|(idx, m)| m.shard_ids.iter().map(move |s| (*s, idx)))
            .collect::<HashMap<_, _>>();
        pairs.into_iter().for_each(|p| {
            pairs_per_node[shard_to_node
                [&shard_index_for_pair(p.index(), self.committee.total_weight, blob_id)]]
                .push(p)
        });
        pairs_per_node
    }

    fn meets_encoding_threshold<T: EncodingAxis>(&self, num: usize) -> bool {
        if T::IS_PRIMARY {
            self.committee.is_validity(num)
        } else {
            self.committee.is_quorum(num)
        }
    }
}

#[cfg(test)]
mod tests {
    use walrus_core::encoding::Primary;
    use walrus_sui::test_utils::MockSuiReadClient;

    use super::*;
    use crate::test_utils;

    #[tokio::test]
    #[ignore = "ignore E2E tests by default"]
    async fn test_store_and_read_blob() {
        let encoding_config = EncodingConfig::new(2, 4, 10);
        let blob = walrus_test_utils::random_data(31415);
        let blob_id = encoding_config
            .get_blob_encoder(&blob)
            .unwrap()
            .compute_metadata()
            .blob_id()
            .to_owned();
        let sui_read_client = MockSuiReadClient::new_with_blob_ids([blob_id], None);
        // Create a new committee of 4 nodes, 2 with 2 shards and 2 with 3 shards.
        let config = test_utils::spawn_test_committee(
            encoding_config,
            &[&[0, 1], &[2, 3], &[4, 5, 6], &[7, 8, 9]],
            sui_read_client,
        )
        .await;
        tokio::time::sleep(Duration::from_millis(1)).await;
        let client = Client::new(config).unwrap();

        // Store a blob and get confirmations from each node.
        let (metadata, confirmation) = client.store_blob(&blob).await.unwrap();
        assert!(confirmation.len() == 4);

        // Read the blob.
        let read_blob = client
            .read_blob::<Primary>(metadata.blob_id())
            .await
            .unwrap();
        assert_eq!(read_blob, blob);
    }
}
