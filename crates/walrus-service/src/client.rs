// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, time::Instant};

use anyhow::{anyhow, Result};
use futures::{stream::FuturesUnordered, Future, Stream};
use reqwest::{Client as ReqwestClient, ClientBuilder};
use tokio::time::Duration;
use walrus_core::{
    encoding::{get_encoding_config, BlobDecoder, EncodingAxis, Sliver, SliverPair},
    metadata::VerifiedBlobMetadataWithId,
    BlobId,
    SignedStorageConfirmation,
    Sliver as SliverEnum,
};
use walrus_sui::types::{Committee, StorageNode};

use crate::mapping::shard_index_for_pair;

mod communication;
mod config;
mod utils;

pub use self::config::Config;
use self::{
    communication::NodeCommunication,
    utils::{WeightedFutures, WeightedResult},
};

/// A client to communicate with Walrus shards and storage nodes.
#[derive(Debug)]
pub struct Client {
    client: ReqwestClient,
    committee: Committee,
    concurrent_requests: usize,
}

impl Client {
    /// Creates a new client starting from a config file.
    // TODO(giac): Remove once fetching the configuration from the chain is available.
    pub fn new(config: Config) -> Result<Self> {
        let client = ClientBuilder::new()
            .timeout(config.connection_timeout)
            .build()?;
        Ok(Self {
            client,
            committee: config.committee,
            concurrent_requests: config.concurrent_requests,
        })
    }

    /// Encodes and stores a blob into Walrus by sending sliver pairs to at least 2f+1 shards.
    pub async fn store_blob(
        &self,
        blob: &[u8],
    ) -> Result<(VerifiedBlobMetadataWithId, Vec<SignedStorageConfirmation>)> {
        let (pairs, metadata) = get_encoding_config()
            .get_blob_encoder(blob)?
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
            .execute_weight(self.committee.quorum_threshold(), self.concurrent_requests)
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
        Ok((metadata, results))
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
        let futures = comms.iter().flat_map(|n| {
            n.node
                .shard_ids
                .iter()
                .map(|s| n.retrieve_verified_sliver::<T>(metadata, *s))
        });
        let mut decoder = get_encoding_config()
            .get_blob_decoder::<T>(metadata.metadata().unencoded_length.try_into()?)?;
        // Get the first ~1/3 or ~2/3 of slivers directly, and decode with these.
        let mut requests = WeightedFutures::new(futures);
        requests
            .execute_weight(
                get_encoding_config().n_source_symbols::<T>().into(),
                self.concurrent_requests,
            )
            .await;

        let slivers = requests.take_results();

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
    async fn decode_sliver_by_sliver<'a, I, Fut, T>(
        &self,
        requests: &mut WeightedFutures<I, Fut, Sliver<T>>,
        decoder: &mut BlobDecoder<'a, T>,
        blob_id: &BlobId,
    ) -> Result<Vec<u8>>
    where
        T: EncodingAxis,
        I: Iterator<Item = Fut>,
        Fut: Future<Output = WeightedResult<Sliver<T>>>,
        FuturesUnordered<Fut>: Stream<Item = WeightedResult<Sliver<T>>>,
    {
        while let Some(sliver) = requests.execute_next(self.concurrent_requests).await {
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
        requests.execute_weight(1, self.concurrent_requests).await;
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
}

#[cfg(test)]
mod tests {
    use walrus_core::encoding::{initialize_encoding_config, Primary};
    use walrus_sui::client::MockSuiReadClient;

    use super::*;
    use crate::test_utils::spawn_test_committee;

    #[tokio::test]
    #[ignore = "ignore E2E tests by default"]
    async fn test_store_and_read_blob() {
        let n_symbols_primary = 2;
        let n_symbols_secondary = 4;
        let n_shards = 10;
        initialize_encoding_config(n_symbols_primary, n_symbols_secondary, n_shards as u32);
        let blob = walrus_test_utils::random_data(31415);
        let blob_id = get_encoding_config()
            .get_blob_encoder(&blob)
            .unwrap()
            .compute_metadata()
            .blob_id()
            .to_owned();
        let sui_read_client = MockSuiReadClient::new_with_blob_ids([blob_id]);
        // Create a new committee of 4 nodes, 2 with 2 shards and 2 with 3 shards.
        let config = spawn_test_committee(
            n_symbols_primary,
            n_symbols_secondary,
            n_shards,
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
