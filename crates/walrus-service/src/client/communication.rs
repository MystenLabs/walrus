// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::num::NonZeroU16;

use anyhow::Result;
use futures::future::join_all;
use reqwest::{Client as ReqwestClient, Url};
use tracing::{Level, Span};
use walrus_core::{
    encoding::{EncodingAxis, EncodingConfig, Sliver, SliverPair},
    ensure,
    metadata::VerifiedBlobMetadataWithId,
    BlobId,
    Epoch,
    PublicKey,
    ShardIndex,
    SignedStorageConfirmation,
    Sliver as SliverEnum,
    SliverPairIndex,
};
use walrus_sdk::{client::Client as StorageNodeClient, error::NodeError};
use walrus_sui::types::StorageNode;

use super::{error::StoreError, string_prefix, utils::WeightedResult};
use crate::client::error::SliverStoreError;

/// Represents the index of the node in the vector of members of the committee.
pub type NodeIndex = usize;

/// Represents the result of an interaction with a storage node.
///
/// Contains the epoch, the "weight" of the interaction (e.g., the number of shards for which an
/// operation was performed), the storage node that issued it, and the result of the operation.
pub struct NodeResult<T, E>(pub Epoch, pub usize, pub NodeIndex, pub Result<T, E>);

impl<T, E> WeightedResult for NodeResult<T, E> {
    type Inner = T;
    type Error = E;
    fn weight(&self) -> usize {
        self.1
    }
    fn inner_result(&self) -> &Result<Self::Inner, Self::Error> {
        &self.3
    }
    fn take_inner_result(self) -> Result<Self::Inner, Self::Error> {
        self.3
    }
}

pub(crate) struct NodeCommunication<'a> {
    pub node_index: NodeIndex,
    pub epoch: Epoch,
    pub node: &'a StorageNode,
    pub encoding_config: &'a EncodingConfig,
    pub span: Span,
    pub client: StorageNodeClient,
}

impl<'a> NodeCommunication<'a> {
    pub fn new(
        node_index: NodeIndex,
        epoch: Epoch,
        client: &'a ReqwestClient,
        node: &'a StorageNode,
        encoding_config: &'a EncodingConfig,
    ) -> Self {
        let url = Url::parse(&format!("http://{}", node.network_address)).unwrap();

        Self {
            node_index,
            epoch,
            node,
            encoding_config,
            span: tracing::span!(
                Level::ERROR,
                "node",
                index=?node_index,
                ?epoch,
                pk_prefix=string_prefix(&node.public_key)
            ),
            client: StorageNodeClient::from_url(url, client.clone()),
        }
    }

    /// Returns the number of shards.
    pub fn n_shards(&self) -> NonZeroU16 {
        self.encoding_config.n_shards()
    }

    fn to_node_result<T, E>(&self, weight: usize, result: Result<T, E>) -> NodeResult<T, E> {
        NodeResult(self.epoch, weight, self.node_index, result)
    }

    // Read operations.

    /// Requests the metadata for a blob ID from the node.
    #[tracing::instrument(level="trace", parent=&self.span, skip_all)]
    pub async fn retrieve_verified_metadata(
        &self,
        blob_id: &BlobId,
    ) -> NodeResult<VerifiedBlobMetadataWithId, NodeError> {
        tracing::debug!("retrieving metadata");
        let result = self
            .client
            .get_and_verify_metadata(blob_id, self.encoding_config)
            .await;
        self.to_node_result(1, result)
    }

    /// Requests a sliver from the storage node, and verifies that it matches the metadata and
    /// encoding config.
    #[tracing::instrument(level="trace", parent=&self.span, skip(self, metadata))]
    pub async fn retrieve_verified_sliver<T: EncodingAxis>(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        shard_index: ShardIndex,
    ) -> NodeResult<Sliver<T>, NodeError>
    where
        Sliver<T>: TryFrom<SliverEnum>,
    {
        tracing::debug!("retrieving verified sliver");
        let sliver_pair_index = shard_index.to_pair_index(self.n_shards(), metadata.blob_id());
        let sliver = self
            .client
            .get_and_verify_sliver(sliver_pair_index, metadata, self.encoding_config)
            .await;

        // Each sliver is in this case requested individually, so the weight is 1.
        self.to_node_result(1, sliver)
    }

    // Write operations.

    /// Stores metadata and sliver pairs on a node, and requests a storage confirmation.
    ///
    /// Returns a [`NodeResult`], where the weight is the number of shards for which the storage
    /// confirmation was issued.
    #[tracing::instrument(level="trace", parent=&self.span, skip_all)]
    pub async fn store_metadata_and_pairs(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        pairs: Vec<SliverPair>,
    ) -> NodeResult<SignedStorageConfirmation, StoreError> {
        tracing::debug!("storing metadata and sliver pairs",);

        let result = async {
            // TODO(giac): add retry for metadata.
            self.client
                .store_metadata(metadata)
                .await
                .map_err(StoreError::Metadata)?;

            // TODO(giac): check the slivers that were not successfully stored and possibly retry.
            let results = self.store_pairs(metadata.blob_id(), pairs).await;

            // It is useless to request the confirmation if storing any of the slivers failed.
            let failed_requests = results
                .into_iter()
                .filter_map(Result::err)
                .collect::<Vec<_>>();
            ensure!(
                failed_requests.is_empty(),
                StoreError::SliverStore(failed_requests)
            );

            self.client
                .get_and_verify_confirmation(metadata.blob_id(), self.epoch, self.public_key())
                .await
                .map_err(StoreError::Confirmation)
        }
        .await;

        self.to_node_result(self.node.shard_ids.len(), result)
    }

    /// Stores the sliver pairs on the node.
    ///
    /// Returns the result of the [`store_sliver`][Self::store_sliver] operation for all the slivers
    /// in the storage node. The order of the returned results matches the order of the provided
    /// pairs, and for every pair the primary sliver precedes the secondary.
    async fn store_pairs(
        &self,
        blob_id: &BlobId,
        pairs: Vec<SliverPair>,
    ) -> Vec<Result<(), SliverStoreError>> {
        let futures: Vec<_> = pairs
            .into_iter()
            .flat_map(|pair| {
                let index = pair.index();
                [
                    self.store_sliver(blob_id, SliverEnum::Primary(pair.primary), index),
                    self.store_sliver(blob_id, SliverEnum::Secondary(pair.secondary), index),
                ]
            })
            .collect();

        join_all(futures).await
    }

    /// Stores a sliver on a node.
    async fn store_sliver(
        &self,
        blob_id: &BlobId,
        sliver: SliverEnum,
        pair_index: SliverPairIndex,
    ) -> Result<(), SliverStoreError> {
        self.client
            .store_sliver(blob_id, pair_index, &sliver)
            .await
            .map_err(|error| SliverStoreError {
                pair_index,
                sliver_type: sliver.r#type(),
                error,
            })
    }

    // Verification flows.

    /// Converts the public key of the node.
    fn public_key(&self) -> &PublicKey {
        &self.node.public_key
    }
}
