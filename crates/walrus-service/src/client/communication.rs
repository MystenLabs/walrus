// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fmt::Display, num::NonZeroU16};

use anyhow::Result;
use fastcrypto::traits::VerifyingKey;
use futures::future::join_all;
use reqwest::Client as ReqwestClient;
use tracing::{Level, Span};
use walrus_core::{
    encoding::{EncodingAxis, EncodingConfig, Sliver, SliverPair},
    ensure,
    messages::{Confirmation, StorageConfirmation},
    metadata::{UnverifiedBlobMetadataWithId, VerifiedBlobMetadataWithId},
    BlobId,
    Epoch,
    PublicKey,
    ShardIndex,
    SignedStorageConfirmation,
    Sliver as SliverEnum,
    SliverPairIndex,
    SliverType,
};
use walrus_sui::types::StorageNode;

use super::{
    error::{
        CommunicationError,
        ConfirmationRetrieveError,
        MetadataRetrieveError,
        MetadataStoreError,
        SliverRetrieveError,
        StoreError,
    },
    utils::{unwrap_response, WeightedResult},
};
use crate::{
    client::error::SliverStoreError,
    server::{METADATA_ENDPOINT, SLIVER_ENDPOINT, STORAGE_CONFIRMATION_ENDPOINT},
};

/// Represents the index of the node in the vector of members of the committee.
pub type NodeIndex = usize;

/// Represents the result of an interaction with a storage node. Contains the epoch, the "weight" of
/// the interaction (e.g., the number of shards for which an operation was performed), the storage
/// node that issued it, and the result of the operation.
// NOTE(giac): the `StorageNode` in the following will be changed to the node index in the members
// vector in PR #243.
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
    pub client: &'a ReqwestClient,
    pub node: &'a StorageNode,
    pub encoding_config: &'a EncodingConfig,
    pub span: Span,
}

impl<'a> NodeCommunication<'a> {
    pub fn new(
        node_index: NodeIndex,
        epoch: Epoch,
        client: &'a ReqwestClient,
        node: &'a StorageNode,
        encoding_config: &'a EncodingConfig,
    ) -> Self {
        Self {
            node_index,
            epoch,
            client,
            node,
            encoding_config,
            span: tracing::span!(Level::ERROR, "node", ?epoch, public_key=?node.public_key),
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
    ) -> NodeResult<VerifiedBlobMetadataWithId, MetadataRetrieveError> {
        tracing::debug!("retrieving metadata");
        let inner = || async {
            let response = self
                .client
                .get(self.metadata_endpoint(blob_id))
                .send()
                .await
                .map_err(CommunicationError::from)?;
            unwrap_response::<UnverifiedBlobMetadataWithId>(response)
                .await?
                .verify(self.encoding_config)
                .map_err(MetadataRetrieveError::from)
        };
        self.to_node_result(1, inner().await)
    }

    async fn retrieve_verified_confirmation(
        &self,
        blob_id: &BlobId,
    ) -> Result<SignedStorageConfirmation, ConfirmationRetrieveError> {
        let confirmation = self.retrieve_confirmation(blob_id).await?;
        self.verify_confirmation(blob_id, &confirmation)?;
        Ok(confirmation)
    }

    /// Requests the storage confirmation from the node.
    async fn retrieve_confirmation(
        &self,
        blob_id: &BlobId,
    ) -> Result<SignedStorageConfirmation, CommunicationError> {
        tracing::debug!("retrieving storage confirmation");
        let response = self
            .client
            .get(self.storage_confirmation_endpoint(blob_id))
            .json(&blob_id)
            .send()
            .await
            .map_err(CommunicationError::from)?;
        let confirmation = unwrap_response::<StorageConfirmation>(response).await?;
        // NOTE(giac): in the future additional values may be possible here.
        let StorageConfirmation::Signed(signed_confirmation) = confirmation;
        Ok(signed_confirmation)
    }

    /// Requests a sliver from the storage node, and verifies that it matches the metadata and
    /// encoding config.
    #[tracing::instrument(level="trace", parent=&self.span, skip(self, metadata))]
    pub async fn retrieve_verified_sliver<T: EncodingAxis>(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        shard_idx: ShardIndex,
    ) -> NodeResult<Sliver<T>, SliverRetrieveError>
    where
        Sliver<T>: TryFrom<SliverEnum>,
    {
        tracing::debug!("retrieving sliver from shard",);
        let inner = || async {
            let sliver = self.retrieve_sliver::<T>(metadata, shard_idx).await?;
            sliver.verify(self.encoding_config, metadata)?;
            Ok(sliver)
        };
        // Each sliver is in this case requested individually, so the weight is 1.
        self.to_node_result(1, inner().await)
    }

    /// Requests a sliver from a shard.
    async fn retrieve_sliver<T: EncodingAxis>(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        shard_idx: ShardIndex,
    ) -> Result<Sliver<T>, SliverRetrieveError>
    where
        Sliver<T>: TryFrom<SliverEnum>,
    {
        let response = self
            .client
            .get(self.sliver_endpoint(
                metadata.blob_id(),
                shard_idx.to_pair_index(self.n_shards(), metadata.blob_id()),
                SliverType::for_encoding::<T>(),
            ))
            .send()
            .await
            .map_err(CommunicationError::from)?;
        let sliver_enum = unwrap_response::<SliverEnum>(response).await?;
        Ok(sliver_enum.to_raw::<T>()?)
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
        let inner = || async {
            // TODO(giac): add retry for metadata.
            self.store_metadata(metadata).await?;
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
            self.retrieve_verified_confirmation(metadata.blob_id())
                .await
                .map_err(StoreError::ConfirmationRetrieve)
        };
        self.to_node_result(self.node.shard_ids.len(), inner().await)
    }

    /// Stores the metadata on the node.
    async fn store_metadata(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
    ) -> Result<(), MetadataStoreError> {
        let response = self
            .client
            .put(self.metadata_endpoint(metadata.blob_id()))
            .body(
                bcs::to_bytes(metadata.metadata()).expect("is bcs encodable within limit defaults"),
            )
            .send()
            .await
            .map_err(CommunicationError::from)?;
        ensure!(
            response.status().is_success(),
            CommunicationError::HttpFailure(response.status()).into(),
        );
        Ok(())
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
        let mut futures = Vec::with_capacity(2 * pairs.len());
        for pair in pairs {
            let pair_index = pair.index();
            let SliverPair { primary, secondary } = pair;
            futures.extend([
                self.store_sliver(blob_id, SliverEnum::Primary(primary), pair_index),
                self.store_sliver(blob_id, SliverEnum::Secondary(secondary), pair_index),
            ]);
        }
        join_all(futures).await
    }

    /// Stores a sliver on a node.
    async fn store_sliver(
        &self,
        blob_id: &BlobId,
        sliver: SliverEnum,
        pair_index: SliverPairIndex,
    ) -> Result<(), SliverStoreError> {
        let response = self
            .client
            .put(self.sliver_endpoint(blob_id, pair_index, sliver.r#type()))
            .body(bcs::to_bytes(&sliver).expect("internal types to be bcs encodable"))
            .send()
            .await
            .map_err(|e| SliverStoreError {
                pair_idx: pair_index,
                sliver_type: sliver.r#type(),
                error: e.into(),
            })?;
        ensure!(
            response.status().is_success(),
            SliverStoreError {
                pair_idx: pair_index,
                sliver_type: sliver.r#type(),
                error: CommunicationError::HttpFailure(response.status())
            }
        );
        Ok(())
    }

    // Verification flows.

    /// Converts the public key of the node.
    fn public_key(&self) -> &PublicKey {
        &self.node.public_key
    }

    /// Checks the signature and the contents of a storage confirmation.
    fn verify_confirmation(
        &self,
        blob_id: &BlobId,
        confirmation: &SignedStorageConfirmation,
    ) -> Result<(), ConfirmationRetrieveError> {
        let deserialized: Confirmation = bcs::from_bytes(&confirmation.confirmation)?;
        ensure!(
            // TODO(giac): when the chain integration is added, ensure that the Epoch checks are
            // consistent and do not cause problems at epoch change.
            self.epoch == deserialized.epoch && *blob_id == deserialized.blob_id,
            ConfirmationRetrieveError::EpochBlobIdMismatch
        );
        Ok(self
            .public_key()
            .verify(&confirmation.confirmation, &confirmation.signature)?)
    }

    // Endpoints.

    /// Returns the URL of the storage confirmation endpoint.
    fn storage_confirmation_endpoint(&self, blob_id: &BlobId) -> String {
        Self::request_url(
            &self.node.network_address.to_string(),
            &STORAGE_CONFIRMATION_ENDPOINT.replace(":blobId", &blob_id.to_string()),
        )
    }

    /// Returns the URL of the metadata endpoint.
    fn metadata_endpoint(&self, blob_id: &BlobId) -> String {
        Self::request_url(
            &self.node.network_address.to_string(),
            &METADATA_ENDPOINT.replace(":blobId", &blob_id.to_string()),
        )
    }

    /// Returns the URL of the primary/secondary sliver endpoint.
    fn sliver_endpoint(
        &self,
        blob_id: &BlobId,
        pair_index: SliverPairIndex,
        sliver_type: SliverType,
    ) -> String {
        Self::request_url(
            &self.node.network_address.to_string(),
            &SLIVER_ENDPOINT
                .replace(":blobId", &blob_id.to_string())
                .replace(":sliverPairIdx", &pair_index.0.to_string())
                .replace(":sliverType", &sliver_type.to_string()),
        )
    }

    fn request_url(addr: &str, path: &str) -> String {
        format!("http://{}{}", addr, path)
    }
}

impl<'a> Display for NodeCommunication<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // TODO(giac): after or in PR #243, move to node index, public key prefix, and epoch.
        write!(f, "ID: {}; Epoch: {};", self.node.public_key, self.epoch)
    }
}
