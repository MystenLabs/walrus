// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::{anyhow, Context, Result};
use fastcrypto::{hash::Blake2b256, traits::VerifyingKey};
use futures::future::join_all;
use reqwest::Client as ReqwestClient;
use walrus_core::{
    encoding::{EncodingAxis, EncodingConfig, Sliver, SliverPair},
    messages::{Confirmation, StorageConfirmation},
    metadata::{
        SliverIndex,
        SliverPairIndex,
        UnverifiedBlobMetadataWithId,
        VerifiedBlobMetadataWithId,
    },
    BlobId,
    Epoch,
    PublicKey,
    ShardIndex,
    SignedStorageConfirmation,
    Sliver as SliverEnum,
    SliverType,
};
use walrus_sui::types::StorageNode;

use super::utils::{unwrap_response, WeightedResult};
use crate::{
    mapping::pair_index_for_shard,
    server::{METADATA_ENDPOINT, SLIVER_ENDPOINT, STORAGE_CONFIRMATION_ENDPOINT},
};

pub(crate) struct NodeCommunication<'a> {
    pub epoch: Epoch,
    pub client: &'a ReqwestClient,
    pub node: &'a StorageNode,
    pub total_weight: usize,
    pub encoding_config: &'a EncodingConfig,
}

impl<'a> NodeCommunication<'a> {
    pub fn new(
        epoch: Epoch,
        client: &'a ReqwestClient,
        node: &'a StorageNode,
        total_weight: usize,
        encoding_config: &'a EncodingConfig,
    ) -> Self {
        Self {
            epoch,
            client,
            node,
            total_weight,
            encoding_config,
        }
    }

    // Read operations.

    /// Requests the metadata for a blob ID from the node.
    pub async fn retrieve_verified_metadata(
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
            .json(&blob_id)
            .send()
            .await?;
        let confirmation = unwrap_response::<StorageConfirmation>(response).await?;
        // NOTE(giac): in the future additional values may be possible here.
        let StorageConfirmation::Signed(signed_confirmation) = confirmation;
        self.verify_confirmation(blob_id, &signed_confirmation)?;
        Ok(signed_confirmation)
    }

    pub async fn retrieve_verified_sliver<T: EncodingAxis>(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        shard_idx: ShardIndex,
    ) -> WeightedResult<Sliver<T>>
    where
        Sliver<T>: TryFrom<SliverEnum>,
    {
        let sliver = self.retrieve_sliver::<T>(metadata, shard_idx).await?;
        self.verify_sliver(metadata, &sliver, shard_idx)?;
        // Each sliver is in this case requested individually, so the weight is 1.
        Ok((1, sliver))
    }

    /// Requests a sliver from a shard.
    async fn retrieve_sliver<T: EncodingAxis>(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        shard_idx: ShardIndex,
    ) -> Result<Sliver<T>>
    where
        Sliver<T>: TryFrom<SliverEnum>,
    {
        let response = self
            .client
            .get(self.sliver_endpoint(
                metadata.blob_id(),
                SliverIndex(
                    pair_index_for_shard(shard_idx, self.total_weight, metadata.blob_id()) as u16,
                ),
                SliverType::for_encoding::<T>(),
            ))
            .send()
            .await?;
        let sliver_enum = unwrap_response::<SliverEnum>(response).await?;
        Ok(sliver_enum.to_raw::<T>()?)
    }

    // TODO(giac): this function should be added to `Sliver` as soon as the mapping has been moved
    // to walrus-core (issue #169). At that point, proper errors should be added.
    /// Checks that the provided sliver matches the corresponding hash in the metadata.
    fn verify_sliver<T: EncodingAxis>(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        sliver: &Sliver<T>,
        shard_idx: ShardIndex,
    ) -> Result<()> {
        let pair_metadata = metadata
            .metadata()
            .hashes
            .get(pair_index_for_shard(
                shard_idx,
                self.total_weight,
                metadata.blob_id(),
            ))
            .ok_or(anyhow!("missing hashes for the sliver"))?;
        anyhow::ensure!(
            sliver.symbols.len()
                == self.encoding_config.n_source_symbols::<T::OrthogonalAxis>() as usize
                && sliver.symbols.symbol_size()
                    == self
                        .encoding_config
                        .symbol_size_for_blob(metadata.metadata().unencoded_length.try_into()?)
                        .ok_or(anyhow!("the blob size in the metadata is too large"))?,
            "the size of the sliver does not match the expected size for the blob",
        );
        anyhow::ensure!(
            sliver.get_merkle_root::<Blake2b256>(self.encoding_config)?
                == *pair_metadata.hash::<T>(),
            "the sliver's Merkle root does not match the hash in the metadata"
        );
        Ok(())
    }

    // Write operations.

    /// Stores metadata and sliver pairs on a node, and requests a storage confirmation.
    ///
    /// Returns a [`WeightedResult`], where the weight is the number of shards for which the storage
    /// confirmation was issued.
    pub async fn store_metadata_and_pairs(
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
            .json(metadata.metadata())
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
        let mut futures = Vec::with_capacity(2 * pairs.len());
        for pair in pairs {
            let pair_index = pair.index();
            let SliverPair { primary, secondary } = pair;
            futures.extend([
                self.store_sliver(blob_id, SliverEnum::Primary(primary), pair_index),
                self.store_sliver(blob_id, SliverEnum::Secondary(secondary), pair_index),
            ]);
        }
        join_all(futures).await;
        Ok(())
    }

    /// Stores a sliver on a node.
    async fn store_sliver(
        &self,
        blob_id: &BlobId,
        sliver: SliverEnum,
        pair_index: SliverPairIndex,
    ) -> Result<()> {
        let response = self
            .client
            .put(self.sliver_endpoint(blob_id, pair_index, sliver.r#type()))
            .json(&sliver)
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
    fn public_key(&self) -> &PublicKey {
        &self.node.public_key
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
                .replace(":sliverPairIdx", &pair_index.to_string())
                .replace(":sliverType", &sliver_type.to_string()),
        )
    }

    fn request_url(addr: &str, path: &str) -> String {
        format!("http://{}{}", addr, path)
    }
}
