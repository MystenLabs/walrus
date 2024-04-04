// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use fastcrypto::{hash::Blake2b256, traits::VerifyingKey};
use futures::future::join_all;
use reqwest::Client as ReqwestClient;
use walrus_core::{
    encoding::{EncodingAxis, EncodingConfig, Sliver, SliverPair},
    ensure,
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

use super::{
    error::{
        CommunicationError,
        ConfirmationVerificationError,
        NodeError,
        NodeResult,
        SliverVerificationError,
        WrongInfoError,
    },
    utils::unwrap_response,
};
use crate::{
    mapping::pair_index_for_shard,
    server::{METADATA_ENDPOINT, SLIVER_ENDPOINT, STORAGE_CONFIRMATION_ENDPOINT},
};

macro_rules! propagate_node_error {
    ($self:ident, $result:expr) => {
        match $result {
            Ok(r) => r,
            Err(e) => return $self.to_node_result(Err(e.into())),
        }
    };
}

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

    fn to_node_result<T, E>(&self, result: Result<T, E>) -> NodeResult<T, E> {
        (self.epoch, self.node.clone(), result)
    }

    // Read operations.

    /// Requests the metadata for a blob ID from the node.
    pub async fn retrieve_verified_metadata(
        &self,
        blob_id: &BlobId,
    ) -> NodeResult<VerifiedBlobMetadataWithId, NodeError> {
        let response = propagate_node_error!(
            self,
            self.client
                .get(self.metadata_endpoint(blob_id))
                .send()
                .await
                .map_err(CommunicationError::from)
        );
        let unverified_metadata = propagate_node_error!(
            self,
            unwrap_response::<UnverifiedBlobMetadataWithId>(response).await
        );
        let metadata = propagate_node_error!(
            self,
            unverified_metadata
                .verify(get_encoding_config())
                .map_err(WrongInfoError::from)
        );
        self.to_node_result(Ok(metadata))
    }

    /// Requests the storage confirmation from the node.
    async fn retrieve_confirmation(
        &self,
        blob_id: &BlobId,
    ) -> Result<SignedStorageConfirmation, NodeError> {
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

    pub async fn retrieve_verified_sliver<T: EncodingAxis>(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        shard_idx: ShardIndex,
    ) -> NodeResult<Sliver<T>, NodeError>
    where
        Sliver<T>: TryFrom<SliverEnum>,
    {
        let sliver =
            propagate_node_error!(self, self.retrieve_sliver::<T>(metadata, shard_idx).await);
        propagate_node_error!(
            self,
            self.verify_sliver(metadata, &sliver, shard_idx)
                .map_err(WrongInfoError::from)
        );
        // Each sliver is in this case requested individually, so the weight is 1.
        self.to_node_result(Ok(sliver))
    }

    /// Requests a sliver from a shard.
    async fn retrieve_sliver<T: EncodingAxis>(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        shard_idx: ShardIndex,
    ) -> Result<Sliver<T>, NodeError>
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
            .await
            .map_err(CommunicationError::from)?;
        let sliver_enum = unwrap_response::<SliverEnum>(response).await?;
        Ok(sliver_enum.to_raw::<T>().map_err(WrongInfoError::from)?)
    }

    // TODO(giac): this function should be added to `Sliver` as soon as the mapping has been moved
    // to walrus-core (issue #169). At that point, proper errors should be added.
    /// Checks that the provided sliver matches the corresponding hash in the metadata.
    fn verify_sliver<T: EncodingAxis>(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        sliver: &Sliver<T>,
        shard_idx: ShardIndex,
    ) -> Result<(), SliverVerificationError> {
        ensure!(
            (shard_idx.0 as usize) < metadata.metadata().hashes.len(),
            SliverVerificationError::ShardIndexTooLarge
        );
        ensure!(
            metadata.metadata().hashes.len() == self.encoding_config.n_shards() as usize,
            SliverVerificationError::WrongNumberOfHashes
        );
        ensure!(
            sliver.symbols.len()
                == self.encoding_config.n_source_symbols::<T::OrthogonalAxis>() as usize,
            SliverVerificationError::SliverSizeMismatch
        );
        let symbol_size_from_metadata = self
            .encoding_config
            .symbol_size_for_blob(
                metadata
                    .metadata()
                    .unencoded_length
                    .try_into()
                    .expect("checked in `UnverifiedBlobMetadataWithId::verify`"),
            )
            .expect("the symbol size is checked in `UnverifiedBlobMetadataWithId::verify`");
        ensure!(
            sliver.symbols.symbol_size() == symbol_size_from_metadata,
            SliverVerificationError::SymbolSizeMismatch
        );
        let pair_metadata = metadata
            .metadata()
            .hashes
            .get(pair_index_for_shard(
                shard_idx,
                self.total_weight,
                metadata.blob_id(),
            ))
            .expect("n_shards and shard_index < n_shards are checked above");
        ensure!(
            sliver.get_merkle_root::<Blake2b256>(self.encoding_config)?
                == *pair_metadata.hash::<T>(),
            SliverVerificationError::MerkleRootMismatch
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
    ) -> NodeResult<SignedStorageConfirmation, NodeError> {
        // TODO(giac): add error handling and retries.
        propagate_node_error!(self, self.store_metadata(metadata).await);
        // TODO(giac): check the slivers that were not successfully stored and possibly retry.
        let results = self.store_pairs(metadata.blob_id(), pairs).await;
        // It is useless to request the confirmation if storing any of the slivers failed.
        let failed_requests: Vec<(SliverPairIndex, SliverType)> = results
            .into_iter()
            .filter_map(|r| match r {
                Err(CommunicationError::SliverStoreFailed(mut v)) => {
                    Some(v.pop().expect("there is exactly one pair per error"))
                }
                _ => None,
            })
            .collect();
        if !failed_requests.is_empty() {
            return self
                .to_node_result(Err(
                    CommunicationError::SliverStoreFailed(failed_requests).into()
                ));
        }
        let confirmation =
            propagate_node_error!(self, self.retrieve_confirmation(metadata.blob_id()).await);
        propagate_node_error!(
            self,
            self.verify_confirmation(metadata.blob_id(), &confirmation)
                .map_err(WrongInfoError::from)
        );
        self.to_node_result(Ok(confirmation))
    }

    /// Stores the metadata on the node.
    async fn store_metadata(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
    ) -> Result<(), CommunicationError> {
        let response = self
            .client
            .put(self.metadata_endpoint(metadata.blob_id()))
            .json(metadata.metadata())
            .send()
            .await?;
        ensure!(
            response.status().is_success(),
            CommunicationError::MetadataStoreFailed,
        );
        Ok(())
    }

    /// Stores the sliver pairs on the node _sequentially_.
    async fn store_pairs(
        &self,
        blob_id: &BlobId,
        pairs: Vec<SliverPair>,
    ) -> Vec<Result<(), CommunicationError>> {
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
    ) -> Result<(), CommunicationError> {
        let response = self
            .client
            .put(self.sliver_endpoint(blob_id, pair_index, sliver.r#type()))
            .json(&sliver)
            .send()
            .await?;
        ensure!(
            response.status().is_success(),
            CommunicationError::SliverStoreFailed(vec![(pair_index, sliver.r#type())])
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
    ) -> Result<(), ConfirmationVerificationError> {
        let deserialized: Confirmation = bcs::from_bytes(&confirmation.confirmation)?;
        ensure!(
            // TODO(giac): when the chain integration is added, ensure that the Epoch checks are
            // consistent and do not cause problems at epoch change.
            self.epoch == deserialized.epoch && *blob_id == deserialized.blob_id,
            ConfirmationVerificationError::EpochBlobIdMismatch
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
