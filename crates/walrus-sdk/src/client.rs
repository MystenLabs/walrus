// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Client for interacting with the StorageNode API.

use reqwest::{Client as ReqwestClient, Url};
use walrus_core::{
    encoding::{
        EncodingAxis,
        EncodingConfig,
        Primary,
        RecoverySymbol,
        Secondary,
        Sliver,
        SliverPair,
    },
    inconsistency::InconsistencyProof,
    merkle::MerkleProof,
    messages::{InvalidBlobIdAttestation, SignedStorageConfirmation, StorageConfirmation},
    metadata::{UnverifiedBlobMetadataWithId, VerifiedBlobMetadataWithId},
    BlobId,
    Epoch,
    InconsistencyProof as InconsistencyProofEnum,
    PublicKey,
    Sliver as SliverEnum,
    SliverPairIndex,
    SliverType,
};

use crate::{
    api::BlobStatus,
    error::{Kind, NodeError},
    node_response::NodeResponse as _,
};

#[derive(Debug, Clone)]
struct UrlEndpoints(Url);

impl UrlEndpoints {
    fn blob_resource(&self, blob_id: &BlobId) -> Url {
        self.0.join(&format!("/v1/blobs/{blob_id}/")).unwrap()
    }

    fn metadata(&self, blob_id: &BlobId) -> Url {
        self.blob_resource(blob_id).join("metadata").unwrap()
    }

    fn confirmation(&self, blob_id: &BlobId) -> Url {
        self.blob_resource(blob_id).join("confirmation").unwrap()
    }

    fn blob_status(&self, blob_id: &BlobId) -> Url {
        self.blob_resource(blob_id).join("status").unwrap()
    }

    fn recovery_symbol<A: EncodingAxis>(
        &self,
        blob_id: &BlobId,
        sliver_pair_at_remote: SliverPairIndex,
        intersecting_pair_index: SliverPairIndex,
    ) -> Url {
        let mut url = self.sliver::<A>(blob_id, sliver_pair_at_remote);
        url.path_segments_mut()
            .unwrap()
            .push(&intersecting_pair_index.0.to_string());
        url
    }

    fn sliver<A: EncodingAxis>(
        &self,
        blob_id: &BlobId,
        SliverPairIndex(sliver_pair_index): SliverPairIndex,
    ) -> Url {
        let sliver_type = SliverType::for_encoding::<A>();
        let path = format!("slivers/{sliver_pair_index}/{sliver_type}");
        self.blob_resource(blob_id).join(&path).unwrap()
    }

    fn inconsistency_proof<A: EncodingAxis>(&self, blob_id: &BlobId) -> Url {
        let sliver_type = SliverType::for_encoding::<A>();
        let path = format!("inconsistent/{sliver_type}");
        self.blob_resource(blob_id).join(&path).unwrap()
    }
}

/// A client for communicating with a StorageNode.
#[derive(Debug, Clone)]
pub struct Client {
    inner: ReqwestClient,
    endpoints: UrlEndpoints,
}

impl Client {
    /// Creates a new client for the storage node at the specified URL.
    pub fn from_url(url: Url, inner: ReqwestClient) -> Self {
        Self {
            endpoints: UrlEndpoints(url),
            inner,
        }
    }

    /// Converts this to the inner client.
    pub fn into_inner(self) -> ReqwestClient {
        self.inner
    }

    /// Requests the metadata for a blob ID from the node.
    pub async fn get_metadata(
        &self,
        blob_id: &BlobId,
    ) -> Result<UnverifiedBlobMetadataWithId, NodeError> {
        let url = self.endpoints.metadata(blob_id);
        let response = self.inner.get(url).send().await.map_err(Kind::Reqwest)?;
        let metadata: UnverifiedBlobMetadataWithId =
            response.response_error_for_status().await?.bcs().await?;

        Ok(metadata)
    }

    /// Get the metadata and verify it against the provided config.
    pub async fn get_and_verify_metadata(
        &self,
        blob_id: &BlobId,
        encoding_config: &EncodingConfig,
    ) -> Result<VerifiedBlobMetadataWithId, NodeError> {
        let metadata = self
            .get_metadata(blob_id)
            .await?
            .verify(encoding_config)
            .map_err(NodeError::other)?;
        Ok(metadata)
    }

    /// Requests the status of a blob ID from the node.
    pub async fn get_blob_status(&self, blob_id: &BlobId) -> Result<BlobStatus, NodeError> {
        let url = self.endpoints.blob_status(blob_id);
        let response = self.inner.get(url).send().await.map_err(Kind::Reqwest)?;
        let blob_status: BlobStatus = response
            .response_error_for_status()
            .await?
            .json()
            .await
            .map_err(Kind::Json)?;

        Ok(blob_status)
    }

    /// Requests a storage confirmation from the node for the Blob specified by the given ID
    pub async fn get_confirmation(
        &self,
        blob_id: &BlobId,
    ) -> Result<SignedStorageConfirmation, NodeError> {
        let url = self.endpoints.confirmation(blob_id);
        let response = self.inner.get(url).send().await.map_err(Kind::Reqwest)?;

        // NOTE(giac): in the future additional values may be possible here.
        let StorageConfirmation::Signed(confirmation) = response.service_response().await?;

        Ok(confirmation)
    }

    /// Requests a storage confirmation from the node for the Blob specified by the given ID
    pub async fn get_and_verify_confirmation(
        &self,
        blob_id: &BlobId,
        epoch: Epoch,
        public_key: &PublicKey,
    ) -> Result<SignedStorageConfirmation, NodeError> {
        let confirmation = self.get_confirmation(blob_id).await?;
        let _ = confirmation
            .verify(public_key, epoch, blob_id)
            .map_err(NodeError::other)?;
        Ok(confirmation)
    }

    /// Gets a primary or secondary sliver for the identified sliver pair.
    pub async fn get_sliver<A: EncodingAxis>(
        &self,
        blob_id: &BlobId,
        sliver_pair_index: SliverPairIndex,
    ) -> Result<Sliver<A>, NodeError> {
        let url = self.endpoints.sliver::<A>(blob_id, sliver_pair_index);
        let response = self.inner.get(url).send().await.map_err(Kind::Reqwest)?;
        response.response_error_for_status().await?.bcs().await
    }

    /// Gets a primary or secondary sliver for the identified sliver pair.
    pub async fn get_sliver_by_type(
        &self,
        blob_id: &BlobId,
        sliver_pair_index: SliverPairIndex,
        sliver_type: SliverType,
    ) -> Result<SliverEnum, NodeError> {
        match sliver_type {
            SliverType::Primary => self
                .get_sliver::<Primary>(blob_id, sliver_pair_index)
                .await
                .map(SliverEnum::Primary),
            SliverType::Secondary => self
                .get_sliver::<Secondary>(blob_id, sliver_pair_index)
                .await
                .map(SliverEnum::Secondary),
        }
    }

    /// Requests the sliver identified by `metadata.blob_id()` and the pair index from the storage
    /// node, and verifies it against the provided metadata and encoding config.
    ///
    /// # Panics
    ///
    /// Panics if the provided encoding config is not applicable to the metadata, i.e., if
    /// [`VerifiedBlobMetadataWithId::is_encoding_config_applicable`] returns false.
    pub async fn get_and_verify_sliver<A: EncodingAxis>(
        &self,
        sliver_pair_index: SliverPairIndex,
        metadata: &VerifiedBlobMetadataWithId,
        encoding_config: &EncodingConfig,
    ) -> Result<Sliver<A>, NodeError> {
        assert!(
            metadata.is_encoding_config_applicable(encoding_config),
            "encoding config is not applicable to the provided metadata and blob"
        );

        let sliver = self
            .get_sliver(metadata.blob_id(), sliver_pair_index)
            .await?;

        sliver
            .verify(encoding_config, metadata.metadata())
            .map_err(NodeError::other)?;

        Ok(sliver)
    }

    /// Gets the recovery symbol for a primary or secondary sliver.
    ///
    /// The symbol is identified by the (A, sliver_pair_at_remote, intersecting_pair_index) tuple.
    pub async fn get_recovery_symbol<A: EncodingAxis>(
        &self,
        blob_id: &BlobId,
        sliver_pair_at_remote: SliverPairIndex,
        intersecting_pair_index: SliverPairIndex,
    ) -> Result<RecoverySymbol<A, MerkleProof>, NodeError> {
        let url = self.endpoints.recovery_symbol::<A>(
            blob_id,
            sliver_pair_at_remote,
            intersecting_pair_index,
        );
        let response = self.inner.get(url).send().await.map_err(Kind::Reqwest)?;
        response.response_error_for_status().await?.bcs().await
    }

    /// Gets the recovery symbol for a primary or secondary sliver.
    ///
    /// The symbol is identified by the (A, sliver_pair_at_remote, intersecting_pair_index) tuple.
    pub async fn get_and_verify_recovery_symbol<A: EncodingAxis>(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        encoding_config: &EncodingConfig,
        sliver_pair_at_remote: SliverPairIndex,
        intersecting_pair_index: SliverPairIndex,
    ) -> Result<RecoverySymbol<A, MerkleProof>, NodeError> {
        let symbol = self
            .get_recovery_symbol::<A>(
                metadata.blob_id(),
                sliver_pair_at_remote,
                intersecting_pair_index,
            )
            .await?;

        symbol
            .verify(
                metadata.as_ref(),
                encoding_config,
                intersecting_pair_index.to_sliver_index::<A>(encoding_config.n_shards()),
            )
            .map_err(NodeError::other)?;

        Ok(symbol)
    }

    /// Stores the metadata on the node.
    pub async fn store_metadata(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
    ) -> Result<(), NodeError> {
        let url = self.endpoints.metadata(metadata.blob_id());
        let encoded_metadata = bcs::to_bytes(metadata.as_ref())
            .expect("successfully bcs encodes within limit defaults");

        let request = self.inner.put(url).body(encoded_metadata);
        request
            .send()
            .await
            .map_err(Kind::Reqwest)?
            .response_error_for_status()
            .await?;

        Ok(())
    }

    /// Stores a sliver on a node.
    pub async fn store_sliver_by_axis<A: EncodingAxis>(
        &self,
        blob_id: &BlobId,
        pair_index: SliverPairIndex,
        sliver: &Sliver<A>,
    ) -> Result<(), NodeError> {
        let url = self.endpoints.sliver::<A>(blob_id, pair_index);
        let encoded_sliver = bcs::to_bytes(&sliver).expect("internal types to be bcs encodable");

        let request = self.inner.put(url).body(encoded_sliver);
        request
            .send()
            .await
            .map_err(Kind::Reqwest)?
            .response_error_for_status()
            .await?;

        Ok(())
    }

    /// Stores a sliver on a node.
    pub async fn store_sliver(
        &self,
        blob_id: &BlobId,
        pair_index: SliverPairIndex,
        sliver: &SliverEnum,
    ) -> Result<(), NodeError> {
        match sliver {
            SliverEnum::Primary(sliver) => {
                self.store_sliver_by_axis(blob_id, pair_index, sliver).await
            }
            SliverEnum::Secondary(sliver) => {
                self.store_sliver_by_axis(blob_id, pair_index, sliver).await
            }
        }
    }

    /// Stores a sliver pair on the node.
    ///
    /// Returns an error if either storing the primary or secondary sliver fails.
    pub async fn store_sliver_pair(
        &self,
        blob_id: &BlobId,
        pair: &SliverPair,
    ) -> Result<(), NodeError> {
        let (primary, secondary) = tokio::join!(
            self.store_sliver_by_axis(blob_id, pair.index(), &pair.primary),
            self.store_sliver_by_axis(blob_id, pair.index(), &pair.secondary),
        );
        primary.and(secondary)
    }

    /// Sends an inconsistency proof for the specified [`EncodingAxis`] to a node.
    pub async fn send_inconsistency_proof_by_axis<A: EncodingAxis>(
        &self,
        blob_id: &BlobId,
        inconsistency_proof: &InconsistencyProof<A, MerkleProof>,
    ) -> Result<InvalidBlobIdAttestation, NodeError> {
        let url = self.endpoints.inconsistency_proof::<A>(blob_id);
        let encoded_proof =
            bcs::to_bytes(&inconsistency_proof).expect("internal types to be bcs encodable");

        let request = self.inner.put(url).body(encoded_proof);
        let attestation = request
            .send()
            .await
            .map_err(Kind::Reqwest)?
            .response_error_for_status()
            .await?
            .service_response()
            .await?;

        Ok(attestation)
    }

    /// Sends an inconsistency proof to a node and requests the invalid blob id
    /// attestation from the node.
    pub async fn send_inconsistency_proof(
        &self,
        blob_id: &BlobId,
        inconsistency_proof: &InconsistencyProofEnum,
    ) -> Result<InvalidBlobIdAttestation, NodeError> {
        match inconsistency_proof {
            InconsistencyProofEnum::Primary(proof) => {
                self.send_inconsistency_proof_by_axis(blob_id, proof).await
            }
            InconsistencyProofEnum::Secondary(proof) => {
                self.send_inconsistency_proof_by_axis(blob_id, proof).await
            }
        }
    }

    /// Sends an inconsistency proof to a node and verifies the returned invalid blob id
    /// attestation.
    pub async fn get_and_verify_invalid_blob_attestation(
        &self,
        blob_id: &BlobId,
        inconsistency_proof: &InconsistencyProofEnum,
        epoch: Epoch,
        public_key: &PublicKey,
    ) -> Result<InvalidBlobIdAttestation, NodeError> {
        let attestation = self
            .send_inconsistency_proof(blob_id, inconsistency_proof)
            .await?;
        let _ = attestation
            .verify(public_key, epoch, blob_id)
            .map_err(NodeError::other)?;
        Ok(attestation)
    }
}

#[cfg(test)]
mod tests {
    use walrus_core::{encoding::Primary, test_utils};
    use walrus_test_utils::param_test;

    use super::*;

    const BLOB_ID: BlobId = test_utils::blob_id_from_u64(99);

    param_test! {
        url_endpoint: [
            blob: (|e| e.blob_resource(&BLOB_ID), ""),
            metadata: (|e| e.metadata(&BLOB_ID), "metadata"),
            confirmation: (|e| e.confirmation(&BLOB_ID), "confirmation"),
            sliver: (|e| e.sliver::<Primary>(&BLOB_ID, SliverPairIndex(1)), "slivers/1/primary"),
            recovery_symbol: (
                |e| e.recovery_symbol::<Primary>(&BLOB_ID, SliverPairIndex(1), SliverPairIndex(2)),
                "slivers/1/primary/2"
            ),
            inconsistency_proof: (
                |e| e.inconsistency_proof::<Primary>(&BLOB_ID), "inconsistent/primary"
            ),
        ]
    }
    fn url_endpoint<F>(url_fn: F, expected_path: &str)
    where
        F: FnOnce(UrlEndpoints) -> Url,
    {
        let endpoints = UrlEndpoints(Url::parse("http://node.com").unwrap());
        let url = url_fn(endpoints);
        let expected = format!("http://node.com/v1/blobs/{BLOB_ID}/{expected_path}");

        assert_eq!(url.to_string(), expected);
    }
}
