// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Client for interacting with the StorageNode API.

use reqwest::{Client as ReqwestClient, Url};
use walrus_core::{
    encoding::{EncodingAxis, EncodingConfig, Sliver, SliverPair},
    messages::StorageConfirmation,
    metadata::{UnverifiedBlobMetadataWithId, VerifiedBlobMetadataWithId},
    BlobId,
    Epoch,
    PublicKey,
    SignedStorageConfirmation,
    Sliver as SliverEnum,
    SliverPairIndex,
    SliverType,
};

use crate::{
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

    fn sliver<A: EncodingAxis>(
        &self,
        blob_id: &BlobId,
        SliverPairIndex(sliver_pair_index): SliverPairIndex,
    ) -> Url {
        let sliver_type = SliverType::for_encoding::<A>();
        let path = format!("slivers/{sliver_pair_index}/{sliver_type}");
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
            .verify(public_key, blob_id, epoch)
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
        response.bcs().await
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
            .verify(encoding_config, metadata)
            .map_err(NodeError::other)?;

        Ok(sliver)
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
        pair: SliverPair,
    ) -> Result<(), NodeError> {
        let (primary, secondary) = tokio::join!(
            self.store_sliver_by_axis(blob_id, pair.index(), &pair.primary),
            self.store_sliver_by_axis(blob_id, pair.index(), &pair.secondary),
        );
        primary.and(secondary)
    }
}

#[cfg(test)]
mod tests {
    use walrus_core::{encoding::Primary, test_utils};

    use super::*;

    const BLOB_ID: BlobId = test_utils::blob_id_from_u64(99);

    fn endpoints() -> UrlEndpoints {
        UrlEndpoints(Url::parse("http://sn1.com").unwrap())
    }

    #[test]
    fn blob_url() {
        assert_eq!(
            endpoints().blob_resource(&BLOB_ID).to_string(),
            format!("http://sn1.com/v1/blobs/{BLOB_ID}/")
        );
    }

    #[test]
    fn metadata_url() {
        assert_eq!(
            endpoints().metadata(&BLOB_ID).to_string(),
            format!("http://sn1.com/v1/blobs/{BLOB_ID}/metadata")
        );
    }

    #[test]
    fn confirmation_url() {
        assert_eq!(
            endpoints().confirmation(&BLOB_ID).to_string(),
            format!("http://sn1.com/v1/blobs/{BLOB_ID}/confirmation")
        );
    }

    #[test]
    fn sliver_url() {
        assert_eq!(
            endpoints()
                .sliver::<Primary>(&BLOB_ID, SliverPairIndex(1))
                .to_string(),
            format!("http://sn1.com/v1/blobs/{BLOB_ID}/slivers/1/primary")
        );
    }
}
