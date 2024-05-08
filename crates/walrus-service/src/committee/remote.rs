// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use walrus_core::{
    encoding::{EncodingAxis, EncodingConfig, RecoverySymbol},
    merkle::MerkleProof,
    messages::InvalidBlobIdAttestation,
    metadata::VerifiedBlobMetadataWithId,
    BlobId,
    Epoch,
    InconsistencyProof,
    PublicKey,
    SliverPairIndex,
};
use walrus_sdk::client::Client as StorageNodeClient;

use super::NodeClient;

impl NodeClient for StorageNodeClient {
    async fn get_and_verify_metadata(
        &self,
        blob_id: &BlobId,
        encoding_config: &EncodingConfig,
    ) -> Option<VerifiedBlobMetadataWithId> {
        tracing::debug!("requesting verified metadata from remote storage node");

        self.get_and_verify_metadata(blob_id, encoding_config)
            .await
            .inspect(|_| tracing::debug!("metadata request succeeded"))
            .inspect_err(|err| tracing::debug!(%err, "metadata request failed"))
            .ok()
    }

    async fn get_and_verify_recovery_symbol<A: EncodingAxis>(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        encoding_config: &EncodingConfig,
        sliver_pair_at_remote: SliverPairIndex,
        intersecting_pair_index: SliverPairIndex,
    ) -> Option<RecoverySymbol<A, MerkleProof>> {
        tracing::debug!("requesting a verified symbol from the remote storage node");

        self.get_and_verify_recovery_symbol::<A>(
            metadata,
            encoding_config,
            sliver_pair_at_remote,
            intersecting_pair_index,
        )
        .await
        .inspect(|_| tracing::debug!("symbol request succeeded"))
        .inspect_err(|err| tracing::debug!(%err, "symbol request failed"))
        .ok()
    }

    async fn get_invalid_blob_attestation(
        &self,
        blob_id: &BlobId,
        inconsistency_proof: &InconsistencyProof,
        epoch: Epoch,
        public_key: &PublicKey,
    ) -> Option<InvalidBlobIdAttestation> {
        tracing::debug!("requesting an invalid blob attestation from the remote storage node");

        self.get_and_verify_invalid_blob_attestation(
            blob_id,
            inconsistency_proof,
            epoch,
            public_key,
        )
        .await
        .inspect(|_| tracing::debug!("invalid blob attestation request succeeded"))
        .inspect_err(|err| tracing::debug!(%err, "invalid blob attestation request failed"))
        .ok()
    }
}
