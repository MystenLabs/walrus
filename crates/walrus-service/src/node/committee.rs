// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Walrus committee service and associated types.

use std::{future::Future, num::NonZeroU16, sync::Arc};

use anyhow::Context;
use async_trait::async_trait;
use walrus_core::{
    encoding::EncodingConfig,
    keys::ProtocolKeyPair,
    merkle::MerkleProof,
    messages::InvalidBlobCertificate,
    metadata::VerifiedBlobMetadataWithId,
    BlobId,
    Epoch,
    InconsistencyProof as InconsistencyProofEnum,
    PublicKey,
    ShardIndex,
    Sliver,
    SliverPairIndex,
    SliverType,
};
use walrus_sdk::error::ClientBuildError;
use walrus_sui::{
    client::ReadClient,
    types::{Committee, StorageNode},
};

use self::node_service::NodeService;
use crate::common::active_committees::ActiveCommittees;

mod committee_service;
mod node_service;
mod request_futures;

pub(crate) use self::{
    committee_service::NodeCommitteeService,
    node_service::default_node_service_factory,
};
use super::errors::SyncShardClientError;

/// Alias to the default type used for recovery symbols.
pub(crate) type DefaultRecoverySymbol = walrus_core::RecoverySymbol<MerkleProof>;

/// Service used to query the current, previous, and next committees.
#[async_trait]
pub(crate) trait CommitteeLookupService: Send + Sync + std::fmt::Debug {
    /// Returns the active committees, which are possibly already transitioning.
    async fn get_active_committees(&self) -> Result<ActiveCommittees, anyhow::Error>;
}

#[async_trait]
impl<T: ReadClient + std::fmt::Debug> CommitteeLookupService for T {
    async fn get_active_committees(&self) -> Result<ActiveCommittees, anyhow::Error> {
        let committee = self
            .current_committee()
            .await
            .context("unable to get the current committee")?;

        if committee.epoch == 0 {
            Ok(ActiveCommittees::new(committee, None))
        } else {
            // TODO(jsmith): Use new contract calls
            tracing::warn!("using invalid previous committee for testing");
            let fake_previous_committee = Committee::new(
                committee.members().to_vec(),
                committee.epoch - 1,
                committee.n_shards(),
            )
            .unwrap();
            Ok(ActiveCommittees::new(
                committee,
                Some(fake_previous_committee),
            ))
        }
    }
}

/// A `CommitteeService` provides information on the current committee, as well as interactions
/// with committee members.
///
/// It is associated with a single storage epoch.
#[async_trait]
pub trait CommitteeService: std::fmt::Debug + Send + Sync {
    /// Returns the epoch associated with the committee.
    fn get_epoch(&self) -> Epoch;

    /// Returns the number of shards in the committee.
    fn get_shard_count(&self) -> NonZeroU16;

    /// Returns the current committee used by the service.
    fn committee(&self) -> Arc<Committee>;

    /// Returns the encoding config associated with the shards in the committee size.
    fn encoding_config(&self) -> &Arc<EncodingConfig>;

    /// Get and verify metadata.
    async fn get_and_verify_metadata(
        &self,
        blob_id: BlobId,
        certified_epoch: Epoch,
    ) -> VerifiedBlobMetadataWithId;

    /// Recovers a sliver from symbols stored by the committee.
    async fn recover_sliver(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        sliver_id: SliverPairIndex,
        sliver_type: SliverType,
        certified_epoch: Epoch,
    ) -> Result<Sliver, InconsistencyProofEnum<MerkleProof>>;

    /// Sends the inconsistency proofs to other nodes and gets a certificate of
    /// the blob's invalidity.
    async fn get_invalid_blob_certificate(
        &self,
        blob_id: BlobId,
        inconsistency_proof: &InconsistencyProofEnum,
    ) -> InvalidBlobCertificate;

    /// Syncs a shard to the given epoch.
    async fn sync_shard_before_epoch(
        &self,
        shard: ShardIndex,
        starting_blob_id: BlobId,
        sliver_type: SliverType,
        sliver_count: u64,
        epoch: Epoch,
        key_pair: &ProtocolKeyPair,
    ) -> Result<Vec<(BlobId, Sliver)>, SyncShardClientError>;

    /// Checks if the given public key belongs to a Walrus storage node.
    /// TODO (#629): once node catching up is implemented, we need to make sure that the node
    /// may not be part of the current committee (node from past committee in the previous epoch
    /// or will be come new committee in the future) can still communicate with each other.
    fn is_walrus_storage_node(&self, public_key: &PublicKey) -> bool;
}

/// Interface for creating new [`NodeService`]s, such as during epoch change.
#[async_trait]
pub(crate) trait NodeServiceFactory: Send {
    type Service: NodeService;

    async fn make_service(
        &mut self,
        info: &StorageNode,
        encoding_config: &Arc<EncodingConfig>,
    ) -> Result<Self::Service, ClientBuildError>;
}

#[async_trait]
impl<F, S, Fut> NodeServiceFactory for F
where
    F: for<'a> FnMut(&'a StorageNode, &'a Arc<EncodingConfig>) -> Fut + Send,
    Fut: Future<Output = Result<S, ClientBuildError>> + Send,
    S: NodeService,
{
    type Service = S;

    async fn make_service(
        &mut self,
        info: &StorageNode,
        encoding_config: &Arc<EncodingConfig>,
    ) -> Result<Self::Service, ClientBuildError> {
        (self)(info, encoding_config).await
    }
}
