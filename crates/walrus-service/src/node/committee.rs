// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Committee lookup and management.

use std::{
    future::Future,
    iter,
    num::NonZeroU16,
    sync::{Arc, Mutex},
};

use anyhow::{ensure, Context as _};
use async_trait::async_trait;
use fastcrypto::bls12381::min_pk::BLS12381PublicKey;
use futures::{stream::FuturesUnordered, FutureExt, StreamExt as _};
use rand::{rngs::StdRng, Rng, SeedableRng};
use reqwest::Url;
use tokio::sync::Semaphore;
use tracing::{Instrument, Span};
use walrus_core::{
    bft,
    encoding::{
        self,
        EncodingAxis,
        EncodingConfig,
        Primary,
        PrimarySliver,
        RecoverySymbol,
        Secondary,
        SecondarySliver,
        Sliver,
        SliverRecoveryOrVerificationError,
        SliverVerificationError,
    },
    inconsistency::{InconsistencyProof, SliverOrInconsistencyProof},
    merkle::MerkleProof,
    messages::{CertificateError, InvalidBlobCertificate, InvalidBlobIdAttestation},
    metadata::VerifiedBlobMetadataWithId,
    BlobId,
    Epoch,
    InconsistencyProof as InconsistencyProofEnum,
    PublicKey,
    ShardIndex,
    SliverPairIndex,
};
use walrus_sdk::client::Client as StorageNodeClient;
use walrus_sui::{
    client::ReadClient,
    types::{Committee, StorageNode as SuiStorageNode},
};

use crate::{
    common::utils::{self, ExponentialBackoff, FutureHelpers},
    node::config::CommitteeServiceConfig,
};

mod remote;

fn default_retry_strategy(
    seed: u64,
    config: &CommitteeServiceConfig,
) -> ExponentialBackoff<StdRng> {
    ExponentialBackoff::new_with_seed(
        config.retry_interval_min,
        config.retry_interval_max,
        None,
        seed,
    )
}

/// Factory used to create services for interacting with the committee on each epoch.
#[async_trait]
pub trait CommitteeServiceFactory: std::fmt::Debug + Send + Sync {
    /// Returns a new `Self::Service` for the current epoch.
    ///
    /// The public key `local_identity` identifies a node to be treated as the originator of
    /// requests.  As such, requests on the `CommitteeService` may avoid sending to that node.
    async fn new_for_epoch(
        &self,
        local_identity: Option<&PublicKey>,
    ) -> Result<Box<dyn CommitteeService>, anyhow::Error>;
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

    /// Returns the committee used by the service.
    fn committee(&self) -> &Committee;

    /// Get and verify metadata.
    async fn get_and_verify_metadata(
        &self,
        blob_id: &BlobId,
        encoding_config: &EncodingConfig,
    ) -> VerifiedBlobMetadataWithId;

    /// Recovers a primary sliver from symbols stored by the committee.
    async fn recover_primary_sliver(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        sliver_id: SliverPairIndex,
        encoding_config: &EncodingConfig,
    ) -> Result<PrimarySliver, InconsistencyProof<Primary, MerkleProof>>;

    /// Recovers a secondary sliver from symbols stored by the committee.
    async fn recover_secondary_sliver(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        sliver_id: SliverPairIndex,
        encoding_config: &EncodingConfig,
    ) -> Result<SecondarySliver, InconsistencyProof<Secondary, MerkleProof>>;

    /// Sends the inconsistency proofs to other nodes and gets a certificate of
    /// the blob's invalidity.
    async fn get_invalid_blob_certificate(
        &self,
        blob_id: &BlobId,
        inconsistency_proof: &InconsistencyProofEnum,
        n_shards: NonZeroU16,
    ) -> InvalidBlobCertificate;

    /// Checks if the given public key belongs to a Walrus storage node.
    /// TODO (#629): once node catching up is implemented, we need to make sure that the node
    /// may not be part of the current committee (node from past committee in the previous epoch
    /// or will be come new committee in the future) can still communicate with each other.
    fn is_walrus_storage_node(&self, public_key: &PublicKey) -> bool;
}

#[cfg_attr(test, mockall::automock)]
trait NodeClient {
    fn get_and_verify_metadata(
        &self,
        blob_id: &BlobId,
        encoding_config: &EncodingConfig,
    ) -> impl Future<Output = Option<VerifiedBlobMetadataWithId>>;

    async fn get_and_verify_recovery_symbol<A: EncodingAxis + 'static>(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        encoding_config: &EncodingConfig,
        sliver_pair_at_remote: SliverPairIndex,
        intersecting_pair_index: SliverPairIndex,
    ) -> Option<RecoverySymbol<A, MerkleProof>>;

    async fn get_invalid_blob_attestation(
        &self,
        blob_id: &BlobId,
        inconsistency_proof: &InconsistencyProofEnum,
        epoch: Epoch,
        public_key: &PublicKey,
    ) -> Option<InvalidBlobIdAttestation>;
}

/// Constructs [`NodeCommitteeService`]s by reading the current storage committee from the chain.
#[derive(Debug, Clone)]
pub struct SuiCommitteeServiceFactory<T> {
    read_client: T,
    config: CommitteeServiceConfig,
}

impl<T> SuiCommitteeServiceFactory<T>
where
    T: ReadClient,
{
    /// Creates a new service factory for the provided read client.
    pub fn new(read_client: T, config: CommitteeServiceConfig) -> Self {
        Self {
            read_client,
            config,
        }
    }
}

#[async_trait]
impl<T> CommitteeServiceFactory for SuiCommitteeServiceFactory<T>
where
    T: ReadClient + std::fmt::Debug + Send + Sync,
{
    async fn new_for_epoch(
        &self,
        local_identity: Option<&PublicKey>,
    ) -> Result<Box<dyn CommitteeService>, anyhow::Error> {
        let committee = self
            .read_client
            .current_committee()
            .await
            .context("unable to create a committee service for the current epoch")?;

        let service = NodeCommitteeService::new(committee, local_identity, self.config.clone())?;

        Ok(Box::new(service))
    }
}

/// Provides details of the committee, as well as an API to perform requests against its members.
#[derive(Debug)]
pub struct NodeCommitteeService {
    inner: NodeCommitteeServiceInner<StorageNodeClient>,
}

impl NodeCommitteeService {
    /// Creates a new `NodeCommitteeService`.
    pub fn new(
        committee: Committee,
        local_identity: Option<&PublicKey>,
        config: CommitteeServiceConfig,
    ) -> Result<Self, anyhow::Error> {
        Ok(Self {
            inner: NodeCommitteeServiceInner::new(committee, local_identity, config)?,
        })
    }
}

#[derive(Debug)]
struct NodeCommitteeServiceInner<T> {
    committee: Committee,
    /// Clients corresponding to the respective committee members.
    node_clients: Vec<Option<T>>,
    local_identity: Option<PublicKey>,
    config: CommitteeServiceConfig,
    rng: Arc<Mutex<StdRng>>,
}

impl NodeCommitteeServiceInner<StorageNodeClient> {
    fn new_with_seed(
        committee: Committee,
        local_identity: Option<&PublicKey>,
        config: CommitteeServiceConfig,
        seed: u64,
    ) -> Result<Self, anyhow::Error> {
        let client_builder = reqwest::Client::builder();

        // reqwest proxy uses lazy initialization, which breaks determinism. Turn it off in simtest.
        #[cfg(msim)]
        let client_builder = client_builder.no_proxy();

        let http_client = client_builder.build()?;

        let node_clients: Vec<_> = committee
            .members()
            .iter()
            .map(|member| {
                let url = Url::parse(&member.rest_api_url())
                    .inspect_err(|error| {
                        tracing::warn!(
                            ?member,
                            %error,
                            "unable to parse REST-API URL, skipping node"
                        )
                    })
                    .ok()?;

                Some(StorageNodeClient::from_url(url, http_client.clone()))
            })
            .collect();

        ensure!(
            !node_clients.is_empty(),
            "service list generated from the committee is empty"
        );

        Ok(Self {
            committee,
            node_clients,
            local_identity: local_identity.cloned(),
            config,
            rng: Arc::new(Mutex::new(StdRng::seed_from_u64(seed))),
        })
    }

    fn new(
        committee: Committee,
        local_identity: Option<&PublicKey>,
        config: CommitteeServiceConfig,
    ) -> Result<Self, anyhow::Error> {
        Self::new_with_seed(committee, local_identity, config, rand::thread_rng().gen())
    }
}

impl<T> NodeCommitteeServiceInner<T>
where
    T: NodeClient + std::fmt::Debug,
{
    fn node(&self, index: usize) -> NodeRef<'_, T> {
        NodeRef { index, inner: self }
    }

    fn shuffle_nodes_with_clients<R: Rng>(
        &self,
        rng: &mut R,
    ) -> impl Iterator<Item = NodeRef<'_, T>> {
        let n_nodes = self.committee.n_members();
        rand::seq::index::sample(rng, n_nodes, n_nodes)
            .into_iter()
            .filter_map(|index| {
                let node = self.node(index);
                node.client().is_some().then_some(node)
            })
    }

    #[tracing::instrument(name = "get_and_verify_metadata committee", skip_all)]
    async fn get_and_verify_metadata(
        &self,
        blob_id: &BlobId,
        config: &EncodingConfig,
    ) -> VerifiedBlobMetadataWithId {
        let simultaneous_requests =
            Arc::new(Semaphore::new(self.config.max_concurrent_metadata_requests));

        let mut rng = StdRng::seed_from_u64(self.rng.lock().unwrap().gen());

        let mut metadata_requests = self
            .shuffle_nodes_with_clients(&mut rng)
            .filter(|node| !node.is_local())
            .map(|node| {
                let public_key = node.public_key();
                let retry_strategy =
                    default_retry_strategy(self.rng.lock().unwrap().gen(), &self.config);
                let simultaneous_requests = &simultaneous_requests;

                utils::retry(retry_strategy, move || {
                    let mut maybe_span: Option<Span> = None;
                    async move {
                        // We only create the span if we've been granted a permit to run.
                        let span = maybe_span.get_or_insert_with(|| {
                            tracing::info_span!(
                                "get_and_verify_metadata node", walrus.node.public_key = %public_key
                            )
                        });

                        node.client()
                            .expect("only nodes with clients provided")
                            .get_and_verify_metadata(blob_id, config)
                            .timeout_after(self.config.metadata_request_timeout)
                            .instrument(span.clone())
                            .await
                    }
                    .limit(simultaneous_requests.clone())
                })
            })
            .collect::<FuturesUnordered<_>>();

        metadata_requests
            .next()
            .await
            .expect("there is at least 1 node from which to get the metadata")
            .expect("the backoff strategy ensures we wait until there is one success")
    }

    #[tracing::instrument(name = "recover_sliver committee", skip_all)]
    async fn recover_sliver<A: EncodingAxis + 'static>(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        sliver_id: SliverPairIndex,
        config: &EncodingConfig,
    ) -> Result<Sliver<A>, InconsistencyProof<A, MerkleProof>> {
        assert!(sliver_id.get() < config.n_shards().get());

        let blob_id = metadata.blob_id();
        let mut rng = StdRng::seed_from_u64(self.rng.lock().unwrap().gen());

        let min_symbols_required =
            usize::from(encoding::min_symbols_for_recovery::<A>(config.n_shards()));
        tracing::debug!(
            n_symbols = min_symbols_required,
            is_primary = A::IS_PRIMARY,
            "attempting to get symbols for recovery"
        );
        let simultaneous_requests = Arc::new(Semaphore::new(min_symbols_required));

        let mut symbol_requests = self
            .shuffle_nodes_with_clients(&mut rng)
            .flat_map(|node| iter::repeat(node).zip(node.shard_ids()))
            .map(|(node, shard_id)| {
                let public_key = node.public_key();

                // Convert to a sliver pair id. It may be the case that this pair ID is the same as
                // sliver_id, however, this is fine since we're requesting a symbol from the
                // orthogonal sliver which may be already stored at this node.
                let sliver_pair_at_remote = shard_id.to_pair_index(config.n_shards(), blob_id);
                let simultaneous_requests = &simultaneous_requests;
                let retry_strategy =
                    default_retry_strategy(self.rng.lock().unwrap().gen(), &self.config);

                utils::retry(retry_strategy, move || {
                    let mut maybe_span: Option<Span> = None;
                    async move {
                        // We only create the span if we've been granted a permit to run.
                        let span = maybe_span.get_or_insert_with(|| {
                            tracing::info_span!(
                                "recover_sliver node", walrus.node.public_key = %public_key
                            )
                        });
                        node.client()
                            .expect("only nodes with clients provided")
                            .get_and_verify_recovery_symbol(
                                metadata,
                                config,
                                sliver_pair_at_remote,
                                sliver_id,
                            )
                            .timeout_after(self.config.sliver_request_timeout)
                            .instrument(span.clone())
                            .await
                    }
                    .limit(simultaneous_requests.clone())
                })
                .map(|result| {
                    result.expect("the strategy ensures we wait until a result is available")
                })
            })
            .collect::<FuturesUnordered<_>>();

        let mut recovery_symbols: Vec<_> = (&mut symbol_requests)
            .take(min_symbols_required)
            .collect()
            .await;

        loop {
            tracing::debug!(
                n_symbols = recovery_symbols.len(),
                "attempting to recover sliver using collected symbols"
            );

            let result = tracing::info_span!("recover_sliver_or_generate_inconsistency_proof")
                .in_scope(|| {
                    Sliver::<A>::recover_sliver_or_generate_inconsistency_proof(
                        recovery_symbols.clone(),
                        sliver_id.to_sliver_index::<A>(config.n_shards()),
                        metadata.as_ref(),
                        config,
                    )
                });

            match result {
                Ok(SliverOrInconsistencyProof::Sliver(sliver)) => {
                    tracing::debug!("successfully recovered sliver");
                    return Ok(sliver);
                }
                Ok(SliverOrInconsistencyProof::InconsistencyProof(proof)) => return Err(proof),
                Err(SliverRecoveryOrVerificationError::RecoveryError(err)) => match err {
                    encoding::SliverRecoveryError::BlobSizeTooLarge(_) => {
                        panic!("blob size from verified metadata should not be too large")
                    }
                    encoding::SliverRecoveryError::DecodingFailure => {
                        tracing::debug!(
                            n_symbols = min_symbols_required,
                            "unable to decode with collected symbols, increasing"
                        );

                        simultaneous_requests.add_permits(1);
                        let next_symbol = symbol_requests.next().await.expect(
                            "there are more symbols pending since decoding did not complete",
                        );
                        recovery_symbols.push(next_symbol);
                    }
                },
                Err(SliverRecoveryOrVerificationError::VerificationError(err)) => match err {
                    SliverVerificationError::IndexTooLarge => {
                        panic!("checked above by pre-condition")
                    }
                    SliverVerificationError::SliverSizeMismatch
                    | SliverVerificationError::SymbolSizeMismatch => panic!(
                        "should not occur since symbols were verified and sliver constructed here"
                    ),
                    SliverVerificationError::MerkleRootMismatch => {
                        panic!("should have been converted to an inconsistency proof")
                    }
                    SliverVerificationError::RecoveryFailed(_) => todo!("what generates this?"),
                },
            }
        }
    }

    #[tracing::instrument(name = "get_invalid_blob_certificate committee", skip_all)]
    async fn get_invalid_blob_certificate(
        &self,
        blob_id: &BlobId,
        inconsistency_proof: &InconsistencyProofEnum,
        n_shards: NonZeroU16,
    ) -> InvalidBlobCertificate {
        let mut rng = StdRng::seed_from_u64(self.rng.lock().unwrap().gen());

        let mut signature_requests = self
            .shuffle_nodes_with_clients(&mut rng)
            .map(|node| {
                let public_key = node.public_key();
                let retry_strategy =
                    default_retry_strategy(self.rng.lock().unwrap().gen(), &self.config);
                let span = tracing::info_span!(
                    "get_invalid_blob_certificate node", walrus.node.public_key = %public_key
                );
                utils::retry(retry_strategy, move || async move {
                    node.client()
                        .expect("only nodes with clients provided")
                        .get_invalid_blob_attestation(
                            blob_id,
                            inconsistency_proof,
                            self.committee.epoch,
                            node.public_key(),
                        )
                        .timeout_after(self.config.invalidity_sync_timeout)
                        .await
                        .map(|sig| (sig, node))
                })
                .map(|result| {
                    result.expect("the strategy ensures we wait until a result is available")
                })
                .instrument(span)
            })
            .collect::<FuturesUnordered<_>>();

        // get quorum and assemble certificate
        let weight_threshold = bft::min_n_correct(n_shards).get();
        let mut total_weight = 0;
        let mut signed_messages = Vec::with_capacity(self.committee.n_members());
        let mut signer_indices = Vec::with_capacity(self.committee.n_members());
        while let Some((signed_message, node)) = signature_requests.next().await {
            signed_messages.push(signed_message);
            signer_indices.push(node.index_in_committee());
            total_weight += node.n_shards();
            if total_weight >= weight_threshold {
                break;
            }
        }

        match InvalidBlobCertificate::from_signed_messages_and_indices(
            signed_messages,
            signer_indices,
        ) {
            Ok(certificate) => certificate,
            Err(CertificateError::SignatureAggregation(err)) => {
                panic!(
                    concat!(
                        "should not occur since all inputs to the signature ",
                        "aggregation are verified: {:?}",
                    ),
                    err
                )
            }
            Err(CertificateError::MessageMismatch) => {
                panic!(concat!(
                    "should not occur since all messages are verified ",
                    "against the same epoch and blob id",
                ))
            }
        }
    }
}

/// Helper to access a storage node in the committee and its client.
#[derive(Debug)]
struct NodeRef<'a, T> {
    index: usize,
    inner: &'a NodeCommitteeServiceInner<T>,
}

impl<'a, T> NodeRef<'a, T>
where
    T: NodeClient + std::fmt::Debug,
{
    fn client(&self) -> Option<&'a T> {
        self.inner.node_clients[self.index].as_ref()
    }

    fn as_member(&self) -> &'a SuiStorageNode {
        &self.inner.committee.members()[self.index]
    }

    fn public_key(&self) -> &'a BLS12381PublicKey {
        &self.as_member().public_key
    }

    fn shard_ids(&self) -> &'a [ShardIndex] {
        &self.as_member().shard_ids
    }

    fn n_shards(&self) -> u16 {
        u16::try_from(self.as_member().shard_ids.len()).expect("number of shards to fit in u16")
    }

    fn index_in_committee(&self) -> u16 {
        u16::try_from(self.index).expect("node index to fit in u16")
    }

    /// Returns true if this node corresponds to the local storage node.
    fn is_local(&self) -> bool {
        self.inner
            .local_identity
            .as_ref()
            .map(|local_identity| self.public_key() == local_identity)
            .unwrap_or(false)
    }
}

// Manually implement copy and clone to simply copy the shared references, without any constraint
// on T needing to be copy.
impl<'a, T> Copy for NodeRef<'a, T> {}
impl<'a, T> Clone for NodeRef<'a, T> {
    fn clone(&self) -> Self {
        *self
    }
}

#[async_trait]
impl CommitteeService for NodeCommitteeService {
    fn get_epoch(&self) -> Epoch {
        self.inner.committee.epoch
    }

    fn get_shard_count(&self) -> NonZeroU16 {
        self.inner.committee.n_shards()
    }

    fn committee(&self) -> &Committee {
        &self.inner.committee
    }

    fn is_walrus_storage_node(&self, public_key: &PublicKey) -> bool {
        self.inner
            .committee
            .members()
            .iter()
            .any(|member| member.public_key == *public_key)
    }

    async fn get_and_verify_metadata(
        &self,
        blob_id: &BlobId,
        encoding_config: &EncodingConfig,
    ) -> VerifiedBlobMetadataWithId {
        self.inner
            .get_and_verify_metadata(blob_id, encoding_config)
            .await
    }

    async fn recover_primary_sliver(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        sliver_id: SliverPairIndex,
        encoding_config: &EncodingConfig,
    ) -> Result<PrimarySliver, InconsistencyProof<Primary, MerkleProof>> {
        self.inner
            .recover_sliver::<Primary>(metadata, sliver_id, encoding_config)
            .await
    }

    async fn recover_secondary_sliver(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        sliver_id: SliverPairIndex,
        encoding_config: &EncodingConfig,
    ) -> Result<SecondarySliver, InconsistencyProof<Secondary, MerkleProof>> {
        self.inner
            .recover_sliver::<Secondary>(metadata, sliver_id, encoding_config)
            .await
    }

    async fn get_invalid_blob_certificate(
        &self,
        blob_id: &BlobId,
        inconsistency_proof: &InconsistencyProofEnum,
        n_shards: NonZeroU16,
    ) -> InvalidBlobCertificate {
        // TODO(kwuest): Handle epoch change (#405)
        // During epoch change, we may receive messages from some honest nodes for an unexpected
        // epoch which will cause the verification of the response to fail resulting in an
        // infinite loop in the call to `inner.get_invalid_blob_certificate`.
        self.inner
            .get_invalid_blob_certificate(blob_id, inconsistency_proof, n_shards)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rand::{rngs::SmallRng, seq::SliceRandom, SeedableRng};
    use walrus_test_utils::{async_param_test, Result as TestResult};

    use super::*;
    use crate::test_utils;

    mod get_and_verify_metadata {
        use super::*;

        async_param_test! {
            succeeds -> TestResult: [
                all_honest: (10, 0, 0),
                byzantine_node_store_but_do_not_reply: (4, 3, 3),
                byzantine_nodes_do_not_store: (7, 0, 3),
            ]
        }
        async fn succeeds(
            n_honest_and_storing: usize,
            n_byzantine_and_storing: usize,
            n_not_storing: usize,
        ) -> TestResult {
            let n_nodes = n_not_storing + n_byzantine_and_storing + n_honest_and_storing;
            let committee = test_utils::test_committee(&vec![1; n_nodes]);

            let mut node_clients: Vec<_> = committee
                .members()
                .iter()
                .map(|_| MockNodeClient::new())
                .collect();

            let (failing, succeeding) =
                node_clients.split_at_mut(n_not_storing + n_byzantine_and_storing);

            failing.iter_mut().for_each(|client| {
                client
                    .expect_get_and_verify_metadata()
                    .returning(|_, _| Box::pin(async { None }));
            });
            succeeding.iter_mut().for_each(|client| {
                let _ = client.expect_get_and_verify_metadata().returning(|_, _| {
                    Box::pin(async { Some(walrus_core::test_utils::verified_blob_metadata()) })
                });
            });

            node_clients.shuffle(&mut SmallRng::seed_from_u64(41));

            let inner = NodeCommitteeServiceInner {
                committee,
                node_clients: node_clients.into_iter().map(Option::Some).collect(),
                rng: Arc::new(Mutex::new(StdRng::seed_from_u64(32))),
                local_identity: None,
                config: Default::default(),
            };

            let _ = tokio::time::timeout(
                Duration::from_millis(10),
                inner.get_and_verify_metadata(
                    &walrus_core::test_utils::blob_id_from_u64(99),
                    &walrus_core::test_utils::encoding_config(),
                ),
            )
            .await
            .expect("should successfully return metadata");

            Ok(())
        }

        #[tokio::test(start_paused = true)]
        async fn does_not_block_indefinitely_on_requests() -> TestResult {
            let n_honest_and_storing = 4;
            let n_nodes = 10;
            let committee = test_utils::test_committee(&vec![1; n_nodes]);

            let mut node_clients: Vec<_> = committee
                .members()
                .iter()
                .map(|_| MockNodeClient::new())
                .collect();

            let (honest, other) = node_clients.split_at_mut(n_honest_and_storing);

            honest.iter_mut().for_each(|client| {
                let _ = client.expect_get_and_verify_metadata().returning(|_, _| {
                    Box::pin(async {
                        tracing::debug!("successfully returning blob metadata");
                        Some(walrus_core::test_utils::verified_blob_metadata())
                    })
                });
            });
            other.iter_mut().for_each(|client| {
                client.expect_get_and_verify_metadata().returning(|_, _| {
                    Box::pin(async {
                        tracing::debug!("sleeping for 1 hour");
                        tokio::time::sleep(Duration::from_secs(3600)).await;
                        tracing::debug!("bailing without returning metadata");
                        None
                    })
                });
            });

            node_clients.shuffle(&mut SmallRng::seed_from_u64(44));

            let inner = NodeCommitteeServiceInner {
                committee,
                node_clients: node_clients.into_iter().map(Option::Some).collect(),
                rng: Arc::new(Mutex::new(StdRng::seed_from_u64(33))),
                local_identity: None,
                config: Default::default(),
            };

            let _ = tokio::time::timeout(
                Duration::from_secs(5),
                inner.get_and_verify_metadata(
                    &walrus_core::test_utils::blob_id_from_u64(99),
                    &walrus_core::test_utils::encoding_config(),
                ),
            )
            .await
            .expect("should successfully return metadata");

            Ok(())
        }
    }
}
