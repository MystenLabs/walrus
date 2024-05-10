// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Committee lookup and management.

use std::{
    collections::HashMap,
    future::Future,
    num::NonZeroU16,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{ensure, Context as _};
use async_trait::async_trait;
use rand::{rngs::StdRng, Rng, SeedableRng};
use reqwest::Url;
use tracing::Instrument;
use walrus_core::{
    encoding::EncodingConfig,
    metadata::VerifiedBlobMetadataWithId,
    BlobId,
    Epoch,
    PublicKey,
};
use walrus_sdk::client::Client as StorageNodeClient;
use walrus_sui::{client::ReadClient, types::Committee};

use crate::utils::{CommitteeSampler, ExponentialBackoff};

const MIN_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const MAX_RETRY_INTERVAL: Duration = Duration::from_secs(3600);
const MAX_REQUEST_DURATION: Duration = Duration::from_secs(1);

/// Factory used to create services for interacting with the committee on each epoch.
#[async_trait]
pub trait CommitteeServiceFactory: std::fmt::Debug + Send + Sync {
    /// Returns a new `Self::Service` for the current epoch.
    async fn new_for_epoch(&self) -> Result<Box<dyn CommitteeService>, anyhow::Error>;
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

    // TODO(jsmith): Move to the service factory method, so that it's guaranteed to be called
    /// Excludes a member from calls made to the committee.
    ///
    /// An excluded member will not be contacted when making calls against the committee.
    fn exclude_member(&mut self, identity: &PublicKey);

    /// Get and verify metadata.
    async fn get_and_verify_metadata(
        &self,
        blob_id: &BlobId,
        encoding_config: &EncodingConfig,
    ) -> VerifiedBlobMetadataWithId;
}

/// Constructs [`NodeCommitteeService`]s by reading the current storage committee from the chain.
#[derive(Debug, Clone)]
pub struct SuiCommitteeServiceFactory<T> {
    read_client: T,
}

impl<T> SuiCommitteeServiceFactory<T>
where
    T: ReadClient,
{
    /// Creates a new service factory for the provided read client.
    pub fn new(read_client: T) -> Self {
        Self { read_client }
    }
}

#[async_trait]
impl<T> CommitteeServiceFactory for SuiCommitteeServiceFactory<T>
where
    T: ReadClient + std::fmt::Debug + Send + Sync,
{
    async fn new_for_epoch(&self) -> Result<Box<dyn CommitteeService>, anyhow::Error> {
        let committee = self
            .read_client
            .current_committee()
            .await
            .context("unable to create a committee service for the current epoch")?;

        let service = NodeCommitteeService::new(committee).await?;

        Ok(Box::new(service))
    }
}

/// Provides details of the committee, as well as an API to perform requests
/// against committee members.
#[derive(Debug)]
pub struct NodeCommitteeService {
    inner: NodeCommitteeServiceInner<StorageNodeClient>,
}

impl NodeCommitteeService {
    /// Creates a new `NodeCommitteeService`.
    pub async fn new(committee: Committee) -> Result<Self, anyhow::Error> {
        Ok(Self {
            inner: NodeCommitteeServiceInner::new(committee)?,
        })
    }
}

#[derive(Debug)]
struct NodeCommitteeServiceInner<T> {
    committee: Committee,
    node_clients: HashMap<PublicKey, T>,
    rng: Arc<Mutex<StdRng>>,
}

impl NodeCommitteeServiceInner<StorageNodeClient> {
    fn new_with_seed(committee: Committee, seed: u64) -> Result<Self, anyhow::Error> {
        let http_client = reqwest::Client::builder().build()?;

        // The hash of the public key should not change despite its mutability,
        // so it should be safe to use it as the key to the hashmap.
        #[allow(clippy::mutable_key_type)]
        let node_clients: HashMap<_, _> = committee
            .members()
            .iter()
            .filter_map(|member| {
                let url = Url::parse(&member.rest_api_url())
                    .inspect_err(|err| {
                        tracing::warn!(?member, ?err, "unable to parse REST-API URL, skipping node")
                    })
                    .ok()?;

                let node_client = StorageNodeClient::from_url(url, http_client.clone());

                Some((member.public_key.clone(), node_client))
            })
            .collect();

        ensure!(
            !node_clients.is_empty(),
            "service list generated from the committee is empty"
        );

        Ok(Self {
            committee,
            node_clients,
            rng: Arc::new(Mutex::new(StdRng::seed_from_u64(seed))),
        })
    }

    fn new(committee: Committee) -> Result<Self, anyhow::Error> {
        Self::new_with_seed(committee, rand::thread_rng().gen())
    }
}

impl<T: NodeClient> NodeCommitteeServiceInner<T> {
    #[tracing::instrument(skip_all)]
    async fn get_and_verify_metadata(
        &self,
        blob_id: &BlobId,
        encoding_config: &EncodingConfig,
    ) -> VerifiedBlobMetadataWithId {
        let mut rng = StdRng::seed_from_u64(self.rng.lock().unwrap().gen());
        let mut backoff =
            ExponentialBackoff::new_with_seed(MIN_RETRY_INTERVAL, MAX_RETRY_INTERVAL, rng.gen());

        // We store the sampler at this level to allow reusing allocated buffers.
        let mut sampler = CommitteeSampler::new(&self.committee);

        loop {
            for index in sampler.sample_quorum_filtered(
                |member| self.node_clients.contains_key(&member.public_key),
                &mut rng,
            ) {
                let public_key = &self.committee.members()[index].public_key;
                let client = &self.node_clients[public_key];

                let span = tracing::info_span!("node", ?public_key);
                let _guard = span.enter();

                tracing::debug!("requesting metadata from storage node");

                let client_request = tokio::time::timeout(
                    MAX_REQUEST_DURATION,
                    client.get_and_verify_metadata(blob_id, encoding_config),
                );

                match client_request.in_current_span().await {
                    Ok(Ok(metadata)) => {
                        tracing::debug!("metadata retrieved successfully");
                        return metadata;
                    }
                    Ok(Err(err)) => tracing::debug!(?err, "metadata request failed"),
                    Err(_) => tracing::debug!("request to node timed out"),
                }
            }

            tracing::error!("failed to retrieve metadata from an entire quorum, backing off");
            backoff.wait().await;
        }
    }
}

// Private trait used internally for testing the inner service.
#[cfg_attr(test, mockall::automock)]
trait NodeClient {
    fn get_and_verify_metadata(
        &self,
        blob_id: &BlobId,
        encoding_config: &EncodingConfig,
    ) -> impl Future<Output = Result<VerifiedBlobMetadataWithId, anyhow::Error>>;
}

impl NodeClient for StorageNodeClient {
    async fn get_and_verify_metadata(
        &self,
        blob_id: &BlobId,
        encoding_config: &EncodingConfig,
    ) -> Result<VerifiedBlobMetadataWithId, anyhow::Error> {
        self.get_and_verify_metadata(blob_id, encoding_config)
            .await
            .context("request for metadata failed")
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

    fn exclude_member(&mut self, identity: &PublicKey) {
        self.inner.node_clients.remove(identity);
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
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use anyhow::bail;
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
                .map(|member| (member.public_key.clone(), MockNodeClient::new()))
                .collect();

            let mut iterator = node_clients.iter_mut();
            (&mut iterator).take(n_not_storing).for_each(|(_, client)| {
                client.expect_get_and_verify_metadata().returning(|_, _| {
                    Box::pin(async { bail!("node does not store the metadata") })
                });
            });
            (&mut iterator)
                .take(n_byzantine_and_storing)
                .for_each(|(_, client)| {
                    client
                        .expect_get_and_verify_metadata()
                        .returning(|_, _| Box::pin(async { bail!("node is byzantine") }));
                });
            (&mut iterator).for_each(|(_, client)| {
                let _ = client.expect_get_and_verify_metadata().returning(|_, _| {
                    Box::pin(async { Ok(walrus_core::test_utils::verified_blob_metadata()) })
                });
            });

            node_clients.shuffle(&mut SmallRng::seed_from_u64(41));

            let inner = NodeCommitteeServiceInner {
                committee,
                node_clients: node_clients.into_iter().collect(),
                rng: Arc::new(Mutex::new(StdRng::seed_from_u64(32))),
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
                .map(|member| (member.public_key.clone(), MockNodeClient::new()))
                .collect();

            let mut iterator = node_clients.iter_mut();
            (&mut iterator)
                .take(n_honest_and_storing)
                .for_each(|(_, client)| {
                    let _ = client.expect_get_and_verify_metadata().returning(|_, _| {
                        Box::pin(async {
                            tracing::debug!("successfully returning blob metadata");
                            Ok(walrus_core::test_utils::verified_blob_metadata())
                        })
                    });
                });
            (&mut iterator).for_each(|(_, client)| {
                client.expect_get_and_verify_metadata().returning(|_, _| {
                    Box::pin(async {
                        tracing::debug!("sleeping for 5 minutes");
                        tokio::time::sleep(Duration::from_secs(3600)).await;
                        tracing::debug!("bailing without returning metadata");
                        bail!("node is byzantine or does not store")
                    })
                });
            });
            node_clients.shuffle(&mut SmallRng::seed_from_u64(44));

            let inner = NodeCommitteeServiceInner {
                committee,
                node_clients: node_clients.into_iter().collect(),
                rng: Arc::new(Mutex::new(StdRng::seed_from_u64(33))),
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
