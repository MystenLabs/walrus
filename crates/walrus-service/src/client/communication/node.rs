// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{num::NonZeroU16, sync::Arc};

use anyhow::Result;
use futures::{future::Either, stream::FuturesUnordered, Future, StreamExt};
use rand::rngs::StdRng;
use tokio::sync::Semaphore;
use tracing::{Level, Span};
use walrus_core::{
    encoding::{EncodingAxis, EncodingConfig, SliverData, SliverPair},
    messages::{BlobPersistenceType, SignedStorageConfirmation},
    metadata::VerifiedBlobMetadataWithId,
    BlobId,
    Epoch,
    PublicKey,
    ShardIndex,
    Sliver,
    SliverPairIndex,
};
use walrus_sdk::{
    api::{BlobStatus, StoredOnNodeStatus},
    client::Client as StorageNodeClient,
    error::NodeError,
};
use walrus_sui::types::StorageNode;
use walrus_utils::backoff::{self, ExponentialBackoff};

use crate::{
    client::{
        config::RequestRateConfig,
        error::{SliverStoreError, StoreError},
        utils::{string_prefix, WeightedResult},
    },
    common::utils::FutureHelpers,
};

/// Below this threshold, the `NodeCommunication` client will not check if the sliver is present on
/// the node, but directly try to store it.
///
/// The threshold is chosend in a somewhat arbitrary way, but with the guiding principle that the
/// direct sliver store should only take 1 RTT, therefore having similar latency to the sliver
/// status check. To ensure this is the case, we take compute the threshold as follows: take the TCP
/// payload size (1440 B); multiply it for an initial congestion window of 4 packets (although in
/// modern systems this is usually 10, there may be other data being sent in this window); and
/// conservatively subtract 200 B to account for HTTP headers and other overheads.
const SLIVER_CHECK_THRESHOLD: usize = 5560;

/// Represents the index of the node in the vector of members of the committee.
pub type NodeIndex = usize;

/// Represents the result of an interaction with a storage node.
///
/// Contains the epoch, the "weight" of the interaction (e.g., the number of shards for which an
/// operation was performed), the storage node that issued it, and the result of the operation.
#[derive(Debug, Clone)]
pub struct NodeResult<T, E>(
    #[allow(dead_code)] pub Epoch,
    pub usize,
    pub NodeIndex,
    pub Result<T, E>,
);

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

pub(crate) struct NodeCommunication<'a, W = ()> {
    pub node_index: NodeIndex,
    pub committee_epoch: Epoch,
    pub node: &'a StorageNode,
    pub encoding_config: &'a EncodingConfig,
    pub span: Span,
    pub client: StorageNodeClient,
    pub config: RequestRateConfig,
    pub node_write_limit: W,
    pub sliver_write_limit: W,
}

pub type NodeReadCommunication<'a> = NodeCommunication<'a, ()>;
pub type NodeWriteCommunication<'a> = NodeCommunication<'a, Arc<Semaphore>>;

impl<'a> NodeReadCommunication<'a> {
    /// Creates a new [`NodeCommunication`].
    ///
    /// Returns `None` if the `node` has no shards.
    pub fn new(
        node_index: NodeIndex,
        committee_epoch: Epoch,
        client: StorageNodeClient,
        node: &'a StorageNode,
        encoding_config: &'a EncodingConfig,
        config: RequestRateConfig,
    ) -> Option<Self> {
        if node.shard_ids.is_empty() {
            tracing::debug!("do not create NodeCommunication for node without shards");
            return None;
        }

        tracing::trace!(
            %node_index,
            %config.max_node_connections,
            "initializing communication with node"
        );
        Some(Self {
            node_index,
            committee_epoch,
            node,
            encoding_config,
            span: tracing::span!(
                Level::ERROR,
                "node",
                index = node_index,
                committee_epoch,
                pk_prefix = string_prefix(&node.public_key)
            ),
            client,
            config,
            node_write_limit: (),
            sliver_write_limit: (),
        })
    }

    pub fn with_write_limits(
        self,
        sliver_write_limit: Arc<Semaphore>,
    ) -> NodeWriteCommunication<'a> {
        let node_write_limit = Arc::new(Semaphore::new(self.config.max_node_connections));
        let Self {
            node_index,
            committee_epoch,
            node,
            encoding_config,
            span,
            client,
            config,
            ..
        } = self;
        NodeWriteCommunication {
            node_index,
            committee_epoch,
            node,
            encoding_config,
            span,
            client,
            config,
            node_write_limit,
            sliver_write_limit,
        }
    }
}

impl<W> NodeCommunication<'_, W> {
    /// Returns the number of shards.
    pub fn n_shards(&self) -> NonZeroU16 {
        self.encoding_config.n_shards()
    }

    /// Returns the number of shards owned by the node.
    pub fn n_owned_shards(&self) -> NonZeroU16 {
        NonZeroU16::new(
            self.node
                .shard_ids
                .len()
                .try_into()
                .expect("the number of shards is capped"),
        )
        .expect("each node has >0 shards")
    }

    fn to_node_result<T, E>(&self, weight: usize, result: Result<T, E>) -> NodeResult<T, E> {
        NodeResult(self.committee_epoch, weight, self.node_index, result)
    }

    fn to_node_result_with_n_shards<T, E>(&self, result: Result<T, E>) -> NodeResult<T, E> {
        self.to_node_result(self.n_owned_shards().get().into(), result)
    }

    // Read operations.

    /// Requests the metadata for a blob ID from the node.
    #[tracing::instrument(level = Level::TRACE, parent = &self.span, skip_all)]
    pub async fn retrieve_verified_metadata(
        &self,
        blob_id: &BlobId,
    ) -> NodeResult<VerifiedBlobMetadataWithId, NodeError> {
        tracing::debug!(%blob_id, "retrieving metadata");
        let result = self
            .client
            .get_and_verify_metadata(blob_id, self.encoding_config)
            .await;
        self.to_node_result_with_n_shards(result)
    }

    /// Requests a sliver from the storage node, and verifies that it matches the metadata and
    /// encoding config.
    #[tracing::instrument(level = Level::TRACE, parent = &self.span, skip(self, metadata))]
    pub async fn retrieve_verified_sliver<A: EncodingAxis>(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        shard_index: ShardIndex,
    ) -> NodeResult<SliverData<A>, NodeError>
    where
        SliverData<A>: TryFrom<Sliver>,
    {
        tracing::debug!(
            walrus.shard_index = %shard_index,
            sliver_type = A::NAME,
            "retrieving verified sliver"
        );
        let sliver_pair_index = shard_index.to_pair_index(self.n_shards(), metadata.blob_id());
        let sliver = self
            .client
            .get_and_verify_sliver(sliver_pair_index, metadata, self.encoding_config)
            .await;

        // Each sliver is in this case requested individually, so the weight is 1.
        self.to_node_result(1, sliver)
    }

    /// Requests the status for a blob ID from the node.
    #[tracing::instrument(level = Level::TRACE, parent = &self.span, skip_all)]
    pub async fn get_blob_status(&self, blob_id: &BlobId) -> NodeResult<BlobStatus, NodeError> {
        tracing::debug!(%blob_id, "retrieving blob status");
        self.to_node_result_with_n_shards(self.client.get_blob_status(blob_id).await)
    }

    /// Retries getting the confirmation for the blob ID.
    async fn get_confirmation_with_retries_inner(
        &self,
        blob_id: &BlobId,
        epoch: Epoch,
        blob_persistence_type: &BlobPersistenceType,
    ) -> Result<SignedStorageConfirmation, NodeError> {
        let confirmation = backoff::retry(self.backoff_strategy(), || {
            self.client.get_confirmation(blob_id, blob_persistence_type)
        })
        .await
        .map_err(|error| {
            tracing::warn!(?error, "could not retrieve confirmation after retrying");
            NodeError::other(error)
        })?;

        let _ = confirmation
            .verify(self.public_key(), epoch, *blob_id, *blob_persistence_type)
            .map_err(NodeError::other)?;

        Ok(confirmation)
    }

    #[tracing::instrument(level = Level::TRACE, parent = &self.span, skip_all)]
    pub async fn get_confirmation_with_retries(
        &self,
        blob_id: &BlobId,
        epoch: Epoch,
        blob_persistence_type: &BlobPersistenceType,
    ) -> NodeResult<SignedStorageConfirmation, NodeError> {
        tracing::debug!("retrieving confirmation");
        let result = self
            .get_confirmation_with_retries_inner(blob_id, epoch, blob_persistence_type)
            .await;
        self.to_node_result_with_n_shards(result)
    }

    /// Gets the backoff strategy for the node.
    fn backoff_strategy(&self) -> ExponentialBackoff<StdRng> {
        ExponentialBackoff::new_with_seed(
            self.config.backoff_config.min_backoff,
            self.config.backoff_config.max_backoff,
            self.config.backoff_config.max_retries,
            self.node_index as u64,
        )
    }

    /// Converts the public key of the node.
    fn public_key(&self) -> &PublicKey {
        &self.node.public_key
    }
}

impl NodeWriteCommunication<'_> {
    /// Stores metadata and sliver pairs on a node, and requests a storage confirmation.
    ///
    /// Returns a [`NodeResult`], where the weight is the number of shards for which the storage
    /// confirmation was issued.
    #[tracing::instrument(level = Level::TRACE, parent = &self.span, skip_all)]
    pub async fn store_metadata_and_pairs(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        pairs: impl IntoIterator<Item = &SliverPair>,
        blob_persistence_type: &BlobPersistenceType,
    ) -> NodeResult<SignedStorageConfirmation, StoreError> {
        tracing::info!(
            "debug-store: storing metadata and sliver pairs {}, {}",
            metadata.blob_id(),
            self.node.node_id
        );
        tracing::debug!(blob_id = %metadata.blob_id(), "storing metadata and sliver pairs");
        let result = async {
            let metadata_status = self
                .store_metadata_with_retries(metadata)
                .await
                .map_err(StoreError::Metadata)?;
            tracing::debug!(
                node = %self.node.public_key,
                ?metadata_status,
                blob_id = %metadata.blob_id(),
                "finished storing metadata on node");

            let n_stored_slivers = self
                .store_pairs(metadata.blob_id(), &metadata_status, pairs)
                .await?;
            tracing::debug!(
                node = %self.node.public_key,
                n_stored_slivers,
                blob_id = %metadata.blob_id(),
                "finished storing slivers on node");

            self.get_confirmation_with_retries_inner(
                metadata.blob_id(),
                self.committee_epoch,
                blob_persistence_type,
            )
            .await
            .map_err(StoreError::Confirmation)
        }
        .await;
        tracing::debug!(
            blob_id = %metadata.blob_id(),
            node = %self.node.public_key,
            ?result,
            "storing metadata and sliver pairs finished"
        );
        self.to_node_result_with_n_shards(result)
    }

    /// Stores the metadata on the storage node.
    ///
    /// Before storing the metadata, it checks whether the metadata is already stored.
    /// Returns the [`StoredOnNodeStatus`] of the metadata.
    async fn store_metadata_with_retries(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
    ) -> Result<StoredOnNodeStatus, NodeError> {
        let metadata_status = self
            .retry_with_limits_and_backoff(|| self.client.get_metadata_status(metadata.blob_id()))
            .await?;

        match metadata_status {
            StoredOnNodeStatus::Stored => {
                tracing::debug!("the metadata is already stored on the node");
            }
            StoredOnNodeStatus::Nonexistent => {
                self.retry_with_limits_and_backoff(|| self.client.store_metadata(metadata))
                    .await?;
            }
        }
        Ok(metadata_status)
    }

    /// Stores the sliver pairs on the node.
    ///
    /// Internally retries to store each of the slivers according to the `backoff_strategy`. If
    /// after `max_reties` a sliver cannot be stored, the function returns a [`SliverStoreError`]
    /// and terminates.
    ///
    /// The `metadata_status` is used to decide internally whether to check if the slivers are
    /// stored. If the metadata was not stored on the node, it is highly likely that the slivers
    /// are also not stored (the only unlikely scenario is when the same blob is uploaded by
    /// multiple clients concurrently).
    ///
    /// Returns the number of slivers stored (twice the number of pairs).
    async fn store_pairs(
        &self,
        blob_id: &BlobId,
        metadata_status: &StoredOnNodeStatus,
        pairs: impl IntoIterator<Item = &SliverPair>,
    ) -> Result<usize, SliverStoreError> {
        let mut requests = pairs
            .into_iter()
            .flat_map(|pair| {
                vec![
                    Either::Left(self.check_and_store_sliver(
                        blob_id,
                        metadata_status,
                        &pair.primary,
                        pair.index(),
                    )),
                    Either::Right(self.check_and_store_sliver(
                        blob_id,
                        metadata_status,
                        &pair.secondary,
                        pair.index(),
                    )),
                ]
            })
            .collect::<FuturesUnordered<_>>();

        let n_slivers = requests.len();

        while let Some(result) = requests.next().await {
            if let Err(error) = result {
                tracing::warn!(
                    node_permits=?self.node_write_limit.available_permits(),
                    sliver_permits=?self.sliver_write_limit.available_permits(),
                    ?error,
                    ?self.config.backoff_config.max_retries,
                    "could not store sliver after retrying; stopping storing on the node"
                );
                return Err(error);
            }
            tracing::trace!(
                node_permits=?self.node_write_limit.available_permits(),
                sliver_permits=?self.sliver_write_limit.available_permits(),
                progress = format!("{}/{}", n_slivers - requests.len(), n_slivers),
                "sliver stored"
            );
        }
        Ok(n_slivers)
    }

    /// Stores a sliver on a node, first checking that the sliver is not already stored.
    ///
    /// If the sliver is already stored, the function returns.
    ///
    /// If the metadata was not previously stored on the node, it means that likely the slivers
    /// weren't either. Therefore, in this case, the checks are skipped.
    async fn check_and_store_sliver<A: EncodingAxis>(
        &self,
        blob_id: &BlobId,
        metdadata_status: &StoredOnNodeStatus,
        sliver: &SliverData<A>,
        pair_index: SliverPairIndex,
    ) -> Result<(), SliverStoreError> {
        let print_debug = |message| {
            tracing::debug!(
                ?pair_index,
                sliver_type=?A::sliver_type(),
                sliver_len=sliver.len(),
                message
            );
        };
        if metdadata_status == &StoredOnNodeStatus::Nonexistent {
            print_debug(
                "the metadata has just been stored on the node; storing the sliver directly",
            );
        } else if sliver.len() < SLIVER_CHECK_THRESHOLD {
            print_debug(
                "the sliver is sufficiently small not to require a status check; storing the sliver"
            );
        } else if self.get_sliver_status::<A>(blob_id, pair_index).await?
            == StoredOnNodeStatus::Nonexistent
        {
            print_debug("the sliver is not stored on the node; storing the sliver");
        } else {
            tracing::debug!(
                ?pair_index,
                sliver_type=?A::sliver_type(),
                sliver_len=sliver.len(),
                "the sliver is already stored on the node"
            );
            return Ok(());
        }

        self.store_sliver(blob_id, sliver, pair_index).await
    }

    /// Stores a sliver on a node.
    async fn store_sliver<A: EncodingAxis>(
        &self,
        blob_id: &BlobId,
        sliver: &SliverData<A>,
        pair_index: SliverPairIndex,
    ) -> Result<(), SliverStoreError> {
        self.retry_with_limits_and_backoff(|| self.client.store_sliver(blob_id, pair_index, sliver))
            .await
            .map_err(|error| SliverStoreError {
                pair_index,
                sliver_type: A::sliver_type(),
                error,
            })
    }

    /// Requests the status for sliver after retrying.
    async fn get_sliver_status<A: EncodingAxis>(
        &self,
        blob_id: &BlobId,
        pair_index: SliverPairIndex,
    ) -> Result<StoredOnNodeStatus, SliverStoreError> {
        self.retry_with_limits_and_backoff(|| {
            self.client.get_sliver_status::<A>(blob_id, pair_index)
        })
        .await
        .map_err(|error| SliverStoreError {
            pair_index,
            sliver_type: A::sliver_type(),
            error,
        })
    }

    async fn retry_with_limits_and_backoff<F, Fut, T, E>(&self, f: F) -> Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        backoff::retry(self.backoff_strategy(), f)
            .batch_limit(self.node_write_limit.clone())
            .batch_limit(self.sliver_write_limit.clone())
            .await
    }
}
