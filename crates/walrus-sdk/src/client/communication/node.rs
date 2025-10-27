// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    num::NonZeroU16,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::Result;
use futures::{Future, StreamExt, future::Either, stream::FuturesUnordered};
use rand::rngs::StdRng;
use tokio::sync::Semaphore;
use tracing::{Level, Span};
use walrus_core::{
    BlobId,
    Epoch,
    PublicKey,
    ShardIndex,
    Sliver,
    SliverPairIndex,
    encoding::{EncodingAxis, EncodingConfig, SliverData, SliverPair},
    messages::{BlobPersistenceType, SignedStorageConfirmation},
    metadata::VerifiedBlobMetadataWithId,
};
use walrus_storage_node_client::{
    NodeError,
    StorageNodeClient,
    api::{BlobStatus, StoredOnNodeStatus},
};
use walrus_sui::types::StorageNode;
use walrus_utils::backoff::{self, ExponentialBackoff};

use crate::{
    client::auto_tune::AutoTuneHandle,
    config::RequestRateConfig,
    error::{SliverStoreError, StoreError},
    utils::{WeightedResult, string_prefix},
};

/// Represents the index of the node in the vector of members of the committee.
pub type NodeIndex = usize;

/// Represents the result of an interaction with a storage node.
///
/// Contains the epoch, the "weight" of the interaction (e.g., the number of shards for which an
/// operation was performed), the storage node that issued it, and the result of the operation.

#[derive(Debug, Clone)]
pub struct NodeResult<T, E> {
    #[allow(dead_code)]
    pub committee_epoch: Epoch,
    pub weight: usize,
    pub node: NodeIndex,
    pub result: Result<T, E>,
}

impl<T, E> NodeResult<T, E> {
    pub fn new(
        committee_epoch: Epoch,
        weight: usize,
        node: NodeIndex,
        result: Result<T, E>,
    ) -> Self {
        Self {
            committee_epoch,
            weight,
            node,
            result,
        }
    }
}

impl<T, E> WeightedResult for NodeResult<T, E> {
    type Inner = T;
    type Error = E;
    fn weight(&self) -> usize {
        self.weight
    }
    fn inner_result(&self) -> &Result<Self::Inner, Self::Error> {
        &self.result
    }
    fn take_inner_result(self) -> Result<Self::Inner, Self::Error> {
        self.result
    }
}

#[derive(Debug)]
pub struct NodeCommunication<W = ()> {
    pub node_index: NodeIndex,
    pub committee_epoch: Epoch,
    pub node: StorageNode,
    pub encoding_config: Arc<EncodingConfig>,
    pub span: Span,
    pub client: StorageNodeClient,
    pub config: RequestRateConfig,
    pub sliver_status_check_threshold: usize,
    pub(crate) auto_tune_handle: Option<AutoTuneHandle>,
    pub(crate) throughput_stats: Arc<Mutex<NodeThroughputStats>>,
    pub node_write_limit: W,
    pub sliver_write_limit: W,
}

pub type NodeReadCommunication = NodeCommunication;
pub type NodeWriteCommunication = NodeCommunication<Arc<Semaphore>>;

#[derive(Debug, Default)]
pub(crate) struct NodeThroughputStats {
    total_bytes: u64,
    total_duration: Duration,
    sliver_count: u64,
    last_throughput: f64,
}

impl NodeReadCommunication {
    /// Creates a new [`NodeCommunication`].
    ///
    /// Returns `None` if the `node` has no shards.
    pub fn new(
        node_index: NodeIndex,
        committee_epoch: Epoch,
        client: StorageNodeClient,
        node: StorageNode,
        encoding_config: Arc<EncodingConfig>,
        config: RequestRateConfig,
        sliver_status_check_threshold: usize,
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
        let pk_prefix = string_prefix(&node.public_key);
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
                pk_prefix = pk_prefix
            ),
            client,
            config,
            auto_tune_handle: None,
            sliver_status_check_threshold,
            throughput_stats: Arc::new(Mutex::new(NodeThroughputStats::default())),
            node_write_limit: (),
            sliver_write_limit: (),
        })
    }

    pub(crate) fn with_write_limits(
        self,
        sliver_write_limit: Arc<Semaphore>,
        auto_tune_handle: Option<AutoTuneHandle>,
    ) -> NodeWriteCommunication {
        let node_write_limit = Arc::new(Semaphore::new(self.config.max_node_connections));
        let Self {
            node_index,
            committee_epoch,
            node,
            encoding_config,
            span,
            client,
            config,
            sliver_status_check_threshold,
            throughput_stats,
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
            auto_tune_handle,
            sliver_status_check_threshold,
            throughput_stats,
            node_write_limit,
            sliver_write_limit,
        }
    }
}

impl<W> NodeCommunication<W> {
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
        NodeResult::new(self.committee_epoch, weight, self.node_index, result)
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
            .get_and_verify_metadata(blob_id, self.encoding_config.as_ref())
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
            .get_and_verify_sliver(sliver_pair_index, metadata, self.encoding_config.as_ref())
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

impl NodeWriteCommunication {
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
        let result = async {
            self.store_metadata_and_pairs_without_confirmation(metadata, pairs)
                .await
                .take_inner_result()?;

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
            "retrieved storage confirmation"
        );
        self.to_node_result_with_n_shards(result)
    }

    /// Stores metadata and sliver pairs on a node, but does _not_ request a storage confirmation.
    #[tracing::instrument(level = Level::TRACE, parent = &self.span, skip_all)]
    pub async fn store_metadata_and_pairs_without_confirmation(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        pairs: impl IntoIterator<Item = &SliverPair>,
    ) -> NodeResult<(), StoreError> {
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
            Ok(())
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
        } else if self.should_skip_sliver_status_check(sliver.len()) {
            print_debug(
                "the sliver is sufficiently small not to require a status check; \
                storing the sliver",
            );
        } else {
            match self.get_sliver_status::<A>(blob_id, pair_index).await {
                Ok(StoredOnNodeStatus::Nonexistent) => {
                    print_debug("the sliver is not stored on the node; storing the sliver");
                }
                Ok(_) => {
                    tracing::debug!(
                        ?pair_index,
                        sliver_type=?A::sliver_type(),
                        sliver_len=sliver.len(),
                        "the sliver is already stored on the node"
                    );
                    return Ok(());
                }
                Err(error) => {
                    return Err(error);
                }
            }
        }

        let start = Instant::now();
        match self.store_sliver(blob_id, sliver, pair_index).await {
            Ok(()) => {
                let completed_at = Instant::now();
                self.record_throughput_success::<A>(
                    sliver.len(),
                    completed_at,
                    completed_at.duration_since(start),
                );
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    fn record_throughput_success<A: EncodingAxis>(
        &self,
        bytes: usize,
        completed_at: Instant,
        elapsed: Duration,
    ) {
        let elapsed_secs = elapsed.as_secs_f64().max(f64::EPSILON);
        let instantaneous = bytes as f64 / elapsed_secs;

        let mut stats = self
            .throughput_stats
            .lock()
            .expect("node throughput mutex poisoned");
        stats.total_bytes = stats
            .total_bytes
            .saturating_add(bytes.try_into().unwrap_or(u64::MAX));
        stats.total_duration += elapsed;
        stats.sliver_count = stats.sliver_count.saturating_add(1);
        stats.last_throughput = instantaneous;

        let average = if stats.total_duration.is_zero() {
            instantaneous
        } else {
            stats.total_bytes as f64 / stats.total_duration.as_secs_f64().max(f64::EPSILON)
        };

        tracing::debug!(
            node_index = self.node_index,
            node = %self.node.public_key,
            blob_bytes = bytes,
            elapsed_millis = elapsed.as_millis(),
            instantaneous_bps = instantaneous,
            average_bps = average,
            sliver_count = stats.sliver_count,
            total_bytes = stats.total_bytes,
            "sliver upload completed on node"
        );

        if let Some(handle) = &self.auto_tune_handle {
            handle.record_success(bytes, A::sliver_type(), completed_at);
        }
    }

    fn should_skip_sliver_status_check(&self, sliver_len: usize) -> bool {
        sliver_len < self.sliver_status_check_threshold
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
        batch_limit(
            self.sliver_write_limit.clone(),
            batch_limit(
                self.node_write_limit.clone(),
                backoff::retry(self.backoff_strategy(), f),
            ),
        )
        .await
    }
}

async fn batch_limit<F>(permits: Arc<Semaphore>, f: F) -> F::Output
where
    F: Future + Sized,
{
    let _permit = permits
        .acquire_owned()
        .await
        .expect("semaphore never closed");
    f.await
}
