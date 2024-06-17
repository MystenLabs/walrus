// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{Arc, Mutex},
};

use futures::{
    future::{try_join_all, Either},
    stream::FuturesUnordered,
    StreamExt,
};
use mysten_metrics::{GaugeGuard, GaugeGuardFutureExt};
use sui_types::event::EventID;
use tokio::{select, sync::Semaphore, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{instrument, Instrument};
use typed_store::TypedStoreError;
use walrus_core::{
    encoding::{EncodingAxis, EncodingConfig, Primary, Secondary},
    metadata::VerifiedBlobMetadataWithId,
    BlobId,
    InconsistencyProof,
    ShardIndex,
    Sliver,
    SliverPairIndex,
};
use walrus_sui::types::BlobCertified;

use super::{
    metrics::{self, NodeMetricSet, STATUS_IN_PROGRESS, STATUS_QUEUED},
    StorageNodeInner,
};
use crate::{
    committee::CommitteeService,
    contract_service::SystemContractService,
    storage::Storage,
    utils::FutureHelpers as _,
};

#[derive(Debug, Clone)]
pub(crate) struct BlobSyncHandler {
    // INV: For each blob id at most one sync is in progress at a time.
    blob_syncs_in_progress: Arc<Mutex<HashMap<BlobId, InProgressSyncHandle>>>,
    node: Arc<StorageNodeInner>,
    semaphore: Arc<Semaphore>,
}

impl BlobSyncHandler {
    pub fn new(node: Arc<StorageNodeInner>, max_concurrent_blob_syncs: usize) -> Self {
        Self {
            blob_syncs_in_progress: Arc::default(),
            node,
            semaphore: Arc::new(Semaphore::new(max_concurrent_blob_syncs)),
        }
    }

    pub async fn cancel_sync(&self, blob_id: &BlobId) -> anyhow::Result<Option<(usize, EventID)>> {
        let Some(handle) = self
            .blob_syncs_in_progress
            .lock()
            .expect("should be able to acquire lock")
            .get_mut(blob_id)
            .and_then(|sync| sync.cancel())
        else {
            return Ok(None);
        };
        Ok(handle.await??)
    }

    async fn remove_sync_handle(&self, blob_id: &BlobId) -> Option<InProgressSyncHandle> {
        self.blob_syncs_in_progress
            .lock()
            .expect("should be able to acquire lock")
            .remove(blob_id)
    }

    pub async fn start_sync(
        &self,
        event: BlobCertified,
        event_sequence_number: usize,
        start: tokio::time::Instant,
    ) -> Result<(), TypedStoreError> {
        let blob_id = event.blob_id;
        let mut blob_syncs_guard = self
            .blob_syncs_in_progress
            .lock()
            .expect("should be able to acquire lock");
        if let Entry::Vacant(entry) = blob_syncs_guard.entry(blob_id) {
            let cancel_token = CancellationToken::new();
            let synchronizer = BlobSynchronizer::new(
                event,
                event_sequence_number,
                self.node.clone(),
                cancel_token.clone(),
            );
            let sync_handle = tokio::spawn(
                self.clone()
                    .sync(synchronizer, start, self.semaphore.clone())
                    .in_current_span(),
            );
            entry.insert(InProgressSyncHandle {
                cancel_token,
                blob_sync_handle: Some(sync_handle),
            });
        } else {
            // A blob sync with a lower sequence number is already in progress. We can safely try to
            // increase the event cursor since it will only be advanced once that sync is finished
            // or cancelled due to an invalid blob event.
            self.node
                .mark_event_completed(event_sequence_number, &event.event_id)?;
        }
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(blob_id = %blob_synchronizer.blob_id), err)]
    pub async fn sync(
        self,
        blob_synchronizer: BlobSynchronizer,
        start: tokio::time::Instant,
        semaphore: Arc<Semaphore>,
    ) -> Result<Option<(usize, EventID)>, TypedStoreError> {
        let node = &blob_synchronizer.node;
        let queued_gauge = metrics::with_label!(node.metrics.recover_blob_backlog, STATUS_QUEUED);

        let _permit = semaphore
            .acquire_owned()
            .count_in_flight(&queued_gauge)
            .await
            .expect("semaphore should not be dropped");

        let in_progress_gauge =
            metrics::with_label!(node.metrics.recover_blob_backlog, STATUS_IN_PROGRESS);
        let _decrement_guard = GaugeGuard::acquire(&in_progress_gauge);

        let histogram_set = node.metrics.recover_blob_duration_seconds.clone();
        let (is_cancelled, label) = select! {
            _ = blob_synchronizer.cancel_token.cancelled() => {
                tracing::info!("cancelled blob sync");
                (true, metrics::STATUS_CANCELLED)
            }
            sync_result = blob_synchronizer.run() => {
                let sync_result = sync_result.and_then(|_| Ok(node.mark_event_completed(
                    blob_synchronizer.event_sequence_number,
                    &blob_synchronizer.event_id,
                )?));
                if sync_result.is_err() {
                    tracing::error!(?sync_result, "blob synchronizer failed");
                    (false, metrics::STATUS_FAILURE)
                } else {
                    (false, metrics::STATUS_SUCCESS)
                }
            }
        };

        self.remove_sync_handle(&blob_synchronizer.blob_id).await;
        histogram_set
            .with_label_values(&[label])
            .observe(start.elapsed().as_secs_f64());
        if is_cancelled {
            Ok(Some((
                blob_synchronizer.event_sequence_number,
                blob_synchronizer.event_id,
            )))
        } else {
            Ok(None)
        }
    }

    pub async fn cancel_all(&self) -> anyhow::Result<()> {
        let join_handles: Vec<_> = self
            .blob_syncs_in_progress
            .lock()
            .expect("should be able to acquire lock")
            .iter_mut()
            .filter_map(|(_, sync)| sync.cancel())
            .collect();

        try_join_all(join_handles)
            .await?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        Ok(())
    }
}

type SyncJoinHandle = JoinHandle<Result<Option<(usize, EventID)>, TypedStoreError>>;

#[derive(Debug)]
struct InProgressSyncHandle {
    cancel_token: CancellationToken,
    blob_sync_handle: Option<SyncJoinHandle>,
}

impl InProgressSyncHandle {
    fn cancel(&mut self) -> Option<SyncJoinHandle> {
        self.cancel_token.cancel();
        self.blob_sync_handle.take()
    }
}

#[derive(Debug, thiserror::Error)]
enum RecoverSliverError {
    #[error("sliver inconsistent with metadata")]
    Inconsistent(InconsistencyProof),
    #[error(transparent)]
    Database(#[from] TypedStoreError),
}

#[derive(Debug)]
pub(super) struct BlobSynchronizer {
    blob_id: BlobId,
    // TODO(jsmith): Consider making this a weak pointer.
    node: Arc<StorageNodeInner>,
    event_sequence_number: usize,
    event_id: EventID,
    cancel_token: CancellationToken,
}

impl BlobSynchronizer {
    pub fn new(
        event: BlobCertified,
        event_sequence_number: usize,
        node: Arc<StorageNodeInner>,
        cancel_token: CancellationToken,
    ) -> Self {
        Self {
            blob_id: event.blob_id,
            node,
            event_id: event.event_id,
            event_sequence_number,
            cancel_token,
        }
    }

    fn storage(&self) -> &Storage {
        &self.node.storage
    }

    fn encoding_config(&self) -> &EncodingConfig {
        &self.node.encoding_config
    }

    fn committee_service(&self) -> &dyn CommitteeService {
        self.node.committee_service.as_ref()
    }

    fn contract_service(&self) -> &dyn SystemContractService {
        self.node.contract_service.as_ref()
    }

    fn metrics(&self) -> &NodeMetricSet {
        &self.node.metrics
    }

    #[tracing::instrument(skip_all, fields(blob_id = %self.blob_id))]
    async fn run(&self) -> anyhow::Result<()> {
        let histograms = &self.metrics().recover_blob_part_duration_seconds;

        let (_, metadata) = self
            .recover_metadata()
            .observe(histograms.clone(), labels_from_metadata_result)
            .await?;

        let mut sliver_sync_futures: FuturesUnordered<_> = self
            .storage()
            .shards()
            .iter()
            .flat_map(|&shard| {
                [
                    Either::Left(
                        self.recover_sliver::<Primary>(shard, &metadata)
                            .observe(histograms.clone(), labels_from_sliver_result::<Primary>),
                    ),
                    Either::Right(
                        self.recover_sliver::<Secondary>(shard, &metadata)
                            .observe(histograms.clone(), labels_from_sliver_result::<Secondary>),
                    ),
                ]
            })
            .collect();

        while let Some(result) = sliver_sync_futures.next().await {
            match result {
                Err(RecoverSliverError::Inconsistent(inconsistency_proof)) => {
                    tracing::warn!("received an inconsistency proof");
                    // No need to recover other slivers, sync the inconsistency proof and return
                    self.sync_inconsistency_proof(&inconsistency_proof)
                        .observe(histograms.clone(), labels_from_inconsistency_sync_result)
                        .await;
                    break;
                }
                Err(RecoverSliverError::Database(err)) => {
                    panic!("database operations should not fail: {:?}", err)
                }
                _ => (),
            }
        }

        Ok(())
    }

    /// Returns the metadata and true if it was recovered, false if it was retrieved from storage.
    async fn recover_metadata(
        &self,
    ) -> Result<(bool, VerifiedBlobMetadataWithId), TypedStoreError> {
        if let Some(metadata) = self.storage().get_metadata(&self.blob_id)? {
            tracing::debug!("not syncing metadata: already stored");
            return Ok((false, metadata));
        }

        tracing::debug!("syncing metadata");

        let metadata = self
            .node
            .committee_service
            .get_and_verify_metadata(&self.blob_id, &self.node.encoding_config)
            .await;

        self.storage().put_verified_metadata(&metadata)?;

        tracing::debug!("metadata successfully synced");
        Ok((true, metadata))
    }

    #[instrument(skip_all, fields(axis = ?A::default()))]
    async fn recover_sliver<A: EncodingAxis>(
        &self,
        shard: ShardIndex,
        metadata: &VerifiedBlobMetadataWithId,
    ) -> Result<bool, RecoverSliverError> {
        let shard_storage = self
            .storage()
            .shard_storage(shard)
            .expect("shard is managed by this node");

        // TODO(jsmith): Persist sync across reboots (#395)
        // Handling certified messages does not work for handling reboots etc,
        // because the event is already recorded. We need a way to scan and see of all the blobs we
        // know about, which are stored and which are not fully stored.
        if shard_storage.is_sliver_stored::<A>(&self.blob_id)? {
            tracing::debug!("not syncing sliver: already stored");
            return Ok(false);
        }

        let sliver_id = shard.to_pair_index(self.encoding_config().n_shards(), &self.blob_id);
        let sliver_or_proof = recover_sliver::<A>(
            self.committee_service(),
            metadata,
            sliver_id,
            self.encoding_config(),
        )
        .await;

        match sliver_or_proof {
            Ok(sliver) => {
                shard_storage.put_sliver(&self.blob_id, &sliver)?;
                tracing::debug!("sliver successfully synced");
                Ok(true)
            }
            Err(proof) => Err(RecoverSliverError::Inconsistent(proof)),
        }
    }

    async fn sync_inconsistency_proof(&self, inconsistency_proof: &InconsistencyProof) {
        let invalid_blob_certificate = self
            .committee_service()
            .get_invalid_blob_certificate(
                &self.blob_id,
                inconsistency_proof,
                self.encoding_config().n_shards(),
            )
            .await;
        self.contract_service()
            .invalidate_blob_id(&invalid_blob_certificate)
            .await
    }
}

async fn recover_sliver<A: EncodingAxis>(
    committee_service: &dyn CommitteeService,
    metadata: &VerifiedBlobMetadataWithId,
    sliver_id: SliverPairIndex,
    encoding_config: &EncodingConfig,
) -> Result<Sliver, InconsistencyProof> {
    if A::IS_PRIMARY {
        committee_service
            .recover_primary_sliver(metadata, sliver_id, encoding_config)
            .await
            .map(Sliver::Primary)
            .map_err(InconsistencyProof::Primary)
    } else {
        committee_service
            .recover_secondary_sliver(metadata, sliver_id, encoding_config)
            .await
            .map(Sliver::Secondary)
            .map_err(InconsistencyProof::Secondary)
    }
}

fn labels_from_metadata_result(
    result: Option<&Result<(bool, VerifiedBlobMetadataWithId), TypedStoreError>>,
) -> [&'static str; 2] {
    const METADATA: &str = "metadata";

    let status = match result {
        None => metrics::STATUS_ABORTED,
        Some(Ok((true, _))) => metrics::STATUS_SUCCESS,
        Some(Ok((false, _))) => metrics::STATUS_SKIPPED,
        Some(Err(_)) => metrics::STATUS_FAILURE,
    };

    [METADATA, status]
}

const fn labels_from_sliver_result<A: EncodingAxis>(
    result: Option<&Result<bool, RecoverSliverError>>,
) -> [&'static str; 2] {
    let part = A::NAME;

    let status = match result {
        None => metrics::STATUS_ABORTED,
        Some(Ok(true)) => metrics::STATUS_SUCCESS,
        Some(Ok(false)) => metrics::STATUS_SKIPPED,
        Some(Err(RecoverSliverError::Database(_))) => metrics::STATUS_FAILURE,
        Some(Err(RecoverSliverError::Inconsistent(_))) => metrics::STATUS_INCONSISTENT,
    };

    [part, status]
}

const fn labels_from_inconsistency_sync_result(result: Option<&()>) -> [&'static str; 2] {
    const INCONSISTENCY_PROOF: &str = "inconsistency-proof";

    let status = if result.is_none() {
        metrics::STATUS_ABORTED
    } else {
        metrics::STATUS_SUCCESS
    };

    [INCONSISTENCY_PROOF, status]
}
