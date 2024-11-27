// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{hash_map::Entry, HashMap},
    ops::Not,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::{
    future::try_join_all,
    stream::{self, FuturesUnordered},
    FutureExt as _,
    StreamExt,
};
use mysten_metrics::{GaugeGuard, GaugeGuardFutureExt};
use tokio::{
    select,
    sync::{Notify, Semaphore},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{field, Instrument as _, Span};
use typed_store::TypedStoreError;
use walrus_core::{
    encoding::{EncodingAxis, EncodingConfig, Primary, Secondary},
    metadata::VerifiedBlobMetadataWithId,
    BlobId,
    Epoch,
    InconsistencyProof,
    ShardIndex,
};

use super::{
    committee::CommitteeService,
    contract_service::SystemContractService,
    metrics::{self, NodeMetricSet, STATUS_IN_PROGRESS, STATUS_QUEUED},
    storage::Storage,
    system_events::{CompletableHandle, EventHandle},
    StorageNodeInner,
};
use crate::common::utils::FutureHelpers as _;

#[derive(Debug, Clone)]
struct Permits {
    blob: Arc<Semaphore>,
    sliver: Arc<Semaphore>,
}

#[derive(Debug, Clone)]
pub(crate) struct BlobSyncHandler {
    // INV: For each blob id at most one sync is in progress at a time.
    blob_syncs_in_progress: Arc<Mutex<HashMap<BlobId, InProgressSyncHandle>>>,
    node: Arc<StorageNodeInner>,
    permits: Permits,
}

impl BlobSyncHandler {
    pub fn new(
        node: Arc<StorageNodeInner>,
        max_concurrent_blob_syncs: usize,
        max_concurrent_sliver_syncs: usize,
    ) -> Self {
        Self {
            blob_syncs_in_progress: Arc::default(),
            node,
            permits: Permits {
                blob: Arc::new(Semaphore::new(max_concurrent_blob_syncs)),
                sliver: Arc::new(Semaphore::new(max_concurrent_sliver_syncs)),
            },
        }
    }

    // Periodically checks the status of all in-progress blob syncs and return any that panicked.
    pub fn spawn_task_monitor(&self) -> JoinHandle<()> {
        let blob_syncs = self.blob_syncs_in_progress.clone();
        tokio::spawn(async move {
            loop {
                // Collect all finished in-progress syncs. Note that at the end of blob sync, the
                // handle should have been removed from the map. So handles that are still in the
                // map are either still running or panicked.
                let mut handles = Vec::new();
                {
                    // Collect all finished in-progress syncs;
                    let mut syncs = blob_syncs.lock().expect("should be able to acquire lock");
                    let mut completed = Vec::new();

                    // Collect handles to be awaited
                    for (blob_id, handle) in syncs.iter_mut() {
                        if let Some(sync_handle) = &handle.blob_sync_handle {
                            if sync_handle.is_finished() {
                                tracing::info!(
                                    walrus.blob_id = %blob_id,
                                    "blob sync monitor observed blob sync finished"
                                );
                                if let Some(join_handle) = handle.blob_sync_handle.take() {
                                    handles.push((*blob_id, join_handle));
                                }
                                completed.push(*blob_id);
                            }
                        }
                    }

                    // Remove completed syncs
                    for blob_id in completed {
                        syncs.remove(&blob_id);
                    }
                }

                // Now await the handles
                for (blob_id, handle) in handles {
                    match handle.await {
                        Ok(_) => {
                            // Normal completion. This should not happen.
                            // The blob sync handler should have removed the handle after the sync
                            // finished. So technically the sync handle should not be in the map.
                            tracing::warn!(
                                walrus.blob_id = %blob_id,
                                "blob sync finished with success, but still exists in \
                                BlobSyncHandler"
                            );
                        }
                        Err(error) => {
                            tracing::error!(
                                walrus.blob_id = %blob_id,
                                ?error,
                                "blob sync task exited with error"
                            );
                            if error.is_panic() {
                                std::panic::resume_unwind(error.into_panic());
                            }
                        }
                    }
                }

                if cfg!(test) {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                } else {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }
            }
        })
    }

    /// Cancels any existing blob sync for the provided `blob_id` and marks the corresponding event
    /// as completed.
    ///
    /// To avoid interference with later events (e.g., cancelling a sync initiated by a later event
    /// or immediately restarting the sync that is cancelled here), this function should be called
    /// before any later events are being handled.
    #[tracing::instrument(skip_all)]
    pub async fn cancel_sync_and_mark_event_complete(
        &self,
        blob_id: &BlobId,
    ) -> anyhow::Result<()> {
        let Some(handle) = self
            .blob_syncs_in_progress
            .lock()
            .expect("should be able to acquire lock")
            .get_mut(blob_id)
            .and_then(|sync| {
                tracing::debug!("cancelling in-progress sync");
                sync.cancel()
            })
        else {
            return Ok(());
        };

        handle.await?.mark_as_complete();

        Ok(())
    }

    /// Cancels all existing blob syncs for blobs that are already expired in the `current_epoch`
    /// and marks the corresponding events as completed.
    ///
    /// Returns the number of thus completed events.
    ///
    /// To avoid interference with later events (e.g., cancelling a sync initiated by a later event
    /// or immediately restarting the sync that is cancelled here), this function should be called
    /// before any later events are being handled.
    #[tracing::instrument(skip(self))]
    pub async fn cancel_all_expired_syncs_and_mark_events_completed(
        &self,
    ) -> anyhow::Result<usize> {
        tracing::debug!("cancelling all blob syncs for expired blobs");

        let join_handles: Vec<_> = self
            .blob_syncs_in_progress
            .lock()
            .expect("should be able to acquire lock")
            .iter_mut()
            .filter_map(|(blob_id, sync)| {
                self.node
                    .is_blob_certified(blob_id)
                    .is_ok_and(Not::not)
                    .then(|| sync.cancel())
                    .flatten()
            })
            .collect();
        let count = join_handles.len();

        try_join_all(join_handles)
            .await?
            .into_iter()
            .for_each(CompletableHandle::mark_as_complete);

        if count > 0 {
            tracing::info!("cancelled {count} blob syncs for now expired blobs");
        } else {
            tracing::debug!("no blob syncs cancelled");
        }

        Ok(count)
    }

    async fn remove_sync_handle(&self, blob_id: &BlobId) {
        self.blob_syncs_in_progress
            .lock()
            .expect("should be able to acquire lock")
            .remove(blob_id);
    }

    #[tracing::instrument(skip_all, fields(otel.kind = "PRODUCER"))]
    pub async fn start_sync(
        &self,
        blob_id: BlobId,
        certified_epoch: Epoch,
        event_handle: Option<EventHandle>,
        start: tokio::time::Instant,
    ) -> Result<Arc<Notify>, TypedStoreError> {
        let mut in_progress = self
            .blob_syncs_in_progress
            .lock()
            .expect("should be able to acquire lock");

        let finish_notify = Arc::new(Notify::new());
        match in_progress.entry(blob_id) {
            Entry::Vacant(entry) => {
                let spawned_trace = tracing::info_span!(
                    parent: &Span::current(),
                    "blob_sync",
                    "otel.kind" = "CONSUMER",
                    "otel.status_code" = field::Empty,
                    "otel.status_message" = field::Empty,
                    "walrus.event.index" = event_handle.as_ref().map(|e| e.index()),
                    "walrus.event.tx_digest" = ?event_handle.as_ref().map(
                        |e| e.event_id().tx_digest
                    ),
                    "walrus.event.event_seq" = event_handle.as_ref().map(
                        |e| e.event_id().event_seq
                    ),
                    "walrus.event.kind" = "certified",
                    "walrus.blob_id" = %blob_id,
                    "error.type" = field::Empty,
                );
                spawned_trace.follows_from(Span::current());

                let cancel_token = CancellationToken::new();
                let synchronizer = BlobSynchronizer::new(
                    blob_id,
                    certified_epoch,
                    event_handle,
                    self.node.clone(),
                    cancel_token.clone(),
                );

                let notify_clone = finish_notify.clone();
                let blob_sync_handler_clone = self.clone();
                let permits_clone = self.permits.clone();

                let sync_handle = tokio::spawn(async move {
                    let result = blob_sync_handler_clone
                        .sync(synchronizer, start, permits_clone)
                        .instrument(spawned_trace)
                        .await;
                    notify_clone.notify_one();
                    result
                });
                entry.insert(InProgressSyncHandle {
                    cancel_token,
                    blob_sync_handle: Some(sync_handle),
                });
            }
            Entry::Occupied(_) => {
                // A blob sync with a lower sequence number is already in progress. We can safely
                // try to increase the event cursor since it will only be advanced once that sync is
                // finished or cancelled due to an invalid blob event.
                event_handle.mark_as_complete();
                finish_notify.notify_one();
            }
        }
        Ok(finish_notify)
    }

    #[tracing::instrument(skip_all)]
    pub async fn sync(
        self,
        synchronizer: BlobSynchronizer,
        start: tokio::time::Instant,
        permits: Permits,
    ) -> Option<EventHandle> {
        let queued_gauge =
            metrics::with_label!(self.node.metrics.recover_blob_backlog, STATUS_QUEUED);
        let _permit = permits
            .blob
            .acquire_owned()
            .count_in_flight(&queued_gauge)
            .await
            .expect("semaphore should not be dropped");

        let in_progress_gauge =
            metrics::with_label!(self.node.metrics.recover_blob_backlog, STATUS_IN_PROGRESS);
        let _decrement_guard = GaugeGuard::acquire(&in_progress_gauge);
        let blob_id = synchronizer.blob_id;

        let (label, event_handle) = async move {
            select! {
                _ = synchronizer.cancel_token.cancelled() => {
                    tracing::info!("cancelled blob sync");
                    (metrics::STATUS_CANCELLED, synchronizer.event_handle)
                }
                _ = synchronizer.run(permits.sliver) => {
                    synchronizer.event_handle.mark_as_complete();
                    (metrics::STATUS_SUCCESS, None)
                }
            }
        }
        .await;

        // We remove the blob handler regardless of the result.
        self.remove_sync_handle(&blob_id).await;

        metrics::with_label!(self.node.metrics.recover_blob_duration_seconds, label)
            .observe(start.elapsed().as_secs_f64());

        event_handle
    }

    /// Cancels all blob syncs and returns the number of cancelled syncs.
    #[tracing::instrument(skip_all)]
    pub async fn cancel_all(&self) -> anyhow::Result<usize> {
        let join_handles: Vec<_> = self
            .blob_syncs_in_progress
            .lock()
            .expect("should be able to acquire lock")
            .iter_mut()
            .filter_map(|(_, sync)| sync.cancel())
            .collect();
        let count = join_handles.len();

        try_join_all(join_handles).await?.into_iter().for_each(drop);

        Ok(count)
    }
}

type SyncJoinHandle = JoinHandle<Option<EventHandle>>;

#[derive(Debug)]
struct InProgressSyncHandle {
    cancel_token: CancellationToken,
    blob_sync_handle: Option<SyncJoinHandle>,
}

impl InProgressSyncHandle {
    // Important: Awaiting the returned `SyncJoinHandle` requires a lock on the
    // `blob_syncs_in_progress`.
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
    node: Arc<StorageNodeInner>,
    event_handle: Option<EventHandle>,
    certified_epoch: Epoch,
    cancel_token: CancellationToken,
}

impl BlobSynchronizer {
    pub fn new(
        blob_id: BlobId,
        certified_epoch: Epoch,
        event_handle: Option<EventHandle>,
        node: Arc<StorageNodeInner>,
        cancel_token: CancellationToken,
    ) -> Self {
        Self {
            blob_id,
            node,
            event_handle,
            certified_epoch,
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

    #[tracing::instrument(skip_all)]
    async fn run(&self, sliver_permits: Arc<Semaphore>) {
        let histograms = &self.metrics().recover_blob_part_duration_seconds;

        let (_, metadata) = self
            .recover_metadata()
            .observe(histograms.clone(), labels_from_metadata_result)
            .await
            .expect("database operations should not fail");
        let metadata = Arc::new(metadata);

        let futures_iter = self.storage().shards().into_iter().flat_map(|shard| {
            [
                self.recover_sliver::<Primary>(shard, metadata.clone())
                    .observe(histograms.clone(), labels_from_sliver_result::<Primary>)
                    .left_future(),
                self.recover_sliver::<Secondary>(shard, metadata.clone())
                    .observe(histograms.clone(), labels_from_sliver_result::<Secondary>)
                    .right_future(),
            ]
        });

        let mut futures_with_permits = stream::iter(futures_iter).then(move |future| {
            let permits = sliver_permits.clone();

            // We use a future to get the permit. Only then is the future returned from the stream
            // to be awaited.
            #[allow(clippy::async_yields_async)]
            async move {
                let claimed_permit = permits
                    .acquire_owned()
                    .await
                    .expect("semaphore has not been dropped");
                // Attach the permit to the future, so that it is held until the future completes.
                future.map(|result| (result, claimed_permit))
            }
        });
        let mut futures_with_permits = std::pin::pin!(futures_with_permits);
        let mut pending_futures = FuturesUnordered::new();

        loop {
            tokio::select! {
                biased;

                Some(future) = futures_with_permits.next() => {
                    // Add the permit and the future to the list of pending futures
                    pending_futures.push(future);
                }
                Some((result, _permit)) = pending_futures.next() => {
                    match result {
                        Err(RecoverSliverError::Inconsistent(inconsistency_proof)) => {
                            tracing::warn!("received an inconsistency proof");
                            // No need to recover other slivers, sync the proof and return
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
                else => {
                    // Both the pending futures and the waiting futures streams have completed, we
                    // are therefore complete.
                    break;
                }
            }
        }
    }

    /// Returns the metadata and `true` if it was recovered, `false` if it was retrieved from
    /// storage.
    #[tracing::instrument(skip_all, err)]
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
            .get_and_verify_metadata(self.blob_id, self.certified_epoch)
            .await;

        self.storage().put_verified_metadata(&metadata)?;

        tracing::debug!("metadata successfully synced");
        Ok((true, metadata))
    }

    #[tracing::instrument(
        skip_all,
        fields(
            walrus.shard_index = %shard,
            walrus.sliver.r#type = A::NAME,
            walrus.sliver.pair_index
        )
    )]
    async fn recover_sliver<A: EncodingAxis>(
        &self,
        shard: ShardIndex,
        metadata: Arc<VerifiedBlobMetadataWithId>,
    ) -> Result<bool, RecoverSliverError> {
        {
            let shard_storage = self
                .storage()
                .shard_storage(shard)
                .expect("shard is managed by this node");
            let sliver_id = shard.to_pair_index(self.encoding_config().n_shards(), &self.blob_id);

            Span::current().record("walrus.sliver.pair_index", field::display(sliver_id));

            if shard_storage.is_sliver_stored::<A>(&self.blob_id)? {
                tracing::debug!("not syncing sliver: already stored");
                return Ok(false);
            }
            tracing::debug!("syncing sliver");

            let sliver_or_proof = self
                .committee_service()
                .recover_sliver(metadata, sliver_id, A::sliver_type(), self.certified_epoch)
                .await;

            match sliver_or_proof {
                Ok(sliver) => {
                    shard_storage.put_sliver(&self.blob_id, &sliver)?;
                    tracing::debug!("sliver successfully synced");
                    Ok(true)
                }
                Err(proof) => {
                    tracing::debug!("sliver inconsistent");
                    Err(RecoverSliverError::Inconsistent(proof))
                }
            }
        }
        .inspect_err(|error| match error {
            RecoverSliverError::Inconsistent(_) => tracing::debug!(?error),
            RecoverSliverError::Database(_) => {
                tracing::error!(?error, "database error during sliver sync")
            }
        })
    }

    async fn sync_inconsistency_proof(&self, inconsistency_proof: &InconsistencyProof) {
        let invalid_blob_certificate = self
            .committee_service()
            .get_invalid_blob_certificate(self.blob_id, inconsistency_proof)
            .await;
        self.contract_service()
            .invalidate_blob_id(&invalid_blob_certificate)
            .await
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
