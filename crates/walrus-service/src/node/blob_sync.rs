// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{hash_map::Entry, HashMap},
    ops::Not,
    sync::{Arc, Mutex},
};

use futures::{
    future::try_join_all,
    stream::{self, FuturesUnordered},
    FutureExt as _,
    StreamExt,
    TryFutureExt,
};
use mysten_metrics::{GaugeGuard, GaugeGuardFutureExt};
use sui_types::event::EventID;
use tokio::{select, sync::Semaphore, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{field, info_span, instrument, Instrument, Span};
use typed_store::TypedStoreError;
use walrus_core::{
    encoding::{EncodingAxis, EncodingConfig, Primary, Secondary},
    metadata::VerifiedBlobMetadataWithId,
    BlobId,
    Epoch,
    InconsistencyProof,
    ShardIndex,
};
use walrus_sui::types::BlobCertified;

use super::{
    committee::CommitteeService,
    contract_service::SystemContractService,
    metrics::{self, NodeMetricSet, TelemetryLabel as _, STATUS_IN_PROGRESS, STATUS_QUEUED},
    storage::Storage,
    StorageNodeInner,
};
use crate::common::utils::FutureHelpers as _;

#[derive(Debug, Clone)]
struct Permits {
    blob: Arc<Semaphore>,
    sliver: Arc<Semaphore>,
}

#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct BlobSyncResult {
    pub event_index: usize,
    pub event_id: EventID,
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

    fn mark_event_completed(&self, sync_result: BlobSyncResult) -> Result<(), TypedStoreError> {
        self.node
            .mark_event_completed(sync_result.event_index, &sync_result.event_id)
    }

    /// Cancels any existing blob sync for the provided `blob_id` and marks the corresponding event
    /// as completed.
    ///
    /// Returns `true` if an event was marked as complete.
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

        if let Some(sync_result) = handle.await?? {
            self.mark_event_completed(sync_result)?;
        }

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

        let join_results = try_join_all(join_handles)
            .await?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        for sync_result in join_results.into_iter().flatten() {
            self.mark_event_completed(sync_result)?;
        }

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
        event: BlobCertified,
        event_index: usize,
        start: tokio::time::Instant,
    ) -> Result<(), TypedStoreError> {
        let mut in_progress = self
            .blob_syncs_in_progress
            .lock()
            .expect("should be able to acquire lock");

        match in_progress.entry(event.blob_id) {
            Entry::Vacant(entry) => {
                let spawned_trace = info_span!(
                    parent: None,
                    "blob_sync",
                    "otel.kind" = "CONSUMER",
                    "otel.status_code" = field::Empty,
                    "otel.status_message" = field::Empty,
                    "walrus.event.index" = event_index,
                    "walrus.event.tx_digest" = ?event.event_id.tx_digest,
                    "walrus.event.event_seq" = ?event.event_id.event_seq,
                    "walrus.event.kind" = event.label(),
                    "walrus.blob_id" = %event.blob_id,
                    "error.type" = field::Empty,
                );
                spawned_trace.follows_from(Span::current());

                let cancel_token = CancellationToken::new();
                let synchronizer = BlobSynchronizer::new(
                    event,
                    event_index,
                    self.node.clone(),
                    cancel_token.clone(),
                );

                let sync_handle = tokio::spawn(
                    self.clone()
                        .sync(synchronizer, start, self.permits.clone())
                        .inspect_err(|err| {
                            let span = Span::current();
                            span.record("otel.status_code", "ERROR");
                            span.record("otel.status_message", field::display(err));
                            span.record("error.type", "_OTHER");
                        })
                        .instrument(spawned_trace),
                );
                entry.insert(InProgressSyncHandle {
                    cancel_token,
                    blob_sync_handle: Some(sync_handle),
                });
            }
            Entry::Occupied(_) => {
                // A blob sync with a lower sequence number is already in progress. We can safely
                // try to increase the event cursor since it will only be advanced once that sync is
                // finished or cancelled when the blob expires, is deleted, or marked as invalid.
                self.node
                    .mark_event_completed(event_index, &event.event_id)?;
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip_all, err)]
    pub async fn sync(
        self,
        synchronizer: BlobSynchronizer,
        start: tokio::time::Instant,
        permits: Permits,
    ) -> Result<Option<BlobSyncResult>, anyhow::Error> {
        let node = &synchronizer.node;

        let queued_gauge = metrics::with_label!(node.metrics.recover_blob_backlog, STATUS_QUEUED);
        let _permit = permits
            .blob
            .acquire_owned()
            .count_in_flight(&queued_gauge)
            .await
            .expect("semaphore should not be dropped");

        let in_progress_gauge =
            metrics::with_label!(node.metrics.recover_blob_backlog, STATUS_IN_PROGRESS);
        let _decrement_guard = GaugeGuard::acquire(&in_progress_gauge);

        let output = async {
            select! {
                _ = synchronizer.cancel_token.cancelled() => {
                    tracing::info!("cancelled blob sync");
                    Ok(Some(synchronizer.to_result()))
                }
                sync_result = synchronizer.run(permits.sliver) => match sync_result {
                    Ok(()) => {
                        self.mark_event_completed(synchronizer.to_result())?;
                        Ok(None)
                    }
                    Err(err) => Err(err),
                }

            }
        }
        .await
        .inspect_err(|error| tracing::error!(?error, "blob synchronization failed"));

        // We remove the bob handler regardless of the result.
        self.remove_sync_handle(&synchronizer.blob_id).await;

        let label = match output {
            Ok(Some(_)) => metrics::STATUS_SUCCESS,
            Ok(None) => metrics::STATUS_CANCELLED,
            Err(_) => metrics::STATUS_FAILURE,
        };
        metrics::with_label!(node.metrics.recover_blob_duration_seconds, label)
            .observe(start.elapsed().as_secs_f64());

        output
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

        try_join_all(join_handles)
            .await?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        Ok(count)
    }
}

type SyncJoinHandle = JoinHandle<Result<Option<BlobSyncResult>, anyhow::Error>>;

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
    event_index: usize,
    event_id: EventID,
    certified_epoch: Epoch,
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
            event_index: event_sequence_number,
            certified_epoch: event.epoch,
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

    fn to_result(&self) -> BlobSyncResult {
        BlobSyncResult {
            event_index: self.event_index,
            event_id: self.event_id,
        }
    }

    #[tracing::instrument(skip_all)]
    async fn run(&self, sliver_permits: Arc<Semaphore>) -> anyhow::Result<()> {
        let histograms = &self.metrics().recover_blob_part_duration_seconds;

        let (_, metadata) = self
            .recover_metadata()
            .observe(histograms.clone(), labels_from_metadata_result)
            .await?;
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

        Ok(())
    }

    /// Returns the metadata and true if it was recovered, false if it was retrieved from storage.
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

    #[instrument(
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
        .inspect_err(|err| match err {
            RecoverSliverError::Inconsistent(_) => tracing::debug!(error = %err),
            RecoverSliverError::Database(_) => tracing::error!(error = ?err),
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
