// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::{
    FutureExt as _, StreamExt, TryFutureExt,
    future::{self, try_join_all},
    stream,
};
use mysten_metrics::{GaugeGuard, InflightGuardFutureExt as _};
use rayon::prelude::*;
use tokio::{
    sync::{Notify, Semaphore, watch},
    task::{JoinHandle, JoinSet},
};
use tokio_metrics::TaskMonitor;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument as _, Span, field};
use typed_store::TypedStoreError;
use walrus_core::{
    BlobId, Epoch, InconsistencyProof, ShardIndex,
    encoding::{EncodingAxis, EncodingConfig, Primary, Secondary},
    metadata::VerifiedBlobMetadataWithId,
};
use walrus_utils::metrics::TaskMonitorFamily;

use super::{
    StorageNodeInner,
    committee::CommitteeService,
    contract_service::SystemContractService,
    metrics::{self, NodeMetricSet, STATUS_IN_PROGRESS, STATUS_QUEUED},
    storage::Storage,
    system_events::{CompletableHandle, EventHandle},
};
use crate::{common::utils::FutureHelpers as _, node::NodeStatus};

#[derive(Debug, Clone)]
struct Permits {
    blob: Arc<Semaphore>,
    sliver_pairs: Arc<Semaphore>,
}

#[derive(Debug, Clone)]
pub(crate) struct BlobSyncHandler {
    // INV: For each blob id at most one sync is in progress at a time.
    blob_syncs_in_progress: Arc<Mutex<HashMap<BlobId, InProgressSyncHandle>>>,
    node: Arc<StorageNodeInner>,
    permits: Permits,
    task_monitors: TaskMonitorFamily<&'static str>,
    monitor_interval: Duration,
}

impl BlobSyncHandler {
    pub fn new(
        node: Arc<StorageNodeInner>,
        max_concurrent_blob_syncs: usize,
        max_concurrent_sliver_syncs: usize,
        monitor_interval: Duration,
    ) -> Self {
        Self {
            blob_syncs_in_progress: Arc::default(),
            task_monitors: TaskMonitorFamily::new(node.registry.clone()),
            permits: Permits {
                blob: Arc::new(Semaphore::new(max_concurrent_blob_syncs)),
                sliver_pairs: Arc::new(Semaphore::new(max_concurrent_sliver_syncs)),
            },
            node,
            monitor_interval,
        }
    }

    /// Periodically checks the status of all in-progress blob syncs, cancelling those that are no
    /// longer certified and returning any that panicked.
    pub fn spawn_task_monitor(this: Arc<BlobSyncHandler>) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                {
                    if let Err(error) = this
                        .cancel_all_expired_syncs_and_mark_events_completed()
                        .await
                    {
                        tracing::error!(?error, "error cancelling expired blob syncs");
                    }
                }

                // Collect all finished in-progress syncs. Note that at the end of blob sync, the
                // handle should have been removed from the map. So handles that are still in the
                // map are either still running or panicked.
                let mut finished_sync_handles = Vec::new();

                {
                    let mut syncs = this
                        .blob_syncs_in_progress
                        .lock()
                        .expect("should be able to acquire lock");
                    let mut completed_blob_ids = Vec::new();

                    for (blob_id, handle) in syncs.iter_mut() {
                        // Collect handles to be awaited.
                        if let Some(sync_handle) = &handle.blob_sync_handle
                            && sync_handle.is_finished()
                        {
                            tracing::info!(
                            walrus.blob_id = %blob_id,
                                "blob sync monitor observed blob sync finished"
                            );
                            if let Some(join_handle) = handle.blob_sync_handle.take() {
                                finished_sync_handles.push((*blob_id, join_handle));
                            }
                            completed_blob_ids.push(*blob_id);
                        }
                    }

                    // Remove completed syncs.
                    for blob_id in completed_blob_ids {
                        syncs.remove(&blob_id);
                    }
                }

                // Now await the handles.
                for (blob_id, handle) in finished_sync_handles {
                    let Err(error) = handle.await else {
                        // Normal completion. This should not happen.
                        // The blob sync handler should have removed the handle after the sync
                        // finished. So technically the sync handle should not be in the map.
                        tracing::warn!(
                            walrus.blob_id = %blob_id,
                            "blob sync finished with success, but still exists in \
                            BlobSyncHandler"
                        );
                        continue;
                    };
                    tracing::error!(
                        walrus.blob_id = %blob_id,
                        ?error,
                        "blob sync task exited with error"
                    );
                    if error.is_panic() {
                        std::panic::resume_unwind(error.into_panic());
                    }
                }

                tokio::time::sleep(this.monitor_interval).await;
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

    /// Cancels all blob syncs using the provided closure and returns the number of cancelled syncs.
    ///
    /// If `should_mark_events_complete` is `true`, the corresponding events are marked as complete.
    #[tracing::instrument(skip(self, filter_map_closure))]
    async fn cancel_syncs<F>(
        &self,
        filter_map_closure: F,
        should_mark_events_complete: bool,
    ) -> anyhow::Result<usize>
    where
        F: Fn((&BlobId, &mut InProgressSyncHandle)) -> Option<SyncJoinHandle> + Sync + Send,
    {
        tracing::debug!("cancelling matching blob syncs");

        let join_handles: Vec<_> = {
            let mut in_progress_guard = self
                .blob_syncs_in_progress
                .lock()
                .expect("should be able to acquire lock");
            tracing::debug!("acquired lock on the in-progress blob recoveries");

            if cfg!(not(msim)) {
                in_progress_guard
                    .par_iter_mut()
                    .filter_map(filter_map_closure)
                    .collect()
            } else {
                in_progress_guard
                    .iter_mut()
                    .filter_map(filter_map_closure)
                    .collect()
            }
        };
        tracing::debug!("released lock on the in-progress blob recoveries");

        let count = join_handles.len();

        let event_handles = try_join_all(join_handles).await?;

        if should_mark_events_complete {
            event_handles
                .into_iter()
                .for_each(CompletableHandle::mark_as_complete);
        }

        if count > 0 {
            tracing::info!("cancelled {count} blob syncs");
        } else {
            tracing::debug!("no blob syncs cancelled");
        }
        Ok(count)
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

        let closure = |(blob_id, sync): (&BlobId, &mut InProgressSyncHandle)| {
            self.node
                .is_blob_not_certified(blob_id)
                .then(|| sync.cancel())
                .flatten()
        };

        self.cancel_syncs(closure, true).await
    }

    /// Similar to [`Self::cancel_all_expired_syncs_and_mark_events_completed`], but for all blob
    /// syncs.
    #[tracing::instrument(skip(self))]
    pub async fn cancel_all_syncs_and_mark_events_completed(&self) -> anyhow::Result<usize> {
        tracing::info!("cancelling all blob syncs");

        self.cancel_syncs(|(_, sync)| sync.cancel(), true).await
    }

    /// Cancels all blob syncs and returns the number of cancelled syncs. Does not mark the
    /// corresponding events as complete.
    #[tracing::instrument(skip_all)]
    pub async fn cancel_all(&self) -> anyhow::Result<usize> {
        self.cancel_syncs(|(_, sync)| sync.cancel(), false).await
    }

    async fn remove_sync_handle(&self, blob_id: &BlobId) {
        self.blob_syncs_in_progress
            .lock()
            .expect("should be able to acquire lock")
            .remove(blob_id);
    }

    /// Starts a blob sync for the given `blob_id` and `certified_epoch`.
    ///
    /// Returns a `Notify` that is notified when the sync task is finished.
    #[tracing::instrument(skip_all, fields(otel.kind = "PRODUCER"))]
    pub async fn start_sync(
        &self,
        blob_id: BlobId,
        certified_epoch: Epoch,
        event_handle: Option<EventHandle>,
    ) -> Result<Arc<Notify>, TypedStoreError> {
        let mut in_progress = self
            .blob_syncs_in_progress
            .lock()
            .expect("should be able to acquire lock");

        let finish_notify = match in_progress.entry(blob_id) {
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
                let finish_notify = Arc::new(Notify::new());

                let cancel_token = CancellationToken::new();
                let synchronizer = BlobSynchronizer::new(
                    blob_id,
                    certified_epoch,
                    self.node.clone(),
                    cancel_token.clone(),
                );

                let notify_clone = finish_notify.clone();
                let blob_sync_handler_clone = self.clone();
                let permits_clone = self.permits.clone();

                let monitor = self.task_monitors.get_or_insert(&"blob_recovery");
                let sync_handle = tokio::spawn(TaskMonitor::instrument(&monitor, async move {
                    let result = blob_sync_handler_clone
                        .sync_blob_for_all_shards(synchronizer, permits_clone, event_handle)
                        .instrument(spawned_trace)
                        .await;
                    notify_clone.notify_one();
                    result
                }));

                entry.insert(InProgressSyncHandle {
                    cancel_token,
                    blob_sync_handle: Some(sync_handle),
                    finish_notify: finish_notify.clone(),
                });
                finish_notify
            }
            Entry::Occupied(existing_sync) => {
                // A blob sync with a lower sequence number is already in progress. We can safely
                // try to increase the event cursor since it will only be advanced once that sync is
                // finished or cancelled due to an invalid blob event.
                event_handle.mark_as_complete();

                existing_sync.get().finish_notify.clone()
            }
        };
        Ok(finish_notify)
    }

    #[tracing::instrument(skip_all)]
    async fn sync_blob_for_all_shards(
        self,
        synchronizer: BlobSynchronizer,
        permits: Permits,
        mut event_handle: Option<EventHandle>,
    ) -> Option<EventHandle> {
        let start = tokio::time::Instant::now();
        let blob_id = synchronizer.blob_id;

        let queued_gauge =
            walrus_utils::with_label!(self.node.metrics.recover_blob_backlog, STATUS_QUEUED);
        let in_progress_gauge =
            walrus_utils::with_label!(self.node.metrics.recover_blob_backlog, STATUS_IN_PROGRESS);

        let cancel_token = synchronizer.cancel_token.clone();

        // Before running the blob sync, check if the node is in recovery catch up, since the node
        // status may have changed since the blob sync was started.
        // Note that if the node status is not in recovery catch up, it is safe to start the sync
        // without further status checking since node entering recovery catch up will cancel all
        // the blob syncs, including this one.
        let (label, _guard) = match synchronizer
            .node
            .storage
            .node_status()
            .expect("node status should be set")
        {
            NodeStatus::RecoveryCatchUp => {
                tracing::debug!(
                    "about to start blob sync, but node is in recovery catch up, skipping blob sync"
                );
                event_handle.mark_as_complete();
                event_handle = None;
                (metrics::STATUS_SKIPPED, None)
            }
            _ => {
                tokio::select! {
                    biased;

                    _ = cancel_token.cancelled() => {
                        tracing::debug!("cancelled blob sync");
                        (metrics::STATUS_CANCELLED, None)
                    },

                    guard = async {
                        // Await claiming the permit inside this async closure, to enable
                        // cancellation to also cancel waiting for the permit.
                        let _permit = permits
                            .blob
                            .acquire_owned()
                            .count_in_flight(queued_gauge)
                            .await
                            .expect("semaphore should not be dropped");

                        let decrement_guard = GaugeGuard::acquire(&in_progress_gauge);

                        synchronizer.run(permits.sliver_pairs).await;

                        decrement_guard
                    } => {
                        event_handle.mark_as_complete();
                        event_handle = None;
                        (metrics::STATUS_SUCCESS, Some(guard))
                    }
                }
            }
        };

        // We remove the blob handler regardless of the result.
        self.remove_sync_handle(&blob_id).await;

        walrus_utils::with_label!(self.node.metrics.recover_blob_duration_seconds, label)
            .observe(start.elapsed().as_secs_f64());

        event_handle
    }

    /// Returns the list of blob ids that are currently being synced.
    pub fn blob_sync_in_progress(&self) -> HashSet<BlobId> {
        self.blob_syncs_in_progress
            .lock()
            .expect("should be able to acquire lock")
            .keys()
            .cloned()
            .collect()
    }
}

type SyncJoinHandle = JoinHandle<Option<EventHandle>>;

#[derive(Debug)]
struct InProgressSyncHandle {
    cancel_token: CancellationToken,
    blob_sync_handle: Option<SyncJoinHandle>,
    finish_notify: Arc<Notify>,
}

impl InProgressSyncHandle {
    // Important: Awaiting the returned `SyncJoinHandle` requires a lock on the
    // `blob_syncs_in_progress`.
    fn cancel(&mut self) -> Option<SyncJoinHandle> {
        self.cancel_token.cancel();
        self.finish_notify.notify_one();
        self.blob_sync_handle.take()
    }
}

#[derive(Debug, thiserror::Error)]
enum RecoverSliverError {
    #[error("sliver inconsistent with metadata")]
    Inconsistent(InconsistencyProof),
    #[error(transparent)]
    Database(#[from] TypedStoreError),
    #[error(transparent)]
    WatchRecvError(#[from] watch::error::RecvError),
}

#[derive(Debug)]
pub(super) struct BlobSynchronizer {
    blob_id: BlobId,
    node: Arc<StorageNodeInner>,
    certified_epoch: Epoch,
    cancel_token: CancellationToken,
}

impl BlobSynchronizer {
    pub fn new(
        blob_id: BlobId,
        certified_epoch: Epoch,
        node: Arc<StorageNodeInner>,
        cancel_token: CancellationToken,
    ) -> Self {
        Self {
            blob_id,
            node,
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

    /// Runs the synchronizer until blob sync is complete.
    #[tracing::instrument(skip_all)]
    async fn run(self, sliver_permits: Arc<Semaphore>) {
        let this = Arc::new(self);
        let histograms = &this.metrics().recover_blob_part_duration_seconds;

        let shared_metadata = this
            .clone()
            .recover_metadata()
            .observe_future(histograms.clone(), labels_from_metadata_result)
            .map_ok(|(_, metadata)| Arc::new(metadata))
            .await
            .expect("database operations should not fail");

        while let Err(error) = this
            .clone()
            .recover_blob_slivers(sliver_permits.clone(), shared_metadata.clone())
            .await
        {
            let backoff_duration = if cfg!(msim) {
                // Doing fast retry in simtest to save some time.
                Duration::from_secs(0)
            } else {
                Duration::from_secs(30)
            };
            tracing::warn!(
                ?error,
                "recovering sliver failed with error {:?}, retrying after {} seconds",
                error,
                backoff_duration.as_secs()
            );
            tokio::time::sleep(backoff_duration).await;
        }
    }

    /// Starts the task that syncs all the missing slivers for a blob.
    async fn recover_blob_slivers(
        self: Arc<Self>,
        sliver_permits: Arc<Semaphore>,
        shared_metadata: Arc<VerifiedBlobMetadataWithId>,
    ) -> Result<(), RecoverSliverError> {
        let histograms = &self.metrics().recover_blob_part_duration_seconds;

        // Note that we only need to recover the slivers in the shards that are assigned to the
        // node at the time of the start of blob sync. Due to slow event processing, the contract
        // might have entered the new epoch with new shard assignment. Upon discovery of the new
        // shard assignment, the node only needs to recover the slivers assigned in the epoch
        // of the current event due to that:
        //   - The node may not have created the new shards yet.
        //   - Since the blob is certified in an earlier epoch, it is this node's responsibility to
        //     hold, recover, and transfer the shard to the new owner.
        //
        // If the node is severally lagging behind and the certified_epoch is 2 epochs older,
        // this function panics since no shard assignment info is found. Upon restarting the node,
        // the node will enter recovery mode until catching up with all the events and start
        // recovering all the missing blobs.
        let latest_event_epoch = self.node.current_event_epoch().await?;
        let futures_iter = self
            .node
            .owned_shards_at_epoch(latest_event_epoch)
            .unwrap_or_else(|_| {
                tracing::error!(
                    "shard assignment must be found at the certified epoch {}",
                    latest_event_epoch
                );
                panic!("shard assignment must be found at the certified epoch {latest_event_epoch}")
            })
            .into_iter()
            .map(|shard| {
                self.clone()
                    .recover_slivers_for_shard(shared_metadata.clone(), shard)
            });

        let futures_with_permits = stream::iter(futures_iter).then(move |future| {
            let permits = sliver_permits.clone();

            // We use a future to get the permit. Only then is the future returned from the stream
            // to be awaited.
            #[allow(clippy::async_yields_async)]
            async move {
                let claimed_permit = permits
                    .acquire_owned()
                    .await
                    .expect("semaphore should not been dropped");
                // Attach the permit to the future, so that it is held until the future completes.
                future.map(|result| (result, claimed_permit))
            }
        });
        let mut futures_with_permits = std::pin::pin!(futures_with_permits);
        // When stored in a JoinSet, tasks are aborted on drop.
        let mut pending_tasks = JoinSet::new();

        loop {
            tokio::select! {
                biased;

                Some(future) = futures_with_permits.next() => {
                    // Add the permit and the future to the list of pending futures
                    pending_tasks.spawn(future);
                }
                Some(join_result) = pending_tasks.join_next() => {
                    match join_result {
                        Ok((Err(RecoverSliverError::Inconsistent(inconsistency_proof)), _permit)) =>
                        {
                            tracing::warn!("received an inconsistency proof");
                            // No need to recover other slivers, sync the proof and return
                            self.sync_inconsistency_proof(&inconsistency_proof)
                                .observe_future(histograms.clone(),
                                                labels_from_inconsistency_sync_result)
                                .await;
                            break;
                        }
                        Ok((Err(RecoverSliverError::Database(err)), _)) => {
                            match err {
                                TypedStoreError::UnregisteredColumn(ref column) => {
                                    // It is possible that when the blob recovery is running, a
                                    // shard is removed from the storage, and therefore encounter
                                    // UnregisteredColumn error. In this case, we should not panic
                                    // and instead return an error to retry the blob sync.
                                    tracing::error!(
                                        "database error during sliver sync: unregistered column \
                                        {:?}",
                                        column
                                    );
                                    return Err(RecoverSliverError::Database(err));
                                }
                                // When storage node is shutting down and the runtime is turning
                                // off, internal tasks may get cancelled and this is expected.
                                TypedStoreError::TaskError(ref message) => {
                                    tracing::error!(
                                        "database error during sliver sync: task error {:?}",
                                        message
                                    );
                                    return Err(RecoverSliverError::Database(err));
                                }
                                _ => {
                                    panic!("database operations should not fail: {err:?}")
                                }
                            }
                        }
                        Ok(_) => (),
                        Err(join_err) => match join_err.try_into_panic() {
                            Ok(reason) => {
                                // Cancel other tasks (as a precaution) and resume the panic on
                                // the main task, which was the prior behaviour when we were
                                // using futures instead of tasks.
                                pending_tasks.abort_all();
                                std::panic::resume_unwind(reason);
                            }
                            Err(join_err) => {
                                assert!(join_err.is_cancelled());
                                // We do not poll after cancelling the tasks, and as we have the
                                // join handle in our JoinSet, no one else should be able to
                                // abort the task, therefore should never happen.
                                //
                                // Returning from this function as if the task was cancelled
                                // would leave a sliver unrecovered and block progress, since the
                                // recovery was not cancelled.
                                //
                                // Treating it as success would be worse as we would progress
                                // but not have the sliver stored. We therefore panic.
                                tracing::error!(
                                    error = ?join_err,
                                    "a sliver recovery task cancelled unexpectedly"
                                );
                                panic!("a sliver recovery task cancelled unexpectedly");
                            }
                        }
                    }
                }
                else => {
                    // Both the pending futures and the waiting futures streams have completed,
                    // we are therefore complete.
                    break;
                }
            }
        }

        Ok(())
    }

    /// Drives the recovery and storage of the slivers associated with this blob, for one shard.
    ///
    /// May end early if either sliver results in an inconsistency proof.
    #[tracing::instrument(skip_all, fields(shard))]
    async fn recover_slivers_for_shard(
        self: Arc<Self>,
        metadata: Arc<VerifiedBlobMetadataWithId>,
        shard: ShardIndex,
    ) -> Result<(), RecoverSliverError> {
        let histograms = &self.metrics().recover_blob_part_duration_seconds;

        future::try_join(
            self.clone()
                .recover_sliver::<Primary>(shard, metadata.clone())
                .observe_future(histograms.clone(), labels_from_sliver_result::<Primary>),
            self.clone()
                .recover_sliver::<Secondary>(shard, metadata.clone())
                .observe_future(histograms.clone(), labels_from_sliver_result::<Secondary>),
        )
        .await?;

        Ok(())
    }

    /// Returns the metadata and `true` if it was recovered, `false` if it was retrieved from
    /// storage.
    #[tracing::instrument(skip_all, err)]
    async fn recover_metadata(
        self: Arc<Self>,
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

        self.storage().put_verified_metadata(&metadata).await?;

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
        self: Arc<Self>,
        shard: ShardIndex,
        metadata: Arc<VerifiedBlobMetadataWithId>,
    ) -> Result<bool, RecoverSliverError> {
        {
            let shard_storage = self
                .storage()
                .shard_storage(shard)
                .await
                .unwrap_or_else(|| panic!("shard {shard} is managed by this node"));
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

            sui_macros::fail_point_async!("fail_point_recover_sliver_before_put_sliver");

            match sliver_or_proof {
                Ok(sliver) => {
                    shard_storage.put_sliver(self.blob_id, sliver).await?;
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
            RecoverSliverError::WatchRecvError(_) => {
                tracing::error!(?error, "watch recv error during sliver sync")
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
        Some(Err(RecoverSliverError::WatchRecvError(_))) => metrics::STATUS_FAILURE,
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
