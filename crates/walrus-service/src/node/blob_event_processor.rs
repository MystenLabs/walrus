// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    sync::{
        Arc,
        Mutex,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use sui_macros::fail_point_async;
use tokio::{
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use walrus_sui::types::{BlobCertified, BlobDeleted, BlobEvent, InvalidBlobId};
use walrus_utils::metrics::monitored_scope;

use super::{StorageNodeInner, blob_sync::BlobSyncHandler, metrics, system_events::EventHandle};
use crate::{
    event::events::CheckpointEventPosition,
    node::{
        storage::blob_info::{BlobInfoApi, CertifiedBlobInfoApi},
        system_events::CompletableHandle,
    },
};

// Poll interval for checking pending background events.
const PENDING_EVENTS_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// A utility struct that wraps an `Arc<AtomicU32>` for tracking pending events.
/// Provides convenient `inc()` and `dec()` methods that return the new value.
#[derive(Debug, Clone)]
struct PendingEventCounter {
    inner: Arc<AtomicU32>,
}

impl PendingEventCounter {
    /// Creates a new counter initialized to 0.
    fn new() -> Self {
        Self {
            inner: Arc::new(AtomicU32::new(0)),
        }
    }

    /// Increments the counter and returns the new value.
    fn inc(&self) -> u32 {
        // We must ensure the increment happens-before the event is observable by the worker via
        // the channel. Without a strong ordering, the send could be observed before the increment,
        // allowing the receiver to drop the guard and do fetch_sub first, which would underflow
        // the counter.
        self.inner.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Decrements the counter and returns the new value.
    fn dec(&self) -> u32 {
        // Same reasoning as inc() applies hereto use SeqCst ordering.
        let prev = self.inner.fetch_sub(1, Ordering::SeqCst);
        if prev == 0 {
            // This should never happen, since the counter is incremented before the event is
            // observable by the worker via the channel.
            panic!("pending event counter underflow");
        }
        prev.saturating_sub(1)
    }

    /// Gets the current value of the counter.
    fn get(&self) -> u32 {
        self.inner.load(Ordering::SeqCst)
    }
}

/// A guard that automatically decrements the pending event counter when dropped.
/// This ensures robust tracking of pending events even if processing errors occur.
#[derive(Debug)]
struct PendingEventGuard {
    pending_event_count: PendingEventCounter,
    worker_index: usize,
    metrics: Arc<crate::node::metrics::NodeMetricSet>,
}

impl PendingEventGuard {
    fn new(
        pending_event_count: PendingEventCounter,
        worker_index: usize,
        metrics: Arc<crate::node::metrics::NodeMetricSet>,
    ) -> Self {
        Self {
            pending_event_count,
            worker_index,
            metrics,
        }
    }
}

impl Drop for PendingEventGuard {
    fn drop(&mut self) {
        let current_pending_event_count = self.pending_event_count.dec();
        walrus_utils::with_label!(
            self.metrics
                .pending_processing_blob_event_in_background_processors,
            &self.worker_index.to_string()
        )
        .set(<i64 as From<u32>>::from(current_pending_event_count));
    }
}

/// Wrapper for EventHandle and BlobEvent that includes a pending event guard.
/// When this struct is dropped, the guard automatically decrements the counter.
#[derive(Debug)]
struct TrackedEvent {
    event_handle: EventHandle,
    blob_event: BlobEvent,
    checkpoint_position: CheckpointEventPosition,
    _guard: PendingEventGuard,
}

/// Background event processor that processes blob events in the background. It processes events
/// sequentially based on the order of events in the channel.
#[derive(Debug)]
struct BackgroundEventProcessor {
    node: Arc<StorageNodeInner>,
    blob_sync_handler: Arc<BlobSyncHandler>,
    event_receiver: UnboundedReceiver<TrackedEvent>,
    worker_index: usize,
    /// Cancellation token to signal this processor to shut down gracefully.
    cancel_token: CancellationToken,
    /// Node-level cancellation token to trigger node-wide shutdown on errors.
    node_cancel_token: CancellationToken,
}

impl BackgroundEventProcessor {
    fn new(
        node: Arc<StorageNodeInner>,
        blob_sync_handler: Arc<BlobSyncHandler>,
        event_receiver: UnboundedReceiver<TrackedEvent>,
        worker_index: usize,
        cancel_token: CancellationToken,
        node_cancel_token: CancellationToken,
    ) -> Self {
        Self {
            node,
            blob_sync_handler,
            event_receiver,
            worker_index,
            cancel_token,
            node_cancel_token,
        }
    }

    /// Runs the background event processor.
    async fn run_background_event_processor(&mut self) {
        loop {
            tokio::select! {
                _ = self.cancel_token.cancelled() => {
                    tracing::info!(
                        worker_index = self.worker_index,
                        "background event processor shutting down gracefully"
                    );
                    break;
                }
                tracked_event = self.event_receiver.recv() => {
                    match tracked_event {
                        Some(tracked_event) => {
                            walrus_utils::with_label!(
                                self.node.metrics.pending_processing_blob_event_in_queue,
                                &self.worker_index.to_string()
                            )
                            .dec();

                            let TrackedEvent {
                                event_handle,
                                blob_event,
                                checkpoint_position,
                                _guard,
                            } = tracked_event;

                            // The guard will automatically decrement the counter when dropped.
                            if let Err(error) = self
                                .process_event(event_handle, blob_event, checkpoint_position)
                                .await
                            {
                                // Propagate error to the node by triggering node-wide shutdown.
                                // All event processing errors should cause the node to exit.
                                tracing::error!(
                                    ?error,
                                    worker_index = self.worker_index,
                                    "error processing blob event, triggering node shutdown"
                                );
                                break;
                            }
                        }
                        None => {
                            // Channel closed, exit gracefully
                            tracing::info!(
                                worker_index = self.worker_index,
                                "background event processor channel closed, shutting down"
                            );
                            break;
                        }
                    }
                }
            }
        }
        // If any background event processor exits its loop, we should trigger cancellation to the
        // entire node.
        self.node_cancel_token.cancel();
    }

    /// Processes a blob event.
    async fn process_event(
        &self,
        event_handle: EventHandle,
        blob_event: BlobEvent,
        checkpoint_position: CheckpointEventPosition,
    ) -> anyhow::Result<()> {
        match blob_event {
            BlobEvent::Certified(event) => {
                let _scope = monitored_scope::monitored_scope("ProcessEvent::BlobEvent::Certified");
                self.process_blob_certified_event(event_handle, event, checkpoint_position)
                    .await?;
            }
            BlobEvent::Deleted(event) => {
                let _scope = monitored_scope::monitored_scope("ProcessEvent::BlobEvent::Deleted");
                self.process_blob_deleted_event(event_handle, event).await?;
            }
            BlobEvent::InvalidBlobID(event) => {
                let _scope =
                    monitored_scope::monitored_scope("ProcessEvent::BlobEvent::InvalidBlobID");
                self.process_blob_invalid_event(event_handle, event).await?;
            }
            BlobEvent::DenyListBlobDeleted(_) => {
                // TODO (WAL-424): Implement DenyListBlobDeleted event handling.
                todo!("DenyListBlobDeleted event handling is not yet implemented");
            }
            BlobEvent::Registered(_) => {
                unreachable!("registered event should be processed immediately");
            }
        }

        Ok(())
    }

    /// Processes a blob certified event.
    #[tracing::instrument(skip_all)]
    async fn process_blob_certified_event(
        &self,
        event_handle: EventHandle,
        event: BlobCertified,
        checkpoint_position: CheckpointEventPosition,
    ) -> anyhow::Result<()> {
        let start = tokio::time::Instant::now();

        let histogram_set = self.node.metrics.recover_blob_duration_seconds.clone();

        // Get the current event epoch to check if the blob is fully stored up to the shard
        // assignment in this epoch. If the current event epoch is not set (the node may just
        // started), we always start the blob sync.
        let current_event_epoch = self.node.try_get_current_event_epoch();

        #[allow(unused_mut)]
        let mut skip_blob_sync_in_test = false;
        // No op when not in simtest mode.
        sui_macros::fail_point_if!(
            "skip_non_extension_certified_event_triggered_blob_sync",
            || {
                skip_blob_sync_in_test = !event.is_extension;
            }
        );

        if skip_blob_sync_in_test
            || !self.node.is_blob_certified(&event.blob_id)?
            || self.node.storage.node_status()?.is_catching_up()
            || (current_event_epoch.is_some()
                && self
                    .node
                    .is_stored_at_all_shards_at_epoch(
                        &event.blob_id,
                        current_event_epoch.expect("just checked that current event epoch is set"),
                    )
                    .await?)
        {
            event_handle.mark_as_complete();

            tracing::debug!(
                %event.blob_id,
                %event.epoch,
                %event.is_extension,
                "skipping syncing blob during certified event processing",
            );

            walrus_utils::with_label!(histogram_set, metrics::STATUS_SKIPPED)
                .observe(start.elapsed().as_secs_f64());

            return Ok(());
        }

        fail_point_async!("fail_point_process_blob_certified_event");

        self.node
            .maybe_apply_live_upload_deferral(
                event.blob_id,
                checkpoint_position.checkpoint_sequence_number,
            )
            .await;

        // Slivers and (possibly) metadata are not stored, so initiate blob sync.
        self.blob_sync_handler
            .start_sync(event.blob_id, event.epoch, Some(event_handle))
            .await?;

        walrus_utils::with_label!(histogram_set, metrics::STATUS_QUEUED)
            .observe(start.elapsed().as_secs_f64());

        Ok(())
    }

    /// Processes a blob deleted event.
    #[tracing::instrument(
        skip_all,
        fields(walrus.blob_id = %event.blob_id, walrus.epoch = tracing::field::Empty),
    )]
    async fn process_blob_deleted_event(
        &self,
        event_handle: EventHandle,
        event: BlobDeleted,
    ) -> anyhow::Result<()> {
        let blob_id = event.blob_id;
        let current_epoch = self.node.current_committee_epoch();
        tracing::Span::current().record("walrus.epoch", current_epoch);

        if let Some(blob_info) = self.node.storage.get_blob_info(&blob_id)? {
            if !blob_info.is_certified(current_epoch) {
                self.node
                    .blob_retirement_notifier
                    .notify_blob_retirement(&blob_id);
                self.blob_sync_handler
                    .cancel_sync_and_mark_event_complete(&blob_id)
                    .await?;
            }
            // Note that this function is called *after* the blob info has already been updated with
            // the event. So it can happen that the only registered blob was deleted and the blob is
            // now no longer registered.
            // *Important*: We use the event's epoch for this check (as opposed to the current
            // epoch) as subsequent certify or delete events may update the `blob_info`; so we
            // cannot remove it even if it is no longer valid in the *current* epoch
            if blob_info.can_data_be_deleted(event.epoch)
                && self.node.garbage_collection_config.enable_data_deletion
            {
                tracing::debug!("deleting data for deleted blob");
                self.node
                    .storage
                    .attempt_to_delete_blob_data(&blob_id, event.epoch, &self.node.metrics)
                    .await?;
            }
        } else if self
            .node
            .storage
            .node_status()?
            .is_catching_up_with_incomplete_history()
        {
            tracing::debug!(
                "handling a `BlobDeleted` event for an untracked blob while catching up with \
                incomplete history; not deleting blob data"
            );
        } else {
            tracing::warn!("handling a `BlobDeleted` event for an untracked blob");
        }

        event_handle.mark_as_complete();

        Ok(())
    }

    /// Processes a blob invalid event.
    #[tracing::instrument(skip_all)]
    async fn process_blob_invalid_event(
        &self,
        event_handle: EventHandle,
        event: InvalidBlobId,
    ) -> anyhow::Result<()> {
        self.node
            .blob_retirement_notifier
            .notify_blob_retirement(&event.blob_id);
        self.blob_sync_handler
            .cancel_sync_and_mark_event_complete(&event.blob_id)
            .await?;
        self.node.storage.delete_blob_data(&event.blob_id).await?;

        event_handle.mark_as_complete();
        Ok(())
    }
}

#[derive(Debug)]
enum BlobEventProcessorImpl {
    // Background processors that process events in parallel.
    BackgroundProcessors {
        background_processor_senders: Vec<UnboundedSender<TrackedEvent>>,
        /// Join handles for background processors, wrapped in Mutex for shutdown access.
        /// These are taken out during `wait_for_shutdown()`.
        background_processors: Mutex<Option<Vec<JoinHandle<()>>>>,
        // The number of events that are pending to be processed in each background processor.
        // Each counter is shared with individual BackgroundEventProcessor.
        //
        // We use per background processor count to avoid high contention on the Atomic variable
        // when tracking the total number of pending events to be processed.
        background_per_processor_pending_event_count: Vec<PendingEventCounter>,
        /// Cancellation token to signal all background processors to shut down.
        cancel_token: CancellationToken,
    },

    // When there are no background workers, we use a sequential processor to process events using
    // this processor. This is to keep the same behavior as before BackgroundEventProcessor.
    // INVARIANT: sequential_processor must be Some if background_processor_senders is empty.
    SequentialProcessor {
        background_event_processor: Arc<BackgroundEventProcessor>,
    },
}

impl BlobEventProcessorImpl {
    async fn process_event_impl(
        &self,
        node: &Arc<StorageNodeInner>,
        event_handle: EventHandle,
        blob_event: BlobEvent,
        checkpoint_position: CheckpointEventPosition,
    ) -> Result<(), anyhow::Error> {
        match self {
            BlobEventProcessorImpl::BackgroundProcessors {
                background_processor_senders,
                background_processors: _,
                background_per_processor_pending_event_count,
                cancel_token: _,
            } => {
                // We send the event to one of the workers to process in parallel.
                // Note that in order to remain sequential processing for the same BlobID, we always
                // send events for the same BlobID to the same worker.
                // Currently the number of workers is fixed through the lifetime of the node. But in
                // case the node want to dynamically adjust the worker, we need to be careful to not
                // break this requirement.
                let processor_index = blob_event.blob_id().first_two_bytes() as usize
                    % background_processor_senders.len();

                walrus_utils::with_label!(
                    node.metrics.pending_processing_blob_event_in_queue,
                    &processor_index.to_string()
                )
                .inc();

                let current_processor_pending_event_count =
                    background_per_processor_pending_event_count[processor_index].clone();

                // Increment the counter and create a guard that will decrement it when dropped
                let current_pending_event_count = current_processor_pending_event_count.inc();
                walrus_utils::with_label!(
                    node.metrics
                        .pending_processing_blob_event_in_background_processors,
                    &processor_index.to_string()
                )
                .set(<i64 as From<u32>>::from(current_pending_event_count));

                // Create the guard that will automatically decrement the counter when dropped
                let guard = PendingEventGuard::new(
                    current_processor_pending_event_count,
                    processor_index,
                    node.metrics.clone(),
                );

                // Send the wrapped event with the guard
                let tracked_event = TrackedEvent {
                    event_handle,
                    blob_event,
                    checkpoint_position,
                    _guard: guard,
                };

                background_processor_senders[processor_index]
                    .send(tracked_event)
                    .map_err(|e| {
                        anyhow::anyhow!("failed to send event to background processor: {}", e)
                    })?;
            }
            BlobEventProcessorImpl::SequentialProcessor {
                background_event_processor: sequential_processor,
            } => {
                sequential_processor
                    .process_event(event_handle, blob_event, checkpoint_position)
                    .await?;
            }
        }

        Ok(())
    }
}
/// Blob event processor that processes blob events. It can be configured to process events
/// sequentially or in parallel using background workers.
#[derive(Debug)]
pub struct BlobEventProcessor {
    node: Arc<StorageNodeInner>,
    blob_event_processor_impl: BlobEventProcessorImpl,
}

impl BlobEventProcessor {
    pub fn new(
        node: Arc<StorageNodeInner>,
        blob_sync_handler: Arc<BlobSyncHandler>,
        num_workers: usize,
        node_cancel_token: CancellationToken,
    ) -> Self {
        let mut background_processor_senders = Vec::with_capacity(num_workers);
        let mut background_processors = Vec::with_capacity(num_workers);
        let mut background_per_processor_pending_event_count = Vec::with_capacity(num_workers);
        let blob_event_processor_impl = if num_workers > 0 {
            // Create a cancellation token shared by all background processors.
            let cancel_token = CancellationToken::new();
            for worker_index in 0..num_workers {
                let (tx, rx) = mpsc::unbounded_channel();
                background_processor_senders.push(tx);
                let pending_event_count = PendingEventCounter::new();
                background_per_processor_pending_event_count.push(pending_event_count);
                let mut background_processor = BackgroundEventProcessor::new(
                    node.clone(),
                    blob_sync_handler.clone(),
                    rx,
                    worker_index,
                    cancel_token.clone(),
                    node_cancel_token.clone(),
                );
                background_processors.push(tokio::spawn(async move {
                    background_processor.run_background_event_processor().await;
                }));
            }
            BlobEventProcessorImpl::BackgroundProcessors {
                background_processor_senders,
                background_processors: Mutex::new(Some(background_processors)),
                background_per_processor_pending_event_count,
                cancel_token,
            }
        } else {
            let background_event_processor = {
                // Create a sequential processor to process events sequentially if no background
                // workers are configured.
                let (_tx, rx) = mpsc::unbounded_channel();
                // For sequential processor, create dummy cancellation tokens since the processor
                // runs in the caller's context and doesn't need separate shutdown coordination.
                let cancel_token = CancellationToken::new();
                Arc::new(BackgroundEventProcessor::new(
                    node.clone(),
                    blob_sync_handler.clone(),
                    rx,
                    0, // worker_index for sequential processor
                    cancel_token,
                    node_cancel_token,
                ))
            };
            BlobEventProcessorImpl::SequentialProcessor {
                background_event_processor,
            }
        };

        Self {
            node,
            blob_event_processor_impl,
        }
    }

    /// Processes a blob event.
    pub async fn process_event(
        &self,
        event_handle: EventHandle,
        blob_event: BlobEvent,
        checkpoint_position: CheckpointEventPosition,
    ) -> anyhow::Result<()> {
        // Update the blob info based on the event.
        // This processing must be sequential and cannot be parallelized, since there is logical
        // dependency between events.
        self.node
            .storage
            .update_blob_info(event_handle.index(), &blob_event)?;

        if let BlobEvent::Registered(_) = &blob_event {
            // Registered event is marked as complete immediately. We need to process registered
            // events as fast as possible to catch up to the latest event in order to not miss
            // blob sliver uploads.
            //
            // If we want to do this in parallel, we shouldn't mix registered event processing with
            // certified event processing, as certified events take longer and can block following
            // registered events.
            let _scope = monitored_scope::monitored_scope("ProcessEvent::BlobEvent::Registered");
            event_handle.mark_as_complete();
            return Ok(());
        }

        self.blob_event_processor_impl
            .process_event_impl(&self.node, event_handle, blob_event, checkpoint_position)
            .await
    }

    /// Waits for all events to be processed in the background processors.
    pub async fn wait_for_all_events_to_be_processed(&self) {
        match &self.blob_event_processor_impl {
            BlobEventProcessorImpl::SequentialProcessor { .. } => {
                // When using sequential processor, all events are processed in the same thread,
                // so there are no pending events to wait for.
            }
            BlobEventProcessorImpl::BackgroundProcessors {
                background_per_processor_pending_event_count,
                ..
            } => {
                // Check if any of the background processors still have pending events.
                while background_per_processor_pending_event_count
                    .iter()
                    .any(|c| c.get() > 0)
                {
                    tokio::time::sleep(PENDING_EVENTS_POLL_INTERVAL).await;
                }
            }
        }
    }

    /// Triggers graceful shutdown of all background event processors.
    /// This signals all processors to stop processing new events.
    pub fn shutdown(&self) {
        match &self.blob_event_processor_impl {
            BlobEventProcessorImpl::BackgroundProcessors { cancel_token, .. } => {
                tracing::info!("triggering shutdown of background event processors");
                cancel_token.cancel();
            }
            BlobEventProcessorImpl::SequentialProcessor { .. } => {
                // Sequential processor runs in the caller's context and doesn't need
                // separate shutdown coordination.
            }
        }
    }

    /// Waits for all background event processors to finish after shutdown is triggered.
    /// This method should be called after `shutdown()` to ensure all processors have exited.
    pub async fn wait_for_shutdown(&self) {
        match &self.blob_event_processor_impl {
            BlobEventProcessorImpl::BackgroundProcessors {
                background_processors,
                ..
            } => {
                // Take the join handles out of the mutex
                let handles = background_processors
                    .lock()
                    .expect("background_processors mutex poisoned")
                    .take();

                if let Some(handles) = handles {
                    tracing::info!(
                        num_processors = handles.len(),
                        "waiting for background event processors to finish"
                    );

                    // Wait for all background processors to finish
                    for handle in handles {
                        if let Err(error) = handle.await {
                            tracing::error!(?error, "background event processor task panicked");
                        }
                    }

                    tracing::info!("all background event processors have finished");
                } else {
                    tracing::warn!("wait_for_shutdown called multiple times");
                }
            }
            BlobEventProcessorImpl::SequentialProcessor { .. } => {
                // Sequential processor runs in the caller's context, nothing to wait for.
            }
        }
    }
}
