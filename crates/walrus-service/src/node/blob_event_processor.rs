// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{num::NonZeroUsize, sync::Arc};

use sui_macros::fail_point_async;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use walrus_core::BlobId;
use walrus_sui::types::{BlobCertified, BlobDeleted, BlobEvent, BlobRegistered, InvalidBlobId};
use walrus_utils::metrics::monitored_scope;

use self::pending_events::{PendingEventCounter, PendingEventGuard};
use super::{StorageNodeInner, blob_sync::BlobSyncHandler, metrics, system_events::EventHandle};
use crate::{
    event::events::CheckpointEventPosition,
    node::{
        storage::blob_info::{BlobInfoApi, CertifiedBlobInfoApi},
        system_events::CompletableHandle,
    },
};

pub mod pending_events;

/// Work handled by the background processor. Flushing shares the same queue as event
/// processing so that all operations for a blob are serialized.
#[derive(Debug)]
enum BackgroundTask {
    ProcessEvent {
        event_handle: EventHandle,
        blob_event: BlobEvent,
        checkpoint_position: CheckpointEventPosition,
    },
    FlushPendingCaches {
        blob_id: BlobId,
    },
}

/// Wrapper for background work that includes a pending event guard. When this struct is dropped,
/// the guard automatically decrements the counter.
#[derive(Debug)]
struct TrackedBackgroundTask {
    task: BackgroundTask,
    _guard: PendingEventGuard,
}

/// Background event processor that processes blob events in the background. It processes events
/// sequentially based on the order of events in the channel.
#[derive(Debug)]
struct BackgroundEventProcessor {
    node: Arc<StorageNodeInner>,
    blob_sync_handler: Arc<BlobSyncHandler>,
    worker_index: usize,
}

impl BackgroundEventProcessor {
    fn new(
        node: Arc<StorageNodeInner>,
        blob_sync_handler: Arc<BlobSyncHandler>,
        worker_index: usize,
    ) -> Self {
        Self {
            node,
            blob_sync_handler,
            worker_index,
        }
    }

    /// Runs the background event processor.
    async fn run_background_event_processor(
        &mut self,
        mut event_receiver: UnboundedReceiver<TrackedBackgroundTask>,
    ) {
        while let Some(tracked_task) = event_receiver.recv().await {
            walrus_utils::with_label!(
                self.node.metrics.pending_processing_blob_event_in_queue,
                &self.worker_index.to_string()
            )
            .dec();

            // The guard will automatically decrement the counter when dropped
            if let Err(error) = self.process_task(tracked_task.task).await {
                tracing::error!(?error, "error processing blob event");

                // By breaking here we cause the event_receiver to be dropped, and thus for any
                // subsequent event send attempts (to this processor) to fail. Eventually the
                // error from that failure will bubble up and cause the node to shut down.
                break;
            }
        }
    }

    async fn process_task(&self, task: BackgroundTask) -> anyhow::Result<()> {
        match task {
            BackgroundTask::ProcessEvent {
                event_handle,
                blob_event,
                checkpoint_position,
            } => {
                self.process_event(event_handle, blob_event, checkpoint_position)
                    .await?
            }
            BackgroundTask::FlushPendingCaches { blob_id } => {
                // Keep the worker busy until the flush completes so later events for this blob
                // (which are routed to the same worker) never observe stale pending data.
                self.node.flush_pending_caches_with_logging(blob_id).await
            }
        }

        Ok(())
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
        let current_committee_epoch = self.node.current_committee_epoch();
        tracing::Span::current().record("walrus.epoch", current_committee_epoch);

        if let Some(blob_info) = self.node.storage.get_blob_info(&blob_id)? {
            if !blob_info.is_certified(current_committee_epoch) {
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

/// Blob event processor that processes blob events.
#[derive(Debug, Clone)]
pub struct BlobEventProcessor {
    node: Arc<StorageNodeInner>,
    background_processor_senders: Vec<UnboundedSender<TrackedBackgroundTask>>,
    // The number of events pending over all background processors, shared with the corresponding
    // `BackgroundEventProcessor`. Per-worker counters avoid contention on a single atomic when
    // tracking pending events.
    background_pending_event_count: PendingEventCounter,
}

impl BlobEventProcessor {
    fn dispatch_task(&self, blob_id: BlobId, task: BackgroundTask) -> anyhow::Result<()> {
        // We send the event to one of the workers to process in parallel.
        // Note that in order to remain sequential processing for the same BlobID, we always
        // send events for the same BlobID to the same worker.
        // Currently the number of workers is fixed through the lifetime of the node. But in
        // case the node want to dynamically adjust the worker, we need to be careful to not
        // break this requirement.
        let processor_index =
            blob_id.first_two_bytes() as usize % self.background_processor_senders.len();

        walrus_utils::with_label!(
            self.node.metrics.pending_processing_blob_event_in_queue,
            &processor_index.to_string()
        )
        .inc();

        // Wrap the task with the guard to track the pending event count
        // and send it to the corresponding worker.
        let tracked_task = TrackedBackgroundTask {
            task,
            _guard: self
                .background_pending_event_count
                .track_event(self.node.metrics.clone()),
        };
        self.background_processor_senders[processor_index]
            .send(tracked_task)
            .map_err(|e| anyhow::anyhow!("failed to send task to background processor: {}", e))?;

        Ok(())
    }

    pub fn new(
        node: Arc<StorageNodeInner>,
        blob_sync_handler: Arc<BlobSyncHandler>,
        num_workers: NonZeroUsize,
        pending_event_counter: PendingEventCounter,
    ) -> Self {
        let num_workers = num_workers.get();
        let mut background_processor_senders = Vec::with_capacity(num_workers);

        for worker_index in 0..num_workers {
            let (tx, rx) = mpsc::unbounded_channel();
            background_processor_senders.push(tx);
            let mut background_processor = BackgroundEventProcessor::new(
                node.clone(),
                blob_sync_handler.clone(),
                worker_index,
            );

            // Note that we drop the JoinHandle since the rest of the system will terminate if it
            // cannot send to `tx`.
            tokio::spawn({
                async move {
                    background_processor
                        .run_background_event_processor(rx)
                        .await
                }
            });
        }

        Self {
            node,
            background_processor_senders,
            background_pending_event_count: pending_event_counter,
        }
    }

    /// Processes a blob event.
    pub(super) async fn process_event(
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

        if let BlobEvent::Registered(event) = &blob_event {
            self.handle_registered_event(event_handle, event).await?;
            return Ok(());
        }

        self.dispatch_task(
            blob_event.blob_id(),
            BackgroundTask::ProcessEvent {
                event_handle,
                blob_event,
                checkpoint_position,
            },
        )?;
        Ok(())
    }

    async fn handle_registered_event(
        &self,
        event_handle: EventHandle,
        event: &BlobRegistered,
    ) -> anyhow::Result<()> {
        // Registered event is marked as complete immediately. We need to process registered events
        // as fast as possible to catch up to the latest event in order to not miss blob sliver
        // uploads.
        //
        // If we want to do this in parallel, we shouldn't mix registered event processing with
        // certified event processing, as certified events take longer and can block following
        // registered events.
        let _scope = monitored_scope::monitored_scope("ProcessEvent::BlobEvent::Registered");

        self.node.notify_registration(&event.blob_id);

        if let Err(error) = self.dispatch_task(
            event.blob_id,
            BackgroundTask::FlushPendingCaches {
                blob_id: event.blob_id,
            },
        ) {
            tracing::error!(
                blob_id = %event.blob_id,
                ?error,
                "failed to enqueue cache flush after registration, flushing inline",
            );
            self.node
                .flush_pending_caches_with_logging(event.blob_id)
                .await;
        }

        event_handle.mark_as_complete();
        Ok(())
    }

    pub fn get_pending_event_counter(&self) -> PendingEventCounter {
        self.background_pending_event_count.clone()
    }
}
