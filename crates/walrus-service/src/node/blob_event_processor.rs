// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use sui_macros::fail_point_async;
use tokio::{
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};
use walrus_sui::types::{BlobCertified, BlobDeleted, BlobEvent, InvalidBlobId};
use walrus_utils::metrics::monitored_scope;

use super::{StorageNodeInner, blob_sync::BlobSyncHandler, metrics, system_events::EventHandle};
use crate::node::{
    storage::blob_info::{BlobInfoApi, CertifiedBlobInfoApi},
    system_events::CompletableHandle,
};

/// Background event processor that processes blob events in the background. It processes events
/// sequentially based on the order of events in the channel.
#[derive(Debug)]
struct BackgroundEventProcessor {
    node: Arc<StorageNodeInner>,
    blob_sync_handler: Arc<BlobSyncHandler>,
    event_receiver: UnboundedReceiver<(EventHandle, BlobEvent)>,
}

impl BackgroundEventProcessor {
    fn new(
        node: Arc<StorageNodeInner>,
        blob_sync_handler: Arc<BlobSyncHandler>,
        event_receiver: UnboundedReceiver<(EventHandle, BlobEvent)>,
    ) -> Self {
        Self {
            node,
            blob_sync_handler,
            event_receiver,
        }
    }

    /// Runs the background event processor.
    async fn run(&mut self, worker_index: usize) {
        while let Some((event_handle, blob_event)) = self.event_receiver.recv().await {
            walrus_utils::with_label!(
                self.node.metrics.pending_processing_blob_event_in_queue,
                &worker_index.to_string()
            )
            .dec();

            if let Err(error) = self.process_event(event_handle, blob_event).await {
                // TODO(WAL-874): to keep the same behavior as before BackgroundEventProcessor, we
                // should propagate the error to the node and exit the process if necessary.
                tracing::error!(?error, "error processing blob event");
            }
        }
    }

    /// Processes a blob event.
    async fn process_event(
        &self,
        event_handle: EventHandle,
        blob_event: BlobEvent,
    ) -> anyhow::Result<()> {
        match blob_event {
            BlobEvent::Certified(event) => {
                let _scope = monitored_scope::monitored_scope("ProcessEvent::BlobEvent::Certified");
                self.process_blob_certified_event(event_handle, event)
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
    ) -> anyhow::Result<()> {
        let start = tokio::time::Instant::now();

        let histogram_set = self.node.metrics.recover_blob_duration_seconds.clone();

        if !self.node.is_blob_certified(&event.blob_id)?
            // For blob extension events, the original blob certified event should already recover
            // the entire blob, and we can skip the recovery.
            || event.is_extension
            || self.node.storage.node_status()?.is_catching_up()
            || self
                .node
                .is_stored_at_all_shards_at_epoch(
                    &event.blob_id,
                    self.node.current_event_epoch().await?,
                )
                .await?
        {
            event_handle.mark_as_complete();

            walrus_utils::with_label!(histogram_set, metrics::STATUS_SKIPPED)
                .observe(start.elapsed().as_secs_f64());

            return Ok(());
        }

        fail_point_async!("fail_point_process_blob_certified_event");

        // Slivers and (possibly) metadata are not stored, so initiate blob sync.
        self.blob_sync_handler
            .start_sync(event.blob_id, event.epoch, Some(event_handle))
            .await?;

        walrus_utils::with_label!(histogram_set, metrics::STATUS_QUEUED)
            .observe(start.elapsed().as_secs_f64());

        Ok(())
    }

    /// Processes a blob deleted event.
    #[tracing::instrument(skip_all)]
    async fn process_blob_deleted_event(
        &self,
        event_handle: EventHandle,
        event: BlobDeleted,
    ) -> anyhow::Result<()> {
        let blob_id = event.blob_id;

        if let Some(blob_info) = self.node.storage.get_blob_info(&blob_id)? {
            if !blob_info.is_certified(self.node.current_epoch()) {
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
            // We use the event's epoch for this check (as opposed to the current epoch) as
            // subsequent certify or delete events may update the `blob_info`; so we cannot remove
            // it even if it is no longer valid in the *current* epoch
            if !blob_info.is_registered(event.epoch) {
                tracing::debug!(walrus.blob_id = %blob_id, "deleting data for deleted blob");
                // TODO (WAL-201): Actually delete blob data.
            }
        } else if self
            .node
            .storage
            .node_status()?
            .is_catching_up_with_incomplete_history()
        {
            tracing::debug!(
                walrus.blob_id = %blob_id,
                "handling a `BlobDeleted` event for an untracked blob while catching up with \
                incomplete history; not deleting blob data"
            );
        } else {
            tracing::warn!(
                walrus.blob_id = %blob_id,
                "handling a `BlobDeleted` event for an untracked blob"
            );
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

/// Blob event processor that processes blob events. It can be configured to process events
/// sequentially or in parallel using background workers.
#[derive(Debug, Clone)]
pub struct BlobEventProcessor {
    node: Arc<StorageNodeInner>,

    // Background processors that process events in parallel.
    background_processor_senders: Vec<UnboundedSender<(EventHandle, BlobEvent)>>,
    _background_processors: Vec<Arc<JoinHandle<()>>>,

    // When there are no background workers, we use a sequential processor to process events using
    // this processor. This is to keep the same behavior as before BackgroundEventProcessor.
    // INVARIANT: sequential_processor must be Some if background_processor_senders is empty.
    sequential_processor: Option<Arc<BackgroundEventProcessor>>,
}

impl BlobEventProcessor {
    pub fn new(
        node: Arc<StorageNodeInner>,
        blob_sync_handler: Arc<BlobSyncHandler>,
        num_workers: usize,
    ) -> Self {
        let mut senders = Vec::with_capacity(num_workers);
        let mut workers = Vec::with_capacity(num_workers);
        for worker_index in 0..num_workers {
            let (tx, rx) = mpsc::unbounded_channel();
            senders.push(tx);
            let mut background_processor =
                BackgroundEventProcessor::new(node.clone(), blob_sync_handler.clone(), rx);
            // TODO(WAL-876): gracefully shut down the background processor when the node is
            // shutting down.
            workers.push(Arc::new(tokio::spawn(async move {
                background_processor.run(worker_index).await;
            })));
        }

        let sequential_processor = if num_workers == 0 {
            // Create a sequential processor to process events sequentially if no background workers
            // are configured.
            let (_tx, rx) = mpsc::unbounded_channel();
            Some(Arc::new(BackgroundEventProcessor::new(
                node.clone(),
                blob_sync_handler.clone(),
                rx,
            )))
        } else {
            None
        };

        Self {
            node,
            background_processor_senders: senders,
            _background_processors: workers,
            sequential_processor,
        }
    }

    /// Processes a blob event.
    pub async fn process_event(
        &self,
        event_handle: EventHandle,
        blob_event: BlobEvent,
    ) -> anyhow::Result<()> {
        // Update the blob info based on the event.
        // This processing must be sequential and cannot be parallelized, since there is logical
        // dependency between events.
        self.node
            .storage
            .update_blob_info(event_handle.index(), &blob_event)?;

        if let BlobEvent::Registered(event) = &blob_event {
            // Registered event is marked as complete immediately. We need to process registered
            // events as fast as possible to catch up to the latest event in order to not miss
            // blob sliver uploads.
            //
            // If we want to do this in parallel, we shouldn't mix registered event processing with
            // certified event processing, as certified events take longer and can block following
            // registered events.
            let _scope = monitored_scope::monitored_scope("ProcessEvent::BlobEvent::Registered");

            // Notify CachedBlobManager about the registration
            // This will trigger processing if data has already arrived
            if let Some(cached_manager) = self.node.cached_blob_manager().await {
                cached_manager.register(&blob_event).await;
                tracing::debug!(
                    blob_id = %event.blob_id,
                    "Notified CachedBlobManager about registration"
                );
            }

            event_handle.mark_as_complete();
            return Ok(());
        }

        // If there are no background workers, we use a sequential processor to process events
        // sequentially.
        if self.background_processor_senders.is_empty() {
            assert!(self.sequential_processor.is_some());
            self.sequential_processor
                .as_ref()
                .expect(
                    "sequential processor must be configured when no background \
                            workers are configured",
                )
                .process_event(event_handle, blob_event)
                .await?;
        } else {
            // We send the event to one of the workers to process in parallel.
            // Note that in order to remain sequential processing for the same BlobID, we always
            // send events for the same BlobID to the same worker.
            // Currently the number of workers is fixed through the lifetime of the node. But in
            // case the node want to dynamically adjust the worker, we need to be careful to not
            // break this requirement.
            let processor_index = blob_event.blob_id().first_two_bytes() as usize
                % self.background_processor_senders.len();

            walrus_utils::with_label!(
                self.node.metrics.pending_processing_blob_event_in_queue,
                &processor_index.to_string()
            )
            .inc();

            self.background_processor_senders[processor_index]
                .send((event_handle, blob_event))
                .map_err(|e| {
                    anyhow::anyhow!("failed to send event to background processor: {}", e)
                })?;
        }
        Ok(())
    }
}
