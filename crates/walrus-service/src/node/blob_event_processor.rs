// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use tokio::{
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};
use walrus_sui::types::{BlobCertified, BlobDeleted, BlobEvent, InvalidBlobId};
use walrus_utils::metrics::monitored_scope;

use super::{
    NodeStatus,
    StorageNodeInner,
    blob_sync::BlobSyncHandler,
    metrics,
    system_events::EventHandle,
};
use crate::node::{
    storage::blob_info::{BlobInfoApi, CertifiedBlobInfoApi},
    system_events::CompletableHandle,
};

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

    async fn run(&mut self) {
        // TODO: add metrics to measure events stay in the channel (queue length).
        while let Some((event_handle, blob_event)) = self.event_receiver.recv().await {
            if let Err(e) = self.process_event(event_handle, blob_event).await {
                tracing::error!("error processing blob event: {}", e);
            }
        }
    }

    async fn process_event(
        &self,
        event_handle: EventHandle,
        blob_event: BlobEvent,
    ) -> anyhow::Result<()> {
        // TODO: what should happen to the processor if this returns error?
        match blob_event {
            BlobEvent::Certified(event) => {
                monitored_scope::monitored_scope("ProcessEvent::BlobEvent::Certified");
                self.process_blob_certified_event(event_handle, event)
                    .await?;
            }
            BlobEvent::Deleted(event) => {
                monitored_scope::monitored_scope("ProcessEvent::BlobEvent::Deleted");
                self.process_blob_deleted_event(event_handle, event).await?;
            }
            BlobEvent::InvalidBlobID(event) => {
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
            || self.node.storage.node_status()? == NodeStatus::RecoveryCatchUp
            || self
                .node
                .is_stored_at_all_shards_at_epoch(&event.blob_id, self.node.current_event_epoch())
                .await?
        {
            event_handle.mark_as_complete();

            walrus_utils::with_label!(histogram_set, metrics::STATUS_SKIPPED)
                .observe(start.elapsed().as_secs_f64());

            return Ok(());
        }

        // Slivers and (possibly) metadata are not stored, so initiate blob sync.
        self.blob_sync_handler
            .start_sync(event.blob_id, event.epoch, Some(event_handle))
            .await?;

        walrus_utils::with_label!(histogram_set, metrics::STATUS_QUEUED)
            .observe(start.elapsed().as_secs_f64());

        Ok(())
    }

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
                tracing::debug!("deleting data for deleted blob");
                // TODO (WAL-201): Actually delete blob data.
            }
        } else {
            tracing::warn!(
                walrus.blob_id = %blob_id,
                "handling `BlobDeleted` event for an untracked blob"
            );
        }

        event_handle.mark_as_complete();

        Ok(())
    }

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

#[derive(Debug, Clone)]
pub struct BlobEventProcessor {
    node: Arc<StorageNodeInner>,
    background_processor_senders: Vec<UnboundedSender<(EventHandle, BlobEvent)>>,
    _background_processors: Vec<Arc<JoinHandle<()>>>,
}

impl BlobEventProcessor {
    pub fn new(node: Arc<StorageNodeInner>, blob_sync_handler: Arc<BlobSyncHandler>) -> Self {
        let num_workers = 4;

        let mut senders = Vec::with_capacity(num_workers);
        let mut workers = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            let (tx, rx) = mpsc::unbounded_channel();
            senders.push(tx);
            let mut background_processor =
                BackgroundEventProcessor::new(node.clone(), blob_sync_handler.clone(), rx);
            workers.push(Arc::new(tokio::spawn(async move {
                background_processor.run().await;
            })));
        }

        Self {
            node,
            background_processor_senders: senders,
            _background_processors: workers,
        }
    }

    pub async fn process_event(
        &self,
        event_handle: EventHandle,
        blob_event: BlobEvent,
    ) -> anyhow::Result<()> {
        self.node
            .storage
            .update_blob_info(event_handle.index(), &blob_event)?;

        if let BlobEvent::Registered(_) = &blob_event {
            monitored_scope::monitored_scope("ProcessEvent::BlobEvent::Registered");
            event_handle.mark_as_complete();
        } else {
            let processor_index = blob_event.blob_id().first_two_bytes() as usize
                % self.background_processor_senders.len();
            self.background_processor_senders[processor_index]
                .send((event_handle, blob_event))
                .map_err(|e| {
                    anyhow::anyhow!("failed to send event to background processor: {}", e)
                })?;
        }
        Ok(())
    }
}
