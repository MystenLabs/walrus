// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use walrus_core::BlobId;
use walrus_utils::metrics::monitored_scope;

use super::{StorageNodeInner, ref_counted_notify_map::*};
use crate::node::metrics::NodeMetricSet;

type BlobRetirementNotify = RefCountedNotify<BlobId>;

/// The result of an operation with a retirement check.
#[derive(Debug)]
pub enum ExecutionResultWithRetirementCheck<T> {
    /// The operation was executed successfully.
    Executed(T),
    /// The blob has retired. The operation may or may not have been executed.
    BlobRetired,
}

/// BlobRetirementNotifier is a wrapper around Notify to notify blob expiration, deletion, or
/// invalidation.
/// Caller acquires a BlobRetirementNotify and wait for notification.
/// When a blob expires, BlobRetirementNotifier will notify all BlobRetirementNotify.
#[derive(Clone, Debug)]
pub struct BlobRetirementNotifier {
    registered_blobs: RefCountedNotifyMap<BlobId>,
}

impl BlobRetirementNotifier {
    /// Create a new BlobRetirementNotifier.
    pub fn new(_metrics: Arc<NodeMetricSet>) -> Self {
        Self {
            registered_blobs: RefCountedNotifyMap::new(),
        }
    }

    /// Acquire a BlobRetirementNotify for a blob.
    fn acquire_blob_retirement_notify(&self, blob_id: &BlobId) -> BlobRetirementNotify {
        self.registered_blobs.acquire(blob_id)
    }

    /// Notify all BlobRetirementNotify for a blob.
    pub fn notify_blob_retirement(&self, blob_id: &BlobId) {
        if self.registered_blobs.notify(blob_id) {
            tracing::debug!(%blob_id, "notify blob retirement");
        }
    }

    /// Notify all BlobRetirementNotify for all blobs.
    /// This is used when epoch changes.
    pub fn epoch_change_notify_all_pending_blob_retirement(
        &self,
        node: Arc<StorageNodeInner>,
    ) -> anyhow::Result<()> {
        let _scope = monitored_scope::monitored_scope("EpochChange::NotifyRetiredBlobs");

        let blob_ids = self.registered_blobs.keys();

        let mut notify_blob_ids = Vec::new();
        for blob_id in blob_ids {
            if !node.is_blob_certified(&blob_id)? {
                tracing::debug!(%blob_id, "epoch change notify blob retirement");
                notify_blob_ids.push(blob_id);
            }
        }

        for notify in self.registered_blobs.take_many(notify_blob_ids) {
            notify.notify_waiters();
        }

        Ok(())
    }

    /// Execute an operation with a retirement check.
    ///
    /// If the blob has retired, it does not wait for the operation to complete, and returns
    /// `BlobRetired`.
    ///
    /// When the operation is executed, the execution result including any error is wrapped inside
    /// `Executed`.
    pub async fn execute_with_retirement_check<T, E, Fut>(
        &self,
        node: &Arc<StorageNodeInner>,
        blob_id: BlobId,
        operation: impl FnOnce() -> Fut,
    ) -> Result<ExecutionResultWithRetirementCheck<Result<T, E>>, anyhow::Error>
    where
        Fut: std::future::Future<Output = Result<T, E>>,
    {
        let blob_retirement_notify = self.acquire_blob_retirement_notify(&blob_id);
        let notified = blob_retirement_notify.notified();

        // Return `BlobRetired` early since the blob's certification status may have changed since
        // the call.
        // Note that the above `notified` will be awakened by the next call to notify_waiters, even
        // though the `notified` is not polled yet.
        if !node.is_blob_certified(&blob_id)? {
            return Ok(ExecutionResultWithRetirementCheck::BlobRetired);
        }

        tokio::select! {
            _ = notified => Ok(ExecutionResultWithRetirementCheck::BlobRetired),
            result = operation() => Ok(ExecutionResultWithRetirementCheck::Executed(result)),
        }
    }
}
