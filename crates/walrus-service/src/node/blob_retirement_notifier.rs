// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use mysten_metrics::monitored_scope;
use tokio::sync::{futures::Notified, Notify};
use walrus_core::BlobId;

use super::StorageNodeInner;

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
    // Blobs registered to be notified when the blob expires/gets deleted/gets invalidated.
    registered_blobs: Arc<Mutex<HashMap<BlobId, BlobRetirementNotify>>>,
}

impl BlobRetirementNotifier {
    /// Create a new BlobRetirementNotifier.
    pub fn new() -> Self {
        Self {
            registered_blobs: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Acquire a BlobRetirementNotify for a blob.
    pub fn acquire_blob_retirement_notify(&self, blob_id: &BlobId) -> BlobRetirementNotify {
        let mut registered_blobs = self.registered_blobs.lock().unwrap();
        registered_blobs
            .entry(*blob_id)
            .or_insert_with(|| BlobRetirementNotify::new(*blob_id, Arc::new(self.clone())))
            .clone()
    }

    /// Notify all BlobRetirementNotify for a blob.
    pub fn notify_blob_retirement(&self, blob_id: &BlobId) {
        let notify = {
            let mut registered_blobs = self.registered_blobs.lock().unwrap();
            registered_blobs.remove(blob_id)
        };
        if let Some(notify) = notify {
            tracing::debug!(%blob_id, "notify blob retirement");
            notify.notify_waiters();
        }
    }

    /// Notify all BlobRetirementNotify for all blobs.
    /// This is used when epoch changes.
    pub fn epoch_change_notify_all_pending_blob_retirement(
        &self,
        node: Arc<StorageNodeInner>,
    ) -> anyhow::Result<()> {
        let _scope = monitored_scope("EpochChange::NotifyRetiredBlobs");
        let mut registered_blobs = self.registered_blobs.lock().unwrap();
        for (blob_id, notify) in registered_blobs.iter_mut() {
            if !node.is_blob_certified(blob_id)? {
                tracing::debug!(%blob_id, "epoch change notify blob retirement");
                notify.notify_waiters();
            }
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

        if !node.is_blob_certified(&blob_id)? {
            return Ok(ExecutionResultWithRetirementCheck::BlobRetired);
        }

        tokio::select! {
            _ = notified => Ok(ExecutionResultWithRetirementCheck::BlobRetired),
            result = operation() => Ok(ExecutionResultWithRetirementCheck::Executed(result)),
        }
    }
}

/// BlobRetirementNotify is a wrapper around Notify to notify one blob expiration.
/// It is ref counted, and when ref count is 0, it will be removed from BlobRetirementNotifier.
#[derive(Debug)]
pub struct BlobRetirementNotify {
    /// The blob id of the registered notify.
    blob_id: BlobId,
    /// The notify to notify the blob retirement.
    notify: Arc<Notify>,
    /// The ref count of the notify. When ref count is 1 (the only ref is the one stored in the
    /// HashMap), it will be removed from BlobRetirementNotifier.
    ref_count: Arc<Mutex<usize>>,
    /// The notifier of the notify.
    node_wide_blob_retirement_notifier: Arc<BlobRetirementNotifier>,
}

impl BlobRetirementNotify {
    /// Create a new BlobRetirementNotify.
    pub fn new(blob_id: BlobId, notifier: Arc<BlobRetirementNotifier>) -> Self {
        Self {
            blob_id,
            notify: Arc::new(Notify::new()),
            ref_count: Arc::new(Mutex::new(1)),
            node_wide_blob_retirement_notifier: notifier,
        }
    }

    /// Wait for notification.
    /// Note that in order to be awakened, notified must be called before any future possible
    /// notify_waiters.
    pub fn notified(&self) -> Notified {
        self.notify.notified()
    }

    /// Notify all waiters.
    pub fn notify_waiters(&self) {
        self.notify.notify_waiters();
    }
}

/// Clone BlobRetirementNotify will increase the ref count.
impl Clone for BlobRetirementNotify {
    fn clone(&self) -> Self {
        let mut ref_count = self.ref_count.lock().unwrap();
        *ref_count = ref_count.checked_add(1).unwrap_or(0);
        Self {
            notify: self.notify.clone(),
            ref_count: self.ref_count.clone(),
            node_wide_blob_retirement_notifier: self.node_wide_blob_retirement_notifier.clone(),
            blob_id: self.blob_id,
        }
    }
}

/// Drop BlobRetirementNotify will decrease the ref count.
/// When ref count is 0, it will be removed from BlobRetirementNotifier.
impl Drop for BlobRetirementNotify {
    fn drop(&mut self) {
        let new_ref_count = {
            let mut ref_count = self.ref_count.lock().unwrap();
            tracing::trace!(
                "BlobRetirementNotifier drop NotifyWrapper for blob {} ref count: {}",
                self.blob_id,
                *ref_count
            );
            *ref_count = ref_count.checked_sub(1).unwrap_or(0);
            *ref_count
        };

        // If ref count is 1, remove from BlobRetirementNotifier. The only ref is the one stored in
        // the HashMap.
        // Note that the blob may have already being dropped after notify_waiters is called.
        if new_ref_count == 1 {
            self.node_wide_blob_retirement_notifier
                .registered_blobs
                .lock()
                .unwrap()
                .remove(&self.blob_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::{timeout, Duration};
    use walrus_core::test_utils::random_blob_id;

    use super::*;

    /// Test that BlobRetirementNotifier can notify one blob retirement.
    #[tokio::test]
    async fn test_blob_retirement_notification() {
        let notifier = BlobRetirementNotifier::new();
        let blob_id = random_blob_id();
        let blob_id2 = random_blob_id();

        // Spawn a task that waits for notification
        let notifier_clone = notifier.clone();
        let wait_handle = tokio::spawn(async move {
            let notify = notifier_clone.acquire_blob_retirement_notify(&blob_id);
            notify.notified().await;
        });

        let notifier_clone = notifier.clone();
        let wait_handle2 = tokio::spawn(async move {
            let notify = notifier_clone.acquire_blob_retirement_notify(&blob_id2);
            notify.notified().await;
        });

        // Small delay to ensure the waiting task is running
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Only notify the first blob
        notifier.notify_blob_retirement(&blob_id);

        // Verify that the waiting task completes
        assert!(timeout(Duration::from_secs(1), wait_handle)
            .await
            .unwrap()
            .is_ok());

        // The second task should still be waiting
        assert!(!wait_handle2.is_finished());
        assert!(notifier.registered_blobs.lock().unwrap().len() == 1);

        // Notify another unrelatedblob, the second task should still be waiting
        notifier.notify_blob_retirement(&random_blob_id());
        assert!(!wait_handle2.is_finished());
        assert!(notifier.registered_blobs.lock().unwrap().len() == 1);
    }

    /// Test that BlobRetirementNotifier can notify multiple blob retirement.
    #[tokio::test]
    async fn test_multiple_waiters() {
        let notifier = BlobRetirementNotifier::new();
        let blob_id = random_blob_id();

        // Spawn two tasks that wait for notification
        let notifier_clone1 = notifier.clone();
        let wait_handle1 = tokio::spawn(async move {
            let notify = notifier_clone1.acquire_blob_retirement_notify(&blob_id);
            notify.notified().await;
        });

        let notifier_clone2 = notifier.clone();
        let wait_handle2 = tokio::spawn(async move {
            let notify = notifier_clone2.acquire_blob_retirement_notify(&blob_id);
            notify.notified().await;
        });

        // Create a notified but do not poll it yet. This tests that even we poll the notified after
        // `notify_blob_retirement` is called, the notified will still be notified.
        let wait_notify_3 = notifier.acquire_blob_retirement_notify(&blob_id);
        let wait_notified_3 = wait_notify_3.notified();

        // Small delay to ensure waiting tasks are running
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Notify retirement
        notifier.notify_blob_retirement(&blob_id);

        // Verify that both waiting tasks complete
        assert!(timeout(Duration::from_secs(1), wait_handle1)
            .await
            .unwrap()
            .is_ok());
        assert!(timeout(Duration::from_secs(1), wait_handle2)
            .await
            .unwrap()
            .is_ok());
        assert!(timeout(Duration::from_secs(1), wait_notified_3)
            .await
            .is_ok());
    }

    /// Test dropping BlobRetirementNotify will decrease the ref count, and remove from
    /// BlobRetirementNotifier when ref count is 0.
    #[tokio::test]
    async fn test_multiple_waiters_drop() {
        let notifier = BlobRetirementNotifier::new();
        let blob_id = random_blob_id();
        let blob_id2 = random_blob_id();

        let notify = notifier.acquire_blob_retirement_notify(&blob_id);
        let notify1 = notify.clone();
        let notify2 = notifier.acquire_blob_retirement_notify(&blob_id);
        let notify3 = notifier.acquire_blob_retirement_notify(&blob_id2);

        assert!(notifier.registered_blobs.lock().unwrap().len() == 2);
        drop(notify);
        assert!(notifier.registered_blobs.lock().unwrap().len() == 2);
        drop(notify1);
        assert!(notifier.registered_blobs.lock().unwrap().len() == 2);
        drop(notify2);
        assert!(notifier.registered_blobs.lock().unwrap().len() == 1);

        // Notify the second blob, the ref count should be 0, and the notify should be removed from
        // BlobRetirementNotifier.
        let notified_3 = notify3.notified();
        notifier.notify_blob_retirement(&blob_id2);
        notifier.notify_blob_retirement(&blob_id2);
        assert!(notifier.registered_blobs.lock().unwrap().is_empty());
        assert!(timeout(Duration::from_secs(1), notified_3).await.is_ok());
        drop(notify3);
        assert!(notifier.registered_blobs.lock().unwrap().is_empty());
    }
}
