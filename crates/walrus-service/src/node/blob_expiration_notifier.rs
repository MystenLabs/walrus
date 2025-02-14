// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tokio::sync::{futures::Notified, Notify};
use walrus_core::BlobId;

/// BlobExpirationNotifier is a wrapper around Notify to notify blob expiration.
/// Caller acquires a BlobExpirationNotify and wait for notification.
/// When a blob expires, BlobExpirationNotifier will notify all BlobExpirationNotify.
#[derive(Clone, Debug)]
pub struct BlobExpirationNotifier {
    pending_blob: Arc<Mutex<HashMap<BlobId, BlobExpirationNotify>>>,
}

impl BlobExpirationNotifier {
    /// Create a new BlobExpirationNotifier.
    pub fn new() -> Self {
        Self {
            pending_blob: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Acquire a BlobExpirationNotify for a blob.
    pub fn acquire_blob_expiration_notify(&self, blob_id: &BlobId) -> BlobExpirationNotify {
        let mut pending = self.pending_blob.lock().unwrap();
        pending
            .entry(*blob_id)
            .or_insert_with(|| BlobExpirationNotify::new(*blob_id, Arc::new(self.clone())))
            .clone()
    }

    /// Notify all BlobExpirationNotify for a blob.
    pub fn notify_blob_expiration(&self, blob_id: &BlobId) {
        let notify = {
            let mut pending = self.pending_blob.lock().unwrap();
            pending.remove(blob_id)
        };
        if let Some(notify) = notify {
            notify.notify_waiters();
        }
    }
}

/// BlobExpirationNotify is a wrapper around Notify to notify one blob expiration.
/// It is ref counted, and when ref count is 0, it will be removed from BlobExpirationNotifier.
#[derive(Debug)]
pub struct BlobExpirationNotify {
    blob_id: BlobId,
    notify: Arc<Notify>,
    ref_count: Arc<Mutex<usize>>,
    notifier: Arc<BlobExpirationNotifier>,
}

impl BlobExpirationNotify {
    /// Create a new BlobExpirationNotify.
    pub fn new(blob_id: BlobId, notifier: Arc<BlobExpirationNotifier>) -> Self {
        Self {
            blob_id,
            notify: Arc::new(Notify::new()),
            ref_count: Arc::new(Mutex::new(1)),
            notifier,
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

/// Clone BlobExpirationNotify will increase the ref count.
impl Clone for BlobExpirationNotify {
    fn clone(&self) -> Self {
        let mut ref_count = self.ref_count.lock().unwrap();
        *ref_count = ref_count.checked_add(1).unwrap_or(0);
        Self {
            notify: self.notify.clone(),
            ref_count: self.ref_count.clone(),
            notifier: self.notifier.clone(),
            blob_id: self.blob_id,
        }
    }
}

/// Drop BlobExpirationNotify will decrease the ref count.
/// When ref count is 0, it will be removed from BlobExpirationNotifier.
impl Drop for BlobExpirationNotify {
    fn drop(&mut self) {
        let new_ref_count = {
            let mut ref_count = self.ref_count.lock().unwrap();
            tracing::trace!(
                "BlobExpirationNotifier drop NotifyWrapper for blob {} ref count: {}",
                self.blob_id,
                *ref_count
            );
            *ref_count = ref_count.checked_sub(1).unwrap_or(0);
            *ref_count
        };

        // If ref count is 0, remove from BlobExpirationNotifier.
        // Note that the blob may have already being dropped after notify_waiters is called.
        if new_ref_count == 1 {
            self.notifier
                .pending_blob
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

    /// Test that BlobExpirationNotifier can notify one blob expiration.
    #[tokio::test]
    async fn test_blob_expiration_notification() {
        let notifier = BlobExpirationNotifier::new();
        let blob_id = random_blob_id();
        let blob_id2 = random_blob_id();

        // Spawn a task that waits for notification
        let notifier_clone = notifier.clone();
        let wait_handle = tokio::spawn(async move {
            let notify = notifier_clone.acquire_blob_expiration_notify(&blob_id);
            notify.notified().await;
        });

        let notifier_clone = notifier.clone();
        let wait_handle2 = tokio::spawn(async move {
            let notify = notifier_clone.acquire_blob_expiration_notify(&blob_id2);
            notify.notified().await;
        });

        // Small delay to ensure the waiting task is running
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Only notify the first blob
        notifier.notify_blob_expiration(&blob_id);

        // Verify that the waiting task completes
        assert!(timeout(Duration::from_secs(1), wait_handle)
            .await
            .unwrap()
            .is_ok());

        // The second task should still be waiting
        assert!(!wait_handle2.is_finished());
        assert!(notifier.pending_blob.lock().unwrap().len() == 1);

        // Notify another unrelatedblob, the second task should still be waiting
        notifier.notify_blob_expiration(&random_blob_id());
        assert!(!wait_handle2.is_finished());
        assert!(notifier.pending_blob.lock().unwrap().len() == 1);
    }

    /// Test that BlobExpirationNotifier can notify multiple blob expiration.
    #[tokio::test]
    async fn test_multiple_waiters() {
        let notifier = BlobExpirationNotifier::new();
        let blob_id = random_blob_id();

        // Spawn two tasks that wait for notification
        let notifier_clone1 = notifier.clone();
        let wait_handle1 = tokio::spawn(async move {
            let notify = notifier_clone1.acquire_blob_expiration_notify(&blob_id);
            notify.notified().await;
        });

        let notifier_clone2 = notifier.clone();
        let wait_handle2 = tokio::spawn(async move {
            let notify = notifier_clone2.acquire_blob_expiration_notify(&blob_id);
            notify.notified().await;
        });

        // Create a notified but do not poll it yet. This tests that even we poll the notified after
        // notify_blob_expiration is called, the notified will still be notified.
        let wait_notify_3 = notifier.acquire_blob_expiration_notify(&blob_id);
        let wait_notified_3 = wait_notify_3.notified();

        // Small delay to ensure waiting tasks are running
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Notify expiration
        notifier.notify_blob_expiration(&blob_id);

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

    /// Test dropping BlobExpirationNotify will decrease the ref count, and remove from
    /// BlobExpirationNotifier when ref count is 0.
    #[tokio::test]
    async fn test_multiple_waiters_drop() {
        let notifier = BlobExpirationNotifier::new();
        let blob_id = random_blob_id();
        let blob_id2 = random_blob_id();

        let notify = notifier.acquire_blob_expiration_notify(&blob_id);
        let notify1 = notify.clone();
        let notify2 = notifier.acquire_blob_expiration_notify(&blob_id);
        let notify3 = notifier.acquire_blob_expiration_notify(&blob_id2);

        assert!(notifier.pending_blob.lock().unwrap().len() == 2);
        drop(notify);
        assert!(notifier.pending_blob.lock().unwrap().len() == 2);
        drop(notify1);
        assert!(notifier.pending_blob.lock().unwrap().len() == 2);
        drop(notify2);
        assert!(notifier.pending_blob.lock().unwrap().len() == 1);

        // Notify the second blob, the ref count should be 0, and the notify should be removed from
        // BlobExpirationNotifier.
        let notified_3 = notify3.notified();
        notifier.notify_blob_expiration(&blob_id2);
        notifier.notify_blob_expiration(&blob_id2);
        assert!(notifier.pending_blob.lock().unwrap().len() == 0);
        assert!(timeout(Duration::from_secs(1), notified_3).await.is_ok());
        drop(notify3);
        assert!(notifier.pending_blob.lock().unwrap().len() == 0);
    }
}
