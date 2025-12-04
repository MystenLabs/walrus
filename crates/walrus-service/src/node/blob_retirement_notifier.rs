// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tokio::sync::{Notify, futures::Notified};
use walrus_core::BlobId;
use walrus_utils::metrics::monitored_scope;

use super::StorageNodeInner;
use crate::node::metrics::NodeMetricSet;

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
    metrics: Arc<NodeMetricSet>,
}

impl BlobRetirementNotifier {
    /// Create a new BlobRetirementNotifier.
    pub fn new(metrics: Arc<NodeMetricSet>) -> Self {
        Self {
            registered_blobs: Arc::new(Mutex::new(HashMap::new())),
            metrics,
        }
    }

    /// Acquire a BlobRetirementNotify for a blob.
    fn acquire_blob_retirement_notify(&self, blob_id: &BlobId) -> BlobRetirementNotify {
        let mut registered_blobs = self
            .registered_blobs
            .lock()
            .expect("mutex should not be poisoned");

        // Note that every call to acquire_blob_retirement_notify creates a new BlobRetirementNotify
        // reference (clone() is always called even for the first time a blob is registered).
        let notify = registered_blobs
            .entry(*blob_id)
            .or_insert_with(|| BlobRetirementNotify::new(*blob_id, Arc::new(self.clone())))
            .clone();
        self.metrics
            .blob_retirement_notifier_registered_blobs
            .set(i64::try_from(registered_blobs.len()).unwrap_or(i64::MAX));
        notify
    }

    /// Notify all BlobRetirementNotify for a blob.
    pub fn notify_blob_retirement(&self, blob_id: &BlobId) {
        let registered_blobs = self
            .registered_blobs
            .lock()
            .expect("mutex should not be poisoned");
        if let Some(notify) = registered_blobs.get(blob_id) {
            tracing::debug!(%blob_id, "notify blob retirement");
            notify.notify_waiters();
        };
    }

    /// Notify all BlobRetirementNotify for all blobs.
    /// This is used when epoch changes.
    pub fn epoch_change_notify_all_pending_blob_retirement(
        &self,
        node: Arc<StorageNodeInner>,
    ) -> anyhow::Result<()> {
        let _scope = monitored_scope::monitored_scope("EpochChange::NotifyRetiredBlobs");
        let mut registered_blobs = self
            .registered_blobs
            .lock()
            .expect("mutex should not be poisoned");
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

    /// Cleanup the blob retirement notify if it is the only remaining reference.
    fn cleanup_blob_retirement_notify(&self, blob_id: &BlobId) {
        let mut registered_blobs = self
            .registered_blobs
            .lock()
            .expect("mutex should not be poisoned");

        if let Some(notify) = registered_blobs.get(blob_id) {
            let current_count = *notify
                .ref_count
                .lock()
                .expect("mutex should not be poisoned");

            // If the ref count is 1, there is no active notify acquired from
            // acquire_blob_retirement_notify (every acquire_blob_retirement_notify call creates
            // a new BlobRetirementNotify reference). This is the original reference created when
            // we first register the blob in BlobRetirementNotifier. We can safely remove it from
            // the HashMap.
            if current_count == 1 {
                registered_blobs.remove(blob_id);
                self.metrics
                    .blob_retirement_notifier_registered_blobs
                    .set(i64::try_from(registered_blobs.len()).unwrap_or(i64::MAX));
            }
        } else {
            tracing::debug!(
                %blob_id,
                "cleanup_blob_retirement_notify: blob not found in registered_blobs",
            );
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
    fn notified(&self) -> Notified<'_> {
        self.notify.notified()
    }

    /// Notify all waiters.
    fn notify_waiters(&self) {
        self.notify.notify_waiters();
    }
}

/// Clone BlobRetirementNotify will increase the ref count.
impl Clone for BlobRetirementNotify {
    fn clone(&self) -> Self {
        let mut ref_count = self.ref_count.lock().expect("mutex should not be poisoned");
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
            let mut ref_count = self.ref_count.lock().expect("mutex should not be poisoned");
            tracing::trace!(
                "BlobRetirementNotifier drop NotifyWrapper for blob {} ref count: {}",
                self.blob_id,
                *ref_count
            );

            if *ref_count == 0 {
                // This should not happen since the ref count should be at least 1 when there is
                // BlobRetirementNotify instance.
                tracing::error!(
                    %self.blob_id,
                    "BlobRetirementNotifier drop NotifyWrapper: ref count is 0",
                );
            }

            *ref_count = ref_count.checked_sub(1).unwrap_or(0);
            *ref_count
        };

        // If ref count is 1, this is the only remaining reference in the HashMap.
        // Cleanup the blob retirement notify if it is the only remaining reference.
        if new_ref_count == 1 {
            self.node_wide_blob_retirement_notifier
                .cleanup_blob_retirement_notify(&self.blob_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::mpsc, thread, time::Duration};

    use rand::{Rng, prelude::Distribution};
    use tokio::{select, time::timeout};
    use walrus_core::test_utils::random_blob_id;
    use walrus_utils::metrics::Registry;

    use super::*;

    /// Test that BlobRetirementNotifier can notify one blob retirement.
    #[tokio::test]
    async fn test_blob_retirement_notification() {
        let notifier =
            BlobRetirementNotifier::new(Arc::new(NodeMetricSet::new(&Registry::default())));
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
        assert!(
            timeout(Duration::from_secs(1), wait_handle)
                .await
                .unwrap()
                .is_ok()
        );

        // The second task should still be waiting
        assert!(!wait_handle2.is_finished());
        assert!(
            notifier
                .registered_blobs
                .lock()
                .expect("mutex should not be poisoned")
                .len()
                == 1
        );

        // Notify another unrelated blob, the second task should still be waiting
        notifier.notify_blob_retirement(&random_blob_id());
        assert!(!wait_handle2.is_finished());
        assert!(
            notifier
                .registered_blobs
                .lock()
                .expect("mutex should not be poisoned")
                .len()
                == 1
        );
    }

    /// Test that BlobRetirementNotifier can notify multiple blob retirement.
    #[tokio::test]
    async fn test_multiple_waiters() {
        let notifier =
            BlobRetirementNotifier::new(Arc::new(NodeMetricSet::new(&Registry::default())));
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
        assert!(
            timeout(Duration::from_secs(1), wait_handle1)
                .await
                .unwrap()
                .is_ok()
        );
        assert!(
            timeout(Duration::from_secs(1), wait_handle2)
                .await
                .unwrap()
                .is_ok()
        );
        assert!(
            timeout(Duration::from_secs(1), wait_notified_3)
                .await
                .is_ok()
        );
    }

    /// Test dropping BlobRetirementNotify will decrease the ref count, and remove from
    /// BlobRetirementNotifier when ref count is 0.
    #[tokio::test]
    async fn test_multiple_waiters_drop() {
        let notifier =
            BlobRetirementNotifier::new(Arc::new(NodeMetricSet::new(&Registry::default())));
        let blob_id = random_blob_id();
        let blob_id2 = random_blob_id();

        let notify = notifier.acquire_blob_retirement_notify(&blob_id);
        let notify1 = notify.clone();
        let notify2 = notifier.acquire_blob_retirement_notify(&blob_id);
        let notify3 = notifier.acquire_blob_retirement_notify(&blob_id2);

        assert!(
            notifier
                .registered_blobs
                .lock()
                .expect("mutex should not be poisoned")
                .len()
                == 2
        );
        drop(notify);
        assert!(
            notifier
                .registered_blobs
                .lock()
                .expect("mutex should not be poisoned")
                .len()
                == 2
        );
        drop(notify1);
        assert!(
            notifier
                .registered_blobs
                .lock()
                .expect("mutex should not be poisoned")
                .len()
                == 2
        );
        drop(notify2);
        assert!(
            notifier
                .registered_blobs
                .lock()
                .expect("mutex should not be poisoned")
                .len()
                == 1
        );

        // Notify the second blob, the ref count should be 0, and the notify should be removed from
        // BlobRetirementNotifier.
        let notified_3 = notify3.notified();
        notifier.notify_blob_retirement(&blob_id2);
        assert!(timeout(Duration::from_secs(1), notified_3).await.is_ok());
        drop(notify3);
        assert!(
            notifier
                .registered_blobs
                .lock()
                .expect("mutex should not be poisoned")
                .is_empty()
        );
    }

    /// Test that BlobRetirementNotifier can handle a large number of concurrent acquire and drop
    /// operations without deadlocking or panicking.
    ///
    /// This test uses a thread-based timeout mechanism to detect deadlocks. The test spawns
    /// multiple concurrent tasks in a separate thread, and if they don't complete within the
    /// timeout, the main test thread will panic and terminate the test, even if worker threads
    /// are deadlocked on mutexes.
    #[test]
    fn test_concurrent_acquire_and_notify() {
        let _ = tracing_subscriber::fmt::try_init();

        const TEST_TIMEOUT: Duration = Duration::from_secs(5);
        let (tx, rx) = mpsc::channel();

        // Spawn test in separate thread to enable timeout-based termination
        thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(10)
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(test_concurrent_acquire_and_notify_impl());
            let _ = tx.send(());
        });

        // Wait for completion or timeout
        rx.recv_timeout(TEST_TIMEOUT).unwrap_or_else(|_| {
            panic!(
                "Test timed out after {:?} - possible deadlock",
                TEST_TIMEOUT
            )
        });
    }

    /// Helper function that runs the actual concurrent test logic.
    /// This is called from a separate thread to enable timeout-based termination.
    async fn test_concurrent_acquire_and_notify_impl() {
        const NUM_TASKS: usize = 500;
        const TASK_TIMEOUT: Duration = Duration::from_secs(1);

        let notifier =
            BlobRetirementNotifier::new(Arc::new(NodeMetricSet::new(&Registry::default())));
        let blob_id = random_blob_id();

        // Spawn concurrent tasks that acquire and drop notifications
        let handles: Vec<_> = (0..NUM_TASKS)
            .map(|_| {
                let notifier = notifier.clone();
                tokio::spawn(async move {
                    let notify = notifier.acquire_blob_retirement_notify(&blob_id);
                    drop(notify);
                })
            })
            .collect();

        tracing::info!("spawned {} concurrent tasks", NUM_TASKS);

        // Wait for all tasks to complete
        let result = timeout(TASK_TIMEOUT, async {
            for handle in handles {
                handle.await.expect("task should not panic");
            }
        })
        .await;

        match result {
            Ok(_) => tracing::info!("all tasks completed successfully"),
            Err(_) => panic!(
                "tasks timed out after {:?} - possible deadlock",
                TASK_TIMEOUT
            ),
        }
    }

    /// Randomized test that concurrently calls acquire, notify, and drop operations
    /// to detect deadlocks and memory leaks under realistic usage patterns.
    ///
    /// This test randomly interleaves three operations:
    /// - acquire_blob_retirement_notify (and drop)
    /// - notify_blob_retirement
    /// - Both operations combined
    ///
    /// The randomization helps expose race conditions and deadlocks that might not
    /// appear in deterministic tests.
    #[test]
    fn test_randomized_concurrent_blob_retirement_operations() {
        let _ = tracing_subscriber::fmt::try_init();

        const TEST_TIMEOUT: Duration = Duration::from_secs(10);
        let (tx, rx) = mpsc::channel();

        // Spawn test in separate thread to enable timeout-based termination
        thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(10)
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(test_randomized_concurrent_blob_retirement_operations_impl());
            let _ = tx.send(());
        });

        // Wait for completion or timeout
        rx.recv_timeout(TEST_TIMEOUT).unwrap_or_else(|_| {
            panic!(
                "Test timed out after {:?} - possible deadlock",
                TEST_TIMEOUT
            )
        });
    }

    enum TestOperation {
        AcquireAndDrop,
        NotifyExpired,
        AcquireAndHoldAndDrop,
    }
    impl Distribution<TestOperation> for rand::distributions::Standard {
        fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> TestOperation {
            match rng.gen_range(0..10) {
                0 => TestOperation::NotifyExpired,             // 10% expiration
                1..=3 => TestOperation::AcquireAndDrop,        // 30% acquire and drop
                4..=9 => TestOperation::AcquireAndHoldAndDrop, // 60% acquire and hold and drop
                _ => unreachable!(),
            }
        }
    }

    /// Helper function that runs the randomized test logic.
    async fn test_randomized_concurrent_blob_retirement_operations_impl() {
        const NUM_BLOBS: usize = 10;
        const NUM_OPERATIONS: usize = 1000;
        const TASK_TIMEOUT: Duration = Duration::from_secs(5);

        let notifier =
            BlobRetirementNotifier::new(Arc::new(NodeMetricSet::new(&Registry::default())));
        let blob_ids: Vec<_> = (0..NUM_BLOBS).map(|_| random_blob_id()).collect();

        tracing::info!(
            "spawning {} randomized operations across {} blobs",
            NUM_OPERATIONS,
            NUM_BLOBS
        );

        // Spawn tasks that perform random operations
        let handles: Vec<_> = (0..NUM_OPERATIONS)
            .map(|_| {
                let notifier = notifier.clone();
                let blob_ids = blob_ids.clone();
                tokio::spawn(async move {
                    let (blob_id, operation) = {
                        let mut rng = rand::thread_rng();
                        (
                            blob_ids[rng.gen_range(0..blob_ids.len())],
                            // Randomly select an operation.
                            rand::random::<TestOperation>(),
                        )
                    };

                    match operation {
                        TestOperation::AcquireAndDrop => {
                            // Operation 1: Acquire and immediately drop
                            let _notify = notifier.acquire_blob_retirement_notify(&blob_id);
                            // Notify should be dropped here.
                            tokio::task::yield_now().await;
                            // Notify should be dropped here.
                        }
                        TestOperation::NotifyExpired => {
                            // Operation 2: Notify retirement
                            notifier.notify_blob_retirement(&blob_id);
                        }
                        TestOperation::AcquireAndHoldAndDrop => {
                            // Operation 3: Acquire, hold briefly, then drop
                            // This case simulates execute_with_retirement_check without the blob
                            // certification check.
                            let notify = notifier.acquire_blob_retirement_notify(&blob_id);
                            select! {
                                _ = notify.notified() => (),
                                _ = tokio::time::sleep(Duration::from_millis(5)) => (),
                            }
                            // Notify should be dropped here.
                        }
                    }
                })
            })
            .collect();

        // Wait for all tasks to complete
        let result = timeout(TASK_TIMEOUT, async {
            for handle in handles {
                handle.await.expect("task should not panic");
            }
        })
        .await;

        match result {
            Ok(_) => tracing::info!("all randomized operations completed successfully"),
            Err(_) => panic!(
                "operations timed out after {:?} - possible deadlock",
                TASK_TIMEOUT
            ),
        }

        // Verify no memory leaks: there should be no remaining notifications registered.
        let registered_count = notifier
            .registered_blobs
            .lock()
            .expect("mutex should not be poisoned")
            .len();

        assert!(
            registered_count == 0,
            "possible memory leak: {} notifications registered",
            registered_count
        );
    }
}
