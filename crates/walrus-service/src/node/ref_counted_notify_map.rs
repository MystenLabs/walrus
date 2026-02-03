// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{HashMap, hash_map::Entry},
    fmt,
    hash::Hash,
    sync::{Arc, Mutex, Weak},
};

use tokio::sync::{Notify, futures::Notified};

#[derive(Clone)]
pub(crate) struct RefCountedNotifyMap<K>
where
    K: Clone + Eq + Hash,
{
    inner: Arc<RefCountedNotifyMapInner<K>>,
}

impl<K> fmt::Debug for RefCountedNotifyMap<K>
where
    K: Clone + Eq + Hash,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let len = self
            .inner
            .waiters
            .lock()
            .expect("mutex should not be poisoned")
            .len();
        f.debug_struct("RefCountedNotifyMap")
            .field("len", &len)
            .finish()
    }
}

impl<K> Default for RefCountedNotifyMap<K>
where
    K: Clone + Eq + Hash,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K> RefCountedNotifyMap<K>
where
    K: Clone + Eq + Hash,
{
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(RefCountedNotifyMapInner {
                waiters: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub(crate) fn acquire(&self, key: &K) -> RefCountedNotify<K> {
        let notify = {
            let mut waiters = self
                .inner
                .waiters
                .lock()
                .expect("mutex should not be poisoned");

            match waiters.entry(key.clone()) {
                Entry::Occupied(mut entry) => match entry.get().upgrade() {
                    Some(notify) => notify,
                    None => {
                        let notify = Arc::new(Notify::new());
                        entry.insert(Arc::downgrade(&notify));
                        notify
                    }
                },
                Entry::Vacant(entry) => {
                    let notify = Arc::new(Notify::new());
                    entry.insert(Arc::downgrade(&notify));
                    notify
                }
            }
        };

        RefCountedNotify {
            key: key.clone(),
            notify,
            map: self.clone(),
        }
    }

    /// Removes the entry for `key` (if present) and notifies any waiters.
    pub(crate) fn notify(&self, key: &K) -> bool {
        let notify = self.remove_notify(key);
        if let Some(notify) = notify {
            notify.notify_waiters();
            true
        } else {
            false
        }
    }

    pub(crate) fn remove_notify(&self, key: &K) -> Option<Arc<Notify>> {
        let mut waiters = self
            .inner
            .waiters
            .lock()
            .expect("mutex should not be poisoned");
        waiters.remove(key).and_then(|weak| weak.upgrade())
    }

    pub(crate) fn keys(&self) -> Vec<K> {
        let waiters = self
            .inner
            .waiters
            .lock()
            .expect("mutex should not be poisoned");
        waiters.keys().cloned().collect()
    }

    pub(crate) fn take_many(&self, keys: impl IntoIterator<Item = K>) -> Vec<Arc<Notify>> {
        let mut waiters = self
            .inner
            .waiters
            .lock()
            .expect("mutex should not be poisoned");
        keys.into_iter()
            .filter_map(|key| waiters.remove(&key).and_then(|weak| weak.upgrade()))
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.inner
            .waiters
            .lock()
            .expect("mutex should not be poisoned")
            .len()
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

struct RefCountedNotifyMapInner<K>
where
    K: Clone + Eq + Hash,
{
    waiters: Mutex<HashMap<K, Weak<Notify>>>,
}

#[derive(Clone, Debug)]
pub(crate) struct RefCountedNotify<K>
where
    K: Clone + Eq + Hash,
{
    key: K,
    notify: Arc<Notify>,
    map: RefCountedNotifyMap<K>,
}

impl<K> RefCountedNotify<K>
where
    K: Clone + Eq + Hash,
{
    pub(crate) fn notified(&self) -> Notified<'_> {
        self.notify.notified()
    }
}

impl<K> Drop for RefCountedNotify<K>
where
    K: Clone + Eq + Hash,
{
    fn drop(&mut self) {
        let mut waiters = self
            .map
            .inner
            .waiters
            .lock()
            .expect("mutex should not be poisoned");

        let Some(weak) = waiters.get(&self.key) else {
            return;
        };

        if weak.as_ptr() != Arc::as_ptr(&self.notify) {
            return;
        }

        if Arc::strong_count(&self.notify) == 1 {
            waiters.remove(&self.key);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::mpsc, thread, time::Duration};

    use rand::{Rng, prelude::Distribution};
    use tokio::{select, time::timeout};
    use walrus_core::{BlobId, test_utils::random_blob_id};

    use super::*;

    /// Test that RefCountedNotifyMap can notify one key without affecting others.
    #[tokio::test]
    async fn test_notify_one_key() {
        let map = RefCountedNotifyMap::<BlobId>::default();
        let blob_id = random_blob_id();
        let blob_id2 = random_blob_id();

        let map_clone = map.clone();
        let wait_handle = tokio::spawn(async move {
            let notify = map_clone.acquire(&blob_id);
            notify.notified().await;
        });

        let map_clone = map.clone();
        let wait_handle2 = tokio::spawn(async move {
            let notify = map_clone.acquire(&blob_id2);
            notify.notified().await;
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        // Only notify the first blob
        assert!(map.notify(&blob_id));

        assert!(
            timeout(Duration::from_secs(1), wait_handle)
                .await
                .unwrap()
                .is_ok()
        );

        // The second task should still be waiting, and the first entry should have been cleaned
        // up.
        assert!(!wait_handle2.is_finished());
        assert!(map.len() == 1);

        // Notify another unrelated blob, the second task should still be waiting.
        assert!(!map.notify(&random_blob_id()));
        assert!(!wait_handle2.is_finished());
        assert!(map.len() == 1);
    }

    /// Test that RefCountedNotifyMap can notify multiple waiters.
    #[tokio::test]
    async fn test_multiple_waiters() {
        let map = RefCountedNotifyMap::<BlobId>::default();
        let blob_id = random_blob_id();

        let map_clone1 = map.clone();
        let wait_handle1 = tokio::spawn(async move {
            let notify = map_clone1.acquire(&blob_id);
            notify.notified().await;
        });

        let map_clone2 = map.clone();
        let wait_handle2 = tokio::spawn(async move {
            let notify = map_clone2.acquire(&blob_id);
            notify.notified().await;
        });

        // Create a notified but do not poll it yet. This tests that even we poll the notified after
        // the notify is called, the notified will still be notified.
        let wait_notify_3 = map.acquire(&blob_id);
        let wait_notified_3 = wait_notify_3.notified();

        tokio::time::sleep(Duration::from_millis(10)).await;

        assert!(map.notify(&blob_id));

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

    /// Test that dropping acquired handles cleans up map entries.
    #[tokio::test]
    async fn test_multiple_waiters_drop() {
        let map = RefCountedNotifyMap::<BlobId>::default();
        let blob_id = random_blob_id();
        let blob_id2 = random_blob_id();

        let notify = map.acquire(&blob_id);
        let notify1 = notify.clone();
        let notify2 = map.acquire(&blob_id);
        let notify3 = map.acquire(&blob_id2);

        assert!(map.len() == 2);
        drop(notify);
        assert!(map.len() == 2);
        drop(notify1);
        assert!(map.len() == 2);
        drop(notify2);
        assert!(map.len() == 1);

        let notified_3 = notify3.notified();
        assert!(map.notify(&blob_id2));
        assert!(timeout(Duration::from_secs(1), notified_3).await.is_ok());
        drop(notify3);
        assert!(map.is_empty());
    }

    /// Test that RefCountedNotifyMap can handle a large number of concurrent acquire operations
    /// without deadlocking or panicking.
    ///
    /// This test uses a thread-based timeout mechanism to detect deadlocks. The test spawns
    /// multiple concurrent tasks in a separate thread, and if they don't complete within the
    /// timeout, the main test thread will panic and terminate the test, even if worker threads
    /// are deadlocked on mutexes.
    #[test]
    fn test_concurrent_acquire_and_drop() {
        let _ = tracing_subscriber::fmt::try_init();

        const TEST_TIMEOUT: Duration = Duration::from_secs(5);
        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(10)
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(test_concurrent_acquire_and_drop_impl());
            let _ = tx.send(());
        });

        rx.recv_timeout(TEST_TIMEOUT).unwrap_or_else(|_| {
            panic!(
                "Test timed out after {:?} - possible deadlock",
                TEST_TIMEOUT
            )
        });
    }

    async fn test_concurrent_acquire_and_drop_impl() {
        const NUM_TASKS: usize = 500;
        const TASK_TIMEOUT: Duration = Duration::from_secs(1);

        let map = RefCountedNotifyMap::<BlobId>::default();
        let blob_id = random_blob_id();

        let handles: Vec<_> = (0..NUM_TASKS)
            .map(|_| {
                let map = map.clone();
                tokio::spawn(async move {
                    let notify = map.acquire(&blob_id);
                    drop(notify);
                })
            })
            .collect();

        let result = timeout(TASK_TIMEOUT, async {
            for handle in handles {
                handle.await.expect("task should not panic");
            }
        })
        .await;

        match result {
            Ok(_) => (),
            Err(_) => panic!(
                "tasks timed out after {:?} - possible deadlock",
                TASK_TIMEOUT
            ),
        }
    }

    /// Randomized test that concurrently calls acquire, notify, and drop operations to detect
    /// deadlocks and memory leaks under realistic usage patterns.
    #[test]
    fn test_randomized_concurrent_operations() {
        let _ = tracing_subscriber::fmt::try_init();

        const TEST_TIMEOUT: Duration = Duration::from_secs(10);
        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(10)
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(test_randomized_concurrent_operations_impl());
            let _ = tx.send(());
        });

        rx.recv_timeout(TEST_TIMEOUT).unwrap_or_else(|_| {
            panic!(
                "Test timed out after {:?} - possible deadlock",
                TEST_TIMEOUT
            )
        });
    }

    enum TestOperation {
        AcquireAndDrop,
        Notify,
        AcquireAndHoldAndDrop,
    }

    impl Distribution<TestOperation> for rand::distributions::Standard {
        fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> TestOperation {
            match rng.gen_range(0..10) {
                0 => TestOperation::Notify,                    // 10% notify
                1..=3 => TestOperation::AcquireAndDrop,        // 30% acquire and drop
                4..=9 => TestOperation::AcquireAndHoldAndDrop, // 60% acquire and hold and drop
                _ => unreachable!(),
            }
        }
    }

    async fn test_randomized_concurrent_operations_impl() {
        const NUM_BLOBS: usize = 10;
        const NUM_OPERATIONS: usize = 1000;
        const TASK_TIMEOUT: Duration = Duration::from_secs(5);

        let map = RefCountedNotifyMap::<BlobId>::default();
        let blob_ids: Vec<_> = (0..NUM_BLOBS).map(|_| random_blob_id()).collect();

        let handles: Vec<_> = (0..NUM_OPERATIONS)
            .map(|_| {
                let map = map.clone();
                let blob_ids = blob_ids.clone();
                tokio::spawn(async move {
                    let (blob_id, operation) = {
                        let mut rng = rand::thread_rng();
                        (
                            blob_ids[rng.gen_range(0..blob_ids.len())],
                            rand::random::<TestOperation>(),
                        )
                    };

                    match operation {
                        TestOperation::AcquireAndDrop => {
                            let _notify = map.acquire(&blob_id);
                            tokio::task::yield_now().await;
                        }
                        TestOperation::Notify => {
                            let _ = map.notify(&blob_id);
                        }
                        TestOperation::AcquireAndHoldAndDrop => {
                            let notify = map.acquire(&blob_id);
                            select! {
                                _ = notify.notified() => (),
                                _ = tokio::time::sleep(Duration::from_millis(5)) => (),
                            }
                        }
                    }
                })
            })
            .collect();

        let result = timeout(TASK_TIMEOUT, async {
            for handle in handles {
                handle.await.expect("task should not panic");
            }
        })
        .await;

        match result {
            Ok(_) => (),
            Err(_) => panic!(
                "operations timed out after {:?} - possible deadlock",
                TASK_TIMEOUT
            ),
        }

        assert!(
            map.is_empty(),
            "possible memory leak: {} notifications registered",
            map.len()
        );
    }
}
