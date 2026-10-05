// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Shared admission for blob work and shard/epoch lifecycle changes.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};

use tokio::sync::{
    Mutex as AsyncMutex,
    OwnedMutexGuard,
    OwnedRwLockReadGuard,
    OwnedRwLockWriteGuard,
    RwLock,
};
use typed_store::TypedStoreError;

use super::StrataLifecycle;
type Result<T> = std::result::Result<T, TypedStoreError>;

#[derive(Debug, Default)]
pub(super) struct Coordination {
    lifecycle: Arc<RwLock<()>>,
    blobs: Mutex<HashMap<Vec<u8>, Weak<BlobLock>>>,
    halt_reason: Mutex<Option<String>>,
}

#[derive(Debug)]
struct BlobLock {
    key: Vec<u8>,
    mutex: Arc<AsyncMutex<()>>,
    owner: Weak<Coordination>,
}

impl Drop for BlobLock {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.upgrade() {
            let mut blobs = owner.blobs.lock().expect("blob lock registry poisoned");
            // A new acquisition may already have replaced our expired weak entry.
            if blobs
                .get(&self.key)
                .is_some_and(|entry| std::ptr::eq(entry.as_ptr(), self))
            {
                blobs.remove(&self.key);
            }
        }
    }
}

#[derive(Debug)]
#[must_use = "hold this guard through the protected blob work and durable acknowledgement"]
pub struct LockedBlobs {
    // Drop mutex guards before releasing their registry entries, and the lifecycle guard last.
    _guards: Vec<OwnedMutexGuard<()>>,
    _entries: Vec<Arc<BlobLock>>,
    _coordination: Arc<Coordination>,
    _lifecycle: OwnedRwLockReadGuard<()>,
}

#[derive(Debug)]
#[must_use = "hold this guard through the shard or epoch change and its durability"]
pub struct LifecycleGuard {
    _coordination: Arc<Coordination>,
    _guard: OwnedRwLockWriteGuard<()>,
}

impl StrataLifecycle {
    pub async fn lock_blobs(&self, keys: &[&[u8]]) -> Result<LockedBlobs> {
        self.check_running()?;
        // Blob locks alone cannot exclude shard recreation or epoch changes: those affect many
        // blobs and take the lifecycle lock exclusively. For example, a delete could validate
        // shard 7 generation 0, then a drop/recreate could make its tombstone target generation 1.
        // Take the shared lifecycle guard first and retain it in LockedBlobs through Strata
        // durability and RocksDB acknowledgement. This also makes epoch advancement wait for
        // an in-flight lifetime extension to finish. Shared mode allows unrelated blobs to run
        // concurrently; exclusive mode would serialize all blob work. Reconciliation must finish
        // the boundary's lifetime updates before advancing the epoch.
        let lifecycle = self.coordination.lifecycle.clone().read_owned().await;
        let mut keys: Vec<_> = keys.iter().map(|key| key.to_vec()).collect();
        keys.sort_unstable();
        keys.dedup();
        let entries: Vec<_> = {
            let mut blobs = self
                .coordination
                .blobs
                .lock()
                .expect("blob lock registry poisoned");
            keys.into_iter()
                .map(|key| {
                    if let Some(entry) = blobs.get(&key).and_then(Weak::upgrade) {
                        return entry;
                    }
                    let entry = Arc::new(BlobLock {
                        key: key.clone(),
                        mutex: Arc::default(),
                        owner: Arc::downgrade(&self.coordination),
                    });
                    blobs.insert(key, Arc::downgrade(&entry));
                    entry
                })
                .collect()
        };
        // Reservations stay alive while waiting. Cancelling an acquisition releases both the
        // already-acquired guards and the registry entries; there is no permanent per-blob map.
        let mut guards = Vec::with_capacity(entries.len());
        for entry in &entries {
            guards.push(entry.mutex.clone().lock_owned().await);
        }
        self.check_running()?;
        Ok(LockedBlobs {
            _guards: guards,
            _entries: entries,
            _coordination: self.coordination.clone(),
            _lifecycle: lifecycle,
        })
    }

    pub async fn lock_lifecycle(&self) -> Result<LifecycleGuard> {
        self.check_running()?;
        let guard = LifecycleGuard {
            _coordination: self.coordination.clone(),
            _guard: self.coordination.lifecycle.clone().write_owned().await,
        };
        self.check_running()?;
        Ok(guard)
    }

    pub fn check_running(&self) -> Result<()> {
        if let Some(reason) = &*self
            .coordination
            .halt_reason
            .lock()
            .expect("halt mutex poisoned")
        {
            return Err(TypedStoreError::TaskError(format!(
                "Strata admission halted: {reason}"
            )));
        }
        Ok(())
    }

    pub fn halt(&self, reason: String) {
        self.coordination
            .halt_reason
            .lock()
            .expect("halt mutex poisoned")
            .get_or_insert(reason);
    }
}
