// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Shared admission for blob work and shard/epoch lifecycle changes.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};

use sui_types::base_types::ObjectID;
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
    keys: Mutex<HashMap<LockKey, Weak<KeyLock>>>,
    halt_reason: Mutex<Option<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum LockKey {
    Blob(Vec<u8>),
    Pool(ObjectID),
}

#[derive(Debug)]
struct KeyLock {
    key: LockKey,
    lock: Arc<RwLock<()>>,
    put_admission: Arc<AsyncMutex<()>>,
    owner: Weak<Coordination>,
}

impl Drop for KeyLock {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.upgrade() {
            let mut keys = owner.keys.lock().expect("key lock registry poisoned");
            // A new acquisition may already have replaced our expired weak entry.
            if keys
                .get(&self.key)
                .is_some_and(|entry| std::ptr::eq(entry.as_ptr(), self))
            {
                keys.remove(&self.key);
            }
        }
    }
}

#[derive(Debug)]
#[must_use = "hold this guard through the protected blob work and durable acknowledgement"]
pub struct LockedBlobs {
    // Release the per-blob locks before allowing a shard or epoch change.
    _keys: LockedKeys,
    _lifecycle: OwnedRwLockReadGuard<()>,
}

#[derive(Debug)]
#[must_use = "hold this guard through the protected updates and acknowledgement"]
pub struct LockedKeys {
    // Keep entries alive while waiting/holding their locks so the weak registry reuses them.
    // Drop write guards before releasing the entries.
    _guards: Vec<OwnedRwLockWriteGuard<()>>,
    _entries: Vec<Arc<KeyLock>>,
}

#[derive(Debug)]
#[must_use = "hold through Strata durability and any RocksDB acknowledgement"]
pub struct SharedLockedBlobs {
    _guards: Vec<OwnedRwLockReadGuard<()>>,
    entries: Vec<Arc<KeyLock>>,
    _lifecycle: OwnedRwLockReadGuard<()>,
}

impl SharedLockedBlobs {
    /// Serialize the delete-marker check and its fsync, not the Strata durability wait.
    /// Otherwise a second put could observe `delete = false` before the first put synced it.
    pub async fn lock_put_admission(&self) -> Vec<OwnedMutexGuard<()>> {
        let mut guards = Vec::with_capacity(self.entries.len());
        for entry in &self.entries {
            guards.push(entry.put_admission.clone().lock_owned().await);
        }
        guards
    }
}

#[derive(Debug)]
#[must_use = "hold this guard through the shard or epoch change and its durability"]
pub struct LifecycleGuard {
    _guard: OwnedRwLockWriteGuard<()>,
}

impl StrataLifecycle {
    /// Puts for the same blob share this lock through durability, so their slivers can be
    /// submitted together and covered by one sync. Metadata changes and reconciliation
    /// use `lock_blobs` exclusively and wait for all those puts to finish.
    pub async fn lock_blobs_shared(&self, keys: &[&[u8]]) -> Result<SharedLockedBlobs> {
        self.check_running()?;
        let lifecycle = self.coordination.lifecycle.clone().read_owned().await;
        let entries =
            self.key_entries(keys.iter().map(|key| LockKey::Blob(key.to_vec())).collect());
        let mut guards = Vec::with_capacity(entries.len());
        for entry in &entries {
            guards.push(entry.lock.clone().read_owned().await);
        }
        self.check_running()?;
        Ok(SharedLockedBlobs {
            _guards: guards,
            entries,
            _lifecycle: lifecycle,
        })
    }

    pub async fn lock_blobs(&self, keys: &[&[u8]]) -> Result<LockedBlobs> {
        self.check_running()?;
        // Example: a put checks shard 7's generation, then submits its data. A shard drop/recreate
        // must not happen between those steps. Likewise, advancing Strata's epoch must wait for
        // an in-flight lifetime update to finish. Shared mode still lets other blobs make progress.
        let lifecycle = self.coordination.lifecycle.clone().read_owned().await;
        let keys = self
            .lock_keys(keys.iter().map(|key| LockKey::Blob(key.to_vec())).collect())
            .await?;
        Ok(LockedBlobs {
            _keys: keys,
            _lifecycle: lifecycle,
        })
    }

    pub async fn lock_pools(&self, pools: &[ObjectID]) -> Result<LockedKeys> {
        // Pool metadata and dirty-marker cleanup do not mutate Strata's shards or clock.
        self.lock_keys(pools.iter().copied().map(LockKey::Pool).collect())
            .await
    }

    async fn lock_keys(&self, keys: Vec<LockKey>) -> Result<LockedKeys> {
        self.check_running()?;
        let entries = self.key_entries(keys);
        // Reservations stay alive while waiting. Cancelling an acquisition releases both the
        // already-acquired guards and the registry entries; idle keys do not accumulate.
        let mut guards = Vec::with_capacity(entries.len());
        for entry in &entries {
            guards.push(entry.lock.clone().write_owned().await);
        }
        self.check_running()?;
        Ok(LockedKeys {
            _guards: guards,
            _entries: entries,
        })
    }

    fn key_entries(&self, mut keys: Vec<LockKey>) -> Vec<Arc<KeyLock>> {
        keys.sort_unstable();
        keys.dedup();
        {
            let mut registry = self
                .coordination
                .keys
                .lock()
                .expect("key lock registry poisoned");
            keys.into_iter()
                .map(|key| {
                    if let Some(entry) = registry.get(&key).and_then(Weak::upgrade) {
                        return entry;
                    }
                    let entry = Arc::new(KeyLock {
                        key: key.clone(),
                        lock: Arc::default(),
                        put_admission: Arc::default(),
                        owner: Arc::downgrade(&self.coordination),
                    });
                    registry.insert(key, Arc::downgrade(&entry));
                    entry
                })
                .collect()
        }
    }

    pub async fn lock_lifecycle(&self) -> Result<LifecycleGuard> {
        self.check_running()?;
        let guard = LifecycleGuard {
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
