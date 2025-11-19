// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! BlobManager metadata cache for storage nodes.
//!
//! This module provides an in-memory cache for BlobManager metadata to reduce
//! RPC calls when processing managed blob operations. The cache stores:
//! - BlobManager storage pool information (capacity, epochs)
//! - Table IDs for efficient lookups
//! - Last update timestamp for cache invalidation

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use sui_types::base_types::ObjectID;
use tokio::sync::RwLock;

/// How long to cache BlobManager metadata before considering it stale.
const CACHE_TTL: Duration = Duration::from_secs(900); // 15 minutes.

/// Cached information about a BlobManager.
#[derive(Debug, Clone)]
pub struct CachedBlobManagerInfo {
    /// The ID of the BlobManager.
    pub manager_id: ObjectID,
    /// The initial shared version of the BlobManager object.
    pub initial_shared_version: u64,
    /// Total storage capacity in bytes.
    pub total_capacity: u64,
    /// Used storage in bytes.
    pub used_capacity: u64,
    /// Storage start epoch.
    pub start_epoch: u32,
    /// Storage end epoch.
    pub end_epoch: u32,
    /// Table ID for blob_id -> vector<ObjectID> mapping.
    pub blob_id_to_objects_table_id: Option<ObjectID>,
    /// Table ID for ObjectID -> ManagedBlob mapping.
    pub blobs_by_object_id_table_id: Option<ObjectID>,
    /// When this cache entry was last updated.
    pub last_updated: Instant,
}

impl CachedBlobManagerInfo {
    /// Returns true if this cache entry is still valid (not expired).
    pub fn is_valid(&self) -> bool {
        self.last_updated.elapsed() < CACHE_TTL
    }

    /// Returns the available capacity in bytes.
    pub fn available_capacity(&self) -> u64 {
        self.total_capacity.saturating_sub(self.used_capacity)
    }

    /// Returns true if the BlobManager has enough capacity for the given size.
    pub fn has_capacity_for(&self, size: u64) -> bool {
        self.available_capacity() >= size
    }

    /// Returns true if the current epoch is within the storage period.
    pub fn is_epoch_valid(&self, current_epoch: u32) -> bool {
        current_epoch >= self.start_epoch && current_epoch < self.end_epoch
    }
}

/// Cache for BlobManager metadata.
#[derive(Debug, Clone)]
pub struct BlobManagerCache {
    inner: Arc<RwLock<BlobManagerCacheInner>>,
}

#[derive(Debug)]
struct BlobManagerCacheInner {
    /// Map from BlobManager ID to cached info.
    managers: HashMap<ObjectID, CachedBlobManagerInfo>,
}

impl BlobManagerCache {
    /// Creates a new empty cache.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(BlobManagerCacheInner {
                managers: HashMap::new(),
            })),
        }
    }

    /// Gets cached info for a BlobManager if it exists and is valid.
    pub async fn get(&self, manager_id: &ObjectID) -> Option<CachedBlobManagerInfo> {
        let cache = self.inner.read().await;
        cache
            .managers
            .get(manager_id)
            .filter(|info| info.is_valid())
            .cloned()
    }

    /// Updates or inserts BlobManager info in the cache.
    pub async fn upsert(&self, info: CachedBlobManagerInfo) {
        let mut cache = self.inner.write().await;
        cache.managers.insert(info.manager_id, info);
    }

    /// Removes a BlobManager from the cache.
    pub async fn remove(&self, manager_id: &ObjectID) {
        let mut cache = self.inner.write().await;
        cache.managers.remove(manager_id);
    }

    /// Invalidates (removes) expired entries from the cache.
    pub async fn invalidate_expired(&self) {
        let mut cache = self.inner.write().await;
        cache.managers.retain(|_, info| info.is_valid());
    }

    /// Clears all entries from the cache.
    pub async fn clear(&self) {
        let mut cache = self.inner.write().await;
        cache.managers.clear();
    }

    /// Returns the number of cached BlobManagers.
    pub async fn size(&self) -> usize {
        let cache = self.inner.read().await;
        cache.managers.len()
    }

    /// Updates the storage epochs for a BlobManager (e.g., after extension).
    pub async fn update_epochs(
        &self,
        manager_id: &ObjectID,
        new_end_epoch: u32,
    ) -> Option<CachedBlobManagerInfo> {
        let mut cache = self.inner.write().await;
        if let Some(info) = cache.managers.get_mut(manager_id) {
            info.end_epoch = new_end_epoch;
            info.last_updated = Instant::now();
            Some(info.clone())
        } else {
            None
        }
    }

    /// Updates the used capacity for a BlobManager after blob registration.
    pub async fn add_used_capacity(
        &self,
        manager_id: &ObjectID,
        additional_size: u64,
    ) -> Option<CachedBlobManagerInfo> {
        let mut cache = self.inner.write().await;
        if let Some(info) = cache.managers.get_mut(manager_id) {
            info.used_capacity = info.used_capacity.saturating_add(additional_size);
            info.last_updated = Instant::now();
            Some(info.clone())
        } else {
            None
        }
    }

    /// Updates the used capacity for a BlobManager after blob deletion.
    pub async fn subtract_used_capacity(
        &self,
        manager_id: &ObjectID,
        freed_size: u64,
    ) -> Option<CachedBlobManagerInfo> {
        let mut cache = self.inner.write().await;
        if let Some(info) = cache.managers.get_mut(manager_id) {
            info.used_capacity = info.used_capacity.saturating_sub(freed_size);
            info.last_updated = Instant::now();
            Some(info.clone())
        } else {
            None
        }
    }
}

impl Default for BlobManagerCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cache_operations() {
        let cache = BlobManagerCache::new();
        let manager_id = ObjectID::random();

        // Test insertion and retrieval.
        let info = CachedBlobManagerInfo {
            manager_id,
            initial_shared_version: 1,
            total_capacity: 1_000_000,
            used_capacity: 100_000,
            start_epoch: 1,
            end_epoch: 10,
            blob_id_to_objects_table_id: Some(ObjectID::random()),
            blobs_by_object_id_table_id: Some(ObjectID::random()),
            last_updated: Instant::now(),
        };

        cache.upsert(info.clone()).await;
        assert_eq!(cache.size().await, 1);

        // Test get.
        let retrieved = cache.get(&manager_id).await;
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.manager_id, manager_id);
        assert_eq!(retrieved.total_capacity, 1_000_000);

        // Test capacity calculations.
        assert_eq!(retrieved.available_capacity(), 900_000);
        assert!(retrieved.has_capacity_for(900_000));
        assert!(!retrieved.has_capacity_for(900_001));

        // Test epoch validation.
        assert!(retrieved.is_epoch_valid(5));
        assert!(!retrieved.is_epoch_valid(0));
        assert!(!retrieved.is_epoch_valid(10));

        // Test capacity updates.
        cache.add_used_capacity(&manager_id, 50_000).await;
        let updated = cache.get(&manager_id).await.unwrap();
        assert_eq!(updated.used_capacity, 150_000);
        assert_eq!(updated.available_capacity(), 850_000);

        cache.subtract_used_capacity(&manager_id, 30_000).await;
        let updated = cache.get(&manager_id).await.unwrap();
        assert_eq!(updated.used_capacity, 120_000);

        // Test epoch update.
        cache.update_epochs(&manager_id, 20).await;
        let updated = cache.get(&manager_id).await.unwrap();
        assert_eq!(updated.end_epoch, 20);

        // Test removal.
        cache.remove(&manager_id).await;
        assert_eq!(cache.size().await, 0);
        assert!(cache.get(&manager_id).await.is_none());
    }

    #[tokio::test]
    async fn test_cache_expiry() {
        // This test would need to mock time or use a configurable TTL.
        // For now, just test that the is_valid() method works.
        let info = CachedBlobManagerInfo {
            manager_id: ObjectID::random(),
            initial_shared_version: 1,
            total_capacity: 1_000_000,
            used_capacity: 0,
            start_epoch: 1,
            end_epoch: 10,
            blob_id_to_objects_table_id: None,
            blobs_by_object_id_table_id: None,
            last_updated: Instant::now(),
        };

        assert!(info.is_valid());

        // Create an expired entry (would need time mocking to test properly).
        let expired_info = CachedBlobManagerInfo {
            last_updated: Instant::now() - Duration::from_secs(1000),
            ..info
        };

        assert!(!expired_info.is_valid());
    }
}
