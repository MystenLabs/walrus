// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! In-memory cache for slivers that arrive before a blob is registered.

use std::{
    collections::{HashMap, hash_map::Entry},
    fmt::Debug,
    sync::Arc,
    time::{Duration, Instant},
};

use indexmap::IndexMap;
use tokio::sync::Mutex;
use walrus_core::{BlobId, Sliver, SliverPairIndex, SliverType};

use super::metrics::NodeMetricSet;

/// All slivers that still need to be persisted for a blob.
#[derive(Debug, Default)]
struct PendingBlobEntry {
    slivers: HashMap<SliverCacheKey, CachedSliverEntry>,
}

impl PendingBlobEntry {
    fn len(&self) -> usize {
        self.slivers.len()
    }
}

/// Key for cached slivers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct SliverCacheKey {
    sliver_pair_index: SliverPairIndex,
    sliver_type: SliverType,
}

impl SliverCacheKey {
    fn new(sliver_pair_index: SliverPairIndex, sliver_type: SliverType) -> Self {
        Self {
            sliver_pair_index,
            sliver_type,
        }
    }
}

#[derive(Debug, Clone)]
struct CachedSliverEntry {
    inserted_at: Instant,
    sliver: Sliver,
}

/// The inner state of the cache.
#[derive(Debug)]
struct PendingSliverCacheInner {
    max_slivers: usize,
    max_bytes: usize,
    ttl: Duration,
    blobs: IndexMap<BlobId, PendingBlobEntry>,
    sliver_count: usize,
    total_bytes: usize,
}

impl PendingSliverCacheInner {
    fn new(max_slivers: usize, max_bytes: usize, ttl: Duration) -> Self {
        Self {
            max_slivers,
            max_bytes,
            ttl,
            blobs: IndexMap::new(),
            sliver_count: 0,
            total_bytes: 0,
        }
    }

    fn evict_expired(&mut self, now: Instant) {
        if self.ttl.is_zero() {
            return;
        }

        let ttl = self.ttl;
        let mut removed_slivers = 0usize;
        let mut removed_bytes = 0usize;

        self.blobs.retain(|_, entry| {
            let mut entry_removed = 0usize;
            let mut entry_bytes = 0usize;
            entry.slivers.retain(|_, cached| {
                if now.saturating_duration_since(cached.inserted_at) < ttl {
                    true
                } else {
                    entry_removed += 1;
                    entry_bytes += cached.sliver.len();
                    false
                }
            });
            removed_slivers += entry_removed;
            removed_bytes += entry_bytes;
            !entry.slivers.is_empty()
        });

        if removed_slivers > 0 {
            self.sliver_count = self.sliver_count.saturating_sub(removed_slivers);
        }
        if removed_bytes > 0 {
            self.total_bytes = self.total_bytes.saturating_sub(removed_bytes);
        }
    }

    fn sliver_count(&self) -> usize {
        self.sliver_count
    }

    fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    fn blob_count(&self) -> usize {
        self.blobs.len()
    }

    fn insert(&mut self, blob_id: BlobId, key: SliverCacheKey, sliver: Sliver) -> Result<bool, ()> {
        if self.max_slivers == 0 || self.max_bytes == 0 {
            return Err(());
        }

        let now = Instant::now();
        self.evict_expired(now);

        let mut entry = self.blobs.shift_remove(&blob_id).unwrap_or_default();
        let new_len = sliver.len();

        let result = match entry.slivers.entry(key) {
            Entry::Occupied(mut occupied) => {
                let previous = occupied.get_mut();
                let previous_len = previous.sliver.len();
                let base_bytes = self.total_bytes.checked_sub(previous_len).ok_or(())?;
                let new_total_bytes = base_bytes.checked_add(new_len).ok_or(())?;
                if new_total_bytes > self.max_bytes {
                    Err(())
                } else {
                    previous.sliver = sliver;
                    previous.inserted_at = now;
                    self.total_bytes = new_total_bytes;
                    Ok(false)
                }
            }
            Entry::Vacant(vacant) => {
                if self.sliver_count >= self.max_slivers {
                    Err(())
                } else {
                    let new_total_bytes = self.total_bytes.checked_add(new_len).ok_or(())?;
                    if new_total_bytes > self.max_bytes {
                        Err(())
                    } else {
                        vacant.insert(CachedSliverEntry {
                            inserted_at: now,
                            sliver,
                        });
                        self.sliver_count += 1;
                        self.total_bytes = new_total_bytes;
                        Ok(true)
                    }
                }
            }
        };

        // Always put the potentially modified entry back.
        self.blobs.insert(blob_id, entry);

        result
    }

    fn contains(&mut self, blob_id: &BlobId, key: SliverCacheKey) -> bool {
        self.evict_expired(Instant::now());
        self.blobs
            .get(blob_id)
            .is_some_and(|entry| entry.slivers.contains_key(&key))
    }

    fn has_blob(&mut self, blob_id: &BlobId) -> bool {
        self.evict_expired(Instant::now());
        self.blobs.contains_key(blob_id)
    }

    fn drain(&mut self, blob_id: &BlobId) -> Vec<CachedSliver> {
        self.evict_expired(Instant::now());
        self.blobs
            .shift_remove(blob_id)
            .map(|entry| {
                let removed_count = entry.len();
                let removed_bytes: usize = entry
                    .slivers
                    .values()
                    .map(|cached| cached.sliver.len())
                    .sum();
                self.sliver_count = self.sliver_count.saturating_sub(removed_count);
                self.total_bytes = self.total_bytes.saturating_sub(removed_bytes);
                entry
                    .slivers
                    .into_iter()
                    .map(|(key, cached)| CachedSliver {
                        sliver_pair_index: key.sliver_pair_index,
                        sliver: cached.sliver,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn insert_many(&mut self, blob_id: BlobId, slivers: Vec<CachedSliver>) -> Result<(), ()> {
        if slivers.is_empty() {
            return Ok(());
        }

        if self.max_slivers == 0 || self.max_bytes == 0 {
            return Err(());
        }

        for cached in slivers {
            let key = SliverCacheKey::new(cached.sliver_pair_index, cached.sliver.r#type());
            self.insert(blob_id, key, cached.sliver)?;
        }
        Ok(())
    }
}

/// Slivers waiting for a blob registration to be observed.
#[derive(Debug)]
pub(crate) struct PendingSliverCache {
    max_slivers: usize,
    max_bytes: usize,
    max_sliver_bytes: usize,
    inner: Mutex<PendingSliverCacheInner>,
    metrics: Arc<NodeMetricSet>,
}

impl PendingSliverCache {
    pub fn new(
        max_slivers: usize,
        max_bytes: usize,
        max_sliver_bytes: usize,
        ttl: Duration,
        metrics: Arc<NodeMetricSet>,
    ) -> Self {
        Self {
            max_slivers,
            max_bytes,
            max_sliver_bytes,
            inner: Mutex::new(PendingSliverCacheInner::new(max_slivers, max_bytes, ttl)),
            metrics,
        }
    }

    pub fn capacity(&self) -> usize {
        if self.max_slivers == 0 || self.max_bytes == 0 || self.max_sliver_bytes == 0 {
            0
        } else {
            self.max_slivers
        }
    }

    pub async fn insert(
        &self,
        blob_id: BlobId,
        sliver_pair_index: SliverPairIndex,
        sliver: Sliver,
    ) -> Result<bool, ()> {
        if self.max_sliver_bytes == 0 || sliver.len() > self.max_sliver_bytes {
            return Err(());
        }
        let mut inner = self.inner.lock().await;
        let result = inner.insert(
            blob_id,
            SliverCacheKey::new(sliver_pair_index, sliver.r#type()),
            sliver,
        );
        self.update_metrics(&inner);
        result
    }

    pub async fn drain(&self, blob_id: &BlobId) -> Vec<CachedSliver> {
        let mut inner = self.inner.lock().await;
        let drained = inner.drain(blob_id);
        self.update_metrics(&inner);
        drained
    }

    pub async fn insert_many(&self, blob_id: BlobId, slivers: Vec<CachedSliver>) {
        if slivers.is_empty() {
            return;
        }

        let mut inner = self.inner.lock().await;
        if inner.insert_many(blob_id, slivers).is_err() {
            tracing::warn!(
                ?blob_id,
                "failed to reinsert pending slivers due to cache saturation"
            );
        }
        self.update_metrics(&inner);
    }

    pub async fn contains(
        &self,
        blob_id: &BlobId,
        sliver_pair_index: SliverPairIndex,
        sliver_type: SliverType,
    ) -> bool {
        let mut inner = self.inner.lock().await;
        let result = inner.contains(blob_id, SliverCacheKey::new(sliver_pair_index, sliver_type));
        self.update_metrics(&inner);
        result
    }

    pub async fn has_blob(&self, blob_id: &BlobId) -> bool {
        let mut inner = self.inner.lock().await;
        let result = inner.has_blob(blob_id);
        self.update_metrics(&inner);
        result
    }

    #[cfg(test)]
    pub async fn sliver_count(&self) -> usize {
        let mut inner = self.inner.lock().await;
        inner.evict_expired(Instant::now());
        inner.sliver_count()
    }

    #[cfg(test)]
    pub async fn total_bytes(&self) -> usize {
        let mut inner = self.inner.lock().await;
        inner.evict_expired(Instant::now());
        inner.total_bytes()
    }

    pub fn max_sliver_bytes(&self) -> usize {
        self.max_sliver_bytes
    }

    fn update_metrics(&self, inner: &PendingSliverCacheInner) {
        self.metrics
            .pending_sliver_cache_slivers
            .set(i64::try_from(inner.sliver_count()).unwrap_or(i64::MAX));
        self.metrics
            .pending_sliver_cache_blobs
            .set(i64::try_from(inner.blob_count()).unwrap_or(i64::MAX));
        self.metrics
            .pending_sliver_cache_bytes
            .set(i64::try_from(inner.total_bytes()).unwrap_or(i64::MAX));
    }
}

/// A cached sliver and the associated pair index.
#[derive(Debug)]
pub(crate) struct CachedSliver {
    pub sliver_pair_index: SliverPairIndex,
    pub sliver: Sliver,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use walrus_core::{BlobId, SliverPairIndex, test_utils::sliver};
    use walrus_utils::metrics::Registry;

    use super::*;

    fn test_cache() -> PendingSliverCache {
        let metrics = Arc::new(NodeMetricSet::new(&Registry::default()));
        PendingSliverCache::new(4, 1 << 20, 1 << 18, Duration::from_secs(60), metrics)
    }

    #[tokio::test]
    async fn insert_and_drain_keeps_sliver() {
        let cache = test_cache();
        let blob_id = BlobId([1; 32]);
        let pair_index = SliverPairIndex::from(0);
        let cached_sliver = sliver();
        let sliver_type = cached_sliver.r#type();

        assert!(
            cache
                .insert(blob_id, pair_index, cached_sliver.clone())
                .await
                .unwrap()
        );
        assert_eq!(cache.sliver_count().await, 1);
        assert!(cache.total_bytes().await > 0);
        assert!(cache.contains(&blob_id, pair_index, sliver_type).await);

        let drained = cache.drain(&blob_id).await;
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].sliver_pair_index, pair_index);
        assert_eq!(drained[0].sliver.r#type(), sliver_type);
        assert_eq!(cache.sliver_count().await, 0);
        assert_eq!(cache.total_bytes().await, 0);
    }

    #[tokio::test]
    async fn duplicate_insert_returns_false() {
        let cache = test_cache();
        let blob_id = BlobId([2; 32]);
        let pair_index = SliverPairIndex::from(0);
        let cached_sliver = sliver();

        assert!(
            cache
                .insert(blob_id, pair_index, cached_sliver.clone())
                .await
                .unwrap()
        );
        assert!(
            !cache
                .insert(blob_id, pair_index, cached_sliver)
                .await
                .unwrap(),
            "re-inserting the same sliver should not be considered new"
        );
        assert_eq!(cache.sliver_count().await, 1);
    }

    #[tokio::test]
    async fn rejects_inserts_when_capacity_exceeded() {
        let cache = test_cache();
        let blob_id1 = BlobId([3; 32]);
        let blob_id2 = BlobId([4; 32]);
        let pair_index = SliverPairIndex::from(0);

        let first_type = sliver().r#type();
        assert!(cache.insert(blob_id1, pair_index, sliver()).await.unwrap());
        assert!(cache.insert(blob_id2, pair_index, sliver()).await.unwrap());
        assert_eq!(cache.sliver_count().await, 2);

        let mut rejected = false;
        for i in 0..4 {
            let mut new_blob = [5u8; 32];
            new_blob[0] = i;
            let blob = BlobId(new_blob);
            if cache.insert(blob, pair_index, sliver()).await.is_err() {
                rejected = true;
                break;
            }
        }

        assert!(rejected, "cache should reject once capacity is exhausted");

        assert!(cache.contains(&blob_id1, pair_index, first_type).await);
    }

    #[tokio::test]
    async fn respects_byte_capacity() {
        let metrics = Arc::new(NodeMetricSet::new(&Registry::default()));
        let cached_sliver = sliver();
        let sliver_len = cached_sliver.len();
        let cache = PendingSliverCache::new(
            10,
            sliver_len * 2,
            sliver_len - 1,
            Duration::from_secs(60),
            metrics,
        );
        let blob_id = BlobId([6; 32]);
        let pair_index = SliverPairIndex::from(0);

        let result = cache.insert(blob_id, pair_index, cached_sliver).await;
        assert!(
            result.is_err(),
            "slivers larger than the threshold must not be cached"
        );
    }
}
