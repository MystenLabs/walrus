// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! In-memory cache for metadata awaiting blob registration.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::RwLock;
use walrus_core::{BlobId, metadata::VerifiedBlobMetadataWithId};

use super::metrics::NodeMetricSet;

#[derive(Debug, Clone)]
struct CachedMetadata {
    inserted_at: Instant,
    metadata: Arc<VerifiedBlobMetadataWithId>,
}

#[derive(Debug)]
struct PendingMetadataCacheInner {
    max_entries: usize,
    ttl: Duration,
    entries: HashMap<BlobId, CachedMetadata>,
}

impl PendingMetadataCacheInner {
    fn new(max_entries: usize, ttl: Duration) -> Self {
        Self {
            max_entries,
            ttl,
            entries: HashMap::new(),
        }
    }

    fn evict_expired(&mut self, now: Instant) {
        if self.ttl.is_zero() {
            return;
        }
        let ttl = self.ttl;
        self.entries
            .retain(|_, cached| now.saturating_duration_since(cached.inserted_at) < ttl);
    }

    fn get(&self, blob_id: &BlobId) -> Option<Arc<VerifiedBlobMetadataWithId>> {
        self.entries
            .get(blob_id)
            .map(|cached| cached.metadata.clone())
    }

    /// Inserts metadata into the cache.
    /// Returns `Err(())` when the cache is at capacity and the entry could not be stored after
    /// evicting expired items.
    fn insert(
        &mut self,
        blob_id: BlobId,
        metadata: Arc<VerifiedBlobMetadataWithId>,
    ) -> Result<(), ()> {
        let now = Instant::now();
        if self.max_entries == 0 {
            return Err(());
        }

        if self.entries.len() >= self.max_entries {
            self.evict_expired(now);
            if self.entries.len() >= self.max_entries {
                return Err(());
            }
        }

        self.entries.insert(
            blob_id,
            CachedMetadata {
                inserted_at: now,
                metadata,
            },
        );
        Ok(())
    }

    fn remove(&mut self, blob_id: &BlobId) -> Option<Arc<VerifiedBlobMetadataWithId>> {
        self.evict_expired(Instant::now());
        self.entries.remove(blob_id).map(|cached| cached.metadata)
    }

    fn len(&self) -> usize {
        self.entries.len()
    }
}

/// Metadata cached prior to registration.
#[derive(Debug)]
pub(crate) struct PendingMetadataCache {
    inner: RwLock<PendingMetadataCacheInner>,
    metrics: Arc<NodeMetricSet>,
}

impl PendingMetadataCache {
    pub fn new(max_entries: usize, ttl: Duration, metrics: Arc<NodeMetricSet>) -> Self {
        Self {
            inner: RwLock::new(PendingMetadataCacheInner::new(max_entries, ttl)),
            metrics,
        }
    }

    pub async fn get(&self, blob_id: &BlobId) -> Option<Arc<VerifiedBlobMetadataWithId>> {
        let inner = self.inner.read().await;
        self.metrics
            .pending_metadata_cache_entries
            .set(i64::try_from(inner.len()).unwrap_or(i64::MAX));
        inner.get(blob_id)
    }

    pub async fn insert(
        &self,
        blob_id: BlobId,
        metadata: Arc<VerifiedBlobMetadataWithId>,
    ) -> Result<(), ()> {
        let mut inner = self.inner.write().await;
        let result = inner.insert(blob_id, metadata);
        self.metrics
            .pending_metadata_cache_entries
            .set(i64::try_from(inner.len()).unwrap_or(i64::MAX));
        result
    }

    pub async fn remove(&self, blob_id: &BlobId) -> Option<Arc<VerifiedBlobMetadataWithId>> {
        let mut inner = self.inner.write().await;
        let removed = inner.remove(blob_id);
        self.metrics
            .pending_metadata_cache_entries
            .set(i64::try_from(inner.len()).unwrap_or(i64::MAX));
        removed
    }

    #[cfg(test)]
    pub async fn entry_count(&self) -> usize {
        let mut inner = self.inner.write().await;
        inner.evict_expired(Instant::now());
        inner.len()
    }
}
