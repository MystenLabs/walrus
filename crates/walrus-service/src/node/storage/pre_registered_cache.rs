// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Cache for pre-registered blob slivers that are waiting for register events.

use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use typed_store::{Map, rocks::DBMap};
use walrus_core::{BlobId, encoding::SliverPair, metadata::VerifiedBlobMetadataWithId};

use super::DatabaseConfig;

/// Cached blob slivers waiting for register event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedBlobSlivers {
    /// All sliver pairs this node should store for this blob.
    pub sliver_pairs: Vec<SliverPair>,
    /// Metadata for the blob (single copy).
    pub metadata: Option<VerifiedBlobMetadataWithId>,
    /// When this was cached.
    pub cached_at: SystemTime,
}

impl CachedBlobSlivers {
    /// Creates a new cached blob.
    pub fn new(
        sliver_pairs: Vec<SliverPair>,
        metadata: Option<VerifiedBlobMetadataWithId>,
    ) -> Self {
        let cached_at = SystemTime::now();
        Self {
            sliver_pairs,
            metadata,
            cached_at,
        }
    }

    /// Returns the number of sliver pairs in this cache entry.
    pub fn num_slivers(&self) -> usize {
        self.sliver_pairs.len()
    }
}

/// Table for managing pre-registered blob cache.
#[derive(Debug, Clone)]
pub struct PreRegisteredCache {
    /// The underlying database map.
    table: DBMap<BlobId, CachedBlobSlivers>,
}

impl PreRegisteredCache {
    /// Opens or creates the pre-registered cache table.
    pub fn new(table: DBMap<BlobId, CachedBlobSlivers>) -> Self {
        Self { table }
    }

    /// Returns the column family options for pre-registered blobs.
    pub fn cf_options(db_config: &DatabaseConfig) -> rocksdb::Options {
        // Use similar options to metadata column family
        db_config.metadata().to_options()
    }

    /// Caches blob slivers and metadata.
    pub fn cache_blob(
        &self,
        blob_id: &BlobId,
        sliver_pairs: Vec<SliverPair>,
        metadata: Option<VerifiedBlobMetadataWithId>,
    ) -> Result<(), typed_store::TypedStoreError> {
        let cached = CachedBlobSlivers::new(sliver_pairs, metadata);
        self.table.insert(blob_id, &cached)
    }

    /// Retrieves cached blob slivers if they exist.
    pub fn get_cached_blob(
        &self,
        blob_id: &BlobId,
    ) -> Result<Option<CachedBlobSlivers>, typed_store::TypedStoreError> {
        self.table.get(blob_id)
    }

    /// Removes cached blob slivers.
    pub fn remove_cached_blob(&self, blob_id: &BlobId) -> Result<(), typed_store::TypedStoreError> {
        self.table.remove(blob_id)
    }

    /// Checks if a blob is cached.
    pub fn is_cached(&self, blob_id: &BlobId) -> Result<bool, typed_store::TypedStoreError> {
        Ok(self.get_cached_blob(blob_id)?.is_some())
    }

    /// Returns the number of cached blobs.
    pub fn num_cached_blobs(&self) -> Result<usize, typed_store::TypedStoreError> {
        let mut count = 0;
        for _ in self.table.safe_iter()? {
            count += 1;
        }
        Ok(count)
    }

    /// Returns statistics about the cache.
    #[allow(dead_code)]
    pub fn cache_stats(&self) -> Result<CacheStats, typed_store::TypedStoreError> {
        let mut total_blobs = 0;
        let mut total_slivers = 0;

        for result in self.table.safe_iter()? {
            let (_blob_id, cached) = result?;
            total_blobs += 1;
            total_slivers += cached.num_slivers();
        }

        Ok(CacheStats {
            total_blobs,
            total_slivers,
            expired_blobs: 0, // No expiry logic anymore
        })
    }
}

/// Statistics about the pre-registered cache.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheStats {
    /// Total number of cached blobs.
    pub total_blobs: usize,
    /// Total number of cached slivers across all blobs.
    pub total_slivers: usize,
    /// Number of expired blobs (not yet cleaned up).
    pub expired_blobs: usize,
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use typed_store::rocks::MetricConf;
    use walrus_core::{encoding::SliverPair, test_utils};
    use walrus_utils::metrics::Registry;

    use crate::node::storage::{DatabaseConfig, Storage};

    #[tokio::test]
    async fn test_cache_blob_slivers() {
        let temp_dir = TempDir::new().unwrap();
        let db_config = DatabaseConfig::default();
        let storage = Storage::open(
            temp_dir.path(),
            db_config,
            MetricConf::default(),
            Registry::default(),
        )
        .unwrap();

        let blob_id = test_utils::random_blob_id();
        let metadata = test_utils::verified_blob_metadata();
        let sliver_pair = SliverPair {
            primary: test_utils::primary_sliver(),
            secondary: test_utils::secondary_sliver(),
        };

        // Cache blob slivers
        storage
            .cache_blob_slivers(&blob_id, vec![sliver_pair.clone()], Some(metadata.clone()))
            .unwrap();

        // Verify blob is cached
        assert!(storage.is_blob_cached(&blob_id).unwrap());

        // Retrieve cached blob
        let cached = storage.get_cached_blob(&blob_id).unwrap().unwrap();
        assert_eq!(cached.sliver_pairs.len(), 1);
        assert_eq!(cached.sliver_pairs[0], sliver_pair);
        assert_eq!(cached.metadata, Some(metadata));

        // Remove cached blob
        storage.remove_cached_blob(&blob_id).unwrap();
        assert!(!storage.is_blob_cached(&blob_id).unwrap());
    }

    #[tokio::test]
    async fn test_cache_stats() {
        let temp_dir = TempDir::new().unwrap();
        let db_config = DatabaseConfig::default();
        let storage = Storage::open(
            temp_dir.path(),
            db_config,
            MetricConf::default(),
            Registry::default(),
        )
        .unwrap();

        // Add blobs with different sliver counts
        for i in 0..3 {
            let blob_id = test_utils::random_blob_id();
            let mut sliver_pairs = Vec::new();

            // Each blob has i+1 sliver pairs
            for _ in 0..=i {
                sliver_pairs.push(SliverPair {
                    primary: test_utils::primary_sliver(),
                    secondary: test_utils::secondary_sliver(),
                });
            }

            storage
                .cache_blob_slivers(&blob_id, sliver_pairs, None)
                .unwrap();
        }

        let stats = storage.pre_registered_cache.cache_stats().unwrap();
        assert_eq!(stats.total_blobs, 3);
        assert_eq!(stats.total_slivers, 6); // 1 + 2 + 3
        assert_eq!(stats.expired_blobs, 0);
    }
}
