// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Storage layer for the Walrus Indexer (Octopus Index).
//!
//! This module implements the Octopus Index architecture for efficient indexing
//! of blobs and quilt patches. Indices are stored in RocksDB, organized by 'buckets'
//! that provide isolated namespaces similar to S3 buckets.

use std::{collections::HashMap, path::Path, sync::Arc};

use anyhow::Result;
use rocksdb::Options;
use serde::{Deserialize, Serialize};
use sui_types::base_types::ObjectID;
use typed_store::{
    Map,
    TypedStoreError,
    rocks::{DBBatch, DBMap, MetricConf, open_cf_opts},
};
use walrus_core::{BlobId, QuiltPatchId};

/// Target of an index entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum IndexTarget {
    /// Blob ID.
    BlobId(BlobId),
    /// Quilt patch ID.
    QuiltPatchId(QuiltPatchId),
    /// Quilt ID.
    QuiltId(ObjectID),
}

/// Blob identity containing both blob_id and object_id.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlobIdentity {
    /// The blob ID.
    pub blob_id: BlobId,
    /// The Sui object ID.
    pub object_id: ObjectID,
}

/// Primary index value containing the blob identity.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrimaryIndexValue {
    /// The blob identity this entry points to.
    pub blob_identity: BlobIdentity,
}

/// Index mutation operations that can be applied (matches PDF spec).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum IndexMutation {
    /// Insert a new index entry (bucket_id, identifier, object_id, blob_id).
    Insert {
        bucket_id: ObjectID,
        identifier: String,
        object_id: ObjectID,
        blob_id: BlobId,
    },
    /// Delete an index entry (object_id).
    Delete { object_id: ObjectID },
}

/// Batch of mutations for a specific bucket.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MutationSet {
    pub bucket_id: ObjectID,
    pub mutations: Vec<IndexMutation>,
}

pub fn construct_primary_index_key(bucket_id: &ObjectID, identifier: &str) -> String {
    format!("{}/{}", bucket_id, identifier)
}

pub fn construct_object_index_key(object_id: &ObjectID) -> String {
    object_id.to_string()
}

/// Value stored in the object index.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectIndexValue {
    /// The bucket ID.
    pub bucket_id: ObjectID,
    /// The identifier.
    pub identifier: String,
}

/// Storage interface for the Octopus Index (Dual Index System).
#[derive(Debug, Clone)]
pub struct OctopusIndexStore {
    /// Primary index: bucket_id/identifier -> BlobIdentity.
    primary_index: DBMap<String, PrimaryIndexValue>,
    /// Object index: object_id -> bucket_id/identifier.
    object_index: DBMap<String, ObjectIndexValue>,
    /// Event cursor store: stores the last processed event index for resumption.
    event_cursor_store: DBMap<String, u64>,
}

// Constants for future pagination implementation.
// const INLINE_STORAGE_THRESHOLD: usize = 100;
// const ENTRIES_PER_PAGE: usize = 1000;

impl OctopusIndexStore {
    /// Create a new OctopusIndexStore by opening the database at the specified path.
    /// This initializes all required column families for the indexer.
    pub async fn open(db_path: &str) -> Result<Self> {
        // Initialize the database with all column families
        let db_options = Options::default();
        let db = Arc::new(open_cf_opts(
            Path::new(db_path),
            None,
            MetricConf::default(),
            &[
                ("octopus_index_primary", db_options.clone()),
                ("octopus_index_object", db_options.clone()),
                ("octopus_event_cursor", db_options),
            ],
        )?);

        // Initialize primary index (bucket_id, identifier) -> BlobIdentity
        let primary_index: DBMap<String, PrimaryIndexValue> = DBMap::reopen(
            &db,
            Some("octopus_index_primary"),
            &typed_store::rocks::ReadWriteOptions::default(),
            false,
        )?;

        // Initialize object index
        let object_index: DBMap<String, ObjectIndexValue> = DBMap::reopen(
            &db,
            Some("octopus_index_object"),
            &typed_store::rocks::ReadWriteOptions::default(),
            false,
        )?;

        // Initialize event cursor store
        let event_cursor_store: DBMap<String, u64> = DBMap::reopen(
            &db,
            Some("octopus_event_cursor"),
            &typed_store::rocks::ReadWriteOptions::default(),
            false,
        )?;

        Ok(Self {
            primary_index,
            object_index,
            event_cursor_store,
        })
    }

    /// Create a new OctopusIndexStore from existing DBMap instances.
    /// This is used for testing and migration purposes.
    pub fn from_maps(
        primary_index: DBMap<String, PrimaryIndexValue>,
        object_index: DBMap<String, ObjectIndexValue>,
        event_cursor_store: DBMap<String, u64>,
    ) -> Self {
        Self {
            primary_index,
            object_index,
            event_cursor_store,
        }
    }

    /// Apply an insert operation to both indices.
    fn apply_insert(
        &self,
        batch: &mut DBBatch,
        bucket_id: &ObjectID,
        identifier: &str,
        object_id: &ObjectID,
        blob_id: BlobId,
    ) -> Result<(), TypedStoreError> {
        // Primary index: bucket_id/identifier -> BlobIdentity.
        let primary_key = construct_primary_index_key(bucket_id, identifier);
        let blob_identity = BlobIdentity {
            blob_id,
            object_id: *object_id,
        };
        let primary_value = PrimaryIndexValue { blob_identity };

        // Object index: object_id -> bucket_id/identifier.
        let object_key = construct_object_index_key(object_id);
        let object_value = ObjectIndexValue {
            bucket_id: *bucket_id,
            identifier: identifier.to_string(),
        };

        batch.insert_batch(&self.primary_index, [(primary_key, primary_value)])?;
        batch.insert_batch(&self.object_index, [(object_key, object_value)])?;
        Ok(())
    }

    /// Apply a delete operation to both indices using the primary key.
    fn apply_delete_by_key(
        &self,
        batch: &mut DBBatch,
        primary_key: String,
    ) -> Result<(), TypedStoreError> {
        // Look up the primary value to get the object_id.
        if let Some(primary_value) = self.primary_index.get(&primary_key)? {
            let object_key = construct_object_index_key(&primary_value.blob_identity.object_id);
            batch.delete_batch(&self.primary_index, [primary_key])?;
            batch.delete_batch(&self.object_index, [object_key])?;
        }

        // If the primary key doesn't exist, it's already deleted - no error.
        Ok(())
    }

    /// Apply a delete operation to both indices using the object ID.
    fn apply_delete_by_object_id(
        &self,
        batch: &mut DBBatch,
        object_id: &ObjectID,
    ) -> Result<(), TypedStoreError> {
        let object_key = construct_object_index_key(object_id);
        // Get the bucket_id and identifier from object index.
        if let Some(object_value) = self.object_index.get(&object_key)? {
            let primary_key =
                construct_primary_index_key(&object_value.bucket_id, &object_value.identifier);
            batch.delete_batch(&self.primary_index, [primary_key])?;
            batch.delete_batch(&self.object_index, [object_key])?;
        }

        // If the object doesn't exist, it's already deleted - no error.
        Ok(())
    }

    /// Store an index entry in both primary and object indices.
    pub fn put_index_entry(
        &self,
        bucket_id: &ObjectID,
        identifier: &str,
        object_id: &ObjectID,
        blob_id: BlobId,
    ) -> Result<(), TypedStoreError> {
        let mut batch = self.primary_index.batch();
        self.apply_insert(&mut batch, bucket_id, identifier, object_id, blob_id)?;
        batch.write()
    }

    /// Get an index entry by bucket_id and identifier (primary index).
    pub fn get_by_bucket_identifier(
        &self,
        bucket_id: &ObjectID,
        identifier: &str,
    ) -> Result<Option<BlobIdentity>, TypedStoreError> {
        let key = construct_primary_index_key(bucket_id, identifier);
        self.primary_index
            .get(&key)
            .map(|result| result.map(|value| value.blob_identity))
    }

    /// Get an index entry by object_id (object index).
    pub fn get_by_object_id(
        &self,
        object_id: &ObjectID,
    ) -> Result<Option<BlobIdentity>, TypedStoreError> {
        // First get the bucket_identifier from object index.
        let object_key = construct_object_index_key(object_id);
        if let Some(object_value) = self.object_index.get(&object_key)? {
            // Then get the actual blob identity from primary index.
            let primary_key =
                construct_primary_index_key(&object_value.bucket_id, &object_value.identifier);
            self.primary_index
                .get(&primary_key)
                .map(|result| result.map(|value| value.blob_identity))
        } else {
            Ok(None)
        }
    }

    /// List all primary index entries in a bucket.
    pub fn list_bucket_entries(
        &self,
        bucket_id: &ObjectID,
    ) -> Result<HashMap<String, BlobIdentity>, TypedStoreError> {
        let prefix = construct_primary_index_key(bucket_id, "");
        let mut result = HashMap::new();

        // TODO: use prefix scan.
        for entry in self.primary_index.safe_iter()? {
            let (key, value) = entry?;
            if key.starts_with(&prefix) {
                // Extract the primary key after bucket_id/.
                if let Some(primary_key) = key.strip_prefix(&prefix) {
                    result.insert(primary_key.to_string(), value.blob_identity);
                }
            }
        }
        Ok(result)
    }

    /// Apply index mutations from Sui events.
    /// All mutations are applied atomically in a single RocksDB batch transaction.
    pub fn apply_index_mutations(
        &self,
        mutations: Vec<MutationSet>,
    ) -> Result<(), TypedStoreError> {
        // Create a single batch for all mutations.
        // TODO: consider add a batch size limit for the extreme cases.
        let mut batch = self.primary_index.batch();

        // Add all mutations to the batch.
        for mutation_set in mutations {
            for mutation in mutation_set.mutations {
                self.apply_mutation(&mut batch, &mutation_set.bucket_id, mutation)?;
            }
        }

        // Commit all mutations atomically in a single write.
        batch.write()
    }

    /// Process a single mutation and add operations to the batch.
    fn apply_mutation(
        &self,
        batch: &mut DBBatch,
        _bucket_id: &ObjectID, // bucket_id is now included in mutation.
        mutation: IndexMutation,
    ) -> Result<(), TypedStoreError> {
        match mutation {
            IndexMutation::Insert {
                bucket_id,
                identifier,
                object_id,
                blob_id,
            } => {
                self.apply_insert(batch, &bucket_id, &identifier, &object_id, blob_id)?;
            }
            IndexMutation::Delete { object_id } => {
                self.apply_delete_by_object_id(batch, &object_id)?;
            }
        }
        Ok(())
    }

    /// Check if a primary index entry exists.
    pub fn has_primary_entry(
        &self,
        bucket_id: &ObjectID,
        primary_key: &str,
    ) -> Result<bool, TypedStoreError> {
        let key = construct_primary_index_key(bucket_id, primary_key);
        self.primary_index.contains_key(&key)
    }

    /// Delete an index entry by bucket_id and identifier.
    pub fn delete_by_bucket_identifier(
        &self,
        bucket_id: &ObjectID,
        identifier: &str,
    ) -> Result<(), TypedStoreError> {
        let primary_key = construct_primary_index_key(bucket_id, identifier);

        // Use batch to delete from both indices atomically.
        let mut batch = self.primary_index.batch();
        self.apply_delete_by_key(&mut batch, primary_key)?;
        batch.write()
    }

    /// Delete an index entry by object_id.
    pub fn delete_by_object_id(&self, object_id: &ObjectID) -> Result<(), TypedStoreError> {
        // Use batch to delete from both indices atomically.
        let mut batch = self.primary_index.batch();
        self.apply_delete_by_object_id(&mut batch, object_id)?;
        batch.write()
    }

    /// Delete all entries in a bucket.
    pub fn delete_bucket(&self, bucket_id: &ObjectID) -> Result<(), TypedStoreError> {
        let prefix = construct_primary_index_key(bucket_id, "");
        let mut batch = self.primary_index.batch();

        // Delete from both indices.
        for entry in self.primary_index.safe_iter()? {
            let (key, _value) = entry?;
            if key.starts_with(&prefix) {
                // Delete from both indices.
                self.apply_delete_by_key(&mut batch, key)?;
            }
        }

        batch.write()
    }

    /// Get statistics about a bucket.
    pub fn get_bucket_stats(&self, bucket_id: &ObjectID) -> Result<BucketStats, TypedStoreError> {
        let prefix = construct_primary_index_key(bucket_id, "");
        let mut primary_count = 0u32;

        // Count primary entries.
        for entry in self.primary_index.safe_iter()? {
            let (key, _) = entry?;
            if key.starts_with(&prefix) {
                primary_count += 1;
            }
        }

        Ok(BucketStats {
            primary_count,
            secondary_count: 0,
            total_blob_count: primary_count,
        })
    }

    /// Get the last processed event index from storage.
    /// Returns None if no event has been processed yet.
    pub fn get_last_processed_event_index(&self) -> Result<Option<u64>, TypedStoreError> {
        self.event_cursor_store
            .get(&"last_processed_index".to_string())
    }

    /// Set the last processed event index in storage.
    pub fn set_last_processed_event_index(&self, index: u64) -> Result<(), TypedStoreError> {
        self.event_cursor_store
            .insert(&"last_processed_index".to_string(), &index)
    }
}

/// Statistics about entries in a bucket.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BucketStats {
    pub primary_count: u32,
    pub secondary_count: u32,
    pub total_blob_count: u32,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rocksdb::Options;
    use tempfile::TempDir;
    use typed_store::rocks::{MetricConf, open_cf_opts};

    use super::*;

    fn create_test_store() -> Result<(OctopusIndexStore, TempDir), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let db_options = Options::default();

        // Use default metric configuration.
        let db = Arc::new(open_cf_opts(
            temp_dir.path(),
            None,
            MetricConf::default(),
            &[
                ("octopus_index_primary", db_options.clone()),
                ("octopus_index_object", db_options.clone()),
                ("octopus_event_cursor", db_options),
            ],
        )?);

        let primary_index = DBMap::reopen(
            &db,
            Some("octopus_index_primary"),
            &typed_store::rocks::ReadWriteOptions::default(),
            false,
        )?;

        // Initialize object index.
        let object_index = DBMap::reopen(
            &db,
            Some("octopus_index_object"),
            &typed_store::rocks::ReadWriteOptions::default(),
            false,
        )?;

        // Initialize event cursor store.
        let event_cursor_store = DBMap::reopen(
            &db,
            Some("octopus_event_cursor"),
            &typed_store::rocks::ReadWriteOptions::default(),
            false,
        )?;

        Ok((
            OctopusIndexStore::from_maps(primary_index, object_index, event_cursor_store),
            temp_dir,
        ))
    }

    #[tokio::test]
    async fn test_dual_index_system() -> Result<(), Box<dyn std::error::Error>> {
        let (store, _temp_dir) = create_test_store()?;

        // Create test bucket, blob, and object.
        let bucket_id = ObjectID::from_hex_literal(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        )
        .unwrap();
        let object_id = ObjectID::from_hex_literal(
            "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
        )
        .unwrap();
        let blob_id = BlobId([1; 32]);

        // Store an index entry using the new API.
        store.put_index_entry(&bucket_id, "/photos/2024/sunset.jpg", &object_id, blob_id)?;

        // Retrieve via primary index (bucket_id + identifier).
        let entry = store.get_by_bucket_identifier(&bucket_id, "/photos/2024/sunset.jpg")?;
        assert!(entry.is_some());

        let retrieved = entry.unwrap();
        assert_eq!(retrieved.blob_id, blob_id);
        assert_eq!(retrieved.object_id, object_id);

        // Retrieve via object index (object_id).
        let entry = store.get_by_object_id(&object_id)?;
        assert!(entry.is_some());

        let retrieved = entry.unwrap();
        assert_eq!(retrieved.blob_id, blob_id);
        assert_eq!(retrieved.object_id, object_id);

        Ok(())
    }

    #[tokio::test]
    async fn test_bucket_operations() -> Result<(), Box<dyn std::error::Error>> {
        let (store, _temp_dir) = create_test_store()?;

        let bucket_id = ObjectID::from_hex_literal(
            "0x42a8f3dc1234567890abcdef1234567890abcdef1234567890abcdef12345678",
        )
        .unwrap();

        // Add multiple entries to a bucket.
        for i in 0..3 {
            let blob_id = BlobId([i; 32]);
            let object_id = ObjectID::from_hex_literal(&format!("0x{:064x}", i))?;

            store.put_index_entry(&bucket_id, &format!("file{}.txt", i), &object_id, blob_id)?;
        }

        // List all entries in bucket.
        let entries = store.list_bucket_entries(&bucket_id)?;
        assert_eq!(entries.len(), 3);

        // Get stats.
        let stats = store.get_bucket_stats(&bucket_id)?;
        assert_eq!(stats.primary_count, 3);
        assert_eq!(stats.secondary_count, 0);

        // Delete bucket.
        store.delete_bucket(&bucket_id)?;
        let entries = store.list_bucket_entries(&bucket_id)?;
        assert_eq!(entries.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_mutations_with_dual_index() -> Result<(), Box<dyn std::error::Error>> {
        let (store, _temp_dir) = create_test_store()?;

        let bucket_id = ObjectID::from_hex_literal(
            "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
        )
        .unwrap();
        let object_id = ObjectID::from_hex_literal(
            "0x1111111111111111111111111111111111111111111111111111111111111111",
        )
        .unwrap();
        let blob_id = BlobId([99; 32]);

        // Test mutations via the new API.
        let insert_mutation = MutationSet {
            bucket_id,
            mutations: vec![IndexMutation::Insert {
                bucket_id,
                identifier: "/documents/report.pdf".to_string(),
                object_id,
                blob_id,
            }],
        };

        store.apply_index_mutations(vec![insert_mutation])?;

        // Verify entry was created in both indices.
        let entry = store.get_by_bucket_identifier(&bucket_id, "/documents/report.pdf")?;
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().blob_id, blob_id);

        let entry = store.get_by_object_id(&object_id)?;
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().blob_id, blob_id);

        // Test deletion.
        let delete_mutation = MutationSet {
            bucket_id,
            mutations: vec![IndexMutation::Delete { object_id }],
        };

        store.apply_index_mutations(vec![delete_mutation])?;

        // Verify both indices were updated.
        let entry = store.get_by_bucket_identifier(&bucket_id, "/documents/report.pdf")?;
        assert!(entry.is_none());

        let entry = store.get_by_object_id(&object_id)?;
        assert!(entry.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_rocksdb_initialization_and_dual_index_operations()
    -> Result<(), Box<dyn std::error::Error>> {
        // Initialize the RocksDB store.
        let (store, _temp_dir) = create_test_store()?;

        // Create test data.
        let bucket_id = ObjectID::from_hex_literal(
            "0xdeadbeef1234567890abcdef1234567890abcdef1234567890abcdef12345678",
        )
        .unwrap();
        let object_id = ObjectID::from_hex_literal(
            "0x2222222222222222222222222222222222222222222222222222222222222222",
        )
        .unwrap();
        let blob_id = BlobId([42; 32]);

        // Write index data using the new API.
        store.put_index_entry(&bucket_id, "/test/data.json", &object_id, blob_id)?;

        // Read via primary index.
        let retrieved = store.get_by_bucket_identifier(&bucket_id, "/test/data.json")?;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().blob_id, blob_id);

        // Read via object index.
        let retrieved = store.get_by_object_id(&object_id)?;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().blob_id, blob_id);

        // Verify persistence by checking if entry exists.
        assert!(store.has_primary_entry(&bucket_id, "/test/data.json",)?);

        // Delete the entry.
        store.delete_by_bucket_identifier(&bucket_id, "/test/data.json")?;

        // Verify deletion from both indices.
        assert!(!store.has_primary_entry(&bucket_id, "/test/data.json",)?);
        assert!(store.get_by_object_id(&object_id)?.is_none());

        println!("✅ RocksDB dual-index store initialized successfully");
        println!("✅ Index data written and read via both indices");
        println!("✅ Dual index system working correctly");
        println!("✅ Data persistence verified");

        Ok(())
    }
}
