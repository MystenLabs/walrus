// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Storage layer for the Walrus Indexer (Octopus Index).
//!
//! This module implements the Octopus Index architecture for efficient indexing
//! of blobs and quilt patches. Indices are stored in RocksDB, organized by 'buckets'
//! that provide isolated namespaces similar to S3 buckets.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use sui_types::base_types::ObjectID;
use typed_store::{
    Map,
    TypedStoreError,
    rocks::{DBBatch, DBMap},
};
use walrus_core::BlobId;

/// Primary index value containing blob metadata and secondary indices
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrimaryIndexValue {
    /// The blob ID this entry points to
    pub blob_id: BlobId,
    /// Optional metadata associated with the blob
    pub metadata: HashMap<String, String>,
    /// Secondary index keys for this entry
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub secondary_indices: HashMap<String, Vec<String>>,
}

/// Secondary index value containing a list of blob IDs
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SecondaryIndexValue {
    /// List of blob IDs that match this secondary index
    pub blob_ids: Vec<BlobId>,
    /// Whether this is paginated (for large lists)
    pub is_paginated: bool,
    /// Current page number if paginated
    pub page_number: Option<u32>,
}

/// Index mutation operations that can be applied
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum IndexMutation {
    /// Insert a new index entry
    /// When index_name is None, writes to primary index
    Insert {
        index_name: Option<String>,
        index_key: String,
        index_value: String,
    },
    /// Delete an index entry
    /// When index_name is None, deletes from primary index
    Delete {
        index_name: Option<String>,
        index_key: String,
    },
}

/// Batch of mutations for a specific bucket
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MutationSet {
    pub bucket_id: ObjectID,
    pub mutations: Vec<IndexMutation>,
}

/// Storage interface for the Octopus Index
#[derive(Debug, Clone)]
pub struct OctopusIndexStore {
    /// Primary index: bucket_id/primary_key -> PrimaryIndexValue
    primary_index: DBMap<String, PrimaryIndexValue>,
    /// Secondary index: bucket_id/index_name/index_value -> SecondaryIndexValue
    secondary_index: DBMap<String, SecondaryIndexValue>,
}

/// Constants for pagination
const INLINE_STORAGE_THRESHOLD: usize = 100;
const ENTRIES_PER_PAGE: usize = 1000;

impl OctopusIndexStore {
    pub fn new(
        primary_index: DBMap<String, PrimaryIndexValue>,
        secondary_index: DBMap<String, SecondaryIndexValue>,
    ) -> Self {
        Self {
            primary_index,
            secondary_index,
        }
    }

    /// Store a primary index entry
    pub fn put_primary_index(
        &self,
        bucket_id: &ObjectID,
        primary_key: &str,
        blob_id: BlobId,
        metadata: HashMap<String, String>,
        secondary_indices: HashMap<String, Vec<String>>,
    ) -> Result<(), TypedStoreError> {
        let key = format!("{}/{}", bucket_id, primary_key);
        let value = PrimaryIndexValue {
            blob_id,
            metadata,
            secondary_indices: secondary_indices.clone(),
        };

        let mut batch = self.primary_index.batch();
        batch.insert_batch(&self.primary_index, [(key.clone(), value)])?;

        // Update secondary indices
        for (index_name, index_values) in secondary_indices {
            for index_value in index_values {
                self.update_secondary_index_batch(
                    &mut batch,
                    bucket_id,
                    &index_name,
                    &index_value,
                    blob_id,
                    true,
                )?;
            }
        }

        batch.write()
    }

    /// Get a primary index entry
    pub fn get_primary_index(
        &self,
        bucket_id: &ObjectID,
        primary_key: &str,
    ) -> Result<Option<PrimaryIndexValue>, TypedStoreError> {
        let key = format!("{}/{}", bucket_id, primary_key);
        self.primary_index.get(&key)
    }

    /// Get secondary index entries
    pub fn get_secondary_index(
        &self,
        bucket_id: &ObjectID,
        index_name: &str,
        index_value: &str,
    ) -> Result<Vec<BlobId>, TypedStoreError> {
        let base_key = format!("{}/{}/{}", bucket_id, index_name, index_value);

        // First try to get inline storage
        if let Some(value) = self.secondary_index.get(&base_key)? {
            if !value.is_paginated {
                return Ok(value.blob_ids);
            }
        }

        // If paginated, collect from all pages
        let mut all_blob_ids = Vec::new();
        let mut page = 0;
        loop {
            let page_key = format!("{}/{:04}", base_key, page);
            match self.secondary_index.get(&page_key)? {
                Some(page_value) => {
                    all_blob_ids.extend(page_value.blob_ids);
                    page += 1;
                }
                None => break,
            }
        }

        Ok(all_blob_ids)
    }

    /// Update secondary index (internal helper)
    fn update_secondary_index_batch(
        &self,
        batch: &mut DBBatch,
        bucket_id: &ObjectID,
        index_name: &str,
        index_value: &str,
        blob_id: BlobId,
        add: bool,
    ) -> Result<(), TypedStoreError> {
        let base_key = format!("{}/{}/{}", bucket_id, index_name, index_value);

        // Get current value
        let mut blob_ids = if let Some(current) = self.secondary_index.get(&base_key)? {
            if current.is_paginated {
                // Handle paginated case
                self.get_secondary_index(bucket_id, index_name, index_value)?
            } else {
                current.blob_ids
            }
        } else {
            Vec::new()
        };

        // Add or remove blob_id
        if add {
            if !blob_ids.contains(&blob_id) {
                blob_ids.push(blob_id);
            }
        } else {
            blob_ids.retain(|&id| id != blob_id);
        }

        // Store based on size
        if blob_ids.len() <= INLINE_STORAGE_THRESHOLD {
            // Use inline storage
            let value = SecondaryIndexValue {
                blob_ids,
                is_paginated: false,
                page_number: None,
            };
            batch.insert_batch(&self.secondary_index, [(base_key, value)])?;
        } else {
            // Use paginated storage
            let pages = blob_ids.chunks(ENTRIES_PER_PAGE);
            for (page_num, page_blob_ids) in pages.enumerate() {
                let page_key = format!("{}/{:04}", base_key, page_num);
                let value = SecondaryIndexValue {
                    blob_ids: page_blob_ids.to_vec(),
                    is_paginated: true,
                    page_number: Some(page_num as u32),
                };
                batch.insert_batch(&self.secondary_index, [(page_key, value)])?;
            }
        }

        Ok(())
    }

    /// List all primary index entries in a bucket
    pub fn list_bucket_entries(
        &self,
        bucket_id: &ObjectID,
    ) -> Result<HashMap<String, PrimaryIndexValue>, TypedStoreError> {
        let prefix = format!("{}/", bucket_id);
        let mut result = HashMap::new();

        for entry in self.primary_index.safe_iter()? {
            let (key, value) = entry?;
            if key.starts_with(&prefix) {
                // Extract the primary key after bucket_id/
                if let Some(primary_key) = key.strip_prefix(&prefix) {
                    result.insert(primary_key.to_string(), value);
                }
            }
        }
        Ok(result)
    }

    /// Apply index mutations from Sui events
    pub fn apply_index_mutations(
        &self,
        mutations: Vec<MutationSet>,
    ) -> Result<(), TypedStoreError> {
        // Process mutations directly without batch for simplicity
        for mutation_set in mutations {
            for mutation in mutation_set.mutations {
                match mutation {
                    IndexMutation::Insert {
                        index_name,
                        index_key,
                        index_value,
                    } => {
                        if let Some(index_name) = index_name {
                            // Secondary index operation
                            // Parse index_value as blob_id
                            if let Ok(blob_id_bytes) = hex::decode(&index_value) {
                                if blob_id_bytes.len() == 32 {
                                    let mut arr = [0u8; 32];
                                    arr.copy_from_slice(&blob_id_bytes);
                                    let blob_id = BlobId(arr);

                                    let base_key = format!(
                                        "{}/{}/{}",
                                        mutation_set.bucket_id, index_name, index_key
                                    );

                                    // Get current value
                                    let mut blob_ids = if let Some(current) =
                                        self.secondary_index.get(&base_key)?
                                    {
                                        current.blob_ids
                                    } else {
                                        Vec::new()
                                    };

                                    // Add blob_id if not present
                                    if !blob_ids.contains(&blob_id) {
                                        blob_ids.push(blob_id);
                                    }

                                    // Store updated value
                                    let value = SecondaryIndexValue {
                                        blob_ids,
                                        is_paginated: false,
                                        page_number: None,
                                    };
                                    self.secondary_index.insert(&base_key, &value)?;
                                }
                            }
                        } else {
                            // Primary index operation
                            // index_key is the primary key, index_value is the blob_id
                            if let Ok(blob_id_bytes) = hex::decode(&index_value) {
                                if blob_id_bytes.len() == 32 {
                                    let mut arr = [0u8; 32];
                                    arr.copy_from_slice(&blob_id_bytes);
                                    let blob_id = BlobId(arr);

                                    let key = format!("{}/{}", mutation_set.bucket_id, index_key);
                                    let value = PrimaryIndexValue {
                                        blob_id,
                                        metadata: HashMap::new(),
                                        secondary_indices: HashMap::new(),
                                    };
                                    self.primary_index.insert(&key, &value)?;
                                }
                            }
                        }
                    }
                    IndexMutation::Delete {
                        index_name,
                        index_key,
                    } => {
                        if let Some(index_name) = index_name {
                            // Secondary index deletion
                            let secondary_key =
                                format!("{}/{}/{}", mutation_set.bucket_id, index_name, index_key);
                            self.secondary_index.remove(&secondary_key)?;
                        } else {
                            // Primary index deletion
                            let key = format!("{}/{}", mutation_set.bucket_id, index_key);
                            self.primary_index.remove(&key)?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Check if a primary index entry exists
    pub fn has_primary_entry(
        &self,
        bucket_id: &ObjectID,
        primary_key: &str,
    ) -> Result<bool, TypedStoreError> {
        let key = format!("{}/{}", bucket_id, primary_key);
        self.primary_index.contains_key(&key)
    }

    /// Delete a primary index entry and its secondary indices
    pub fn delete_primary_entry(
        &self,
        bucket_id: &ObjectID,
        primary_key: &str,
    ) -> Result<(), TypedStoreError> {
        let key = format!("{}/{}", bucket_id, primary_key);

        // Get the entry to find secondary indices
        if let Some(entry) = self.primary_index.get(&key)? {
            let mut batch = self.primary_index.batch();

            // Delete primary index
            batch.delete_batch(&self.primary_index, [key])?;

            // Delete all secondary index references
            for (index_name, index_values) in entry.secondary_indices {
                for index_value in index_values {
                    self.update_secondary_index_batch(
                        &mut batch,
                        bucket_id,
                        &index_name,
                        &index_value,
                        entry.blob_id,
                        false,
                    )?;
                }
            }

            batch.write()
        } else {
            Ok(())
        }
    }

    /// Delete all entries in a bucket
    pub fn delete_bucket(&self, bucket_id: &ObjectID) -> Result<(), TypedStoreError> {
        let prefix = format!("{}/", bucket_id);
        let mut batch = self.primary_index.batch();

        // Delete primary indices
        for entry in self.primary_index.safe_iter()? {
            let (key, _) = entry?;
            if key.starts_with(&prefix) {
                batch.delete_batch(&self.primary_index, [key])?;
            }
        }

        // Delete secondary indices
        for entry in self.secondary_index.safe_iter()? {
            let (key, _) = entry?;
            if key.starts_with(&prefix) {
                batch.delete_batch(&self.secondary_index, [key])?;
            }
        }

        batch.write()
    }

    /// Get statistics about a bucket
    pub fn get_bucket_stats(&self, bucket_id: &ObjectID) -> Result<BucketStats, TypedStoreError> {
        let prefix = format!("{}/", bucket_id);
        let mut primary_count = 0u32;
        let mut secondary_count = 0u32;
        let mut total_blob_count = 0u32;

        // Count primary entries
        for entry in self.primary_index.safe_iter()? {
            let (key, _) = entry?;
            if key.starts_with(&prefix) {
                primary_count += 1;
                total_blob_count += 1;
            }
        }

        // Count secondary entries
        for entry in self.secondary_index.safe_iter()? {
            let (key, value) = entry?;
            if key.starts_with(&prefix) {
                secondary_count += 1;
                total_blob_count += value.blob_ids.len() as u32;
            }
        }

        Ok(BucketStats {
            primary_count,
            secondary_count,
            total_blob_count,
        })
    }
}

/// Statistics about entries in a bucket
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

        // Use default metric configuration
        let db = Arc::new(open_cf_opts(
            temp_dir.path(),
            None,
            MetricConf::default(),
            &[
                ("octopus_index_primary", db_options.clone()),
                ("octopus_index_secondary", db_options),
            ],
        )?);

        let primary_index = DBMap::reopen(
            &db,
            Some("octopus_index_primary"),
            &typed_store::rocks::ReadWriteOptions::default(),
            false,
        )?;

        let secondary_index = DBMap::reopen(
            &db,
            Some("octopus_index_secondary"),
            &typed_store::rocks::ReadWriteOptions::default(),
            false,
        )?;

        Ok((
            OctopusIndexStore::new(primary_index, secondary_index),
            temp_dir,
        ))
    }

    #[tokio::test]
    async fn test_primary_index() -> Result<(), Box<dyn std::error::Error>> {
        let (store, _temp_dir) = create_test_store()?;

        // Create test bucket and blob
        let bucket_id = ObjectID::from_hex_literal(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        )
        .unwrap();
        let blob_id = BlobId([1; 32]);

        // Store a primary index entry with secondary indices
        let mut metadata = HashMap::new();
        metadata.insert("type".to_string(), "image".to_string());
        metadata.insert("size".to_string(), "1024".to_string());

        let mut secondary_indices = HashMap::new();
        secondary_indices.insert("type".to_string(), vec!["jpg".to_string()]);
        secondary_indices.insert("date".to_string(), vec!["2024-01-15".to_string()]);

        store.put_primary_index(
            &bucket_id,
            "/photos/2024/sunset.jpg",
            blob_id,
            metadata.clone(),
            secondary_indices,
        )?;

        // Retrieve the primary index
        let entry = store.get_primary_index(&bucket_id, "/photos/2024/sunset.jpg")?;
        assert!(entry.is_some());

        let retrieved = entry.unwrap();
        assert_eq!(retrieved.blob_id, blob_id);
        assert_eq!(retrieved.metadata, metadata);

        // Verify secondary indices were created
        let type_blobs = store.get_secondary_index(&bucket_id, "type", "jpg")?;
        assert_eq!(type_blobs, vec![blob_id]);

        let date_blobs = store.get_secondary_index(&bucket_id, "date", "2024-01-15")?;
        assert_eq!(date_blobs, vec![blob_id]);

        Ok(())
    }

    #[tokio::test]
    async fn test_secondary_index_multiple_blobs() -> Result<(), Box<dyn std::error::Error>> {
        let (store, _temp_dir) = create_test_store()?;

        let bucket_id = ObjectID::from_hex_literal(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        )
        .unwrap();

        // Add multiple blobs with the same secondary index
        for i in 0..3 {
            let blob_id = BlobId([i; 32]);
            let mut secondary_indices = HashMap::new();
            secondary_indices.insert("type".to_string(), vec!["jpg".to_string()]);
            secondary_indices.insert("location".to_string(), vec!["california".to_string()]);

            store.put_primary_index(
                &bucket_id,
                &format!("/photos/img{}.jpg", i),
                blob_id,
                HashMap::new(),
                secondary_indices,
            )?;
        }

        // Verify secondary index contains all blobs
        let type_blobs = store.get_secondary_index(&bucket_id, "type", "jpg")?;
        assert_eq!(type_blobs.len(), 3);
        assert!(type_blobs.contains(&BlobId([0; 32])));
        assert!(type_blobs.contains(&BlobId([1; 32])));
        assert!(type_blobs.contains(&BlobId([2; 32])));

        let location_blobs = store.get_secondary_index(&bucket_id, "location", "california")?;
        assert_eq!(location_blobs.len(), 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_bucket_operations() -> Result<(), Box<dyn std::error::Error>> {
        let (store, _temp_dir) = create_test_store()?;

        let bucket_id = ObjectID::from_hex_literal(
            "0x42a8f3dc1234567890abcdef1234567890abcdef1234567890abcdef12345678",
        )
        .unwrap();

        // Add multiple entries to a bucket
        for i in 0..3 {
            let blob_id = BlobId([i; 32]);
            let mut secondary_indices = HashMap::new();
            secondary_indices.insert("type".to_string(), vec!["document".to_string()]);

            store.put_primary_index(
                &bucket_id,
                &format!("file{}.txt", i),
                blob_id,
                HashMap::new(),
                secondary_indices,
            )?;
        }

        // List all entries in bucket
        let entries = store.list_bucket_entries(&bucket_id)?;
        assert_eq!(entries.len(), 3);

        // Get stats
        let stats = store.get_bucket_stats(&bucket_id)?;
        assert_eq!(stats.primary_count, 3);
        assert!(stats.secondary_count > 0);

        // Delete bucket
        store.delete_bucket(&bucket_id)?;
        let entries = store.list_bucket_entries(&bucket_id)?;
        assert_eq!(entries.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_mutations_with_primary_index() -> Result<(), Box<dyn std::error::Error>> {
        // Use the same setup as other passing tests
        let (store, _temp_dir) = create_test_store()?;

        let bucket_id = ObjectID::from_hex_literal(
            "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
        )
        .unwrap();
        let blob_id = BlobId([99; 32]);

        // First test direct primary index write (this works in other tests)
        store.put_primary_index(
            &bucket_id,
            "/test/direct.pdf",
            blob_id,
            HashMap::new(),
            HashMap::new(),
        )?;

        // Verify it was stored
        let entry = store.get_primary_index(&bucket_id, "/test/direct.pdf")?;
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().blob_id, blob_id);

        // Now test via mutations
        let primary_mutation = MutationSet {
            bucket_id,
            mutations: vec![IndexMutation::Insert {
                index_name: None, // Primary index
                index_key: "/documents/report.pdf".to_string(),
                index_value: hex::encode(blob_id.0),
            }],
        };

        store.apply_index_mutations(vec![primary_mutation])?;

        // Verify primary index was created
        let entry = store.get_primary_index(&bucket_id, "/documents/report.pdf")?;
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().blob_id, blob_id);

        // Test secondary index mutation (index_name = Some)
        let secondary_mutation = MutationSet {
            bucket_id,
            mutations: vec![IndexMutation::Insert {
                index_name: Some("type".to_string()),
                index_key: "pdf".to_string(),
                index_value: hex::encode(blob_id.0),
            }],
        };

        store.apply_index_mutations(vec![secondary_mutation])?;

        // Verify secondary index was created
        let pdf_blobs = store.get_secondary_index(&bucket_id, "type", "pdf")?;
        assert_eq!(pdf_blobs, vec![blob_id]);

        // Test deletion with None (primary index)
        let delete_primary = MutationSet {
            bucket_id,
            mutations: vec![IndexMutation::Delete {
                index_name: None,
                index_key: "/documents/report.pdf".to_string(),
            }],
        };

        store.apply_index_mutations(vec![delete_primary])?;

        // Verify primary index was deleted
        let entry = store.get_primary_index(&bucket_id, "/documents/report.pdf")?;
        assert!(entry.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_rocksdb_initialization_and_index_operations()
    -> Result<(), Box<dyn std::error::Error>> {
        // Initialize the RocksDB store
        let (store, _temp_dir) = create_test_store()?;

        // Create test data
        let bucket_id = ObjectID::from_hex_literal(
            "0xdeadbeef1234567890abcdef1234567890abcdef1234567890abcdef12345678",
        )
        .unwrap();
        let blob_id = BlobId([42; 32]);

        // Prepare metadata and secondary indices
        let mut metadata = HashMap::new();
        metadata.insert("content-type".to_string(), "application/json".to_string());
        metadata.insert("created".to_string(), "2024-01-20".to_string());

        let mut secondary_indices = HashMap::new();
        secondary_indices.insert(
            "tags".to_string(),
            vec!["test".to_string(), "demo".to_string()],
        );
        secondary_indices.insert("category".to_string(), vec!["development".to_string()]);

        // Write index data
        store.put_primary_index(
            &bucket_id,
            "/test/data.json",
            blob_id,
            metadata.clone(),
            secondary_indices.clone(),
        )?;

        // Read primary index data
        let retrieved = store.get_primary_index(&bucket_id, "/test/data.json")?;
        assert!(retrieved.is_some());

        let primary_data = retrieved.unwrap();
        assert_eq!(primary_data.blob_id, blob_id);
        assert_eq!(primary_data.metadata, metadata);
        assert_eq!(primary_data.secondary_indices.len(), 2);

        // Read secondary index data
        let tag_blobs = store.get_secondary_index(&bucket_id, "tags", "test")?;
        assert_eq!(tag_blobs, vec![blob_id]);

        let category_blobs = store.get_secondary_index(&bucket_id, "category", "development")?;
        assert_eq!(category_blobs, vec![blob_id]);

        // Verify persistence by checking if entry exists
        assert!(store.has_primary_entry(&bucket_id, "/test/data.json")?);

        // Delete the entry
        store.delete_primary_entry(&bucket_id, "/test/data.json")?;

        // Verify deletion
        assert!(!store.has_primary_entry(&bucket_id, "/test/data.json")?);
        let tag_blobs_after = store.get_secondary_index(&bucket_id, "tags", "test")?;
        assert!(tag_blobs_after.is_empty());

        println!("✅ RocksDB store initialized successfully");
        println!("✅ Index data written and read successfully");
        println!("✅ Secondary indices working correctly");
        println!("✅ Data persistence verified");

        Ok(())
    }
}
