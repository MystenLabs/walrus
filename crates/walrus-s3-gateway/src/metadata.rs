// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! S3 metadata management for mapping S3 keys to Walrus blob IDs.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};
use crate::error::{S3Error, S3Result};

/// Metadata for an S3 object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectMetadata {
    /// The Walrus blob ID for this object (as string for serialization).
    pub blob_id: String,
    /// The original S3 key.
    pub key: String,
    /// The bucket name.
    pub bucket: String,
    /// Content type of the object.
    pub content_type: Option<String>,
    /// User-defined metadata.
    pub user_metadata: HashMap<String, String>,
    /// Size in bytes.
    pub size: u64,
    /// ETag (typically a hash of the content).
    pub etag: String,
    /// Last modified timestamp.
    pub last_modified: chrono::DateTime<chrono::Utc>,
}

impl ObjectMetadata {
    /// Create new object metadata.
    pub fn new(
        blob_id: String,
        key: String,
        bucket: String,
        content_type: Option<String>,
        user_metadata: HashMap<String, String>,
        size: u64,
        etag: String,
    ) -> Self {
        Self {
            blob_id,
            key,
            bucket,
            content_type,
            user_metadata,
            size,
            etag,
            last_modified: chrono::Utc::now(),
        }
    }
}

/// In-memory metadata store for S3 objects.
/// 
/// In a production environment, this would be backed by a persistent database
/// like PostgreSQL, SQLite, or a distributed key-value store.
#[derive(Debug, Clone)]
pub struct MetadataStore {
    /// Map from (bucket, key) to object metadata.
    objects: Arc<RwLock<HashMap<(String, String), ObjectMetadata>>>,
    /// Map from bucket to set of keys for listing operations.
    bucket_keys: Arc<RwLock<HashMap<String, Vec<String>>>>,
}

impl MetadataStore {
    /// Create a new metadata store.
    pub fn new() -> Self {
        Self {
            objects: Arc::new(RwLock::new(HashMap::new())),
            bucket_keys: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Store object metadata.
    pub async fn put_object(&self, metadata: ObjectMetadata) -> S3Result<()> {
        let key = (metadata.bucket.clone(), metadata.key.clone());
        
        // Store the metadata
        {
            let mut objects = self.objects.write().await;
            objects.insert(key, metadata.clone());
        }
        
        // Update bucket keys index
        {
            let mut bucket_keys = self.bucket_keys.write().await;
            let keys = bucket_keys.entry(metadata.bucket.clone()).or_default();
            if !keys.contains(&metadata.key) {
                keys.push(metadata.key.clone());
                keys.sort(); // Keep keys sorted for consistent listing
            }
        }
        
        Ok(())
    }

    /// Get object metadata.
    pub async fn get_object(&self, bucket: &str, key: &str) -> S3Result<Option<ObjectMetadata>> {
        let objects = self.objects.read().await;
        Ok(objects.get(&(bucket.to_string(), key.to_string())).cloned())
    }

    /// Delete object metadata.
    pub async fn delete_object(&self, bucket: &str, key: &str) -> S3Result<()> {
        let lookup_key = (bucket.to_string(), key.to_string());
        
        // Remove from objects
        {
            let mut objects = self.objects.write().await;
            objects.remove(&lookup_key);
        }
        
        // Remove from bucket keys index
        {
            let mut bucket_keys = self.bucket_keys.write().await;
            if let Some(keys) = bucket_keys.get_mut(bucket) {
                keys.retain(|k| k != key);
            }
        }
        
        Ok(())
    }

    /// List objects in a bucket.
    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        max_keys: i32,
        marker: Option<&str>,
    ) -> S3Result<(Vec<ObjectMetadata>, bool)> {
        let bucket_keys = self.bucket_keys.read().await;
        let objects = self.objects.read().await;
        
        let keys = bucket_keys.get(bucket).cloned().unwrap_or_default();
        
        let mut filtered_keys: Vec<String> = keys
            .into_iter()
            .filter(|key| {
                // Apply prefix filter
                if let Some(prefix) = prefix {
                    if !key.starts_with(prefix) {
                        return false;
                    }
                }
                
                // Apply marker filter (pagination)
                if let Some(marker) = marker {
                    if key.as_str() <= marker {
                        return false;
                    }
                }
                
                true
            })
            .collect();
        
        // Sort keys for consistent ordering
        filtered_keys.sort();
        
        // Apply max_keys limit
        let is_truncated = filtered_keys.len() > max_keys as usize;
        filtered_keys.truncate(max_keys as usize);
        
        // Get metadata for filtered keys
        let mut result = Vec::new();
        for key in filtered_keys {
            if let Some(metadata) = objects.get(&(bucket.to_string(), key)) {
                result.push(metadata.clone());
            }
        }
        
        Ok((result, is_truncated))
    }

    /// Check if an object exists.
    pub async fn object_exists(&self, bucket: &str, key: &str) -> bool {
        let objects = self.objects.read().await;
        objects.contains_key(&(bucket.to_string(), key.to_string()))
    }

    /// Get the blob ID for an object.
    pub async fn get_blob_id(&self, bucket: &str, key: &str) -> S3Result<Option<String>> {
        let objects = self.objects.read().await;
        Ok(objects.get(&(bucket.to_string(), key.to_string())).map(|meta| meta.blob_id.clone()))
    }

    /// Get all buckets (unique bucket names).
    pub async fn list_buckets(&self) -> S3Result<Vec<String>> {
        let bucket_keys = self.bucket_keys.read().await;
        let mut buckets: Vec<String> = bucket_keys.keys().cloned().collect();
        buckets.sort();
        Ok(buckets)
    }

    /// Check if a bucket exists.
    pub async fn bucket_exists(&self, bucket: &str) -> bool {
        let bucket_keys = self.bucket_keys.read().await;
        bucket_keys.contains_key(bucket)
    }

    /// Delete a bucket (only if empty).
    pub async fn delete_bucket(&self, bucket: &str) -> S3Result<()> {
        let mut bucket_keys = self.bucket_keys.write().await;
        
        if let Some(keys) = bucket_keys.get(bucket) {
            if !keys.is_empty() {
                return Err(S3Error::BucketNotEmpty);
            }
        }
        
        bucket_keys.remove(bucket);
        
        Ok(())
    }

    /// Create a bucket.
    pub async fn create_bucket(&self, bucket: &str) -> S3Result<()> {
        let mut bucket_keys = self.bucket_keys.write().await;
        
        if bucket_keys.contains_key(bucket) {
            return Err(S3Error::BucketAlreadyExists);
        }
        
        bucket_keys.insert(bucket.to_string(), Vec::new());
        
        Ok(())
    }
}

impl Default for MetadataStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use walrus_core::BlobId;

    #[tokio::test]
    async fn test_metadata_store() {
        let store = MetadataStore::new();
        
        // Test bucket creation
        store.create_bucket("test-bucket").await.unwrap();
        assert!(store.bucket_exists("test-bucket").await);
        
        // Test object storage
        let blob_id = BlobId([1u8; 32]);
        let blob_id_string = blob_id.to_string();
        let metadata = ObjectMetadata::new(
            blob_id_string.clone(),
            "test-key".to_string(),
            "test-bucket".to_string(),
            Some("text/plain".to_string()),
            HashMap::new(),
            100,
            "test-etag".to_string(),
        );
        
        store.put_object(metadata.clone()).await.unwrap();
        
        // Test object retrieval
        let retrieved = store.get_object("test-bucket", "test-key").await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().blob_id, blob_id_string);
        
        // Test object listing
        let (objects, truncated) = store.list_objects("test-bucket", None, 10, None).await.unwrap();
        assert_eq!(objects.len(), 1);
        assert!(!truncated);
        
        // Test object deletion
        store.delete_object("test-bucket", "test-key").await.unwrap();
        let retrieved = store.get_object("test-bucket", "test-key").await.unwrap();
        assert!(retrieved.is_none());
    }
}
