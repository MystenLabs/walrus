// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Octopus Index implementation for efficient shard storage and retrieval.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use sui_types::base_types::ObjectID;
use typed_store::{rocks::DBMap, Map, TypedStoreError};
use walrus_core::BlobId;

/// Data structure representing a stored shard.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalrusIndexData {
    /// The blob this shard belongs to.
    pub blob_id: BlobId,
    
    /// The shard index within the blob.
    pub shard_index: u32,
    
    /// Size of the shard data.
    pub size: u64,
    
    /// The actual shard data.
    pub data: Vec<u8>,
    
    /// Optional metadata.
    pub metadata: ShardMetadata,
}

/// Metadata for a shard.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ShardMetadata {
    /// Object ID associated with this shard.
    pub object_id: Option<ObjectID>,
    
    /// Bucket ID where this shard is stored.
    pub bucket_id: Option<ObjectID>,
    
    /// Timestamp when this shard was stored.
    pub stored_at: Option<u64>,
}

/// Wrapper around the DBMap to provide convenient shard operations.
#[derive(Debug, Clone)]
pub struct OctopusIndexStore {
    db: DBMap<String, WalrusIndexData>,
}

impl OctopusIndexStore {
    pub fn new(db: DBMap<String, WalrusIndexData>) -> Self {
        Self { db }
    }

    /// Store a single shard.
    pub fn put_shard(
        &self,
        blob_id: &BlobId,
        shard_index: u32,
        data: &[u8],
    ) -> Result<(), TypedStoreError> {
        let key = format!("blob:{}:shard:{}", blob_id, shard_index);
        let value = WalrusIndexData {
            blob_id: *blob_id,
            shard_index,
            size: data.len() as u64,
            data: data.to_vec(),
            metadata: ShardMetadata::default(),
        };
        self.db.insert(&key, &value)
    }

    /// Get a single shard.
    pub fn get_shard(
        &self,
        blob_id: &BlobId,
        shard_index: u32,
    ) -> Result<Option<Vec<u8>>, TypedStoreError> {
        let key = format!("blob:{}:shard:{}", blob_id, shard_index);
        self.db.get(&key).map(|opt| opt.map(|v| v.data))
    }

    /// Store multiple shards.
    pub fn put_shards(
        &self,
        blob_id: &BlobId,
        shards: Vec<(u32, Vec<u8>)>,
    ) -> Result<(), TypedStoreError> {
        let mut batch = self.db.batch();
        for (shard_index, data) in shards {
            let key = format!("blob:{}:shard:{}", blob_id, shard_index);
            let value = WalrusIndexData {
                blob_id: *blob_id,
                shard_index,
                size: data.len() as u64,
                data,
                metadata: ShardMetadata::default(),
            };
            batch.insert_batch(&self.db, [(key, value)])?;
        }
        batch.write()
    }

    /// Get multiple shards.
    pub fn get_shards(
        &self,
        blob_id: &BlobId,
        indices: Vec<u32>,
    ) -> Result<HashMap<u32, Vec<u8>>, TypedStoreError> {
        let mut result = HashMap::new();
        for index in indices {
            if let Some(data) = self.get_shard(blob_id, index)? {
                result.insert(index, data);
            }
        }
        Ok(result)
    }

    /// Check if a shard exists.
    pub fn has_shard(
        &self,
        blob_id: &BlobId,
        shard_index: u32,
    ) -> Result<bool, TypedStoreError> {
        let key = format!("blob:{}:shard:{}", blob_id, shard_index);
        self.db.contains_key(&key)
    }

    /// Delete a single shard.
    pub fn delete_shard(
        &self,
        blob_id: &BlobId,
        shard_index: u32,
    ) -> Result<(), TypedStoreError> {
        let key = format!("blob:{}:shard:{}", blob_id, shard_index);
        self.db.remove(&key)
    }

    /// Delete all shards for a blob.
    pub fn delete_blob(&self, blob_id: &BlobId) -> Result<(), TypedStoreError> {
        let prefix = format!("blob:{}:shard:", blob_id);
        let mut batch = self.db.batch();
        
        // Iterate through all keys with this prefix and delete them
        for result in self.db.safe_iter()? {
            let (key, _) = result?;
            if key.starts_with(&prefix) {
                batch.delete_batch(&self.db, [key])?;
            }
        }
        batch.write()
    }
    
    /// Get statistics about stored shards.
    pub fn get_stats(&self, blob_id: &BlobId) -> Result<ShardStats, TypedStoreError> {
        let prefix = format!("blob:{}:shard:", blob_id);
        let mut shard_count = 0u32;
        let mut total_size = 0u64;
        let mut shard_indices = Vec::new();
        
        for result in self.db.safe_iter()? {
            let (key, value) = result?;
            if key.starts_with(&prefix) {
                shard_count += 1;
                total_size += value.size;
                shard_indices.push(value.shard_index);
            }
        }
        
        Ok(ShardStats {
            shard_count,
            total_size,
            shard_indices,
        })
    }
}

/// Statistics about shards for a blob.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardStats {
    pub shard_count: u32,
    pub total_size: u64,
    pub shard_indices: Vec<u32>,
}

/// Represents a mutation to be applied to the index.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MutationSet {
    pub blob_id: BlobId,
    pub mutations: Vec<IndexMutation>,
}

/// Individual mutation operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum IndexMutation {
    InsertShard { index: u32, data: Vec<u8> },
    DeleteShard { index: u32 },
    UpdateMetadata { metadata: ShardMetadata },
}