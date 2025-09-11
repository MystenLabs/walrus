// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Storage layer for the Walrus Indexer.
//!
//! This module implements the Walrus Index architecture for efficient indexing
//! of blobs and quilt patches. Indices are stored in RocksDB, organized by 'buckets'
//! that provide isolated namespaces similar to S3 buckets.

use std::{collections::HashMap, path::Path, str::FromStr, sync::Arc};

use anyhow::Result;
use rocksdb::Options;
use serde::{Deserialize, Serialize};
use sui_types::base_types::ObjectID;
use typed_store::{
    Map,
    TypedStoreError,
    rocks::{DBBatch, DBMap, MetricConf, open_cf_opts},
};
use walrus_core::{BlobId, QuiltPatchId, metadata::QuiltIndex};
use walrus_sdk::client::client_types::get_stored_quilt_patches;
use walrus_sui::client::SuiReadClient;

use crate::indexer::{QuiltIndexTask, QuiltIndexTaskId};

// Column family names
const CF_NAME_PRIMARY_INDEX: &str = "walrus_index_primary";
const CF_NAME_OBJECT_INDEX: &str = "walrus_index_object";
const CF_NAME_EVENT_CURSOR: &str = "walrus_event_cursor";
const CF_NAME_QUILT_PATCH_INDEX: &str = "walrus_quilt_patch";
const CF_NAME_PENDING_QUILT_INDEX_TASKS: &str = "walrus_pending_quilt_index_tasks";

/// The target of an index entry, it could be a blob, a quilt patch or a quilt.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum IndexTarget {
    /// Blob ID.
    Blob(BlobIdentity),
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
    /// Whether the blob is a quilt.
    pub is_quilt: bool,
}

fn construct_primary_index_key(bucket_id: &ObjectID, identifier: &str) -> String {
    format!("{}/{}", bucket_id, identifier)
}

fn construct_object_index_key(object_id: &ObjectID) -> String {
    object_id.to_string()
}

fn construct_quilt_patch_index_key(patch_blob_id: &BlobId, object_id: &ObjectID) -> String {
    // Use a simpler format with a delimiter that won't appear in base64
    format!("{}#{}", patch_blob_id, object_id)
}

fn get_quilt_patch_key_bounds(patch_blob_id: &BlobId) -> (String, String) {
    // The key format is "{blob_id}#{object_id}"
    // To get all keys with this blob_id, we need:
    // - Start: "{blob_id}#" (inclusive)
    // - End: "{blob_id}$" (exclusive, $ comes after # in ASCII)
    let start = format!("{}#", patch_blob_id);
    let end = format!("{}$", patch_blob_id);
    (start, end)
}

fn parse_quilt_patch_index_key(key: &String) -> (BlobId, ObjectID) {
    let parts = key.split('#').collect::<Vec<&str>>();
    let patch_blob_id = BlobId::from_str(parts[0]).unwrap();
    let object_id = ObjectID::from_str(parts[1]).unwrap();
    (patch_blob_id, object_id)
}

/// Value stored in the object index.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectIndexValue {
    /// The bucket ID.
    pub bucket_id: ObjectID,
    /// The identifier.
    pub identifier: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QuiltIndexTaskValue {
    pub bucket_id: ObjectID,
    pub identifier: String,
    pub object_id: ObjectID,
}

/// Storage interface for the Walrus Index (Dual Index System).
#[derive(Debug, Clone)]
pub struct WalrusIndexStore {
    /// Primary index: bucket_id/identifier -> IndexTarget.
    primary_index: DBMap<String, IndexTarget>,
    /// Object index: object_id -> bucket_id/identifier.
    object_index: DBMap<String, ObjectIndexValue>,
    /// Event cursor store: stores the last processed event index for resumption.
    event_cursor_store: DBMap<String, u64>,
    /// Index for quilt patches, so that we can look up quilt patches by the corresponding
    /// files' blob IDs.
    quilt_patch_index: DBMap<String, QuiltPatchId>,
    /// Pending quilt index tasks: stores the quilt index tasks that are pending to be processed.
    pending_quilt_index_tasks: DBMap<QuiltIndexTaskId, QuiltIndexTaskValue>,
    /// Walrus read client.
    read_client: Option<Arc<SuiReadClient>>,
}

// Constants for future pagination implementation.
// const INLINE_STORAGE_THRESHOLD: usize = 100;
// const ENTRIES_PER_PAGE: usize = 1000;

impl WalrusIndexStore {
    /// Create a new WalrusIndexStore by opening the database at the specified path.
    /// This initializes all required column families for the indexer.
    pub async fn open(db_path: &Path) -> Result<Self> {
        let db_options = Options::default();
        let db = Arc::new(open_cf_opts(
            db_path,
            None,
            MetricConf::default(),
            &[
                (CF_NAME_PRIMARY_INDEX, db_options.clone()),
                (CF_NAME_OBJECT_INDEX, db_options.clone()),
                (CF_NAME_EVENT_CURSOR, db_options.clone()),
                (CF_NAME_QUILT_PATCH_INDEX, db_options.clone()),
                (CF_NAME_PENDING_QUILT_INDEX_TASKS, db_options.clone()),
            ],
        )?);

        let primary_index: DBMap<String, IndexTarget> = DBMap::reopen(
            &db,
            Some(CF_NAME_PRIMARY_INDEX),
            &typed_store::rocks::ReadWriteOptions::default(),
            false,
        )?;

        let object_index: DBMap<String, ObjectIndexValue> = DBMap::reopen(
            &db,
            Some(CF_NAME_OBJECT_INDEX),
            &typed_store::rocks::ReadWriteOptions::default(),
            false,
        )?;

        let event_cursor_store: DBMap<String, u64> = DBMap::reopen(
            &db,
            Some(CF_NAME_EVENT_CURSOR),
            &typed_store::rocks::ReadWriteOptions::default(),
            false,
        )?;

        let quilt_patch_index: DBMap<String, QuiltPatchId> = DBMap::reopen(
            &db,
            Some(CF_NAME_QUILT_PATCH_INDEX),
            &typed_store::rocks::ReadWriteOptions::default(),
            false,
        )?;

        let pending_quilt_index_tasks: DBMap<QuiltIndexTaskId, QuiltIndexTaskValue> =
            DBMap::reopen(
                &db,
                Some(CF_NAME_PENDING_QUILT_INDEX_TASKS),
                &typed_store::rocks::ReadWriteOptions::default(),
                false,
            )?;

        Ok(Self {
            primary_index,
            object_index,
            event_cursor_store,
            quilt_patch_index,
            pending_quilt_index_tasks,
            read_client: None,
        })
    }

    /// Create a new WalrusIndexStore with a Sui read client.
    pub fn with_read_client(self, read_client: Arc<SuiReadClient>) -> Self {
        Self {
            read_client: Some(read_client),
            ..self
        }
    }

    /// Create a new WalrusIndexStore from existing DBMap instances.
    /// This is used for testing and migration purposes.
    pub fn from_maps(
        primary_index: DBMap<String, IndexTarget>,
        object_index: DBMap<String, ObjectIndexValue>,
        event_cursor_store: DBMap<String, u64>,
        quilt_patch_index: DBMap<String, QuiltPatchId>,
        pending_quilt_index_tasks: DBMap<QuiltIndexTaskId, QuiltIndexTaskValue>,
    ) -> Self {
        Self {
            primary_index,
            object_index,
            event_cursor_store,
            quilt_patch_index,
            pending_quilt_index_tasks,
            read_client: None,
        }
    }

    /// Apply index mutations from Sui events.
    /// All mutations are applied atomically in a single RocksDB batch transaction.
    pub fn apply_index_mutations(
        &self,
        mutation_sets: Vec<walrus_sui::types::IndexMutationSet>,
    ) -> Result<(), TypedStoreError> {
        self.apply_index_mutations_with_cursor(mutation_sets, None)
    }

    /// Apply index mutations from Sui events with optional cursor update.
    /// All mutations and cursor update are applied atomically in a single RocksDB batch
    /// transaction.
    /// This ensures that mutations and cursor progression are consistent.
    pub fn apply_index_mutations_with_cursor(
        &self,
        mutation_sets: Vec<walrus_sui::types::IndexMutationSet>,
        cursor_update: Option<u64>,
    ) -> Result<(), TypedStoreError> {
        // Create a single batch for all mutations and cursor update.
        // TODO: consider add a batch size limit for the extreme cases.
        let mut batch = self.primary_index.batch();

        // Add all mutations to the batch.
        for mutation_set in mutation_sets {
            for mutation in mutation_set.mutations {
                self.apply_mutation(&mut batch, &mutation_set.bucket_id, mutation, cursor_update)?;
            }
        }

        // Add cursor update to the same batch if provided.
        if let Some(cursor_value) = cursor_update {
            batch.insert_batch(
                &self.event_cursor_store,
                [("last_processed_index".to_string(), cursor_value)],
            )?;
        }

        // Commit all mutations and cursor update atomically in a single write.
        batch.write()
    }

    /// Process a single mutation and add operations to the batch.
    fn apply_mutation(
        &self,
        batch: &mut DBBatch,
        bucket_id: &ObjectID, // bucket_id comes from the MutationSet
        mutation: walrus_sui::types::IndexMutation,
        cursor_update: Option<u64>,
    ) -> Result<(), TypedStoreError> {
        match mutation {
            walrus_sui::types::IndexMutation::Insert {
                identifier,
                object_id,
                blob_id,
                is_quilt,
            } => {
                self.apply_insert(batch, bucket_id, &identifier, &object_id, blob_id, is_quilt)?;
            }
            walrus_sui::types::IndexMutation::Delete {
                object_id,
                is_quilt: _,
            } => {
                self.apply_delete_by_object_id(batch, &object_id)?;
            }
        }
        Ok(())
    }

    /// Apply an insert operation to both indices.
    pub fn apply_insert(
        &self,
        batch: &mut DBBatch,
        bucket_id: &ObjectID,
        identifier: &str,
        object_id: &ObjectID,
        blob_id: BlobId,
        is_quilt: bool,
    ) -> Result<(), TypedStoreError> {
        // Primary index: bucket_id/identifier -> IndexTarget.
        let primary_key = construct_primary_index_key(bucket_id, identifier);
        let blob_identity = BlobIdentity {
            blob_id,
            object_id: *object_id,
            is_quilt,
        };
        let index_target = IndexTarget::Blob(blob_identity);

        // Object index: object_id -> bucket_id/identifier.
        let object_key = construct_object_index_key(object_id);
        let object_value = ObjectIndexValue {
            bucket_id: *bucket_id,
            identifier: identifier.to_string(),
        };

        batch.insert_batch(&self.primary_index, [(primary_key, index_target)])?;
        batch.insert_batch(&self.object_index, [(object_key, object_value)])?;

        Ok(())
    }

    pub fn populate_quilt_patch_index(
        &self,
        quilt_index_task: &QuiltIndexTask,
        quilt_index: &QuiltIndex,
    ) -> Result<(), TypedStoreError> {
        let mut batch = self.primary_index.batch();

        self.apply_insert(
            &mut batch,
            &quilt_index_task.bucket_id,
            &quilt_index_task.identifier,
            &quilt_index_task.object_id,
            quilt_index_task.quilt_blob_id,
            true,
        )?;

        for patch in get_stored_quilt_patches(quilt_index, quilt_index_task.quilt_blob_id) {
            let primary_key =
                construct_primary_index_key(&quilt_index_task.bucket_id, &patch.identifier);
            let quilt_patch_id = QuiltPatchId::from_str(&patch.quilt_patch_id)
                .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;
            let primary_value = IndexTarget::QuiltPatchId(quilt_patch_id.clone());

            batch.insert_batch(&self.primary_index, [(primary_key, primary_value)])?;

            if let Some(patch_blob_id) = patch.patch_blob_id {
                let quilt_patch_index_key =
                    construct_quilt_patch_index_key(&patch_blob_id, &quilt_index_task.object_id);
                batch.insert_batch(
                    &self.quilt_patch_index,
                    [(quilt_patch_index_key, quilt_patch_id)],
                )?;
            }
        }

        batch.delete_batch(
            &self.pending_quilt_index_tasks,
            [QuiltIndexTaskId::new(
                quilt_index_task.sequence_number,
                quilt_index_task.quilt_blob_id,
            )],
        )?;

        batch.write()
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

    /// Apply a delete operation to both indices using the primary key.
    fn apply_delete_by_key(
        &self,
        batch: &mut DBBatch,
        primary_key: String,
    ) -> Result<(), TypedStoreError> {
        // Look up the primary value to get the object_id.
        if let Some(index_target) = self.primary_index.get(&primary_key)? {
            // Extract object_id based on the IndexTarget variant
            let object_id = match &index_target {
                IndexTarget::Blob(blob_identity) => &blob_identity.object_id,
                IndexTarget::QuiltPatchId(_) => {
                    // For now, we don't handle QuiltPatchId deletion
                    return Ok(());
                }
                IndexTarget::QuiltId(quilt_id) => quilt_id,
            };
            let object_key = construct_object_index_key(object_id);
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

    /// Get an index entry by bucket_id and identifier (primary index).
    pub fn get_by_bucket_identifier(
        &self,
        bucket_id: &ObjectID,
        identifier: &str,
    ) -> Result<Option<IndexTarget>, TypedStoreError> {
        let key = construct_primary_index_key(bucket_id, identifier);
        self.primary_index.get(&key)
    }

    pub fn get_quilt_patch_id_by_blob_id(
        &self,
        blob_id: &BlobId,
    ) -> Result<Option<QuiltPatchId>, TypedStoreError> {
        tracing::debug!("Querying quilt patch for blob_id: {}", blob_id);
        let (start_key, end_key) = get_quilt_patch_key_bounds(blob_id);
        tracing::debug!("Using bounds: start='{}', end='{}'", start_key, end_key);

        // Use bounded iteration to find all entries with this blob_id
        for entry in self
            .quilt_patch_index
            .safe_iter_with_bounds(Some(start_key), Some(end_key))?
        {
            let (key, value) = entry?;
            tracing::debug!("Found matching key: '{}', value: {:?}", key, value);
            // Since we're using proper bounds, any key in this range should match
            // but let's verify just to be safe
            let (patch_blob_id, _object_id) = parse_quilt_patch_index_key(&key);
            if patch_blob_id == *blob_id {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    /// Get quilt patch ID by both blob_id and object_id (direct lookup).
    pub fn get_quilt_patch_id_by_blob_and_object(
        &self,
        blob_id: &BlobId,
        object_id: &ObjectID,
    ) -> Result<Option<QuiltPatchId>, TypedStoreError> {
        let key = construct_quilt_patch_index_key(blob_id, object_id);
        self.quilt_patch_index.get(&key)
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
            match self.primary_index.get(&primary_key)? {
                Some(IndexTarget::Blob(blob_identity)) => Ok(Some(blob_identity)),
                Some(_) => Ok(None), // Other variants are not BlobIdentity
                None => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    /// List all primary index entries in a bucket.
    pub fn list_blobs_in_bucket_entries(
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
                    // Only include Blob variants in the result
                    if let IndexTarget::Blob(blob_identity) = value {
                        result.insert(primary_key.to_string(), blob_identity);
                    }
                }
            }
        }
        Ok(result)
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

    /// Get all entries from the quilt patch index.
    /// Returns a vector of (key, value) pairs.
    pub fn get_all_quilt_patch_entries(
        &self,
    ) -> Result<Vec<(String, QuiltPatchId)>, TypedStoreError> {
        let mut entries = Vec::new();
        for entry in self.quilt_patch_index.safe_iter()? {
            let (key, value) = entry?;
            entries.push((key, value));
        }
        Ok(entries)
    }

    /// Get all entries from the pending quilt index tasks.
    /// Returns a vector of (key, value) pairs.
    pub fn get_all_pending_quilt_tasks(
        &self,
    ) -> Result<Vec<(QuiltIndexTaskId, QuiltIndexTaskValue)>, TypedStoreError> {
        let mut entries = Vec::new();
        for entry in self.pending_quilt_index_tasks.safe_iter()? {
            let (key, value) = entry?;
            entries.push((key, value));
        }
        Ok(entries)
    }
}

/// Statistics about entries in a bucket.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BucketStats {
    pub primary_count: u32,
    pub secondary_count: u32,
    pub total_blob_count: u32,
}

/// Implementation of OrderedStore<QuiltIndexTask> for WalrusIndexStore.
/// This allows the store to be used with AsyncTaskManager for quilt tasks.
#[async_trait::async_trait]
impl crate::OrderedStore<crate::indexer::QuiltIndexTask> for WalrusIndexStore {
    /// Persist a quilt task to storage.
    async fn store(&self, task: &crate::indexer::QuiltIndexTask) -> Result<(), TypedStoreError> {
        let key = QuiltIndexTaskId::new(task.event_index, task.quilt_blob_id);
        let value = QuiltIndexTaskValue {
            object_id: task.object_id,
            bucket_id: task.bucket_id,
            identifier: task.identifier.clone(),
        };
        self.pending_quilt_index_tasks.insert(&key, &value)
    }

    /// Remove a quilt task from storage by its task ID.
    async fn remove(&self, task_id: &QuiltIndexTaskId) -> Result<(), TypedStoreError> {
        self.pending_quilt_index_tasks.remove(task_id)
    }

    /// Load quilt tasks within a sequence range.
    async fn read_range(
        &self,
        from_seq: Option<u64>,
        to_seq: Option<u64>,
        limit: usize,
    ) -> Result<Vec<crate::indexer::QuiltIndexTask>, TypedStoreError> {
        let from_index = from_seq.unwrap_or(0);
        let mut tasks = Vec::new();
        let mut count = 0;

        for entry in self.pending_quilt_index_tasks.safe_iter()? {
            let (key, value) = entry?;

            if key.sequence <= from_index {
                continue;
            }

            if let Some(to_seq) = to_seq {
                if key.sequence > to_seq {
                    break;
                }
            }

            tasks.push((key, value));
            count += 1;

            if count >= limit {
                break;
            }
        }

        // Sort by index to ensure proper ordering
        tasks.sort_by_key(|(key, _)| key.sequence);

        // Convert storage format to QuiltIndexTask
        let mut result = Vec::new();
        for (key, value) in tasks {
            let task = crate::indexer::QuiltIndexTask::new(
                key.sequence, // Using sequence from key
                key.quilt_id,
                value.object_id,
                value.bucket_id,
                value.identifier,
                key.sequence, // Using sequence as event_index
            );
            result.push(task);
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rocksdb::Options;
    use tempfile::TempDir;
    use typed_store::rocks::{MetricConf, open_cf_opts};

    use super::*;

    fn create_test_store() -> Result<(WalrusIndexStore, TempDir), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let db_options = Options::default();

        // Use default metric configuration.
        let db = Arc::new(open_cf_opts(
            temp_dir.path(),
            None,
            MetricConf::default(),
            &[
                (CF_NAME_PRIMARY_INDEX, db_options.clone()),
                (CF_NAME_OBJECT_INDEX, db_options.clone()),
                (CF_NAME_EVENT_CURSOR, db_options.clone()),
                (CF_NAME_QUILT_PATCH_INDEX, db_options.clone()),
                (CF_NAME_PENDING_QUILT_INDEX_TASKS, db_options.clone()),
            ],
        )?);

        let primary_index = DBMap::reopen(
            &db,
            Some(CF_NAME_PRIMARY_INDEX),
            &typed_store::rocks::ReadWriteOptions::default(),
            false,
        )?;

        // Initialize object index.
        let object_index = DBMap::reopen(
            &db,
            Some(CF_NAME_OBJECT_INDEX),
            &typed_store::rocks::ReadWriteOptions::default(),
            false,
        )?;

        // Initialize event cursor store.
        let event_cursor_store = DBMap::reopen(
            &db,
            Some(CF_NAME_EVENT_CURSOR),
            &typed_store::rocks::ReadWriteOptions::default(),
            false,
        )?;

        let quilt_patch_index = DBMap::reopen(
            &db,
            Some(CF_NAME_QUILT_PATCH_INDEX),
            &typed_store::rocks::ReadWriteOptions::default(),
            false,
        )?;

        let pending_quilt_index_tasks = DBMap::reopen(
            &db,
            Some(CF_NAME_PENDING_QUILT_INDEX_TASKS),
            &typed_store::rocks::ReadWriteOptions::default(),
            false,
        )?;

        Ok((
            WalrusIndexStore::from_maps(
                primary_index,
                object_index,
                event_cursor_store,
                quilt_patch_index,
                pending_quilt_index_tasks,
            ),
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
        store.apply_index_mutations(vec![walrus_sui::types::IndexMutationSet {
            bucket_id,
            mutations: vec![walrus_sui::types::IndexMutation::Insert {
                identifier: "/photos/2024/sunset.jpg".to_string(),
                object_id,
                blob_id,
                is_quilt: false,
            }],
            event_id: sui_types::event::EventID {
                tx_digest: sui_types::base_types::TransactionDigest::new([0; 32]),
                event_seq: 0,
            },
        }])?;

        // Retrieve via primary index (bucket_id + identifier).
        let entry = store.get_by_bucket_identifier(&bucket_id, "/photos/2024/sunset.jpg")?;
        assert!(entry.is_some());

        match entry.unwrap() {
            IndexTarget::Blob(retrieved) => {
                assert_eq!(retrieved.blob_id, blob_id);
                assert_eq!(retrieved.object_id, object_id);
            }
            _ => panic!("Expected IndexTarget::Blob"),
        }

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
        let mut mutations = Vec::new();
        for i in 0..3 {
            let blob_id = BlobId([i; 32]);
            let object_id = ObjectID::from_hex_literal(&format!("0x{:064x}", i))?;

            mutations.push(walrus_sui::types::IndexMutation::Insert {
                identifier: format!("file{}.txt", i),
                object_id,
                blob_id,
                is_quilt: false,
            });
        }

        store.apply_index_mutations(vec![walrus_sui::types::IndexMutationSet {
            bucket_id,
            mutations,
            event_id: sui_types::event::EventID {
                tx_digest: sui_types::base_types::TransactionDigest::new([0; 32]),
                event_seq: 0,
            },
        }])?;

        // List all entries in bucket.
        let entries = store.list_blobs_in_bucket_entries(&bucket_id)?;
        assert_eq!(entries.len(), 3);

        // Get stats.
        let stats = store.get_bucket_stats(&bucket_id)?;
        assert_eq!(stats.primary_count, 3);
        assert_eq!(stats.secondary_count, 0);

        // Delete bucket.
        store.delete_bucket(&bucket_id)?;
        let entries = store.list_blobs_in_bucket_entries(&bucket_id)?;
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
        let insert_mutation = walrus_sui::types::IndexMutationSet {
            bucket_id,
            mutations: vec![walrus_sui::types::IndexMutation::Insert {
                identifier: "/documents/report.pdf".to_string(),
                object_id,
                blob_id,
                is_quilt: false,
            }],
            event_id: sui_types::event::EventID {
                tx_digest: sui_types::base_types::TransactionDigest::new([0; 32]),
                event_seq: 0,
            },
        };

        store.apply_index_mutations(vec![insert_mutation])?;

        // Verify entry was created in both indices.
        let entry = store.get_by_bucket_identifier(&bucket_id, "/documents/report.pdf")?;
        assert!(entry.is_some());
        match entry.unwrap() {
            IndexTarget::Blob(blob_identity) => {
                assert_eq!(blob_identity.blob_id, blob_id);
            }
            _ => panic!("Expected IndexTarget::Blob"),
        }

        let entry = store.get_by_object_id(&object_id)?;
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().blob_id, blob_id);

        // Test deletion.
        let delete_mutation = walrus_sui::types::IndexMutationSet {
            bucket_id,
            mutations: vec![walrus_sui::types::IndexMutation::Delete {
                object_id,
                is_quilt: false,
            }],
            event_id: sui_types::event::EventID {
                tx_digest: sui_types::base_types::TransactionDigest::new([0; 32]),
                event_seq: 1,
            },
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
        store.apply_index_mutations(vec![walrus_sui::types::IndexMutationSet {
            bucket_id,
            mutations: vec![walrus_sui::types::IndexMutation::Insert {
                identifier: "/test/data.json".to_string(),
                object_id,
                blob_id,
                is_quilt: false,
            }],
            event_id: sui_types::event::EventID {
                tx_digest: sui_types::base_types::TransactionDigest::new([0; 32]),
                event_seq: 0,
            },
        }])?;

        // Read via primary index.
        let retrieved = store.get_by_bucket_identifier(&bucket_id, "/test/data.json")?;
        assert!(retrieved.is_some());
        match retrieved.unwrap() {
            IndexTarget::Blob(blob_identity) => {
                assert_eq!(blob_identity.blob_id, blob_id);
            }
            _ => panic!("Expected IndexTarget::Blob"),
        }

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

    #[tokio::test]
    async fn test_is_quilt_field() -> Result<(), Box<dyn std::error::Error>> {
        let (store, _temp_dir) = create_test_store()?;

        let bucket_id = ObjectID::from_hex_literal(
            "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
        )
        .unwrap();

        // Test regular blob (is_quilt = false)
        let regular_blob_id = BlobId([1; 32]);
        let regular_object_id = ObjectID::from_hex_literal(
            "0x1111111111111111111111111111111111111111111111111111111111111111",
        )
        .unwrap();

        // Test quilt blob (is_quilt = true)
        let quilt_blob_id = BlobId([2; 32]);
        let quilt_object_id = ObjectID::from_hex_literal(
            "0x2222222222222222222222222222222222222222222222222222222222222222",
        )
        .unwrap();

        // Insert regular blob
        store.apply_index_mutations(vec![walrus_sui::types::IndexMutationSet {
            bucket_id,
            mutations: vec![walrus_sui::types::IndexMutation::Insert {
                identifier: "/regular/file.txt".to_string(),
                object_id: regular_object_id,
                blob_id: regular_blob_id,
                is_quilt: false,
            }],
            event_id: sui_types::event::EventID {
                tx_digest: sui_types::base_types::TransactionDigest::new([0; 32]),
                event_seq: 0,
            },
        }])?;

        // Insert quilt blob
        store.apply_index_mutations(vec![walrus_sui::types::IndexMutationSet {
            bucket_id,
            mutations: vec![walrus_sui::types::IndexMutation::Insert {
                identifier: "/quilt/patch.quilt".to_string(),
                object_id: quilt_object_id,
                blob_id: quilt_blob_id,
                is_quilt: true,
            }],
            event_id: sui_types::event::EventID {
                tx_digest: sui_types::base_types::TransactionDigest::new([0; 32]),
                event_seq: 1,
            },
        }])?;

        // Verify regular blob has is_quilt = false
        let regular_entry = store.get_by_bucket_identifier(&bucket_id, "/regular/file.txt")?;
        assert!(regular_entry.is_some());
        match regular_entry.unwrap() {
            IndexTarget::Blob(regular_blob) => {
                assert_eq!(regular_blob.is_quilt, false);
                assert_eq!(regular_blob.blob_id, regular_blob_id);
            }
            _ => panic!("Expected IndexTarget::Blob for regular blob"),
        }

        // Verify quilt blob has is_quilt = true
        let quilt_entry = store.get_by_bucket_identifier(&bucket_id, "/quilt/patch.quilt")?;
        assert!(quilt_entry.is_some());
        match quilt_entry.unwrap() {
            IndexTarget::Blob(quilt_blob) => {
                assert_eq!(quilt_blob.is_quilt, true);
                assert_eq!(quilt_blob.blob_id, quilt_blob_id);
            }
            _ => panic!("Expected IndexTarget::Blob for quilt blob"),
        }

        // Verify retrieval by object_id also preserves is_quilt
        let regular_by_obj = store.get_by_object_id(&regular_object_id)?;
        assert!(regular_by_obj.is_some());
        assert_eq!(regular_by_obj.unwrap().is_quilt, false);

        let quilt_by_obj = store.get_by_object_id(&quilt_object_id)?;
        assert!(quilt_by_obj.is_some());
        assert_eq!(quilt_by_obj.unwrap().is_quilt, true);

        println!("✅ is_quilt field properly stored and retrieved");

        Ok(())
    }

    #[tokio::test]
    async fn test_atomic_mutations_with_cursor() -> Result<(), Box<dyn std::error::Error>> {
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

        // Initially no cursor
        assert!(store.get_last_processed_event_index()?.is_none());

        // Test atomic insertion with cursor update
        let insert_mutation = walrus_sui::types::IndexMutationSet {
            bucket_id,
            mutations: vec![walrus_sui::types::IndexMutation::Insert {
                identifier: "/atomic/test.pdf".to_string(),
                object_id,
                blob_id,
                is_quilt: false,
            }],
            event_id: sui_types::event::EventID {
                tx_digest: sui_types::base_types::TransactionDigest::new([0; 32]),
                event_seq: 0,
            },
        };

        // Apply mutation with cursor atomically
        store.apply_index_mutations_with_cursor(vec![insert_mutation], Some(42))?;

        // Verify both the mutation and cursor were applied atomically
        let entry = store.get_by_bucket_identifier(&bucket_id, "/atomic/test.pdf")?;
        assert!(entry.is_some());
        match entry.unwrap() {
            IndexTarget::Blob(blob_identity) => {
                assert_eq!(blob_identity.blob_id, blob_id);
            }
            _ => panic!("Expected IndexTarget::Blob"),
        }

        let cursor = store.get_last_processed_event_index()?;
        assert_eq!(cursor, Some(42));

        // Test atomic deletion with cursor update
        let delete_mutation = walrus_sui::types::IndexMutationSet {
            bucket_id,
            mutations: vec![walrus_sui::types::IndexMutation::Delete {
                object_id,
                is_quilt: false,
            }],
            event_id: sui_types::event::EventID {
                tx_digest: sui_types::base_types::TransactionDigest::new([1; 32]),
                event_seq: 1,
            },
        };

        // Apply deletion with cursor atomically
        store.apply_index_mutations_with_cursor(vec![delete_mutation], Some(100))?;

        // Verify both the deletion and cursor update were applied atomically
        let entry = store.get_by_bucket_identifier(&bucket_id, "/atomic/test.pdf")?;
        assert!(entry.is_none());

        let cursor = store.get_last_processed_event_index()?;
        assert_eq!(cursor, Some(100));

        println!("✅ Atomic mutations with cursor update working correctly");

        Ok(())
    }
}
