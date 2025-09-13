// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Storage layer for the Walrus Indexer.
//!
//! This module implements the Walrus Index architecture for efficient indexing
//! of blobs and quilt patches. Indices are stored in RocksDB, organized by 'buckets'
//! that provide isolated namespaces similar to S3 buckets.

use std::{collections::HashMap, path::Path, str::FromStr, sync::Arc};

use anyhow::Result;
use rocksdb::Options as RocksDbOptions;
use serde::{Deserialize, Serialize};
use sui_types::base_types::ObjectID;
use typed_store::{
    Map,
    TypedStoreError,
    rocks::{DBBatch, DBMap, MetricConf, open_cf_opts},
};
use walrus_core::{BlobId, QuiltPatchId, metadata::QuiltIndex};
use walrus_sdk::client::client_types::get_stored_quilt_patches;

use crate::{
    AsyncTask,
    indexer::{QuiltIndexTask, QuiltIndexTaskId},
};

// Column family names
const CF_NAME_PRIMARY_INDEX: &str = "walrus_index_primary";
const CF_NAME_OBJECT_INDEX: &str = "walrus_index_object";
const CF_NAME_EVENT_CURSOR: &str = "walrus_event_cursor";
const CF_NAME_QUILT_PATCH_INDEX: &str = "walrus_quilt_patch";
const CF_NAME_PENDING_QUILT_INDEX_TASKS: &str = "walrus_pending_quilt_index_tasks";
const CF_NAME_RETRY_QUILT_INDEX_TASKS: &str = "walrus_retry_quilt_index_tasks";
const CURSOR_KEY: &str = "last_processed_index";

/// Status of quilt task processing.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum QuiltTaskStatus {
    /// Not a quilt.
    NotQuilt,
    /// Running.
    Running(QuiltIndexTaskId),
    /// Quilt task completed successfully.
    Completed,
    /// Quilt task is in retry queue with given task ID.
    InRetryQueue(QuiltIndexTaskId),
}

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
    /// Quilt task status if this is a quilt, None for regular blobs.
    pub quilt_status: QuiltTaskStatus,
}

fn construct_primary_index_key(bucket_id: &ObjectID, identifier: &str) -> String {
    format!("{}/{}", bucket_id, identifier)
}

fn get_primary_index_bounds(bucket_id: &ObjectID) -> (String, String) {
    let lower_bound = format!("{}/", bucket_id);
    let upper_bound = format!("{}0", bucket_id);
    (lower_bound, upper_bound)
}

fn construct_object_index_key(object_id: &ObjectID) -> String {
    object_id.to_string()
}

fn construct_quilt_patch_index_key(patch_blob_id: &BlobId, object_id: &ObjectID) -> String {
    // Use \x00 as delimiter since it's a control character that won't appear in blob/object IDs
    // This allows us to use \x01 as a clean upper bound for prefix searches
    format!("{}\x00{}", patch_blob_id, object_id)
}

fn get_quilt_patch_index_bounds(patch_blob_id: &BlobId) -> (String, String) {
    let lower_bound = format!("{}", patch_blob_id);
    let upper_bound = format!("{}\x01", patch_blob_id);
    (lower_bound, upper_bound)
}

fn parse_quilt_patch_index_key(key: &str) -> (BlobId, ObjectID) {
    let parts = key.split('\x00').collect::<Vec<&str>>();
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
    /// Retry quilt index tasks: stores the quilt index tasks that failed and need to be retried.
    retry_quilt_index_tasks: DBMap<QuiltIndexTaskId, QuiltIndexTaskValue>,
}

// Constants for future pagination implementation.
// const INLINE_STORAGE_THRESHOLD: usize = 100;
// const ENTRIES_PER_PAGE: usize = 1000;

impl WalrusIndexStore {
    /// Create a new WalrusIndexStore by opening the database at the specified path.
    /// This initializes all required column families for the indexer.
    pub async fn open(db_path: &Path) -> Result<Self> {
        let db_options = RocksDbOptions::default();
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
                (CF_NAME_RETRY_QUILT_INDEX_TASKS, db_options.clone()),
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

        let retry_quilt_index_tasks: DBMap<QuiltIndexTaskId, QuiltIndexTaskValue> = DBMap::reopen(
            &db,
            Some(CF_NAME_RETRY_QUILT_INDEX_TASKS),
            &typed_store::rocks::ReadWriteOptions::default(),
            false,
        )?;

        Ok(Self {
            primary_index,
            object_index,
            event_cursor_store,
            quilt_patch_index,
            pending_quilt_index_tasks,
            retry_quilt_index_tasks,
        })
    }

    /// Create a new WalrusIndexStore from existing DBMap instances.
    /// This is used for testing and migration purposes.
    pub fn from_maps(
        primary_index: DBMap<String, IndexTarget>,
        object_index: DBMap<String, ObjectIndexValue>,
        event_cursor_store: DBMap<String, u64>,
        quilt_patch_index: DBMap<String, QuiltPatchId>,
        pending_quilt_index_tasks: DBMap<QuiltIndexTaskId, QuiltIndexTaskValue>,
        retry_quilt_index_tasks: DBMap<QuiltIndexTaskId, QuiltIndexTaskValue>,
    ) -> Self {
        Self {
            primary_index,
            object_index,
            event_cursor_store,
            quilt_patch_index,
            pending_quilt_index_tasks,
            retry_quilt_index_tasks,
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
                [(CURSOR_KEY.to_string(), cursor_value)],
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
        _cursor_update: Option<u64>,
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
        _is_quilt: bool,
    ) -> Result<(), TypedStoreError> {
        // Primary index: bucket_id/identifier -> IndexTarget.
        let primary_key = construct_primary_index_key(bucket_id, identifier);
        let blob_identity = BlobIdentity {
            blob_id,
            object_id: *object_id,
            // Quilts start as None (not processed yet).
            // Will be updated to Completed or InRetryQueue when processed.
            quilt_status: QuiltTaskStatus::NotQuilt,
        };
        let index_target = IndexTarget::Blob(blob_identity);

        // Object index: object_id -> bucket_id/identifier.
        let object_key = construct_object_index_key(object_id);
        let object_value = ObjectIndexValue {
            bucket_id: *bucket_id,
            identifier: identifier.to_string(),
        };

        // Use raw string API for primary index to avoid bincode string serialization issues
        batch.insert_batch_with_raw_string(&self.primary_index, [(primary_key, index_target)])?;
        batch.insert_batch_with_raw_string(&self.object_index, [(object_key, object_value)])?;

        Ok(())
    }

    pub fn populate_quilt_patch_index(
        &self,
        quilt_index_task: &QuiltIndexTask,
        quilt_index: &QuiltIndex,
    ) -> Result<(), TypedStoreError> {
        // NOTE: This uses DBBatch for atomic writes but not true RocksDB transactions.
        // There's a small race condition window between checking task existence and writing.
        // To use true transactions, typed-store would need to use TransactionDB instead of 
        // regular DB.
        let task_id = quilt_index_task.task_id();

        // First check if the task still exists in either pending or retry queue
        let task_exists = self.pending_quilt_index_tasks.get(&task_id)?.is_some()
            || self.retry_quilt_index_tasks.get(&task_id)?.is_some();

        if !task_exists {
            // Task was deleted (likely because the quilt was deleted), skip processing
            tracing::info!(
                "Task {:?} no longer exists, skipping quilt patch index population",
                task_id
            );
            return Ok(());
        }

        // Create batch for atomic operations
        let mut batch = self.primary_index.batch();

        // Check again within the batch context to ensure consistency
        // Get the primary key to check if the quilt entry still exists
        let primary_key =
            construct_primary_index_key(&quilt_index_task.bucket_id, &quilt_index_task.identifier);

        // If the primary index entry exists and is still a quilt needing processing
        if let Some(existing_entry) = self
            .primary_index
            .get_with_raw_string::<IndexTarget>(&primary_key)?
        {
            // Check if the quilt was already deleted or completed
            if let IndexTarget::Blob(blob_identity) = existing_entry {
                match blob_identity.quilt_status {
                    QuiltTaskStatus::Completed => {
                        tracing::info!(
                            "Quilt task already completed, skipping duplicate processing"
                        );
                        // Still need to clean up the task from pending queue
                        batch.delete_batch(&self.pending_quilt_index_tasks, [task_id.clone()])?;
                        batch.delete_batch(&self.retry_quilt_index_tasks, [task_id])?;
                        batch.write()?;
                        return Ok(());
                    }
                    QuiltTaskStatus::Running(_) | QuiltTaskStatus::InRetryQueue(_) => {
                        // Update the quilt entry to Completed status
                        let updated_blob_identity = BlobIdentity {
                            blob_id: blob_identity.blob_id,
                            object_id: blob_identity.object_id,
                            quilt_status: QuiltTaskStatus::Completed,
                        };
                        let updated_index_target = IndexTarget::Blob(updated_blob_identity);
                        batch.insert_batch_with_raw_string(
                            &self.primary_index,
                            [(primary_key.clone(), updated_index_target)],
                        )?;
                    }
                    _ => {
                        // For NotQuilt or other statuses, shouldn't happen but log warning
                        tracing::warn!("Unexpected quilt status: {:?}", blob_identity.quilt_status);
                    }
                }
            }
        } else {
            // Entry doesn't exist, this shouldn't happen as we populate it when storing the task
            tracing::warn!("Primary index entry not found for quilt task, creating it");
            // Create the entry with Completed status
            let blob_identity = BlobIdentity {
                blob_id: quilt_index_task.quilt_blob_id,
                object_id: quilt_index_task.object_id,
                quilt_status: QuiltTaskStatus::Completed,
            };
            let index_target = IndexTarget::Blob(blob_identity);
            batch.insert_batch_with_raw_string(
                &self.primary_index,
                [(primary_key.clone(), index_target)],
            )?;
        }

        // Process all quilt patches
        for patch in get_stored_quilt_patches(quilt_index, quilt_index_task.quilt_blob_id) {
            let primary_key =
                construct_primary_index_key(&quilt_index_task.bucket_id, &patch.identifier);
            let quilt_patch_id = QuiltPatchId::from_str(&patch.quilt_patch_id)
                .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;
            let primary_value = IndexTarget::QuiltPatchId(quilt_patch_id.clone());

            tracing::debug!(
                ?primary_key,
                ?primary_value,
                "Inserted quilt patch primary index key",
            );

            // Use raw string API for primary index
            batch.insert_batch_with_raw_string(
                &self.primary_index,
                [(primary_key, primary_value)],
            )?;

            if let Some(patch_blob_id) = patch.patch_blob_id {
                let quilt_patch_index_key =
                    construct_quilt_patch_index_key(&patch_blob_id, &quilt_index_task.object_id);

                tracing::debug!(
                    patch_blob_id = patch_blob_id.to_string(),
                    quilt_patch_id = quilt_patch_id.to_string(),
                    object_id = quilt_index_task.object_id.to_string(),
                    bucket_id = quilt_index_task.bucket_id.to_string(),
                    identifier = patch.identifier,
                    quilt_id = quilt_index_task.quilt_blob_id.to_string(),
                    key = quilt_patch_index_key,
                    "Inserted quilt patch index key",
                );
                // Use raw string API to bypass bincode serialization
                batch.insert_batch_with_raw_string(
                    &self.quilt_patch_index,
                    [(quilt_patch_index_key, quilt_patch_id)],
                )?;
            }
        }

        // Remove the task from both pending and retry queues (in case it's in retry)
        batch.delete_batch(&self.pending_quilt_index_tasks, [task_id.clone()])?;
        batch.delete_batch(&self.retry_quilt_index_tasks, [task_id])?;

        // Commit all operations atomically
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
    //
    // TODO(heliu): This doesn't handle quilt patches.
    pub fn delete_bucket(&self, bucket_id: &ObjectID) -> Result<(), TypedStoreError> {
        let prefix = construct_primary_index_key(bucket_id, "");
        let mut batch = self.primary_index.batch();

        // Use raw string API with bounds for efficient prefix scanning
        let lower_bound = prefix.clone();
        // Use control character for clean upper bound
        let mut upper_bound = prefix.clone();
        upper_bound.push('\u{ff}');

        // Delete from both indices.
        for entry in self
            .primary_index
            .iter_with_raw_string::<IndexTarget>(Some(&lower_bound), Some(&upper_bound))?
        {
            let (key, _value) = entry?;
            // Delete from both indices.
            self.apply_delete_by_key(&mut batch, key)?;
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
        if let Some(index_target) = self.primary_index.get_with_raw_string(&primary_key)? {
            // Extract object_id based on the IndexTarget variant
            let object_id = match &index_target {
                IndexTarget::Blob(blob_identity) => {
                    // Check if there's a retry task associated with this entry.
                    if let QuiltTaskStatus::InRetryQueue(task_id) = &blob_identity.quilt_status {
                        // Clean up the retry task.
                        batch.delete_batch(&self.retry_quilt_index_tasks, [task_id.clone()])?;
                    }
                    &blob_identity.object_id
                }
                IndexTarget::QuiltPatchId(_) => {
                    // For now, we don't handle QuiltPatchId deletion
                    return Ok(());
                }
                IndexTarget::QuiltId(quilt_id) => quilt_id,
            };
            let object_key = construct_object_index_key(object_id);
            batch.delete_batch_with_raw_string(&self.primary_index, [primary_key])?;
            batch.delete_batch_with_raw_string(&self.object_index, [object_key])?;
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
        if let Some(object_value) = self
            .object_index
            .get_with_raw_string::<ObjectIndexValue>(&object_key)?
        {
            let primary_key =
                construct_primary_index_key(&object_value.bucket_id, &object_value.identifier);

            // Check if there's a retry task associated with this entry.
            if let Some(IndexTarget::Blob(blob_identity)) = self
                .primary_index
                .get_with_raw_string::<IndexTarget>(&primary_key)?
            {
                if let QuiltTaskStatus::InRetryQueue(task_id) = blob_identity.quilt_status {
                    // Clean up the retry task.
                    batch.delete_batch(&self.retry_quilt_index_tasks, [task_id])?;
                }
            }

            batch.delete_batch_with_raw_string(&self.primary_index, [primary_key])?;
            batch.delete_batch_with_raw_string(&self.object_index, [object_key])?;
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
        self.primary_index.get_with_raw_string(&key)
    }

    pub fn get_quilt_patch_id_by_blob_id(
        &self,
        blob_id: &BlobId,
    ) -> Result<Option<QuiltPatchId>, TypedStoreError> {
        tracing::debug!("Querying quilt patch for blob_id: {}", blob_id);

        // Use raw string API with bounded iteration for efficient prefix search.
        // Raw string keys preserve lexicographic ordering, allowing proper
        // prefix-based queries.
        // Since we use \x00 as delimiter, we can use \x01 as a clean upper bound

        let (lower_bound, upper_bound) = get_quilt_patch_index_bounds(blob_id);
        // Use raw string iterator with bounds
        for entry in self
            .quilt_patch_index
            .iter_with_raw_string(Some(&lower_bound), Some(&upper_bound))?
        {
            let (key, value) = entry?;
            // Parse and verify it's the correct blob_id (defensive check)
            let (patch_blob_id, _object_id) = parse_quilt_patch_index_key(&key);
            if patch_blob_id == *blob_id {
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    /// Get an index entry by object_id (object index).
    pub fn get_by_object_id(
        &self,
        object_id: &ObjectID,
    ) -> Result<Option<BlobIdentity>, TypedStoreError> {
        // First get the bucket_identifier from object index.
        let object_key = construct_object_index_key(object_id);
        if let Some(object_value) = self
            .object_index
            .get_with_raw_string::<ObjectIndexValue>(&object_key)?
        {
            // Then get the actual blob identity from primary index.
            let primary_key =
                construct_primary_index_key(&object_value.bucket_id, &object_value.identifier);
            match self.primary_index.get_with_raw_string(&primary_key)? {
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
    ) -> Result<HashMap<String, IndexTarget>, TypedStoreError> {
        let (lower_bound, upper_bound) = get_primary_index_bounds(bucket_id);
        let mut result = HashMap::new();

        // Use raw string API with bounds for efficient prefix scanning

        for entry in self
            .primary_index
            .iter_with_raw_string::<IndexTarget>(Some(&lower_bound), Some(&upper_bound))?
        {
            let (key, value) = entry?;
            result.insert(key.to_string(), value);
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
        Ok(self
            .primary_index
            .get_with_raw_string::<IndexTarget>(&key)?
            .is_some())
    }

    /// Get statistics about a bucket.
    pub fn get_bucket_stats(&self, bucket_id: &ObjectID) -> Result<BucketStats, TypedStoreError> {
        let prefix = construct_primary_index_key(bucket_id, "");
        let mut primary_count = 0u32;

        // Use the control character for clean upper bound.
        let mut upper_bound = prefix.clone();
        upper_bound.push('\u{ff}');

        // Count primary entries using raw string iterator.
        for entry in self
            .primary_index
            .iter_with_raw_string::<IndexTarget>(Some(&prefix), Some(&upper_bound))?
        {
            let (_, _) = entry?;
            primary_count += 1;
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
        self.event_cursor_store.get(&CURSOR_KEY.to_string())
    }

    /// Set the last processed event index in storage.
    pub fn set_last_processed_event_index(&self, index: u64) -> Result<(), TypedStoreError> {
        self.event_cursor_store
            .insert(&CURSOR_KEY.to_string(), &index)
    }

    /// Get all entries from the quilt patch index.
    /// Returns a vector of (key, value) pairs.
    pub fn get_all_quilt_patch_entries(
        &self,
    ) -> Result<Vec<(String, QuiltPatchId)>, TypedStoreError> {
        let mut entries = Vec::new();
        for entry in self.quilt_patch_index.iter_with_raw_string(None, None)? {
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

        let mut batch = self.pending_quilt_index_tasks.batch();

        // Store the task in pending queue
        batch.insert_batch(&self.pending_quilt_index_tasks, [(key.clone(), value)])?;

        // Also populate the primary index with Running status
        let primary_key = construct_primary_index_key(&task.bucket_id, &task.identifier);
        let blob_identity = BlobIdentity {
            blob_id: task.quilt_blob_id,
            object_id: task.object_id,
            quilt_status: QuiltTaskStatus::Running(key),
        };
        let index_target = IndexTarget::Blob(blob_identity);
        batch.insert_batch_with_raw_string(&self.primary_index, [(primary_key, index_target)])?;

        // Update event cursor
        batch.insert_batch(
            &self.event_cursor_store,
            [("last_processed_index".to_string(), task.event_index)],
        )?;

        batch.write()
    }

    /// Remove a quilt task from storage by its task ID.
    async fn remove(&self, task_id: &QuiltIndexTaskId) -> Result<(), TypedStoreError> {
        self.pending_quilt_index_tasks.remove(task_id)
    }

    /// Load quilt tasks within a task_id range (exclusive on both ends).
    async fn read_range(
        &self,
        from_task_id: Option<QuiltIndexTaskId>,
        to_task_id: Option<QuiltIndexTaskId>,
        limit: usize,
    ) -> Result<Vec<crate::indexer::QuiltIndexTask>, TypedStoreError> {
        let mut count = 0;
        let mut result = Vec::new();

        for entry in self
            .pending_quilt_index_tasks
            .safe_iter_with_bounds(from_task_id.clone(), to_task_id.clone())?
        {
            let (key, value) = entry?;

            if let Some(ref from) = from_task_id {
                if key <= *from {
                    continue;
                }
            }

            if let Some(ref to) = to_task_id {
                if key >= *to {
                    break;
                }
            }

            result.push(crate::indexer::QuiltIndexTask::new(
                key.sequence,
                key.quilt_id,
                value.object_id,
                value.bucket_id,
                value.identifier,
                key.sequence,
            ));
            count += 1;

            if count >= limit {
                break;
            }
        }

        Ok(result)
    }

    /// Add a task to the retry queue.
    async fn add_to_retry_queue(
        &self,
        task: &crate::indexer::QuiltIndexTask,
    ) -> Result<(), TypedStoreError> {
        let key = QuiltIndexTaskId::new(task.event_index, task.quilt_blob_id);
        let value = QuiltIndexTaskValue {
            object_id: task.object_id,
            bucket_id: task.bucket_id,
            identifier: task.identifier.clone(),
        };

        // Start a batch to atomically move from pending to retry.
        let mut batch = self.pending_quilt_index_tasks.batch();

        let primary_key = construct_primary_index_key(&task.bucket_id, &task.identifier);
        let index_target = IndexTarget::Blob(BlobIdentity {
            blob_id: task.quilt_blob_id,
            object_id: task.object_id,
            quilt_status: QuiltTaskStatus::InRetryQueue(key.clone()),
        });

        batch.insert_batch_with_raw_string(&self.primary_index, [(primary_key, index_target)])?;

        // Remove from pending queue if it exists.
        batch.delete_batch(&self.pending_quilt_index_tasks, [key.clone()])?;

        // Add to retry queue.
        batch.insert_batch(&self.retry_quilt_index_tasks, [(key, value)])?;

        batch.write()
    }

    /// Read tasks from retry queue starting from the given task ID.
    async fn read_retry_tasks(
        &self,
        from_task_id: Option<QuiltIndexTaskId>,
        limit: usize,
    ) -> Result<Vec<crate::indexer::QuiltIndexTask>, TypedStoreError> {
        let mut count = 0;
        let mut result = Vec::new();

        for entry in self
            .retry_quilt_index_tasks
            .safe_iter_with_bounds(from_task_id.clone(), None)?
        {
            let (key, value) = entry?;

            if let Some(ref from) = from_task_id {
                if key <= *from {
                    continue;
                }
            }

            result.push(crate::indexer::QuiltIndexTask::new(
                key.sequence,
                key.quilt_id,
                value.object_id,
                value.bucket_id,
                value.identifier,
                key.sequence,
            ));
            count += 1;

            if count >= limit {
                break;
            }
        }

        Ok(result)
    }

    /// Delete a task from the retry queue.
    async fn delete_retry_task(&self, task_id: &QuiltIndexTaskId) -> Result<(), TypedStoreError> {
        self.retry_quilt_index_tasks.remove(task_id)
    }
}

impl WalrusIndexStore {
    /// Update the quilt status in the primary index.
    pub fn update_quilt_status(
        &self,
        bucket_id: &ObjectID,
        identifier: &str,
        new_status: QuiltTaskStatus,
    ) -> Result<(), TypedStoreError> {
        let key = construct_primary_index_key(bucket_id, identifier);

        // Get the current value.
        if let Some(mut index_target) = self
            .primary_index
            .get_with_raw_string::<IndexTarget>(&key)?
        {
            // Update the quilt status if it's a blob.
            if let IndexTarget::Blob(ref mut blob_identity) = index_target {
                blob_identity.quilt_status = new_status;

                // Write back the updated value.
                let mut batch = self.primary_index.batch();
                batch.insert_batch_with_raw_string(&self.primary_index, [(key, index_target)])?;
                batch.write()?;
            }
        }

        Ok(())
    }

    /// Move a task from pending queue to retry queue.
    pub async fn move_to_retry_queue(
        &self,
        task_id: &QuiltIndexTaskId,
    ) -> Result<(), TypedStoreError> {
        // Get the task from pending queue.
        if let Some(value) = self.pending_quilt_index_tasks.get(task_id)? {
            // Update the quilt_status in the primary index to indicate it's in retry queue.
            self.update_quilt_status(
                &value.bucket_id,
                &value.identifier,
                QuiltTaskStatus::InRetryQueue(task_id.clone()),
            )?;

            // Start a batch transaction.
            let mut batch = self.pending_quilt_index_tasks.batch();

            // Remove from pending queue.
            batch.delete_batch(&self.pending_quilt_index_tasks, [task_id.clone()])?;

            // Add to retry queue.
            batch.insert_batch(&self.retry_quilt_index_tasks, [(task_id.clone(), value)])?;

            // Commit the batch.
            batch.write()
        } else {
            // Task not found in pending queue.
            Ok(())
        }
    }
}

/// OrderedStore implementation for retry tasks.
#[async_trait::async_trait]
impl crate::OrderedStore<crate::indexer::RetryQuiltIndexTask> for WalrusIndexStore {
    /// Persist a retry task to storage.
    async fn store(
        &self,
        task: &crate::indexer::RetryQuiltIndexTask,
    ) -> Result<(), TypedStoreError> {
        let key = QuiltIndexTaskId::new(task.inner.event_index, task.inner.quilt_blob_id);
        let value = QuiltIndexTaskValue {
            object_id: task.inner.object_id,
            bucket_id: task.inner.bucket_id,
            identifier: task.inner.identifier.clone(),
        };
        self.retry_quilt_index_tasks.insert(&key, &value)
    }

    /// Remove a retry task from storage by its task ID.
    async fn remove(&self, task_id: &QuiltIndexTaskId) -> Result<(), TypedStoreError> {
        self.retry_quilt_index_tasks.remove(task_id)
    }

    /// Load retry tasks within a task_id range (exclusive on both ends).
    async fn read_range(
        &self,
        from_task_id: Option<QuiltIndexTaskId>,
        to_task_id: Option<QuiltIndexTaskId>,
        limit: usize,
    ) -> Result<Vec<crate::indexer::RetryQuiltIndexTask>, TypedStoreError> {
        let mut count = 0;
        let mut result = Vec::new();

        for entry in self
            .retry_quilt_index_tasks
            .safe_iter_with_bounds(from_task_id.clone(), to_task_id.clone())?
        {
            let (key, value) = entry?;

            if let Some(ref from) = from_task_id {
                if key <= *from {
                    continue;
                }
            }

            if let Some(ref to) = to_task_id {
                if key >= *to {
                    break;
                }
            }

            let inner = crate::indexer::QuiltIndexTask::new(
                key.sequence,
                key.quilt_id,
                value.object_id,
                value.bucket_id,
                value.identifier,
                key.sequence,
            );
            result.push(crate::indexer::RetryQuiltIndexTask {
                inner,
                retry_count: 0, // Will be tracked separately if needed.
            });
            count += 1;

            if count >= limit {
                break;
            }
        }

        Ok(result)
    }

    /// Add a retry task back to the retry queue (for re-retry).
    async fn add_to_retry_queue(
        &self,
        task: &crate::indexer::RetryQuiltIndexTask,
    ) -> Result<(), TypedStoreError> {
        let key = QuiltIndexTaskId::new(task.inner.event_index, task.inner.quilt_blob_id);
        let value = QuiltIndexTaskValue {
            object_id: task.inner.object_id,
            bucket_id: task.inner.bucket_id,
            identifier: task.inner.identifier.clone(),
        };
        // Simply update the retry queue entry.
        self.retry_quilt_index_tasks.insert(&key, &value)
    }

    /// Read retry tasks from retry queue starting from the given task ID.
    async fn read_retry_tasks(
        &self,
        from_task_id: Option<QuiltIndexTaskId>,
        limit: usize,
    ) -> Result<Vec<crate::indexer::RetryQuiltIndexTask>, TypedStoreError> {
        let mut count = 0;
        let mut result = Vec::new();

        for entry in self
            .retry_quilt_index_tasks
            .safe_iter_with_bounds(from_task_id.clone(), None)?
        {
            let (key, value) = entry?;

            if let Some(ref from) = from_task_id {
                if key <= *from {
                    continue;
                }
            }

            let inner = crate::indexer::QuiltIndexTask::new(
                key.sequence,
                key.quilt_id,
                value.object_id,
                value.bucket_id,
                value.identifier,
                key.sequence,
            );
            result.push(crate::indexer::RetryQuiltIndexTask {
                inner,
                retry_count: 0, // Will be managed by the executor.
            });
            count += 1;

            if count >= limit {
                break;
            }
        }

        Ok(result)
    }

    /// Delete a retry task from the retry queue.
    async fn delete_retry_task(&self, task_id: &QuiltIndexTaskId) -> Result<(), TypedStoreError> {
        self.retry_quilt_index_tasks.remove(task_id)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rocksdb::Options as RocksDbOptions;
    use tempfile::TempDir;
    use typed_store::rocks::{MetricConf, open_cf_opts};

    use super::*;

    fn create_test_store() -> Result<(WalrusIndexStore, TempDir), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let db_options = RocksDbOptions::default();

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
                (CF_NAME_RETRY_QUILT_INDEX_TASKS, db_options.clone()),
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

        let retry_quilt_index_tasks = DBMap::reopen(
            &db,
            Some(CF_NAME_RETRY_QUILT_INDEX_TASKS),
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
                retry_quilt_index_tasks,
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
                assert_eq!(regular_blob.quilt_status, QuiltTaskStatus::NotQuilt);
                assert_eq!(regular_blob.blob_id, regular_blob_id);
            }
            _ => panic!("Expected IndexTarget::Blob for regular blob"),
        }

        // Verify quilt blob has quilt_status = NotQuilt (initial state)
        let quilt_entry = store.get_by_bucket_identifier(&bucket_id, "/quilt/patch.quilt")?;
        assert!(quilt_entry.is_some());
        match quilt_entry.unwrap() {
            IndexTarget::Blob(quilt_blob) => {
                assert_eq!(quilt_blob.quilt_status, QuiltTaskStatus::NotQuilt);
                assert_eq!(quilt_blob.blob_id, quilt_blob_id);
            }
            _ => panic!("Expected IndexTarget::Blob for quilt blob"),
        }

        // Verify retrieval by object_id also preserves quilt_status
        let regular_by_obj = store.get_by_object_id(&regular_object_id)?;
        assert!(regular_by_obj.is_some());
        assert_eq!(
            regular_by_obj.unwrap().quilt_status,
            QuiltTaskStatus::NotQuilt
        );

        let quilt_by_obj = store.get_by_object_id(&quilt_object_id)?;
        assert!(quilt_by_obj.is_some());
        assert_eq!(
            quilt_by_obj.unwrap().quilt_status,
            QuiltTaskStatus::NotQuilt
        );

        println!("✅ quilt_status field properly stored and retrieved");

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
