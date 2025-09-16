// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Storage layer for the Walrus Indexer.
//!
//! This module implements the Walrus Index architecture for efficient indexing
//! of blobs and quilt patches. Indices are stored in RocksDB, organized by 'buckets'
//! that provide isolated namespaces similar to S3 buckets.

// TODO(heliu):
// 1. Remove apply_mutations.
// 2. make the sequence nonoptional.
// 3. Sequence numbers are used throughout for consistent naming.

use std::{collections::HashMap, path::Path, sync::Arc};

use anyhow::Result;
use rocksdb::{OptimisticTransactionDB, Options as RocksDbOptions, Transaction};
use serde::{Deserialize, Serialize};
use sui_types::base_types::ObjectID;
use typed_store::{
    Map,
    TypedStoreError,
    rocks::{DBMap, MetricConf},
};
use walrus_core::{
    BlobId,
    QuiltPatchId,
    encoding::quilt_encoding::{QuiltIndexApi, QuiltPatchApi, QuiltPatchInternalIdApi},
    metadata::QuiltIndex,
};

use crate::{
    AsyncTask,
    indexer::{QuiltIndexTask, QuiltIndexTaskId},
};

// Column family names
const CF_NAME_PRIMARY_INDEX: &str = "walrus_index_primary";
const CF_NAME_OBJECT_INDEX: &str = "walrus_index_object";
const CF_NAME_SEQUENCE_STORE: &str = "walrus_sequence_store";
const CF_NAME_QUILT_PATCH_INDEX: &str = "walrus_quilt_patch";
const CF_NAME_PENDING_QUILT_INDEX_TASKS: &str = "walrus_pending_quilt_index_tasks";
const CF_NAME_RETRY_QUILT_INDEX_TASKS: &str = "walrus_retry_quilt_index_tasks";
const SEQUENCE_KEY: &[u8] = b"sequence";

/// Key structure for primary index: bucket_id/identifier.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PrimaryIndexKey {
    bucket_id: ObjectID,
    identifier: String,
    sequence_number: u64,
}

impl PrimaryIndexKey {
    fn new(bucket_id: ObjectID, identifier: String, sequence_number: u64) -> Self {
        Self {
            bucket_id,
            identifier,
            sequence_number,
        }
    }

    /// Convert to optimized byte representation for RocksDB key.
    /// Format: [bucket_id_bytes(32)] / [identifier_utf8] / [sequence_be_bytes(8)]
    fn to_key(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Add raw ObjectID bytes (32 bytes)
        bytes.extend_from_slice(self.bucket_id.as_ref());

        // Add '/' separator
        bytes.push(b'/');

        // Add identifier as UTF-8 bytes
        bytes.extend_from_slice(self.identifier.as_bytes());

        // Add '/' separator
        bytes.push(b'/');

        // Add sequence number as big-endian bytes (8 bytes) to preserve u64 order
        bytes.extend_from_slice(&self.sequence_number.to_be_bytes());

        bytes
    }

    /// Parse from byte representation.
    /// Optimized parsing: first 32 bytes = ObjectID, last 8 bytes = sequence,
    /// middle = /<identifier>/.
    fn from_key(bytes: &[u8]) -> Result<Self> {
        // Minimum size: 32 (ObjectID) + 1 (/) + 0 (empty identifier) + 1 (/) + 8 (sequence)
        if bytes.len() < 42 {
            return Err(anyhow::anyhow!("Key too short: minimum 42 bytes required"));
        }

        // Extract ObjectID (first 32 bytes)
        let bucket_id = ObjectID::from_bytes(&bytes[0..32])?;

        // Check for first separator
        if bytes[32] != b'/' {
            return Err(anyhow::anyhow!(
                "Missing first '/' separator after ObjectID"
            ));
        }

        // Extract sequence number (last 8 bytes)
        let seq_bytes: [u8; 8] = bytes[bytes.len() - 8..].try_into()?;
        let sequence_number = u64::from_be_bytes(seq_bytes);

        // Check for second separator before sequence
        if bytes[bytes.len() - 9] != b'/' {
            return Err(anyhow::anyhow!(
                "Missing second '/' separator before sequence"
            ));
        }

        // Extract identifier (everything between the two separators)
        // From position 33 to (length - 9)
        let identifier = String::from_utf8(bytes[33..bytes.len() - 9].to_vec())?;

        Ok(Self {
            bucket_id,
            identifier,
            sequence_number,
        })
    }
}

impl std::fmt::Display for PrimaryIndexKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}/{}",
            self.bucket_id, self.identifier, self.sequence_number
        )
    }
}

/// Key structure for object index.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ObjectIndexKey {
    object_id: ObjectID,
}

impl ObjectIndexKey {
    fn new(object_id: ObjectID) -> Self {
        Self { object_id }
    }

    /// Convert to optimized byte representation for RocksDB key.
    /// Format: [object_id_bytes(32)]
    fn to_key(&self) -> Vec<u8> {
        // Simply use the raw ObjectID bytes (32 bytes)
        self.object_id.as_ref().to_vec()
    }

    /// Convert byte key to hex string for use with _with_raw_string methods
    fn to_hex_key(&self) -> String {
        hex::encode(self.to_key())
    }

    /// Parse from byte representation.
    #[allow(unused)]
    fn from_key(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 32 {
            return Err(anyhow::anyhow!(
                "Invalid ObjectID length: expected 32 bytes"
            ));
        }
        Ok(Self {
            object_id: ObjectID::from_bytes(bytes)?,
        })
    }
}

impl std::fmt::Display for ObjectIndexKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_hex_key())
    }
}

/// Key structure for quilt patch index.
#[derive(Debug, Clone, PartialEq, Eq)]
struct QuiltPatchIndexKey {
    patch_blob_id: BlobId,
    object_id: ObjectID,
}

impl std::fmt::Display for QuiltPatchIndexKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.patch_blob_id, self.object_id)
    }
}

impl QuiltPatchIndexKey {
    fn new(patch_blob_id: BlobId, object_id: ObjectID) -> Self {
        Self {
            patch_blob_id,
            object_id,
        }
    }

    /// Convert to optimized byte representation for RocksDB key.
    /// Format: [patch_blob_id_bytes(32)] \x00 [object_id_bytes(32)]
    fn to_key(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Add BlobId bytes (32 bytes)
        bytes.extend_from_slice(self.patch_blob_id.as_ref());

        // Add \x00 separator
        bytes.push(0x00);

        // Add ObjectID bytes (32 bytes)
        bytes.extend_from_slice(self.object_id.as_ref());

        bytes
    }

    /// Parse from byte representation.
    fn from_key(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 65 {
            return Err(anyhow::anyhow!(
                "Invalid key length: expected 65 bytes (32 + 1 + 32)"
            ));
        }

        // Check for separator at position 32
        if bytes[32] != 0x00 {
            return Err(anyhow::anyhow!("Missing \\x00 separator at position 32"));
        }

        // Extract BlobId (first 32 bytes)
        let blob_id_bytes: [u8; 32] = bytes[0..32].try_into()?;
        let patch_blob_id = BlobId(blob_id_bytes);

        // Extract ObjectID (last 32 bytes)
        let object_id = ObjectID::from_bytes(&bytes[33..65])?;

        Ok(Self {
            patch_blob_id,
            object_id,
        })
    }
}

fn get_primary_index_bounds(bucket_id: &ObjectID, identifier: &str) -> (Vec<u8>, Vec<u8>) {
    // For scanning all entries with a specific bucket_id and identifier
    // Format: bucket_id/identifier/sequence

    let mut lower_bound = Vec::new();

    // Add raw ObjectID bytes (32 bytes)
    lower_bound.extend_from_slice(bucket_id.as_ref());

    // Add '/' separator
    lower_bound.push(b'/');

    // Add identifier
    lower_bound.extend_from_slice(identifier.as_bytes());

    let mut upper_bound = lower_bound.clone();

    // Lower bound: add '/' to start the sequence range
    lower_bound.push(b'/');

    // Upper bound: use '0' (ASCII 48, next char after '/' ASCII 47) to exclude all sequences
    upper_bound.push(b'0');

    (lower_bound, upper_bound)
}

fn get_bucket_bounds(bucket_id: &ObjectID) -> (Vec<u8>, Vec<u8>) {
    // For scanning all entries in a bucket
    // We want all keys that start with bucket_id/

    let mut lower_bound = Vec::new();
    let mut upper_bound = Vec::new();

    // Lower bound: bucket_id/
    lower_bound.extend_from_slice(bucket_id.as_ref());
    lower_bound.push(b'/');

    // Upper bound: bucket_id followed by '0' (next ASCII char after '/')
    upper_bound.extend_from_slice(bucket_id.as_ref());
    upper_bound.push(b'0'); // ASCII '0' is next after '/'

    (lower_bound, upper_bound)
}

fn get_quilt_patch_index_bounds(patch_blob_id: &BlobId) -> (Vec<u8>, Vec<u8>) {
    // For scanning all entries with a specific patch_blob_id
    // Format: patch_blob_id \x00 object_id

    let mut lower_bound = Vec::new();
    let mut upper_bound = Vec::new();

    // Lower bound: patch_blob_id followed by \x00
    lower_bound.extend_from_slice(patch_blob_id.as_ref());
    lower_bound.push(0x00);

    // Upper bound: patch_blob_id followed by \x01 (next byte after \x00)
    upper_bound.extend_from_slice(patch_blob_id.as_ref());
    upper_bound.push(0x01);

    (lower_bound, upper_bound)
}

fn parse_quilt_patch_index_key(key: &[u8]) -> Result<(BlobId, ObjectID), anyhow::Error> {
    let key_struct = QuiltPatchIndexKey::from_key(key)?;
    Ok((key_struct.patch_blob_id, key_struct.object_id))
}

/// A concise representation of a quilt patch.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Patch {
    pub identifier: String,
    /// The internal ID, can be combined with quilt_id to get QuiltPatchId.
    pub quilt_patch_internal_id: Vec<u8>,
    pub patch_blob_id: Option<BlobId>,
}

/// Extracts Patches from a QuiltIndex.
fn get_patches_for_primary_index(quilt_index: &QuiltIndex) -> Vec<Patch> {
    assert!(matches!(quilt_index, QuiltIndex::V1(_)));
    let QuiltIndex::V1(quilt_index_v1) = quilt_index;
    quilt_index_v1
        .patches()
        .iter()
        .map(|patch| Patch {
            identifier: patch.identifier.clone(),
            quilt_patch_internal_id: patch.quilt_patch_internal_id().to_bytes(),
            patch_blob_id: patch.patch_blob_id(),
        })
        .collect()
}

/// Status of quilt task processing.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum QuiltTaskStatus {
    /// Not a quilt.
    NotQuilt,
    /// Running.
    Running(QuiltIndexTaskId),
    /// Quilt task completed successfully.
    Completed(Vec<Patch>),
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

/// Value stored in the object index.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectIndexValue {
    /// The bucket ID.
    pub bucket_id: ObjectID,
    /// The identifier.
    pub identifier: String,
    /// The sequence number.
    pub sequence_number: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QuiltIndexTaskValue {
    pub bucket_id: ObjectID,
    pub identifier: String,
    pub object_id: ObjectID,
    pub sequence_number: u64,
}

/// Storage interface for the Walrus Index (Dual Index System).
#[derive(Debug, Clone)]
pub struct WalrusIndexStore {
    // Reference to the underlying RocksDB instance for transaction support.
    db: Arc<typed_store::rocks::RocksDB>,
    // Primary index: bucket_id/identifier -> IndexTarget.
    primary_index: DBMap<String, IndexTarget>,
    // Object index: object_id -> bucket_id/identifier.
    object_index: DBMap<String, ObjectIndexValue>,
    // Sequence store: stores the last processed sequence number for resumption.
    sequence_store: DBMap<String, u64>,
    // Index for quilt patches, so that we can look up quilt patches by the corresponding
    // files' blob IDs.
    quilt_patch_index: DBMap<String, QuiltPatchId>,
    // Pending quilt index tasks: stores the quilt index tasks that are pending to be processed.
    pending_quilt_index_tasks: DBMap<QuiltIndexTaskId, QuiltIndexTaskValue>,
    // Retry quilt index tasks: stores the quilt index tasks that failed and need to be retried.
    retry_quilt_index_tasks: DBMap<QuiltIndexTaskId, QuiltIndexTaskValue>,
}

impl WalrusIndexStore {
    /// Create a new WalrusIndexStore by opening the database at the specified path.
    /// This initializes all required column families for the indexer.
    pub async fn open(db_path: &Path) -> Result<Self> {
        let db_options = RocksDbOptions::default();
        // Use OptimisticTransactionDB for better atomicity
        let db = typed_store::rocks::open_cf_opts_optimistic(
            db_path,
            None,
            MetricConf::default(),
            &[
                (CF_NAME_PRIMARY_INDEX, db_options.clone()),
                (CF_NAME_OBJECT_INDEX, db_options.clone()),
                (CF_NAME_SEQUENCE_STORE, db_options.clone()),
                (CF_NAME_QUILT_PATCH_INDEX, db_options.clone()),
                (CF_NAME_PENDING_QUILT_INDEX_TASKS, db_options.clone()),
                (CF_NAME_RETRY_QUILT_INDEX_TASKS, db_options.clone()),
            ],
        )?;

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

        let sequence_store: DBMap<String, u64> = DBMap::reopen(
            &db,
            Some(CF_NAME_SEQUENCE_STORE),
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
            db,
            primary_index,
            object_index,
            sequence_store,
            quilt_patch_index,
            pending_quilt_index_tasks,
            retry_quilt_index_tasks,
        })
    }

    /// Create a new WalrusIndexStore from existing DBMap instances.
    /// This is used for testing and migration purposes.
    pub fn from_maps(
        db: Arc<typed_store::rocks::RocksDB>,
        primary_index: DBMap<String, IndexTarget>,
        object_index: DBMap<String, ObjectIndexValue>,
        sequence_store: DBMap<String, u64>,
        quilt_patch_index: DBMap<String, QuiltPatchId>,
        pending_quilt_index_tasks: DBMap<QuiltIndexTaskId, QuiltIndexTaskValue>,
        retry_quilt_index_tasks: DBMap<QuiltIndexTaskId, QuiltIndexTaskValue>,
    ) -> Self {
        Self {
            db,
            primary_index,
            object_index,
            sequence_store,
            quilt_patch_index,
            pending_quilt_index_tasks,
            retry_quilt_index_tasks,
        }
    }

    /// Apply index mutations from Sui events with sequence number.
    /// All mutations and sequence number update are applied atomically in a single RocksDB batch
    /// transaction.
    /// This ensures that mutations and sequence progression are consistent.
    pub fn apply_index_mutations(
        &self,
        mutation_sets: Vec<walrus_sui::types::IndexMutationSet>,
        sequence_number: u64,
    ) -> Result<(), TypedStoreError> {
        // Create a single batch for all mutations and sequence number update.
        // TODO(heliu): consider add a batch size limit for the extreme cases.
        let txn = self
            .db
            .as_optimistic()
            .expect("Database is not optimistic transaction DB")
            .transaction();

        // Add all mutations to the transaction.
        for mutation_set in mutation_sets {
            for mutation in mutation_set.mutations {
                self.apply_mutation(&txn, &mutation_set.bucket_id, mutation, sequence_number)?;
            }
        }

        // Add sequence number update to the same transaction if provided.
        let seq_value = bcs::to_bytes(&sequence_number)
            .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;
        txn.put_cf(&self.sequence_store.cf()?, SEQUENCE_KEY, &seq_value)
            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;

        // Commit all mutations and sequence number update atomically in a single write.
        txn.commit()
            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))
    }

    /// Process a single mutation and add operations to the transaction.
    fn apply_mutation(
        &self,
        txn: &Transaction<'_, OptimisticTransactionDB>,
        bucket_id: &ObjectID, // bucket_id comes from the MutationSet
        mutation: walrus_sui::types::IndexMutation,
        sequence_number: u64,
    ) -> Result<(), TypedStoreError> {
        match mutation {
            walrus_sui::types::IndexMutation::Insert {
                identifier,
                object_id,
                blob_id,
                is_quilt,
            } => {
                let quilt_status = if is_quilt {
                    QuiltTaskStatus::Running(QuiltIndexTaskId::new(sequence_number, blob_id))
                } else {
                    QuiltTaskStatus::NotQuilt
                };
                let primary_key =
                    PrimaryIndexKey::new(*bucket_id, identifier.to_string(), sequence_number);
                let primary_value = BlobIdentity {
                    blob_id,
                    object_id,
                    quilt_status,
                };
                self.insert_primary_index(primary_key, primary_value, txn)?;
            }
            walrus_sui::types::IndexMutation::Delete {
                object_id,
                is_quilt: _,
            } => {
                self.apply_delete_by_object_id(txn, &object_id)?;
            }
        }
        Ok(())
    }

    /// Apply an insert operation to both indices.
    fn insert_primary_index(
        &self,
        primary_key: PrimaryIndexKey,
        primary_value: BlobIdentity,
        txn: &Transaction<'_, OptimisticTransactionDB>,
    ) -> Result<(), TypedStoreError> {
        // Primary index: bucket_id/identifier/sequence_number -> IndexTarget.
        let object_key = ObjectIndexKey::new(primary_value.object_id);
        let object_value = ObjectIndexValue {
            bucket_id: primary_key.bucket_id,
            identifier: primary_key.identifier.clone(),
            sequence_number: primary_key.sequence_number,
        };

        let index_target = IndexTarget::Blob(primary_value);
        // Use transaction operations for atomic writes.
        let primary_value_bytes = bcs::to_bytes(&index_target)
            .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;
        txn.put_cf(
            &self.primary_index.cf()?,
            primary_key.to_key(),
            &primary_value_bytes,
        )
        .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;

        let object_value_bytes = bcs::to_bytes(&object_value)
            .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;
        txn.put_cf(
            &self.object_index.cf()?,
            object_key.to_key(),
            &object_value_bytes,
        )
        .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;

        Ok(())
    }

    /// Add index entries for quilt patches, according to the quilt_index.
    pub fn populate_quilt_patch_index(
        &self,
        quilt_index_task: &QuiltIndexTask,
        quilt_index: &QuiltIndex,
    ) -> Result<(), TypedStoreError> {
        let txn = self
            .db
            .as_optimistic()
            .expect("Database is not optimistic transaction DB")
            .transaction();

        // If the task entry is not found, we can skip the processing.
        if !self.task_exists(quilt_index_task, &txn)? {
            tracing::info!(?quilt_index_task, "Quilt task does not exist, skipping");
            return Ok(());
        }

        let existing_entry = self.get_primary_index_entry(quilt_index_task, &txn)?;
        let Some(existing_entry) = existing_entry else {
            tracing::info!(
                "Primary index entry not found for quilt task: \
                bucket_id={}, identifier={}, sequence={}",
                quilt_index_task.bucket_id,
                quilt_index_task.identifier,
                quilt_index_task.sequence_number
            );
            return Ok(());
        };
        let IndexTarget::Blob(blob_identity) = existing_entry else {
            tracing::warn!("Primary index entry is not a blob: {:?}", existing_entry);
            unreachable!();
        };
        let task_id = quilt_index_task.task_id();

        match blob_identity.quilt_status {
            QuiltTaskStatus::Completed(_) => {
                tracing::info!("Quilt task already completed, skipping duplicate processing");
                return Ok(());
            }
            QuiltTaskStatus::Running(stored_task_id) => {
                assert_eq!(task_id, stored_task_id);
                assert!(!quilt_index_task.retry);
            }
            QuiltTaskStatus::InRetryQueue(stored_task_id) => {
                assert_eq!(task_id, stored_task_id);
                assert!(quilt_index_task.retry);
            }
            QuiltTaskStatus::NotQuilt => {
                tracing::error!(?quilt_index_task, "Unexpected quilt status: NotQuilt");
                return Ok(());
            }
        }

        let patches = get_patches_for_primary_index(quilt_index);
        let primary_key = PrimaryIndexKey::new(
            quilt_index_task.bucket_id,
            quilt_index_task.identifier.clone(),
            quilt_index_task.sequence_number,
        );

        // Update the quilt status to completed with patches.
        let updated_index_target = IndexTarget::Blob(BlobIdentity {
            blob_id: blob_identity.blob_id,
            object_id: blob_identity.object_id,
            quilt_status: QuiltTaskStatus::Completed(patches),
        });
        let updated_value_bytes = bcs::to_bytes(&updated_index_target)
            .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;
        txn.put_cf(
            &self.primary_index.cf()?,
            primary_key.to_key(),
            &updated_value_bytes,
        )
        .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;

        // Process all quilt patches.
        self.populate_quilt_patch_index_txn(quilt_index_task, quilt_index, &txn)?;

        // Remove the task.
        self.delete_task(quilt_index_task, &txn)?;

        // Commit the transaction.
        txn.commit()
            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))
    }

    /// Get the primary index entry for a quilt task, within a transaction.
    fn get_primary_index_entry(
        &self,
        quilt_index_task: &QuiltIndexTask,
        txn: &Transaction<'_, OptimisticTransactionDB>,
    ) -> Result<Option<IndexTarget>, TypedStoreError> {
        let primary_key = PrimaryIndexKey::new(
            quilt_index_task.bucket_id,
            quilt_index_task.identifier.clone(),
            quilt_index_task.sequence_number,
        );
        txn.get_for_update_cf(&self.primary_index.cf()?, primary_key.to_key(), true)
            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?
            .map(|v| {
                bcs::from_bytes(&v).map_err(|e| TypedStoreError::SerializationError(e.to_string()))
            })
            .transpose()
    }

    /// Check if a quilt task exists in the pending or retry queues, within a transaction.
    fn task_exists(
        &self,
        quilt_index_task: &QuiltIndexTask,
        txn: &Transaction<'_, OptimisticTransactionDB>,
    ) -> Result<bool, TypedStoreError> {
        let task_id = QuiltIndexTaskId::new(
            quilt_index_task.sequence_number,
            quilt_index_task.quilt_blob_id,
        );

        if quilt_index_task.retry {
            return Ok(txn
                .get_for_update_cf(&self.retry_quilt_index_tasks.cf()?, task_id.to_key(), true)
                .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?
                .is_some());
        }

        Ok(txn
            .get_for_update_cf(
                &self.pending_quilt_index_tasks.cf()?,
                task_id.to_key(),
                true,
            )
            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?
            .is_some())
    }

    fn populate_quilt_patch_index_txn(
        &self,
        quilt_index_task: &QuiltIndexTask,
        quilt_index: &QuiltIndex,
        txn: &Transaction<'_, OptimisticTransactionDB>,
    ) -> Result<(), TypedStoreError> {
        let primary_cf = self.primary_index.cf()?;
        let quilt_patch_cf = self.quilt_patch_index.cf()?;

        // Process all quilt patches.
        for patch in get_patches_for_primary_index(quilt_index) {
            let primary_key = PrimaryIndexKey::new(
                quilt_index_task.bucket_id,
                patch.identifier.clone(),
                quilt_index_task.sequence_number,
            );
            // Reconstruct QuiltPatchId from quilt_blob_id and internal_id.
            let quilt_patch_id = QuiltPatchId::new(
                quilt_index_task.quilt_blob_id,
                patch.quilt_patch_internal_id.clone(),
            );
            let primary_value = IndexTarget::QuiltPatchId(quilt_patch_id.clone());

            tracing::debug!(
                ?primary_key,
                ?primary_value,
                "Inserted quilt patch primary index key",
            );

            // Insert into primary index using transaction.
            let patch_value_bytes = bcs::to_bytes(&primary_value)
                .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;
            txn.put_cf(&primary_cf, primary_key.to_key(), &patch_value_bytes)
                .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;

            if let Some(patch_blob_id) = patch.patch_blob_id {
                let quilt_patch_index_key =
                    QuiltPatchIndexKey::new(patch_blob_id, quilt_index_task.object_id);

                tracing::debug!(
                    ?patch_blob_id,
                    ?quilt_patch_id,
                    ?quilt_index_task.object_id,
                    ?quilt_index_task.bucket_id,
                    ?patch.identifier,
                    ?quilt_index_task.quilt_blob_id,
                    ?quilt_patch_index_key,
                    "Inserted quilt patch index key",
                );

                let patch_id_bytes = bcs::to_bytes(&quilt_patch_id)
                    .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;
                txn.put_cf(
                    &quilt_patch_cf,
                    quilt_patch_index_key.to_key(),
                    &patch_id_bytes,
                )
                .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
            }
        }

        Ok(())
    }

    fn delete_task(
        &self,
        task: &QuiltIndexTask,
        txn: &Transaction<'_, OptimisticTransactionDB>,
    ) -> Result<(), TypedStoreError> {
        if task.retry {
            txn.delete_cf(&self.retry_quilt_index_tasks.cf()?, task.task_id().to_key())
                .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
        } else {
            txn.delete_cf(
                &self.pending_quilt_index_tasks.cf()?,
                task.task_id().to_key(),
            )
            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
        }

        Ok(())
    }

    /// Delete an index entry by bucket_id and identifier.
    /// Uses transactions to ensure atomic deletion across all indices.
    pub fn delete_by_bucket_identifier(
        &self,
        bucket_id: &ObjectID,
        identifier: &str,
    ) -> Result<(), TypedStoreError> {
        // Create a transaction for atomic operations
        let txn = self
            .db
            .as_optimistic()
            .expect("Database is not optimistic transaction DB")
            .transaction();

        // Get bounds for all entries with this bucket_id and identifier
        // This will match all sequence numbers for this identifier
        let (lower_bound, upper_bound) = get_primary_index_bounds(bucket_id, identifier);

        // Find and delete all entries within the bounds (typically only one)
        for entry in txn.iterator_cf_opt(
            &self.primary_index.cf()?,
            rocksdb::ReadOptions::default(),
            rocksdb::IteratorMode::From(&lower_bound, rocksdb::Direction::Forward),
        ) {
            let (key, value) = entry.map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;

            // Check if key is still within bounds
            if key.as_ref() >= upper_bound.as_slice() {
                break;
            }

            // Deserialize the IndexTarget to get the object_id
            let index_target: IndexTarget = bcs::from_bytes(&value)
                .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;

            // Delete from primary index
            txn.delete_cf(&self.primary_index.cf()?, &key)
                .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;

            // Delete from object index if it's a blob
            if let IndexTarget::Blob(blob_identity) = &index_target {
                let object_key = ObjectIndexKey::new(blob_identity.object_id);
                txn.delete_cf(&self.object_index.cf()?, object_key.to_key())
                    .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;

                // Handle quilt-specific cleanup
                match &blob_identity.quilt_status {
                    QuiltTaskStatus::InRetryQueue(task_id) => {
                        // Clean up retry task
                        let task_key = bcs::to_bytes(task_id)
                            .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;
                        txn.delete_cf(&self.retry_quilt_index_tasks.cf()?, task_key)
                            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
                    }
                    QuiltTaskStatus::Running(task_id) => {
                        // Clean up pending task
                        let task_key = bcs::to_bytes(task_id)
                            .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;
                        txn.delete_cf(&self.pending_quilt_index_tasks.cf()?, task_key)
                            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
                    }
                    QuiltTaskStatus::Completed(patches) => {
                        // Delete all associated patch entries
                        for patch in patches {
                            // Parse the primary key to get sequence_number
                            let parsed_key = PrimaryIndexKey::from_key(&key)
                                .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;

                            // Delete patch from primary index
                            let patch_primary_key = PrimaryIndexKey::new(
                                *bucket_id,
                                patch.identifier.clone(),
                                parsed_key.sequence_number,
                            );
                            txn.delete_cf(&self.primary_index.cf()?, patch_primary_key.to_key())
                                .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;

                            // Delete from quilt patch index if patch_blob_id exists
                            if let Some(patch_blob_id) = patch.patch_blob_id {
                                let quilt_patch_key =
                                    QuiltPatchIndexKey::new(patch_blob_id, blob_identity.object_id);
                                txn.delete_cf(
                                    &self.quilt_patch_index.cf()?,
                                    quilt_patch_key.to_key(),
                                )
                                .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
                            }
                        }
                    }
                    QuiltTaskStatus::NotQuilt => {
                        // Regular blob, no special cleanup needed
                    }
                }
            }
        }

        // Commit the transaction
        txn.commit()
            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))
    }

    /// Delete an index entry by object_id.
    pub fn delete_by_object_id(&self, object_id: &ObjectID) -> Result<(), TypedStoreError> {
        // Use transaction to delete from both indices atomically.
        let txn = self
            .db
            .as_optimistic()
            .expect("Database is not optimistic transaction DB")
            .transaction();
        self.apply_delete_by_object_id(&txn, object_id)?;
        txn.commit()
            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))
    }

    /// Delete all entries in a bucket.
    pub fn delete_bucket(&self, bucket_id: &ObjectID) -> Result<(), TypedStoreError> {
        // Use transaction for atomic deletion of all bucket entries
        let optimistic_db = self.db.as_optimistic().ok_or_else(|| {
            TypedStoreError::RocksDBError("Database is not optimistic transaction DB".to_string())
        })?;
        let txn = optimistic_db.transaction();

        // Use bucket bounds for efficient scanning
        let (lower_bound, upper_bound) = get_bucket_bounds(bucket_id);

        // Collect all keys to delete
        let mut keys_to_delete = Vec::new();
        for entry in self
            .primary_index
            .iter_with_bytes::<IndexTarget>(Some(&lower_bound), Some(&upper_bound))?
        {
            let (key, _) = entry?;
            keys_to_delete.push(key);
        }

        // Delete each entry using the transaction
        for key in keys_to_delete {
            self.apply_delete_by_key(&txn, &key)?;
        }

        txn.commit()
            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))
    }

    /// Apply a delete operation to both indices using the primary key.
    /// For quilts with Completed status, also deletes all associated patch entries.
    fn apply_delete_by_key(
        &self,
        txn: &Transaction<'_, OptimisticTransactionDB>,
        primary_key: &[u8],
    ) -> Result<(), TypedStoreError> {
        // Get column families
        let primary_cf = self
            .primary_index
            .cf()
            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
        let object_cf = self
            .object_index
            .cf()
            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;

        // Look up the primary value to get the object_id and handle patches
        let primary_value_bytes = txn
            .get_for_update_cf(&primary_cf, primary_key, true)
            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;

        if let Some(value_bytes) = primary_value_bytes {
            let index_target: IndexTarget = bcs::from_bytes(&value_bytes)
                .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;

            match &index_target {
                IndexTarget::Blob(blob_identity) => {
                    // Handle different quilt statuses
                    match &blob_identity.quilt_status {
                        QuiltTaskStatus::InRetryQueue(task_id) => {
                            // Clean up the retry task
                            let task_id_bytes = task_id.to_key();
                            let retry_cf = self
                                .retry_quilt_index_tasks
                                .cf()
                                .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
                            txn.delete_cf(&retry_cf, &task_id_bytes)
                                .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
                        }
                        QuiltTaskStatus::Running(task_id) => {
                            // Clean up the pending task
                            let task_id_bytes = task_id.to_key();
                            let pending_cf = self
                                .pending_quilt_index_tasks
                                .cf()
                                .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
                            txn.delete_cf(&pending_cf, &task_id_bytes)
                                .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
                        }
                        QuiltTaskStatus::Completed(patches) => {
                            // Delete all patch entries from both indices
                            for patch in patches {
                                // Parse the primary key to get bucket_id and sequence_number
                                if let Ok(parsed_key) = PrimaryIndexKey::from_key(primary_key) {
                                    // Delete patch from primary index
                                    let patch_primary_key = PrimaryIndexKey::new(
                                        parsed_key.bucket_id,
                                        patch.identifier.clone(),
                                        parsed_key.sequence_number,
                                    );
                                    txn.delete_cf(&primary_cf, patch_primary_key.to_key())
                                        .map_err(|e| {
                                            TypedStoreError::RocksDBError(e.to_string())
                                        })?;

                                    // Delete from quilt patch index if patch_blob_id exists
                                    if let Some(patch_blob_id) = patch.patch_blob_id {
                                        let quilt_patch_key = QuiltPatchIndexKey::new(
                                            patch_blob_id,
                                            blob_identity.object_id,
                                        );
                                        let quilt_patch_cf =
                                            self.quilt_patch_index.cf().map_err(|e| {
                                                TypedStoreError::RocksDBError(e.to_string())
                                            })?;
                                        txn.delete_cf(&quilt_patch_cf, quilt_patch_key.to_key())
                                            .map_err(|e| {
                                                TypedStoreError::RocksDBError(e.to_string())
                                            })?;
                                    }
                                }
                            }
                        }
                        QuiltTaskStatus::NotQuilt => {
                            // Regular blob, no special cleanup needed
                        }
                    }

                    // Delete the main entry from both indices
                    txn.delete_cf(&primary_cf, primary_key)
                        .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;

                    let object_key = ObjectIndexKey::new(blob_identity.object_id);
                    txn.delete_cf(&object_cf, object_key.to_key())
                        .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
                }
                IndexTarget::QuiltPatchId(_) => {
                    // Individual patch entries are deleted when their parent quilt is deleted
                    // We don't delete them independently
                    return Ok(());
                }
                IndexTarget::QuiltId(quilt_id) => {
                    // Legacy quilt ID handling
                    txn.delete_cf(&primary_cf, primary_key)
                        .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;

                    let object_key = ObjectIndexKey::new(*quilt_id);
                    txn.delete_cf(&object_cf, object_key.to_key())
                        .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
                }
            }
        }

        Ok(())
    }

    /// Delete quilt-related entries (tasks and patches) in a transaction.
    fn delete_quilt_entries(
        &self,
        txn: &Transaction<'_, OptimisticTransactionDB>,
        blob_identity: &BlobIdentity,
        object_value: &ObjectIndexValue,
    ) -> Result<(), TypedStoreError> {
        match &blob_identity.quilt_status {
            QuiltTaskStatus::InRetryQueue(task_id) => {
                // Clean up the retry task
                let task_id_bytes = bcs::to_bytes(task_id)
                    .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;
                let cf = self
                    .retry_quilt_index_tasks
                    .cf()
                    .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
                txn.delete_cf(&cf, &task_id_bytes)
                    .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
            }
            QuiltTaskStatus::Running(task_id) => {
                // Clean up the pending task
                let task_id_bytes = bcs::to_bytes(task_id)
                    .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;
                let cf = self
                    .pending_quilt_index_tasks
                    .cf()
                    .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
                txn.delete_cf(&cf, &task_id_bytes)
                    .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
            }
            QuiltTaskStatus::Completed(patches) => {
                // Delete all patch entries from both indices
                for patch in patches {
                    // Delete patch from primary index using byte keys
                    let patch_primary_key = PrimaryIndexKey::new(
                        object_value.bucket_id,
                        patch.identifier.clone(),
                        object_value.sequence_number,
                    );
                    let cf = self
                        .primary_index
                        .cf()
                        .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
                    txn.delete_cf(&cf, patch_primary_key.to_key())
                        .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;

                    // Delete from quilt patch index if patch_blob_id exists
                    if let Some(patch_blob_id) = patch.patch_blob_id {
                        let quilt_patch_key =
                            QuiltPatchIndexKey::new(patch_blob_id, blob_identity.object_id);
                        let cf = self
                            .quilt_patch_index
                            .cf()
                            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
                        txn.delete_cf(&cf, quilt_patch_key.to_key())
                            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
                    }
                }
            }
            QuiltTaskStatus::NotQuilt => {
                // Regular blob, no special cleanup needed
            }
        }
        Ok(())
    }

    /// Apply a delete operation to both indices using the object ID.
    /// For quilts with Completed status, also deletes all associated patch entries.
    fn apply_delete_by_object_id(
        &self,
        txn: &Transaction<'_, OptimisticTransactionDB>,
        object_id: &ObjectID,
    ) -> Result<(), TypedStoreError> {
        // Get the object index entry using the transaction
        let object_key = ObjectIndexKey::new(*object_id);
        let object_key_bytes = object_key.to_key();

        // Read from object index using transaction
        let object_cf = self
            .object_index
            .cf()
            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
        let object_value_bytes = txn
            .get_for_update_cf(&object_cf, &object_key_bytes, true)
            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;

        if let Some(value_bytes) = object_value_bytes {
            // Deserialize the object index value
            let object_value: ObjectIndexValue = bcs::from_bytes(&value_bytes)
                .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;

            // Construct the primary key
            let primary_key = PrimaryIndexKey::new(
                object_value.bucket_id,
                object_value.identifier.clone(),
                object_value.sequence_number,
            );
            let primary_key_bytes = primary_key.to_key();

            // Read the primary index entry to check if it's a quilt
            let primary_cf = self
                .primary_index
                .cf()
                .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
            let primary_value_bytes = txn
                .get_for_update_cf(&primary_cf, &primary_key_bytes, true)
                .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;

            if let Some(value_bytes) = primary_value_bytes {
                let index_target: IndexTarget = bcs::from_bytes(&value_bytes)
                    .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;

                // Handle quilt cleanup if needed
                if let IndexTarget::Blob(blob_identity) = &index_target {
                    self.delete_quilt_entries(txn, blob_identity, &object_value)?;
                }
            }

            // Delete from primary index
            txn.delete_cf(&primary_cf, &primary_key_bytes)
                .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;

            // Delete from object index
            txn.delete_cf(&object_cf, &object_key_bytes)
                .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
        }

        // If the object doesn't exist, it's already deleted - no error
        Ok(())
    }

    /// Get all index entries by bucket_id and identifier (primary index).
    /// Returns a vector since there could be multiple entries with different
    /// sequence_number values.
    pub fn get_by_bucket_identifier(
        &self,
        bucket_id: &ObjectID,
        identifier: &str,
    ) -> Result<Vec<IndexTarget>, TypedStoreError> {
        let mut results = Vec::new();
        let (lower_bound, upper_bound) = get_primary_index_bounds(bucket_id, identifier);
        for entry in self
            .primary_index
            .iter_with_bytes::<IndexTarget>(Some(&lower_bound), Some(&upper_bound))?
        {
            let (_, value) = entry?;
            results.push(value);
        }
        Ok(results)
    }

    pub fn get_quilt_patch_id_by_blob_id(
        &self,
        blob_id: &BlobId,
    ) -> Result<Vec<QuiltPatchId>, TypedStoreError> {
        tracing::debug!("Querying quilt patch for blob_id: {}", blob_id);

        let mut results = Vec::new();

        let (lower_bound, upper_bound) = get_quilt_patch_index_bounds(blob_id);
        // Use raw bytes iterator with bounds
        for entry in self
            .quilt_patch_index
            .iter_with_bytes::<QuiltPatchId>(Some(&lower_bound), Some(&upper_bound))?
        {
            let (key, value) = entry?;
            // Parse and verify it's the correct blob_id (defensive check)
            if let Ok((patch_blob_id, _object_id)) = parse_quilt_patch_index_key(&key) {
                if patch_blob_id == *blob_id {
                    results.push(value);
                }
            }
        }

        Ok(results)
    }

    /// Get an index entry by object_id (object index).
    pub fn get_by_object_id(
        &self,
        object_id: &ObjectID,
    ) -> Result<Option<BlobIdentity>, TypedStoreError> {
        // First get the bucket_identifier and sequence_number from object index.
        let object_key = ObjectIndexKey::new(*object_id);
        if let Some(object_value) = self
            .object_index
            .get_with_bytes::<ObjectIndexValue>(&object_key.to_key())?
        {
            // Now we can construct the exact primary key with sequence_number
            let primary_key = PrimaryIndexKey::new(
                object_value.bucket_id,
                object_value.identifier,
                object_value.sequence_number,
            );

            // Get the exact entry from primary index
            match self.primary_index.get_with_bytes(&primary_key.to_key())? {
                Some(IndexTarget::Blob(blob_identity)) => Ok(Some(blob_identity)),
                Some(_) => Ok(None), // Other variants are not BlobIdentity
                None => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    /// List all primary index entries in a bucket.
    pub fn list_blobs_in_bucket(
        &self,
        bucket_id: &ObjectID,
    ) -> Result<HashMap<String, IndexTarget>, TypedStoreError> {
        let (lower_bound, upper_bound) = get_bucket_bounds(bucket_id);
        let mut result = HashMap::new();

        // Use raw bytes API with bounds for efficient prefix scanning

        for entry in self
            .primary_index
            .iter_with_bytes::<IndexTarget>(Some(&lower_bound), Some(&upper_bound))?
        {
            let (key, value) = entry?;
            result.insert(
                PrimaryIndexKey::from_key(&key)
                    .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?
                    .to_string(),
                value,
            );
        }

        Ok(result)
    }

    /// Check if a primary index entry exists.
    pub fn has_primary_entry(
        &self,
        bucket_id: &ObjectID,
        primary_key: &str,
    ) -> Result<bool, TypedStoreError> {
        // Use the bounds function to search for all entries with this bucket and identifier
        let (lower_bound, upper_bound) = get_primary_index_bounds(bucket_id, primary_key);

        // Check if any entry exists with this prefix
        Ok(self
            .primary_index
            .iter_with_bytes::<IndexTarget>(Some(&lower_bound), Some(&upper_bound))?
            .next()
            .is_some())
    }

    /// Get statistics about a bucket.
    pub fn get_bucket_stats(&self, bucket_id: &ObjectID) -> Result<BucketStats, TypedStoreError> {
        let mut primary_count = 0u32;

        // Use the bucket bounds function for efficient scanning
        let (lower_bound, upper_bound) = get_bucket_bounds(bucket_id);

        // Count primary entries using raw bytes iterator.
        for entry in self
            .primary_index
            .iter_with_bytes::<IndexTarget>(Some(&lower_bound), Some(&upper_bound))?
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

    /// Get the last processed sequence number from storage.
    /// Returns None if no event has been processed yet.
    pub fn get_last_processed_sequence_number(&self) -> Result<Option<u64>, TypedStoreError> {
        self.sequence_store.get_with_bytes(SEQUENCE_KEY)
    }

    /// Set the last processed sequence number in storage.
    pub fn set_last_processed_sequence_number(&self, sequence: u64) -> Result<(), TypedStoreError> {
        self.sequence_store
            .insert_with_bytes(SEQUENCE_KEY, &sequence)
    }

    /// Get all entries from the quilt patch index.
    /// Returns a vector of (key, value) pairs.
    pub fn get_all_quilt_patch_entries(
        &self,
    ) -> Result<Vec<(String, QuiltPatchId)>, TypedStoreError> {
        let mut entries = Vec::new();
        for entry in self
            .quilt_patch_index
            .iter_with_bytes::<QuiltPatchId>(None, None)?
        {
            let (key, value) = entry?;
            entries.push((
                QuiltPatchIndexKey::from_key(&key)
                    .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?
                    .to_string(),
                value,
            ));
        }
        Ok(entries)
    }

    /// Get all entries from the pending quilt index tasks.
    /// Returns a vector of (key, value) pairs.
    pub fn get_all_pending_quilt_tasks(
        &self,
    ) -> Result<Vec<(QuiltIndexTaskId, QuiltIndexTaskValue)>, TypedStoreError> {
        let mut entries = Vec::new();
        for entry in self
            .pending_quilt_index_tasks
            .iter_with_bytes::<QuiltIndexTaskValue>(None, None)?
        {
            let (key, value) = entry?;
            entries.push((
                QuiltIndexTaskId::from_key(&key)
                    .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?,
                value,
            ));
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
    /// First checks the primary index status to avoid duplicate processing:
    /// - If already Running with matching task_id: skip (idempotent)
    /// - If already Completed: skip (already processed)
    /// - If in RetryQueue with matching task_id: skip (will be retried)
    /// - Only if not exists: add primary entry and task
    async fn store(&self, task: &crate::indexer::QuiltIndexTask) -> Result<(), TypedStoreError> {
        let task_id = QuiltIndexTaskId::new(task.sequence_number, task.quilt_blob_id);

        // Use transaction to ensure atomicity and visibility
        let optimistic_db = self.db.as_optimistic().ok_or_else(|| {
            TypedStoreError::RocksDBError("Database is not optimistic transaction DB".to_string())
        })?;
        let txn = optimistic_db.transaction();

        // First check if primary index entry already exists
        let primary_key = PrimaryIndexKey::new(
            task.bucket_id,
            task.identifier.clone(),
            task.sequence_number,
        );

        // Get the primary index entry with lock for update
        let existing_entry = txn
            .get_for_update_cf(&self.primary_index.cf()?, primary_key.to_key(), true)
            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;

        if let Some(entry_bytes) = existing_entry {
            // Entry exists, check its status
            let index_target: IndexTarget = bcs::from_bytes(&entry_bytes)
                .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;

            if let IndexTarget::Blob(blob_identity) = index_target {
                match &blob_identity.quilt_status {
                    QuiltTaskStatus::Running(existing_task_id) => {
                        // Already running, verify task_id matches
                        if existing_task_id == &task_id {
                            tracing::debug!(
                                ?task_id,
                                "Task already running with matching ID, skipping (idempotent)"
                            );
                        } else {
                            tracing::warn!(
                                ?task_id,
                                ?existing_task_id,
                                "Task already running with different ID, this shouldn't happen"
                            );
                        }
                        // Skip without error - idempotent behavior
                        return Ok(());
                    }
                    QuiltTaskStatus::Completed(_) => {
                        // Already completed, skip
                        tracing::debug!(?task_id, "Task already completed, skipping");
                        return Ok(());
                    }
                    QuiltTaskStatus::InRetryQueue(existing_task_id) => {
                        // In retry queue, check task_id
                        if existing_task_id == &task_id {
                            tracing::debug!(
                                ?task_id,
                                "Task already in retry queue with matching ID, skipping"
                            );
                        } else {
                            tracing::warn!(
                                ?task_id,
                                ?existing_task_id,
                                "Task in retry queue with different ID, this shouldn't happen"
                            );
                        }
                        return Ok(());
                    }
                    QuiltTaskStatus::NotQuilt => {
                        // Regular blob marked as NotQuilt, skip
                        tracing::debug!(?task_id, "Entry marked as NotQuilt, skipping");
                        return Ok(());
                    }
                }
            }
        }

        // Entry doesn't exist, proceed to add it

        // Store the task in pending queue
        let value = QuiltIndexTaskValue {
            object_id: task.object_id,
            bucket_id: task.bucket_id,
            identifier: task.identifier.clone(),
            sequence_number: task.sequence_number,
        };
        let value_bytes = bcs::to_bytes(&value)
            .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;
        txn.put_cf(
            &self.pending_quilt_index_tasks.cf()?,
            task_id.to_key(),
            &value_bytes,
        )
        .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;

        // Populate the primary index with Running status
        let blob_identity = BlobIdentity {
            blob_id: task.quilt_blob_id,
            object_id: task.object_id,
            quilt_status: QuiltTaskStatus::Running(task_id),
        };
        let index_target = IndexTarget::Blob(blob_identity);
        let index_target_bytes = bcs::to_bytes(&index_target)
            .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;
        txn.put_cf(
            &self.primary_index.cf()?,
            primary_key.to_key(),
            &index_target_bytes,
        )
        .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;

        // Also populate the object index for reverse lookup
        let object_key = ObjectIndexKey::new(task.object_id);
        let object_value = ObjectIndexValue {
            bucket_id: task.bucket_id,
            identifier: task.identifier.clone(),
            sequence_number: task.sequence_number,
        };
        let object_key_bytes = object_key.to_key();
        let object_value_bytes = bcs::to_bytes(&object_value)
            .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;
        txn.put_cf(
            &self.object_index.cf()?,
            &object_key_bytes,
            &object_value_bytes,
        )
        .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;

        // Update sequence number.
        let sequence_value = bcs::to_bytes(&task.sequence_number)
            .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;
        txn.put_cf(&self.sequence_store.cf()?, SEQUENCE_KEY, &sequence_value)
            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;

        // Commit the transaction
        txn.commit()
            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))
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

        // Convert task IDs to byte keys for iteration
        let from_task_id_bytes = from_task_id.as_ref().map(|id| id.to_key());
        let to_task_id_bytes = to_task_id.as_ref().map(|id| id.to_key());

        for entry in self
            .pending_quilt_index_tasks
            .iter_with_bytes::<QuiltIndexTaskValue>(
                from_task_id_bytes.as_deref(),
                to_task_id_bytes.as_deref(),
            )?
        {
            let (key_bytes, value) = entry?;

            // Parse the key from bytes
            let key = QuiltIndexTaskId::from_key(&key_bytes)
                .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;

            // Exclusive lower bound
            if let Some(ref from) = from_task_id {
                if key <= *from {
                    continue;
                }
            }

            // Exclusive upper bound
            if let Some(ref to) = to_task_id {
                if key >= *to {
                    break;
                }
            }

            result.push(crate::indexer::QuiltIndexTask::new(
                value.sequence_number,
                key.quilt_id,
                value.object_id,
                value.bucket_id,
                value.identifier,
                false, // Not a retry task
            ));
            count += 1;

            if count >= limit {
                break;
            }
        }

        Ok(result)
    }

    /// Add a task to the retry queue.
    /// First checks the primary index status to ensure proper state transitions:
    /// - Only moves from Running to InRetryQueue status
    /// - Skips if already Completed or already in retry queue
    /// - Ensures atomic transition with proper cleanup
    async fn add_to_retry_queue(
        &self,
        task: &crate::indexer::QuiltIndexTask,
    ) -> Result<(), TypedStoreError> {
        let task_id = QuiltIndexTaskId::new(task.sequence_number, task.quilt_blob_id);

        // Use transaction to ensure atomicity
        let optimistic_db = self.db.as_optimistic().ok_or_else(|| {
            TypedStoreError::RocksDBError("Database is not optimistic transaction DB".to_string())
        })?;
        let txn = optimistic_db.transaction();

        // 1. First check primary index entry status (main source of truth)
        let primary_key = PrimaryIndexKey::new(
            task.bucket_id,
            task.identifier.clone(),
            task.sequence_number,
        );
        let primary_key_bytes = primary_key.to_key();

        let existing_value_opt = txn
            .get_for_update_cf(&self.primary_index.cf()?, &primary_key_bytes, true)
            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;

        let Some(existing_value) = existing_value_opt else {
            tracing::warn!(
                ?task_id,
                "Primary index entry not found, cannot add to retry queue"
            );
            // If primary doesn't exist, clean up any orphaned pending task
            let task_id_bytes = task_id.to_key();
            txn.delete_cf(&self.pending_quilt_index_tasks.cf()?, &task_id_bytes)
                .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
            txn.commit()
                .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
            return Ok(());
        };

        let existing_entry: IndexTarget = bcs::from_bytes(&existing_value)
            .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;

        let IndexTarget::Blob(blob_identity) = existing_entry else {
            tracing::warn!(?task_id, "Primary index entry is not a blob, skipping");
            return Ok(());
        };

        // 2. Check the current status and handle accordingly
        let task_id_bytes = task_id.to_key();

        match &blob_identity.quilt_status {
            QuiltTaskStatus::Running(stored_task_id) => {
                // Expected case - proceed with retry
                if stored_task_id != &task_id {
                    tracing::warn!(
                        ?task_id,
                        ?stored_task_id,
                        "Task ID mismatch in Running status, but proceeding with retry"
                    );
                }

                // Verify task exists in pending queue before moving
                let pending_task = txn
                    .get_for_update_cf(&self.pending_quilt_index_tasks.cf()?, &task_id_bytes, true)
                    .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;

                if pending_task.is_none() {
                    tracing::warn!(
                        ?task_id,
                        "Task not found in pending queue but primary shows Running, \
                        will still mark as retry"
                    );
                    // We'll still proceed to mark it as InRetryQueue to maintain consistency
                }
                // Continue to step 3 to update status
            }
            QuiltTaskStatus::Completed(_) => {
                tracing::debug!(?task_id, "Task already completed, skipping retry");
                // Clean up any orphaned pending task
                txn.delete_cf(&self.pending_quilt_index_tasks.cf()?, &task_id_bytes)
                    .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
                txn.commit()
                    .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
                return Ok(());
            }
            QuiltTaskStatus::InRetryQueue(existing_task_id) => {
                if existing_task_id == &task_id {
                    tracing::debug!(
                        ?task_id,
                        "Task already in retry queue, skipping (idempotent)"
                    );
                } else {
                    tracing::warn!(
                        ?task_id,
                        ?existing_task_id,
                        "Different task already in retry queue for this entry"
                    );
                }
                return Ok(());
            }
            QuiltTaskStatus::NotQuilt => {
                tracing::debug!(?task_id, "Entry marked as NotQuilt, cannot retry");
                return Ok(());
            }
        }

        // 3. Update primary index to InRetryQueue status
        let updated_blob_identity = BlobIdentity {
            blob_id: blob_identity.blob_id,
            object_id: blob_identity.object_id,
            quilt_status: QuiltTaskStatus::InRetryQueue(task_id.clone()),
        };
        let updated_index_target = IndexTarget::Blob(updated_blob_identity);
        let updated_value_bytes = bcs::to_bytes(&updated_index_target)
            .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;
        txn.put_cf(
            &self.primary_index.cf()?,
            &primary_key_bytes,
            &updated_value_bytes,
        )
        .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;

        // 4. Move task from pending to retry queue
        // Delete from pending queue
        txn.delete_cf(&self.pending_quilt_index_tasks.cf()?, &task_id_bytes)
            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;

        // Add to retry queue
        let task_value = QuiltIndexTaskValue {
            object_id: task.object_id,
            bucket_id: task.bucket_id,
            identifier: task.identifier.clone(),
            sequence_number: task.sequence_number,
        };
        let task_value_bytes = bcs::to_bytes(&task_value)
            .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;
        txn.put_cf(
            &self.retry_quilt_index_tasks.cf()?,
            &task_id_bytes,
            &task_value_bytes,
        )
        .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;

        // 5. Commit the transaction
        txn.commit()
            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))
    }

    /// Read tasks from retry queue starting from the given task ID.
    async fn read_retry_tasks(
        &self,
        from_task_id: Option<QuiltIndexTaskId>,
        limit: usize,
    ) -> Result<Vec<crate::indexer::QuiltIndexTask>, TypedStoreError> {
        let mut count = 0;
        let mut result = Vec::new();

        // Convert task ID to byte key for iteration
        let from_task_id_bytes = from_task_id.as_ref().map(|id| id.to_key());

        for entry in self
            .retry_quilt_index_tasks
            .iter_with_bytes::<QuiltIndexTaskValue>(from_task_id_bytes.as_deref(), None)?
        {
            let (key_bytes, value) = entry?;

            // Parse the key from bytes
            let key = QuiltIndexTaskId::from_key(&key_bytes)
                .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;

            // Exclusive lower bound
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
                false, // Not a retry task
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
        sequence_number: u64,
    ) -> Result<(), TypedStoreError> {
        let primary_key = PrimaryIndexKey::new(*bucket_id, identifier.to_string(), sequence_number);

        // Get the current value.
        if let Some(mut index_target) = self.primary_index.get_with_bytes(&primary_key.to_key())? {
            // Update the quilt status if it's a blob.
            if let IndexTarget::Blob(ref mut blob_identity) = index_target {
                blob_identity.quilt_status = new_status;

                // Write back the updated value.
                let mut batch = self.primary_index.batch();
                batch.insert_batch_with_bytes(
                    &self.primary_index,
                    [(primary_key.to_key(), index_target)],
                )?;
                batch.write()?;
            }
        }

        Ok(())
    }

    /// Move a task from pending queue to retry queue.
    /// Uses transactions to ensure atomicity and checks primary index status.
    pub async fn move_to_retry_queue(
        &self,
        task_id: &QuiltIndexTaskId,
    ) -> Result<(), TypedStoreError> {
        // Use transaction to ensure atomicity
        let optimistic_db = self.db.as_optimistic().ok_or_else(|| {
            TypedStoreError::RocksDBError("Database is not optimistic transaction DB".to_string())
        })?;
        let txn = optimistic_db.transaction();

        // 1. Check if task exists in pending queue
        let task_id_bytes = task_id.to_key();
        let pending_task_opt = txn
            .get_for_update_cf(&self.pending_quilt_index_tasks.cf()?, &task_id_bytes, true)
            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;

        let Some(pending_task_bytes) = pending_task_opt else {
            tracing::debug!(
                ?task_id,
                "Task not found in pending queue, may have already been processed"
            );
            return Ok(());
        };

        let task_value: QuiltIndexTaskValue = bcs::from_bytes(&pending_task_bytes)
            .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;

        // 2. Check primary index status
        let primary_key = PrimaryIndexKey::new(
            task_value.bucket_id,
            task_value.identifier.clone(),
            task_value.sequence_number,
        );
        let primary_key_bytes = primary_key.to_key();

        let primary_entry_opt = txn
            .get_for_update_cf(&self.primary_index.cf()?, &primary_key_bytes, true)
            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;

        let Some(primary_entry_bytes) = primary_entry_opt else {
            tracing::warn!(
                ?task_id,
                "Primary index entry not found, cleaning up orphaned pending task"
            );
            // Clean up the orphaned pending task
            txn.delete_cf(&self.pending_quilt_index_tasks.cf()?, &task_id_bytes)
                .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
            txn.commit()
                .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
            return Ok(());
        };

        let primary_entry: IndexTarget = bcs::from_bytes(&primary_entry_bytes)
            .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;

        let IndexTarget::Blob(mut blob_identity) = primary_entry else {
            tracing::warn!(?task_id, "Primary index entry is not a blob");
            return Ok(());
        };

        // 3. Check current status and handle accordingly
        match &blob_identity.quilt_status {
            QuiltTaskStatus::Running(stored_task_id) => {
                // Expected case - proceed with move to retry
                if stored_task_id != task_id {
                    tracing::warn!(
                        ?task_id,
                        ?stored_task_id,
                        "Task ID mismatch in Running status, but proceeding"
                    );
                }
                // Continue to move to retry queue
            }
            QuiltTaskStatus::Completed(_) => {
                tracing::debug!(?task_id, "Task already completed, skipping move to retry");
                // Clean up the pending task
                txn.delete_cf(&self.pending_quilt_index_tasks.cf()?, &task_id_bytes)
                    .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
                txn.commit()
                    .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
                return Ok(());
            }
            QuiltTaskStatus::InRetryQueue(existing_task_id) => {
                if existing_task_id == task_id {
                    tracing::debug!(?task_id, "Task already in retry queue");
                    // Clean up the pending task if it still exists
                    txn.delete_cf(&self.pending_quilt_index_tasks.cf()?, &task_id_bytes)
                        .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
                } else {
                    tracing::warn!(
                        ?task_id,
                        ?existing_task_id,
                        "Different task in retry queue for this entry"
                    );
                }
                txn.commit()
                    .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
                return Ok(());
            }
            QuiltTaskStatus::NotQuilt => {
                tracing::debug!(?task_id, "Entry marked as NotQuilt, cannot move to retry");
                // Clean up the pending task
                txn.delete_cf(&self.pending_quilt_index_tasks.cf()?, &task_id_bytes)
                    .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
                txn.commit()
                    .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
                return Ok(());
            }
        }

        // 4. Update primary index to InRetryQueue status
        blob_identity.quilt_status = QuiltTaskStatus::InRetryQueue(task_id.clone());
        let updated_index_target = IndexTarget::Blob(blob_identity);
        let updated_value_bytes = bcs::to_bytes(&updated_index_target)
            .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;
        txn.put_cf(
            &self.primary_index.cf()?,
            &primary_key_bytes,
            &updated_value_bytes,
        )
        .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;

        // 5. Move task from pending to retry queue
        // Delete from pending queue
        txn.delete_cf(&self.pending_quilt_index_tasks.cf()?, &task_id_bytes)
            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;

        // Add to retry queue
        let retry_value_bytes = bcs::to_bytes(&task_value)
            .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;
        txn.put_cf(
            &self.retry_quilt_index_tasks.cf()?,
            &task_id_bytes,
            &retry_value_bytes,
        )
        .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;

        // 6. Commit the transaction
        txn.commit()
            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))
    }
}
#[cfg(test)]
mod tests {
    use core::num::NonZeroU16;
    use std::str::FromStr;

    use sui_types::event::EventID;
    use tempfile::TempDir;
    use walrus_core::{
        encoding::{
            EncodingConfigEnum,
            ReedSolomonEncodingConfig,
            quilt_encoding::{QuiltApi, QuiltVersionV1},
        },
        metadata::QuiltIndexV1,
    };
    use walrus_sdk::client::client_types::get_stored_quilt_patches;
    use walrus_sui::types::IndexMutation;

    use super::*;
    use crate::OrderedStore;

    fn create_test_blob_id() -> BlobId {
        // Create a test BlobId using metadata with random merkle root
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let mut blob_id = [0u8; 32];
        rng.fill(&mut blob_id);
        BlobId(blob_id)
    }

    async fn create_test_store() -> Result<(WalrusIndexStore, TempDir), Box<dyn std::error::Error>>
    {
        let temp_dir = TempDir::new()?;
        let store = WalrusIndexStore::open(temp_dir.path()).await?;
        Ok((store, temp_dir))
    }

    /// Helper to create a test quilt with specified number of blobs.
    fn create_test_quilt(
        num_blobs: usize,
    ) -> Result<
        (
            walrus_core::encoding::quilt_encoding::QuiltV1,
            BlobId,
            QuiltIndexV1,
        ),
        Box<dyn std::error::Error>,
    > {
        let quilt_test_data =
            walrus_core::test_utils::QuiltTestData::new_owned(num_blobs, 1024, 4096, 100, 10)?;
        let encoding_config = EncodingConfigEnum::ReedSolomon(&ReedSolomonEncodingConfig::new(
            NonZeroU16::try_from(26).unwrap(),
        ));
        let (quilt, quilt_blob_id) =
            quilt_test_data.construct_quilt::<QuiltVersionV1>(encoding_config)?;
        let quilt_index = quilt.quilt_index()?.clone();
        Ok((quilt, quilt_blob_id, quilt_index))
    }

    /// Helper to create and store a quilt index task.
    async fn create_and_store_quilt_task(
        store: &WalrusIndexStore,
        sequence: u64,
        quilt_blob_id: BlobId,
        object_id: ObjectID,
        bucket_id: ObjectID,
        identifier: &str,
        retry: bool,
    ) -> Result<crate::indexer::QuiltIndexTask, Box<dyn std::error::Error>> {
        let task = crate::indexer::QuiltIndexTask::new(
            sequence,
            quilt_blob_id,
            object_id,
            bucket_id,
            identifier.to_string(),
            retry,
        );
        store.store(&task).await?;
        Ok(task)
    }

    /// Helper to verify quilt status.
    fn verify_quilt_status(
        store: &WalrusIndexStore,
        bucket_id: &ObjectID,
        identifier: &str,
        expected_status: QuiltTaskStatusType,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let entries = store.get_by_bucket_identifier(bucket_id, identifier)?;
        assert_eq!(
            entries.len(),
            1,
            "Expected exactly one entry for {}",
            identifier
        );

        if let IndexTarget::Blob(blob_identity) = &entries[0] {
            match (&blob_identity.quilt_status, expected_status) {
                (QuiltTaskStatus::Running(_), QuiltTaskStatusType::Running) => Ok(()),
                (QuiltTaskStatus::Completed(_), QuiltTaskStatusType::Completed) => Ok(()),
                (QuiltTaskStatus::InRetryQueue(_), QuiltTaskStatusType::InRetryQueue) => Ok(()),
                (actual, expected) => panic!("Expected {:?} status, got {:?}", expected, actual),
            }
        } else {
            panic!("Expected IndexTarget::Blob entry");
        }
    }

    /// Enum for expected quilt status types.
    #[derive(Debug, PartialEq)]
    enum QuiltTaskStatusType {
        Running,
        Completed,
        InRetryQueue,
    }

    /// Helper to verify all patches are indexed.
    fn verify_patches_index(
        store: &WalrusIndexStore,
        bucket_id: &ObjectID,
        quilt_blob_id: &BlobId,
        quilt_index: &QuiltIndex,
        deleted: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for patch in get_stored_quilt_patches(quilt_index, *quilt_blob_id) {
            let patch_entries = store.get_by_bucket_identifier(bucket_id, &patch.identifier)?;
            let quilt_patch_id = QuiltPatchId::from_str(&patch.quilt_patch_id)?;
            if deleted {
                assert!(patch_entries.is_empty());
            } else {
                assert_eq!(patch_entries.len(), 1);
                assert_eq!(
                    patch_entries[0],
                    IndexTarget::QuiltPatchId(quilt_patch_id.clone())
                );

                if let Some(patch_blob_id) = patch.patch_blob_id {
                    let patch_by_blob_id = store.get_quilt_patch_id_by_blob_id(&patch_blob_id)?;
                    assert_eq!(patch_by_blob_id.len(), 1);
                    assert_eq!(patch_by_blob_id[0], quilt_patch_id);
                }
            }
        }
        Ok(())
    }

    /// Delete a quilt from the index.
    fn delete_quilt(
        store: &WalrusIndexStore,
        object_id: ObjectID,
        bucket_id: ObjectID,
        sequence: u64,
    ) -> Result<(), TypedStoreError> {
        let delete_mutation = IndexMutation::Delete {
            object_id,
            is_quilt: true,
        };
        store.apply_index_mutations(
            vec![walrus_sui::types::IndexMutationSet {
                bucket_id,
                mutations: vec![delete_mutation],
                event_id: EventID {
                    tx_digest: sui_types::base_types::TransactionDigest::new([0; 32]),
                    event_seq: sequence,
                },
            }],
            sequence,
        )
    }

    /// Helper to verify task is in pending queue.
    async fn check_pending_task(
        store: &WalrusIndexStore,
        sequence: u64,
        quilt_blob_id: BlobId,
        deleted: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let pending_tasks = store.read_range(None, None, 100).await?;
        let task_exists = pending_tasks
            .iter()
            .any(|t| t.sequence_number == sequence && t.quilt_blob_id == quilt_blob_id);

        if deleted {
            assert!(!task_exists, "Task should be deleted from pending queue");
        } else {
            assert!(task_exists, "Task not found in pending queue");
        }
        Ok(())
    }

    /// Helper to verify task is in retry queue.
    async fn check_retry_task(
        store: &WalrusIndexStore,
        sequence: u64,
        quilt_blob_id: BlobId,
        deleted: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let retry_tasks = store.read_retry_tasks(None, 100).await?;
        let task_exists = retry_tasks
            .iter()
            .any(|t| t.sequence_number == sequence && t.quilt_blob_id == quilt_blob_id);

        if deleted {
            assert!(!task_exists, "Task should be deleted from retry queue");
        } else {
            assert!(task_exists, "Task not found in retry queue");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_regular_blob_multiple_versions() -> Result<(), Box<dyn std::error::Error>> {
        let (store, _temp_dir) = create_test_store().await?;

        // Create test bucket, blob, and object.
        let bucket_id = ObjectID::random();
        let object_id = ObjectID::random();
        let identifier = "/photos/2025-09-14/koi-fish.jpg";
        let blob_id = BlobId([1; 32]);
        let mut sequence = 1;

        // Store an index entry using the new API.
        store.apply_index_mutations(
            vec![walrus_sui::types::IndexMutationSet {
                bucket_id,
                mutations: vec![walrus_sui::types::IndexMutation::Insert {
                    identifier: identifier.to_string(),
                    object_id,
                    blob_id,
                    is_quilt: false,
                }],
                event_id: sui_types::event::EventID {
                    tx_digest: sui_types::base_types::TransactionDigest::new([0; 32]),
                    event_seq: 1,
                },
            }],
            sequence,
        )?;

        sequence += 1;
        let object_id_dup = ObjectID::random();
        // Store an duplicate index entry using the new API.
        store.apply_index_mutations(
            vec![walrus_sui::types::IndexMutationSet {
                bucket_id,
                mutations: vec![walrus_sui::types::IndexMutation::Insert {
                    identifier: identifier.to_string(),
                    object_id: object_id_dup,
                    blob_id,
                    is_quilt: false,
                }],
                event_id: sui_types::event::EventID {
                    tx_digest: sui_types::base_types::TransactionDigest::new([0; 32]),
                    event_seq: 2,
                },
            }],
            sequence,
        )?;

        // Retrieve via primary index (bucket_id + identifier).
        let entries = store.get_by_bucket_identifier(&bucket_id, identifier)?;
        assert_eq!(entries.len(), 2);
        match &entries[0] {
            IndexTarget::Blob(retrieved) => {
                assert_eq!(retrieved.blob_id, blob_id);
                assert_eq!(retrieved.object_id, object_id);
            }
            _ => panic!("Expected IndexTarget::Blob"),
        }

        match &entries[1] {
            IndexTarget::Blob(retrieved) => {
                assert_eq!(retrieved.blob_id, blob_id);
                assert_eq!(retrieved.object_id, object_id_dup);
            }
            _ => panic!("Expected IndexTarget::Blob"),
        }

        // Retrieve via object index (object_id).
        let entry = store.get_by_object_id(&object_id)?;
        assert!(entry.is_some());

        let retrieved = entry.expect("Entry should be some");
        assert_eq!(retrieved.blob_id, blob_id);
        assert_eq!(retrieved.object_id, object_id);

        sequence += 1;
        // Delete by object_id.
        store.apply_index_mutations(
            vec![walrus_sui::types::IndexMutationSet {
                bucket_id,
                mutations: vec![walrus_sui::types::IndexMutation::Delete {
                    object_id,
                    is_quilt: false,
                }],
                event_id: sui_types::event::EventID {
                    tx_digest: sui_types::base_types::TransactionDigest::new([0; 32]),
                    event_seq: sequence,
                },
            }],
            sequence,
        )?;

        // Retrieve via object index (object_id).
        let entry = store.get_by_object_id(&object_id)?;
        assert!(entry.is_none());

        // Retrieve via primary index (bucket_id + identifier).
        let entries = store.get_by_bucket_identifier(&bucket_id, identifier)?;
        assert_eq!(entries.len(), 1);

        Ok(())
    }

    // Test the order of quilt index tasks.
    #[tokio::test]
    async fn test_quilt_index_tasks_order() -> Result<(), Box<dyn std::error::Error>> {
        use rand::Rng;

        let (store, _temp_dir) = create_test_store().await?;

        // Generate 2000 quilt index tasks
        const TOTAL_TASKS: usize = 2000;
        let bucket_id = ObjectID::random();
        let mut tasks = Vec::new();

        println!("Generating {} tasks...", TOTAL_TASKS);
        for i in 0..TOTAL_TASKS {
            let task = crate::indexer::QuiltIndexTask::new(
                i as u64,
                create_test_blob_id(),
                ObjectID::random(),
                bucket_id,
                format!("task_{:04}.quilt", i),
                false,
            );
            store.store(&task).await?;
            tasks.push(task);
        }

        // Read them all back with random batch sizes and verify order is preserved
        let mut rng = rand::thread_rng();
        let mut last_task_id: Option<QuiltIndexTaskId> = None;
        let mut current_task_id = 0;

        println!("Reading tasks with random batch sizes...");
        loop {
            // Generate random batch size between 1 and 200
            let batch_size = rng.gen_range(1..=200);

            // Read next batch
            let batch = store
                .read_range(last_task_id.clone(), None, batch_size)
                .await?;
            if batch.is_empty() {
                break;
            }
            last_task_id = Some(batch.last().expect("Batch should not be empty").task_id());
            for task in batch {
                assert_eq!(task.sequence_number, current_task_id);
                current_task_id += 1;
            }
        }
        assert_eq!(current_task_id, TOTAL_TASKS as u64);

        Ok(())
    }

    #[tokio::test]
    async fn test_quilt_blob_lifecycle_running_to_complete()
    -> Result<(), Box<dyn std::error::Error>> {
        let (store, _temp_dir) = create_test_store().await?;

        // Create test quilt
        let (_quilt, quilt_blob_id, quilt_index) = create_test_quilt(10)?;

        let bucket_id = ObjectID::random();
        let object_id = ObjectID::random();
        let identifier = "test.quilt";
        let sequence = 42u64;

        // Create and store the quilt task
        let task = create_and_store_quilt_task(
            &store,
            sequence,
            quilt_blob_id,
            object_id,
            bucket_id,
            identifier,
            false,
        )
        .await?;

        // Verify it's in Running state
        verify_quilt_status(&store, &bucket_id, identifier, QuiltTaskStatusType::Running)?;

        // Verify task is in pending queue
        check_pending_task(&store, sequence, quilt_blob_id, false).await?;

        let quilt_index = QuiltIndex::V1(quilt_index.clone());
        // Transition to complete state
        store.populate_quilt_patch_index(&task, &quilt_index)?;

        // Verify it's now Completed
        verify_quilt_status(
            &store,
            &bucket_id,
            identifier,
            QuiltTaskStatusType::Completed,
        )?;

        // Check pending queue is cleared
        check_pending_task(&store, sequence, quilt_blob_id, true).await?;

        // Verify all patches are indexed
        verify_patches_index(&store, &bucket_id, &quilt_blob_id, &quilt_index, false)?;

        // Delete the quilt
        delete_quilt(&store, object_id, bucket_id, 0)?;

        // Verify quilt is deleted
        let entries = store.get_by_bucket_identifier(&bucket_id, identifier)?;
        assert!(entries.is_empty());

        // Verify all patches are deleted
        verify_patches_index(&store, &bucket_id, &quilt_blob_id, &quilt_index, true)?;

        println!("✅ Quilt blob lifecycle (Running → Complete → Delete) test passed");
        Ok(())
    }

    #[tokio::test]
    async fn test_quilt_running_to_retry_transition() -> Result<(), Box<dyn std::error::Error>> {
        let (store, _temp_dir) = create_test_store().await?;

        // Create test quilt
        let (_quilt, quilt_blob_id, quilt_index) = create_test_quilt(5)?;

        let bucket_id = ObjectID::random();
        let object_id = ObjectID::random();
        let identifier = "retry_test.quilt";
        let sequence = 55u64;

        // Create and store the quilt task
        let task = create_and_store_quilt_task(
            &store,
            sequence,
            quilt_blob_id,
            object_id,
            bucket_id,
            identifier,
            false,
        )
        .await?;

        // Verify it's in Running state
        verify_quilt_status(&store, &bucket_id, identifier, QuiltTaskStatusType::Running)?;

        // Verify task is in pending queue
        check_pending_task(&store, sequence, quilt_blob_id, false).await?;

        // Transition to retry state
        store.add_to_retry_queue(&task).await?;

        // Verify it's now in InRetryQueue state
        verify_quilt_status(
            &store,
            &bucket_id,
            identifier,
            QuiltTaskStatusType::InRetryQueue,
        )?;

        // Check task is NOT in pending queue anymore
        check_pending_task(&store, sequence, quilt_blob_id, true).await?;

        // Verify task is in retry queue
        check_retry_task(&store, sequence, quilt_blob_id, false).await?;

        // Now complete the task from retry state
        // Create a version of the task with retry=true since it's in retry queue
        let retry_task = crate::indexer::QuiltIndexTask::new(
            sequence,
            quilt_blob_id,
            object_id,
            bucket_id,
            identifier.to_string(),
            true, // retry flag set to true
        );

        let quilt_index = QuiltIndex::V1(quilt_index.clone());
        store.populate_quilt_patch_index(&retry_task, &quilt_index)?;

        // Verify it's now Completed
        verify_quilt_status(
            &store,
            &bucket_id,
            identifier,
            QuiltTaskStatusType::Completed,
        )?;

        // Verify patches are indexed correctly
        verify_patches_index(&store, &bucket_id, &quilt_blob_id, &quilt_index, false)?;

        // Verify task is removed from retry queue
        let retry_tasks = store.read_retry_tasks(None, 10).await?;
        assert!(
            retry_tasks.is_empty(),
            "Retry queue should be empty after completion"
        );

        println!("✅ Quilt blob lifecycle (Running → Retry → Complete) test passed");
        Ok(())
    }

    #[tokio::test]
    async fn test_quilt_deletion_at_different_stages() -> Result<(), Box<dyn std::error::Error>> {
        let (store, _temp_dir) = create_test_store().await?;

        // Test 1: Delete quilt in Running state
        let (_quilt1, quilt_blob_id1, _) = create_test_quilt(3)?;
        let bucket_id1 = ObjectID::random();
        let object_id1 = ObjectID::random();
        let identifier1 = "running_delete.quilt";
        let event_index1 = 50;

        let _task1 = create_and_store_quilt_task(
            &store,
            event_index1,
            quilt_blob_id1,
            object_id1,
            bucket_id1,
            identifier1,
            false,
        )
        .await?;

        // Verify it's in Running state
        verify_quilt_status(
            &store,
            &bucket_id1,
            identifier1,
            QuiltTaskStatusType::Running,
        )?;

        // Delete while in Running state
        delete_quilt(&store, object_id1, bucket_id1, 0)?;

        // Verify deletion and task cleanup
        let entries = store.get_by_bucket_identifier(&bucket_id1, identifier1)?;
        assert!(entries.is_empty());
        let task_id1 = QuiltIndexTaskId::new(event_index1, quilt_blob_id1);
        assert!(store.pending_quilt_index_tasks.get(&task_id1)?.is_none());

        // Test 2: Delete quilt in Completed state with patches
        let (_quilt2, quilt_blob_id2, quilt_index2) = create_test_quilt(4)?;
        let bucket_id2 = ObjectID::random();
        let object_id2 = ObjectID::random();
        let identifier2 = "complete_delete.quilt";
        let event_index2 = 60;

        let task2 = create_and_store_quilt_task(
            &store,
            event_index2,
            quilt_blob_id2,
            object_id2,
            bucket_id2,
            identifier2,
            false,
        )
        .await?;

        // Complete with real quilt patches
        store.populate_quilt_patch_index(
            &task2,
            &walrus_core::metadata::QuiltIndex::V1(quilt_index2.clone()),
        )?;

        // Verify it's completed and patches are indexed
        verify_quilt_status(
            &store,
            &bucket_id2,
            identifier2,
            QuiltTaskStatusType::Completed,
        )?;
        // Task should be removed from pending after completion.
        check_pending_task(&store, event_index2, quilt_blob_id2, true).await?;

        // Delete completed quilt
        delete_quilt(&store, object_id2, bucket_id2, 0)?;

        // Verify quilt and patches are deleted
        assert!(store.get_by_object_id(&object_id2)?.is_none());
        verify_patches_index(
            &store,
            &bucket_id2,
            &quilt_blob_id2,
            &walrus_core::metadata::QuiltIndex::V1(quilt_index2),
            true,
        )?;

        // Test 3: Delete quilt in InRetryQueue state
        let (_quilt3, quilt_blob_id3, _) = create_test_quilt(2)?;
        let bucket_id3 = ObjectID::random();
        let object_id3 = ObjectID::random();
        let identifier3 = "retry_delete.quilt";
        let event_index3 = 70;

        let task3 = create_and_store_quilt_task(
            &store,
            event_index3,
            quilt_blob_id3,
            object_id3,
            bucket_id3,
            identifier3,
            false,
        )
        .await?;

        // Move to retry queue
        store.add_to_retry_queue(&task3).await?;

        // Verify it's in retry queue
        verify_quilt_status(
            &store,
            &bucket_id3,
            identifier3,
            QuiltTaskStatusType::InRetryQueue,
        )?;
        check_retry_task(&store, event_index3, quilt_blob_id3, false).await?;

        // Delete while in retry queue
        delete_quilt(&store, object_id3, bucket_id3, 0)?;

        // Verify deletion and retry task cleanup
        assert!(store.get_by_object_id(&object_id3)?.is_none());
        let task_id3 = QuiltIndexTaskId::new(event_index3, quilt_blob_id3);
        assert!(store.retry_quilt_index_tasks.get(&task_id3)?.is_none());

        println!("✅ Quilt deletion at different stages test passed");
        Ok(())
    }
}
