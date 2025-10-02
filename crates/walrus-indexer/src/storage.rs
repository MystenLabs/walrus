// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(dead_code)]

//! Storage layer for the Walrus Indexer.
//!
//! This module implements the Walrus Index schema for indexing blobs and quilt patches.
//! Indices are stored in RocksDB, organized by 'blob_manager' that provide isolated namespaces
//! similar to S3 buckets.

use std::{path::Path, sync::Arc};

use anyhow::Result;
use rocksdb::{OptimisticTransactionDB, Options as RocksDbOptions, Transaction};
use serde::{Deserialize, Serialize};
use sui_types::base_types::ObjectID;
use typed_store::{
    TypedStoreError,
    raw_key_db_map::{KeyCodec, RawKeyDBMap},
    rocks::{DBMap, MetricConf},
};
use walrus_core::{
    BlobId,
    QuiltPatchId,
    encoding::quilt_encoding::{QuiltIndexApi, QuiltPatchApi, QuiltPatchInternalIdApi},
    metadata::QuiltIndex,
};

/// Primary index, blob_manager/identifier/sequence_number -> blob or quilt patch.
const CF_NAME_PRIMARY_INDEX: &str = "walrus_index_primary";
/// Object index, object_id -> blob_manager/identifier/sequence_number.
const CF_NAME_OBJECT_INDEX: &str = "walrus_index_object";
/// Sequence store, sequence_number.
const CF_NAME_SEQUENCE_STORE: &str = "walrus_sequence_store";
/// Quilt patch index, blob_id of a quilt patch -> quilt_patch_id.
const CF_NAME_QUILT_PATCH_INDEX: &str = "walrus_quilt_patch";
/// Sequence store, sequence_number.
const SEQUENCE_KEY: &[u8] = b"sequence";

/// Key wrapper for String to implement KeyCodec (needed for sequence store).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmptyKey;

impl EmptyKey {
    /// A const instance that can be used anywhere.
    pub const INSTANCE: Self = EmptyKey;
}

impl KeyCodec for EmptyKey {
    fn serialize(&self) -> Result<Vec<u8>, TypedStoreError> {
        Ok(Vec::new())
    }

    fn deserialize(bytes: &[u8]) -> Result<Self, TypedStoreError> {
        if bytes.len() != 0 {
            return Err(TypedStoreError::SerializationError(
                "Invalid empty key length: expected 0 bytes".to_string(),
            ));
        }
        Ok(EmptyKey)
    }
}
/// Key structure for primary index: blob_manager/identifier/sequence_number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrimaryIndexKey {
    /// The blob manager ID.
    blob_manager: ObjectID,
    /// The identifier.
    identifier: String,
    /// The sequence number.
    sequence_number: u64,
}

impl PrimaryIndexKey {
    const SEQUENCE_LEN: usize = 8;

    fn new(blob_manager: ObjectID, identifier: String, sequence_number: u64) -> Self {
        Self {
            blob_manager,
            identifier,
            sequence_number,
        }
    }
}

impl KeyCodec for PrimaryIndexKey {
    fn serialize(&self) -> Result<Vec<u8>, TypedStoreError> {
        let mut bytes = Vec::new();

        // Add raw ObjectID bytes.
        bytes.extend_from_slice(self.blob_manager.as_ref());

        // Add '/' separator.
        bytes.push(b'/');

        // Add identifier as UTF-8 bytes.
        bytes.extend_from_slice(self.identifier.as_bytes());

        // Add '/' separator.
        bytes.push(b'/');

        // Add sequence number as big-endian bytes (8 bytes) to preserve u64 order.
        bytes.extend_from_slice(&self.sequence_number.to_be_bytes());

        Ok(bytes)
    }

    fn deserialize(bytes: &[u8]) -> Result<Self, TypedStoreError> {
        // Minimum size: 32 (blob_manager) + 1 (/) + 0 (empty identifier) + 1 (/) + 8 (sequence).
        if bytes.len() < 42 {
            return Err(TypedStoreError::SerializationError(
                "Key too short: minimum 42 bytes required".to_string(),
            ));
        }

        // Extract blob_manager (first 32 bytes).
        let blob_manager = ObjectID::from_bytes(&bytes[0..32])
            .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;

        // Check for first separator.
        if bytes[32] != b'/' {
            return Err(TypedStoreError::SerializationError(
                "Missing first '/' separator after blob_manager".to_string(),
            ));
        }

        // Extract sequence number (last 8 bytes).
        let seq_bytes: [u8; Self::SEQUENCE_LEN] = bytes[bytes.len() - Self::SEQUENCE_LEN..]
            .try_into()
            .map_err(|_| {
                TypedStoreError::SerializationError("Invalid sequence bytes".to_string())
            })?;
        let sequence_number = u64::from_be_bytes(seq_bytes);

        // Check for second separator before sequence.
        if bytes[bytes.len() - Self::SEQUENCE_LEN - 1] != b'/' {
            return Err(TypedStoreError::SerializationError(
                "Missing second '/' separator before sequence".to_string(),
            ));
        }

        // Extract identifier (everything between the two separators).
        // From position 33 to (length - 9).
        let identifier =
            String::from_utf8(bytes[33..bytes.len() - Self::SEQUENCE_LEN - 1].to_vec())
                .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;

        Ok(Self {
            blob_manager,
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
            self.blob_manager, self.identifier, self.sequence_number
        )
    }
}

/// Key structure for object index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectIndexKey {
    object_id: ObjectID,
}

impl ObjectIndexKey {
    fn new(object_id: ObjectID) -> Self {
        Self { object_id }
    }
}

impl KeyCodec for ObjectIndexKey {
    fn serialize(&self) -> Result<Vec<u8>, TypedStoreError> {
        // Simply use the raw ObjectID bytes (32 bytes).
        Ok(self.object_id.as_ref().to_vec())
    }

    fn deserialize(bytes: &[u8]) -> Result<Self, TypedStoreError> {
        if bytes.len() != 32 {
            return Err(TypedStoreError::SerializationError(
                "Invalid object_id length: expected 32 bytes".to_string(),
            ));
        }
        Ok(Self {
            object_id: ObjectID::from_bytes(bytes)
                .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?,
        })
    }
}

impl std::fmt::Display for ObjectIndexKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.object_id)
    }
}

/// Key structure for quilt patch index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuiltPatchIndexKey {
    /// The blob ID of the quilt patch.
    patch_blob_id: BlobId,
    /// The object ID of the containing quilt.
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
}

impl KeyCodec for QuiltPatchIndexKey {
    fn serialize(&self) -> Result<Vec<u8>, TypedStoreError> {
        let mut bytes = Vec::new();

        // Add BlobId bytes (32 bytes).
        bytes.extend_from_slice(self.patch_blob_id.as_ref());

        // Add \x00 separator.
        bytes.push(0x00);

        // Add ObjectID bytes (32 bytes).
        bytes.extend_from_slice(self.object_id.as_ref());

        Ok(bytes)
    }

    fn deserialize(bytes: &[u8]) -> Result<Self, TypedStoreError> {
        if bytes.len() != 65 {
            return Err(TypedStoreError::SerializationError(
                "invalid key length: expected 65 bytes (32 + 1 + 32)".to_string(),
            ));
        }

        // Check for separator at position 32.
        if bytes[32] != 0x00 {
            return Err(TypedStoreError::SerializationError(
                "invalid quilt patch blob id".to_string(),
            ));
        }

        // Extract BlobId (first 32 bytes).
        let blob_id_bytes: [u8; 32] = bytes[0..32].try_into().map_err(|_| {
            TypedStoreError::SerializationError("Invalid blob id bytes".to_string())
        })?;
        let patch_blob_id = BlobId(blob_id_bytes);

        // Extract ObjectID (last 32 bytes).
        let object_id = ObjectID::from_bytes(&bytes[33..65])
            .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;

        Ok(Self {
            patch_blob_id,
            object_id,
        })
    }
}

// For scanning all entries with a specific blob_manager and identifier.
fn get_primary_index_bounds(blob_manager: &ObjectID, identifier: &str) -> (Vec<u8>, Vec<u8>) {
    let mut lower_bound = Vec::new();

    // Add raw ObjectID bytes (32 bytes).
    lower_bound.extend_from_slice(blob_manager.as_ref());

    // Add '/' separator.
    lower_bound.push(b'/');

    // Add identifier.
    lower_bound.extend_from_slice(identifier.as_bytes());

    let mut upper_bound = lower_bound.clone();

    // Lower bound: add '/' to start the sequence range.
    lower_bound.push(b'/');

    // Upper bound: use '0' (ASCII 48, next char after '/' ASCII 47) to exclude all sequences.
    upper_bound.push(b'0');

    (lower_bound, upper_bound)
}

// For scanning all entries in a blob manager.
fn get_blob_manager_bounds(blob_manager: &ObjectID) -> (Vec<u8>, Vec<u8>) {
    let mut lower_bound = Vec::new();
    let mut upper_bound = Vec::new();

    // Lower bound: blob_manager/.
    lower_bound.extend_from_slice(blob_manager.as_ref());
    lower_bound.push(b'/');

    // Upper bound: blob_manager followed by '0' (next ASCII char after '/').
    upper_bound.extend_from_slice(blob_manager.as_ref());
    upper_bound.push(b'0');

    (lower_bound, upper_bound)
}

// For scanning all entries with a specific patch_blob_id.
fn get_quilt_patch_index_bounds(patch_blob_id: &BlobId) -> (Vec<u8>, Vec<u8>) {
    let mut lower_bound = Vec::new();
    let mut upper_bound = Vec::new();

    // Lower bound: patch_blob_id followed by \x00.
    lower_bound.extend_from_slice(patch_blob_id.as_ref());
    lower_bound.push(0x00);

    // Upper bound: patch_blob_id followed by \x01 (next byte after \x00).
    upper_bound.extend_from_slice(patch_blob_id.as_ref());
    upper_bound.push(0x01);

    (lower_bound, upper_bound)
}

// A concise representation of a quilt patch.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Patch {
    /// The identifier.
    pub identifier: String,
    /// The internal ID, can be combined with quilt_id to get QuiltPatchId.
    pub quilt_patch_internal_id: Vec<u8>,
    /// The patch blob ID.
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
            // TODO: Add the actual blob id when this is supported.
            patch_blob_id: None,
        })
        .collect()
}

/// The target of an index entry, it could be a blob, a quilt patch or a quilt.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum IndexTarget {
    /// Blob ID.
    Blob(BlobIdentity),
    /// Quilt patch ID.
    QuiltPatchId(QuiltPatchId),
}

/// Blob identity containing both blob_id and object_id.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlobIdentity {
    /// The blob ID.
    pub blob_id: BlobId,
    /// The Sui object ID.
    pub object_id: ObjectID,
}

/// Value stored in the object index.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectIndexValue {
    /// The blob manager ID.
    pub blob_manager: ObjectID,
    /// The identifier.
    pub identifier: String,
    /// The sequence number.
    pub sequence_number: u64,
}

/// Storage interface for the Walrus Index (Dual Index System).
#[derive(Debug, Clone)]
pub struct WalrusIndexStore {
    // Reference to the underlying RocksDB instance for transaction support.
    db: Arc<typed_store::rocks::RocksDB>,
    // Primary index: blob_manager/identifier/sequence -> IndexTarget.
    primary_index: RawKeyDBMap<PrimaryIndexKey, IndexTarget>,
    // Object index: object_id -> blob_manager/identifier.
    object_index: RawKeyDBMap<ObjectIndexKey, ObjectIndexValue>,
    // Sequence store: stores the last processed sequence number for resumption.
    sequence_store: RawKeyDBMap<EmptyKey, u64>,
    // Index for quilt patches, so that we can look up quilt patches by the corresponding
    // files' blob IDs.
    quilt_patch_index: RawKeyDBMap<QuiltPatchIndexKey, QuiltPatchId>,
}

impl WalrusIndexStore {
    /// Create a new WalrusIndexStore.
    pub async fn open(db_path: &Path) -> Result<Self> {
        let db_options = RocksDbOptions::default();
        // Use OptimisticTransactionDB for transaction support.
        let db = typed_store::rocks::open_cf_opts_optimistic(
            db_path,
            None,
            MetricConf::default(),
            &[
                (CF_NAME_PRIMARY_INDEX, db_options.clone()),
                (CF_NAME_OBJECT_INDEX, db_options.clone()),
                (CF_NAME_SEQUENCE_STORE, db_options.clone()),
                (CF_NAME_QUILT_PATCH_INDEX, db_options.clone()),
            ],
        )?;

        // Create RawKeyDBMaps for each column family
        let primary_index = RawKeyDBMap::new(DBMap::reopen(
            &db,
            Some(CF_NAME_PRIMARY_INDEX),
            &typed_store::rocks::ReadWriteOptions::default(),
            false,
        )?);

        let object_index = RawKeyDBMap::new(DBMap::reopen(
            &db,
            Some(CF_NAME_OBJECT_INDEX),
            &typed_store::rocks::ReadWriteOptions::default(),
            false,
        )?);

        let sequence_store = RawKeyDBMap::new(DBMap::reopen(
            &db,
            Some(CF_NAME_SEQUENCE_STORE),
            &typed_store::rocks::ReadWriteOptions::default(),
            false,
        )?);

        let quilt_patch_index = RawKeyDBMap::new(DBMap::reopen(
            &db,
            Some(CF_NAME_QUILT_PATCH_INDEX),
            &typed_store::rocks::ReadWriteOptions::default(),
            false,
        )?);

        Ok(Self {
            db,
            primary_index: primary_index?,
            object_index: object_index?,
            sequence_store: sequence_store?,
            quilt_patch_index: quilt_patch_index?,
        })
    }

    /// Apply an insert operation to both indices.
    fn insert_primary_index(
        &self,
        primary_key: &PrimaryIndexKey,
        primary_value: &IndexTarget,
        txn: &Transaction<'_, OptimisticTransactionDB>,
    ) -> Result<(), TypedStoreError> {
        if let IndexTarget::Blob(blob_identity) = &primary_value {
            let object_key = ObjectIndexKey::new(blob_identity.object_id);
            let object_value = ObjectIndexValue {
                blob_manager: primary_key.blob_manager,
                identifier: primary_key.identifier.clone(),
                sequence_number: primary_key.sequence_number,
            };
            self.object_index
                .put_cf_with_txn(txn, &object_key, &object_value)?;
        }

        // Primary index: blob_manager/identifier/sequence_number -> IndexTarget.
        self.primary_index
            .put_cf_with_txn(txn, &primary_key, &primary_value)?;

        Ok(())
    }

    /// Get the blob(s) by blob_manager/identifier, note that there could be multiple
    /// blobs with the same identifier in the same blob manager.
    pub fn get_blob_by_identifier(
        &self,
        blob_manager: &ObjectID,
        identifier: &str,
        txn: &Transaction<'_, OptimisticTransactionDB>,
    ) -> Result<Vec<(PrimaryIndexKey, IndexTarget)>, TypedStoreError> {
        let (begin, end) = get_primary_index_bounds(blob_manager, identifier);
        self.primary_index.read_range(txn, begin, end, usize::MAX)
    }

    /// Get the blob by object ID.
    pub fn get_blob_by_object_id(
        &self,
        object_id: &ObjectID,
        txn: &Transaction<'_, OptimisticTransactionDB>,
    ) -> Result<Option<ObjectIndexValue>, TypedStoreError> {
        let object_key = ObjectIndexKey::new(*object_id);
        self.object_index.get_cf_with_txn(txn, &object_key)
    }
}

#[cfg(test)]
mod tests {
    use sui_types::base_types::ObjectID;
    use tempfile::TempDir;
    use walrus_core::BlobId;

    use super::*;

    #[tokio::test]
    async fn test_keycodec_serialization() {
        // Test PrimaryIndexKey serialization/deserialization
        let object_id = ObjectID::from_single_byte(1);
        let key = PrimaryIndexKey::new(object_id, "test-identifier".to_string(), 42);

        let serialized = key.serialize().expect("Serialization should work");
        let deserialized =
            PrimaryIndexKey::deserialize(&serialized).expect("Deserialization should work");

        assert_eq!(key, deserialized);
        assert_eq!(key.blob_manager, object_id);
        assert_eq!(key.identifier, "test-identifier");
        assert_eq!(key.sequence_number, 42);

        // Test ObjectIndexKey serialization/deserialization
        let obj_key = ObjectIndexKey::new(object_id);
        let obj_serialized = obj_key
            .serialize()
            .expect("Object key serialization should work");
        let obj_deserialized = ObjectIndexKey::deserialize(&obj_serialized)
            .expect("Object key deserialization should work");

        assert_eq!(obj_key, obj_deserialized);
        assert_eq!(obj_key.object_id, object_id);

        // Test QuiltPatchIndexKey serialization/deserialization
        let blob_id = BlobId([1u8; 32]);
        let quilt_key = QuiltPatchIndexKey::new(blob_id, object_id);
        let quilt_serialized = quilt_key
            .serialize()
            .expect("Quilt key serialization should work");
        let quilt_deserialized = QuiltPatchIndexKey::deserialize(&quilt_serialized)
            .expect("Quilt key deserialization should work");

        assert_eq!(quilt_key, quilt_deserialized);
        assert_eq!(quilt_key.patch_blob_id, blob_id);
        assert_eq!(quilt_key.object_id, object_id);

        // Test StringKey serialization/deserialization
        let string_key = EmptyKey;
        let string_serialized = string_key
            .serialize()
            .expect("String key serialization should work");
        let string_deserialized = EmptyKey::deserialize(&string_serialized)
            .expect("String key deserialization should work");

        assert_eq!(string_key, string_deserialized);
    }

    fn generate_random_primary_index_entries(
        bucket_id: &ObjectID,
        identifier: &str,
        start_sequence: u64,
        num: usize,
    ) -> Vec<(PrimaryIndexKey, IndexTarget)> {
        (0..num)
            .map(|i| {
                let primary_key = PrimaryIndexKey::new(
                    *bucket_id,
                    identifier.to_string(),
                    start_sequence + i as u64,
                );
                let blob_identity = BlobIdentity {
                    blob_id: BlobId([(start_sequence + i as u64) as u8; 32]),
                    object_id: ObjectID::random(),
                };
                (primary_key, IndexTarget::Blob(blob_identity))
            })
            .collect()
    }

    #[tokio::test]
    async fn test_raw_key_db_map_operations() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = WalrusIndexStore::open(temp_dir.path())
            .await
            .expect("Failed to open store");
        let bucket_id = ObjectID::random();

        let txn = store
            .db
            .as_optimistic()
            .expect("Should be optimistic DB")
            .transaction();

        let walrus_batch = generate_random_primary_index_entries(&bucket_id, "test-walrus", 1, 3);
        for (primary_key, index_target) in &walrus_batch {
            store
                .insert_primary_index(&primary_key, &index_target, &txn)
                .expect("Insert should work");
        }
        txn.commit().expect("Transaction commit should work");

        let fish_batch = generate_random_primary_index_entries(&bucket_id, "test-fish", 5, 4);
        let txn = store
            .db
            .as_optimistic()
            .expect("Should be optimistic DB")
            .transaction();
        for (primary_key, index_target) in &fish_batch {
            store
                .insert_primary_index(&primary_key, &index_target, &txn)
                .expect("Insert should work");
        }
        txn.commit().expect("Transaction commit should work");

        let txn = store
            .db
            .as_optimistic()
            .expect("Should be optimistic DB")
            .transaction();

        let read_walrus = store
            .get_blob_by_identifier(&bucket_id, "test-walrus", &txn)
            .expect("Get should work");
        assert_eq!(read_walrus.len(), 3);
        for (i, (primary_key, index_target)) in read_walrus.iter().enumerate() {
            assert_eq!(primary_key, &walrus_batch[i].0);
            assert_eq!(index_target, &walrus_batch[i].1);
            if let IndexTarget::Blob(blob_identity) = &walrus_batch[i].1 {
                let object_value = store
                    .get_blob_by_object_id(&blob_identity.object_id, &txn)
                    .expect("Get should work")
                    .expect("Object value should exist");
                assert_eq!(object_value.blob_manager, bucket_id);
                assert_eq!(object_value.identifier, primary_key.identifier);
                assert_eq!(object_value.sequence_number, primary_key.sequence_number);
            }
        }

        let read_fish = store
            .get_blob_by_identifier(&bucket_id, "test-fish", &txn)
            .expect("Get should work");
        assert_eq!(read_fish.len(), 4);
        for (i, (primary_key, index_target)) in read_fish.iter().enumerate() {
            assert_eq!(primary_key, &fish_batch[i].0);
            assert_eq!(index_target, &fish_batch[i].1);
            if let IndexTarget::Blob(blob_identity) = &fish_batch[i].1 {
                let object_value = store
                    .get_blob_by_object_id(&blob_identity.object_id, &txn)
                    .expect("Get should work")
                    .expect("Object value should exist");
                assert_eq!(object_value.blob_manager, bucket_id);
                assert_eq!(object_value.identifier, primary_key.identifier);
                assert_eq!(object_value.sequence_number, primary_key.sequence_number);
            }
        }
    }

    #[tokio::test]
    async fn test_key_ordering() {
        // Test that primary keys maintain proper ordering for sequence numbers.
        let object_id = ObjectID::from_single_byte(1);
        let key1 = PrimaryIndexKey::new(object_id, "test".to_string(), 1);
        let key2 = PrimaryIndexKey::new(object_id, "test".to_string(), 2);
        let key3 = PrimaryIndexKey::new(object_id, "test".to_string(), 10);

        let serialized1 = key1.serialize().unwrap();
        let serialized2 = key2.serialize().unwrap();
        let serialized3 = key3.serialize().unwrap();

        // Verify byte ordering matches numeric ordering.
        assert!(serialized1 < serialized2);
        assert!(serialized2 < serialized3);
        assert!(serialized1 < serialized3);
    }
}
