// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Storage layer for the Walrus Indexer.
//!
//! This module implements the Walrus Index schema for indexing blobs and quilt patches.
//! Indices are stored in RocksDB, organized by 'blob_manager' that provide isolated namespaces
//! similar to S3 buckets.

use std::{path::Path, sync::Arc};

use anyhow::Result;
use bcs;
use rocksdb::{OptimisticTransactionDB, Options as RocksDbOptions, Transaction};
use serde::{Deserialize, Serialize};
use sui_types::base_types::ObjectID;
use typed_store::{
    TypedStoreError,
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

/// Key structure for primary index: blob_manager/identifier/sequence_number.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PrimaryIndexKey {
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

    /// Custom serialization to preserve alphanumeric order.
    /// Format: [blob_manager_bytes(32 bytes)] / [identifier_utf8] / [sequence_be_bytes(8 bytes)]
    fn to_key(&self) -> Vec<u8> {
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

        bytes
    }

    /// Parse from byte representation.
    /// Optimized parsing: first 32 bytes = blob_manager, last 8 bytes = sequence,
    /// middle = /<identifier>/.
    fn from_key(bytes: &[u8]) -> Result<Self> {
        // Minimum size: 32 (blob_manager) + 1 (/) + 0 (empty identifier) + 1 (/) + 8 (sequence)
        if bytes.len() < 42 {
            return Err(anyhow::anyhow!("Key too short: minimum 42 bytes required"));
        }

        // Extract blob_manager (first 32 bytes)
        let blob_manager = ObjectID::from_bytes(&bytes[0..32])?;

        // Check for first separator
        if bytes[32] != b'/' {
            return Err(anyhow::anyhow!(
                "Missing first '/' separator after blob_manager"
            ));
        }

        // Extract sequence number (last 8 bytes).
        let seq_bytes: [u8; Self::SEQUENCE_LEN] =
            bytes[bytes.len() - Self::SEQUENCE_LEN..].try_into()?;
        let sequence_number = u64::from_be_bytes(seq_bytes);

        // Check for second separator before sequence.
        if bytes[bytes.len() - Self::SEQUENCE_LEN - 1] != b'/' {
            return Err(anyhow::anyhow!(
                "Missing second '/' separator before sequence"
            ));
        }

        // Extract identifier (everything between the two separators)
        // From position 33 to (length - 9)
        let identifier =
            String::from_utf8(bytes[33..bytes.len() - Self::SEQUENCE_LEN - 1].to_vec())?;

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
struct ObjectIndexKey {
    object_id: ObjectID,
}

impl ObjectIndexKey {
    fn new(object_id: ObjectID) -> Self {
        Self { object_id }
    }

    /// Convert to optimized byte representation for RocksDB key.
    /// Format: [object_id_bytes(32 bytes)]
    fn to_key(&self) -> Vec<u8> {
        // Simply use the raw ObjectID bytes (32 bytes)
        self.object_id.as_ref().to_vec()
    }

    /// Parse from byte representation.
    #[allow(unused)]
    fn from_key(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 32 {
            return Err(anyhow::anyhow!(
                "Invalid object_id length: expected 32 bytes"
            ));
        }
        Ok(Self {
            object_id: ObjectID::from_bytes(bytes)?,
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
struct QuiltPatchIndexKey {
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

    /// Convert to optimized byte representation for RocksDB key.
    /// Format: [patch_blob_id_bytes(32 bytes)] \x00 [object_id_bytes(32 bytes)]
    /// \x00 is added here for future validation marker, e.g., if a quilt patch blob id is invalid,
    /// we set the \x00 to \x01 to avoid reading it.
    fn to_key(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Add BlobId bytes (32 bytes).
        bytes.extend_from_slice(self.patch_blob_id.as_ref());

        // Add \x00 separator.
        bytes.push(0x00);

        // Add ObjectID bytes (32 bytes)
        bytes.extend_from_slice(self.object_id.as_ref());

        bytes
    }

    /// Parse from byte representation.
    fn from_key(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 65 {
            return Err(anyhow::anyhow!(
                "invalid key length: expected 65 bytes (32 + 1 + 32)"
            ));
        }

        // Check for separator at position 32.
        if bytes[32] != 0x00 {
            return Err(anyhow::anyhow!("invalid quilt patch blob id"));
        }

        // Extract BlobId (first 32 bytes).
        let blob_id_bytes: [u8; 32] = bytes[0..32].try_into()?;
        let patch_blob_id = BlobId(blob_id_bytes);

        // Extract ObjectID (last 32 bytes).
        let object_id = ObjectID::from_bytes(&bytes[33..65])?;

        Ok(Self {
            patch_blob_id,
            object_id,
        })
    }
}

// For scanning all entries with a specific blob_manager and identifier
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

// For scanning all entries in a blob manager
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

// Parse the quilt patch index key.
fn parse_quilt_patch_index_key(key: &[u8]) -> Result<(BlobId, ObjectID), anyhow::Error> {
    let key_struct = QuiltPatchIndexKey::from_key(key)?;
    Ok((key_struct.patch_blob_id, key_struct.object_id))
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
    // Primary index: blob_manager/identifier -> IndexTarget.
    primary_index: DBMap<String, IndexTarget>,
    // Object index: object_id -> blob_manager/identifier.
    object_index: DBMap<String, ObjectIndexValue>,
    // Sequence store: stores the last processed sequence number for resumption.
    sequence_store: DBMap<String, u64>,
    // Index for quilt patches, so that we can look up quilt patches by the corresponding
    // files' blob IDs.
    quilt_patch_index: DBMap<String, QuiltPatchId>,
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

        Ok(Self {
            db,
            primary_index,
            object_index,
            sequence_store,
            quilt_patch_index,
        })
    }

    /// Apply an insert operation to both indices.
    fn insert_primary_index(
        &self,
        primary_key: PrimaryIndexKey,
        primary_value: BlobIdentity,
        txn: &Transaction<'_, OptimisticTransactionDB>,
    ) -> Result<(), TypedStoreError> {
        // Primary index: blob_manager/identifier/sequence_number -> IndexTarget.
        let object_key = ObjectIndexKey::new(primary_value.object_id);
        let object_value = ObjectIndexValue {
            blob_manager: primary_key.blob_manager,
            identifier: primary_key.identifier.clone(),
            sequence_number: primary_key.sequence_number,
        };

        let index_target = IndexTarget::Blob(primary_value);
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
}
