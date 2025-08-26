// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Octopus Index implementation for Walrus blob management.
//!
//! This module provides the data structures and operations for managing
//! primary and secondary indices for blobs stored in Walrus buckets.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use sui_types::base_types::ObjectID;
use walrus_core::{BlobId, QuiltPatchId};

/// Represents the target of an index entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TargetBlob {
    /// A regular blob ID.
    BlobId(BlobId),
    /// A quilt patch ID.
    QuiltPatchId(QuiltPatchId),
}

/// Data structure representing the value stored in the primary index.
/// This contains the target blob/patch and all associated secondary index values.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalrusIndexData {
    /// The target that this index entry points to (either BlobId or QuiltPatchId).
    pub target: TargetBlob,

    /// Secondary index values for this blob.
    /// Key: index name (e.g., "type", "date", "location", "tags").
    /// Value: the index value(s) as a SecondaryIndexValue.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub secondary_indices: HashMap<String, SecondaryIndexValue>,
}

/// Represents different types of secondary index values.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum SecondaryIndexValue {
    /// Single string value (e.g., type: "jpg", date: "2024-01-15").
    Single(String),

    /// Multiple string values (e.g., tags: ["sunset", "nature"]).
    Multiple(Vec<String>),
}

impl WalrusIndexData {
    /// Creates a new WalrusIndexData with just a target and no secondary indices.
    #[allow(dead_code)]
    pub fn new(target: TargetBlob) -> Self {
        Self {
            target,
            secondary_indices: HashMap::new(),
        }
    }

    /// Creates a new WalrusIndexData with target and secondary indices.
    #[allow(dead_code)]
    pub fn with_indices(
        target: TargetBlob,
        secondary_indices: HashMap<String, SecondaryIndexValue>,
    ) -> Self {
        Self {
            target,
            secondary_indices,
        }
    }

    /// Adds a single-valued secondary index.
    #[allow(dead_code)]
    pub fn add_single_index(&mut self, name: String, value: String) {
        self.secondary_indices
            .insert(name, SecondaryIndexValue::Single(value));
    }

    /// Adds a multi-valued secondary index.
    #[allow(dead_code)]
    pub fn add_multiple_index(&mut self, name: String, values: Vec<String>) {
        self.secondary_indices
            .insert(name, SecondaryIndexValue::Multiple(values));
    }
}

impl SecondaryIndexValue {
    /// Returns all values as a vector, whether single or multiple.
    pub fn as_vec(&self) -> Vec<&str> {
        match self {
            SecondaryIndexValue::Single(s) => vec![s.as_str()],
            SecondaryIndexValue::Multiple(v) => v.iter().map(|s| s.as_str()).collect(),
        }
    }

    /// Returns true if this is a single value.
    pub fn is_single(&self) -> bool {
        matches!(self, SecondaryIndexValue::Single(_))
    }

    /// Returns true if this contains multiple values.
    pub fn is_multiple(&self) -> bool {
        matches!(self, SecondaryIndexValue::Multiple(_))
    }
}

/// Represents a mutation operation on the index.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum IndexMutation {
    /// Insert or update a primary index entry with its target and secondary indices.
    Insert {
        /// Primary key (without bucket_id prefix).
        primary_key: String,
        /// The target to store (either BlobId or QuiltPatchId).
        target: TargetBlob,
        /// Secondary indices for this entry.
        secondary_indices: HashMap<String, SecondaryIndexValue>,
    },
    /// Delete a primary index entry and all its secondary indices.
    Delete {
        /// Primary key (without bucket_id prefix).
        primary_key: String,
    },
}

/// A set of mutations to apply to a specific bucket.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MutationSet {
    /// The bucket ID these mutations apply to.
    pub bucket_id: ObjectID,
    /// The list of mutations to apply.
    pub mutations: Vec<IndexMutation>,
}

impl MutationSet {
    /// Creates a new mutation set for a bucket.
    pub fn new(bucket_id: ObjectID) -> Self {
        Self {
            bucket_id,
            mutations: Vec::new(),
        }
    }

    /// Adds an insert mutation to the set.
    pub fn add_insert(
        &mut self,
        primary_key: String,
        target: TargetBlob,
        secondary_indices: HashMap<String, SecondaryIndexValue>,
    ) {
        self.mutations.push(IndexMutation::Insert {
            primary_key,
            target,
            secondary_indices,
        });
    }

    /// Adds a delete mutation to the set.
    pub fn add_delete(&mut self, primary_key: String) {
        self.mutations.push(IndexMutation::Delete { primary_key });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_target_blob() -> TargetBlob {
        // Create a test TargetBlob with a BlobId variant.
        TargetBlob::BlobId(BlobId::from([0u8; 32]))
    }

    fn test_target_quilt_patch() -> TargetBlob {
        // Create a test TargetBlob with a QuiltPatchId variant.
        TargetBlob::QuiltPatchId(QuiltPatchId::from([1u8; 32]))
    }

    #[test]
    fn test_walrus_index_data_creation() {
        let target = test_target_blob();
        let data = WalrusIndexData::new(target.clone());
        assert_eq!(data.target, target);
        assert!(data.secondary_indices.is_empty());

        // Test with QuiltPatchId variant.
        let target_quilt = test_target_quilt_patch();
        let data_quilt = WalrusIndexData::new(target_quilt.clone());
        assert_eq!(data_quilt.target, target_quilt);
        assert!(data_quilt.secondary_indices.is_empty());
    }

    #[test]
    fn test_add_indices() {
        let target = test_target_blob();
        let mut data = WalrusIndexData::new(target);

        // Add single-valued indices.
        data.add_single_index("type".to_string(), "jpg".to_string());
        data.add_single_index("date".to_string(), "2024-01-15".to_string());
        data.add_single_index("location".to_string(), "california".to_string());

        // Add multi-valued index
        data.add_multiple_index(
            "tags".to_string(),
            vec!["sunset".to_string(), "nature".to_string()],
        );

        assert_eq!(data.secondary_indices.len(), 4);
        assert_eq!(
            data.secondary_indices.get("type"),
            Some(&SecondaryIndexValue::Single("jpg".to_string()))
        );
        assert_eq!(
            data.secondary_indices.get("tags"),
            Some(&SecondaryIndexValue::Multiple(vec![
                "sunset".to_string(),
                "nature".to_string()
            ]))
        );
    }

    #[test]
    fn test_secondary_index_value_as_vec() {
        let single = SecondaryIndexValue::Single("jpg".to_string());
        assert_eq!(single.as_vec(), vec!["jpg"]);

        let multiple =
            SecondaryIndexValue::Multiple(vec!["sunset".to_string(), "nature".to_string()]);
        assert_eq!(multiple.as_vec(), vec!["sunset", "nature"]);
    }
}
