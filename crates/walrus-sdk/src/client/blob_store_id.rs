// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
//! A module for defining blob store identifiers.

/// Identifier for a blob store file or a slice within a blob store file.
#[derive(Debug, Copy, Clone, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct BlobStoreId {
    /// Index of the blob store file in this overall update job.
    pub file_index: usize,
    /// Optional index of the slice within the file that's being stored.
    pub slice_index: Option<usize>,
}

impl BlobStoreId {
    /// Create a new BlobStoreId for a whole file.
    pub fn new_file(file_index: usize) -> Self {
        Self {
            file_index,
            slice_index: None,
        }
    }
    /// Create a new BlobStoreId for a slice of a file.
    pub fn new_file_slice(file_index: usize, slice_index: usize) -> Self {
        Self {
            file_index,
            slice_index: Some(slice_index),
        }
    }
}

impl std::fmt::Display for BlobStoreId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.slice_index {
            None => {
                write!(f, "blob_{:06}", self.file_index)
            }
            Some(slice_index) => {
                write!(f, "blob_{:06}_{slice_index:06}", self.file_index)
            }
        }
    }
}
