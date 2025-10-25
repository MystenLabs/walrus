// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Error types for the Walrus Indexer.

use thiserror::Error;

/// Errors that can occur during async task storage operations.
#[derive(Debug, Error)]
pub enum AsyncTaskStoreError {
    /// RocksDB error occurred.
    #[error("rocksdb error: {0}")]
    RocksDBError(String),

    /// Serialization error occurred.
    #[error("serialization error: {0}")]
    SerializationError(String),

    /// Deserialization error occurred.
    #[error("deserialization error: {0}")]
    DeserializationError(String),

    /// Task not found.
    #[error("task not found: {0}")]
    TaskNotFound(String),

    /// Invalid task state.
    #[error("invalid task state: {0}")]
    InvalidTaskState(String),
}

impl From<typed_store::TypedStoreError> for AsyncTaskStoreError {
    fn from(err: typed_store::TypedStoreError) -> Self {
        match err {
            typed_store::TypedStoreError::SerializationError(s) => {
                AsyncTaskStoreError::SerializationError(s)
            }
            typed_store::TypedStoreError::RocksDBError(s) => AsyncTaskStoreError::RocksDBError(s),
            other => AsyncTaskStoreError::RocksDBError(other.to_string()),
        }
    }
}
