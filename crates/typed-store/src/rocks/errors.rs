// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{fmt, fmt::Display};

use bincode::ErrorKind as BincodeErrorKind;
use rocksdb::Error as RocksError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Hash, Debug, Error)]
pub(crate) struct RocksErrorDef {
    message: String,
}

impl Display for RocksErrorDef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> std::result::Result<(), fmt::Error> {
        self.message.fmt(formatter)
    }
}

#[derive(Serialize, Deserialize, Clone, Hash, Eq, PartialEq, Debug, Error)]
pub(crate) enum BincodeErrorDef {
    Io(String),
    InvalidUtf8Encoding(String),
    InvalidBoolEncoding(u8),
    InvalidCharEncoding,
    InvalidTagEncoding(usize),
    DeserializeAnyNotSupported,
    SizeLimit,
    SequenceMustHaveLength,
    Custom(String),
}

impl fmt::Display for BincodeErrorDef {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            BincodeErrorDef::Io(ref ioerr) => write!(fmt, "io error: {ioerr}"),
            BincodeErrorDef::InvalidUtf8Encoding(ref e) => {
                write!(fmt, "{e}")
            }
            BincodeErrorDef::InvalidBoolEncoding(b) => {
                write!(fmt, "expected 0 or 1, found {b}")
            }
            BincodeErrorDef::InvalidCharEncoding => write!(fmt, "{self:?}"),
            BincodeErrorDef::InvalidTagEncoding(tag) => {
                write!(fmt, "found {tag}")
            }
            BincodeErrorDef::SequenceMustHaveLength => write!(fmt, "{self:?}"),
            BincodeErrorDef::SizeLimit => write!(fmt, "{self:?}"),
            BincodeErrorDef::DeserializeAnyNotSupported => write!(
                fmt,
                "Bincode does not support the serde::Deserializer::deserialize_any method"
            ),
            BincodeErrorDef::Custom(ref s) => s.fmt(fmt),
        }
    }
}

impl From<bincode::Error> for BincodeErrorDef {
    fn from(err: bincode::Error) -> Self {
        match err.as_ref() {
            BincodeErrorKind::Io(ioerr) => BincodeErrorDef::Io(ioerr.to_string()),
            BincodeErrorKind::InvalidUtf8Encoding(utf8err) => {
                BincodeErrorDef::InvalidUtf8Encoding(utf8err.to_string())
            }
            BincodeErrorKind::InvalidBoolEncoding(byte) => {
                BincodeErrorDef::InvalidBoolEncoding(*byte)
            }
            BincodeErrorKind::InvalidCharEncoding => BincodeErrorDef::InvalidCharEncoding,
            BincodeErrorKind::InvalidTagEncoding(tag) => BincodeErrorDef::InvalidTagEncoding(*tag),
            BincodeErrorKind::DeserializeAnyNotSupported => {
                BincodeErrorDef::DeserializeAnyNotSupported
            }
            BincodeErrorKind::SizeLimit => BincodeErrorDef::SizeLimit,
            BincodeErrorKind::SequenceMustHaveLength => BincodeErrorDef::SequenceMustHaveLength,
            BincodeErrorKind::Custom(str) => BincodeErrorDef::Custom(str.to_owned()),
        }
    }
}

/// Convert the bincode error to the typed store error
pub fn typed_store_err_from_bincode_err(err: bincode::Error) -> TypedStoreError {
    TypedStoreError::SerializationError(format!("{err}"))
}

/// Convert the bcs error to the typed store error
pub fn typed_store_err_from_bcs_err(err: bcs::Error) -> TypedStoreError {
    TypedStoreError::SerializationError(format!("{err}"))
}

/// Convert the rocksdb error to the typed store error
pub fn typed_store_err_from_rocks_err(err: RocksError) -> TypedStoreError {
    TypedStoreError::RocksDBError(format!("{err}"))
}

impl From<tokio::task::JoinError> for TypedStoreError {
    fn from(err: tokio::task::JoinError) -> Self {
        if err.is_panic() {
            // For panics, we should resume the unwind rather than converting to an error
            tracing::error!("panic in task: {:?}", err);
            std::panic::resume_unwind(err.into_panic());
        } else {
            TypedStoreError::TaskError(err.to_string())
        }
    }
}

#[non_exhaustive]
#[derive(Error, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Ord, PartialOrd)]
/// The error type for the typed store
pub enum TypedStoreError {
    /// The rocksdb error
    #[error("rocksdb error: {0}")]
    RocksDBError(String),
    /// The serialization error
    #[error("(de)serialization error: {0}")]
    SerializationError(String),
    /// The column family was not registered with the database
    #[error("the column family {0} was not registered with the database")]
    UnregisteredColumn(String),
    /// A batch operation can't operate across databases
    #[error("a batch operation can't operate across databases")]
    CrossDBBatch,
    /// The metric reporting thread failed with error
    #[error("Metric reporting thread failed with error")]
    MetricsReporting,
    /// The transaction should be retried
    #[error("Transaction should be retried")]
    RetryableTransactionError,
    /// The iterator is not initialized
    #[error("Iterator is not initialized")]
    IteratorNotInitialized,
    /// A task failure if the task returns a TypedStoreError. This is used to propagate background
    /// tokio tasks errors back to the caller.
    #[error("Task error: {0}")]
    TaskError(String),
    /// The database is being deleted
    #[error("The shard storage is being deleted")]
    ShardBeingDeleted,
}

/// The result type for the typed store
pub type Result<T> = std::result::Result<T, TypedStoreError>;
