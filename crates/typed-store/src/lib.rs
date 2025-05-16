// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! A type-safe wrapper around RocksDB that provides a key-value store interface.
//! This crate provides functionality for storing and retrieving typed data in RocksDB,
//! with support for column families, batch operations, and metrics.

#![warn(
    future_incompatible,
    nonstandard_style,
    rust_2018_idioms,
    rust_2021_compatibility,
    unused,
    missing_docs
)]

/// Re-export rocksdb so that consumers can use the version of rocksdb via typed-store
pub use rocksdb;

/// The rocksdb database
pub mod rocks;

/// The traits for the typed store
pub mod traits;

/// The error type for the typed store
pub use rocks::errors::TypedStoreError;
/// The traits for the typed store
pub use traits::Map;

/// The metrics for the typed store
pub mod metrics;
pub use metrics::DBMetrics;
