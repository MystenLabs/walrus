// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This is the walrus-rust-sdk crate.
//!
//! It provides a Rust API for interacting with the Walrus blob store.
//!
mod blob_id;
mod error;
mod metadata;

use std::path::Path;

pub use crate::{
    blob_id::{BlobId, BlobIdParseError},
    error::Error,
    metadata::BlobMetadata,
};

/// A convenience type for results from the Walrus SDK.
pub type Result<T> = std::result::Result<T, Error>;

/// A client for interacting with the Walrus network.
#[derive(Debug)]
pub struct WalrusClient {
    // TODO: include config, context, etc...
}

impl WalrusClient {
    /// Create a new Walrus client, given a configuration file.
    pub fn from_config(_config: impl AsRef<Path>) -> Result<Self> {
        todo!()
    }

    // TODO: consider more constructors that allow reads and don't require a config file.

    /// Store a blob in the Walrus network.
    pub async fn store_blob(&self, _blob: &[u8]) -> Result<BlobId> {
        todo!()
    }

    /// Read a blob from the Walrus network.
    pub async fn read_blob(&self, _blob_id: BlobId) -> Result<Vec<u8>> {
        todo!()
    }

    /// Get metadata for a blob.
    pub async fn get_blob_metadata(&self, _blob_id: BlobId) -> Result<BlobMetadata> {
        todo!()
    }
}
