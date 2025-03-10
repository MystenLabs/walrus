//! This is the walrus-rust-sdk crate.
//!
//! It provides a Rust API for interacting with the Walrus blob store.
//!
mod blob_id;
mod error;
mod metadata;

use crate::{blob_id::BlobId, error::Error, metadata::BlobMetadata};

type Result<T> = std::result::Result<T, Error>;

/// A client for interacting with the Walrus network.
#[derive(Debug)]
pub struct WalrusClient {
    // TODO: include config, context, etc...
}

impl WalrusClient {
    /// Store a blob in the Walrus network.
    pub async fn store_blob(&self, _blob: &[u8]) -> Result<BlobId> {
        todo!()
    }

    /// Read a blob from the Walrus network.
    pub async fn read_blob(&self, _blob_id: BlobId) -> Result<Vec<u8>> {
        todo!()
    }

    /// Get metadata for a blob.
    pub async fn get_metadata(&self, _blob_id: BlobId) -> Result<BlobMetadata> {
        todo!()
    }
}
