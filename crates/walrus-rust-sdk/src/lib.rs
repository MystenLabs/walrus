//! This is the walrus-rust-sdk crate.
//!
//! It provides a Rust API for interacting with the Walrus blob store.
//!
mod error;
mod blob_id;

use crate::error::Error;
use crate::blob_id::BlobId;

use base64::{display::Base64Display, engine::general_purpose::URL_SAFE_NO_PAD, Engine};

type Result<T> = std::result::Result<T, Error>;
pub struct WalrusClient {
    // TODO: include config, context, etc...
}

impl WalrusClient {
    pub fn store_blob(&self, blob: &[u8]) -> Result<BlobId> {
        todo!()
    }

    pub fn read_blob(&self, blob_id: BlobId) -> Result<BlobId> {
        todo!()
    }
}
