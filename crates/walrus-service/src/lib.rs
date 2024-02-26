//! Service functionality for Walrus.

/// Configuration module.
pub mod config;
/// Cryptographic utilities.
pub mod crypto;
/// Encoding utilities.
pub mod encoding;
/// Client for interacting with Move.
pub mod move_client;

/// The ID of a blob.
pub type BlobId = [u8; 32];
/// The epoch number.
pub type Epoch = u64;
