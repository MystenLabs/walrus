//! Service functionality for Walrus.

/// Configuration module.
pub mod config;
/// Cryptographic utilities.
pub mod crypto;
/// Encoding utilities.
pub mod encoding;
/// Mapping from sliver pairs to shards.
pub mod mapping;
/// Client for interacting with Move.
pub mod move_client;

mod node;
pub use node::StorageNode;

mod storage;
