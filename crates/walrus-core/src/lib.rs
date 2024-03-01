//! Core functionality for Walrus.
pub mod merkle;

pub mod messages;

/// Erasure encoding and decoding.
pub mod encoding;

/// The epoch number.
pub type Epoch = u64;
/// The ID of a blob.
pub type BlobId = [u8; 32];
/// Represents the index of a shard.
pub type ShardIndex = u32;
