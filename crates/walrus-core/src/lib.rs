//! Core functionality for Walrus.
pub mod merkle;

use fastcrypto::bls12381::min_pk::BLS12381Signature;
use serde::{Deserialize, Serialize};

/// Erasure encoding and decoding.
pub mod encoding;

/// The epoch number.
pub type Epoch = u64;

/// The ID of a blob.
pub type BlobId = [u8; 32];

/// Represents the index of a shard.
pub type ShardIndex = u32;

/// Confirmation from a storage node that it has stored the sliver pairs for a given blob.
#[derive(Debug, Serialize, Deserialize)]
pub enum StorageConfirmation {
    /// Confirmation based on the storage node's signature.
    Signed(SignedConfirmation),
}

/// A signed [`Confirmation`] from a storage node.
#[derive(Debug, Serialize, Deserialize)]
pub struct SignedConfirmation {
    /// The blob and shards associated with this confirmation.
    pub confirmation: Confirmation,
    /// The signature over the BCS encoded confirmation.
    pub signature: BLS12381Signature,
}

/// A list of shards, confirmed as storing their sliver pairs for the given blob_id, as of the
/// specified epoch.
#[derive(Debug, Serialize, Deserialize)]
pub struct Confirmation {
    /// The ID of the Blob whose sliver pairs are confirmed as being stored.
    pub blob_id: BlobId,
    /// The epoch in which this confirmation is generated.
    pub epoch: Epoch,
    /// The shards that are confirmed to be storing their slivers.
    pub shards: Vec<ShardIndex>,
}
