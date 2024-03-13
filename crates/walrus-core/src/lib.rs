// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Core functionality for Walrus.
use encoding::{PrimarySliver, SecondarySliver};
use fastcrypto::hash::{Blake2b256, HashFunction};
use merkle::{MerkleTree, Node};
use metadata::SliverPairMetadata;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub mod encoding;
pub mod merkle;
pub mod messages;
pub mod metadata;

pub use messages::SignedStorageConfirmation;

/// The epoch number.
pub type Epoch = u64;

/// The ID of a blob.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(transparent)]
pub struct BlobId(pub [u8; Self::LENGTH]);

impl BlobId {
    /// The length of a blob ID in bytes.
    pub const LENGTH: usize = 32;

    /// Returns the blob ID as a hash over the Merkle root, encoding type,
    /// and unencoded_length of the blob.
    pub fn from_metadata(merkle_root: Node, encoding: EncodingType, unencoded_length: u64) -> Self {
        Self::new_with_hash_function::<Blake2b256>(merkle_root, encoding, unencoded_length)
    }

    /// Computes the Merkle root over the [`SliverPairMetadata`],
    /// and then computes the blob ID.
    pub fn from_sliver_pair_metadata(
        sliver_pair_meta: &[SliverPairMetadata],
        encoding: EncodingType,
        unencoded_length: u64,
    ) -> Self {
        let merkle_root = MerkleTree::<Blake2b256>::build(
            sliver_pair_meta
                .iter()
                .map(|h| h.pair_leaf_input::<Blake2b256>()),
        )
        .root();
        Self::from_metadata(merkle_root, encoding, unencoded_length)
    }

    fn new_with_hash_function<T>(
        merkle_root: Node,
        encoding: EncodingType,
        unencoded_length: u64,
    ) -> BlobId
    where
        T: HashFunction<{ Self::LENGTH }>,
    {
        let mut hasher = T::default();

        // This is equivalent to the bcs encoding of the encoding type,
        // unencoded length, and merkle root.
        hasher.update([encoding.into()]);
        hasher.update(unencoded_length.to_le_bytes());
        hasher.update(merkle_root.bytes());

        Self(hasher.finalize().into())
    }
}

impl AsRef<[u8]> for BlobId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Represents the index of a shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct ShardIndex(pub u16);

/// A sliver of an erasure-encoded blob.
///
/// Can be either a [`PrimarySliver`] or [`SecondarySliver`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Sliver {
    /// A primary sliver.
    Primary(PrimarySliver),
    /// A secondary sliver.
    Secondary(SecondarySliver),
}

impl Sliver {
    /// Returns true iff this sliver is a [`Sliver::Primary`].
    #[inline]
    pub fn is_primary(&self) -> bool {
        matches!(self, Sliver::Primary(_))
    }

    /// Returns true iff this sliver is a [`Sliver::Secondary`].
    #[inline]
    pub fn is_secondary(&self) -> bool {
        matches!(self, Sliver::Secondary(_))
    }

    /// Returns the associated [`SliverType`] of this sliver.
    pub fn r#type(&self) -> SliverType {
        match self {
            Sliver::Primary(_) => SliverType::Primary,
            Sliver::Secondary(_) => SliverType::Secondary,
        }
    }
}

/// A type indicating either a primary or secondary sliver.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SliverType {
    /// Enum indicating a primary sliver.
    Primary,
    /// Enum indicating a secondary sliver.
    Secondary,
}

/// Error returned for an invalid conversion to an encoding type.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("the provided value is not a valid EncodingType")]
pub struct InvalidEncodingType;

/// Supported Walrus encoding types.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Default, Serialize, Deserialize)]
#[repr(u8)]
pub enum EncodingType {
    /// Default RaptorQ encoding.
    #[default]
    RedStuff = 0,
}

impl From<EncodingType> for u8 {
    fn from(value: EncodingType) -> Self {
        value as u8
    }
}

impl TryFrom<u8> for EncodingType {
    type Error = InvalidEncodingType;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(EncodingType::RedStuff),
            _ => Err(InvalidEncodingType),
        }
    }
}

/// Returns an error if the condition evaluates to false.
#[macro_export]
macro_rules! ensure {
    ($cond:expr, $err:expr $(,)?) => {
        if !$cond {
            return Err($err);
        }
    };
}
