// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Core functionality for Walrus.
use std::{
    fmt::{self, Debug, Display, LowerHex},
    str::FromStr,
};

use encoding::{PrimarySliver, RecoveryError, SecondarySliver};
use fastcrypto::{
    bls12381::min_pk::{BLS12381KeyPair, BLS12381PublicKey, BLS12381Signature},
    encoding::{Encoding, Hex},
    hash::{Blake2b256, HashFunction},
};
use merkle::{MerkleTree, Node};
use metadata::BlobMetadata;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub mod encoding;

pub mod merkle;

pub mod messages;
pub use messages::SignedStorageConfirmation;

pub mod metadata;
pub mod utils;

/// A public key.
pub type PublicKey = BLS12381PublicKey;
/// A key pair.
pub type KeyPair = BLS12381KeyPair;
/// A signature for a blob.
pub type Signature = BLS12381Signature;
/// A certificate for a blob, represented as a list of signer-signature pairs.
pub type Certificate = Vec<(PublicKey, Signature)>;
/// The hash function used for building metadata.
pub type DefaultHashFunction = Blake2b256;

/// Utility functions for tests.
///
/// These are available with the "test-utils" feature.
#[cfg(feature = "test-utils")]
pub mod test_utils;

/// The epoch number.
pub type Epoch = u64;

/// The ID of a blob.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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

    /// Computes the Merkle root over the [`SliverPairMetadata`][metadata::SliverPairMetadata],
    /// contained in the `blob_metadata` and then computes the blob ID.
    pub fn from_sliver_pair_metadata(blob_metadata: &BlobMetadata) -> Self {
        let merkle_root = MerkleTree::<Blake2b256>::build(
            blob_metadata
                .hashes
                .iter()
                .map(|h| h.pair_leaf_input::<Blake2b256>()),
        )
        .root();
        Self::from_metadata(
            merkle_root,
            blob_metadata.encoding_type,
            blob_metadata.unencoded_length,
        )
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

impl LowerHex for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", Hex::encode(self.0))
    }
}

impl AsRef<[u8]> for BlobId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Display for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:#x}")
    }
}

impl Debug for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BlobId({self:#x})")
    }
}

/// Error returned when unable to parse a blob ID.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("failed to parse a blob ID")]
pub struct BlobIdParseError;

impl<'a> TryFrom<&'a [u8]> for BlobId {
    type Error = BlobIdParseError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let bytes = <[u8; Self::LENGTH]>::try_from(value).map_err(|_| BlobIdParseError)?;
        Ok(Self(bytes))
    }
}

impl FromStr for BlobId {
    type Err = BlobIdParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        fastcrypto::encoding::decode_bytes_hex(s).map_err(|_| BlobIdParseError)
    }
}

/// Represents the index of a shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct ShardIndex(pub u16);

impl Display for ShardIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "shard-{}", self.0)
    }
}

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

    /// Returns the hash of the sliver, i.e., the Merkle root of the tree computed over the symbols.
    pub fn hash(&self) -> Result<Node, RecoveryError> {
        match self {
            Sliver::Primary(inner) => inner.get_merkle_root::<DefaultHashFunction>(),
            Sliver::Secondary(inner) => inner.get_merkle_root::<DefaultHashFunction>(),
        }
    }

    /// Returns the sliver size in bytes.
    pub fn len(&self) -> usize {
        match self {
            Sliver::Primary(inner) => inner.len(),
            Sliver::Secondary(inner) => inner.len(),
        }
    }

    /// Returns true iff the sliver length is 0.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
