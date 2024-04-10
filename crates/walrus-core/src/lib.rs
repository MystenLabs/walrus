// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Core functionality for Walrus.
use std::{
    fmt::{self, Debug, Display, LowerHex},
    num::{NonZeroU16, NonZeroUsize},
    str::FromStr,
};

use encoding::{
    EncodingAxis,
    EncodingConfig,
    Primary,
    PrimaryDecodingSymbol,
    PrimarySliver,
    RecoveryError,
    Secondary,
    SecondaryDecodingSymbol,
    SecondarySliver,
    WrongSliverVariantError,
};
use fastcrypto::{
    bls12381::min_pk::{BLS12381PublicKey, BLS12381Signature},
    encoding::{Encoding, Hex},
    hash::{Blake2b256, HashFunction},
};
use merkle::{MerkleAuth, MerkleTree, Node};
use metadata::BlobMetadata;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub mod encoding;
pub mod merkle;
pub mod metadata;
pub mod utils;

pub mod keys;
pub use keys::ProtocolKeyPair;

pub mod messages;
pub use messages::SignedStorageConfirmation;

/// A public key.
pub type PublicKey = BLS12381PublicKey;
/// A signature for a blob.
pub type Signature = BLS12381Signature;
/// A certificate for a blob, represented as a list of signer-signature pairs.
pub type Certificate = Vec<(PublicKey, Signature)>;
/// The hash function used for building metadata.
pub type DefaultHashFunction = Blake2b256;
/// The epoch number.
pub type Epoch = u64;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

// Blob ID.

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

// Sliver and shard indices.

macro_rules! index {
    (
        $(#[$outer:meta])*
        $name:ident($display_prefix:expr)
    ) => {
        wrapped_uint!(
            $(#[$outer])*
            #[derive(Default)]
            pub struct $name(pub u16) {
                /// Returns the index as a `usize`.
                pub fn as_usize(&self) -> usize {
                    self.0.into()
                }

                /// Returns the index as a `u32`.
                pub fn as_u32(&self) -> u32 {
                    self.0.into()
                }
            }
        );

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}-{}", $display_prefix, self.0)
            }
        }
    };
}

index!(
    /// Represents the index of a (primary or secondary) sliver.
    SliverIndex("sliver")
);

index!(
    /// Represents the index of a sliver pair.
    SliverPairIndex("sliver-pair")
);

impl From<SliverIndex> for SliverPairIndex {
    fn from(value: SliverIndex) -> Self {
        Self(value.0)
    }
}

impl From<SliverPairIndex> for SliverIndex {
    fn from(value: SliverPairIndex) -> Self {
        Self(value.0)
    }
}

macro_rules! inverse_for_secondary {
    ($self:expr, $encoding_axis:ty, $n_shards:expr) => {
        if <$encoding_axis>::IS_PRIMARY {
            $self.into()
        } else {
            ($n_shards.get() - $self.0 - 1).into()
        }
    };
}

impl SliverPairIndex {
    /// Computes the index of the [`Sliver`] of the corresponding axis starting from the index of
    /// the [`SliverPair`][encoding::SliverPair].
    ///
    /// This is needed because primary slivers are assigned in ascending `pair_index` order, while
    /// secondary slivers are assigned in descending `pair_index` order. I.e., the first primary
    /// sliver is contained in the first sliver pair, but the first secondary sliver is contained in
    /// the last sliver pair.
    pub fn to_sliver_index<E: EncodingAxis>(self, n_shards: NonZeroU16) -> SliverIndex {
        inverse_for_secondary!(self, E, n_shards)
    }
}

impl SliverIndex {
    /// Computes the index of the [`SliverPair`][encoding::SliverPair] of the corresponding axis
    /// starting from the index of the [`Sliver`].
    ///
    /// This is the inverse of [`SliverPairIndex::to_sliver_index`]; see that function for further
    /// information.
    pub fn to_pair_index<E: EncodingAxis>(self, n_shards: NonZeroU16) -> SliverPairIndex {
        inverse_for_secondary!(self, E, n_shards)
    }
}

index!(
    /// Represents the index of a shard.
    #[derive(PartialOrd, Ord)]
    ShardIndex("shard")
);

// Slivers.

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
    pub fn hash(&self, config: &EncodingConfig) -> Result<Node, RecoveryError> {
        match self {
            Sliver::Primary(inner) => inner.get_merkle_root::<DefaultHashFunction>(config),
            Sliver::Secondary(inner) => inner.get_merkle_root::<DefaultHashFunction>(config),
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

    /// Returns the [`Sliver<T>`][Sliver] contained within the enum.
    pub fn to_raw<T>(self) -> Result<encoding::Sliver<T>, WrongSliverVariantError>
    where
        Self: TryInto<encoding::Sliver<T>>,
        T: EncodingAxis,
    {
        self.try_into().map_err(|_| WrongSliverVariantError)
    }

    /// Returns true iff the sliver has the length expected based on the encoding configuration and
    /// blob size.
    pub fn has_correct_length(&self, config: &EncodingConfig, blob_size: usize) -> bool {
        Some(self.len()) == self.expected_length(config, blob_size)
    }

    fn expected_length(&self, config: &EncodingConfig, blob_size: usize) -> Option<usize> {
        match self {
            Self::Primary(_) => config.sliver_size_for_blob::<Primary>(blob_size),
            Self::Secondary(_) => config.sliver_size_for_blob::<Secondary>(blob_size),
        }
        .map(NonZeroUsize::get)
    }
}

impl TryFrom<Sliver> for PrimarySliver {
    type Error = WrongSliverVariantError;

    fn try_from(value: Sliver) -> Result<Self, Self::Error> {
        match value {
            Sliver::Primary(sliver) => Ok(sliver),
            Sliver::Secondary(_) => Err(WrongSliverVariantError),
        }
    }
}

impl TryFrom<Sliver> for SecondarySliver {
    type Error = WrongSliverVariantError;

    fn try_from(value: Sliver) -> Result<Self, Self::Error> {
        match value {
            Sliver::Primary(_) => Err(WrongSliverVariantError),
            Sliver::Secondary(sliver) => Ok(sliver),
        }
    }
}

/// A type indicating either a primary or secondary sliver.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DecodingSymbolType {
    /// Enum indicating a primary decoding symbol.
    Primary,
    /// Enum indicating a secondary decoding symbol.
    Secondary,
}

/// A type indicating either a primary or secondary sliver.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SliverType {
    /// Enum indicating a primary sliver.
    Primary,
    /// Enum indicating a secondary sliver.
    Secondary,
}

impl SliverType {
    /// Returns the opposite sliver type.
    pub fn orthogonal(&self) -> SliverType {
        match self {
            SliverType::Primary => SliverType::Secondary,
            SliverType::Secondary => SliverType::Primary,
        }
    }

    /// Creates the [`SliverType`] for the [`EncodingAxis`].
    pub fn for_encoding<T: EncodingAxis>() -> Self {
        if T::IS_PRIMARY {
            SliverType::Primary
        } else {
            SliverType::Secondary
        }
    }
}

impl Display for SliverType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                SliverType::Primary => "primary",
                SliverType::Secondary => "secondary",
            }
        )
    }
}

// Symbols.

/// A decoding symbol for recovering a sliver
///
/// Can be either a [`PrimaryDecodingSymbol`] or [`SecondaryDecodingSymbol`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DecodingSymbol<U: MerkleAuth> {
    /// A primary decoding symbol to recover a primary sliver
    Primary(PrimaryDecodingSymbol<U>),
    /// A secondary decoding symbol to recover a secondary sliver.
    Secondary(SecondaryDecodingSymbol<U>),
}

impl<U: MerkleAuth> DecodingSymbol<U> {
    /// Returns true iff this decoding symbol is a [`DecodingSymbol::Primary`].
    #[inline]
    pub fn is_primary(&self) -> bool {
        matches!(self, DecodingSymbol::Primary(_))
    }

    /// Returns true iff this decoding symbol is a [`DecodingSymbol::Secondary`].
    #[inline]
    pub fn is_secondary(&self) -> bool {
        matches!(self, DecodingSymbol::Secondary(_))
    }

    /// Returns the associated [`DecodingSymbolType`] of this decoding symbol.
    pub fn r#type(&self) -> DecodingSymbolType {
        match self {
            DecodingSymbol::Primary(_) => DecodingSymbolType::Primary,
            DecodingSymbol::Secondary(_) => DecodingSymbolType::Secondary,
        }
    }
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
