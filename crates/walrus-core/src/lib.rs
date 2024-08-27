// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Core functionality for Walrus.

#![no_std]
#![deny(clippy::std_instead_of_alloc, clippy::std_instead_of_core)]

extern crate alloc;
extern crate std;

use alloc::vec::Vec;
use core::{
    fmt::{self, Debug, Display},
    num::NonZeroU16,
    ops::{Bound, Range, RangeBounds},
    str::FromStr,
};

use base64::{display::Base64Display, engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use encoding::{
    EncodingAxis,
    EncodingConfig,
    PrimaryRecoverySymbol,
    PrimarySliver,
    RecoverySymbolError,
    SecondaryRecoverySymbol,
    SecondarySliver,
    SliverVerificationError,
    WrongSliverVariantError,
};
use fastcrypto::{
    bls12381::min_pk::{BLS12381PublicKey, BLS12381Signature},
    hash::{Blake2b256, HashFunction},
    secp256r1::Secp256r1PublicKey,
};
use inconsistency::{
    InconsistencyVerificationError,
    PrimaryInconsistencyProof,
    SecondaryInconsistencyProof,
};
use merkle::{MerkleAuth, MerkleProof, Node};
use metadata::BlobMetadata;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub mod bft;
pub mod encoding;
pub mod inconsistency;
pub mod keys;
pub mod merkle;
pub mod messages;
pub mod metadata;
pub mod utils;

/// A public key for protocol messages.
pub type PublicKey = BLS12381PublicKey;
/// A public key for network communication.
pub type NetworkPublicKey = Secp256r1PublicKey;
/// A signature for a blob.
pub type Signature = BLS12381Signature;
/// A certificate for a blob, represented as a list of signer-signature pairs.
pub type Certificate = Vec<(PublicKey, Signature)>;
/// The hash function used for building metadata.
pub type DefaultHashFunction = Blake2b256;
/// The epoch number.
pub type Epoch = u64;

/// Walrus epoch.
// Schema definition for the type alias used in OpenAPI schemas.
#[derive(Debug)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "utoipa", schema(as = Epoch))]
#[allow(dead_code)]
pub struct EpochSchema(u64);

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

// Blob ID.

/// The ID of a blob.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
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
        let merkle_root = blob_metadata.compute_root_hash();
        let blob_id = Self::from_metadata(
            merkle_root,
            blob_metadata.encoding_type,
            blob_metadata.unencoded_length,
        );
        tracing::debug!(%blob_id, "computed blob ID from metadata");
        blob_id
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

impl Display for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Base64Display::new(self.as_ref(), &URL_SAFE_NO_PAD).fmt(f)
    }
}

impl Debug for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BlobId({self})")
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
        let mut blob_id = Self([0; Self::LENGTH]);
        if let Ok(Self::LENGTH) = URL_SAFE_NO_PAD.decode_slice(s, &mut blob_id.0) {
            Ok(blob_id)
        } else {
            Err(BlobIdParseError)
        }
    }
}

// Sliver and shard indices.

/// This macro is used to create separate types for specific indices.
///
/// While those could all be represented by the same type (`u16`), having separate types helps
/// finding bugs; for example, when a sliver index is not properly converted to a sliver-pair index.
///
/// The macro adds additional implementations on top of the [`wrapped_uint`] macro.
macro_rules! index_type {
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

index_type!(
    /// Represents the index of a (primary or secondary) sliver.
    SliverIndex("sliver")
);

index_type!(
    /// Represents the index of a sliver pair.
    ///
    /// As blobs are encoded into as many pairs of slivers as there are shards in the committee,
    /// this value ranges be from 0 to the number of shards (exclusive).
    #[derive(Ord, PartialOrd)]
    #[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
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

impl SliverPairIndex {
    /// Computes the index of the [`Sliver`] of the corresponding axis starting from the index of
    /// the [`SliverPair`][encoding::SliverPair].
    ///
    /// This is needed because primary slivers are assigned in ascending `pair_index` order, while
    /// secondary slivers are assigned in descending `pair_index` order. I.e., the first primary
    /// sliver is contained in the first sliver pair, but the first secondary sliver is contained in
    /// the last sliver pair.
    ///
    /// # Panics
    ///
    /// Panics if the index is greater than or equal to `n_shards`.
    pub fn to_sliver_index<E: EncodingAxis>(self, n_shards: NonZeroU16) -> SliverIndex {
        if E::IS_PRIMARY {
            self.into()
        } else {
            (n_shards.get() - self.0 - 1).into()
        }
    }
}

impl SliverIndex {
    /// Computes the index of the [`SliverPair`][encoding::SliverPair] of the corresponding axis
    /// starting from the index of the [`Sliver`].
    ///
    /// This is the inverse of [`SliverPairIndex::to_sliver_index`]; see that function for further
    /// information.
    ///
    /// # Panics
    ///
    /// Panics if the index is greater than or equal to `n_shards`.
    pub fn to_pair_index<E: EncodingAxis>(self, n_shards: NonZeroU16) -> SliverPairIndex {
        if E::IS_PRIMARY {
            self.into()
        } else {
            (n_shards.get() - self.0 - 1).into()
        }
    }
}

index_type!(
    /// Represents the index of a shard.
    #[derive(PartialOrd, Ord)]
    ShardIndex("shard")
);

/// A range of shards.
///
/// Created with the [`ShardIndex::range()`] method.
pub type ShardRange = core::iter::Map<Range<u16>, fn(u16) -> ShardIndex>;

impl ShardIndex {
    /// A range of shard indices.
    ///
    /// # Examples
    ///
    /// ```
    /// # use walrus_core::ShardIndex;
    /// #
    /// assert!(ShardIndex::range(0..3).eq([ShardIndex(0), ShardIndex(1), ShardIndex(2)]));
    /// assert!(ShardIndex::range(0..3).eq(ShardIndex::range(..3)));
    /// assert!(ShardIndex::range(0..3).eq(ShardIndex::range(..=2)));
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if a range with an unbounded end is specified (i.e., `range(3..)`)
    pub fn range(range: impl RangeBounds<u16>) -> ShardRange {
        let start = match range.start_bound() {
            Bound::Included(left) => *left,
            Bound::Excluded(left) => *left + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(right) => *right + 1,
            Bound::Excluded(right) => *right,
            Bound::Unbounded => {
                unimplemented!("cannot create a ShardIndex range with an unbounded end")
            }
        };
        (start..end).map(ShardIndex)
    }
}

impl From<ShardIndex> for usize {
    fn from(value: ShardIndex) -> Self {
        value.get().into()
    }
}

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
    pub fn hash(&self, config: &EncodingConfig) -> Result<Node, RecoverySymbolError> {
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
        match self {
            Sliver::Primary(inner) => inner.is_empty(),
            Sliver::Secondary(inner) => inner.is_empty(),
        }
    }

    /// Checks that the provided sliver is authenticated by the metadata.
    ///
    /// The checks include verifying that the sliver has the correct length and symbol size, and
    /// that the hash in the metadata matches the Merkle root over the sliver's symbols.
    pub fn verify(
        &self,
        encoding_config: &EncodingConfig,
        metadata: &BlobMetadata,
    ) -> Result<(), SliverVerificationError> {
        match self {
            Sliver::Primary(inner) => inner.verify(encoding_config, metadata),
            Sliver::Secondary(inner) => inner.verify(encoding_config, metadata),
        }
    }

    /// Returns the [`Sliver<T>`][Sliver] contained within the enum.
    pub fn to_raw<T>(self) -> Result<encoding::SliverData<T>, WrongSliverVariantError>
    where
        Self: TryInto<encoding::SliverData<T>>,
        T: EncodingAxis,
    {
        self.try_into().map_err(|_| WrongSliverVariantError)
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

impl From<PrimarySliver> for Sliver {
    fn from(value: PrimarySliver) -> Self {
        Self::Primary(value)
    }
}

impl From<SecondarySliver> for Sliver {
    fn from(value: SecondarySliver) -> Self {
        Self::Secondary(value)
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
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
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

    /// Provides a string representation of the enum variant.
    pub fn as_str(&self) -> &'static str {
        match self {
            SliverType::Primary => "primary",
            SliverType::Secondary => "secondary",
        }
    }
}

impl AsRef<str> for SliverType {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Display for SliverType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

// Symbols.

/// A decoding symbol for recovering a sliver
///
/// Can be either a [`PrimaryRecoverySymbol`] or [`SecondaryRecoverySymbol`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(deserialize = "for<'a> U: Deserialize<'a>"))]
pub enum RecoverySymbol<U: MerkleAuth> {
    /// A primary decoding symbol to recover a primary sliver
    Primary(PrimaryRecoverySymbol<U>),
    /// A secondary decoding symbol to recover a secondary sliver.
    Secondary(SecondaryRecoverySymbol<U>),
}

impl<U: MerkleAuth> RecoverySymbol<U> {
    /// Returns true iff this decoding symbol is a [`RecoverySymbol::Primary`].
    #[inline]
    pub fn is_primary(&self) -> bool {
        matches!(self, RecoverySymbol::Primary(_))
    }

    /// Returns true iff this decoding symbol is a [`RecoverySymbol::Secondary`].
    #[inline]
    pub fn is_secondary(&self) -> bool {
        matches!(self, RecoverySymbol::Secondary(_))
    }

    /// Returns the associated [`DecodingSymbolType`] of this decoding symbol.
    pub fn r#type(&self) -> DecodingSymbolType {
        match self {
            RecoverySymbol::Primary(_) => DecodingSymbolType::Primary,
            RecoverySymbol::Secondary(_) => DecodingSymbolType::Secondary,
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
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
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

// Inconsistency Proofs

/// An inconsistency proof for a blob.
///
/// Can be either a [`PrimaryInconsistencyProof`] or a [`SecondaryInconsistencyProof`],
/// proving that either a [`PrimarySliver`] or a [`SecondarySliver`] cannot be recovered
/// from their respective recovery symbols.
#[derive(Debug)]
pub enum InconsistencyProof<T: MerkleAuth = MerkleProof> {
    /// Inconsistency proof for an encoding on the primary axis.
    Primary(PrimaryInconsistencyProof<T>),
    /// Inconsistency proof for an encoding on the secondary axis.
    Secondary(SecondaryInconsistencyProof<T>),
}

impl<T: MerkleAuth> InconsistencyProof<T> {
    /// Verifies the inconsistency proof.
    ///
    /// Returns `Ok(())` if the proof is correct, otherwise returns an
    /// [`InconsistencyVerificationError`].
    pub fn verify(
        self,
        metadata: &BlobMetadata,
        encoding_config: &EncodingConfig,
    ) -> Result<(), InconsistencyVerificationError> {
        match self {
            InconsistencyProof::Primary(proof) => proof.verify(metadata, encoding_config),
            InconsistencyProof::Secondary(proof) => proof.verify(metadata, encoding_config),
        }
    }
}

/// Returns an error if the condition evaluates to false.
///
/// Instead of an error, a message can be provided as a single string literal or as a format string
/// with additional parameters. In those cases, the message is turned into an error using
/// anyhow and then converted to the expected type.
///
/// # Examples
///
/// ```
/// # use thiserror::Error;
/// # use walrus_core::ensure;
/// #
/// # #[derive(Debug, Error, PartialEq)]
/// #[error("some error has occurred")]
/// struct MyError;
///
/// let function = |condition: bool| -> Result::<usize, MyError> {
///     ensure!(condition, MyError);
///     Ok(42)
/// };
/// assert_eq!(function(true).unwrap(), 42);
/// assert_eq!(function(false).unwrap_err(), MyError);
/// ```
///
/// ```
/// # use anyhow;
/// # use walrus_core::ensure;
/// let function = |condition: bool| -> anyhow::Result::<()> {
///     ensure!(condition, "some error message");
///     Ok(())
/// };
/// assert!(function(true).is_ok());
/// assert_eq!(function(false).unwrap_err().to_string(), "some error message");
/// ```
#[macro_export]
macro_rules! ensure {
    ($cond:expr, $msg:literal $(,)?) => {
        if !$cond {
            return Err(anyhow::anyhow!($msg).into());
        }
    };
    ($cond:expr, $err:expr $(,)?) => {
        if !$cond {
            return Err($err);
        }
    };
    ($cond:expr, $fmt:expr, $($arg:tt)*) => {
        if !$cond {
            return Err(anyhow::anyhow!($fmt, $($arg)*).into());
        }
    };
}
