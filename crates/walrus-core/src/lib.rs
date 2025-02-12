// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Core functionality for Walrus.

#![no_std]
#![deny(clippy::std_instead_of_alloc, clippy::std_instead_of_core)]

extern crate alloc;
extern crate std;

use alloc::vec::Vec;
#[allow(unused)]
#[cfg(feature = "utoipa")]
use alloc::{format, string::String};
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
use serde_with::{DeserializeAs, DisplayFromStr, SerializeAs};
#[cfg(feature = "sui-types")]
use sui_types::base_types::ObjectID;
use thiserror::Error;

use crate::metadata::BlobMetadataApi as _;
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
pub type Epoch = u32;
/// The number of epochs.
pub type EpochCount = u32;

/// Walrus epoch.
// Schema definition for the type alias used in OpenAPI schemas.
#[derive(Debug)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema), schema(as = Epoch))]
pub struct EpochSchema(pub u32);

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

// Blob ID.

/// The ID of a blob.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
#[repr(transparent)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema), schema(
    as = BlobId,
    value_type = String,
    format = Byte,
    examples("E7_nNXvFU_3qZVu3OH1yycRG7LZlyn1-UxEDCDDqGGU"),
))]
pub struct BlobId(pub [u8; Self::LENGTH]);

/// Alias for BlobId to represent quilt task IDs
pub type QuiltTaskId = BlobId;

impl BlobId {
    /// The length of a blob ID in bytes.
    pub const LENGTH: usize = 32;

    /// A blob ID with all zeros.
    pub const ZERO: Self = Self([0u8; Self::LENGTH]);

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
            blob_metadata.encoding_type(),
            blob_metadata.unencoded_length(),
        );
        tracing::debug!(%blob_id, "computed blob ID from metadata");
        blob_id
    }

    /// Extracts the first two bytes of the blob ID as a `u16`, with the left most bit being the
    /// most significant.
    ///
    /// The extracted can be used to monitor the progress of tasks that scans over blob IDs.
    pub fn first_two_bytes(&self) -> u16 {
        u16::from_be_bytes(
            self.0[0..2]
                .try_into()
                .expect("two bytes can be converted to a u16"),
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

impl TryFrom<&[u8]> for BlobId {
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

// Sui Object ID.

/// The ID of a Sui object.
///
/// Reimplemented here to not take a mandatory dependency on the sui sdk in the core crate.
/// With the feature `sui-types` enabled, this type can be converted to and from
/// the `ObjectID` type from the sui sdk.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash, Debug)]
#[repr(transparent)]
pub struct SuiObjectId(pub [u8; Self::LENGTH]);

impl SuiObjectId {
    /// The length of a Sui object ID in bytes.
    pub const LENGTH: usize = 32;
}

#[cfg(feature = "sui-types")]
impl From<ObjectID> for SuiObjectId {
    fn from(value: ObjectID) -> Self {
        Self(value.into_bytes())
    }
}

#[cfg(feature = "sui-types")]
impl From<&ObjectID> for SuiObjectId {
    fn from(value: &ObjectID) -> Self {
        (*value).into()
    }
}

#[cfg(feature = "sui-types")]
impl From<SuiObjectId> for ObjectID {
    fn from(value: SuiObjectId) -> Self {
        ObjectID::from_bytes(value.0).expect("valid Sui object ID")
    }
}

#[cfg(feature = "sui-types")]
impl From<&SuiObjectId> for ObjectID {
    fn from(value: &SuiObjectId) -> Self {
        (*value).into()
    }
}

/// Error returned when unable to parse a Sui object ID.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("failed to parse a Sui object ID")]
pub struct SuiObjectIdParseError;

impl TryFrom<&[u8]> for SuiObjectId {
    type Error = SuiObjectIdParseError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let bytes = <[u8; Self::LENGTH]>::try_from(value).map_err(|_| SuiObjectIdParseError)?;
        Ok(Self(bytes))
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

                /// Returns the index as a `u64`.
                pub fn as_u64(&self) -> u64 {
                    self.0.into()
                }
            }
        );

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }
    };
}

index_type!(
    /// Represents the index of a (primary or secondary) sliver.
    #[derive(Ord, PartialOrd)]
    #[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
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

impl PartialOrd<NonZeroU16> for SliverIndex {
    fn partial_cmp(&self, other: &NonZeroU16) -> Option<core::cmp::Ordering> {
        self.0.partial_cmp(&other.get())
    }
}

impl PartialEq<NonZeroU16> for SliverIndex {
    fn eq(&self, other: &NonZeroU16) -> bool {
        self.0.eq(&other.get())
    }
}

impl FromStr for SliverIndex {
    type Err = <u16 as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(SliverIndex(s.parse()?))
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

/// Identifier of a decoding symbol within the set of decoding symbols of a blob.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SymbolId {
    primary: SliverIndex,
    secondary: SliverIndex,
}

impl SymbolId {
    /// Create a new id from a primary [`SliverIndex`], secondary [`SliverIndex`].
    pub fn new(primary: SliverIndex, secondary: SliverIndex) -> Self {
        Self { primary, secondary }
    }

    /// The index of the primary sliver containing the symbol.
    pub fn primary_sliver_index(&self) -> SliverIndex {
        self.primary
    }

    /// The index of the secondary sliver containing the symbol.
    pub fn secondary_sliver_index(&self) -> SliverIndex {
        self.secondary
    }

    /// Returns the corresponding primary or secondary index, as identified by the sliver type.
    ///
    /// That is, returns [`Self::primary_sliver_index()`] when `sliver_type == SliverType::Primary`,
    /// and otherwise [`Self::secondary_sliver_index()`].
    pub fn sliver_index(&self, sliver_type: SliverType) -> SliverIndex {
        match sliver_type {
            SliverType::Primary => self.primary,
            SliverType::Secondary => self.secondary,
        }
    }
}

impl Display for SymbolId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.primary, self.secondary)
    }
}

#[cfg(feature = "utoipa")]
impl utoipa::PartialSchema for SymbolId {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        use alloc::borrow::ToOwned;

        use utoipa::openapi::{ObjectBuilder, RefOr, Schema, Type};

        let object = ObjectBuilder::new()
            .schema_type(Type::String)
            .description(Some(
                "An ID of primary and secondary sliver indices that identifies a recovery symbol"
                    .to_owned(),
            ))
            .examples(["0-0", "999-32"])
            .pattern(Some(r"[0-9]+-[0-9]+"))
            .build();
        RefOr::T(Schema::Object(object))
    }
}

#[cfg(feature = "utoipa")]
impl utoipa::ToSchema for SymbolId {
    fn name() -> alloc::borrow::Cow<'static, str> {
        "SymbolId".into()
    }
}

/// Error returned when failing to parse a [`SymbolId`].
///
/// The string must be a pair of u16's separated by a hyphen, e.g., 73-241.
#[derive(Debug, Clone, thiserror::Error)]
#[error("failed to parse a symbol ID from the string")]
pub struct ParseSymbolIdError;

impl FromStr for SymbolId {
    type Err = ParseSymbolIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (primary_str, secondary_str) = s.split_once('-').ok_or(ParseSymbolIdError)?;
        Ok(Self {
            primary: SliverIndex(primary_str.parse().or(Err(ParseSymbolIdError))?),
            secondary: SliverIndex(secondary_str.parse().or(Err(ParseSymbolIdError))?),
        })
    }
}

impl Serialize for SymbolId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            <DisplayFromStr as SerializeAs<SymbolId>>::serialize_as(self, serializer)
        } else {
            (self.primary, self.secondary).serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for SymbolId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            <DisplayFromStr as DeserializeAs<SymbolId>>::deserialize_as(deserializer)
        } else {
            let (primary, secondary) = <(SliverIndex, SliverIndex)>::deserialize(deserializer)?;
            Ok(Self { primary, secondary })
        }
    }
}

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

/// Error returned when trying to extract the wrong variant (primary or secondary) of
/// [`RecoverySymbol`] from it.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
#[error("cannot convert the `RecoverySymbol` to the variant requested")]
pub struct WrongRecoverySymbolVariantError;

impl<U: MerkleAuth> TryFrom<RecoverySymbol<U>> for PrimaryRecoverySymbol<U> {
    type Error = WrongRecoverySymbolVariantError;

    fn try_from(value: RecoverySymbol<U>) -> Result<Self, Self::Error> {
        match value {
            RecoverySymbol::Primary(primary) => Ok(primary),
            RecoverySymbol::Secondary(_) => Err(WrongRecoverySymbolVariantError),
        }
    }
}

impl<U: MerkleAuth> TryFrom<RecoverySymbol<U>> for SecondaryRecoverySymbol<U> {
    type Error = WrongRecoverySymbolVariantError;

    fn try_from(value: RecoverySymbol<U>) -> Result<Self, Self::Error> {
        match value {
            RecoverySymbol::Primary(_) => Err(WrongRecoverySymbolVariantError),
            RecoverySymbol::Secondary(secondary) => Ok(secondary),
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
#[derive(Debug, Clone)]
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

impl<T: MerkleAuth> From<PrimaryInconsistencyProof<T>> for InconsistencyProof<T> {
    fn from(value: PrimaryInconsistencyProof<T>) -> Self {
        Self::Primary(value)
    }
}

impl<T: MerkleAuth> From<SecondaryInconsistencyProof<T>> for InconsistencyProof<T> {
    fn from(value: SecondaryInconsistencyProof<T>) -> Self {
        Self::Secondary(value)
    }
}

/// Error returned when trying to extract the wrong variant (primary or secondary) of
/// [`InconsistencyProof`] from it.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
#[error("cannot convert the `InconsistencyProof` to the variant requested")]
pub struct WrongProofVariantError;

impl<T: MerkleAuth> TryFrom<InconsistencyProof<T>> for PrimaryInconsistencyProof<T> {
    type Error = WrongProofVariantError;

    fn try_from(value: InconsistencyProof<T>) -> Result<Self, Self::Error> {
        if let InconsistencyProof::Primary(primary) = value {
            Ok(primary)
        } else {
            Err(WrongProofVariantError)
        }
    }
}

impl<T: MerkleAuth> TryFrom<InconsistencyProof<T>> for SecondaryInconsistencyProof<T> {
    type Error = WrongProofVariantError;

    fn try_from(value: InconsistencyProof<T>) -> Result<Self, Self::Error> {
        if let InconsistencyProof::Secondary(secondary) = value {
            Ok(secondary)
        } else {
            Err(WrongProofVariantError)
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

#[cfg(test)]
mod tests {
    use serde_test::{Configure as _, Token};

    use super::*;

    #[test]
    fn symbol_id_serde_compact() {
        let symbol_id = SymbolId::new(17.into(), 21.into());
        serde_test::assert_tokens(
            &symbol_id.compact(),
            &[
                Token::Tuple { len: 2 },
                Token::U16(17),
                Token::U16(21),
                Token::TupleEnd,
            ],
        );
    }

    #[test]
    fn symbol_id_serde_human_readable() {
        let symbol_id = SymbolId::new(17.into(), 21.into());
        serde_test::assert_tokens(&symbol_id.readable(), &[Token::String("17-21")]);
    }
}
