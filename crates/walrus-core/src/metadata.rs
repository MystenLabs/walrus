// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Metadata associated with a Blob and stored by storage nodes.

use alloc::vec::Vec;
use core::num::NonZeroU16;

use enum_dispatch::enum_dispatch;
use fastcrypto::hash::{Blake2b256, HashFunction};
use serde::{Deserialize, Serialize};

use crate::{
    encoding::{
        encoded_blob_length_for_n_shards,
        source_symbols_for_n_shards,
        DataTooLargeError,
        EncodingAxis,
        EncodingConfig,
        EncodingConfigTrait as _,
    },
    merkle::{MerkleTree, Node as MerkleNode, DIGEST_LEN},
    BlobId,
    EncodingType,
    SliverPairIndex,
    SliverType,
};

/// Errors returned by [`UnverifiedBlobMetadataWithId::verify`] when unable to verify the metadata.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum VerificationError {
    /// The number of sliver hashes present does not match the expected number.
    #[error("the metadata contained an invalid number of hashes (expected {expected}): {actual}")]
    InvalidHashCount {
        /// The number of hash elements in the metadata.
        actual: usize,
        /// The expected number of hash elements.
        expected: usize,
    },
    /// The blob ID does not match the value computed from the provided metadata.
    #[error("the blob ID does not match the provided metadata")]
    BlobIdMismatch,
    /// The unencoded blob length in the metadata cannot be encoded with the number of symbols
    /// available in the configuration provided.
    #[error("the unencoded blob length is too large for the given config")]
    UnencodedLengthTooLarge,
}

/// [`BlobMetadataWithId`] that has been verified with [`UnverifiedBlobMetadataWithId::verify`].
///
/// This ensures the following properties:
/// - The unencoded length is nonzero and not larger than the maximum blob size.
/// - The number of sliver hashes matches the number of slivers (twice the number of shards).
/// - The blob ID is correctly computed from the sliver hashes.
pub type VerifiedBlobMetadataWithId = BlobMetadataWithId<true>;

/// [`BlobMetadataWithId`] that has yet to be verified.
///
/// This is the default type of [`BlobMetadataWithId`].
pub type UnverifiedBlobMetadataWithId = BlobMetadataWithId<false>;

/// Metadata associated with a blob.
///
/// Stores the [`BlobId`] as well as additional details such as the encoding type,
/// unencoded length of the blob, and the hashes associated the slivers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobMetadataWithId<const V: bool = false> {
    blob_id: BlobId,
    metadata: BlobMetadata,
}

impl<const V: bool> BlobMetadataWithId<V> {
    /// Creates a new unverified metadata with the corresponding blob ID.
    pub fn new(blob_id: BlobId, metadata: BlobMetadata) -> UnverifiedBlobMetadataWithId {
        BlobMetadataWithId { blob_id, metadata }
    }

    /// Creates a new verified metadata starting from the components of the metadata.
    ///
    /// The verification is implicit as the blob ID is created directly from the metadata.
    pub fn new_verified_from_metadata(
        sliver_pair_meta: Vec<SliverPairMetadata>,
        encoding: EncodingType,
        unencoded_length: u64,
    ) -> VerifiedBlobMetadataWithId {
        let blob_metadata = BlobMetadata::new(encoding, unencoded_length, sliver_pair_meta);
        Self::new_verified_unchecked(
            BlobId::from_sliver_pair_metadata(&blob_metadata),
            blob_metadata,
        )
    }

    /// Creates a new verified metadata with the corresponding blob ID, without running the
    /// verification.
    pub fn new_verified_unchecked(
        blob_id: BlobId,
        metadata: BlobMetadata,
    ) -> VerifiedBlobMetadataWithId {
        BlobMetadataWithId { blob_id, metadata }
    }

    /// The ID of the blob associated with the metadata.
    pub fn blob_id(&self) -> &BlobId {
        &self.blob_id
    }

    /// The associated [`BlobMetadata`].
    pub fn metadata(&self) -> &BlobMetadata {
        &self.metadata
    }
}

impl VerifiedBlobMetadataWithId {
    /// Converts the verified metadata into an unverified one.
    pub fn into_unverified(self) -> UnverifiedBlobMetadataWithId {
        BlobMetadataWithId {
            blob_id: self.blob_id,
            metadata: self.metadata,
        }
    }

    /// Checks if the number of symbols and number of shards in the config matches the the metadata.
    ///
    /// Returns true if the number of symbols and number of shards in the provided encoding config,
    /// matches that which was used to verify the metadata.
    pub fn is_encoding_config_applicable(&self, config: &EncodingConfig) -> bool {
        let encoding_type = self.metadata.encoding_type();
        let (n_primary, n_secondary) = source_symbols_for_n_shards(self.n_shards(), encoding_type);
        let config = config.get_for_type(encoding_type);

        self.n_shards() == config.n_shards()
            && n_primary == config.n_primary_source_symbols()
            && n_secondary == config.n_secondary_source_symbols()
    }

    /// Returns the number of shards in the committee for which this metadata was constructed.
    ///
    /// As this metadata has been verified, this is guaranteed to correspond to the number
    /// of shards in the encoding config with which this was verified.
    pub fn n_shards(&self) -> NonZeroU16 {
        let n_hashes = self.metadata.hashes().len();
        NonZeroU16::new(n_hashes as u16).expect("verified metadata has a valid number of shards")
    }
}

impl UnverifiedBlobMetadataWithId {
    /// Attempts to verify the relationship between the contained metadata and blob ID.
    ///
    /// Consumes the metadata. On success, returns a [`VerifiedBlobMetadataWithId`].
    pub fn verify(
        self,
        config: &EncodingConfig,
    ) -> Result<VerifiedBlobMetadataWithId, VerificationError> {
        let n_hashes = self.metadata().hashes().len();
        let n_shards = config.n_shards.get().into();
        crate::ensure!(
            n_hashes == n_shards,
            VerificationError::InvalidHashCount {
                actual: n_hashes,
                expected: n_shards,
            }
        );
        crate::ensure!(
            self.metadata.unencoded_length()
                <= config
                    .get_for_type(self.metadata.encoding_type())
                    .max_blob_size(),
            VerificationError::UnencodedLengthTooLarge
        );
        let computed_blob_id = BlobId::from_sliver_pair_metadata(&self.metadata);
        crate::ensure!(
            computed_blob_id == *self.blob_id(),
            VerificationError::BlobIdMismatch
        );
        Ok(BlobMetadataWithId {
            blob_id: self.blob_id,
            metadata: self.metadata,
        })
    }
}

impl<const V: bool> AsRef<BlobMetadata> for BlobMetadataWithId<V> {
    fn as_ref(&self) -> &BlobMetadata {
        &self.metadata
    }
}

/// Trait for the API of [`BlobMetadata`].
#[enum_dispatch]
pub trait BlobMetadataApi {
    /// Return the hash of the sliver pair at the given index and type.
    fn get_sliver_hash(
        &self,
        sliver_pair_index: SliverPairIndex,
        sliver_type: SliverType,
    ) -> Option<&MerkleNode>;

    /// Returns the root hash of the Merkle tree over the sliver pairs.
    fn compute_root_hash(&self) -> MerkleNode;

    /// Returns the symbol size associated with the blob.
    fn symbol_size(
        &self,
        encoding_config: &EncodingConfig,
    ) -> Result<NonZeroU16, DataTooLargeError>;

    /// Returns the encoded size of the blob.
    fn encoded_size(&self) -> Option<u64>;

    /// Returns the encoding type of the blob.
    fn encoding_type(&self) -> EncodingType;

    /// Returns the unencoded length of the blob.
    fn unencoded_length(&self) -> u64;

    /// Returns the hashes of the sliver pairs of the blob.
    fn hashes(&self) -> &Vec<SliverPairMetadata>;
}

/// Metadata about a blob.
#[enum_dispatch(BlobMetadataApi)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlobMetadata {
    /// Version 1 of the blob metadata.
    V1(BlobMetadataV1),
}

impl BlobMetadata {
    /// Creates a new [`BlobMetadata`] with the given encoding type, unencoded length, and sliver
    /// hashes.
    pub fn new(
        encoding_type: EncodingType,
        unencoded_length: u64,
        hashes: Vec<SliverPairMetadata>,
    ) -> BlobMetadata {
        BlobMetadata::V1(BlobMetadataV1 {
            encoding_type,
            unencoded_length,
            hashes,
        })
    }

    /// Returns the encoding type of the blob.
    pub fn encoding_type(&self) -> EncodingType {
        match self {
            BlobMetadata::V1(inner) => inner.encoding_type,
        }
    }

    /// Returns a mutable reference to the inner [`BlobMetadataV1`].
    ///
    /// This is only available in tests.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn mut_inner(&mut self) -> &mut BlobMetadataV1 {
        match self {
            BlobMetadata::V1(inner) => inner,
        }
    }
}

/// Metadata about a blob, without its corresponding [`BlobId`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobMetadataV1 {
    /// The type of encoding used to erasure encode the blob.
    pub encoding_type: EncodingType,
    /// The length of the unencoded blob.
    pub unencoded_length: u64,
    /// The hashes over the slivers of the blob.
    pub hashes: Vec<SliverPairMetadata>,
}

impl BlobMetadataApi for BlobMetadataV1 {
    /// Return the hash of the sliver pair at the given index and type.
    fn get_sliver_hash(
        &self,
        sliver_pair_index: SliverPairIndex,
        sliver_type: SliverType,
    ) -> Option<&MerkleNode> {
        self.hashes.get(sliver_pair_index.as_usize()).map(
            |sliver_pair_metadata| match sliver_type {
                SliverType::Primary => &sliver_pair_metadata.primary_hash,
                SliverType::Secondary => &sliver_pair_metadata.secondary_hash,
            },
        )
    }

    /// Returns the root hash of the Merkle tree over the sliver pairs.
    fn compute_root_hash(&self) -> MerkleNode {
        MerkleTree::<Blake2b256>::build(
            self.hashes
                .iter()
                .map(|h| h.pair_leaf_input::<Blake2b256>()),
        )
        .root()
    }

    /// Returns the symbol size associated with the blob.
    fn symbol_size(
        &self,
        encoding_config: &EncodingConfig,
    ) -> Result<NonZeroU16, DataTooLargeError> {
        encoding_config
            .get_for_type(self.encoding_type)
            .symbol_size_for_blob(self.unencoded_length)
    }

    /// Returns the encoded size of the blob.
    ///
    /// This infers the number of shards from the length of the `hashes` vector.
    ///
    /// Returns `None` if `hashes.len()` is not between `1` and `u16::MAX` or if the
    /// `unencoded_length` cannot be encoded.
    fn encoded_size(&self) -> Option<u64> {
        encoded_blob_length_for_n_shards(
            NonZeroU16::new(self.hashes.len().try_into().ok()?)?,
            self.unencoded_length,
            self.encoding_type,
        )
    }

    fn encoding_type(&self) -> EncodingType {
        self.encoding_type
    }

    fn unencoded_length(&self) -> u64 {
        self.unencoded_length
    }

    fn hashes(&self) -> &Vec<SliverPairMetadata> {
        &self.hashes
    }
}

/// Metadata about a sliver pair, i.e., the root hashes of the primary and secondary slivers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SliverPairMetadata {
    /// The hash of the primary sliver in the sliver pair.
    pub primary_hash: MerkleNode,
    /// The hash of the secondary sliver in the sliver pair.
    pub secondary_hash: MerkleNode,
}

impl SliverPairMetadata {
    /// Creates a new [`SliverPairMetadata`] with empty hashes ([`MerkleNode::Empty`]).
    pub fn new_empty() -> SliverPairMetadata {
        SliverPairMetadata {
            primary_hash: MerkleNode::Empty,
            secondary_hash: MerkleNode::Empty,
        }
    }

    /// Concatenates the Merkle roots over the primary and secondary slivers.
    ///
    /// This is then to be used as input to compute the Merkle tree over the sliver pairs.
    pub fn pair_leaf_input<T: HashFunction<DIGEST_LEN>>(&self) -> [u8; 2 * DIGEST_LEN] {
        let mut concat = [0u8; 2 * DIGEST_LEN];
        concat[0..DIGEST_LEN].copy_from_slice(&self.primary_hash.bytes());
        concat[DIGEST_LEN..2 * DIGEST_LEN].copy_from_slice(&self.secondary_hash.bytes());
        concat
    }

    /// Returns a reference to the hash for the sliver of the given [`EncodingAxis`].
    pub fn hash<T: EncodingAxis>(&self) -> &MerkleNode {
        if T::IS_PRIMARY {
            &self.primary_hash
        } else {
            &self.secondary_hash
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BLOB_ID: BlobId = BlobId([7; 32]);

    mod verify {
        use super::*;
        use crate::test_utils;

        #[test]
        fn fails_for_incorrect_blob_id() {
            let valid_metadata = test_utils::unverified_blob_metadata();
            assert_ne!(*valid_metadata.blob_id(), BLOB_ID);

            let invalid_metadata = UnverifiedBlobMetadataWithId {
                blob_id: BLOB_ID,
                ..valid_metadata
            };

            let err = invalid_metadata
                .verify(&test_utils::encoding_config())
                .expect_err("verification should fail");

            assert_eq!(err, VerificationError::BlobIdMismatch);
        }

        #[test]
        fn succeeds_for_correct_metadata() {
            let metadata = test_utils::unverified_blob_metadata();
            let _ = metadata
                .verify(&test_utils::encoding_config())
                .expect("verification should succeed");
        }

        #[test]
        fn verified_metadata_has_the_same_data_as_unverified() {
            let unverified = test_utils::unverified_blob_metadata();
            let verified = unverified
                .clone()
                .verify(&test_utils::encoding_config())
                .expect("verification should succeed");

            assert_eq!(verified.blob_id(), unverified.blob_id());
            assert_eq!(verified.metadata(), unverified.metadata());
        }

        #[test]
        fn fails_for_hash_count_mismatch() {
            let mut metadata = test_utils::unverified_blob_metadata();
            let expected = metadata.metadata().hashes().len();
            metadata
                .metadata
                .mut_inner()
                .hashes
                .push(SliverPairMetadata {
                    primary_hash: MerkleNode::Digest([42u8; 32]),
                    secondary_hash: MerkleNode::Digest([23u8; 32]),
                });
            let actual = metadata.metadata().hashes().len();

            let err = metadata
                .verify(&test_utils::encoding_config())
                .expect_err("verification should fail");

            assert_eq!(
                err,
                VerificationError::InvalidHashCount { actual, expected }
            );
        }

        #[test]
        fn fails_for_unencoded_length_too_large() {
            let config = test_utils::encoding_config();
            let mut metadata = test_utils::unverified_blob_metadata();
            let encoding_type = metadata.metadata().encoding_type();
            metadata.metadata.mut_inner().unencoded_length = u64::from(u16::MAX)
                * u64::from(
                    config
                        .get_for_type(encoding_type)
                        .n_primary_source_symbols()
                        .get(),
                )
                * u64::from(
                    config
                        .get_for_type(encoding_type)
                        .n_secondary_source_symbols()
                        .get(),
                )
                + 1;

            let err = metadata
                .verify(&config)
                .expect_err("verification should fail");

            assert_eq!(err, VerificationError::UnencodedLengthTooLarge);
        }
    }
}
