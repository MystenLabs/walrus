// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Metadata associated with a Blob and stored by storage nodes.
use std::num::NonZeroUsize;

use fastcrypto::hash::HashFunction;
use serde::{Deserialize, Serialize};

use crate::{
    merkle::{Node as MerkleNode, DIGEST_LEN},
    BlobId,
    EncodingType,
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
}

/// [`BlobMetadataWithId`] that has been verified with [`UnverifiedBlobMetadataWithId::verify`].
pub type VerifiedBlobMetadataWithId = BlobMetadataWithId<true>;

/// [`BlobMetadataWithId`] that has yet to be verified, this is the default
/// type of [`BlobMetadataWithId`].
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
        Self::new_verified_unchecked(
            BlobId::from_sliver_pair_metadata(&sliver_pair_meta, encoding, unencoded_length),
            BlobMetadata {
                encoding_type: encoding,
                unencoded_length,
                hashes: sliver_pair_meta,
            },
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

impl UnverifiedBlobMetadataWithId {
    /// Consumes the metadata and attempts to verify it the relationship between the contained
    /// metadata and blob ID. On success, returns a [`VerifiedBlobMetadataWithId`].
    pub fn verify(
        self,
        n_shards: NonZeroUsize,
    ) -> Result<VerifiedBlobMetadataWithId, VerificationError> {
        let n_hashes = self.metadata().hashes.len();
        crate::ensure!(
            n_hashes == n_shards.get(),
            VerificationError::InvalidHashCount {
                actual: n_hashes,
                expected: n_shards.get(),
            }
        );

        let computed_blob_id = BlobId::from_sliver_pair_metadata(
            &self.metadata.hashes,
            EncodingType::RedStuff,
            self.metadata().unencoded_length,
        );
        crate::ensure!(
            computed_blob_id == *self.blob_id(),
            VerificationError::BlobIdMismatch
        );

        Ok(BlobMetadataWithId {
            blob_id: self.blob_id,
            metadata: self.metadata,
        })
    }

    /// Returns an arbitrary metadata object.
    // todo(@asonnino): Move this function to `walrus-test-utils`, #109
    pub fn arbitrary_metadata_for_test() -> UnverifiedBlobMetadataWithId {
        let unencoded_length = 7_000_000_000;
        let hashes: Vec<_> = (0..100u8)
            .map(|i| SliverPairMetadata {
                primary_hash: MerkleNode::Digest([i; 32]),
                secondary_hash: MerkleNode::Digest([i; 32]),
            })
            .collect();
        let blob_id =
            BlobId::from_sliver_pair_metadata(&hashes, EncodingType::RedStuff, unencoded_length);

        UnverifiedBlobMetadataWithId::new(
            blob_id,
            BlobMetadata {
                encoding_type: EncodingType::RedStuff,
                unencoded_length,
                hashes,
            },
        )
    }
}

impl<const V: bool> AsRef<BlobMetadata> for BlobMetadataWithId<V> {
    fn as_ref(&self) -> &BlobMetadata {
        self.metadata()
    }
}

/// Metadata about a blob, without its corresponding [`BlobId`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobMetadata {
    /// The type of encoding used to erasure encode the blob.
    pub encoding_type: EncodingType,
    /// The length of the unencoded blob.
    pub unencoded_length: u64,
    /// The hashes over the slivers of the blob.
    pub hashes: Vec<SliverPairMetadata>,
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
}

#[cfg(test)]
mod tests {

    use super::*;

    const BLOB_ID: BlobId = BlobId([7; 32]);

    mod verify {
        use super::*;

        #[test]
        fn fails_for_incorrect_blob_id() {
            let valid_metadata = UnverifiedBlobMetadataWithId::arbitrary_metadata_for_test();
            let n_shards = valid_metadata.metadata().hashes.len();
            assert_ne!(*valid_metadata.blob_id(), BLOB_ID);

            let invalid_metadata = UnverifiedBlobMetadataWithId {
                blob_id: BLOB_ID,
                ..valid_metadata
            };

            let err = invalid_metadata
                .verify(NonZeroUsize::new(n_shards).unwrap())
                .expect_err("verification should fail");

            assert_eq!(err, VerificationError::BlobIdMismatch);
        }

        #[test]
        fn succeeds_for_correct_metadata() {
            let metadata = UnverifiedBlobMetadataWithId::arbitrary_metadata_for_test();
            let actual = metadata.metadata().hashes.len();

            let _ = metadata
                .verify(NonZeroUsize::new(actual).unwrap())
                .expect("verification should succeed");
        }

        #[test]
        fn verified_metadata_has_the_same_data_as_unverified() {
            let unverified = UnverifiedBlobMetadataWithId::arbitrary_metadata_for_test();
            let actual = unverified.metadata().hashes.len();

            let verified = unverified
                .clone()
                .verify(NonZeroUsize::new(actual).unwrap())
                .expect("verification should succeed");

            assert_eq!(verified.blob_id(), unverified.blob_id());
            assert_eq!(verified.metadata(), unverified.metadata());
        }

        #[test]
        fn fails_for_hash_count_mismatch() {
            let metadata = UnverifiedBlobMetadataWithId::arbitrary_metadata_for_test();
            let actual = metadata.metadata().hashes.len();
            let expected = actual + 1;

            let err = metadata
                .verify(NonZeroUsize::new(expected).unwrap())
                .expect_err("verification should fail");

            assert_eq!(
                err,
                VerificationError::InvalidHashCount { actual, expected }
            );
        }
    }
}
