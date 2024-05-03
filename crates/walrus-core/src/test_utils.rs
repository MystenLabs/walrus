// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Utility functions for tests.

use std::num::{NonZeroU16, NonZeroU64};

use fastcrypto::traits::{KeyPair, Signer};
use rand::{rngs::StdRng, RngCore, SeedableRng};

use crate::{
    encoding::{self, EncodingConfig, PrimarySliver},
    merkle::{MerkleProof, Node},
    metadata::{
        BlobMetadata,
        SliverPairMetadata,
        UnverifiedBlobMetadataWithId,
        VerifiedBlobMetadataWithId,
    },
    BlobId,
    EncodingType,
    ProtocolKeyPair,
    RecoverySymbol,
    SignedStorageConfirmation,
    Sliver,
    SliverIndex,
    SliverPairIndex,
};

/// Returns a deterministic fixed key pair for testing.
///
/// Various testing facilities can use this key and unit-test can re-generate it to verify the
/// correctness of inputs and outputs.
pub fn keypair() -> ProtocolKeyPair {
    let mut rng = StdRng::seed_from_u64(0);
    ProtocolKeyPair::new(KeyPair::generate(&mut rng))
}

/// Returns an arbitrary sliver for testing.
pub fn sliver() -> Sliver {
    Sliver::Primary(primary_sliver())
}

/// Returns an arbitrary primary sliver with 7 symbols (compatible with 10 shards) for testing.
pub fn primary_sliver() -> PrimarySliver {
    encoding::Sliver::new(
        [
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28,
        ],
        4.try_into().unwrap(),
        SliverIndex(1),
    )
}

/// Returns a BFT-compatible encoding configuration with 10 shards.
pub fn encoding_config() -> EncodingConfig {
    EncodingConfig::new(NonZeroU16::new(10).unwrap())
}

/// Returns an arbitrary decoding symbol for testing.
pub fn recovery_symbol() -> RecoverySymbol<MerkleProof> {
    primary_sliver()
        .recovery_symbol_for_sliver(SliverPairIndex(1), &encoding_config())
        .map(RecoverySymbol::Secondary)
        .unwrap()
}

/// Returns an empty Merkle proof for testing.
pub fn merkle_proof() -> MerkleProof {
    MerkleProof::new(&[])
}

/// Returns an arbitrary storage confirmation for tests.
pub fn signed_storage_confirmation() -> SignedStorageConfirmation {
    let mut rng = StdRng::seed_from_u64(0);
    let mut confirmation = vec![0; 32];
    rng.fill_bytes(&mut confirmation);

    let signer = keypair();
    let signature = signer.as_ref().sign(&confirmation);
    SignedStorageConfirmation {
        confirmation,
        signature,
    }
}

/// Returns a random blob ID for testing.
pub fn random_blob_id() -> BlobId {
    let mut rng = StdRng::seed_from_u64(0);
    let mut bytes = [0; BlobId::LENGTH];
    rng.fill_bytes(&mut bytes);
    BlobId(bytes)
}

/// Returns a blob ID of given number for testing.
pub const fn blob_id_from_u64(num: u64) -> BlobId {
    let mut blob_id = [0u8; 32];
    let u64_bytes = num.to_be_bytes();

    let mut i = 0usize;
    while i < 8 {
        blob_id[24 + i] = u64_bytes[i];
        i += 1;
    }
    BlobId(blob_id)
}

/// Returns an arbitrary metadata object.
pub fn blob_metadata() -> BlobMetadata {
    let config = encoding_config();
    let unencoded_length = NonZeroU64::new(62_831).unwrap();
    let hashes: Vec<_> = (0..config.n_shards.into())
        .map(|i| SliverPairMetadata {
            primary_hash: Node::Digest([(i % 256) as u8; 32]),
            secondary_hash: Node::Digest([(i % 256) as u8; 32]),
        })
        .collect();
    BlobMetadata {
        encoding_type: EncodingType::RedStuff,
        unencoded_length,
        hashes,
    }
}

/// Returns an arbitrary unverified metadata object with blob ID.
pub fn unverified_blob_metadata() -> UnverifiedBlobMetadataWithId {
    let metadata = blob_metadata();
    UnverifiedBlobMetadataWithId::new(BlobId::from_sliver_pair_metadata(&metadata), metadata)
}

/// Returns an arbitrary verified metadata object with blob ID.
pub fn verified_blob_metadata() -> VerifiedBlobMetadataWithId {
    let metadata = blob_metadata();
    VerifiedBlobMetadataWithId::new_verified_unchecked(
        BlobId::from_sliver_pair_metadata(&metadata),
        metadata,
    )
}
