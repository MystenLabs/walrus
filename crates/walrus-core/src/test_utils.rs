// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use fastcrypto::{
    bls12381::min_pk::BLS12381KeyPair,
    traits::{KeyPair, Signer},
};
use rand::{rngs::StdRng, RngCore, SeedableRng};

use crate::{
    encoding,
    merkle::{MerkleProof, Node},
    metadata::{
        BlobMetadata,
        SliverIndex,
        SliverPairIndex,
        SliverPairMetadata,
        UnverifiedBlobMetadataWithId,
        VerifiedBlobMetadataWithId,
    },
    BlobId,
    DecodingSymbol,
    EncodingType,
    SignedStorageConfirmation,
    Sliver,
};

/// Returns a deterministic fixed key pair for testing.
///
/// Various testing facilities can use this key and unit-test can re-generate it to verify the
/// correctness of inputs and outputs.
pub fn keypair() -> BLS12381KeyPair {
    let mut rng = StdRng::seed_from_u64(0);
    BLS12381KeyPair::generate(&mut rng)
}

/// Returns an arbitrary sliver for testing.
pub fn sliver() -> Sliver {
    Sliver::Primary(encoding::Sliver::new(
        [1, 2, 3, 4],
        2,
        SliverPairIndex::new(1),
    ))
}

/// Returns an arbitrary decoding symbol for testing.

pub fn recovery_symbol() -> DecodingSymbol<MerkleProof> {
    encoding::initialize_encoding_config(1, 2, 4);
    match sliver() {
        Sliver::Primary(inner) => inner
            .recovery_symbol_for_sliver_with_proof(SliverIndex::new(1))
            .map(DecodingSymbol::Secondary)
            .unwrap(),
        Sliver::Secondary(_) => unreachable!("Primary sliver expected"),
    }
}

/// Returns an arbitrary storage confirmation for tests.
pub fn signed_storage_confirmation() -> SignedStorageConfirmation {
    let mut rng = StdRng::seed_from_u64(0);
    let mut confirmation = vec![0; 32];
    rng.fill_bytes(&mut confirmation);

    let signer = keypair();
    let signature = signer.sign(&confirmation);
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
pub fn blob_id_from_u64(num: u64) -> BlobId {
    let mut blob_id = [0u8; 32];
    blob_id[24..].copy_from_slice(&num.to_be_bytes());
    BlobId(blob_id)
}

/// Returns an arbitrary metadata object.
pub fn blob_metadata() -> BlobMetadata {
    let unencoded_length = 7_000_000_000;
    let hashes: Vec<_> = (0..100u8)
        .map(|i| SliverPairMetadata {
            primary_hash: Node::Digest([i; 32]),
            secondary_hash: Node::Digest([i; 32]),
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
