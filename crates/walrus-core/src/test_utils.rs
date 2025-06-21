// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// Allowing `unwrap`s in test utils.
#![allow(clippy::unwrap_used)]

//! Utility functions for tests.

use alloc::{collections::BTreeMap, vec, vec::Vec};
use core::num::NonZeroU16;
use std::collections::HashSet;

use fastcrypto::traits::{KeyPair, Signer as _};
use rand::{Rng, RngCore, SeedableRng, rngs::StdRng, seq::SliceRandom};

use crate::{
    BlobId,
    DEFAULT_ENCODING,
    RecoverySymbol,
    Sliver,
    SliverIndex,
    SliverPairIndex,
    encoding::{
        self,
        EncodingConfig,
        EncodingConfigTrait as _,
        PrimaryRecoverySymbol,
        PrimarySliver,
        SecondarySliver,
        quilt_encoding::QuiltStoreBlob,
    },
    keys::{NetworkKeyPair, ProtocolKeyPair},
    merkle::{MerkleProof, Node},
    messages::SignedMessage,
    metadata::{
        BlobMetadata,
        SliverPairMetadata,
        UnverifiedBlobMetadataWithId,
        VerifiedBlobMetadataWithId,
    },
};

/// Returns a deterministic fixed protocol key pair for testing.
///
/// Various testing facilities can use this key and unit-test can re-generate it to verify the
/// correctness of inputs and outputs.
pub fn protocol_key_pair() -> ProtocolKeyPair {
    let mut rng = StdRng::seed_from_u64(0);
    ProtocolKeyPair::new(KeyPair::generate(&mut rng))
}

/// Returns a deterministic fixed network key pair for testing.
pub fn network_key_pair() -> NetworkKeyPair {
    let mut rng = StdRng::seed_from_u64(0);
    NetworkKeyPair::generate_with_rng(&mut rng)
}

/// Returns an arbitrary signed message for tests.
pub fn random_signed_message<T>() -> SignedMessage<T> {
    let mut rng = StdRng::seed_from_u64(0);
    let mut message = vec![0; 32];
    rng.fill_bytes(&mut message);

    let signer = protocol_key_pair();
    let signature = signer.as_ref().sign(&message);
    SignedMessage::new_from_encoded(message, signature)
}

/// Returns an arbitrary sliver for testing.
pub fn sliver() -> Sliver {
    Sliver::Primary(primary_sliver())
}

/// Returns an arbitrary primary sliver with 7 symbols (compatible with 10 shards) for testing.
pub fn primary_sliver() -> PrimarySliver {
    encoding::SliverData::new(
        [
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28,
        ],
        4.try_into().unwrap(),
        SliverIndex(1),
    )
}

/// Returns an arbitrary secondary sliver with 4 symbols (compatible with 10 shards) for testing.
pub fn secondary_sliver() -> SecondarySliver {
    encoding::SliverData::new(
        [
            21, 22, 23, 24, 25, 26, 27, 28, 29, 210, 211, 212, 213, 214, 215, 216,
        ],
        4.try_into().unwrap(),
        SliverIndex(1),
    )
}

/// Returns a BFT-compatible encoding configuration with 10 shards.
pub fn encoding_config() -> EncodingConfig {
    EncodingConfig::new(NonZeroU16::new(10).unwrap())
}

/// Returns an arbitrary recovery symbol for testing.
pub fn recovery_symbol() -> RecoverySymbol<MerkleProof> {
    primary_sliver()
        .recovery_symbol_for_sliver(
            SliverPairIndex(1),
            &encoding_config().get_for_type(DEFAULT_ENCODING),
        )
        .map(RecoverySymbol::Secondary)
        .unwrap()
}

/// Returns an arbitrary secondary recovery symbol for testing.
pub fn primary_recovery_symbol() -> RecoverySymbol<MerkleProof> {
    secondary_sliver()
        .recovery_symbol_for_sliver(
            SliverPairIndex(2),
            &encoding_config().get_for_type(DEFAULT_ENCODING),
        )
        .map(RecoverySymbol::Primary)
        .unwrap()
}

/// Returns an empty Merkle proof for testing.
pub fn merkle_proof() -> MerkleProof {
    MerkleProof::new(&[])
}

/// Returns a random blob ID for testing.
pub fn random_blob_id() -> BlobId {
    let mut bytes = [0; BlobId::LENGTH];
    rand::thread_rng().fill_bytes(&mut bytes);
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
    let hashes: Vec<_> = (0..config.n_shards.into())
        .map(|i| {
            let byte = u8::try_from(i % 256).expect("this is guaranteed to fit into a u8");
            SliverPairMetadata {
                primary_hash: Node::Digest([byte; 32]),
                secondary_hash: Node::Digest([byte; 32]),
            }
        })
        .collect();
    BlobMetadata::new(DEFAULT_ENCODING, 62_831, hashes)
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

/// Tuple containing an [`EncodingConfig`], [`VerifiedBlobMetadataWithId`], a
/// [`SliverIndex`] and a valid vector of [`PrimaryRecoverySymbol`]s for that index.
pub type RecoverySymbolsWithConfigAndMetadata = (
    EncodingConfig,
    VerifiedBlobMetadataWithId,
    SliverIndex,
    Vec<PrimaryRecoverySymbol<MerkleProof>>,
);

/// Generates an [`EncodingConfig`], [`VerifiedBlobMetadataWithId`], a [`SliverIndex`]
/// and a valid vector of [`PrimaryRecoverySymbol`]s for that index.
pub fn generate_config_metadata_and_valid_recovery_symbols()
-> walrus_test_utils::Result<RecoverySymbolsWithConfigAndMetadata> {
    let blob = walrus_test_utils::random_data(314);
    let encoding_config = encoding_config();
    let config_enum = encoding_config.get_for_type(DEFAULT_ENCODING);
    let (sliver_pairs, metadata) = config_enum.encode_with_metadata(&blob)?;
    let target_sliver_index = SliverIndex(0);
    let recovery_symbols = walrus_test_utils::random_subset(
        (1..encoding_config.n_shards.get()).map(|i| {
            sliver_pairs[i as usize]
                .secondary
                .recovery_symbol_for_sliver(
                    target_sliver_index.into(),
                    &encoding_config.get_for_type(DEFAULT_ENCODING),
                )
                .unwrap()
        }),
        config_enum.n_secondary_source_symbols().get().into(),
    )
    .collect();
    Ok((
        encoding_config,
        metadata,
        target_sliver_index,
        recovery_symbols,
    ))
}

/// Generates random quilt store blobs from the input raw blobs.
///
/// A random unique identifier is generated for each blob.
/// If `include_tags` is true, random numbers of random tags are generated for each blob.
pub fn generate_random_quilt_store_blobs<'a>(
    blob_data: &'a [&'a [u8]],
    max_string_length: usize,
    include_tags: bool,
    max_num_tags: usize,
) -> Vec<QuiltStoreBlob<'a>> {
    let mut rng = rand::thread_rng();
    let num_tags = if include_tags {
        rng.gen_range(1..=max_num_tags)
    } else {
        0
    };

    let mut res = Vec::with_capacity(blob_data.len());
    let mut identifiers = HashSet::with_capacity(blob_data.len());
    while identifiers.len() < blob_data.len() {
        identifiers.insert(hex::encode(walrus_test_utils::random_data(
            rng.gen_range(1..max_string_length),
        )));
    }

    let raw_tag_values = walrus_test_utils::generate_random_data(num_tags, 1, max_string_length);
    let raw_tag_keys = walrus_test_utils::generate_random_data(num_tags, 1, max_string_length);
    let tag_values = raw_tag_values.iter().map(hex::encode).collect::<Vec<_>>();
    let tag_keys = raw_tag_keys.iter().map(hex::encode).collect::<Vec<_>>();

    for (data, identifier) in blob_data.iter().zip(identifiers.iter()) {
        let mut tags = BTreeMap::new();
        let num_keys_for_blob = if include_tags {
            rng.gen_range(0..=num_tags)
        } else {
            0
        };

        if num_keys_for_blob > 0 {
            let selected_keys: Vec<_> = tag_keys
                .as_slice()
                .choose_multiple(&mut rng, num_keys_for_blob)
                .collect();

            for key in selected_keys {
                let value = tag_values.choose(&mut rng).expect("Should choose a value");
                tags.insert(key.clone(), value.clone());
            }
        }

        let mut blob = QuiltStoreBlob::new(data, identifier);
        if !tags.is_empty() {
            blob = blob.with_tags(tags);
        }
        res.push(blob);
    }

    res
}
