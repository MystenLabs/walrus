// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// Allowing `unwrap`s in test utils.
#![allow(clippy::unwrap_used)]

//! Utility functions for tests.

use alloc::{
    borrow::Cow,
    collections::BTreeMap,
    string::{String, ToString},
    vec,
    vec::Vec,
};
use core::num::NonZeroU16;
use std::collections::{HashMap, HashSet};

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
        QuiltError,
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

/// A struct containing the test data for Quilt.
#[derive(Debug, Clone)]
pub struct QuiltTestData<'a> {
    /// A map of QuiltStoreBlobs, keyed by their identifiers.
    pub quilt_store_blobs: Vec<QuiltStoreBlob<'a>>,
    /// A map of blob identifiers, keyed by tag keys and then tag values.
    pub blob_identifiers_by_tag: HashMap<String, HashMap<String, HashSet<String>>>,
}

impl<'a> QuiltTestData<'a> {
    /// Generates a new QuiltTestData from existing data.
    pub fn new(
        blob_data: &'a [&'a [u8]],
        max_value_length: usize,
        max_num_tags: usize,
    ) -> Result<Self, QuiltError> {
        let cow_data: Vec<Cow<'a, [u8]>> =
            blob_data.iter().map(|&data| Cow::Borrowed(data)).collect();

        let quilt_store_blobs_vec =
            Self::generate_random_quilt_store_blobs(cow_data, max_value_length, max_num_tags)?;

        Self::from_quilt_store_blobs(quilt_store_blobs_vec)
    }

    /// Generates a new QuiltTestData with new owned random data.
    pub fn new_owned(
        num_blobs: usize,
        min_blob_size: usize,
        max_blob_size: usize,
        max_value_length: usize,
        max_num_tags: usize,
    ) -> Result<QuiltTestData<'static>, QuiltError> {
        let blob_data =
            walrus_test_utils::generate_random_data(num_blobs, min_blob_size, max_blob_size);

        let cow_data: Vec<Cow<'static, [u8]>> = blob_data.into_iter().map(Cow::Owned).collect();

        let quilt_store_blobs_vec =
            Self::generate_random_quilt_store_blobs(cow_data, max_value_length, max_num_tags)?;

        QuiltTestData::from_quilt_store_blobs(quilt_store_blobs_vec)
    }

    /// Returns the quilt store blobs and clears the internal vector.
    pub fn take_blobs(&mut self) -> Vec<QuiltStoreBlob<'a>> {
        core::mem::take(&mut self.quilt_store_blobs)
    }

    fn from_quilt_store_blobs(
        quilt_store_blobs: Vec<QuiltStoreBlob<'a>>,
    ) -> Result<Self, QuiltError> {
        let mut blob_identifiers_by_tag: HashMap<String, HashMap<String, HashSet<String>>> =
            HashMap::new();

        for blob in &quilt_store_blobs {
            let identifier = blob.identifier().to_string();

            // Build tag index
            for (key, value) in blob.tags() {
                blob_identifiers_by_tag
                    .entry(key.clone())
                    .or_default()
                    .entry(value.clone())
                    .or_default()
                    .insert(identifier.clone());
            }
        }

        Ok(Self {
            quilt_store_blobs,
            blob_identifiers_by_tag,
        })
    }

    /// Generates random QuiltStoreBlobs from the input raw blobs.
    ///
    /// A random unique identifier is generated for each blob.
    /// Random numbers of random tags are generated for each blob.
    fn generate_random_quilt_store_blobs<'b>(
        blob_data: Vec<Cow<'b, [u8]>>,
        max_value_length: usize,
        max_num_tags: usize,
    ) -> Result<Vec<QuiltStoreBlob<'b>>, QuiltError> {
        tracing::debug!("generating random quilt store blobs...");
        let mut rng = rand::thread_rng();
        let num_tags = rng.gen_range(0..=max_num_tags);

        let mut res = Vec::with_capacity(blob_data.len());
        let mut identifiers = HashSet::with_capacity(blob_data.len());
        while identifiers.len() < blob_data.len() {
            let identifier_length = rng.gen_range(1..=max_value_length);
            let random_data = walrus_test_utils::random_data_from_rng(identifier_length, &mut rng);
            let encoded = hex::encode(random_data);
            identifiers.insert(encoded);
        }

        let raw_tag_values = walrus_test_utils::generate_random_data(num_tags, 1, max_value_length);
        let raw_tag_keys = walrus_test_utils::generate_random_data(num_tags, 1, max_value_length);
        let tag_values = raw_tag_values.iter().map(hex::encode).collect::<Vec<_>>();
        let tag_keys = raw_tag_keys.iter().map(hex::encode).collect::<Vec<_>>();

        for (data, identifier) in blob_data.into_iter().zip(identifiers.iter()) {
            let mut tags = BTreeMap::new();
            let num_keys_for_blob = rng.gen_range(0..=num_tags);

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

            let mut blob = match data {
                Cow::Borrowed(b) => QuiltStoreBlob::new(b, identifier)?,
                Cow::Owned(v) => QuiltStoreBlob::new_owned(v, identifier)?,
            };
            if !tags.is_empty() {
                blob = blob.with_tags(tags);
            }
            res.push(blob);
        }

        Ok(res)
    }
}

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
