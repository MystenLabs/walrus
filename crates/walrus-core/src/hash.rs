// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! BLAKE2b-256 with runtime SIMD selection on x86.

use core::fmt;

use fastcrypto::hash::{Digest, HashFunction};

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
type Backend = blake2b_simd::State;
#[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
type Backend = fastcrypto::hash::Blake2b256;

/// The same unkeyed BLAKE2b-256 used by Walrus commitments, with an optimized x86 backend.
///
/// On x86, `blake2b_simd` selects AVX2, SSE4.1, or portable code at runtime. Other architectures
/// retain the RustCrypto backend used by `fastcrypto`. Digest parameters and input bytes are
/// unchanged; this is BLAKE2b configured for a 32-byte output, not a truncated BLAKE2b-512 digest.
pub struct Blake2b256(Backend);

impl fmt::Debug for Blake2b256 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Blake2b256").finish_non_exhaustive()
    }
}

impl Default for Blake2b256 {
    #[inline]
    fn default() -> Self {
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            Self(blake2b_simd::Params::new().hash_length(32).to_state())
        }
        #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
        {
            Self(Backend::default())
        }
    }
}

impl HashFunction<32> for Blake2b256 {
    #[inline]
    fn update<Data: AsRef<[u8]>>(&mut self, data: Data) {
        self.0.update(data.as_ref());
    }

    #[inline]
    fn finalize(self) -> Digest<32> {
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            let mut digest = [0; 32];
            digest.copy_from_slice(self.0.finalize().as_bytes());
            Digest::new(digest)
        }
        #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
        {
            self.0.finalize()
        }
    }
}

#[cfg(test)]
mod tests {
    use alloc::vec::Vec;
    use core::num::NonZeroU16;

    use fastcrypto::hash::Blake2b256 as Reference;
    use proptest::prelude::*;

    use super::*;
    use crate::{
        BlobId,
        encoding::{EncodingFactory as _, ReedSolomonEncodingConfig},
        merkle::MerkleTree,
    };

    #[test]
    fn encoded_blob_commitments_match_reference_backend() {
        for (shards, blob_size) in [(4, 0), (5, 129), (100, 5 * 1024 * 1024), (1000, 4096)] {
            let config = ReedSolomonEncodingConfig::new(NonZeroU16::new(shards).unwrap());
            let blob = (0..=255).cycle().take(blob_size).collect();
            let (pairs, metadata) = config.encode_with_metadata(blob).unwrap();
            let config = config.into();
            let reference_leaves: Vec<_> = pairs
                .iter()
                .map(|pair| pair.pair_leaf_input::<Reference>(&config).unwrap())
                .collect();
            let optimized_leaves: Vec<_> = pairs
                .iter()
                .map(|pair| pair.pair_leaf_input::<Blake2b256>(&config).unwrap())
                .collect();
            assert_eq!(optimized_leaves, reference_leaves);
            let reference_root = MerkleTree::<Reference>::build(&reference_leaves).root();
            let reference_id = BlobId::new_with_hash_function::<Reference>(
                reference_root,
                crate::EncodingType::RS2,
                u64::try_from(blob_size).unwrap(),
            );
            assert_eq!(metadata.blob_id(), &reference_id);
        }
    }

    #[test]
    fn empty_digest_matches_blake2b_256_vector() {
        assert_eq!(
            hex::encode(Blake2b256::digest([]).digest),
            "0e5751c026e543b2e8ab2eb06099daa1d1e5df47778f7787faab45cdf12fe3a8"
        );
    }

    #[test]
    fn block_boundaries_and_split_updates_match_reference() {
        for length in [
            0, 1, 31, 32, 63, 64, 65, 127, 128, 129, 255, 256, 257, 4096, 65535,
        ] {
            let input: Vec<u8> = (0..=255).cycle().take(length).collect();
            for chunk_size in [1, 31, 64, 127, 128, 129, 1024] {
                let mut hasher = Blake2b256::default();
                hasher.update([]);
                for chunk in input.chunks(chunk_size) {
                    hasher.update(chunk);
                }
                hasher.update([]);
                assert_eq!(hasher.finalize(), Reference::digest(&input));
            }
        }
    }

    proptest! {
        #[test]
        fn arbitrary_inputs_and_chunking_match_reference(
            input in proptest::collection::vec(any::<u8>(), 0..8192),
            chunk_size in 1usize..1024,
        ) {
            let mut hasher = Blake2b256::default();
            for chunk in input.chunks(chunk_size) {
                hasher.update(chunk);
            }
            prop_assert_eq!(hasher.finalize(), Reference::digest(&input));
        }
    }
}
