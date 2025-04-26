// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! The mapping between the encoded sliver pairs and shards.

use core::num::NonZeroU16;

use thiserror::Error;

use crate::{BlobId, ShardIndex, SliverPairIndex, encoding::SliverPair};

/// Errors returned if the slice of sliver pairs has already been shuffled in a way that is
/// inconsistent with the provided blob id.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum SliverAssignmentError {
    /// The rotation is inconsistent with respect to the blob id
    #[error("the sliver pairs are already rotated, but not according to the blob id")]
    InconsistentRotation,
    /// The input slice of pairs is not a valid rotation
    #[error("the sliver pairs have been incorrectly shuffled")]
    InvalidInputOrder,
}

/// Rotate the slice of sliver pairs in place, based on the rotation specified by the blob ID.
///
/// Does nothing if the `pairs` have already been rotated correctly, according to the blob ID.
/// Returns a [`SliverAssignmentError`] if the slice is shuffled in a way that is inconsistent with
/// the provided `blob_id`.
///
/// The `blob_id` -- which is typically the blob ID of the blob that produced the sliver pairs -- is
/// interpreted as a big-endian unsigned integer, and is then used to compute the amount for which
/// to rotate the slice. The rotation is such that the last `blob_id % slice.len()` elements of the
/// slice move to the front.
///
/// # Errors
///
/// Returns a [`SliverAssignmentError`] if the `pairs` have already been shuffled in a way that is
/// inconsistent with the provided `blob_id`.
///
/// # Panics
///
/// Panics if the length of the provided slice is larger than `u16::MAX`.
pub fn rotate_pairs(
    pairs: &mut [SliverPair],
    blob_id: &BlobId,
) -> Result<(), SliverAssignmentError> {
    let Some(n_pairs) = NonZeroU16::new(
        pairs
            .len()
            .try_into()
            .expect("there must not be more than `u16::MAX` sliver pairs"),
    ) else {
        // Nothing to do for an empty slice.
        return Ok(());
    };
    if is_rotation(pairs) {
        if pairs[0].index() == SliverPairIndex(0) {
            rotate_by_bytes(pairs, blob_id.as_ref());
        } else if pairs[0].index() != ShardIndex(0).to_pair_index(n_pairs, blob_id) {
            return Err(SliverAssignmentError::InconsistentRotation);
        }
        Ok(())
    } else {
        Err(SliverAssignmentError::InvalidInputOrder)
    }
}

/// Rotate the slice of sliver pairs in place, based on the rotation specified by the blob ID.
///
/// This function does not check whether the pairs have already been rotated. See [`rotate_pairs`]
/// for the checked version and the details on how the rotation is performed.
pub fn rotate_pairs_unchecked(pairs: &mut [SliverPair], blob_id: &BlobId) {
    if pairs.is_empty() {
        return;
    }
    rotate_by_bytes(pairs, blob_id.as_ref());
}

/// Check that the slice of sliver pairs is a valid rotation.
///
/// This only checks the first sliver of the pair, it does not check the internal state of the pair.
fn is_rotation(pairs: &[SliverPair]) -> bool {
    pairs.iter().enumerate().all(|(index, pair)| {
        pair.index().as_usize() == (index + pairs[0].index().as_usize()) % pairs.len()
    })
}

impl SliverPairIndex {
    /// Returns the index of the shard on which the sliver pair with this index is stored.
    ///
    /// The mapping depends on the total number of shards, `n_shards`, and the blob ID to which this
    /// sliver corresponds, `blob_id`. The `blob_id` is interpreted as a big-endian unsigned
    /// integer, and then used to compute the offset for the sliver pair index.
    pub fn to_shard_index(&self, n_shards: NonZeroU16, blob_id: &BlobId) -> ShardIndex {
        ((self.as_usize() + rotation_offset(n_shards, blob_id)) % usize::from(n_shards.get()))
            .try_into()
            .expect("definitely fits into a u16 because `n_shards` is a u16")
    }
}

impl ShardIndex {
    /// Returns the index of the sliver pair of this blob corresponding to this shard index.
    ///
    /// This is the reverse operation of [`SliverPairIndex::to_shard_index`].
    pub fn to_pair_index(&self, n_shards: NonZeroU16, blob_id: &BlobId) -> SliverPairIndex {
        let n_shards_usize = usize::from(n_shards.get());
        ((n_shards_usize + self.as_usize() - rotation_offset(n_shards, blob_id)) % n_shards_usize)
            .try_into()
            .expect("definitely fits into a u16 because `n_shards` is a u16")
    }
}

fn rotation_offset(n_shards: NonZeroU16, blob_id: &BlobId) -> usize {
    bytes_mod(blob_id.as_ref(), n_shards.get().into())
}

/// Rotate the input `slice` in place, based on the rotation specified by the `rotation` byte array.
///
/// The `rotation` byte array is the amount for which to rotate the slice, and it is interpreted as
/// a big-endian unsigned integer. The `rotation` will typically be the blob ID. The resulting
/// rotation of the slice is such that the last `rotation % slice.len()` elements of the slice move
/// to the front.
fn rotate_by_bytes<T>(slice: &mut [T], rotation: &[u8]) {
    slice.rotate_right(bytes_mod(rotation, slice.len()))
}

/// Compute the modulo of the input byte array interpreted as an big-endian unsigned integer.
///
/// Uses Horner's method.
fn bytes_mod(bytes: &[u8], modulus: usize) -> usize {
    bytes
        .iter()
        .fold(0, |acc, &byte| (acc * 256 + usize::from(byte)) % modulus)
}

#[cfg(test)]
mod tests {
    use alloc::vec::Vec;

    use walrus_test_utils::param_test;

    use super::*;
    use crate::{encoding::RaptorQEncodingConfig, test_utils};

    // Fixture
    fn sliver_pairs(num: u16) -> Vec<SliverPair> {
        let encoding_config = RaptorQEncodingConfig::new_for_test(1, 1, num);
        (0..num)
            .map(|n| {
                SliverPair::new_empty(
                    &(&encoding_config).into(),
                    1.try_into().unwrap(),
                    SliverPairIndex(n),
                )
            })
            .collect()
    }

    param_test! {
        shard_index_pair_index_conversion_works: [
            zero: (0, 0),
            one_1: (1, 0),
            one_2: (0, 1),
            arbitrary: (11, 27),
        ]
    }
    fn shard_index_pair_index_conversion_works(index: u16, blob_id: u64) {
        let n_shards = 13.try_into().unwrap();
        let blob_id = &test_utils::blob_id_from_u64(blob_id);
        assert_eq!(
            ShardIndex(index),
            ShardIndex(index)
                .to_pair_index(n_shards, blob_id)
                .to_shard_index(n_shards, blob_id)
        );
        assert_eq!(
            SliverPairIndex(index),
            SliverPairIndex(index)
                .to_shard_index(n_shards, blob_id)
                .to_pair_index(n_shards, blob_id)
        );
    }

    #[test]
    fn test_rotate_pairs() {
        let mut pairs = sliver_pairs(7);
        let blob_id = test_utils::blob_id_from_u64(17);
        rotate_pairs(&mut pairs, &blob_id).unwrap();
        // Check that all the pairs are is in the correct spot
        assert!(pairs.iter().enumerate().all(|(index, pair)| {
            ShardIndex(index as u16).to_pair_index(7.try_into().unwrap(), &blob_id) == pair.index()
        }));
    }

    #[test]
    fn test_rotate_pairs_unchecked() {
        let mut pairs = sliver_pairs(7);
        let blob_id = test_utils::blob_id_from_u64(17);
        rotate_pairs_unchecked(&mut pairs, &blob_id);
        // Check that all the pairs are is in the correct spot
        for (index, pair) in pairs.iter().enumerate() {
            assert_eq!(
                ShardIndex(index as u16).to_pair_index(7.try_into().unwrap(), &blob_id),
                pair.index()
            );
        }
        // Rotate again and check if the two rotations combined have been applied
        let blob_id_2 = test_utils::blob_id_from_u64(15);
        let combined_blob_id = test_utils::blob_id_from_u64(18); // 17 % 7 + 15 % 7 = 18 % 7
        rotate_pairs_unchecked(&mut pairs, &blob_id_2);
        assert!(pairs.iter().enumerate().all(|(index, pair)| {
            ShardIndex(index as u16).to_pair_index(7.try_into().unwrap(), &combined_blob_id)
                == pair.index()
        }));
    }

    #[test]
    fn test_is_rotation() {
        // No rotation
        let mut slivers_1 = sliver_pairs(7);
        assert!(is_rotation(&slivers_1));
        // Rotation
        rotate_pairs(&mut slivers_1, &test_utils::blob_id_from_u64(17)).unwrap();
        assert!(is_rotation(&slivers_1));
        // Incorrect shuffling
        slivers_1.swap(2, 5);
        assert!(!is_rotation(&slivers_1));
    }

    #[test]
    fn test_wrong_rotation_pairs() {
        let mut pairs = sliver_pairs(7);
        let blob_id_1 = test_utils::blob_id_from_u64(17);
        let blob_id_2 = test_utils::blob_id_from_u64(18);
        rotate_pairs(&mut pairs, &blob_id_1).unwrap();
        assert!(
            rotate_pairs(&mut pairs, &blob_id_2)
                == Err(SliverAssignmentError::InconsistentRotation)
        );
    }

    #[test]
    fn test_idempotent_rotation() {
        let mut pairs = sliver_pairs(7);
        let blob_id = test_utils::blob_id_from_u64(17);
        rotate_pairs(&mut pairs, &blob_id).unwrap();
        let cloned = pairs.clone();
        // Check that rotating again does not have an effect
        rotate_pairs(&mut pairs, &blob_id).unwrap();
        assert_eq!(cloned, pairs);
    }

    param_test! {
        test_shard_index_for_pair: [
                start: (7, 15, SliverPairIndex(0), ShardIndex(1)),
                mid: (7, 15, SliverPairIndex(5), ShardIndex(6)),
                end: (7, 15, SliverPairIndex(6), ShardIndex(0)),
            ]
    }
    fn test_shard_index_for_pair(
        n_shards: u16,
        blob_id_value: u64,
        pair_index: SliverPairIndex,
        shard_index: ShardIndex,
    ) {
        let blob_id = test_utils::blob_id_from_u64(blob_id_value);
        assert_eq!(
            pair_index.to_shard_index(n_shards.try_into().unwrap(), &blob_id),
            shard_index
        );
    }

    param_test! {
            test_pair_index_for_shard: [
                start: (7, 16, 0, 5),
                mid: (7, 16, 1, 6),
                end: (7, 16, 6, 4),
            ]
    }
    fn test_pair_index_for_shard(
        n_shards: u16,
        blob_id_value: u64,
        shard_index: u16,
        pair_index: u16,
    ) {
        let blob_id = test_utils::blob_id_from_u64(blob_id_value);
        assert_eq!(
            ShardIndex(shard_index).to_pair_index(n_shards.try_into().unwrap(), &blob_id),
            SliverPairIndex(pair_index)
        );
    }

    #[test]
    fn test_bytes_mod() {
        for x in 0..10_000 {
            for y in [2, 3, 5, 7, 11, 13, 17, 19, 23, 29] {
                bytes_mod_test(x, y);
            }
        }
        bytes_mod_test(185601938467, 17);
        bytes_mod_test(usize::MAX, 17);
    }

    fn bytes_mod_test(num: usize, modulus: usize) {
        assert_eq!(bytes_mod(&num.to_be_bytes(), modulus), num % modulus);
    }

    #[test]
    fn test_rotate_by_bytes() {
        let mut pairs = Vec::from_iter(0..5);
        let blob_id = test_utils::blob_id_from_u64(11);
        rotate_by_bytes(&mut pairs, blob_id.as_ref());
        assert_eq!(pairs, [4, 0, 1, 2, 3]);
    }
}
