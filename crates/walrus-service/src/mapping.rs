// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The mapping between the encoded sliver pairs and shards.

use thiserror::Error;
use walrus_core::{encoding::SliverPair, BlobId};

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

/// Rotate the slice of sliver pairs in place, based on the rotation specified by the Blob ID.
///
/// Does nothing if the pairs have already been rotated correctly, according to the Blob ID.
/// Returns a [`SliverAssignmentError`] if the slice is shuffled in a way that is inconsistent with
/// the provided `blob_id`.
///
/// # Arguments
///
/// * `pairs` - The slice of sliver pairs to rotate in place. This is assumed to be already of
/// length equal to the number of shards.
/// * `blob_id` - The Blob ID that produced the sliver pairs. It is interpreted as a big-endian
/// unsigned integer, and the used to compute the amount for which to rotate the slice. The rotation
/// is such that the last `blob_id % slice.len()` elements of the slice move to the front.
///
/// # Errors
///
/// Returns a [`SliverAssignmentError`] if the `pairs` have already been shuffled in a way that is
/// inconsistent with the provided `blob_id`.
pub fn rotate_pairs(
    pairs: &mut [SliverPair],
    blob_id: &BlobId,
) -> Result<(), SliverAssignmentError> {
    if pairs.is_empty() {
        return Ok(());
    }
    if is_rotation(pairs) {
        if pairs[0].index() == 0 {
            rotate_by_bytes(pairs, blob_id.as_ref());
        } else if pairs[0].index() as usize != pair_index_for_shard(0, pairs.len(), blob_id) {
            return Err(SliverAssignmentError::InconsistentRotation);
        }
        Ok(())
    } else {
        Err(SliverAssignmentError::InvalidInputOrder)
    }
}

/// Rotate the slice of sliver pairs in place, based on the rotation specified by the Blob ID.
///
/// This function does not check whether the pairs have already been rotated. See [`rotate_pairs`]
/// for the checked version.
///
/// # Arguments
///
/// * `pairs` - The slice of sliver pairs to rotate in place. This is assumed to be already of
/// length equal to the number of shards.
/// * `blob_id` - The Blob ID that produced the sliver pairs. It is interpreted as a big-endian
/// unsigned integer, and the used to compute the amount for which to rotate the slice. The rotation
/// is such that the last `blob_id % slice.len()` elements of the slice move to the front.
pub fn rotate_pairs_unchecked(pairs: &mut [SliverPair], blob_id: &BlobId) {
    if pairs.is_empty() {
        return;
    }
    rotate_by_bytes(pairs, blob_id.as_ref());
}

/// Check that the slice of sliver pairs is a valid rotation.
fn is_rotation(pairs: &[SliverPair]) -> bool {
    // TODO(mlegner): also check internal state of pair?
    pairs
        .iter()
        .enumerate()
        .all(|(idx, pair)| pair.index() as usize == (idx + pairs[0].index() as usize) % pairs.len())
}

/// Get the index of the shard on which the sliver pair of the given index is stored.
///
/// # Arguments
///
/// * `pair_idx` - The index of the sliver pair, as returned by the blob-encoding function.
/// * `total_shards` - The total number of shards in the system.
/// * `blob_id` - The Blob ID that produced the sliver. It is interpreted as a big-endian unsigned
/// integer, and then used to compute the offset for the sliver pair index.
pub fn shard_index_for_pair(pair_idx: usize, total_shards: usize, blob_id: &BlobId) -> usize {
    (bytes_mod(blob_id.as_ref(), total_shards) + pair_idx) % total_shards
}

/// Get the index of the sliver pair which is store on the shard of the given index.
///
/// # Arguments
///
/// * `shard_idx` - The index of the shard.
/// * `total_shards` - The total number of shards in the system.
/// * `blob_id` - The Blob ID that produced the sliver pair. It is interpreted as a big-endian
/// unsigned integer, and the used to compute the offset for the sliver pair index.
pub fn pair_index_for_shard(shard_idx: usize, total_shards: usize, blob_id: &BlobId) -> usize {
    (total_shards - bytes_mod(blob_id.as_ref(), total_shards) + shard_idx) % total_shards
}

/// Rotate the input slice in place, based on the rotation specified by the byte array.
///
/// # Arguments
///
/// * `slice` - The slice to rotate in place.
/// * `rotation` - Interpreted as a big-endian unsigned integer, this is the amount for which to
/// rotate the slice. The rotation is such that the last `rotation % slice.len()` elements of the
/// slice move to the front.
fn rotate_by_bytes<T>(slice: &mut [T], rotation: &[u8]) {
    slice.rotate_right(bytes_mod(rotation, slice.len()))
}

/// Compute the modulo of the input byte array interpreted as an big-endian unsigned integer.
/// Uses Horner'r method.
fn bytes_mod(bytes: &[u8], modulus: usize) -> usize {
    bytes
        .iter()
        .fold(0, |acc, &byte| (acc * 256 + byte as usize) % modulus)
}

#[cfg(test)]
mod tests {
    use walrus_core::encoding::Sliver;
    use walrus_test_utils::param_test;

    use super::*;

    // Get a blob ID of given number for testing.
    fn blob_id_for_testing(num: u64) -> BlobId {
        let mut blob_id = [0u8; 32];
        blob_id[24..].copy_from_slice(&num.to_be_bytes());
        BlobId(blob_id)
    }

    // Fixture
    fn sliver_pairs(num: u32) -> Vec<SliverPair> {
        (0..num)
            .map(|n| SliverPair {
                primary: Sliver::new_empty(0, 1, n),
                secondary: Sliver::new_empty(0, 1, num - n - 1),
            })
            .collect()
    }

    #[test]
    fn test_rotate_pairs() {
        let mut pairs = sliver_pairs(7);
        let blob_id = blob_id_for_testing(17);
        rotate_pairs(&mut pairs, &blob_id).unwrap();
        // Check that all the pairs are is in the correct spot
        assert!(pairs
            .iter()
            .enumerate()
            .all(|(idx, pair)| pair_index_for_shard(idx, 7, &blob_id) == pair.index() as usize));
    }

    #[test]
    fn test_rotate_pairs_unchecked() {
        let mut pairs = sliver_pairs(7);
        let blob_id = blob_id_for_testing(17);
        rotate_pairs_unchecked(&mut pairs, &blob_id);
        // Check that all the pairs are is in the correct spot
        for (idx, pair) in pairs.iter().enumerate() {
            assert_eq!(
                pair_index_for_shard(idx, 7, &blob_id),
                pair.index() as usize
            );
        }
        // Rotate again and check if the two rotations combined have been applied
        let blob_id_2 = blob_id_for_testing(15);
        let combined_blob_id = blob_id_for_testing(18); // 17 % 7 + 15 % 7 = 18 % 7
        rotate_pairs_unchecked(&mut pairs, &blob_id_2);
        assert!(pairs
            .iter()
            .enumerate()
            .all(
                |(idx, pair)| pair_index_for_shard(idx, 7, &combined_blob_id)
                    == pair.index() as usize
            ));
    }

    #[test]
    fn test_is_rotation() {
        // No rotation
        let mut slivers_1 = sliver_pairs(7);
        assert!(is_rotation(&slivers_1));
        // Rotation
        rotate_pairs(&mut slivers_1, &blob_id_for_testing(17)).unwrap();
        assert!(is_rotation(&slivers_1));
        // Incorrect shuffling
        slivers_1.swap(2, 5);
        assert!(!is_rotation(&slivers_1));
    }

    #[test]
    fn test_wrong_rotation_pairs() {
        let mut pairs = sliver_pairs(7);
        let blob_id_1 = blob_id_for_testing(17);
        let blob_id_2 = blob_id_for_testing(18);
        rotate_pairs(&mut pairs, &blob_id_1).unwrap();
        assert!(
            rotate_pairs(&mut pairs, &blob_id_2)
                == Err(SliverAssignmentError::InconsistentRotation)
        );
    }

    #[test]
    fn test_idempotent_rotation() {
        let mut pairs = sliver_pairs(7);
        let blob_id = blob_id_for_testing(17);
        rotate_pairs(&mut pairs, &blob_id).unwrap();
        let cloned = pairs.clone();
        // Check that rotating again does not have an effect
        rotate_pairs(&mut pairs, &blob_id).unwrap();
        assert_eq!(cloned, pairs);
    }

    fn test_shard_index_for_pair(
        total_shards: usize,
        blob_id_value: u64,
        pair_idx: usize,
        shard_idx: usize,
    ) {
        let blob_id = blob_id_for_testing(blob_id_value);
        assert_eq!(
            shard_index_for_pair(pair_idx, total_shards, &blob_id),
            shard_idx
        );
    }

    param_test! {
        test_shard_index_for_pair: [
                start: (7, 15, 0, 1),
                mid: (7, 15, 5,6),
                end: (7, 15, 6, 0),
            ]
    }

    fn test_pair_index_for_shard(
        total_shards: usize,
        blob_id_value: u64,
        shard_idx: usize,
        pair_idx: usize,
    ) {
        let blob_id = blob_id_for_testing(blob_id_value);
        assert_eq!(
            pair_index_for_shard(shard_idx, total_shards, &blob_id),
            pair_idx
        );
    }

    param_test! {
            test_pair_index_for_shard: [
                start: (7, 16, 0, 5),
                mid: (7, 16, 1, 6),
                end: (7, 16, 6, 4),
            ]
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
        let blob_id = blob_id_for_testing(11);
        rotate_by_bytes(&mut pairs, blob_id.as_ref());
        assert_eq!(pairs, [4, 0, 1, 2, 3]);
    }
}
