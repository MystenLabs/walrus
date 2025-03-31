// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Definitions and computations related to Byzantine fault tolerance (BFT).

use core::num::NonZeroU16;

/// Returns the maximum number of faulty/Byzantine failures in a BFT system with `n` components.
///
/// This number is often called `f` and must be strictly less than `n as f64 / 3.0`.
#[inline]
pub fn max_n_faulty(n: NonZeroU16) -> u16 {
    (n.get() - 1) / 3
}

/// Returns the minimum number of correct (non-faulty) instances in a BFT system with `n`
/// components.
///
/// If `n == 3f + 1`, then this is equal to `2f + 1`. In other cases, this can be slightly higher.
#[inline]
pub fn min_n_correct(n: NonZeroU16) -> NonZeroU16 {
    (n.get() - max_n_faulty(n))
        .try_into()
        .expect("max_n_faulty < n")
}

#[cfg(test)]
mod tests {
    use walrus_test_utils::param_test;

    use super::*;

    param_test! {
        test_bft_computations: [
            one: (1, 0, 1),
            three: (3, 0, 3),
            four: (4, 1, 3),
            five: (5, 1, 4),
            six: (6, 1, 5),
            hundred: (100, 33, 67),
            multiple_of_three: (300, 99, 201),
        ]
    }
    fn test_bft_computations(
        n_shards: u16,
        expected_max_n_faulty: u16,
        expected_min_n_correct: u16,
    ) {
        let nonzero_n_shards = n_shards.try_into().unwrap();
        let actual_max_n_faulty = max_n_faulty(nonzero_n_shards);
        let actual_min_n_correct = min_n_correct(nonzero_n_shards);
        assert_eq!(actual_max_n_faulty, expected_max_n_faulty);
        assert_eq!(
            actual_min_n_correct,
            expected_min_n_correct.try_into().unwrap()
        );
        assert!(3 * actual_max_n_faulty < n_shards);
    }
}
