// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use core::num::{NonZeroU16, NonZeroU32};

use super::DataTooLargeError;

/// Computes the correct symbol size given the data length and the number of source symbols.
#[inline]
pub fn compute_symbol_size(
    data_length: u64,
    n_symbols: NonZeroU32,
    required_alignment: u16,
) -> Result<NonZeroU16, DataTooLargeError> {
    // Use a 1-byte symbol size for the empty blob.
    let data_length = data_length.max(1);
    let symbol_size = data_length
        .div_ceil(u64::from(n_symbols.get()))
        .next_multiple_of(required_alignment.into());

    Ok(
        NonZeroU16::new(u16::try_from(symbol_size).map_err(|_| DataTooLargeError)?)
            .expect("we start with something positive and always round up"),
    )
}

/// Computes the correct symbol size given the data length and the number of source symbols.
#[inline]
pub fn compute_symbol_size_from_usize(
    data_length: usize,
    n_symbols: NonZeroU32,
    required_alignment: u16,
) -> Result<NonZeroU16, DataTooLargeError> {
    compute_symbol_size(
        data_length.try_into().map_err(|_| DataTooLargeError)?,
        n_symbols,
        required_alignment,
    )
}

/// The number of symbols a blob is split into.
#[inline]
pub fn source_symbols_per_blob(
    source_symbols_primary: NonZeroU16,
    source_symbols_secondary: NonZeroU16,
) -> NonZeroU32 {
    NonZeroU32::from(source_symbols_primary)
        .checked_mul(source_symbols_secondary.into())
        .expect("product of two u16 always fits into a u32")
}

#[cfg(test)]
mod tests {
    use walrus_test_utils::param_test;

    use super::*;

    param_test! {
        compute_symbol_size_matches_expectation: [
            empty_data_1: (0, 1, 1, 1),
            empty_data_2: (0, 42, 1, 1),
            aligned_data_1: (15, 5, 1, 3),
            aligned_data_2: (13, 13, 1, 1),
            misaligned_data_1: (16, 5, 1, 4),
            misaligned_data_2: (19, 5, 1, 4),
            empty_data_alignment_2_1: (0, 1, 2, 2),
            empty_data_alignment_2_2: (0, 42, 2, 2),
            aligned_data_alignment_2_1: (15, 5, 2, 4),
            aligned_data_alignment_2_2: (13, 13, 2, 2),
            misaligned_data_alignment_2_1: (21, 5, 2, 6),
            misaligned_data_alignment_2_2: (24, 5, 2, 6),
        ]
    }
    fn compute_symbol_size_matches_expectation(
        data_length: u64,
        n_symbols: u32,
        required_alignment: u16,
        expected_result: u16,
    ) {
        assert_eq!(
            compute_symbol_size(
                data_length,
                NonZeroU32::new(n_symbols).unwrap(),
                required_alignment
            )
            .unwrap(),
            NonZeroU16::new(expected_result).unwrap()
        );
    }
}
