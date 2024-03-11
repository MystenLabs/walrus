// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use raptorq::{EncodingPacket, ObjectTransmissionInformation};

/// Creates a new [`ObjectTransmissionInformation`] for the given `symbol_size`.
///
/// # Panics
///
/// Panics if the `symbol_size` is larger than [`MAX_SYMBOL_SIZE`][super::MAX_SYMBOL_SIZE].
pub(super) fn get_transmission_info(symbol_size: usize) -> ObjectTransmissionInformation {
    ObjectTransmissionInformation::new(
        0,
        symbol_size
            .try_into()
            .expect("`symbol_size` must not be larger than `MAX_SYMBOL_SIZE`"),
        0,
        1,
        1,
    )
}

/// Computes the correct symbol size given the number of source symbols, `n_symbols`, and the
/// data length, `data_length`.
///
/// Returns `None` if `n_symbols == 0` or the computed symbol size is larger than
/// [`MAX_SYMBOL_SIZE`][super::MAX_SYMBOL_SIZE].
#[inline]
pub(super) fn compute_symbol_size(data_length: usize, n_symbols: usize) -> Option<u16> {
    if n_symbols == 0 {
        return None;
    }
    if data_length == 0 {
        return Some(0);
    }
    u16::try_from((data_length - 1) / n_symbols + 1).ok()
}

#[inline]
pub(super) fn packet_to_data(packet: EncodingPacket) -> Vec<u8> {
    packet.split().1
}

#[cfg(test)]
mod tests {
    use walrus_test_utils::param_test;

    use super::*;
    use crate::encoding::MAX_SYMBOL_SIZE;

    param_test! {
        get_transmission_info_succeeds: [
            symbol_size_zero: (0),
            symbol_size_1: (1),
            symbol_size_random: (42),
            max_symbol_size: (MAX_SYMBOL_SIZE),
        ]
    }
    fn get_transmission_info_succeeds(symbol_size: usize) {
        get_transmission_info(symbol_size);
    }

    #[test]
    #[should_panic]
    fn get_transmission_info_panics_above_max_symbol_size() {
        get_transmission_info(MAX_SYMBOL_SIZE + 1);
    }

    param_test! {
        compute_symbol_size_matches_expectation: [
            n_symbols_zero_1: (0, 0, None),
            n_symbols_zero_2: (42, 0, None),
            empty_data_1: (0, 1, Some(0)),
            empty_data_2: (0, 42, Some(0)),
            aligned_data_1: (15, 5, Some(3)),
            aligned_data_2: (13, 13, Some(1)),
            misaligned_data_1: (16, 5, Some(4)),
            misaligned_data_2: (19, 5, Some(4)),
        ]
    }
    fn compute_symbol_size_matches_expectation(
        data_length: usize,
        n_symbols: usize,
        expected_result: Option<u16>,
    ) {
        assert_eq!(compute_symbol_size(data_length, n_symbols), expected_result);
    }
}
