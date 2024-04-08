// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::num::{NonZeroU16, NonZeroUsize};

use raptorq::{EncodingPacket, ObjectTransmissionInformation};

/// Creates a new [`ObjectTransmissionInformation`] for the given `symbol_size`.
pub fn get_transmission_info(symbol_size: NonZeroU16) -> ObjectTransmissionInformation {
    ObjectTransmissionInformation::new(0, symbol_size.get(), 0, 1, 1)
}

/// Computes the correct symbol size given the number of source symbols, `n_symbols`, and the
/// data length, `data_length`.
///
/// Returns `None` if `data_length == 0` or the computed symbol size is larger than
/// [`MAX_SYMBOL_SIZE`][super::MAX_SYMBOL_SIZE].
#[inline]
pub fn compute_symbol_size(data_length: usize, n_symbols: NonZeroUsize) -> Option<NonZeroU16> {
    if data_length == 0 {
        return None;
    }
    NonZeroU16::new(u16::try_from((data_length - 1) / (n_symbols.get()) + 1).ok()?)
}

#[inline]
pub fn packet_to_data(packet: EncodingPacket) -> Vec<u8> {
    packet.split().1
}

#[cfg(test)]
mod tests {
    use walrus_test_utils::param_test;

    use super::*;

    param_test! {
        get_transmission_info_succeeds: [
            symbol_size_1: (1),
            symbol_size_random: (42),
            max_symbol_size: (u16::MAX),
        ]
    }
    fn get_transmission_info_succeeds(symbol_size: u16) {
        get_transmission_info(NonZeroU16::new(symbol_size).unwrap());
    }

    param_test! {
        compute_symbol_size_matches_expectation: [
            empty_data_1: (0, 1, None),
            empty_data_2: (0, 42, None),
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
        assert_eq!(
            compute_symbol_size(data_length, NonZeroUsize::new(n_symbols).unwrap()),
            expected_result.map(|r| NonZeroU16::new(r).unwrap())
        );
    }
}
