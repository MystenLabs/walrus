// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::num::{NonZeroU16, NonZeroU32};

use raptorq::{EncodingPacket, ObjectTransmissionInformation};

use super::InvalidDataSizeError;

/// Creates a new [`ObjectTransmissionInformation`] for the given `symbol_size`.
pub fn get_transmission_info(symbol_size: NonZeroU16) -> ObjectTransmissionInformation {
    ObjectTransmissionInformation::new(0, symbol_size.get(), 0, 1, 1)
}

/// Computes the correct symbol size given the number of source symbols, `n_symbols`, and the data
/// length, `data_length`.
#[inline]
pub fn compute_symbol_size(
    data_length: u64,
    n_symbols: NonZeroU32,
) -> Result<NonZeroU16, InvalidDataSizeError> {
    if data_length == 0 {
        return Err(InvalidDataSizeError::EmptyData);
    }
    u16::try_from((data_length - 1) / u64::from(n_symbols.get()) + 1)
        .ok()
        .and_then(NonZeroU16::new)
        .ok_or(InvalidDataSizeError::DataTooLarge)
}

/// Computes the correct symbol size given the number of source symbols, `n_symbols`, and the data
/// length, `data_length`.
#[inline]
pub fn compute_symbol_size_from_usize(
    data_length: usize,
    n_symbols: NonZeroU32,
) -> Result<NonZeroU16, InvalidDataSizeError> {
    compute_symbol_size(
        data_length
            .try_into()
            .map_err(|_| InvalidDataSizeError::DataTooLarge)?,
        n_symbols,
    )
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
            empty_data_1: (0, 1, Err(InvalidDataSizeError::EmptyData)),
            empty_data_2: (0, 42, Err(InvalidDataSizeError::EmptyData)),
            aligned_data_1: (15, 5, Ok(3)),
            aligned_data_2: (13, 13, Ok(1)),
            misaligned_data_1: (16, 5, Ok(4)),
            misaligned_data_2: (19, 5, Ok(4)),
        ]
    }
    fn compute_symbol_size_matches_expectation(
        data_length: u64,
        n_symbols: u32,
        expected_result: Result<u16, InvalidDataSizeError>,
    ) {
        assert_eq!(
            compute_symbol_size(data_length, NonZeroU32::new(n_symbols).unwrap()),
            expected_result.map(|r| NonZeroU16::new(r).unwrap())
        );
    }
}
