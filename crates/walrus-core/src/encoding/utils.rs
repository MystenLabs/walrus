// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(test)]
use rand::RngCore;
use raptorq::{EncodingPacket, ObjectTransmissionInformation, PayloadId};

use super::{DecodingSymbol, EncodingAxis};

/// Creates a new [`ObjectTransmissionInformation`] for the given `symbol_size`.
///
/// # Panics
///
/// Panics if the `symbol_size` is larger than [`MAX_SYMBOL_SIZE`][super::MAX_SYMBOL_SIZE].
pub fn get_transmission_info(symbol_size: usize) -> ObjectTransmissionInformation {
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
pub fn compute_symbol_size(data_length: usize, n_symbols: usize) -> Option<u16> {
    if n_symbols == 0 {
        return None;
    }
    if data_length == 0 {
        return Some(0);
    }
    u16::try_from((data_length - 1) / n_symbols + 1).ok()
}

#[inline]
pub fn packet_to_data(packet: EncodingPacket) -> Vec<u8> {
    packet.split().1
}

/// This function is necessary to convert from the index to the symbol ID used by the raptorq
/// library.
///
/// It is needed because currently the [raptorq] library currently uses the ISI in the
/// [`PayloadId`]. The two can be converted with the knowledge of the number of source symbols (`K`
/// in the RFC's terminology) and padding symbols (`K' - K` in the RFC's terminology).
// TODO(mlegner): Update if the raptorq library changes its behavior.
pub fn encoding_packet_from_symbol<T: EncodingAxis, U>(
    symbol: DecodingSymbol<T, U>,
    n_source_symbols: u16,
    n_padding_symbols: u16,
) -> EncodingPacket {
    let isi = symbol.index
        + if n_padding_symbols == 0 || symbol.index < n_source_symbols as u32 {
            0
        } else {
            n_padding_symbols as u32
        };
    EncodingPacket::new(PayloadId::new(0, isi), symbol.data)
}

#[cfg(test)]
pub(crate) fn large_random_data(data_length: usize) -> Vec<u8> {
    use rand::{rngs::StdRng, SeedableRng};

    let mut rng = StdRng::seed_from_u64(42);
    let mut result = vec![0u8; data_length];
    rng.fill_bytes(&mut result);
    result
}

#[cfg(test)]
pub(crate) fn get_random_subset<T: Clone>(
    data: impl IntoIterator<Item = T>,
    mut rng: &mut impl RngCore,
    count: usize,
) -> impl Iterator<Item = T> + Clone {
    use rand::seq::SliceRandom;

    let mut data: Vec<_> = data.into_iter().collect();
    data.shuffle(&mut rng);
    data.into_iter().take(count)
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
