// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::ops::Range;

use raptorq::{SourceBlockDecoder, SourceBlockEncoder, SourceBlockEncodingPlan};

use super::{utils, DecodingSymbol, EncodeError, EncodingAxis, MAX_SYMBOL_SIZE};

/// Wrapper to perform a single encoding with RaptorQ for the provided parameters.
pub struct Encoder {
    raptorq_encoder: SourceBlockEncoder,
    n_source_symbols: u16,
    n_shards: u32,
    symbol_size: u16,
}

impl Encoder {
    /// Creates a new `Encoder` for the provided `data` with the specified arguments.
    ///
    /// # Arguments
    ///
    /// * `data` - The data to encode. This *must* be a multiple of `n_source_symbols` and must not
    ///   be empty.
    /// * `n_source_symbols` - The number of source symbols into which the data should be split.
    /// * `n_shards` - The total number of shards for which symbols should be generated.
    /// * `encoding_plan` - A pre-generated [`SourceBlockEncodingPlan`] consistent with
    ///   `n_source_symbols`.
    ///
    /// # Errors
    ///
    /// Returns an [`EncodeError`] if the `data` is empty, not a multiple of `n_source_symbols`, or
    /// too large to be encoded with the provided `n_source_symbols` (this includes the case
    /// `n_source_symbols == 0`).
    ///
    /// If the `encoding_plan` was generated for a different number of source symbols than
    /// `n_source_symbols`, later methods called on the returned `Encoder` may exhibit unexpected
    /// behavior.
    pub fn new(
        data: &[u8],
        n_source_symbols: u16,
        n_shards: u32,
        encoding_plan: &SourceBlockEncodingPlan,
    ) -> Result<Self, EncodeError> {
        if data.is_empty() {
            return Err(EncodeError::EmptyData);
        }
        if data.len() % n_source_symbols as usize != 0 {
            return Err(EncodeError::MisalignedData(n_source_symbols));
        }

        let Some(symbol_size) = utils::compute_symbol_size(data.len(), n_source_symbols.into())
        else {
            return Err(EncodeError::DataTooLarge(
                n_source_symbols as usize * MAX_SYMBOL_SIZE,
            ));
        };

        Ok(Self {
            raptorq_encoder: SourceBlockEncoder::with_encoding_plan(
                0,
                &utils::get_transmission_info(symbol_size.into()),
                data,
                encoding_plan,
            ),
            n_source_symbols,
            n_shards,
            symbol_size,
        })
    }

    /// Creates a new `Encoder` for the provided `data` with the specified arguments.
    ///
    /// This generates an appropriate [`SourceBlockEncodingPlan`] and then calls [`Self::new`].
    ///
    /// See [`Self::new`] for further details.
    pub fn new_with_new_encoding_plan(
        data: &[u8],
        n_source_symbols: u16,
        n_shards: u32,
    ) -> Result<Self, EncodeError> {
        let encoding_plan = SourceBlockEncodingPlan::generate(n_source_symbols);
        Self::new(data, n_source_symbols, n_shards, &encoding_plan)
    }

    /// Gets the symbol size of this encoder.
    #[inline]
    pub fn symbol_size(&self) -> u16 {
        self.symbol_size
    }

    /// Returns an iterator over all source symbols.
    pub fn source_symbols(&self) -> impl Iterator<Item = Vec<u8>> {
        self.raptorq_encoder
            .source_packets()
            .into_iter()
            .map(utils::packet_to_data)
    }

    /// Returns an iterator over a range of source and/or repair symbols.
    pub fn encode_range(&self, range: Range<u32>) -> impl Iterator<Item = Vec<u8>> {
        let repair_end = if range.end > self.n_source_symbols as u32 {
            range.end - self.n_source_symbols as u32
        } else {
            0
        };

        self.raptorq_encoder
            .source_packets()
            .into_iter()
            .chain(self.raptorq_encoder.repair_packets(0, repair_end))
            .skip(range.start as usize)
            .take(range.len())
            .map(utils::packet_to_data)
    }

    /// Returns an iterator over all `n_shards` source and repair symbols.
    pub fn encode_all(&self) -> impl Iterator<Item = Vec<u8>> {
        self.raptorq_encoder
            .source_packets()
            .into_iter()
            .chain(
                self.raptorq_encoder
                    .repair_packets(0, self.n_shards - self.n_source_symbols as u32),
            )
            .map(utils::packet_to_data)
    }

    /// Returns an iterator over all `n_shards - self.n_source_symbols` repair symbols.
    pub fn encode_all_repair_symbols(&self) -> impl Iterator<Item = Vec<u8>> {
        self.raptorq_encoder
            .repair_packets(0, self.n_shards - self.n_source_symbols as u32)
            .into_iter()
            .map(utils::packet_to_data)
    }
}

/// Wrapper to perform a single decoding with RaptorQ for the provided parameters.
#[derive(Debug, Clone)]
pub struct Decoder {
    raptorq_decoder: SourceBlockDecoder,
}

impl Decoder {
    /// Creates a new `Decoder`.
    ///
    /// Assumes that the length of the data to be decoded is the product of `n_source_symbols` and
    /// `symbol_size`.
    pub fn new(n_source_symbols: u16, symbol_size: u16) -> Self {
        Self {
            raptorq_decoder: SourceBlockDecoder::new(
                0,
                &utils::get_transmission_info(symbol_size as usize),
                (n_source_symbols as u64) * (symbol_size as u64),
            ),
        }
    }

    /// Attempts to decode the source data from the provided iterator over
    /// [`DecodingSymbol`s][DecodingSymbol].
    ///
    /// Returns the source data as a byte vector if decoding succeeds or `None` if decoding fails.
    ///
    /// If decoding failed due to an insufficient number of provided symbols, it can be continued
    /// by additional calls to [`decode`][Self::decode] providing more symbols.
    pub fn decode<T, U, V>(&mut self, symbols: T) -> Option<Vec<u8>>
    where
        T: IntoIterator<Item = DecodingSymbol<U, V>>,
        U: EncodingAxis,
    {
        let packets = symbols
            .into_iter()
            .map(DecodingSymbol::<U, V>::into_encoding_packet);
        self.raptorq_decoder.decode(packets)
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use walrus_test_utils::{param_test, random_data, Result};

    use super::*;
    use crate::encoding::Primary;

    #[test]
    fn encoding_empty_data_fails() {
        assert!(matches!(
            Encoder::new_with_new_encoding_plan(&[], 42, 314),
            Err(EncodeError::EmptyData)
        ));
    }

    #[test]
    fn encoding_misaligned_data_fails() {
        assert!(matches!(
            Encoder::new_with_new_encoding_plan(&[1, 2], 3, 314),
            Err(EncodeError::MisalignedData(3))
        ));
    }

    param_test! {
        test_encode_decode -> Result: [
            aligned_data_source_symbols_1: (&[1, 2], 2, 0..2, true),
            aligned_data_source_symbols_2: (&[1, 2, 3, 4], 2, 0..2, true),
            aligned_data_repair_symbols_1: (&[1, 2], 2, 2..4, true),
            aligned_data_repair_symbols_2: (&[1, 2, 3, 4, 5, 6], 2, 2..4, true),
            aligned_data_repair_symbols_3: (&[1, 2, 3, 4, 5, 6], 3, 3..6, true),
            aligned_large_data_repair_symbols: (
                &random_data(42000), 100, 100..200, true
            ),
            aligned_data_too_few_symbols_1: (&[1, 2], 2, 2..3, false),
            aligned_data_too_few_symbols_2: (&[1, 2, 3], 3, 0..2, false),
        ]
    }
    fn test_encode_decode(
        data: &[u8],
        n_source_symbols: u16,
        encoded_symbols_range: Range<u32>,
        should_succeed: bool,
    ) -> Result {
        let start = encoded_symbols_range.start;

        let encoder =
            Encoder::new_with_new_encoding_plan(data, n_source_symbols, encoded_symbols_range.end)?;
        let encoded_symbols = encoder
            .encode_range(encoded_symbols_range)
            .enumerate()
            .map(|(i, symbol)| DecodingSymbol::<Primary>::new(i as u32 + start, symbol));
        let mut decoder = Decoder::new(n_source_symbols, encoder.symbol_size());
        let decoding_result = decoder.decode(encoded_symbols);

        if should_succeed {
            assert_eq!(decoding_result.unwrap(), data);
        } else {
            assert_eq!(decoding_result, None)
        }

        Ok(())
    }

    #[test]
    fn can_decode_in_multiple_steps() -> Result {
        let n_source_symbols = 3;
        let data = [1, 2, 3, 4, 5, 6];
        let encoder = Encoder::new_with_new_encoding_plan(&data, n_source_symbols, 10)?;
        let mut encoded_symbols =
            encoder
                .encode_all_repair_symbols()
                .enumerate()
                .map(|(i, symbol)| {
                    vec![DecodingSymbol::<Primary>::new(
                        i as u32 + n_source_symbols as u32,
                        symbol,
                    )]
                });
        let mut decoder = Decoder::new(n_source_symbols, encoder.symbol_size());

        assert_eq!(
            decoder.decode(encoded_symbols.next().unwrap().clone()),
            None
        );
        assert_eq!(
            decoder.decode(encoded_symbols.next().unwrap().clone()),
            None
        );
        assert_eq!(
            decoder
                .decode(encoded_symbols.next().unwrap().clone())
                .unwrap(),
            data
        );

        Ok(())
    }
}
