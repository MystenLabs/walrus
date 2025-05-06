// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use alloc::vec::Vec;
use core::num::NonZeroU16;

use raptorq::{SourceBlockDecoder, SourceBlockEncoder, SourceBlockEncodingPlan};
use tracing::Level;

use super::Decoder;
use crate::{
    EncodingAxis,
    EncodingType,
    encoding::{DecodingSymbol, EncodeError, InvalidDataSizeError, RaptorQEncodingConfig, utils},
};

/// Wrapper to perform a single encoding with RaptorQ for the provided parameters.
#[derive(Debug)]
pub struct RaptorQEncoder {
    raptorq_encoder: SourceBlockEncoder,
    n_source_symbols: NonZeroU16,
    // INV: n_source_symbols <= n_shards.
    n_shards: NonZeroU16,
    symbol_size: NonZeroU16,
}

impl RaptorQEncoder {
    const ASSOCIATED_ENCODING_TYPE: EncodingType = EncodingType::RedStuffRaptorQ;

    /// Creates a new `Encoder` for the provided `data` with the specified arguments.
    ///
    /// The length of the `data` to encode _must_ be a multiple of `n_source_symbols` -- the number
    /// of source symbols in which the data should be split -- and must not be empty.
    ///
    /// `n_shards` is the total number of shards for which symbols should be generated.
    ///
    /// The `encoding_plan` is a pre-generated [`SourceBlockEncodingPlan`] consistent with
    /// `n_source_symbols`.
    ///
    /// # Errors
    ///
    /// Returns an [`EncodeError`] if the `data` is empty, not a multiple of `n_source_symbols`, or
    /// too large to be encoded with the provided `n_source_symbols`.
    ///
    /// If the `encoding_plan` was generated for a different number of source symbols than
    /// `n_source_symbols`, later methods called on the returned `Encoder` may exhibit unexpected
    /// behavior.
    ///
    /// # Panics
    ///
    /// Panics if `n_source_symbols > n_shards`.
    #[tracing::instrument(
        level = Level::ERROR,
        err(level = Level::WARN),
        skip(data, encoding_plan)
    )]
    pub fn new(
        data: &[u8],
        n_source_symbols: NonZeroU16,
        n_shards: NonZeroU16,
        encoding_plan: &SourceBlockEncodingPlan,
    ) -> Result<Self, EncodeError> {
        tracing::trace!("creating a new Encoder");
        assert!(n_shards >= n_source_symbols);
        if data.is_empty() {
            return Err(InvalidDataSizeError::EmptyData.into());
        }
        if data.len() % usize::from(n_source_symbols.get()) != 0 {
            return Err(EncodeError::MisalignedData(n_source_symbols));
        }
        let symbol_size = utils::compute_symbol_size_from_usize(
            data.len(),
            n_source_symbols.into(),
            Self::ASSOCIATED_ENCODING_TYPE.required_alignment(),
        )
        .map_err(InvalidDataSizeError::from)?;

        Ok(Self {
            raptorq_encoder: SourceBlockEncoder::with_encoding_plan(
                0,
                &utils::get_transmission_info(symbol_size),
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
        n_source_symbols: NonZeroU16,
        n_shards: NonZeroU16,
    ) -> Result<Self, EncodeError> {
        let encoding_plan = SourceBlockEncodingPlan::generate(n_source_symbols.get());
        Self::new(data, n_source_symbols, n_shards, &encoding_plan)
    }

    /// Gets the symbol size of this encoder.
    #[inline]
    pub fn symbol_size(&self) -> NonZeroU16 {
        self.symbol_size
    }

    /// Returns an iterator over all source symbols.
    pub fn source_symbols(&self) -> impl Iterator<Item = Vec<u8>> {
        self.raptorq_encoder
            .source_packets()
            .into_iter()
            .map(utils::packet_to_data)
    }

    /// Returns an iterator over all `n_shards` source and repair symbols.
    pub fn encode_all(&self) -> impl Iterator<Item = Vec<u8>> {
        tracing::trace!("encoding all symbols");
        self.raptorq_encoder
            .source_packets()
            .into_iter()
            .chain(self.raptorq_encoder.repair_packets(
                0,
                (self.n_shards.get() - self.n_source_symbols.get()).into(),
            ))
            .map(utils::packet_to_data)
    }

    /// Returns an iterator over all `n_shards - self.n_source_symbols` repair symbols.
    pub fn encode_all_repair_symbols(&self) -> impl Iterator<Item = Vec<u8>> {
        tracing::trace!("encoding all repair symbols");
        self.raptorq_encoder
            .repair_packets(
                0,
                (self.n_shards.get() - self.n_source_symbols.get()).into(),
            )
            .into_iter()
            .map(utils::packet_to_data)
    }
}

/// Wrapper to perform a single decoding with RaptorQ for the provided parameters.
#[derive(Debug, Clone)]
pub struct RaptorQDecoder {
    raptorq_decoder: SourceBlockDecoder,
    symbol_size: NonZeroU16,
}

impl Decoder for RaptorQDecoder {
    type Config = RaptorQEncodingConfig;

    #[tracing::instrument]
    fn new(n_source_symbols: NonZeroU16, _n_shards: NonZeroU16, symbol_size: NonZeroU16) -> Self {
        tracing::trace!("creating a new Decoder");
        Self {
            raptorq_decoder: SourceBlockDecoder::new(
                0,
                &utils::get_transmission_info(symbol_size),
                u64::from(n_source_symbols.get()) * u64::from(symbol_size.get()),
            ),
            symbol_size,
        }
    }

    fn decode<T, U>(&mut self, symbols: T) -> Option<Vec<u8>>
    where
        T: IntoIterator,
        T::IntoIter: Iterator<Item = DecodingSymbol<U>>,
        U: EncodingAxis,
    {
        let expected_symbol_size: usize = self.symbol_size.get().into();
        let packets = symbols.into_iter().filter_map(|symbol| {
            let actual_symbol_size = symbol.len();
            if actual_symbol_size == expected_symbol_size {
                Some(symbol.into_encoding_packet())
            } else {
                // Drop symbols of incorrect length and log a warning.
                tracing::warn!(
                    %symbol,
                    expected_symbol_size,
                    actual_symbol_size,
                    "input to decoder has incorrect length",
                );
                None
            }
        });
        self.raptorq_decoder.decode(packets)
    }
}

#[cfg(test)]
mod tests {
    use alloc::vec;
    use core::ops::Range;

    use walrus_test_utils::{Result, param_test, random_data};

    use super::*;
    use crate::encoding::{InvalidDataSizeError, Primary};

    #[test]
    fn encoding_empty_data_fails() {
        assert!(matches!(
            RaptorQEncoder::new_with_new_encoding_plan(
                &[],
                NonZeroU16::new(42).unwrap(),
                NonZeroU16::new(314).unwrap()
            ),
            Err(EncodeError::InvalidDataSize(
                InvalidDataSizeError::EmptyData
            ))
        ));
    }

    #[test]
    fn encoding_misaligned_data_fails() {
        let n_source_symbols = NonZeroU16::new(3).unwrap();
        assert!(matches!(
            RaptorQEncoder::new_with_new_encoding_plan(
                &[1, 2],
                n_source_symbols,
                NonZeroU16::new(314).unwrap()
            ),
            Err(EncodeError::MisalignedData(_n_source_symbols))
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
            aligned_data_too_few_symbols_2: (&[1, 2, 3], 3, 1..3, false),
        ]
    }
    fn test_encode_decode(
        data: &[u8],
        n_source_symbols: u16,
        encoded_symbols_range: Range<u16>,
        should_succeed: bool,
    ) -> Result {
        let n_source_symbols = n_source_symbols.try_into().unwrap();
        let start = encoded_symbols_range.start;
        let end = encoded_symbols_range.end;
        let n_shards = end.try_into().unwrap();

        let encoder = RaptorQEncoder::new_with_new_encoding_plan(data, n_source_symbols, n_shards)?;
        let encoded_symbols = encoder
            .encode_all()
            .skip(start.into())
            .enumerate()
            .map(|(i, symbol)| DecodingSymbol::<Primary>::new(i as u16 + start, symbol));
        let mut decoder = RaptorQDecoder::new(n_source_symbols, n_shards, encoder.symbol_size());
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
        let n_source_symbols = NonZeroU16::new(3).unwrap();
        let n_shards = NonZeroU16::new(10).unwrap();
        let data = [1, 2, 3, 4, 5, 6];
        let encoder =
            RaptorQEncoder::new_with_new_encoding_plan(&data, n_source_symbols, n_shards)?;
        let mut encoded_symbols =
            encoder
                .encode_all_repair_symbols()
                .enumerate()
                .map(|(i, symbol)| {
                    vec![DecodingSymbol::<Primary>::new(
                        i as u16 + n_source_symbols.get(),
                        symbol,
                    )]
                });
        let mut decoder = RaptorQDecoder::new(n_source_symbols, n_shards, encoder.symbol_size());

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
