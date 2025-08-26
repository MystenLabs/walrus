// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use alloc::vec::Vec;
use core::{fmt, num::NonZeroU16};

use reed_solomon_simd;
use tracing::Level;

use super::{DecodingSymbol, EncodingAxis, EncodingConfigTrait};
use crate::{
    EncodingType,
    encoding::{EncodeError, InvalidDataSizeError, ReedSolomonEncodingConfig, utils},
};

/// The key of blob attribute, used to identify the type of the blob.
pub const BLOB_TYPE_ATTRIBUTE_KEY: &str = "_walrusBlobType";

/// The type attribute value for quilt blobs.
pub const QUILT_TYPE_VALUE: &str = "quilt";

/// Trait implemented for all basic (1D) decoders.
pub trait Decoder {
    /// The type of the associated encoding configuration.
    type Config: EncodingConfigTrait;

    /// Creates a new `Decoder`.
    ///
    /// Assumes that the length of the data to be decoded is the product of `n_source_symbols` and
    /// `symbol_size`.
    fn new(n_source_symbols: NonZeroU16, n_shards: NonZeroU16, symbol_size: NonZeroU16) -> Self;

    /// Attempts to decode the source data from the provided iterator over
    /// [`DecodingSymbol`s][DecodingSymbol].
    ///
    /// Returns the source data as a byte vector if decoding succeeds or `None` if decoding fails.
    ///
    /// If decoding failed due to an insufficient number of provided symbols, it can be continued
    /// by additional calls to [`decode`][Self::decode] providing more symbols.
    fn decode<T, U>(&mut self, symbols: T) -> Option<Vec<u8>>
    where
        T: IntoIterator,
        T::IntoIter: Iterator<Item = DecodingSymbol<U>>,
        U: EncodingAxis;
}

/// Wrapper to perform a single encoding with Reed-Solomon for the provided parameters.
pub struct ReedSolomonEncoder {
    encoder: reed_solomon_simd::ReedSolomonEncoder,
    n_source_symbols: NonZeroU16,
    // INV: n_source_symbols <= n_shards.
    n_shards: NonZeroU16,
    symbol_size: NonZeroU16,
    source_symbols: Vec<Vec<u8>>,
}

impl fmt::Debug for ReedSolomonEncoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RSEncoder")
            .field("n_source_symbols", &self.n_source_symbols)
            .field("n_shards", &self.n_shards)
            .field("symbol_size", &self.symbol_size)
            .finish()
    }
}

impl ReedSolomonEncoder {
    const ASSOCIATED_ENCODING_TYPE: EncodingType = EncodingType::RS2;

    /// Creates a new `Encoder` for the provided `data` with the specified arguments.
    ///
    /// The length of the `data` to encode _must_ be a multiple of `n_source_symbols` -- the number
    /// of source symbols in which the data should be split -- and must not be empty.
    ///
    /// `n_shards` is the total number of shards for which symbols should be generated.
    ///
    /// # Errors
    ///
    /// Returns an [`EncodeError`] if the `data` is empty, not a multiple of `n_source_symbols`, or
    /// too large to be encoded with the provided `n_source_symbols`.
    #[tracing::instrument(
        level = Level::ERROR,
        err(level = Level::WARN),
        skip(data)
    )]
    pub fn new(
        data: &[u8],
        n_source_symbols: NonZeroU16,
        n_shards: NonZeroU16,
    ) -> Result<Self, EncodeError> {
        tracing::trace!("creating a new Encoder");
        if data.is_empty() {
            return Err(InvalidDataSizeError::EmptyData.into());
        }
        let symbol_size = utils::compute_symbol_size_from_usize(
            data.len(),
            n_source_symbols.into(),
            Self::ASSOCIATED_ENCODING_TYPE.required_alignment(),
        )
        .map_err(InvalidDataSizeError::from)?;
        if data.len() != usize::from(n_source_symbols.get()) * usize::from(symbol_size.get()) {
            return Err(EncodeError::MisalignedData(n_source_symbols));
        }
        let source_symbols: Vec<Vec<u8>> = data
            .chunks(symbol_size.get().into())
            .map(Vec::from)
            .collect();
        assert_eq!(source_symbols.len(), usize::from(n_source_symbols.get()));

        let mut encoder = reed_solomon_simd::ReedSolomonEncoder::new(
            n_source_symbols.get().into(),
            (n_shards.get() - n_source_symbols.get()).into(),
            symbol_size.get().into(),
        )?;

        for symbol in &source_symbols {
            encoder.add_original_shard(symbol)?;
        }

        Ok(Self {
            encoder,
            n_source_symbols,
            n_shards,
            symbol_size,
            source_symbols,
        })
    }

    /// Gets the symbol size of this encoder.
    #[inline]
    pub fn symbol_size(&self) -> NonZeroU16 {
        self.symbol_size
    }

    /// Returns an iterator over all `n_shards` source and repair symbols.
    // TODO (WAL-611): Return an iterator over all symbols.
    pub fn encode_all(&mut self) -> Vec<Vec<u8>> {
        tracing::trace!("encoding all symbols");
        self.source_symbols
            .iter()
            .cloned()
            .chain(
                self.encoder
                    .encode()
                    .expect("we have added all source symbols, so this cannot fail")
                    .recovery_iter()
                    .map(Vec::from),
            )
            .collect()
    }

    /// Returns an iterator over all `n_shards - self.n_source_symbols` repair symbols.
    pub fn encode_all_repair_symbols(&mut self) -> Vec<Vec<u8>> {
        tracing::trace!("encoding all repair symbols");
        self.encoder
            .encode()
            .expect("we have added all source symbols, so this cannot fail")
            .recovery_iter()
            .map(Vec::from)
            .collect()
    }
}

/// Wrapper to perform a single decoding with Reed-Solomon for the provided parameters.
pub struct ReedSolomonDecoder {
    decoder: reed_solomon_simd::ReedSolomonDecoder,
    n_source_symbols: NonZeroU16,
    symbol_size: NonZeroU16,
    source_symbols: Vec<Vec<u8>>,
}

impl fmt::Debug for ReedSolomonDecoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RSDecoder")
            .field("symbol_size", &self.symbol_size)
            .finish()
    }
}

impl Decoder for ReedSolomonDecoder {
    type Config = ReedSolomonEncodingConfig;

    #[tracing::instrument]
    fn new(n_source_symbols: NonZeroU16, n_shards: NonZeroU16, symbol_size: NonZeroU16) -> Self {
        tracing::trace!("creating a new Decoder");
        Self {
            decoder: reed_solomon_simd::ReedSolomonDecoder::new(
                n_source_symbols.get().into(),
                (n_shards.get() - n_source_symbols.get()).into(),
                symbol_size.get().into(),
            )
            // TODO (WAL-621): Replace this by an error.
            .expect("parameters must be consistent with the Reed-Solomon decoder"),
            n_source_symbols,
            symbol_size,
            source_symbols: alloc::vec![alloc::vec![]; n_source_symbols.get().into()],
        }
    }

    fn decode<T, U>(&mut self, symbols: T) -> Option<Vec<u8>>
    where
        T: IntoIterator,
        T::IntoIter: Iterator<Item = DecodingSymbol<U>>,
        U: EncodingAxis,
    {
        let decoder = &mut self.decoder;
        for symbol in symbols.into_iter() {
            if symbol.index < self.n_source_symbols.get() {
                self.source_symbols[usize::from(symbol.index)] = symbol.data.clone();
                let _ = decoder.add_original_shard(symbol.index.into(), symbol.data);
            } else {
                let _ = decoder.add_recovery_shard(
                    usize::from(symbol.index - self.n_source_symbols.get()),
                    symbol.data,
                );
            }
        }
        let result = decoder.decode().ok()?;
        for (index, symbol) in result.restored_original_iter() {
            self.source_symbols[index] = symbol.to_vec();
        }

        Some(
            (0..self.n_source_symbols.get())
                .flat_map(|i| self.source_symbols[usize::from(i)].clone())
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use alloc::vec;
    use core::ops::Range;

    use walrus_test_utils::{Result, param_test};

    use super::*;
    use crate::encoding::{InvalidDataSizeError, Primary};

    #[test]
    fn encoding_empty_data_fails() {
        assert!(matches!(
            ReedSolomonEncoder::new(
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
            ReedSolomonEncoder::new(&[1, 2], n_source_symbols, NonZeroU16::new(314).unwrap()),
            Err(EncodeError::MisalignedData(_n_source_symbols))
        ));
    }

    param_test! {
        test_encode_decode -> Result: [
            aligned_data_source_symbols: (&[1, 2, 3, 4], 2, 0..2, true),
            aligned_data_repair_symbols_1: (&[1, 2], 1, 1..2, true),
            aligned_data_repair_symbols_2: (&[1, 2, 3, 4, 5, 6, 7, 8], 2, 2..4, true),
            aligned_data_repair_symbols_3: (&[1, 2, 3, 4, 5, 6], 3, 3..6, true),
            aligned_large_data_repair_symbols: (
                &walrus_test_utils::random_data(42000), 100, 100..200, true
            ),
            aligned_data_too_few_symbols_1: (&[1, 2, 3, 4], 2, 2..3, false),
            aligned_data_too_few_symbols_2: (&[1, 2, 3, 4, 5, 6], 3, 1..3, false),
        ]
    }
    fn test_encode_decode(
        data: &[u8],
        n_source_symbols: u16,
        encoded_symbols_range: Range<u16>,
        should_succeed: bool,
    ) -> Result {
        let n_source_symbols = NonZeroU16::new(n_source_symbols).unwrap();
        let start = encoded_symbols_range.start;
        let end = encoded_symbols_range.end;
        let n_shards = end.max(n_source_symbols.get() + 1).try_into().unwrap();

        let mut encoder = ReedSolomonEncoder::new(data, n_source_symbols, n_shards)?;
        let encoded_symbols = encoder
            .encode_all()
            .into_iter()
            .skip(start.into())
            .take((end - start).into())
            .enumerate()
            .map(|(i, symbol)| {
                DecodingSymbol::<Primary>::new(u16::try_from(i).unwrap() + start, symbol)
            });
        let mut decoder =
            ReedSolomonDecoder::new(n_source_symbols, n_shards, encoder.symbol_size());
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
        let mut encoder = ReedSolomonEncoder::new(&data, n_source_symbols, n_shards)?;
        let mut encoded_symbols = encoder
            .encode_all_repair_symbols()
            .into_iter()
            .enumerate()
            .map(|(i, symbol)| {
                vec![DecodingSymbol::<Primary>::new(
                    u16::try_from(i).unwrap() + n_source_symbols.get(),
                    symbol,
                )]
            });
        let mut decoder =
            ReedSolomonDecoder::new(n_source_symbols, n_shards, encoder.symbol_size());

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
