// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use alloc::vec::Vec;
use core::num::NonZeroU16;

use super::{DecodingSymbol, EncodingAxis, EncodingConfigTrait};

pub mod raptorq;
pub mod reed_solomon;

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
