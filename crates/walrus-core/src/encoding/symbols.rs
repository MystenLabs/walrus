// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The representation on encoded symbols.

use std::{
    marker::PhantomData,
    ops::{Index, IndexMut, Range},
    slice::{Chunks, ChunksMut},
};

use serde::{Deserialize, Serialize};

use super::{EncodingAxis, Primary, Secondary, WrongSymbolSizeError};

/// A set of encoded symbols.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Symbols {
    /// The encoded symbols.
    data: Vec<u8>,
    /// The number of bytes for each symbol.
    symbol_size: u16,
}

impl Symbols {
    /// Creates a new `Symbols` struct by taking ownership of a vector.
    ///
    /// # Panics
    ///
    /// Panics if the slice does not contain complete symbols, i.e., if
    /// `slice.len() % symbol_size != 0` or if `symbol_size == 0`.
    pub fn new(symbols: Vec<u8>, symbol_size: u16) -> Self {
        assert!(
            symbols.len() % symbol_size as usize == 0,
            "the slice must contain complete symbols"
        );
        Symbols {
            data: symbols,
            symbol_size,
        }
    }

    /// Creates a new `Symbols` struct with zeroed-out data of specified length.
    ///
    /// # Arguments
    ///
    /// * `n_symbols` - The number of (empty) symbols to create.
    /// * `symbol_size` - The size of each symbol.
    ///
    /// # Panics
    ///
    /// Panics if `symbol_size == 0`.
    pub fn zeros(n_symbols: usize, symbol_size: u16) -> Self {
        assert!(symbol_size != 0);
        Symbols {
            data: vec![0; n_symbols * symbol_size as usize],
            symbol_size,
        }
    }

    /// Creates a new `Symbols` struct copying the provided slice of bytes.
    ///
    /// # Panics
    ///
    /// Panics if the slice does not contain complete symbols, i.e., if
    /// `slice.len() % symbol_size != 0` or if `symbol_size == 0`.
    pub fn from_slice(slice: &[u8], symbol_size: u16) -> Self {
        Self::new(slice.into(), symbol_size)
    }

    /// The number of symbols.
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len() / self.symbol_usize()
    }

    /// True iff it does not contain any symbols.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Obtain a reference to the symbol at `index`.
    ///
    /// Returns an `Option` with a reference to the symbol at the `index`, if `index` is within the
    /// set of symbols, `None` otherwise.
    #[inline]
    pub fn get(&self, index: usize) -> Option<&[u8]> {
        if index >= self.len() {
            None
        } else {
            Some(&self[index])
        }
    }

    /// Obtain a mutable reference to the symbol at `index`.
    ///
    /// Returns an `Option` with a mutable reference to the symbol at the `index`, if `index` is
    /// within the set of symbols, `None` otherwise.
    pub fn get_mut(&mut self, index: usize) -> Option<&mut [u8]> {
        if index >= self.len() {
            None
        } else {
            Some(&mut self[index])
        }
    }

    /// Returns a [`DecodingSymbol`] at the provided index.
    ///
    /// Returns `None` if the `index` is out of bounds.
    #[inline]
    pub fn decoding_symbol_at(
        &self,
        data_index: usize,
        symbol_index: u32,
    ) -> Option<DecodingSymbol> {
        Some(DecodingSymbol {
            index: symbol_index,
            data: self[data_index].into(),
        })
    }

    /// Returns an iterator of references to symbols.
    #[inline]
    pub fn to_symbols(&self) -> Chunks<'_, u8> {
        self.data.chunks(self.symbol_usize())
    }

    /// Returns an iterator of mutable references to symbols.
    #[inline]
    pub fn to_symbols_mut(&mut self) -> ChunksMut<'_, u8> {
        let symbol_size = self.symbol_usize();
        self.data.chunks_mut(symbol_size)
    }

    /// Returns an iterator of [`DecodingSymbol`s][DecodingSymbol].
    ///
    /// The `index` of each resulting [`DecodingSymbol`] is its position in this object's data.
    ///
    /// Returns `None` if the length of [`self.data`][Self::data] is larger than
    /// `u32::MAX * self.symbol_size`.
    pub fn to_decoding_symbols(&self) -> Option<impl Iterator<Item = DecodingSymbol> + '_> {
        if self.len() > u32::MAX as usize {
            None
        } else {
            Some(self.to_symbols().enumerate().map(|(i, s)| DecodingSymbol {
                index: i.try_into().expect("checked limit above"),
                data: s.into(),
            }))
        }
    }

    /// Add one or more symbols to the collection.
    ///
    /// # Errors
    ///
    /// Returns a [`WrongSymbolSizeError`] error if the provided symbols do not match the
    /// `symbol_size` of the struct.
    #[inline]
    pub fn extend(&mut self, symbols: &[u8]) -> Result<(), WrongSymbolSizeError> {
        if symbols.len() % self.symbol_usize() != 0 {
            return Err(WrongSymbolSizeError);
        }
        self.data.extend(symbols);
        Ok(())
    }

    /// Returns the `symbol_size`.
    #[inline]
    pub fn symbol_size(&self) -> u16 {
        self.symbol_size
    }

    /// Returns the `symbol_size` as a `usize`.
    #[inline]
    pub fn symbol_usize(&self) -> usize {
        self.symbol_size.into()
    }

    /// Returns a reference to the inner vector of `data` representing the symbols.
    #[inline]
    pub fn data(&self) -> &Vec<u8> {
        &self.data
    }

    /// Returns a mutable reference to the inner vector of `data` representing the symbols.
    #[inline]
    pub fn data_mut(&mut self) -> &mut Vec<u8> {
        &mut self.data
    }

    /// Returns the range of the underlying byte vector that contains symbol at index `index`.
    #[inline]
    pub fn symbol_range(&self, index: usize) -> Range<usize> {
        self.symbol_usize() * index..self.symbol_usize() * (index + 1)
    }

    /// Returns the underlying byte vector as an owned object.
    #[inline]
    pub fn into_vec(self) -> Vec<u8> {
        self.data
    }
}

impl Index<usize> for Symbols {
    type Output = [u8];

    fn index(&self, index: usize) -> &Self::Output {
        &self.data[self.symbol_range(index)]
    }
}

impl IndexMut<usize> for Symbols {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        let range = self.symbol_range(index);
        &mut self.data[range]
    }
}

impl AsRef<[u8]> for Symbols {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl AsMut<[u8]> for Symbols {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

/// A single symbol used for decoding, consisting of the data and the symbol's index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodingSymbol {
    /// The index of the symbol.
    ///
    /// This is equal to the ESI as defined in [RFC 6330][rfc6330s5.3.1].
    ///
    /// [rfc6330s5.3.1]: https://datatracker.ietf.org/doc/html/rfc6330#section-5.3.1
    pub index: u32,
    /// The symbol data as a byte vector.
    pub data: Vec<u8>,
}

/// A recovery symbol to recover a single sliver.
///
/// The generic argument specifies the type of the sliver to be recovered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverySymbol<T: EncodingAxis> {
    _symbol_type: PhantomData<T>,
    /// The symbol data and index.
    pub symbol: DecodingSymbol,
}

impl<T: EncodingAxis> RecoverySymbol<T> {
    /// Creates a new recovery symbol.
    pub fn new(symbol: DecodingSymbol) -> Self {
        Self {
            _symbol_type: PhantomData,
            symbol,
        }
    }
}

/// A pair of recovery symbols to recover a sliver pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverySymbolPair {
    /// Symbol to recover the primary sliver.
    pub primary: RecoverySymbol<Primary>,
    /// Symbol to recover the secondary sliver.
    pub secondary: RecoverySymbol<Secondary>,
}

#[cfg(test)]
mod tests {
    use walrus_test_utils::param_test;

    use super::*;

    param_test! {
        get_correct_symbol: [
            non_empty_1: (&[1, 2, 3] , 1, 1, Some(&[2])),
            non_empty_2: (&[1, 2, 3, 4] , 2, 1, Some(&[3, 4])),
            out_of_bounds_1: (&[1, 2, 3], 1, 3, None),
            out_of_bounds_2: (&[1, 2, 3, 4], 2, 10, None),
            empty_1: (&[], 1, 0, None),
            empty_2: (&[], 2, 0, None),
            empty_3: (&[], 2, 1, None),
        ]
    }
    fn get_correct_symbol(symbols: &[u8], symbol_size: u16, index: usize, target: Option<&[u8]>) {
        assert_eq!(Symbols::from_slice(symbols, symbol_size).get(index), target)
    }

    #[test]
    fn test_wrong_symbol_size() {
        let mut symbols = Symbols::new(vec![1, 2, 3, 4, 5, 6], 2);
        assert_eq!(symbols.extend(&[1]), Err(WrongSymbolSizeError));
    }

    param_test! {
        correct_symbols_from_slice: [
            empty_1: (&[], 1),
            empty_2: (&[], 2),
            #[should_panic] empty_panic: (&[], 0),
            non_empty_1: (&[1,2,3,4,5,6], 2),
            non_empty_2: (&[1,2,3,4,5,6], 3),
            #[should_panic] non_empty_panic_1: (&[1,2,3,4,5,6], 4),
        ]
    }
    fn correct_symbols_from_slice(slice: &[u8], symbol_size: u16) {
        let symbols = Symbols::from_slice(slice, symbol_size);
        assert_eq!(symbols.data, slice.to_vec());
        assert_eq!(symbols.symbol_size, symbol_size);
    }

    param_test! {
        correct_symbols_new_empty: [
            #[should_panic] symbol_size_zero_1: (10,0),
            #[should_panic] symbol_size_zero_2: (0,0),
            init_1: (10, 3),
            init_2: (0, 3),
        ]
    }
    fn correct_symbols_new_empty(n_symbols: usize, symbol_size: u16) {
        let symbols = Symbols::zeros(n_symbols, symbol_size);
        assert_eq!(symbols.data.len(), n_symbols * symbol_size as usize);
        assert_eq!(symbols.symbol_size, symbol_size);
    }
}
