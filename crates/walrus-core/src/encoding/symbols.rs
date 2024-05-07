// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The representation on encoded symbols.

use core::{
    fmt::Display,
    marker::PhantomData,
    num::NonZeroU16,
    ops::{Index, IndexMut, Range},
    slice::{Chunks, ChunksMut},
};

use raptorq::{EncodingPacket, PayloadId};
use serde::{Deserialize, Serialize};

use super::{
    errors::SymbolVerificationError,
    EncodingAxis,
    EncodingConfig,
    Primary,
    Secondary,
    WrongSymbolSizeError,
};
use crate::{
    merkle::{MerkleAuth, Node},
    metadata::BlobMetadata,
    utils,
    SliverIndex,
};

/// A set of encoded symbols.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Symbols {
    /// The encoded symbols.
    // INV: The length of this vector is a multiple of `symbol_size`.
    data: Vec<u8>,
    /// The number of bytes for each symbol.
    symbol_size: NonZeroU16,
}

impl Symbols {
    /// Creates a new [`Symbols`] struct by taking ownership of a vector.
    ///
    /// # Panics
    ///
    /// Panics if the `data` does not contain complete symbols, i.e., if
    /// `data.len() % symbol_size != 0`.
    pub fn new(data: Vec<u8>, symbol_size: NonZeroU16) -> Self {
        assert!(
            data.len() % usize::from(symbol_size.get()) == 0,
            "the provided data must contain complete symbols"
        );
        Symbols { data, symbol_size }
    }

    /// Shortens the `data` in [`Symbols`], keeping the first `len` symbols and dropping the
    /// rest. If `len` is greater or equal to the [`Symbols`]’ current number of symbols, this has
    /// no effect.
    pub fn truncate(&mut self, len: usize) {
        self.data
            .truncate(len * usize::from(self.symbol_size.get()));
    }

    /// Creates a new [`Symbols`] struct with zeroed-out data of length `n_symbols * symbol_size`.
    pub fn zeros(n_symbols: usize, symbol_size: NonZeroU16) -> Self {
        Symbols {
            data: vec![0; n_symbols * usize::from(symbol_size.get())],
            symbol_size,
        }
    }

    /// Creates a new empty [`Symbols`] struct with an internal vector of provided capacity.
    ///
    /// # Examples
    ///
    /// ```
    /// # use walrus_core::encoding::Symbols;
    /// #
    /// assert!(Symbols::with_capacity(42, 1.try_into().unwrap()).is_empty());
    /// ```
    pub fn with_capacity(capacity: usize, symbol_size: NonZeroU16) -> Self {
        Symbols {
            data: Vec::<u8>::with_capacity(capacity),
            symbol_size,
        }
    }

    /// Creates a new [`Symbols`] struct copying the provided slice of bytes.
    ///
    /// # Panics
    ///
    /// Panics if the slice does not contain complete symbols, i.e., if
    /// `slice.len() % symbol_size != 0`.
    pub fn from_slice(slice: &[u8], symbol_size: NonZeroU16) -> Self {
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
        self.data.is_empty()
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
    pub fn decoding_symbol_at<T: EncodingAxis>(
        &self,
        data_index: usize,
        symbol_index: u16,
    ) -> Option<DecodingSymbol<T>> {
        Some(DecodingSymbol::<T>::new(
            symbol_index,
            self[data_index].into(),
        ))
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
    pub fn to_decoding_symbols<T: EncodingAxis>(
        &self,
    ) -> Option<impl Iterator<Item = DecodingSymbol<T>> + '_> {
        let _ = u32::try_from(self.len()).ok()?;
        Some(self.to_symbols().enumerate().map(|(i, s)| {
            DecodingSymbol::new(i.try_into().expect("checked limit above"), s.into())
        }))
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
    pub fn symbol_size(&self) -> NonZeroU16 {
        self.symbol_size
    }

    /// Returns the `symbol_size` as a `usize`.
    #[inline]
    pub fn symbol_usize(&self) -> usize {
        self.symbol_size.get().into()
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

    /// Returns the range of the underlying byte vector that contains the symbols in the range.
    #[inline]
    pub fn symbol_range(&self, range: Range<usize>) -> Range<usize> {
        self.symbol_usize() * range.start..self.symbol_usize() * range.end
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
        &self.data[self.symbol_range(index..index + 1)]
    }
}

impl IndexMut<usize> for Symbols {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        let range = self.symbol_range(index..index + 1);
        &mut self.data[range]
    }
}

impl Index<Range<usize>> for Symbols {
    type Output = [u8];

    fn index(&self, index: Range<usize>) -> &Self::Output {
        &self.data[self.symbol_range(index)]
    }
}

impl IndexMut<Range<usize>> for Symbols {
    fn index_mut(&mut self, index: Range<usize>) -> &mut Self::Output {
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
///
/// The type parameter `T` represents the [`EncodingAxis`] of the sliver that can be recovered from
/// this symbol.  I.e., a [`DecodingSymbol<Primary>`] is used to recover a
/// [`Sliver<Primary>`][super::slivers::Sliver<Primary>].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecodingSymbol<T: EncodingAxis> {
    /// The index of the symbol.
    ///
    /// This is equal to the ESI as defined in [RFC 6330][rfc6330s5.3.1].
    ///
    /// [rfc6330s5.3.1]: https://datatracker.ietf.org/doc/html/rfc6330#section-5.3.1
    pub index: u16,
    /// The symbol data as a byte vector.
    pub data: Vec<u8>,
    /// Marker representing whether this symbol is used to decode primary or secondary slivers.
    _axis: PhantomData<T>,
}

impl<T: EncodingAxis> DecodingSymbol<T> {
    /// Converts the `DecodingSymbol` to an [`EncodingPacket`] expected by the [`raptorq::Decoder`].
    pub fn into_encoding_packet(self) -> EncodingPacket {
        EncodingPacket::new(PayloadId::new(0, self.index.into()), self.data)
    }

    /// Returns the symbol size in bytes.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns true iff the symbol size is 0.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl<T: EncodingAxis> DecodingSymbol<T> {
    /// Creates a new `DecodingSymbol`.
    pub fn new(index: u16, data: Vec<u8>) -> Self {
        Self {
            index,
            data,
            _axis: PhantomData,
        }
    }

    /// Adds a Merkle proof to the [`DecodingSymbol`], converting it into a [`RecoverySymbol`].
    ///
    /// This method consumes the original [`DecodingSymbol<T>`], and returns a
    /// [`RecoverySymbol<T, U>`].
    pub fn with_proof<U: MerkleAuth>(self, proof: U) -> RecoverySymbol<T, U> {
        RecoverySymbol {
            symbol: self,
            proof,
        }
    }
}

impl<T: EncodingAxis> Display for DecodingSymbol<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "DecodingSymbol{{ type: {}, index: {}, {} }}",
            T::NAME,
            self.index,
            utils::data_prefix_string(&self.data, 5),
        )
    }
}

/// A symbol for sliver recovery.
///
/// This wraps a [`DecodingSymbol`] with a Merkle proof for verification.
///
/// The type parameter `U` represents the type of the Merkle proof associated with the symbol.
#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
#[serde(bound(
    deserialize = "for<'a> DecodingSymbol<T>: Deserialize<'a>, for<'a> U: Deserialize<'a>",
    serialize = "DecodingSymbol<T>: Serialize, U: Serialize"
))]
pub struct RecoverySymbol<T: EncodingAxis, U: MerkleAuth> {
    /// The decoding symbol.
    symbol: DecodingSymbol<T>,
    /// A proof that the decoding symbol is correctly computed from a valid orthogonal sliver.
    proof: U,
}

/// A primary decoding symbol to recover a [`PrimarySliver`][super::PrimarySliver].
pub type PrimaryRecoverySymbol<U> = RecoverySymbol<Primary, U>;

/// A secondary decoding symbol to recover a [`SecondarySliver`][super::SecondarySliver].
pub type SecondaryRecoverySymbol<U> = RecoverySymbol<Secondary, U>;

impl<T: EncodingAxis, U: MerkleAuth> RecoverySymbol<T, U> {
    /// Verifies that the decoding symbol belongs to a committed sliver by checking the Merkle proof
    /// against the `root` hash stored.
    pub fn verify_proof(&self, root: &Node, target_index: usize) -> bool {
        self.proof
            .verify_proof(root, &self.symbol.data, target_index)
    }

    /// Verifies that the decoding symbol belongs to a committed sliver by checking the Merkle proof
    /// against the root hash in the provided [`BlobMetadata`].
    ///
    /// The symbol's index is the index of the *source* sliver from which is was created. If the
    /// index is out of range or any other check fails, this returns `false`.
    ///
    /// Returns `Ok(())` if the verification succeeds, a [`SymbolVerificationError`] otherwise.
    pub fn verify(
        &self,
        metadata: &BlobMetadata,
        encoding_config: &EncodingConfig,
        target_index: SliverIndex,
    ) -> Result<(), SymbolVerificationError> {
        let n_shards = encoding_config.n_shards();
        if self.symbol.index >= n_shards.get() {
            return Err(SymbolVerificationError::IndexTooLarge);
        }
        if !metadata
            .symbol_size(encoding_config)
            .is_ok_and(|s| self.symbol.len() == usize::from(s.get()))
        {
            return Err(SymbolVerificationError::SymbolSizeMismatch);
        }
        if self.verify_proof(
            metadata
                .get_sliver_hash(
                    SliverIndex(self.symbol.index).to_pair_index::<T::OrthogonalAxis>(n_shards),
                    T::OrthogonalAxis::sliver_type(),
                )
                .ok_or(SymbolVerificationError::InvalidMetadata)?,
            target_index.get().into(),
        ) {
            Ok(())
        } else {
            Err(SymbolVerificationError::InvalidProof)
        }
    }

    /// Consumes the [`RecoverySymbol<T, U>`], removing the proof, and returns the
    /// [`DecodingSymbol<T>`] without the proof.
    pub fn into_decoding_symbol(self) -> DecodingSymbol<T> {
        self.symbol
    }
}

impl<T: EncodingAxis, U: MerkleAuth> Display for RecoverySymbol<T, U> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "RecoverySymbol{{ type: {}, index: {}, proof_type: {}, {} }}",
            T::NAME,
            self.symbol.index,
            core::any::type_name::<U>(),
            utils::data_prefix_string(&self.symbol.data, 5),
        )
    }
}

/// A pair of recovery symbols to recover a sliver pair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(deserialize = "for<'a> U: Deserialize<'a>"))]
pub struct RecoverySymbolPair<U: MerkleAuth> {
    /// Symbol to recover the primary sliver.
    pub primary: PrimaryRecoverySymbol<U>,
    /// Symbol to recover the secondary sliver.
    pub secondary: SecondaryRecoverySymbol<U>,
}

/// Filters an iterator of [`DecodingSymbol`s][Self], dropping and logging any that
/// don't have a valid Merkle proof.
pub(crate) fn filter_recovery_symbols_and_log_invalid<'a, T, I, V>(
    recovery_symbols: I,
    metadata: &'a BlobMetadata,
    encoding_config: &'a EncodingConfig,
    target_index: SliverIndex,
) -> impl Iterator<Item = RecoverySymbol<T, V>> + 'a
where
    T: EncodingAxis,
    I: IntoIterator,
    I::IntoIter: Iterator<Item = RecoverySymbol<T, V>> + 'a,
    V: MerkleAuth,
{
    recovery_symbols
        .into_iter()
        .filter(move |symbol: &RecoverySymbol<T, V>| {
            symbol
                .verify(metadata, encoding_config, target_index)
                .map_err(|error| {
                    tracing::warn!(
                        %error,
                        %symbol,
                        %target_index,
                        "invalid recovery symbol encountered during sliver recovery",
                    )
                })
                .is_ok()
        })
}

#[cfg(test)]
mod tests {
    use walrus_test_utils::param_test;

    use super::*;
    use crate::test_utils;

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
        assert_eq!(
            Symbols::from_slice(symbols, symbol_size.try_into().unwrap()).get(index),
            target
        )
    }

    #[test]
    fn test_wrong_symbol_size() {
        let mut symbols = Symbols::new(vec![1, 2, 3, 4, 5, 6], 2.try_into().unwrap());
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
        let symbol_size = symbol_size.try_into().unwrap();
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
        let symbol_size = symbol_size.try_into().unwrap();
        let symbols = Symbols::zeros(n_symbols, symbol_size);
        assert_eq!(
            symbols.data.len(),
            n_symbols * usize::from(symbol_size.get())
        );
        assert_eq!(symbols.symbol_size, symbol_size);
    }

    param_test! {
        correct_display_for_decoding_symbol: [
            empty_primary: (
                DecodingSymbol::<Primary>::new(0, vec![]),
                "RecoverySymbol{ type: primary, index: 0, proof_type: \
                    walrus_core::merkle::MerkleProof, data: [] }",
            ),
            empty_secondary: (
                DecodingSymbol::<Secondary>::new(0, vec![]),
                "RecoverySymbol{ type: secondary, index: 0, proof_type: \
                    walrus_core::merkle::MerkleProof, data: [] }",
            ),
            primary_with_short_data: (
                DecodingSymbol::<Primary>::new(0, vec![1, 2, 3, 4, 5]),
                "RecoverySymbol{ type: primary, index: 0, proof_type: \
                    walrus_core::merkle::MerkleProof, data: [1, 2, 3, 4, 5] }",
            ),
            primary_with_long_data: (
                DecodingSymbol::<Primary>::new(3, vec![1, 2, 3, 4, 5, 6]),
                "RecoverySymbol{ type: primary, index: 3, proof_type: \
                    walrus_core::merkle::MerkleProof, data_prefix: [1, 2, 3, 4, 5, ...] }",
            ),
        ]
    }
    fn correct_display_for_decoding_symbol<T: EncodingAxis>(
        symbol: DecodingSymbol<T>,
        expected_display_string: &str,
    ) {
        assert_eq!(
            format!("{}", symbol.with_proof(test_utils::merkle_proof()),),
            expected_display_string
        );
    }
}
