// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::marker::PhantomData;

use serde::{Deserialize, Serialize};

use super::{
    get_encoding_config,
    DataTooLargeError,
    Decoder,
    DecodingSymbol,
    EncodeError,
    Encoder,
    EncodingAxis,
    Primary,
    RecoveryError,
    RecoverySymbol,
    RecoverySymbolPair,
    Secondary,
    Symbols,
    MAX_ENCODING_SYMBOL_ID,
};

/// A primary sliver resulting from an encoding of a blob.
pub type PrimarySliver = Sliver<Primary>;

/// A secondary sliver resulting from an encoding of a blob.
pub type SecondarySliver = Sliver<Secondary>;

/// Encoded data corresponding to a single [`EncodingAxis`] assigned to one shard.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Sliver<T: EncodingAxis> {
    /// The encoded data.
    pub symbols: Symbols,
    /// Index of this sliver.
    ///
    /// This is needed for the decoding to be able to identify the encoded symbols.
    pub index: u32,
    _sliver_type: PhantomData<T>,
}

impl<T: EncodingAxis> Sliver<T> {
    /// Creates a new `Sliver` copying the provided slice of bytes.
    ///
    /// # Panics
    ///
    /// Panics if the slice does not contain complete symbols, i.e., if
    /// `slice.len() % symbol_size != 0` or if `symbol_size == 0`.
    pub fn new<U: Into<Vec<u8>>>(data: U, symbol_size: u16, index: u32) -> Self {
        Self {
            symbols: Symbols::new(data.into(), symbol_size),
            index,
            _sliver_type: PhantomData,
        }
    }

    /// Creates a new `Sliver` with empty data of specified length.
    ///
    /// # Panics
    ///
    /// Panics if `symbol_size == 0`.
    pub fn new_empty(length: usize, symbol_size: u16, index: u32) -> Self {
        Self {
            symbols: Symbols::zeros(length, symbol_size),
            index,
            _sliver_type: PhantomData,
        }
    }

    /// Copies the provided symbol to the location specified by the index.
    ///
    /// # Panics
    ///
    /// Panics if `self.data.len() < index * (symbol.len() + 1)` and if the symbol size does not
    /// match the length specified in the [`Symbols`] struct.
    pub fn copy_symbol_to(&mut self, index: usize, symbol: &[u8]) -> &mut Self {
        assert!(symbol.len() == self.symbols.symbol_usize());
        self.symbols[index].copy_from_slice(symbol);
        self
    }

    /// Creates the first `n_shards` recovery symbols from the sliver.
    ///
    /// [`Primary`] slivers are encoded with the [`Secondary`] encoding, and vice versa, to obtain
    /// the fully-expanded set of recovery symbols.
    ///
    /// # Errors
    ///
    /// Returns an [`RecoveryError::EncodeError`] if the `data` cannot be encoded. See
    /// [`Encoder::new`] for further details about the returned errors.
    pub fn recovery_symbols(&self) -> Result<Symbols, RecoveryError> {
        Ok(Symbols::new(
            self.get_sliver_encoder()?.encode_all().flatten().collect(),
            self.symbols.symbol_size(), // The symbol size remains unvaried when re-encoding.
        ))
    }

    /// Creates the bytes of the recovery symbol at the provided `index`.
    ///
    /// # Errors
    ///
    /// Returns an [`RecoveryError::EncodeError`] error if the `data` cannot be encoded. See
    /// [`Encoder::new`] for further details about the returned errors. Returns a
    /// [`RecoveryError::IndexTooLarge`] error if `index > MAX_ENCODING_SYMBOL_ID` (2^24 - 1), as
    /// the symbol ID encoding fails.
    pub fn single_recovery_symbol(&self, index: u32) -> Result<Vec<u8>, RecoveryError> {
        if index > MAX_ENCODING_SYMBOL_ID {
            return Err(RecoveryError::IndexTooLarge);
        }
        Ok(self
            .get_sliver_encoder()?
            // TODO(mlegner): add more efficient function to encode a single symbol
            .encode_range(index..index + 1)
            .next()
            .expect("the encoder should always be able to produce an encoding symbol"))
    }

    /// Gets the recovery symbol for a specific target sliver starting from the current sliver.
    ///
    /// A [`Primary`] sliver is used to reconstruct symbols for a [`Secondary`] sliver, and
    /// vice-versa.
    ///
    /// # Arguments
    ///
    /// * `self_pair_idx` - the index of the [`SliverPair`] to which this sliver belongs.
    /// * `target_pair_idx` - the index of the [`SliverPair`] to which the target sliver belongs.
    ///
    /// # Errors
    ///
    /// Returns a [`RecoveryError::EncodeError`] if the sliver cannot be encoded. See
    /// [`Encoder::new`] for further details about the returned errors.
    ///
    /// # Panics
    ///
    /// Panics if `self_pair_idx` or `target_pair_idx` is larger than `n_shards` in the encoding
    /// config.
    pub fn recovery_symbol_for_sliver(
        &self,
        self_pair_idx: u32,
        target_pair_idx: u32,
    ) -> Result<RecoverySymbol<T::OrthogonalAxis>, RecoveryError> {
        let (self_sliver_idx, other_sliver_idx) =
            Self::relative_sliver_indices(self_pair_idx, target_pair_idx);
        Ok(RecoverySymbol::new(DecodingSymbol {
            index: self_sliver_idx,
            data: self.single_recovery_symbol(other_sliver_idx)?,
        }))
    }

    /// Recovers a [`Sliver`] from the recovery symbols.
    ///
    /// Returns the recovered [`Sliver`] if decoding succeeds or `None` if decoding fails.
    ///
    /// # Errors
    ///
    /// Returns a [`RecoveryError::DataTooLarge`] if the recovery symbols provided are larger than
    /// what the [`Decoder`] can process, i.e., if the `symbol_size` of the `recovery_symbols` is
    /// larger than [`MAX_SYMBOL_SIZE`][super::MAX_SYMBOL_SIZE]. See [`Decoder::new`] for further
    /// details. Returns a [`RecoveryError::InvalidSymbolSizes`] error if the symbols provided have
    /// different symbol sizes or if their symbol size is 0 (they are empty).
    pub fn recover_sliver<I, U>(
        recovery_symbols: U,
        index: u32,
    ) -> Result<Option<Self>, RecoveryError>
    where
        I: Iterator<Item = DecodingSymbol> + Clone,
        U: IntoIterator<Item = DecodingSymbol, IntoIter = I>,
    {
        let recovery_iter = recovery_symbols.into_iter();

        // Check that all the symbols have the same positive size.
        let mut inner_iter = recovery_iter.clone();
        let symbol_size = if let Some(s) = inner_iter.next() {
            s.data.len()
        } else {
            return Ok(None);
        };
        if symbol_size == 0 || inner_iter.any(|s| s.data.len() != symbol_size) {
            return Err(RecoveryError::InvalidSymbolSizes);
        }
        let symbol_size: u16 = symbol_size.try_into().map_err(|_| DataTooLargeError)?;

        // Pass the symbols to the decoder.
        Ok(Decoder::new(Self::n_source_symbols(), symbol_size)
            .decode(recovery_iter)
            .map(|data| Sliver::new(data, symbol_size, index)))
    }

    /// Returns the number of source symbol for the current sliver's [`EncodingAxis`].
    fn n_source_symbols() -> u16 {
        get_encoding_config().n_source_symbols::<T::OrthogonalAxis>()
    }

    /// Creates an encoder for the current sliver.
    fn get_sliver_encoder(&self) -> Result<Encoder, EncodeError> {
        get_encoding_config().get_encoder::<T::OrthogonalAxis>(self.symbols.data())
    }

    /// Returns the sliver indices starting from the sliver pair indices, considering the encoding
    /// axis for the current sliver.
    ///
    /// Concretely, if we are recovering a secondary sliver from a primary sliver, then the `index`
    /// of the decoding symbol is the index of the sliver pair of the source primary sliver
    /// (`self_pair_idx`), while the `data` should be taken from position
    /// `n_shards - other_pair_idx - 1` on the expanded source primary sliver.
    /// The opposite conversion happens when recovering a primary sliver.
    fn relative_sliver_indices(self_pair_idx: u32, other_pair_idx: u32) -> (u32, u32) {
        (
            T::sliver_index_from_pair_index(self_pair_idx),
            T::OrthogonalAxis::sliver_index_from_pair_index(other_pair_idx),
        )
    }
}

/// Combination of a primary and secondary sliver of one shard.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SliverPair {
    /// The sliver corresponding to the [`Primary`] encoding.
    pub primary: Sliver<Primary>,
    /// The sliver corresponding to the [`Secondary`] encoding.
    pub secondary: Sliver<Secondary>,
}

impl SliverPair {
    /// Index of this sliver pair.
    ///
    /// Sliver pair `i` contains the primary sliver `i` and the secondary sliver `n_shards-i-1`.
    pub fn index(&self) -> u32 {
        self.primary.index
    }

    /// Gets the two recovery symbols for a specific target sliver pair starting from the current
    /// sliver pair.
    ///
    /// # Arguments
    ///
    /// * `target_pair_idx` - the index of the target [`SliverPair`] (the one to be recovered).
    ///
    /// # Errors
    ///
    /// Returns a [`RecoveryError::EncodeError`] if any of the the slivers cannot be encoded. See
    /// [`Encoder::new`] for further details about the returned errors.
    ///
    /// # Panics
    ///
    /// Panics if `target_pair_idx` is larger than `n_shards` in the encoding config.
    pub fn recovery_symbols_for_sliver(
        &self,
        target_pair_idx: u32,
    ) -> Result<RecoverySymbolPair, RecoveryError> {
        Ok(RecoverySymbolPair {
            primary: self
                .secondary
                .recovery_symbol_for_sliver(self.index(), target_pair_idx)?,
            secondary: self
                .primary
                .recovery_symbol_for_sliver(self.index(), target_pair_idx)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use rand::{rngs::StdRng, SeedableRng};
    use walrus_test_utils::{param_test, Result};

    use super::*;
    use crate::encoding::{initialize_encoding_config, utils};

    fn init_config_and_encode_pairs(
        source_symbols_primary: u16,
        source_symbols_secondary: u16,
        n_shards: u32,
        blob: &[u8],
    ) -> Vec<SliverPair> {
        initialize_encoding_config(source_symbols_primary, source_symbols_secondary, n_shards);
        let config = get_encoding_config();
        config.get_blob_encoder(blob).unwrap().encode()
    }

    param_test! {
        copy_symbol_to_modifies_empty_sliver_correctly: [
            #[should_panic] empty_symbol_1: (0, 0, 2, &[], &[]),
            #[should_panic] empty_symbol_2: (2, 0, 2, &[], &[0,0]),
            non_empty_symbol_aligned_1: (2, 0, 2, &[1,2], &[1,2,0,0]),
            non_empty_symbol_aligned_2: (2, 1, 2, &[1,2], &[0,0,1,2]),
            #[should_panic] non_empty_wrong_symbol_size_1: (3, 0, 2, &[1,2,3], &[]),
            #[should_panic] non_empty_wrong_symbol_size_2: (3, 1, 2, &[1], &[]),
        ]
    }
    fn copy_symbol_to_modifies_empty_sliver_correctly(
        sliver_n_symbols: usize,
        index: usize,
        symbol_size: u16,
        symbol: &[u8],
        expected_sliver_data: &[u8],
    ) {
        assert_eq!(
            Sliver::<Primary>::new_empty(sliver_n_symbols, symbol_size, 0)
                .copy_symbol_to(index, symbol)
                .symbols
                .data(),
            expected_sliver_data
        );
    }

    #[test]
    fn new_sliver_copies_provided_slice() {
        let slice = [1, 2, 3, 4, 5];
        assert_eq!(Sliver::<Primary>::new(slice, 1, 0).symbols.data(), &slice)
    }

    param_test! {
        test_create_recovery_symbols -> Result: [
            square_one_byte_symbol: (2, 2, &[1,2,3,4]),
            square_two_byte_symbol: (2, 2, &[1,2,3,4,5,6,7,8]),
            rectangle_two_byte_symbol: (2, 3, &[1,2,3,4,5,6,7,8,9,10,11,12]),
        ]
    }
    fn test_create_recovery_symbols(
        source_symbols_primary: u16,
        source_symbols_secondary: u16,
        blob: &[u8],
    ) -> Result {
        let n_shards = 3 * (source_symbols_primary + source_symbols_secondary) as u32;
        let pairs = init_config_and_encode_pairs(
            source_symbols_primary,
            source_symbols_secondary,
            n_shards,
            blob,
        );

        // Check that the whole expansion is correct.
        for (idx, sliver) in pairs.iter().enumerate() {
            // Check the expansion of the primary sliver.
            let expanded_primary = sliver.primary.recovery_symbols()?;
            for (col, other_pair) in pairs.iter().rev().enumerate() {
                let rec = other_pair.secondary.recovery_symbols()?;
                assert_eq!(expanded_primary[col], rec[idx]);
            }
            // Check the expansion of the secondary sliver.
            let expanded_secondary = sliver.secondary.recovery_symbols()?;
            for (row, other_pair) in pairs.iter().enumerate() {
                let rec = other_pair.primary.recovery_symbols()?;
                assert_eq!(expanded_secondary[row], rec[n_shards as usize - 1 - idx]);
            }
        }
        Ok(())
    }

    param_test! {
        test_recover_sliver_failure: [
            no_symbols: (&[], 2, Ok(None)),
            empty_symbols: (&[&[], &[]], 2, Err(RecoveryError::InvalidSymbolSizes)),
            too_few_symbols: (&[&[1,2]], 2, Ok(None)),
            inconsistent_size: (&[&[1,2],&[3]], 2, Err(RecoveryError::InvalidSymbolSizes)),
            inconsistent_and_empty_size: (&[&[1],&[]], 2, Err(RecoveryError::InvalidSymbolSizes)),
        ]
    }
    // Only testing for failures in the decoding. The correct decoding is tested below.
    fn test_recover_sliver_failure(
        symbols: &[&[u8]],
        n_source_symbols: u16,
        result: std::result::Result<Option<Sliver<Primary>>, RecoveryError>,
    ) {
        initialize_encoding_config(
            n_source_symbols,
            n_source_symbols,
            n_source_symbols as u32 * 3 + 1,
        );
        let recovery_symbols = symbols
            .iter()
            .enumerate()
            .map(|(idx, &s)| DecodingSymbol {
                index: idx as u32,
                data: s.into(),
            })
            .collect::<Vec<_>>();
        let recovered = Sliver::<Primary>::recover_sliver(recovery_symbols, 0);
        assert_eq!(recovered, result);
    }

    param_test! {
        test_recover_sliver_from_symbols -> Result: [
            square_one_byte_symbol: (2, 2, &[1,2,3,4]),
            square_two_byte_symbol: (2, 2, &[1,2,3,4,5,6,7,8]),
            rectangle_two_byte_symbol_1: (2, 3, &[1,2,3,4,5,6,7,8,9,10,11,12]),
            rectangle_two_byte_symbol_2: (2, 4, &[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]),
            rectangle_two_byte_symbol_3: (4, 2, &[11,20,3,13,5,110,77,17,111,56,11,0,0,14,15,1]),
        ]
    }
    fn test_recover_sliver_from_symbols(
        source_symbols_primary: u16,
        source_symbols_secondary: u16,
        blob: &[u8],
    ) -> Result {
        let n_shards = 3 * (source_symbols_primary + source_symbols_secondary) as u32;
        let pairs = init_config_and_encode_pairs(
            source_symbols_primary,
            source_symbols_secondary,
            n_shards,
            blob,
        );
        let n_to_recover_from = source_symbols_primary.max(source_symbols_secondary) as usize;
        let mut rng = StdRng::seed_from_u64(42);

        for pair in pairs.iter() {
            // Get a random subset of recovery symbols.
            let recovery_symbols: Vec<_> = utils::get_random_subset(
                pairs
                    .iter()
                    .map(|p| p.recovery_symbols_for_sliver(pair.index()).unwrap()),
                &mut rng,
                n_to_recover_from,
            )
            .collect();

            // Recover the primary sliver.
            let recovered = Sliver::<Primary>::recover_sliver(
                recovery_symbols.iter().map(|s| s.primary.symbol.clone()),
                pair.primary.index,
            )?;
            assert_eq!(recovered.unwrap(), pair.primary);

            // Recover the secondary sliver.
            let recovered = Sliver::<Secondary>::recover_sliver(
                recovery_symbols.iter().map(|s| s.secondary.symbol.clone()),
                pair.secondary.index,
            )?;
            assert_eq!(recovered.unwrap(), pair.secondary);
        }
        Ok(())
    }

    param_test! {
        test_single_recovery_symbol -> Result : [
            one_byte_sliver: (&[1,2,3,4,5], 1, 5, 5, 16),
            two_byte_sliver: (&[1,2,3,4,5,6], 2, 3, 3, 10),
            two_byte_sliver_large_n_shards: (&[1,2,3,4,5,6], 2, 3, 3, 100),
            ]
    }
    fn test_single_recovery_symbol(
        sliver_bytes: &[u8],
        symbol_size: u16,
        source_symbols_primary: u16,
        source_symbols_secondary: u16,
        n_shards: u32,
    ) -> Result {
        initialize_encoding_config(source_symbols_primary, source_symbols_secondary, n_shards);

        // Interpret the sliver as both primary and secondary for testing.
        let primary = Sliver::<Primary>::new(sliver_bytes, symbol_size, 0);
        let secondary = Sliver::<Secondary>::new(sliver_bytes, symbol_size, 0);

        for (idx, symbol) in primary.recovery_symbols()?.to_symbols().enumerate() {
            println!("idx {}", idx);
            assert_eq!(primary.single_recovery_symbol(idx as u32)?, symbol)
        }
        for (idx, symbol) in secondary.recovery_symbols()?.to_symbols().enumerate() {
            assert_eq!(primary.single_recovery_symbol(idx as u32)?, symbol)
        }
        Ok(())
    }

    #[test]
    fn test_single_recovery_symbol_empty_sliver() {
        initialize_encoding_config(3, 3, 10);
        assert_eq!(
            Sliver::<Primary>::new([], 1, 0).single_recovery_symbol(42),
            Err(RecoveryError::EncodeError(EncodeError::EmptyData))
        );
        assert_eq!(
            Sliver::<Secondary>::new([], 1, 0).single_recovery_symbol(42),
            Err(RecoveryError::EncodeError(EncodeError::EmptyData))
        );
    }

    #[test]
    fn test_recovery_symbols_empty_sliver() {
        initialize_encoding_config(3, 3, 10);
        assert_eq!(
            Sliver::<Primary>::new([], 1, 0).recovery_symbols(),
            Err(RecoveryError::EncodeError(EncodeError::EmptyData))
        );
        assert_eq!(
            Sliver::<Secondary>::new([], 1, 0).recovery_symbols(),
            Err(RecoveryError::EncodeError(EncodeError::EmptyData))
        );
    }

    param_test! {
        test_single_recovery_symbol_indexes: [
            index_1: (0, true),
            index_2: (42, true),
            index_3: (113, true),
            index_4: (u16::MAX as u32 + 13, true),
            index_5 : (MAX_ENCODING_SYMBOL_ID + 1, false),
            index_6 : (2u32.pow(24), false),
            index_7 : (u32::MAX, false),
        ]
    }
    fn test_single_recovery_symbol_indexes(index: u32, is_ok: bool) {
        initialize_encoding_config(3, 3, 10);
        let result = Sliver::<Primary>::new([1, 2, 3, 4, 5, 6], 2, 0).single_recovery_symbol(index);
        if is_ok {
            assert!(result.is_ok());
        } else {
            assert_eq!(result, Err(RecoveryError::IndexTooLarge))
        }
    }

    param_test! {
        test_recover_all_slivers_from_f_plus_1: [
            #[should_panic] recover_empty: (3, &[]),
            recover_single_byte: (3, &[1]),
            recover_one_byte_symbol: (3, &[
                1,2,3,4,5,6,7,8,9,
                1,2,3,4,5,6,7,8,9,
            ]),
            recover_two_byte_symbol: (3, &[
                1,2,3,4,5,6,7,8,9,
                1,2,3,4,5,6,7,8,9,
                1,2,3,4,5,6,7,8,9,
                1,2,3,4,5,6,7,8,9,
            ]),
        ]
    }
    fn test_recover_all_slivers_from_f_plus_1(f: usize, blob: &[u8]) {
        let n_shards = 3 * f + 1;
        let source_symbols_primary = f as u16;
        let source_symbols_secondary = 2 * f as u16;
        let pairs = init_config_and_encode_pairs(
            source_symbols_primary,
            source_symbols_secondary,
            n_shards as u32,
            blob,
        );

        // Keep only the first `to_reconstruct_from` primary slivers.
        let to_reconstruct_from = f + 1;
        let mut primary_slivers = pairs
            .iter()
            .take(to_reconstruct_from)
            .map(|p| p.primary.clone())
            .collect::<Vec<_>>();

        // Reconstruct the secondary slivers from the primary ones.
        let secondary_slivers = (0..n_shards)
            .map(|target_idx| {
                Sliver::<Secondary>::recover_sliver(
                    primary_slivers.iter().map(|p| {
                        p.recovery_symbol_for_sliver(p.index, target_idx as u32)
                            .unwrap()
                            .symbol
                    }),
                    (n_shards - 1 - target_idx) as u32,
                )
                .unwrap()
                .unwrap()
            })
            .collect::<Vec<_>>();

        // Recover the missing primary slivers from 2f+1 of the reconstructed secondary slivers.
        primary_slivers.extend((to_reconstruct_from..n_shards).map(|target_idx| {
            Sliver::<Primary>::recover_sliver(
                secondary_slivers
                    .iter()
                    .take(2 * f + 1)
                    .enumerate()
                    .map(|(source_idx, s)| {
                        s.recovery_symbol_for_sliver(source_idx as u32, target_idx as u32)
                            .unwrap()
                            .symbol
                    }),
                target_idx as u32,
            )
            .unwrap()
            .unwrap()
        }));

        // Check that the reconstructed slivers match the original pairs.
        assert!(pairs
            .iter()
            .enumerate()
            .all(|(idx, pair)| pair.primary == primary_slivers[idx]
                && pair.secondary == secondary_slivers[idx]));
    }
}
