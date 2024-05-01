// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fmt::Display, marker::PhantomData, num::NonZeroU16};

use fastcrypto::hash::{Blake2b256, HashFunction};
use serde::{Deserialize, Serialize};

use super::{
    errors::SliverVerificationError,
    Decoder,
    DecodingSymbol,
    DecodingSymbolPair,
    EncodeError,
    Encoder,
    EncodingAxis,
    EncodingConfig,
    Primary,
    RecoveryError,
    Secondary,
    Symbols,
};
use crate::{
    ensure,
    merkle::{MerkleProof, MerkleTree, Node, DIGEST_LEN},
    metadata::{SliverPairMetadata, VerifiedBlobMetadataWithId},
    utils,
    SliverIndex,
    SliverPairIndex,
};

/// A primary sliver resulting from an encoding of a blob.
pub type PrimarySliver = Sliver<Primary>;

/// A secondary sliver resulting from an encoding of a blob.
pub type SecondarySliver = Sliver<Secondary>;

/// Encoded data corresponding to a single [`EncodingAxis`] assigned to one shard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sliver<T: EncodingAxis> {
    /// The encoded data.
    pub symbols: Symbols,
    /// Index of this sliver.
    ///
    /// This is needed for the decoding to be able to identify the encoded symbols.
    pub index: SliverIndex,
    _sliver_type: PhantomData<T>,
}

impl<T: EncodingAxis> Sliver<T> {
    /// Creates a new `Sliver` copying the provided slice of bytes.
    ///
    /// # Panics
    ///
    /// Panics if the slice does not contain complete symbols, i.e., if
    /// `data.len() % symbol_size != 0`.
    pub fn new<U: Into<Vec<u8>>>(data: U, symbol_size: NonZeroU16, index: SliverIndex) -> Self {
        Self {
            symbols: Symbols::new(data.into(), symbol_size),
            index,
            _sliver_type: PhantomData,
        }
    }

    /// Creates a new `Sliver` with empty data of specified length.
    ///
    /// The `length` parameter specifies the number of symbols.
    pub fn new_empty(length: u16, symbol_size: NonZeroU16, index: SliverIndex) -> Self {
        Self {
            symbols: Symbols::zeros(length as usize, symbol_size),
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

    /// Checks that the provided sliver is authenticated by the metadata.
    ///
    /// The checks include verifying that the sliver has the correct number of symbols and symbol
    /// size, and that the hash in the metadata matches the Merkle root over the sliver's symbols.
    pub fn verify(
        &self,
        encoding_config: &EncodingConfig,
        metadata: &VerifiedBlobMetadataWithId,
    ) -> Result<(), SliverVerificationError> {
        ensure!(
            self.index.as_usize() < metadata.metadata().hashes.len(),
            SliverVerificationError::IndexTooLarge
        );
        ensure!(
            self.symbols.len()
                == encoding_config
                    .n_source_symbols::<T::OrthogonalAxis>()
                    .get() as usize,
            SliverVerificationError::SliverSizeMismatch
        );
        let symbol_size_from_metadata = encoding_config
            .symbol_size_for_blob(
                metadata
                    .metadata()
                    .unencoded_length
                    .try_into()
                    .expect("conversion u64 -> usize failed"),
            )
            .expect("the symbol size is checked in `UnverifiedBlobMetadataWithId::verify`");
        ensure!(
            self.symbols.symbol_size() == symbol_size_from_metadata,
            SliverVerificationError::SymbolSizeMismatch
        );
        let pair_metadata = metadata
            .metadata()
            .hashes
            .get(
                self.index
                    .to_pair_index::<T>(encoding_config.n_shards)
                    .as_usize(),
            )
            .expect("n_shards and shard_index < n_shards are checked above");
        ensure!(
            self.get_merkle_root::<Blake2b256>(encoding_config)? == *pair_metadata.hash::<T>(),
            SliverVerificationError::MerkleRootMismatch
        );
        Ok(())
    }

    /// Creates the first `n_shards` recovery symbols from the sliver.
    ///
    /// [`Primary`] slivers are encoded with the [`Secondary`] encoding, and vice versa, to obtain
    /// the fully-expanded set of recovery symbols.
    ///
    /// # Errors
    ///
    /// Returns an [`RecoveryError::EncodeError`] if the `symbols` cannot be encoded. See
    /// [`Encoder::new`] for further details about the returned errors.
    pub fn recovery_symbols(&self, config: &EncodingConfig) -> Result<Symbols, RecoveryError> {
        Ok(Symbols::new(
            self.get_sliver_encoder(config)?
                .encode_all()
                .flatten()
                .collect(),
            self.symbols.symbol_size(), // The symbol size remains unvaried when re-encoding.
        ))
    }

    /// Creates the bytes of the recovery symbol at the provided `index`.
    ///
    /// # Errors
    ///
    /// Returns a [`RecoveryError::EncodeError`] error if the `symbols` cannot be encoded. See
    /// [`Encoder::new`] for further details about the returned errors. Returns a
    /// [`RecoveryError::IndexTooLarge`] error if `index >= n_shards`.
    pub fn single_recovery_symbol(
        &self,
        index: u16,
        config: &EncodingConfig,
    ) -> Result<Vec<u8>, RecoveryError> {
        Self::check_index(index, config.n_shards)?;
        Ok(self
            .get_sliver_encoder(config)?
            // TODO(mlegner): add more efficient function to encode a single symbol
            .encode_range(index..index + 1)
            .next()
            .expect("the encoder should always be able to produce an encoding symbol"))
    }

    /// Gets the recovery symbol for a specific target sliver starting from the current sliver.
    ///
    /// A [`Primary`] sliver is used to reconstruct symbols for a [`Secondary`] sliver, and
    /// vice-versa. The `target_pair_index` is the index of the [`SliverPair`] to which the sliver
    /// to be recovered belongs.
    ///
    /// # Errors
    ///
    /// Returns a [`RecoveryError::EncodeError`] if the sliver cannot be encoded. See
    /// [`Encoder::new`] for further details about the returned errors. Returns a
    /// [`RecoveryError::IndexTooLarge`] error if `target_pair_index >= n_shards`.
    pub fn recovery_symbol_for_sliver(
        &self,
        target_pair_index: SliverPairIndex,
        config: &EncodingConfig,
    ) -> Result<DecodingSymbol<T::OrthogonalAxis>, RecoveryError> {
        Self::check_index(target_pair_index.into(), config.n_shards)?;
        Ok(DecodingSymbol::new(
            self.index.into(),
            self.single_recovery_symbol(
                target_pair_index
                    .to_sliver_index::<T::OrthogonalAxis>(config.n_shards)
                    .into(),
                config,
            )?,
        ))
    }

    /// Gets the recovery symbol for a specific target sliver starting from the current sliver,
    /// together with the Merkle proof computed over the `n_shards` symbols of the [`Sliver`].
    ///
    /// The `target_pair_index` is the index of the [`SliverPair`] to which the sliver to be
    /// recovered belongs.
    ///
    /// # Errors
    ///
    /// Returns a [`RecoveryError::EncodeError`] if the sliver cannot be encoded. See
    /// [`Encoder::new`] for further details about the returned errors. Returns a
    /// [`RecoveryError::IndexTooLarge`] error if `target_pair_index >= n_shards`.
    pub fn recovery_symbol_for_sliver_with_proof<U>(
        &self,
        target_pair_index: SliverPairIndex,
        config: &EncodingConfig,
    ) -> Result<DecodingSymbol<T::OrthogonalAxis, MerkleProof<U>>, RecoveryError>
    where
        U: HashFunction<DIGEST_LEN>,
    {
        Self::check_index(target_pair_index.into(), config.n_shards)?;
        let recovery_symbols = self.recovery_symbols(config)?;
        Ok(recovery_symbols
            .decoding_symbol_at(
                target_pair_index
                    .to_sliver_index::<T::OrthogonalAxis>(config.n_shards)
                    .as_usize(),
                self.index.into(),
            )
            .expect("we have exactly `n_shards` symbols and the bound was checked")
            .with_proof(
                MerkleTree::<U>::build(recovery_symbols.to_symbols())
                    .get_proof(target_pair_index.as_usize())
                    .expect("bound already checked above"),
            ))
    }

    /// Recovers a [`Sliver`] from the recovery symbols.
    ///
    /// Returns the recovered [`Sliver`] if decoding succeeds or `None` if decoding fails.
    ///
    /// # Errors
    ///
    /// Returns an [`RecoveryError::EncodeError`] if the recovery symbols provided are larger than
    /// what the [`Decoder`] can process, i.e., if the `symbol_size` of the `recovery_symbols` is
    /// larger than [`MAX_SYMBOL_SIZE`][super::MAX_SYMBOL_SIZE]. See [`Decoder::new`] for further
    /// details. Returns a [`RecoveryError::InvalidSymbolSizes`] error if the symbols provided have
    /// different symbol sizes or if their symbol size is 0 (they are empty).
    pub fn recover_sliver<I, V>(
        recovery_symbols: I,
        index: SliverIndex,
        config: &EncodingConfig,
    ) -> Result<Option<Self>, RecoveryError>
    where
        I: IntoIterator,
        I::IntoIter: Iterator<Item = DecodingSymbol<T, V>> + Clone,
        V: std::fmt::Debug,
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
        let symbol_size = symbol_size
            .try_into()
            .ok()
            .and_then(NonZeroU16::new)
            .ok_or(EncodeError::DataTooLarge)?;

        // Pass the symbols to the decoder.
        Ok(Decoder::new(Self::n_source_symbols(config), symbol_size)
            .decode(recovery_iter)
            .map(|data| Sliver::new(data, symbol_size, index)))
    }

    /// Computes the Merkle root [`Node`][`crate::merkle::Node`] of the
    /// [`MerkleTree`][`crate::merkle::MerkleTree`] over the symbols of the expanded [`Sliver`].
    ///
    /// # Errors
    ///
    /// Returns an [`RecoveryError::EncodeError`] if the `symbols` cannot be encoded. See
    /// [`Encoder::new`] for further details about the returned errors.
    pub fn get_merkle_root<U: HashFunction<DIGEST_LEN>>(
        &self,
        config: &EncodingConfig,
    ) -> Result<Node, RecoveryError> {
        Ok(MerkleTree::<U>::build(self.recovery_symbols(config)?.to_symbols()).root())
    }

    /// Returns the number of source symbol for the current sliver's [`EncodingAxis`].
    fn n_source_symbols(config: &EncodingConfig) -> NonZeroU16 {
        config.n_source_symbols::<T::OrthogonalAxis>()
    }

    /// Creates an encoder for the current sliver.
    fn get_sliver_encoder(&self, config: &EncodingConfig) -> Result<Encoder, EncodeError> {
        config.get_encoder::<T::OrthogonalAxis>(self.symbols.data())
    }

    /// Returns the sliver size in bytes.
    pub fn len(&self) -> usize {
        self.symbols.data().len()
    }

    /// Returns true iff the sliver length is 0.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Ensures the provided index is smaller than the number of shards; otherwise returns a
    /// [`RecoveryError::IndexTooLarge`].
    fn check_index(index: u16, n_shards: NonZeroU16) -> Result<(), RecoveryError> {
        if index >= n_shards.get() {
            Err(RecoveryError::IndexTooLarge)
        } else {
            Ok(())
        }
    }
}

impl<T: EncodingAxis> Display for Sliver<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Sliver{{ type: {}, index: {}, {} }}",
            T::NAME,
            self.index,
            utils::data_prefix_string(self.symbols.data(), 5),
        )
    }
}

/// Combination of a primary and secondary sliver of one shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SliverPair {
    /// The sliver corresponding to the [`Primary`] encoding.
    pub primary: PrimarySliver,
    /// The sliver corresponding to the [`Secondary`] encoding.
    pub secondary: SecondarySliver,
}

impl SliverPair {
    /// Index of this sliver pair.
    ///
    /// Sliver pair `i` contains the primary sliver `i` and the secondary sliver `n_shards-i-1`.
    pub fn index(&self) -> SliverPairIndex {
        self.primary.index.into()
    }

    /// Creates a new sliver pair containing two empty [`Sliver`] instances of the specified size.
    pub fn new_empty(
        config: &EncodingConfig,
        symbol_size: NonZeroU16,
        index: SliverPairIndex,
    ) -> Self {
        SliverPair {
            primary: Sliver::new_empty(
                config.source_symbols_secondary.get(),
                symbol_size,
                index.into(),
            ),
            secondary: Sliver::new_empty(
                config.source_symbols_primary.get(),
                symbol_size,
                index.to_sliver_index::<Secondary>(config.n_shards),
            ),
        }
    }

    /// Gets the two recovery symbols for a specific target sliver pair starting from the current
    /// sliver pair.
    ///
    /// The `target_pair_index` is the index of the [`SliverPair`] to be recovered.
    ///
    /// # Errors
    ///
    /// Returns a [`RecoveryError::EncodeError`] if any of the the slivers cannot be encoded. See
    /// [`Encoder::new`] for further details about the returned errors. Returns a
    /// [`RecoveryError::IndexTooLarge`] error if `target_pair_index >= n_shards`.
    pub fn recovery_symbol_pair_for_sliver(
        &self,
        target_pair_index: SliverPairIndex,
        config: &EncodingConfig,
    ) -> Result<DecodingSymbolPair, RecoveryError> {
        Ok(DecodingSymbolPair {
            primary: self
                .secondary
                .recovery_symbol_for_sliver(target_pair_index, config)?,
            secondary: self
                .primary
                .recovery_symbol_for_sliver(target_pair_index, config)?,
        })
    }

    /// Gets the two recovery symbols for a specific target sliver pair starting from the current
    /// sliver pair, together with the Merkle proofs for the two symbols, computed over the
    /// `n_shards` symbols of the respective expanded slivers.
    ///
    /// The `target_pair_index` is the index of the [`SliverPair`] to be recovered.
    ///
    /// # Errors
    ///
    /// Returns a [`RecoveryError::EncodeError`] if any of the the slivers cannot be encoded. See
    /// [`Encoder::new`] for further details about the returned errors. Returns a
    /// [`RecoveryError::IndexTooLarge`] error if `target_pair_index >= n_shards`.
    pub fn recovery_symbol_pair_for_sliver_with_proof<T>(
        &self,
        target_pair_index: SliverPairIndex,
        config: &EncodingConfig,
    ) -> Result<DecodingSymbolPair<MerkleProof<T>>, RecoveryError>
    where
        T: HashFunction<DIGEST_LEN>,
    {
        Ok(DecodingSymbolPair {
            primary: self
                .secondary
                .recovery_symbol_for_sliver_with_proof(target_pair_index, config)?,
            secondary: self
                .primary
                .recovery_symbol_for_sliver_with_proof(target_pair_index, config)?,
        })
    }

    /// Concatenates the Merkle roots over the primary and secondary slivers.
    ///
    /// The concatenated Merkle roots are then used as inputs to compute the blob ID (as the root of
    /// the Merkle tree over these concatenated roots).
    ///
    /// # Errors
    ///
    /// Returns a [`RecoveryError::EncodeError`] if any of the the slivers cannot be encoded. See
    /// [`Encoder::new`] for further details about the returned errors.
    pub fn pair_leaf_input<T: HashFunction<DIGEST_LEN>>(
        &self,
        config: &EncodingConfig,
    ) -> Result<[u8; 2 * DIGEST_LEN], RecoveryError> {
        Ok(SliverPairMetadata {
            primary_hash: self.primary.get_merkle_root::<T>(config)?,
            secondary_hash: self.secondary.get_merkle_root::<T>(config)?,
        }
        .pair_leaf_input::<T>())
    }
}

#[cfg(test)]
mod tests {
    use fastcrypto::hash::Blake2b256;
    use walrus_test_utils::{param_test, random_subset, Result};

    use super::*;
    use crate::test_utils;

    fn create_config_and_encode_pairs(
        source_symbols_primary: u16,
        source_symbols_secondary: u16,
        n_shards: u16,
        blob: &[u8],
    ) -> (EncodingConfig, Vec<SliverPair>) {
        let config = EncodingConfig::new_for_test(
            source_symbols_primary,
            source_symbols_secondary,
            n_shards,
        );
        let pairs = config.get_blob_encoder(blob).unwrap().encode();
        (config, pairs)
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
        sliver_n_symbols: u16,
        index: usize,
        symbol_size: u16,
        symbol: &[u8],
        expected_sliver_data: &[u8],
    ) {
        assert_eq!(
            Sliver::<Primary>::new_empty(
                sliver_n_symbols,
                symbol_size.try_into().unwrap(),
                SliverIndex(0)
            )
            .copy_symbol_to(index, symbol)
            .symbols
            .data(),
            expected_sliver_data
        );
    }

    #[test]
    fn new_sliver_copies_provided_slice() {
        let slice = [1, 2, 3, 4, 5];
        assert_eq!(
            Sliver::<Primary>::new(slice, 1.try_into().unwrap(), SliverIndex(0))
                .symbols
                .data(),
            &slice
        )
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
        let n_shards = 3 * (source_symbols_primary + source_symbols_secondary);
        let (config, pairs) = create_config_and_encode_pairs(
            source_symbols_primary,
            source_symbols_secondary,
            n_shards,
            blob,
        );

        // Check that the whole expansion is correct.
        for (index, sliver) in pairs.iter().enumerate() {
            // Check the expansion of the primary sliver.
            let expanded_primary = sliver.primary.recovery_symbols(&config)?;
            for (col, other_pair) in pairs.iter().rev().enumerate() {
                let rec = other_pair.secondary.recovery_symbols(&config)?;
                assert_eq!(expanded_primary[col], rec[index]);
            }
            // Check the expansion of the secondary sliver.
            let expanded_secondary = sliver.secondary.recovery_symbols(&config)?;
            for (row, other_pair) in pairs.iter().enumerate() {
                let rec = other_pair.primary.recovery_symbols(&config)?;
                assert_eq!(expanded_secondary[row], rec[n_shards as usize - 1 - index]);
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
        let config = EncodingConfig::new_for_test(
            n_source_symbols,
            n_source_symbols,
            n_source_symbols * 3 + 1,
        );
        let recovery_symbols = symbols
            .iter()
            .enumerate()
            .map(|(index, &s)| DecodingSymbol::new(index.try_into().unwrap(), s.into()))
            .collect::<Vec<_>>();
        let recovered =
            Sliver::<Primary>::recover_sliver(recovery_symbols, SliverIndex(0), &config);
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
        let n_shards = 3 * (source_symbols_primary + source_symbols_secondary);
        let (config, pairs) = create_config_and_encode_pairs(
            source_symbols_primary,
            source_symbols_secondary,
            n_shards,
            blob,
        );
        let n_to_recover_from = source_symbols_primary.max(source_symbols_secondary) as usize;

        for pair in pairs.iter() {
            // Get a random subset of recovery symbols.
            let recovery_symbols: Vec<_> = random_subset(
                pairs.iter().map(|p| {
                    p.recovery_symbol_pair_for_sliver(pair.index(), &config)
                        .unwrap()
                }),
                n_to_recover_from,
            )
            .collect();

            // Recover the primary sliver.
            let recovered = Sliver::recover_sliver(
                recovery_symbols.iter().map(|s| s.primary.clone()),
                pair.primary.index,
                &config,
            )?;
            assert_eq!(recovered.unwrap(), pair.primary);

            // Recover the secondary sliver.
            let recovered = Sliver::recover_sliver(
                recovery_symbols.iter().map(|s| s.secondary.clone()),
                pair.secondary.index,
                &config,
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
        n_shards: u16,
    ) -> Result {
        let symbol_size = symbol_size.try_into().unwrap();
        let config = EncodingConfig::new_for_test(
            source_symbols_primary,
            source_symbols_secondary,
            n_shards,
        );

        // Interpret the sliver as both primary and secondary for testing.
        let primary = Sliver::<Primary>::new(sliver_bytes, symbol_size, SliverIndex(0));
        let secondary = Sliver::<Secondary>::new(sliver_bytes, symbol_size, SliverIndex(0));

        for (index, symbol) in primary.recovery_symbols(&config)?.to_symbols().enumerate() {
            assert_eq!(
                primary.single_recovery_symbol(index.try_into().unwrap(), &config)?,
                symbol
            )
        }
        for (index, symbol) in secondary
            .recovery_symbols(&config)?
            .to_symbols()
            .enumerate()
        {
            assert_eq!(
                primary.single_recovery_symbol(index.try_into().unwrap(), &config)?,
                symbol
            )
        }
        Ok(())
    }

    param_test! {
        test_single_recovery_symbol_empty_sliver: [
            primary: <Primary>(),
            secondary: <Secondary>(),
        ]
    }
    fn test_single_recovery_symbol_empty_sliver<T: EncodingAxis>() {
        let config = test_utils::encoding_config();
        assert_eq!(
            Sliver::<T>::new([], 1.try_into().unwrap(), SliverIndex::new(0))
                .single_recovery_symbol(3, &config),
            Err(RecoveryError::EncodeError(EncodeError::EmptyData))
        );
    }

    param_test! {
        test_recovery_symbols_empty_sliver: [
            primary: <Primary>(),
            secondary: <Secondary>(),
        ]
    }
    fn test_recovery_symbols_empty_sliver<T: EncodingAxis>() {
        let config = EncodingConfig::new_for_test(3, 3, 10);
        assert_eq!(
            Sliver::<T>::new([], 1.try_into().unwrap(), SliverIndex::new(0))
                .recovery_symbols(&config),
            Err(RecoveryError::EncodeError(EncodeError::EmptyData))
        );
    }

    param_test! {
        test_single_recovery_symbol_indexes: [
            index_1: (0, 10, true),
            index_2: (9, 10,true),
            index_3: (10, 10,false),
            index_4: (113, 10, false),
            index_5: (10, 11, true),
        ]
    }
    fn test_single_recovery_symbol_indexes(index: u32, n_shards: u16, is_ok: bool) {
        let config = EncodingConfig::new_for_test(3, 3, n_shards);
        let result =
            Sliver::<Primary>::new([1, 2, 3, 4, 5, 6], 2.try_into().unwrap(), SliverIndex(0))
                .single_recovery_symbol(index.try_into().unwrap(), &config);
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
    fn test_recover_all_slivers_from_f_plus_1(f: u16, blob: &[u8]) {
        let n_shards = 3 * f + 1;
        let (config, pairs) = create_config_and_encode_pairs(f, 2 * f, n_shards, blob);

        // Keep only the first `to_reconstruct_from` primary slivers.
        let to_reconstruct_from = f + 1;
        let mut primary_slivers = pairs
            .iter()
            .take(to_reconstruct_from.into())
            .map(|p| p.primary.clone())
            .collect::<Vec<_>>();

        // Reconstruct the secondary slivers from the primary ones.
        let secondary_slivers = (0..n_shards)
            .map(|target_index| {
                let index = SliverPairIndex(target_index);
                Sliver::<Secondary>::recover_sliver(
                    primary_slivers
                        .iter()
                        .map(|p| p.recovery_symbol_for_sliver(index, &config).unwrap()),
                    SliverIndex(n_shards - 1 - target_index),
                    &config,
                )
                .unwrap()
                .unwrap()
            })
            .collect::<Vec<_>>();

        // Recover the missing primary slivers from 2f+1 of the reconstructed secondary slivers.
        primary_slivers.extend((to_reconstruct_from..n_shards).map(|target_index| {
            let index = SliverPairIndex(target_index);
            Sliver::<Primary>::recover_sliver(
                secondary_slivers
                    .iter()
                    .take(config.source_symbols_secondary.get().into())
                    .map(|s| s.recovery_symbol_for_sliver(index, &config).unwrap()),
                index.into(),
                &config,
            )
            .unwrap()
            .unwrap()
        }));

        // Check that the reconstructed slivers match the original pairs.
        assert!(pairs
            .iter()
            .enumerate()
            .all(|(index, pair)| pair.primary == primary_slivers[index]
                && pair.secondary == secondary_slivers[index]));
    }

    param_test! {
        test_recovery_symbol_proof: [
            one_byte_symbol: (&[1,2,3,4], 4, 1),
            two_byte_symbol: (&[1,2,3,4], 2, 2)
        ]
    }
    fn test_recovery_symbol_proof(slice: &[u8], f: u16, symbol_size: u16) {
        let n_shards = 3 * f + 1;
        let config = EncodingConfig::new_for_test(f, 2 * f, n_shards);
        let sliver =
            Sliver::<Secondary>::new(slice, symbol_size.try_into().unwrap(), SliverIndex(0));
        let merkle_tree =
            MerkleTree::<Blake2b256>::build(sliver.recovery_symbols(&config).unwrap().to_symbols());
        for index in 0..n_shards {
            let shard_pair_index = SliverPairIndex(index);
            assert!(sliver
                .recovery_symbol_for_sliver_with_proof::<Blake2b256>(shard_pair_index, &config)
                .unwrap()
                .verify_proof(&merkle_tree.root()));
        }
    }
}
