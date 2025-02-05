// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use alloc::vec::Vec;
use core::{
    fmt::Display,
    marker::PhantomData,
    num::{NonZeroU16, NonZeroU32},
};

use fastcrypto::hash::{Blake2b256, HashFunction};
use serde::{Deserialize, Serialize};

use super::{
    errors::{SliverRecoveryError, SliverVerificationError},
    symbols,
    DecodingSymbol,
    EncodeError,
    Encoder,
    EncodingAxis,
    EncodingConfig,
    Primary,
    RecoverySymbol,
    RecoverySymbolError,
    RecoverySymbolPair,
    Secondary,
    SliverRecoveryOrVerificationError,
    Symbols,
};
use crate::{
    ensure,
    inconsistency::{InconsistencyProof, SliverOrInconsistencyProof},
    merkle::{MerkleAuth, MerkleProof, MerkleTree, Node, DIGEST_LEN},
    metadata::{BlobMetadata, BlobMetadataApi as _, SliverPairMetadata},
    utils,
    SliverIndex,
    SliverPairIndex,
};

/// A primary sliver resulting from an encoding of a blob.
pub type PrimarySliver = SliverData<Primary>;

/// A secondary sliver resulting from an encoding of a blob.
pub type SecondarySliver = SliverData<Secondary>;

/// Encoded data corresponding to a single [`EncodingAxis`] assigned to one shard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SliverData<T: EncodingAxis> {
    /// The encoded data.
    pub symbols: Symbols,
    /// Index of this sliver.
    ///
    /// This is needed for the decoding to be able to identify the encoded symbols.
    pub index: SliverIndex,
    _sliver_type: PhantomData<T>,
}

impl<T: EncodingAxis> SliverData<T> {
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
            symbols: Symbols::zeros(length.into(), symbol_size),
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
    /// The checks include verifying that the sliver has the correct length and symbol size, and
    /// that the hash in the metadata matches the Merkle root over the sliver's symbols.
    pub fn verify(
        &self,
        encoding_config: &EncodingConfig,
        metadata: &BlobMetadata,
    ) -> Result<(), SliverVerificationError> {
        ensure!(
            self.index.as_usize() < metadata.hashes().len(),
            SliverVerificationError::IndexTooLarge
        );
        ensure!(
            self.has_correct_length(encoding_config, metadata.unencoded_length()),
            SliverVerificationError::SliverSizeMismatch
        );
        ensure!(
            Ok(self.symbols.symbol_size()) == metadata.symbol_size(encoding_config),
            SliverVerificationError::SymbolSizeMismatch
        );
        let pair_metadata = metadata
            .hashes()
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

    /// Returns true iff the sliver has the length expected based on the encoding configuration and
    /// blob size.
    fn has_correct_length(&self, config: &EncodingConfig, blob_size: u64) -> bool {
        self.expected_length(config, blob_size).is_some_and(|l| {
            self.len() == usize::try_from(l).expect("we assume at least a 32-bit architecture")
        })
    }

    fn expected_length(&self, config: &EncodingConfig, blob_size: u64) -> Option<u32> {
        config
            .sliver_size_for_blob::<T>(blob_size)
            .map(NonZeroU32::get)
            .ok()
    }

    /// Creates the first `n_shards` recovery symbols from the sliver.
    ///
    /// [`Primary`] slivers are encoded with the [`Secondary`] encoding, and vice versa, to obtain
    /// the fully-expanded set of recovery symbols.
    ///
    /// # Errors
    ///
    /// Returns a [`RecoverySymbolError::EncodeError`] if the `symbols` cannot be encoded. See
    /// [`Encoder::new`] for further details about the returned errors.
    pub fn recovery_symbols(
        &self,
        config: &EncodingConfig,
    ) -> Result<Symbols, RecoverySymbolError> {
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
    /// Returns a [`RecoverySymbolError::EncodeError`] error if the `symbols` cannot be encoded. See
    /// [`Encoder::new`] for further details about the returned errors. Returns a
    /// [`RecoverySymbolError::IndexTooLarge`] error if `index >= n_shards`.
    pub fn single_recovery_symbol(
        &self,
        index: u16,
        config: &EncodingConfig,
    ) -> Result<Vec<u8>, RecoverySymbolError> {
        Self::check_index(index, config.n_shards)?;
        Ok(self
            .get_sliver_encoder(config)?
            // TODO(mlegner): Add more efficient function to encode a single symbol. (#624)
            .encode_range(index..index + 1)
            .next()
            .expect("the encoder should always be able to produce an encoding symbol"))
    }

    /// Gets the decoding symbol for a specific target sliver starting from the current sliver.
    ///
    /// A [`Primary`] sliver is used to reconstruct symbols for a [`Secondary`] sliver, and
    /// vice-versa. The `target_pair_index` is the index of the [`SliverPair`] to which the sliver
    /// to be recovered belongs.
    ///
    /// # Errors
    ///
    /// Returns a [`RecoverySymbolError::EncodeError`] if the sliver cannot be encoded. See
    /// [`Encoder::new`] for further details about the returned errors. Returns a
    /// [`RecoverySymbolError::IndexTooLarge`] error if `target_pair_index >= n_shards`.
    pub fn decoding_symbol_for_sliver(
        &self,
        target_pair_index: SliverPairIndex,
        config: &EncodingConfig,
    ) -> Result<DecodingSymbol<T::OrthogonalAxis>, RecoverySymbolError> {
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

    /// Gets the recovery symbol for a specific target sliver starting from the current sliver.
    ///
    /// This contains the Merkle proof computed over the `n_shards` symbols of the [`SliverData`].
    ///
    /// The `target_pair_index` is the index of the [`SliverPair`] to which the sliver to be
    /// recovered belongs.
    ///
    /// # Errors
    ///
    /// Returns a [`RecoverySymbolError::EncodeError`] if the sliver cannot be encoded. See
    /// [`Encoder::new`] for further details about the returned errors. Returns a
    /// [`RecoverySymbolError::IndexTooLarge`] error if `target_pair_index >= n_shards`.
    pub fn recovery_symbol_for_sliver(
        &self,
        target_pair_index: SliverPairIndex,
        config: &EncodingConfig,
    ) -> Result<RecoverySymbol<T::OrthogonalAxis, MerkleProof<Blake2b256>>, RecoverySymbolError>
    {
        Self::check_index(target_pair_index.into(), config.n_shards)?;
        let recovery_symbols = self.recovery_symbols(config)?;
        let target_sliver_index =
            target_pair_index.to_sliver_index::<T::OrthogonalAxis>(config.n_shards);
        Ok(recovery_symbols
            .decoding_symbol_at(target_sliver_index.as_usize(), self.index.into())
            .expect("we have exactly `n_shards` symbols and the bound was checked")
            .with_proof(
                MerkleTree::<Blake2b256>::build(recovery_symbols.to_symbols())
                    .get_proof(target_sliver_index.as_usize())
                    .expect("bound already checked above"),
            ))
    }

    /// Recovers a [`Sliver`] from the provided recovery symbols.
    ///
    /// Returns the recovered [`Sliver`] if decoding succeeds or `None` if decoding fails.
    ///
    /// Does *not perform any checks* on the provided symbols; in particular, it does not verify any
    /// proofs.
    fn recover_sliver_without_verification<I, U>(
        recovery_symbols: I,
        target_index: SliverIndex,
        symbol_size: NonZeroU16,
        config: &EncodingConfig,
    ) -> Option<Self>
    where
        I: IntoIterator,
        I::IntoIter: Iterator<Item = RecoverySymbol<T, U>>,
        U: MerkleAuth,
    {
        config
            .get_decoder::<T::OrthogonalAxis>(symbol_size)
            .decode(
                recovery_symbols
                    .into_iter()
                    .map(RecoverySymbol::into_decoding_symbol),
            )
            .map(|data| SliverData::new(data, symbol_size, target_index))
    }

    /// Recovers a [`SliverData`] from the provided recovery symbols, verifying each symbol.
    ///
    /// Returns the recovered [`SliverData`] if decoding succeeds or a [`SliverRecoveryError`] if
    /// decoding fails.
    ///
    /// Symbols that fail verification are logged and dropped.
    pub fn recover_sliver<I, U>(
        recovery_symbols: I,
        target_index: SliverIndex,
        metadata: &BlobMetadata,
        encoding_config: &EncodingConfig,
    ) -> Result<Self, SliverRecoveryError>
    where
        I: IntoIterator,
        I::IntoIter: Iterator<Item = RecoverySymbol<T, U>>,
        U: MerkleAuth,
    {
        let symbol_size = metadata.symbol_size(encoding_config)?;
        Self::recover_sliver_without_verification(
            symbols::filter_recovery_symbols_and_log_invalid(
                recovery_symbols,
                metadata,
                encoding_config,
                target_index,
            ),
            target_index,
            symbol_size,
            encoding_config,
        )
        .ok_or(SliverRecoveryError::DecodingFailure)
    }

    /// Attempts to recover a sliver from the provided recovery symbols.
    ///
    /// If recovery succeeds an the recovered sliver is verified successfully against the metadata,
    /// that sliver is returned. If the recovered sliver is inconsistent with the metadata, an
    /// [`InconsistencyProof`] is generated and returned. If recovery fails or another error occurs,
    /// a [`SliverRecoveryError`] is returned.
    ///
    /// If `verify_symbols` is set to true, symbols that fail verification are logged and
    /// dropped. Otherwise, it is the caller's responsibility to ensure that the recovery symbols
    /// have been verified to be useable to recover the identified symbol.
    pub fn recover_sliver_or_generate_inconsistency_proof<I, U>(
        recovery_symbols: I,
        target_index: SliverIndex,
        metadata: &BlobMetadata,
        encoding_config: &EncodingConfig,
        verify_symbols: bool,
    ) -> Result<SliverOrInconsistencyProof<T, U>, SliverRecoveryOrVerificationError>
    where
        I: IntoIterator,
        I::IntoIter: Iterator<Item = RecoverySymbol<T, U>>,
        U: MerkleAuth,
    {
        let symbol_size = metadata.symbol_size(encoding_config)?;
        let filtered_recovery_symbols: Vec<_> = if verify_symbols {
            symbols::filter_recovery_symbols_and_log_invalid(
                recovery_symbols,
                metadata,
                encoding_config,
                target_index,
            )
            .collect()
        } else {
            recovery_symbols.into_iter().collect()
        };

        let sliver = Self::recover_sliver_without_verification(
            filtered_recovery_symbols.clone(),
            target_index,
            symbol_size,
            encoding_config,
        )
        .ok_or(SliverRecoveryError::DecodingFailure)?;

        match sliver.verify(encoding_config, metadata) {
            Ok(()) => Ok(sliver.into()),
            Err(SliverVerificationError::MerkleRootMismatch) => {
                Ok(InconsistencyProof::new(target_index, filtered_recovery_symbols).into())
            }
            // Any other error indicates an internal problem, not an inconsistent blob.
            Err(e) => Err(e.into()),
        }
    }

    /// Computes the Merkle root [`Node`][`crate::merkle::Node`] of the
    /// [`MerkleTree`][`crate::merkle::MerkleTree`] over the symbols of the expanded [`SliverData`].
    ///
    /// # Errors
    ///
    /// Returns an [`RecoverySymbolError::EncodeError`] if the `symbols` cannot be encoded. See
    /// [`Encoder::new`] for further details about the returned errors.
    pub fn get_merkle_root<U: HashFunction<DIGEST_LEN>>(
        &self,
        config: &EncodingConfig,
    ) -> Result<Node, RecoverySymbolError> {
        Ok(MerkleTree::<U>::build(self.recovery_symbols(config)?.to_symbols()).root())
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
        self.symbols.is_empty()
    }

    /// Ensures the provided index is smaller than the number of shards; otherwise returns a
    /// [`RecoverySymbolError::IndexTooLarge`].
    fn check_index(index: u16, n_shards: NonZeroU16) -> Result<(), RecoverySymbolError> {
        if index >= n_shards.get() {
            Err(RecoverySymbolError::IndexTooLarge)
        } else {
            Ok(())
        }
    }
}

impl<T: EncodingAxis> Display for SliverData<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "Sliver{{ type: {}, index: {}, data: {} }}",
            T::NAME,
            self.index,
            utils::data_prefix_string(self.symbols.data(), 5),
        )
    }
}

/// Combination of a primary and secondary sliver of one shard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

    /// Creates a new sliver pair containing two empty [`SliverData`] instances of the specified
    /// size.
    pub fn new_empty(
        config: &EncodingConfig,
        symbol_size: NonZeroU16,
        index: SliverPairIndex,
    ) -> Self {
        SliverPair {
            primary: SliverData::new_empty(
                config.source_symbols_secondary.get(),
                symbol_size,
                index.into(),
            ),
            secondary: SliverData::new_empty(
                config.source_symbols_primary.get(),
                symbol_size,
                index.to_sliver_index::<Secondary>(config.n_shards),
            ),
        }
    }

    /// Gets the two recovery symbols for a specific target sliver pair starting from the current
    /// sliver pair.
    ///
    /// The result contains the Merkle proofs for the two symbols, computed over the `n_shards`
    /// symbols of the respective expanded slivers.
    ///
    /// The `target_pair_index` is the index of the [`SliverPair`] to be recovered.
    ///
    /// # Errors
    ///
    /// Returns a [`RecoverySymbolError::EncodeError`] if any of the the slivers cannot be encoded.
    /// See [`Encoder::new`] for further details about the returned errors. Returns a
    /// [`RecoverySymbolError::IndexTooLarge`] error if `target_pair_index >= n_shards`.
    pub fn recovery_symbol_pair_for_sliver(
        &self,
        target_pair_index: SliverPairIndex,
        config: &EncodingConfig,
    ) -> Result<RecoverySymbolPair<MerkleProof<Blake2b256>>, RecoverySymbolError> {
        Ok(RecoverySymbolPair {
            primary: self
                .secondary
                .recovery_symbol_for_sliver(target_pair_index, config)?,
            secondary: self
                .primary
                .recovery_symbol_for_sliver(target_pair_index, config)?,
        })
    }

    /// Concatenates the Merkle roots over the primary and secondary slivers.
    ///
    /// The concatenated Merkle roots are then used as inputs to compute the blob ID (as the root of
    /// the Merkle tree over these concatenated roots).
    ///
    /// # Errors
    ///
    /// Returns a [`RecoverySymbolError::EncodeError`] if any of the the slivers cannot be encoded.
    /// See [`Encoder::new`] for further details about the returned errors.
    pub fn pair_leaf_input<T: HashFunction<DIGEST_LEN>>(
        &self,
        config: &EncodingConfig,
    ) -> Result<[u8; 2 * DIGEST_LEN], RecoverySymbolError> {
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
    use crate::{encoding::InvalidDataSizeError, test_utils};

    fn create_config_and_encode_pairs_and_get_metadata(
        source_symbols_primary: u16,
        source_symbols_secondary: u16,
        n_shards: u16,
        blob: &[u8],
    ) -> (EncodingConfig, Vec<SliverPair>, BlobMetadata) {
        let config = EncodingConfig::new_for_test(
            source_symbols_primary,
            source_symbols_secondary,
            n_shards,
        );
        let (pairs, metadata) = config
            .get_blob_encoder(blob)
            .unwrap()
            .encode_with_metadata();
        (config, pairs, metadata.metadata().clone())
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
            SliverData::<Primary>::new_empty(
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
            SliverData::<Primary>::new(slice, 1.try_into().unwrap(), SliverIndex(0))
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
        let (config, pairs, _) = create_config_and_encode_pairs_and_get_metadata(
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
                assert_eq!(
                    expanded_secondary[row],
                    rec[usize::from(n_shards) - 1 - index]
                );
            }
        }
        Ok(())
    }

    param_test! {
        test_recover_sliver_failure: [
            no_symbols: (&[], 2, 1),
            empty_symbols: (&[&[], &[]], 2, 1),
            too_few_symbols: (&[&[1,2]], 2, 2),
            inconsistent_size: (&[&[1,2],&[3]], 2, 2),
            inconsistent_and_empty_size: (&[&[1],&[]], 2, 1),
        ]
    }
    // Only testing for failures in the decoding. The correct decoding is tested below.
    fn test_recover_sliver_failure(symbols: &[&[u8]], n_source_symbols: u16, symbol_size: u16) {
        let config = EncodingConfig::new_for_test(
            n_source_symbols,
            n_source_symbols,
            n_source_symbols * 3 + 1,
        );
        let recovery_symbols = symbols
            .iter()
            .enumerate()
            .map(|(index, &s)| {
                DecodingSymbol::new(index.try_into().unwrap(), s.into())
                    .with_proof(test_utils::merkle_proof())
            })
            .collect::<Vec<_>>();
        let recovered = SliverData::<Primary>::recover_sliver_without_verification(
            recovery_symbols,
            SliverIndex(0),
            symbol_size.try_into().unwrap(),
            &config,
        );
        assert_eq!(recovered, None);
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
        let (config, pairs, metadata) = create_config_and_encode_pairs_and_get_metadata(
            source_symbols_primary,
            source_symbols_secondary,
            n_shards,
            blob,
        );
        let n_to_recover_from = source_symbols_primary.max(source_symbols_secondary).into();

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
            let recovered = SliverData::recover_sliver(
                recovery_symbols.iter().map(|s| s.primary.clone()),
                pair.primary.index,
                &metadata,
                &config,
            )
            .unwrap();
            assert_eq!(recovered, pair.primary);

            // Recover the secondary sliver.
            let recovered = SliverData::recover_sliver(
                recovery_symbols.iter().map(|s| s.secondary.clone()),
                pair.secondary.index,
                &metadata,
                &config,
            )
            .unwrap();
            assert_eq!(recovered, pair.secondary);
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
        let primary = SliverData::<Primary>::new(sliver_bytes, symbol_size, SliverIndex(0));
        let secondary = SliverData::<Secondary>::new(sliver_bytes, symbol_size, SliverIndex(0));

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
            SliverData::<T>::new([], 1.try_into().unwrap(), SliverIndex::new(0))
                .single_recovery_symbol(3, &config),
            Err(InvalidDataSizeError::EmptyData.into())
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
            SliverData::<T>::new([], 1.try_into().unwrap(), SliverIndex::new(0))
                .recovery_symbols(&config),
            Err(InvalidDataSizeError::EmptyData.into())
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
            SliverData::<Primary>::new([1, 2, 3, 4, 5, 6], 2.try_into().unwrap(), SliverIndex(0))
                .single_recovery_symbol(index.try_into().unwrap(), &config);
        if is_ok {
            assert!(result.is_ok());
        } else {
            assert_eq!(result, Err(RecoverySymbolError::IndexTooLarge))
        }
    }

    param_test! {
        test_recover_all_slivers_from_f_plus_1: [
            recover_empty: (3, &[]),
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
        let (config, pairs, metadata) =
            create_config_and_encode_pairs_and_get_metadata(f, 2 * f, n_shards, blob);

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
                SliverData::<Secondary>::recover_sliver(
                    primary_slivers
                        .iter()
                        .map(|p| p.recovery_symbol_for_sliver(index, &config).unwrap()),
                    SliverIndex(n_shards - 1 - target_index),
                    &metadata,
                    &config,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();

        // Recover the missing primary slivers from 2f+1 of the reconstructed secondary slivers.
        primary_slivers.extend((to_reconstruct_from..n_shards).map(|target_index| {
            let index = SliverPairIndex(target_index);
            SliverData::<Primary>::recover_sliver(
                secondary_slivers
                    .iter()
                    .take(config.source_symbols_secondary.get().into())
                    .map(|s| s.recovery_symbol_for_sliver(index, &config).unwrap()),
                index.into(),
                &metadata,
                &config,
            )
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
            SliverData::<Secondary>::new(slice, symbol_size.try_into().unwrap(), SliverIndex(0));
        let merkle_tree =
            MerkleTree::<Blake2b256>::build(sliver.recovery_symbols(&config).unwrap().to_symbols());
        for index in 0..n_shards {
            let shard_pair_index = SliverPairIndex(index);
            assert!(sliver
                .recovery_symbol_for_sliver(shard_pair_index, &config)
                .unwrap()
                .verify_proof(&merkle_tree.root(), index.into()));
        }
    }
}
