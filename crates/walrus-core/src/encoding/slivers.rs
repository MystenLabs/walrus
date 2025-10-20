// Copyright (c) Walrus Foundation
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
    DecodingSymbol,
    EncodingAxis,
    EncodingConfig,
    EncodingConfigEnum,
    EncodingFactory as _,
    Primary,
    RecoverySymbol,
    RecoverySymbolError,
    RecoverySymbolPair,
    Secondary,
    SliverRecoveryOrVerificationError,
    Symbols,
    errors::{SliverRecoveryError, SliverVerificationError},
};
use crate::{
    SliverIndex,
    SliverPairIndex,
    encoding::{DecodeError, RequiredCount},
    ensure,
    inconsistency::{InconsistencyProof, SliverOrInconsistencyProof},
    merkle::{DIGEST_LEN, MerkleAuth, MerkleProof, MerkleTree, Node},
    metadata::{BlobMetadata, BlobMetadataApi as _, SliverPairMetadata},
    utils,
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
        let encoding_config_for_type = encoding_config.get_for_type(metadata.encoding_type());
        ensure!(
            self.index.as_usize() < metadata.hashes().len(),
            SliverVerificationError::IndexTooLarge
        );
        ensure!(
            self.has_correct_length(&encoding_config_for_type, metadata.unencoded_length()),
            SliverVerificationError::SliverSizeMismatch
        );
        ensure!(
            Ok(self.symbols.symbol_size()) == metadata.symbol_size(encoding_config),
            SliverVerificationError::SymbolSizeMismatch
        );
        self.check_hash(&encoding_config_for_type, metadata)
    }

    /// Checks that the hash of the sliver matches the hash in the metadata.
    ///
    /// This assumes that all relevant size checks have already been performed.
    pub(crate) fn check_hash(
        &self,
        encoding_config: &EncodingConfigEnum,
        metadata: &BlobMetadata,
    ) -> Result<(), SliverVerificationError> {
        let pair_metadata = metadata
            .hashes()
            .get(
                self.index
                    .to_pair_index::<T>(encoding_config.n_shards())
                    .as_usize(),
            )
            .expect("hash must exist if all size checks have been performed");
        ensure!(
            self.get_merkle_root::<Blake2b256>(encoding_config)
                .expect("encoding must work if all size checks have been performed")
                == *pair_metadata.hash::<T>(),
            SliverVerificationError::MerkleRootMismatch
        );
        Ok(())
    }

    /// Returns true iff the sliver has the length expected based on the encoding configuration and
    /// blob size.
    fn has_correct_length(&self, config: &EncodingConfigEnum, blob_size: u64) -> bool {
        self.expected_length(config, blob_size).is_some_and(|l| {
            self.len() == usize::try_from(l).expect("we assume at least a 32-bit architecture")
        })
    }

    fn expected_length(&self, config: &EncodingConfigEnum, blob_size: u64) -> Option<u32> {
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
    /// Returns a [`RecoverySymbolError::EncodeError`] if the `symbols` cannot be encoded.
    pub fn recovery_symbols(
        &self,
        config: &EncodingConfigEnum,
    ) -> Result<Symbols, RecoverySymbolError> {
        let symbol_list = config.encode_all_symbols::<T::OrthogonalAxis>(self.symbols.data())?;
        assert!(!symbol_list.is_empty(), "must be at least 1 symbol");
        assert!(!symbol_list[0].is_empty(), "symbols must have data");

        let symbol_size = self.symbols.symbol_size(); // Symbol size does not vary when re-encoding.
        let data_length = symbol_list.len() * usize::from(symbol_size.get());

        let mut flattened: Vec<u8> = Vec::with_capacity(data_length);
        for symbol in symbol_list {
            // Vec's specialize this to use a memcpy since symbol is `&[T: Copy]`.
            flattened.extend_from_slice(&symbol);
        }

        Ok(Symbols::new(flattened, symbol_size))
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
    /// Returns a [`RecoverySymbolError::EncodeError`] if the sliver cannot be encoded. Returns a
    /// [`RecoverySymbolError::IndexTooLarge`] error if `target_pair_index >= n_shards`.
    pub fn recovery_symbol_for_sliver(
        &self,
        target_pair_index: SliverPairIndex,
        config: &EncodingConfigEnum,
    ) -> Result<RecoverySymbol<T::OrthogonalAxis, MerkleProof<Blake2b256>>, RecoverySymbolError>
    {
        Self::check_index(target_pair_index.into(), config.n_shards())?;

        let recovery_symbols = self.recovery_symbols(config)?;
        let target_sliver_index =
            target_pair_index.to_sliver_index::<T::OrthogonalAxis>(config.n_shards());

        Ok(recovery_symbols
            .decoding_symbol_at(target_sliver_index.as_usize(), self.index.into())
            .expect("we have exactly `n_shards` symbols and the bound was checked")
            .with_proof(
                MerkleTree::<Blake2b256>::build(recovery_symbols.to_symbols())
                    .get_proof(target_sliver_index.as_usize())
                    .expect("bound already checked above"),
            ))
    }

    /// Gets the decoding symbol for a specific target sliver starting from the current sliver.
    ///
    /// The `target_pair_index` is the index of the [`SliverPair`] to which the sliver to be
    /// recovered belongs.
    ///
    /// # Errors
    ///
    /// Returns a [`RecoverySymbolError::EncodeError`] if the sliver cannot be encoded. Returns a
    /// [`RecoverySymbolError::IndexTooLarge`] error if `target_pair_index >= n_shards`.
    pub fn decoding_symbol_for_sliver(
        &self,
        target_pair_index: SliverPairIndex,
        config: &EncodingConfigEnum,
    ) -> Result<DecodingSymbol<T::OrthogonalAxis>, RecoverySymbolError> {
        Self::check_index(target_pair_index.into(), config.n_shards())?;
        let target_sliver_index =
            target_pair_index.to_sliver_index::<T::OrthogonalAxis>(config.n_shards());

        Ok(DecodingSymbol::<T::OrthogonalAxis>::new(
            self.index.get(),
            config.encode_symbol::<T::OrthogonalAxis>(
                self.symbols.data(),
                target_sliver_index.into(),
            )?,
        ))
    }

    /// Recovers a [`SliverData`] from the provided recovery symbols.
    ///
    /// Returns the recovered [`SliverData`] if decoding succeeds or `None` if decoding fails.
    ///
    /// Does *not perform any checks* on the provided symbols; in particular, it does not verify any
    /// proofs.
    pub fn recover_sliver_without_verification<I, U>(
        recovery_symbols: I,
        target_index: SliverIndex,
        symbol_size: NonZeroU16,
        config: EncodingConfigEnum,
    ) -> Result<Self, DecodeError>
    where
        I: IntoIterator,
        I::IntoIter: Iterator<Item = RecoverySymbol<T, U>> + ExactSizeIterator,
        U: MerkleAuth,
    {
        let recovery_symbols = recovery_symbols.into_iter();

        // Note: The following code may have to be changed if we add encodings that require a
        // variable number of symbols to recover a sliver.
        let RequiredCount::Exact(n_symbols_required) = config.n_symbols_for_recovery::<T>();
        if recovery_symbols.len() < n_symbols_required {
            // We don't even have to attempt decoding if we don't have enough recovery symbols.
            return Err(DecodeError::DecodingUnsuccessful);
        }

        config
            .decode_from_decoding_symbols(
                symbol_size,
                recovery_symbols.map(RecoverySymbol::into_decoding_symbol),
            )
            .map(|data| SliverData::new(data, symbol_size, target_index))
    }

    /// Attempts to recover a sliver from the provided recovery symbols.
    ///
    /// The function *does not* verify the recovery symbols. It is the caller's responsibility to
    /// ensure that the recovery symbols have been verified to be useable to recover the identified
    /// symbol.
    ///
    /// It is also the caller's responsibility to ensure that the number of recovery symbols is
    /// sufficient to recover the sliver (an error is returned if this is not the case).
    ///
    /// If recovery succeeds an the recovered sliver is verified successfully against the metadata,
    /// that sliver is returned. If the recovered sliver is inconsistent with the metadata, an
    /// [`InconsistencyProof`] is generated and returned. If recovery fails or another error occurs,
    /// a [`SliverRecoveryError`] is returned.
    pub fn recover_sliver_or_generate_inconsistency_proof<I, U>(
        verified_recovery_symbols: I,
        target_index: SliverIndex,
        metadata: &BlobMetadata,
        encoding_config: &EncodingConfig,
    ) -> Result<SliverOrInconsistencyProof<T, U>, SliverRecoveryOrVerificationError>
    where
        I: IntoIterator,
        I::IntoIter: Iterator<Item = RecoverySymbol<T, U>>,
        U: MerkleAuth,
    {
        let symbol_size = metadata.symbol_size(encoding_config)?;
        let config_enum = encoding_config.get_for_type(metadata.encoding_type());

        // Note: The following code may have to be changed if we add encodings that require a
        // variable number of symbols to recover a sliver.
        let RequiredCount::Exact(n_symbols_required) = config_enum.n_symbols_for_recovery::<T>();
        let verified_recovery_symbols: Vec<_> = verified_recovery_symbols
            .into_iter()
            .take(n_symbols_required)
            .collect();

        let sliver = Self::recover_sliver_without_verification(
            verified_recovery_symbols.clone(),
            target_index,
            symbol_size,
            config_enum,
        )
        .map_err(|_| SliverRecoveryError::DecodingFailure)?;

        match sliver.verify(encoding_config, metadata) {
            Ok(()) => Ok(sliver.into()),
            Err(SliverVerificationError::MerkleRootMismatch) => {
                Ok(InconsistencyProof::new(target_index, verified_recovery_symbols).into())
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
    /// Returns an [`RecoverySymbolError::EncodeError`] if the `symbols` cannot be encoded.
    pub fn get_merkle_root<U: HashFunction<DIGEST_LEN>>(
        &self,
        config: &EncodingConfigEnum,
    ) -> Result<Node, RecoverySymbolError> {
        Ok(MerkleTree::<U>::build(self.recovery_symbols(config)?.to_symbols()).root())
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
        config: EncodingConfigEnum,
        symbol_size: NonZeroU16,
        index: SliverPairIndex,
    ) -> Self {
        SliverPair {
            primary: SliverData::new_empty(
                config.n_secondary_source_symbols().get(),
                symbol_size,
                index.into(),
            ),
            secondary: SliverData::new_empty(
                config.n_primary_source_symbols().get(),
                symbol_size,
                index.to_sliver_index::<Secondary>(config.n_shards()),
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
    /// Returns a [`RecoverySymbolError::IndexTooLarge`] error if `target_pair_index >= n_shards`.
    pub fn recovery_symbol_pair_for_sliver(
        &self,
        target_pair_index: SliverPairIndex,
        config: &EncodingConfigEnum,
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
    pub fn pair_leaf_input<T: HashFunction<DIGEST_LEN>>(
        &self,
        config: &EncodingConfigEnum,
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
    use walrus_test_utils::{Result, param_test, random_subset};

    use super::*;
    use crate::{
        EncodingType,
        encoding::{DecodingSymbol, InvalidDataSizeError, ReedSolomonEncodingConfig},
        test_utils,
    };

    fn create_config_and_encode_pairs_and_get_metadata(
        source_symbols_primary: u16,
        source_symbols_secondary: u16,
        n_shards: u16,
        encoding_type: EncodingType,
        blob: &[u8],
    ) -> (EncodingConfig, Vec<SliverPair>, BlobMetadata) {
        let config = EncodingConfig::new_for_test(
            source_symbols_primary,
            source_symbols_secondary,
            n_shards,
        );
        let (pairs, metadata) = config
            .get_for_type(encoding_type)
            .encode_with_metadata(blob)
            .unwrap();
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
            square_two_byte_symbol: (EncodingType::RS2, 2, 2, &[1,2,3,4,5,6,7,8]),
            rectangle_two_byte_symbol: (
                EncodingType::RS2, 2, 3, &[1,2,3,4,5,6,7,8,9,10,11,12]
            ),
        ]
    }
    fn test_create_recovery_symbols(
        encoding_type: EncodingType,
        source_symbols_primary: u16,
        source_symbols_secondary: u16,
        blob: &[u8],
    ) -> Result {
        let n_shards = 3 * (source_symbols_primary + source_symbols_secondary);
        let (config, pairs, metadata) = create_config_and_encode_pairs_and_get_metadata(
            source_symbols_primary,
            source_symbols_secondary,
            n_shards,
            encoding_type,
            blob,
        );
        let config_enum = config.get_for_type(metadata.encoding_type());

        // Check that the whole expansion is correct.
        for (index, sliver) in pairs.iter().enumerate() {
            // Check the expansion of the primary sliver.
            let expanded_primary = sliver.primary.recovery_symbols(&config_enum)?;
            for (col, other_pair) in pairs.iter().rev().enumerate() {
                let rec = other_pair.secondary.recovery_symbols(&config_enum)?;
                assert_eq!(expanded_primary[col], rec[index]);
            }
            // Check the expansion of the secondary sliver.
            let expanded_secondary = sliver.secondary.recovery_symbols(&config_enum)?;
            for (row, other_pair) in pairs.iter().enumerate() {
                let rec = other_pair.primary.recovery_symbols(&config_enum)?;
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
            no_symbols: (&[], 2, 2),
            empty_symbols: (&[&[], &[]], 2, 2),
            too_few_symbols: (&[&[1,2]], 2, 2),
            inconsistent_size: (&[&[1,2],&[3]], 2, 2),
            inconsistent_and_empty_size: (&[&[1],&[]], 2, 2),
        ]
    }
    // Only testing for failures in the decoding. The correct decoding is tested below.
    fn test_recover_sliver_failure(symbols: &[&[u8]], n_source_symbols: u16, symbol_size: u16) {
        let config = ReedSolomonEncodingConfig::new_for_test(
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
            config.into(),
        );
        assert!(recovered.is_err());
    }

    param_test! {
        test_recover_sliver_from_symbols -> Result: [
            square_one_byte_symbol: (EncodingType::RS2, 2, 2, &[1,2,3,4]),
            square_two_byte_symbol: (EncodingType::RS2, 2, 2, &[1,2,3,4,5,6,7,8]),
            rectangle_two_byte_symbol_1: (
                EncodingType::RS2, 2, 3, &[1,2,3,4,5,6,7,8,9,10,11,12]
            ),
            rectangle_two_byte_symbol_2: (
                EncodingType::RS2, 4, 2, &[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]
            ),
            rectangle_two_byte_symbol_3: (
                EncodingType::RS2, 4, 2, &[11,20,3,13,5,110,77,17,111,56,11,0,0,14,15,1]
            ),
        ]
    }
    fn test_recover_sliver_from_symbols(
        encoding_type: EncodingType,
        source_symbols_primary: u16,
        source_symbols_secondary: u16,
        blob: &[u8],
    ) -> Result {
        let n_shards = 3 * (source_symbols_primary + source_symbols_secondary);
        let (encoding_config, pairs, metadata) = create_config_and_encode_pairs_and_get_metadata(
            source_symbols_primary,
            source_symbols_secondary,
            n_shards,
            encoding_type,
            blob,
        );
        let n_to_recover_from = source_symbols_primary.max(source_symbols_secondary).into();
        let encoding_config_enum = encoding_config.get_for_type(metadata.encoding_type());
        let symbol_size = metadata.symbol_size(&encoding_config)?;

        for pair in pairs.iter() {
            // Get a random subset of recovery symbols.
            let recovery_symbols: Vec<_> = random_subset(
                pairs.iter().map(|p| {
                    p.recovery_symbol_pair_for_sliver(pair.index(), &encoding_config_enum)
                        .unwrap()
                }),
                n_to_recover_from,
            )
            .collect();

            // Recover the primary sliver.
            let recovered = SliverData::recover_sliver_without_verification(
                recovery_symbols.iter().map(|s| s.primary.clone()),
                pair.primary.index,
                symbol_size,
                encoding_config_enum,
            )
            .unwrap();
            assert_eq!(recovered, pair.primary);

            // Recover the secondary sliver.
            let recovered = SliverData::recover_sliver_without_verification(
                recovery_symbols.iter().map(|s| s.secondary.clone()),
                pair.secondary.index,
                symbol_size,
                encoding_config_enum,
            )
            .unwrap();
            assert_eq!(recovered, pair.secondary);
        }
        Ok(())
    }

    param_test! {
        test_recovery_symbols_empty_sliver: [
            primary: <Primary>(EncodingType::RS2),
            secondary: <Secondary>(EncodingType::RS2),
        ]
    }
    fn test_recovery_symbols_empty_sliver<T: EncodingAxis>(encoding_type: EncodingType) {
        let config = EncodingConfig::new_for_test(3, 3, 10);
        assert_eq!(
            SliverData::<T>::new([], 1.try_into().unwrap(), SliverIndex::new(0))
                .recovery_symbols(&config.get_for_type(encoding_type)),
            Err(InvalidDataSizeError::EmptyData.into())
        );
    }

    param_test! {
        test_recover_all_slivers_from_f_plus_1: [
            recover_empty: (EncodingType::RS2, 3, &[]),
            recover_single_byte: (EncodingType::RS2, 3, &[1]),
            recover_one_byte_symbol: (EncodingType::RS2, 3, &[
                1,2,3,4,5,6,7,8,9,
                1,2,3,4,5,6,7,8,9,
            ]),
            recover_two_byte_symbol: (EncodingType::RS2, 3, &[
                1,2,3,4,5,6,7,8,9,
                1,2,3,4,5,6,7,8,9,
                1,2,3,4,5,6,7,8,9,
                1,2,3,4,5,6,7,8,9,
            ]),
        ]
    }
    fn test_recover_all_slivers_from_f_plus_1(encoding_type: EncodingType, f: u16, blob: &[u8]) {
        let n_shards = 3 * f + 1;
        let (encoding_config, pairs, metadata) = create_config_and_encode_pairs_and_get_metadata(
            f,
            2 * f,
            n_shards,
            encoding_type,
            blob,
        );
        let encoding_config_enum = encoding_config.get_for_type(metadata.encoding_type());
        let symbol_size = metadata.symbol_size(&encoding_config).unwrap();

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
                SliverData::<Secondary>::recover_sliver_without_verification(
                    primary_slivers.iter().map(|p| {
                        p.recovery_symbol_for_sliver(index, &encoding_config_enum)
                            .unwrap()
                    }),
                    SliverIndex(n_shards - 1 - target_index),
                    symbol_size,
                    encoding_config_enum,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();

        // Recover the missing primary slivers from 2f+1 of the reconstructed secondary slivers.
        primary_slivers.extend((to_reconstruct_from..n_shards).map(|target_index| {
            let index = SliverPairIndex(target_index);
            SliverData::<Primary>::recover_sliver_without_verification(
                secondary_slivers
                    .iter()
                    .take(
                        encoding_config_enum
                            .n_secondary_source_symbols()
                            .get()
                            .into(),
                    )
                    .map(|s| {
                        s.recovery_symbol_for_sliver(index, &encoding_config_enum)
                            .unwrap()
                    }),
                index.into(),
                symbol_size,
                encoding_config_enum,
            )
            .unwrap()
        }));

        // Check that the reconstructed slivers match the original pairs.
        assert!(
            pairs
                .iter()
                .enumerate()
                .all(|(index, pair)| pair.primary == primary_slivers[index]
                    && pair.secondary == secondary_slivers[index])
        );
    }

    param_test! {
        test_recovery_symbol_proof: [
            two_byte_symbol: (EncodingType::RS2, &[1,2,3,4], 2, 2),
            four_byte_symbol: (EncodingType::RS2, &[1,2,3,4,5,6,7,8], 2, 4),
        ]
    }
    fn test_recovery_symbol_proof(
        encoding_type: EncodingType,
        slice: &[u8],
        f: u16,
        symbol_size: u16,
    ) {
        let n_shards = 3 * f + 1;
        let config = EncodingConfig::new_for_test(f, 2 * f, n_shards);
        let config = config.get_for_type(encoding_type);
        let sliver =
            SliverData::<Secondary>::new(slice, symbol_size.try_into().unwrap(), SliverIndex(0));
        let merkle_tree =
            MerkleTree::<Blake2b256>::build(sliver.recovery_symbols(&config).unwrap().to_symbols());
        for index in 0..n_shards {
            let shard_pair_index = SliverPairIndex(index);
            assert!(
                sliver
                    .recovery_symbol_for_sliver(shard_pair_index, &config)
                    .unwrap()
                    .verify_proof(&merkle_tree.root(), n_shards.into(), index.into())
                    .is_ok()
            );
        }
    }
}
