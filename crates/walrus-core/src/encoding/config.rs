// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use alloc::vec::Vec;
use core::num::{NonZeroU16, NonZeroU32};

use enum_dispatch::enum_dispatch;

use super::{
    BlobDecoder,
    BlobEncoder,
    DataTooLargeError,
    DecodingSymbol,
    EncodeError,
    EncodingAxis,
    ReedSolomonDecoder,
    ReedSolomonEncoder,
    SliverPair,
    basic_encoding::Decoder as _,
    utils,
};
use crate::{
    BlobId,
    EncodingType,
    bft,
    encoding::{DecodeError, SliverData},
    merkle::DIGEST_LEN,
    metadata::VerifiedBlobMetadataWithId,
};

/// The maximum number of source symbols that can be encoded with our encoding (currently
/// Reed-Solomon). This is dictated by [`reed_solomon_simd::engine::GF_ORDER`].
pub const MAX_SOURCE_SYMBOLS: u16 = u16::MAX;

/// Trait for encoding functionality, including configuration, encoding, and decoding.
#[enum_dispatch]
pub trait EncodingFactory {
    /// The encoding type associated with this factory.
    fn encoding_type(&self) -> EncodingType;

    /// The maximum symbol size associated with this factory.
    fn max_symbol_size(&self) -> u16 {
        self.encoding_type().max_symbol_size()
    }

    /// Returns a vector of all `n_shards` source and repair symbols for a single 1D encoding.
    fn encode_all_symbols<E: EncodingAxis>(&self, data: &[u8])
    -> Result<Vec<Vec<u8>>, EncodeError>;

    /// Returns a vector of all repair symbols for a single 1D encoding.
    fn encode_all_repair_symbols<E: EncodingAxis>(
        &self,
        data: &[u8],
    ) -> Result<Vec<Vec<u8>>, EncodeError>;

    /// Returns the symbol at the given index.
    ///
    /// This may or may not be more efficient than doing a full encoding.
    fn encode_symbol<E: EncodingAxis>(
        &self,
        data: &[u8],
        index: u16,
    ) -> Result<Vec<u8>, EncodeError>;

    /// Attempts to decode the source data from the provided iterator over
    /// [`DecodingSymbol`s][DecodingSymbol].
    ///
    /// Returns the source data as a byte vector if decoding succeeds or `None` if decoding fails.
    fn decode_from_decoding_symbols<T, E>(
        &self,
        symbol_size: NonZeroU16,
        symbols: T,
    ) -> Result<Vec<u8>, DecodeError>
    where
        T: IntoIterator,
        T::IntoIter: Iterator<Item = DecodingSymbol<E>>,
        E: EncodingAxis;

    /// Encodes the blob with which `self` was created to a vector of [`SliverPair`s][SliverPair],
    /// and provides the relative [`VerifiedBlobMetadataWithId`].
    ///
    /// This function operates on the fully expanded message matrix for the blob. This matrix is
    /// used to compute the Merkle trees for the metadata, and to extract the sliver pairs. The
    /// returned blob metadata is considered to be verified as it is directly built from the data.
    ///
    /// # Panics
    ///
    /// This function can panic if there is insufficient virtual memory for the encoded data,
    /// notably on 32-bit architectures. As there is an expansion factor of approximately 4.5, blobs
    /// larger than roughly 800 MiB cannot be encoded on 32-bit architectures.
    fn encode_with_metadata(
        &self,
        blob: &[u8],
    ) -> Result<(Vec<SliverPair>, VerifiedBlobMetadataWithId), DataTooLargeError>;

    /// Computes the metadata (blob ID, hashes) for the blob, without returning the slivers.
    fn compute_metadata(
        &self,
        blob: &[u8],
    ) -> Result<VerifiedBlobMetadataWithId, DataTooLargeError>;

    /// Attempts to decode the source blob from the provided slivers.
    ///
    /// Returns the source blob as a byte vector if decoding succeeds or `None` if decoding fails.
    ///
    /// # Panics
    ///
    /// This function can panic if there is insufficient virtual memory for the decoded blob in
    /// addition to the slivers, notably on 32-bit architectures.
    fn decode<S, E>(&self, blob_size: u64, slivers: S) -> Result<Vec<u8>, DecodeError>
    where
        S: IntoIterator<Item = SliverData<E>>,
        E: EncodingAxis;

    /// Attempts to decode the source blob from the provided slivers, and to verify that the decoded
    /// blob matches the blob ID.
    ///
    /// If the decoding and the checks are successful, the function returns a tuple of two values:
    /// * the reconstructed source blob as a byte vector; and
    /// * the [`VerifiedBlobMetadataWithId`] corresponding to the source blob.
    ///
    /// # Errors
    ///
    /// Returns a [`DecodeError::DecodingUnsuccessful`] if the decoding fails.
    ///
    /// If, upon successful decoding, the recomputed blob ID does not match the input blob ID,
    /// returns a [`DecodeError::VerificationError`].
    ///
    /// # Panics
    ///
    /// This function can panic if there is insufficient virtual memory for the encoded data,
    /// notably on 32-bit architectures. As this function re-encodes the blob to verify the
    /// metadata, similar limits apply as in [`BlobEncoder::encode_with_metadata`].
    fn decode_and_verify<S, E>(
        &self,
        blob_id: &BlobId,
        blob_size: u64,
        slivers: S,
    ) -> Result<(Vec<u8>, VerifiedBlobMetadataWithId), DecodeError>
    where
        S: IntoIterator<Item = SliverData<E>>,
        E: EncodingAxis;

    /// Returns the number of primary source symbols as a `NonZeroU16`.
    fn n_primary_source_symbols(&self) -> NonZeroU16;

    /// Returns the number of secondary source symbols as a `NonZeroU16`.
    fn n_secondary_source_symbols(&self) -> NonZeroU16;

    /// Returns the number of shards as a `NonZeroU16`.
    fn n_shards(&self) -> NonZeroU16;

    /// Returns the number of source symbols configured for this type.
    #[inline]
    fn n_source_symbols<T: EncodingAxis>(&self) -> NonZeroU16 {
        if T::IS_PRIMARY {
            self.n_primary_source_symbols()
        } else {
            self.n_secondary_source_symbols()
        }
    }

    /// Returns the number of valid symbols required to recover a sliver of [`EncodingAxis`] `A`.
    #[inline]
    fn n_symbols_for_recovery<A: EncodingAxis>(&self) -> RequiredCount {
        self.n_slivers_for_reconstruction::<A::OrthogonalAxis>()
    }

    /// Returns the number of slivers of [`EncodingAxis`] `A` required to reconstruct a blob.
    #[inline]
    fn n_slivers_for_reconstruction<A: EncodingAxis>(&self) -> RequiredCount {
        RequiredCount::Exact(self.n_source_symbols::<A>().get().into())
    }

    /// Returns the number of shards as a `usize`.
    #[inline]
    fn n_shards_as_usize(&self) -> usize {
        self.n_shards().get().into()
    }

    /// The maximum size in bytes of data that can be encoded with this encoding.
    #[inline]
    fn max_data_size<T: EncodingAxis>(&self) -> u32 {
        u32::from(self.n_source_symbols::<T>().get()) * u32::from(self.max_symbol_size())
    }

    /// The maximum size in bytes of a blob that can be encoded.
    ///
    /// See [`max_blob_size_for_n_shards`] for additional documentation.
    #[inline]
    fn max_blob_size(&self) -> u64 {
        max_blob_size_for_n_shards(self.n_shards(), self.encoding_type())
    }

    /// The number of symbols a blob is split into.
    #[inline]
    fn source_symbols_per_blob(&self) -> NonZeroU32 {
        utils::source_symbols_per_blob(
            self.n_primary_source_symbols(),
            self.n_secondary_source_symbols(),
        )
    }

    /// The symbol size when encoding a blob of size `blob_size`.
    ///
    /// # Errors
    ///
    /// Returns a [`DataTooLargeError`] if the computed symbol size is larger than the maximum
    /// symbol size.
    #[inline]
    fn symbol_size_for_blob(&self, blob_size: u64) -> Result<NonZeroU16, DataTooLargeError> {
        utils::compute_symbol_size(
            blob_size,
            self.source_symbols_per_blob(),
            self.encoding_type().required_alignment(),
        )
    }

    /// The symbol size when encoding a blob of size `blob_size`.
    ///
    /// # Errors
    ///
    /// Returns a [`DataTooLargeError`] if the computed symbol size is larger than the maximum
    /// symbol size.
    #[inline]
    fn symbol_size_for_blob_from_nonzero(
        &self,
        blob_size: u64,
    ) -> Result<NonZeroU16, DataTooLargeError> {
        utils::compute_symbol_size(
            blob_size,
            self.source_symbols_per_blob(),
            self.encoding_type().required_alignment(),
        )
    }

    /// The symbol size when encoding a blob of size `blob_size`.
    ///
    /// # Errors
    ///
    /// Returns a[`DataTooLargeError`] if the data_length cannot be converted to a `u64` or
    /// the computed symbol size is larger than the maximum symbol size.
    #[inline]
    fn symbol_size_for_blob_from_usize(
        &self,
        blob_size: usize,
    ) -> Result<NonZeroU16, DataTooLargeError> {
        utils::compute_symbol_size_from_usize(
            blob_size,
            self.source_symbols_per_blob(),
            self.encoding_type().required_alignment(),
        )
    }

    /// The size (in bytes) of a sliver corresponding to a blob of size `blob_size`.
    ///
    /// Returns a [`DataTooLargeError`] `blob_size > self.max_blob_size()`.
    #[inline]
    fn sliver_size_for_blob<T: EncodingAxis>(
        &self,
        blob_size: u64,
    ) -> Result<NonZeroU32, DataTooLargeError> {
        NonZeroU32::from(self.n_source_symbols::<T::OrthogonalAxis>())
            .checked_mul(self.symbol_size_for_blob(blob_size)?.into())
            .ok_or(DataTooLargeError)
    }

    /// Computes the length of a blob of given `unencoded_length`, once encoded.
    ///
    /// See [`encoded_blob_length_for_n_shards`] for additional documentation.
    #[inline]
    fn encoded_blob_length(&self, unencoded_length: u64) -> Option<u64> {
        encoded_blob_length_for_n_shards(self.n_shards(), unencoded_length, self.encoding_type())
    }

    /// Computes the length of a blob of given `unencoded_length`, once encoded.
    ///
    /// Same as [`Self::encoded_blob_length`], but taking a `usize` as input.
    #[inline]
    fn encoded_blob_length_from_usize(&self, unencoded_length: usize) -> Option<u64> {
        self.encoded_blob_length(unencoded_length.try_into().ok()?)
    }

    /// Computes the length of the metadata produced by this encoding config.
    ///
    /// This is independent of the blob size.
    #[inline]
    fn metadata_length(&self) -> u64 {
        metadata_length_for_n_shards(self.n_shards())
    }

    /// Returns the maximum size of a sliver for the current configuration.
    ///
    /// This is the size of a primary sliver with `u16::MAX` symbol size.
    #[inline]
    fn max_sliver_size(&self) -> u64 {
        max_sliver_size_for_n_secondary(self.n_secondary_source_symbols(), self.encoding_type())
    }
}

/// The number of symbols required to recover a sliver.
///
/// For our current RS2 encoding, which is based on Reed-Solomon codes, the number of symbols
/// required to recover a sliver is exactly the number of source symbols. This could be different
/// for future encodings that use other types of erasure codes.
// Important: Currently, quite some code assumes that the number of symbols required to recover a
// sliver is an exact number. This code needs to be changed if this enum is extended in the future.
// By explicitly matching on `Exact`, we ensure that the compiler will warn us in that case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequiredCount {
    /// An exact number of symbols required to recover a sliver.
    Exact(usize),
}

/// Configuration parameters for the encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodingConfig {
    /// The number of shards.
    pub(crate) n_shards: NonZeroU16,
    /// The Reed-Solomon encoding config.
    pub reed_solomon: ReedSolomonEncodingConfig,
}

impl EncodingConfig {
    /// Creates a new encoding config, given the number of shards.
    ///
    /// The number of shards determines the the appropriate number of primary and secondary source
    /// symbols.
    ///
    /// # Panics
    ///
    /// Panics if the number of shards causes the number of primary or secondary source symbols
    /// to be larger than [`MAX_SOURCE_SYMBOLS`].
    pub fn new(n_shards: NonZeroU16) -> Self {
        Self {
            n_shards,
            reed_solomon: ReedSolomonEncodingConfig::new(n_shards),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(
        source_symbols_primary: u16,
        source_symbols_secondary: u16,
        n_shards: u16,
    ) -> Self {
        Self {
            n_shards: NonZeroU16::new(n_shards).expect("n_shards must be non-zero"),
            reed_solomon: ReedSolomonEncodingConfig::new_for_test(
                source_symbols_primary,
                source_symbols_secondary,
                n_shards,
            ),
        }
    }

    /// Returns the encoding config for the given encoding type wrapped as an
    /// [`EncodingConfigEnum`].
    pub fn get_for_type(&self, encoding_type: EncodingType) -> EncodingConfigEnum<'_> {
        match encoding_type {
            EncodingType::RS2 => EncodingConfigEnum::ReedSolomon(&self.reed_solomon),
            _ => panic!("unsupported encoding type: {:?}", encoding_type),
        }
    }

    /// Returns the number of shards.
    pub fn n_shards(&self) -> NonZeroU16 {
        self.n_shards
    }
}

#[enum_dispatch(EncodingFactory)]
#[derive(Debug, Clone, PartialEq, Eq)]
/// A wrapper around the encoding config for different encoding types.
pub enum EncodingConfigEnum<'a> {
    /// Configuration of the Reed-Solomon encoding.
    ReedSolomon(&'a ReedSolomonEncodingConfig),
}

/// Configuration of the Reed-Solomon encoding.
///
/// This consists of the number of source symbols for the two encodings and the total number of
/// shards.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReedSolomonEncodingConfig {
    /// The number of source symbols for the primary encoding, which is, simultaneously, the number
    /// of symbols per secondary sliver. It must be at most `n_shards - 2f`, where `f` is
    /// the Byzantine parameter.
    pub(crate) source_symbols_primary: NonZeroU16,
    /// The number of source symbols for the secondary encoding, which is, simultaneously, the
    /// number of symbols per primary sliver. It must be at most `n_shards - f`, where `f`
    /// is the Byzantine parameter.
    pub(crate) source_symbols_secondary: NonZeroU16,
    /// The number of shards.
    pub(crate) n_shards: NonZeroU16,
}

impl ReedSolomonEncodingConfig {
    const ENCODING_TYPE: EncodingType = EncodingType::RS2;

    /// Creates a new encoding config, given the number of shards.
    ///
    /// The number of shards determines the the appropriate number of primary and secondary source
    /// symbols.
    ///
    /// # Panics
    ///
    /// Panics if the number of shards causes the number of primary or secondary source symbols
    /// to be larger than [`MAX_SOURCE_SYMBOLS`].
    pub fn new(n_shards: NonZeroU16) -> Self {
        let (primary_source_symbols, secondary_source_symbols) =
            source_symbols_for_n_shards(n_shards);
        tracing::debug!(
            n_shards,
            primary_source_symbols,
            secondary_source_symbols,
            "creating new encoding config"
        );
        Self::new_from_nonzero_parameters(
            primary_source_symbols,
            secondary_source_symbols,
            n_shards,
        )
    }

    /// Creates a new encoding configuration for the provided system parameters.
    ///
    /// In a setup with `n_shards` total shards -- among which `f` are Byzantine, and
    /// `f < n_shards / 3` -- `source_symbols_primary` is the number of source symbols for the
    /// primary encoding (must be equal to or below `n_shards - 2f`), and `source_symbols_secondary`
    /// is the number of source symbols for the secondary encoding (must be equal to or below
    /// `n_shards - f`).
    ///
    /// # Panics
    ///
    /// Panics if the parameters are inconsistent with Byzantine fault tolerance; i.e., if the
    /// number of source symbols of the primary encoding is equal to or greater than `n_shards -
    /// 2f`, or if the number of source symbols of the secondary encoding equal to or greater than
    /// `n_shards - f` of the number of shards.
    ///
    /// Panics if the number of primary or secondary source symbols is larger than
    /// [`MAX_SOURCE_SYMBOLS`].
    pub(crate) fn new_from_nonzero_parameters(
        source_symbols_primary: NonZeroU16,
        source_symbols_secondary: NonZeroU16,
        n_shards: NonZeroU16,
    ) -> Self {
        let f = bft::max_n_faulty(n_shards);
        assert!(
            source_symbols_primary.get() < MAX_SOURCE_SYMBOLS
                && source_symbols_secondary.get() < MAX_SOURCE_SYMBOLS,
            "the number of source symbols can be at most `MAX_SOURCE_SYMBOLS`"
        );
        assert!(
            source_symbols_secondary.get() <= n_shards.get() - f,
            "the secondary encoding can be at most a n-f encoding"
        );
        assert!(
            source_symbols_primary.get() <= n_shards.get() - 2 * f,
            "the primary encoding can be at most an n-2f encoding"
        );

        Self {
            source_symbols_primary,
            source_symbols_secondary,
            n_shards,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(
        source_symbols_primary: u16,
        source_symbols_secondary: u16,
        n_shards: u16,
    ) -> Self {
        let source_symbols_primary = NonZeroU16::new(source_symbols_primary)
            .expect("the number of source symbols must not be 0");
        let source_symbols_secondary = NonZeroU16::new(source_symbols_secondary)
            .expect("the number of source symbols must not be 0");
        let n_shards = NonZeroU16::new(n_shards).expect("implied by previous checks");

        Self::new_from_nonzero_parameters(
            source_symbols_primary,
            source_symbols_secondary,
            n_shards,
        )
    }

    fn get_encoder<'a, E: EncodingAxis>(
        &'_ self,
        data: &'a [u8],
    ) -> Result<ReedSolomonEncoder<'a>, EncodeError> {
        ReedSolomonEncoder::new(data, self.n_source_symbols::<E>(), self.n_shards())
    }

    fn get_decoder<E: EncodingAxis>(
        &self,
        symbol_size: NonZeroU16,
    ) -> Result<ReedSolomonDecoder, DecodeError> {
        ReedSolomonDecoder::new(self.n_source_symbols::<E>(), self.n_shards(), symbol_size)
    }

    /// Returns a [`BlobEncoder`] for the given blob.
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn get_blob_encoder<'a>(
        &'a self,
        blob: &'a [u8],
    ) -> Result<BlobEncoder<'a>, DataTooLargeError> {
        BlobEncoder::new(self.into(), blob)
    }

    /// Returns a [`BlobDecoder`] for the given `blob_size`.
    pub fn get_blob_decoder<E: EncodingAxis>(
        &self,
        blob_size: u64,
    ) -> Result<BlobDecoder<'_, ReedSolomonDecoder, E>, DecodeError> {
        BlobDecoder::new(self, blob_size)
    }
}

impl EncodingFactory for &ReedSolomonEncodingConfig {
    #[inline]
    fn n_primary_source_symbols(&self) -> NonZeroU16 {
        self.source_symbols_primary
    }

    #[inline]
    fn n_secondary_source_symbols(&self) -> NonZeroU16 {
        self.source_symbols_secondary
    }

    #[inline]
    fn n_shards(&self) -> NonZeroU16 {
        self.n_shards
    }

    #[inline]
    fn encoding_type(&self) -> EncodingType {
        ReedSolomonEncodingConfig::ENCODING_TYPE
    }

    fn encode_with_metadata(
        &self,
        blob: &[u8],
    ) -> Result<(Vec<SliverPair>, VerifiedBlobMetadataWithId), DataTooLargeError> {
        Ok(self.get_blob_encoder(blob)?.encode_with_metadata())
    }

    fn compute_metadata(
        &self,
        blob: &[u8],
    ) -> Result<VerifiedBlobMetadataWithId, DataTooLargeError> {
        Ok(self.get_blob_encoder(blob)?.compute_metadata())
    }

    fn decode<S, E>(&self, blob_size: u64, slivers: S) -> Result<Vec<u8>, DecodeError>
    where
        S: IntoIterator<Item = SliverData<E>>,
        E: EncodingAxis,
    {
        self.get_blob_decoder::<E>(blob_size)?.decode(slivers)
    }

    fn decode_and_verify<S, E>(
        &self,
        blob_id: &BlobId,
        blob_size: u64,
        slivers: S,
    ) -> Result<(Vec<u8>, VerifiedBlobMetadataWithId), DecodeError>
    where
        S: IntoIterator<Item = SliverData<E>>,
        E: EncodingAxis,
    {
        self.get_blob_decoder::<E>(blob_size)?
            .decode_and_verify(blob_id, slivers)
    }

    fn encode_all_symbols<E: EncodingAxis>(
        &self,
        data: &[u8],
    ) -> Result<Vec<Vec<u8>>, EncodeError> {
        Ok(self.get_encoder::<E>(data)?.encode_all())
    }

    fn encode_all_repair_symbols<E: EncodingAxis>(
        &self,
        data: &[u8],
    ) -> Result<Vec<Vec<u8>>, EncodeError> {
        Ok(self.get_encoder::<E>(data)?.encode_all_repair_symbols())
    }

    /// Returns the symbol at the given index.
    ///
    /// If the index corresponds to a source symbol, the function directly returns the corresponding
    /// data slice without performing any encoding.
    fn encode_symbol<E: EncodingAxis>(
        &self,
        data: &[u8],
        index: u16,
    ) -> Result<Vec<u8>, EncodeError> {
        let symbol_size: usize = ReedSolomonEncoder::check_parameters_and_compute_symbol_size(
            data,
            self.n_source_symbols::<E>(),
        )?
        .get()
        .into();
        if index < self.n_source_symbols::<E>().get() {
            let symbol_start = usize::from(index) * symbol_size;
            Ok(data[symbol_start..symbol_start + symbol_size].to_vec())
        } else {
            Ok(self.get_encoder::<E>(data)?.into_symbol(index))
        }
    }

    fn decode_from_decoding_symbols<T, E>(
        &self,
        symbol_size: NonZeroU16,
        symbols: T,
    ) -> Result<Vec<u8>, DecodeError>
    where
        T: IntoIterator,
        T::IntoIter: Iterator<Item = DecodingSymbol<E>>,
        E: EncodingAxis,
    {
        self.get_decoder::<E::OrthogonalAxis>(symbol_size)?
            .decode(symbols)
    }
}

impl EncodingFactory for ReedSolomonEncodingConfig {
    fn encoding_type(&self) -> EncodingType {
        (&self).encoding_type()
    }

    fn encode_with_metadata(
        &self,
        blob: &[u8],
    ) -> Result<(Vec<SliverPair>, VerifiedBlobMetadataWithId), DataTooLargeError> {
        (&self).encode_with_metadata(blob)
    }

    fn compute_metadata(
        &self,
        blob: &[u8],
    ) -> Result<VerifiedBlobMetadataWithId, DataTooLargeError> {
        (&self).compute_metadata(blob)
    }

    fn decode<S, E>(&self, blob_size: u64, slivers: S) -> Result<Vec<u8>, DecodeError>
    where
        S: IntoIterator<Item = SliverData<E>>,
        E: EncodingAxis,
    {
        (&self).decode(blob_size, slivers)
    }

    fn decode_and_verify<S, E>(
        &self,
        blob_id: &BlobId,
        blob_size: u64,
        slivers: S,
    ) -> Result<(Vec<u8>, VerifiedBlobMetadataWithId), DecodeError>
    where
        S: IntoIterator<Item = SliverData<E>>,
        E: EncodingAxis,
    {
        (&self).decode_and_verify(blob_id, blob_size, slivers)
    }

    fn n_primary_source_symbols(&self) -> NonZeroU16 {
        (&self).n_primary_source_symbols()
    }

    fn n_secondary_source_symbols(&self) -> NonZeroU16 {
        (&self).n_secondary_source_symbols()
    }

    fn n_shards(&self) -> NonZeroU16 {
        (&self).n_shards()
    }

    fn encode_all_symbols<E: EncodingAxis>(
        &self,
        data: &[u8],
    ) -> Result<Vec<Vec<u8>>, EncodeError> {
        (&self).encode_all_symbols::<E>(data)
    }

    fn encode_all_repair_symbols<E: EncodingAxis>(
        &self,
        data: &[u8],
    ) -> Result<Vec<Vec<u8>>, EncodeError> {
        (&self).encode_all_repair_symbols::<E>(data)
    }

    fn encode_symbol<E: EncodingAxis>(
        &self,
        data: &[u8],
        index: u16,
    ) -> Result<Vec<u8>, EncodeError> {
        (&self).encode_symbol::<E>(data, index)
    }

    fn decode_from_decoding_symbols<T, E>(
        &self,
        symbol_size: NonZeroU16,
        symbols: T,
    ) -> Result<Vec<u8>, DecodeError>
    where
        T: IntoIterator,
        T::IntoIter: Iterator<Item = DecodingSymbol<E>>,
        E: EncodingAxis,
    {
        (&self).decode_from_decoding_symbols::<T, E>(symbol_size, symbols)
    }
}

/// Computes the number of primary and secondary source symbols starting from the number of shards.
///
/// The computation is as follows:
/// - `source_symbols_primary = n_shards - 2f`
/// - `source_symbols_secondary = n_shards - f`
#[inline]
pub fn source_symbols_for_n_shards(n_shards: NonZeroU16) -> (NonZeroU16, NonZeroU16) {
    let min_n_correct = bft::min_n_correct(n_shards);
    (
        (min_n_correct.get() - bft::max_n_faulty(n_shards))
            .try_into()
            .expect("implied by BFT computations"),
        min_n_correct,
    )
}

/// Computes the length of the metadata produced for a system with `n_shards` shards.
///
/// This is independent of the blob size.
#[inline]
pub fn metadata_length_for_n_shards(n_shards: NonZeroU16) -> u64 {
    (
        // The hashes.
        usize::from(n_shards.get()) * DIGEST_LEN * 2
        // The blob ID.
        + BlobId::LENGTH
    )
        .try_into()
        .expect("this always fits into a `u64`")
}

/// Returns the maximum size of a sliver for a system with `n_secondary_source_symbols`.
#[inline]
pub fn max_sliver_size_for_n_secondary(
    n_secondary_source_symbols: NonZeroU16,
    encoding_type: EncodingType,
) -> u64 {
    u64::from(n_secondary_source_symbols.get()) * u64::from(encoding_type.max_symbol_size())
}

/// Returns the maximum size of a sliver for a system with `n_shards` shards.
///
/// This is the size of a primary sliver with `u16::MAX` symbol size.
#[inline]
pub fn max_sliver_size_for_n_shards(n_shards: NonZeroU16) -> u64 {
    let (_, secondary) = source_symbols_for_n_shards(n_shards);
    max_sliver_size_for_n_secondary(secondary, EncodingType::RS2)
}

/// The maximum size in bytes of a blob that can be encoded, given the number of shards and the
/// encoding type.
///
/// This is limited by the total number of source symbols, which is fixed by the dimensions
/// `source_symbols_primary` x `source_symbols_secondary` of the message matrix, and the maximum
/// symbol size supported by the encoding type.
///
/// Note that on 32-bit architectures, the actual limit can be smaller than that due to the limited
/// address space.
#[inline]
pub fn max_blob_size_for_n_shards(n_shards: NonZeroU16, encoding_type: EncodingType) -> u64 {
    u64::from(source_symbols_per_blob_for_n_shards(n_shards).get())
        * u64::from(encoding_type.max_symbol_size())
}

#[inline]
fn source_symbols_per_blob_for_n_shards(n_shards: NonZeroU16) -> NonZeroU32 {
    let (source_symbols_primary, source_symbols_secondary) = source_symbols_for_n_shards(n_shards);
    NonZeroU32::from(source_symbols_primary)
        .checked_mul(source_symbols_secondary.into())
        .expect("product of two u16 always fits into a u32")
}

/// Computes the length of a blob of given `unencoded_length` and `n_shards`, once encoded.
///
/// The output length includes the metadata and the blob ID sizes. Returns `None` if the blob
/// size cannot be computed.
///
/// This computation is the same as done by the function of the same name in
/// `contracts/walrus/sources/system/redstuff.move` and should be kept in sync.
#[inline]
pub fn encoded_blob_length_for_n_shards(
    n_shards: NonZeroU16,
    unencoded_length: u64,
    encoding_type: EncodingType,
) -> Option<u64> {
    let slivers_size =
        encoded_slivers_length_for_n_shards(n_shards, unencoded_length, encoding_type)?;
    Some(u64::from(n_shards.get()) * metadata_length_for_n_shards(n_shards) + slivers_size)
}

/// Computes the total length of the slivers for a blob of `unencoded_length` encoded on `n_shards".
///
/// This is the total length of the slivers stored on all shards. The length does not include the
/// metadata and the blob ID. Returns `None` if the blob size cannot be computed.
///
/// This computation is the same as done by the function of the same name in
/// `contracts/walrus/sources/system/redstuff.move` and should be kept in sync.
pub fn encoded_slivers_length_for_n_shards(
    n_shards: NonZeroU16,
    unencoded_length: u64,
    encoding_type: EncodingType,
) -> Option<u64> {
    let (source_symbols_primary, source_symbols_secondary) = source_symbols_for_n_shards(n_shards);
    let single_shard_slivers_size = (u64::from(source_symbols_primary.get())
        + u64::from(source_symbols_secondary.get()))
        * u64::from(
            utils::compute_symbol_size(
                unencoded_length,
                source_symbols_per_blob_for_n_shards(n_shards),
                encoding_type.required_alignment(),
            )
            .ok()?
            .get(),
        );
    Some(u64::from(n_shards.get()) * single_shard_slivers_size)
}

#[cfg(test)]
mod tests {
    use walrus_test_utils::param_test;

    use super::*;
    use crate::encoding::Primary;

    param_test! {
        test_sliver_size_for_blob: [
            zero: (1, Ok(10)),
            one: (1, Ok(10)),
            full_matrix: (30, Ok(10)),
            full_matrix_plus_1: (31, Ok(20)),
            too_large: (
                3 * 5 * u64::from(EncodingType::RS2.max_symbol_size()) + 1,
                Err(DataTooLargeError)
            ),
        ]
    }
    fn test_sliver_size_for_blob(
        blob_size: u64,
        expected_primary_sliver_size: Result<u32, DataTooLargeError>,
    ) {
        assert_eq!(
            ReedSolomonEncodingConfig::new_for_test(3, 5, 10)
                .sliver_size_for_blob::<Primary>(blob_size),
            expected_primary_sliver_size.map(|e| NonZeroU32::new(e).unwrap())
        );
    }

    param_test! {
        test_encoded_size_reed_solomon: [
            zero_small_committee: (0, 10, 10 * (2*(4 + 7) + 10 * 2 * 32 + 32)),
            one_small_committee: (1, 10, 10 * (2*(4 + 7) + 10 * 2 * 32 + 32)),
            #[ignore] one_large_committee:
                (1, 1000, 1000 * (2*(334 + 667) + 1000 * 2 * 32 + 32)),
            larger_blob_small_committee:
                ((4*7)*100, 10, 10 * ((4 + 7) * 100 + 10 * 2 * 32 + 32)),
            #[ignore] larger_blob_large_committee: (
                (334 * 667) * 100,
                1000,
                1000 * ((334 + 667) * 100 + 1000 * 2 * 32 + 32),
            ),

        ]
    }
    /// These tests replicate the tests for `encoded_size_reed_solomon` in
    /// `contracts/walrus/sources/system/redstuff.move` and should be kept in sync.
    fn test_encoded_size_reed_solomon(blob_size: usize, n_shards: u16, expected_encoded_size: u64) {
        assert_eq!(
            ReedSolomonEncodingConfig::new(NonZeroU16::new(n_shards).unwrap())
                .encoded_blob_length_from_usize(blob_size),
            Some(expected_encoded_size),
        );
    }

    param_test! {
        test_source_symbols_for_n_shards: [
            one_rs2: (1, 1, 1),
            three_rs2: (3, 3, 3),
            seven_rs2: (7, 3, 5),
            ten_rs2: (10, 4, 7),
            thirty_one_rs2: (31, 11, 21),
            hundred_rs2: (100, 34, 67),
            three_hundred_and_one_rs2: (301, 101, 201),
            thousand_rs2: (1000, 334, 667),
        ]
    }
    fn test_source_symbols_for_n_shards(
        n_shards: u16,
        expected_primary: u16,
        expected_secondary: u16,
    ) {
        let (actual_primary, actual_secondary) =
            source_symbols_for_n_shards(n_shards.try_into().unwrap());
        assert_eq!(actual_primary.get(), expected_primary);
        assert_eq!(actual_secondary.get(), expected_secondary);
    }

    param_test! {
        test_new_for_n_shards: [
            one: (1, 1, 1),
            three: (3, 3, 3),
            four: (4, 2, 3),
            nine: (9, 5, 7),
            ten: (10, 4, 7),
            fifty_one: (51, 19, 35),
            one_hundred: (100, 34, 67),
            one_hundred_and_one: (101, 35, 68),
        ]
    }
    fn test_new_for_n_shards(n_shards: u16, primary: u16, secondary: u16) {
        let config = ReedSolomonEncodingConfig::new(n_shards.try_into().unwrap());
        assert_eq!(config.source_symbols_primary.get(), primary);
        assert_eq!(config.source_symbols_secondary.get(), secondary);
    }
}
