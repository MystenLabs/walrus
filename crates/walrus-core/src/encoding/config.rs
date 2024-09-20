// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use core::num::{NonZeroU16, NonZeroU32};

use raptorq::SourceBlockEncodingPlan;

use super::{
    utils,
    BlobDecoder,
    BlobEncoder,
    DataTooLargeError,
    Decoder,
    EncodeError,
    Encoder,
    EncodingAxis,
    MAX_SOURCE_SYMBOLS_PER_BLOCK,
    MAX_SYMBOL_SIZE,
};
use crate::{bft, merkle::DIGEST_LEN, BlobId};

/// Configuration of the Walrus encoding.
///
/// This consists of the number of source symbols for the two encodings, the total number of shards,
/// and contains pre-generated encoding plans to speed up encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodingConfig {
    /// The number of source symbols for the primary encoding, which is, simultaneously, the number
    /// of symbols per secondary sliver. It must be strictly less than `n_shards - 2f`, where `f` is
    /// the Byzantine parameter.
    pub(crate) source_symbols_primary: NonZeroU16,
    /// The number of source symbols for the secondary encoding, which is, simultaneously, the
    /// number of symbols per primary sliver.It must be strictly less than `n_shards - f`, where `f`
    /// is the Byzantine parameter.
    pub(crate) source_symbols_secondary: NonZeroU16,
    /// The number of shards.
    pub(crate) n_shards: NonZeroU16,
    /// Encoding plan to speed up the primary encoding.
    encoding_plan_primary: SourceBlockEncodingPlan,
    /// Encoding plan to speed up the secondary encoding.
    encoding_plan_secondary: SourceBlockEncodingPlan,
}

impl EncodingConfig {
    /// Creates a new encoding config, given the number of shards.
    ///
    /// The number of shards determines the the appropriate number of primary and secondary source
    /// symbols.
    ///
    /// The decoding probability is given by the [`decoding_safety_limit`]. See the documentation of
    /// [`decoding_safety_limit`] and [`source_symbols_for_n_shards`] for more details.
    ///
    /// # Panics
    ///
    /// Panics if the number of shards causes the number of primary or secondary source symbols
    /// to be larger than [`MAX_SOURCE_SYMBOLS_PER_BLOCK`].
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
    /// [`MAX_SOURCE_SYMBOLS_PER_BLOCK`].
    pub(crate) fn new_from_nonzero_parameters(
        source_symbols_primary: NonZeroU16,
        source_symbols_secondary: NonZeroU16,
        n_shards: NonZeroU16,
    ) -> Self {
        let f = bft::max_n_faulty(n_shards);
        assert!(
            source_symbols_primary.get() < MAX_SOURCE_SYMBOLS_PER_BLOCK
                && source_symbols_secondary.get() < MAX_SOURCE_SYMBOLS_PER_BLOCK,
            "the number of source symbols can be at most `MAX_SOURCE_SYMBOLS_PER_BLOCK`"
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
            encoding_plan_primary: SourceBlockEncodingPlan::generate(source_symbols_primary.get()),
            encoding_plan_secondary: SourceBlockEncodingPlan::generate(
                source_symbols_secondary.get(),
            ),
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

    /// Returns the number of source symbols configured for this type.
    #[inline]
    pub fn n_source_symbols<T: EncodingAxis>(&self) -> NonZeroU16 {
        if T::IS_PRIMARY {
            self.source_symbols_primary
        } else {
            self.source_symbols_secondary
        }
    }

    /// Returns the number of primary source symbols as a `NonZeroU16`.
    #[inline]
    pub fn n_primary_source_symbols(&self) -> NonZeroU16 {
        self.source_symbols_primary
    }

    /// Returns the number of secondary source symbols as a `NonZeroU16`.
    #[inline]
    pub fn n_secondary_source_symbols(&self) -> NonZeroU16 {
        self.source_symbols_secondary
    }

    /// Returns the number of shards as a `NonZeroU16`.
    #[inline]
    pub fn n_shards(&self) -> NonZeroU16 {
        self.n_shards
    }

    /// Returns the number of shards as a `usize`.
    #[inline]
    pub fn n_shards_as_usize(&self) -> usize {
        self.n_shards.get().into()
    }

    /// Returns the pre-generated encoding plan this type.
    #[inline]
    pub fn encoding_plan<T: EncodingAxis>(&self) -> &SourceBlockEncodingPlan {
        if T::IS_PRIMARY {
            &self.encoding_plan_primary
        } else {
            &self.encoding_plan_secondary
        }
    }

    /// The maximum size in bytes of data that can be encoded with this encoding.
    #[inline]
    pub fn max_data_size<T: EncodingAxis>(&self) -> u32 {
        u32::from(self.n_source_symbols::<T>().get()) * u32::from(MAX_SYMBOL_SIZE)
    }

    /// The maximum size in bytes of a blob that can be encoded.
    ///
    /// See [`max_blob_size_for_n_shards`] for additional documentation.
    #[inline]
    pub fn max_blob_size(&self) -> u64 {
        max_blob_size_for_n_shards(self.n_shards())
    }

    /// The number of symbols a blob is split into.
    #[inline]
    pub fn source_symbols_per_blob(&self) -> NonZeroU32 {
        NonZeroU32::from(self.source_symbols_primary)
            .checked_mul(self.source_symbols_secondary.into())
            .expect("product of two u16 always fits into a u32")
    }

    /// The symbol size when encoding a blob of size `blob_size`.
    ///
    /// # Errors
    ///
    /// Returns a [`DataTooLargeError`] if the computed symbol size is larger than
    /// [`MAX_SYMBOL_SIZE`].
    #[inline]
    pub fn symbol_size_for_blob(&self, blob_size: u64) -> Result<NonZeroU16, DataTooLargeError> {
        utils::compute_symbol_size(blob_size, self.source_symbols_per_blob())
    }

    /// The symbol size when encoding a blob of size `blob_size`.
    ///
    /// # Errors
    ///
    /// Returns a [`DataTooLargeError`] if the computed symbol size is larger than
    /// [`MAX_SYMBOL_SIZE`].
    #[inline]
    pub fn symbol_size_for_blob_from_nonzero(
        &self,
        blob_size: u64,
    ) -> Result<NonZeroU16, DataTooLargeError> {
        utils::compute_symbol_size(blob_size, self.source_symbols_per_blob())
    }

    /// The symbol size when encoding a blob of size `blob_size`.
    ///
    /// # Errors
    ///
    /// Returns a[`DataTooLargeError`] if the data_length cannot be converted to a `u64` or
    /// the computed symbol size is larger than [`MAX_SYMBOL_SIZE`].
    #[inline]
    pub fn symbol_size_for_blob_from_usize(
        &self,
        blob_size: usize,
    ) -> Result<NonZeroU16, DataTooLargeError> {
        utils::compute_symbol_size_from_usize(blob_size, self.source_symbols_per_blob())
    }

    /// The size (in bytes) of a sliver corresponding to a blob of size `blob_size`.
    ///
    /// Returns a [`DataTooLargeError`] `blob_size > self.max_blob_size()`.
    #[inline]
    pub fn sliver_size_for_blob<T: EncodingAxis>(
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
    pub fn encoded_blob_length(&self, unencoded_length: u64) -> Option<u64> {
        encoded_blob_length_for_n_shards(self.n_shards(), unencoded_length)
    }

    /// Computes the length of a blob of given `unencoded_length`, once encoded.
    ///
    /// Same as [`Self::encoded_blob_length`], but taking a `usize` as input.
    #[inline]
    pub fn encoded_blob_length_from_usize(&self, unencoded_length: usize) -> Option<u64> {
        self.encoded_blob_length(unencoded_length.try_into().ok()?)
    }

    /// Computes the length of the metadata produced by this encoding config.
    ///
    /// This is independent of the blob size.
    #[inline]
    pub fn metadata_length(&self) -> u64 {
        metadata_length_for_n_shards(self.n_shards())
    }

    /// Returns the maximum size of a sliver for the current configuration.
    ///
    /// This is the size of a primary sliver with `u16::MAX` symbol size.
    #[inline]
    pub fn max_sliver_size(&self) -> u64 {
        max_sliver_size_for_n_secondary(self.n_secondary_source_symbols())
    }

    /// Returns an [`Encoder`] to perform a single primary or secondary encoding of the data.
    ///
    /// The `data` to be encoded _does not_ have to be aligned/padded. The `encoding_axis` specifies
    /// which encoding parameters the encoder uses, i.e., the parameters for either the primary or
    /// the secondary encoding.
    ///
    /// # Errors
    ///
    /// Returns an [`EncodeError`] if the `data` cannot be encoded. See [`Encoder::new`] for further
    /// details about the returned errors.
    pub fn get_encoder<E: EncodingAxis>(&self, data: &[u8]) -> Result<Encoder, EncodeError> {
        Encoder::new(
            data,
            self.n_source_symbols::<E>(),
            self.n_shards(),
            self.encoding_plan::<E>(),
        )
    }

    /// Returns a [`Decoder`] to perform a single primary or secondary decoding for the provided
    /// `symbol_size`.
    pub fn get_decoder<E: EncodingAxis>(&self, symbol_size: NonZeroU16) -> Decoder {
        Decoder::new(self.n_source_symbols::<E>(), symbol_size)
    }

    /// Returns a [`BlobEncoder`] to encode a blob into [`SliverPair`s][super::SliverPair].
    ///
    /// The `blob` to be encoded does not have to be padded.
    ///
    /// # Errors
    ///
    /// Returns a [`DataTooLargeError`] if the `blob` is too large to be encoded.
    pub fn get_blob_encoder<'a>(
        &'a self,
        blob: &'a [u8],
    ) -> Result<BlobEncoder, DataTooLargeError> {
        BlobEncoder::new(self, blob)
    }

    /// Returns a [`BlobDecoder`] to reconstruct a blob of provided size from either
    /// [`Primary`][super::PrimarySliver] or [`Secondary`][super::SecondarySliver] slivers.
    ///
    /// `blob_size` is the _unencoded_ size (i.e., before encoding) of the blob to be decoded.
    ///
    /// # Errors
    ///
    /// Returns a [`DataTooLargeError`] if the `blob_size` is too large to be decoded.
    pub fn get_blob_decoder<T: EncodingAxis>(
        &self,
        blob_size: u64,
    ) -> Result<BlobDecoder<T>, DataTooLargeError> {
        BlobDecoder::new(self, blob_size)
    }
}

/// Returns the "safety limit" for the encoding.
///
/// The safety limit is the minimum difference between the number of primary source symbols and
/// 1/3rd of the number of shards, and between the number of secondary source symbols and 2/3rds of
/// the number of shards.
///
/// This safety limit ensures that, when collecting symbols for reconstruction or recovery, f+1
/// replies (for primary symbols) or 2f+1 replies (for secondary symbols) from a committee of 3f+1
/// nodes have at least `decoding_safety_limit(n_shards)` redundant symbols to increase the
/// probability of recovery.
///
/// For RaptorQ, the probability of successful reconstruction for K source symbols when K + H
/// symbols are received is greater than `1 - (1 / 256)^(H + 1)`. Therefore, e.g, the probability of
/// reconstruction after receiving f+1 primary slivers is at least
/// `1 - (1 / 256)^(decoding_safety_limit(n_shards) + 1)`.
#[inline]
pub fn decoding_safety_limit(n_shards: NonZeroU16) -> u16 {
    // These ranges are chosen to ensure that the safety limit is at most 20% of f, up to a safety
    // limit of 5.
    match n_shards.get() {
        0..=15 => 0,
        16..=30 => 1, // f=5, 3f+1=16, 0.2*f=1
        31..=45 => 2, // f=10, 3f+1=31, 0.2*f=2
        46..=60 => 3, // f=15, 3f+1=46, 0.2*f=3
        61..=75 => 4, // f=20, 3f+1=61, 0.2*f=4
        76.. => 5,    // f=25, 3f+1=76, 0.2*f=5
    }
}

/// Computes the number of primary and secondary source symbols starting from the number of shards.
///
/// The computation is as follows:
/// - `source_symbols_primary = n_shards - 2f - decoding_safety_limit(n_shards)`
/// - `source_symbols_secondary = n_shards - f - decoding_safety_limit(n_shards)`
#[inline]
pub fn source_symbols_for_n_shards(n_shards: NonZeroU16) -> (NonZeroU16, NonZeroU16) {
    let safety_limit = decoding_safety_limit(n_shards);
    let min_n_correct = bft::min_n_correct(n_shards).get();
    (
        (min_n_correct - bft::max_n_faulty(n_shards) - safety_limit)
            .try_into()
            .expect("implied by BFT computations and definition of safety_limit"),
        (min_n_correct - safety_limit)
            .try_into()
            .expect("implied by BFT computations and definition of safety_limit"),
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
pub fn max_sliver_size_for_n_secondary(n_secondary_source_symbols: NonZeroU16) -> u64 {
    u64::from(n_secondary_source_symbols.get()) * u64::from(u16::MAX)
}

/// Returns the maximum size of a sliver for a system with `n_shards` shards.
///
/// This is the size of a primary sliver with `u16::MAX` symbol size.
#[inline]
pub fn max_sliver_size_for_n_shards(n_shards: NonZeroU16) -> u64 {
    let (_, secondary) = source_symbols_for_n_shards(n_shards);
    max_sliver_size_for_n_secondary(secondary)
}

/// The maximum size in bytes of a blob that can be encoded, given the number of shards.
///
/// This is limited by the total number of source symbols, which is fixed by the dimensions
/// `source_symbols_primary` x `source_symbols_secondary` of the message matrix, and the maximum
/// symbol size supported by RaptorQ.
///
/// Note that on 32-bit architectures, the actual limit can be smaller than that due to the
/// limited address space.
#[inline]
pub fn max_blob_size_for_n_shards(n_shards: NonZeroU16) -> u64 {
    u64::from(source_symbols_per_blob_for_n_shards(n_shards).get()) * u64::from(MAX_SYMBOL_SIZE)
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
) -> Option<u64> {
    let slivers_size = encoded_slivers_length_for_n_shards(n_shards, unencoded_length)?;
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
) -> Option<u64> {
    let (source_symbols_primary, source_symbols_secondary) = source_symbols_for_n_shards(n_shards);
    let single_shard_slivers_size = (u64::from(source_symbols_primary.get())
        + u64::from(source_symbols_secondary.get()))
        * u64::from(
            utils::compute_symbol_size(
                unencoded_length,
                source_symbols_per_blob_for_n_shards(n_shards),
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
            zero: (1, Ok(5)),
            one: (1, Ok(5)),
            full_matrix: (15, Ok(5)),
            full_matrix_plus_1: (16, Ok(10)),
            too_large: (
                3 * 5 * u64::from(MAX_SYMBOL_SIZE) + 1,
                Err(DataTooLargeError)
            ),
        ]
    }
    fn test_sliver_size_for_blob(
        blob_size: u64,
        expected_primary_sliver_size: Result<u32, DataTooLargeError>,
    ) {
        assert_eq!(
            EncodingConfig::new_for_test(3, 5, 10).sliver_size_for_blob::<Primary>(blob_size),
            expected_primary_sliver_size.map(|e| NonZeroU32::new(e).unwrap())
        );
    }

    param_test! {
        test_encoded_size: [
            zero_small_committee: (1, 10, 10*((4+7) + 10*2*32 + 32)),
            one_small_committee: (1, 10, 10*((4+7) + 10*2*32 + 32)),
            #[ignore] one_large_committee:
                (1, 1000, 1000*((329+662) + 1000*2*32 + 32)),
            larger_blob_small_committee:
                ((4*7)*100, 10, 10*((4+7)*100 + 10*2*32 + 32)),
            #[ignore] larger_blob_large_committee: (
                (329*662)*100,
                1000,
                1000*((329+662)*100 + 1000*2*32 + 32),
            ),

        ]
    }
    /// These tests replicate the tests for `encoded_blob_length` in
    /// `contracts/walrus/sources/system/redstuff.move` and should be kept in sync.
    fn test_encoded_size(blob_size: usize, n_shards: u16, expected_encoded_size: u64) {
        assert_eq!(
            EncodingConfig::new(NonZeroU16::new(n_shards).unwrap())
                .encoded_blob_length_from_usize(blob_size),
            Some(expected_encoded_size),
        );
    }

    param_test! {
        test_source_symbols_for_n_shards: [
            one: (1, 1, 1),
            three: (3, 3, 3),
            four: (4, 2, 3),
            nine: (9, 5, 7),
            ten: (10, 4, 7),
            fifty: (51, 16, 32),
            one_hundred_and_one: (101, 30, 63),
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
            fifty: (51, 16, 32),
            one_hundred_and_one: (101, 30, 63),
        ]
    }
    fn test_new_for_n_shards(n_shards: u16, primary: u16, secondary: u16) {
        let config = EncodingConfig::new(n_shards.try_into().unwrap());
        assert_eq!(config.source_symbols_primary.get(), primary);
        assert_eq!(config.source_symbols_secondary.get(), secondary);
    }
}
