// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::num::{NonZeroU16, NonZeroUsize};

use raptorq::SourceBlockEncodingPlan;

use super::{
    utils,
    BlobDecoder,
    BlobEncoder,
    EncodeError,
    Encoder,
    EncodingAxis,
    MAX_SOURCE_SYMBOLS_PER_BLOCK,
    MAX_SYMBOL_SIZE,
};
use crate::metadata::{SliverIndex, SliverPairIndex};

/// Configuration of the Walrus encoding.
///
/// This consists of the number of source symbols for the two encodings, the total number of shards,
/// and contains pre-generated encoding plans to speed up encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodingConfig {
    /// The number of source symbols for the primary encoding, which is, simultaneously, the number
    /// of symbols per secondary sliver. It must be strictly less than 1/3 of `n_shards`.
    pub(crate) source_symbols_primary: NonZeroU16,
    /// The number of source symbols for the secondary encoding, which is, simultaneously, the
    /// number of symbols per primary sliver. It must be strictly less than 2/3 of `n_shards`.
    pub(crate) source_symbols_secondary: NonZeroU16,
    /// The number of shards.
    pub(crate) n_shards: NonZeroU16,
    /// Encoding plan to speed up the primary encoding.
    encoding_plan_primary: SourceBlockEncodingPlan,
    /// Encoding plan to speed up the secondary encoding.
    encoding_plan_secondary: SourceBlockEncodingPlan,
}

impl EncodingConfig {
    /// Creates a new encoding configuration for the provided system parameters.
    ///
    /// # Arguments
    ///
    /// * `source_symbols_primary` - The number of source symbols for the primary encoding. This
    ///   should be slightly below `f`, where `f` is the Byzantine parameter.
    /// * `source_symbols_secondary` - The number of source symbols for the secondary encoding. This
    ///   should be slightly below `2f`.
    /// * `n_shards` - The total number of shards.
    ///
    /// Ideally, both `source_symbols_primary` and `source_symbols_secondary` should be chosen from
    /// the list of supported values of K' provided in [RFC 6330, Section 5.6][rfc6330s5.6] to avoid
    /// the need for padding symbols.
    ///
    /// # Returns
    ///
    /// The encoding configuration.
    ///
    /// # Panics
    ///
    /// Panics if any of the parameters are 0.
    ///
    /// Panics if the parameters are inconsistent with Byzantine fault tolerance; i.e., if the
    /// number of source symbols of the primary encoding is equal to or greater than 1/3 of the
    /// number of shards, or if the number of source symbols of the secondary encoding equal to or
    /// greater than 2/3 of the number of shards.
    ///
    /// Panics if the number of primary or secondary source symbols is larger than
    /// [`MAX_SOURCE_SYMBOLS_PER_BLOCK`].
    ///
    /// [rfc6330s5.6]: https://datatracker.ietf.org/doc/html/rfc6330#section-5.6
    pub fn new(source_symbols_primary: u16, source_symbols_secondary: u16, n_shards: u16) -> Self {
        assert!(
            source_symbols_primary < MAX_SOURCE_SYMBOLS_PER_BLOCK
                && source_symbols_secondary < MAX_SOURCE_SYMBOLS_PER_BLOCK,
            "the number of source symbols can be at most `MAX_SOURCE_SYMBOLS_PER_BLOCK`"
        );
        assert!(
            3 * source_symbols_secondary < 2 * n_shards,
            "the secondary encoding can be at most a 2/3 encoding"
        );
        assert!(
            3 * source_symbols_primary < n_shards,
            "the primary encoding can be at most a 1/3 encoding"
        );

        let source_symbols_primary = NonZeroU16::new(source_symbols_primary)
            .expect("the number of source symbols must not be 0");
        let source_symbols_secondary = NonZeroU16::new(source_symbols_secondary)
            .expect("the number of source symbols must not be 0");
        let n_shards = NonZeroU16::new(n_shards).expect("implied by previous checks");

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

    /// Returns the number of source symbols configured for this type.
    #[inline]
    pub fn n_source_symbols<T: EncodingAxis>(&self) -> NonZeroU16 {
        if T::IS_PRIMARY {
            self.source_symbols_primary
        } else {
            self.source_symbols_secondary
        }
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
    pub fn max_data_size<T: EncodingAxis>(&self) -> usize {
        self.n_source_symbols::<T>().get() as usize * MAX_SYMBOL_SIZE
    }

    /// The maximum size in bytes of a blob that can be encoded.
    ///
    /// This is limited by the total number of source symbols, which is fixed by the dimensions
    /// `source_symbols_primary` x `source_symbols_secondary` of the message matrix, and the maximum
    /// symbol size supported by RaptorQ.
    #[inline]
    pub fn max_blob_size(&self) -> usize {
        self.source_symbols_per_blob().get() * MAX_SYMBOL_SIZE
    }

    /// The number of symbols a blob is split into.
    #[inline]
    pub fn source_symbols_per_blob(&self) -> NonZeroUsize {
        NonZeroUsize::from(self.source_symbols_primary)
            .checked_mul(self.source_symbols_secondary.into())
            .expect("result fits into a usize")
    }

    /// The symbol size when encoding a blob of size `blob_size`.
    ///
    /// Returns `None` if the `blob_size` is 0 or the computed symbol size is larger than
    /// [`MAX_SYMBOL_SIZE`].
    #[inline]
    pub fn symbol_size_for_blob(&self, blob_size: usize) -> Option<NonZeroU16> {
        utils::compute_symbol_size(blob_size, self.source_symbols_per_blob())
    }

    /// The size (in bytes) of a sliver corresponding to a blob of size `blob_size`.
    ///
    /// Returns `None` if `blob_size == 0` or `blob_size > self.max_blob_size()`.
    #[inline]
    pub fn sliver_size_for_blob<T: EncodingAxis>(&self, blob_size: usize) -> Option<NonZeroUsize> {
        NonZeroUsize::from(self.n_source_symbols::<T::OrthogonalAxis>())
            .checked_mul(self.symbol_size_for_blob(blob_size)?.into())
    }

    /// Computes the index of the [`Sliver`][super::Sliver] of the axis specified by the generic
    /// parameter starting from the index of the [`SliverPair`][super::SliverPair].
    ///
    /// This is needed because primary slivers are assigned in ascending `pair_index` order, while
    /// secondary slivers are assigned in descending `pair_index` order. I.e., the first primary
    /// sliver is contained in the first sliver pair, but the first secondary sliver is contained in
    /// the last sliver pair.
    // TODO(giac): Point to the redstuff documentation when ready!
    pub fn sliver_index_from_pair_index<T: EncodingAxis>(
        &self,
        pair_index: SliverPairIndex,
    ) -> SliverIndex {
        T::sliver_index_from_pair_index(pair_index, self.n_shards())
    }

    /// Returns an [`Encoder`] to perform a single primary or secondary encoding of the provided
    /// data.
    ///
    /// # Arguments
    ///
    /// * `encoding_axis` - Sets the encoding parameters for the primary or secondary encoding.
    /// * `data` - The data to be encoded. This *does not* have to be aligned/padded.
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

    /// Returns a [`BlobEncoder`] to encode a blob into [`SliverPair`s][super::SliverPair].
    ///
    /// # Arguments
    ///
    /// * `blob` - The blob to be encoded. Does not have to be padded.
    ///
    /// # Errors
    ///
    /// Returns a [`EncodeError::DataTooLarge`] if the `blob` is too large to be encoded.
    pub fn get_blob_encoder<'a>(&'a self, blob: &'a [u8]) -> Result<BlobEncoder, EncodeError> {
        BlobEncoder::new(self, blob)
    }

    /// Returns a [`BlobDecoder`] to reconstruct a blob from either
    /// [`Primary`][super::PrimarySliver] or [`Secondary`][super::SecondarySliver] slivers.
    ///
    /// # Arguments
    ///
    /// * `blob_size` - The size of the blob to be decoded.
    ///
    /// # Errors
    ///
    /// Returns a [`EncodeError::DataTooLarge`] if the `blob_size` is too large to be decoded.
    pub fn get_blob_decoder<T: EncodingAxis>(
        &self,
        blob_size: usize,
    ) -> Result<BlobDecoder<T>, EncodeError> {
        BlobDecoder::new(self, blob_size)
    }
}

#[cfg(test)]
mod tests {
    use walrus_test_utils::param_test;

    use super::*;
    use crate::encoding::Primary;

    param_test! {
        test_sliver_size_for_blob: [
            zero: (0, None),
            one: (1, Some(5)),
            full_matrix: (15, Some(5)),
            full_matrix_plus_1: (16, Some(10)),
            too_large: (3 * 5 * MAX_SYMBOL_SIZE + 1, None),
        ]
    }
    fn test_sliver_size_for_blob(blob_size: usize, expected_primary_sliver_size: Option<usize>) {
        assert_eq!(
            EncodingConfig::new(3, 5, 10).sliver_size_for_blob::<Primary>(blob_size),
            expected_primary_sliver_size.and_then(NonZeroUsize::new)
        );
    }
}
