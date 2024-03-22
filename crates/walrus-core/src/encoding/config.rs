// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(test)]
use std::cell::RefCell;
#[cfg(not(test))]
use std::sync::OnceLock;

use raptorq::SourceBlockEncodingPlan;

use super::{
    utils,
    BlobDecoder,
    BlobEncoder,
    DataTooLargeError,
    EncodeError,
    Encoder,
    EncodingAxis,
    MAX_SOURCE_SYMBOLS_PER_BLOCK,
    MAX_SYMBOL_SIZE,
};
use crate::{encoding::common::MAX_N_SHARDS, metadata::SliverPairIndex};

/// Global encoding configuration with pre-generated encoding plans.
#[cfg(not(test))]
static ENCODING_CONFIG: OnceLock<EncodingConfig> = OnceLock::new();
#[cfg(test)]
// NOTE: for tests, we need to use the `ENCODING_CONFIG` as a thread-local variable. Tests are run
// in parallel on separate threads, but still share the static `ENCODING_CONFIG` in the normal case.
// This creates issues when running tests that require different configs. Therefore, a thread local
// variable allows us to keep the configs separated during tests.
// PROBLEM: will this create issues during end-to-end tests?
thread_local! {
    static ENCODING_CONFIG: RefCell<Option<EncodingConfig>> = RefCell::new(None);
}

/// Creates a new global encoding configuration for the provided system parameters.
///
/// Performs no action if the global configuration is already initialized.
///
/// # Arguments
///
/// * `source_symbols_primary` - The number of source symbols for the primary encoding. This
///   should be slightly below `f`, where `f` is the Byzantine parameter.
/// * `source_symbols_secondary` - The number of source symbols for the secondary encoding. This
///   should be slightly below `2f`.
/// * `n_shards` - The total number of shards.
///
/// Ideally, both `source_symbols_primary` and `source_symbols_secondary` should be chosen from the
/// list of supported values of K' provided in [RFC 6330, Section 5.6][rfc6330s5.6] to avoid the
/// need for padding symbols.
///
/// # Returns
///
/// The global encoding configuration.
///
/// # Panics
///
/// Panics if the parameters are inconsistent with Byzantine fault tolerance; i.e., if the
/// number of source symbols of the primary encoding is equal to or greater than 1/3 of the
/// number of shards, or if the number of source symbols of the secondary encoding equal to or
/// greater than 2/3 of the number of shards.
///
/// Panics if the number of primary or secondary source symbols is 0 or larger than
/// [`MAX_SOURCE_SYMBOLS_PER_BLOCK`].
///
/// Panics if the total number of shards `n_shards` is greater than `MAX_N_SHARDS`.
///
/// [rfc6330s5.6]: https://datatracker.ietf.org/doc/html/rfc6330#section-5.6
#[cfg(not(test))]
pub fn initialize_encoding_config(
    source_symbols_primary: u16,
    source_symbols_secondary: u16,
    n_shards: u32,
) -> &'static EncodingConfig {
    ENCODING_CONFIG.get_or_init(|| {
        EncodingConfig::new(source_symbols_primary, source_symbols_secondary, n_shards)
    })
}

/// Test version of encoding config initialization.
#[cfg(test)]
pub fn initialize_encoding_config(
    source_symbols_primary: u16,
    source_symbols_secondary: u16,
    n_shards: u32,
) {
    ENCODING_CONFIG.with(|f| {
        *f.borrow_mut() = Some(EncodingConfig::new(
            source_symbols_primary,
            source_symbols_secondary,
            n_shards,
        ))
    });
}

/// Gets the global encoding configuration.
///
/// # Returns
///
/// The global [`EncodingConfig`].
///
/// # Panics
///
/// Must only be called after the global encoding configuration was initialized with
/// [`initialize_encoding_config`], panics otherwise.
#[cfg(not(test))]
pub fn get_encoding_config() -> &'static EncodingConfig {
    ENCODING_CONFIG
        .get()
        .expect("must first be initialized with `initialize_encoding_config`")
}

/// Test version of getting the "global" (thread local) encoding configuration.
#[cfg(test)]
pub fn get_encoding_config() -> EncodingConfig {
    ENCODING_CONFIG
        .with(|f| f.borrow().clone())
        .expect("must first be initialized with `initialize_encoding_config`")
}

/// Configuration of the Walrus encoding.
///
/// This consists of the number of source symbols for the two encodings, the total number of shards,
/// and contains pre-generated encoding plans to speed up encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodingConfig {
    /// The number of source symbols for the primary encoding, which is, simultaneously, the number
    /// of symbols per secondary sliver. It must be strictly less than 1/3 of `n_shards`.
    pub(crate) source_symbols_primary: u16,
    /// The number of source symbols for the secondary encoding, which is, simultaneously, the
    /// number of symbols per primary sliver. It must be strictly less than 2/3 of `n_shards`.
    pub(crate) source_symbols_secondary: u16,
    /// The number of shards.
    pub(crate) n_shards: u32,
    /// Encoding plan to speed up the primary encoding.
    encoding_plan_primary: SourceBlockEncodingPlan,
    /// Encoding plan to speed up the secondary encoding.
    encoding_plan_secondary: SourceBlockEncodingPlan,
}

impl EncodingConfig {
    pub(crate) fn new(
        source_symbols_primary: u16,
        source_symbols_secondary: u16,
        n_shards: u32,
    ) -> Self {
        assert!(
            source_symbols_primary * source_symbols_secondary > 0,
            "the number of source symbols must not be 0"
        );
        assert!(
            n_shards <= MAX_N_SHARDS,
            "the number of shards can be at most `MAX_N_SHARDS`",
        );
        assert!(
            3 * (source_symbols_primary as u32) < n_shards,
            "the primary encoding can be at most a 1/3 encoding"
        );
        assert!(
            3 * (source_symbols_secondary as u32) < 2 * n_shards,
            "the secondary encoding can be at most a 2/3 encoding"
        );
        assert!(
            source_symbols_primary < MAX_SOURCE_SYMBOLS_PER_BLOCK
                && source_symbols_secondary < MAX_SOURCE_SYMBOLS_PER_BLOCK,
            "the number of source symbols can be at most `MAX_SOURCE_SYMBOLS_PER_BLOCK`"
        );

        Self {
            source_symbols_primary,
            source_symbols_secondary,
            n_shards,
            encoding_plan_primary: SourceBlockEncodingPlan::generate(source_symbols_primary),
            encoding_plan_secondary: SourceBlockEncodingPlan::generate(source_symbols_secondary),
        }
    }

    /// Returns the number of source symbols configured for this type.
    pub fn n_source_symbols<T: EncodingAxis>(&self) -> u16 {
        if T::IS_PRIMARY {
            self.source_symbols_primary
        } else {
            self.source_symbols_secondary
        }
    }

    /// Returns the pre-generated encoding plan this type.
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
        self.n_source_symbols::<T>() as usize * MAX_SYMBOL_SIZE
    }

    /// The maximum size in bytes of a blob that can be encoded.
    ///
    /// This is limited by the total number of source symbols, which is fixed by the dimensions
    /// `source_symbols_primary` x `source_symbols_secondary` of the message matrix, and the maximum
    /// symbol size supported by RaptorQ.
    #[inline]
    pub fn max_blob_size(&self) -> usize {
        self.source_symbols_per_blob() * MAX_SYMBOL_SIZE
    }

    /// The number of symbols a blob is split into.
    #[inline]
    pub fn source_symbols_per_blob(&self) -> usize {
        self.source_symbols_primary as usize * self.source_symbols_secondary as usize
    }

    /// The symbol size when encoding a blob of size `blob_size`.
    ///
    /// Returns `None` if the computed symbol size is larger than [`MAX_SYMBOL_SIZE`].
    #[inline]
    pub fn symbol_size_for_blob(&self, blob_size: usize) -> Option<u16> {
        utils::compute_symbol_size(blob_size, self.source_symbols_per_blob())
    }

    /// The size (in bytes) of a sliver corresponding to a blob of size `blob_size`.
    ///
    /// Returns `None` if `blob_size > self.max_blob_size()`.
    #[inline]
    pub fn sliver_size_for_blob<T: EncodingAxis>(&self, blob_size: usize) -> Option<usize> {
        Some(
            self.n_source_symbols::<T::OrthogonalAxis>() as usize
                * self.symbol_size_for_blob(blob_size)? as usize,
        )
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
    ) -> SliverPairIndex {
        if T::IS_PRIMARY {
            pair_index
        } else {
            let idx = self.n_shards - pair_index.as_u32() - 1;
            SliverPairIndex(idx.try_into().expect("shard index out of bounds"))
        }
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
            self.n_shards,
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
    /// Returns a [`DataTooLargeError`] if the `blob` is too large to be encoded.
    pub fn get_blob_encoder(&self, blob: &[u8]) -> Result<BlobEncoder, DataTooLargeError> {
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
    /// Returns a [`DataTooLargeError`] if the `blob_size` is too large to be decoded.
    pub fn get_blob_decoder<T: EncodingAxis>(
        &self,
        blob_size: usize,
    ) -> Result<BlobDecoder<T>, DataTooLargeError> {
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
            zero: (0, Some(0)),
            one: (1, Some(5)),
            full_matrix: (15, Some(5)),
            full_matrix_plus_1: (16, Some(10)),
            too_large: (3 * 5 * MAX_SYMBOL_SIZE + 1, None),
        ]
    }
    fn test_sliver_size_for_blob(blob_size: usize, expected_primary_sliver_size: Option<usize>) {
        let config = EncodingConfig::new(3, 5, 10);
        assert_eq!(
            config.sliver_size_for_blob::<Primary>(blob_size),
            expected_primary_sliver_size
        );
    }
}
