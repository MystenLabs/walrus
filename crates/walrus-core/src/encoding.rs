// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! TODO(mlegner): Describe encoding algorithm in detail (#50).

#[cfg(test)]
use std::cell::RefCell;
#[cfg(not(test))]
use std::sync::OnceLock;
use std::{cmp::min, marker::PhantomData, ops::Range};

use raptorq::{
    EncodingPacket,
    PayloadId,
    SourceBlockDecoder,
    SourceBlockEncoder,
    SourceBlockEncodingPlan,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use self::utils::{compute_symbol_size, get_transmission_info};

pub mod symbols;
pub use symbols::Symbols;

mod utils;

/// A primary sliver resulting from an encoding of a blob.
pub type PrimarySliver = Sliver<Primary>;

/// A secondary sliver resulting from an encoding of a blob.
pub type SecondarySliver = Sliver<Secondary>;

/// The maximum length in bytes of a single symbol in RaptorQ.
pub const MAX_SYMBOL_SIZE: usize = u16::MAX as usize;

/// The maximum number of source symbols per block for RaptorQ.
pub const MAX_SOURCE_SYMBOLS_PER_BLOCK: u16 = 56403;

/// The largest symbol ID possible
pub const MAX_ENCODING_SYMBOL_ID: u32 = (1 << 24) - 1;

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
/// Panics if the number of primary or secondary source symbols is larger than
/// [`MAX_SOURCE_SYMBOLS_PER_BLOCK`].
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

/// Error returned when the data is too large to be encoded with this encoder.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("the data is too large to be encoded")]
pub struct DataTooLargeError;

/// Error type returned when encoding fails.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum EncodeError {
    /// The data is too large to be encoded with this encoder.
    #[error("the data is to large to be encoded (max size: {0})")]
    DataTooLarge(usize),
    /// The data to be encoded is empty.
    #[error("empty data cannot be encoded")]
    EmptyData,
    /// The data is not properly aligned; i.e., it is not a multiple of the symbol size or symbol
    /// count.
    #[error("the data is not properly aligned (must be a multiple of {0})")]
    MisalignedData(u16),
}

/// Error type returned when sliver recovery fails.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RecoveryError {
    /// The symbols provided are empty or have different sizes.
    #[error("the symbols provided are empty or have different sizes")]
    InvalidSymbolSizes,
    /// The index of the recovery symbol can be at most [`MAX_ENCODING_SYMBOL_ID`].
    #[error("the index of the recovery symbol can be at most `MAX_ENCODING_SYMBOL_ID`")]
    IndexTooLarge,
    /// The underlying [`Encoder`] returned an error.
    #[error(transparent)]
    EncodeError(#[from] EncodeError),
    /// The underlying [`Decoder`] returned an error.
    #[error(transparent)]
    DataTooLarge(#[from] DataTooLargeError),
}

/// Marker trait to indicate the encoding axis (primary or secondary).
pub trait EncodingAxis: Clone + PartialEq + Eq + Default {
    /// The complementary encoding axis.
    type OrthogonalAxis: EncodingAxis;
    /// Whether this corresponds to the primary (true) or secondary (false) encoding.
    const IS_PRIMARY: bool;

    /// Computes the index of the [`Sliver`] starting from the index of the [`SliverPair`].
    ///
    /// This is needed because primary slivers are assigned in ascending `pair_index` order, while
    /// secondary slivers are assigned in descending `pair_index` order. I.e., the first primary
    /// sliver is contained in the first sliver pair, but the first secondary sliver is contained in
    /// the last sliver pair.
    // TODO(giac): Point to the redstuff documentation when ready!
    fn sliver_index_from_pair_index(pair_index: usize) -> usize;
}

/// Marker type to indicate the primary encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Primary;
impl EncodingAxis for Primary {
    type OrthogonalAxis = Secondary;
    const IS_PRIMARY: bool = true;

    fn sliver_index_from_pair_index(pair_index: usize) -> usize {
        pair_index
    }
}

/// Marker type to indicate the secondary encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Secondary;
impl EncodingAxis for Secondary {
    type OrthogonalAxis = Primary;
    const IS_PRIMARY: bool = false;

    fn sliver_index_from_pair_index(pair_index: usize) -> usize {
        get_encoding_config().n_shards as usize - pair_index - 1
    }
}

/// Encoded data corresponding to a single [`EncodingAxis`] assigned to one shard.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Sliver<T: EncodingAxis> {
    /// The encoded data.
    pub symbols: Symbols,
    phantom: PhantomData<T>,
}

impl<T: EncodingAxis> Sliver<T> {
    /// Creates a new `Sliver` copying the provided slice of bytes.
    ///
    /// # Panics
    ///
    /// Panics if the slice does not contain complete symbols, i.e., if
    /// `slice.len() % symbol_size != 0` or if `symbol_size == 0`.
    pub fn new<U: Into<Vec<u8>>>(data: U, symbol_size: u16) -> Self {
        Self {
            symbols: Symbols::new(data.into(), symbol_size),
            phantom: PhantomData,
        }
    }

    /// Creates a new `Sliver` with empty data of specified length.
    ///
    /// # Panics
    ///
    /// Panics if `symbol_size == 0`.
    pub fn new_empty(length: usize, symbol_size: u16) -> Self {
        Self {
            symbols: Symbols::zeros(length, symbol_size),
            phantom: PhantomData,
        }
    }

    /// Copies the provided symbol to the location specified by the index.
    ///
    /// # Panics
    ///
    /// Panics if `self.data.len() < index * (symbol.len() + 1)` and if the symbol size does not
    /// match the length specified in the [`Symbols`] struct.
    pub fn copy_symbol_to(&mut self, index: usize, symbol: &[u8]) -> &mut Self {
        assert!(symbol.len() == self.symbols.symbol_size());
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
            self.symbols.symbol_size() as u16, // The symbol size remains unvaried when re-encoding.
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
            .encode_range(index..index + 1)
            .next()
            .expect("the encoder should always be able to produce an encoding symbol"))
    }

    /// Gets the recovery symbol for a specific target sliver starting from the current sliver.
    ///
    /// A [`Primary`] sliver is used to reconstruct symbols for a [`Secondary`] sliver, and
    /// conversely.
    ///
    /// # Arguments
    ///
    /// * `self_pair_idx` - the index of the [`SliverPair`] to which the source sliver belongs.
    /// * `target_pair_idx` - the index of the [`SliverPair`] to which the target sliver belongs.
    ///
    /// # Errors
    ///
    /// Returns an [`RecoveryError::EncodeError`] if the `data` cannot be encoded. See
    /// [`Encoder::new`] for further details about the returned errors.
    ///
    /// # Panics
    ///
    /// Panics if any of the inputs is larger than `n_shards` in the encoding config.
    pub fn recovery_symbol_for_sliver(
        &self,
        self_pair_idx: usize,
        target_pair_idx: usize,
    ) -> Result<DecodingSymbol, RecoveryError> {
        let (self_sliver_idx, other_sliver_idx) =
            Self::relative_sliver_indices(self_pair_idx, target_pair_idx);
        Ok(DecodingSymbol {
            index: self_sliver_idx as u32,
            data: self.single_recovery_symbol(other_sliver_idx as u32)?,
        })
    }

    /// Recovers a [`Sliver`] from the recovery symbols.
    ///
    /// Returns the recovered [`Sliver`] if decoding succeeds or `None` if decoding fails.
    ///
    /// # Errors
    ///
    /// Returns a [`RecoveryError::DataTooLarge`] if the recovery symbols provided are larger than
    /// what the [`Decoder`] can process, i.e., if the `symbol_size` of the `recovery_symbols` is
    /// larger than [`MAX_SYMBOL_SIZE`]. See [`Decoder::new`] for further details.  Returns a
    /// [`RecoveryError::InvalidSymbolSizes`] error if the symbols provided have different
    /// symbol sizes or if their symbol size is 0 (they are empty).
    pub fn recover_sliver<I, U>(recovery_symbols: U) -> Result<Option<Self>, RecoveryError>
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

        // Pass the symbols to the decoder.
        Ok(Decoder::new(
            Self::n_source_symbols(),
            symbol_size * Self::n_source_symbols() as usize,
        )?
        .decode(recovery_iter)
        .map(|data| {
            Sliver::new(
                data,
                symbol_size.try_into().expect(
                    "if larger than `u16::MAX`, we couldn't have created the `Decoder` above",
                ),
            )
        }))
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
    fn relative_sliver_indices(self_pair_idx: usize, other_pair_idx: usize) -> (usize, usize) {
        (
            T::sliver_index_from_pair_index(self_pair_idx),
            T::OrthogonalAxis::sliver_index_from_pair_index(other_pair_idx),
        )
    }
}

/// Combination of a primary and secondary sliver of one shard.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SliverPair {
    /// Index of this sliver pair.
    ///
    /// Sliver pair `i` contains the primary sliver `i` and the secondary sliver `n_shards-i-1`.
    // TODO(mlegner): Link to sliver->shard assignment (#49).
    pub index: u32,
    /// The sliver corresponding to the [`Primary`] encoding.
    pub primary: Sliver<Primary>,
    /// The sliver corresponding to the [`Secondary`] encoding.
    pub secondary: Sliver<Secondary>,
}

/// Configuration of the Walrus encoding.
///
/// This consists of the number of source symbols for the two encodings, the total number of shards,
/// and contains pre-generated encoding plans to speed up encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodingConfig {
    /// The number of source symbols for the primary encoding, which is, simultaneously, the number
    /// of symbols per secondary sliver. It must be strictly less than 1/3 of `n_shards`.
    source_symbols_primary: u16,
    /// The number of source symbols for the secondary encoding, which is, simultaneously, the
    /// number of symbols per primary sliver. It must be strictly less than 2/3 of `n_shards`.
    source_symbols_secondary: u16,
    /// The number of shards.
    n_shards: u32,
    /// Encoding plan to speed up the primary encoding.
    encoding_plan_primary: SourceBlockEncodingPlan,
    /// Encoding plan to speed up the secondary encoding.
    encoding_plan_secondary: SourceBlockEncodingPlan,
}

impl EncodingConfig {
    fn new(source_symbols_primary: u16, source_symbols_secondary: u16, n_shards: u32) -> Self {
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
        self.source_symbols_primary as usize
            * self.source_symbols_secondary as usize
            * MAX_SYMBOL_SIZE
    }

    /// The number of symbols a blob is split into.
    #[inline]
    pub fn source_symbols_per_blob(&self) -> usize {
        self.source_symbols_primary as usize * self.source_symbols_secondary as usize
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

    /// Returns a [`BlobEncoder`] to encode a blob into [`SliverPair`s][SliverPair].
    ///
    /// # Arguments
    ///
    /// * `blob` - The blob to be encoded. Does not have to be padded.
    ///
    /// # Errors
    ///
    /// Returns an [`DataTooLargeError`] if the `blob` is too large to be encoded.
    pub fn get_blob_encoder(&self, blob: &[u8]) -> Result<BlobEncoder, DataTooLargeError> {
        BlobEncoder::new(blob, self)
    }
}

/// Wrapper to perform a single encoding with RaptorQ for the provided parameters.
pub struct Encoder {
    raptorq_encoder: SourceBlockEncoder,
    n_source_symbols: u16,
    n_shards: u32,
}

// TODO(mlegner): Check if memory management and copying can be improved for Encoder (#45).
impl Encoder {
    /// Creates a new `Encoder` for the provided `data` with the specified arguments.
    ///
    /// # Arguments
    ///
    /// * `data` - The data to encode. This *must* be a multiple of `n_source_symbols` and must not
    ///   be empty.
    /// * `n_source_symbols` - The number of source symbols into which the data should be split.
    /// * `n_shards` - The total number of shards for which symbols should be generated.
    /// * `encoding_plan` - A pre-generated [`SourceBlockEncodingPlan`] consistent with
    ///   `n_source_symbols`.
    ///
    /// # Errors
    ///
    /// Returns an [`EncodeError`] if the `data` is empty, not a multiple of `n_source_symbols`, or
    /// too large to be encoded with the provided `n_source_symbols` (this includes the case
    /// `n_source_symbols == 0`).
    ///
    /// If the `encoding_plan` was generated for a different number of source symbols than
    /// `n_source_symbols`, later methods called on the returned `Encoder` may exhibit unexpected
    /// behavior.
    pub fn new(
        data: &[u8],
        n_source_symbols: u16,
        n_shards: u32,
        encoding_plan: &SourceBlockEncodingPlan,
    ) -> Result<Self, EncodeError> {
        if data.is_empty() {
            return Err(EncodeError::EmptyData);
        }
        if data.len() % n_source_symbols as usize != 0 {
            return Err(EncodeError::MisalignedData(n_source_symbols));
        }

        let Some(symbol_size) = utils::compute_symbol_size(data.len(), n_source_symbols.into())
        else {
            return Err(EncodeError::DataTooLarge(
                n_source_symbols as usize * MAX_SYMBOL_SIZE,
            ));
        };

        Ok(Self {
            raptorq_encoder: SourceBlockEncoder::with_encoding_plan2(
                0,
                &utils::get_transmission_info(symbol_size),
                data,
                encoding_plan,
            ),
            n_source_symbols,
            n_shards,
        })
    }

    /// Creates a new `Encoder` for the provided `data` with the specified arguments.
    ///
    /// This generates an appropriate [`SourceBlockEncodingPlan`] and then calls [`Self::new`].
    ///
    /// See [`Self::new`] for further details.
    pub fn new_with_new_encoding_plan(
        data: &[u8],
        n_source_symbols: u16,
        n_shards: u32,
    ) -> Result<Self, EncodeError> {
        let encoding_plan = SourceBlockEncodingPlan::generate(n_source_symbols);
        Self::new(data, n_source_symbols, n_shards, &encoding_plan)
    }

    /// Returns an iterator over all source symbols.
    pub fn source_symbols(&self) -> impl Iterator<Item = Vec<u8>> {
        self.raptorq_encoder
            .source_packets()
            .into_iter()
            .map(utils::packet_to_data)
    }

    /// Returns an iterator over a range of source and/or repair symbols.
    pub fn encode_range(&self, range: Range<u32>) -> impl Iterator<Item = Vec<u8>> {
        let repair_end = if range.end > self.n_source_symbols as u32 {
            range.end - self.n_source_symbols as u32
        } else {
            0
        };

        self.raptorq_encoder
            .source_packets()
            .into_iter()
            .chain(self.raptorq_encoder.repair_packets(0, repair_end))
            .skip(range.start as usize)
            .take(range.len())
            .map(utils::packet_to_data)
    }

    /// Returns an iterator over all `n_shards` source and repair symbols.
    pub fn encode_all(&self) -> impl Iterator<Item = Vec<u8>> {
        self.raptorq_encoder
            .source_packets()
            .into_iter()
            .chain(
                self.raptorq_encoder
                    .repair_packets(0, self.n_shards - self.n_source_symbols as u32),
            )
            .map(utils::packet_to_data)
    }

    /// Returns an iterator over all `n_shards - self.n_source_symbols` repair symbols.
    pub fn encode_all_repair_symbols(&self) -> impl Iterator<Item = Vec<u8>> {
        self.raptorq_encoder
            .repair_packets(0, self.n_shards - self.n_source_symbols as u32)
            .into_iter()
            .map(utils::packet_to_data)
    }
}

/// A single symbol used for decoding, consisting of the data and the symbol's index.
// TODO(mlegner): align this with the `Symbols` struct added in #61?
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

/// Wrapper to perform a single decoding with RaptorQ for the provided parameters.
pub struct Decoder {
    raptorq_decoder: SourceBlockDecoder,
    n_source_symbols: u16,
    n_padding_symbols: u16,
}

impl Decoder {
    /// Creates a new `Decoder`.
    ///
    /// # Errors
    ///
    /// Returns a [`DataTooLargeError`] if the provided parameters lead to a symbol size larger than
    /// [`MAX_SYMBOL_SIZE`].
    pub fn new(n_source_symbols: u16, data_length: usize) -> Result<Self, DataTooLargeError> {
        let Some(symbol_size) = compute_symbol_size(data_length, n_source_symbols.into()) else {
            return Err(DataTooLargeError);
        };
        let raptorq_decoder = SourceBlockDecoder::new2(
            0,
            &get_transmission_info(symbol_size),
            data_length
                .try_into()
                .expect("if this conversion failed, we would already have returned an error above"),
        );
        let n_padding_symbols = u16::try_from(raptorq::extended_source_block_symbols(
            n_source_symbols as u32,
        ))
        .expect("the largest value that is ever returned is smaller than u16::MAX")
            - n_source_symbols;
        Ok(Self {
            raptorq_decoder,
            n_source_symbols,
            n_padding_symbols,
        })
    }

    /// Attempts to encode the source data from the provided iterator over
    /// [`DecodingSymbol`s][DecodingSymbol].
    ///
    /// Returns the source data as a byte vector if decoding succeeds or `None` if decoding fails.
    ///
    /// If decoding failed due to an insufficient number of provided symbols, it can be continued
    /// by additional calls to [`decode`][Self::decode] providing more symbols.
    pub fn decode<T: IntoIterator<Item = DecodingSymbol>>(
        &mut self,
        symbols: T,
    ) -> Option<Vec<u8>> {
        let packets = symbols
            .into_iter()
            .map(|s| encoding_packet_from_symbol(s, self.n_source_symbols, self.n_padding_symbols));
        self.raptorq_decoder.decode(packets)
    }
}

/// This function is necessary to convert from the index to the symbol ID used by the raptorq
/// library.
///
/// It is needed because currently the [raptorq] library currently uses the ISI in the
/// [`PayloadId`]. The two can be converted with the knowledge of the number of source symbols (`K`
/// in the RFC's terminology) and padding symbols (`K' - K` in the RFC's terminology).
// TODO(mlegner): Update if the raptorq library changes its behavior.
fn encoding_packet_from_symbol(
    symbol: DecodingSymbol,
    n_source_symbols: u16,
    n_padding_symbols: u16,
) -> EncodingPacket {
    let isi = symbol.index
        + if n_padding_symbols == 0 || symbol.index < n_source_symbols as u32 {
            0
        } else {
            n_padding_symbols as u32
        };
    EncodingPacket::new(PayloadId::new(0, isi), symbol.data)
}

/// Struct to perform the full blob encoding.
pub struct BlobEncoder<'a> {
    /// Rows of the message matrix.
    ///
    /// The outer vector has length `source_symbols_primary`, and each inner vector has length
    /// `source_symbols_secondary * symbol_size`.
    rows: Vec<Vec<u8>>,
    /// Columns of the message matrix.
    ///
    /// The outer vector has length `source_symbols_secondary`, and each inner vector has length
    /// `source_symbols_primary * symbol_size`.
    columns: Vec<Vec<u8>>,
    /// The size of the encoded and decoded symbols.
    symbol_size: usize,
    /// Reference to the encoding configuration of this encoder.
    config: &'a EncodingConfig,
}

// TODO(mlegner): Improve memory management and copying for BlobEncoder (#45).
impl<'a> BlobEncoder<'a> {
    /// Creates a new `BlobEncoder` to encode the provided `blob` with the provided configuration.
    ///
    /// This creates the message matrix, padding with zeros if necessary. The actual encoding can be
    /// performed with the [`encode()`][Self::encode] method.
    pub fn new(blob: &[u8], config: &'a EncodingConfig) -> Result<Self, DataTooLargeError> {
        let Some(symbol_size) =
            utils::compute_symbol_size(blob.len(), config.source_symbols_per_blob())
        else {
            return Err(DataTooLargeError);
        };
        let n_columns = config.source_symbols_secondary as usize;
        let n_rows = config.source_symbols_primary as usize;
        let row_step = n_columns * symbol_size;
        let column_step = n_rows * symbol_size;

        // Initializing rows and columns with 0s implicitly takes care of padding.
        let mut rows = vec![vec![0u8; row_step]; n_rows];
        let mut columns = vec![vec![0u8; column_step]; n_columns];

        for (row, chunk) in rows.iter_mut().zip(blob.chunks(row_step)) {
            row[..chunk.len()].copy_from_slice(chunk);
        }
        for (c, col) in columns.iter_mut().enumerate() {
            for (r, target_chunk) in col.chunks_mut(symbol_size).enumerate() {
                let copy_index_start = min(r * row_step + c * symbol_size, blob.len());
                let copy_index_end = min(copy_index_start + symbol_size, blob.len());
                target_chunk[..copy_index_end - copy_index_start]
                    .copy_from_slice(&blob[copy_index_start..copy_index_end])
            }
        }
        Ok(Self {
            rows,
            columns,
            symbol_size,
            config,
        })
    }

    /// Encodes the blob with which `self` was created to a vector of [`SliverPair`s][SliverPair].
    pub fn encode(&self) -> Vec<SliverPair> {
        let n_rows = self.rows.len();
        let n_columns = self.columns.len();
        let mut sliver_pairs: Vec<SliverPair> = Vec::with_capacity(self.config.n_shards as usize);

        // Initialize `n_shards` empty sliver pairs with the correct lengths and indices.
        for i in 0..self.config.n_shards {
            sliver_pairs.push(SliverPair {
                index: i,
                primary: Sliver::new_empty(n_columns, self.symbol_size as u16),
                secondary: Sliver::new_empty(n_rows, self.symbol_size as u16),
            })
        }

        // The first `n_rows` primary slivers and the last `n_columns` secondary slivers can be
        // directly copied from the message matrix.
        for (row, sliver_pair) in self.rows.iter().zip(sliver_pairs.iter_mut()) {
            sliver_pair.primary.symbols.data_mut().copy_from_slice(row)
        }
        for (column, sliver_pair) in self.columns.iter().zip(sliver_pairs.iter_mut().rev()) {
            sliver_pair
                .secondary
                .symbols
                .data_mut()
                .copy_from_slice(column)
        }

        // Compute the remaining primary slivers by encoding the columns.
        for (c, column) in self.columns.iter().enumerate() {
            for (symbol, sliver_pair) in self
                .config
                .get_encoder::<Primary>(column)
                .expect("size has already been checked")
                .encode_all_repair_symbols() // We only need the repair symbols.
                .zip(sliver_pairs.iter_mut().skip(n_rows))
            {
                sliver_pair.primary.copy_symbol_to(c, &symbol);
            }
        }

        // Compute the remaining secondary slivers by encoding the rows.
        for (r, row) in self.rows.iter().enumerate() {
            for (symbol, sliver_pair) in self
                .config
                .get_encoder::<Secondary>(row)
                .expect("size has already been checked")
                .encode_all_repair_symbols()
                .zip(sliver_pairs.iter_mut().rev().skip(n_columns))
            {
                sliver_pair.secondary.copy_symbol_to(r, &symbol);
            }
        }

        sliver_pairs
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use rand::{rngs::StdRng, seq::SliceRandom, RngCore, SeedableRng};
    use walrus_test_utils::{param_test, Result};

    use super::*;

    fn large_random_data(data_length: usize) -> Vec<u8> {
        let mut rng = StdRng::seed_from_u64(42);
        let mut result = vec![0u8; data_length];
        rng.fill_bytes(&mut result);
        result
    }

    // TODO(mlegner): Add more tests for the encoding/decoding (#28)!

    mod slivers {
        use super::*;

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
            symbol_size: usize,
            symbol: &[u8],
            expected_sliver_data: &[u8],
        ) {
            assert_eq!(
                Sliver::<Primary>::new_empty(sliver_n_symbols, symbol_size as u16)
                    .copy_symbol_to(index, symbol)
                    .symbols
                    .data(),
                expected_sliver_data
            );
        }

        #[test]
        fn new_sliver_copies_provided_slice() {
            let slice = [1, 2, 3, 4, 5];
            assert_eq!(Sliver::<Primary>::new(slice, 1).symbols.data(), &slice)
        }
    }

    mod encoding {
        use super::*;

        #[test]
        fn encoding_empty_data_fails() {
            assert!(matches!(
                Encoder::new_with_new_encoding_plan(&[], 42, 314),
                Err(EncodeError::EmptyData)
            ));
        }

        #[test]
        fn encoding_misaligned_data_fails() {
            assert!(matches!(
                Encoder::new_with_new_encoding_plan(&[1, 2], 3, 314),
                Err(EncodeError::MisalignedData(3))
            ));
        }

        param_test! {
            test_encode_decode -> Result: [
                aligned_data_source_symbols_1: (&[1, 2], 2, 0..2, true),
                aligned_data_source_symbols_2: (&[1, 2, 3, 4], 2, 0..2, true),
                aligned_data_repair_symbols_1: (&[1, 2], 2, 2..4, true),
                aligned_data_repair_symbols_2: (&[1, 2, 3, 4, 5, 6], 2, 2..4, true),
                aligned_data_repair_symbols_3: (&[1, 2, 3, 4, 5, 6], 3, 3..6, true),
                aligned_large_data_repair_symbols: (&large_random_data(42000), 100, 100..200, true),
                aligned_data_too_few_symbols_1: (&[1, 2], 2, 2..3, false),
                aligned_data_too_few_symbols_2: (&[1, 2, 3], 3, 0..2, false),
            ]
        }
        fn test_encode_decode(
            data: &[u8],
            n_source_symbols: u16,
            encoded_symbols_range: Range<u32>,
            should_succeed: bool,
        ) -> Result {
            let start = encoded_symbols_range.start;

            let encoder = Encoder::new_with_new_encoding_plan(
                data,
                n_source_symbols,
                encoded_symbols_range.end,
            )?;
            let encoded_symbols =
                encoder
                    .encode_range(encoded_symbols_range)
                    .enumerate()
                    .map(|(i, symbol)| DecodingSymbol {
                        index: i as u32 + start,
                        data: symbol,
                    });
            let mut decoder = Decoder::new(n_source_symbols, data.len())?;
            let decoding_result = decoder.decode(encoded_symbols);

            if should_succeed {
                assert_eq!(decoding_result.unwrap(), data);
            } else {
                assert_eq!(decoding_result, None)
            }

            Ok(())
        }

        #[test]
        fn can_decode_in_multiple_steps() -> Result {
            let n_source_symbols = 3;
            let data = [1, 2, 3, 4, 5, 6];
            let encoder = Encoder::new_with_new_encoding_plan(&data, n_source_symbols, 10)?;
            let mut encoded_symbols =
                encoder
                    .encode_all_repair_symbols()
                    .enumerate()
                    .map(|(i, symbol)| {
                        vec![DecodingSymbol {
                            index: i as u32 + n_source_symbols as u32,
                            data: symbol,
                        }]
                    });
            let mut decoder = Decoder::new(n_source_symbols, data.len())?;

            assert_eq!(
                decoder.decode(encoded_symbols.next().unwrap().clone()),
                None
            );
            assert_eq!(
                decoder.decode(encoded_symbols.next().unwrap().clone()),
                None
            );
            assert_eq!(
                decoder
                    .decode(encoded_symbols.next().unwrap().clone())
                    .unwrap(),
                data
            );

            Ok(())
        }
    }

    mod blob_encoding {
        use super::*;

        param_test! {
            test_matrix_construction: [
                aligned_square_single_byte_symbols: (
                    2,
                    2,
                    &[1,2,3,4],
                    &[&[1,2], &[3,4]],
                    &[&[1,3], &[2,4]]
                ),
                aligned_square_double_byte_symbols: (
                    2,
                    2,
                    &[1,2,3,4,5,6,7,8],
                    &[&[1,2,3,4], &[5,6,7,8]],
                    &[&[1,2,5,6],&[3,4,7,8]]
                ),
                aligned_rectangle_single_byte_symbols: (
                    2,
                    4,
                    &[1,2,3,4,5,6,7,8],
                    &[&[1,2,3,4], &[5,6,7,8]],
                    &[&[1,5], &[2,6], &[3,7], &[4,8]]
                ),
                aligned_rectangle_double_byte_symbols: (
                    2,
                    3,
                    &[1,2,3,4,5,6,7,8,9,10,11,12],
                    &[&[1,2,3,4,5,6], &[7,8,9,10,11,12]],
                    &[&[1,2,7,8], &[3,4,9,10], &[5,6,11,12]]
                ),
                misaligned_square_double_byte_symbols: (
                    2,
                    2,
                    &[1,2,3,4,5],
                    &[&[1,2,3,4], &[5,0,0,0]],
                    &[&[1,2,5,0],&[3,4,0,0]]
                ),
                misaligned_rectangle_double_byte_symbols: (
                    2,
                    3,
                    &[1,2,3,4,5,6,7,8],
                    &[&[1,2,3,4,5,6], &[7,8,0,0,0,0]],
                    &[&[1,2,7,8], &[3,4,0,0], &[5,6,0,0]]
                ),
            ]
        }
        fn test_matrix_construction(
            source_symbols_primary: u16,
            source_symbols_secondary: u16,
            blob: &[u8],
            rows: &[&[u8]],
            columns: &[&[u8]],
        ) {
            let config = EncodingConfig::new(
                source_symbols_primary,
                source_symbols_secondary,
                3 * (source_symbols_primary + source_symbols_secondary) as u32,
            );
            let blob_encoder = config.get_blob_encoder(blob).unwrap();
            assert_eq!(blob_encoder.rows, rows);
            assert_eq!(blob_encoder.columns, columns);
        }
    }

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
        let recovered = Sliver::<Primary>::recover_sliver(recovery_symbols);
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
        let n_to_recover_from = source_symbols_primary.max(source_symbols_secondary);
        let mut rng = StdRng::seed_from_u64(42);

        for (idx, pair) in pairs.iter().enumerate() {
            // Randomize the indices from where to get the recovery symbols.
            let mut recover_from = Vec::from_iter(0..n_shards as usize);
            recover_from.shuffle(&mut rng);
            let _ = recover_from.split_off(n_to_recover_from as usize);

            // Recover the primary sliver.
            let recovery_symbols = recover_from
                .iter()
                .map(|&i| pairs[i].secondary.recovery_symbol_for_sliver(i, idx))
                .collect::<std::result::Result<Vec<_>, RecoveryError>>()?;
            let recovered = Sliver::<Primary>::recover_sliver(recovery_symbols)?;
            assert_eq!(recovered.unwrap(), pair.primary);

            // Recover the secondary sliver.
            let recovery_symbols = recover_from
                .iter()
                .map(|&i| pairs[i].primary.recovery_symbol_for_sliver(i, idx))
                .collect::<std::result::Result<Vec<_>, RecoveryError>>()?;
            let recovered = Sliver::<Secondary>::recover_sliver(recovery_symbols)?;
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
        let primary = Sliver::<Primary>::new(sliver_bytes, symbol_size);
        let secondary = Sliver::<Secondary>::new(sliver_bytes, symbol_size);

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
            Sliver::<Primary>::new([], 1).single_recovery_symbol(42),
            Err(RecoveryError::EncodeError(EncodeError::EmptyData))
        );
        assert_eq!(
            Sliver::<Secondary>::new([], 1).single_recovery_symbol(42),
            Err(RecoveryError::EncodeError(EncodeError::EmptyData))
        );
    }

    #[test]
    fn test_recovery_symbols_empty_sliver() {
        initialize_encoding_config(3, 3, 10);
        assert_eq!(
            Sliver::<Primary>::new([], 1).recovery_symbols(),
            Err(RecoveryError::EncodeError(EncodeError::EmptyData))
        );
        assert_eq!(
            Sliver::<Secondary>::new([], 1).recovery_symbols(),
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
        let result = Sliver::<Primary>::new([1, 2, 3, 4, 5, 6], 2).single_recovery_symbol(index);
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
                Sliver::<Secondary>::recover_sliver(primary_slivers.iter().enumerate().map(
                    |(source_idx, p)| {
                        p.recovery_symbol_for_sliver(source_idx, target_idx)
                            .unwrap()
                    },
                ))
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
                        s.recovery_symbol_for_sliver(source_idx, target_idx)
                            .unwrap()
                    }),
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
