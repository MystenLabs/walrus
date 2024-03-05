//! TODO(mlegner): Describe encoding algorithm in detail (#50).

use std::{cmp::min, marker::PhantomData, ops::Range, sync::OnceLock};

use raptorq::{SourceBlockEncoder, SourceBlockEncodingPlan};
use thiserror::Error;

mod utils;

/// The maximum length in bytes of a single symbol in RaptorQ.
pub const MAX_SYMBOL_SIZE: usize = u16::MAX as usize;

/// The maximum number of source symbols per block for RaptorQ.
pub const MAX_SOURCE_SYMBOLS_PER_BLOCK: u16 = 56403;

/// Global encoding configuration with pre-generated encoding plans.
static ENCODING_CONFIG: OnceLock<EncodingConfig> = OnceLock::new();

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
pub fn initialize_encoding_config(
    source_symbols_primary: u16,
    source_symbols_secondary: u16,
    n_shards: u32,
) -> &'static EncodingConfig {
    ENCODING_CONFIG.get_or_init(|| {
        EncodingConfig::new(source_symbols_primary, source_symbols_secondary, n_shards)
    })
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
pub fn get_encoding_config() -> &'static EncodingConfig {
    ENCODING_CONFIG
        .get()
        .expect("must first be initialized with `initialize_encoding_config`")
}

/// Error returned when the data is too large to be encoded with this encoder.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("the data is to large to be encoded")]
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

/// Marker trait to indicate the encoding axis (primary or secondary).
pub trait EncodingAxis: Clone + PartialEq + Eq + Default {
    /// Whether this corresponds to the primary (true) or secondary (false) encoding.
    const IS_PRIMARY: bool;
}

/// Marker type to indicate the primary encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Primary;
impl EncodingAxis for Primary {
    const IS_PRIMARY: bool = true;
}

/// Marker type to indicate the secondary encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Secondary;
impl EncodingAxis for Secondary {
    const IS_PRIMARY: bool = false;
}

/// Encoded data corresponding to a single [`EncodingAxis`] assigned to one shard.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Sliver<T: EncodingAxis> {
    /// The encoded data.
    pub data: Vec<u8>,
    phantom: PhantomData<T>,
}

impl<T: EncodingAxis> Sliver<T> {
    /// Creates a new `Sliver` copying the provided slice of bytes.
    pub fn new(slice: &[u8]) -> Self {
        Self {
            data: slice.into(),
            phantom: PhantomData,
        }
    }

    /// Creates a new `Sliver` with empty data of specified length.
    pub fn new_empty(length: usize) -> Self {
        Self {
            data: vec![0; length],
            phantom: PhantomData,
        }
    }

    /// Copies the provided symbol to the location specified by the index.
    ///
    /// # Panics
    ///
    /// Panics if `self.data.len() < index * (symbol.len() + 1)`.
    pub fn copy_symbol_to(&mut self, index: usize, symbol: &[u8]) -> &mut Self {
        self.data[symbol.len() * index..symbol.len() * (index + 1)].copy_from_slice(symbol);
        self
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

/// Struct to perform the full blob encoding.
pub struct BlobEncoder<'a> {
    /// Rows of the message matrix.
    ///
    /// The outer vector has length `source_symbols_primary`, and each inner vector has length
    /// `source_symbols_secondary`.
    rows: Vec<Vec<u8>>,
    /// Columns of the message matrix.
    ///
    /// The outer vector has length `source_symbols_secondary`, and each inner vector has length
    /// `source_symbols_primary`.
    columns: Vec<Vec<u8>>,
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
                primary: Sliver::new_empty(n_columns),
                secondary: Sliver::new_empty(n_rows),
            })
        }

        // The first `n_rows` primary slivers and the last `n_columns` secondary slivers can be
        // directly copied from the message matrix.
        for (row, sliver_pair) in self.rows.iter().zip(sliver_pairs.iter_mut()) {
            sliver_pair.primary.data.copy_from_slice(row)
        }
        for (column, sliver_pair) in self.columns.iter().zip(sliver_pairs.iter_mut().rev()) {
            sliver_pair.secondary.data.copy_from_slice(column)
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
    use walrus_test_utils::param_test;

    use super::*;

    // TODO(mlegner): Add tests for the actual encoding (#28)!

    mod slivers {
        use super::*;

        param_test! {
            copy_symbol_to_modifies_empty_sliver_correctly: [
                empty_symbol_1: (0, 0, &[], &[]),
                empty_symbol_2: (2, 0, &[], &[0,0]),
                non_empty_symbol_aligned_1: (4, 0, &[1,2], &[1,2,0,0]),
                non_empty_symbol_aligned_2: (4, 1, &[1,2], &[0,0,1,2]),
                non_empty_symbol_misaligned_1: (5, 0, &[1,2], &[1,2,0,0,0]),
                non_empty_symbol_misaligned_2: (5, 1, &[1,2], &[0,0,1,2,0]),
            ]
        }
        fn copy_symbol_to_modifies_empty_sliver_correctly(
            sliver_length: usize,
            index: usize,
            symbol: &[u8],
            expected_sliver_data: &[u8],
        ) {
            assert_eq!(
                Sliver::<Primary>::new_empty(sliver_length)
                    .copy_symbol_to(index, symbol)
                    .data,
                expected_sliver_data
            );
        }

        #[test]
        fn new_sliver_copies_provided_slice() {
            let slice = [1, 2, 3, 4, 5];
            assert_eq!(Sliver::<Primary>::new(&slice).data, slice)
        }
    }

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
