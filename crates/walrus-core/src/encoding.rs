#![allow(missing_docs, unused)]

//! TODO(mlegner): Describe encoding algorithm.

use std::{cmp::min, iter::repeat, marker::PhantomData, sync::OnceLock};

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
/// # Returns
///
/// The global encoding configuration.
///
/// # Panics
///
/// Panics if the parameters are inconsistent with Byzantine fault tolerance; i.e., if the
/// number of source symbols of the primary encoding is equal to or greater than a 1/3 of the
/// number of shards, or if the number of source symbols of the secondary encoding equal to or
/// greater than a 1/3 of the number of shards.
///
/// Panics if the number of primary or secondary source symbols is larger than
/// [`MAX_SOURCE_SYMBOLS_PER_BLOCK`].
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
/// [`initialize_encoding_config`]. Panics otherwise.
/// `None` otherwise.
pub fn get_encoding_config() -> &'static EncodingConfig {
    ENCODING_CONFIG
        .get()
        .expect("must first be initialized with `initialize_encoding_config`")
}

/// Error type returned when encoding a blob or sliver fails.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum EncodeError {
    /// Error returned if the provided data is too large to be encoded with this encoder.
    #[error("the data is to large to be encoded")]
    DataTooLarge,
}

/// Marker trait to indicate the encoding axis (primary or secondary).
pub trait EncodingAxis: Clone + PartialEq + Eq {
    /// Returns the number of source symbols configured for this type.
    fn source_symbols_count(config: &EncodingConfig) -> u16;

    /// Returns the pre-generated encoding plan this type.
    fn encoding_plan(config: &EncodingConfig) -> &SourceBlockEncodingPlan;

    /// The maximum size in bytes of data that can be encoded with this encoding.
    #[inline]
    fn max_data_size_primary(config: &EncodingConfig) -> usize {
        Self::source_symbols_count(config) as usize * MAX_SYMBOL_SIZE
    }
}

/// Marker type to indicate the primary encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Primary;
impl EncodingAxis for Primary {
    #[inline]
    fn source_symbols_count(config: &EncodingConfig) -> u16 {
        config.source_symbols_primary
    }

    #[inline]
    fn encoding_plan(config: &EncodingConfig) -> &SourceBlockEncodingPlan {
        &config.encoding_plan_primary
    }
}

/// Marker type to indicate the secondary encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Secondary;
impl EncodingAxis for Secondary {
    #[inline]
    fn source_symbols_count(config: &EncodingConfig) -> u16 {
        config.source_symbols_secondary
    }

    #[inline]
    fn encoding_plan(config: &EncodingConfig) -> &SourceBlockEncodingPlan {
        &config.encoding_plan_secondary
    }
}

/// Encoded data corresponding to a single [`EncodingAxis`] assigned to one shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sliver<T: EncodingAxis> {
    /// The encoded data.
    pub data: Vec<u8>,
    phantom: PhantomData<T>,
}

/// Combination of a primary and secondary sliver of one shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SliverPair {
    /// Index of this sliver pair.
    ///
    /// Sliver pair `i` contains the primary sliver `i` and the secondary sliver `n_shards-i-1`.
    // TODO(mlegner): Link to sliver->shard assignment.
    pub index: u16,
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
    /// of symbols per secondary sliver. It must be strictly less than 2/3 of `n_shards`.
    source_symbols_primary: u16,
    /// The number of source symbols for the secondary encoding, which is, simultaneously, the
    /// number of symbols per primary sliver. It must be strictly less than 1/3 of `n_shards`.
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
    /// * `data` - The data to be encoded. Does not have to be padded.
    ///
    /// # Errors
    ///
    /// Returns an [`EncodeError::DataTooLarge`] if the `data` is too large.
    pub fn get_encoder<'a, E: EncodingAxis>(
        &self,
        data: &'a [u8],
    ) -> Result<Encoder<'a>, EncodeError> {
        Encoder::new(
            data,
            E::source_symbols_count(self),
            self.n_shards,
            E::encoding_plan(self),
        )
    }

    /// Returns a [`BlobEncoder`] to encode a blob into [`SliverPair`s][`SliverPair`].
    ///
    /// # Arguments
    ///
    /// * `blob` - The blob to be encoded. Does not have to be padded.
    ///
    /// # Errors
    ///
    /// Returns an [`EncodeError::DataTooLarge`] if the `blob` is too large to be encoded.
    pub fn get_blob_encoder(&self, blob: &[u8]) -> Result<BlobEncoder, EncodeError> {
        BlobEncoder::new(blob, self)
    }
}

pub struct Encoder<'a> {
    raptorq_encoder: SourceBlockEncoder,
    // Reference to the unpadded data.
    data: &'a [u8],
    // INV: symbol_size <= MAX_SYMBOL_SIZE.
    symbol_size: usize,
    source_symbols_count: u16,
    n_shards: u32,
}

impl<'a> Encoder<'a> {
    pub fn new(
        data: &'a [u8],
        source_symbols_count: u16,
        n_shards: u32,
        encoding_plan: &SourceBlockEncodingPlan,
    ) -> Result<Self, EncodeError> {
        let Some(symbol_size) = utils::compute_symbol_size(data.len(), source_symbols_count.into())
        else {
            return Err(EncodeError::DataTooLarge);
        };
        Ok(Self {
            raptorq_encoder: SourceBlockEncoder::with_encoding_plan2(
                0,
                &utils::get_transmission_info(symbol_size),
                data,
                encoding_plan,
            ),
            data,
            symbol_size,
            source_symbols_count,
            n_shards,
        })
    }

    fn padded_data(&self) -> impl Iterator<Item = &u8> {
        let source_length = self.symbol_size * self.source_symbols_count as usize;
        self.data
            .iter()
            .chain(repeat(&0u8).take(self.n_shards as usize - source_length))
    }

    pub fn encode_range(&self, start: u32, end: u32) -> Vec<Vec<u8>> {
        assert!(end >= start);
        if end <= self.source_symbols_count as u32 {
            self.raptorq_encoder.source_packets()[start as usize..end as usize]
                .iter()
                .map(|packet| packet.data().into())
                .collect()
        } else if start < self.source_symbols_count as u32 {
            self.raptorq_encoder.source_packets()[start as usize..]
                .iter()
                .chain(
                    self.raptorq_encoder
                        .repair_packets(0, end - self.source_symbols_count as u32)
                        .iter(),
                )
                .map(|packet| packet.data().into())
                .collect()
        } else {
            self.raptorq_encoder
                .repair_packets(start - self.source_symbols_count as u32, end - start)
                .into_iter()
                .map(|packet| packet.data().into())
                .collect()
        }
    }

    // pub fn all_source_symbols(&self) -> IntoChunks<impl Iterator<Item = &u8>> {
    //     self.padded_data().chunks(self.symbol_size)
    // }

    pub fn encode_all(&self) -> impl Iterator<Item = Vec<u8>> {
        self.raptorq_encoder
            .source_packets()
            .into_iter()
            .chain(
                self.raptorq_encoder
                    .repair_packets(0, self.n_shards - self.source_symbols_count as u32),
            )
            .map(|packet| packet.data().into())
    }

    pub fn encode_all_repair_symbols(&self) -> impl Iterator<Item = Vec<u8>> {
        self.raptorq_encoder
            .repair_packets(0, self.n_shards - self.source_symbols_count as u32)
            .into_iter()
            .map(|packet| packet.data().into())
    }
}

pub struct BlobEncoder<'a> {
    // INV: symbol_size <= MAX_SYMBOL_SIZE
    symbol_size: usize,
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
    config: &'a EncodingConfig,
}

impl<'a> BlobEncoder<'a> {
    pub fn new(blob: &[u8], config: &'a EncodingConfig) -> Result<Self, EncodeError> {
        let Some(symbol_size) =
            utils::compute_symbol_size(blob.len(), config.source_symbols_per_blob())
        else {
            return Err(EncodeError::DataTooLarge);
        };
        let count_columns = config.source_symbols_secondary as usize;
        let count_rows = config.source_symbols_primary as usize;
        let row_step = count_columns * symbol_size;
        let column_step = count_rows * symbol_size;

        // Initializing rows and columns with 0s implicitly takes care of padding.
        let mut rows = vec![vec![0u8; row_step]; count_rows];
        let mut columns = vec![vec![0u8; column_step]; count_columns];

        // TODO(mlegner): make this more efficient and avoid copying as much as possible.
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
            symbol_size,
            rows,
            columns,
            config,
        })
    }

    pub fn encode(&self) -> Vec<SliverPair> {
        let config = get_encoding_config();
        let mut result: Vec<SliverPair> = Vec::with_capacity(self.config.n_shards as usize);

        // TODO(mlegner): Can we reduce the amount of duplicated code here?
        for (r, row) in self.rows.iter().enumerate() {
            // We can take the rows directly as the first few primary slivers.
            result[r].primary.data[..].copy_from_slice(row);

            // The secondary slivers contain several symbols resulting from encoding the rows.
            // We reverse the order of the secondary slivers to ensure that the source data is
            // spread over as many shards as possible.
            for (n, symbol) in config
                .get_encoder::<Secondary>(row)
                .expect("size has already been checked")
                .encode_all_repair_symbols() // We only need the repair symbols.
                .enumerate()
            {
                result[self.config.n_shards as usize
                    - 1
                    - self.config.source_symbols_secondary as usize
                    - n]
                    .secondary
                    .data[self.symbol_size * r..self.symbol_size * (r + 1)]
                    .copy_from_slice(&symbol)
            }
        }
        for (c, column) in self.columns.iter().enumerate() {
            // We can take the columns directly as the last few secondary slivers.
            result[self.config.n_shards as usize - 1 - c].secondary.data[..]
                .copy_from_slice(column);

            // The primary slivers contain several symbols resulting from encoding the columns.
            for (n, symbol) in config
                .get_encoder::<Primary>(column)
                .expect("size has already been checked")
                .encode_all_repair_symbols() // We only need the repair symbols.
                .enumerate()
            {
                result[n].primary.data[self.symbol_size * c..self.symbol_size * (c + 1)]
                    .copy_from_slice(&symbol)
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use walrus_test_utils::param_test;

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
