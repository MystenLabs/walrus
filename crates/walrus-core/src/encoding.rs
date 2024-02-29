#![allow(missing_docs, unused)]

//! TODO(mlegner): Describe encoding algorithm.

use std::sync::OnceLock;

use raptorq::{SourceBlockEncoder, SourceBlockEncodingPlan};
use thiserror::Error;

mod utils;

/// The maximum length in bytes of a single symbol in RaptorQ.
const MAX_SYMBOL_SIZE: u16 = u16::MAX;

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
/// * `total_shards` - The total number of shards.
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
pub fn initialize_encoding_config(
    source_symbols_primary: u16,
    source_symbols_secondary: u16,
    total_shards: u32,
) -> &'static EncodingConfig {
    ENCODING_CONFIG.get_or_init(|| {
        EncodingConfig::new(
            source_symbols_primary,
            source_symbols_secondary,
            total_shards,
        )
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sliver {
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SliverPair {
    pub index: u16,
    pub primary: Sliver,
    pub secondary: Sliver,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodingConfig {
    /// The number of source symbols for the primary encoding, which is, simultaneously, the number
    /// of symbols per secondary sliver. It must be strictly less than 2/3 of `total_shards`.
    source_symbols_primary: u16,
    /// The number of source symbols for the secondary encoding, which is, simultaneously, the
    /// number of symbols per primary sliver. It must be strictly less than 1/3 of `total_shards`.
    source_symbols_secondary: u16,
    /// The number of shards.
    total_shards: u32,
    /// Encoding plan to speed up the primary encoding.
    encoding_plan_primary: SourceBlockEncodingPlan,
    /// Encoding plan to speed up the secondary encoding.
    encoding_plan_secondary: SourceBlockEncodingPlan,
}

pub enum EncodingType {
    Primary,
    Secondary,
}

impl EncodingConfig {
    fn new(source_symbols_primary: u16, source_symbols_secondary: u16, total_shards: u32) -> Self {
        assert!(
            3 * (source_symbols_primary as u32) < total_shards,
            "the primary encoding can be at most a 1/3 encoding"
        );
        assert!(
            3 * (source_symbols_secondary as u32) < 2 * total_shards,
            "the secondary encoding can be at most a 2/3 encoding"
        );
        // TODO(mlegner): Check that the parameters are compatible with the block size of RaptorQ.

        Self {
            source_symbols_primary,
            source_symbols_secondary,
            total_shards,
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
            * MAX_SYMBOL_SIZE as usize
    }

    /// The maximum size in bytes of data that can be encoded with the primary encoding.
    #[inline]
    pub fn max_data_size_primary(&self) -> usize {
        self.source_symbols_primary as usize * MAX_SYMBOL_SIZE as usize
    }

    /// The maximum size in bytes of data that can be encoded with the secondary encoding.
    #[inline]
    pub fn max_data_size_secondary(&self) -> usize {
        self.source_symbols_secondary as usize * MAX_SYMBOL_SIZE as usize
    }

    /// The number of symbols a blob is split into.
    #[inline]
    pub fn source_symbols_per_blob(&self) -> usize {
        self.source_symbols_primary as usize * self.source_symbols_secondary as usize
    }

    pub fn get_encoder<'a>(
        &self,
        encoding_type: EncodingType,
        data: &'a [u8],
    ) -> Result<Encoder<'a>, EncodeError> {
        let (max_data_size, source_symbols_count, encoding_plan) = match encoding_type {
            EncodingType::Primary => (
                self.max_data_size_primary(),
                self.source_symbols_primary,
                &self.encoding_plan_primary,
            ),
            EncodingType::Secondary => (
                self.max_data_size_secondary(),
                self.source_symbols_secondary,
                &self.encoding_plan_secondary,
            ),
        };
        if data.len() > max_data_size {
            return Err(EncodeError::DataTooLarge);
        }
        Ok(Encoder::new(
            data,
            source_symbols_count,
            self.total_shards,
            encoding_plan,
        ))
    }

    pub fn get_blob_encoder(&self, data: &[u8]) -> BlobEncoder {
        BlobEncoder::new(
            data,
            self.source_symbols_primary,
            self.source_symbols_secondary,
            utils::compute_symbol_size(data.len(), self.source_symbols_per_blob()),
            self.total_shards,
        )
    }
}

pub struct Encoder<'a> {
    raptorq_encoder: SourceBlockEncoder,
    data: &'a [u8],
    symbol_size: u16,
    source_symbols_count: u16,
    total_shards: u32,
}

impl<'a> Encoder<'a> {
    pub fn new(
        data: &'a [u8],
        source_symbols_count: u16,
        total_shards: u32,
        encoding_plan: &SourceBlockEncodingPlan,
    ) -> Self {
        let symbol_size = utils::compute_symbol_size(data.len(), source_symbols_count.into());
        Self {
            raptorq_encoder: SourceBlockEncoder::with_encoding_plan2(
                0,
                &utils::get_transmission_info(symbol_size),
                data,
                encoding_plan,
            ),
            data,
            symbol_size,
            source_symbols_count,
            total_shards,
        }
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

    pub fn encode_all(&self) -> impl Iterator<Item = Vec<u8>> {
        self.raptorq_encoder
            .source_packets()
            .into_iter()
            .chain(
                self.raptorq_encoder
                    .repair_packets(0, self.total_shards - self.source_symbols_count as u32),
            )
            .map(|packet| packet.data().into())
    }

    pub fn encode_all_repair_symbols(&self) -> impl Iterator<Item = Vec<u8>> {
        self.raptorq_encoder
            .repair_packets(0, self.total_shards - self.source_symbols_count as u32)
            .into_iter()
            .map(|packet| packet.data().into())
    }
}

pub struct BlobEncoder {
    source_symbols_primary: u16,
    source_symbols_secondary: u16,
    symbol_size: u16,
    matrix_rows: Vec<Vec<u8>>,
    matrix_columns: Vec<Vec<u8>>,
    total_shards: u32,
}

impl BlobEncoder {
    pub fn new(
        blob: &[u8],
        source_symbols_primary: u16,
        source_symbols_secondary: u16,
        symbol_size: u16,
        total_shards: u32,
    ) -> Self {
        let count_columns = source_symbols_secondary as usize;
        let count_rows = source_symbols_primary as usize;
        let row_step = count_columns * symbol_size as usize;
        let column_step = count_rows * symbol_size as usize;
        let mut rows = vec![vec![0u8; row_step]; count_rows];
        let mut columns = vec![vec![0u8; column_step]; count_columns];

        // TODO(mlegner): add appropriate padding.
        // TODO(mlegner): make this more efficient and avoid copying as much as possible.
        for i in 0..count_rows {
            rows[i].copy_from_slice(&blob[row_step * i..row_step * (i + 1)])
        }
        for i in 0..count_columns {
            for j in 0..count_rows {
                columns[i][symbol_size as usize * j..symbol_size as usize * (j + 1)]
                    .copy_from_slice(
                        &blob[(count_columns * j + i) * symbol_size as usize
                            ..(count_columns * j + i + 1) * symbol_size as usize],
                    )
            }
        }
        Self {
            source_symbols_primary,
            source_symbols_secondary,
            symbol_size,
            matrix_rows: rows,
            matrix_columns: columns,
            total_shards,
        }
    }

    pub fn encode(&self) -> Vec<SliverPair> {
        let config = get_encoding_config();
        let mut result: Vec<SliverPair> = Vec::with_capacity(self.total_shards as usize);

        // TODO(mlegner): Can we reduce the amount of duplicated code here?
        for (r, row) in self.matrix_rows.iter().enumerate() {
            // We can take the rows directly as the first few primary slivers.
            result[r].primary.data[..].copy_from_slice(row);

            // The secondary slivers contain several symbols resulting from encoding the rows.
            // We reverse the order of the secondary slivers to ensure that the source data is
            // spread over as many shards as possible.
            for (n, symbol) in config
                .get_encoder(EncodingType::Secondary, row)
                .expect("size has already been checked")
                .encode_all_repair_symbols() // We only need the repair symbols.
                .enumerate()
            {
                result[self.total_shards as usize - 1 - self.source_symbols_secondary as usize - n]
                    .secondary
                    .data[self.symbol_size as usize * r..self.symbol_size as usize * (r + 1)]
                    .copy_from_slice(&symbol)
            }
        }
        for (c, column) in self.matrix_columns.iter().enumerate() {
            // We can take the columns directly as the last few secondary slivers.
            result[self.total_shards as usize - 1 - c].secondary.data[..].copy_from_slice(column);

            // The primary slivers contain several symbols resulting from encoding the columns.
            for (n, symbol) in config
                .get_encoder(EncodingType::Primary, column)
                .expect("size has already been checked")
                .encode_all_repair_symbols() // We only need the repair symbols.
                .enumerate()
            {
                result[n].primary.data
                    [self.symbol_size as usize * c..self.symbol_size as usize * (c + 1)]
                    .copy_from_slice(&symbol)
            }
        }
        result
    }
}
