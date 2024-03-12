// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{cmp, marker::PhantomData};

use super::{
    utils,
    DataTooLargeError,
    Decoder,
    DecodingSymbol,
    EncodingAxis,
    EncodingConfig,
    Primary,
    Secondary,
    Sliver,
    SliverPair,
};

/// Struct to perform the full blob encoding.
#[derive(Debug)]
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
    symbol_size: u16,
    /// Reference to the encoding configuration of this encoder.
    config: &'a EncodingConfig,
}

// TODO(mlegner): Improve memory management and copying for BlobEncoder (#45).
impl<'a> BlobEncoder<'a> {
    /// Creates a new `BlobEncoder` to encode the provided `blob` with the provided configuration.
    ///
    /// This creates the message matrix, padding with zeros if necessary. The actual encoding can be
    /// performed with the [`encode()`][Self::encode] method.
    pub fn new(config: &'a EncodingConfig, blob: &[u8]) -> Result<Self, DataTooLargeError> {
        let Some(symbol_size) =
            utils::compute_symbol_size(blob.len(), config.source_symbols_per_blob())
        else {
            return Err(DataTooLargeError);
        };
        let symbol_usize = symbol_size as usize;
        let n_columns = config.source_symbols_secondary as usize;
        let n_rows = config.source_symbols_primary as usize;
        let row_step = n_columns * symbol_usize;
        let column_step = n_rows * symbol_usize;

        // Initializing rows and columns with 0s implicitly takes care of padding.
        let mut rows = vec![vec![0u8; row_step]; n_rows];
        let mut columns = vec![vec![0u8; column_step]; n_columns];

        for (row, chunk) in rows.iter_mut().zip(blob.chunks(row_step)) {
            row[..chunk.len()].copy_from_slice(chunk);
        }
        for (c, col) in columns.iter_mut().enumerate() {
            for (r, target_chunk) in col.chunks_mut(symbol_usize).enumerate() {
                let copy_index_start = cmp::min(r * row_step + c * symbol_usize, blob.len());
                let copy_index_end = cmp::min(copy_index_start + symbol_usize, blob.len());
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
                primary: Sliver::new_empty(n_columns, self.symbol_size, i),
                secondary: Sliver::new_empty(
                    n_rows,
                    self.symbol_size,
                    self.config.sliver_index_from_pair_index::<Secondary>(i),
                ),
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

/// Struct to reconstruct a blob from either [`Primary`] (default) or [`Secondary`]
/// [`Sliver`s][Sliver].
#[derive(Debug)]
pub struct BlobDecoder<T: EncodingAxis = Primary> {
    _decoding_axis: PhantomData<T>,
    decoders: Vec<Decoder>,
    blob_size: usize,
    symbol_size: u16,
    n_source_symbols: u16,
}

impl<T: EncodingAxis> BlobDecoder<T> {
    /// Creates a new `BlobDecoder` to decode a blob of size `blob_size` using the provided
    /// configuration.
    ///
    /// The generic parameter specifies from which type of slivers the decoding will be performed.
    ///
    /// This function creates the necessary [`Decoder`s][Decoder] for the decoding; actual decoding
    /// can be performed with the [`decode()`][Self::decode] method.
    ///
    /// # Errors
    ///
    /// Returns a [`DataTooLargeError`] if the `blob_size` is too large to be decoded.
    pub fn new(config: &EncodingConfig, blob_size: usize) -> Result<Self, DataTooLargeError> {
        let n_source_symbols = config.n_source_symbols::<T>();
        let Some(symbol_size) = config.symbol_size_for_blob(blob_size) else {
            return Err(DataTooLargeError);
        };
        Ok(Self {
            _decoding_axis: PhantomData,
            decoders: vec![
                Decoder::new(n_source_symbols, symbol_size);
                config.n_source_symbols::<T::OrthogonalAxis>() as usize
            ],
            blob_size,
            symbol_size,
            n_source_symbols,
        })
    }

    /// Attempts to decode the source blob from the provided slivers.
    ///
    /// Returns the source blob as a byte vector if decoding succeeds or `None` if decoding fails.
    ///
    /// Slivers of incorrect length are ignored and silently dropped.
    ///
    /// If decoding failed due to an insufficient number of provided slivers, it can be continued
    /// by additional calls to [`decode`][Self::decode] providing more slivers.
    pub fn decode(&mut self, slivers: impl IntoIterator<Item = Sliver<T>>) -> Option<Vec<u8>> {
        // Depending on the decoding axis, this represents the message matrix's columns (primary)
        // or rows (secondary).
        let mut columns_or_rows = Vec::with_capacity(self.decoders.len());
        let mut decoding_successful = false;

        for sliver in slivers {
            if sliver.symbols.len() != self.decoders.len()
                || sliver.symbols.symbol_size() != self.symbol_size
            {
                // Ignore slivers of incorrect length or incorrect symbol size.
                // Question(mlegner): Should we return an error instead? Or at least log this?
                continue;
            }
            for (i, symbol) in sliver.symbols.to_symbols().enumerate() {
                if let Some(decoded_data) = self.decoders[i]
                    // NOTE: The encoding axis of the following symbol is irrelevant, but since we
                    // are reconstructing from slivers of type `T`, it should be of type `T`.
                    .decode([DecodingSymbol::<T>::new(sliver.index, symbol.into())])
                {
                    // If one decoding succeeds, all succeed as they have identical
                    // encoding/decoding matrices.
                    decoding_successful = true;
                    columns_or_rows.push(decoded_data);
                }
            }
            // Stop decoding as soon as we are done.
            if decoding_successful {
                break;
            }
        }

        if !decoding_successful {
            return None;
        }

        let mut blob: Vec<u8> = if T::IS_PRIMARY {
            // Primary decoding: transpose columns to get to the original blob.
            let mut columns: Vec<_> = columns_or_rows.into_iter().map(|c| c.into_iter()).collect();
            (0..self.n_source_symbols)
                .flat_map(|_| {
                    { columns.iter_mut().map(|c| c.take(self.symbol_size.into())) }
                        .flatten()
                        .collect::<Vec<u8>>()
                })
                .collect()
        } else {
            // Secondary decoding: these are the rows and can be used directly as the blob.
            columns_or_rows.into_iter().flatten().collect()
        };

        blob.truncate(self.blob_size);
        Some(blob)
    }
}

#[cfg(test)]
mod tests {

    use rand::{rngs::StdRng, SeedableRng};
    use walrus_test_utils::param_test;

    use super::*;
    use crate::encoding::EncodingConfig;

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

    #[test]
    fn test_blob_encode_decode() {
        let blob = utils::large_random_data(31415);
        let source_symbols_primary = 11;
        let source_symbols_secondary = 23;

        let config = EncodingConfig::new(
            source_symbols_primary,
            source_symbols_secondary,
            3 * (source_symbols_primary + source_symbols_secondary) as u32,
        );

        let slivers_for_decoding = utils::get_random_subset(
            config.get_blob_encoder(&blob).unwrap().encode(),
            &mut StdRng::seed_from_u64(42),
            cmp::max(source_symbols_primary, source_symbols_secondary) as usize,
        );

        let mut primary_decoder = config.get_blob_decoder::<Primary>(blob.len()).unwrap();
        assert_eq!(
            primary_decoder
                .decode(
                    slivers_for_decoding
                        .clone()
                        .map(|p| p.primary)
                        .take(source_symbols_primary.into())
                )
                .unwrap(),
            blob
        );

        let mut secondary_decoder = config.get_blob_decoder::<Secondary>(blob.len()).unwrap();
        assert_eq!(
            secondary_decoder
                .decode(
                    slivers_for_decoding
                        .map(|p| p.secondary)
                        .take(source_symbols_secondary.into())
                )
                .unwrap(),
            blob
        );
    }
}
