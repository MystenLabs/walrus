// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{cmp, marker::PhantomData};

use fastcrypto::hash::Blake2b256;

use super::{
    utils,
    DataTooLargeError,
    Decoder,
    DecodingSymbol,
    DecodingVerificationError,
    EncodingAxis,
    EncodingConfig,
    Primary,
    Secondary,
    Sliver,
    SliverPair,
    Symbols,
};
use crate::{
    merkle::MerkleTree,
    metadata::{SliverPairMetadata, VerifiedBlobMetadataWithId},
    BlobId,
    EncodingType,
};

/// Struct to perform the full blob encoding.
#[derive(Debug)]
pub struct BlobEncoder<'a> {
    /// The size of the unencoded blob to be encoded.
    blob_size: usize,
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
            blob_size: blob.len(),
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
        let mut sliver_pairs = self.empty_sliver_pairs();

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

    /// Computes the metadata (blob ID, hashes) for the blob, without returning the slivers.
    pub fn compute_metadata(&self) -> VerifiedBlobMetadataWithId {
        let expanded_matrix = self.get_expanded_matrix();
        self.get_metadata(&expanded_matrix)
    }

    /// Encodes the blob with which `self` was created to a vector of [`SliverPair`s][SliverPair],
    /// and provides the relative [`VerifiedBlobMetadataWithId`].
    ///
    /// This function operates on the fully expanded message matrix for the blob. This matrix is
    /// used to compute the Merkle trees for the metadata, and to extract the sliver pairs. The
    /// returned blob metadata is considered to be verified as it is directly built from the data.
    pub fn encode_with_metadata(&self) -> (Vec<SliverPair>, VerifiedBlobMetadataWithId) {
        let expanded_matrix = self.get_expanded_matrix();
        let metadata = self.get_metadata(&expanded_matrix);

        let mut sliver_pairs = self.empty_sliver_pairs();
        // First compute the secondary slivers -- does not require consuming the matrix.
        self.write_secondary_slivers(&expanded_matrix, &mut sliver_pairs);
        // Then consume the matrix to get the primary slivers.
        self.write_primary_slivers(expanded_matrix, &mut sliver_pairs);

        (sliver_pairs, metadata)
    }

    /// Return a vector of empty [`SliverPair`] of length `n_shards`. Primary and secondary slivers
    /// are initialized with the appropriate `symbol_size` and `length`.
    fn empty_sliver_pairs(&self) -> Vec<SliverPair> {
        (0..self.config.n_shards)
            .map(|i| SliverPair::new_empty(self.config, self.symbol_size, i))
            .collect()
    }

    /// Compute the fully expanded message matrix by encoding rows and columns.
    ///
    /// The "matrix" is represented as vector of rows, where each row is a [`Symbols`] object. This
    /// choice simplifies indexing, and the rows of [`Symbols`] can then be directly truncated into
    /// primary slivers.
    fn get_expanded_matrix(&self) -> Vec<Symbols> {
        let mut expanded_matrix =
            vec![
                Symbols::zeros(self.config.n_shards as usize, self.symbol_size);
                self.config.n_shards as usize
            ];
        self.fill_systematic_with_rows(&mut expanded_matrix);
        self.expand_columns_for_primary(&mut expanded_matrix);
        self.expand_all_rows(&mut expanded_matrix);
        expanded_matrix
    }

    /// Fills the systematic part of the matrix using `self.rows`.
    fn fill_systematic_with_rows(&self, matrix: &mut [Symbols]) {
        for (destination_row, row) in matrix.iter_mut().zip(self.rows.iter()) {
            destination_row[0..self.config.source_symbols_secondary as usize].copy_from_slice(row);
        }
    }

    /// Expands the first `source_symbols_secondary` columns from `self.columns` to get all
    /// remaining primary slivers.
    fn expand_columns_for_primary(&self, matrix: &mut [Symbols]) {
        for (col_idx, col) in self.columns.iter().enumerate() {
            for (row_idx, symbol) in self
                .config
                .get_encoder::<Primary>(col)
                .expect("size has already been checked")
                .encode_all_repair_symbols()
                .enumerate()
            {
                matrix[self.rows.len() + row_idx][col_idx].copy_from_slice(&symbol);
            }
        }
    }

    /// Expands all `n_shards` primary slivers (rows) to completely fill the `n_shards * n_shards`
    /// expanded message matrix.
    fn expand_all_rows(&self, matrix: &mut [Symbols]) {
        for row in matrix.iter_mut() {
            for (col_idx, symbol) in self
                .config
                .get_encoder::<Secondary>(&row[0..self.columns.len()])
                .expect("size has already been checked")
                .encode_all_repair_symbols()
                .enumerate()
            {
                row[self.columns.len() + col_idx].copy_from_slice(&symbol)
            }
        }
    }

    /// Computes the sliver pair metadata from the expanded message matrix.
    fn get_metadata(&self, matrix: &[Symbols]) -> VerifiedBlobMetadataWithId {
        let mut metadata = vec![SliverPairMetadata::new_empty(); matrix.len()];
        self.write_secondary_metadata(matrix, &mut metadata);
        self.write_primary_metadata(matrix, &mut metadata);
        VerifiedBlobMetadataWithId::new_verified_from_metadata(
            metadata,
            EncodingType::RedStuff,
            self.blob_size as u64,
        )
    }

    fn col_symbols(
        &self,
        matrix: &'a [Symbols],
    ) -> impl Iterator<Item = impl ExactSizeIterator<Item = &'a [u8]> + '_> {
        (0..matrix.len()).map(move |col_idx| {
            matrix
                .iter()
                // Get the columns in reverse order `n_shards - col_idx - 1`.
                .map(move |row| {
                    row[self
                        .config
                        .sliver_index_from_pair_index::<Secondary>(col_idx as u32)
                        as usize]
                        .as_ref()
                })
        })
    }

    /// Writes the secondary metadata to the provided mutable slice.
    fn write_secondary_metadata(&self, matrix: &[Symbols], metadata: &mut [SliverPairMetadata]) {
        metadata
            .iter_mut()
            .zip(self.col_symbols(matrix))
            .for_each(|(metadata, symbols)| {
                metadata.secondary_hash = MerkleTree::<Blake2b256>::build(symbols).root();
            });
    }

    /// Writes the secondary slivers to the provided mutable slice.
    fn write_secondary_slivers(&self, matrix: &[Symbols], sliver_pairs: &mut [SliverPair]) {
        sliver_pairs
            .iter_mut()
            .zip(self.col_symbols(matrix))
            .for_each(|(sliver_pair, symbols)| {
                for (target_slice, symbol) in
                    sliver_pair.secondary.symbols.to_symbols_mut().zip(symbols)
                {
                    target_slice.copy_from_slice(symbol);
                }
            })
    }

    /// Writes the primary metadata to the provided mutable slice.
    fn write_primary_metadata(&self, matrix: &[Symbols], metadata: &mut [SliverPairMetadata]) {
        for (metadata, row) in metadata.iter_mut().zip(matrix.iter()) {
            metadata.primary_hash = MerkleTree::<Blake2b256>::build(row.to_symbols()).root();
        }
    }

    /// Writes the primary slivers to the provided mutable slice.
    ///
    /// Consumes the original matrix, as it creates the primary slivers by truncating the rows of
    /// the matrix.
    fn write_primary_slivers(&self, matrix: Vec<Symbols>, sliver_pairs: &mut [SliverPair]) {
        for (sliver_pair, mut row) in sliver_pairs.iter_mut().zip(matrix.into_iter()) {
            row.truncate(self.columns.len());
            sliver_pair.primary.symbols = row;
        }
    }
}

/// Struct to reconstruct a blob from either [`Primary`] (default) or [`Secondary`]
/// [`Sliver`s][Sliver].
#[derive(Debug)]
pub struct BlobDecoder<'a, T: EncodingAxis = Primary> {
    _decoding_axis: PhantomData<T>,
    decoders: Vec<Decoder>,
    blob_size: usize,
    symbol_size: u16,
    config: &'a EncodingConfig,
}

impl<'a, T: EncodingAxis> BlobDecoder<'a, T> {
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
    pub fn new(config: &'a EncodingConfig, blob_size: usize) -> Result<Self, DataTooLargeError> {
        let Some(symbol_size) = config.symbol_size_for_blob(blob_size) else {
            return Err(DataTooLargeError);
        };
        Ok(Self {
            _decoding_axis: PhantomData,
            decoders: vec![
                Decoder::new(config.n_source_symbols::<T>(), symbol_size);
                config.n_source_symbols::<T::OrthogonalAxis>() as usize
            ],
            blob_size,
            symbol_size,
            config,
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
            for (decoder, symbol) in self.decoders.iter_mut().zip(sliver.symbols.to_symbols()) {
                if let Some(decoded_data) = decoder
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
            (0..self.config.n_source_symbols::<T>())
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

    /// Attempts to decode the source blob from the provided slivers, and to verify that the decoded
    /// blob matches the blob ID.
    ///
    /// Internally, this function uses a [`BlobEncoder`] to recompute the metadata. This metadata is
    /// then compared against the provided [`BlobId`].
    ///
    /// If the decoding and the checks are successful, the function returns a tuple of two values:
    /// * the reconstructed source blob as a byte vector; and
    /// * the [`VerifiedBlobMetadataWithId`] corresponding to the source blob.
    ///
    /// It returns `None` if the decoding fails. If decoding failed due to an insufficient number of
    /// provided slivers, the decoding can be continued by additional calls to
    /// [`decode_and_verify`][Self::decode_and_verify] providing more slivers.
    ///
    /// # Errors
    ///
    /// If, upon successful decoding, the recomputed blob ID does not match the input blob ID,
    /// returns a [`DecodingVerificationError`].
    pub fn decode_and_verify(
        &mut self,
        blob_id: &BlobId,
        slivers: impl IntoIterator<Item = Sliver<T>>,
    ) -> Result<Option<(Vec<u8>, VerifiedBlobMetadataWithId)>, DecodingVerificationError> {
        if let Some(decoded_blob) = self.decode(slivers) {
            let blob_metadata = self
                .config
                .get_blob_encoder(&decoded_blob)
                .expect("the blob size cannot be too large since we were able to decode")
                .compute_metadata();
            if blob_metadata.blob_id() == blob_id {
                Ok(Some((decoded_blob, blob_metadata)))
            } else {
                Err(DecodingVerificationError)
            }
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use walrus_test_utils::{param_test, random_data, random_subset};

    use super::*;
    use crate::{
        encoding::{get_encoding_config, initialize_encoding_config, EncodingConfig},
        metadata::UnverifiedBlobMetadataWithId,
    };

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
        let blob = random_data(31415);
        let source_symbols_primary = 11;
        let source_symbols_secondary = 23;

        let config = EncodingConfig::new(
            source_symbols_primary,
            source_symbols_secondary,
            3 * (source_symbols_primary + source_symbols_secondary) as u32,
        );

        let slivers_for_decoding = random_subset(
            config.get_blob_encoder(&blob).unwrap().encode(),
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

    #[test]
    fn test_encode_with_metadata() {
        // A big test checking that:
        // 1. The sliver pairs produced by `encode_with_metadata` are the same as the ones produced
        //    by `encode`;
        // 2. the metadata produced by `encode_with_metadata` is the same as
        //    the metadata that can be computed from the sliver pairs directly.
        // 3. the metadata produced by `encode_with_metadata` is the same as
        //    the metadata produced by `compute_metadata_only`.
        // Takes long (O(1s)) to run.
        let blob = random_data(27182);
        let source_symbols_primary = 11;
        let source_symbols_secondary = 23;
        let n_shards = 3 * (source_symbols_primary + source_symbols_secondary) as u32;

        initialize_encoding_config(source_symbols_primary, source_symbols_secondary, n_shards);

        // Check that the encoding with and without metadata are identical.
        let sliver_pairs_1 = get_encoding_config()
            .get_blob_encoder(&blob)
            .unwrap()
            .encode();
        let blob_metadata_1 = get_encoding_config()
            .get_blob_encoder(&blob)
            .unwrap()
            .compute_metadata();
        let (sliver_pairs_2, blob_metadata_2) = get_encoding_config()
            .get_blob_encoder(&blob)
            .unwrap()
            .encode_with_metadata();
        assert_eq!(sliver_pairs_1, sliver_pairs_2);
        assert_eq!(blob_metadata_1, blob_metadata_2);

        // Check that the hashes obtained by re-encoding the sliver pairs are equivalent to the ones
        // obtained in the `encode_with_metadata` function.
        for (sliver_pair, pair_meta) in sliver_pairs_2
            .iter()
            .zip(blob_metadata_2.metadata().hashes.iter())
        {
            let pair_hash = sliver_pair
                .pair_leaf_input::<Blake2b256>()
                .expect("should be able to encode");
            let meta_hash = pair_meta.pair_leaf_input::<Blake2b256>();
            assert_eq!(pair_hash, meta_hash);
        }

        // Check that the blob metadata verifies.
        let unverified = UnverifiedBlobMetadataWithId::new(
            *blob_metadata_2.blob_id(),
            blob_metadata_2.metadata().clone(),
        );
        assert!(unverified
            .verify((n_shards as usize).try_into().unwrap())
            .is_ok());
    }

    #[test]
    fn test_encode_decode_and_verify() {
        let blob = random_data(16180);
        let source_symbols_primary = 11;
        let source_symbols_secondary = 23;
        let n_shards = 3 * (source_symbols_primary + source_symbols_secondary) as u32;

        initialize_encoding_config(source_symbols_primary, source_symbols_secondary, n_shards);

        let (slivers, metadata_enc) = get_encoding_config()
            .get_blob_encoder(&blob)
            .unwrap()
            .encode_with_metadata();
        let slivers_for_decoding = random_subset(slivers, source_symbols_primary as usize)
            .map(|s| s.primary)
            .collect::<Vec<_>>();
        let (blob_dec, metadata_dec) = get_encoding_config()
            .get_blob_decoder(blob.len())
            .unwrap()
            .decode_and_verify(metadata_enc.blob_id(), slivers_for_decoding)
            .unwrap()
            .unwrap();

        assert_eq!(blob, blob_dec);
        assert_eq!(metadata_enc, metadata_dec);
    }
}
