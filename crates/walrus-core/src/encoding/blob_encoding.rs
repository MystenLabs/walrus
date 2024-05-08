// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use alloc::{vec, vec::Vec};
use core::{
    cmp,
    marker::PhantomData,
    num::{NonZeroU16, NonZeroU64, NonZeroUsize},
    slice::Chunks,
};

use fastcrypto::hash::Blake2b256;

use super::{
    utils,
    Decoder,
    DecodingSymbol,
    DecodingVerificationError,
    EncodingAxis,
    EncodingConfig,
    InvalidDataSizeError,
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
    SliverIndex,
    SliverPairIndex,
};

/// Struct to perform the full blob encoding.
#[derive(Debug)]
pub struct BlobEncoder<'a> {
    /// A reference to the blob.
    // INV: `blob.len() > 0`
    blob: &'a [u8],
    /// The size of the encoded and decoded symbols.
    symbol_size: NonZeroU16,
    /// The number of rows of the message matrix.
    ///
    /// Stored as a `usize` for convenience, but guaranteed to be non-zero.
    n_rows: usize,
    /// The number of columns of the message matrix.
    ///
    /// Stored as a `usize` for convenience, but guaranteed to be non-zero.
    n_columns: usize,
    /// Reference to the encoding configuration of this encoder.
    config: &'a EncodingConfig,
}

impl<'a> BlobEncoder<'a> {
    /// Creates a new `BlobEncoder` to encode the provided `blob` with the provided configuration.
    ///
    /// The actual encoding can be performed with the [`encode()`][Self::encode] method.
    ///
    /// # Errors
    ///
    /// Returns an [`InvalidDataSizeError`] if the blob is empty or too large to be encoded. The
    /// latter happens in two cases:
    ///
    /// 1. If the blob is too large to fit into the message matrix with valid symbols. The maximum
    ///    blob size for a given [`EncodingConfig`] is accessible through the
    ///    [`EncodingConfig::max_blob_size`] method.
    /// 2. On 32-bit architectures, the maximally supported blob size can actually be smaller than
    ///    that due to limitations of the address space.
    pub fn new(config: &'a EncodingConfig, blob: &'a [u8]) -> Result<Self, InvalidDataSizeError> {
        let symbol_size =
            utils::compute_symbol_size_from_usize(blob.len(), config.source_symbols_per_blob())?;
        let n_rows = config.n_source_symbols::<Primary>().get().into();
        let n_columns = config.n_source_symbols::<Secondary>().get().into();

        Ok(Self {
            blob,
            symbol_size,
            n_rows,
            n_columns,
            config,
        })
    }

    /// Encodes the blob with which `self` was created to a vector of [`SliverPair`s][SliverPair].
    ///
    /// # Panics
    ///
    /// This function can panic if there is insufficient virtual memory for the encoded data,
    /// notably on 32-bit architectures. As there is an expansion factor of approximately 4.5, blobs
    /// larger than roughly 800 MiB cannot be encoded on 32-bit architectures.
    pub fn encode(&self) -> Vec<SliverPair> {
        let mut primary_slivers: Vec<_> = self.empty_slivers::<Primary>();
        let mut secondary_slivers: Vec<_> = self.empty_slivers::<Secondary>();

        // The first `n_rows` primary slivers and the last `n_columns` secondary slivers can be
        // directly copied from the blob.
        for (row, sliver) in self.rows().zip(primary_slivers.iter_mut()) {
            sliver.symbols.data_mut()[..row.len()].copy_from_slice(row);
        }
        for (column, sliver) in self.column_symbols().zip(secondary_slivers.iter_mut()) {
            sliver
                .symbols
                .to_symbols_mut()
                .zip(column)
                .for_each(|(dest, src)| dest[..src.len()].copy_from_slice(src))
        }

        // Compute the remaining primary slivers by encoding the columns (i.e., secondary slivers).
        for (col_index, column) in secondary_slivers.iter().take(self.n_columns).enumerate() {
            for (symbol, sliver) in self
                .config
                .get_encoder::<Primary>(column.symbols.data())
                .expect("size has already been checked")
                .encode_all_repair_symbols()
                .zip(primary_slivers.iter_mut().skip(self.n_rows))
            {
                sliver.copy_symbol_to(col_index, &symbol);
            }
        }

        // Compute the remaining secondary slivers by encoding the rows (i.e., primary slivers).
        for (r, row) in primary_slivers.iter().take(self.n_rows).enumerate() {
            for (symbol, sliver) in self
                .config
                .get_encoder::<Secondary>(row.symbols.data())
                .expect("size has already been checked")
                .encode_all_repair_symbols()
                .zip(secondary_slivers.iter_mut().skip(self.n_columns))
            {
                sliver.copy_symbol_to(r, &symbol);
            }
        }

        primary_slivers
            .into_iter()
            .zip(secondary_slivers.into_iter().rev())
            .map(|(primary, secondary)| SliverPair { primary, secondary })
            .collect()
    }

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
    pub fn encode_with_metadata(&self) -> (Vec<SliverPair>, VerifiedBlobMetadataWithId) {
        let mut expanded_matrix = self.get_expanded_matrix();
        let metadata = expanded_matrix.get_metadata();

        // This is just an optimization to free memory that is no longer needed at this point.
        expanded_matrix.drop_recovery_symbols();

        let mut sliver_pairs = self.empty_sliver_pairs();
        // First compute the secondary slivers -- does not require consuming the matrix.
        expanded_matrix.write_secondary_slivers(&mut sliver_pairs);
        // Then consume the matrix to get the primary slivers.
        expanded_matrix.write_primary_slivers(&mut sliver_pairs);

        (sliver_pairs, metadata)
    }

    /// Computes the metadata (blob ID, hashes) for the blob, without returning the slivers.
    pub fn compute_metadata(&self) -> VerifiedBlobMetadataWithId {
        self.get_expanded_matrix().get_metadata()
    }

    fn symbol_usize(&self) -> usize {
        self.symbol_size.get().into()
    }

    /// Returns a reference to the symbol at the provided indices in the message matrix.
    ///
    /// The length of the returned slice can be lower than `self.symbol_size` if the blob needs to
    /// be padded.
    fn symbol_at(&self, row_index: usize, col_index: usize) -> &[u8] {
        let start_index = cmp::min(
            self.symbol_usize() * (self.n_columns * row_index + col_index),
            self.blob.len(),
        );
        let end_index = cmp::min(start_index + self.symbol_usize(), self.blob.len());
        self.blob[start_index..end_index].as_ref()
    }

    fn column_symbols(
        &self,
    ) -> impl ExactSizeIterator<Item = impl ExactSizeIterator<Item = &[u8]>> {
        (0..self.n_columns).map(move |col_index| {
            (0..self.n_rows).map(move |row_index| self.symbol_at(row_index, col_index))
        })
    }

    fn rows(&self) -> Chunks<u8> {
        self.blob.chunks(self.n_columns * self.symbol_usize())
    }

    fn empty_slivers<T: EncodingAxis>(&self) -> Vec<Sliver<T>> {
        (0..self.config.n_shards().get())
            .map(|i| {
                Sliver::<T>::new_empty(
                    self.config.n_source_symbols::<T::OrthogonalAxis>().get(),
                    self.symbol_size,
                    SliverIndex(i),
                )
            })
            .collect()
    }

    /// Returns a vector of empty [`SliverPair`] of length `n_shards`. Primary and secondary slivers
    /// are initialized with the appropriate `symbol_size` and `length`.
    fn empty_sliver_pairs(&self) -> Vec<SliverPair> {
        (0..self.config.n_shards().get())
            .map(|i| SliverPair::new_empty(self.config, self.symbol_size, SliverPairIndex(i)))
            .collect()
    }

    /// Computes the fully expanded message matrix by encoding rows and columns.
    fn get_expanded_matrix(&self) -> ExpandedMessageMatrix {
        ExpandedMessageMatrix::new(self.config, self.symbol_size, self.blob)
    }
}

/// The representation of the expanded message matrix.
///
/// The expanded message matrix is represented as vector of rows, where each row is a [`Symbols`]
/// object. This choice simplifies indexing, and the rows of [`Symbols`] can then be directly
/// truncated into primary slivers.
struct ExpandedMessageMatrix<'a> {
    matrix: Vec<Symbols>,
    // INV: `blob.len() > 0`
    blob: &'a [u8],
    config: &'a EncodingConfig,
    /// The number of rows in the non-expanded message matrix.
    n_rows: usize,
    /// The number of columns in the non-expanded message matrix.
    n_columns: usize,
    symbol_size: NonZeroU16,
}

impl<'a> ExpandedMessageMatrix<'a> {
    fn new(config: &'a EncodingConfig, symbol_size: NonZeroU16, blob: &'a [u8]) -> Self {
        assert!(!blob.is_empty());
        let matrix = vec![
            Symbols::zeros(config.n_shards().get().into(), symbol_size);
            config.n_shards().get().into()
        ];
        let mut expanded_matrix = Self {
            matrix,
            blob,
            config,
            n_rows: config.n_source_symbols::<Primary>().get().into(),
            n_columns: config.n_source_symbols::<Secondary>().get().into(),
            symbol_size,
        };
        expanded_matrix.fill_systematic_with_rows();
        expanded_matrix.expand_columns_for_primary();
        expanded_matrix.expand_all_rows();
        expanded_matrix
    }

    /// Fills the systematic part of the matrix using `self.rows`.
    fn fill_systematic_with_rows(&mut self) {
        for (destination_row, row) in self.matrix.iter_mut().zip(
            self.blob
                .chunks(self.n_columns * usize::from(self.symbol_size.get())),
        ) {
            destination_row.data_mut()[0..row.len()].copy_from_slice(row);
        }
    }

    fn expanded_column_symbols(
        &'a self,
    ) -> impl Iterator<Item = impl ExactSizeIterator<Item = &'a [u8]> + '_> {
        (0..self.matrix.len()).map(move |col_index| {
            self.matrix
                .iter()
                // Get the columns in reverse order `n_shards - col_index - 1`.
                .map(move |row| {
                    row[SliverPairIndex::try_from(col_index)
                        .expect("size has already been checked")
                        .to_sliver_index::<Secondary>(self.config.n_shards)
                        .as_usize()]
                    .as_ref()
                })
        })
    }

    /// Expands the first `source_symbols_secondary` columns from `self.columns` to get all
    /// remaining primary slivers.
    fn expand_columns_for_primary(&mut self) {
        for col_index in 0..self.n_columns {
            let mut column = Symbols::with_capacity(self.n_rows, self.symbol_size);
            self.matrix.iter().take(self.n_rows).for_each(|row| {
                let _ = column.extend(&row[col_index]);
            });

            for (row_index, symbol) in self
                .config
                .get_encoder::<Primary>(column.data())
                .expect("size has already been checked")
                .encode_all_repair_symbols()
                .enumerate()
            {
                self.matrix[self.n_rows + row_index][col_index].copy_from_slice(&symbol);
            }
        }
    }

    /// Expands all `n_shards` primary slivers (rows) to completely fill the `n_shards * n_shards`
    /// expanded message matrix.
    fn expand_all_rows(&mut self) {
        for row in self.matrix.iter_mut() {
            for (col_index, symbol) in self
                .config
                .get_encoder::<Secondary>(&row[0..self.n_columns])
                .expect("size has already been checked")
                .encode_all_repair_symbols()
                .enumerate()
            {
                row[self.n_columns + col_index].copy_from_slice(&symbol)
            }
        }
    }

    /// Computes the sliver pair metadata from the expanded message matrix.
    fn get_metadata(&self) -> VerifiedBlobMetadataWithId {
        let mut metadata = vec![SliverPairMetadata::new_empty(); self.matrix.len()];
        self.write_secondary_metadata(&mut metadata);
        self.write_primary_metadata(&mut metadata);
        VerifiedBlobMetadataWithId::new_verified_from_metadata(
            metadata,
            EncodingType::RedStuff,
            NonZeroU64::new(
                u64::try_from(self.blob.len()).expect("any valid blob size fits into a `u64`"),
            )
            .expect("`self.blob` is guaranteed to be non-zero"),
        )
    }

    /// Writes the secondary metadata to the provided mutable slice.
    fn write_secondary_metadata(&self, metadata: &mut [SliverPairMetadata]) {
        metadata
            .iter_mut()
            .zip(self.expanded_column_symbols())
            .for_each(|(metadata, symbols)| {
                metadata.secondary_hash = MerkleTree::<Blake2b256>::build(symbols).root();
            });
    }

    /// Writes the secondary slivers to the provided mutable slice.
    fn write_secondary_slivers(&self, sliver_pairs: &mut [SliverPair]) {
        sliver_pairs
            .iter_mut()
            .zip(self.expanded_column_symbols())
            .for_each(|(sliver_pair, symbols)| {
                for (target_slice, symbol) in
                    sliver_pair.secondary.symbols.to_symbols_mut().zip(symbols)
                {
                    target_slice.copy_from_slice(symbol);
                }
            })
    }

    /// Drops the part of the matrix that only contains recovery symbols.
    ///
    /// This part is only necessary for the metadata but not for any of the slivers.
    ///
    /// After this function is called, the functions [`get_metadata`][Self::get_metadata],
    /// [`write_secondary_metadata`][Self::write_secondary_metadata], and
    /// [`write_primary_metadata`][Self::write_primary_metadata] no longer produce meaningful
    /// results.
    fn drop_recovery_symbols(&mut self) {
        self.matrix
            .iter_mut()
            .skip(self.n_rows)
            .for_each(|row| row.truncate(self.n_columns));
    }

    /// Writes the primary metadata to the provided mutable slice.
    fn write_primary_metadata(&self, metadata: &mut [SliverPairMetadata]) {
        for (metadata, row) in metadata.iter_mut().zip(self.matrix.iter()) {
            metadata.primary_hash = MerkleTree::<Blake2b256>::build(row.to_symbols()).root();
        }
    }

    /// Writes the primary slivers to the provided mutable slice.
    ///
    /// Consumes the original matrix, as it creates the primary slivers by truncating the rows of
    /// the matrix.
    fn write_primary_slivers(self, sliver_pairs: &mut [SliverPair]) {
        for (sliver_pair, mut row) in sliver_pairs.iter_mut().zip(self.matrix.into_iter()) {
            row.truncate(self.config.n_source_symbols::<Secondary>().get().into());
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
    blob_size: NonZeroUsize,
    symbol_size: NonZeroU16,
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
    /// Returns an [`InvalidDataSizeError::DataTooLarge`] if the `blob_size` is too large to be
    /// decoded.
    pub fn new(
        config: &'a EncodingConfig,
        blob_size: NonZeroU64,
    ) -> Result<Self, InvalidDataSizeError> {
        let symbol_size = config.symbol_size_for_blob(blob_size.get())?;
        let blob_size = blob_size
            .try_into()
            .map_err(|_| InvalidDataSizeError::DataTooLarge)?;
        Ok(Self {
            _decoding_axis: PhantomData,
            decoders: vec![
                Decoder::new(config.n_source_symbols::<T>(), symbol_size);
                config.n_source_symbols::<T::OrthogonalAxis>().get().into()
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
    /// Slivers of incorrect length are dropped with a warning.
    ///
    /// If decoding failed due to an insufficient number of provided slivers, it can be continued by
    /// additional calls to [`decode`][Self::decode] providing more slivers.
    ///
    /// # Panics
    ///
    /// This function can panic if there is insufficient virtual memory for the decoded blob in
    /// addition to the slivers, notably on 32-bit architectures.
    pub fn decode<S>(&mut self, slivers: S) -> Option<Vec<u8>>
    where
        S: IntoIterator<Item = Sliver<T>>,
        T: EncodingAxis,
    {
        // Depending on the decoding axis, this represents the message matrix's columns (primary)
        // or rows (secondary).
        let mut columns_or_rows = Vec::with_capacity(self.decoders.len());
        let mut decoding_successful = false;

        for sliver in slivers {
            let expected_len = self.decoders.len();
            let expected_symbol_size = self.symbol_size;
            if sliver.symbols.len() != expected_len
                || sliver.symbols.symbol_size() != expected_symbol_size
            {
                // Drop slivers of incorrect length or incorrect symbol size and log a warning.
                tracing::warn!(
                    %sliver,
                    expected_len,
                    expected_symbol_size,
                    "Sliver has incorrect length or symbol size"
                );
                continue;
            }
            for (decoder, symbol) in self.decoders.iter_mut().zip(sliver.symbols.to_symbols()) {
                if let Some(decoded_data) = decoder
                    // NOTE: The encoding axis of the following symbol is irrelevant, but since we
                    // are reconstructing from slivers of type `T`, it should be of type `T`.
                    .decode([DecodingSymbol::<T>::new(sliver.index.0, symbol.into())])
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

        let mut blob: Vec<_> = if T::IS_PRIMARY {
            // Primary decoding: transpose columns to get to the original blob.
            let mut columns: Vec<_> = columns_or_rows
                .into_iter()
                .map(|col_index| col_index.into_iter())
                .collect();
            (0..self.config.n_source_symbols::<T>().get())
                .flat_map(|_| {
                    {
                        columns
                            .iter_mut()
                            .map(|column| column.take(self.symbol_size.get().into()))
                    }
                    .flatten()
                    .collect::<Vec<u8>>()
                })
                .collect()
        } else {
            // Secondary decoding: these are the rows and can be used directly as the blob.
            columns_or_rows.into_iter().flatten().collect()
        };

        blob.truncate(self.blob_size.get());
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
    ///
    /// # Panics
    ///
    /// This function can panic if there is insufficient virtual memory for the encoded data,
    /// notably on 32-bit architectures. As this function re-encodes the blob to verify the
    /// metadata, similar limits apply as in [`BlobEncoder::encode_with_metadata`].
    pub fn decode_and_verify(
        &mut self,
        blob_id: &BlobId,
        slivers: impl IntoIterator<Item = Sliver<T>>,
    ) -> Result<Option<(Vec<u8>, VerifiedBlobMetadataWithId)>, DecodingVerificationError> {
        let Some(decoded_blob) = self.decode(slivers) else {
            return Ok(None);
        };
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
    }
}

#[cfg(test)]
mod tests {
    use walrus_test_utils::{param_test, random_data, random_subset};

    use super::*;
    use crate::metadata::UnverifiedBlobMetadataWithId;

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
        expected_rows: &[&[u8]],
        expected_columns: &[&[u8]],
    ) {
        let config = EncodingConfig::new_for_test(
            source_symbols_primary,
            source_symbols_secondary,
            3 * (source_symbols_primary + source_symbols_secondary),
        );
        let blob_encoder = config.get_blob_encoder(blob).unwrap();
        let sliver_pairs = blob_encoder.encode();
        let rows: Vec<_> = sliver_pairs
            .iter()
            .take(blob_encoder.n_rows)
            .map(|pair| pair.primary.symbols.data())
            .collect();
        let columns: Vec<_> = sliver_pairs
            .iter()
            .rev()
            .take(blob_encoder.n_columns)
            .map(|pair| pair.secondary.symbols.data())
            .collect();

        assert_eq!(rows, expected_rows);
        assert_eq!(columns, expected_columns);
    }

    #[test]
    fn test_blob_encode_decode() {
        let blob = random_data(31415);
        let blob_size = NonZeroU64::new(blob.len().try_into().unwrap()).unwrap();

        let config = EncodingConfig::new(NonZeroU16::new(102).unwrap());

        let slivers_for_decoding: Vec<_> = random_subset(
            config.get_blob_encoder(&blob).unwrap().encode(),
            cmp::max(
                config.source_symbols_primary.get(),
                config.source_symbols_secondary.get(),
            )
            .into(),
        )
        .collect();

        let mut primary_decoder = config.get_blob_decoder::<Primary>(blob_size).unwrap();
        assert_eq!(
            primary_decoder
                .decode(
                    slivers_for_decoding
                        .iter()
                        .cloned()
                        .map(|p| p.primary)
                        .take(config.source_symbols_primary.get().into())
                )
                .unwrap(),
            blob
        );

        let mut secondary_decoder = config.get_blob_decoder::<Secondary>(blob_size).unwrap();
        assert_eq!(
            secondary_decoder
                .decode(
                    slivers_for_decoding
                        .into_iter()
                        .map(|p| p.secondary)
                        .take(config.source_symbols_secondary.get().into())
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
        let n_shards = 102;

        let config = EncodingConfig::new(NonZeroU16::new(n_shards).unwrap());

        // Check that the encoding with and without metadata are identical.
        let sliver_pairs_1 = config.get_blob_encoder(&blob).unwrap().encode();
        let blob_metadata_1 = config.get_blob_encoder(&blob).unwrap().compute_metadata();
        let (sliver_pairs_2, blob_metadata_2) = config
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
                .pair_leaf_input::<Blake2b256>(&config)
                .expect("should be able to encode");
            let meta_hash = pair_meta.pair_leaf_input::<Blake2b256>();
            assert_eq!(pair_hash, meta_hash);
        }

        // Check that the blob metadata verifies.
        let unverified = UnverifiedBlobMetadataWithId::new(
            *blob_metadata_2.blob_id(),
            blob_metadata_2.metadata().clone(),
        );
        assert!(unverified.verify(&config).is_ok());
    }

    #[test]
    fn test_encode_decode_and_verify() {
        let blob = random_data(16180);
        let blob_size = NonZeroU64::new(blob.len().try_into().unwrap()).unwrap();
        let n_shards = 102;

        let config = EncodingConfig::new(NonZeroU16::new(n_shards).unwrap());

        let (slivers, metadata_enc) = config
            .get_blob_encoder(&blob)
            .unwrap()
            .encode_with_metadata();
        let slivers_for_decoding =
            random_subset(slivers, config.source_symbols_primary.get().into())
                .map(|s| s.primary)
                .collect::<Vec<_>>();
        let (blob_dec, metadata_dec) = config
            .get_blob_decoder(blob_size)
            .unwrap()
            .decode_and_verify(metadata_enc.blob_id(), slivers_for_decoding)
            .unwrap()
            .unwrap();

        assert_eq!(blob, blob_dec);
        assert_eq!(metadata_enc, metadata_dec);
    }
}
