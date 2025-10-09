// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use alloc::{collections::BTreeSet, format, vec, vec::Vec};
use core::{cmp, marker::PhantomData, num::NonZeroU16, slice::Chunks};

use fastcrypto::hash::Blake2b256;
use tracing::{Level, Span};

use super::{
    DataTooLargeError,
    DecodeError,
    DecodingSymbol,
    EncodingAxis,
    EncodingConfigEnum,
    Primary,
    Secondary,
    SliverData,
    SliverPair,
    Symbols,
    basic_encoding::{Decoder, ReedSolomonDecoder},
    utils,
};
use crate::{
    BlobId,
    DefaultHashFunction,
    EncodingType,
    SliverIndex,
    SliverPairIndex,
    encoding::config::{EncodingFactory as _, ReedSolomonEncodingConfig},
    ensure,
    merkle::{DIGEST_LEN, MerkleTree, Node as MerkleNode, leaf_hash},
    metadata::{
        BlobMetadata,
        BlobMetadataApi as _,
        BlobMetadataV2,
        SliverPairMetadata,
        VerifiedBlobMetadataWithId,
    },
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
    config: EncodingConfigEnum,
    /// A tracing span associated with this blob encoder.
    span: Span,
}

impl<'a> BlobEncoder<'a> {
    /// Creates a new `BlobEncoder` to encode the provided `blob` with the provided configuration.
    ///
    /// The actual encoding can be performed with the [`encode()`][Self::encode] method.
    ///
    /// # Errors
    ///
    /// Returns a [`DataTooLargeError`] if the blob is too large to be encoded. This can happen in
    /// two cases:
    ///
    /// 1. If the blob is too large to fit into the message matrix with valid symbols. The maximum
    ///    blob size for a given [`EncodingConfigEnum`] is accessible through the
    ///    [`EncodingConfigEnum::max_blob_size`] method.
    /// 2. On 32-bit architectures, the maximally supported blob size can actually be smaller than
    ///    that due to limitations of the address space.
    pub fn new(config: EncodingConfigEnum, blob: &'a [u8]) -> Result<Self, DataTooLargeError> {
        tracing::debug!("creating new blob encoder");
        let symbol_size = utils::compute_symbol_size_from_usize(
            blob.len(),
            config.source_symbols_per_blob(),
            config.encoding_type().required_alignment(),
        )?;
        let n_rows = config.n_source_symbols::<Primary>().get().into();
        let n_columns = config.n_source_symbols::<Secondary>().get().into();

        Ok(Self {
            blob,
            symbol_size,
            n_rows,
            n_columns,
            config,
            span: tracing::span!(
                Level::ERROR,
                "BlobEncoder",
                blob_size = blob.len(),
                blob = crate::utils::data_prefix_string(blob, 5),
            ),
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
        tracing::debug!(parent: &self.span, "starting to encode blob");
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
                .encode_all_repair_symbols::<Primary>(column.symbols.data())
                .expect("size has already been checked")
                .into_iter()
                .zip(primary_slivers.iter_mut().skip(self.n_rows))
            {
                sliver.copy_symbol_to(col_index, &symbol);
            }
        }

        // Compute the remaining secondary slivers by encoding the rows (i.e., primary slivers).
        for (r, row) in primary_slivers.iter().take(self.n_rows).enumerate() {
            for (symbol, sliver) in self
                .config
                .encode_all_repair_symbols::<Secondary>(row.symbols.data())
                .expect("size has already been checked")
                .into_iter()
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
        let _guard = self.span.enter();
        tracing::debug!("starting to encode blob with metadata");
        let mut expanded_matrix = self.get_expanded_matrix();
        let metadata = expanded_matrix.get_metadata();

        // This is just an optimization to free memory that is no longer needed at this point.
        expanded_matrix.drop_recovery_symbols();

        let mut sliver_pairs = self.empty_sliver_pairs();
        // First compute the secondary slivers -- does not require consuming the matrix.
        expanded_matrix.write_secondary_slivers(&mut sliver_pairs);
        // Then consume the matrix to get the primary slivers.
        expanded_matrix.write_primary_slivers(&mut sliver_pairs);
        tracing::debug!(
            blob_id = %metadata.blob_id(),
            "successfully encoded blob"
        );

        (sliver_pairs, metadata)
    }

    /// Computes the metadata (blob ID, hashes) for the blob, without returning the slivers.
    pub fn compute_metadata(&self) -> VerifiedBlobMetadataWithId {
        tracing::debug!(parent: &self.span, "starting to compute metadata");
        self.get_expanded_matrix().get_metadata()
    }

    /// Returns the size of the symbol in bytes.
    pub fn symbol_usize(&self) -> usize {
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

    fn rows(&self) -> Chunks<'_, u8> {
        self.blob.chunks(self.n_columns * self.symbol_usize())
    }

    fn empty_slivers<T: EncodingAxis>(&self) -> Vec<SliverData<T>> {
        (0..self.config.n_shards().get())
            .map(|i| {
                SliverData::<T>::new_empty(
                    self.config.n_source_symbols::<T::OrthogonalAxis>().get(),
                    self.symbol_size,
                    SliverIndex(i),
                )
            })
            .collect()
    }

    /// Returns a vector of empty [`SliverPair`] of length `n_shards`. Primary and secondary slivers
    /// are initialized with the appropriate `symbol_size` and `length`.
    #[tracing::instrument(level = "debug", skip_all)]
    fn empty_sliver_pairs(&self) -> Vec<SliverPair> {
        (0..self.config.n_shards().get())
            .map(|i| SliverPair::new_empty(self.config, self.symbol_size, SliverPairIndex(i)))
            .collect()
    }

    /// Computes the fully expanded message matrix by encoding rows and columns.
    #[tracing::instrument(level = "debug", skip_all)]
    fn get_expanded_matrix(&self) -> ExpandedMessageMatrix<'_> {
        self.span
            .in_scope(|| ExpandedMessageMatrix::new(self.config, self.symbol_size, self.blob))
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
    config: EncodingConfigEnum,
    /// The number of rows in the non-expanded message matrix.
    n_rows: usize,
    /// The number of columns in the non-expanded message matrix.
    n_columns: usize,
    symbol_size: NonZeroU16,
}

impl<'a> ExpandedMessageMatrix<'a> {
    fn new(config: EncodingConfigEnum, symbol_size: NonZeroU16, blob: &'a [u8]) -> Self {
        tracing::debug!("computing expanded message matrix");
        let matrix = vec![
            Symbols::zeros(config.n_shards_as_usize(), symbol_size);
            config.n_shards_as_usize()
        ];
        let mut expanded_matrix = Self {
            matrix,
            blob,
            n_rows: config.n_source_symbols::<Primary>().get().into(),
            n_columns: config.n_source_symbols::<Secondary>().get().into(),
            config,
            symbol_size,
        };
        expanded_matrix.fill_systematic_with_rows();
        expanded_matrix.expand_rows_for_secondary();
        expanded_matrix.expand_all_columns();
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
    ) -> impl Iterator<Item = impl ExactSizeIterator<Item = &'a [u8]> + 'a> {
        (0..self.matrix.len()).map(move |col_index| {
            self.matrix
                .iter()
                // Get the columns in reverse order `n_shards - col_index - 1`.
                .map(move |row| {
                    row[SliverPairIndex::try_from(col_index)
                        .expect("size has already been checked")
                        .to_sliver_index::<Secondary>(self.config.n_shards())
                        .as_usize()]
                    .as_ref()
                })
        })
    }

    /// Expands all columns to completely fill the `n_shards * n_shards` expanded message matrix.
    fn expand_all_columns(&mut self) {
        for col_index in 0..self.config.n_shards().get().into() {
            let mut column = Symbols::with_capacity(self.n_rows, self.symbol_size);
            self.matrix.iter().take(self.n_rows).for_each(|row| {
                let _ = column.extend(&row[col_index]);
            });

            for (row_index, symbol) in self
                .config
                .encode_all_repair_symbols::<Primary>(column.data())
                .expect("size has already been checked")
                .into_iter()
                .enumerate()
            {
                self.matrix[self.n_rows + row_index][col_index].copy_from_slice(&symbol);
            }
        }
    }

    /// Expands the first `source_symbols_primary` primary slivers (rows) to get all remaining
    /// secondary slivers.
    fn expand_rows_for_secondary(&mut self) {
        for row in self.matrix.iter_mut().take(self.n_rows) {
            for (col_index, symbol) in self
                .config
                .encode_all_repair_symbols::<Secondary>(&row[0..self.n_columns])
                .expect("size has already been checked")
                .into_iter()
                .enumerate()
            {
                row[self.n_columns + col_index].copy_from_slice(&symbol)
            }
        }
    }

    /// Computes the sliver pair metadata from the expanded message matrix.
    #[tracing::instrument(level = "debug", skip_all)]
    fn get_metadata(&self) -> VerifiedBlobMetadataWithId {
        tracing::debug!("computing blob metadata and ID");

        let n_shards = self.config.n_shards_as_usize();
        let mut leaf_hashes = Vec::with_capacity(n_shards * n_shards);
        for row in 0..n_shards {
            for col in 0..n_shards {
                leaf_hashes.push(leaf_hash::<Blake2b256>(&self.matrix[row][col]));
            }
        }

        let mut metadata = Vec::with_capacity(n_shards);
        for sliver_index in 0..n_shards {
            let primary_hash = MerkleTree::<Blake2b256>::build_from_leaf_hashes(
                leaf_hashes[n_shards * sliver_index..n_shards * (sliver_index + 1)]
                    .iter()
                    .cloned(),
            )
            .root();
            let secondary_hash = MerkleTree::<Blake2b256>::build_from_leaf_hashes(
                (0..n_shards).map(|symbol_index| {
                    leaf_hashes[n_shards * symbol_index + n_shards - 1 - sliver_index].clone()
                }),
            )
            .root();
            metadata.push(SliverPairMetadata {
                primary_hash,
                secondary_hash,
            })
        }

        VerifiedBlobMetadataWithId::new_verified_from_metadata(
            metadata,
            self.config.encoding_type(),
            u64::try_from(self.blob.len()).expect("any valid blob size fits into a `u64`"),
        )
    }

    /// Writes the secondary metadata to the provided mutable slice.
    ///
    /// This is no longer used in the actual code and just kept for testing.
    #[cfg(test)]
    fn write_secondary_metadata(&self, metadata: &mut [SliverPairMetadata]) {
        metadata
            .iter_mut()
            .zip(self.expanded_column_symbols())
            .for_each(|(metadata, symbols)| {
                metadata.secondary_hash = MerkleTree::<Blake2b256>::build(symbols).root();
            });
    }

    /// Writes the secondary slivers to the provided mutable slice.
    #[tracing::instrument(level = "debug", skip_all)]
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
    #[tracing::instrument(level = "debug", skip_all)]
    fn drop_recovery_symbols(&mut self) {
        self.matrix
            .iter_mut()
            .skip(self.n_rows)
            .for_each(|row| row.truncate(self.n_columns));
    }

    /// Writes the primary metadata to the provided mutable slice.
    ///
    /// This is no longer used in the actual code and just kept for testing.
    #[cfg(test)]
    fn write_primary_metadata(&self, metadata: &mut [SliverPairMetadata]) {
        for (metadata, row) in metadata.iter_mut().zip(self.matrix.iter()) {
            metadata.primary_hash = MerkleTree::<Blake2b256>::build(row.to_symbols()).root();
        }
    }

    /// Writes the primary slivers to the provided mutable slice.
    ///
    /// Consumes the original matrix, as it creates the primary slivers by truncating the rows of
    /// the matrix.
    #[tracing::instrument(level = "debug", skip_all)]
    fn write_primary_slivers(self, sliver_pairs: &mut [SliverPair]) {
        for (sliver_pair, mut row) in sliver_pairs.iter_mut().zip(self.matrix.into_iter()) {
            row.truncate(self.config.n_source_symbols::<Secondary>().get().into());
            sliver_pair.primary.symbols = row;
        }
    }
}

/// Struct to reconstruct a blob from either [`Primary`] (default) or [`Secondary`]
/// [`Sliver`s][SliverData].
#[derive(Debug)]
pub struct BlobDecoder<'a, D: Decoder, E: EncodingAxis = Primary> {
    _decoding_axis: PhantomData<E>,
    decoder: D,
    blob_size: usize,
    symbol_size: NonZeroU16,
    sliver_length: usize,
    /// The number of columns of the blob's message matrix (i.e., the number of secondary slivers).
    n_columns: usize,
    config: &'a D::Config,
    /// The workspace used to store the sliver data and iteratively overwrite it with the decoded
    /// blob. While a flat byte array, this is interpreted as a matrix of symbols. The layout is the
    /// same as the blob's message matrix; that is, primary slivers are written as "rows" while
    /// secondary slivers are written as "columns".
    workspace: Symbols,
    /// The indices of the slivers that have been provided and added to the workspace.
    sliver_indices: Vec<SliverIndex>,
}

impl<'a, D: Decoder, E: EncodingAxis> BlobDecoder<'a, D, E> {
    /// Creates a new `BlobDecoder` to decode a blob of size `blob_size` using the provided
    /// configuration.
    ///
    /// The generic parameter specifies from which type of slivers the decoding will be performed.
    ///
    /// This function creates the necessary decoders for the decoding; actual decoding can be
    /// performed with the [`decode()`][Self::decode] method.
    ///
    /// # Errors
    ///
    /// Returns a [`DecodeError::DataTooLarge`] if the `blob_size` is too large to be decoded.
    /// Returns a [`DecodeError::IncompatibleParameters`] if the parameters are incompatible with
    /// the decoder.
    pub fn new(config: &'a D::Config, blob_size: u64) -> Result<Self, DecodeError> {
        tracing::debug!("creating new blob decoder");
        let symbol_size = config.symbol_size_for_blob(blob_size)?;
        let blob_size = blob_size.try_into().map_err(|_| DataTooLargeError::new())?;
        let n_source_symbols = config.n_source_symbols::<E>();

        let decoder = D::new(n_source_symbols, config.n_shards(), symbol_size)?;

        let sliver_length = config.n_source_symbols::<E::OrthogonalAxis>().get().into();
        let sliver_count = usize::from(n_source_symbols.get());
        let workspace_size = sliver_length * sliver_count;

        let (n_columns, workspace) = if E::IS_PRIMARY {
            (
                sliver_length,
                Symbols::with_capacity(workspace_size, symbol_size),
            )
        } else {
            (sliver_count, Symbols::zeros(workspace_size, symbol_size))
        };

        Ok(Self {
            _decoding_axis: PhantomData,
            decoder,
            blob_size,
            symbol_size,
            sliver_length,
            n_columns,
            config,
            workspace,
            sliver_indices: Vec::with_capacity(n_source_symbols.get().into()),
        })
    }

    /// Attempts to decode the source blob from the provided slivers.
    ///
    /// Returns the source blob as a byte vector if decoding succeeds.
    ///
    /// Slivers of incorrect length are dropped with a warning.
    ///
    /// # Errors
    ///
    /// Returns a [`DecodeError::DecodingUnsuccessful`] if decoding was unsuccessful.
    ///
    /// # Panics
    ///
    /// This function can panic if there is insufficient virtual memory for the decoded blob in
    /// addition to the slivers, notably on 32-bit architectures.
    #[tracing::instrument(skip_all, level = Level::ERROR, "BlobDecoder")]
    pub fn decode<S>(mut self, slivers: S) -> Result<Vec<u8>, DecodeError>
    where
        S: IntoIterator<Item = SliverData<E>>,
        E: EncodingAxis,
    {
        tracing::debug!(axis = E::NAME, "starting to decode");

        self.check_and_write_slivers_to_workspace(slivers)?;
        self.perform_decoding()?;

        let mut blob = self.workspace.into_vec();
        blob.truncate(self.blob_size);
        tracing::debug!("returning truncated decoded blob");
        Ok(blob)
    }

    fn check_and_write_slivers_to_workspace(
        &mut self,
        slivers: impl IntoIterator<Item = SliverData<E>>,
    ) -> Result<(), DecodeError> {
        let expected_sliver_count = self.n_source_symbols();

        let mut sliver_indices_set = BTreeSet::new();
        let mut slivers_count = 0;
        for sliver in slivers {
            if slivers_count == expected_sliver_count {
                tracing::info!("dropping surplus slivers during blob decoding");
                break;
            }

            if sliver_indices_set.contains(&sliver.index) {
                tracing::warn!("dropping duplicate sliver during blob decoding");
                continue;
            }

            let expected_len = self.sliver_length;
            let expected_symbol_size = self.symbol_size;
            if sliver.symbols.len() != expected_len
                || sliver.symbols.symbol_size() != expected_symbol_size
            {
                // Drop slivers of incorrect length or incorrect symbol size and log a warning.
                tracing::warn!(
                    %sliver,
                    expected_len,
                    expected_symbol_size,
                    "sliver has incorrect length or symbol size"
                );
                continue;
            }

            if E::IS_PRIMARY {
                self.write_primary_sliver_to_workspace(sliver.symbols);
            } else {
                self.write_secondary_sliver_to_workspace(sliver.symbols, slivers_count);
            }
            self.sliver_indices.push(sliver.index);
            sliver_indices_set.insert(sliver.index);
            slivers_count += 1;
        }

        ensure!(
            slivers_count == expected_sliver_count,
            DecodeError::DecodingUnsuccessful
        );
        Ok(())
    }

    /// Writes the primary sliver as a new row in the workspace.
    fn write_primary_sliver_to_workspace(&mut self, sliver: Symbols) {
        self.workspace
            .extend(sliver.data())
            .expect("we checked above that the symbol size is correct");
    }

    /// Writes the secondary sliver as a column in the workspace.
    fn write_secondary_sliver_to_workspace(&mut self, sliver: Symbols, column: usize) {
        sliver.to_symbols().enumerate().for_each(|(row, symbol)| {
            self.workspace[row * self.n_columns + column].copy_from_slice(symbol);
        });
    }

    fn perform_decoding(&mut self) -> Result<(), DecodeError> {
        for decoder_index in 0..self.sliver_length {
            let symbols = self.sliver_indices.iter().enumerate().map(
                |(sliver_index_in_workspace, sliver_index)| {
                    let index = if E::IS_PRIMARY {
                        sliver_index_in_workspace * self.n_columns + decoder_index
                    } else {
                        decoder_index * self.n_columns + sliver_index_in_workspace
                    };
                    DecodingSymbol::<E>::new(sliver_index.0, self.workspace[index].to_vec())
                },
            );
            let decoded_data = self.decoder.decode(symbols)?;
            // Overwrite the decoding symbols in the workspace with the decoded data.
            if E::IS_PRIMARY {
                for (row_index, symbol) in decoded_data.chunks(self.symbol_usize()).enumerate() {
                    self.workspace[self.n_columns * row_index + decoder_index]
                        .copy_from_slice(symbol);
                }
            } else {
                self.workspace
                    [self.n_columns * decoder_index..self.n_columns * (decoder_index + 1)]
                    .copy_from_slice(&decoded_data);
            }
        }
        Ok(())
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
    /// # Errors
    ///
    /// Returns a [`DecodeError::DecodingUnsuccessful`] if decoding was unsuccessful. If decoding
    /// failed due to an insufficient number of provided slivers, the decoding can be continued by
    /// additional calls to [`decode_and_verify`][Self::decode_and_verify] providing more slivers.
    ///
    /// If, upon successful decoding, the recomputed blob ID does not match the input blob ID,
    /// returns a [`DecodeError::VerificationError`].
    ///
    /// # Panics
    ///
    /// This function can panic if there is insufficient virtual memory for the encoded data,
    /// notably on 32-bit architectures. As this function re-encodes the blob to verify the
    /// metadata, similar limits apply as in [`BlobEncoder::encode_with_metadata`].
    #[tracing::instrument(skip_all,err(level = Level::INFO))]
    pub fn decode_and_verify(
        self,
        blob_id: &BlobId,
        slivers: impl IntoIterator<Item = SliverData<E>>,
    ) -> Result<(Vec<u8>, VerifiedBlobMetadataWithId), DecodeError> {
        let config = self.config;
        let decoded_blob = self.decode(slivers)?;
        let blob_metadata = config
            .compute_metadata(&decoded_blob)
            .expect("the blob size cannot be too large since we were able to decode");
        if blob_metadata.blob_id() == blob_id {
            Ok((decoded_blob, blob_metadata))
        } else {
            Err(DecodeError::VerificationError)
        }
    }

    fn n_source_symbols(&self) -> usize {
        self.config.n_source_symbols::<E>().get().into()
    }

    fn symbol_usize(&self) -> usize {
        self.symbol_size.get().into()
    }
}

/// A blob encoder for chunked encoding (RS2Chunked).
///
/// This encoder splits large blobs into chunks, encodes each chunk independently using
/// Reed-Solomon encoding, and builds Merkle trees over chunk-level hashes to produce
/// blob-level metadata. This enables:
/// - Streaming uploads/downloads: chunks can be processed independently
/// - Memory efficiency: don't need to load entire blob into memory
/// - Proof-based verification: each chunk can be verified against blob-level hashes
#[derive(Debug)]
pub struct ChunkedBlobEncoder<'a> {
    /// Reference to the full blob data.
    blob: &'a [u8],
    /// The encoding configuration.
    config: &'a ReedSolomonEncodingConfig,
    /// Number of chunks to split the blob into.
    num_chunks: u32,
    /// Size of each chunk (last chunk may be smaller).
    chunk_size: u64,
}

impl<'a> ChunkedBlobEncoder<'a> {
    /// Creates a new chunked blob encoder with the specified chunk size.
    ///
    /// The blob will be split into chunks of the specified size (last chunk may be smaller).
    ///
    /// # Parameters
    ///
    /// * `config` - The Reed-Solomon encoding configuration
    /// * `blob` - The blob data to encode
    /// * `chunk_size` - The size of each chunk in bytes
    ///
    /// # Errors
    ///
    /// Returns [`DataTooLargeError`] if the chunk size computation fails or if validation fails.
    pub fn new(
        config: &'a ReedSolomonEncodingConfig,
        blob: &'a [u8],
        chunk_size: u64,
    ) -> Result<Self, DataTooLargeError> {
        let blob_size = blob.len() as u64;
        let num_chunks = super::config::compute_num_chunks(blob_size, chunk_size);

        // Validate minimum chunk size
        if chunk_size < super::config::MIN_CHUNK_SIZE {
            return Err(DataTooLargeError::with_message(format!(
                "Chunk size {} is below minimum {}. Use at least {} MB chunks to prevent excessive metadata overhead.",
                chunk_size,
                super::config::MIN_CHUNK_SIZE,
                super::config::MIN_CHUNK_SIZE / (1024 * 1024)
            )));
        }

        // Validate maximum number of chunks
        if num_chunks > super::config::MAX_CHUNKS {
            let min_required = blob_size.div_ceil(u64::from(super::config::MAX_CHUNKS));
            return Err(DataTooLargeError::with_message(format!(
                "Blob size {} with chunk size {} would create {} chunks (max {}). Use chunk size of at least {} bytes ({} MB) to stay within chunk limit.",
                blob_size,
                chunk_size,
                num_chunks,
                super::config::MAX_CHUNKS,
                min_required,
                min_required / (1024 * 1024)
            )));
        }

        tracing::debug!(
            blob_size,
            num_chunks,
            chunk_size,
            metadata_size_estimate = num_chunks * 64 * 1024, // Approximate: num_chunks × 64 KB
            "creating chunked blob encoder"
        );

        Ok(Self {
            blob,
            config,
            num_chunks,
            chunk_size,
        })
    }

    #[cfg(test)]
    /// Creates a new chunked blob encoder for testing purposes, bypassing validation.
    ///
    /// This method should only be used in tests where you want to test with non-standard
    /// chunk sizes that would otherwise fail validation.
    pub fn new_for_test(
        config: &'a ReedSolomonEncodingConfig,
        blob: &'a [u8],
        chunk_size: u64,
    ) -> Self {
        let blob_size = blob.len() as u64;
        let num_chunks = super::config::compute_num_chunks(blob_size, chunk_size);

        tracing::debug!(
            blob_size,
            num_chunks,
            chunk_size,
            "creating chunked blob encoder (test mode, validation bypassed)"
        );

        Self {
            blob,
            config,
            num_chunks,
            chunk_size,
        }
    }

    /// Encodes the blob and returns the sliver pairs (per-chunk) and verified metadata.
    ///
    /// This method:
    /// 1. Splits the blob into chunks
    /// 2. Encodes each chunk using the standard BlobEncoder
    /// 3. Collects chunk-level hashes
    /// 4. Builds Merkle trees over chunk hashes to get blob-level hashes
    /// 5. Returns BlobMetadataV2 with chunk information
    ///
    /// Returns `Vec<Vec<SliverPair>>` where outer vec is indexed by chunk_idx
    /// and inner vec contains the sliver pairs for that chunk.
    pub fn encode_with_metadata(
        &self,
    ) -> Result<(Vec<Vec<SliverPair>>, VerifiedBlobMetadataWithId), DataTooLargeError> {
        tracing::debug!("starting chunked encode with metadata");

        let n_shards = self.config.n_shards().get() as usize;

        // Store all chunk-level sliver pair metadata
        let mut all_chunk_hashes: Vec<Vec<SliverPairMetadata>> =
            Vec::with_capacity(self.num_chunks as usize);
        // Store all sliver pairs from all chunks (preserving chunk structure)
        let mut all_sliver_pairs: Vec<Vec<SliverPair>> =
            Vec::with_capacity(self.num_chunks as usize);

        // Encode each chunk
        for chunk_idx in 0..self.num_chunks {
            let chunk_start = usize::try_from(u64::from(chunk_idx) * self.chunk_size)
                .expect("chunk start offset must fit in usize since blob is in memory");
            let chunk_end = core::cmp::min(
                chunk_start
                    + usize::try_from(self.chunk_size)
                        .expect("chunk size must fit in usize since blob is in memory"),
                self.blob.len(),
            );
            let chunk_data = &self.blob[chunk_start..chunk_end];

            tracing::debug!(chunk_idx, chunk_size = chunk_data.len(), "encoding chunk");

            // Encode this chunk (create config_enum fresh for each iteration)
            let config_enum = EncodingConfigEnum::ReedSolomon(self.config);
            let chunk_encoder = BlobEncoder::new(config_enum, chunk_data)?;
            let (chunk_sliver_pairs, chunk_metadata) = chunk_encoder.encode_with_metadata();

            // Store the chunk-level hashes
            all_chunk_hashes.push(chunk_metadata.metadata().hashes().clone());

            // Store sliver pairs for this chunk (preserve structure)
            all_sliver_pairs.push(chunk_sliver_pairs);
        }

        // Now build blob-level hashes as Merkle trees over chunk hashes
        // For each sliver index j, build a Merkle tree over [chunk1_hash_j, chunk2_hash_j, ..., chunkk_hash_j]
        let mut blob_level_hashes: Vec<SliverPairMetadata> = Vec::with_capacity(n_shards);

        for sliver_idx in 0..n_shards {
            // Collect primary hashes for this sliver across all chunks
            let primary_chunk_hashes: Vec<[u8; DIGEST_LEN]> = all_chunk_hashes
                .iter()
                .map(|chunk_hashes| chunk_hashes[sliver_idx].primary_hash.bytes())
                .collect();

            // Collect secondary hashes for this sliver across all chunks
            let secondary_chunk_hashes: Vec<[u8; DIGEST_LEN]> = all_chunk_hashes
                .iter()
                .map(|chunk_hashes| chunk_hashes[sliver_idx].secondary_hash.bytes())
                .collect();

            // Build Merkle trees over the chunk hashes
            let primary_root = if primary_chunk_hashes.len() == 1 {
                // Single chunk: root is just the chunk hash
                MerkleNode::Digest(primary_chunk_hashes[0])
            } else {
                MerkleTree::<DefaultHashFunction>::build(primary_chunk_hashes.iter()).root()
            };

            let secondary_root = if secondary_chunk_hashes.len() == 1 {
                // Single chunk: root is just the chunk hash
                MerkleNode::Digest(secondary_chunk_hashes[0])
            } else {
                MerkleTree::<DefaultHashFunction>::build(secondary_chunk_hashes.iter()).root()
            };

            blob_level_hashes.push(SliverPairMetadata {
                primary_hash: primary_root,
                secondary_hash: secondary_root,
            });
        }

        // Create BlobMetadataV2 with chunk information
        let metadata_v2 = BlobMetadataV2 {
            encoding_type: EncodingType::RS2Chunked,
            unencoded_length: self.blob.len() as u64,
            hashes: blob_level_hashes,
            num_chunks: self.num_chunks,
            chunk_size: self.chunk_size,
            chunk_hashes: all_chunk_hashes,
        };

        let blob_metadata = BlobMetadata::V2(metadata_v2);
        let blob_id = BlobId::from_sliver_pair_metadata(&blob_metadata);
        let verified_metadata =
            VerifiedBlobMetadataWithId::new_verified_unchecked(blob_id, blob_metadata);

        tracing::debug!(
            blob_id = %verified_metadata.blob_id(),
            num_chunks = self.num_chunks,
            "successfully encoded chunked blob"
        );

        Ok((all_sliver_pairs, verified_metadata))
    }

    /// Computes only the metadata without encoding the sliver pairs.
    ///
    /// This is more memory-efficient when you only need the blob ID and hashes.
    pub fn compute_metadata(&self) -> Result<VerifiedBlobMetadataWithId, DataTooLargeError> {
        tracing::debug!("computing chunked metadata only");

        let n_shards = self.config.n_shards().get() as usize;

        // Store all chunk-level sliver pair metadata
        let mut all_chunk_hashes: Vec<Vec<SliverPairMetadata>> =
            Vec::with_capacity(self.num_chunks as usize);

        // Compute metadata for each chunk
        for chunk_idx in 0..self.num_chunks {
            let chunk_start = usize::try_from(u64::from(chunk_idx) * self.chunk_size)
                .expect("chunk start offset must fit in usize since blob is in memory");
            let chunk_end = core::cmp::min(
                chunk_start
                    + usize::try_from(self.chunk_size)
                        .expect("chunk size must fit in usize since blob is in memory"),
                self.blob.len(),
            );
            let chunk_data = &self.blob[chunk_start..chunk_end];

            let config_enum = EncodingConfigEnum::ReedSolomon(self.config);
            let chunk_encoder = BlobEncoder::new(config_enum, chunk_data)?;
            let chunk_metadata = chunk_encoder.compute_metadata();

            all_chunk_hashes.push(chunk_metadata.metadata().hashes().clone());
        }

        // Build blob-level hashes
        let mut blob_level_hashes: Vec<SliverPairMetadata> = Vec::with_capacity(n_shards);

        for sliver_idx in 0..n_shards {
            let primary_chunk_hashes: Vec<[u8; DIGEST_LEN]> = all_chunk_hashes
                .iter()
                .map(|chunk_hashes| chunk_hashes[sliver_idx].primary_hash.bytes())
                .collect();

            let secondary_chunk_hashes: Vec<[u8; DIGEST_LEN]> = all_chunk_hashes
                .iter()
                .map(|chunk_hashes| chunk_hashes[sliver_idx].secondary_hash.bytes())
                .collect();

            let primary_root = if primary_chunk_hashes.len() == 1 {
                MerkleNode::Digest(primary_chunk_hashes[0])
            } else {
                MerkleTree::<DefaultHashFunction>::build(primary_chunk_hashes.iter()).root()
            };

            let secondary_root = if secondary_chunk_hashes.len() == 1 {
                MerkleNode::Digest(secondary_chunk_hashes[0])
            } else {
                MerkleTree::<DefaultHashFunction>::build(secondary_chunk_hashes.iter()).root()
            };

            blob_level_hashes.push(SliverPairMetadata {
                primary_hash: primary_root,
                secondary_hash: secondary_root,
            });
        }

        let metadata_v2 = BlobMetadataV2 {
            encoding_type: EncodingType::RS2Chunked,
            unencoded_length: self.blob.len() as u64,
            hashes: blob_level_hashes,
            num_chunks: self.num_chunks,
            chunk_size: self.chunk_size,
            chunk_hashes: all_chunk_hashes,
        };

        let blob_metadata = BlobMetadata::V2(metadata_v2);
        let blob_id = BlobId::from_sliver_pair_metadata(&blob_metadata);

        Ok(VerifiedBlobMetadataWithId::new_verified_unchecked(
            blob_id,
            blob_metadata,
        ))
    }
}

/// Decoder for chunked blobs encoded with RS2Chunked encoding.
///
/// This decoder reconstructs blobs chunk-by-chunk, enabling streaming decoding
/// of large blobs without loading everything into memory at once.
#[derive(Debug)]
pub struct ChunkedBlobDecoder<'a> {
    /// The Reed-Solomon encoding configuration.
    config: &'a ReedSolomonEncodingConfig,
    /// Metadata for the chunked blob.
    metadata: &'a BlobMetadataV2,
}

impl<'a> ChunkedBlobDecoder<'a> {
    /// Creates a new chunked blob decoder.
    ///
    /// # Parameters
    ///
    /// * `config` - The Reed-Solomon encoding configuration
    /// * `metadata` - The verified blob metadata (must be V2 for chunked blobs)
    ///
    /// # Returns
    ///
    /// Returns `Some(decoder)` if the metadata is V2 (chunked), `None` otherwise.
    pub fn new(config: &'a ReedSolomonEncodingConfig, metadata: &'a BlobMetadata) -> Option<Self> {
        match metadata {
            BlobMetadata::V2(metadata_v2) => Some(Self {
                config,
                metadata: metadata_v2,
            }),
            _ => None,
        }
    }

    /// Returns the number of chunks in this blob.
    pub fn num_chunks(&self) -> u32 {
        self.metadata.num_chunks
    }

    /// Returns the chunk size used for this blob.
    pub fn chunk_size(&self) -> u64 {
        self.metadata.chunk_size
    }

    /// Decodes a single chunk from the provided sliver pairs.
    ///
    /// # Parameters
    ///
    /// * `chunk_index` - The index of the chunk to decode (0-based)
    /// * `sliver_pairs` - The sliver pairs for this chunk
    ///
    /// # Errors
    ///
    /// Returns a [`DecodeError`] if:
    /// - The chunk index is out of bounds
    /// - Decoding fails (insufficient or invalid slivers)
    /// - The chunk size exceeds limits
    ///
    /// # Returns
    ///
    /// The decoded chunk data as a byte vector.
    pub fn decode_chunk(
        &self,
        chunk_index: u32,
        sliver_pairs: Vec<SliverPair>,
    ) -> Result<Vec<u8>, DecodeError> {
        ensure!(
            chunk_index < self.metadata.num_chunks,
            DecodeError::DecodingUnsuccessful
        );

        // Determine chunk size (last chunk may be smaller)
        let chunk_size = if chunk_index == self.metadata.num_chunks - 1 {
            // Last chunk: compute remaining size
            let full_chunks_size =
                u64::from(self.metadata.num_chunks - 1) * self.metadata.chunk_size;
            self.metadata.unencoded_length - full_chunks_size
        } else {
            self.metadata.chunk_size
        };

        tracing::debug!(
            chunk_index,
            chunk_size,
            num_slivers = sliver_pairs.len(),
            "decoding chunk"
        );

        // Create a decoder for this chunk size
        let decoder: BlobDecoder<'_, ReedSolomonDecoder> =
            BlobDecoder::new(self.config, chunk_size)?;

        // Extract primary slivers from sliver pairs
        let primary_slivers = sliver_pairs
            .into_iter()
            .map(|pair| pair.primary)
            .collect::<Vec<_>>();

        // Decode the chunk
        decoder.decode(primary_slivers)
    }

    /// Decodes the entire blob from all chunks.
    ///
    /// This method reconstructs the complete blob by decoding all chunks in sequence.
    ///
    /// # Parameters
    ///
    /// * `all_chunk_sliver_pairs` - A vector containing sliver pairs for each chunk,
    ///   indexed by chunk number. Must have length equal to `num_chunks`.
    ///
    /// # Errors
    ///
    /// Returns a [`DecodeError`] if:
    /// - The number of chunk sliver sets doesn't match `num_chunks`
    /// - Any chunk fails to decode
    ///
    /// # Returns
    ///
    /// The complete decoded blob as a byte vector.
    pub fn decode_all(
        &self,
        all_chunk_sliver_pairs: Vec<Vec<SliverPair>>,
    ) -> Result<Vec<u8>, DecodeError> {
        ensure!(
            all_chunk_sliver_pairs.len() == self.metadata.num_chunks as usize,
            DecodeError::DecodingUnsuccessful
        );

        tracing::debug!(
            num_chunks = self.metadata.num_chunks,
            total_size = self.metadata.unencoded_length,
            "decoding all chunks"
        );

        let mut result = Vec::with_capacity(
            usize::try_from(self.metadata.unencoded_length)
                .expect("unencoded length must fit in usize to decode in memory"),
        );

        for (chunk_index, chunk_sliver_pairs) in all_chunk_sliver_pairs.into_iter().enumerate() {
            let chunk_data = self.decode_chunk(
                u32::try_from(chunk_index)
                    .expect("chunk index must fit in u32 (max 1000 chunks enforced by validation)"),
                chunk_sliver_pairs,
            )?;
            result.extend(chunk_data);
        }

        tracing::debug!("successfully decoded all chunks");
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use alloc::string::ToString;

    use walrus_test_utils::{param_test, random_data, random_subset};

    use super::*;
    use crate::{
        EncodingType,
        encoding::{EncodingConfig, ReedSolomonEncodingConfig, config},
        metadata::UnverifiedBlobMetadataWithId,
    };

    param_test! {
        test_matrix_construction: [
            aligned_square_double_byte_symbols: (
                2,
                2,
                &[1,2,3,4,5,6,7,8],
                &[&[1,2,3,4], &[5,6,7,8]],
                &[&[1,2,5,6],&[3,4,7,8]]
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
        let config = ReedSolomonEncodingConfig::new_for_test(
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
    fn test_metadata_computations_are_equal() {
        let blob = random_data(1000);
        let config = ReedSolomonEncodingConfig::new(NonZeroU16::new(10).unwrap());
        let encoder = config.get_blob_encoder(&blob).unwrap();
        let matrix = encoder.get_expanded_matrix();

        let mut expected_metadata = vec![SliverPairMetadata::new_empty(); matrix.matrix.len()];
        matrix.write_primary_metadata(&mut expected_metadata);
        matrix.write_secondary_metadata(&mut expected_metadata);

        assert_eq!(
            matrix.get_metadata().metadata().hashes(),
            &expected_metadata
        );
    }

    #[test]
    fn test_blob_encode_decode() {
        let blob = random_data(31415);
        let blob_size = blob.len().try_into().unwrap();

        let config = ReedSolomonEncodingConfig::new(NonZeroU16::new(102).unwrap());

        let slivers_for_decoding: Vec<_> = random_subset(
            config.get_blob_encoder(&blob).unwrap().encode(),
            cmp::max(
                config.source_symbols_primary.get(),
                config.source_symbols_secondary.get(),
            )
            .into(),
        )
        .collect();

        let primary_decoder = config.get_blob_decoder::<Primary>(blob_size).unwrap();
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

        let secondary_decoder = config.get_blob_decoder::<Secondary>(blob_size).unwrap();
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

    /// A big test checking that:
    /// 1. The sliver pairs produced by `encode_with_metadata` are the same as the ones produced by
    ///    `encode`;
    /// 2. the metadata produced by `encode_with_metadata` is the same as the metadata that can be
    ///    computed from the sliver pairs directly.
    /// 3. the metadata produced by `encode_with_metadata` is the same as the metadata produced by
    ///    `compute_metadata_only`.
    #[test]
    fn test_encode_with_metadata() {
        let encoding_type = EncodingType::RS2;
        let blob = random_data(27182);
        let n_shards = 102;

        let config = EncodingConfig::new(NonZeroU16::new(n_shards).unwrap());
        let config_enum = config.get_for_type(encoding_type);

        // Check that the encoding with and without metadata are identical.
        let blob_encoder = config.reed_solomon.get_blob_encoder(&blob).unwrap();
        let sliver_pairs_1 = blob_encoder.encode();
        let blob_metadata_1 = blob_encoder.compute_metadata();

        let (sliver_pairs_2, blob_metadata_2) = config_enum.encode_with_metadata(&blob).unwrap();
        assert_eq!(sliver_pairs_1, sliver_pairs_2);
        assert_eq!(blob_metadata_1, blob_metadata_2);

        // Check that the hashes obtained by re-encoding the sliver pairs are equivalent to the ones
        // obtained in the `encode_with_metadata` function.
        for (sliver_pair, pair_meta) in sliver_pairs_2
            .iter()
            .zip(blob_metadata_2.metadata().hashes().iter())
        {
            let pair_hash = sliver_pair
                .pair_leaf_input::<Blake2b256>(&config_enum)
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
        let blob_size = blob.len().try_into().unwrap();
        let n_shards = 102;

        let config = ReedSolomonEncodingConfig::new(NonZeroU16::new(n_shards).unwrap());

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
            .unwrap();

        assert_eq!(blob, blob_dec);
        assert_eq!(metadata_enc, metadata_dec);
    }

    param_test! {
        test_chunked_encoding_decoding_roundtrip: [
            small_chunks: (32, 1024, 10),
            medium_chunks: (1024, 10 * 1024, 10),
            large_chunks: (10 * 1024, 100 * 1024, 10),
            very_large_chunks: (100 * 1024, 1024 * 1024, 100),
        ]
    }
    fn test_chunked_encoding_decoding_roundtrip(chunk_size: u64, blob_size: u64, n_shards: u16) {
        let blob = random_data(usize::try_from(blob_size).expect("blob size must fit in usize"));
        let config = ReedSolomonEncodingConfig::new(NonZeroU16::new(n_shards).unwrap());

        // Encode with chunked encoder (using test constructor to bypass validation)
        let chunked_encoder = ChunkedBlobEncoder::new_for_test(&config, &blob, chunk_size);

        let (all_sliver_pairs, metadata) = chunked_encoder
            .encode_with_metadata()
            .expect("encoding failed");

        // Verify metadata is V2
        match metadata.metadata() {
            BlobMetadata::V2(v2) => {
                assert_eq!(v2.encoding_type, EncodingType::RS2Chunked);
                assert_eq!(v2.unencoded_length, blob_size);
                assert_eq!(v2.chunk_size, chunk_size);
                let expected_num_chunks = u32::try_from(blob_size.div_ceil(chunk_size)).unwrap();
                assert_eq!(v2.num_chunks, expected_num_chunks);
            }
            _ => panic!("expected BlobMetadataV2"),
        }

        // Create decoder
        let decoder =
            ChunkedBlobDecoder::new(&config, metadata.metadata()).expect("decoder creation failed");

        // all_sliver_pairs is already Vec<Vec<SliverPair>> with chunk structure preserved

        // Decode all chunks
        let decoded_blob = decoder
            .decode_all(all_sliver_pairs)
            .expect("decoding failed");

        // Verify roundtrip
        assert_eq!(blob.len(), decoded_blob.len());
        assert_eq!(blob, decoded_blob);
    }

    param_test! {
        test_chunked_decoding_individual_chunks: [
            medium_blob: (10 * 1024, 100 * 1024, 10),
            small_blob: (1024, 10 * 1024, 100),
        ]
    }
    fn test_chunked_decoding_individual_chunks(chunk_size: u64, blob_size: u64, n_shards: u16) {
        let blob = random_data(usize::try_from(blob_size).unwrap());
        let config = ReedSolomonEncodingConfig::new(NonZeroU16::new(n_shards).unwrap());

        // Encode (using test constructor to bypass validation)
        let chunked_encoder = ChunkedBlobEncoder::new_for_test(&config, &blob, chunk_size);
        let (all_chunk_sliver_pairs, metadata) = chunked_encoder
            .encode_with_metadata()
            .expect("encoding failed");

        // Create decoder
        let decoder =
            ChunkedBlobDecoder::new(&config, metadata.metadata()).expect("decoder creation failed");

        // Decode and verify each chunk individually
        let num_chunks = decoder.num_chunks();
        let mut decoded_blob = Vec::new();

        for chunk_idx in 0..num_chunks {
            let chunk_slivers = &all_chunk_sliver_pairs[chunk_idx as usize];

            let decoded_chunk = decoder
                .decode_chunk(chunk_idx, chunk_slivers.clone())
                .expect("chunk decoding failed");
            decoded_blob.extend(decoded_chunk);
        }

        // Verify roundtrip
        assert_eq!(blob, decoded_blob);
    }

    #[test]
    fn test_chunk_size_validation_too_small() {
        let blob = random_data(100 * 1024 * 1024); // 100 MB blob
        let config = ReedSolomonEncodingConfig::new(NonZeroU16::new(10).unwrap());
        let chunk_size = 1024 * 1024; // 1 MB, below the 10 MB minimum

        let result = ChunkedBlobEncoder::new(&config, &blob, chunk_size);

        assert!(result.is_err(), "Should reject chunk size below minimum");
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("below minimum"),
            "Error message should mention minimum: {}",
            err
        );
    }

    #[test]
    fn test_chunk_size_validation_too_many_chunks() {
        // Note: With MIN_CHUNK_SIZE = 10 MB and MAX_CHUNKS = 1000, a blob would need to be
        // at least 10 GB to trigger MAX_CHUNKS validation with minimum chunk size.
        // For practical testing, we verify that small chunk sizes are rejected for
        // being below minimum.

        let blob = random_data(10 * 1024 * 1024); // 10 MB blob for testing
        let rs_config = ReedSolomonEncodingConfig::new(NonZeroU16::new(10).unwrap());

        // Use a very small chunk size to trigger validation
        // With 10 MB blob and 1 KB chunks, we'd get 10,240 chunks
        let small_chunk_size = 1024; // 1 KB

        let result = ChunkedBlobEncoder::new(&rs_config, &blob, small_chunk_size);

        // This should fail for being below minimum
        assert!(result.is_err(), "Should reject chunk size below minimum");
    }

    #[test]
    fn test_chunk_size_validation_valid_min_size() {
        let blob = random_data(100 * 1024 * 1024); // 100 MB blob
        let rs_config = ReedSolomonEncodingConfig::new(NonZeroU16::new(10).unwrap());
        let chunk_size = config::MIN_CHUNK_SIZE; // Exactly at minimum

        let result = ChunkedBlobEncoder::new(&rs_config, &blob, chunk_size);

        assert!(
            result.is_ok(),
            "Should accept chunk size at minimum: {:?}",
            result
        );
    }

    #[test]
    fn test_chunk_size_validation_valid_large_size() {
        let blob = random_data(500 * 1024 * 1024); // 500 MB blob
        let config = ReedSolomonEncodingConfig::new(NonZeroU16::new(10).unwrap());
        let chunk_size = 50 * 1024 * 1024; // 50 MB, well above minimum

        let result = ChunkedBlobEncoder::new(&config, &blob, chunk_size);

        assert!(
            result.is_ok(),
            "Should accept large chunk size: {:?}",
            result
        );
    }
}
