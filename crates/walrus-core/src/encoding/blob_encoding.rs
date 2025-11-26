// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use alloc::{collections::BTreeSet, vec, vec::Vec};
use core::{cmp, marker::PhantomData, num::NonZeroU16, ops::Range, slice::Chunks};

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
    basic_encoding::Decoder,
    utils,
};
use crate::{
    SliverIndex,
    SliverPairIndex,
    encoding::{ReedSolomonEncoder, config::EncodingFactory as _},
    ensure,
    merkle::{MerkleTree, Node, leaf_hash},
    metadata::{SliverPairMetadata, VerifiedBlobMetadataWithId},
};

/// A wrapper around a blob that can be either owned (i.e., `Vec<u8>`) or borrowed (`&[u8]`).
#[derive(Debug)]
pub enum OwnedOrBorrowedBlob<'a> {
    /// An owned blob.
    Owned(Vec<u8>),
    /// A borrowed blob.
    Borrowed(&'a [u8]),
}

impl<'a> OwnedOrBorrowedBlob<'a> {
    /// Creates a new `OwnedOrBorrowedBlob` from a borrowed blob.
    pub fn new(blob: &'a [u8]) -> Self {
        Self::Borrowed(blob)
    }

    /// Converts the `OwnedOrBorrowedBlob` into an owned blob, copying the data if necessary.
    pub fn into_owned(self) -> OwnedOrBorrowedBlob<'static> {
        let blob = match self {
            Self::Borrowed(blob) => blob.to_vec(),
            Self::Owned(blob) => blob,
        };
        OwnedOrBorrowedBlob::Owned(blob)
    }

    /// Returns the length of the blob.
    pub fn len(&self) -> usize {
        match self {
            Self::Borrowed(blob) => blob.len(),
            Self::Owned(blob) => blob.len(),
        }
    }

    /// Returns true if the blob is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl OwnedOrBorrowedBlob<'static> {
    /// Creates a new `OwnedOrBorrowedBlob` from an owned blob.
    pub fn new_owned(blob: Vec<u8>) -> Self {
        Self::Owned(blob)
    }
}

impl<'a> AsRef<[u8]> for OwnedOrBorrowedBlob<'a> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Borrowed(blob) => blob,
            Self::Owned(blob) => blob,
        }
    }
}

/// Inner state of the blob encoder that doesn't contain the actual blob data.
#[derive(Debug, Clone)]
struct BlobEncoderData {
    /// The size of the encoded and decoded symbols.
    symbol_size: NonZeroU16,
    /// The number of rows of the message matrix.
    ///
    /// Stored as a `usize` for convenience, but guaranteed to be non-zero.
    n_rows: NonZeroU16,
    /// The number of columns of the message matrix.
    ///
    /// Stored as a `usize` for convenience, but guaranteed to be non-zero.
    n_columns: NonZeroU16,
    /// Reference to the encoding configuration of this encoder.
    config: EncodingConfigEnum,
    /// A tracing span associated with this blob encoder.
    span: Span,
}

impl BlobEncoderData {
    fn get_encoder<E: EncodingAxis>(&self) -> ReedSolomonEncoder {
        let EncodingConfigEnum::ReedSolomon(encoding_config) = self.config;
        encoding_config
            .get_encoder::<E>(self.sliver_length::<E::OrthogonalAxis>())
            .expect("this length is compatible with the encoder")
    }

    /// Returns the size of the symbol in bytes.
    pub fn symbol_usize(&self) -> usize {
        self.symbol_size.get().into()
    }

    fn sliver_length<E: EncodingAxis>(&self) -> usize {
        usize::from(
            self.config
                .n_source_symbols::<E::OrthogonalAxis>()
                .get(),
        ) * self.symbol_usize()
    }

    fn empty_sliver<E: EncodingAxis>(&self, index: SliverIndex) -> SliverData<E> {
        SliverData::<E>::new_empty(
            self.config.n_source_symbols::<E::OrthogonalAxis>().get(),
            self.symbol_size,
            index,
        )
    }

    fn empty_slivers<E: EncodingAxis>(&self) -> Vec<SliverData<E>> {
        self.empty_slivers_range::<E>(0..self.config.n_shards().get())
            .collect()
    }

    fn empty_slivers_range<E: EncodingAxis>(
        &self,
        range: Range<u16>,
    ) -> impl Iterator<Item = SliverData<E>> {
        range.map(|i| self.empty_sliver::<E>(SliverIndex(i)))
    }

    /// Returns a vector of empty [`SliverPair`] of length `n_shards`. Primary and secondary slivers
    /// are initialized with the appropriate `symbol_size` and `length`.
    #[tracing::instrument(level = "debug", skip_all)]
    fn empty_sliver_pairs(&self) -> Vec<SliverPair> {
        (0..self.config.n_shards().get())
            .map(|i| SliverPair::new_empty(self.config, self.symbol_size, SliverPairIndex(i)))
            .collect()
    }

    /// Computes the blob metadata from the provided leaf hashes of all symbols.
    ///
    /// The provided slice *must* be of length `n_shards * n_shards`, where `n_shards` is the number
    /// of shards. The slice is interpreted as a matrix in row-major order.
    ///
    /// # Panics
    ///
    /// Panics if the length of the provided slice is not equal to `n_shards * n_shards`.
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn compute_metadata_from_symbol_hashes(
        config: EncodingConfigEnum,
        symbol_hashes: &[Node],
        unencoded_length: u64,
    ) -> VerifiedBlobMetadataWithId {
        tracing::debug!("computing blob metadata and ID");

        let n_shards = config.n_shards_as_usize();
        assert_eq!(symbol_hashes.len(), n_shards * n_shards);

        let mut metadata = Vec::with_capacity(n_shards);
        for sliver_index in 0..n_shards {
            let primary_hash = MerkleTree::<Blake2b256>::build_from_leaf_hashes(
                symbol_hashes[n_shards * sliver_index..n_shards * (sliver_index + 1)]
                    .iter()
                    .cloned(),
            )
            .root();
            let secondary_hash = MerkleTree::<Blake2b256>::build_from_leaf_hashes(
                (0..n_shards).map(|symbol_index| {
                    symbol_hashes[n_shards * symbol_index + n_shards - 1 - sliver_index].clone()
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
            config.encoding_type(),
            unencoded_length,
        )
    }

    fn n_rows_usize(&self) -> usize {
        self.n_rows.get().into()
    }

    fn n_columns_usize(&self) -> usize {
        self.n_columns.get().into()
    }

    fn n_shards_usize(&self) -> usize {
        self.config.n_shards_as_usize()
    }
}

/// Struct to perform the full blob encoding.
#[derive(Debug)]
pub struct BlobEncoder<'a> {
    /// A reference to the blob.
    // INV: `blob.len() > 0`
    // TODO(WAL-1093): Consider using a `Bytes` object here instead or add it as a variant to the
    // `OwnedOrBorrowedBlob`.
    blob: OwnedOrBorrowedBlob<'a>,
    /// Inner state that doesn't depend on the blob data.
    inner: BlobEncoderData,
}

impl<'a> BlobEncoder<'a> {
    /// Creates a new `BlobEncoder` to encode the provided `blob` with the provided configuration.
    ///
    /// The actual encoding can be performed with the
    /// [`encode_with_metadata()`][Self::encode_with_metadata] method.
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
    pub fn new(
        config: EncodingConfigEnum,
        blob: OwnedOrBorrowedBlob<'a>,
    ) -> Result<Self, DataTooLargeError> {
        tracing::debug!("creating new blob encoder");
        let symbol_size = utils::compute_symbol_size_from_usize(
            blob.len(),
            config.source_symbols_per_blob(),
            config.encoding_type().required_alignment(),
        )?;
        let n_rows = config.n_source_symbols::<Primary>();
        let n_columns = config.n_source_symbols::<Secondary>();
        let blob_size = blob.len();
        let blob_prefix = crate::utils::data_prefix_string(blob.as_ref(), 5);

        Ok(Self {
            blob,
            inner: BlobEncoderData {
                symbol_size,
                n_rows,
                n_columns,
                config,
                span: tracing::span!(Level::ERROR, "BlobEncoder", blob_size, blob = blob_prefix,),
            },
        })
    }

    /// Encodes the blob with which `self` was created to a vector of [`SliverPair`s][SliverPair],
    /// and provides the relative [`VerifiedBlobMetadataWithId`].
    ///
    /// The returned blob metadata is considered to be verified as it is directly built from the
    /// data.
    ///
    /// # Panics
    ///
    /// This function can panic if there is insufficient virtual memory for the encoded data,
    /// notably on 32-bit architectures. As there is an expansion factor of approximately 4.5, blobs
    /// larger than roughly 800 MiB cannot be encoded on 32-bit architectures.
    pub fn encode_with_metadata(self) -> (Vec<SliverPair>, VerifiedBlobMetadataWithId) {
        let _guard = self.inner.span.enter();
        tracing::debug!("starting to encode blob with metadata");
        let unencoded_length =
            u64::try_from(self.blob.len()).expect("any valid blob size fits into a `u64`");

        // Only allocate empty systematic primary slivers upfront to limit peak memory usage.
        let mut primary_slivers = Vec::with_capacity(self.inner.n_shards_usize());
        primary_slivers.extend(
            self.inner
                .empty_slivers_range::<Primary>(0..self.inner.n_rows.get()),
        );
        let mut secondary_slivers = self.inner.empty_slivers::<Secondary>();

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

        drop(self.blob);

        // Compute the remaining secondary slivers by encoding the rows (i.e., primary slivers)
        // using the secondary encoding.
        let mut secondary_encoder = self.inner.get_encoder::<Secondary>();
        for (r, row) in primary_slivers
            .iter()
            .take(self.inner.n_rows_usize())
            .enumerate()
        {
            let encode_result = secondary_encoder
                .encode(row.symbols.data())
                .expect("size has already been checked");
            for (symbol, sliver) in encode_result.recovery_iter().zip(
                secondary_slivers
                    .iter_mut()
                    .skip(self.inner.n_columns_usize()),
            ) {
                sliver.copy_symbol_to(r, symbol);
            }
        }
        drop(secondary_encoder);

        // Now we can encode all secondary slivers, computing the remaining primary slivers and all
        // symbol hashes.
        let n_shards = self.inner.config.n_shards_as_usize();
        let mut symbol_hashes = vec![Node::Empty; n_shards * n_shards];

        // Create the non-systematic primary slivers.
        primary_slivers.extend(self.inner.empty_slivers_range::<Primary>(
            self.inner.n_rows.get()..self.inner.config.n_shards().get(),
        ));

        let mut primary_encoder = self.inner.get_encoder::<Primary>();
        for (col_index, column) in secondary_slivers.iter().enumerate() {
            let symbols = primary_encoder
                .encode_all_ref(column.symbols.data())
                .expect("size has already been checked");
            for (row_index, symbol) in symbols.to_symbols().enumerate() {
                symbol_hashes[n_shards * row_index + col_index] = leaf_hash::<Blake2b256>(symbol);
            }
            if col_index < self.inner.n_columns_usize() {
                for (symbol, sliver) in symbols
                    .to_symbols()
                    .zip(primary_slivers.iter_mut())
                    .skip(self.inner.n_rows_usize())
                {
                    sliver.copy_symbol_to(col_index, symbol);
                }
            }
        }
        drop(primary_encoder);

        let sliver_pairs = primary_slivers
            .into_iter()
            .zip(secondary_slivers.into_iter().rev())
            .map(|(primary, secondary)| SliverPair { primary, secondary })
            .collect();
        let metadata = BlobEncoderData::compute_metadata_from_symbol_hashes(
            self.inner.config,
            &symbol_hashes,
            unencoded_length,
        );
        (sliver_pairs, metadata)
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
    #[deprecated(note = "use `encode_with_metadata` instead")]
    pub fn encode_with_metadata_legacy(&self) -> (Vec<SliverPair>, VerifiedBlobMetadataWithId) {
        let _guard = self.inner.span.enter();
        tracing::debug!("starting to encode blob with metadata");
        let mut expanded_matrix = self.get_expanded_matrix();
        let metadata = expanded_matrix.get_metadata();

        // This is just an optimization to free memory that is no longer needed at this point.
        expanded_matrix.drop_recovery_symbols();

        let mut sliver_pairs = self.inner.empty_sliver_pairs();
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
        let _guard = self.inner.span.enter();
        tracing::debug!("starting to compute metadata");
        let unencoded_length =
            u64::try_from(self.blob.len()).expect("any valid blob size fits into a `u64`");
        let n_shards = self.inner.n_shards_usize();

        let n_non_systematic_secondary_slivers = n_shards - self.inner.n_columns_usize();
        // Use a dummy sliver index for the non-systematic secondary slivers.
        let mut non_systematic_secondary_slivers =
            vec![
                self.inner.empty_sliver::<Secondary>(SliverIndex(0));
                n_non_systematic_secondary_slivers
            ];

        // Compute the non-systematic secondary slivers by encoding the rows (i.e., primary
        // slivers).
        let mut secondary_encoder = self.inner.get_encoder::<Secondary>();
        let mut buffer = Symbols::zeros(self.inner.n_columns_usize(), self.inner.symbol_size);
        let row_length_bytes =
            usize::from(self.inner.symbol_size.get()) * self.inner.n_columns_usize();
        for (r, row) in self.rows_all().enumerate() {
            let current_row_len = row.len();
            let data = if current_row_len < row_length_bytes {
                buffer.data_mut()[current_row_len..row_length_bytes].fill(0);
                buffer.data_mut()[..current_row_len].copy_from_slice(row);
                buffer.data()
            } else {
                row
            };
            let encode_result = secondary_encoder
                .encode(data)
                .expect("size has already been checked");
            for (symbol, sliver) in encode_result
                .recovery_iter()
                .zip(non_systematic_secondary_slivers.iter_mut())
            {
                sliver.copy_symbol_to(r, symbol);
            }
        }
        drop(secondary_encoder);

        // Now we can encode all secondary slivers, computing all symbol hashes.
        let mut symbol_hashes = vec![Node::Empty; n_shards * n_shards];

        // First encode the systematic secondary slivers.
        let mut primary_encoder = self.inner.get_encoder::<Primary>();
        let mut buffer = Symbols::zeros(self.inner.n_rows_usize(), self.inner.symbol_size);
        for (col_index, column_symbols) in self.column_symbols().enumerate() {
            buffer.data_mut().fill(0);
            buffer
                .to_symbols_mut()
                .zip(column_symbols)
                .for_each(|(dest, src)| dest[..src.len()].copy_from_slice(src));
            let symbols = primary_encoder
                .encode_all_ref(buffer.data())
                .expect("size has already been checked");
            for (row_index, symbol) in symbols.to_symbols().enumerate() {
                symbol_hashes[n_shards * row_index + col_index] = leaf_hash::<Blake2b256>(symbol);
            }
        }

        // Then encode the non-systematic secondary slivers.
        for (col_index, column) in non_systematic_secondary_slivers.iter().enumerate() {
            let col_index = col_index + self.inner.n_columns_usize();
            let symbols = primary_encoder
                .encode_all_ref(column.symbols.data())
                .expect("size has already been checked");
            for (row_index, symbol) in symbols.to_symbols().enumerate() {
                symbol_hashes[n_shards * row_index + col_index] = leaf_hash::<Blake2b256>(symbol);
            }
        }

        drop(primary_encoder);

        BlobEncoderData::compute_metadata_from_symbol_hashes(
            self.inner.config,
            &symbol_hashes,
            unencoded_length,
        )
    }

    /// Returns a reference to the blob data.
    pub fn blob(&self) -> &[u8] {
        self.blob.as_ref()
    }

    // Forwarding methods to inner

    /// Returns the size of the symbol in bytes.
    pub fn symbol_usize(&self) -> usize {
        self.inner.symbol_usize()
    }

    /// Returns a reference to the symbol at the provided indices in the message matrix.
    ///
    /// The length of the returned slice can be lower than `self.symbol_size` if the blob needs to
    /// be padded.
    fn symbol_at(&self, row_index: usize, col_index: usize) -> &[u8] {
        let start_index = cmp::min(
            self.symbol_usize() * (self.inner.n_columns_usize() * row_index + col_index),
            self.blob.len(),
        );
        let end_index = cmp::min(start_index + self.symbol_usize(), self.blob.len());
        self.blob()[start_index..end_index].as_ref()
    }

    fn column_symbols(
        &self,
    ) -> impl ExactSizeIterator<Item = impl ExactSizeIterator<Item = &[u8]>> {
        (0..self.inner.n_columns_usize()).map(move |col_index| {
            (0..self.inner.n_rows_usize())
                .map(move |row_index| self.symbol_at(row_index, col_index))
        })
    }

    fn rows(&self) -> Chunks<'_, u8> {
        self.blob()
            .chunks(self.inner.n_columns_usize() * self.symbol_usize())
    }

    fn rows_all(&self) -> impl ExactSizeIterator<Item = &[u8]> {
        (0..self.inner.n_rows_usize()).map(move |row_index| {
            let start_index = cmp::min(
                self.symbol_usize() * (self.inner.n_columns_usize() * row_index),
                self.blob.len(),
            );
            let end_index = cmp::min(
                start_index + self.symbol_usize() * self.inner.n_columns_usize(),
                self.blob.len(),
            );
            self.blob()[start_index..end_index].as_ref()
        })
    }

    /// Computes the fully expanded message matrix by encoding rows and columns.
    #[tracing::instrument(level = "debug", skip_all)]
    fn get_expanded_matrix(&self) -> ExpandedMessageMatrix<'_> {
        self.inner.span.in_scope(|| {
            ExpandedMessageMatrix::new(self.inner.config, self.inner.symbol_size, self.blob())
        })
    }

    /// Returns the systematic primary sliver at the given index.
    ///
    /// For indices greater than the number of systematic primary slivers, the function returns a
    /// sliver with the correct length but all symbols set to 0.
    fn systematic_primary_sliver(&self, index: SliverIndex) -> SliverData<Primary> {
        let mut sliver = self.inner.empty_sliver::<Primary>(index);
        if let Some(row) = self.rows().nth(index.as_usize()) {
            sliver.symbols.data_mut()[..row.len()].copy_from_slice(row);
        }
        sliver
    }

    /// Performs the default consistency check.
    ///
    /// This checks the primary sliver hashes against the metadata.
    ///
    /// Also takes an optional vector of booleans indicating for each systematic primary sliver
    /// whether it has already been verified (and thus can be skipped). If not `None`, the length of
    /// the vector must be the number of systematic primary slivers.
    ///
    /// # Errors
    ///
    /// Returns a [`DecodeError::VerificationError`] if the check fails.
    ///
    /// Returns a [`DecodeError::DataTooLarge`] if the blob size is too large to be encoded.
    ///
    /// # Panics
    ///
    /// Panics if the length of the `already_verified_slivers` vector is not equal to the number of
    /// systematic primary slivers.
    pub(crate) fn default_consistency_check(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        already_verified_slivers: &Option<Vec<bool>>,
    ) -> Result<(), DecodeError> {
        let n_systematic_primary_slivers =
            self.inner.config.n_systematic_slivers::<Primary>().get();
        tracing::debug!(
            n_systematic_primary_slivers,
            "performing default consistency check"
        );
        if let Some(already_verified_slivers) = already_verified_slivers {
            assert_eq!(
                already_verified_slivers.len(),
                usize::from(n_systematic_primary_slivers)
            );
        }
        for i in 0..n_systematic_primary_slivers {
            if let Some(already_verified_slivers) = already_verified_slivers
                && already_verified_slivers[usize::from(i)]
            {
                tracing::trace!(
                    "skipping verification of already verified systematic primary sliver {i}"
                );
                continue;
            }
            tracing::trace!("verifying systematic primary sliver {i}");
            let sliver = self.systematic_primary_sliver(SliverIndex(i));
            sliver
                .check_hash(&self.inner.config, metadata.metadata())
                .map_err(|_| DecodeError::VerificationError)?;
        }
        Ok(())
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
                .to_symbols()
                .enumerate()
            {
                self.matrix[self.n_rows + row_index][col_index].copy_from_slice(symbol);
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
                .to_symbols()
                .enumerate()
            {
                row[self.n_columns + col_index].copy_from_slice(symbol)
            }
        }
    }

    /// Computes the sliver pair metadata from the expanded message matrix.
    #[tracing::instrument(level = "debug", skip_all)]
    fn get_metadata(&self) -> VerifiedBlobMetadataWithId {
        tracing::debug!("computing blob metadata and ID");

        let n_shards = self.config.n_shards_as_usize();
        let mut symbol_hashes = Vec::with_capacity(n_shards * n_shards);
        for row in 0..n_shards {
            for col in 0..n_shards {
                symbol_hashes.push(leaf_hash::<Blake2b256>(&self.matrix[row][col]));
            }
        }

        BlobEncoderData::compute_metadata_from_symbol_hashes(
            self.config,
            &symbol_hashes,
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
    /// After this function is called, the function [`get_metadata`][Self::get_metadata] no longer
    /// produces meaningful results.
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
pub struct BlobDecoder<D: Decoder, E: EncodingAxis = Primary> {
    _decoding_axis: PhantomData<E>,
    decoder: D,
    blob_size: usize,
    symbol_size: NonZeroU16,
    sliver_count: usize,
    sliver_length: usize,
    /// The number of columns of the blob's message matrix (i.e., the number of secondary slivers).
    n_columns: usize,
    /// The workspace used to store the sliver data and iteratively overwrite it with the decoded
    /// blob. While a flat byte array, this is interpreted as a matrix of symbols. The layout is the
    /// same as the blob's message matrix; that is, primary slivers are written as "rows" while
    /// secondary slivers are written as "columns".
    workspace: Symbols,
    /// The indices of the slivers that have been provided and added to the workspace.
    sliver_indices: Vec<SliverIndex>,
}

impl<D: Decoder, E: EncodingAxis> BlobDecoder<D, E> {
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
    pub fn new(config: &D::Config, blob_size: u64) -> Result<Self, DecodeError> {
        tracing::debug!("creating new blob decoder");
        let symbol_size = config.symbol_size_for_blob(blob_size)?;
        let blob_size = blob_size.try_into().map_err(|_| DataTooLargeError)?;
        let n_source_symbols = config.n_source_symbols::<E>();

        let decoder = D::new(n_source_symbols, config.n_shards(), symbol_size)?;

        let sliver_length = config.n_source_symbols::<E::OrthogonalAxis>().get().into();
        let sliver_count = usize::from(n_source_symbols.get());
        let n_symbols_in_workspace = sliver_length * sliver_count;

        let (n_columns, workspace) = if E::IS_PRIMARY {
            (
                sliver_length,
                Symbols::with_capacity(n_symbols_in_workspace, symbol_size),
            )
        } else {
            (
                sliver_count,
                Symbols::zeros(n_symbols_in_workspace, symbol_size),
            )
        };

        Ok(Self {
            _decoding_axis: PhantomData,
            decoder,
            blob_size,
            symbol_size,
            sliver_count,
            sliver_length,
            n_columns,
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
        let mut sliver_indices_set = BTreeSet::new();
        let mut slivers_count = 0;
        for sliver in slivers {
            if slivers_count == self.sliver_count {
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
            slivers_count == self.sliver_count,
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

    fn symbol_usize(&self) -> usize {
        self.symbol_size.get().into()
    }
}

#[cfg(test)]
mod tests {
    use walrus_test_utils::{param_test, random_data, random_subset};

    use super::*;
    use crate::{
        EncodingType,
        encoding::{EncodingConfig, ReedSolomonEncodingConfig, common::ConsistencyCheckType},
        metadata::{BlobMetadataApi as _, UnverifiedBlobMetadataWithId},
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
        let n_rows = blob_encoder.inner.n_rows_usize();
        let n_columns = blob_encoder.inner.n_columns_usize();
        let (sliver_pairs, _metadata) = blob_encoder.encode_with_metadata();
        let rows: Vec<_> = sliver_pairs
            .iter()
            .take(n_rows)
            .map(|pair| pair.primary.symbols.data())
            .collect();
        let columns: Vec<_> = sliver_pairs
            .iter()
            .rev()
            .take(n_columns)
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

        let (sliver_pairs, _metadata) = config
            .get_blob_encoder(&blob)
            .unwrap()
            .encode_with_metadata();

        let slivers_for_decoding: Vec<_> = random_subset(
            sliver_pairs,
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
    #[allow(deprecated)]
    fn test_encode_with_metadata() {
        let encoding_type = EncodingType::RS2;
        let blob = random_data(27182);
        let n_shards = 102;

        let config = EncodingConfig::new(NonZeroU16::new(n_shards).unwrap());
        let config_enum = config.get_for_type(encoding_type);

        // Check that old and new encoding  are identical.
        let blob_encoder = config.reed_solomon.get_blob_encoder(&blob).unwrap();
        let blob_metadata_0 = blob_encoder.compute_metadata();
        let (sliver_pairs_1, blob_metadata_1) = blob_encoder.encode_with_metadata_legacy();
        assert_eq!(blob_metadata_0, blob_metadata_1);

        let (sliver_pairs_2, blob_metadata_2) = config_enum.encode_with_metadata(blob).unwrap();
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
        walrus_test_utils::init_tracing();

        let blob = random_data(16180);
        let blob_size: u64 = blob.len().try_into().unwrap();
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

        assert_eq!(metadata_enc.metadata().unencoded_length(), blob_size);

        for consistency_check in [
            ConsistencyCheckType::Strict,
            ConsistencyCheckType::Default,
            ConsistencyCheckType::Skip,
        ] {
            let blob_dec = config
                .decode_and_verify(
                    &metadata_enc,
                    slivers_for_decoding.clone(),
                    consistency_check,
                )
                .expect("should be able to decode and verify blob");
            assert_eq!(blob, blob_dec, "decoded blob does not match original blob");
        }
    }
}
