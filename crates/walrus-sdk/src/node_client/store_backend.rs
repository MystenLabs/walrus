// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Internal store backend selection.

use super::*;
use crate::node_client::client_types::WalrusStoreFinalResultApi;

mod bucket;
mod owned;

use bucket::BlobBucketStoreBackend;
use owned::OwnedStoreBackend;
use walrus_sui::client::BlobBucketHandle;

type StoreBackendFuture<'a, R> =
    Pin<Box<dyn Future<Output = ClientResult<Vec<WalrusStoreBlobFinished<R>>>> + Send + 'a>>;

/// The internal backend selected for a store operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StoreBackendKind {
    Owned,
    BlobBucket,
}

/// Internal backend interface for blob storage flows.
pub(super) trait StoreBackend {
    /// Final result type emitted by the backend.
    type FinalResult: WalrusStoreFinalResultApi;

    /// Returns the selected backend kind.
    fn kind(&self) -> StoreBackendKind;

    /// Reserves, uploads, certifies, and finalizes encoded blobs using the backend.
    fn reserve_and_store_encoded_blobs<'a>(
        &'a self,
        encoded_blobs: Vec<WalrusStoreBlobUnfinished<EncodedBlob>>,
        store_args: &'a StoreArgs,
    ) -> StoreBackendFuture<'a, Self::FinalResult>;
}

impl WalrusNodeClient<SuiContractClient> {
    /// Selects the internal store backend for the current request.
    pub(super) fn store_backend(&self, _store_args: &StoreArgs) -> OwnedStoreBackend<'_> {
        OwnedStoreBackend::new(self)
    }

    /// Selects the internal blob bucket backend for the current request.
    pub(super) fn blob_bucket_store_backend(
        &self,
        blob_bucket: BlobBucketHandle,
    ) -> BlobBucketStoreBackend<'_> {
        BlobBucketStoreBackend::new(self, blob_bucket)
    }
}
