// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Internal store backend selection.

use super::*;

mod owned;

use owned::OwnedStoreBackend;

/// The internal backend selected for a store operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StoreBackendKind {
    Owned,
}

/// Internal backend interface for blob storage flows.
pub(super) trait StoreBackend {
    /// Returns the selected backend kind.
    fn kind(&self) -> StoreBackendKind;

    /// Reserves, uploads, certifies, and finalizes encoded blobs using the backend.
    fn reserve_and_store_encoded_blobs<'a>(
        &'a self,
        encoded_blobs: Vec<WalrusStoreBlobUnfinished<EncodedBlob>>,
        store_args: &'a StoreArgs,
    ) -> Pin<Box<dyn Future<Output = ClientResult<Vec<WalrusStoreBlobFinished>>> + Send + 'a>>;
}

impl WalrusNodeClient<SuiContractClient> {
    /// Selects the internal store backend for the current request.
    pub(super) fn store_backend(&self, _store_args: &StoreArgs) -> impl StoreBackend + '_ {
        OwnedStoreBackend::new(self)
    }
}
