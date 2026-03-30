// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Owned blob store backend.

use super::*;

pub(super) struct OwnedStoreBackend<'a> {
    client: &'a WalrusNodeClient<SuiContractClient>,
}

impl<'a> OwnedStoreBackend<'a> {
    pub(super) fn new(client: &'a WalrusNodeClient<SuiContractClient>) -> Self {
        Self { client }
    }
}

impl StoreBackend for OwnedStoreBackend<'_> {
    fn kind(&self) -> StoreBackendKind {
        StoreBackendKind::Owned
    }

    fn reserve_and_store_encoded_blobs<'a>(
        &'a self,
        encoded_blobs: Vec<WalrusStoreBlobUnfinished<EncodedBlob>>,
        store_args: &'a StoreArgs,
    ) -> Pin<Box<dyn Future<Output = ClientResult<Vec<WalrusStoreBlobFinished>>> + Send + 'a>> {
        Box::pin(async move {
            self.client
                .reserve_and_store_encoded_blobs(encoded_blobs, store_args)
                .await
        })
    }
}
