// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Owned blob store backend.

use walrus_sui::client::SuiContractClient;

use super::{StoreBackend, StoreBackendKind};
use crate::node_client::{
    StoreArgs,
    WalrusNodeClient,
    client_types::{EncodedBlob, WalrusStoreBlobUnfinished},
    responses::BlobStoreResult,
};

pub(super) struct OwnedStoreBackend<'a> {
    client: &'a WalrusNodeClient<SuiContractClient>,
}

impl<'a> OwnedStoreBackend<'a> {
    pub(super) fn new(client: &'a WalrusNodeClient<SuiContractClient>) -> Self {
        Self { client }
    }
}

impl StoreBackend for OwnedStoreBackend<'_> {
    type FinalResult = BlobStoreResult;

    fn kind(&self) -> StoreBackendKind {
        StoreBackendKind::Owned
    }

    fn reserve_and_store_encoded_blobs<'a>(
        &'a self,
        encoded_blobs: Vec<WalrusStoreBlobUnfinished<EncodedBlob>>,
        store_args: &'a StoreArgs,
    ) -> super::StoreBackendFuture<'a, Self::FinalResult> {
        Box::pin(async move {
            self.client
                .reserve_and_store_encoded_blobs(encoded_blobs, store_args)
                .await
        })
    }
}
