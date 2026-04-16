// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Internal store backend selection.

use std::{future::Future, pin::Pin};

use sui_types::base_types::ObjectID;
use walrus_sui::client::SuiContractClient;

use crate::{
    error::ClientResult,
    node_client::{
        StoreArgs,
        WalrusNodeClient,
        client_types::{
            EncodedBlob,
            WalrusStoreBlobFinished,
            WalrusStoreBlobUnfinished,
            WalrusStoreFinalResultApi,
        },
        responses::{BlobStoreResult, PooledBlobStoreResult},
    },
};

mod owned;
mod pooled;

use owned::OwnedStoreBackend;
use pooled::PooledStoreBackend;

type StoreBackendFuture<'a, R> =
    Pin<Box<dyn Future<Output = ClientResult<Vec<WalrusStoreBlobFinished<R>>>> + Send + 'a>>;

/// The internal backend selected for a store operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StoreBackendKind {
    Owned,
    Pooled,
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
    pub(super) fn store_backend(
        &self,
        _store_args: &StoreArgs,
    ) -> impl StoreBackend<FinalResult = BlobStoreResult> + '_ {
        OwnedStoreBackend::new(self)
    }

    /// Selects the internal pooled store backend for the current request.
    pub(super) fn pooled_store_backend(
        &self,
        storage_pool_object_id: ObjectID,
    ) -> impl StoreBackend<FinalResult = PooledBlobStoreResult> + '_ {
        PooledStoreBackend::new(self, storage_pool_object_id)
    }
}
