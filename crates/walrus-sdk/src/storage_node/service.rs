// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! [`Service`] trait implementations for the storage node client.
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use tower_service::Service;
use walrus_core::{metadata::UnverifiedBlobMetadataWithId, BlobId};

use super::Client;
use crate::error::NodeError;

/// Requests the unverified metadata corresponding to a Blob ID.
#[derive(Debug)]
pub struct UnverifiedMetadataRequest(pub BlobId);

impl Service<UnverifiedMetadataRequest> for Client {
    type Response = UnverifiedBlobMetadataWithId;
    type Error = NodeError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: UnverifiedMetadataRequest) -> Self::Future {
        let client = self.clone();
        Box::pin(async move {
            let blob_id = req.0;
            client.get_metadata(&blob_id).await
        })
    }
}
