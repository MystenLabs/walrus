// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! S3 API handlers.

pub mod bucket;
pub mod object;

use crate::auth::SigV4Authenticator;
use crate::metadata::MetadataStore;
use axum::http::{HeaderMap, Method, Uri};
use std::sync::Arc;
use walrus_sdk::client::Client;

/// Shared state for S3 handlers.
#[derive(Clone)]
pub struct S3State {
    /// Walrus client.
    pub walrus_client: Arc<Client<walrus_sui::client::SuiContractClient>>,
    
    /// SigV4 authenticator.
    pub authenticator: SigV4Authenticator,
    
    /// Default bucket name (since Walrus doesn't have bucket concept).
    pub default_bucket: String,
    
    /// Metadata store for S3 objects.
    pub metadata_store: MetadataStore,
}

impl S3State {
    /// Create new S3 state.
    pub fn new(
        walrus_client: Client<walrus_sui::client::SuiContractClient>,
        authenticator: SigV4Authenticator,
        default_bucket: String,
    ) -> Self {
        Self {
            walrus_client: Arc::new(walrus_client),
            authenticator,
            default_bucket,
            metadata_store: MetadataStore::new(),
        }
    }
    
    /// Authenticate a request.
    pub fn authenticate(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<(), crate::error::S3Error> {
        self.authenticator.authenticate(method, uri, headers, body)
    }
}
