// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Arguments for the store operations in the client.

use std::sync::Arc;

use walrus_core::{DEFAULT_ENCODING, EncodingType, EpochCount};
use walrus_sui::client::{BlobPersistence, PostStoreAction};

use crate::{client::metrics::ClientMetrics, store_optimizations::StoreOptimizations};

/// Arguments for store operations that are frequently passed together.
#[derive(Debug, Clone)]
pub struct StoreArgs {
    /// The encoding type to use for encoding the files.
    pub encoding_type: EncodingType,
    /// The number of epochs ahead to store the blob.
    pub epochs_ahead: EpochCount,
    /// The store optimizations to use for the blob.
    pub store_optimizations: StoreOptimizations,
    /// The persistence type to use for the blob.
    pub persistence: BlobPersistence,
    /// The post store action to use for the blob.
    pub post_store: PostStoreAction,
    /// The metrics to use for the blob.
    pub metrics: Option<Arc<ClientMetrics>>,
}

impl Default for StoreArgs {
    fn default() -> Self {
        Self {
            encoding_type: DEFAULT_ENCODING,
            epochs_ahead: 1,
            store_optimizations: StoreOptimizations::all(),
            persistence: BlobPersistence::Permanent,
            post_store: PostStoreAction::Keep,
            metrics: None,
        }
    }
}

impl StoreArgs {
    /// Creates a new `StoreArgs` with the given parameters.
    pub fn new(
        encoding_type: EncodingType,
        epochs_ahead: EpochCount,
        store_optimizations: StoreOptimizations,
        persistence: BlobPersistence,
        post_store: PostStoreAction,
    ) -> Self {
        Self {
            encoding_type,
            epochs_ahead,
            store_optimizations,
            persistence,
            post_store,
            metrics: None,
        }
    }

    /// Sets the encoding type.
    pub fn with_encoding_type(mut self, encoding_type: EncodingType) -> Self {
        self.encoding_type = encoding_type;
        self
    }

    /// Sets the number of epochs ahead.
    pub fn with_epochs_ahead(mut self, epochs_ahead: EpochCount) -> Self {
        self.epochs_ahead = epochs_ahead;
        self
    }

    /// Sets the store optimizations.
    pub fn with_store_optimizations(mut self, store_optimizations: StoreOptimizations) -> Self {
        self.store_optimizations = store_optimizations;
        self
    }

    /// Sets the persistence type.
    pub fn with_persistence(mut self, persistence: BlobPersistence) -> Self {
        self.persistence = persistence;
        self
    }

    /// Sets the post store action.
    pub fn with_post_store(mut self, post_store: PostStoreAction) -> Self {
        self.post_store = post_store;
        self
    }

    /// Adds metrics to the `StoreArgs`.
    pub fn with_metrics(mut self, metrics: Arc<ClientMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Returns a reference to the metrics if present.
    pub fn metrics_ref(&self) -> Option<&Arc<ClientMetrics>> {
        self.metrics.as_ref()
    }

    /// Convenience method for `with_store_optimizations(StoreOptimizations::none())`.
    pub fn no_store_optimizations(self) -> Self {
        self.with_store_optimizations(StoreOptimizations::none())
    }

    /// Convenience method for `with_persistence(BlobPersistence::Deletable)`.
    pub fn deletable(self) -> Self {
        self.with_persistence(BlobPersistence::Deletable)
    }
}
