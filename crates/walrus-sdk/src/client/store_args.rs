// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Arguments for the store operations in the client.

use std::{num::NonZeroU16, sync::Arc, time::Duration};

use walrus_core::{DEFAULT_ENCODING, EncodingType, EpochCount};
use walrus_sui::client::{BlobPersistence, PostStoreAction};

use super::{metrics::ClientMetrics, upload_relay_client::UploadRelayClient};
use crate::{
    client::upload_relay_client::UploadRelayClientError,
    store_optimizations::StoreOptimizations,
    upload_relay::tip_config::TipConfig,
};

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
    /// The optional upload relay client, that allows to store the blob via the relay.
    pub upload_relay_client: Option<UploadRelayClient>,
}

impl Default for StoreArgs {
    fn default() -> Self {
        Self {
            encoding_type: DEFAULT_ENCODING,
            epochs_ahead: 1,
            store_optimizations: StoreOptimizations::all(),
            // TODO(WAL-911): Ensure this is changed to `Deletable`, once we switch the defaults.
            persistence: BlobPersistence::Permanent,
            post_store: PostStoreAction::Keep,
            metrics: None,
            upload_relay_client: None,
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
            upload_relay_client: None,
        }
    }

    /// Sets the upload relay client.
    pub fn with_upload_relay_client(mut self, upload_relay_client: UploadRelayClient) -> Self {
        self.upload_relay_client = Some(upload_relay_client);
        self
    }

    /// Returns a reference to the upload relay client if present.
    pub fn upload_relay_client_ref(&self) -> Option<&UploadRelayClient> {
        self.upload_relay_client.as_ref()
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

    /// Observe the encoding latency, if metrics are present.
    pub fn maybe_observe_encoding_latency(&self, duration: Duration) {
        if let Some(metrics) = self.metrics_ref() {
            metrics.observe_encoding_latency(duration);
        }
    }

    /// Observe the blob status check latency, if metrics are present.
    pub fn maybe_observe_checking_blob_status(&self, duration: Duration) {
        if let Some(metrics) = self.metrics_ref() {
            metrics.observe_checking_blob_status(duration);
        }
    }

    /// Observe the store operation latency, if metrics are present.
    pub fn maybe_observe_store_operation(&self, duration: Duration) {
        if let Some(metrics) = self.metrics_ref() {
            metrics.observe_store_operation(duration);
        }
    }

    /// Observe the latency to get certificates, if metrics are present.
    pub fn maybe_observe_get_certificates(&self, duration: Duration) {
        if let Some(metrics) = self.metrics_ref() {
            metrics.observe_get_certificates(duration);
        }
    }

    /// Observe the latency to upload the certificate, if metrics are present.
    pub fn maybe_observe_upload_certificate(&self, duration: Duration) {
        if let Some(metrics) = self.metrics_ref() {
            metrics.observe_upload_certificate(duration);
        }
    }

    /// Computes the total tip amount for all blobs based on their unencoded lengths.
    pub fn compute_total_tip_amount(
        &self,
        n_shards: NonZeroU16,
        unencoded_lengths: &[u64],
    ) -> Result<Option<u64>, UploadRelayClientError> {
        let Some(upload_relay_client) = self.upload_relay_client_ref() else {
            return Ok(None);
        };

        let TipConfig::SendTip { address: _, kind } = upload_relay_client.tip_config() else {
            return Ok(None);
        };

        let mut total_tip = 0u64;
        for &unencoded_length in unencoded_lengths {
            let tip_amount = kind
                .compute_tip(n_shards, unencoded_length, self.encoding_type)
                .ok_or(UploadRelayClientError::TipComputationFailed {
                    unencoded_length,
                    n_shards,
                    encoding_type: self.encoding_type,
                })?;
            total_tip += tip_amount;
        }

        Ok(Some(total_tip))
    }
}
