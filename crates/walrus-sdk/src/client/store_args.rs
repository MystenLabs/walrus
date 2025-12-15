// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Arguments for the store operations in the client.

use std::{num::NonZeroU16, sync::Arc, time::Duration};

use tokio::{
    sync::{
        Mutex,
        mpsc::{Sender as MpscSender, UnboundedSender},
    },
    task::JoinHandle,
};
use walrus_core::{DEFAULT_ENCODING, EncodingType, EpochCount};
use walrus_sui::client::{BlobPersistence, PostStoreAction};

use super::{
    communication::node::NodeIndex,
    metrics::ClientMetrics,
    upload_relay_client::UploadRelayClient,
};
use crate::{
    client::upload_relay_client::UploadRelayClientError,
    store_optimizations::StoreOptimizations,
    upload_relay::tip_config::TipConfig,
    uploader::{TailHandling, UploaderEvent},
};

/// Events emitted while encoding blobs before upload.
#[derive(Debug, Clone)]
pub enum EncodingProgressEvent {
    /// Encoding is starting with the given total number of blobs.
    Started {
        /// The total number of blobs that will be encoded.
        total: usize,
    },
    /// A blob finished encoding.
    BlobCompleted {
        /// The number of blobs that have finished encoding so far (1-indexed).
        completed: usize,
        /// The total number of blobs scheduled for encoding.
        total: usize,
    },
    /// Encoding finished (all blobs encoded).
    Finished,
}

/// Arguments for store operations that are frequently passed together.
// NOTE: In the future, if the struct grows larger, we may need to consider using a builder.
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
    pub upload_relay_client: Option<Arc<UploadRelayClient>>,
    /// Tail handling preference for sliver uploads. `Detached` allows the caller to decide whether
    /// to await the tail uploads or hand them off.
    pub tail_handling: TailHandling,
    /// Optional channel to forward uploader events to. When set in combination with
    /// `TailHandling::Detached`, events can be forwarded to another task.
    pub quorum_event_tx: Option<MpscSender<UploaderEvent>>,
    /// Optional collector that receives detached tail handles for later awaiting; this keeps
    /// ownership of the handles with the caller so they can decide when to await them. Without a
    /// collector, detached handles are awaited in a background task and only surfaced via logging.
    pub tail_handle_collector: Option<Arc<Mutex<Vec<JoinHandle<()>>>>>,
    /// Optional channel to forward encoding progress events to.
    pub encoding_event_tx: Option<UnboundedSender<EncodingProgressEvent>>,
    /// Initial upload weight accumulated before certification (e.g., from pending uploads).
    pub initial_upload_weight: Option<std::collections::HashMap<walrus_core::BlobId, usize>>,
    /// Nodes that failed during pending uploads, keyed by blob ID, to scope immediate retries.
    pub initial_failed_nodes:
        Option<std::collections::HashMap<walrus_core::BlobId, Vec<NodeIndex>>>,
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
            ..Self::default_inner()
        }
    }

    /// Creates a `StoreArgs` with default values and the specified number of epochs ahead.
    pub fn default_with_epochs(epochs_ahead: EpochCount) -> Self {
        Self::default_inner().with_epochs_ahead(epochs_ahead)
    }

    fn default_inner() -> Self {
        Self {
            encoding_type: DEFAULT_ENCODING,
            epochs_ahead: 1,
            store_optimizations: StoreOptimizations::all(),
            persistence: BlobPersistence::Deletable,
            post_store: PostStoreAction::Keep,
            metrics: None,
            upload_relay_client: None,
            tail_handling: TailHandling::Blocking,
            quorum_event_tx: None,
            tail_handle_collector: None,
            encoding_event_tx: None,
            initial_upload_weight: None,
            initial_failed_nodes: None,
        }
    }

    /// Sets the upload relay client.
    pub fn with_upload_relay_client(mut self, upload_relay_client: UploadRelayClient) -> Self {
        self.upload_relay_client = Some(Arc::new(upload_relay_client));
        self
    }

    /// Sets the tail handling strategy. When switching to `Detached`, use it with
    /// either `with_tail_handle_collector` or a quorum event channel so completion can be tracked.
    pub fn with_tail_handling(mut self, tail_handling: TailHandling) -> Self {
        self.tail_handling = tail_handling;
        self
    }

    /// Sets the uploader event channel used to forward quorum notifications.
    pub fn with_quorum_event_tx(mut self, tx: MpscSender<UploaderEvent>) -> Self {
        self.quorum_event_tx = Some(tx);
        self
    }

    /// Sets the encoding event channel used to forward encoding progress.
    pub fn with_encoding_event_tx(mut self, tx: UnboundedSender<EncodingProgressEvent>) -> Self {
        self.encoding_event_tx = Some(tx);
        self
    }

    /// Sets the collector for detached tail handles so the caller can await them explicitly.
    pub fn with_tail_handle_collector(
        mut self,
        collector: Arc<Mutex<Vec<JoinHandle<()>>>>,
    ) -> Self {
        self.tail_handle_collector = Some(collector);
        self
    }

    /// Marks blob IDs that already had optimistic uploads scheduled earlier.
    pub fn with_initial_upload_weight(
        mut self,
        weight: std::collections::HashMap<walrus_core::BlobId, usize>,
    ) -> Self {
        self.initial_upload_weight = Some(weight);
        self
    }

    /// Sets the initial failed nodes map.
    pub fn with_initial_failed_nodes(
        mut self,
        failed: std::collections::HashMap<walrus_core::BlobId, Vec<NodeIndex>>,
    ) -> Self {
        self.initial_failed_nodes = Some(failed);
        self
    }

    /// Returns a reference to the upload relay client if present.
    pub fn upload_relay_client_ref(&self) -> Option<&UploadRelayClient> {
        self.upload_relay_client
            .as_ref()
            .map(|client| client.as_ref())
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

    /// Convenience method for `with_persistence(BlobPersistence::Permanent)`.
    pub fn permanent(self) -> Self {
        self.with_persistence(BlobPersistence::Permanent)
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
