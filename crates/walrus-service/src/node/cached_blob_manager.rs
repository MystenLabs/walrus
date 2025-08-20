// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Manages cached blob data and registration state for parallel blob uploads.
//!
//! This manager coordinates between two asynchronous events:
//! 1. Blob data arrival from MultiPut requests
//! 2. Blob registration events from the blockchain
//!
//! When both conditions are met for a blob, it triggers the actual storage process.
//!
//! After successful processing, confirmations are cached in the `Completed` state,
//! allowing immediate reuse for duplicate requests to the same blob_id. This provides
//! server-side deduplication and performance optimization.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use dashmap::DashMap;
use futures::future::join_all;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use walrus_core::{
    BlobId,
    by_axis::ByAxis,
    encoding::SliverPair,
    messages::{BlobPersistenceType, Confirmation, SignedMessage, StorageConfirmation},
    metadata::{UnverifiedBlobMetadataWithId, VerifiedBlobMetadataWithId},
};
use walrus_storage_node_client::api::MultiPutBundle;

use crate::node::{ServiceState, StorageNodeInner};

/// State of a cached blob entry.
#[derive(Debug)]
enum CachedBlobState {
    /// Only data has arrived, waiting for registration.
    DataOnly {
        data: CachedBlobData,
        arrived_at: Instant,
        // Sender to notify when processing completes
        result_sender: oneshot::Sender<anyhow::Result<SignedMessage<Confirmation>>>,
    },
    /// Only registration has arrived, waiting for data.
    RegisteredOnly { registered_at: Instant },
    /// Both data and registration have arrived, currently processing.
    Processing,
    /// Processing completed successfully, confirmation cached for reuse.
    Completed {
        confirmation: SignedMessage<Confirmation>,
        completed_at: Instant,
    },
}

/// Cached blob data from a MultiPut request.
#[derive(Debug, Clone)]
struct CachedBlobData {
    pairs: Vec<SliverPair>,
    metadata: Arc<VerifiedBlobMetadataWithId>,
    persistence_type: BlobPersistenceType,
}

/// Manages cached blob data and coordinates with registration events.
#[derive(Debug)]
pub struct CachedBlobManager {
    /// Cache of blob states indexed by BlobId.
    cache: Arc<DashMap<BlobId, CachedBlobState>>,
    /// Reference to the storage node for processing blobs.
    node: Arc<StorageNodeInner>,
    /// Cancellation token for shutdown handling.
    shutdown_token: CancellationToken,
}

impl CachedBlobManager {
    /// Creates a new CachedBlobManager.
    pub fn new(node: Arc<StorageNodeInner>) -> Self {
        Self {
            cache: Arc::new(DashMap::new()),
            node,
            shutdown_token: CancellationToken::new(),
        }
    }

    /// Initiates shutdown of the CachedBlobManager.
    /// This cancels all pending tasks and notifies waiting futures.
    pub async fn shutdown(&self) {
        info!("Shutting down CachedBlobManager");

        // Cancel all background tasks
        self.shutdown_token.cancel();

        // Clear the cache and notify any waiting futures
        let cache_entries: Vec<_> = self.cache.iter().map(|entry| *entry.key()).collect();
        for blob_id in cache_entries {
            if let Some(entry) = self.cache.remove(&blob_id) {
                if let CachedBlobState::DataOnly { result_sender, .. } = entry.1 {
                    let _ = result_sender
                        .send(Err(anyhow::anyhow!("CachedBlobManager is shutting down")));
                }
            }
        }

        info!("CachedBlobManager shutdown complete");
    }

    /// Adds blob data from a MultiPut request.
    /// Returns a future that resolves to the confirmation certificate.
    /// If registration has already arrived, processes immediately.
    /// Otherwise, returns a future that will resolve when registration arrives.
    pub async fn add_blob_data(
        &self,
        blob_id: BlobId,
        bundle: MultiPutBundle,
    ) -> anyhow::Result<
        impl std::future::Future<Output = anyhow::Result<SignedMessage<Confirmation>>>
            + Send
            + 'static,
    > {
        info!(
            blob_id = %blob_id,
            "CachedBlobManager::add_blob_data called with {} sliver pairs",
            bundle.sliver_pairs.len()
        );

        // Check if we're shutting down
        if self.shutdown_token.is_cancelled() {
            info!(
                blob_id = %blob_id,
                "Rejecting add_blob_data due to shutdown in progress"
            );
            return Err(anyhow::anyhow!("CachedBlobManager is shutting down"));
        }

        let metadata = bundle.metadata.ok_or_else(|| {
            warn!(
                blob_id = %blob_id,
                "add_blob_data failed: metadata is required but not provided"
            );
            anyhow::anyhow!("Metadata is required")
        })?;
        let data = CachedBlobData {
            pairs: bundle.sliver_pairs,
            metadata: Arc::new(metadata),
            persistence_type: bundle.blob_persistence_type,
        };

        let (result_sender, result_receiver) = oneshot::channel();

        // Use entry API for atomic operation
        let entry = self.cache.entry(blob_id);
        match entry {
            dashmap::mapref::entry::Entry::Occupied(mut occupied) => {
                match occupied.get_mut() {
                    CachedBlobState::RegisteredOnly { .. } => {
                        info!(
                            blob_id = %blob_id,
                            "Registration already arrived, processing immediately (fast path)"
                        );

                        // Registration already arrived, transition to processing
                        occupied.insert(CachedBlobState::Processing);

                        // Process immediately
                        let node = self.node.clone();
                        let cache = self.cache.clone();
                        let shutdown_token = self.shutdown_token.clone();

                        tokio::spawn(async move {
                            // Check if we're shutting down before processing
                            if shutdown_token.is_cancelled() {
                                info!(
                                    blob_id = %blob_id,
                                    "Aborting blob processing due to shutdown during fast path"
                                );
                                let _ = result_sender.send(Err(anyhow::anyhow!(
                                    "CachedBlobManager is shutting down"
                                )));
                                return;
                            }

                            info!(
                                blob_id = %blob_id,
                                "Starting immediate blob processing (fast path)"
                            );
                            let result = Self::process_blob(&node, blob_id, data).await;

                            // Handle result - cache success, remove on error
                            match &result {
                                Ok(confirmation) => {
                                    info!(
                                        blob_id = %blob_id,
                                        "Processing successful - caching confirmation for reuse"
                                    );
                                    cache.insert(
                                        blob_id,
                                        CachedBlobState::Completed {
                                            confirmation: confirmation.clone(),
                                            completed_at: Instant::now(),
                                        },
                                    );
                                }
                                Err(e) => {
                                    warn!(
                                        blob_id = %blob_id,
                                        error = %e,
                                        "Processing failed - removing from cache"
                                    );
                                    cache.remove(&blob_id);
                                }
                            }

                            let _ = result_sender.send(result);
                        });
                    }
                    CachedBlobState::Completed { confirmation, .. } => {
                        info!(
                            blob_id = %blob_id,
                            "Blob already processed successfully - returning cached confirmation"
                        );
                        let _ = result_sender.send(Ok(confirmation.clone()));
                    }
                    CachedBlobState::DataOnly { .. } => {
                        warn!(
                            blob_id = %blob_id,
                            "Duplicate blob data received - data already cached"
                        );
                    }
                    CachedBlobState::Processing => {
                        warn!(
                            blob_id = %blob_id,
                            "Duplicate blob data received - already processing"
                        );
                    }
                }
            }
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                info!(
                    blob_id = %blob_id,
                    "First to arrive - caching blob data and waiting for registration"
                );

                // First to arrive, store data with the sender
                vacant.insert(CachedBlobState::DataOnly {
                    data: data.clone(),
                    arrived_at: Instant::now(),
                    result_sender,
                });
            }
        }

        // Return a future that waits for the result
        Ok(async move {
            result_receiver
                .await
                .unwrap_or_else(|_| Err(anyhow::anyhow!("Processing task was cancelled")))
        })
    }

    /// Marks a blob as registered from the event processor.
    /// If data has already arrived, triggers processing.
    pub async fn register(&self, blob_id: BlobId) {
        info!(
            blob_id = %blob_id,
            "CachedBlobManager::register called - blob registration event received"
        );

        // Check if we're shutting down
        if self.shutdown_token.is_cancelled() {
            info!(
                blob_id = %blob_id,
                "Ignoring registration during shutdown"
            );
            return;
        }
        let mut should_process = false;
        let mut data_to_process = None;
        let mut sender_to_use = None;

        // Use entry API for atomic operation
        self.cache
            .entry(blob_id)
            .and_modify(|state| {
                match state {
                    CachedBlobState::DataOnly { data, result_sender, .. } => {
                        info!(
                            blob_id = %blob_id,
                            "Blob data already cached - triggering processing \
                             (registration arrived second)"
                        );

                        // Data already arrived, transition to processing
                        data_to_process = Some(data.clone());
                        // Take ownership of the sender
                        sender_to_use = Some(std::mem::replace(result_sender, {
                            let (dummy_sender, _) = oneshot::channel();
                            dummy_sender
                        }));
                        *state = CachedBlobState::Processing;
                        should_process = true;
                    }
                    CachedBlobState::RegisteredOnly { .. } => {
                        warn!(
                            blob_id = %blob_id,
                            "Duplicate registration call - registration already received"
                        );
                    }
                    CachedBlobState::Processing => {
                        info!(
                            blob_id = %blob_id,
                            "Registration received while blob is already processing"
                        );
                    }
                    CachedBlobState::Completed { .. } => {
                        info!(
                            blob_id = %blob_id,
                            "Registration received for already completed blob - no action needed"
                        );
                    }
                }
            })
            .or_insert_with(|| {
                info!(
                    blob_id = %blob_id,
                    "First to arrive - marking blob as registered and waiting for data"
                );

                // First to arrive, mark as registered
                CachedBlobState::RegisteredOnly {
                    registered_at: Instant::now(),
                }
            });

        // Process if data was already there
        if should_process {
            if let (Some(data), Some(sender)) = (data_to_process, sender_to_use) {
                info!(
                    blob_id = %blob_id,
                    "Both data and registration available - spawning background processing task"
                );

                let node = self.node.clone();
                let cache = self.cache.clone();

                // Spawn processing task
                let shutdown_token = self.shutdown_token.clone();
                tokio::spawn(async move {
                    // Check if we're shutting down before processing
                    if shutdown_token.is_cancelled() {
                        info!(
                            blob_id = %blob_id,
                            "Aborting blob processing due to shutdown during register path"
                        );
                        let _ =
                            sender.send(Err(anyhow::anyhow!("CachedBlobManager is shutting down")));
                        return;
                    }

                    info!(
                        blob_id = %blob_id,
                        "Starting blob processing from registration event"
                    );
                    let result = Self::process_blob(&node, blob_id, data).await;

                    // Handle result - cache success, remove on error
                    match &result {
                        Ok(confirmation) => {
                            info!(
                                blob_id = %blob_id,
                                "Processing successful - caching confirmation for reuse"
                            );
                            cache.insert(
                                blob_id,
                                CachedBlobState::Completed {
                                    confirmation: confirmation.clone(),
                                    completed_at: Instant::now(),
                                },
                            );
                        }
                        Err(e) => {
                            warn!(
                                blob_id = %blob_id,
                                error = %e,
                                "Processing failed - removing from cache"
                            );
                            cache.remove(&blob_id);
                        }
                    }

                    // Send result through the channel
                    let _ = sender.send(result);
                });
            } else {
                warn!(
                    blob_id = %blob_id,
                    "Processing triggered but missing data or sender - internal error"
                );
            }
        }
    }

    /// Processes a blob by storing metadata and slivers, then computing the confirmation
    /// certificate.
    async fn process_blob(
        node: &Arc<StorageNodeInner>,
        blob_id: BlobId,
        data: CachedBlobData,
    ) -> anyhow::Result<SignedMessage<Confirmation>> {
        info!(
            blob_id = %blob_id,
            "CachedBlobManager::process_blob starting with {} sliver pairs",
            data.pairs.len()
        );

        // Store metadata first
        info!(
            blob_id = %blob_id,
            "Step 1: Storing blob metadata"
        );

        let unverified_metadata =
            UnverifiedBlobMetadataWithId::new(blob_id, data.metadata.metadata().clone());

        let metadata_stored = node
            .store_metadata(unverified_metadata)
            .await
            .map_err(|e| {
                warn!(
                    blob_id = %blob_id,
                    error = %e,
                    "Failed to store metadata"
                );
                anyhow::anyhow!("Failed to store metadata: {}", e)
            })?;

        info!(
            blob_id = %blob_id,
            metadata_new = metadata_stored,
            "Step 1 complete: Metadata stored successfully"
        );

        // Store all slivers in parallel
        info!(
            blob_id = %blob_id,
            "Step 2: Storing {} sliver pairs ({} total slivers) in parallel",
            data.pairs.len(),
            data.pairs.len() * 2
        );

        let sliver_futures = data.pairs.iter().flat_map(|pair| {
            let primary_future =
                node.store_sliver(blob_id, pair.index(), ByAxis::Primary(pair.primary.clone()));
            let secondary_future = node.store_sliver(
                blob_id,
                pair.index(),
                ByAxis::Secondary(pair.secondary.clone()),
            );
            vec![primary_future, secondary_future]
        });

        let sliver_results = join_all(sliver_futures).await;

        // Check if all slivers were stored successfully
        let mut successful_slivers = 0;
        for (idx, result) in sliver_results.iter().enumerate() {
            if let Err(e) = result {
                warn!(
                    blob_id = %blob_id,
                    sliver_index = idx,
                    error = %e,
                    "Failed to store sliver"
                );
                return Err(anyhow::anyhow!(
                    "Failed to store sliver {} for blob {}: {}",
                    idx,
                    blob_id,
                    e
                ));
            } else {
                successful_slivers += 1;
            }
        }

        info!(
            blob_id = %blob_id,
            "Step 2 complete: All {} slivers stored successfully",
            successful_slivers
        );

        // Compute storage confirmation
        info!(
            blob_id = %blob_id,
            "Step 3: Computing storage confirmation certificate"
        );

        let confirmation = node
            .compute_storage_confirmation(&blob_id, &data.persistence_type)
            .await
            .map_err(|e| {
                warn!(
                    blob_id = %blob_id,
                    error = %e,
                    "Failed to compute storage confirmation"
                );
                anyhow::anyhow!("Failed to compute confirmation: {}", e)
            })?;

        // Convert StorageConfirmation to SignedMessage<Confirmation>
        match confirmation {
            StorageConfirmation::Signed(signed_msg) => {
                info!(
                    blob_id = %blob_id,
                    "Step 3 complete: Storage confirmation certificate generated successfully"
                );
                info!(
                    blob_id = %blob_id,
                    "CachedBlobManager::process_blob completed successfully - blob fully processed"
                );
                Ok(signed_msg)
            }
        }
    }

    /// Cleans up stale entries that have been waiting too long.
    pub async fn cleanup_stale_entries(&self, timeout: Duration) {
        let now = Instant::now();
        let mut to_remove = Vec::new();

        for entry in self.cache.iter() {
            let should_remove = match entry.value() {
                CachedBlobState::DataOnly { arrived_at, .. } => {
                    now.duration_since(*arrived_at) > timeout
                }
                CachedBlobState::RegisteredOnly { registered_at, .. } => {
                    now.duration_since(*registered_at) > timeout
                }
                CachedBlobState::Processing => false,
                CachedBlobState::Completed { completed_at, .. } => {
                    // Keep completed entries for a reasonable time for reuse
                    now.duration_since(*completed_at) > timeout * 2
                }
            };

            if should_remove {
                to_remove.push(*entry.key());
            }
        }

        for blob_id in to_remove {
            warn!(
                blob_id = %blob_id,
                "Removing stale cached blob entry"
            );
            // For DataOnly state, we should notify the waiting future
            if let Some(entry) = self.cache.remove(&blob_id) {
                if let CachedBlobState::DataOnly { result_sender, .. } = entry.1 {
                    let _ = result_sender.send(Err(anyhow::anyhow!("Blob registration timed out")));
                }
            }
        }
    }

    /// Returns the number of cached entries.
    pub fn cache_size(&self) -> usize {
        self.cache.len()
    }

    /// Returns statistics about the cache state.
    /// Returns (data_only, registered_only, processing, completed)
    pub fn cache_stats(&self) -> (usize, usize, usize, usize) {
        let mut data_only = 0;
        let mut registered_only = 0;
        let mut processing = 0;
        let mut completed = 0;

        for entry in self.cache.iter() {
            match entry.value() {
                CachedBlobState::DataOnly { .. } => data_only += 1,
                CachedBlobState::RegisteredOnly { .. } => registered_only += 1,
                CachedBlobState::Processing => processing += 1,
                CachedBlobState::Completed { .. } => completed += 1,
            }
        }

        (data_only, registered_only, processing, completed)
    }
}
