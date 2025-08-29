// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Checkpoint module for processing checkpoint data for indexing.

use std::{
    fmt::{self},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::Result;
use sui_types::{
    base_types::ObjectID,
    event::Event,
    full_checkpoint_content::CheckpointData,
    messages_checkpoint::VerifiedCheckpoint,
};
use typed_store::Map;

use super::db::IndexerEventProcessorStores;
use crate::IndexOperation;

/// A struct that processes checkpoint data for indexing.
#[derive(Clone)]
pub struct IndexerCheckpointProcessor {
    stores: IndexerEventProcessorStores,
    walrus_package_id: ObjectID,
    latest_checkpoint_seq_number: Arc<AtomicU64>,
}

impl fmt::Debug for IndexerCheckpointProcessor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexerCheckpointProcessor")
            .field("stores", &self.stores)
            .field("walrus_package_id", &self.walrus_package_id)
            .finish()
    }
}

impl IndexerCheckpointProcessor {
    /// Creates a new instance of the indexer checkpoint processor.
    pub fn new(stores: IndexerEventProcessorStores, walrus_package_id: ObjectID) -> Self {
        Self {
            stores,
            walrus_package_id,
            latest_checkpoint_seq_number: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Gets the latest checkpoint sequence number, preferring the cache.
    pub fn get_latest_checkpoint_sequence_number(&self) -> Option<u64> {
        let cached = self.latest_checkpoint_seq_number.load(Ordering::Acquire);
        if cached > 0 {
            Some(cached)
        } else {
            // Try to get from storage
            if let Ok(Some(checkpoint)) = self.stores.checkpoint_store.get(&()) {
                let seq_num = *checkpoint.inner().sequence_number();
                self.latest_checkpoint_seq_number
                    .store(seq_num, Ordering::Release);
                Some(seq_num)
            } else {
                None
            }
        }
    }

    /// Updates the cached latest checkpoint sequence number.
    pub fn update_cached_latest_checkpoint_seq_number(&self, seq_num: u64) {
        self.latest_checkpoint_seq_number
            .store(seq_num, Ordering::Release);
    }

    /// Processes checkpoint data and extracts indexing events.
    pub async fn process_checkpoint_data(
        &self,
        checkpoint: CheckpointData,
        verified_checkpoint: VerifiedCheckpoint,
        next_event_index: u64,
    ) -> Result<u64> {
        let checkpoint_seq = *verified_checkpoint.sequence_number();
        tracing::debug!(
            "Processing checkpoint {} for indexing events",
            checkpoint_seq
        );

        let mut event_index = next_event_index;

        // Process all transactions in the checkpoint
        for (tx_index, tx_data) in checkpoint.transactions.iter().enumerate() {
            // Process events from this transaction
            if let Some(ref events) = tx_data.events {
                for (event_index_in_tx, event) in events.data.iter().enumerate() {
                    if let Some(index_operation) =
                        self.extract_index_operation(event, checkpoint_seq)
                    {
                        // Store the processed event
                        let processed_event = super::db::IndexerProcessedEvent {
                            checkpoint: checkpoint_seq,
                            operation: index_operation,
                        };

                        self.stores
                            .processed_events_store
                            .insert(&event_index, &processed_event)?;
                        event_index += 1;

                        tracing::debug!(
                            "Extracted index operation from checkpoint {} tx {} event {}",
                            checkpoint_seq,
                            tx_index,
                            event_index_in_tx
                        );
                    }
                }
            }
        }

        // Update the checkpoint store with the latest processed checkpoint
        self.stores
            .checkpoint_store
            .insert(&(), verified_checkpoint.serializable_ref())?;
        self.update_cached_latest_checkpoint_seq_number(checkpoint_seq);

        Ok(event_index)
    }

    /// Extracts index operation from a Sui event if it's relevant for indexing.
    fn extract_index_operation(&self, event: &Event, _checkpoint: u64) -> Option<IndexOperation> {
        // Check if event is from the Walrus package
        if event.package_id != self.walrus_package_id {
            return None;
        }

        // TODO: Implement actual event parsing based on Walrus package events
        // This is a placeholder that should be replaced with real event parsing logic

        // Example pattern matching on event type:
        /*
        match event.type_.to_canonical_string().as_str() {
            "IndexAdded" => {
                // Parse the event data to extract index information
                // This would require knowledge of the actual Walrus event structure
                Some(IndexOperation::IndexAdded {
                    bucket_id: ObjectID::ZERO, // Parse from event data
                    primary_key: String::new(), // Parse from event data
                    blob_id: BlobId([0; 32]), // Parse from event data
                    secondary_indices: vec![], // Parse from event data
                })
            }
            "IndexRemoved" => {
                Some(IndexOperation::IndexRemoved {
                    bucket_id: ObjectID::ZERO, // Parse from event data
                    primary_key: String::new(), // Parse from event data
                })
            }
            _ => None,
        }
        */

        // For now, return None since we don't have the actual event structure
        None
    }
}
