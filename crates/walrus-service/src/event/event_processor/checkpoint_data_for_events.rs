// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Alternative checkpoint data structure for event processing that doesn't require
//! deserializing Transaction and TransactionEffects objects.

use sui_types::{
    base_types::TransactionDigest,
    effects::TransactionEvents,
    messages_checkpoint::{CertifiedCheckpointSummary, CheckpointContents},
    object::Object,
};

/// Checkpoint data optimized for event processing.
///
/// This structure contains only the fields needed for extracting events:
/// - Checkpoint summary (for verification)
/// - Checkpoint contents (for transaction digests)
/// - Events per transaction
/// - Output objects per transaction (for package store updates)
///
/// Key difference from CheckpointData: Does NOT contain Transaction or TransactionEffects
/// objects, making it resilient to BCS deserialization failures when those types change.
#[derive(Debug, Clone)]
pub struct CheckpointDataForEvents {
    /// The certified checkpoint summary.
    pub checkpoint_summary: CertifiedCheckpointSummary,
    /// The checkpoint contents (contains all transaction digests).
    pub checkpoint_contents: CheckpointContents,
    /// Transaction data for event processing (indexed to match checkpoint_contents).
    pub transactions: Vec<TransactionDataForEvents>,
}

/// Transaction data needed for event processing.
///
/// Contains only events and output objects, avoiding Transaction and TransactionEffects
/// deserialization.
#[derive(Debug, Clone)]
pub struct TransactionDataForEvents {
    /// Transaction events (if any).
    pub events: Option<TransactionEvents>,
    /// Output objects from the transaction.
    pub output_objects: Vec<Object>,
}

impl CheckpointDataForEvents {
    /// Gets the transaction digest for a given transaction index.
    ///
    /// This uses the pre-computed digest from CheckpointContents, avoiding
    /// the need to deserialize the Transaction object.
    pub fn transaction_digest(&self, index: usize) -> Option<TransactionDigest> {
        self.checkpoint_contents
            .iter()
            .nth(index)
            .map(|exec_digest| exec_digest.transaction)
    }

    /// Returns the number of transactions in this checkpoint.
    pub fn num_transactions(&self) -> usize {
        self.checkpoint_contents.size()
    }
}
