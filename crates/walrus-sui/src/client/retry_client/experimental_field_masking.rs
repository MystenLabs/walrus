// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Experimental implementation of field masking for checkpoint fetching.
//!
//! This module demonstrates how to use gRPC field masking to fetch only the necessary
//! fields from checkpoints, avoiding BCS deserialization of Transaction and TransactionEffects.

use sui_types::{
    base_types::TransactionDigest,
    effects::TransactionEvents,
    messages_checkpoint::{CertifiedCheckpointSummary, CheckpointContents},
    object::Object,
};

/// Minimal checkpoint data for event processing.
///
/// Contains only the fields needed for extracting and validating events:
/// - Checkpoint summary and contents (for verification and transaction digests)
/// - Events and output objects per transaction
#[derive(Debug, Clone)]
pub struct CheckpointForEvents {
    pub checkpoint_summary: CertifiedCheckpointSummary,
    pub checkpoint_contents: CheckpointContents,
    pub transactions: Vec<TransactionForEvents>,
}

/// Transaction data needed for event processing.
#[derive(Debug, Clone)]
pub struct TransactionForEvents {
    pub events: Option<TransactionEvents>,
    pub output_objects: Vec<Object>,
}

impl CheckpointForEvents {
    /// Gets the transaction digest for a given transaction index.
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


/// Deserializes CheckpointForEvents from protobuf response.
pub(super) fn deserialize_checkpoint_for_events(
    checkpoint: &sui_rpc_api::proto::sui::rpc::v2beta2::Checkpoint,
) -> anyhow::Result<CheckpointForEvents> {

    // Deserialize checkpoint summary.
    let summary_bcs = checkpoint
        .summary
        .as_ref()
        .and_then(|s| s.bcs.as_ref())
        .and_then(|bcs| bcs.value.as_ref())
        .ok_or_else(|| anyhow::anyhow!("missing summary.bcs"))?;
    let summary = bcs::from_bytes(summary_bcs)
        .map_err(|e| anyhow::anyhow!("failed to deserialize summary: {}", e))?;

    // Deserialize signature.
    let signature_proto = checkpoint
        .signature
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("missing signature"))?;

    // Convert signature using bcs serialization.
    let signature_bcs = bcs::to_bytes(signature_proto)
        .map_err(|e| anyhow::anyhow!("failed to serialize signature: {}", e))?;
    let signature: sui_types::crypto::AuthorityStrongQuorumSignInfo = bcs::from_bytes(&signature_bcs)
        .map_err(|e| anyhow::anyhow!("failed to deserialize signature: {}", e))?;

    let checkpoint_summary =
        CertifiedCheckpointSummary::new_from_data_and_sig(summary, signature);

    // Deserialize checkpoint contents.
    let contents_bcs = checkpoint
        .contents
        .as_ref()
        .and_then(|c| c.bcs.as_ref())
        .and_then(|bcs| bcs.value.as_ref())
        .ok_or_else(|| anyhow::anyhow!("missing contents.bcs"))?;
    let checkpoint_contents: CheckpointContents = bcs::from_bytes(contents_bcs)
        .map_err(|e| anyhow::anyhow!("failed to deserialize contents: {}", e))?;

    // Deserialize transactions (only events and output_objects).
    let transactions = checkpoint
        .transactions
        .iter()
        .map(|tx| -> anyhow::Result<TransactionForEvents> {
            // Deserialize events if present.
            let events = tx
                .events
                .as_ref()
                .and_then(|e| e.bcs.as_ref())
                .and_then(|bcs| bcs.value.as_ref())
                .map(|bytes| {
                    bcs::from_bytes::<TransactionEvents>(bytes)
                        .map_err(|e| anyhow::anyhow!("failed to deserialize events: {}", e))
                })
                .transpose()?;

            // Deserialize output objects.
            let output_objects = tx
                .output_objects
                .iter()
                .map(|obj| {
                    obj.bcs
                        .as_ref()
                        .and_then(|bcs| bcs.value.as_ref())
                        .ok_or_else(|| anyhow::anyhow!("missing object.bcs"))
                        .and_then(|bytes| {
                            bcs::from_bytes::<Object>(bytes)
                                .map_err(|e| anyhow::anyhow!("failed to deserialize object: {}", e))
                        })
                })
                .collect::<anyhow::Result<Vec<_>>>()?;

            Ok(TransactionForEvents {
                events,
                output_objects,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(CheckpointForEvents {
        checkpoint_summary,
        checkpoint_contents,
        transactions,
    })
}
