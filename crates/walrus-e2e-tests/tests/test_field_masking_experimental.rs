// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// Allowing `unwrap`s in tests.
#![allow(clippy::unwrap_used)]

//! E2E test for experimental field masking checkpoint fetching.

use std::time::Duration;

use anyhow::Context;
use walrus_service::test_utils::{TestNodesConfig, test_cluster};
use walrus_sui::client::retry_client::RetriableRpcClient;

#[tokio::test]
#[ignore = "ignore E2E tests by default"]
async fn test_field_masked_checkpoint_fetching() -> anyhow::Result<()> {
    let (_sui_cluster, _cluster, client, _) = test_cluster::E2eTestSetupBuilder::new()
        .with_test_nodes_config(TestNodesConfig {
            node_weights: vec![2, 2],
            use_legacy_event_processor: false,
            ..Default::default()
        })
        .build()
        .await?;

    // Wait for some checkpoints to be created.
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Get the RPC client from the test setup.
    let rpc_client = client.inner.sui_client().read_client.clone();

    // Get the latest checkpoint number.
    let latest_checkpoint = rpc_client
        .get_latest_checkpoint()
        .await
        .context("failed to get latest checkpoint")?;

    tracing::info!(
        checkpoint = latest_checkpoint.sequence_number,
        "testing field-masked checkpoint fetch"
    );

    // Test 1: Fetch checkpoint with field masking (experimental).
    let checkpoint_for_events = rpc_client
        .get_checkpoint_for_events_experimental(latest_checkpoint.sequence_number)
        .await
        .context("failed to fetch checkpoint with field masking")?;

    // Test 2: Fetch the same checkpoint with standard method for comparison.
    let full_checkpoint = rpc_client
        .get_full_checkpoint(latest_checkpoint.sequence_number)
        .await
        .context("failed to fetch full checkpoint")?;

    // Verify that both methods get the same basic data.
    assert_eq!(
        checkpoint_for_events.checkpoint_summary.sequence_number(),
        full_checkpoint.checkpoint_summary.sequence_number(),
        "checkpoint sequence numbers should match"
    );

    assert_eq!(
        checkpoint_for_events.checkpoint_summary.content_digest,
        full_checkpoint.checkpoint_summary.content_digest,
        "content digests should match"
    );

    assert_eq!(
        checkpoint_for_events.transactions.len(),
        full_checkpoint.transactions.len(),
        "number of transactions should match"
    );

    // Verify transaction counts and events.
    for (i, (field_masked_tx, full_tx)) in checkpoint_for_events
        .transactions
        .iter()
        .zip(full_checkpoint.transactions.iter())
        .enumerate()
    {
        // Check that events match.
        match (&field_masked_tx.events, &full_tx.events) {
            (Some(field_masked_events), Some(full_events)) => {
                assert_eq!(
                    field_masked_events.data.len(),
                    full_events.data.len(),
                    "transaction {} should have same number of events",
                    i
                );
            }
            (None, None) => {
                // Both have no events - this is fine.
            }
            _ => {
                panic!(
                    "transaction {} event presence mismatch: \
                     field_masked={}, full={}",
                    i,
                    field_masked_tx.events.is_some(),
                    full_tx.events.is_some()
                );
            }
        }

        // Check that output objects match in count.
        assert_eq!(
            field_masked_tx.output_objects.len(),
            full_tx.output_objects.len(),
            "transaction {} should have same number of output objects",
            i
        );
    }

    tracing::info!(
        checkpoint = latest_checkpoint.sequence_number,
        num_transactions = checkpoint_for_events.transactions.len(),
        "field-masked checkpoint fetch successful and matches full checkpoint"
    );

    Ok(())
}
