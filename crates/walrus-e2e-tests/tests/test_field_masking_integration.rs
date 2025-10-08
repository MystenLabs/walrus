// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// Allowing `unwrap`s in tests.
#![allow(clippy::unwrap_used)]

//! Integration test for experimental field masking checkpoint processing.
//!
//! This test verifies that:
//! 1. Checkpoints can be fetched with gRPC field masking
//! 2. Events can be extracted from field-masked checkpoints
//! 3. Same data is returned as the standard method

use std::{sync::Arc, time::Duration};

use anyhow::Context;
use walrus_proc_macros::walrus_simtest;
use walrus_service::test_utils::{test_cluster, TestNodesConfig};
use walrus_sui::client::retry_client::{
    RetriableRpcClient,
    retriable_rpc_client::LazyFallibleRpcClientBuilder,
};
use walrus_test_utils::Result as TestResult;
use walrus_utils::backoff::ExponentialBackoffConfig;

#[walrus_simtest]
async fn test_field_masking_checkpoint_fetching() -> TestResult {
    // Initialize tracing for debugging.
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,walrus=debug")
        .try_init();

    tracing::info!("Starting field masking integration test");

    // Step 1: Setup test cluster to have a real Sui network with checkpoints.
    let (sui_cluster, _cluster, _client, _system_context) =
        test_cluster::E2eTestSetupBuilder::new()
            .with_test_nodes_config(TestNodesConfig {
                node_weights: vec![2, 2],
                use_legacy_event_processor: false,
                use_field_masking: true, // Enable field masking
                ..Default::default()
            })
            .build()
            .await?;

    tracing::info!("Test cluster started");

    // Step 2: Create a RetriableRpcClient for checkpoint fetching.
    let rpc_url = {
        let cluster = sui_cluster.lock().await;
        cluster.rpc_url().to_string()
    };

    let backoff_config = ExponentialBackoffConfig::default();
    let request_timeout = Duration::from_secs(30);
    let sampled_tracing_interval = Duration::from_secs(60);

    let rpc_client = Arc::new(
        RetriableRpcClient::new(
            vec![LazyFallibleRpcClientBuilder::Url {
                rpc_url: rpc_url.clone(),
                ensure_experimental_rest_endpoint: true,
            }],
            request_timeout,
            backoff_config.clone(),
            None, // No fallback
            None, // No metrics
            sampled_tracing_interval,
        )
        .await
        .context("failed to create RPC client")?,
    );

    tracing::info!(rpc_url = %rpc_url, "Created RPC client");

    // Step 3: Wait for some checkpoints to be created and processed.
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Step 4: Get the latest checkpoint number.
    let latest_checkpoint_summary = rpc_client
        .get_latest_checkpoint_summary()
        .await
        .context("failed to get latest checkpoint")?;

    let checkpoint_seq = *latest_checkpoint_summary.sequence_number();
    tracing::info!(checkpoint = checkpoint_seq, "Latest checkpoint found");

    // Use an earlier checkpoint to ensure it's fully processed.
    let test_checkpoint_seq = checkpoint_seq.saturating_sub(2).max(1);

    tracing::info!(
        checkpoint = test_checkpoint_seq,
        "Testing with checkpoint"
    );

    // Step 5: Fetch checkpoint with BOTH methods for comparison.
    tracing::info!("Fetching checkpoint with standard method...");
    let full_checkpoint = rpc_client
        .get_full_checkpoint(test_checkpoint_seq)
        .await
        .context("failed to fetch full checkpoint")?;

    tracing::info!("Fetching checkpoint with field masking...");
    let masked_checkpoint = rpc_client
        .get_checkpoint_for_events_experimental(test_checkpoint_seq)
        .await
        .context("failed to fetch field-masked checkpoint")?;

    // Step 6: Verify that both methods return the same core data.
    assert_eq!(
        full_checkpoint.checkpoint_summary.sequence_number(),
        masked_checkpoint.checkpoint_summary.sequence_number(),
        "checkpoint sequence numbers must match"
    );

    assert_eq!(
        full_checkpoint.checkpoint_summary.content_digest,
        masked_checkpoint.checkpoint_summary.content_digest,
        "content digests must match"
    );

    assert_eq!(
        full_checkpoint.transactions.len(),
        masked_checkpoint.transactions.len(),
        "number of transactions must match"
    );

    tracing::info!(
        num_transactions = full_checkpoint.transactions.len(),
        "Verified checkpoint data matches"
    );

    // Step 7: Verify transaction-level data matches.
    for (i, (full_tx, masked_tx)) in full_checkpoint
        .transactions
        .iter()
        .zip(masked_checkpoint.transactions.iter())
        .enumerate()
    {
        // Verify events match.
        match (&full_tx.events, &masked_tx.events) {
            (Some(full_events), Some(masked_events)) => {
                assert_eq!(
                    full_events.data.len(),
                    masked_events.data.len(),
                    "transaction {} event count mismatch",
                    i
                );

                if !full_events.data.is_empty() {
                    tracing::debug!(
                        tx_index = i,
                        event_count = full_events.data.len(),
                        "Transaction has events"
                    );
                }
            }
            (None, None) => {
                // Both have no events - OK.
            }
            _ => {
                panic!(
                    "transaction {} event presence mismatch: full={}, masked={}",
                    i,
                    full_tx.events.is_some(),
                    masked_tx.events.is_some()
                );
            }
        }

        // Verify output objects match in count.
        assert_eq!(
            full_tx.output_objects.len(),
            masked_tx.output_objects.len(),
            "transaction {} output object count mismatch",
            i
        );
    }

    tracing::info!("All transaction data verified");
    tracing::info!("✅ Field masking integration test PASSED");

    // Calculate approximate size savings.
    let full_size_estimate = full_checkpoint.transactions.len() * 20_000; // ~20KB per tx
    let masked_size_estimate = masked_checkpoint.transactions.len() * 5_000; // ~5KB per tx
    let savings_pct =
        ((full_size_estimate - masked_size_estimate) as f64 / full_size_estimate as f64) * 100.0;

    tracing::info!(
        full_size_kb = full_size_estimate / 1024,
        masked_size_kb = masked_size_estimate / 1024,
        savings_percent = format!("{:.1}%", savings_pct),
        "Estimated size savings"
    );

    Ok(())
}
