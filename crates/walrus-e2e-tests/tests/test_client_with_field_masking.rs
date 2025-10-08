// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// Allowing `unwrap`s in tests.
#![allow(clippy::unwrap_used)]

//! Example test showing how to enable field masking in E2E tests.

use walrus_proc_macros::walrus_simtest;
use walrus_service::test_utils::{TestNodesConfig, test_cluster};
use walrus_test_utils::Result as TestResult;

/// Example test showing how to enable field masking for checkpoint fetching.
///
/// This test demonstrates how to configure the test cluster to use experimental
/// field masking, which reduces bandwidth by ~75% and provides immunity to
/// Sui protocol changes in Transaction and TransactionEffects types.
#[walrus_simtest]
async fn test_with_field_masking_enabled() -> TestResult {
    // Configure the test cluster to use field masking
    let (_sui_cluster, _cluster, _client, _) = test_cluster::E2eTestSetupBuilder::new()
        .with_test_nodes_config(TestNodesConfig {
            node_weights: vec![2, 2],
            use_legacy_event_processor: false,
            use_field_masking: true, // ← Enable field masking
            ..Default::default()
        })
        .build()
        .await?;

    // Your test code here - the cluster is now using field masking for
    // checkpoint fetching. Event processing works exactly the same,
    // but with reduced bandwidth and immunity to schema changes.

    Ok(())
}
