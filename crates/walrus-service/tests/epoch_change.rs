// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Contains end-to-end tests for the epoch change mechanism.

use std::time::Duration;

use tokio::time;
use walrus_core::Epoch;
use walrus_proc_macros::walrus_simtest;
use walrus_service::{
    client::ClientCommunicationConfig,
    test_utils::{test_cluster, StorageNodeHandle, TestNodesConfig},
};
use walrus_test_utils::Result as TestResult;

#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn nodes_drive_epoch_change() -> TestResult {
    telemetry_subscribers::init_for_testing();
    let epoch_duration = Duration::from_secs(5);
    let (_sui, storage_nodes, _) =
        test_cluster::default_setup_with_epoch_duration_generic::<StorageNodeHandle>(
            epoch_duration,
            TestNodesConfig {
                node_weights: vec![1, 1],
                use_legacy_event_processor: true,
                disable_event_blob_writer: false,
                blocklist_dir: None,
                enable_node_config_synchronizer: false,
            },
            None,
            ClientCommunicationConfig::default_for_test(),
        )
        .await?;

    let target_epoch: Epoch = 3;
    // Allow thrice the expected time to reach the desired epoch.
    let time_to_reach_epoch = epoch_duration * target_epoch * 3;

    time::timeout(
        time_to_reach_epoch,
        storage_nodes.wait_for_nodes_to_reach_epoch(target_epoch),
    )
    .await
    .expect("target epoch much be reached in allotted time");

    Ok(())
}
