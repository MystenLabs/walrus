// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Contains end-to-end tests for the epoch change mechanism.

#[ignore = "ignore E2E tests by default"]
#[cfg(msim)]
#[walrus_proc_macros::walrus_simtest]
async fn nodes_drive_epoch_change() -> walrus_test_utils::Result {
    use std::time::Duration;

    use tokio::time;
    use walrus_core::Epoch;
    use walrus_service::{
        client::ClientCommunicationConfig,
        test_utils::{test_cluster, StorageNodeHandle, StorageNodeHandleTrait, TestNodesConfig},
    };

    telemetry_subscribers::init_for_testing();
    let epoch_duration = Duration::from_secs(5);
    let (_sui, storage_nodes, _) =
        test_cluster::default_setup_with_num_checkpoints_generic::<StorageNodeHandle>(
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
            false,
            None,
        )
        .await?;

    let target_epoch: Epoch = 3;
    // Allow five times the expected time to reach the desired epoch.
    let time_to_reach_epoch = epoch_duration * target_epoch * 5;

    time::timeout(
        time_to_reach_epoch,
        storage_nodes.wait_for_nodes_to_reach_epoch(target_epoch),
    )
    .await
    .expect("target epoch much be reached in allotted time");

    // Shut the nodes down gracefully to prevent panics when event handles are dropped.
    storage_nodes.nodes.iter().for_each(|node| node.cancel());
    tokio::time::sleep(Duration::from_secs(1)).await;

    Ok(())
}
