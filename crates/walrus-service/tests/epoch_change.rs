// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use tokio::time;
use walrus_core::Epoch;
use walrus_service::test_utils::test_cluster;
use walrus_test_utils::Result as TestResult;

#[ignore = "ignore E2E tests by default"]
#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn nodes_drive_epoch_change() -> TestResult {
    let epoch_duration = Duration::from_secs(10);
    let (_sui, storage_nodes, _) =
        test_cluster::default_setup_with_epoch_duration(epoch_duration).await?;

    let target_epoch: Epoch = 4;
    // Allow time to reach the desired epoch, with an additional 20% for the jitter.
    let time_to_reach_epoch = epoch_duration * target_epoch * 12 / 10;

    time::timeout(
        time_to_reach_epoch,
        storage_nodes.wait_for_nodes_to_reach_epoch(target_epoch),
    )
    .await
    .expect("target epoch much be reached in allotted time");

    Ok(())
}
