// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Contains simtests for the cross-node determinism of blob info snapshots.

#![recursion_limit = "256"]

#[cfg(msim)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use walrus_proc_macros::walrus_simtest;
    use walrus_service::test_utils::{SimStorageNodeHandle, TestNodesConfig, test_cluster};
    use walrus_simtest::test_utils::simtest_utils::{self, BlobInfoConsistencyCheck};

    /// Checks that all nodes serialize identical blob info snapshots at each epoch boundary.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_blob_info_snapshot_digests_match_across_nodes() {
        let blob_info_consistency_check = BlobInfoConsistencyCheck::new();

        let (_sui_cluster, _walrus_cluster, client, _, _) =
            test_cluster::E2eTestSetupBuilder::new()
                .with_epoch_duration(Duration::from_secs(30))
                .with_test_nodes_config(
                    TestNodesConfig::builder()
                        .with_node_weights(&[1, 2, 3, 3, 4])
                        .build(),
                )
                .build_generic::<SimStorageNodeHandle>()
                .await
                .unwrap();

        let workload_handle =
            simtest_utils::start_background_workload(Arc::new(client), false, None, None);

        // Let several epoch boundaries pass so that multiple snapshots are produced.
        tokio::time::sleep(Duration::from_mins(3)).await;

        workload_handle.abort();

        blob_info_consistency_check.check_storage_node_consistency();
    }
}
