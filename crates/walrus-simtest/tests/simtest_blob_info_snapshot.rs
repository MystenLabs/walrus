// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Contains simtests for the cross-node determinism of blob info snapshots.

#![recursion_limit = "256"]

#[cfg(msim)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use walrus_core::EpochCount;
    use walrus_proc_macros::walrus_simtest;
    use walrus_service::test_utils::{SimStorageNodeHandle, TestNodesConfig, test_cluster};
    use walrus_simtest::test_utils::simtest_utils::{self, BlobInfoConsistencyCheck};

    /// Lifetime of the storage pool backing the pooled workload.
    const POOL_EPOCHS: EpochCount = 30;
    /// Lifetime requested for each pooled blob. The pooled store does not extend the pool.
    const POOLED_BLOB_EPOCHS: EpochCount = 1;

    /// Checks that all nodes serialize identical blob info snapshots at each epoch boundary,
    /// with both owned and pooled blobs in them.
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

        let client_arc = Arc::new(client);
        let pool_id = client_arc
            .inner
            .sui_client()
            .create_storage_pool(64 * 1024 * 1024, POOL_EPOCHS)
            .await
            .expect("creating the storage pool should succeed");

        let workload_handle =
            simtest_utils::start_background_workload(client_arc.clone(), false, None, None);
        let pooled_workload_handle = simtest_utils::start_pooled_background_workload(
            client_arc.clone(),
            pool_id,
            POOLED_BLOB_EPOCHS,
        );

        // Let several epoch boundaries pass so that multiple snapshots are produced.
        tokio::time::sleep(Duration::from_mins(3)).await;

        workload_handle.abort();
        pooled_workload_handle.abort();

        blob_info_consistency_check.check_storage_node_consistency();
    }
}
