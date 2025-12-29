// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Simtests focused on pending upload behavior and follow-up retries.

#![recursion_limit = "256"]

#[cfg(msim)]
mod tests {
    use std::time::Duration;

    use rand::Rng;
    use sui_simulator::task::NodeId;
    use tokio::task::JoinHandle;
    use walrus_core::encoding::Primary;
    use walrus_proc_macros::walrus_simtest;
    use walrus_sdk::{
        node_client::{StoreArgs, StoreBlobsApi as _},
        store_optimizations::StoreOptimizations,
        uploader::TailHandling,
    };
    use walrus_service::{
        client::ClientCommunicationConfig,
        node::config::{PendingMetadataCacheConfig, PendingSliverCacheConfig},
        test_utils::{SimStorageNodeHandle, TestCluster, TestNodesConfig, test_cluster},
    };
    use walrus_simtest::test_utils::simtest_utils;
    use walrus_sui::client::BlobPersistence;

    #[walrus_simtest]
    async fn pending_uploads_retry_until_quorum() {
        walrus_test_utils::init_tracing();

        let mut communication_config =
            ClientCommunicationConfig::default_for_test_with_reqwest_timeout(Duration::from_secs(
                1,
            ));
        communication_config
            .request_rate_config
            .backoff_config
            .max_retries = Some(3);
        communication_config
            .request_rate_config
            .backoff_config
            .min_backoff = Duration::from_millis(50);
        communication_config
            .request_rate_config
            .backoff_config
            .max_backoff = Duration::from_millis(100);

        let (_sui_cluster, mut walrus_cluster, client, _, _) =
            test_cluster::E2eTestSetupBuilder::new()
                .with_test_nodes_config(
                    TestNodesConfig::builder()
                        .with_node_weights(&[1, 1, 1, 1])
                        .build(),
                )
                .with_communication_config(communication_config)
                .build_generic::<SimStorageNodeHandle>()
                .await
                .unwrap();

        // Force pending uploads to fail for slivers so the client must follow up with
        // the immediate intent path.
        shrink_pending_caches(&mut walrus_cluster).await;
        simtest_utils::restart_nodes_with_checkpoints(&mut walrus_cluster, |_| 20).await;

        // Take two nodes down briefly so the initial pending + immediate attempts cannot hit a
        // quorum and the client has to retry when confirmations are missing.
        let restart_handles =
            crash_nodes(&mut walrus_cluster, &[0, 1], Duration::from_millis(1_500));

        let blob = walrus_test_utils::random_data(512);
        let store_args = StoreArgs::default_with_epochs(1)
            .with_store_optimizations(StoreOptimizations::all().with_optimistic_uploads(true))
            .with_persistence(BlobPersistence::Deletable)
            .with_tail_handling(TailHandling::Blocking);

        // This test relies on the retry loop/backoff spanning the downtime; if the nodes are
        // still down when retries are exhausted, this call fails with NotEnoughConfirmations.
        let store_results = client
            .inner
            .reserve_and_store_blobs_retry_committees(vec![blob.clone()], vec![], &store_args)
            .await
            .expect("client should recover when pending uploads cannot reach quorum");

        // Wait for restarts to finish so cluster node IDs are valid before the read below.
        apply_restarts(&mut walrus_cluster, restart_handles).await;

        let blob_id = match store_results.first().expect("one blob result expected") {
            walrus_sdk::node_client::responses::BlobStoreResult::NewlyCreated {
                blob_object,
                ..
            } => blob_object.blob_id,
            other => panic!("unexpected store result: {other:?}"),
        };

        let round_tripped = client
            .inner
            .read_blob::<Primary>(&blob_id)
            .await
            .expect("blob should be readable after follow-up uploads");
        assert_eq!(round_tripped, blob);
    }

    async fn shrink_pending_caches(cluster: &mut TestCluster<SimStorageNodeHandle>) {
        for node in &mut cluster.nodes {
            if rand::thread_rng().gen_bool(0.5) {
                continue;
            }
            node.storage_node_config.pending_sliver_cache = PendingSliverCacheConfig {
                max_cached_slivers: 1,
                max_cached_bytes: 1,
                max_cached_sliver_bytes: 1,
                cache_ttl: Duration::from_secs(1),
            };
            node.storage_node_config.pending_metadata_cache = PendingMetadataCacheConfig {
                max_cached_entries: 1,
                cache_ttl: Duration::from_secs(1),
            };

            let mut config = node.node_config_arc.write().await;
            config.pending_sliver_cache = node.storage_node_config.pending_sliver_cache.clone();
            config.pending_metadata_cache = node.storage_node_config.pending_metadata_cache.clone();
        }
    }

    fn crash_nodes(
        cluster: &mut TestCluster<SimStorageNodeHandle>,
        node_indices: &[usize],
        downtime: Duration,
    ) -> Vec<(usize, JoinHandle<NodeId>)> {
        let handle = sui_simulator::runtime::Handle::current();
        node_indices
            .iter()
            .map(|&idx| {
                if let Some(node_id) = cluster.nodes[idx].node_id.take() {
                    handle.delete_node(node_id);
                }
                let cancel_token = cluster.nodes[idx].cancel_token.clone();
                let config = cluster.nodes[idx].node_config_arc.clone();

                let join = tokio::spawn(async move {
                    tokio::time::sleep(downtime).await;
                    SimStorageNodeHandle::spawn_node(config, None, cancel_token)
                        .await
                        .id()
                });
                (idx, join)
            })
            .collect()
    }

    async fn apply_restarts(
        cluster: &mut TestCluster<SimStorageNodeHandle>,
        handles: Vec<(usize, JoinHandle<NodeId>)>,
    ) {
        for (idx, join) in handles {
            let new_id = join.await.expect("node restart task should succeed");
            cluster.nodes[idx].node_id = Some(new_id);
        }
        // Give restarted nodes a moment to finish bootstrapping before the read.
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
