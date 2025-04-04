// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Contains event blob related simtests.

#![recursion_limit = "256"]

#[cfg(msim)]
mod tests {
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };

    use sui_macros::{clear_fail_point, register_fail_point_if};
    use sui_rpc_api::Client as RpcClient;
    use tokio::sync::RwLock;
    use walrus_core::test_utils;
    use walrus_proc_macros::walrus_simtest;
    use walrus_service::{
        client::{Client, ClientCommunicationConfig},
        test_utils::{test_cluster, SimStorageNodeHandle, TestCluster, TestNodesConfig},
    };
    use walrus_simtest::test_utils::{simtest_utils, simtest_utils::BlobInfoConsistencyCheck};
    use walrus_sui::{
        client::{ReadClient, SuiContractClient},
        types::move_structs::EventBlob,
    };
    use walrus_test_utils::WithTempDir;

    async fn wait_for_certification_stuck(
        client: &Arc<WithTempDir<Client<SuiContractClient>>>,
    ) -> EventBlob {
        let start = Instant::now();
        let mut last_blob_time = Instant::now();
        let mut last_blob = EventBlob {
            blob_id: test_utils::random_blob_id(),
            ending_checkpoint_sequence_number: 0,
        };

        loop {
            let current_blob = get_last_certified_event_blob_must_succeed(client).await;

            if current_blob.blob_id != last_blob.blob_id {
                tracing::info!("New event blob seen during fork wait: {:?}", current_blob);
                last_blob = current_blob;
                last_blob_time = Instant::now();
            }

            if last_blob_time.elapsed() > Duration::from_secs(20) {
                tracing::info!("Event blob certification stuck for 20s");
                break;
            }

            if start.elapsed() > Duration::from_secs(180) {
                panic!("Timeout waiting for event blob to get stuck");
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        last_blob
    }

    /// Gets the last certified event blob from the client.
    /// Returns the last certified event blob if it exists, otherwise panics.
    async fn get_last_certified_event_blob_must_succeed(
        client: &Arc<WithTempDir<Client<SuiContractClient>>>,
    ) -> EventBlob {
        const TIMEOUT: Duration = Duration::from_secs(10);
        let start = Instant::now();

        while start.elapsed() <= TIMEOUT {
            if let Some(blob) = client
                .inner
                .sui_client()
                .read_client
                .last_certified_event_blob()
                .await
                .unwrap()
            {
                return blob;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        panic!("Timeout waiting for last certified event blob");
    }

    /// Kills all the storage nodes in the cluster.
    async fn kill_all_storage_nodes(node_ids: &[sui_simulator::task::NodeId]) {
        let handle = sui_simulator::runtime::Handle::current();
        for &node_id in node_ids {
            handle.delete_node(node_id);
        }
    }

    async fn restart_nodes_with_checkpoints(
        walrus_cluster: &mut TestCluster<SimStorageNodeHandle>,
        checkpoint_fn: impl Fn(usize) -> u32,
    ) {
        let node_handles = walrus_cluster
            .nodes
            .iter()
            .map(|n| n.node_id.expect("simtest must set node id"))
            .collect::<Vec<_>>();

        kill_all_storage_nodes(&node_handles).await;

        for (i, node) in walrus_cluster.nodes.iter_mut().enumerate() {
            node.node_id = Some(
                SimStorageNodeHandle::spawn_node(
                    Arc::new(RwLock::new(node.storage_node_config.clone())),
                    Some(checkpoint_fn(i)),
                    node.cancel_token.clone(),
                )
                .await
                .id(),
            );
        }
    }

    /// This test verifies that the node can correctly recover from a forked event blob.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_event_blob_fork_recovery() {
        let (_sui_cluster, mut walrus_cluster, client) =
            test_cluster::default_setup_with_num_checkpoints_generic::<SimStorageNodeHandle>(
                Duration::from_secs(15),
                TestNodesConfig {
                    node_weights: vec![2, 2, 3, 3, 3],
                    ..Default::default()
                },
                Some(20),
                ClientCommunicationConfig::default_for_test_with_reqwest_timeout(
                    Duration::from_secs(2),
                ),
                false,
                None,
            )
            .await
            .unwrap();

        let blob_info_consistency_check = BlobInfoConsistencyCheck::new();

        let client = Arc::new(client);

        // Run workload to get some event blobs certified
        tokio::time::sleep(Duration::from_secs(30)).await;

        // Restart nodes with different checkpoint numbers to create fork
        restart_nodes_with_checkpoints(&mut walrus_cluster, |i| 30 + i as u32).await;

        // Wait for event blob certification to get stuck
        let stuck_blob = wait_for_certification_stuck(&client).await;

        // Restart nodes with same checkpoint number to recover
        restart_nodes_with_checkpoints(&mut walrus_cluster, |_| 20).await;

        // Verify recovery
        tokio::time::sleep(Duration::from_secs(30)).await;
        let recovered_blob = get_last_certified_event_blob_must_succeed(&client).await;

        // Event blob should make progress again.
        assert_ne!(stuck_blob.blob_id, recovered_blob.blob_id);

        blob_info_consistency_check.check_storage_node_consistency();
    }

    /// Waits for all nodes to download checkpoints up to the specified sequence number
    async fn wait_for_nodes_at_checkpoint(
        node_refs: &[&SimStorageNodeHandle],
        target_sequence_number: u64,
        timeout: Duration,
    ) -> Result<(), anyhow::Error> {
        let start_time = Instant::now();
        let poll_interval = Duration::from_secs(1);
        let mut nodes_to_check: Vec<&SimStorageNodeHandle> = node_refs.iter().copied().collect();

        tracing::info!(
            "Waiting for all nodes to download checkpoint up to sequence number: {}",
            target_sequence_number
        );

        while !nodes_to_check.is_empty() {
            if start_time.elapsed() > timeout {
                return Err(anyhow::anyhow!(
                    "Timeout waiting for nodes to download checkpoint.",
                ));
            }

            let node_health_infos = simtest_utils::get_nodes_health_info(&nodes_to_check).await;

            let lagging_nodes: Vec<&SimStorageNodeHandle> = node_health_infos
                .iter()
                .zip(nodes_to_check.iter())
                .filter_map(
                    |(info, node)| match info.latest_checkpoint_sequence_number {
                        Some(seq) if seq < target_sequence_number => Some(*node),
                        None => Some(*node),
                        _ => None,
                    },
                )
                .collect();

            nodes_to_check = lagging_nodes;

            tokio::time::sleep(poll_interval).await;
        }

        Ok(())
    }

    /// This test verifies that the node can correctly download checkpoints from
    /// additional fullnodes.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_checkpoint_downloader_with_additional_fullnodes() {
        let (sui_cluster, walrus_cluster, client) =
            test_cluster::default_setup_with_num_checkpoints_generic::<SimStorageNodeHandle>(
                Duration::from_secs(15),
                TestNodesConfig {
                    node_weights: vec![2, 2, 3, 3, 3],
                    ..Default::default()
                },
                Some(20),
                ClientCommunicationConfig::default_for_test_with_reqwest_timeout(
                    Duration::from_secs(2),
                ),
                false,
                Some(4),
            )
            .await
            .unwrap();

        // Register a fail point that will fail the first attempt.
        register_fail_point_if("fallback_client_inject_error", move || true);
        tracing::info!(
            "Additional fullnodes: {:?}",
            sui_cluster.lock().await.additional_rpc_urls()
        );
        let client_arc = Arc::new(client);

        // Wait for the cluster to process some events.
        let workload_handle = simtest_utils::start_background_workload(client_arc.clone(), false);
        tokio::time::sleep(Duration::from_secs(30)).await;

        // Get the latest checkpoint from Sui.
        let rpc_client = RpcClient::new(sui_cluster.lock().await.additional_rpc_urls()[0].clone())
            .expect("Failed to create RPC client");
        let latest_sui_checkpoint = rpc_client
            .get_latest_checkpoint()
            .await
            .expect("Failed to get latest checkpoint from Sui");

        let latest_sui_checkpoint_seq = latest_sui_checkpoint.sequence_number;
        tracing::info!(
            "Latest Sui checkpoint sequence number: {}",
            latest_sui_checkpoint_seq
        );
        workload_handle.abort();

        // Get the highest processed event and checkpoint for each storage node.
        let node_refs: Vec<&SimStorageNodeHandle> = walrus_cluster.nodes.iter().collect();
        let node_health_infos = simtest_utils::get_nodes_health_info(&node_refs).await;

        tracing::info!("Node health infos: {:?}", node_health_infos);

        wait_for_nodes_at_checkpoint(
            &node_refs,
            latest_sui_checkpoint_seq,
            Duration::from_secs(100),
        )
        .await
        .expect("All nodes should have downloaded the checkpoint");

        clear_fail_point("fallback_client_inject_error");
    }
}
