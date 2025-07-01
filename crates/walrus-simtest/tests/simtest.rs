// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Contains dedicated Walrus simulation tests.
#![recursion_limit = "256"]

#[cfg(msim)]
mod tests {
    use std::{
        collections::HashSet,
        sync::{Arc, atomic::AtomicBool},
        time::Duration,
    };

    use rand::{Rng, SeedableRng};
    use sui_macros::{
        clear_fail_point,
        register_fail_point,
        register_fail_point_async,
        register_fail_points,
    };
    use sui_protocol_config::ProtocolConfig;
    use sui_simulator::configs::{env_config, uniform_latency_ms};
    use tokio::sync::RwLock;
    use walrus_proc_macros::walrus_simtest;
    use walrus_service::{
        client::ClientCommunicationConfig,
        node::config::NodeRecoveryConfig,
        test_utils::{SimStorageNodeHandle, TestNodesConfig, test_cluster},
    };
    use walrus_simtest::test_utils::simtest_utils::{
        self,
        BlobInfoConsistencyCheck,
        CRASH_NODE_FAIL_POINTS,
    };
    use walrus_storage_node_client::api::ShardStatus;
    use walrus_sui::client::ReadClient;

    /// Returns a simulator configuration that adds random network latency between nodes.
    ///
    /// The latency is uniformly distributed for all RPCs between nodes.
    /// This simulates real-world network conditions where requests arrive at different nodes
    /// with varying delays. The random latency helps test the system's behavior when events
    /// and messages arrive asynchronously and in different orders at different nodes.
    ///
    /// For example, when a node sends a state update, some nodes may receive and process it
    /// quickly while others experience delay. This creates race conditions and helps verify
    /// that the system remains consistent despite message reordering.
    ///
    /// This latency applies to both Sui cluster and Walrus cluster.
    fn latency_config() -> sui_simulator::SimConfig {
        env_config(uniform_latency_ms(5..15), [])
    }

    // Tests that we can create a Walrus cluster with a Sui cluster and run basic
    // operations deterministically.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest(check_determinism)]
    async fn walrus_basic_determinism() {
        let _guard = ProtocolConfig::apply_overrides_for_testing(|_, mut config| {
            // TODO: remove once Sui simtest can work with these features.
            config.set_enable_jwk_consensus_updates_for_testing(false);
            config.set_random_beacon_for_testing(false);
            config
        });

        let blob_info_consistency_check = BlobInfoConsistencyCheck::new();

        let (_sui_cluster, _cluster, client, _) = test_cluster::E2eTestSetupBuilder::new()
            .with_test_nodes_config(TestNodesConfig {
                node_weights: vec![1, 2, 3, 3, 4],
                ..Default::default()
            })
            .build_generic::<SimStorageNodeHandle>()
            .await
            .unwrap();

        let mut blobs_written = HashSet::new();
        simtest_utils::write_read_and_check_random_blob(
            &client,
            31415,
            false,
            false,
            &mut blobs_written,
            0,
        )
        .await
        .expect("workload should not fail");

        loop {
            if let Some(_blob) = client
                .inner
                .sui_client()
                .read_client
                .last_certified_event_blob()
                .await
                .unwrap()
            {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        blob_info_consistency_check.check_storage_node_consistency();
    }

    // This test simulates a scenario where a node is repeatedly moving shards among storage nodes,
    // and a workload is running concurrently.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest(config = "latency_config()")]
    async fn test_repeated_shard_move_with_workload() {
        const MAX_NODE_WEIGHT: u16 = 6;

        // Adding jitter in the epoch change start event so that different nodes don't start the
        // epoch change at the exact same time.
        register_fail_point_async("epoch_change_start_entry", || async move {
            tokio::time::sleep(Duration::from_millis(
                rand::rngs::StdRng::from_entropy().gen_range(0..=100),
            ))
            .await;
        });

        let blob_info_consistency_check = BlobInfoConsistencyCheck::new();

        // We use a very short epoch duration of 60 seconds so that we can exercise more epoch
        // changes in the test.
        let mut node_weights = vec![2, 2, 3, 3, 3];
        let (_sui_cluster, walrus_cluster, client, _) = test_cluster::E2eTestSetupBuilder::new()
            .with_epoch_duration(Duration::from_secs(30))
            .with_test_nodes_config(TestNodesConfig {
                node_weights: node_weights.clone(),
                use_legacy_event_processor: false,
                ..Default::default()
            })
            .with_communication_config(
                ClientCommunicationConfig::default_for_test_with_reqwest_timeout(
                    Duration::from_secs(2),
                ),
            )
            .build_generic::<SimStorageNodeHandle>()
            .await
            .unwrap();

        let client_arc = Arc::new(client);
        let workload_handle = simtest_utils::start_background_workload(client_arc.clone(), true, 0);

        // Run the workload for 60 seconds to get some data in the system.
        tokio::time::sleep(Duration::from_secs(60)).await;

        // Repeatedly move shards among storage nodes.
        for _i in 0..3 {
            let (node_to_move_shard_into, shard_move_weight) = loop {
                let node_to_move_shard_into = rand::thread_rng().gen_range(0..=4);
                let shard_move_weight = rand::thread_rng().gen_range(1..=3);
                let node_weight = node_weights[node_to_move_shard_into] + shard_move_weight;
                if node_weight <= MAX_NODE_WEIGHT {
                    node_weights[node_to_move_shard_into] = node_weight;
                    break (node_to_move_shard_into, shard_move_weight);
                }
            };

            tracing::info!(
                "triggering shard move with stake weight {shard_move_weight} to node \
                {node_to_move_shard_into}"
            );
            client_arc
                .as_ref()
                .as_ref()
                .stake_with_node_pool(
                    walrus_cluster.nodes[node_to_move_shard_into]
                        .storage_node_capability
                        .as_ref()
                        .unwrap()
                        .node_id,
                    test_cluster::FROST_PER_NODE_WEIGHT * u64::from(shard_move_weight),
                )
                .await
                .expect("stake with node pool should not fail");

            tokio::time::sleep(Duration::from_secs(70)).await;
        }

        workload_handle.abort();

        loop {
            if let Some(_blob) = client_arc
                .inner
                .sui_client()
                .read_client
                .last_certified_event_blob()
                .await
                .unwrap()
            {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        blob_info_consistency_check.check_storage_node_consistency();
    }

    // Simulates node crash and restart with sim node id.
    // We only trigger the crash once.
    fn crash_target_node(
        target_node_id: sui_simulator::task::NodeId,
        fail_triggered: Arc<AtomicBool>,
        crash_duration: Duration,
    ) {
        if fail_triggered.load(std::sync::atomic::Ordering::SeqCst) {
            // We only need to trigger failure once.
            return;
        }

        let current_node = sui_simulator::current_simnode_id();
        if target_node_id != current_node {
            return;
        }

        tracing::warn!("crashing node {current_node} for {:?}", crash_duration);
        fail_triggered.store(true, std::sync::atomic::Ordering::SeqCst);
        sui_simulator::task::kill_current_node(Some(crash_duration));
    }

    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_new_node_joining_cluster() {
        register_fail_point("fail_point_direct_shard_sync_recovery", move || {
            panic!("shard sync should not enter recovery mode in this test");
        });

        let mut node_recovery_config = NodeRecoveryConfig::default();

        // 20% of the time using a more restrictive node recovery config.
        if rand::thread_rng().gen_bool(0.2) {
            let max_concurrent_blob_syncs_during_recovery = rand::thread_rng().gen_range(1..=3);
            tracing::info!(
                "using more restrictive node recovery config, \
                max_concurrent_blob_syncs_during_recovery: {}",
                max_concurrent_blob_syncs_during_recovery
            );
            node_recovery_config.max_concurrent_blob_syncs_during_recovery =
                max_concurrent_blob_syncs_during_recovery;
        }

        let (_sui_cluster, mut walrus_cluster, client, _) =
            test_cluster::E2eTestSetupBuilder::new()
                .with_epoch_duration(Duration::from_secs(30))
                .with_test_nodes_config(TestNodesConfig {
                    node_weights: vec![1, 2, 3, 3, 4, 0],
                    node_recovery_config: Some(node_recovery_config),
                    ..Default::default()
                })
                .with_communication_config(
                    ClientCommunicationConfig::default_for_test_with_reqwest_timeout(
                        Duration::from_secs(2),
                    ),
                )
                .build_generic::<SimStorageNodeHandle>()
                .await
                .unwrap();

        let blob_info_consistency_check = BlobInfoConsistencyCheck::new();

        assert!(walrus_cluster.nodes[5].node_id.is_none());

        let client_arc = Arc::new(client);

        // Starts a background workload that a client keeps writing and retrieving data.
        // All requests should succeed even if a node crashes.
        let workload_handle =
            simtest_utils::start_background_workload(client_arc.clone(), false, 0);

        // Running the workload for 60 seconds to get some data in the system.
        tokio::time::sleep(Duration::from_secs(90)).await;

        walrus_cluster.nodes[5].node_id = Some(
            SimStorageNodeHandle::spawn_node(
                Arc::new(RwLock::new(
                    walrus_cluster.nodes[5].storage_node_config.clone(),
                )),
                None,
                walrus_cluster.nodes[5].cancel_token.clone(),
            )
            .await
            .id(),
        );

        // Adding stake to the new node so that it can be in Active state.
        client_arc
            .as_ref()
            .as_ref()
            .stake_with_node_pool(
                walrus_cluster.nodes[5]
                    .storage_node_capability
                    .as_ref()
                    .unwrap()
                    .node_id,
                test_cluster::FROST_PER_NODE_WEIGHT * 3,
            )
            .await
            .expect("stake with node pool should not fail");

        if rand::thread_rng().gen_bool(0.1) {
            // Probabilistically crash the node to test shard sync with source node down.
            // In this test, shard sync should not enter recovery mode.
            let fail_triggered = Arc::new(AtomicBool::new(false));
            let target_fail_node_id = walrus_cluster.nodes[0]
                .node_id
                .expect("node id should be set");
            let fail_triggered_clone = fail_triggered.clone();

            register_fail_points(CRASH_NODE_FAIL_POINTS, move || {
                crash_target_node(
                    target_fail_node_id,
                    fail_triggered_clone.clone(),
                    Duration::from_secs(5),
                );
            });
        }

        tokio::time::sleep(Duration::from_secs(150)).await;

        let node_refs: Vec<&SimStorageNodeHandle> = walrus_cluster.nodes.iter().collect();
        let node_health_info = simtest_utils::get_nodes_health_info(&node_refs).await;

        let committees = client_arc
            .inner
            .get_latest_committees_in_test()
            .await
            .unwrap();
        let current_committee = committees.current_committee();
        assert!(current_committee.contains(&walrus_cluster.nodes[5].public_key));

        assert!(node_health_info[5].shard_detail.is_some());

        // Check that shards in the new node matches the shards in the committees.
        let shards_in_new_node = committees
            .current_committee()
            .shards_for_node_public_key(&walrus_cluster.nodes[5].public_key);
        let new_node_shards = node_health_info[5]
            .shard_detail
            .as_ref()
            .unwrap()
            .owned
            .clone();
        assert_eq!(shards_in_new_node.len(), new_node_shards.len());
        for shard in new_node_shards {
            assert!(shards_in_new_node.contains(&shard.shard));
        }

        for shard in &node_health_info[5].shard_detail.as_ref().unwrap().owned {
            assert_eq!(shard.status, ShardStatus::Ready);

            // These shards should not exist in any of the other nodes.
            for i in 0..node_health_info.len() - 1 {
                assert_eq!(
                    node_health_info[i]
                        .shard_detail
                        .as_ref()
                        .unwrap()
                        .owned
                        .iter()
                        .find(|s| s.shard == shard.shard),
                    None
                );
                let shard_i_status = node_health_info[i]
                    .shard_detail
                    .as_ref()
                    .unwrap()
                    .owned
                    .iter()
                    .find(|s| s.shard == shard.shard);
                assert!(
                    shard_i_status.is_none()
                        || shard_i_status.unwrap().status != ShardStatus::ReadOnly
                );
            }
        }

        assert_eq!(
            simtest_utils::get_nodes_health_info(&[&walrus_cluster.nodes[5]])
                .await
                .get(0)
                .unwrap()
                .node_status,
            "Active"
        );

        workload_handle.abort();

        loop {
            if let Some(_blob) = client_arc
                .inner
                .sui_client()
                .read_client
                .last_certified_event_blob()
                .await
                .unwrap()
            {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        blob_info_consistency_check.check_storage_node_consistency();

        clear_fail_point("fail_point_direct_shard_sync_recovery");
    }

    // The node recovery process is artificially prolonged to be longer than 1 epoch.
    // We should expect the recovering node should eventually become Active.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_long_node_recovery() {
        let mut node_recovery_config = NodeRecoveryConfig::default();

        // 20% of the time using a more restrictive node recovery config.
        if rand::thread_rng().gen_bool(0.2) {
            let max_concurrent_blob_syncs_during_recovery = rand::thread_rng().gen_range(1..=3);
            tracing::info!(
                "using more restrictive node recovery config, \
                max_concurrent_blob_syncs_during_recovery: {}",
                max_concurrent_blob_syncs_during_recovery
            );
            node_recovery_config.max_concurrent_blob_syncs_during_recovery =
                max_concurrent_blob_syncs_during_recovery;
        }

        let (_sui_cluster, walrus_cluster, client, _) = test_cluster::E2eTestSetupBuilder::new()
            .with_epoch_duration(Duration::from_secs(30))
            .with_test_nodes_config(TestNodesConfig {
                node_weights: vec![1, 2, 3, 3, 4],
                use_legacy_event_processor: false,
                node_recovery_config: Some(node_recovery_config),
                ..Default::default()
            })
            .with_communication_config(
                ClientCommunicationConfig::default_for_test_with_reqwest_timeout(
                    Duration::from_secs(2),
                ),
            )
            .with_default_num_checkpoints_per_blob()
            .build_generic::<SimStorageNodeHandle>()
            .await
            .unwrap();

        let blob_info_consistency_check = BlobInfoConsistencyCheck::new();

        let client_arc = Arc::new(client);

        // Starts a background workload that a client keeps writing and retrieving data.
        // All requests should succeed even if a node crashes.
        let workload_handle =
            simtest_utils::start_background_workload(client_arc.clone(), false, 0);

        // Running the workload for 60 seconds to get some data in the system.
        tokio::time::sleep(Duration::from_secs(60)).await;

        // Register a fail point to have a temporary pause in the node recovery process that is
        // longer than epoch length.
        register_fail_point_async("start_node_recovery_entry", || async move {
            tokio::time::sleep(Duration::from_secs(60)).await;
        });

        // Tracks if a crash has been triggered.
        let fail_triggered = Arc::new(AtomicBool::new(false));
        let target_fail_node_id = walrus_cluster.nodes[0]
            .node_id
            .expect("node id should be set");
        let fail_triggered_clone = fail_triggered.clone();

        register_fail_points(CRASH_NODE_FAIL_POINTS, move || {
            crash_target_node(
                target_fail_node_id,
                fail_triggered_clone.clone(),
                Duration::from_secs(60),
            );
        });

        tokio::time::sleep(Duration::from_secs(180)).await;

        let node_refs: Vec<&SimStorageNodeHandle> = walrus_cluster.nodes.iter().collect();
        let node_health_info = simtest_utils::get_nodes_health_info(&node_refs).await;

        assert!(node_health_info[0].shard_detail.is_some());
        for shard in &node_health_info[0].shard_detail.as_ref().unwrap().owned {
            // For all the shards that the crashed node owns, they should be in ready state.
            assert_eq!(shard.status, ShardStatus::Ready);
        }

        assert_eq!(
            simtest_utils::get_nodes_health_info(&[&walrus_cluster.nodes[0]])
                .await
                .get(0)
                .unwrap()
                .node_status,
            "Active"
        );

        workload_handle.abort();

        blob_info_consistency_check.check_storage_node_consistency();

        clear_fail_point("start_node_recovery_entry");
    }

    #[walrus_simtest]
    async fn walrus_certified_event_processing_jitter() {
        let blob_info_consistency_check = BlobInfoConsistencyCheck::new();

        register_fail_point_async("fail_point_process_blob_certified_event", || async move {
            tokio::time::sleep(Duration::from_millis(
                rand::rngs::StdRng::from_entropy().gen_range(0..=500),
            ))
            .await;
        });

        let (_sui_cluster, walrus_cluster, client, _) = test_cluster::E2eTestSetupBuilder::new()
            .with_test_nodes_config(TestNodesConfig {
                node_weights: vec![1, 2, 3, 3, 4],
                ..Default::default()
            })
            .with_epoch_duration(Duration::from_secs(30))
            .build_generic::<SimStorageNodeHandle>()
            .await
            .unwrap();

        let workload_handle = simtest_utils::start_background_workload(Arc::new(client), true, 0);

        // Run the workload for 120 seconds to get some data in the system.
        tokio::time::sleep(Duration::from_secs(120)).await;

        workload_handle.abort();

        // Wait for event to catch up.
        tokio::time::sleep(Duration::from_secs(60)).await;

        let node_refs: Vec<&SimStorageNodeHandle> = walrus_cluster.nodes.iter().collect();
        let health_info = simtest_utils::get_nodes_health_info(&node_refs).await;
        for node_health in health_info {
            assert!(node_health.event_progress.pending < 10);
        }

        blob_info_consistency_check.check_storage_node_consistency();
    }
}
