// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Contains simtest related to storage node failures.

#![recursion_limit = "256"]

#[cfg(msim)]
mod tests {
    use std::{
        collections::HashSet,
        sync::{
            Arc,
            Mutex,
            atomic::{AtomicBool, AtomicUsize},
        },
        time::{Duration, Instant},
    };

    use rand::{Rng, SeedableRng, thread_rng};
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
        NodeCrashConfig,
        repeatedly_crash_target_node,
    };
    use walrus_storage_node_client::api::ShardStatus;
    use walrus_sui::client::{ReadClient, rpc_client};

    const FAILURE_TRIGGER_PROBABILITY: f64 = 0.01;

    // Tests the scenario where a single node crashes and restarts.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_walrus_with_single_node_crash_and_restart() {
        let (sui_cluster, _walrus_cluster, client, _) = test_cluster::E2eTestSetupBuilder::new()
            .with_test_nodes_config(
                TestNodesConfig::builder()
                    .with_node_weights(vec![1, 2, 3, 3, 4])
                    .with_enable_event_blob_writer()
                    .build(),
            )
            .build_generic::<SimStorageNodeHandle>()
            .await
            .unwrap();

        let blob_info_consistency_check = BlobInfoConsistencyCheck::new();

        // Tracks if a crash has been triggered.
        let fail_triggered = Arc::new(AtomicBool::new(false));

        // Do not fail any nodes in the sui cluster.
        let mut do_not_fail_nodes = sui_cluster
            .lock()
            .await
            .cluster()
            .all_node_handles()
            .iter()
            .map(|n| n.with(|n| n.get_sim_node_id()))
            .collect::<HashSet<_>>();
        do_not_fail_nodes.insert(sui_cluster.lock().await.sim_node_handle().id());

        let fail_triggered_clone = fail_triggered.clone();
        sui_macros::register_fail_points(CRASH_NODE_FAIL_POINTS, move || {
            handle_failpoint_and_crash_node_once(
                do_not_fail_nodes.clone(),
                fail_triggered_clone.clone(),
                FAILURE_TRIGGER_PROBABILITY,
            );
        });

        // Run workload and wait until a crash is triggered.
        let mut data_length = 31415;
        let mut blobs_written = HashSet::new();
        loop {
            // TODO(#995): use stress client for better coverage of the workload.
            simtest_utils::write_read_and_check_random_blob(
                &client,
                data_length,
                false,
                false,
                &mut blobs_written,
                None,
                None,
            )
            .await
            .expect("workload should not fail");

            data_length += 1024;
            if fail_triggered.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
        }

        // Continue running the workload for another 60 seconds.
        let _ = tokio::time::timeout(Duration::from_mins(1), async {
            let mut blobs_written = HashSet::new();
            loop {
                // TODO(#995): use stress client for better coverage of the workload.
                simtest_utils::write_read_and_check_random_blob(
                    &client,
                    data_length,
                    false,
                    false,
                    &mut blobs_written,
                    None,
                    None,
                )
                .await
                .expect("workload should not fail");

                data_length += 1024;
            }
        })
        .await;

        assert!(fail_triggered.load(std::sync::atomic::Ordering::SeqCst));

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

    // Action taken during a various failpoints. There is a chance with `probability` that the
    // current node will be crashed, and restarted after a random duration.
    fn handle_failpoint_and_crash_node_once(
        keep_alive_nodes: HashSet<sui_simulator::task::NodeId>,
        fail_triggered: Arc<AtomicBool>,
        probability: f64,
    ) {
        if fail_triggered.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }

        let current_node = sui_simulator::current_simnode_id();
        if keep_alive_nodes.contains(&current_node) {
            return;
        }

        let mut rng = rand::thread_rng();
        if rng.gen_range(0.0..1.0) < probability {
            let restart_after = Duration::from_secs(rng.gen_range(10..30));

            tracing::warn!(
                "crashing node {} for {} seconds",
                current_node,
                restart_after.as_secs()
            );

            fail_triggered.store(true, std::sync::atomic::Ordering::SeqCst);

            sui_simulator::task::kill_current_node(Some(restart_after));
            // Do not put any code after this point, as it won't be executed.
            // kill_current_node is implemented using a panic.
        }
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
        // Do not put any code after this point, as it won't be executed.
        // kill_current_node is implemented using a panic.
    }

    // This integration test simulates a scenario where a node is lagging behind and recovers.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_lagging_node_recovery() {
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
            .with_test_nodes_config(
                TestNodesConfig::builder()
                    .with_node_weights(vec![1, 2, 3, 3, 4])
                    .with_node_recovery_config(node_recovery_config)
                    .build(),
            )
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
            simtest_utils::start_background_workload(client_arc.clone(), false, None, None);

        // Running the workload for 60 seconds to get some data in the system.
        tokio::time::sleep(Duration::from_mins(1)).await;

        // Tracks if a crash has been triggered.
        let fail_triggered = Arc::new(AtomicBool::new(false));
        let target_fail_node_id = walrus_cluster.nodes[0]
            .node_id
            .expect("node id should be set");
        let fail_triggered_clone = fail_triggered.clone();

        sui_macros::register_fail_points(CRASH_NODE_FAIL_POINTS, move || {
            crash_target_node(
                target_fail_node_id,
                fail_triggered_clone.clone(),
                Duration::from_mins(2),
            );
        });

        // Changes the stake of the crashed node so that it will gain some shards after the next
        // epoch change. Note that the expectation is the node will be back only after more than
        // 2 epoch changes, so that the node can be in a RecoveryInProgress state.
        client_arc
            .as_ref()
            .as_ref()
            .stake_with_node_pool(
                walrus_cluster.nodes[0]
                    .storage_node_capability
                    .as_ref()
                    .unwrap()
                    .node_id,
                test_cluster::FROST_PER_NODE_WEIGHT * 3,
            )
            .await
            .expect("stake with node pool should not fail");

        // Probabilistically trigger a node crash during node recovery.
        let crash_during_recovery = rand::thread_rng().gen_bool(0.1);
        if crash_during_recovery {
            let fail_triggered = Arc::new(AtomicBool::new(false));
            let target_fail_node_id = walrus_cluster.nodes[0]
                .node_id
                .expect("node id should be set");
            let fail_triggered_clone = fail_triggered.clone();

            sui_macros::register_fail_point("fail_point_node_recovery_start_sync", move || {
                crash_target_node(
                    target_fail_node_id,
                    fail_triggered_clone.clone(),
                    Duration::from_secs(5),
                );
            });
        }

        tokio::time::sleep(Duration::from_secs(150)).await;

        let node_health_info = simtest_utils::get_nodes_health_info(&walrus_cluster.nodes).await;

        assert!(node_health_info[0].shard_detail.is_some());
        for shard in &node_health_info[0].shard_detail.as_ref().unwrap().owned {
            // For all the shards that the crashed node owns, they should be in ready state.
            assert_eq!(shard.status, ShardStatus::Ready);

            // These shards should not exist in any of the other nodes.
            for i in 1..node_health_info.len() {
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

        assert_eq!(node_health_info[0].node_status, "Active");

        workload_handle.abort();

        blob_info_consistency_check.check_storage_node_consistency();
    }

    // This integration test simulates a scenario where a node is repeatedly crashing and
    // recovering.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_repeated_node_crash() {
        // We use a very short epoch duration of 10 seconds so that we can exercise more epoch
        // changes in the test.
        let (_sui_cluster, walrus_cluster, client, _) = test_cluster::E2eTestSetupBuilder::new()
            .with_epoch_duration(Duration::from_secs(10))
            .with_test_nodes_config(
                TestNodesConfig::builder()
                    .with_node_weights(vec![2, 2, 3, 3, 3])
                    .build(),
            )
            .with_communication_config(
                ClientCommunicationConfig::default_for_test_with_reqwest_timeout(
                    Duration::from_secs(1),
                ),
            )
            .with_default_num_checkpoints_per_blob()
            .build_generic::<SimStorageNodeHandle>()
            .await
            .unwrap();

        let blob_info_consistency_check = BlobInfoConsistencyCheck::new();

        let node_index_to_crash = thread_rng().gen_range(0..walrus_cluster.nodes.len());
        let target_fail_node_id = walrus_cluster.nodes[node_index_to_crash]
            .node_id
            .expect("node id should be set");

        // We probabilistically cause the target shard to slow down processing events, so that
        // certified blob events require blob recovery, and mix with epoch change.
        let cause_target_shard_slow_processing_event = thread_rng().gen_bool(0.5);
        if cause_target_shard_slow_processing_event {
            sui_macros::register_fail_point_async("epoch_change_start_entry", move || async move {
                if sui_simulator::current_simnode_id() == target_fail_node_id {
                    tokio::time::sleep(Duration::from_secs(
                        rand::rngs::StdRng::from_entropy().gen_range(2..=7),
                    ))
                    .await;
                }
            });
        }

        // 20% of the test cases, we trigger a fail point to make blob recovery slower.
        if rand::thread_rng().gen_bool(0.2) {
            tracing::info!(
                "triggering fail point fail_point_recover_sliver_before_put_sliver, make \
                blob recovery slower"
            );
            sui_macros::register_fail_point_async(
                "fail_point_recover_sliver_before_put_sliver",
                move || async move {
                    if sui_simulator::current_simnode_id() == target_fail_node_id {
                        tokio::time::sleep(Duration::from_secs(30)).await;
                    }
                },
            );
        }

        let client_arc = Arc::new(client);

        // Use a higher write retry limit given that the epoch duration is short.
        let workload_handle =
            simtest_utils::start_background_workload(client_arc.clone(), true, Some(5), None);

        let next_fail_triggered = Arc::new(Mutex::new(Instant::now()));
        let next_fail_triggered_clone = next_fail_triggered.clone();
        let crash_end_time = Instant::now() + Duration::from_mins(2);

        sui_macros::register_fail_points(CRASH_NODE_FAIL_POINTS, move || {
            repeatedly_crash_target_node(
                target_fail_node_id,
                next_fail_triggered_clone.clone(),
                crash_end_time,
                NodeCrashConfig {
                    min_crash_duration_secs: 5,
                    max_crash_duration_secs: 25,
                    min_live_duration_secs: 5,
                    max_live_duration_secs: 25,
                },
            );
        });

        // We probabilistically trigger a shard move to the crashed node to test the recovery.
        // The additional stake assigned are randomly chosen between 2 and 10 times of the original
        // stake the per-node.
        let shard_move_weight = rand::thread_rng().gen_range(2..=10);

        // 30% of the time, we move shards to the crashed node. The other 70% of the time, we move
        // shards to a different node.
        let node_index_to_move = if thread_rng().gen_bool(0.3) {
            node_index_to_crash
        } else {
            thread_rng().gen_range(0..walrus_cluster.nodes.len())
        };

        tracing::info!(
            "triggering shard move with stake weight {}, target node {}",
            shard_move_weight,
            node_index_to_move
        );

        client_arc
            .as_ref()
            .as_ref()
            .stake_with_node_pool(
                walrus_cluster.nodes[node_index_to_move]
                    .storage_node_capability
                    .as_ref()
                    .unwrap()
                    .node_id,
                test_cluster::FROST_PER_NODE_WEIGHT * shard_move_weight,
            )
            .await
            .expect("stake with node pool should not fail");

        tokio::time::sleep(Duration::from_mins(3)).await;

        workload_handle.abort();

        // Check the final state of storage node after a few crash and recovery.
        let mut last_persist_event_index = 0;
        let mut last_persisted_event_time = Instant::now();
        let start_time = Instant::now();
        loop {
            if start_time.elapsed() > Duration::from_mins(1) {
                break;
            }
            let node_health_info =
                simtest_utils::get_nodes_health_info([&walrus_cluster.nodes[node_index_to_crash]])
                    .await;
            tracing::info!(
                "event progress: persisted {:?}, pending {:?}",
                node_health_info[0].event_progress.persisted,
                node_health_info[0].event_progress.pending
            );
            if last_persist_event_index == node_health_info[0].event_progress.persisted {
                // We expect that there shouldn't be any stuck event progress.
                assert!(last_persisted_event_time.elapsed() < Duration::from_secs(30));
            } else {
                last_persist_event_index = node_health_info[0].event_progress.persisted;
                last_persisted_event_time = Instant::now();
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }

        // And finally the node should be in Active state.
        assert_eq!(
            simtest_utils::get_nodes_health_info([&walrus_cluster.nodes[node_index_to_crash]])
                .await
                .get(0)
                .unwrap()
                .node_status,
            "Active"
        );

        blob_info_consistency_check.check_storage_node_consistency();

        if cause_target_shard_slow_processing_event {
            sui_macros::clear_fail_point("epoch_change_start_entry");
        }
    }

    // This integration test simulates a scenario where a node is repeatedly crashing and
    // recovering.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_checkpoint_lag_error() {
        // We use a very short epoch duration of 10 seconds so that we can exercise more epoch
        // changes in the test.
        let (_sui_cluster, walrus_cluster, _, _) = test_cluster::E2eTestSetupBuilder::new()
            .with_epoch_duration(Duration::from_secs(10))
            .with_test_nodes_config(
                TestNodesConfig::builder()
                    .with_node_weights(vec![2, 2, 3, 3, 3])
                    .build(),
            )
            .with_communication_config(
                ClientCommunicationConfig::default_for_test_with_reqwest_timeout(
                    Duration::from_secs(1),
                ),
            )
            .with_default_num_checkpoints_per_blob()
            .build_generic::<SimStorageNodeHandle>()
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(20)).await;

        sui_macros::register_fail_point_if("fail_point_current_checkpoint_lag_error", move || true);

        // Make sure that checkpoint downloader is continuing making progress.
        let mut last_checkpoint_seq_number = 0;
        let mut last_checkpoint_update_time = Instant::now();
        let start_time = Instant::now();
        loop {
            if start_time.elapsed() > Duration::from_mins(1) {
                break;
            }
            let node_health_info =
                simtest_utils::get_nodes_health_info([&walrus_cluster.nodes[0]]).await;
            tracing::info!(
                "last checkpoint seq number in node 0: {:?}",
                node_health_info[0].latest_checkpoint_sequence_number,
            );
            if last_checkpoint_seq_number
                == node_health_info[0]
                    .latest_checkpoint_sequence_number
                    .unwrap()
            {
                // We expect that there shouldn't be any stuck event progress.
                assert!(last_checkpoint_update_time.elapsed() < Duration::from_secs(15));
            } else {
                last_checkpoint_seq_number = node_health_info[0]
                    .latest_checkpoint_sequence_number
                    .unwrap();
                last_checkpoint_update_time = Instant::now();
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    // Simulates node event processing is severely lagging behind without crashing the node.
    async fn blocking_target_node_event_processing(
        target_node_id: sui_simulator::task::NodeId,
        fail_triggered: Arc<AtomicBool>,
        block_duration: Duration,
    ) {
        if fail_triggered.load(std::sync::atomic::Ordering::SeqCst) {
            // We only need to trigger failure once.
            return;
        }

        let current_node = sui_simulator::current_simnode_id();
        if target_node_id != current_node {
            return;
        }

        tracing::warn!("blocking node {current_node} for {:?}", block_duration);

        fail_triggered.store(true, std::sync::atomic::Ordering::SeqCst);
        tokio::time::sleep(block_duration).await;
    }

    // This integration test simulates a scenario where a node is lagging behind and should enter
    // recovery mode while processing events.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_node_slow_process_events_entering_recovery() {
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
            .with_test_nodes_config(
                TestNodesConfig::builder()
                    .with_node_weights(vec![1, 2, 3, 3, 4])
                    .build(),
            )
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
        // All requests should succeed even if a node is lagging behind.
        let workload_handle =
            simtest_utils::start_background_workload(client_arc.clone(), false, None, None);

        // Running the workload for 60 seconds to get some data in the system.
        tokio::time::sleep(Duration::from_mins(1)).await;

        // Tracks if a lagging event processing has been triggered.
        let fail_triggered = Arc::new(AtomicBool::new(false));
        let target_fail_node_id = walrus_cluster.nodes[0]
            .node_id
            .expect("node id should be set");
        let fail_triggered_clone = fail_triggered.clone();

        sui_macros::register_fail_point_async("before-process-event-impl", move || {
            let fail_triggered_clone = fail_triggered_clone.clone();
            async move {
                blocking_target_node_event_processing(
                    target_fail_node_id,
                    fail_triggered_clone.clone(),
                    Duration::from_secs(90),
                )
                .await;
            }
        });

        // Tracks the number of times the node enters recovery mode.
        let enter_recovery_mode_count = Arc::new(AtomicUsize::new(0));
        let enter_recovery_mode_count_clone = enter_recovery_mode_count.clone();
        sui_macros::register_fail_point("fail-point-enter-recovery-mode", move || {
            enter_recovery_mode_count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        });

        // Changes the stake of the lagging node so that it will gain some shards after the next
        // epoch change. Note that the expectation is the node will be back only after more than
        // 2 epoch changes, so that the node can be in a RecoveryInProgress state.
        client_arc
            .as_ref()
            .as_ref()
            .stake_with_node_pool(
                walrus_cluster.nodes[0]
                    .storage_node_capability
                    .as_ref()
                    .unwrap()
                    .node_id,
                test_cluster::FROST_PER_NODE_WEIGHT * 3,
            )
            .await
            .expect("stake with node pool should not fail");

        tokio::time::sleep(Duration::from_secs(150)).await;

        let node_health_info = simtest_utils::get_nodes_health_info(&walrus_cluster.nodes).await;

        assert!(node_health_info[0].shard_detail.is_some());
        for shard in &node_health_info[0].shard_detail.as_ref().unwrap().owned {
            // For all the shards that the lagging node owns, they should be in ready state.
            assert_eq!(shard.status, ShardStatus::Ready);

            // These shards should not exist in any of the other nodes.
            for i in 1..node_health_info.len() {
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

        assert_eq!(node_health_info[0].node_status, "Active");

        workload_handle.abort();

        assert!(
            enter_recovery_mode_count.load(std::sync::atomic::Ordering::SeqCst) > 0,
            "the node should enter recovery mode"
        );

        blob_info_consistency_check.check_storage_node_consistency();
        sui_macros::clear_fail_point("before-process-event-impl");
        sui_macros::clear_fail_point("fail-point-enter-recovery-mode");
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

            let node_health_infos =
                simtest_utils::get_nodes_health_info(nodes_to_check.clone()).await;

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
        let checkpoints_per_event_blob = 20;
        let (sui_cluster, mut walrus_cluster, client, _) = test_cluster::E2eTestSetupBuilder::new()
            .with_epoch_duration(Duration::from_secs(15))
            .with_test_nodes_config(
                TestNodesConfig::builder()
                    .with_node_weights(vec![2, 2, 3, 3, 3])
                    .with_enable_event_blob_writer()
                    .build(),
            )
            .with_num_checkpoints_per_blob(checkpoints_per_event_blob)
            .with_communication_config(
                ClientCommunicationConfig::default_for_test_with_reqwest_timeout(
                    Duration::from_secs(2),
                ),
            )
            .with_additional_fullnodes(4)
            .build_generic::<SimStorageNodeHandle>()
            .await
            .unwrap();

        // Register a fail point that will fail the first attempt.
        sui_macros::register_fail_point_if("fallback_client_inject_error", move || true);
        let primary_rpc_url = sui_cluster.lock().await.rpc_url();
        let primary_rpc_url_clone = primary_rpc_url.clone();

        // Always fail sui client creation for the primary rpc node.
        sui_macros::register_fail_point_arg(
            "failpoint_sui_client_build_client",
            move || -> Option<String> { Some(primary_rpc_url.clone()) },
        );

        // Always fail rpc client creation for the primary rpc node.
        sui_macros::register_fail_point_arg(
            "failpoint_rpc_client_build_client",
            move || -> Option<String> { Some(primary_rpc_url_clone.clone()) },
        );

        tracing::info!(
            "Additional fullnodes: {:?}",
            sui_cluster.lock().await.additional_rpc_urls()
        );
        let client_arc = Arc::new(client);

        // Restart all nodes, this should still form a cluster with all nodes running.
        simtest_utils::restart_nodes_with_checkpoints(&mut walrus_cluster, |_| {
            checkpoints_per_event_blob
        })
        .await;

        // Wait for the cluster to process some events.
        let workload_handle =
            simtest_utils::start_background_workload(client_arc.clone(), false, None, None);
        tokio::time::sleep(Duration::from_secs(30)).await;

        // Get the latest checkpoint from Sui.
        let mut rpc_client = rpc_client::create_sui_rpc_client(
            &sui_cluster.lock().await.additional_rpc_urls()[0].clone(),
        )
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
        let node_health_infos = simtest_utils::get_nodes_health_info(node_refs.clone()).await;

        tracing::info!("Node health infos: {:?}", node_health_infos);

        wait_for_nodes_at_checkpoint(
            &node_refs,
            latest_sui_checkpoint_seq,
            Duration::from_secs(100),
        )
        .await
        .expect("All nodes should have downloaded the checkpoint");

        sui_macros::clear_fail_point("fallback_client_inject_error");
        sui_macros::clear_fail_point("failpoint_sui_client_build_client");
        sui_macros::clear_fail_point("failpoint_rpc_client_build_client");
    }
}
