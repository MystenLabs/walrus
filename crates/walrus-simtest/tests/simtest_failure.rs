// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Contains simtest related to storage node failures.

#![recursion_limit = "256"]

#[cfg(msim)]
mod tests {
    use std::{
        collections::HashSet,
        sync::{Arc, Mutex, atomic::AtomicBool},
        time::{Duration, Instant},
    };

    use rand::{Rng, SeedableRng, thread_rng};
    use sui_macros::{clear_fail_point, register_fail_point_async, register_fail_point_if};
    use sui_protocol_config::ProtocolConfig;
    use walrus_proc_macros::walrus_simtest;
    use walrus_service::{
        client::ClientCommunicationConfig,
        test_utils::{SimStorageNodeHandle, TestNodesConfig, test_cluster},
    };
    use walrus_simtest::test_utils::simtest_utils::{
        self,
        BlobInfoConsistencyCheck,
        CRASH_NODE_FAIL_POINTS,
    };
    use walrus_storage_node_client::api::ShardStatus;
    use walrus_sui::client::ReadClient;

    const FAILURE_TRIGGER_PROBABILITY: f64 = 0.01;

    // Tests the scenario where a single node crashes and restarts.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn walrus_with_single_node_crash_and_restart() {
        let _guard = ProtocolConfig::apply_overrides_for_testing(|_, mut config| {
            // TODO: remove once Sui simtest can work with these features.
            config.set_enable_jwk_consensus_updates_for_testing(false);
            config.set_random_beacon_for_testing(false);
            config
        });

        let (sui_cluster, _walrus_cluster, client, _) = test_cluster::E2eTestSetupBuilder::new()
            .with_test_nodes_config(TestNodesConfig {
                node_weights: vec![1, 2, 3, 3, 4],
                ..Default::default()
            })
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
                &mut blobs_written,
            )
            .await
            .expect("workload should not fail");

            data_length += 1024;
            if fail_triggered.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
        }

        // Continue running the workload for another 60 seconds.
        let _ = tokio::time::timeout(Duration::from_secs(60), async {
            let mut blobs_written = HashSet::new();
            loop {
                // TODO(#995): use stress client for better coverage of the workload.
                simtest_utils::write_read_and_check_random_blob(
                    &client,
                    data_length,
                    false,
                    &mut blobs_written,
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
    }

    // This integration test simulates a scenario where a node is lagging behind and recovers.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_lagging_node_recovery() {
        let (_sui_cluster, walrus_cluster, client, _) = test_cluster::E2eTestSetupBuilder::new()
            .with_epoch_duration(Duration::from_secs(30))
            .with_test_nodes_config(TestNodesConfig {
                node_weights: vec![1, 2, 3, 3, 4],
                use_legacy_event_processor: false,
                disable_event_blob_writer: false,
                blocklist_dir: None,
                enable_node_config_synchronizer: false,
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
        let workload_handle = simtest_utils::start_background_workload(client_arc.clone(), false);

        // Running the workload for 60 seconds to get some data in the system.
        tokio::time::sleep(Duration::from_secs(60)).await;

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
                Duration::from_secs(120),
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

        tokio::time::sleep(Duration::from_secs(150)).await;

        let node_refs: Vec<&SimStorageNodeHandle> = walrus_cluster.nodes.iter().collect();
        let node_health_info = simtest_utils::get_nodes_health_info(&node_refs).await;

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
    }

    // Simulates repeated node crash and restart with sim node id.
    fn repeatedly_crash_target_node(
        target_node_id: sui_simulator::task::NodeId,
        next_fail_triggered_clone: Arc<Mutex<Instant>>,
        crash_end_time: Instant,
    ) {
        let time_now = Instant::now();
        if time_now > crash_end_time {
            // No more crash is needed.
            return;
        }

        if time_now < *next_fail_triggered_clone.lock().unwrap() {
            // Not time to crash yet.
            return;
        }

        let current_node = sui_simulator::current_simnode_id();
        if target_node_id != current_node {
            return;
        }

        let mut rng = rand::thread_rng();
        let node_down_duration = Duration::from_secs(rng.gen_range(5..=25));
        let next_crash_time =
            Instant::now() + node_down_duration + Duration::from_secs(rng.gen_range(5..=25));

        tracing::warn!(
            "crashing node {current_node} for {} seconds; next crash is set to {:?}",
            node_down_duration.as_secs(),
            next_crash_time
        );
        sui_simulator::task::kill_current_node(Some(node_down_duration));
        *next_fail_triggered_clone.lock().unwrap() = next_crash_time;
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
            .with_test_nodes_config(TestNodesConfig {
                node_weights: vec![2, 2, 3, 3, 3],
                use_legacy_event_processor: false,
                disable_event_blob_writer: false,
                blocklist_dir: None,
                enable_node_config_synchronizer: false,
            })
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
            register_fail_point_async("epoch_change_start_entry", move || async move {
                if sui_simulator::current_simnode_id() == target_fail_node_id {
                    tokio::time::sleep(Duration::from_secs(
                        rand::rngs::StdRng::from_entropy().gen_range(2..=7),
                    ))
                    .await;
                }
            });
        }

        let client_arc = Arc::new(client);
        let client_clone = client_arc.clone();

        // First, we inject some data into the cluster. Note that to control the test duration, we
        // stopped the workload once started crashing the node.
        let mut data_length = 64;
        let workload_start_time = Instant::now();
        let mut blobs_written = HashSet::new();
        loop {
            if workload_start_time.elapsed() > Duration::from_secs(20) {
                tracing::info!("generated 60s of data; stopping workload");
                break;
            }
            tracing::info!("writing data with size {data_length}");

            // TODO(#995): use stress client for better coverage of the workload.
            simtest_utils::write_read_and_check_random_blob(
                client_clone.as_ref(),
                data_length,
                true,
                &mut blobs_written,
            )
            .await
            .expect("workload should not fail");

            tracing::info!("finished writing data with size {data_length}");

            data_length += 1;
        }

        let next_fail_triggered = Arc::new(Mutex::new(Instant::now()));
        let next_fail_triggered_clone = next_fail_triggered.clone();
        let crash_end_time = Instant::now() + Duration::from_secs(2 * 60);

        sui_macros::register_fail_points(CRASH_NODE_FAIL_POINTS, move || {
            repeatedly_crash_target_node(
                target_fail_node_id,
                next_fail_triggered_clone.clone(),
                crash_end_time,
            );
        });

        // We probabilistically trigger a shard move to the crashed node to test the recovery.
        // The additional stake assigned are randomly chosen between 2 and 5 times of the original
        // stake the per-node.
        let shard_move_weight = rand::thread_rng().gen_range(2..=5);
        tracing::info!(
            "triggering shard move with stake weight {}",
            shard_move_weight
        );

        client_arc
            .as_ref()
            .as_ref()
            .stake_with_node_pool(
                walrus_cluster.nodes[node_index_to_crash]
                    .storage_node_capability
                    .as_ref()
                    .unwrap()
                    .node_id,
                test_cluster::FROST_PER_NODE_WEIGHT * shard_move_weight,
            )
            .await
            .expect("stake with node pool should not fail");

        tokio::time::sleep(Duration::from_secs(3 * 60)).await;

        // Check the final state of storage node after a few crash and recovery.
        let mut last_persist_event_index = 0;
        let mut last_persisted_event_time = Instant::now();
        let start_time = Instant::now();
        loop {
            if start_time.elapsed() > Duration::from_secs(1 * 60) {
                break;
            }
            let node_health_info =
                simtest_utils::get_nodes_health_info(&[&walrus_cluster.nodes[node_index_to_crash]])
                    .await;
            tracing::info!(
                "event progress: persisted {:?}, pending {:?}",
                node_health_info[0].event_progress.persisted,
                node_health_info[0].event_progress.pending
            );
            if last_persist_event_index == node_health_info[0].event_progress.persisted {
                // We expect that there shouldn't be any stuck event progress.
                assert!(last_persisted_event_time.elapsed() < Duration::from_secs(15));
            } else {
                last_persist_event_index = node_health_info[0].event_progress.persisted;
                last_persisted_event_time = Instant::now();
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }

        // And finally the node should be in Active state.
        assert_eq!(
            simtest_utils::get_nodes_health_info(&[&walrus_cluster.nodes[node_index_to_crash]])
                .await
                .get(0)
                .unwrap()
                .node_status,
            "Active"
        );

        blob_info_consistency_check.check_storage_node_consistency();

        if cause_target_shard_slow_processing_event {
            clear_fail_point("epoch_change_start_entry");
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
            .with_test_nodes_config(TestNodesConfig {
                node_weights: vec![2, 2, 3, 3, 3],
                use_legacy_event_processor: false,
                disable_event_blob_writer: false,
                blocklist_dir: None,
                enable_node_config_synchronizer: false,
            })
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

        register_fail_point_if("fail_point_current_checkpoint_lag_error", move || true);

        // Make sure that checkpoint downloader is continuing making progress.
        let mut last_checkpoint_seq_number = 0;
        let mut last_checkpoint_update_time = Instant::now();
        let start_time = Instant::now();
        loop {
            if start_time.elapsed() > Duration::from_secs(60) {
                break;
            }
            let node_health_info =
                simtest_utils::get_nodes_health_info(&[&walrus_cluster.nodes[0]]).await;
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
}
