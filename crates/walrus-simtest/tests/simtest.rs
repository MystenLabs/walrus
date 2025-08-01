// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Contains dedicated Walrus simulation tests.
#![recursion_limit = "256"]

#[cfg(msim)]
mod tests {
    use std::{
        collections::HashSet,
        sync::{
            Arc,
            Mutex,
            atomic::{AtomicBool, Ordering},
        },
        time::{Duration, Instant},
    };

    use rand::{Rng, SeedableRng, seq::SliceRandom};
    use sui_macros::{
        clear_fail_point,
        register_fail_point,
        register_fail_point_async,
        register_fail_points,
    };
    use sui_protocol_config::ProtocolConfig;
    use sui_simulator::configs::{env_config, uniform_latency_ms};
    use tempfile::TempDir;
    use tokio::sync::RwLock;
    use walrus_core::EpochCount;
    use walrus_proc_macros::walrus_simtest;
    use walrus_sdk::client::{Client, StoreArgs, metrics::ClientMetrics};
    use walrus_service::{
        client::ClientCommunicationConfig,
        event::event_processor::config::EventProcessorConfig,
        node::config::{NodeRecoveryConfig, StorageNodeConfig},
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
    use walrus_stress::single_client_workload::{
        SingleClientWorkload,
        single_client_workload_config::{
            RequestTypeDistributionConfig,
            SizeDistributionConfig,
            StoreLengthDistributionConfig,
        },
    };
    use walrus_sui::{
        client::{BlobPersistence, PostStoreAction, ReadClient, SuiContractClient, UpgradeType},
        system_setup::copy_recursively,
        test_utils::system_setup::{development_contract_dir, testnet_contract_dir},
        types::move_structs::EventBlob,
    };
    use walrus_test_utils::WithTempDir;

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
            None,
            None,
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
        let workload_handle =
            simtest_utils::start_background_workload(client_arc.clone(), true, None, None);

        // Run the workload to get some data in the system.
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
        if fail_triggered.load(Ordering::SeqCst) {
            // We only need to trigger failure once.
            return;
        }

        let current_node = sui_simulator::current_simnode_id();
        if target_node_id != current_node {
            return;
        }

        tracing::warn!("crashing node {current_node} for {:?}", crash_duration);
        fail_triggered.store(true, Ordering::SeqCst);
        sui_simulator::task::kill_current_node(Some(crash_duration));
        // Do not put any code after this point, as it won't be executed.
        // kill_current_node is implemented using a panic.
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
            simtest_utils::start_background_workload(client_arc.clone(), false, None, None);

        // Run the workload to get some data in the system.
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

        let node_health_info = simtest_utils::get_nodes_health_info(&walrus_cluster.nodes).await;

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
            simtest_utils::get_nodes_health_info([&walrus_cluster.nodes[5]])
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

    // TODO(WAL-896): Extend this test to include the following scenarios:
    // - The catching-up node crashes and restarts.
    // - A blob is registered and certified in different epochs.
    // - A blob is deleted.
    // - A blob is marked as invalid.
    // - The cursor of the event-blob writer is lagging more than MAX_EPOCHS_AHEAD epochs, but the
    //   node cursor is not.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_recovery_with_incomplete_history() {
        const MAX_EPOCHS_AHEAD: EpochCount = 3;
        const EPOCH_DURATION: Duration = Duration::from_secs(30);

        // We need to wait at least until `MAX_EPOCHS_AHEAD + 1` until the first event blob is
        // expired. The additional +2 is to account for the fact that the first event blob may only
        // be certified in a later epoch.
        const TARGET_EPOCH: EpochCount = MAX_EPOCHS_AHEAD + 1 + 2;

        // Tracks if a node enters recovery mode with incomplete history.
        let recovery_with_incomplete_history_triggered = Arc::new(AtomicBool::new(false));
        // Tracks if a node starts catchup using event blobs.
        let catchup_using_event_blobs_triggered = Arc::new(AtomicBool::new(false));

        {
            let recovery_with_incomplete_history_triggered =
                recovery_with_incomplete_history_triggered.clone();
            register_fail_point("fail_point_recovery_with_incomplete_history", move || {
                recovery_with_incomplete_history_triggered.store(true, Ordering::SeqCst);
            });
            let catchup_using_event_blobs_triggered = catchup_using_event_blobs_triggered.clone();
            register_fail_point("fail_point_catchup_using_event_blobs_start", move || {
                catchup_using_event_blobs_triggered.store(true, Ordering::SeqCst);
            });
        }

        let (_sui_cluster, mut walrus_cluster, client, _) =
            test_cluster::E2eTestSetupBuilder::new()
                .with_epoch_duration(EPOCH_DURATION)
                .with_max_epochs_ahead(MAX_EPOCHS_AHEAD)
                .with_test_nodes_config(TestNodesConfig {
                    node_weights: vec![1, 2, 3, 3, 4, 0],
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
        let workload_handle = simtest_utils::start_background_workload(
            client_arc.clone(),
            false,
            None,
            Some(MAX_EPOCHS_AHEAD),
        );

        tracing::info!("waiting for nodes to reach target epoch {}", TARGET_EPOCH);
        // We need to wait at least for`EPOCH_DURATION * TARGET_EPOCH`. We allow for some more
        // epochs to increase the test robustness.
        // TODO(WAL-896): Maybe better check for the first event blob to become expired?
        tokio::time::sleep(EPOCH_DURATION * TARGET_EPOCH).await;
        simtest_utils::wait_for_nodes_to_reach_epoch(
            &walrus_cluster.nodes[..4],
            TARGET_EPOCH,
            2 * EPOCH_DURATION,
        )
        .await;

        let new_node = &mut walrus_cluster.nodes[5];
        let storage_node_config = new_node.storage_node_config.clone();
        let new_node_id = SimStorageNodeHandle::spawn_node(
            Arc::new(RwLock::new(StorageNodeConfig {
                event_processor_config: EventProcessorConfig {
                    event_stream_catchup_min_checkpoint_lag: 0,
                    ..storage_node_config.event_processor_config
                },
                ..storage_node_config
            })),
            None,
            new_node.cancel_token.clone(),
        )
        .await
        .id();
        new_node.node_id = Some(new_node_id);
        tracing::info!("spawned a new node with node ID {}", new_node_id);

        let new_node_starting_epoch =
            simtest_utils::get_current_epoch_from_node(&walrus_cluster.nodes[5])
                .await
                .max(TARGET_EPOCH);

        tracing::info!("adding stake to the new node");
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

        let new_node_initial_persisted_event_progress =
            simtest_utils::get_nodes_health_info([&walrus_cluster.nodes[5]]).await[0]
                .event_progress
                .persisted;
        tracing::info!(
            "new node initial persisted event progress: {new_node_initial_persisted_event_progress}"
        );

        tracing::info!("waiting for the new node to catch up and recover");
        workload_handle.abort();
        tokio::time::sleep(Duration::from_secs(150)).await;

        tracing::info!("checking the cluster's health info");
        let node_health_info = simtest_utils::get_nodes_health_info(&walrus_cluster.nodes).await;
        let committees = client_arc
            .inner
            .get_latest_committees_in_test()
            .await
            .unwrap();
        let current_committee = committees.current_committee();
        let new_node_health_info = &node_health_info[5];

        assert!(current_committee.contains(&walrus_cluster.nodes[5].public_key));
        assert!(new_node_health_info.shard_detail.is_some());

        tracing::info!("checking that shards in the new node matches the shards in the committees");
        let shards_in_new_node_based_on_committee = committees
            .current_committee()
            .shards_for_node_public_key(&walrus_cluster.nodes[5].public_key);
        let shards_in_new_node_owned = new_node_health_info
            .shard_detail
            .as_ref()
            .unwrap()
            .owned
            .clone();
        assert_eq!(
            shards_in_new_node_based_on_committee.len(),
            shards_in_new_node_owned.len()
        );
        for shard in shards_in_new_node_owned {
            assert!(shards_in_new_node_based_on_committee.contains(&shard.shard));
        }

        assert_eq!(new_node_health_info.node_status, "Active");
        assert!(
            new_node_health_info.event_progress.persisted
                > new_node_initial_persisted_event_progress,
            "the new node should have persisted some new events"
        );

        blob_info_consistency_check
            .check_storage_node_consistency_from_epoch(new_node_starting_epoch);

        assert!(
            catchup_using_event_blobs_triggered.load(Ordering::SeqCst),
            "catchup using event blobs should be triggered"
        );
        let was_recovery_with_incomplete_history_triggered =
            recovery_with_incomplete_history_triggered.load(Ordering::SeqCst);
        tracing::info!(
            "recovery with incomplete history was triggered: {}",
            was_recovery_with_incomplete_history_triggered
        );
        // TODO(WAL-896): Make the test more robust and enable this again.
        // assert!(
        //     recovery_with_incomplete_history_triggered.load(Ordering::SeqCst),
        //     "recovery with incomplete history should be triggered"
        // );
        clear_fail_point("fail_point_recovery_with_incomplete_history");
        clear_fail_point("fail_point_catchup_using_event_blobs_start");
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
            simtest_utils::start_background_workload(client_arc.clone(), false, None, None);

        // Run the workload to get some data in the system.
        tokio::time::sleep(Duration::from_secs(60)).await;

        // Register a fail point to have a temporary pause in the first node recovery process that
        // is longer than epoch length.
        // Note that when a node is in RecoveryInProgress state, it will not start a new recovery
        // everytime when a new epoch change start event is processed. So here we only delay the
        // first recovery.
        let delay_triggered = Arc::new(AtomicBool::new(false));
        register_fail_point_async("start_node_recovery_entry", move || {
            let delay_triggered_clone = delay_triggered.clone();
            async move {
                if !delay_triggered_clone.load(Ordering::SeqCst) {
                    delay_triggered_clone.store(true, Ordering::SeqCst);
                    tracing::info!("delaying node recovery for 60s");
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }
            }
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

        let node_health_info = simtest_utils::get_nodes_health_info(&walrus_cluster.nodes).await;

        assert!(node_health_info[0].shard_detail.is_some());
        for shard in &node_health_info[0].shard_detail.as_ref().unwrap().owned {
            // For all the shards that the crashed node owns, they should be in ready state.
            assert_eq!(shard.status, ShardStatus::Ready);
        }

        assert_eq!(
            simtest_utils::get_nodes_health_info([&walrus_cluster.nodes[0]])
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

    // Tests that non-blocking, out of ordering event processing does not block event progress.
    #[ignore = "ignore integration simtests by default"]
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

        let workload_handle =
            simtest_utils::start_background_workload(Arc::new(client), true, None, None);

        // Run the workload to get some data in the system.
        tokio::time::sleep(Duration::from_secs(120)).await;

        workload_handle.abort();

        // Wait for event to catch up.
        tokio::time::sleep(Duration::from_secs(60)).await;

        // Wait for all nodes to have event_progress.pending < 10 with timeout
        let timeout = Duration::from_secs(60);
        let start_time = Instant::now();

        loop {
            let health_info = simtest_utils::get_nodes_health_info(&walrus_cluster.nodes).await;
            let mut some_nodes_have_long_pending_events = false;

            for (index, node_health) in health_info.iter().enumerate() {
                tracing::info!(
                    "checking node event progress, index {index}, node id: {:?}, \
                    event progress: {:?}",
                    node_health.public_key,
                    node_health.event_progress
                );

                if node_health.event_progress.pending >= 10 {
                    some_nodes_have_long_pending_events = true;
                    break;
                }
            }

            if !some_nodes_have_long_pending_events {
                tracing::info!("all nodes do not have long pending events");
                break;
            }

            if start_time.elapsed() > timeout {
                panic!("timeout waiting for all nodes to consume pending events",);
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        blob_info_consistency_check.check_storage_node_consistency();
    }

    // Tests that when a node is in RecoveryInProgress state, restarting the node repeatedly
    // will not cause the node to be stuck/malfunction.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_recovery_in_progress_with_node_restart() {
        let (_sui_cluster, walrus_cluster, client, _) = test_cluster::E2eTestSetupBuilder::new()
            .with_epoch_duration(Duration::from_secs(30))
            .with_test_nodes_config(TestNodesConfig {
                node_weights: vec![1, 2, 3, 3, 4],
                use_legacy_event_processor: false,
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
            simtest_utils::start_background_workload(client_arc.clone(), false, None, None);

        // Run the workload to get some data in the system.
        tokio::time::sleep(Duration::from_secs(60)).await;

        // Register a fail point to have a temporary pause in the first node recovery process that
        // is longer than epoch length.
        // Note that when a node is in RecoveryInProgress state, it will not start a new recovery
        // everytime when a new epoch change start event is processed. So here we only delay the
        // first recovery.
        let delay_triggered = Arc::new(AtomicBool::new(false));
        register_fail_point_async("start_node_recovery_entry", move || {
            let delay_triggered_clone = delay_triggered.clone();
            async move {
                if !delay_triggered_clone.load(Ordering::SeqCst) {
                    delay_triggered_clone.store(true, Ordering::SeqCst);
                    tracing::info!("delaying node recovery for 60s");
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }
            }
        });

        // First, trigger a node crash with long delay to bring node into recovery mode.
        {
            // Tracks if a crash has been triggered.
            let fail_triggered = Arc::new(AtomicBool::new(false));
            let target_fail_node_id = walrus_cluster.nodes[0]
                .node_id
                .expect("node id should be set");
            let fail_triggered_clone = fail_triggered.clone();

            register_fail_point("fail_point_process_event", move || {
                crash_target_node(
                    target_fail_node_id,
                    fail_triggered_clone.clone(),
                    Duration::from_secs(60),
                );
            });

            // Wait until fail_triggered is set to true with a timeout.
            let timeout = Instant::now() + Duration::from_secs(20);
            while !fail_triggered.load(Ordering::SeqCst) {
                if Instant::now() > timeout {
                    panic!("fail_triggered is not set to true within 20 seconds");
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }

        // During recovery, repeatedly restart the node.
        {
            let next_fail_triggered = Arc::new(Mutex::new(Instant::now()));
            let next_fail_triggered_clone = next_fail_triggered.clone();
            let crash_end_time = Instant::now() + Duration::from_secs(120);
            let target_fail_node_id = walrus_cluster.nodes[0]
                .node_id
                .expect("node id should be set");

            sui_macros::register_fail_points(CRASH_NODE_FAIL_POINTS, move || {
                repeatedly_crash_target_node(
                    target_fail_node_id,
                    next_fail_triggered_clone.clone(),
                    crash_end_time,
                    NodeCrashConfig {
                        min_crash_duration_secs: 1,
                        max_crash_duration_secs: 3,
                        min_live_duration_secs: 5,
                        max_live_duration_secs: 40,
                    },
                );
            });
        }

        tokio::time::sleep(Duration::from_secs(180)).await;

        let node_health_info = simtest_utils::get_nodes_health_info(&walrus_cluster.nodes).await;

        assert!(node_health_info[0].shard_detail.is_some());
        for shard in &node_health_info[0].shard_detail.as_ref().unwrap().owned {
            // For all the shards that the crashed node owns, they should be in ready state.
            assert_eq!(shard.status, ShardStatus::Ready);
        }

        assert_eq!(
            simtest_utils::get_nodes_health_info([&walrus_cluster.nodes[0]])
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

    // Tests upgrading the walrus contracts.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_quorum_contract_upgrade() -> anyhow::Result<()> {
        let deploy_dir = tempfile::TempDir::new().unwrap();
        let epoch_duration = Duration::from_secs(30);
        let (_sui_cluster_handle, mut walrus_cluster, client, system_ctx) =
            test_cluster::E2eTestSetupBuilder::new()
                .with_deploy_directory(deploy_dir.path().to_path_buf())
                .with_delegate_governance_to_admin_wallet()
                .with_contract_directory(testnet_contract_dir().unwrap())
                .with_epoch_duration(epoch_duration)
                .with_num_checkpoints_per_blob(20)
                .build_generic::<SimStorageNodeHandle>()
                .await?;
        let client = Arc::new(client);
        let previous_version = client
            .as_ref()
            .inner
            .sui_client()
            .read_client()
            .system_object_version()
            .await?;

        // Copy new contracts to fresh directory
        let upgrade_dir = TempDir::new()?;
        copy_recursively(development_contract_dir()?, upgrade_dir.path()).await?;

        // Copy Move.lock files of walrus contract and dependencies to new directory
        for contract in ["wal", "walrus"] {
            std::fs::copy(
                deploy_dir.path().join(contract).join("Move.lock"),
                upgrade_dir.path().join(contract).join("Move.lock"),
            )?;
        }

        // Change the version in the contracts
        let walrus_package_path = upgrade_dir.path().join("walrus");

        let upgrade_epoch = client.as_ref().inner.sui_client().current_epoch().await?;
        tracing::info!("upgrade_epoch: {}", upgrade_epoch);

        let digest = client
            .as_ref()
            .inner
            .sui_client()
            .read_client()
            .compute_package_digest(walrus_package_path.clone())
            .await?;

        for node in walrus_cluster.nodes.iter() {
            let node_id = node
                .storage_node_capability
                .as_ref()
                .expect("capability should be set")
                .node_id;
            client
                .as_ref()
                .inner
                .sui_client()
                .vote_for_upgrade_with_digest(system_ctx.upgrade_manager_object, node_id, digest)
                .await?;
        }

        tracing::info!("voted for upgrade");

        // Commit the upgrade in a loop to handle the case where the upgrade fails
        // with ENotEnoughVotes due to simtest environment not registering the
        // upgrade immediately.
        let new_package_id = loop {
            match client
                .as_ref()
                .inner
                .sui_client()
                .upgrade(
                    system_ctx.upgrade_manager_object,
                    walrus_package_path.clone(),
                    UpgradeType::Quorum,
                )
                .await
            {
                Ok(package_id) => break package_id,
                Err(e) => {
                    tracing::info!("Upgrade failed, retrying in 5 seconds: {:?}", e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        };

        // Set the migration epoch on the staking object to the following epoch.
        client
            .as_ref()
            .inner
            .sui_client()
            .set_migration_epoch(new_package_id)
            .await?;

        // Check that the upgrade was completed within one epoch. A failure here indicates that the
        // epoch duration for the test is set too short.
        let end_upgrade_epoch = client.as_ref().inner.sui_client().current_epoch().await?;
        assert_eq!(end_upgrade_epoch, upgrade_epoch);
        tracing::info!(upgrade_epoch, "upgraded contract");

        // Wait for the nodes to reach the migration epoch.
        let target_epoch = upgrade_epoch + 1;
        simtest_utils::wait_for_nodes_to_reach_epoch(
            &walrus_cluster.nodes[..4],
            target_epoch,
            2 * epoch_duration,
        )
        .await;

        // Migrate the objects
        client
            .as_ref()
            .inner
            .sui_client()
            .migrate_contracts(new_package_id)
            .await?;

        // Check the version
        assert_eq!(
            client
                .as_ref()
                .inner
                .sui_client()
                .read_client()
                .system_object_version()
                .await?,
            previous_version + 1
        );

        let blob_data = walrus_test_utils::random_data_list(314, 1);
        let blobs: Vec<&[u8]> = blob_data.iter().map(AsRef::as_ref).collect();
        let store_args = StoreArgs::default_with_epochs(1)
            .no_store_optimizations()
            .with_post_store(PostStoreAction::Keep)
            .with_persistence(BlobPersistence::Permanent);
        let _results = client
            .as_ref()
            .inner
            .reserve_and_store_blobs_retry_committees(&blobs, &store_args)
            .await?;

        let last_certified_event_blob =
            simtest_utils::get_last_certified_event_blob_must_succeed(&client).await;
        println!("last_certified_event_blob: {:?}", last_certified_event_blob);

        // Restart a random subset of nodes. This helps surface quorum issues
        // if some nodes fall behind during the upgrade.
        let num_nodes = walrus_cluster.nodes.len();
        let mut rng = rand::rngs::StdRng::from_entropy();
        // Restart at least one node and leave at least one running.
        let num_restart = rng.gen_range(1..num_nodes);
        let mut node_indices: Vec<usize> = (0..num_nodes).collect();
        node_indices.shuffle(&mut rng);
        for i in node_indices.into_iter().take(num_restart) {
            simtest_utils::restart_node_with_checkpoints(&mut walrus_cluster, i, |_| 20).await;
        }

        wait_for_event_blob_writer_to_make_progress(&client, last_certified_event_blob).await;

        // Store a blob after the upgrade to check if everything works after the upgrade.
        let blob_data = walrus_test_utils::random_data_list(314, 1);
        let blobs: Vec<&[u8]> = blob_data.iter().map(AsRef::as_ref).collect();

        let store_args = StoreArgs::default_with_epochs(1)
            .no_store_optimizations()
            .with_post_store(PostStoreAction::Keep)
            .with_persistence(BlobPersistence::Permanent);
        let _results = client
            .as_ref()
            .inner
            .reserve_and_store_blobs_retry_committees(&blobs, &store_args)
            .await?;

        Ok(())
    }

    async fn wait_for_event_blob_writer_to_make_progress(
        client: &Arc<WithTempDir<Client<SuiContractClient>>>,
        last_certified_event_blob: EventBlob,
    ) {
        loop {
            let last_certified_event_blob_after_upgrade =
                simtest_utils::get_last_certified_event_blob_must_succeed(&client).await;
            if last_certified_event_blob_after_upgrade.blob_id != last_certified_event_blob.blob_id
            {
                break;
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    // Basic test for single client workload.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_single_client_workload() {
        let blob_info_consistency_check = BlobInfoConsistencyCheck::new();

        let (_sui_cluster, _cluster, client, _) = test_cluster::E2eTestSetupBuilder::new()
            .with_test_nodes_config(TestNodesConfig {
                node_weights: vec![1, 2, 3, 3, 4],
                ..Default::default()
            })
            // Use a long epoch duration to avoid operation across epoch change.
            // TODO(WAL-937): shorten this and fix any issues exposed by this.
            .with_epoch_duration(Duration::from_secs(600))
            .with_communication_config(
                ClientCommunicationConfig::default_for_test_with_reqwest_timeout(
                    Duration::from_secs(2),
                ),
            )
            .build_generic::<SimStorageNodeHandle>()
            .await
            .unwrap();

        let metrics = Arc::new(ClientMetrics::new(&walrus_utils::metrics::Registry::new(
            prometheus::Registry::new(),
        )));

        let handle = tokio::spawn(async move {
            let single_client_workload = SingleClientWorkload::new(
                client.inner,
                60,
                true,
                1000,
                SizeDistributionConfig::Poisson {
                    lambda: 10.0,
                    size_multiplier: 1024,
                },
                StoreLengthDistributionConfig::Uniform {
                    min_epochs: 1,
                    max_epochs: 10,
                },
                RequestTypeDistributionConfig {
                    read_weight: 4,
                    write_permanent_weight: 7,
                    write_deletable_weight: 7,
                    delete_weight: 1,
                    extend_weight: 1,
                },
                metrics,
            );

            single_client_workload
                .run()
                .await
                .expect("single client workload exited with error");
        });

        tokio::time::sleep(Duration::from_secs(240)).await;

        handle.abort();

        blob_info_consistency_check.check_storage_node_consistency();
    }
}
