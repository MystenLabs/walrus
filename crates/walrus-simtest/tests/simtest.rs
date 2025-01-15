// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Contains dedicated Walrus simulation tests.
#[cfg(msim)]
mod tests {
    use std::{
        collections::HashSet,
        sync::{atomic::AtomicBool, Arc, Mutex},
        time::Duration,
    };

    use anyhow::Context;
    use rand::{Rng, SeedableRng};
    use sui_macros::{register_fail_point_async, register_fail_points};
    use sui_protocol_config::ProtocolConfig;
    use sui_simulator::configs::{env_config, uniform_latency_ms};
    use tokio::{task::JoinHandle, time::Instant};
    use walrus_core::encoding::{Primary, Secondary};
    use walrus_proc_macros::walrus_simtest;
    use walrus_sdk::api::{ServiceHealthInfo, ShardStatus};
    use walrus_service::{
        client::{responses::BlobStoreResult, Client, ClientCommunicationConfig, StoreWhen},
        test_utils::{test_cluster, SimStorageNodeHandle},
    };
    use walrus_sui::client::{BlobPersistence, PostStoreAction, ReadClient, SuiContractClient};
    use walrus_test_utils::WithTempDir;

    const FAILURE_TRIGGER_PROBABILITY: f64 = 0.01;
    const DB_FAIL_POINTS: &[&str] = &[
        "batch-write-before",
        "batch-write-after",
        "put-cf-before",
        "put-cf-after",
        "delete-cf-before",
        "delete-cf-after",
        "create-cf-before",
    ];

    fn latency_config() -> sui_simulator::SimConfig {
        env_config(uniform_latency_ms(10..30), [])
    }

    // Helper function to write a random blob, read it back and check that it is the same.
    // If `write_only` is true, only write the blob and do not read it back.
    async fn write_read_and_check_random_blob(
        client: &WithTempDir<Client<SuiContractClient>>,
        data_length: usize,
        write_only: bool,
    ) -> anyhow::Result<()> {
        // Write a random blob.
        let blob = walrus_test_utils::random_data(data_length);

        let store_results = client
            .as_ref()
            .reserve_and_store_blobs_retry_epoch(
                &[blob.as_slice()],
                5,
                StoreWhen::Always,
                BlobPersistence::Permanent,
                PostStoreAction::Keep,
            )
            .await
            .context("store blob should not fail")?;
        let store_result = &store_results
            .first()
            .expect("should have exactly one result");

        let BlobStoreResult::NewlyCreated {
            blob_object: blob_confirmation,
            ..
        } = store_result
        else {
            panic!("expect newly stored blob")
        };

        if write_only {
            return Ok(());
        }

        // Read the blob using primary slivers. Retry because nodes may not have received the blob
        // certify event yet and until then the slivers will not be readable.
        let mut read_blob_result = client
            .as_ref()
            .read_blob::<Primary>(&blob_confirmation.blob_id)
            .await;
        while read_blob_result.is_err() {
            tokio::time::sleep(Duration::from_secs(1)).await;
            read_blob_result = client
                .as_ref()
                .read_blob::<Primary>(&blob_confirmation.blob_id)
                .await;
        }

        let read_blob = read_blob_result.context("should be able to read blob we just stored")?;

        // Check that blob is what we wrote.
        assert_eq!(read_blob, blob);

        // Read using secondary slivers and check the result. Retry because nodes may not have
        // received the blob certify event yet and until then the slivers will not be readable.
        let mut read_blob_result = client
            .as_ref()
            .read_blob::<Secondary>(&blob_confirmation.blob_id)
            .await;
        while read_blob_result.is_err() {
            tokio::time::sleep(Duration::from_secs(1)).await;
            read_blob_result = client
                .as_ref()
                .read_blob::<Secondary>(&blob_confirmation.blob_id)
                .await;
        }
        let read_blob = read_blob_result.context("should be able to read blob we just stored")?;
        assert_eq!(read_blob, blob);

        Ok(())
    }

    fn start_background_workload(
        client_clone: Arc<WithTempDir<Client<SuiContractClient>>>,
        write_only: bool,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut data_length = 64;
            loop {
                tracing::info!("writing data with size {data_length}");

                // TODO(#995): use stress client for better coverage of the workload.
                write_read_and_check_random_blob(client_clone.as_ref(), data_length, write_only)
                    .await
                    .expect("workload should not fail");

                tracing::info!("finished writing data with size {data_length}");

                data_length += 1;
            }
        })
    }

    // Tests that we can create a Walrus cluster with a Sui cluster and run basic
    // operations deterministically.
    #[walrus_simtest(check_determinism)]
    #[ignore = "ignore simtests by default"]
    async fn walrus_basic_determinism() {
        let _guard = ProtocolConfig::apply_overrides_for_testing(|_, mut config| {
            // TODO: remove once Sui simtest can work with these features.
            config.set_enable_jwk_consensus_updates_for_testing(false);
            config.set_random_beacon_for_testing(false);
            config
        });

        let (_sui_cluster, _cluster, client) =
            test_cluster::default_setup_with_epoch_duration_generic::<SimStorageNodeHandle>(
                Duration::from_secs(60 * 60),
                &[1, 2, 3, 3, 4],
                false,
                ClientCommunicationConfig::default_for_test(),
                None,
                Some(10),
            )
            .await
            .unwrap();

        write_read_and_check_random_blob(&client, 31415, false)
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
    }

    // Tests the scenario where a single node crashes and restarts.
    #[walrus_simtest]
    #[ignore = "ignore integration simtests by default"]
    async fn walrus_with_single_node_crash_and_restart() {
        let _guard = ProtocolConfig::apply_overrides_for_testing(|_, mut config| {
            // TODO: remove once Sui simtest can work with these features.
            config.set_enable_jwk_consensus_updates_for_testing(false);
            config.set_random_beacon_for_testing(false);
            config
        });

        let (sui_cluster, _walrus_cluster, client) =
            test_cluster::default_setup_with_epoch_duration_generic::<SimStorageNodeHandle>(
                Duration::from_secs(60 * 60),
                &[1, 2, 3, 3, 4],
                false,
                ClientCommunicationConfig::default_for_test(),
                None,
                Some(10),
            )
            .await
            .unwrap();

        // Tracks if a crash has been triggered.
        let fail_triggered = Arc::new(AtomicBool::new(false));

        // Do not fail any nodes in the sui cluster.
        let mut do_not_fail_nodes = sui_cluster
            .cluster()
            .all_node_handles()
            .iter()
            .map(|n| n.with(|n| n.get_sim_node_id()))
            .collect::<HashSet<_>>();
        do_not_fail_nodes.insert(sui_cluster.sim_node_handle().id());

        let fail_triggered_clone = fail_triggered.clone();
        register_fail_points(DB_FAIL_POINTS, move || {
            handle_failpoint(
                do_not_fail_nodes.clone(),
                fail_triggered_clone.clone(),
                FAILURE_TRIGGER_PROBABILITY,
            );
        });

        // Run workload and wait until a crash is triggered.
        let mut data_length = 31415;
        loop {
            // TODO(#995): use stress client for better coverage of the workload.
            write_read_and_check_random_blob(&client, data_length, false)
                .await
                .expect("workload should not fail");

            data_length += 1024;
            if fail_triggered.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
        }

        // Continue running the workload for another 60 seconds.
        let _ = tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                // TODO(#995): use stress client for better coverage of the workload.
                write_read_and_check_random_blob(&client, data_length, false)
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
    }

    // Action taken during a various failpoints. There is a chance with `probability` that the
    // current node will be crashed, and restarted after a random duration.
    fn handle_failpoint(
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

    /// Helper function to get health info for a list of nodes.
    async fn get_nodes_health_info(nodes: &[&SimStorageNodeHandle]) -> Vec<ServiceHealthInfo> {
        futures::future::join_all(
            nodes
                .iter()
                .map(|node_handle| async {
                    let client = walrus_sdk::client::Client::builder()
                        .authenticate_with_public_key(node_handle.network_public_key.clone())
                        // Disable proxy and root certs from the OS for tests.
                        .no_proxy()
                        .tls_built_in_root_certs(false)
                        .build_for_remote_ip(node_handle.rest_api_address)
                        .expect("create node client failed");
                    client
                        .get_server_health_info(true)
                        .await
                        .expect("getting server health info should succeed")
                })
                .collect::<Vec<_>>(),
        )
        .await
    }

    // Simulates node crash and restart with sim node id.
    // We only trigger the crash once.
    fn crash_target_node(
        target_node_id: sui_simulator::task::NodeId,
        fail_triggered: Arc<AtomicBool>,
    ) {
        if fail_triggered.load(std::sync::atomic::Ordering::SeqCst) {
            // We only need to trigger failure once.
            return;
        }

        let current_node = sui_simulator::current_simnode_id();
        if target_node_id != current_node {
            return;
        }

        tracing::warn!("crashing node {current_node} for 120 seconds");
        fail_triggered.store(true, std::sync::atomic::Ordering::SeqCst);
        sui_simulator::task::kill_current_node(Some(Duration::from_secs(120)));
    }

    // This integration test simulates a scenario where a node is lagging behind and recovers.
    #[ignore = "ignore E2E tests by default"]
    #[walrus_simtest]
    async fn test_lagging_node_recovery() {
        let (_sui_cluster, walrus_cluster, client) =
            test_cluster::default_setup_with_epoch_duration_generic::<SimStorageNodeHandle>(
                Duration::from_secs(30),
                &[1, 2, 3, 3, 4],
                true,
                ClientCommunicationConfig::default_for_test_with_reqwest_timeout(
                    Duration::from_secs(2),
                ),
                None,
                None,
            )
            .await
            .unwrap();

        let client_arc = Arc::new(client);

        // Starts a background workload that a client keeps writing and retrieving data.
        // All requests should succeed even if a node crashes.
        let workload_handle = start_background_workload(client_arc.clone(), false);

        // Running the workload for 60 seconds to get some data in the system.
        tokio::time::sleep(Duration::from_secs(60)).await;

        // Tracks if a crash has been triggered.
        let fail_triggered = Arc::new(AtomicBool::new(false));
        let target_fail_node_id = walrus_cluster.nodes[0]
            .node_id
            .expect("node id should be set");
        let fail_triggered_clone = fail_triggered.clone();

        // Trigger node crash during some DB access.
        register_fail_points(DB_FAIL_POINTS, move || {
            crash_target_node(target_fail_node_id, fail_triggered_clone.clone());
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
        let node_health_info = get_nodes_health_info(&node_refs).await;

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
            get_nodes_health_info(&[&walrus_cluster.nodes[0]])
                .await
                .get(0)
                .unwrap()
                .node_status,
            "Active"
        );

        workload_handle.abort();
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
    #[ignore = "ignore E2E tests by default"]
    #[walrus_simtest]
    async fn test_repeated_node_crash() {
        // We use a very short epoch duration of 10 seconds so that we can exercise more epoch
        // changes in the test.
        let (_sui_cluster, walrus_cluster, client) =
            test_cluster::default_setup_with_epoch_duration_generic::<SimStorageNodeHandle>(
                Duration::from_secs(10),
                &[1, 2, 3, 3, 4],
                true,
                ClientCommunicationConfig::default_for_test_with_reqwest_timeout(
                    Duration::from_secs(1),
                ),
                None,
                None,
            )
            .await
            .unwrap();

        let client_arc = Arc::new(client);
        let client_clone = client_arc.clone();

        // First, we inject some data into the cluster. Note that to control the test duration, we
        // stopped the workload once started crashing the node.
        let mut data_length = 64;
        let workload_start_time = Instant::now();
        loop {
            if workload_start_time.elapsed() > Duration::from_secs(20) {
                tracing::info!("generated 60s of data; stopping workload");
                break;
            }
            tracing::info!("writing data with size {data_length}");

            // TODO(#995): use stress client for better coverage of the workload.
            write_read_and_check_random_blob(client_clone.as_ref(), data_length, true)
                .await
                .expect("workload should not fail");

            tracing::info!("finished writing data with size {data_length}");

            data_length += 1;
        }

        let next_fail_triggered = Arc::new(Mutex::new(tokio::time::Instant::now()));
        let target_fail_node_id = walrus_cluster.nodes[0]
            .node_id
            .expect("node id should be set");
        let next_fail_triggered_clone = next_fail_triggered.clone();
        let crash_end_time = Instant::now() + Duration::from_secs(2 * 60);

        // Trigger node crash during some DB access.
        register_fail_points(DB_FAIL_POINTS, move || {
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
                walrus_cluster.nodes[0]
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
            let node_health_info = get_nodes_health_info(&[&walrus_cluster.nodes[0]]).await;
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
            get_nodes_health_info(&[&walrus_cluster.nodes[0]])
                .await
                .get(0)
                .unwrap()
                .node_status,
            "Active"
        );
    }

    // This test simulates a scenario where a node is repeatedly moving shards among storage nodes,
    // and a workload is running concurrently.
    #[ignore = "ignore E2E tests by default"]
    #[walrus_simtest(config = "latency_config()")]
    async fn test_repeated_shard_move_with_workload() {
        // Adding jitter in the epoch change start event so that different nodes don't start the
        // epoch change at the exact same time.
        register_fail_point_async("epoch_change_start_entry", || async move {
            tokio::time::sleep(Duration::from_millis(
                rand::rngs::StdRng::from_entropy().gen_range(0..=100),
            ))
            .await;
        });

        // We use a very short epoch duration of 60 seconds so that we can exercise more epoch
        // changes in the test.
        let (_sui_cluster, walrus_cluster, client) =
            test_cluster::default_setup_with_epoch_duration_generic::<SimStorageNodeHandle>(
                Duration::from_secs(30),
                &[1, 2, 3, 3, 4],
                true,
                ClientCommunicationConfig::default_for_test_with_reqwest_timeout(
                    Duration::from_secs(2),
                ),
                None,
                Some(100),
            )
            .await
            .unwrap();

        let client_arc = Arc::new(client);
        let workload_handle = start_background_workload(client_arc.clone(), true);

        // Run the workload for 60 seconds to get some data in the system.
        tokio::time::sleep(Duration::from_secs(60)).await;

        // Repeatedly move shards among storage nodes.
        for _i in 0..3 {
            let node_to_move_shard_into = rand::thread_rng().gen_range(0..=4);
            let shard_move_weight = rand::thread_rng().gen_range(1..=5);
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
                    test_cluster::FROST_PER_NODE_WEIGHT * shard_move_weight,
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
    }

    #[ignore = "ignore E2E tests by default"]
    #[walrus_simtest]
    async fn test_new_node_joining_cluster() {
        let (_sui_cluster, mut walrus_cluster, client) =
            test_cluster::default_setup_with_epoch_duration_generic::<SimStorageNodeHandle>(
                Duration::from_secs(30),
                &[1, 2, 3, 3, 4, 0],
                false,
                ClientCommunicationConfig::default_for_test_with_reqwest_timeout(
                    Duration::from_secs(2),
                ),
                None,
                Some(10),
            )
            .await
            .unwrap();

        assert!(walrus_cluster.nodes[5].node_id.is_none());

        let client_arc = Arc::new(client);

        // Starts a background workload that a client keeps writing and retrieving data.
        // All requests should succeed even if a node crashes.
        let workload_handle = start_background_workload(client_arc.clone(), false);

        // Running the workload for 60 seconds to get some data in the system.
        tokio::time::sleep(Duration::from_secs(90)).await;

        walrus_cluster.nodes[5].node_id = Some(
            SimStorageNodeHandle::spawn_node(
                walrus_cluster.nodes[5].storage_node_config.clone(),
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

        tokio::time::sleep(Duration::from_secs(150)).await;

        let node_refs: Vec<&SimStorageNodeHandle> = walrus_cluster.nodes.iter().collect();
        let node_health_info = get_nodes_health_info(&node_refs).await;

        let committees = client_arc
            .inner
            .get_latest_committees_in_test()
            .await
            .unwrap();

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
            get_nodes_health_info(&[&walrus_cluster.nodes[5]])
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
    }
}
