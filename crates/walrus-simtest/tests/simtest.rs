// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Contains dedicated Walrus simulation tests.
#![recursion_limit = "256"]

#[cfg(msim)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        sync::{atomic::AtomicBool, Arc, Mutex},
        time::Duration,
    };

    use anyhow::Context;
    use rand::{Rng, SeedableRng};
    use sui_macros::{
        clear_fail_point,
        register_fail_point,
        register_fail_point_arg,
        register_fail_point_async,
        register_fail_points,
    };
    use sui_protocol_config::ProtocolConfig;
    use sui_simulator::configs::{env_config, uniform_latency_ms};
    use sui_types::base_types::ObjectID;
    use tokio::{sync::RwLock, task::JoinHandle, time::Instant};
    use walrus_core::{
        encoding::{Primary, Secondary},
        keys::{NetworkKeyPair, ProtocolKeyPair},
        test_utils::random_blob_id,
        Epoch,
        DEFAULT_ENCODING,
    };
    use walrus_proc_macros::walrus_simtest;
    use walrus_sdk::api::{ServiceHealthInfo, ShardStatus};
    use walrus_service::{
        client::{responses::BlobStoreResult, Client, ClientCommunicationConfig, StoreWhen},
        node::config::{CommissionRateData, PathOrInPlace, StorageNodeConfig, SyncedNodeConfigSet},
        test_utils::{test_cluster, SimStorageNodeHandle, TestCluster, TestNodesConfig},
    };
    use walrus_sui::{
        client::{BlobPersistence, PostStoreAction, ReadClient, SuiContractClient},
        types::{
            move_structs::{EventBlob, VotingParams},
            NetworkAddress,
            NodeMetadata,
        },
    };
    use walrus_test_utils::{async_param_test, WithTempDir};

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

    /// Fetches the synced node config set from the contract.
    async fn test_get_synced_node_config_set(
        contract_client: &SuiContractClient,
        node_id: ObjectID,
    ) -> anyhow::Result<SyncedNodeConfigSet> {
        let pool = contract_client
            .read_client
            .get_staking_pool(node_id)
            .await?;

        let node_info = pool.node_info.clone();
        let metadata = contract_client
            .read_client
            .get_node_metadata(node_info.metadata)
            .await?;

        Ok(SyncedNodeConfigSet {
            name: node_info.name,
            network_address: node_info.network_address,
            network_public_key: node_info.network_public_key,
            public_key: node_info.public_key,
            next_public_key: node_info.next_epoch_public_key,
            voting_params: pool.voting_params,
            metadata,
            commission_rate_data: CommissionRateData {
                pending_commission_rate: pool.pending_commission_rate,
                commission_rate: pool.commission_rate,
            },
        })
    }

    // Helper function to write a random blob, read it back and check that it is the same.
    // If `write_only` is true, only write the blob and do not read it back.
    async fn write_read_and_check_random_blob(
        client: &WithTempDir<Client<SuiContractClient>>,
        data_length: usize,
        write_only: bool,
    ) -> anyhow::Result<()> {
        // Get a random epoch length for the blob to be stored.
        let epoch_ahead = rand::thread_rng().gen_range(1..=5);

        tracing::info!(
            "generating random blobs of length {data_length} and store them for {epoch_ahead} \
            epochs"
        );
        let blob = walrus_test_utils::random_data(data_length);

        let store_results = client
            .as_ref()
            .reserve_and_store_blobs_retry_committees(
                &[blob.as_slice()],
                DEFAULT_ENCODING,
                epoch_ahead,
                StoreWhen::Always,
                BlobPersistence::Permanent,
                PostStoreAction::Keep,
            )
            .await
            .context("store blob should not fail")?;

        tracing::info!(
            "got store results with {} items\n{:?}",
            store_results.len(),
            store_results
        );
        let store_result = &store_results
            .first()
            .expect("should have exactly one result");

        let BlobStoreResult::NewlyCreated {
            blob_object: blob_confirmation,
            ..
        } = store_result
        else {
            tracing::error!("unexpected store result type: {:?}", store_result);
            panic!("expect newly stored blob")
        };

        tracing::info!(
            "successfully stored blob with id {}",
            blob_confirmation.blob_id
        );

        if write_only {
            tracing::info!("write-only mode, skipping read verification");
            return Ok(());
        }

        tracing::info!("attempting to read blob using primary slivers");
        let mut read_blob_result = client
            .as_ref()
            .read_blob::<Primary>(&blob_confirmation.blob_id)
            .await;
        let mut retry_count = 0;
        while read_blob_result.is_err() {
            retry_count += 1;
            tracing::info!(
                "primary read attempt {} failed, retrying in 1s: {:?}",
                retry_count,
                read_blob_result.unwrap_err()
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
            read_blob_result = client
                .as_ref()
                .read_blob::<Primary>(&blob_confirmation.blob_id)
                .await;
        }

        let read_blob = read_blob_result.context("should be able to read blob we just stored")?;
        tracing::info!(
            "successfully read blob using primary slivers after {} retries",
            retry_count
        );

        // Check that blob is what we wrote.
        assert_eq!(read_blob, blob);

        tracing::info!("attempting to read blob using secondary slivers");
        let mut read_blob_result = client
            .as_ref()
            .read_blob::<Secondary>(&blob_confirmation.blob_id)
            .await;
        let mut retry_count = 0;
        while read_blob_result.is_err() {
            retry_count += 1;
            tracing::info!(
                "secondary read attempt {} failed, retrying in 1s: {:?}",
                retry_count,
                read_blob_result.unwrap_err()
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
            read_blob_result = client
                .as_ref()
                .read_blob::<Secondary>(&blob_confirmation.blob_id)
                .await;
        }
        let read_blob = read_blob_result.context("should be able to read blob we just stored")?;
        tracing::info!(
            "successfully read blob using secondary slivers after {} retries",
            retry_count
        );

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

    /// StorageNodeConfig update parameters for testing.
    #[derive(Debug, Clone, Default)]
    struct TestUpdateParams {
        pub name: Option<String>,
        pub public_host: Option<String>,
        pub public_port: Option<u16>,
        pub network_key_pair: Option<PathOrInPlace<NetworkKeyPair>>,
        pub next_protocol_key_pair: Option<PathOrInPlace<ProtocolKeyPair>>,
        pub voting_params: Option<VotingParams>,
        pub metadata: Option<NodeMetadata>,
        pub commission_rate: Option<u16>,
    }

    /// Update StorageNodeConfig based on TestUpdateParams where all fields are Options
    async fn update_config_from_test_params(
        client: &SuiContractClient,
        node_id: ObjectID,
        config: &Arc<RwLock<StorageNodeConfig>>,
        params: &TestUpdateParams,
    ) -> Result<(), anyhow::Error> {
        let remote_config = test_get_synced_node_config_set(client, node_id).await?;
        let mut config_write = config.write().await;

        // Update all optional fields
        if let Some(name) = &params.name {
            assert_ne!(&remote_config.name, name);
            config_write.name = name.clone();
        }

        if let Some(public_host) = &params.public_host {
            assert_ne!(&remote_config.network_address.get_host(), &public_host);
            config_write.public_host = public_host.clone();
        }

        if let Some(public_port) = params.public_port {
            assert_ne!(
                &remote_config
                    .network_address
                    .try_get_port()
                    .expect("should get port"),
                &Some(public_port)
            );
            config_write.public_port = public_port;
        }

        if let Some(network_key_pair) = &params.network_key_pair {
            assert_ne!(
                &remote_config.network_public_key,
                network_key_pair
                    .get()
                    .expect("should get network key pair")
                    .public()
            );
            config_write.network_key_pair = network_key_pair.clone();
        }

        if let Some(next_protocol_key_pair) = &params.next_protocol_key_pair {
            assert_ne!(
                remote_config.next_public_key.as_ref(),
                next_protocol_key_pair.get().map(|kp| kp.public())
            );
            config_write.next_protocol_key_pair = Some(next_protocol_key_pair.clone());
        }

        if let Some(voting_params) = &params.voting_params {
            assert_ne!(&remote_config.voting_params, voting_params);
            config_write.voting_params = voting_params.clone();
        }

        if let Some(metadata) = &params.metadata {
            assert_ne!(&remote_config.metadata, metadata);
            config_write.metadata = metadata.clone();
        }

        if let Some(commission_rate) = params.commission_rate {
            config_write.commission_rate = commission_rate;
        }

        Ok(())
    }

    /// Wait until the node's configuration is synced with the on-chain state.
    async fn wait_till_node_config_synced(
        client: &SuiContractClient,
        node_id: ObjectID,
        config: &Arc<RwLock<StorageNodeConfig>>,
        timeout: Duration,
    ) -> Result<(), anyhow::Error> {
        let start_time = std::time::Instant::now();

        loop {
            if start_time.elapsed() > timeout {
                return Err(anyhow::anyhow!("Timed out waiting for node config to sync"));
            }

            // Fetch the remote config.
            let remote_config = test_get_synced_node_config_set(client, node_id).await?;

            let local_config = config.read().await;

            tracing::info!(
                "Comparing local and remote configs:\n\
                local.name: {:?}, remote.name: {:?}\n\
                local.network_address: {:?}, remote.network_address: {:?}\n\
                local.voting_params: {:?}, remote.voting_params: {:?}\n\
                local.metadata: {:?}, remote.metadata: {:?}\n\
                local.commission_rate: {:?}, remote.commission_rate: {:?}\n\
                local.network_public_key: {:?}, remote.network_public_key: {:?}\n\
                local.next_protocol_public_key: {:?}\n\
                local.public_key: {:?}, remote.public_key: {:?}",
                local_config.name,
                remote_config.name,
                NetworkAddress(format!(
                    "{}:{}",
                    local_config.public_host, local_config.public_port
                )),
                remote_config.network_address,
                local_config.voting_params,
                remote_config.voting_params,
                local_config.metadata,
                remote_config.metadata,
                local_config.commission_rate,
                remote_config.commission_rate_data.commission_rate,
                local_config.network_key_pair().public(),
                remote_config.network_public_key,
                local_config.next_protocol_key_pair().map(|kp| kp.public()),
                local_config.protocol_key_pair().public(),
                remote_config.public_key,
            );

            let configs_match = remote_config.name == local_config.name
                && remote_config.network_address
                    == NetworkAddress(format!(
                        "{}:{}",
                        local_config.public_host, local_config.public_port
                    ))
                && remote_config.voting_params == local_config.voting_params
                && remote_config.metadata == local_config.metadata
                && remote_config.commission_rate_data.commission_rate
                    == local_config.commission_rate
                && &remote_config.network_public_key == local_config.network_key_pair().public()
                && (local_config.next_protocol_key_pair.is_none()
                    && &remote_config.public_key == local_config.protocol_key_pair().public());

            if configs_match {
                tracing::info!("Node config is now in sync with on-chain state\n");
                return Ok(());
            }
            // Release the read lock explicitly before sleeping.
            drop(local_config);

            tokio::time::sleep(Duration::from_secs(5)).await;
        }
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

        let (_sui_cluster, _cluster, client) =
            test_cluster::default_setup_with_num_checkpoints_generic::<SimStorageNodeHandle>(
                Duration::from_secs(60 * 60),
                TestNodesConfig {
                    node_weights: vec![1, 2, 3, 3, 4],
                    ..Default::default()
                },
                Some(10),
                ClientCommunicationConfig::default_for_test(),
                false,
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
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn walrus_with_single_node_crash_and_restart() {
        let _guard = ProtocolConfig::apply_overrides_for_testing(|_, mut config| {
            // TODO: remove once Sui simtest can work with these features.
            config.set_enable_jwk_consensus_updates_for_testing(false);
            config.set_random_beacon_for_testing(false);
            config
        });

        let (sui_cluster, _walrus_cluster, client) =
            test_cluster::default_setup_with_num_checkpoints_generic::<SimStorageNodeHandle>(
                Duration::from_secs(60 * 60),
                TestNodesConfig {
                    node_weights: vec![1, 2, 3, 3, 4],
                    ..Default::default()
                },
                Some(10),
                ClientCommunicationConfig::default_for_test(),
                false,
            )
            .await
            .unwrap();

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

    /// Helper function to get health info for a single node.
    async fn wait_until_node_is_active(
        node: &SimStorageNodeHandle,
        timeout: Duration,
    ) -> anyhow::Result<ServiceHealthInfo> {
        let client = walrus_sdk::client::Client::builder()
            .authenticate_with_public_key(node.network_public_key.clone())
            // Disable proxy and root certs from the OS for tests.
            .no_proxy()
            .tls_built_in_root_certs(false)
            .build_for_remote_ip(node.rest_api_address)
            .context("failed to create node client")?;

        let start = tokio::time::Instant::now();

        loop {
            if start.elapsed() > timeout {
                anyhow::bail!(
                    "timed out waiting for node to become active after {:?}",
                    timeout
                );
            }

            match client.get_server_health_info(true).await {
                Ok(info) if info.node_status == "Active" => {
                    return Ok(info);
                }
                Ok(info) => {
                    tracing::debug!("node status: {}, waiting...", info.node_status);
                }
                Err(e) => {
                    tracing::debug!("failed to get node health info: {}, retrying...", e);
                }
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
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
        let (_sui_cluster, walrus_cluster, client) =
            test_cluster::default_setup_with_num_checkpoints_generic::<SimStorageNodeHandle>(
                Duration::from_secs(30),
                TestNodesConfig {
                    node_weights: vec![1, 2, 3, 3, 4],
                    use_legacy_event_processor: true,
                    disable_event_blob_writer: false,
                    blocklist_dir: None,
                    enable_node_config_synchronizer: false,
                },
                None,
                ClientCommunicationConfig::default_for_test_with_reqwest_timeout(
                    Duration::from_secs(2),
                ),
                false,
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
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_repeated_node_crash() {
        // We use a very short epoch duration of 10 seconds so that we can exercise more epoch
        // changes in the test.
        let (_sui_cluster, walrus_cluster, client) =
            test_cluster::default_setup_with_num_checkpoints_generic::<SimStorageNodeHandle>(
                Duration::from_secs(10),
                TestNodesConfig {
                    node_weights: vec![1, 2, 3, 3, 4],
                    use_legacy_event_processor: true,
                    disable_event_blob_writer: false,
                    blocklist_dir: None,
                    enable_node_config_synchronizer: false,
                },
                None,
                ClientCommunicationConfig::default_for_test_with_reqwest_timeout(
                    Duration::from_secs(1),
                ),
                false,
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

        let certified_blob_digest_map = Arc::new(Mutex::new(HashMap::new()));
        let certified_blob_digest_map_clone = certified_blob_digest_map.clone();
        register_fail_point_arg(
            "storage_node_certified_blob_digest",
            move || -> Option<Arc<Mutex<HashMap<Epoch, HashMap<ObjectID, u64>>>>> {
                Some(certified_blob_digest_map_clone.clone())
            },
        );

        // We use a very short epoch duration of 60 seconds so that we can exercise more epoch
        // changes in the test.
        let mut node_weights = vec![2, 2, 3, 3, 3];
        let (_sui_cluster, walrus_cluster, client) =
            test_cluster::default_setup_with_num_checkpoints_generic::<SimStorageNodeHandle>(
                Duration::from_secs(30),
                TestNodesConfig {
                    node_weights: node_weights.clone(),
                    use_legacy_event_processor: true,
                    disable_event_blob_writer: false,
                    blocklist_dir: None,
                    enable_node_config_synchronizer: false,
                },
                Some(100),
                ClientCommunicationConfig::default_for_test_with_reqwest_timeout(
                    Duration::from_secs(2),
                ),
                false,
            )
            .await
            .unwrap();

        let client_arc = Arc::new(client);
        let workload_handle = start_background_workload(client_arc.clone(), true);

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

        // Ensure that for all the epoch, all the nodes have the same certified blob digest.
        let digest_map = certified_blob_digest_map.lock().unwrap();
        for (epoch, node_digest_map) in digest_map.iter() {
            // Ensure that for the same epoch, all the nodes have the same certified blob digest.
            let mut epoch_digest = None;
            for (node_id, digest) in node_digest_map.iter() {
                tracing::info!(
                    "blob info consistency check: node {node_id} has digest \
                    {digest} in epoch {epoch}",
                );
                if epoch_digest.is_none() {
                    epoch_digest = Some(digest);
                } else {
                    assert_eq!(epoch_digest, Some(digest));
                }
            }
        }
    }

    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_new_node_joining_cluster() {
        register_fail_point("fail_point_direct_shard_sync_recovery", move || {
            panic!("shard sync should not enter recovery mode in this test");
        });

        let (_sui_cluster, mut walrus_cluster, client) =
            test_cluster::default_setup_with_num_checkpoints_generic::<SimStorageNodeHandle>(
                Duration::from_secs(30),
                TestNodesConfig {
                    node_weights: vec![1, 2, 3, 3, 4, 0],
                    ..Default::default()
                },
                Some(10),
                ClientCommunicationConfig::default_for_test_with_reqwest_timeout(
                    Duration::from_secs(2),
                ),
                false,
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
            register_fail_points(DB_FAIL_POINTS, move || {
                crash_target_node(
                    target_fail_node_id,
                    fail_triggered_clone.clone(),
                    Duration::from_secs(5),
                );
            });
        }

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
        clear_fail_point("fail_point_direct_shard_sync_recovery");
    }

    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_sync_node_config_params_basic() {
        let (_sui_cluster, mut walrus_cluster, client) =
            test_cluster::default_setup_with_num_checkpoints_generic::<SimStorageNodeHandle>(
                Duration::from_secs(30),
                TestNodesConfig {
                    node_weights: vec![1, 2, 3, 3, 4, 0],
                    use_legacy_event_processor: false,
                    disable_event_blob_writer: false,
                    blocklist_dir: None,
                    enable_node_config_synchronizer: true,
                },
                Some(10),
                ClientCommunicationConfig::default_for_test_with_reqwest_timeout(
                    Duration::from_secs(2),
                ),
                false,
            )
            .await
            .expect("Failed to setup test cluster");

        assert!(walrus_cluster.nodes[5].node_id.is_none());
        let client_arc = Arc::new(client);

        let new_address = walrus_service::test_utils::unused_socket_address(true);
        let network_key_pair = walrus_core::keys::NetworkKeyPair::generate();
        // Generate random voting params
        let voting_params = VotingParams {
            storage_price: rand::thread_rng().gen_range(1..1000),
            write_price: rand::thread_rng().gen_range(1..100),
            node_capacity: rand::thread_rng().gen_range(1_000_000..1_000_000_000),
        };
        let metadata = NodeMetadata::new(
            "https://walrus.io/images/walrus.jpg".to_string(),
            "https://walrus.io".to_string(),
            "Alias for walrus is sea elephant".to_string(),
        );

        // Check that the registered node has the original network address.
        let pool = client_arc
            .as_ref()
            .as_ref()
            .sui_client()
            .read_client
            .get_staking_pool(
                walrus_cluster.nodes[5]
                    .storage_node_capability
                    .as_ref()
                    .unwrap()
                    .node_id,
            )
            .await
            .expect("Failed to get staking pool");
        assert_eq!(
            pool.node_info.network_address,
            walrus_cluster.nodes[5]
                .storage_node_config
                .rest_api_address
                .into()
        );

        // Make sure the new params are different from the on-chain values.
        assert_ne!(
            NetworkAddress::from(new_address),
            pool.node_info.network_address
        );
        assert_ne!(pool.voting_params, voting_params);
        assert_ne!(
            &pool.node_info.network_public_key,
            network_key_pair.public()
        );
        let metadata_on_chain = client_arc
            .as_ref()
            .as_ref()
            .sui_client()
            .read_client
            .get_node_metadata(pool.node_info.metadata)
            .await
            .expect("Failed to get node metadata");
        assert_ne!(metadata, metadata_on_chain);

        // Update the node config with the new params.
        walrus_cluster.nodes[5].storage_node_config.public_port = new_address.port();
        walrus_cluster.nodes[5].storage_node_config.public_host = new_address.ip().to_string();
        walrus_cluster.nodes[5].storage_node_config.rest_api_address = new_address;
        walrus_cluster.nodes[5].storage_node_config.network_key_pair =
            network_key_pair.clone().into();
        walrus_cluster.nodes[5].storage_node_config.voting_params = voting_params.clone();
        walrus_cluster.nodes[5].rest_api_address = new_address;
        walrus_cluster.nodes[5].network_public_key = network_key_pair.public().clone();
        walrus_cluster.nodes[5].storage_node_config.metadata = metadata.clone();

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

        tokio::time::sleep(Duration::from_secs(65)).await;

        let committees = client_arc
            .inner
            .get_latest_committees_in_test()
            .await
            .expect("Should get committees");

        assert!(committees
            .current_committee()
            .find_by_public_key(&walrus_cluster.nodes[5].public_key)
            .is_some());

        // Check that the new params are updated on-chain.
        let pool = client_arc
            .as_ref()
            .as_ref()
            .sui_client()
            .read_client
            .get_staking_pool(
                walrus_cluster.nodes[5]
                    .storage_node_capability
                    .as_ref()
                    .unwrap()
                    .node_id,
            )
            .await
            .expect("Should get staking pool");
        assert_eq!(
            pool.node_info.network_address,
            walrus_cluster.nodes[5]
                .storage_node_config
                .rest_api_address
                .into()
        );
        assert_eq!(
            &pool.node_info.network_public_key,
            walrus_cluster.nodes[5]
                .storage_node_config
                .network_key_pair()
                .public()
        );
        assert_eq!(pool.voting_params, voting_params);

        let metadata_on_chain = client_arc
            .as_ref()
            .as_ref()
            .sui_client()
            .read_client
            .get_node_metadata(pool.node_info.metadata)
            .await
            .expect("Failed to get node metadata");
        assert_eq!(metadata, metadata_on_chain);

        assert_eq!(
            get_nodes_health_info(&[&walrus_cluster.nodes[5]])
                .await
                .get(0)
                .unwrap()
                .node_status,
            "Active"
        );
    }

    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_registered_node_update_protocol_key() {
        let (_sui_cluster, walrus_cluster, client) =
            test_cluster::default_setup_with_num_checkpoints_generic::<SimStorageNodeHandle>(
                Duration::from_secs(10),
                TestNodesConfig {
                    node_weights: vec![1, 2, 3, 3, 4, 2],
                    use_legacy_event_processor: false,
                    disable_event_blob_writer: false,
                    blocklist_dir: None,
                    enable_node_config_synchronizer: true,
                },
                Some(10),
                ClientCommunicationConfig::default_for_test_with_reqwest_timeout(
                    Duration::from_secs(2),
                ),
                false,
            )
            .await
            .unwrap();

        assert!(walrus_cluster.nodes[5].node_id.is_some());
        let client_arc = Arc::new(client);

        // Generate new protocol key pair
        let new_protocol_key_pair = walrus_core::keys::ProtocolKeyPair::generate();

        let config = walrus_cluster.nodes[5].node_config_arc.clone();
        // Make sure the new protocol key is different from the current one
        assert_ne!(
            config.read().await.protocol_key_pair().public(),
            new_protocol_key_pair.public()
        );
        config.write().await.next_protocol_key_pair = Some(new_protocol_key_pair.clone().into());

        wait_till_node_config_synced(
            &client_arc.as_ref().as_ref().sui_client(),
            walrus_cluster.nodes[5]
                .storage_node_capability
                .as_ref()
                .unwrap()
                .node_id,
            &walrus_cluster.nodes[5].node_config_arc,
            Duration::from_secs(100),
        )
        .await
        .expect("Node config should be synced");

        // Check that the protocol key in StakePool is updated to the new one
        let pool = client_arc
            .as_ref()
            .as_ref()
            .sui_client()
            .read_client
            .get_staking_pool(
                walrus_cluster.nodes[5]
                    .storage_node_capability
                    .as_ref()
                    .unwrap()
                    .node_id,
            )
            .await
            .expect("Failed to get staking pool");

        assert_eq!(&pool.node_info.public_key, new_protocol_key_pair.public());
        let public_key = config.read().await.protocol_key_pair().public().clone();
        assert_eq!(&public_key, new_protocol_key_pair.public());

        wait_until_node_is_active(&walrus_cluster.nodes[5], Duration::from_secs(100))
            .await
            .expect("Node should be active");
    }

    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_node_config_synchronizer() {
        let (_sui_cluster, mut walrus_cluster, client) =
            test_cluster::default_setup_with_num_checkpoints_generic::<SimStorageNodeHandle>(
                Duration::from_secs(30),
                TestNodesConfig {
                    node_weights: vec![1, 2, 3, 3, 4, 0],
                    use_legacy_event_processor: false,
                    disable_event_blob_writer: false,
                    blocklist_dir: None,
                    enable_node_config_synchronizer: true,
                },
                Some(10),
                ClientCommunicationConfig::default_for_test_with_reqwest_timeout(
                    Duration::from_secs(2),
                ),
                false,
            )
            .await
            .unwrap();

        assert!(walrus_cluster.nodes[5].node_id.is_none());
        let client_arc = Arc::new(client);

        // Get current committee and verify node[5] is in it
        let committees = client_arc
            .inner
            .get_latest_committees_in_test()
            .await
            .expect("Should get committees");

        assert!(committees
            .current_committee()
            .find_by_public_key(&walrus_cluster.nodes[5].public_key)
            .is_none());

        let config = Arc::new(RwLock::new(
            walrus_cluster.nodes[5].storage_node_config.clone(),
        ));
        walrus_cluster.nodes[5].node_id = Some(
            SimStorageNodeHandle::spawn_node(
                config.clone(),
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

        wait_until_node_is_active(&walrus_cluster.nodes[5], Duration::from_secs(100))
            .await
            .expect("Node should be active");

        // Generate new protocol key pair
        let new_protocol_key_pair = walrus_core::keys::ProtocolKeyPair::generate();
        // Update the next protocol key pair in the node's config,
        // The config will be loaded by the config synchronizer.
        config.write().await.next_protocol_key_pair = Some(new_protocol_key_pair.clone().into());
        tracing::debug!(
            "current protocol key: {:?}, next protocol key: {:?}",
            config.read().await.protocol_key_pair().public(),
            config
                .read()
                .await
                .next_protocol_key_pair()
                .as_ref()
                .map(|kp| kp.public())
        );

        wait_for_public_key_change(
            &config,
            new_protocol_key_pair.public(),
            Duration::from_secs(100),
        )
        .await
        .expect("Protocol key should be updated");

        // Check that the protocol key in StakePool is updated to the new one
        let pool = client_arc
            .as_ref()
            .as_ref()
            .sui_client()
            .read_client
            .get_staking_pool(
                walrus_cluster.nodes[5]
                    .storage_node_capability
                    .as_ref()
                    .unwrap()
                    .node_id,
            )
            .await
            .expect("Failed to get staking pool");

        assert_eq!(&pool.node_info.public_key, new_protocol_key_pair.public());
        let public_key = config.read().await.protocol_key_pair().public().clone();
        assert_eq!(&public_key, new_protocol_key_pair.public());
        assert_eq!(
            new_protocol_key_pair.public(),
            config.read().await.protocol_key_pair().public()
        );
    }

    /// Waits until the node's protocol key pair in the config matches the target public key.
    /// Returns Ok(()) if the key matches within the timeout duration, or an error if it times out.
    async fn wait_for_public_key_change(
        config: &Arc<RwLock<StorageNodeConfig>>,
        target_public_key: &walrus_core::PublicKey,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        let start = tokio::time::Instant::now();

        loop {
            if start.elapsed() > timeout {
                anyhow::bail!(
                    "timed out waiting for public key change after {:?}",
                    timeout
                );
            }

            let current_public_key = config.read().await.protocol_key_pair().public().clone();
            if &current_public_key == target_public_key {
                return Ok(());
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    // The node recovery process is artificially prolonged to be longer than 1 epoch.
    // We should expect the recovering node should eventually become Active.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_long_node_recovery() {
        let (_sui_cluster, walrus_cluster, client) =
            test_cluster::default_setup_with_num_checkpoints_generic::<SimStorageNodeHandle>(
                Duration::from_secs(30),
                TestNodesConfig {
                    node_weights: vec![1, 2, 3, 3, 4],
                    use_legacy_event_processor: true,
                    disable_event_blob_writer: false,
                    blocklist_dir: None,
                    enable_node_config_synchronizer: false,
                },
                None,
                ClientCommunicationConfig::default_for_test_with_reqwest_timeout(
                    Duration::from_secs(2),
                ),
                false,
            )
            .await
            .unwrap();

        let client_arc = Arc::new(client);

        // Starts a background workload that a client keeps writing and retrieving data.
        // All requests should succeed even if a node crashes.
        let workload_handle = start_background_workload(client_arc.clone(), false);

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

        // Trigger node crash during some DB access.
        register_fail_points(DB_FAIL_POINTS, move || {
            crash_target_node(
                target_fail_node_id,
                fail_triggered_clone.clone(),
                Duration::from_secs(60),
            );
        });

        tokio::time::sleep(Duration::from_secs(180)).await;

        let node_refs: Vec<&SimStorageNodeHandle> = walrus_cluster.nodes.iter().collect();
        let node_health_info = get_nodes_health_info(&node_refs).await;

        assert!(node_health_info[0].shard_detail.is_some());
        for shard in &node_health_info[0].shard_detail.as_ref().unwrap().owned {
            // For all the shards that the crashed node owns, they should be in ready state.
            assert_eq!(shard.status, ShardStatus::Ready);
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

        clear_fail_point("start_node_recovery_entry");
    }

    async_param_test! {
        #[ignore = "ignore E2E tests by default"]
        #[walrus_simtest]
        test_sync_node_params : [
            commission_rate: (
                &TestUpdateParams {
                    commission_rate: Some(100),
                    ..Default::default()
                }
            ),
            voting_params: (
                &TestUpdateParams {
                    voting_params: Some(VotingParams {
                        storage_price: 100,
                        write_price: 100,
                        node_capacity: 100,
                    }),
                    ..Default::default()
                }
            ),
            metadata: (
                &TestUpdateParams {
                    metadata: Some(
                        NodeMetadata::new(
                            "https://walrus.io/images/walrus.jpg".to_string(),
                            "https://walrus.io".to_string(),
                            "Alias for walrus is sea elephant".to_string(),
                        )
                    ),
                    ..Default::default()
                }
            ),
            network_public_key: (
                &TestUpdateParams {
                    network_key_pair: Some(PathOrInPlace::InPlace(NetworkKeyPair::generate())),
                    ..Default::default()
                }
            ),
            next_protocol_key_pair: (
                &TestUpdateParams {
                    next_protocol_key_pair:
                        Some(PathOrInPlace::InPlace(ProtocolKeyPair::generate())),
                    ..Default::default()
                }
            ),
            network_address: (
                &TestUpdateParams {
                    public_port: Some(8080),
                    ..Default::default()
                }
            ),
            random_combi: (
                &{
                    let mut rng = rand::thread_rng();
                    let mut params = TestUpdateParams::default();

                    // For each field, randomly decide whether to set it (50% chance)
                    if rng.gen_bool(0.5) {
                        params.commission_rate = Some(rng.gen_range(0..=100));
                    }

                    if rng.gen_bool(0.5) {
                        params.voting_params = Some(VotingParams {
                            storage_price: rng.gen_range(1..1000),
                            write_price: rng.gen_range(1..100),
                            node_capacity: rng.gen_range(1_000_000..1_000_000_000),
                        });
                    }

                    if rng.gen_bool(0.5) {
                        params.metadata = Some(NodeMetadata::new(
                            "https://walrus.io/images/walrus.jpg".to_string(),
                            "https://walrus.io".to_string(),
                            "Random description".to_string(),
                        ));
                    }

                    if rng.gen_bool(0.5) {
                        params.network_key_pair =
                            Some(PathOrInPlace::InPlace(NetworkKeyPair::generate()));
                    }

                    if rng.gen_bool(0.5) {
                        params.next_protocol_key_pair =
                            Some(PathOrInPlace::InPlace(ProtocolKeyPair::generate()));
                    }

                    if rng.gen_bool(0.5) {
                        params.public_port = Some(rng.gen_range(8000..9000));
                    }

                    params
                }
            ),
        ]
    }
    async fn test_sync_node_params(update_params: &TestUpdateParams) {
        let (_sui_cluster, walrus_cluster, client) =
            test_cluster::default_setup_with_num_checkpoints_generic::<SimStorageNodeHandle>(
                Duration::from_secs(10),
                TestNodesConfig {
                    node_weights: vec![1, 2, 3, 3, 4, 2],
                    use_legacy_event_processor: false,
                    disable_event_blob_writer: false,
                    blocklist_dir: None,
                    enable_node_config_synchronizer: true,
                },
                Some(10),
                ClientCommunicationConfig::default_for_test_with_reqwest_timeout(
                    Duration::from_secs(2),
                ),
                false,
            )
            .await
            .unwrap();

        assert!(walrus_cluster.nodes[5].node_id.is_some());
        let client_arc = Arc::new(client);

        let config = walrus_cluster.nodes[5].node_config_arc.clone();

        let node_id = walrus_cluster.nodes[5]
            .storage_node_capability
            .as_ref()
            .unwrap()
            .node_id;

        // Update the node config with the new params.
        update_config_from_test_params(
            &client_arc.as_ref().as_ref().sui_client(),
            node_id,
            &config,
            update_params,
        )
        .await
        .expect("Failed to update node config");

        // Wait for the node config to be synced again.
        wait_till_node_config_synced(
            &client_arc.as_ref().as_ref().sui_client(),
            node_id,
            &config,
            Duration::from_secs(100),
        )
        .await
        .expect("Node config should be synced");
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

    async fn wait_for_certification_stuck(
        client: &Arc<WithTempDir<Client<SuiContractClient>>>,
    ) -> EventBlob {
        let start = Instant::now();
        let mut last_blob_time = Instant::now();
        let mut last_blob = EventBlob {
            blob_id: random_blob_id(),
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

    /// This test verifies that the node can correctly recover from a forked event blob.
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
            )
            .await
            .unwrap();

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
    }
}
