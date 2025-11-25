// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Contains simtest related to sync node parameters to the contract.

#![recursion_limit = "256"]

#[cfg(msim)]
mod tests {
    use std::{net::SocketAddr, sync::Arc, time::Duration};

    use rand::Rng;
    use sui_types::base_types::ObjectID;
    use tokio::sync::RwLock;
    use walrus_core::keys::{NetworkKeyPair, ProtocolKeyPair};
    use walrus_proc_macros::walrus_simtest;
    use walrus_service::{
        client::ClientCommunicationConfig,
        node::config::{CommissionRateData, PathOrInPlace, StorageNodeConfig, SyncedNodeConfigSet},
        test_utils::{SimStorageNodeHandle, TestNodesConfig, UnusedSocketAddress, test_cluster},
    };
    use walrus_simtest::test_utils::simtest_utils::{self, BlobInfoConsistencyCheck};
    use walrus_sui::{
        client::SuiContractClient,
        types::{NetworkAddress, NodeMetadata, move_structs::VotingParams},
    };
    use walrus_test_utils::async_param_test;

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

            let local_config = config.read().await.clone();

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
                tracing::info!("node config is now in sync with on-chain state");
                return Ok(());
            }

            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_sync_node_config_params_basic() {
        let (_sui_cluster, mut walrus_cluster, client, _) =
            test_cluster::E2eTestSetupBuilder::new()
                .with_epoch_duration(Duration::from_secs(30))
                .with_test_nodes_config(
                    TestNodesConfig::builder()
                        .with_node_weights(&[1, 2, 3, 3, 4, 0])
                        .with_enable_node_config_synchronizer()
                        .build(),
                )
                .with_communication_config(
                    ClientCommunicationConfig::default_for_test_with_reqwest_timeout(
                        Duration::from_secs(2),
                    ),
                )
                .build_generic::<SimStorageNodeHandle>()
                .await
                .expect("Failed to setup test cluster");

        let blob_info_consistency_check = BlobInfoConsistencyCheck::new();

        assert!(walrus_cluster.nodes[5].node_id.is_none());
        let client_arc = Arc::new(client);

        let new_address: SocketAddr = UnusedSocketAddress::DistinctIp
            .try_into()
            .expect("should get unused socket address");
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

        assert!(
            committees
                .current_committee()
                .find_by_public_key(&walrus_cluster.nodes[5].public_key)
                .is_some()
        );

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
            simtest_utils::get_nodes_health_info([&walrus_cluster.nodes[5]])
                .await
                .get(0)
                .unwrap()
                .node_status,
            "Active"
        );

        blob_info_consistency_check.check_storage_node_consistency();
    }

    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_registered_node_update_protocol_key() {
        let (_sui_cluster, walrus_cluster, client, _) = test_cluster::E2eTestSetupBuilder::new()
            .with_epoch_duration(Duration::from_secs(10))
            .with_test_nodes_config(
                TestNodesConfig::builder()
                    .with_node_weights(&[1, 2, 3, 3, 4, 2])
                    .with_enable_node_config_synchronizer()
                    .build(),
            )
            .with_communication_config(
                ClientCommunicationConfig::default_for_test_with_reqwest_timeout(
                    Duration::from_secs(2),
                ),
            )
            .build_generic::<SimStorageNodeHandle>()
            .await
            .unwrap();

        let blob_info_consistency_check = BlobInfoConsistencyCheck::new();

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

        simtest_utils::wait_until_node_is_active(
            &walrus_cluster.nodes[5],
            Duration::from_secs(100),
        )
        .await
        .expect("Node should be active");

        blob_info_consistency_check.check_storage_node_consistency();
    }

    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_node_config_synchronizer() {
        let (_sui_cluster, mut walrus_cluster, client, _) =
            test_cluster::E2eTestSetupBuilder::new()
                .with_epoch_duration(Duration::from_secs(30))
                .with_test_nodes_config(
                    TestNodesConfig::builder()
                        .with_node_weights(&[1, 2, 3, 3, 4, 0])
                        .with_enable_node_config_synchronizer()
                        .build(),
                )
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

        // Get current committee and verify node[5] is in it
        let committees = client_arc
            .inner
            .get_latest_committees_in_test()
            .await
            .expect("Should get committees");

        assert!(
            committees
                .current_committee()
                .find_by_public_key(&walrus_cluster.nodes[5].public_key)
                .is_none()
        );

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

        simtest_utils::wait_until_node_is_active(
            &walrus_cluster.nodes[5],
            Duration::from_secs(100),
        )
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

        blob_info_consistency_check.check_storage_node_consistency();
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
        let (_sui_cluster, walrus_cluster, client, _) = test_cluster::E2eTestSetupBuilder::new()
            .with_epoch_duration(Duration::from_secs(10))
            .with_test_nodes_config(
                TestNodesConfig::builder()
                    .with_node_weights(&[1, 2, 3, 3, 4, 2])
                    .with_enable_node_config_synchronizer()
                    .build(),
            )
            .with_communication_config(
                ClientCommunicationConfig::default_for_test_with_reqwest_timeout(
                    Duration::from_secs(2),
                ),
            )
            .build_generic::<SimStorageNodeHandle>()
            .await
            .unwrap();

        let blob_info_consistency_check = BlobInfoConsistencyCheck::new();

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

        blob_info_consistency_check.check_storage_node_consistency();
    }
}
