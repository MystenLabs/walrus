// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Contains event blob related simtests.

#![recursion_limit = "256"]

#[cfg(msim)]
mod tests {
    use std::{
        fs,
        sync::Arc,
        time::{Duration, Instant},
    };

    use rocksdb::Options as RocksdbOptions;
    use sui_rpc_api::Client as RpcClient;
    use tokio::sync::RwLock;
    use typed_store::rocks::be_fix_int_ser;
    use walrus_core::{BlobId, test_utils};
    use walrus_proc_macros::walrus_simtest;
    use walrus_sdk::{client::Client, config::ClientCommunicationConfig};
    use walrus_service::{
        node::{DatabaseConfig, event_blob_writer::CertifiedEventBlobMetadata},
        test_utils::{SimStorageNodeHandle, TestCluster, TestNodesConfig, test_cluster},
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
                tracing::info!("new event blob seen during fork wait: {:?}", current_blob);
                last_blob = current_blob;
                last_blob_time = Instant::now();
            }

            if last_blob_time.elapsed() > Duration::from_secs(20) {
                tracing::info!("event blob certification stuck for 20s");
                break;
            }

            if start.elapsed() > Duration::from_secs(180) {
                panic!("Timeout waiting for event blob to get stuck");
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        last_blob
    }

    async fn wait_for_event_blob_writer_to_fork(
        walrus_cluster: &mut TestCluster<SimStorageNodeHandle>,
        client: &Arc<WithTempDir<Client<SuiContractClient>>>,
        node_index: usize,
    ) -> Result<(), anyhow::Error> {
        let node = &walrus_cluster.nodes[node_index];

        let mut last_certified_blob = get_last_certified_event_blob_must_succeed(client).await;
        loop {
            let current_blob = get_last_certified_event_blob_must_succeed(client).await;

            if current_blob.blob_id != last_certified_blob.blob_id {
                tracing::info!("new event blob seen during fork wait: {:?}", current_blob);
                last_certified_blob = current_blob;
                tokio::time::sleep(Duration::from_secs(30)).await;

                let prev_blob_id = get_last_certified_event_blob_from_node(node).await?.blob_id;
                if prev_blob_id != last_certified_blob.blob_id {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    let current_blob_id =
                        get_last_certified_event_blob_from_node(node).await?.blob_id;
                    if current_blob_id == prev_blob_id {
                        tracing::info!("node forked");
                        break;
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        Ok(())
    }

    async fn wait_for_event_blob_writer_to_recover(
        node: &SimStorageNodeHandle,
    ) -> Result<(), anyhow::Error> {
        let mut last_certified_blob = get_last_certified_event_blob_from_node(node).await?;
        let mut previous_blob = BlobId::ZERO;
        let mut num_certified_updates = 0;
        // Wait for 4 certified updates to ensure the node has recovered
        // and is certifying blobs again.
        while num_certified_updates < 4 {
            if last_certified_blob.blob_id == previous_blob {
                tokio::time::sleep(Duration::from_secs(1)).await;
                last_certified_blob = get_last_certified_event_blob_from_node(node).await?;
                // Check if the blob writer is recovering without metadata
                // If so, return early because node is not going to recover
                if check_blob_writer_recovery_without_metadata(node) {
                    return Ok(());
                }
                continue;
            }
            previous_blob = last_certified_blob.blob_id;
            num_certified_updates += 1;
        }
        Ok(())
    }

    fn remove_both_recovery_files_before_start(node: &SimStorageNodeHandle) {
        let path = node
            .storage_node_config
            .storage_path
            .join("event_blob_writer")
            .join("db");
        let file_path = path.join("last_certified_blob_without_metadata");
        if file_path.exists() {
            fs::remove_file(file_path).unwrap();
        }
        let file_path = path.join("last_certified_blob_with_metadata");
        if file_path.exists() {
            fs::remove_file(file_path).unwrap();
        }
    }

    fn check_blob_writer_recovery_without_metadata(node: &SimStorageNodeHandle) -> bool {
        let path = node
            .storage_node_config
            .storage_path
            .join("event_blob_writer")
            .join("db");
        let file_path = path.join("last_certified_blob_without_metadata");
        file_path.exists()
    }

    async fn get_last_certified_event_blob_from_node(
        node: &SimStorageNodeHandle,
    ) -> Result<CertifiedEventBlobMetadata, anyhow::Error> {
        let db_path = node
            .storage_node_config
            .storage_path
            .join("event_blob_writer")
            .join("db");
        let db = Arc::new(rocksdb::DB::open_cf_with_opts_for_read_only(
            &RocksdbOptions::default(),
            db_path,
            [
                (
                    "pending_blob_store",
                    DatabaseConfig::default().pending().to_options(),
                ),
                (
                    "attested_blob_store",
                    DatabaseConfig::default().attested().to_options(),
                ),
                (
                    "certified_blob_store",
                    DatabaseConfig::default().certified().to_options(),
                ),
                (
                    "failed_to_attest_blob_store",
                    DatabaseConfig::default().failed_to_attest().to_options(),
                ),
            ],
            false,
        )?);
        let cf = db
            .cf_handle("certified_blob_store")
            .expect("Certified blob store column family should exist");
        let key = be_fix_int_ser(&())?;
        let data = db
            .get_cf(&cf, &key)?
            .expect("Node certified blob should exist");
        let metadata: CertifiedEventBlobMetadata = bcs::from_bytes(&data)?;
        Ok(metadata)
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

    async fn kill_node(node_id: sui_simulator::task::NodeId) {
        let handle = sui_simulator::runtime::Handle::current();
        handle.delete_node(node_id);
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

        // Doing a short sleep so that the connection between the old nodes to the fullnodes are
        // fully cleared. We've seen cases where when the new node is started, the old connection
        // on the fullnode still exists and the new node will not be able to connect to the
        //fullnode.
        tokio::time::sleep(Duration::from_secs(2)).await;

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

    async fn restart_node_with_checkpoints(
        walrus_cluster: &mut TestCluster<SimStorageNodeHandle>,
        node_index: usize,
        checkpoint_fn: impl Fn(usize) -> u32,
    ) {
        kill_node(walrus_cluster.nodes[node_index].node_id.unwrap()).await;
        let node = &mut walrus_cluster.nodes[node_index];
        node.node_id = Some(
            SimStorageNodeHandle::spawn_node(
                Arc::new(RwLock::new(node.storage_node_config.clone())),
                Some(checkpoint_fn(node_index)),
                node.cancel_token.clone(),
            )
            .await
            .id(),
        );
    }

    /// This test verifies that the node can correctly recover from a forked event blob.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_event_blob_fork_recovery() {
        let (_sui_cluster, mut walrus_cluster, client, _) =
            test_cluster::E2eTestSetupBuilder::new()
                .with_epoch_duration(Duration::from_secs(15))
                .with_test_nodes_config(TestNodesConfig {
                    node_weights: vec![2, 2, 3, 3, 3],
                    ..Default::default()
                })
                .with_num_checkpoints_per_blob(20)
                // Low event_stream_catchup_min_checkpoint_lag may cause reading latest event blob
                // fail since the event blob's certified events have not been processed yet.
                // We can revisit this once we have more robust client read.
                .with_event_stream_catchup_min_checkpoint_lag(Some(20000))
                .with_communication_config(
                    ClientCommunicationConfig::default_for_test_with_reqwest_timeout(
                        Duration::from_secs(2),
                    ),
                )
                .build_generic::<SimStorageNodeHandle>()
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

        tracing::info!("stuck blob: {:?}", stuck_blob);

        // Restart nodes with same checkpoint number to recover
        restart_nodes_with_checkpoints(&mut walrus_cluster, |_| 20).await;

        // Verify recovery
        tokio::time::sleep(Duration::from_secs(40)).await;
        let recovered_blob = get_last_certified_event_blob_must_succeed(&client).await;

        // Event blob should make progress again.
        assert_ne!(stuck_blob.blob_id, recovered_blob.blob_id);

        blob_info_consistency_check.check_storage_node_consistency();
    }

    /// This test verifies that the node can correctly recover from a forked event blob.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_event_blob_local_fork_recovery() {
        let (_sui_cluster, mut walrus_cluster, client, _) =
            test_cluster::E2eTestSetupBuilder::new()
                .with_epoch_duration(Duration::from_secs(15))
                .with_test_nodes_config(TestNodesConfig {
                    node_weights: vec![2, 2, 3, 3, 3],
                    ..Default::default()
                })
                .with_num_checkpoints_per_blob(20)
                // Low event_stream_catchup_min_checkpoint_lag may cause reading latest event blob
                // fail since the event blob's certified events have not been processed yet.
                // We can revisit this once we have more robust client read.
                .with_event_stream_catchup_min_checkpoint_lag(Some(20000))
                .with_communication_config(
                    ClientCommunicationConfig::default_for_test_with_reqwest_timeout(
                        Duration::from_secs(2),
                    ),
                )
                .build_generic::<SimStorageNodeHandle>()
                .await
                .unwrap();

        let client = Arc::new(client);

        // Run workload to get some event blobs certified
        tokio::time::sleep(Duration::from_secs(30)).await;

        // Restart nodes with different checkpoint numbers to create fork
        restart_node_with_checkpoints(&mut walrus_cluster, 0, |i| 30 + i as u32).await;

        // Wait for event blob certification to get stuck
        wait_for_event_blob_writer_to_fork(&mut walrus_cluster, &client, 0)
            .await
            .unwrap();

        remove_both_recovery_files_before_start(&walrus_cluster.nodes[0]);

        // Restart nodes with same checkpoint number to recover
        restart_node_with_checkpoints(&mut walrus_cluster, 0, |_| 20).await;

        // Verify recovery
        wait_for_event_blob_writer_to_recover(&walrus_cluster.nodes[0])
            .await
            .unwrap();
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
        let checkpoints_per_event_blob = 20;
        let (sui_cluster, mut walrus_cluster, client, _) = test_cluster::E2eTestSetupBuilder::new()
            .with_epoch_duration(Duration::from_secs(15))
            .with_test_nodes_config(TestNodesConfig {
                node_weights: vec![2, 2, 3, 3, 3],
                ..Default::default()
            })
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
        restart_nodes_with_checkpoints(&mut walrus_cluster, |_| checkpoints_per_event_blob).await;

        // Wait for the cluster to process some events.
        let workload_handle =
            simtest_utils::start_background_workload(client_arc.clone(), false, 0);
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

        tracing::info!("node health infos: {:?}", node_health_infos);

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
