// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Contains event blob related simtests.

#![recursion_limit = "256"]

#[cfg(msim)]
mod tests {
    use std::{
        fs,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::{Duration, Instant},
    };

    use rocksdb::Options as RocksdbOptions;
    use typed_store::rocks::be_fix_int_ser;
    use walrus_core::{BlobId, test_utils};
    use walrus_proc_macros::walrus_simtest;
    use walrus_sdk::{client::WalrusNodeClient, config::ClientCommunicationConfig};
    use walrus_service::{
        node::{
            DatabaseConfig,
            DatabaseTableOptionsFactory,
            event_blob_writer::CertifiedEventBlobMetadata,
        },
        test_utils::{SimStorageNodeHandle, TestCluster, TestNodesConfig, test_cluster},
    };
    use walrus_simtest::test_utils::{simtest_utils, simtest_utils::BlobInfoConsistencyCheck};
    use walrus_sui::{client::SuiContractClient, types::move_structs::EventBlob};
    use walrus_test_utils::WithTempDir;

    async fn wait_for_certification_stuck(
        client: &Arc<WithTempDir<WalrusNodeClient<SuiContractClient>>>,
    ) -> EventBlob {
        let start = Instant::now();
        let mut last_blob_time = Instant::now();
        let mut last_blob = EventBlob {
            blob_id: test_utils::random_blob_id(),
            ending_checkpoint_sequence_number: 0,
        };

        loop {
            let current_blob =
                simtest_utils::get_last_certified_event_blob_must_succeed(client).await;

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
        client: &Arc<WithTempDir<WalrusNodeClient<SuiContractClient>>>,
        node_index: usize,
    ) -> Result<(), anyhow::Error> {
        let node = &walrus_cluster.nodes[node_index];

        let mut last_certified_blob =
            simtest_utils::get_last_certified_event_blob_must_succeed(client).await;
        loop {
            let current_blob =
                simtest_utils::get_last_certified_event_blob_must_succeed(client).await;

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
        let db_table_opts_factory =
            DatabaseTableOptionsFactory::new(DatabaseConfig::default(), false);
        let db = Arc::new(rocksdb::DB::open_cf_with_opts_for_read_only(
            &RocksdbOptions::default(),
            db_path,
            [
                ("pending_blob_store", db_table_opts_factory.pending()),
                ("attested_blob_store", db_table_opts_factory.attested()),
                ("certified_blob_store", db_table_opts_factory.certified()),
                (
                    "failed_to_attest_blob_store",
                    db_table_opts_factory.failed_to_attest(),
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

    /// This test verifies that the node can correctly recover from a forked event blob.
    //#[ignore = "ignore integration simtests by default"]
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
        simtest_utils::restart_nodes_with_checkpoints(&mut walrus_cluster, |i| 30 + i as u32).await;

        // Wait for event blob certification to get stuck
        let stuck_blob = wait_for_certification_stuck(&client).await;

        tracing::info!("stuck blob: {:?}", stuck_blob);

        // Restart nodes with same checkpoint number to recover
        simtest_utils::restart_nodes_with_checkpoints(&mut walrus_cluster, |_| 20).await;

        // Verify recovery
        tokio::time::sleep(Duration::from_secs(40)).await;
        let recovered_blob =
            simtest_utils::get_last_certified_event_blob_must_succeed(&client).await;

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
        simtest_utils::restart_node_with_checkpoints(&mut walrus_cluster, 0, |i| 30 + i as u32)
            .await;

        // Wait for event blob certification to get stuck
        wait_for_event_blob_writer_to_fork(&mut walrus_cluster, &client, 0)
            .await
            .unwrap();

        remove_both_recovery_files_before_start(&walrus_cluster.nodes[0]);

        // Restart nodes with same checkpoint number to recover
        simtest_utils::restart_node_with_checkpoints(&mut walrus_cluster, 0, |_| 20).await;

        // Verify recovery
        wait_for_event_blob_writer_to_recover(&walrus_cluster.nodes[0])
            .await
            .unwrap();
    }

    /// This integration test simulates pausing checkpoint tailing to trigger runtime catchup.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_runtime_catchup_triggers_on_tailing_pause() {
        use walrus_service::client::ClientCommunicationConfig;

        let (_sui_cluster, mut walrus_cluster, client, _) =
            test_cluster::E2eTestSetupBuilder::new()
                .with_epoch_duration(Duration::from_secs(15))
                .with_num_checkpoints_per_blob(20)
                //.with_event_stream_catchup_min_checkpoint_lag(Some(u64::MAX))
                .with_test_nodes_config(TestNodesConfig {
                    node_weights: vec![2, 2, 3, 3, 3],
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

        // Wait until all nodes have non zero latest checkpoint sequence number
        loop {
            let node_health_info =
                simtest_utils::get_nodes_health_info(&walrus_cluster.nodes).await;
            if node_health_info
                .iter()
                .all(|info| info.latest_checkpoint_sequence_number.unwrap() > 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        tracing::info!("All nodes have non zero latest checkpoint sequence number");

        // Get latest certified event blob
        let mut latest_certified_blob;
        loop {
            latest_certified_blob =
                simtest_utils::get_last_certified_event_blob(&client_arc, Duration::from_secs(30))
                    .await;
            if latest_certified_blob.is_some() {
                tracing::info!(
                    "Latest certified blob: {:?}",
                    latest_certified_blob.clone().unwrap()
                );
                break;
            }
            tracing::info!("Waiting for latest certified blob");
        }

        // Track whether event-blob catchup path is exercised
        let saw_event_blob_catchup = Arc::new(AtomicBool::new(false));
        let saw_event_blob_catchup_clone = saw_event_blob_catchup.clone();
        sui_macros::register_fail_point("fail_point_catchup_using_event_blobs_start", move || {
            saw_event_blob_catchup_clone.store(true, Ordering::SeqCst);
        });

        // Pause checkpoint tailing to build lag
        sui_macros::register_fail_point_async(
            "pause_checkpoint_tailing_entry",
            move || async move {
                // Sleep enough to build lag and trigger catchup monitoring
                tracing::info!("Pausing checkpoint tailing");
                tokio::time::sleep(Duration::from_secs(45)).await;
            },
        );

        // Restart node 0 to apply failpoint into its runtime
        simtest_utils::restart_node_with_checkpoints(&mut walrus_cluster, 0, |_| 20).await;

        // Wait until latest certified blob is updated
        loop {
            if let Some(blob) =
                simtest_utils::get_last_certified_event_blob(&client_arc, Duration::from_secs(30))
                    .await
            {
                tracing::info!("Latest certified blob: {:?}", blob);
                if blob.blob_id != latest_certified_blob.clone().unwrap().blob_id {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        let mut node_health_info =
            simtest_utils::get_nodes_health_info([&walrus_cluster.nodes[0]]).await;
        let prev_seq = node_health_info[0]
            .latest_checkpoint_sequence_number
            .unwrap();

        // Let lag build and catchup trigger
        tokio::time::sleep(Duration::from_secs(90)).await;

        node_health_info = simtest_utils::get_nodes_health_info([&walrus_cluster.nodes[0]]).await;

        // Verify that latest checkpoint advances after the pause window and node is healthy
        let latest_seq = node_health_info[0]
            .latest_checkpoint_sequence_number
            .unwrap();
        assert!(latest_seq > prev_seq);

        // Verify event-blob catchup ran
        assert!(saw_event_blob_catchup.load(Ordering::SeqCst));

        sui_macros::clear_fail_point("pause_checkpoint_tailing_entry");
        sui_macros::clear_fail_point("fail_point_catchup_using_event_blobs_start");
    }
}
