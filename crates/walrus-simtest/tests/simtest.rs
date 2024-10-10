// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(msim)]
mod tests {
    use std::{
        collections::HashSet,
        sync::{atomic::AtomicBool, Arc},
        time::Duration,
    };

    use anyhow::Context;
    use rand::Rng;
    use sui_macros::register_fail_points;
    use sui_protocol_config::ProtocolConfig;
    use walrus_core::encoding::{Primary, Secondary};
    use walrus_proc_macros::walrus_simtest;
    use walrus_service::{
        client::{responses::BlobStoreResult, Client, StoreWhen},
        test_utils::{test_cluster, SimStorageNodeHandle},
    };
    use walrus_sui::client::{BlobPersistence, SuiContractClient};
    use walrus_test_utils::WithTempDir;

    const FAILURE_TRIGGER_PROBABILITY: f64 = 0.01;

    // Helper function to write a random blob, read it back and check that it is the same.
    async fn write_read_and_check_random_blob(
        client: &WithTempDir<Client<SuiContractClient>>,
        data_length: usize,
    ) -> anyhow::Result<()> {
        // Write a random blob.
        let blob = walrus_test_utils::random_data(data_length);
        let BlobStoreResult::NewlyCreated {
            blob_object: blob_confirmation,
            ..
        } = client
            .as_ref()
            .reserve_and_store_blob(&blob, 1, StoreWhen::Always, BlobPersistence::Permanent)
            .await
            .context("store blob should not fail")?
        else {
            panic!("expect newly stored blob")
        };

        // Read the blob using primary slivers.
        let read_blob = client
            .as_ref()
            .read_blob::<Primary>(&blob_confirmation.blob_id)
            .await
            .context("should be able to read blob we just stored")?;

        // Check that blob is what we wrote.
        assert_eq!(read_blob, blob);

        // Read using secondary slivers and check the result.
        let read_blob = client
            .as_ref()
            .read_blob::<Secondary>(&blob_confirmation.blob_id)
            .await
            .context("should be able to read blob we just stored")?;
        assert_eq!(read_blob, blob);

        Ok(())
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
            )
            .await
            .unwrap();

        write_read_and_check_random_blob(&client, 31415)
            .await
            .expect("workload should not fail");
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
        register_fail_points(
            &[
                "batch-write-before",
                "batch-write-after",
                "put-cf-before",
                "put-cf-after",
                "delete-cf-before",
                "delete-cf-after",
            ],
            move || {
                handle_failpoint(
                    do_not_fail_nodes.clone(),
                    fail_triggered_clone.clone(),
                    FAILURE_TRIGGER_PROBABILITY,
                );
            },
        );

        // Run workload and wait until a crash is triggered.
        let mut data_length = 31415;
        loop {
            // TODO(#995): use stress client for better coverage of the workload.
            write_read_and_check_random_blob(&client, data_length)
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
                write_read_and_check_random_blob(&client, data_length)
                    .await
                    .expect("workload should not fail");

                data_length += 1024;
            }
        })
        .await;

        assert!(fail_triggered.load(std::sync::atomic::Ordering::SeqCst));
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
                "Crashing node {} for {} seconds",
                current_node,
                restart_after.as_secs()
            );

            fail_triggered.store(true, std::sync::atomic::Ordering::SeqCst);

            sui_simulator::task::kill_current_node(Some(restart_after));
        }
    }
}
