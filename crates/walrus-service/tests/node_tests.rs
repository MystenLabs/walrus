// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Simtests for storage-node logic, moved from `node.rs` so that each test gets its own binary
//! target (required by `seed-search.py`).  See WAL-872.
#![recursion_limit = "256"]

#[cfg(msim)]
mod simtests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    use anyhow::Context;
    use rand::{Rng as _, SeedableRng, rngs::StdRng};
    use sui_macros::{
        clear_fail_point,
        register_fail_point,
        register_fail_point_arg,
        register_fail_point_async,
        register_fail_point_if,
    };
    use sui_types::base_types::ObjectID;
    use tokio::sync::Notify;
    use walrus_core::{
        ShardIndex,
        Sliver,
        SliverType,
        test_utils::generate_config_metadata_and_valid_recovery_symbols,
    };
    use walrus_proc_macros::walrus_simtest;
    use walrus_sdk::sui::types::{ContractEvent, EpochChangeEvent, EpochChangeStart};
    use walrus_service::{
        event::events::EventStreamCursor,
        node::{
            ServiceState,
            UploadIntent,
            config::ShardSyncConfig,
            test_helpers::{
                BLOB,
                BLOB_ID,
                BlobInfoApi as _,
                BlobStatus,
                EncodedBlob,
                NodeStatus,
                OTHER_BLOB_ID,
                SHARD_INDEX,
                ShardStatus,
                ShardSyncHandler,
                StorageNodeHandleTrait,
                StorageNodeInner,
                StoredOnNodeStatus,
                TIMEOUT,
                advance_cluster_to_epoch,
                check_all_blobs_are_synced,
                cluster_at_epoch1_without_blobs,
                cluster_at_epoch1_without_blobs_waiting_for_active_nodes,
                cluster_with_initial_epoch_and_certified_blobs,
                cluster_with_partially_stored_blob,
                expect_sliver_pair_stored_before_timeout,
                set_up_node_with_metadata,
                setup_cluster_for_shard_sync_tests,
                setup_shard_recovery_test_cluster,
                setup_shard_recovery_test_cluster_with_blob_count,
                store_at_shards,
                wait_for_shard_in_active_state,
                wait_until_events_processed_exact,
            },
        },
    };
    use walrus_sui::{
        test_utils::{EventForTesting, FIXED_OBJECT_ID, event_id_for_testing},
        types::{BlobCertified, BlobDeleted, BlobRegistered, InvalidBlobId},
    };
    use walrus_test_utils::{Result as TestResult, async_param_test, simtest_param_test};

    // ── Top-level simtests (moved from node.rs #[cfg(test)] mod tests) ──────────

    #[walrus_simtest]
    async fn returns_correct_metadata_status() -> TestResult {
        let (_ec, metadata, _idx, _rs) = generate_config_metadata_and_valid_recovery_symbols()?;
        let storage_node = set_up_node_with_metadata(metadata.clone().into_unverified()).await?;

        let metadata_status = storage_node
            .as_ref()
            .inner_for_test()
            .metadata_status(metadata.blob_id())?;
        assert_eq!(metadata_status, StoredOnNodeStatus::Stored);
        Ok(())
    }

    #[walrus_simtest]
    async fn cancel_expired_blob_sync_upon_epoch_change() -> TestResult {
        walrus_test_utils::init_tracing();

        let shards: &[&[u16]] = &[&[1], &[0, 2, 3, 4]];

        let (cluster, events, blob) =
            cluster_with_partially_stored_blob(shards, BLOB, |shard, _| shard.get() != 1).await?;
        events.send(
            BlobCertified {
                epoch: 1,
                blob_id: *blob.blob_id(),
                end_epoch: 2,
                deletable: false,
                object_id: FIXED_OBJECT_ID,
                is_extension: false,
                event_id: event_id_for_testing(),
            }
            .into(),
        )?;
        advance_cluster_to_epoch(&cluster, &[&events], 2).await?;

        // Node 1 which has the blob stored should finish processing 6 events: epoch change start 1,
        // epoch change done 1, blob registered, blob certified, epoch change start 2, epoch change
        // done 2.
        wait_until_events_processed_exact(&cluster.nodes[1], 6).await?;

        // Node 0 should also finish all events as blob syncs of expired blobs are cancelled on
        // epoch change.
        wait_until_events_processed_exact(&cluster.nodes[0], 6).await?;

        Ok(())
    }

    #[walrus_simtest]
    #[ignore = "ignore long-running test by default"]
    async fn deletes_expired_blob_data() -> TestResult {
        walrus_test_utils::init_tracing();

        let (cluster, events) =
            cluster_at_epoch1_without_blobs_waiting_for_active_nodes(&[&[0, 1, 2, 3]], None)
                .await?;
        let node = cluster.nodes[0].storage_node().clone();
        let config = cluster.encoding_config();

        let blob_end_epochs_and_deletable = [
            (2, true),
            (3, false),
            (4, true),
            (4, false),
            (5, true),
            (5, true),
        ];
        assert!(blob_end_epochs_and_deletable.is_sorted_by_key(|(end_epoch, _)| end_epoch));
        let blob_count = blob_end_epochs_and_deletable.len();
        let mut rng = StdRng::seed_from_u64(42);
        let mut blobs_with_end_epoch = Vec::with_capacity(blob_count);

        // Add the blobs at epoch 1, the epoch at which the cluster starts.
        for (i, &(end_epoch, deletable)) in blob_end_epochs_and_deletable.iter().enumerate() {
            tracing::info!("adding blob {i} with end epoch {end_epoch}");
            let blob_details = EncodedBlob::new(
                &walrus_test_utils::random_data_from_rng(100, &mut rng),
                config.clone(),
            );
            let registered_event = BlobRegistered {
                end_epoch,
                deletable,
                ..BlobRegistered::for_testing_with_random_object_id(*blob_details.blob_id())
            };
            // Note: register and certify the blob are always using epoch 0.
            events.send(registered_event.clone().into())?;
            store_at_shards(&blob_details, &cluster, |_, _| true).await?;
            events.send(
                registered_event
                    .into_corresponding_certified_event_for_testing()
                    .into(),
            )?;
            blobs_with_end_epoch.push((*blob_details.blob_id(), end_epoch));
        }

        for i in 0..blob_count {
            let (_blob_id, end_epoch) = blobs_with_end_epoch[i];
            advance_cluster_to_epoch(&cluster, &[&events], end_epoch).await?;

            // Wait for garbage-collection task to complete for this epoch
            walrus_service::test_utils::wait_for_garbage_collection_to_complete(
                &cluster.nodes[0..=0],
                end_epoch,
                Duration::from_secs(5),
            )
            .await?;

            let node_epoch = node.inner_for_test().current_committee_epoch_for_test();

            for (blob_id, end_epoch) in blobs_with_end_epoch[i..].iter() {
                if *end_epoch > node_epoch {
                    assert!(
                        node.inner_for_test()
                            .is_stored_at_all_shards_at_latest_epoch_for_test(blob_id)
                            .await?
                    );
                } else {
                    node.inner_for_test()
                        .check_does_not_store_for_blob_for_test(blob_id)
                        .await?;
                }
            }
        }

        Ok(())
    }

    #[walrus_simtest]
    async fn caches_metadata_and_slivers_until_registration() -> TestResult {
        let (cluster, _events) = cluster_at_epoch1_without_blobs(&[&[0, 1, 2, 3]], None).await?;
        let storage_node = cluster.nodes[0].storage_node.clone();

        let encoding_config = storage_node
            .as_ref()
            .inner_for_test()
            .encoding_config_for_test()
            .as_ref()
            .clone();
        let encoded = EncodedBlob::new(BLOB, encoding_config);
        let blob_id = *encoded.blob_id();
        let pair = encoded.assigned_sliver_pair(SHARD_INDEX);

        assert!(
            storage_node
                .as_ref()
                .store_metadata(
                    encoded.metadata.clone().into_unverified(),
                    UploadIntent::Pending
                )
                .await?
        );

        assert!(
            storage_node
                .as_ref()
                .store_sliver(
                    blob_id,
                    pair.index(),
                    Sliver::Primary(pair.primary.clone()),
                    UploadIntent::Pending,
                )
                .await?
        );

        assert_eq!(
            storage_node
                .as_ref()
                .inner_for_test()
                .pending_metadata_entry_count_for_test()
                .await,
            1
        );
        assert_eq!(
            storage_node
                .as_ref()
                .inner_for_test()
                .pending_sliver_count_for_test()
                .await,
            1
        );

        storage_node
            .as_ref()
            .inner_for_test()
            .storage_for_test()
            .update_blob_info(0, &BlobRegistered::for_testing(blob_id).into())?;

        storage_node
            .as_ref()
            .inner_for_test()
            .flush_pending_metadata_for_test(&blob_id)
            .await?;
        let persisted_metadata = Arc::new(
            storage_node
                .as_ref()
                .inner_for_test()
                .storage_for_test()
                .get_metadata(&blob_id)?
                .expect("metadata should be persisted before flushing slivers"),
        );
        storage_node
            .as_ref()
            .inner_for_test()
            .flush_pending_slivers_for_test(&blob_id, persisted_metadata)
            .await?;

        assert!(
            storage_node
                .as_ref()
                .inner_for_test()
                .storage_for_test()
                .get_metadata(&blob_id)?
                .is_some()
        );

        let shard_storage = storage_node
            .as_ref()
            .inner_for_test()
            .storage_for_test()
            .shard_storage(SHARD_INDEX)
            .await
            .expect("shard storage must exist");

        assert!(
            shard_storage
                .get_sliver(&blob_id, SliverType::Primary)
                .context("sliver should be persisted after registration")?
                .is_some()
        );

        assert_eq!(
            storage_node
                .as_ref()
                .inner_for_test()
                .pending_metadata_entry_count_for_test()
                .await,
            0
        );
        assert_eq!(
            storage_node
                .as_ref()
                .inner_for_test()
                .pending_sliver_count_for_test()
                .await,
            0
        );

        Ok(())
    }

    #[walrus_simtest]
    async fn skip_storing_sliver_if_already_stored() -> TestResult {
        let (cluster, _, blob) =
            cluster_with_partially_stored_blob(&[&[0, 1, 2, 3]], BLOB, |_, _| true).await?;

        let assigned_sliver_pair = blob.assigned_sliver_pair(ShardIndex(0));
        let is_newly_stored = cluster.nodes[0]
            .storage_node
            .store_sliver(
                *blob.blob_id(),
                assigned_sliver_pair.index(),
                Sliver::Primary(assigned_sliver_pair.primary.clone()),
                UploadIntent::Immediate,
            )
            .await?;

        assert!(!is_newly_stored);

        Ok(())
    }

    // ── Failure injection tests (moved from node.rs failure_injection_tests module) ─

    async fn wait_until_no_sync_tasks(shard_sync_handler: &ShardSyncHandler) -> TestResult {
        // Timeout needs to be longer than shard sync retry interval.
        tokio::time::timeout(Duration::from_mins(2), async {
            loop {
                if shard_sync_handler.current_sync_task_count().await == 0
                    && shard_sync_handler.no_pending_recover_metadata().await
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .map_err(|_| anyhow::anyhow!("Timed out waiting for shard sync tasks to complete"))?;

        Ok(())
    }

    // Tests that shard sync can be resumed from a specific progress point.
    // `break_index` is the index of the blob to break the sync process.
    // Note that currently, each sync batch contains 10 blobs. So testing various interesting
    // places to break the sync process.
    simtest_param_test! {
        sync_shard_start_from_progress -> TestResult: [
            primary1: (1, SliverType::Primary),
            primary5: (5, SliverType::Primary),
            primary10: (10, SliverType::Primary),
            primary11: (11, SliverType::Primary),
            primary15: (15, SliverType::Primary),
            primary23: (23, SliverType::Primary),
            secondary1: (1, SliverType::Secondary),
            secondary5: (5, SliverType::Secondary),
            secondary10: (10, SliverType::Secondary),
            secondary11: (11, SliverType::Secondary),
            secondary15: (15, SliverType::Secondary),
            secondary23: (23, SliverType::Secondary),
        ]
    }
    async fn sync_shard_start_from_progress(
        break_index: u64,
        sliver_type: SliverType,
    ) -> TestResult {
        let (cluster, blob_details, storage_dst, shard_storage_set) =
            setup_cluster_for_shard_sync_tests(None, None).await?;

        assert_eq!(shard_storage_set.shard_storage.len(), 1);
        let shard_storage_dst = shard_storage_set.shard_storage[0].clone();
        register_fail_point_arg(
            "fail_point_fetch_sliver",
            move || -> Option<(SliverType, u64, bool)> { Some((sliver_type, break_index, false)) },
        );

        // Skip retry loop in shard sync to simulate a reboot.
        register_fail_point_if("fail_point_shard_sync_no_retry", || true);

        // Starts the shard syncing process in the new shard, which will fail at the specified
        // break index.
        cluster.nodes[1]
            .storage_node
            .shard_sync_handler_for_test()
            .start_sync_shards(vec![ShardIndex(0)], false)
            .await?;

        // Waits for the shard sync process to stop.
        wait_until_no_sync_tasks(&cluster.nodes[1].storage_node.shard_sync_handler_for_test())
            .await?;

        // Check that shard sync process is not finished.
        let shard_storage_src = cluster.nodes[0]
            .storage_node
            .inner_for_test()
            .storage_for_test()
            .shard_storage(ShardIndex(0))
            .await
            .expect("shard storage should exist");
        assert!(
            shard_storage_dst.sliver_count(SliverType::Primary)
                < shard_storage_src.sliver_count(SliverType::Primary)
                || shard_storage_dst.sliver_count(SliverType::Secondary)
                    < shard_storage_src.sliver_count(SliverType::Secondary)
        );

        clear_fail_point("fail_point_fetch_sliver");

        // restart the shard syncing process, to simulate a reboot.
        cluster.nodes[1]
            .storage_node
            .shard_sync_handler_for_test()
            .restart_syncs()
            .await?;

        // Waits for the shard to be synced.
        wait_until_no_sync_tasks(&cluster.nodes[1].storage_node.shard_sync_handler_for_test())
            .await?;

        // Checks that the shard is completely migrated.
        check_all_blobs_are_synced(&blob_details, &storage_dst, &shard_storage_dst, &[])?;

        // Checks that the shard sync progress is reset.
        assert!(
            shard_storage_dst
                .get_last_synced_blob_id()
                .expect("getting last synced blob id should succeed")
                .is_none()
        );

        Ok(())
    }

    // Tests that restarting shard sync will retry shard transfer first, even though last sync
    // entered recovery mode.
    simtest_param_test! {
        sync_shard_restart_recover_retry_transfer -> TestResult: [
            primary5: (5, SliverType::Primary),
            primary15: (15, SliverType::Primary),
            secondary5: (5, SliverType::Secondary),
            secondary15: (15, SliverType::Secondary),
        ]
    }
    async fn sync_shard_restart_recover_retry_transfer(
        break_index: u64,
        sliver_type: SliverType,
    ) -> TestResult {
        let (cluster, blob_details, storage_dst, shard_storage_set) =
            setup_cluster_for_shard_sync_tests(None, None).await?;

        assert_eq!(shard_storage_set.shard_storage.len(), 1);
        let shard_storage_dst = shard_storage_set.shard_storage[0].clone();

        // Register two fail points here. `fail_point_fetch_sliver` will cause shard transfer
        // to fail in the middle, which makes node entering recover mode, and
        // `fail_point_after_start_recovery` will cause the shard recovery to fail, which
        // terminates the shard sync process. Upon restart, we should see that shards retry
        // shard transfer first.
        register_fail_point_arg(
            "fail_point_fetch_sliver",
            move || -> Option<(SliverType, u64, bool)> { Some((sliver_type, break_index, false)) },
        );
        register_fail_point_if("fail_point_after_start_recovery", || true);

        // Starts the shard syncing process in the new shard, which will fail at the specified
        // break index.
        cluster.nodes[1]
            .storage_node
            .shard_sync_handler_for_test()
            .start_sync_shards(vec![ShardIndex(0)], false)
            .await?;

        // Waits for the shard sync process to stop.
        wait_until_no_sync_tasks(&cluster.nodes[1].storage_node.shard_sync_handler_for_test())
            .await?;

        // Check that shard sync process is not finished.
        let shard_storage_src = cluster.nodes[0]
            .storage_node
            .inner_for_test()
            .storage_for_test()
            .shard_storage(ShardIndex(0))
            .await
            .expect("shard storage should exist");
        assert!(
            shard_storage_dst.sliver_count(SliverType::Primary)
                < shard_storage_src.sliver_count(SliverType::Primary)
                || shard_storage_dst.sliver_count(SliverType::Secondary)
                    < shard_storage_src.sliver_count(SliverType::Secondary)
        );

        // Register a fail point to check that the node will not enter recovery mode from this
        // point after restart.
        register_fail_point("fail_point_shard_sync_recover_blob", move || {
            panic!("shard sync should not enter recovery mode in this test");
        });
        clear_fail_point("fail_point_fetch_sliver");

        // restart the shard syncing process, to simulate a reboot.
        cluster.nodes[1]
            .storage_node
            .shard_sync_handler_for_test()
            .clear_shard_sync_tasks()
            .await;
        cluster.nodes[1]
            .storage_node
            .shard_sync_handler_for_test()
            .restart_syncs()
            .await?;

        // Waits for the shard to be synced.
        wait_until_no_sync_tasks(&cluster.nodes[1].storage_node.shard_sync_handler_for_test())
            .await?;

        // Checks that the shard is completely migrated.
        check_all_blobs_are_synced(&blob_details, &storage_dst, &shard_storage_dst, &[])?;

        clear_fail_point("fail_point_shard_sync_recover_blob");
        clear_fail_point("fail_point_after_start_recovery");
        Ok(())
    }

    // Tests that shard sync's behavior when encountering repeated sync failures.
    // More specifically, it tests that
    //   - When the shard sync repeatedly encounters retriable error, but the shard sync is
    //     able to make progress, the node continue to use shard sync to sync the shard.
    //   - When the shard sync repeatedly encounters retriable error, and the shard sync is
    //     not able to make progress, the node will eventually enter recovery mode.
    simtest_param_test! {
        sync_shard_repeated_failures -> TestResult: [
            // In this test case, we inject a retriable error fetching every 4 blobs. Since
            // the size is larger than `sliver_count_per_sync_request`, shard sync will
            // continue to make progress.
            with_progress: (4, false),
            // In this test case, we inject a retriable error fetching every 1 blob. Since
            // the size is smaller than `sliver_count_per_sync_request`, shard sync will
            // not be able to make progress.
            without_progress: (1, true),
        ]
    }
    async fn sync_shard_repeated_failures(break_index: u64, must_use_recovery: bool) -> TestResult {
        let shard_sync_config = ShardSyncConfig {
            sliver_count_per_sync_request: 2,
            // Align SST flush threshold with the request size for this test.
            // For this test, we need one flush (and thus one progress persist) per request
            // to preserve the original progress semantics being asserted (whether shard sync
            // "makes progress" between retriable failures). Setting `sst_max_entries` equal
            // or less to the request size ensures each request triggers a flush and records
            // `last_synced_blob_id` deterministically.
            sst_ingestion_config: Some(walrus_service::node::config::SstIngestionConfig {
                max_entries: Some(2),
                compact_after_sync: true,
            }),
            shard_sync_retry_min_backoff: Duration::from_secs(1),
            shard_sync_retry_max_backoff: Duration::from_secs(5),
            shard_sync_retry_switch_to_recovery_interval: Duration::from_secs(10),
            ..Default::default()
        };
        let (cluster, blob_details, storage_dst, shard_storage_set) =
            setup_cluster_for_shard_sync_tests(None, Some(shard_sync_config)).await?;

        assert_eq!(shard_storage_set.shard_storage.len(), 1);
        let shard_storage_dst = shard_storage_set.shard_storage[0].clone();

        // Simulate repeated retriable errors. break_index indicates how many blobs fetched
        // before generating the retriable error.
        register_fail_point_arg(
            "fail_point_fetch_sliver",
            move || -> Option<(SliverType, u64, bool)> {
                Some((SliverType::Primary, break_index, true))
            },
        );

        let enter_recovery_mode = Arc::new(AtomicBool::new(false));
        let enter_recovery_mode_clone = enter_recovery_mode.clone();
        // Fail point to track if the shard sync enters recovery mode.
        register_fail_point("fail_point_shard_sync_recover_blob", move || {
            enter_recovery_mode_clone.store(true, Ordering::SeqCst);
        });

        // Starts the shard syncing process in the new shard, which will fail at the specified
        // break index.
        cluster.nodes[1]
            .storage_node
            .shard_sync_handler_for_test()
            .start_sync_shards(vec![ShardIndex(0)], false)
            .await?;

        // Waits for the shard sync process to stop.
        wait_until_no_sync_tasks(&cluster.nodes[1].storage_node.shard_sync_handler_for_test())
            .await?;

        // Checks that the shard is completely migrated.
        check_all_blobs_are_synced(&blob_details, &storage_dst, &shard_storage_dst, &[])?;

        // Checks that the shard sync progress is reset.
        assert!(
            shard_storage_dst
                .get_last_synced_blob_id()
                .expect("getting last synced blob id should succeed")
                .is_none()
        );

        if must_use_recovery {
            assert!(enter_recovery_mode.load(Ordering::SeqCst));
        } else {
            assert!(!enter_recovery_mode.load(Ordering::SeqCst));
        }

        clear_fail_point("fail_point_fetch_sliver");

        Ok(())
    }

    simtest_param_test! {
        sync_shard_src_abnormal_return -> TestResult: [
            // Tests that there is a discrepancy between the source and destination shards in
            // terms of certified blobs. If the source doesn't return any blobs, the destination
            // should finish the sync process.
            return_empty: ("fail_point_sync_shard_return_empty"),
            // Tests that when direct shard sync request fails, the shard sync process will be
            // retried using shard recovery.
            return_error: ("fail_point_sync_shard_return_error")
        ]
    }
    async fn sync_shard_src_abnormal_return(fail_point: &'static str) -> TestResult {
        let (cluster, _blob_details, storage_dst, shard_storage_set) =
            setup_cluster_for_shard_sync_tests(None, None).await?;

        assert_eq!(shard_storage_set.shard_storage.len(), 1);
        let shard_storage_dst = shard_storage_set.shard_storage[0].clone();
        register_fail_point_if(fail_point, || true);

        // Starts the shard syncing process in the new shard, which will return empty slivers.
        cluster.nodes[1]
            .storage_node
            .shard_sync_handler_for_test()
            .start_sync_shards(vec![ShardIndex(0)], false)
            .await?;

        // Waits for the shard sync process to stop.
        wait_until_no_sync_tasks(&cluster.nodes[1].storage_node.shard_sync_handler_for_test())
            .await?;
        check_all_blobs_are_synced(&_blob_details, &storage_dst, &shard_storage_dst, &[])?;

        // Checks that the shard sync progress is reset.
        assert!(
            shard_storage_dst
                .get_last_synced_blob_id()
                .expect("getting last synced blob id should succeed")
                .is_none()
        );

        Ok(())
    }

    // Tests that non-certified blobs are not synced during shard sync. And expired certified
    // blobs do not cause shard sync to enter recovery directly.
    #[walrus_simtest]
    async fn sync_shard_ignore_non_certified_blobs() -> TestResult {
        // Creates some regular blobs that will be synced.
        let blobs: Vec<[u8; 32]> = (9..13).map(|i| [i; 32]).collect();
        let blobs: Vec<_> = blobs.iter().map(|b| &b[..]).collect();

        // Creates some expired certified blobs that will not be synced.
        let blobs_expired: Vec<[u8; 32]> = (1..21).map(|i| [i; 32]).collect();
        let blobs_expired: Vec<_> = blobs_expired.iter().map(|b| &b[..]).collect();

        // Generates a cluster with two nodes and one shard each.
        let (cluster, events) = cluster_at_epoch1_without_blobs(&[&[0, 1], &[2, 3]], None).await?;

        // Uses fail point to track whether shard sync recovery is triggered.
        let shard_sync_recovery_triggered = Arc::new(AtomicBool::new(false));
        let trigger = shard_sync_recovery_triggered.clone();
        register_fail_point("fail_point_shard_sync_recovery", move || {
            trigger.store(true, Ordering::SeqCst)
        });

        // Certifies all the blobs and upload data.
        let mut details = Vec::new();
        {
            let config = cluster.encoding_config();

            for blob in blobs {
                let blob_details = EncodedBlob::new(blob, config.clone());
                let object_id = ObjectID::random();
                // Note: register and certify the blob are always using epoch 0.
                events.send(
                    BlobRegistered::for_testing_with_object_id(*blob_details.blob_id(), object_id)
                        .into(),
                )?;
                store_at_shards(&blob_details, &cluster, |_, _| true).await?;
                events.send(
                    BlobCertified::for_testing_with_object_id(*blob_details.blob_id(), object_id)
                        .into(),
                )?;
                details.push(blob_details);
            }

            // These blobs will be expired at epoch 3.
            for blob in blobs_expired {
                let blob_details = EncodedBlob::new(blob, config.clone());
                let object_id = ObjectID::random();
                events.send(
                    BlobRegistered {
                        end_epoch: 3,
                        ..BlobRegistered::for_testing_with_object_id(
                            *blob_details.blob_id(),
                            object_id,
                        )
                    }
                    .into(),
                )?;
                store_at_shards(&blob_details, &cluster, |_, _| false).await?;
                events.send(
                    BlobCertified {
                        end_epoch: 3,
                        ..BlobCertified::for_testing_with_object_id(
                            *blob_details.blob_id(),
                            object_id,
                        )
                    }
                    .into(),
                )?;
            }

            // Advance cluster to epoch 4.
            advance_cluster_to_epoch(&cluster, &[&events], 4).await?;
        }

        // Makes storage inner mutable so that we can manually add another shard to node 1.
        let node_inner = unsafe {
            &mut *(Arc::as_ptr(cluster.nodes[1].storage_node.inner_for_test())
                as *mut StorageNodeInner)
        };
        node_inner
            .storage_for_test()
            .create_storage_for_shards(&[ShardIndex(0)])
            .await?;
        let shard_storage_dst = node_inner
            .storage_for_test()
            .shard_storage(ShardIndex(0))
            .await
            .expect("shard storage should exist");
        shard_storage_dst
            .update_status_in_test(ShardStatus::None)
            .await?;

        // Starts the shard syncing process in the new shard, which should only use happy path
        // shard sync to sync non-expired certified blobs.
        cluster.nodes[1]
            .storage_node
            .shard_sync_handler_for_test()
            .start_sync_shards(vec![ShardIndex(0)], false)
            .await?;

        // Waits for the shard sync process to stop.
        wait_until_no_sync_tasks(&cluster.nodes[1].storage_node.shard_sync_handler_for_test())
            .await?;

        // All blobs should be recovered in the new dst node.
        check_all_blobs_are_synced(
            &details,
            node_inner.storage_for_test(),
            &shard_storage_dst,
            &[],
        )?;

        // Checks that the shard sync progress is reset.
        assert!(
            shard_storage_dst
                .get_last_synced_blob_id()
                .expect("getting last synced blob id should succeed")
                .is_none()
        );

        // Checks that shard sync recovery is not triggered.
        assert!(!shard_sync_recovery_triggered.load(Ordering::SeqCst));

        Ok(())
    }

    // Tests crash recovery of shard transfer partially using shard recovery functionality
    // and partially using shard sync.
    simtest_param_test! {
        sync_shard_shard_recovery_restart -> TestResult: [
            primary1: (1, SliverType::Primary, false),
            primary5: (5, SliverType::Primary, false),
            primary10: (10, SliverType::Primary, false),
            secondary1: (1, SliverType::Secondary, false),
            secondary5: (5, SliverType::Secondary, false),
            secondary10: (10, SliverType::Secondary, false),
            restart_after_recovery: (10, SliverType::Secondary, true),
        ]
    }
    async fn sync_shard_shard_recovery_restart(
        break_index: u64,
        sliver_type: SliverType,
        restart_after_recovery: bool,
    ) -> TestResult {
        register_fail_point_if("fail_point_after_start_recovery", move || {
            restart_after_recovery
        });
        if !restart_after_recovery {
            register_fail_point_arg(
                "fail_point_fetch_sliver",
                move || -> Option<(SliverType, u64, bool)> {
                    Some((sliver_type, break_index, false))
                },
            );
        }

        // Skip retry loop in shard sync to simulate a reboot.
        register_fail_point_if("fail_point_shard_sync_no_retry", || true);

        let skip_stored_blob_index: [usize; 12] = [3, 4, 5, 9, 10, 11, 15, 18, 19, 20, 21, 22];
        let (cluster, blob_details, _) = setup_shard_recovery_test_cluster(
            |blob_index| !skip_stored_blob_index.contains(&blob_index),
            |_| 42,
            |_| false,
        )
        .await?;

        let node_inner = unsafe {
            &mut *(Arc::as_ptr(cluster.nodes[1].storage_node.inner_for_test())
                as *mut StorageNodeInner)
        };
        node_inner
            .storage_for_test()
            .create_storage_for_shards(&[ShardIndex(0)])
            .await?;
        let shard_storage_dst = node_inner
            .storage_for_test()
            .shard_storage(ShardIndex(0))
            .await
            .expect("shard storage should exist");
        shard_storage_dst
            .update_status_in_test(ShardStatus::None)
            .await?;

        cluster.nodes[1]
            .storage_node
            .shard_sync_handler_for_test()
            .start_sync_shards(vec![ShardIndex(0)], false)
            .await?;
        // Waits for the shard sync process to stop.
        wait_until_no_sync_tasks(&cluster.nodes[1].storage_node.shard_sync_handler_for_test())
            .await?;

        // Check that shard sync process is not finished.
        if !restart_after_recovery {
            let shard_storage_src = cluster.nodes[0]
                .storage_node
                .inner_for_test()
                .storage_for_test()
                .shard_storage(ShardIndex(0))
                .await
                .expect("shard storage should exist");
            assert!(
                shard_storage_dst.sliver_count(SliverType::Primary)
                    < shard_storage_src.sliver_count(SliverType::Primary)
                    || shard_storage_dst.sliver_count(SliverType::Secondary)
                        < shard_storage_src.sliver_count(SliverType::Secondary)
            );
        }

        clear_fail_point("fail_point_after_start_recovery");
        if !restart_after_recovery {
            clear_fail_point("fail_point_fetch_sliver");
        }

        // restart the shard syncing process, to simulate a reboot.
        cluster.nodes[1]
            .storage_node
            .shard_sync_handler_for_test()
            .restart_syncs()
            .await?;

        wait_for_shard_in_active_state(shard_storage_dst.as_ref()).await?;
        check_all_blobs_are_synced(
            &blob_details,
            node_inner.storage_for_test(),
            shard_storage_dst.as_ref(),
            &[],
        )?;

        // Checks that the shard sync progress is reset.
        assert!(
            shard_storage_dst
                .get_last_synced_blob_id()
                .expect("getting last synced blob id should succeed")
                .is_none()
        );

        Ok(())
    }

    // Tests shard metadata recovery with failure injection.
    simtest_param_test! {
        sync_shard_recovery_metadata_restart -> TestResult: [
            fail_before_start_fetching: (true),
            fail_during_fetching: (false),
        ]
    }
    async fn sync_shard_recovery_metadata_restart(fail_before_start_fetching: bool) -> TestResult {
        let (cluster, blob_details, storage_dst, shard_storage_set) =
            setup_cluster_for_shard_sync_tests(None, None).await?;

        assert_eq!(shard_storage_set.shard_storage.len(), 1);
        let shard_storage_dst = shard_storage_set.shard_storage[0].clone();
        if fail_before_start_fetching {
            register_fail_point_if(
                "fail_point_shard_sync_recovery_metadata_error_before_fetch",
                || true,
            );
        } else {
            let total_blobs = blob_details.len() as u64;
            // Randomly pick a blob index to inject failure.
            // Note that the scan count starts from 1.
            let break_scan_count = rand::thread_rng().gen_range(1..=total_blobs);

            register_fail_point_arg(
                "fail_point_shard_sync_recovery_metadata_error_during_fetch",
                move || -> Option<u64> { Some(break_scan_count) },
            );
        }

        storage_dst.clear_metadata_in_test()?;
        storage_dst.set_node_status(NodeStatus::RecoverMetadata)?;

        // Starts the shard syncing process in the new shard, which will fail at the specified
        // break index.
        cluster.nodes[1]
            .storage_node
            .shard_sync_handler_for_test()
            .start_sync_shards(vec![ShardIndex(0)], true)
            .await?;

        // Waits for the shard sync process to stop.
        wait_until_no_sync_tasks(&cluster.nodes[1].storage_node.shard_sync_handler_for_test())
            .await?;

        assert!(
            shard_storage_dst
                .status()
                .await
                .expect("should succeed in test")
                == ShardStatus::None
        );

        if fail_before_start_fetching {
            clear_fail_point("fail_point_shard_sync_recovery_metadata_error_before_fetch");
        } else {
            clear_fail_point("fail_point_shard_sync_recovery_metadata_error_during_fetch");
        }

        // restart the shard syncing process, to simulate a reboot.
        cluster.nodes[1]
            .storage_node
            .shard_sync_handler_for_test()
            .restart_syncs()
            .await?;

        // Waits for the shard to be synced.
        wait_until_no_sync_tasks(&cluster.nodes[1].storage_node.shard_sync_handler_for_test())
            .await?;

        // Checks that the shard is completely migrated.
        check_all_blobs_are_synced(&blob_details, &storage_dst, &shard_storage_dst, &[])?;

        // Checks that the shard sync progress is reset.
        assert!(
            shard_storage_dst
                .get_last_synced_blob_id()
                .expect("getting last synced blob id should succeed")
                .is_none()
        );

        Ok(())
    }

    #[walrus_simtest]
    async fn finish_epoch_change_start_should_not_block_event_processing() -> TestResult {
        walrus_test_utils::init_tracing();

        // It is important to only use one node in this test, so that no other node would
        // drive epoch change on chain, and send events to the nodes.
        let (cluster, events, _blob_detail) =
            cluster_with_initial_epoch_and_certified_blobs(&[&[0, 1, 2, 3]], &[BLOB], 2, None)
                .await?;
        cluster.nodes[0]
            .storage_node
            .start_epoch_change_finisher_for_test()
            .wait_until_previous_task_done()
            .await;

        // There should be 6 initial events:
        //  - 2x EpochChangeStart
        //  - 2x EpochChangeDone
        //  - BlobRegistered
        //  - BlobCertified
        wait_until_events_processed_exact(&cluster.nodes[0], 6).await?;

        let processed_event_count_initial = &cluster.nodes[0]
            .storage_node
            .inner_for_test()
            .storage_for_test()
            .get_sequentially_processed_event_count()?;

        // Use fail point to block finishing epoch change start event.
        let unblock = Arc::new(Notify::new());
        let unblock_clone = unblock.clone();
        register_fail_point_async("blocking_finishing_epoch_change_start", move || {
            let unblock_clone = unblock_clone.clone();
            async move {
                unblock_clone.notified().await;
            }
        });

        // Update mocked on chain committee to the new epoch.
        cluster
            .lookup_service_handle
            .clone()
            .unwrap()
            .advance_epoch();

        // Sends one epoch change start event which will be blocked finishing.
        events.send(ContractEvent::EpochChangeEvent(
            EpochChangeEvent::EpochChangeStart(EpochChangeStart {
                epoch: 3,
                event_id: walrus_sui::test_utils::event_id_for_testing(),
            }),
        ))?;

        // Register and certified a blob, and then check the blob should be certified in the
        // node indicating that the event processing is not blocked.
        assert_eq!(
            cluster.nodes[0]
                .storage_node
                .as_ref()
                .blob_status(&OTHER_BLOB_ID)
                .expect("getting blob status should succeed"),
            BlobStatus::Nonexistent
        );

        // Must send the blob registered and certified events with the same epoch as the
        // epoch change start event.
        events.send(
            BlobRegistered {
                epoch: 3,
                ..BlobRegistered::for_testing(OTHER_BLOB_ID)
            }
            .into(),
        )?;
        events.send(
            BlobCertified {
                epoch: 3,
                ..BlobCertified::for_testing(OTHER_BLOB_ID)
            }
            .into(),
        )?;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if cluster.nodes[0]
                    .storage_node
                    .inner_for_test()
                    .is_blob_certified_for_test(&OTHER_BLOB_ID)
                    .expect("getting blob status should succeed")
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await?;

        // Persist event count should remain the same as the beginning since we haven't
        // unblock epoch change start event.
        assert_eq!(
            processed_event_count_initial,
            &cluster.nodes[0]
                .storage_node
                .inner_for_test()
                .storage_for_test()
                .get_sequentially_processed_event_count()?
        );

        // Unblock the epoch change start event, and expect that processed event count should
        // make progress. Use `+2` instead of `+3` is because certify blob initiates a blob
        // sync, and sync we don't upload the blob data, so it won't get processed. The point
        // here is that the epoch change start event should be marked completed.
        unblock.notify_one();
        wait_until_events_processed_exact(&cluster.nodes[0], processed_event_count_initial + 2)
            .await?;

        Ok(())
    }

    // Tests that storage node lag check is not affected by the blob writer cursor.
    #[walrus_simtest]
    async fn event_blob_cursor_should_not_affect_node_state() -> TestResult {
        walrus_test_utils::init_tracing();

        // Set the initial cursor to a high value to simulate a severe lag to
        // blob writer cursor.
        register_fail_point_arg(
            "storage_node_initial_cursor",
            || -> Option<EventStreamCursor> {
                Some(EventStreamCursor::new(Some(event_id_for_testing()), 10000))
            },
        );

        // Set the epoch check to a high value to simulate a severe lag to epoch check.
        register_fail_point_arg(
            "event_processing_epoch_check",
            || -> Option<walrus_core::Epoch> { Some(100) },
        );

        // Create a cluster and send some events.
        let (cluster, events) = cluster_at_epoch1_without_blobs(&[&[0]], None).await?;
        events.send(BlobRegistered::for_testing(BLOB_ID).into())?;
        tokio::time::sleep(Duration::from_secs(10)).await;

        // Expect that the node is not in recovery catch up mode because the lag check should
        // not be triggered.
        assert!(
            !cluster.nodes[0]
                .storage_node
                .inner_for_test()
                .storage_for_test()
                .node_status()?
                .is_catching_up()
        );

        Ok(())
    }

    // Tests shard recovery with expired, invalid, and deleted blobs.
    //
    // When `skip_blob_certification_at_recovery_beginning` is true, it simulates the case where
    // the shard recovery of the blob is already in progress, and then the blob becomes expired,
    // invalid, or deleted.
    //
    // Although both tests can run under `cargo nextest`, `check_certification_during_recovery`
    // only works when running in simtest, since it uses failpoints to skip initial blob
    // certification check.
    simtest_param_test! {
        shard_recovery_blob_not_recover_expired_invalid_deleted_blobs -> TestResult: [
            check_certification_at_beginning: (false),
            check_certification_during_recovery: (true),
        ]
    }
    async fn shard_recovery_blob_not_recover_expired_invalid_deleted_blobs(
        skip_blob_certification_at_recovery_beginning: bool,
    ) -> TestResult {
        register_fail_point_if(
            "shard_recovery_skip_initial_blob_certification_check",
            move || skip_blob_certification_at_recovery_beginning,
        );

        let skip_stored_blob_index = [3, 4, 5, 9, 10, 11, 15, 18, 19, 20, 21, 22];
        let expired_blob_index = [3];
        let deletable_blob_index = [9];
        let invalid_blob_index = [19];
        let non_synced_blob_index =
            [expired_blob_index, deletable_blob_index, invalid_blob_index].concat();

        let (cluster, blob_details, event_senders) = setup_shard_recovery_test_cluster(
            |blob_index| !skip_stored_blob_index.contains(&blob_index),
            // Blob 3 expires at epoch 2, which is the current epoch when
            // `setup_shard_recovery_test_cluster` returns.
            |blob_index| {
                if expired_blob_index.contains(&blob_index) {
                    2
                } else {
                    42
                }
            },
            |blob_index| deletable_blob_index.contains(&blob_index),
        )
        .await?;

        // Delete blob 9 and invalidate blob 19.
        for i in invalid_blob_index {
            event_senders
                .all_other_node_events
                .send(InvalidBlobId::for_testing(*blob_details[i].blob_id()).into())?;
        }
        for i in deletable_blob_index {
            event_senders.all_other_node_events.send(
                BlobDeleted::for_testing_with_object_id(
                    *blob_details[i].blob_id(),
                    blob_details[i].object_id.unwrap(),
                )
                .into(),
            )?;
        }

        // Make sure that blobs in `skip_stored_blob_index` are not certified in node 0.
        for i in skip_stored_blob_index {
            let blob_id = blob_details[i].blob_id();
            let blob_info = cluster.nodes[0]
                .storage_node
                .inner_for_test()
                .storage_for_test()
                .get_blob_info(blob_id);
            if deletable_blob_index.contains(&i) {
                assert!(matches!(
                    blob_info.unwrap().unwrap().to_blob_status(2),
                    BlobStatus::Deletable {
                        deletable_counts: walrus_storage_node_client::api::DeletableCounts {
                            count_deletable_total: 1,
                            count_deletable_certified: 0,
                        },
                        ..
                    }
                ));
            } else {
                let blob_status = blob_info.unwrap().unwrap().to_blob_status(2);
                if expired_blob_index.contains(&i) {
                    assert!(
                        matches!(blob_status, BlobStatus::Nonexistent),
                        "unexpected blob status for expired blob {i} ({blob_id}): \
                        {blob_status:?}",
                    );
                } else {
                    assert!(
                        matches!(blob_status, BlobStatus::Permanent { .. }),
                        "unexpected blob status for blob {i} ({blob_id}): {blob_status:?}",
                    );
                }
            }
        }

        let node_inner = unsafe {
            &mut *(Arc::as_ptr(cluster.nodes[1].storage_node.inner_for_test())
                as *mut StorageNodeInner)
        };
        node_inner
            .storage_for_test()
            .create_storage_for_shards(&[ShardIndex(0)])
            .await?;
        let shard_storage_dst = node_inner
            .storage_for_test()
            .shard_storage(ShardIndex(0))
            .await
            .expect("shard storage should exist");
        shard_storage_dst
            .update_status_in_test(ShardStatus::None)
            .await?;

        cluster.nodes[1]
            .storage_node
            .shard_sync_handler_for_test()
            .start_sync_shards(vec![ShardIndex(0)], false)
            .await?;

        // Shard recovery should be completed, and all the data should be synced.
        wait_for_shard_in_active_state(shard_storage_dst.as_ref()).await?;
        check_all_blobs_are_synced(
            &blob_details,
            node_inner.storage_for_test(),
            shard_storage_dst.as_ref(),
            &non_synced_blob_index,
        )?;

        // Checks that the shard sync progress is reset.
        assert!(
            shard_storage_dst
                .get_last_synced_blob_id()
                .expect("getting last synced blob id should succeed")
                .is_none()
        );

        clear_fail_point("shard_recovery_skip_initial_blob_certification_check");

        Ok(())
    }

    // Tests that blob metadata sync can be cancelled when the blob is expired, deleted, or
    //invalidated.
    #[walrus_simtest]
    async fn shard_recovery_cancel_metadata_sync_when_blob_expired_deleted_invalidated()
    -> TestResult {
        register_fail_point_if("get_metadata_return_unavailable", move || true);

        // The test creates 3 blobs:
        //  - Blob 0 expires at epoch 3.
        //  - Blob 1 is a deletable blob.
        //  - Blob 2 is an invalid blob.

        let deletable_blob_index: [usize; 1] = [1];
        let (cluster, blob_details, event_senders) =
            setup_shard_recovery_test_cluster_with_blob_count(
                3,
                |_blob_index| true,
                // Blob 0 expires at epoch 3, which is the next epoch when
                // `setup_shard_recovery_test_cluster` returns.
                |blob_index| if blob_index == 0 { 3 } else { 42 },
                |blob_index| deletable_blob_index.contains(&blob_index),
            )
            .await?;

        // Setup node 1 to sync recovery shard 0 from node 0.
        let node_inner = unsafe {
            &mut *(Arc::as_ptr(cluster.nodes[1].storage_node.inner_for_test())
                as *mut StorageNodeInner)
        };
        node_inner
            .storage_for_test()
            .create_storage_for_shards(&[ShardIndex(0)])
            .await?;
        node_inner.storage_for_test().clear_metadata_in_test()?;
        node_inner.set_node_status_for_test(NodeStatus::RecoverMetadata)?;

        let shard_storage_dst = node_inner
            .storage_for_test()
            .shard_storage(ShardIndex(0))
            .await
            .expect("shard storage should exist");
        shard_storage_dst
            .update_status_in_test(ShardStatus::None)
            .await?;

        cluster.nodes[1]
            .storage_node
            .shard_sync_handler_for_test()
            .start_sync_shards(vec![ShardIndex(0)], true)
            .await?;

        tokio::time::sleep(Duration::from_secs(1)).await;
        // After the sync starts, the node status should stay at `RecoverMetadata`.
        assert_eq!(
            node_inner.storage_for_test().node_status().unwrap(),
            NodeStatus::RecoverMetadata
        );
        // Setup complete, now we can start the test.

        let unblock = Arc::new(Notify::new());

        // Send blob deletion event for blob 1.
        {
            let blob = &blob_details[1];
            tracing::info!("send blob deletion event for blob {:?}", blob.blob_id());
            event_senders.all_other_node_events.send(
                BlobDeleted::for_testing_with_object_id(
                    *blob.blob_id(),
                    blob.object_id.expect("object id should be set"),
                )
                .into(),
            )?;
        }

        // Send invalid blob event for blob 2.
        {
            let blob = &blob_details[2];
            tracing::info!("send invalid blob event for blob {:?}", blob.blob_id());
            event_senders
                .all_other_node_events
                .send(InvalidBlobId::for_testing(*blob.blob_id()).into())?;
        }

        // Advance to epoch 3, so that blob 0 expires.
        {
            let unblock_clone = unblock.clone();
            register_fail_point_async("blocking_finishing_epoch_change_start", move || {
                let unblock_clone = unblock_clone.clone();
                async move {
                    unblock_clone.notified().await;
                }
            });

            tracing::info!("advance to epoch 3");
            advance_cluster_to_epoch(
                &cluster,
                &[
                    &event_senders.node_0_events,
                    &event_senders.all_other_node_events,
                ],
                3,
            )
            .await?;
        }

        // Shard recovery should be completed, and all the data should be synced.
        wait_for_shard_in_active_state(shard_storage_dst.as_ref()).await?;

        // Cleanup the test environment.
        unblock.notify_one();
        clear_fail_point("get_metadata_return_unavailable");
        clear_fail_point("blocking_finishing_epoch_change_start");
        Ok(())
    }

    // Tests that blob events for the same blob are always processed in order. This is a
    // randomized test in a way that the order of events is random based on the random seed.
    // So single test run passing is not a guarantee to cover all the test cases.
    #[walrus_simtest]
    async fn no_out_of_order_blob_certify_and_delete_event_processing() -> TestResult {
        let shards: &[&[u16]] = &[&[1], &[0, 2, 3, 4, 5, 6]];
        let test_shard = ShardIndex(1);

        // Add delay between checking if a blob needs to recover and actual starting the
        // recover process. This window is where other events can break event processing order.
        register_fail_point_async("fail_point_process_blob_certified_event", || async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let (cluster, events) = cluster_at_epoch1_without_blobs(shards, None).await?;

        // Randomly generated blob.
        let random_blob = walrus_test_utils::random_data_list(10, 1);

        let config = cluster.encoding_config();
        let blob = EncodedBlob::new(&random_blob[0], config);

        let object_id = ObjectID::random();

        // Do not store the sliver in the first node.
        events.send(
            BlobRegistered {
                deletable: true,
                object_id: object_id.clone(),
                ..BlobRegistered::for_testing(*blob.blob_id())
            }
            .into(),
        )?;
        store_at_shards(&blob, &cluster, |&shard, _| shard != test_shard).await?;

        let node_client = cluster.client(0);
        let pair_to_sync = blob.assigned_sliver_pair(test_shard);

        node_client
            .get_sliver_by_type(blob.blob_id(), pair_to_sync.index(), SliverType::Primary)
            .await
            .expect_err("sliver should not yet be available");

        // Sends certified event followed by delete event.
        events.send(
            BlobCertified {
                deletable: true,
                object_id: object_id.clone(),
                ..BlobCertified::for_testing(*blob.blob_id()).into()
            }
            .into(),
        )?;
        tokio::time::sleep(Duration::from_secs(1)).await;
        events.send(
            BlobDeleted {
                object_id: object_id.clone(),
                ..BlobDeleted::for_testing(*blob.blob_id())
            }
            .into(),
        )?;

        // Wait for the blob sync to complete.
        tokio::time::sleep(Duration::from_secs(10)).await;

        // There shouldn't be any blob sync in progress.
        assert!(
            cluster.nodes[0]
                .storage_node
                .blob_sync_in_progress_for_test()
                .is_empty()
        );

        clear_fail_point("fail_point_process_blob_certified_event");
        Ok(())
    }

    // Tests that the blob event processor can correctly wait for all events to be processed.
    #[walrus_simtest]
    async fn test_blob_event_processor_wait_for_all_events_to_be_processed() -> TestResult {
        let shards: &[&[u16]] = &[&[1], &[0, 2, 3, 4, 5, 6]];
        let test_shard = ShardIndex(1);

        let blocking_notify = Arc::new(Notify::new());
        let unblock_notify = Arc::new(Notify::new());
        let blocking_notify_clone = blocking_notify.clone();
        let unblock_notify_clone = unblock_notify.clone();

        // Here, we block the certified event processing in storage node 0. In this test, the
        // initial blob upload will not upload to storage node 0, shard index 1.
        register_fail_point_async("fail_point_process_blob_certified_event", move || {
            let blocking_notify_clone = blocking_notify_clone.clone();
            let unblock_notify_clone = unblock_notify_clone.clone();
            async move {
                // Notify the test body that we are blocking.
                blocking_notify_clone.notify_one();
                // Wait for the test body to unblock.
                unblock_notify_clone.notified().await;
            }
        });

        let (cluster, events) = cluster_at_epoch1_without_blobs(shards, None).await?;

        // Randomly generated blob.
        let random_blob = walrus_test_utils::random_data_list(10, 1);

        let config = cluster.encoding_config();
        let blob = EncodedBlob::new(&random_blob[0], config);

        let object_id = ObjectID::random();

        // Do not store the sliver in the first node.
        events.send(
            BlobRegistered {
                deletable: true,
                object_id: object_id.clone(),
                ..BlobRegistered::for_testing(*blob.blob_id())
            }
            .into(),
        )?;
        store_at_shards(&blob, &cluster, |&shard, _| shard != test_shard).await?;

        let node = cluster.nodes[0].storage_node.clone();
        // Initially, no event processing is blocked, so wait_for_all_events_to_be_processed()
        // should return promptly.
        tokio::time::timeout(
            Duration::from_secs(5),
            node.get_pending_event_counter()
                .wait_for_all_events_to_be_processed(),
        )
        .await
        .expect("wait for all events to be processed should succeed");

        // Now we unblock the certified event processing.
        events.send(
            BlobCertified {
                deletable: true,
                object_id: object_id.clone(),
                ..BlobCertified::for_testing(*blob.blob_id()).into()
            }
            .into(),
        )?;

        // Wait for the fail point to be triggered, and certified event processing to be
        // blocked.
        blocking_notify.notified().await;

        // Now since the certified event is blocked, the blob event processor should not be able
        // to process the certified event, and therefore wait_for_all_events_to_be_processed()
        // should timeout.
        tokio::time::timeout(
            Duration::from_secs(10),
            node.get_pending_event_counter()
                .wait_for_all_events_to_be_processed(),
        )
        .await
        .expect_err("wait for all events to be processed should timeout");

        // Notify the fail point to unblock the certified event processing. After this, all the
        // events should have been processed.
        unblock_notify.notify_one();

        // After certified event processing is unblocked, all the events should have been
        // processed, and therefore wait_for_all_events_to_be_processed() should return
        // promptly.
        tokio::time::timeout(
            Duration::from_secs(5),
            node.get_pending_event_counter()
                .wait_for_all_events_to_be_processed(),
        )
        .await
        .expect("wait for all events to be processed should succeed");

        clear_fail_point("fail_point_process_blob_certified_event");
        Ok(())
    }

    #[walrus_simtest]
    async fn flush_after_registration() -> TestResult {
        walrus_test_utils::init_tracing();

        let flush_blocked = Arc::new(Notify::new());
        let release_flush = Arc::new(Notify::new());
        let flush_blocked_clone = flush_blocked.clone();
        let release_flush_clone = release_flush.clone();
        let certify_started = Arc::new(Notify::new());
        let certify_started_clone = certify_started.clone();

        register_fail_point_async("fail_point_flush_pending_caches_with_logging", move || {
            let flush_blocked = flush_blocked_clone.clone();
            let release_flush = release_flush_clone.clone();
            async move {
                flush_blocked.notify_one();
                release_flush.notified().await;
            }
        });

        register_fail_point_async("fail_point_process_blob_certified_event", move || {
            let certify_started = certify_started_clone.clone();
            async move {
                certify_started.notify_one();
            }
        });

        let (cluster, events) = cluster_at_epoch1_without_blobs(&[&[0, 1, 2, 3]], None).await?;
        let storage_node = cluster.nodes[0].storage_node.clone();

        let encoded = EncodedBlob::new(BLOB, cluster.encoding_config());
        let blob_id = *encoded.blob_id();
        let sliver_pair = encoded.assigned_sliver_pair(SHARD_INDEX);

        storage_node
            .store_metadata(
                encoded.metadata.clone().into_unverified(),
                UploadIntent::Pending,
            )
            .await?;
        storage_node
            .store_sliver(
                blob_id,
                sliver_pair.index(),
                Sliver::Primary(sliver_pair.primary.clone()),
                UploadIntent::Pending,
            )
            .await?;

        events.send(BlobRegistered::for_testing(blob_id).into())?;
        events.send(BlobCertified::for_testing(blob_id).into())?;

        tokio::time::timeout(Duration::from_secs(5), flush_blocked.notified())
            .await
            .expect("cache flush should hit failpoint before certification runs");
        tokio::time::timeout(Duration::from_secs(1), certify_started.notified())
            .await
            .expect_err(
                "certified event processing should not start before pending caches are flushed",
            );
        release_flush.notify_one();
        tokio::time::timeout(Duration::from_secs(5), certify_started.notified())
            .await
            .expect("certified event processing should start after flush completes");

        storage_node
            .get_pending_event_counter()
            .wait_for_all_events_to_be_processed()
            .await;

        let shard_storage = storage_node
            .as_ref()
            .inner_for_test()
            .storage_for_test()
            .shard_storage(SHARD_INDEX)
            .await
            .expect("shard storage should exist");

        let stored_sliver = shard_storage
            .get_sliver(&blob_id, SliverType::Primary)
            .expect("sliver lookup should succeed")
            .expect("pending sliver should be flushed before certified handling");
        assert_eq!(
            stored_sliver,
            Sliver::Primary(sliver_pair.primary.clone()),
            "sliver contents should match the uploaded data"
        );
        assert!(
            storage_node
                .as_ref()
                .inner_for_test()
                .storage_for_test()
                .get_metadata(&blob_id)
                .expect("metadata lookup should succeed")
                .is_some(),
            "pending metadata should be flushed before certified handling"
        );

        clear_fail_point("fail_point_flush_pending_caches_with_logging");
        clear_fail_point("fail_point_process_blob_certified_event");
        Ok(())
    }

    // Tests that blob extension can trigger blob sync.
    #[walrus_simtest]
    async fn blob_extension_triggers_blob_sync() -> TestResult {
        let _ = tracing_subscriber::fmt::try_init();

        // Set the fail point to skip non-extension certified event triggered blob sync.
        register_fail_point_if(
            "skip_non_extension_certified_event_triggered_blob_sync",
            move || true,
        );

        let shards: &[&[u16]] = &[&[1, 6], &[0, 2, 3, 4, 5]];
        let own_shards = [ShardIndex(1), ShardIndex(6)];

        let (cluster, events, blob) =
            cluster_with_partially_stored_blob(shards, BLOB, |shard, _| {
                !own_shards.contains(shard)
            })
            .await?;
        let node_client = cluster.client(0);

        // Send a certified event for the blob.
        events.send(BlobCertified::for_testing(*blob.blob_id()).into())?;

        // Send a certified event for the blob with extension. This should trigger blob sync.
        events.send(
            BlobCertified {
                end_epoch: 62,
                is_extension: true,
                ..BlobCertified::for_testing(*blob.blob_id())
            }
            .into(),
        )?;

        // Wait for the blob sync to complete.
        tokio::time::sleep(Duration::from_secs(10)).await;

        // Check that the sliver pairs are stored in the shards.
        for shard in own_shards {
            let synced_sliver_pair =
                expect_sliver_pair_stored_before_timeout(&blob, node_client, shard, TIMEOUT).await;
            let expected = blob.assigned_sliver_pair(shard);

            assert_eq!(
                synced_sliver_pair, *expected,
                "invalid sliver pair for {shard}"
            );
        }

        Ok(())
    }
}
