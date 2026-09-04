// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Contains simtests for the cross-node determinism and the on-chain certification of blob info
//! snapshots.

#![recursion_limit = "256"]

#[cfg(msim)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        sync::Arc,
        time::Duration,
    };

    use sui_types::base_types::ObjectID;
    use twox_hash::XxHash64;
    use walrus_core::{BlobId, Epoch, encoding::Primary};
    use walrus_proc_macros::walrus_simtest;
    use walrus_service::test_utils::{SimStorageNodeHandle, TestNodesConfig, test_cluster};
    use walrus_simtest::test_utils::simtest_utils::{self, BlobInfoConsistencyCheck};
    use walrus_sui::client::ReadClient;

    const EPOCH_DURATION: Duration = Duration::from_secs(30);

    /// The node weights of the certification clusters: 13 shards, so that a quorum needs 9.
    const NODE_WEIGHTS: [u16; 5] = [1, 2, 3, 3, 4];

    /// Checks that all nodes serialize identical blob info snapshots at each epoch boundary.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_blob_info_snapshot_digests_match_across_nodes() {
        let blob_info_consistency_check = BlobInfoConsistencyCheck::new();

        let (_sui_cluster, _walrus_cluster, client, _, _) =
            test_cluster::E2eTestSetupBuilder::new()
                .with_epoch_duration(Duration::from_secs(30))
                .with_test_nodes_config(
                    TestNodesConfig::builder()
                        .with_node_weights(&[1, 2, 3, 3, 4])
                        .build(),
                )
                .build_generic::<SimStorageNodeHandle>()
                .await
                .unwrap();

        let workload_handle =
            simtest_utils::start_background_workload(Arc::new(client), false, None, None);

        // Let several epoch boundaries pass so that multiple snapshots are produced.
        tokio::time::sleep(Duration::from_mins(3)).await;

        workload_handle.abort();

        blob_info_consistency_check.check_storage_node_consistency();
    }

    /// Returns a cluster builder with [`NODE_WEIGHTS`] in which the nodes flagged in `certify`
    /// certify their blob info snapshots on chain.
    fn certification_cluster_builder(certify: &[bool]) -> test_cluster::E2eTestSetupBuilder {
        test_cluster::E2eTestSetupBuilder::new()
            .with_epoch_duration(EPOCH_DURATION)
            .with_test_nodes_config(
                TestNodesConfig::builder()
                    .with_node_weights(&NODE_WEIGHTS)
                    .with_blob_info_snapshot_certify(certify)
                    .build(),
            )
    }

    fn node_capability_id(node: &SimStorageNodeHandle) -> ObjectID {
        node.storage_node_capability
            .as_ref()
            .expect("nodes of an e2e cluster have a storage node capability")
            .id
    }

    /// Returns the blob ID all nodes reported for the snapshot of `epoch`, asserting that they
    /// agree.
    fn single_blob_id(blob_ids: &HashMap<ObjectID, BlobId>, epoch: Epoch) -> BlobId {
        let distinct: HashSet<_> = blob_ids.values().copied().collect();
        assert_eq!(
            distinct.len(),
            1,
            "nodes disagree on the snapshot blob ID of epoch {epoch}: {blob_ids:?}"
        );
        *distinct
            .into_iter()
            .next()
            .as_ref()
            .expect("one distinct blob ID")
    }

    /// Checks that, with certification enabled on every node, the snapshot of each epoch is
    /// certified on chain, that the certified blob ID is the one every node produced, that every
    /// node finds its snapshot certified at the next boundary (and so keeps it), and that a
    /// certified snapshot can be read back through the regular read path.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_blob_info_snapshot_certified_by_quorum() {
        certified_by_quorum_scenario([true; 5]).await;
    }

    /// Checks the same with certification enabled on a quorum subset only (the staged rollout):
    /// the non-attesting node produces the same snapshot, keeps nothing to reconcile, and the
    /// snapshots of all nodes stay identical after the certified blob is recovered by it.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_blob_info_snapshot_certified_by_quorum_subset() {
        // The lightest node does not certify; the other four hold 12 of the 13 shards.
        certified_by_quorum_scenario([false, true, true, true, true]).await;
    }

    async fn certified_by_quorum_scenario(certify: [bool; 5]) {
        let consistency_check = BlobInfoConsistencyCheck::new();
        let (_sui_cluster, walrus_cluster, client, _, _) = certification_cluster_builder(&certify)
            .build_generic::<SimStorageNodeHandle>()
            .await
            .unwrap();
        let client = Arc::new(client);
        let workload_handle =
            simtest_utils::start_background_workload(client.clone(), false, None, None);
        let nodes = &walrus_cluster.nodes;
        let node_ids: Vec<_> = nodes.iter().map(node_capability_id).collect();

        // The snapshot of epoch E is certified during E, and the contract keeps every
        // certification whose storage has not ended, so waiting for epoch 4 leaves a few epochs
        // of history to check.
        const TARGET_EPOCH: Epoch = 4;
        let latest = simtest_utils::wait_for_certified_snapshot_blob(
            &client,
            TARGET_EPOCH,
            EPOCH_DURATION * (TARGET_EPOCH + 2),
        )
        .await;
        tracing::info!(?latest, "latest certified blob info snapshot");
        // Once every node has applied the latest epoch, all of them have processed the
        // certification events of the earlier epochs and reconciled those publications.
        simtest_utils::wait_for_nodes_to_reach_epoch(nodes, latest.epoch, 2 * EPOCH_DURATION).await;

        let mut certified_blob_ids = HashMap::new();
        for epoch in latest.epoch - 2..=latest.epoch {
            let certified = client
                .inner
                .sui_client()
                .read_client
                .certified_snapshot_blob_for_epoch(epoch)
                .await
                .expect("reading the certification history should succeed")
                .unwrap_or_else(|| panic!("the snapshot of epoch {epoch} must be certified"));
            let blob_ids = consistency_check
                .wait_for_blob_info_snapshot_blob_ids(epoch, node_ids.len(), EPOCH_DURATION)
                .await;
            for node_id in &node_ids {
                assert_eq!(
                    blob_ids.get(node_id),
                    Some(&certified.blob_id),
                    "node {node_id} must have produced the certified snapshot of epoch {epoch}"
                );
            }
            if epoch < latest.epoch {
                let reconciled = consistency_check.blob_info_snapshot_reconciled(epoch);
                for (node_id, certifies) in node_ids.iter().zip(certify) {
                    let expected = if certifies { Some(&true) } else { None };
                    assert_eq!(
                        reconciled.get(node_id),
                        expected,
                        "node {node_id} (certifies: {certifies}) has the wrong reconciliation \
                        outcome for epoch {epoch}"
                    );
                }
            }
            certified_blob_ids.insert(epoch, certified.blob_id);
        }
        workload_handle.abort();

        // Reading a certified snapshot back exercises the slivers stored by the committee of its
        // epoch, and the content must hash to the digest the nodes reported.
        let read_epoch = latest.epoch - 1;
        let content = client
            .inner
            .read_blob::<Primary>(&certified_blob_ids[&read_epoch])
            .await
            .expect("the certified blob info snapshot must be readable");
        let digest = XxHash64::oneshot(0, &content);
        let node_digests = consistency_check.blob_info_snapshot_digests(read_epoch);
        assert_eq!(node_digests.len(), node_ids.len());
        for (node_id, node_digest) in node_digests {
            assert_eq!(
                node_digest, digest,
                "the certified snapshot of epoch {read_epoch} read back must match the digest \
                reported by node {node_id}"
            );
        }

        consistency_check.check_storage_node_consistency();
    }

    /// Checks that, with certification enabled on a sub-quorum of the committee only, nothing is
    /// certified on chain, and that the attesting nodes store the snapshot blob during its epoch
    /// and clean it up at the next epoch boundary.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_blob_info_snapshot_cleanup_without_quorum() {
        let consistency_check = BlobInfoConsistencyCheck::new();
        // The two heaviest nodes hold 7 of the 13 shards, below the quorum of 9.
        let certify = [false, false, false, true, true];
        let (_sui_cluster, walrus_cluster, client, _, _) = certification_cluster_builder(&certify)
            .build_generic::<SimStorageNodeHandle>()
            .await
            .unwrap();
        let client = Arc::new(client);
        let workload_handle =
            simtest_utils::start_background_workload(client.clone(), false, None, None);
        let nodes = &walrus_cluster.nodes;
        let attesting_node_ids: HashSet<_> = nodes
            .iter()
            .zip(certify)
            .filter(|(_, certifies)| *certifies)
            .map(|(node, _)| node_capability_id(node))
            .collect();
        let silent_node_ids: Vec<_> = nodes
            .iter()
            .zip(certify)
            .filter(|(_, certifies)| !*certifies)
            .map(|(node, _)| node_capability_id(node))
            .collect();

        // The first boundary is processed while the cluster starts up; observe the next ones.
        for epoch in 2..=3 {
            simtest_utils::wait_for_nodes_to_reach_epoch(nodes, epoch, 2 * EPOCH_DURATION).await;
            let blob_ids = consistency_check
                .wait_for_blob_info_snapshot_blob_ids(epoch, nodes.len(), EPOCH_DURATION)
                .await;
            let blob_id = single_blob_id(&blob_ids, epoch);

            let stored = consistency_check
                .wait_for_blob_info_snapshot_stored(epoch, attesting_node_ids.len(), EPOCH_DURATION)
                .await;
            assert_eq!(
                stored.keys().copied().collect::<HashSet<_>>(),
                attesting_node_ids,
                "exactly the attesting nodes must store the snapshot of epoch {epoch}"
            );
            for (node_id, stored_blob_id) in &stored {
                assert_eq!(
                    *stored_blob_id, blob_id,
                    "node {node_id} must store the snapshot it produced for epoch {epoch}"
                );
            }

            // The uncertified attempts are reconciled at the next boundary, before the nodes
            // apply the new epoch.
            simtest_utils::wait_for_nodes_to_reach_epoch(nodes, epoch + 1, 2 * EPOCH_DURATION)
                .await;
            let reconciled = consistency_check.blob_info_snapshot_reconciled(epoch);
            for node_id in &attesting_node_ids {
                assert_eq!(
                    reconciled.get(node_id),
                    Some(&false),
                    "node {node_id} must clean up its uncertified snapshot of epoch {epoch}"
                );
            }
            for node_id in &silent_node_ids {
                assert_eq!(
                    reconciled.get(node_id),
                    None,
                    "node {node_id} does not certify and has nothing to reconcile for epoch \
                    {epoch}"
                );
            }
            assert!(
                client
                    .inner
                    .sui_client()
                    .read_client
                    .last_certified_snapshot_blob()
                    .await
                    .expect("reading the certified blob info snapshot should succeed")
                    .is_none(),
                "no snapshot may be certified without a quorum"
            );
        }
        workload_handle.abort();

        consistency_check.check_storage_node_consistency();
    }

    /// Checks that a node whose snapshot blob differs from the one a quorum certifies cleans up
    /// its uncertified attempt at the next epoch boundary, while the other nodes certify and keep
    /// theirs.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_blob_info_snapshot_divergent_node_cleans_up() {
        let consistency_check = BlobInfoConsistencyCheck::new();
        let (_sui_cluster, walrus_cluster, client, _, _) =
            certification_cluster_builder(&[true; 5])
                .build_generic::<SimStorageNodeHandle>()
                .await
                .unwrap();
        let client = Arc::new(client);
        let workload_handle =
            simtest_utils::start_background_workload(client.clone(), false, None, None);
        let nodes = &walrus_cluster.nodes;

        // The lightest node diverges; the other four hold 12 of the 13 shards and still certify.
        let divergent_node = &nodes[0];
        assert_eq!(NODE_WEIGHTS[0], 1);
        let divergent_node_id = node_capability_id(divergent_node);
        let honest_node_ids: Vec<_> = nodes.iter().skip(1).map(node_capability_id).collect();
        simtest_utils::diverge_blob_info_snapshot_of_node(divergent_node);

        // The first boundary may have been processed before the divergence was armed; observe
        // the next ones.
        for epoch in 2..=3 {
            simtest_utils::wait_for_nodes_to_reach_epoch(nodes, epoch, 2 * EPOCH_DURATION).await;
            let blob_ids = consistency_check
                .wait_for_blob_info_snapshot_blob_ids(epoch, nodes.len(), EPOCH_DURATION)
                .await;
            let honest_blob_ids: HashMap<_, _> = blob_ids
                .iter()
                .filter(|(node_id, _)| **node_id != divergent_node_id)
                .map(|(node_id, blob_id)| (*node_id, *blob_id))
                .collect();
            let honest_blob_id = single_blob_id(&honest_blob_ids, epoch);
            let divergent_blob_id = blob_ids[&divergent_node_id];
            assert_ne!(
                divergent_blob_id, honest_blob_id,
                "the divergent node must produce a different snapshot blob for epoch {epoch}"
            );

            // The honest nodes' quorum certifies their snapshot during the epoch.
            simtest_utils::wait_for_certified_snapshot_blob(&client, epoch, 2 * EPOCH_DURATION)
                .await;
            let certified = client
                .inner
                .sui_client()
                .read_client
                .certified_snapshot_blob_for_epoch(epoch)
                .await
                .expect("reading the certification history should succeed")
                .unwrap_or_else(|| panic!("the snapshot of epoch {epoch} must be certified"));
            assert_eq!(
                certified.blob_id, honest_blob_id,
                "the quorum must certify the honest nodes' snapshot of epoch {epoch}"
            );

            // At the next boundary, the honest nodes keep their certified snapshot, and the
            // divergent node cleans up its attempt.
            simtest_utils::wait_for_nodes_to_reach_epoch(nodes, epoch + 1, 2 * EPOCH_DURATION)
                .await;
            let reconciled = consistency_check.blob_info_snapshot_reconciled(epoch);
            for node_id in &honest_node_ids {
                assert_eq!(
                    reconciled.get(node_id),
                    Some(&true),
                    "node {node_id} must have found its snapshot of epoch {epoch} certified"
                );
            }
            assert_eq!(
                reconciled.get(&divergent_node_id),
                Some(&false),
                "the divergent node must clean up its uncertified snapshot of epoch {epoch}"
            );
        }
        workload_handle.abort();

        // The snapshot files stay identical across nodes: the divergence is injected between the
        // file and its encoding.
        consistency_check.check_storage_node_consistency();
    }

    /// Checks that a node stopping in the middle of reconciling its previous snapshot
    /// publication, as a storage error there makes it do, neither loses that publication nor
    /// publishes over it: after the restart it replays the boundary, reconciles the previous
    /// publication, and publishes the new epoch's snapshot.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_blob_info_snapshot_reconciliation_survives_a_crash() {
        let consistency_check = BlobInfoConsistencyCheck::new();
        // Sub-quorum, so that every publication must be cleaned up by the reconciliation.
        let certify = [false, false, false, true, true];
        let (_sui_cluster, walrus_cluster, client, _, _) = certification_cluster_builder(&certify)
            .build_generic::<SimStorageNodeHandle>()
            .await
            .unwrap();
        let client = Arc::new(client);
        let workload_handle =
            simtest_utils::start_background_workload(client.clone(), false, None, None);
        let nodes = &walrus_cluster.nodes;
        let (silent_nodes, attesting_nodes) = nodes.split_at(3);
        assert!(certify[..3].iter().all(|certifies| !certifies));
        assert!(certify[3..].iter().all(|certifies| *certifies));
        let attesting_node_ids: HashSet<_> =
            attesting_nodes.iter().map(node_capability_id).collect();

        // Epoch 2: the attesting nodes publish as usual.
        simtest_utils::wait_for_nodes_to_reach_epoch(nodes, 2, 2 * EPOCH_DURATION).await;
        let stored = consistency_check
            .wait_for_blob_info_snapshot_stored(2, attesting_node_ids.len(), EPOCH_DURATION)
            .await;
        assert_eq!(
            stored.keys().copied().collect::<HashSet<_>>(),
            attesting_node_ids
        );

        // At the boundary into epoch 3, the attesting nodes stop while reconciling the epoch-2
        // publication, before it is cleaned up, and restart shortly after.
        let crashes = simtest_utils::crash_nodes_once_at_blob_info_snapshot_reconciliation(
            attesting_nodes,
            Duration::from_secs(5),
        );
        simtest_utils::wait_for_nodes_to_reach_epoch(silent_nodes, 3, 2 * EPOCH_DURATION).await;
        // The restarted nodes replay the boundary: the reconciliation runs again and cleans up
        // the epoch-2 publication, and only then is the epoch-3 snapshot published. A restarted
        // node reports the chain's epoch before it has replayed the boundary, so wait for the
        // reconciliation itself rather than for the epoch.
        simtest_utils::wait_for_nodes_to_reach_epoch(nodes, 3, 2 * EPOCH_DURATION).await;
        assert_eq!(
            crashes.load(std::sync::atomic::Ordering::SeqCst),
            attesting_node_ids.len(),
            "every attesting node must have crashed once at the reconciliation"
        );
        let reconciled = consistency_check
            .wait_for_blob_info_snapshot_reconciled(2, attesting_node_ids.len(), EPOCH_DURATION)
            .await;
        assert_eq!(
            reconciled,
            attesting_node_ids
                .iter()
                .map(|node_id| (*node_id, false))
                .collect(),
            "exactly the attesting nodes must clean up their epoch-2 publication when replaying \
            the boundary"
        );
        let blob_ids = consistency_check
            .wait_for_blob_info_snapshot_blob_ids(3, nodes.len(), EPOCH_DURATION)
            .await;
        let blob_id = single_blob_id(&blob_ids, 3);
        let stored = consistency_check
            .wait_for_blob_info_snapshot_stored(3, attesting_node_ids.len(), EPOCH_DURATION)
            .await;
        assert_eq!(
            stored.keys().copied().collect::<HashSet<_>>(),
            attesting_node_ids,
            "the attesting nodes must publish the epoch-3 snapshot after replaying the boundary"
        );
        for (node_id, stored_blob_id) in &stored {
            assert_eq!(
                *stored_blob_id, blob_id,
                "node {node_id} must store the epoch-3 snapshot"
            );
        }

        // The epoch-3 publication is reconciled at the next boundary like any other.
        simtest_utils::wait_for_nodes_to_reach_epoch(nodes, 4, 2 * EPOCH_DURATION).await;
        let reconciled = consistency_check
            .wait_for_blob_info_snapshot_reconciled(3, attesting_node_ids.len(), EPOCH_DURATION)
            .await;
        assert_eq!(
            reconciled,
            attesting_node_ids
                .iter()
                .map(|node_id| (*node_id, false))
                .collect(),
            "exactly the attesting nodes must clean up their epoch-3 publication at the next \
            boundary"
        );
        assert!(
            client
                .inner
                .sui_client()
                .read_client
                .last_certified_snapshot_blob()
                .await
                .expect("reading the certified blob info snapshot should succeed")
                .is_none(),
            "no snapshot may be certified without a quorum"
        );
        workload_handle.abort();

        consistency_check.check_storage_node_consistency();
    }

    /// Checks that a node that disables snapshots after publishing one still reconciles that
    /// publication at the next boundary, so that its stored data is cleaned up.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_blob_info_snapshot_publication_is_reconciled_when_snapshots_are_disabled() {
        let consistency_check = BlobInfoConsistencyCheck::new();
        // Sub-quorum, so that the publication must be cleaned up by the reconciliation.
        let certify = [false, false, false, true, true];
        let (_sui_cluster, mut walrus_cluster, client, _, _) =
            certification_cluster_builder(&certify)
                .build_generic::<SimStorageNodeHandle>()
                .await
                .unwrap();
        let client = Arc::new(client);
        let workload_handle =
            simtest_utils::start_background_workload(client.clone(), false, None, None);
        let attesting_indices: Vec<usize> = (0..certify.len()).filter(|i| certify[*i]).collect();
        let attesting_node_ids: HashSet<_> = attesting_indices
            .iter()
            .map(|i| node_capability_id(&walrus_cluster.nodes[*i]))
            .collect();

        // Epoch 2: the attesting nodes publish as usual.
        simtest_utils::wait_for_nodes_to_reach_epoch(&walrus_cluster.nodes, 2, 2 * EPOCH_DURATION)
            .await;
        let stored = consistency_check
            .wait_for_blob_info_snapshot_stored(2, attesting_node_ids.len(), EPOCH_DURATION)
            .await;
        assert_eq!(
            stored.keys().copied().collect::<HashSet<_>>(),
            attesting_node_ids
        );

        // Restart the attesting nodes with snapshots disabled before the next boundary.
        for index in &attesting_indices {
            walrus_cluster.nodes[*index]
                .storage_node_config
                .blob_info_snapshot
                .enabled = false;
            simtest_utils::restart_node_with_checkpoints(&mut walrus_cluster, *index, |_| 20).await;
        }
        let nodes = &walrus_cluster.nodes;

        // At the boundary into epoch 3 they reconcile the epoch-2 publication (cleaned up, as it
        // never certified) although they no longer produce snapshots.
        simtest_utils::wait_for_nodes_to_reach_epoch(nodes, 3, 2 * EPOCH_DURATION).await;
        let reconciled = consistency_check
            .wait_for_blob_info_snapshot_reconciled(2, attesting_node_ids.len(), EPOCH_DURATION)
            .await;
        assert_eq!(
            reconciled,
            attesting_node_ids
                .iter()
                .map(|node_id| (*node_id, false))
                .collect(),
            "the restarted nodes must clean up their epoch-2 publication with snapshots disabled"
        );
        let blob_ids = consistency_check.blob_info_snapshot_blob_ids(3);
        assert!(
            attesting_node_ids
                .iter()
                .all(|node_id| !blob_ids.contains_key(node_id)),
            "nodes with snapshots disabled must not produce the epoch-3 snapshot: {blob_ids:?}"
        );
        assert!(
            client
                .inner
                .sui_client()
                .read_client
                .last_certified_snapshot_blob()
                .await
                .expect("reading the certified blob info snapshot should succeed")
                .is_none(),
            "no snapshot may be certified without a quorum"
        );
        workload_handle.abort();

        consistency_check.check_storage_node_consistency();
    }
}
