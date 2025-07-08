// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

#[cfg(msim)]
/// Utilities for simulation testing, including blob operations, node health checks,
/// and consistency verification.
pub mod simtest_utils {
    use std::{
        collections::{HashMap, HashSet},
        sync::{Arc, Mutex, atomic::AtomicBool},
        time::{Duration, Instant},
    };

    use anyhow::Context;
    use itertools::Itertools;
    use rand::{Rng, seq::IteratorRandom};
    use sui_types::base_types::ObjectID;
    use tokio::{sync::RwLock, task::JoinHandle};
    use walrus_core::{
        DEFAULT_ENCODING,
        Epoch,
        EpochCount,
        encoding::{Primary, Secondary},
    };
    use walrus_sdk::{
        client::{Client, responses::BlobStoreResult},
        store_optimizations::StoreOptimizations,
    };
    use walrus_service::test_utils::{SimStorageNodeHandle, TestCluster};
    use walrus_storage_node_client::api::ServiceHealthInfo;
    use walrus_sui::client::{BlobPersistence, PostStoreAction, SuiContractClient};
    use walrus_test_utils::WithTempDir;

    /// The fail points related to node crash that can be used to trigger failures in the storage
    /// node.
    pub const CRASH_NODE_FAIL_POINTS: &[&str] = &[
        "batch-write-before",
        "batch-write-after",
        "put-cf-before",
        "put-cf-after",
        "delete-cf-before",
        "delete-cf-after",
        "create-cf-before",
        "process-event-before",
        "process-event-after",
        "write-event-before",
        "write-event-after",
    ];

    /// Helper function to write a random blob, read it back and check that it is the same.
    /// If `write_only` is true, only write the blob and do not read it back.
    ///
    // TODO(zhewu): restructure this to a test client.
    pub async fn write_read_and_check_random_blob(
        client: &WithTempDir<Client<SuiContractClient>>,
        data_length: usize,
        write_only: bool,
        deletable: bool,
        blobs_written: &mut HashSet<ObjectID>,
        max_retry_count: usize,
        epochs_max: Option<EpochCount>,
    ) -> anyhow::Result<()> {
        // Get a random epoch length for the blob to be stored.
        let epoch_ahead = rand::thread_rng().gen_range(1..=epochs_max.unwrap_or(5));

        tracing::info!(
            "generating random blobs of length {data_length} and store them for {epoch_ahead} \
            epochs"
        );
        let blob = walrus_test_utils::random_data(data_length);

        let mut retry_count = 0;
        let store_results = loop {
            let result = client
                .as_ref()
                .reserve_and_store_blobs_retry_committees(
                    &[blob.as_slice()],
                    DEFAULT_ENCODING,
                    epoch_ahead,
                    StoreOptimizations::none(),
                    BlobPersistence::from_deletable_and_permanent(deletable, !deletable)?,
                    PostStoreAction::Keep,
                    None,
                )
                .await;
            if let Ok(result) = result {
                break result;
            }
            if retry_count >= max_retry_count {
                tracing::error!(
                    "store blob failed: {:?}, max retry count reached {}",
                    result,
                    max_retry_count
                );
                return Err(result.unwrap_err().into());
            }
            tracing::error!(
                "store blob failed: {:?}, retry count {}",
                result.unwrap_err(),
                retry_count
            );
            retry_count += 1;
        };

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

        blobs_written.insert(blob_confirmation.id);

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

    /// Probabilistically extend one of the blobs from blobs_written.
    async fn maybe_extend_blob(
        client: &WithTempDir<Client<SuiContractClient>>,
        blobs_written: &HashSet<ObjectID>,
    ) {
        // Probabilistically extend one of the blobs from blobs_written.
        if rand::thread_rng().gen_bool(0.1) {
            let blob_obj_id = blobs_written
                .iter()
                .choose(&mut rand::thread_rng())
                .unwrap();
            let result = client
                .as_ref()
                .sui_client()
                .extend_blob(*blob_obj_id, 5)
                .await;
            // TODO(zhewu): account for already expired blobs.
            tracing::info!("extend blob {:?} result: {:?}", blob_obj_id, result);
        }
    }

    /// Probabilistically delete one of the blobs from blobs_written.
    async fn maybe_delete_blob(
        client: &WithTempDir<Client<SuiContractClient>>,
        blobs_written: &HashSet<ObjectID>,
    ) {
        if rand::thread_rng().gen_bool(0.1) {
            let blob_obj_id = blobs_written
                .iter()
                .choose(&mut rand::thread_rng())
                .unwrap();
            let result = client.as_ref().sui_client().delete_blob(*blob_obj_id).await;
            tracing::info!("delete blob {:?} result: {:?}", blob_obj_id, result);
        }
    }

    /// Starts a background workload that writes and reads random blobs.
    pub fn start_background_workload(
        client_clone: Arc<WithTempDir<Client<SuiContractClient>>>,
        write_only: bool,
        max_retry_count: usize,
        epochs_max: Option<EpochCount>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut data_length = 64;
            let mut blobs_written = HashSet::new();
            loop {
                tracing::info!("writing data with size {data_length}");

                let deletable = rand::thread_rng().gen_bool(0.5);

                // TODO(#995): use stress client for better coverage of the workload.
                write_read_and_check_random_blob(
                    client_clone.as_ref(),
                    data_length,
                    write_only,
                    deletable,
                    &mut blobs_written,
                    max_retry_count,
                    epochs_max,
                )
                .await
                .expect("workload should not fail");

                maybe_extend_blob(client_clone.as_ref(), &blobs_written).await;
                if deletable {
                    maybe_delete_blob(client_clone.as_ref(), &blobs_written).await;
                }

                tracing::info!("finished writing data with size {data_length}");

                data_length += 1;
            }
        })
    }

    /// BlobInfoConsistencyCheck is a helper struct to check the consistency of the blob info.
    #[derive(Debug)]
    pub struct BlobInfoConsistencyCheck {
        // Per event index, the event source of the event in all nodes.
        event_source_map: Arc<Mutex<HashMap<u64, HashMap<ObjectID, u64>>>>,
        // Per epoch, the certified blob digest of all nodes.
        certified_blob_digest_map: Arc<Mutex<HashMap<Epoch, HashMap<ObjectID, u64>>>>,
        // Per epoch, the per object blob digest of all nodes.
        per_object_blob_digest_map: Arc<Mutex<HashMap<Epoch, HashMap<ObjectID, u64>>>>,
        // Per epoch, the existence check of all nodes.
        blob_existence_check_map: Arc<Mutex<HashMap<Epoch, HashMap<ObjectID, f64>>>>,
        checked: Arc<AtomicBool>,
    }

    impl BlobInfoConsistencyCheck {
        /// Creates a new BlobInfoConsistencyCheck.
        pub fn new() -> Self {
            let event_source_map = Arc::new(Mutex::new(HashMap::new()));
            let event_source_map_clone = event_source_map.clone();
            let certified_blob_digest_map = Arc::new(Mutex::new(HashMap::new()));
            let certified_blob_digest_map_clone = certified_blob_digest_map.clone();
            let per_object_blob_digest_map = Arc::new(Mutex::new(HashMap::new()));
            let per_object_blob_digest_map_clone = per_object_blob_digest_map.clone();
            let blob_existence_check_map = Arc::new(Mutex::new(HashMap::new()));
            let blob_existence_check_map_clone = blob_existence_check_map.clone();

            sui_macros::register_fail_point_arg(
                "storage_node_event_index_source",
                move || -> Option<Arc<Mutex<HashMap<u64, HashMap<ObjectID, u64>>>>> {
                    Some(event_source_map_clone.clone())
                },
            );

            sui_macros::register_fail_point_arg(
                "storage_node_certified_blob_digest",
                move || -> Option<Arc<Mutex<HashMap<Epoch, HashMap<ObjectID, u64>>>>> {
                    Some(certified_blob_digest_map_clone.clone())
                },
            );

            sui_macros::register_fail_point_arg(
                "storage_node_certified_blob_object_digest",
                move || -> Option<Arc<Mutex<HashMap<Epoch, HashMap<ObjectID, u64>>>>> {
                    Some(per_object_blob_digest_map_clone.clone())
                },
            );

            sui_macros::register_fail_point_arg(
                "storage_node_certified_blob_existence_check",
                move || -> Option<Arc<Mutex<HashMap<Epoch, HashMap<ObjectID, f64>>>>> {
                    Some(blob_existence_check_map_clone.clone())
                },
            );

            Self {
                event_source_map,
                certified_blob_digest_map,
                per_object_blob_digest_map,
                blob_existence_check_map,
                checked: Arc::new(AtomicBool::new(false)),
            }
        }

        /// Checks the consistency of the storage node.
        pub fn check_storage_node_consistency(&self) {
            self.check_storage_node_consistency_from_epoch(0);
        }

        /// Checks the consistency of the storage node for all epochs starting with `min_epoch`.
        pub fn check_storage_node_consistency_from_epoch(&self, min_epoch: Epoch) {
            tracing::info!("checking storage node consistency starting with epoch {min_epoch}");
            self.checked
                .store(true, std::sync::atomic::Ordering::SeqCst);

            let event_source_map = self.event_source_map.lock().unwrap();
            tracing::info!("event source map: {:?}", event_source_map.len());
            for (event_index, node_event_source_map) in event_source_map.iter() {
                let mut event_source = None;
                for (node_id, source) in node_event_source_map.iter() {
                    tracing::debug!(
                        "event source check: node {node_id} has event source {source} for \
                        event index {event_index}",
                    );

                    if event_source.is_none() {
                        event_source = Some(source);
                    } else {
                        assert_eq!(event_source, Some(source));
                    }
                }
            }

            self.check_certified_blob_digest(min_epoch);
            self.check_per_object_blob_digest(min_epoch);
            self.check_blob_existence(min_epoch);
        }

        /// Ensures that for all epochs, all nodes have the same certified blob digest.
        #[tracing::instrument(skip(self))]
        fn check_certified_blob_digest(&self, min_epoch: Epoch) {
            tracing::info!("checking blob digest consistency starting with epoch {min_epoch}");
            let digest_map = self.certified_blob_digest_map.lock().unwrap();
            for (epoch, node_digest_map) in digest_map.iter().sorted_by_key(|(epoch, _)| *epoch) {
                if *epoch < min_epoch {
                    tracing::info!(
                        "skipping epoch {epoch} because it is before the minimum epoch {min_epoch}"
                    );
                    continue;
                }
                Self::check_digests_are_equal(*epoch, node_digest_map);
            }
        }

        /// Ensures that for all epochs, all nodes have the same per object blob digest.
        #[tracing::instrument(skip(self))]
        fn check_per_object_blob_digest(&self, min_epoch: Epoch) {
            tracing::info!(
                "checking per object blob digest consistency starting with epoch {min_epoch}"
            );
            let digest_map = self.per_object_blob_digest_map.lock().unwrap();
            for (epoch, node_digest_map) in digest_map.iter().sorted_by_key(|(epoch, _)| *epoch) {
                if *epoch < min_epoch {
                    tracing::info!(
                        "skipping epoch {epoch} because it is before the minimum epoch {min_epoch}"
                    );
                    continue;
                }
                Self::check_digests_are_equal(*epoch, node_digest_map);
            }
        }

        fn check_digests_are_equal(epoch: Epoch, node_digest_map: &HashMap<ObjectID, u64>) {
            let mut epoch_digest = None;
            for (node_id, digest) in node_digest_map.iter() {
                tracing::info!("node {node_id} has digest {digest} in epoch {epoch}",);
                match epoch_digest {
                    None => {
                        epoch_digest = Some(digest);
                    }
                    Some(expected_digest) => {
                        assert_eq!(
                            expected_digest, digest,
                            "consistency check failed for epoch {epoch}; node {node_id} has \
                            digest {digest} but expected {expected_digest}"
                        );
                    }
                }
            }
        }

        #[tracing::instrument(skip(self))]
        fn check_blob_existence(&self, min_epoch: Epoch) {
            tracing::info!("checking blob existence starting with epoch {min_epoch}");
            let existence_check_map = self.blob_existence_check_map.lock().unwrap();
            for (epoch, node_existence_check_map) in existence_check_map
                .iter()
                .sorted_by_key(|(epoch, _)| *epoch)
            {
                if *epoch < min_epoch {
                    tracing::info!(
                        "skipping epoch {epoch} because it is before the minimum epoch {min_epoch}"
                    );
                    continue;
                }
                // 100% of blobs are fully stored all the time.
                for (node_id, existence_check) in node_existence_check_map.iter() {
                    tracing::info!(
                        "blob sliver data existence check: node {node_id} has existence check \
                    {existence_check} in epoch {epoch}",
                    );

                    assert_eq!(*existence_check, 1.0);
                }
            }
        }
    }

    impl Drop for BlobInfoConsistencyCheck {
        fn drop(&mut self) {
            assert!(self.checked.load(std::sync::atomic::Ordering::SeqCst));
            sui_macros::clear_fail_point("storage_node_event_index_source");
            sui_macros::clear_fail_point("storage_node_certified_blob_digest");
            sui_macros::clear_fail_point("storage_node_certified_blob_object_digest");
            sui_macros::clear_fail_point("storage_node_certified_blob_existence_check");
        }
    }

    /// Helper function to get health info for a single node.
    pub async fn wait_until_node_is_active(
        node: &SimStorageNodeHandle,
        timeout: Duration,
    ) -> anyhow::Result<ServiceHealthInfo> {
        let client = walrus_storage_node_client::StorageNodeClient::builder()
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
    pub async fn get_nodes_health_info(
        nodes: impl IntoIterator<Item = &SimStorageNodeHandle>,
    ) -> Vec<ServiceHealthInfo> {
        try_get_nodes_health_info(nodes)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("get health info should succeed")
    }

    /// Helper function to get health info for a list of nodes.
    pub async fn try_get_nodes_health_info(
        nodes: impl IntoIterator<Item = &SimStorageNodeHandle>,
    ) -> Vec<anyhow::Result<ServiceHealthInfo>> {
        futures::future::join_all(
            nodes
                .into_iter()
                .map(|node_handle| async {
                    let client = walrus_storage_node_client::StorageNodeClient::builder()
                        .authenticate_with_public_key(node_handle.network_public_key.clone())
                        // Disable proxy and root certs from the OS for tests.
                        .no_proxy()
                        .tls_built_in_root_certs(false)
                        .build_for_remote_ip(node_handle.rest_api_address)
                        .context("create node client failed")?;
                    client
                        .get_server_health_info(true)
                        .await
                        .context("get health info failed")
                })
                .collect::<Vec<_>>(),
        )
        .await
    }

    /// Gets the minimum epoch from a list of nodes by looking at the health info.
    pub async fn get_min_epoch_from_nodes(
        nodes: impl IntoIterator<Item = &SimStorageNodeHandle>,
    ) -> Epoch {
        try_get_nodes_health_info(nodes)
            .await
            .iter()
            .map(|result| result.as_ref().map(|info| info.epoch).unwrap_or_default())
            .min()
            .expect("at least one node should be running")
    }

    /// Returns the current epoch of a node based on the health info.
    pub async fn get_current_epoch_from_node(node: &SimStorageNodeHandle) -> Epoch {
        get_min_epoch_from_nodes([node]).await
    }

    /// Waits until all nodes reach the given epoch based on their health info.
    pub async fn wait_for_nodes_to_reach_epoch(
        nodes: &[SimStorageNodeHandle],
        target_epoch: Epoch,
        timeout: Duration,
    ) {
        tokio::time::timeout(timeout, async {
            loop {
                let min_epoch = get_min_epoch_from_nodes(nodes).await;
                if min_epoch >= target_epoch {
                    break;
                }
                tracing::info!(
                    "waiting for {} nodes to reach epoch {}, current min epoch: {}",
                    nodes.len(),
                    target_epoch,
                    min_epoch
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        })
        .await
        .expect(
            format!(
                "timed out waiting for all nodes to reach epoch {}",
                target_epoch
            )
            .as_str(),
        );
    }

    /// Kills all the storage nodes in the cluster.
    async fn kill_all_storage_nodes(node_ids: &[sui_simulator::task::NodeId]) {
        let handle = sui_simulator::runtime::Handle::current();
        for &node_id in node_ids {
            handle.delete_node(node_id);
        }
    }

    /// Restarts all nodes in the cluster with number of checkpoints per event blob configuration.
    pub async fn restart_nodes_with_checkpoints(
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

    /// Simulates repeated node crash and restart with sim node id.
    pub fn repeatedly_crash_target_node(
        target_node_id: sui_simulator::task::NodeId,
        next_fail_triggered_clone: Arc<Mutex<Instant>>,
        crash_end_time: Instant,
        min_crash_duration_secs: u64,
        max_crash_duration_secs: u64,
        min_live_duration_secs: u64,
        max_live_duration_secs: u64,
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

        let node_down_duration =
            Duration::from_secs(rng.gen_range(min_crash_duration_secs..=max_crash_duration_secs));
        let next_crash_time = Instant::now()
            + node_down_duration
            + Duration::from_secs(rng.gen_range(min_live_duration_secs..=max_live_duration_secs));

        tracing::warn!(
            "crashing node {current_node} for {} seconds; next crash is set to {:?}",
            node_down_duration.as_secs(),
            next_crash_time
        );
        sui_simulator::task::kill_current_node(Some(node_down_duration));
        *next_fail_triggered_clone.lock().unwrap() = next_crash_time;
    }
}
