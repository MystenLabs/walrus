// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

#[cfg(msim)]
/// Utilities for simulation testing, including blob operations, node health checks,
/// and consistency verification.
pub mod simtest_utils {
    use std::{
        collections::{HashMap, HashSet},
        sync::{atomic::AtomicBool, Arc, Mutex},
        time::Duration,
    };

    use anyhow::Context;
    use rand::{seq::IteratorRandom, Rng};
    use sui_types::base_types::ObjectID;
    use tokio::task::JoinHandle;
    use walrus_core::{
        encoding::{Primary, Secondary},
        Epoch,
        DEFAULT_ENCODING,
    };
    use walrus_rest_client::api::ServiceHealthInfo;
    use walrus_sdk::{
        client::{responses::BlobStoreResult, Client},
        store_when::StoreWhen,
    };
    use walrus_service::test_utils::SimStorageNodeHandle;
    use walrus_sui::client::{BlobPersistence, PostStoreAction, SuiContractClient};
    use walrus_test_utils::WithTempDir;

    /// The fail points related to DB access that can be used to trigger failures in the storage
    /// node.
    pub const DB_FAIL_POINTS: &[&str] = &[
        "batch-write-before",
        "batch-write-after",
        "put-cf-before",
        "put-cf-after",
        "delete-cf-before",
        "delete-cf-after",
        "create-cf-before",
    ];

    /// Helper function to write a random blob, read it back and check that it is the same.
    /// If `write_only` is true, only write the blob and do not read it back.
    ///
    // TODO(zhewu): restructure this to a test client.
    pub async fn write_read_and_check_random_blob(
        client: &WithTempDir<Client<SuiContractClient>>,
        data_length: usize,
        write_only: bool,
        blobs_written: &mut HashSet<ObjectID>,
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
                None,
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

        blobs_written.insert(blob_confirmation.id);

        if write_only {
            tracing::info!("write-only mode, skipping read verification");
            return Ok(());
        }

        // Probabilstically extend one of the blobs from blobs_written.
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

    /// Starts a background workload that writes and reads random blobs.
    pub fn start_background_workload(
        client_clone: Arc<WithTempDir<Client<SuiContractClient>>>,
        write_only: bool,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut data_length = 64;
            let mut blobs_written = HashSet::new();
            loop {
                tracing::info!("writing data with size {data_length}");

                // TODO(#995): use stress client for better coverage of the workload.
                write_read_and_check_random_blob(
                    client_clone.as_ref(),
                    data_length,
                    write_only,
                    &mut blobs_written,
                )
                .await
                .expect("workload should not fail");

                tracing::info!("finished writing data with size {data_length}");

                data_length += 1;
            }
        })
    }

    /// BlobInfoConsistencyCheck is a helper struct to check the consistency of the blob info.
    #[derive(Debug)]
    pub struct BlobInfoConsistencyCheck {
        certified_blob_digest_map: Arc<Mutex<HashMap<Epoch, HashMap<ObjectID, u64>>>>,
        per_object_blob_digest_map: Arc<Mutex<HashMap<Epoch, HashMap<ObjectID, u64>>>>,
        checked: Arc<AtomicBool>,
    }

    impl BlobInfoConsistencyCheck {
        /// Creates a new BlobInfoConsistencyCheck.
        pub fn new() -> Self {
            let certified_blob_digest_map = Arc::new(Mutex::new(HashMap::new()));
            let certified_blob_digest_map_clone = certified_blob_digest_map.clone();
            let per_object_blob_digest_map = Arc::new(Mutex::new(HashMap::new()));
            let per_object_blob_digest_map_clone = per_object_blob_digest_map.clone();

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
            Self {
                certified_blob_digest_map,
                per_object_blob_digest_map,
                checked: Arc::new(AtomicBool::new(false)),
            }
        }

        /// Checks the consistency of the storage node.
        pub fn check_storage_node_consistency(&self) {
            self.checked
                .store(true, std::sync::atomic::Ordering::SeqCst);

            // Ensure that for all epochs, all nodes have the same certified blob digest.
            let digest_map = self.certified_blob_digest_map.lock().unwrap();
            for (epoch, node_digest_map) in digest_map.iter() {
                // Ensure that for the same epoch, all nodes have the same certified blob digest.
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

            // Ensure that for all epochs, all nodes have the same per object blob digest.
            let digest_map = self.per_object_blob_digest_map.lock().unwrap();
            for (epoch, node_digest_map) in digest_map.iter() {
                // Ensure that for the same epoch, all nodes have the same per object blob digest.
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
    }

    impl Drop for BlobInfoConsistencyCheck {
        fn drop(&mut self) {
            assert!(self.checked.load(std::sync::atomic::Ordering::SeqCst));
            sui_macros::clear_fail_point("storage_node_certified_blob_digest");
            sui_macros::clear_fail_point("storage_node_certified_blob_object_digest");
        }
    }

    /// Helper function to get health info for a single node.
    pub async fn wait_until_node_is_active(
        node: &SimStorageNodeHandle,
        timeout: Duration,
    ) -> anyhow::Result<ServiceHealthInfo> {
        let client = walrus_rest_client::client::Client::builder()
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
    pub async fn get_nodes_health_info(nodes: &[&SimStorageNodeHandle]) -> Vec<ServiceHealthInfo> {
        futures::future::join_all(
            nodes
                .iter()
                .map(|node_handle| async {
                    let client = walrus_rest_client::client::Client::builder()
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
}
