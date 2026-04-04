// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Contains simtests for storage pool (bucket) operations.

#![recursion_limit = "256"]

#[cfg(msim)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        sync::Arc,
        time::Duration,
    };

    use rand::Rng;
    use sui_types::base_types::ObjectID;
    use tokio::task::JoinHandle;
    use walrus_core::{
        BlobId,
        EncodingType,
        Epoch,
        EpochCount,
        encoding::{EncodingFactory as _, Primary, Secondary},
        messages::{BlobPersistenceType, ConfirmationCertificate},
    };
    use walrus_proc_macros::walrus_simtest;
    use walrus_sdk::{node_client::WalrusNodeClient, uploader::TailHandling};
    use walrus_service::{
        client::ClientCommunicationConfig,
        test_utils::{SimStorageNodeHandle, TestCluster, TestNodesConfig, test_cluster},
    };
    use walrus_simtest::test_utils::simtest_utils::{self, BlobInfoConsistencyCheck};
    use walrus_sui::client::{BlobObjectMetadata, BlobPersistence, SuiContractClient};
    use walrus_test_utils::WithTempDir;

    // ── Helpers ──────────────────────────────────────────────────────────

    /// Shared test context returned by [`setup_cluster`].
    ///
    /// `_keep_alive` holds the Sui cluster handle; dropping it shuts down the Sui network.
    struct TestCtx {
        _keep_alive: Box<dyn std::any::Any + Send + Sync>,
        client: Arc<WithTempDir<WalrusNodeClient<SuiContractClient>>>,
        walrus_cluster: TestCluster<SimStorageNodeHandle>,
        workload_handle: JoinHandle<()>,
        consistency_check: BlobInfoConsistencyCheck,
    }

    impl TestCtx {
        fn sui(&self) -> &SuiContractClient {
            self.client.inner.sui_client()
        }

        /// Abort the background workload, wait for events to settle, and run
        /// the consistency check.
        async fn finish(self) {
            self.workload_handle.abort();
            tokio::time::sleep(Duration::from_secs(5)).await;
            self.consistency_check.check_storage_node_consistency();
        }
    }

    /// Build a default test cluster with a mixed background workload already running.
    async fn setup_cluster(
        epoch_duration: Duration,
        max_epochs_ahead: Option<EpochCount>,
    ) -> TestCtx {
        let consistency_check = BlobInfoConsistencyCheck::new();

        let mut builder = test_cluster::E2eTestSetupBuilder::new()
            .with_epoch_duration(epoch_duration)
            .with_test_nodes_config(
                TestNodesConfig::builder()
                    .with_node_weights(&[2, 2, 3, 3, 3])
                    .build(),
            )
            .with_communication_config(
                ClientCommunicationConfig::default_for_test_with_reqwest_timeout(
                    Duration::from_secs(2),
                ),
            )
            .with_default_num_checkpoints_per_blob();

        if let Some(max) = max_epochs_ahead {
            builder = builder.with_max_epochs_ahead(max);
        }

        let (sui_cluster, walrus_cluster, client, _, _) = builder
            .build_generic::<SimStorageNodeHandle>()
            .await
            .unwrap();

        let client = Arc::new(client);

        let workload_epochs = max_epochs_ahead.unwrap_or(5);
        let workload_bucket = client
            .inner
            .sui_client()
            .create_storage_pool(10 * 1024 * 1024, workload_epochs)
            .await
            .expect("create workload bucket");
        let workload_handle =
            start_mixed_background_workload(client.clone(), workload_bucket, Some(workload_epochs));

        TestCtx {
            _keep_alive: Box::new(sui_cluster),
            client,
            walrus_cluster,
            workload_handle,
            consistency_check,
        }
    }

    /// Store a blob through the storage-pool path (register → upload → certify).
    async fn store_blob(
        client: &WithTempDir<WalrusNodeClient<SuiContractClient>>,
        bucket_id: ObjectID,
        data: Vec<u8>,
        deletable: bool,
    ) -> anyhow::Result<(BlobId, ObjectID)> {
        let sui_client = client.inner.sui_client();
        let encoder = client
            .as_ref()
            .encoding_config()
            .get_for_type(EncodingType::RS2);
        let (sliver_pairs, metadata) = encoder.encode_with_metadata(data)?;

        let blob_id = *metadata.blob_id();
        let blob_metadata = BlobObjectMetadata::try_from(&metadata)?;
        let persistence = if deletable {
            BlobPersistence::Deletable
        } else {
            BlobPersistence::Permanent
        };

        let obj_id = sui_client
            .register_pooled_blob(bucket_id, blob_metadata, persistence)
            .await?;

        let blob_persistence_type = if deletable {
            BlobPersistenceType::Deletable {
                object_id: obj_id.into(),
            }
        } else {
            BlobPersistenceType::Permanent
        };

        let cert: ConfirmationCertificate = client
            .as_ref()
            .send_blob_data_and_get_certificate(
                &metadata,
                Arc::new(sliver_pairs),
                &blob_persistence_type,
                None,
                TailHandling::Blocking,
                None,
                None,
                None,
                None,
            )
            .await?;

        sui_client
            .certify_pooled_blob(bucket_id, blob_id, &cert)
            .await?;

        tracing::info!(%blob_id, %obj_id, "stored pooled blob");
        Ok((blob_id, obj_id))
    }

    /// Panicking wrapper around [`store_blob`].
    async fn store_blob_ok(
        client: &WithTempDir<WalrusNodeClient<SuiContractClient>>,
        bucket_id: ObjectID,
        data: Vec<u8>,
        deletable: bool,
    ) -> (BlobId, ObjectID) {
        store_blob(client, bucket_id, data, deletable)
            .await
            .expect("store_blob should succeed")
    }

    /// Read a blob via primary *and* secondary slivers and assert it matches.
    async fn read_and_verify(
        client: &WithTempDir<WalrusNodeClient<SuiContractClient>>,
        blob_id: &BlobId,
        expected: &[u8],
    ) {
        let mut result = client.as_ref().read_blob::<Primary>(blob_id).await;
        for attempt in 1..=10 {
            if result.is_ok() {
                break;
            }
            tracing::info!(attempt, "primary read retrying");
            tokio::time::sleep(Duration::from_secs(1)).await;
            result = client.as_ref().read_blob::<Primary>(blob_id).await;
        }
        assert_eq!(result.expect("primary read"), expected);

        let secondary = client
            .as_ref()
            .read_blob::<Secondary>(blob_id)
            .await
            .expect("secondary read");
        assert_eq!(secondary, expected);
    }

    /// Store a blob and immediately read it back.
    async fn store_and_verify(
        client: &WithTempDir<WalrusNodeClient<SuiContractClient>>,
        bucket_id: ObjectID,
        data: Vec<u8>,
        deletable: bool,
    ) -> (BlobId, ObjectID) {
        let (id, obj) = store_blob_ok(client, bucket_id, data.clone(), deletable).await;
        read_and_verify(client, &id, &data).await;
        (id, obj)
    }

    fn blob_id_from_u256_decimal(s: &str) -> BlobId {
        let mut le_bytes = [0u8; 32];
        for ch in s.bytes() {
            assert!(ch >= b'0' && ch <= b'9', "expected decimal digit");
            let digit = ch - b'0';
            let mut carry = u16::from(digit);
            for byte in le_bytes.iter_mut() {
                let v = u16::from(*byte) * 10 + carry;
                *byte = v as u8;
                carry = v >> 8;
            }
            assert_eq!(carry, 0, "u256 overflow");
        }
        BlobId::try_from(le_bytes.as_slice()).expect("valid BlobId")
    }

    async fn list_blob_ids_in_bucket(
        sui_client: &SuiContractClient,
        bucket_id: ObjectID,
    ) -> Vec<(BlobId, ObjectID)> {
        let pool = sui_client
            .get_storage_pool(bucket_id)
            .await
            .expect("fetch bucket");
        let mut entries = Vec::new();
        let mut cursor = None;
        loop {
            let page = sui_client
                .retriable_sui_client()
                .get_dynamic_fields(pool.blobs(), cursor, None)
                .await
                .expect("get_dynamic_fields");
            for e in &page.data {
                let key = e.name.value.as_str().expect("u256 key");
                entries.push((blob_id_from_u256_decimal(key), e.object_id));
            }
            if page.has_next_page {
                cursor = page.next_cursor;
            } else {
                break;
            }
        }
        entries
    }

    fn encoded_size_for(
        client: &WithTempDir<WalrusNodeClient<SuiContractClient>>,
        data: &[u8],
    ) -> u64 {
        let encoder = client
            .as_ref()
            .encoding_config()
            .get_for_type(EncodingType::RS2);
        let (_, metadata) = encoder
            .encode_with_metadata(data.to_vec())
            .expect("encode_with_metadata");
        BlobObjectMetadata::try_from(&metadata)
            .expect("BlobObjectMetadata")
            .encoded_size
    }

    async fn wait_until_blob_unreadable(
        client: &WithTempDir<WalrusNodeClient<SuiContractClient>>,
        blob_id: &BlobId,
    ) {
        for attempt in 1..=10 {
            if client.as_ref().read_blob::<Primary>(blob_id).await.is_err() {
                return;
            }
            tracing::info!(attempt, "blob still readable, retrying unreadable check");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        panic!("blob should be unreadable");
    }

    async fn wait_for_pool_expiry(ctx: &TestCtx, end_epoch: Epoch, epoch_dur: Duration) -> Epoch {
        let target = end_epoch + 2;
        tokio::time::sleep(epoch_dur * target).await;
        simtest_utils::wait_for_nodes_to_reach_epoch(
            &ctx.walrus_cluster.nodes,
            target,
            epoch_dur * 4,
        )
        .await;
        target
    }

    /// Background workload alternating regular and pooled blob store+read.
    fn start_mixed_background_workload(
        client: Arc<WithTempDir<WalrusNodeClient<SuiContractClient>>>,
        bucket_id: ObjectID,
        epochs_max: Option<EpochCount>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut len = 64usize;
            let mut regular_written = HashSet::new();
            let mut i = 0u64;
            loop {
                i += 1;
                let deletable = rand::thread_rng().gen_bool(0.5);
                if i % 2 == 0 {
                    let r = simtest_utils::write_read_and_check_random_blob(
                        client.as_ref(),
                        len,
                        false,
                        deletable,
                        &mut regular_written,
                        Some(3),
                        epochs_max,
                    )
                    .await;
                    if let Err(e) = r {
                        tracing::warn!(i, "workload: regular blob failed: {e:#}");
                        continue;
                    }
                } else {
                    let data = walrus_test_utils::random_data(len);
                    match store_blob(&client, bucket_id, data.clone(), deletable).await {
                        Ok((id, _)) => {
                            read_and_verify(&client, &id, &data).await;
                        }
                        Err(e) => {
                            tracing::warn!(i, "workload: pooled blob failed: {e:#}");
                            continue;
                        }
                    }
                }
                len += 1;
            }
        })
    }

    // ── Tests ────────────────────────────────────────────────────────────

    /// Core storage-pool operations: create pools, store/read blobs across two pools,
    /// extend a pool, delete a blob, verify remaining blobs readable.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_storage_pool_operations() {
        let ctx = setup_cluster(Duration::from_secs(30), None).await;
        let sui = ctx.sui();
        let cap: u64 = 10 * 1024 * 1024;

        // Two pools.
        let p1 = sui.create_storage_pool(cap, 5).await.unwrap();
        let p2 = sui.create_storage_pool(cap, 5).await.unwrap();

        // Store blobs across both pools.
        let d1 = walrus_test_utils::random_data(1024);
        let d2 = walrus_test_utils::random_data(2048);
        let d3 = walrus_test_utils::random_data(512);
        let d4 = walrus_test_utils::random_data(768);
        let (id1, _) = store_and_verify(&ctx.client, p1, d1.clone(), true).await;
        let (id2, _) = store_and_verify(&ctx.client, p1, d2.clone(), true).await;
        let (id3, _) = store_and_verify(&ctx.client, p2, d3.clone(), true).await;
        let (id4, _) = store_and_verify(&ctx.client, p2, d4.clone(), false).await;

        // Extend pool 1.
        sui.extend_storage_pool(p1, 2).await.unwrap();
        for (id, d) in [(&id1, &d1), (&id2, &d2), (&id3, &d3), (&id4, &d4)] {
            read_and_verify(&ctx.client, id, d).await;
        }

        // Delete blob 1 from pool 1.
        sui.delete_pooled_blob(p1, id1).await.unwrap();
        read_and_verify(&ctx.client, &id2, &d2).await;
        read_and_verify(&ctx.client, &id3, &d3).await;
        read_and_verify(&ctx.client, &id4, &d4).await;

        ctx.finish().await;
    }

    /// Listing blobs via `get_dynamic_fields`, verifying blob_count and object IDs.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_storage_pool_list_blobs() {
        let ctx = setup_cluster(Duration::from_secs(30), None).await;
        let sui = ctx.sui();

        let bucket = sui.create_storage_pool(10 * 1024 * 1024, 5).await.unwrap();
        assert!(list_blob_ids_in_bucket(sui, bucket).await.is_empty());

        // Store 3 blobs.
        let d1 = walrus_test_utils::random_data(512);
        let d3 = walrus_test_utils::random_data(768);
        let (id1, obj1) = store_blob_ok(&ctx.client, bucket, d1.clone(), true).await;
        let (id2, obj2) = store_blob_ok(
            &ctx.client,
            bucket,
            walrus_test_utils::random_data(1024),
            true,
        )
        .await;
        let (id3, obj3) = store_blob_ok(&ctx.client, bucket, d3.clone(), true).await;

        // Verify listing.
        let map: HashMap<BlobId, ObjectID> = list_blob_ids_in_bucket(sui, bucket)
            .await
            .into_iter()
            .collect();
        assert_eq!(map.len(), 3);
        assert_eq!(map[&id1], obj1);
        assert_eq!(map[&id2], obj2);
        assert_eq!(map[&id3], obj3);
        assert_eq!(sui.get_storage_pool(bucket).await.unwrap().blob_count(), 3);

        // Delete blob 2 and re-check.
        sui.delete_pooled_blob(bucket, id2).await.unwrap();
        let map: HashMap<BlobId, ObjectID> = list_blob_ids_in_bucket(sui, bucket)
            .await
            .into_iter()
            .collect();
        assert_eq!(map.len(), 2);
        assert_eq!(map[&id1], obj1);
        assert_eq!(map[&id3], obj3);
        assert_eq!(sui.get_storage_pool(bucket).await.unwrap().blob_count(), 2);

        // Remaining blobs still readable.
        read_and_verify(&ctx.client, &id1, &d1).await;
        read_and_verify(&ctx.client, &id3, &d3).await;

        ctx.finish().await;
    }

    /// Capacity enforcement: exceed → fail, increase capacity → succeed.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_storage_pool_increase_capacity() {
        let ctx = setup_cluster(Duration::from_secs(30), None).await;
        let sui = ctx.sui();

        let bucket = sui.create_storage_pool(512 * 1024, 5).await.unwrap();

        // Store one small blob.
        let d1 = walrus_test_utils::random_data(512);
        let (id1, _) = store_and_verify(&ctx.client, bucket, d1.clone(), true).await;
        let pool = sui.get_storage_pool(bucket).await.unwrap();
        assert_eq!(pool.blob_count(), 1);

        // A large blob should be rejected due to insufficient capacity.
        let d2 = walrus_test_utils::random_data(512 * 1024);
        let enc = ctx
            .client
            .inner
            .encoding_config()
            .get_for_type(EncodingType::RS2);
        let (_, meta) = enc.encode_with_metadata(d2.clone()).unwrap();
        let bm = BlobObjectMetadata::try_from(&meta).unwrap();
        assert!(
            sui.register_pooled_blob(bucket, bm, BlobPersistence::Deletable)
                .await
                .is_err()
        );

        // Increase capacity, then store the *same* large blob that was just rejected.
        sui.increase_storage_pool_capacity(bucket, 10 * 1024 * 1024)
            .await
            .unwrap();
        let pool2 = sui.get_storage_pool(bucket).await.unwrap();
        assert!(pool2.reserved_encoded_capacity_bytes() > pool.reserved_encoded_capacity_bytes());

        store_and_verify(&ctx.client, bucket, d2, true).await;
        assert_eq!(sui.get_storage_pool(bucket).await.unwrap().blob_count(), 2);

        read_and_verify(&ctx.client, &id1, &d1).await;
        ctx.finish().await;
    }

    /// Pool expiration causes blobs to become unreadable.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_storage_pool_expiration() {
        let epoch_dur = Duration::from_secs(20);
        let ctx = setup_cluster(epoch_dur, Some(3)).await;
        let sui = ctx.sui();

        let bucket = sui.create_storage_pool(10 * 1024 * 1024, 1).await.unwrap();
        let end_epoch = sui.get_storage_pool(bucket).await.unwrap().end_epoch();

        let d = walrus_test_utils::random_data(1024);
        let (id, _) = store_and_verify(&ctx.client, bucket, d, true).await;

        // Advance past expiration.
        let target = end_epoch + 2;
        tokio::time::sleep(epoch_dur * target).await;
        simtest_utils::wait_for_nodes_to_reach_epoch(
            &ctx.walrus_cluster.nodes,
            target,
            epoch_dur * 4,
        )
        .await;

        assert!(
            ctx.client.inner.read_blob::<Primary>(&id).await.is_err(),
            "blob should be unreadable after pool expiration"
        );

        ctx.finish().await;
    }

    /// Same blob in two pools — survives one pool expiring.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_storage_pool_same_blob_two_pools() {
        let epoch_dur = Duration::from_secs(20);
        let ctx = setup_cluster(epoch_dur, Some(5)).await;
        let sui = ctx.sui();
        let cap: u64 = 10 * 1024 * 1024;

        let short = sui.create_storage_pool(cap, 1).await.unwrap();
        let long = sui.create_storage_pool(cap, 5).await.unwrap();
        let short_end = sui.get_storage_pool(short).await.unwrap().end_epoch();

        let data = walrus_test_utils::random_data(1024);
        let (id_s, _) = store_blob_ok(&ctx.client, short, data.clone(), true).await;
        let (id_l, _) = store_blob_ok(&ctx.client, long, data.clone(), true).await;
        assert_eq!(id_s, id_l, "same data → same blob ID");

        read_and_verify(&ctx.client, &id_s, &data).await;

        // Expire the short pool.
        let target = short_end + 2;
        tokio::time::sleep(epoch_dur * target).await;
        simtest_utils::wait_for_nodes_to_reach_epoch(
            &ctx.walrus_cluster.nodes,
            target,
            epoch_dur * 4,
        )
        .await;

        // Blob still readable thanks to the long pool.
        read_and_verify(&ctx.client, &id_s, &data).await;
        ctx.finish().await;
    }

    /// Same blob in two pools remains readable after deleting one reference, and
    /// becomes unreadable once the last pooled reference is removed.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_storage_pool_same_blob_two_pools_delete_lifecycle() {
        let ctx = setup_cluster(Duration::from_secs(30), None).await;
        let sui = ctx.sui();
        let cap: u64 = 10 * 1024 * 1024;

        let p1 = sui.create_storage_pool(cap, 5).await.unwrap();
        let p2 = sui.create_storage_pool(cap, 5).await.unwrap();

        let data = walrus_test_utils::random_data(2048);
        let encoded_size = encoded_size_for(&ctx.client, &data);

        let (id1, obj1) = store_blob_ok(&ctx.client, p1, data.clone(), true).await;
        let (id2, obj2) = store_blob_ok(&ctx.client, p2, data.clone(), true).await;
        assert_eq!(id1, id2, "same data should map to the same blob id");
        assert_ne!(
            obj1, obj2,
            "separate pools should create separate pooled blob objects"
        );

        let pool1 = sui.get_storage_pool(p1).await.unwrap();
        let pool2 = sui.get_storage_pool(p2).await.unwrap();
        assert_eq!(pool1.blob_count(), 1);
        assert_eq!(pool2.blob_count(), 1);
        assert_eq!(pool1.used_encoded_bytes(), encoded_size);
        assert_eq!(pool2.used_encoded_bytes(), encoded_size);

        sui.delete_pooled_blob(p1, id1).await.unwrap();
        read_and_verify(&ctx.client, &id1, &data).await;

        let pool1_after_delete = sui.get_storage_pool(p1).await.unwrap();
        let pool2_after_delete = sui.get_storage_pool(p2).await.unwrap();
        assert_eq!(pool1_after_delete.blob_count(), 0);
        assert_eq!(pool1_after_delete.used_encoded_bytes(), 0);
        assert_eq!(pool2_after_delete.blob_count(), 1);
        assert_eq!(pool2_after_delete.used_encoded_bytes(), encoded_size);

        sui.delete_pooled_blob(p2, id2).await.unwrap();
        wait_until_blob_unreadable(&ctx.client, &id2).await;
        assert_eq!(sui.get_storage_pool(p2).await.unwrap().blob_count(), 0);

        ctx.finish().await;
    }

    /// Permanent blobs cannot be deleted; deletable ones can.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_storage_pool_permanent_blob() {
        let ctx = setup_cluster(Duration::from_secs(30), None).await;
        let sui = ctx.sui();
        let cap: u64 = 10 * 1024 * 1024;

        let bucket = sui.create_storage_pool(cap, 5).await.unwrap();

        let perm_data = walrus_test_utils::random_data(1024);
        let del_data = walrus_test_utils::random_data(512);
        let (perm_id, _) = store_and_verify(&ctx.client, bucket, perm_data.clone(), false).await;
        let (del_id, _) = store_and_verify(&ctx.client, bucket, del_data, true).await;

        // Permanent blob cannot be deleted.
        assert!(sui.delete_pooled_blob(bucket, perm_id).await.is_err());
        // Deletable blob can.
        sui.delete_pooled_blob(bucket, del_id).await.unwrap();

        // Extend pool; permanent blob still readable.
        sui.extend_storage_pool(bucket, 2).await.unwrap();
        read_and_verify(&ctx.client, &perm_id, &perm_data).await;
        assert_eq!(sui.get_storage_pool(bucket).await.unwrap().blob_count(), 1);

        ctx.finish().await;
    }

    /// Verify exact pool-state accounting across store, delete, capacity increase,
    /// and extension.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_storage_pool_state_accounting() {
        let ctx = setup_cluster(Duration::from_secs(30), None).await;
        let sui = ctx.sui();

        let data = walrus_test_utils::random_data(64 * 1024);
        let encoded_size = encoded_size_for(&ctx.client, &data);
        let extra_capacity = encoded_size + 1234;

        let bucket = sui.create_storage_pool(encoded_size, 3).await.unwrap();
        let pool_before = sui.get_storage_pool(bucket).await.unwrap();
        assert_eq!(pool_before.reserved_encoded_capacity_bytes(), encoded_size);
        assert_eq!(pool_before.used_encoded_bytes(), 0);
        assert_eq!(pool_before.blob_count(), 0);
        let original_end_epoch = pool_before.end_epoch();

        let (blob_id, blob_obj_id) = store_blob_ok(&ctx.client, bucket, data.clone(), true).await;
        read_and_verify(&ctx.client, &blob_id, &data).await;

        let pool_after_store = sui.get_storage_pool(bucket).await.unwrap();
        assert_eq!(
            pool_after_store.reserved_encoded_capacity_bytes(),
            encoded_size
        );
        assert_eq!(pool_after_store.used_encoded_bytes(), encoded_size);
        assert_eq!(pool_after_store.blob_count(), 1);
        let entries = list_blob_ids_in_bucket(sui, bucket).await;
        assert_eq!(entries, vec![(blob_id, blob_obj_id)]);

        sui.delete_pooled_blob(bucket, blob_id).await.unwrap();
        wait_until_blob_unreadable(&ctx.client, &blob_id).await;

        let pool_after_delete = sui.get_storage_pool(bucket).await.unwrap();
        assert_eq!(
            pool_after_delete.reserved_encoded_capacity_bytes(),
            encoded_size
        );
        assert_eq!(pool_after_delete.used_encoded_bytes(), 0);
        assert_eq!(pool_after_delete.blob_count(), 0);
        assert!(list_blob_ids_in_bucket(sui, bucket).await.is_empty());

        sui.increase_storage_pool_capacity(bucket, extra_capacity)
            .await
            .unwrap();
        let pool_after_capacity = sui.get_storage_pool(bucket).await.unwrap();
        assert_eq!(
            pool_after_capacity.reserved_encoded_capacity_bytes(),
            encoded_size + extra_capacity
        );
        assert_eq!(pool_after_capacity.used_encoded_bytes(), 0);
        assert_eq!(pool_after_capacity.blob_count(), 0);

        sui.extend_storage_pool(bucket, 2).await.unwrap();
        let pool_after_extend = sui.get_storage_pool(bucket).await.unwrap();
        assert_eq!(
            pool_after_extend.reserved_encoded_capacity_bytes(),
            encoded_size + extra_capacity
        );
        assert_eq!(pool_after_extend.used_encoded_bytes(), 0);
        assert_eq!(pool_after_extend.blob_count(), 0);
        assert_eq!(pool_after_extend.end_epoch(), original_end_epoch + 2);

        ctx.finish().await;
    }

    /// Delete a blob to free capacity, then reuse it for a new blob.
    ///
    /// Uses a tight capacity so the replacement blob cannot fit without deletion.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_storage_pool_delete_and_reuse_capacity() {
        let ctx = setup_cluster(Duration::from_secs(30), None).await;
        let sui = ctx.sui();

        // Use 512 KiB — tight enough that one large blob fills it.
        let bucket = sui.create_storage_pool(512 * 1024, 5).await.unwrap();

        // Store one blob that consumes most of the capacity.
        let d1 = walrus_test_utils::random_data(100 * 1024);
        let (id1, _) = store_and_verify(&ctx.client, bucket, d1, true).await;
        let pool = sui.get_storage_pool(bucket).await.unwrap();
        assert_eq!(pool.blob_count(), 1);

        // A second blob of similar size should be rejected (pool is full).
        let d2 = walrus_test_utils::random_data(100 * 1024 + 1);
        assert!(
            store_blob(&ctx.client, bucket, d2.clone(), true)
                .await
                .is_err(),
            "second blob should fail: pool is full"
        );

        // Delete blob 1 to free capacity.
        sui.delete_pooled_blob(bucket, id1).await.unwrap();
        assert_eq!(sui.get_storage_pool(bucket).await.unwrap().blob_count(), 0);

        // Now the same blob that was rejected can be stored.
        let (id2, _) = store_and_verify(&ctx.client, bucket, d2.clone(), true).await;
        assert_eq!(sui.get_storage_pool(bucket).await.unwrap().blob_count(), 1);

        read_and_verify(&ctx.client, &id2, &d2).await;
        ctx.finish().await;
    }

    /// Expired pools should reject further mutations through the Rust client.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_storage_pool_expired_pool_rejects_mutations() {
        let epoch_dur = Duration::from_secs(20);
        let ctx = setup_cluster(epoch_dur, Some(3)).await;
        let sui = ctx.sui();

        let bucket = sui.create_storage_pool(10 * 1024 * 1024, 1).await.unwrap();
        let end_epoch = sui.get_storage_pool(bucket).await.unwrap().end_epoch();

        let original = walrus_test_utils::random_data(1024);
        let (blob_id, _) = store_and_verify(&ctx.client, bucket, original, true).await;

        wait_for_pool_expiry(&ctx, end_epoch, epoch_dur).await;

        assert!(sui.delete_pooled_blob(bucket, blob_id).await.is_err());
        assert!(sui.extend_storage_pool(bucket, 1).await.is_err());
        assert!(
            sui.increase_storage_pool_capacity(bucket, 1024)
                .await
                .is_err()
        );

        let new_blob = walrus_test_utils::random_data(2048);
        let encoder = ctx
            .client
            .inner
            .encoding_config()
            .get_for_type(EncodingType::RS2);
        let (_, metadata) = encoder.encode_with_metadata(new_blob).unwrap();
        let blob_metadata = BlobObjectMetadata::try_from(&metadata).unwrap();
        assert!(
            sui.register_pooled_blob(bucket, blob_metadata, BlobPersistence::Deletable)
                .await
                .is_err()
        );

        ctx.finish().await;
    }
}
