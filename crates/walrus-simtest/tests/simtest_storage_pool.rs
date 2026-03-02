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

    use sui_types::base_types::ObjectID;
    use walrus_core::{
        BlobId,
        EncodingType,
        encoding::{EncodingFactory as _, Primary, Secondary},
        messages::{BlobPersistenceType, ConfirmationCertificate},
    };
    use walrus_proc_macros::walrus_simtest;
    use walrus_sdk::{node_client::WalrusNodeClient, uploader::TailHandling};
    use walrus_service::{
        client::ClientCommunicationConfig,
        test_utils::{SimStorageNodeHandle, TestNodesConfig, test_cluster},
    };
    use walrus_simtest::test_utils::simtest_utils::BlobInfoConsistencyCheck;
    use walrus_sui::{
        client::{BlobObjectMetadata, BlobPersistence, SuiContractClient},
        types::move_structs::StoragePoolResource,
    };
    use walrus_test_utils::WithTempDir;

    /// Helper: store a blob in a storage pool bucket.
    ///
    /// Encodes the blob, registers it in the bucket, uploads slivers to storage nodes,
    /// certifies it on-chain, and returns the blob ID and PooledBlob object ID.
    async fn store_blob_in_bucket(
        client: &WithTempDir<WalrusNodeClient<SuiContractClient>>,
        bucket_id: ObjectID,
        data: Vec<u8>,
        deletable: bool,
    ) -> (BlobId, ObjectID) {
        let sui_client = client.inner.sui_client();

        // Encode the blob.
        let encoding_config = client.as_ref().encoding_config();
        let encoder = encoding_config.get_for_type(EncodingType::RS2);
        let (sliver_pairs, metadata) = encoder
            .encode_with_metadata(data)
            .expect("encoding should succeed");

        let blob_id: BlobId = *metadata.blob_id();
        let blob_metadata =
            BlobObjectMetadata::try_from(&metadata).expect("metadata conversion should succeed");

        let persistence = if deletable {
            BlobPersistence::Deletable
        } else {
            BlobPersistence::Permanent
        };

        // Register blob in the bucket on-chain.
        let blob_obj_id: ObjectID = sui_client
            .register_pooled_blob(bucket_id, blob_metadata, persistence)
            .await
            .expect("register blob in bucket should succeed");

        tracing::info!(%blob_id, %blob_obj_id, "registered blob in bucket");

        // Construct the persistence type for slivers upload.
        let blob_persistence_type = if deletable {
            BlobPersistenceType::Deletable {
                object_id: blob_obj_id.into(),
            }
        } else {
            BlobPersistenceType::Permanent
        };

        // Upload slivers to storage nodes and collect the certificate.
        let certificate: ConfirmationCertificate = client
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
            .await
            .expect("upload slivers and get certificate should succeed");

        tracing::info!(%blob_id, "got confirmation certificate");

        // Certify the blob on-chain.
        sui_client
            .certify_pooled_blob(bucket_id, blob_id, &certificate)
            .await
            .expect("certify blob in bucket should succeed");

        tracing::info!(%blob_id, %blob_obj_id, "certified blob in bucket");

        (blob_id, blob_obj_id)
    }

    /// Helper: fetch a bucket's end_epoch and log each blob's effective end epoch.
    ///
    /// Blobs in storage pool inherit their end_epoch from the parent bucket.
    async fn print_blob_end_epochs(
        sui_client: &SuiContractClient,
        label: &str,
        blobs: &[(&str, &BlobId, ObjectID)],
    ) {
        tracing::info!("--- blob end epochs ({label}) ---");
        // Collect unique bucket IDs from the blob entries.
        let mut bucket_ids: Vec<ObjectID> = blobs.iter().map(|(_, _, bucket)| *bucket).collect();
        bucket_ids.sort();
        bucket_ids.dedup();

        for bucket_id in &bucket_ids {
            let pool: StoragePoolResource = sui_client
                .retriable_sui_client()
                .get_sui_object(*bucket_id)
                .await
                .expect("should be able to fetch bucket");
            tracing::info!(
                %bucket_id,
                end_epoch = pool.end_epoch,
                "bucket end_epoch"
            );
        }

        for (name, blob_id, bucket_id) in blobs {
            let pool: StoragePoolResource = sui_client
                .retriable_sui_client()
                .get_sui_object(*bucket_id)
                .await
                .expect("should be able to fetch bucket");
            tracing::info!(
                blob = %name,
                %blob_id,
                end_epoch = pool.end_epoch,
                %bucket_id,
                "blob end_epoch (from parent bucket)"
            );
        }
        tracing::info!("--- end blob end epochs ({label}) ---");
    }

    /// Helper: read a blob and verify it matches the expected data.
    async fn read_and_verify_blob(
        client: &WithTempDir<WalrusNodeClient<SuiContractClient>>,
        blob_id: &BlobId,
        expected_data: &[u8],
    ) {
        // Read using primary slivers.
        let mut read_result = client.as_ref().read_blob::<Primary>(blob_id).await;
        let mut retries = 0;
        while read_result.is_err() && retries < 10 {
            retries += 1;
            tracing::info!(
                "read attempt {} failed, retrying: {:?}",
                retries,
                read_result.unwrap_err()
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
            read_result = client.as_ref().read_blob::<Primary>(blob_id).await;
        }
        let read_data = read_result.expect("should be able to read blob");
        assert_eq!(
            read_data, expected_data,
            "blob data mismatch on primary read"
        );

        // Read using secondary slivers.
        let read_data = client
            .as_ref()
            .read_blob::<Secondary>(blob_id)
            .await
            .expect("should be able to read blob via secondary");
        assert_eq!(
            read_data, expected_data,
            "blob data mismatch on secondary read"
        );
    }

    /// Parses a decimal u256 string into a BlobId.
    ///
    /// In Move, blob IDs are stored as `u256`. BCS encodes `u256` as 32 little-endian bytes,
    /// which is the same layout as `BlobId([u8; 32])`. The Sui JSON-RPC represents `u256`
    /// values as decimal strings.
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
        BlobId::try_from(le_bytes.as_slice()).expect("32 bytes should produce a valid BlobId")
    }

    /// Helper: list all blob entries stored in a bucket by paginating its ObjectTable
    /// via the Sui `get_dynamic_fields` RPC.
    ///
    /// Returns `(blob_id, object_id)` pairs, where `blob_id` is the table key (u256)
    /// and `object_id` is the PooledBlob's Sui object ID.
    async fn list_blob_ids_in_bucket(
        sui_client: &SuiContractClient,
        bucket_id: ObjectID,
    ) -> Vec<(BlobId, ObjectID)> {
        let pool: StoragePoolResource = sui_client
            .retriable_sui_client()
            .get_sui_object(bucket_id)
            .await
            .expect("should be able to fetch bucket");

        let table_id = pool.blobs;
        let mut all_entries = Vec::new();
        let mut cursor = None;

        loop {
            let page = sui_client
                .retriable_sui_client()
                .get_dynamic_fields(table_id, cursor, None)
                .await
                .expect("get_dynamic_fields should succeed");

            for entry in &page.data {
                // The key is a u256 (blob ID). DynamicFieldInfo.name.value is a JSON
                // representation; parse the decimal string back into a BlobId.
                let key_str = entry
                    .name
                    .value
                    .as_str()
                    .expect("ObjectTable<u256, _> key should be a JSON string");
                let blob_id = blob_id_from_u256_decimal(key_str);
                // For ObjectTable, entry.object_id is the PooledBlob's Sui object ID.
                all_entries.push((blob_id, entry.object_id));
            }

            if page.has_next_page {
                cursor = page.next_cursor;
            } else {
                break;
            }
        }

        all_entries
    }

    /// End-to-end test for storage pool (bucket) operations:
    /// - Create buckets
    /// - Register, upload, and certify blobs through the storage pool path
    /// - Read blobs back
    /// - Extend bucket lifetime
    /// - Delete blobs from a bucket
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_storage_pool_operations() {
        let blob_info_consistency_check = BlobInfoConsistencyCheck::new();

        let (_sui_cluster, _walrus_cluster, client, _, _) =
            test_cluster::E2eTestSetupBuilder::new()
                .with_epoch_duration(Duration::from_secs(30))
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
                .with_default_num_checkpoints_per_blob()
                .build_generic::<SimStorageNodeHandle>()
                .await
                .unwrap();

        let sui_client = client.inner.sui_client();

        // --- Step 1: Create bucket 1 ---
        tracing::info!("creating storage pool (bucket 1)");
        let storage_amount: u64 = 10 * 1024 * 1024; // 10 MiB
        let epochs_ahead: u32 = 5;
        let bucket1_id = sui_client
            .create_storage_pool(storage_amount, epochs_ahead)
            .await
            .expect("create storage pool should succeed");
        tracing::info!(%bucket1_id, "created bucket 1");

        // Print bucket 1 end epoch after creation.
        print_blob_end_epochs(sui_client, "after creating bucket 1", &[]).await;
        {
            let pool: StoragePoolResource = sui_client
                .retriable_sui_client()
                .get_sui_object(bucket1_id)
                .await
                .expect("fetch bucket 1");
            tracing::info!(
                %bucket1_id,
                end_epoch = pool.end_epoch,
                "bucket 1 created"
            );
        }

        // --- Step 2: Store blob 1 in bucket 1 (deletable) ---
        tracing::info!("storing blob 1 in bucket 1 (deletable)");
        let blob1_data = walrus_test_utils::random_data(1024);
        let (blob1_id, blob1_obj_id) =
            store_blob_in_bucket(&client, bucket1_id, blob1_data.clone(), true).await;
        tracing::info!(%blob1_id, %blob1_obj_id, "stored blob 1");

        // Print end epochs after blob 1.
        print_blob_end_epochs(
            sui_client,
            "after storing blob 1",
            &[("blob_1", &blob1_id, bucket1_id)],
        )
        .await;

        // Read blob 1 back.
        read_and_verify_blob(&client, &blob1_id, &blob1_data).await;
        tracing::info!("blob 1 read successfully");

        // --- Step 3: Store blob 2 in bucket 1 (deletable) ---
        tracing::info!("storing blob 2 in bucket 1 (deletable)");
        let blob2_data = walrus_test_utils::random_data(2048);
        let (blob2_id, _blob2_obj_id) =
            store_blob_in_bucket(&client, bucket1_id, blob2_data.clone(), true).await;
        tracing::info!(%blob2_id, "stored blob 2");

        // Print end epochs after blob 2.
        print_blob_end_epochs(
            sui_client,
            "after storing blob 2",
            &[
                ("blob_1", &blob1_id, bucket1_id),
                ("blob_2", &blob2_id, bucket1_id),
            ],
        )
        .await;

        // Read blob 2 back.
        read_and_verify_blob(&client, &blob2_id, &blob2_data).await;
        tracing::info!("blob 2 read successfully");

        // --- Step 4: Create bucket 2 ---
        tracing::info!("creating storage pool (bucket 2)");
        let bucket2_id = sui_client
            .create_storage_pool(storage_amount, epochs_ahead)
            .await
            .expect("create storage pool 2 should succeed");
        tracing::info!(%bucket2_id, "created bucket 2");

        {
            let pool: StoragePoolResource = sui_client
                .retriable_sui_client()
                .get_sui_object(bucket2_id)
                .await
                .expect("fetch bucket 2");
            tracing::info!(
                %bucket2_id,
                end_epoch = pool.end_epoch,
                "bucket 2 created"
            );
        }

        // --- Step 5: Store blob 3 in bucket 2 (deletable) ---
        tracing::info!("storing blob 3 in bucket 2 (deletable)");
        let blob3_data = walrus_test_utils::random_data(512);
        let (blob3_id, _blob3_obj_id) =
            store_blob_in_bucket(&client, bucket2_id, blob3_data.clone(), true).await;
        tracing::info!(%blob3_id, "stored blob 3");

        // Read blob 3 back.
        read_and_verify_blob(&client, &blob3_id, &blob3_data).await;
        tracing::info!("blob 3 read successfully");

        // --- Step 6: Store blob 4 in bucket 2 (permanent) ---
        tracing::info!("storing blob 4 in bucket 2 (permanent)");
        let blob4_data = walrus_test_utils::random_data(768);
        let (blob4_id, _blob4_obj_id) =
            store_blob_in_bucket(&client, bucket2_id, blob4_data.clone(), false).await;
        tracing::info!(%blob4_id, "stored permanent blob 4");

        // Print end epochs after all blobs stored.
        print_blob_end_epochs(
            sui_client,
            "after storing all blobs",
            &[
                ("blob_1", &blob1_id, bucket1_id),
                ("blob_2", &blob2_id, bucket1_id),
                ("blob_3", &blob3_id, bucket2_id),
                ("blob_4", &blob4_id, bucket2_id),
            ],
        )
        .await;

        // Read blob 4 back.
        read_and_verify_blob(&client, &blob4_id, &blob4_data).await;
        tracing::info!("permanent blob 4 read successfully");

        // --- Step 7: Extend bucket 1 by 2 epochs ---
        tracing::info!("extending bucket 1 by 2 epochs");
        sui_client
            .extend_storage_pool(bucket1_id, 2, storage_amount)
            .await
            .expect("extend storage pool should succeed");
        tracing::info!("bucket 1 extended successfully");

        // Print end epochs after extending bucket 1.
        print_blob_end_epochs(
            sui_client,
            "after extending bucket 1",
            &[
                ("blob_1", &blob1_id, bucket1_id),
                ("blob_2", &blob2_id, bucket1_id),
                ("blob_3", &blob3_id, bucket2_id),
                ("blob_4", &blob4_id, bucket2_id),
            ],
        )
        .await;

        // --- Step 8: Verify all 4 blobs still readable ---
        tracing::info!("re-reading all blobs after extend");
        read_and_verify_blob(&client, &blob1_id, &blob1_data).await;
        read_and_verify_blob(&client, &blob2_id, &blob2_data).await;
        read_and_verify_blob(&client, &blob3_id, &blob3_data).await;
        read_and_verify_blob(&client, &blob4_id, &blob4_data).await;
        tracing::info!("all blobs still readable after extend");

        // --- Step 9: Delete blob 1 from bucket 1 ---
        tracing::info!("deleting blob 1 from bucket 1");
        sui_client
            .delete_pooled_blob(bucket1_id, blob1_id)
            .await
            .expect("delete blob from bucket should succeed");
        tracing::info!("blob 1 deleted from bucket 1");

        // Print end epochs after deleting blob 1.
        print_blob_end_epochs(
            sui_client,
            "after deleting blob 1",
            &[
                ("blob_2", &blob2_id, bucket1_id),
                ("blob_3", &blob3_id, bucket2_id),
                ("blob_4", &blob4_id, bucket2_id),
            ],
        )
        .await;

        // --- Step 10: Verify blobs 2, 3, 4 still readable ---
        read_and_verify_blob(&client, &blob2_id, &blob2_data).await;
        read_and_verify_blob(&client, &blob3_id, &blob3_data).await;
        read_and_verify_blob(&client, &blob4_id, &blob4_data).await;
        tracing::info!("remaining blobs still readable after deletion");

        // Wait for event processing to settle.
        tokio::time::sleep(Duration::from_secs(5)).await;

        blob_info_consistency_check.check_storage_node_consistency();
    }

    /// Test listing blobs in a bucket via the Sui `get_dynamic_fields` RPC.
    ///
    /// Creates a bucket, stores several blobs, lists the ObjectTable keys, verifies the
    /// returned set matches expectations, then deletes a blob and verifies the listing
    /// updates accordingly.
    #[ignore = "ignore integration simtests by default"]
    #[walrus_simtest]
    async fn test_storage_pool_list_blobs() {
        let blob_info_consistency_check = BlobInfoConsistencyCheck::new();

        let (_sui_cluster, _walrus_cluster, client, _, _) =
            test_cluster::E2eTestSetupBuilder::new()
                .with_epoch_duration(Duration::from_secs(30))
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
                .with_default_num_checkpoints_per_blob()
                .build_generic::<SimStorageNodeHandle>()
                .await
                .unwrap();

        let sui_client = client.inner.sui_client();

        // Create a bucket.
        let storage_amount: u64 = 10 * 1024 * 1024;
        let epochs_ahead: u32 = 5;
        let bucket_id = sui_client
            .create_storage_pool(storage_amount, epochs_ahead)
            .await
            .expect("create bucket should succeed");
        tracing::info!(%bucket_id, "created bucket");

        // Empty bucket should have no blobs.
        let listed = list_blob_ids_in_bucket(sui_client, bucket_id).await;
        assert!(listed.is_empty(), "new bucket should have no blobs");
        tracing::info!("empty bucket listing verified");

        // Store 3 deletable blobs.
        let blob1_data = walrus_test_utils::random_data(512);
        let (blob1_id, blob1_obj_id) =
            store_blob_in_bucket(&client, bucket_id, blob1_data.clone(), true).await;
        tracing::info!(%blob1_id, %blob1_obj_id, "stored blob 1");

        let blob2_data = walrus_test_utils::random_data(1024);
        let (blob2_id, blob2_obj_id) =
            store_blob_in_bucket(&client, bucket_id, blob2_data.clone(), true).await;
        tracing::info!(%blob2_id, %blob2_obj_id, "stored blob 2");

        let blob3_data = walrus_test_utils::random_data(768);
        let (blob3_id, blob3_obj_id) =
            store_blob_in_bucket(&client, bucket_id, blob3_data.clone(), true).await;
        tracing::info!(%blob3_id, %blob3_obj_id, "stored blob 3");

        // List blobs — should contain exactly the 3 blob IDs with correct object IDs.
        let listed = list_blob_ids_in_bucket(sui_client, bucket_id).await;
        let listed_map: HashMap<BlobId, ObjectID> = listed.into_iter().collect();
        let listed_set: HashSet<BlobId> = listed_map.keys().copied().collect();
        let expected_set: HashSet<BlobId> = [blob1_id, blob2_id, blob3_id].into_iter().collect();
        tracing::info!(?listed_set, ?expected_set, "bucket listing after 3 stores");
        assert_eq!(
            listed_set, expected_set,
            "bucket should contain exactly the 3 stored blobs"
        );
        // Verify that the object IDs in the ObjectTable match those returned at registration.
        assert_eq!(
            listed_map[&blob1_id], blob1_obj_id,
            "blob 1 object ID mismatch"
        );
        assert_eq!(
            listed_map[&blob2_id], blob2_obj_id,
            "blob 2 object ID mismatch"
        );
        assert_eq!(
            listed_map[&blob3_id], blob3_obj_id,
            "blob 3 object ID mismatch"
        );

        // Verify blob_count on the pool object matches.
        let pool: StoragePoolResource = sui_client
            .retriable_sui_client()
            .get_sui_object(bucket_id)
            .await
            .expect("fetch bucket");
        assert_eq!(pool.blob_count, 3, "blob_count should be 3");

        // Delete blob 2.
        tracing::info!("deleting blob 2 from bucket");
        sui_client
            .delete_pooled_blob(bucket_id, blob2_id)
            .await
            .expect("delete blob 2 should succeed");

        // List blobs — should now contain only blob 1 and blob 3.
        let listed = list_blob_ids_in_bucket(sui_client, bucket_id).await;
        let listed_map: HashMap<BlobId, ObjectID> = listed.into_iter().collect();
        let listed_set: HashSet<BlobId> = listed_map.keys().copied().collect();
        let expected_after_delete: HashSet<BlobId> = [blob1_id, blob3_id].into_iter().collect();
        tracing::info!(
            ?listed_set,
            ?expected_after_delete,
            "bucket listing after deleting blob 2"
        );
        assert_eq!(
            listed_set, expected_after_delete,
            "bucket should contain only blobs 1 and 3 after deletion"
        );
        // Verify object IDs still match after deletion of blob 2.
        assert_eq!(
            listed_map[&blob1_id], blob1_obj_id,
            "blob 1 object ID mismatch after delete"
        );
        assert_eq!(
            listed_map[&blob3_id], blob3_obj_id,
            "blob 3 object ID mismatch after delete"
        );

        // Verify blob_count updated.
        let pool: StoragePoolResource = sui_client
            .retriable_sui_client()
            .get_sui_object(bucket_id)
            .await
            .expect("fetch bucket");
        assert_eq!(pool.blob_count, 2, "blob_count should be 2 after deletion");

        // Verify remaining blobs are still readable.
        read_and_verify_blob(&client, &blob1_id, &blob1_data).await;
        read_and_verify_blob(&client, &blob3_id, &blob3_data).await;
        tracing::info!("remaining blobs still readable");

        // Wait for event processing to settle.
        tokio::time::sleep(Duration::from_secs(5)).await;

        blob_info_consistency_check.check_storage_node_consistency();
    }
}
