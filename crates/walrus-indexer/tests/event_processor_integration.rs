// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Integration test for indexer with event processor.

// Allowing `unwrap`s in tests.
#![allow(clippy::unwrap_used)]

use std::{str::FromStr, sync::Arc, time::Duration};

use anyhow::Result;
use sui_types::base_types::ObjectID;
use tempfile::TempDir;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use walrus_core::encoding::quilt_encoding::{QuiltStoreBlob, QuiltVersionV1};
use walrus_indexer::{IndexerConfig, WalrusIndexer, storage::IndexTarget};
use walrus_proc_macros::walrus_simtest;
use walrus_sdk::client::{StoreArgs, WalrusNodeClient, responses};
use walrus_service::{
    common::config::SuiConfig,
    event::event_processor::config::EventProcessorConfig,
    test_utils::test_cluster,
};
use walrus_sui::{
    client::{BlobBucketIdentifier, SuiContractClient},
    config::WalletConfig,
};
use walrus_test_utils::WithTempDir;

/// Start the Walrus indexer with the given configuration.
async fn start_walrus_indexer(
    sui_cluster_handle: &Arc<Mutex<walrus_sui::test_utils::TestClusterHandle>>,
    client: &WithTempDir<WalrusNodeClient<SuiContractClient>>,
    temp_dir: &TempDir,
) -> Result<(
    Arc<WalrusIndexer>,
    CancellationToken,
    tokio::task::JoinHandle<Result<()>>,
)> {
    // Configure indexer.
    let rpc_url = sui_cluster_handle
        .lock()
        .await
        .cluster()
        .rpc_url()
        .to_string();
    let cluster_wallet_path = sui_cluster_handle.lock().await.wallet_path().await;
    let wallet_config = WalletConfig::from_path(cluster_wallet_path);
    let contract_config = client
        .as_ref()
        .sui_client()
        .read_client()
        .contract_config()
        .clone();

    let sui_config = SuiConfig {
        rpc: rpc_url,
        contract_config,
        event_polling_interval: Duration::from_millis(500),
        wallet_config,
        backoff_config: Default::default(),
        gas_budget: None,
        rpc_fallback_config: None,
        additional_rpc_endpoints: vec![],
        request_timeout: None,
    };

    let indexer_config = IndexerConfig {
        db_path: temp_dir.path().to_path_buf(),
        event_processor_config: Some(EventProcessorConfig::default()),
        sui_config: Some(sui_config),
        ..Default::default()
    };

    // Start indexer.
    let indexer = WalrusIndexer::new(indexer_config).await?;
    let cancel_token = CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();

    // Create a metrics registry.
    let prometheus_registry = prometheus::Registry::new();
    let metrics_registry = walrus_utils::metrics::Registry::new(prometheus_registry);

    // Start the indexer in a background task.
    let indexer_for_run = indexer.clone();
    let indexer_handle = tokio::spawn(async move {
        indexer_for_run
            .run(&metrics_registry, cancel_token_clone)
            .await
    });

    Ok((indexer, cancel_token, indexer_handle))
}

/// Store multiple blobs to the cluster and return a mapping of primary_index to IndexTarget.
/// Returns: HashMap<String, IndexTarget> where key is "{bucket_id}/{identifier}".
async fn store_blobs(
    client: &WithTempDir<WalrusNodeClient<SuiContractClient>>,
    bucket_id: ObjectID,
    identifier_prefix: &str,
    count: usize,
    min_size: usize,
    max_size: usize,
) -> Result<std::collections::HashMap<String, walrus_indexer::storage::IndexTarget>> {
    use std::collections::HashMap;

    // Generate random data for blobs.
    let data_list = walrus_test_utils::generate_random_data(count, min_size, max_size);

    // Create unencoded blobs with bucket identifiers.
    let unencoded_blobs: Vec<_> = data_list
        .iter()
        .enumerate()
        .map(|(i, data)| {
            walrus_sdk::client::UnencodedBlob::new(data, 0).with_bucket_identifier(
                BlobBucketIdentifier {
                    bucket_id,
                    identifier: format!("{}-{}", identifier_prefix, i),
                },
            )
        })
        .collect();

    // Store the blobs.
    let store_args = StoreArgs::default_with_epochs(1).no_store_optimizations();
    let store_results = client
        .as_ref()
        .reserve_and_store_blobs_retry_committees(&unencoded_blobs, &store_args)
        .await?;

    // Build the mapping of primary_index to IndexTarget.
    let mut mapping = HashMap::new();
    for (i, blob) in unencoded_blobs.iter().enumerate() {
        let identifier = &blob.bucket_identifier.as_ref().unwrap().identifier;
        let primary_index = format!("{}/{}", bucket_id, identifier);
        let blob_id = store_results[i].blob_id().expect("blob should have ID");

        // Extract the object_id from the BlobStoreResult
        let object_id = match &store_results[i] {
            responses::BlobStoreResult::NewlyCreated { blob_object, .. } => blob_object.id,
            responses::BlobStoreResult::AlreadyCertified {
                event_or_object, ..
            } => match event_or_object {
                responses::EventOrObjectId::Object(id) => *id,
                responses::EventOrObjectId::Event(_) => {
                    panic!("Expected object ID for blob, got event ID")
                }
            },
            _ => panic!("Unexpected BlobStoreResult variant for blob"),
        };

        mapping.insert(
            primary_index,
            IndexTarget::Blob(walrus_indexer::storage::BlobIdentity {
                blob_id,
                object_id,
                quilt_status: walrus_indexer::storage::QuiltTaskStatus::NotQuilt,
            }),
        );
    }

    Ok(mapping)
}

/// Store a quilt to the cluster and return mappings.
/// Returns:
/// - primary_index_mapping: HashMap<String, IndexTarget> where key is "{bucket_id}/{identifier}"
/// - patch_blob_mapping: HashMap<BlobId, QuiltPatchId> where key is patch_blob_id
async fn store_quilt(
    client: &WithTempDir<WalrusNodeClient<SuiContractClient>>,
    bucket_id: ObjectID,
    quilt_identifier: &str,
    patch_prefix: &str,
    patch_count: usize,
    min_size: usize,
    max_size: usize,
) -> Result<(
    std::collections::HashMap<String, walrus_indexer::storage::IndexTarget>,
    std::collections::HashMap<walrus_core::BlobId, walrus_core::QuiltPatchId>,
)> {
    use std::collections::HashMap;

    use walrus_core::QuiltPatchId;
    use walrus_indexer::storage::IndexTarget;

    // Generate random data for quilt patches.
    let data_list = walrus_test_utils::generate_random_data(patch_count, min_size, max_size);

    // Get encoder config.
    let encoder_config = client
        .as_ref()
        .encoding_config()
        .get_for_type(walrus_core::EncodingType::RS2);

    // Create quilt store blobs.
    let quilt_store_blobs: Vec<_> = data_list
        .iter()
        .enumerate()
        .map(|(i, data)| {
            QuiltStoreBlob::new(data, format!("{}-{}", patch_prefix, i))
                .expect("Should create blob")
                .with_blob_id(&encoder_config)
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Construct and store the quilt.
    let store_args = StoreArgs::default_with_epochs(1).no_store_optimizations();
    let quilt = client
        .as_ref()
        .quilt_client()
        .construct_quilt::<QuiltVersionV1>(&quilt_store_blobs, store_args.encoding_type)
        .await?;

    let quilt_bucket_identifier = BlobBucketIdentifier {
        bucket_id,
        identifier: quilt_identifier.to_string(),
    };

    let quilt_store_result = client
        .as_ref()
        .quilt_client()
        .reserve_and_store_quilt::<QuiltVersionV1>(
            &quilt,
            Some(quilt_bucket_identifier),
            &store_args,
        )
        .await?;

    // Build the primary index mapping.
    let mut primary_index_mapping = HashMap::new();

    // Add the main quilt entry.
    let quilt_primary_index = format!("{}/{}", bucket_id, quilt_identifier);
    let quilt_blob_id = quilt_store_result
        .blob_store_result
        .blob_id()
        .expect("quilt should have blob ID");

    // Extract the object_id from the BlobStoreResult.
    let quilt_object_id = match &quilt_store_result.blob_store_result {
        walrus_sdk::client::responses::BlobStoreResult::NewlyCreated { blob_object, .. } => {
            blob_object.id
        }
        walrus_sdk::client::responses::BlobStoreResult::AlreadyCertified {
            event_or_object,
            ..
        } => match event_or_object {
            responses::EventOrObjectId::Object(id) => *id,
            responses::EventOrObjectId::Event(_) => {
                panic!("Expected object ID for quilt, got event ID")
            }
        },
        _ => panic!("Unexpected BlobStoreResult variant for quilt"),
    };

    primary_index_mapping.insert(
        quilt_primary_index,
        IndexTarget::Blob(walrus_indexer::storage::BlobIdentity {
            blob_id: quilt_blob_id,
            object_id: quilt_object_id,
            quilt_status: walrus_indexer::storage::QuiltTaskStatus::Completed,
        }),
    );

    // Build the patch mappings.
    let mut patch_blob_mapping = HashMap::new();

    for stored_patch in &quilt_store_result.stored_quilt_blobs {
        // Add to primary index mapping.
        let patch_primary_index = format!("{}/{}", bucket_id, stored_patch.identifier);
        let patch_id =
            QuiltPatchId::from_str(&stored_patch.quilt_patch_id).expect("Valid patch ID");
        tracing::info!(
            ?patch_primary_index,
            ?patch_id,
            "Adding patch to primary index mapping"
        );
        primary_index_mapping.insert(
            patch_primary_index,
            IndexTarget::QuiltPatchId(patch_id.clone()),
        );

        // Add to patch blob mapping if the patch has a blob_id.
        if let Some(patch_blob_id) = stored_patch.patch_blob_id {
            tracing::info!(
                ?patch_blob_id,
                ?patch_id,
                "Adding patch to patch blob mapping"
            );
            patch_blob_mapping.insert(patch_blob_id, patch_id);
        }
    }

    Ok((primary_index_mapping, patch_blob_mapping))
}

/// Verify that all primary index mappings are correctly indexed.
/// This includes regular blobs, quilt blobs, and quilt patch identifiers.
async fn verify_primary_index_mappings(
    indexer: &Arc<WalrusIndexer>,
    bucket_id: ObjectID,
    primary_mapping: &std::collections::HashMap<String, walrus_indexer::storage::IndexTarget>,
) -> Result<()> {
    for (primary_index, expected_target) in primary_mapping {
        let parts: Vec<&str> = primary_index.split('/').collect();
        let identifier = parts[1];

        let index_target = indexer
            .get_index_target_from_bucket(&bucket_id, identifier)
            .await?;

        match (index_target.as_ref(), expected_target) {
            (Some(IndexTarget::Blob(found)), IndexTarget::Blob(expected)) => {
                assert_eq!(
                    found.blob_id, expected.blob_id,
                    "Blob ID mismatch for {}",
                    primary_index
                );
                assert_eq!(
                    found.quilt_status, expected.quilt_status,
                    "Quilt status mismatch for {}",
                    primary_index
                );
            }
            (Some(IndexTarget::QuiltPatchId(found)), IndexTarget::QuiltPatchId(expected)) => {
                assert_eq!(
                    found, expected,
                    "Quilt patch ID mismatch for {}",
                    primary_index
                );
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "Target mismatch for {}: found {:?}, expected {:?}",
                    primary_index,
                    index_target,
                    expected_target
                ));
            }
        }
    }
    Ok(())
}

/// Verify that all quilt patch blob_id to patch_id mappings are correctly indexed.
async fn verify_quilt_patch_mappings(
    indexer: &Arc<WalrusIndexer>,
    patch_blob_mapping: &std::collections::HashMap<walrus_core::BlobId, walrus_core::QuiltPatchId>,
) -> Result<()> {
    for (patch_blob_id, expected_patch_id) in patch_blob_mapping {
        let found_patch_id = indexer
            .storage
            .get_quilt_patch_id_by_blob_id(patch_blob_id)?;

        match found_patch_id {
            Some(found) => {
                assert_eq!(
                    found, *expected_patch_id,
                    "Patch ID mismatch for blob_id {:?}",
                    patch_blob_id
                );
            }
            None => {
                return Err(anyhow::anyhow!(
                    "Expected to find quilt patch for blob_id {:?}",
                    patch_blob_id
                ));
            }
        }
    }

    Ok(())
}

#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_walrus_indexer_basic() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("walrus_indexer=debug,walrus_service=debug,walrus_service::event=trace")
        .try_init();

    let (sui_cluster_handle, _walrus_cluster, client, _) = test_cluster::E2eTestSetupBuilder::new()
        .with_epoch_duration(Duration::from_secs(10))
        .build()
        .await?;

    let bucket_id_1 = ObjectID::random();
    let bucket_id_2 = ObjectID::random();

    // === PHASE 1: Start the indexer ===
    // The indexer will start processing events from the blockchain.
    let temp_dir = TempDir::new()?;
    let (indexer, cancel_token, indexer_handle) =
        start_walrus_indexer(&sui_cluster_handle, &client, &temp_dir).await?;

    // === PHASE 2: Write data while indexer is running ===
    // Store blobs and quilts that will be indexed in real-time.

    // Store blobs for bucket 1.
    let blob_mapping_1 = store_blobs(&client, bucket_id_1, "test1", 5, 1024, 2048).await?;

    // Store blobs for bucket 2.
    let blob_mapping_2 = store_blobs(&client, bucket_id_2, "test2", 5, 1024, 2048).await?;

    // Store a quilt with patches.
    let (quilt_primary_mapping, quilt_patch_blob_mapping) = store_quilt(
        &client,
        bucket_id_1,
        "quilt-main",
        "quilt-patch",
        5,
        1024,
        2048,
    )
    .await?;

    // === PHASE 3: Wait for indexer to process events ===
    // Give the indexer time to catch up with the new events.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // === PHASE 4: Verify indexed data ===
    // Check that all blobs were properly indexed.

    // Verify primary index mappings for blobs from both buckets.
    verify_primary_index_mappings(&indexer, bucket_id_1, &blob_mapping_1).await?;
    verify_primary_index_mappings(&indexer, bucket_id_2, &blob_mapping_2).await?;

    // === PHASE 5: Wait for quilt processing to complete ===
    // Quilt processing happens asynchronously, so we need more time.
    tokio::time::sleep(Duration::from_secs(30)).await;

    // === PHASE 6: Verify quilt patches ===
    // Check that all quilt patches were properly indexed.

    // Verify quilt primary index mappings and patch blob mappings.
    verify_primary_index_mappings(&indexer, bucket_id_1, &quilt_primary_mapping).await?;
    verify_quilt_patch_mappings(&indexer, &quilt_patch_blob_mapping).await?;

    // === PHASE 8: Shutdown ===
    // Gracefully shutdown the indexer.
    cancel_token.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), indexer_handle).await;

    println!("\n✅ Test completed successfully");
    Ok(())
}

/// Test indexer catch-up behavior: start clusters without indexer,
/// write blobs/quilts, then start indexer and verify it catches up with all entries.
#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_walrus_indexer_catchup() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("walrus_indexer=debug,walrus_service=debug,walrus_service::event=trace")
        .try_init();

    // Set up the test environment without starting the indexer yet.
    let (sui_cluster_handle, _walrus_cluster, client, _) = test_cluster::E2eTestSetupBuilder::new()
        .with_epoch_duration(Duration::from_secs(10))
        .build()
        .await?;

    let bucket_id = ObjectID::random();

    // Store initial blobs.
    let initial_blob_mapping = store_blobs(&client, bucket_id, "initial", 3, 512, 1024).await?;

    // Store initial quilt.
    let (initial_quilt_mapping, initial_quilt_patch_mapping) = store_quilt(
        &client,
        bucket_id,
        "initial-quilt-main",
        "initial-quilt-patch",
        3,
        512,
        1024,
    )
    .await?;

    assert_eq!(initial_quilt_mapping.len(), 4);
    assert_eq!(initial_quilt_patch_mapping.len(), 3);
    tracing::info!(
        "Stored {} initial blobs and 1 quilt with {} patches",
        initial_blob_mapping.len(),
        initial_quilt_patch_mapping.len(),
    );

    // Now start the indexer, which should catch up with all the events from phases 1-2.
    let temp_dir = TempDir::new()?;
    let (indexer, cancel_token, indexer_handle) =
        start_walrus_indexer(&sui_cluster_handle, &client, &temp_dir).await?;

    // Store additional blobs in two groups.
    let additional_blob_mapping_1 =
        store_blobs(&client, bucket_id, "additional-1", 2, 512, 1024).await?;

    // Store additional quilt.
    let (additional_quilt_mapping, additional_quilt_patch_mapping) = store_quilt(
        &client,
        bucket_id,
        "additional-quilt-main",
        "additional-quilt-patch",
        2,
        512,
        1024,
    )
    .await?;

    tokio::time::sleep(Duration::from_secs(30)).await;

    // Verify all primary index mappings for blobs.
    verify_primary_index_mappings(&indexer, bucket_id, &initial_blob_mapping).await?;
    verify_primary_index_mappings(&indexer, bucket_id, &additional_blob_mapping_1).await?;
    verify_primary_index_mappings(&indexer, bucket_id, &initial_quilt_mapping).await?;
    verify_primary_index_mappings(&indexer, bucket_id, &additional_quilt_mapping).await?;
    verify_quilt_patch_mappings(&indexer, &initial_quilt_patch_mapping).await?;
    verify_quilt_patch_mappings(&indexer, &additional_quilt_patch_mapping).await?;

    // === PHASE 8: Verify complete state ===
    // Final verification of the complete index state.
    tracing::info!("📊 Phase 8: Verifying complete index state");

    // Count expected totals
    let expected_bucket_count = initial_blob_mapping.len()
        + additional_blob_mapping_1.len()
        + initial_quilt_mapping.len()
        + additional_quilt_mapping.len();
    // Note: quilt_mapping includes both the main quilt and its patches

    let bucket_entries = indexer.list_blobs_in_bucket(&bucket_id).await?;

    assert_eq!(
        bucket_entries.len(),
        expected_bucket_count,
        "Bucket should have {} entries, but found {}",
        expected_bucket_count,
        bucket_entries.len()
    );

    tracing::info!("✅ Bucket verification complete:");
    tracing::info!("  - Bucket: {} entries", bucket_entries.len());

    // Verify quilt patch index completeness
    let patch_entries = indexer.storage.get_all_quilt_patch_entries()?;
    let expected_patch_count =
        initial_quilt_patch_mapping.len() + additional_quilt_patch_mapping.len();
    assert_eq!(
        patch_entries.len(),
        expected_patch_count,
        "Quilt patch index should have {} entries, but found {}",
        expected_patch_count,
        patch_entries.len()
    );

    tracing::info!(
        "✅ Quilt patch index verification complete: {} entries",
        patch_entries.len()
    );

    // Check pending quilt tasks - should be empty after processing
    let pending_tasks = indexer.storage.get_all_pending_quilt_tasks()?;
    tracing::info!(
        "📋 Pending quilt tasks: {} (should be 0 when complete)",
        pending_tasks.len()
    );

    // === PHASE 9: Shutdown ===
    // Gracefully shutdown the indexer.
    cancel_token.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), indexer_handle).await;

    Ok(())
}
