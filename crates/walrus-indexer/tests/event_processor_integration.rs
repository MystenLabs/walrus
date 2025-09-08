// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Integration test for indexer with event processor.

// Allowing `unwrap`s in tests.
#![allow(clippy::unwrap_used)]

use std::{str::FromStr, time::Duration};

use anyhow::Result;
use sui_types::base_types::ObjectID;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use walrus_core::{
    BlobId,
    encoding::quilt_encoding::{QuiltStoreBlob, QuiltVersionV1},
};
use walrus_indexer::{Bucket, IndexerConfig, WalrusIndexer, storage::IndexTarget};
use walrus_proc_macros::walrus_simtest;
use walrus_sdk::client::StoreArgs;
use walrus_service::{
    common::config::SuiConfig,
    event::event_processor::config::EventProcessorConfig,
    test_utils::test_cluster,
};
use walrus_sui::{client::BlobBucketIdentifier, config::WalletConfig};

#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_indexer_with_event_processor() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("walrus_indexer=debug,walrus_service=debug,walrus_service::event=trace")
        .try_init();

    let (sui_cluster_handle, _walrus_cluster, client, _) = test_cluster::E2eTestSetupBuilder::new()
        .with_epoch_duration(Duration::from_secs(10))
        .build()
        .await?;

    let bucket_id_1 = ObjectID::random();
    let bucket_id_2 = ObjectID::random();
    let data_list_1 = walrus_test_utils::generate_random_data(5, 1024, 2048);
    let data_list_2 = walrus_test_utils::generate_random_data(5, 1024, 2048);
    let unencoded_blobs_1 = data_list_1.iter().enumerate().map(|(i, data)| {
        walrus_sdk::client::UnencodedBlob::new(data, 0)
            .with_bucket_identifier(BlobBucketIdentifier {
                bucket_id: bucket_id_1,
                identifier: format!("test1-{}", i),
            })
    }).collect::<Vec<_>>();
    let unencoded_blobs_2 = data_list_2.iter().enumerate().map(|(i, data)| {
        walrus_sdk::client::UnencodedBlob::new(data, 0)
            .with_bucket_identifier(BlobBucketIdentifier {
                bucket_id: bucket_id_2,
                identifier: format!("test2-{}", i),
            })
    }).collect::<Vec<_>>();

    let data_list_quilt = walrus_test_utils::generate_random_data(5, 1024, 2048);
    let encoder_config = client.as_ref().encoding_config().get_for_type(walrus_core::EncodingType::RS2);
    let quilt_store_blobs = data_list_quilt.iter().enumerate().map(|(i, data)| {
        QuiltStoreBlob::new(data, format!("quilt-patch-{}", i))
            .expect("Should create blob").with_blob_id(&encoder_config)
    }).collect::<Result<Vec<_>, _>>()?;


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
    let temp_dir = TempDir::new()?;
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

    // Create the indexer.
    let indexer = WalrusIndexer::new(indexer_config).await?;

    // Create a cancellation token for the indexer.
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

    let store_args = StoreArgs::default_with_epochs(1).no_store_optimizations();
    let store_results_1 = client
        .as_ref()
        .reserve_and_store_blobs_retry_committees(&unencoded_blobs_1, &store_args)
        .await?;
    let store_results_2 = client
        .as_ref()
        .reserve_and_store_blobs_retry_committees(&unencoded_blobs_2, &store_args)
        .await?;
    let quilt = client
        .as_ref()
        .quilt_client()
        .construct_quilt::<QuiltVersionV1>(&quilt_store_blobs, store_args.encoding_type)
        .await?;
    let quilt_bucket_identifier = BlobBucketIdentifier {
        bucket_id: bucket_id_1,
        identifier: "quilt-main".to_string(),
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

    tokio::time::sleep(Duration::from_secs(3)).await;

    for (i, unencoded_blob) in unencoded_blobs_1.iter().enumerate() {
        let identifier = &unencoded_blob.bucket_identifier.as_ref().unwrap().identifier;
        let expected_blob_id = store_results_1[i].blob_id().expect("blob should have ID");
        
        let index_target = indexer.get_index_target_from_bucket(&bucket_id_1, identifier).await?;
        match index_target {
            Some(IndexTarget::Blob(blob_identity)) => {
                assert_eq!(blob_identity.blob_id, expected_blob_id);
            }
            Some(other) => panic!("Expected IndexTarget::Blob, got {:?}", other),
            None => panic!("Expected to find blob for identifier {}", identifier),
        }
    }

    for (i, unencoded_blob) in unencoded_blobs_2.iter().enumerate() {
        let identifier = &unencoded_blob.bucket_identifier.as_ref().unwrap().identifier;
        let expected_blob_id = store_results_2[i].blob_id().expect("blob should have ID");
        
        let index_target = indexer.get_index_target_from_bucket(&bucket_id_2, identifier).await?;
        match index_target {
            Some(IndexTarget::Blob(blob_identity)) => {
                assert_eq!(blob_identity.blob_id, expected_blob_id);
            }
            Some(other) => panic!("Expected IndexTarget::Blob, got {:?}", other),
            None => panic!("Expected to find blob for identifier {}", identifier),
        }
    }

    let  quilt_target = indexer.get_index_target_from_bucket(&bucket_id_1, "quilt-main").await?;
    match quilt_target {
        Some(IndexTarget::Blob(blob_identity)) => {
            let expected_quilt_blob_id = quilt_store_result.blob_store_result.blob_id().expect("quilt should have blob ID");
            assert_eq!(blob_identity.blob_id, expected_quilt_blob_id);
        }
        Some(other) => panic!("Expected IndexTarget::Blob for quilt, got {:?}", other),
        None => panic!("Expected to find quilt for identifier quilt-main"),
    }

    tokio::time::sleep(Duration::from_secs(30)).await;

    let quilt_patches = &quilt_store_result.stored_quilt_blobs;
    for stored_quilt_patch in quilt_patches.iter() {
        let patch_identifier = stored_quilt_patch.identifier.as_str();
        let expected_patch_id = walrus_core::QuiltPatchId::from_str(&stored_quilt_patch.quilt_patch_id).expect("Valid patch ID");
        let patch_blob_id = stored_quilt_patch.patch_blob_id.expect("QuiltStoreBlob should have blob_id");

        let patch_index_target = indexer
            .get_index_target_from_bucket(&bucket_id_1, patch_identifier)
            .await?;
        
        match patch_index_target {
            Some(IndexTarget::QuiltPatchId(found_patch_id)) => {
                assert_eq!(found_patch_id, expected_patch_id);
            }
            Some(other) => {
                panic!("Expected IndexTarget::QuiltPatchId, got {:?}", other);
            }
            None => {
                panic!("Expected to find quilt patch for identifier {}", patch_identifier);
            }
        }

        // Check if the patch is in the quilt patch index
        let quilt_patch_from_index = indexer
            .storage
            .get_quilt_patch_id_by_blob_id(&patch_blob_id)?;
        
        match quilt_patch_from_index {
            Some(found_patch_id) => {
                assert_eq!(found_patch_id, expected_patch_id);
            }
            None => {
                panic!("Expected to find quilt patch for blob_id {:?}", patch_blob_id);
            }
        }
    }

    // List all entries to show the complete state
    println!("\n📝 Complete index state:");
    
    println!("  Bucket_1 entries:");
    let bucket_1_entries = indexer.list_blobs_in_bucket(&bucket_id_1).await?;
    for (identifier, blob_identity) in &bucket_1_entries {
        println!("    - {}: {:?}", identifier, blob_identity.blob_id);
    }
    
    println!("  Bucket_2 entries:");  
    let bucket_2_entries = indexer.list_blobs_in_bucket(&bucket_id_2).await?;
    for (identifier, blob_identity) in &bucket_2_entries {
        println!("    - {}: {:?}", identifier, blob_identity.blob_id);
    }

    // Check quilt patch index directly
    println!("\n🔍 Checking quilt patch index directly:");
    match indexer.storage.get_all_quilt_patch_entries() {
        Ok(patch_entries) => {
            println!(
                "Found {} entries in quilt patch index:",
                patch_entries.len()
            );
            for (key, patch_id) in patch_entries {
                println!("  - key: '{}', patch_id: {:?}", key, patch_id);
            }
        }
        Err(e) => println!("❌ Failed to read quilt patch index: {}", e),
    }

    // Check pending quilt tasks
    println!("\n📋 Checking pending quilt tasks:");
    match indexer.storage.get_all_pending_quilt_tasks() {
        Ok(pending_tasks) => {
            println!("Found {} pending quilt tasks:", pending_tasks.len());
            for (key, task) in pending_tasks {
                println!(
                    "  - key: {:?}, object_id: {:?}, bucket_id: {:?}",
                    key, task.object_id, task.bucket_id
                );
            }
        }
        Err(e) => println!("❌ Failed to read pending tasks: {}", e),
    }

    // Shutdown the indexer.
    cancel_token.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), indexer_handle).await;

    println!("\n✅ Test completed successfully");
    Ok(())
}

/// Test the indexer with REST API functionality.
#[ignore = "ignore E2E tests by default"]
#[tokio::test]
async fn test_indexer_with_rest_api() -> Result<()> {
    // Initialize logging for debugging.
    let _ = tracing_subscriber::fmt()
        .with_env_filter("walrus_indexer=debug")
        .try_init();

    println!("🚀 Testing indexer with REST API");

    // Create test clusters.
    let (sui_cluster_handle, _walrus_cluster, client, _) =
        test_cluster::E2eTestSetupBuilder::new().build().await?;

    // Get configuration from test cluster.
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

    // Create indexer configuration with REST API.
    let temp_dir = TempDir::new()?;
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

    // Create and start the indexer.
    let indexer = WalrusIndexer::new(indexer_config).await?;
    let cancel_token = CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();

    // Create a metrics registry
    let prometheus_registry = prometheus::Registry::new();
    let metrics_registry = walrus_utils::metrics::Registry::new(prometheus_registry);

    let indexer_handle =
        tokio::spawn(async move { indexer.run(&metrics_registry, cancel_token_clone).await });

    // Give the indexer time to start up and bind to a port.
    tokio::time::sleep(Duration::from_secs(3)).await;

    println!("✅ Indexer started with REST API");

    // Create test data directly in storage.
    let indexer_for_data = WalrusIndexer::new(IndexerConfig {
        db_path: temp_dir.path().to_path_buf(),
        ..Default::default()
    })
    .await?;

    // Create a bucket.
    let bucket_id = ObjectID::from_hex_literal(
        "0xbbbbaaaaffffeeeedddccccbbbbaaaafffeeedddccccbbbaaaafffeeeddcc",
    )
    .unwrap();

    indexer_for_data
        .create_bucket(Bucket {
            bucket_id,
            name: "api-test-bucket".to_string(),
            secondary_indices: vec![],
        })
        .await?;

    // Add some test entries.
    let entries = vec![
        ("/api/test/file1.txt", BlobId([1; 32]), ObjectID::random()),
        ("/api/test/file2.txt", BlobId([2; 32]), ObjectID::random()),
        (
            "/api/test/subdir/file3.txt",
            BlobId([3; 32]),
            ObjectID::random(),
        ),
    ];

    let mutations: Vec<_> = entries
        .iter()
        .map(
            |(path, blob_id, object_id)| walrus_sui::types::IndexMutation::Insert {
                identifier: path.to_string(),
                object_id: *object_id,
                blob_id: *blob_id,
                is_quilt: false,
            },
        )
        .collect();

    indexer_for_data
        .storage
        .apply_index_mutations(vec![walrus_sui::types::IndexMutationSet {
            bucket_id,
            mutations,
            event_id: sui_types::event::EventID {
                tx_digest: sui_types::base_types::TransactionDigest::new([0; 32]),
                event_seq: 0,
            },
        }])
        .map_err(|e| anyhow::anyhow!("Failed to add entries: {}", e))?;

    println!("✅ Test data created");

    // Test REST API endpoints (Note: actual HTTP calls would require knowing the bound port).
    // For now, we'll test the indexer's query methods that the REST API uses.

    // Test get blob by index.
    let entry = indexer_for_data
        .get_blob_from_bucket(&bucket_id, "/api/test/file1.txt")
        .await?;
    assert!(entry.is_some());
    assert_eq!(entry.unwrap().blob_id, entries[0].1);

    // Test list bucket.
    let bucket_entries = indexer_for_data.list_blobs_in_bucket(&bucket_id).await?;
    assert_eq!(bucket_entries.len(), 3);

    // Test bucket stats.
    let stats = indexer_for_data.get_bucket_stats(&bucket_id).await?;
    assert_eq!(stats.primary_count, 3);
    assert_eq!(stats.secondary_count, 0);

    // Stop the indexer.
    cancel_token.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), indexer_handle).await;

    println!("✅ REST API integration test passed!");

    Ok(())
}
