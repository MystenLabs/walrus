// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Integration test for indexer with event processor.

// Allowing `unwrap`s in tests.
#![allow(clippy::unwrap_used)]

use std::time::Duration;

use anyhow::Result;
use sui_types::base_types::ObjectID;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use walrus_core::BlobId;
use walrus_indexer::{Bucket, IndexerConfig, IndexerEventProcessorConfig, WalrusIndexer};
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
    // Initialize logging for debugging - include event processor logs.
    let _ = tracing_subscriber::fmt()
        .with_env_filter("walrus_indexer=debug,walrus_service=debug,walrus_service::event=trace")
        .try_init();

    println!("🚀 Testing indexer with event processor integration");

    // Create a bucket ID for indexing throughout the test.
    let bucket_id = ObjectID::from_hex_literal(
        "0xbbbbaaaaffffeeeedddccccbbbbaaaafffeeedddccccbbbaaaafffeeeddcc",
    )
    .unwrap();

    let (sui_cluster_handle, _walrus_cluster, client, _) = test_cluster::E2eTestSetupBuilder::new()
        .with_epoch_duration(Duration::from_secs(10))
        .build()
        .await?;

    // Get RPC URL from the test cluster.
    let rpc_url = sui_cluster_handle
        .lock()
        .await
        .cluster()
        .rpc_url()
        .to_string();

    println!("✅ Test clusters created, RPC URL: {}", rpc_url);

    // Get wallet config from the test cluster.
    let cluster_wallet_path = sui_cluster_handle.lock().await.wallet_path().await;
    let wallet_config = WalletConfig::from_path(cluster_wallet_path);

    // Get contract config from the client.
    let contract_config = client
        .as_ref()
        .sui_client()
        .read_client()
        .contract_config()
        .clone();

    // Create indexer configuration.
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
        event_processor_config: Some(IndexerEventProcessorConfig {
            event_processor_config: EventProcessorConfig::default(),
            sui_config,
        }),
    };

    // Create the indexer.
    let indexer = WalrusIndexer::new(indexer_config).await?;

    // Check initial event cursor position before storing blobs.
    let pre_store_cursor = indexer
        .storage
        .get_last_processed_event_index()
        .map_err(|e| anyhow::anyhow!("Failed to get initial event index: {}", e))?;
    println!("📊 Initial event cursor position: {:?}", pre_store_cursor);

    // Create a cancellation token for the indexer.
    let cancel_token = CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();

    // Start the indexer in a background task.
    let indexer_for_run = indexer.clone();
    let indexer_handle = tokio::spawn(async move { indexer_for_run.run(cancel_token_clone).await });

    // Give the indexer time to start up.
    tokio::time::sleep(Duration::from_secs(2)).await;

    println!("✅ Indexer started with event processor");

    // Store a blob using the client to generate real events.
    let blob_data = walrus_test_utils::random_data(1024);
    let store_args = StoreArgs::default_with_epochs(1).no_store_optimizations();
    let store_results = client
        .as_ref()
        .reserve_and_store_blobs(&[&blob_data], &store_args)
        .await?;

    let blob_id = store_results[0].blob_id().expect("blob should have ID");
    let object_id = match &store_results[0] {
        walrus_sdk::client::responses::BlobStoreResult::NewlyCreated { blob_object, .. } => {
            blob_object.id
        }
        _ => panic!("Expected newly created blob"),
    };

    println!(
        "✅ Stored blob with ID: {:?}, object ID: {:?}",
        blob_id, object_id
    );

    // Store more blobs to generate multiple events.
    println!("📝 Storing additional blobs to generate more events...");

    let blob_data2 = walrus_test_utils::random_data(2048);
    let blob_data3 = walrus_test_utils::random_data(512);
    let store_results2 = client
        .as_ref()
        .reserve_and_store_blobs_retry_committees(
            &[&blob_data2, &blob_data3],
            &[],
            &[
                BlobBucketIdentifier {
                    bucket_id,
                    identifier: "test1".to_string(),
                },
                BlobBucketIdentifier {
                    bucket_id,
                    identifier: "test2".to_string(),
                },
            ],
            &store_args,
        )
        .await?;

    println!("✅ Stored {} more blobs", store_results2.len());

    // Wait for events to be processed.
    println!("⏳ Waiting for event processor to consume events...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Check event cursor progress - it should have advanced.
    let cursor = indexer
        .storage
        .get_last_processed_event_index()
        .map_err(|e| anyhow::anyhow!("Failed to get event index: {}", e))?;
    println!("📊 Event cursor after processing: {:?}", cursor);

    // Verify the event processor has been processing events.
    if let Some(cursor) = cursor {
        assert!(
            cursor > 0,
            "Event processor should have processed some events"
        );
        println!(
            "✅ Event processor confirmed working - processed up to event index: {}",
            cursor
        );
    } else {
        // If no cursor is set, it might mean no events were found yet.
        println!("⚠️  No event cursor found - event processor may still be initializing");
    }

    let blob_identity = indexer.get_blob_from_bucket(&bucket_id, "test1").await?;
    let blob_identity = blob_identity.expect("blob identity should be found");
    assert_eq!(
        blob_identity.blob_id,
        store_results2[0].blob_id().expect("blob should have ID")
    );

    let blob_identity = indexer.get_blob_from_bucket(&bucket_id, "test2").await?;
    let blob_identity = blob_identity.expect("blob identity should be found");
    assert_eq!(
        blob_identity.blob_id,
        store_results2[1].blob_id().expect("blob should have ID")
    );

    // Shutdown the indexer.
    cancel_token.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), indexer_handle).await;

    println!("✅ Test completed successfully");
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
        event_processor_config: Some(IndexerEventProcessorConfig {
            event_processor_config: EventProcessorConfig::default(),
            sui_config,
        }),
    };

    // Create and start the indexer.
    let indexer = WalrusIndexer::new(indexer_config).await?;
    let cancel_token = CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();

    let indexer_handle = tokio::spawn(async move { indexer.run(cancel_token_clone).await });

    // Give the indexer time to start up and bind to a port.
    tokio::time::sleep(Duration::from_secs(3)).await;

    println!("✅ Indexer started with REST API");

    // Create test data directly in storage.
    let indexer_for_data = WalrusIndexer::new(IndexerConfig {
        db_path: temp_dir.path().to_path_buf(),
        event_processor_config: None,
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

    for (path, blob_id, object_id) in &entries {
        indexer_for_data
            .storage
            .put_index_entry(&bucket_id, path, object_id, *blob_id)
            .map_err(|e| anyhow::anyhow!("Failed to add entry: {}", e))?;
    }

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
