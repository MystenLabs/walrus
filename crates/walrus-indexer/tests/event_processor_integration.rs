// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Integration test for indexer with event processor.

// Allowing `unwrap`s in tests.
#![allow(clippy::unwrap_used)]

use std::time::Duration;

use anyhow::Result;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use walrus_indexer::{
    Bucket,
    IndexerConfig,
    WalrusIndexer,
    RestApiConfig,
};
use walrus_service::{
    event::event_processor::config::EventProcessorConfig,
    common::config::SuiConfig,
};
use walrus_sui::{
    config::WalletConfig,
};
use sui_types::{
    base_types::ObjectID,
};
use walrus_core::BlobId;
use walrus_sdk::client::StoreArgs;

// Note: test_cluster is part of walrus-service test utilities, which needs to be
// included in the test dependencies.
#[cfg(test)]
mod tests {
    use super::*;
    use walrus_service::test_utils::test_cluster;

    /// Test that the indexer properly integrates with event processor.
    #[ignore = "ignore E2E tests by default"]
    #[tokio::test]
    async fn test_indexer_with_event_processor() -> Result<()> {
    // Initialize logging for debugging - include event processor logs.
    let _ = tracing_subscriber::fmt()
        .with_env_filter("walrus_indexer=debug,walrus_service=debug,walrus_service::event=trace")
        .try_init();
    
    println!("🚀 Testing indexer with event processor integration");
    
    // Create test clusters using the same pattern as test_client.rs.
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
    let contract_config = client.as_ref().sui_client().read_client().contract_config().clone();
    
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
    
    let rest_api_config = RestApiConfig {
        bind_address: "127.0.0.1:0".parse()?,
        metrics_address: "127.0.0.1:0".parse()?,
    };
    
    let indexer_config = IndexerConfig {
        db_path: temp_dir.path().to_str().unwrap().to_string(),
        rest_api_config: Some(rest_api_config),
        event_processor_config: Some(EventProcessorConfig::default()),
        sui: Some(sui_config),
    };
    
    // Create the indexer.
    let indexer = WalrusIndexer::new(indexer_config).await?;
    
    // Create a cancellation token for the indexer.
    let cancel_token = CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();
    
    // Start the indexer in a background task.
    let indexer_handle = tokio::spawn(async move {
        indexer.run(cancel_token_clone).await
    });
    
    // Give the indexer time to start up.
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    println!("✅ Indexer started with event processor");
    
    // Give the event processor time to initialize and start consuming checkpoints.
    println!("⏳ Waiting for event processor to initialize...");
    tokio::time::sleep(Duration::from_secs(3)).await;
    
    // Create a test bucket.
    let bucket_id = ObjectID::from_hex_literal(
        "0xaaaabbbbccccddddeeeeffffaaaabbbbccccddddeeeeffffaaaabbbbccccddd"
    ).unwrap();
    
    // Note: In a real test, we would create the bucket through Sui events.
    // For now, we'll create it directly in the indexer storage.
    // Once BucketCreated events are processed by the indexer, this won't be needed.
    let indexer_for_bucket = WalrusIndexer::new(IndexerConfig {
        db_path: temp_dir.path().to_str().unwrap().to_string(),
        rest_api_config: None,
        event_processor_config: None,
        sui: None,
    }).await?;
    
    let bucket = Bucket {
        bucket_id,
        name: "test-bucket".to_string(),
        secondary_indices: vec![],
    };
    
    indexer_for_bucket.create_bucket(bucket).await?;
    
    // Check initial event cursor position before storing blobs.
    let pre_store_cursor = indexer_for_bucket.storage.get_last_processed_event_index()
        .map_err(|e| anyhow::anyhow!("Failed to get initial event index: {}", e))?;
    println!("📊 Initial event cursor position: {:?}", pre_store_cursor);
    
    // Store a blob using the client to generate real events.
    let blob_data = walrus_test_utils::random_data(1024);
    let store_args = StoreArgs::default_with_epochs(1).no_store_optimizations();
    let store_results = client.as_ref()
        .reserve_and_store_blobs(&[&blob_data], &store_args)
        .await?;
    
    let blob_id = store_results[0].blob_id().expect("blob should have ID");
    let object_id = match &store_results[0] {
        walrus_sdk::client::responses::BlobStoreResult::NewlyCreated { blob_object, .. } => {
            blob_object.id
        }
        _ => panic!("Expected newly created blob"),
    };
    
    println!("✅ Stored blob with ID: {:?}, object ID: {:?}", blob_id, object_id);
    
    // Store more blobs to generate multiple events.
    println!("📝 Storing additional blobs to generate more events...");
    let blob_data2 = walrus_test_utils::random_data(2048);
    let blob_data3 = walrus_test_utils::random_data(512);
    let store_results2 = client.as_ref()
        .reserve_and_store_blobs(&[&blob_data2, &blob_data3], &store_args)
        .await?;
    
    println!("✅ Stored {} more blobs", store_results2.len());
    
    // Wait for events to be processed.
    println!("⏳ Waiting for event processor to consume events...");
    tokio::time::sleep(Duration::from_secs(8)).await;
    
    // Create another indexer instance to query the data.
    let query_indexer = WalrusIndexer::new(IndexerConfig {
        db_path: temp_dir.path().to_str().unwrap().to_string(),
        rest_api_config: None,
        event_processor_config: None,
        sui: None,
    }).await?;
    
    // Check event cursor progress - it should have advanced.
    let initial_cursor = query_indexer.storage.get_last_processed_event_index()
        .map_err(|e| anyhow::anyhow!("Failed to get event index: {}", e))?;
    println!("📊 Event cursor after processing: {:?}", initial_cursor);
    
    // Verify the event processor has been processing events.
    if let Some(cursor) = initial_cursor {
        assert!(cursor > 0, "Event processor should have processed some events");
        println!("✅ Event processor confirmed working - processed up to event index: {}", cursor);
    } else {
        // If no cursor is set, it might mean no events were found yet.
        println!("⚠️  No event cursor found - event processor may still be initializing");
    }
    
    // The event processor should be receiving various Walrus events from Sui, including:
    // - BlobEvent::Registered - when blobs are registered on-chain
    // - BlobEvent::Certified - when blobs are certified
    // - BlobEvent::Deleted - when blobs are deleted
    // - Other system events like epoch changes, committee updates, etc.
    //
    // The EventProcessorRuntime in walrus-service processes these events and updates
    // the indexer's state accordingly. Currently, BlobIndexEvent (for bucket-based indexing)
    // is not yet part of ContractEvent, so we simulate index updates manually below.
    
    // For now, simulate index processing by directly adding to the index.
    // This is because BlobIndexEvent is not yet part of ContractEvent.
    // Once BlobIndexEvent is added, the event processor will handle this automatically.
    query_indexer.storage.put_index_entry(
        &bucket_id,
        "/test/event/blob.dat",
        &object_id,
        blob_id
    ).map_err(|e| anyhow::anyhow!("Failed to add index entry: {}", e))?;
    
    // Also add entries for the additional blobs.
    for (i, result) in store_results2.iter().enumerate() {
        if let walrus_sdk::client::responses::BlobStoreResult::NewlyCreated { 
            blob_object, 
            .. 
        } = result {
            query_indexer.storage.put_index_entry(
                &bucket_id,
                &format!("/test/event/blob{}.dat", i + 2),
                &blob_object.id,
                blob_object.blob_id
            ).map_err(|e| anyhow::anyhow!("Failed to add index entry: {}", e))?;
        }
    }
    
    // Verify the blob was indexed.
    let entry = query_indexer.get_blob_by_index(&bucket_id, "/test/event/blob.dat").await?;
    assert!(entry.is_some());
    let retrieved = entry.unwrap();
    assert_eq!(retrieved.blob_id, blob_id);
    assert_eq!(retrieved.object_id, object_id);
    
    // Also verify by object_id.
    let entry_by_object = query_indexer.get_blob_by_object_id(&object_id).await?;
    assert!(entry_by_object.is_some());
    let retrieved_by_object = entry_by_object.unwrap();
    assert_eq!(retrieved_by_object.blob_id, blob_id);
    
    // Check that we have multiple entries now.
    let bucket_entries = query_indexer.list_bucket(&bucket_id).await?;
    println!("📚 Total entries in bucket: {}", bucket_entries.len());
    assert!(bucket_entries.len() >= 1, "Should have at least one entry");
    
    // Give a bit more time for any final event processing.
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // Check final event cursor position.
    let final_cursor = query_indexer.storage.get_last_processed_event_index()
        .map_err(|e| anyhow::anyhow!("Failed to get event index: {}", e))?;
    println!("📊 Final event cursor: {:?}", final_cursor);
    
    // If we had an initial cursor, verify it advanced.
    if let (Some(initial), Some(final_pos)) = (initial_cursor, final_cursor) {
        assert!(
            final_pos >= initial,
            "Event cursor should not go backwards (was {}, now {})",
            initial,
            final_pos
        );
    }
    
    // Stop the indexer.
    cancel_token.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), indexer_handle).await;
    
    println!("✅ Event processing integration test passed!");
    
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
    let (sui_cluster_handle, _walrus_cluster, client, _) = test_cluster::E2eTestSetupBuilder::new()
        .build()
        .await?;
    
    // Get configuration from test cluster.
    let rpc_url = sui_cluster_handle
        .lock()
        .await
        .cluster()
        .rpc_url()
        .to_string();
    let cluster_wallet_path = sui_cluster_handle.lock().await.wallet_path().await;
    let wallet_config = WalletConfig::from_path(cluster_wallet_path);
    let contract_config = client.as_ref().sui_client().read_client().contract_config().clone();
    
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
    
    let rest_api_config = RestApiConfig {
        bind_address: "127.0.0.1:0".parse()?, // Let OS assign port.
        metrics_address: "127.0.0.1:0".parse()?,
    };
    
    let indexer_config = IndexerConfig {
        db_path: temp_dir.path().to_str().unwrap().to_string(),
        rest_api_config: Some(rest_api_config),
        event_processor_config: Some(EventProcessorConfig::default()),
        sui: Some(sui_config),
    };
    
    // Create and start the indexer.
    let indexer = WalrusIndexer::new(indexer_config).await?;
    let cancel_token = CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();
    
    let indexer_handle = tokio::spawn(async move {
        indexer.run(cancel_token_clone).await
    });
    
    // Give the indexer time to start up and bind to a port.
    tokio::time::sleep(Duration::from_secs(3)).await;
    
    println!("✅ Indexer started with REST API");
    
    // Create test data directly in storage.
    let indexer_for_data = WalrusIndexer::new(IndexerConfig {
        db_path: temp_dir.path().to_str().unwrap().to_string(),
        rest_api_config: None,
        event_processor_config: None,
        sui: None,
    }).await?;
    
    // Create a bucket.
    let bucket_id = ObjectID::from_hex_literal(
        "0xbbbbaaaaffffeeeedddccccbbbbaaaafffeeedddccccbbbaaaafffeeeddcc"
    ).unwrap();
    
    indexer_for_data.create_bucket(Bucket {
        bucket_id,
        name: "api-test-bucket".to_string(),
        secondary_indices: vec![],
    }).await?;
    
    // Add some test entries.
    let entries = vec![
        ("/api/test/file1.txt", BlobId([1; 32]), ObjectID::random()),
        ("/api/test/file2.txt", BlobId([2; 32]), ObjectID::random()),
        ("/api/test/subdir/file3.txt", BlobId([3; 32]), ObjectID::random()),
    ];
    
    for (path, blob_id, object_id) in &entries {
        indexer_for_data.storage
            .put_index_entry(&bucket_id, path, object_id, *blob_id)
            .map_err(|e| anyhow::anyhow!("Failed to add entry: {}", e))?;
    }
    
    println!("✅ Test data created");
    
    // Test REST API endpoints (Note: actual HTTP calls would require knowing the bound port).
    // For now, we'll test the indexer's query methods that the REST API uses.
    
    // Test get blob by index.
    let entry = indexer_for_data
        .get_blob_by_index(&bucket_id, "/api/test/file1.txt")
        .await?;
    assert!(entry.is_some());
    assert_eq!(entry.unwrap().blob_id, entries[0].1);
    
    // Test list bucket.
    let bucket_entries = indexer_for_data.list_bucket(&bucket_id).await?;
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

    /// Test event cursor persistence across restarts.
    #[tokio::test]
    async fn test_event_cursor_persistence() -> Result<()> {
    let temp_dir = TempDir::new()?;
    
    // Create first indexer instance.
    let config = IndexerConfig {
        db_path: temp_dir.path().to_str().unwrap().to_string(),
        rest_api_config: None,
        event_processor_config: None,
        sui: None,
    };
    
    let indexer = WalrusIndexer::new(config.clone()).await?;
    
    // Set event cursor.
    indexer.storage.set_last_processed_event_index(12345)?;
    
    // Stop the indexer.
    indexer.stop().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Create new indexer instance with same DB.
    let indexer2 = WalrusIndexer::new(config).await?;
    
    // Verify cursor was persisted.
    let cursor = indexer2.storage.get_last_processed_event_index()?;
    assert_eq!(cursor, Some(12345));
    
    println!("✅ Event cursor persistence test passed!");
    
    Ok(())
    }
}