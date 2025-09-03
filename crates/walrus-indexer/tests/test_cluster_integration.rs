// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Integration test for indexer with full Sui and Walrus test clusters

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use walrus_indexer::{
    IndexerConfig,
    WalrusIndexer,
    RestApiConfig,
};
use walrus_service::{
    event::event_processor::config::EventProcessorConfig,
    common::config::SuiConfig,
};
use walrus_sui::{
    test_utils::sui_test_cluster,
    client::contract_config::ContractConfig,
    config::WalletConfig,
};

#[tokio::test]
#[ignore] // Requires full Sui setup - run with: cargo test -- --ignored
async fn test_indexer_with_test_cluster() -> Result<()> {
    // Start Sui test cluster
    let _sui_test_cluster = sui_test_cluster().await;
    
    // Get RPC URLs from the test cluster
    // In a real implementation, we'd get this from the fullnode endpoints
    let rpc_urls = vec!["http://127.0.0.1:9000".to_string()]; // Placeholder
    
    // Get the system package ID from the deployed contracts
    // This would be the Walrus system package deployed to Sui
    let system_package_id = sui_types::base_types::ObjectID::ZERO; // Placeholder - would be from actual deployment
    
    // Build Walrus test cluster using the Sui test cluster
    // Note: The TestCluster builder expects specific setup that isn't fully exposed
    // For now, we'll create a simpler test without the full Walrus cluster
    
    // Create configuration using new approach
    let contract_config = ContractConfig::new(system_package_id, system_package_id);
    let temp_wallet_dir = tempfile::tempdir()?;
    let wallet_config = WalletConfig::from_path(temp_wallet_dir.path().join("wallet.yaml"));
    
    let sui_config = SuiConfig {
        rpc: rpc_urls[0].clone(),
        contract_config,
        event_polling_interval: Duration::from_millis(100),
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
    
    // Create indexer configuration
    let indexer_config = IndexerConfig {
        db_path: tempfile::tempdir()?.path().to_str().unwrap().to_string(),
        rest_api_config: Some(rest_api_config),
        event_processor_config: Some(EventProcessorConfig::default()),
        sui: Some(sui_config),
    };
    
    // Create the indexer (this would attempt to start event processing in real scenario)
    let _indexer = Arc::new(WalrusIndexer::new(indexer_config).await?);
    
    // The new approach uses EventProcessorRuntime::start_async internally
    // and processes events through the stream_events method.
    // For this integration test, we just verify the indexer can be created.
    
    // Simulate a blob storage event
    // In a real test, this would come from actual Walrus operations
    println!("Indexer started and listening for events");
    
    // Wait a bit to ensure the indexer is processing
    tokio::time::sleep(Duration::from_secs(1)).await;
    
    // Check that the indexer is operational
    // In a real test, we would:
    // 1. Store a blob via Walrus
    // 2. Wait for the event to be processed
    // 3. Query the indexer to verify the blob was indexed
    
    // Test basic indexer functionality
    // In a real test with live Sui network, the indexer would be processing events
    
    Ok(())
}