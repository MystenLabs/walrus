// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Example demonstrating how to use the async blob upload feature.
//!
//! This example shows how to use the async upload functionality that returns
//! immediately after quorum (2f+1 shards) is reached and continues uploading
//! to additional nodes in the background for better resilience.

use std::path::Path;
use walrus_sdk::{
    client::WalrusNodeClient,
    config::{ClientConfig, load_configuration},
    sui::client::SuiContractClient,
};

/// Example of using async upload with a cloneable client type.
///
/// Note: This example uses a mock client type that implements Clone.
/// The real `SuiContractClient` doesn't implement Clone, so this is
/// for demonstration purposes only.
pub async fn async_upload_example() -> anyhow::Result<()> {
    // Load the client configuration
    let config_path = Path::new("working_dir/client_config.yaml");
    let config: ClientConfig = load_configuration(config_path)?;

    println!("Async upload enabled: {}", config.async_upload.enabled);

    if !config.async_upload.enabled {
        println!("Async upload is not enabled in the configuration.");
        println!("To enable it, set async_upload.enabled to true in your client config.");
        return Ok(());
    }

    // For this example, we would need a cloneable client type
    // The real implementation would need to make SuiContractClient cloneable
    // or use a different approach

    println!("Async upload configuration:");
    println!("  Max concurrent tasks: {}", config.async_upload.max_concurrent_tasks);
    println!("  Task timeout: {}s", config.async_upload.task_timeout.as_secs());
    println!("  Wait on shutdown: {}", config.async_upload.wait_on_shutdown);

    // Example of how async upload would work:
    // 1. Call send_blob_data_and_get_certificate_async() instead of the regular version
    // 2. The function returns immediately after 2f+1 shards confirm the blob
    // 3. Additional uploads continue in the background for better data durability
    // 4. Background uploads are logged for monitoring

    println!("\nAsync upload workflow:");
    println!("1. Upload starts to all storage nodes");
    println!("2. Returns certificate immediately after 2f+1 shards confirm (quorum reached)");
    println!("3. Background task continues uploading to remaining nodes");
    println!("4. Background uploads complete within the configured timeout");
    println!("5. Logs show the final status of all uploads");

    Ok(())
}

/// Example configuration for testing async upload
pub fn create_test_async_config() -> walrus_sdk::config::AsyncUploadConfig {
    walrus_sdk::config::AsyncUploadConfig {
        enabled: true,
        max_concurrent_tasks: 5,
        task_timeout: std::time::Duration::from_secs(180), // 3 minutes
        wait_on_shutdown: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_async_upload_config() {
        let config = create_test_async_config();
        assert!(config.enabled);
        assert_eq!(config.max_concurrent_tasks, 5);
        assert_eq!(config.task_timeout.as_secs(), 180);
        assert!(config.wait_on_shutdown);
    }
}

fn main() -> anyhow::Result<()> {
    println!("Walrus Async Upload Example");
    println!("============================");
    println!();

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async_upload_example())?;

    println!("\nTo test async upload:");
    println!("1. Set async_upload.enabled = true in your client config");
    println!("2. Use a client type that implements Clone + Send + Sync + 'static");
    println!("3. Call send_blob_data_and_get_certificate_async() on the client");
    println!("4. Monitor logs for 'Using async upload' and background completion messages");

    Ok(())
}
