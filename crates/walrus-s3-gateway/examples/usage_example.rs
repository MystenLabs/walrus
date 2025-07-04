// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Example script to demonstrate how to integrate the S3 gateway.

use walrus_s3_gateway::{Config, S3GatewayServer};

/// This is an example showing how to set up and use the S3 gateway.
/// 
/// To actually run this, you would need to:
/// 1. Set up a Sui client with proper configuration
/// 2. Create a Walrus client
/// 3. Configure the S3 gateway
/// 4. Start the server
///
/// For now, this serves as documentation of the intended usage.
pub async fn example_usage() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Create configuration
    let mut config = Config::default();
    config.bind_address = "127.0.0.1:8080".parse()?;
    config.access_key = "demo-access-key".to_string();
    config.secret_key = "demo-secret-key".to_string();
    config.region = "us-east-1".to_string();

    // 2. TODO: Create Walrus client
    // This would involve:
    // - Setting up Sui client with RPC endpoint and wallet
    // - Creating Walrus client with proper configuration
    // - Setting up committee refresh and other Walrus-specific settings
    
    println!("S3 Gateway configuration created:");
    println!("  Bind address: {}", config.bind_address);
    println!("  Access key: {}", config.access_key);
    println!("  Region: {}", config.region);
    
    // 3. TODO: Create and start server
    // let walrus_client = create_walrus_client().await?;
    // let server = S3GatewayServer::new(config, walrus_client).await?;
    // server.serve().await?;
    
    println!("S3 Gateway setup complete!");
    println!();
    println!("Once implemented, you can use any S3 client:");
    println!();
    println!("AWS CLI:");
    println!("  aws --endpoint-url http://127.0.0.1:8080 s3 ls");
    println!("  aws --endpoint-url http://127.0.0.1:8080 s3 cp file.txt s3://walrus-bucket/");
    println!();
    println!("Python boto3:");
    println!("  s3 = boto3.client('s3', endpoint_url='http://127.0.0.1:8080', ...)");
    println!("  s3.put_object(Bucket='walrus-bucket', Key='test.txt', Body=b'Hello!')");
    println!();
    println!("JavaScript:");
    println!("  const s3 = new AWS.S3({{ endpoint: 'http://127.0.0.1:8080', ... }});");
    println!("  s3.putObject({{ Bucket: 'walrus-bucket', Key: 'test.txt', Body: 'Hello!' }});");
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    example_usage().await
}
