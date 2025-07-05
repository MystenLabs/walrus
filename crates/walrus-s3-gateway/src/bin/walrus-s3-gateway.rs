// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Main binary for the Walrus S3 Gateway with Client-Side Signing.

use clap::{Arg, Command};
use std::path::{Path, PathBuf};
use tracing::{error, info};
use walrus_s3_gateway::Config;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing with default info level
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
        )
        .init();

    // Parse command line arguments
    let matches = Command::new("walrus-s3-gateway")
        .version("1.29.0")
        .author("Mysten Labs <build@mystenlabs.com>")
        .about("S3-compatible gateway for Walrus storage with Client-Side Signing")
        .arg(
            Arg::new("config")
                .short('c')
                .long("config")
                .value_name("FILE")
                .help("Configuration file path")
                .value_parser(clap::value_parser!(PathBuf)),
        )
        .arg(
            Arg::new("bind")
                .short('b')
                .long("bind")
                .value_name("ADDRESS")
                .help("Address to bind the server to (overrides config file)"),
        )
        .arg(
            Arg::new("region")
                .long("region")
                .value_name("REGION")
                .help("S3 region")
                .default_value("us-east-1"),
        )
        .arg(
            Arg::new("walrus-config")
                .long("walrus-config")
                .value_name("FILE")
                .help("Walrus client configuration file")
                .value_parser(clap::value_parser!(PathBuf)),
        )
        .arg(
            Arg::new("enable-tls")
                .long("enable-tls")
                .help("Enable TLS/HTTPS")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("tls-cert")
                .long("tls-cert")
                .value_name("FILE")
                .help("TLS certificate file")
                .value_parser(clap::value_parser!(PathBuf)),
        )
        .arg(
            Arg::new("tls-key")
                .long("tls-key")
                .value_name("FILE")
                .help("TLS private key file")
                .value_parser(clap::value_parser!(PathBuf)),
        )
        .get_matches();

    // Load or create configuration
    let mut config = if let Some(config_path) = matches.get_one::<PathBuf>("config") {
        info!("Loading configuration from: {}", config_path.display());
        Config::from_file(config_path)?
    } else {
        // Always look for config.toml relative to this main binary file location
        // Get the directory containing this source file
        let main_file_path = PathBuf::from(file!());
        let gateway_dir = main_file_path
            .parent() // src/bin/
            .and_then(|p| p.parent()) // src/
            .and_then(|p| p.parent()) // crates/walrus-s3-gateway/
            .unwrap_or_else(|| Path::new("."));
        
        let default_config_path = gateway_dir.join("config.toml");
        
        if default_config_path.exists() {
            info!("Loading configuration from default file: {}", default_config_path.display());
            Config::from_file(&default_config_path)?
        } else {
            info!("No configuration file found at: {}", default_config_path.display());
            info!("Create a config.toml file in the walrus-s3-gateway directory to customize settings");
            Config::default()
        }
    };

    // Override config with command line arguments
    if let Some(bind_addr) = matches.get_one::<String>("bind") {
        config.bind_address = bind_addr.parse()?;
    }

    if let Some(region) = matches.get_one::<String>("region") {
        config.region = region.clone();
    }

    if let Some(walrus_config) = matches.get_one::<PathBuf>("walrus-config") {
        config.walrus_config_path = Some(walrus_config.clone());
    }

    if matches.get_flag("enable-tls") {
        config.enable_tls = true;
    }

    if let Some(tls_cert) = matches.get_one::<PathBuf>("tls-cert") {
        config.tls_cert_path = Some(tls_cert.clone());
    }

    if let Some(tls_key) = matches.get_one::<PathBuf>("tls-key") {
        config.tls_key_path = Some(tls_key.clone());
    }

    // Validate configuration
    if let Err(e) = config.validate() {
        error!("Configuration validation failed: {}", e);
        std::process::exit(1);
    }

    info!("Configuration:");
    info!("  Bind address: {}", config.bind_address);
    info!("  Access key: {}", config.access_key);
    info!("  Region: {}", config.region);
    info!("  TLS enabled: {}", config.enable_tls);
    if let Some(ref walrus_config) = config.walrus_config_path {
        info!("  Walrus config: {}", walrus_config.display());
    }

    // Create Walrus client
    info!("Creating Walrus client...");
    
    let walrus_client = match walrus_s3_gateway::server::create_walrus_client(&config).await {
        Ok(client) => client,
        Err(e) => {
            error!("Failed to create Walrus client: {}", e);
            error!("Make sure you have:");
            error!("1. Configured the Sui client with proper RPC endpoints");
            error!("2. Network connectivity to Sui testnet");
            error!("3. Valid Walrus contract object IDs");
            std::process::exit(1);
        }
    };

    // Create and start the server
    info!("Starting S3 gateway server...");
    let server = match walrus_s3_gateway::server::S3GatewayServer::new(config, walrus_client).await {
        Ok(server) => server,
        Err(e) => {
            error!("Failed to create server: {}", e);
            std::process::exit(1);
        }
    };
    
    if let Err(e) = server.serve().await {
        error!("Server error: {}", e);
        std::process::exit(1);
    }

    Ok(())
}
