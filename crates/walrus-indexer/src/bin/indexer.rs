// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Walrus Indexer Binary
//!
//! This binary runs the Walrus indexer service (Octopus Index) that processes Sui events
//! and maintains the bucket-based blob indexing system.

use std::{net::SocketAddr, process::ExitCode};

use anyhow::Result;
use clap::Parser;
use tokio::runtime::Runtime;
use tracing::{info, warn};
use walrus_indexer::{
    IndexerConfig,
    RestApiConfig,
    WalrusIndexer,
    default,
};

#[derive(Parser, Debug)]
#[command(name = "walrus-indexer")]
#[command(about = "Walrus Indexer Service (Octopus Index)", version)]
pub struct IndexerArgs {
    /// Path to the configuration file (YAML format).
    #[arg(short = 'c', long)]
    config: Option<std::path::PathBuf>,
    
    /// Path to the database directory (overrides config file).
    #[arg(long)]
    db_path: Option<String>,

    /// API server bind address (overrides config file).
    #[arg(long)]
    bind_address: Option<SocketAddr>,

    /// Socket address on which the Prometheus server should export its metrics
    /// (overrides config file).
    #[arg(short = 'a', long)]
    metrics_address: Option<SocketAddr>,

    /// Enable detailed logging.
    #[arg(long)]
    verbose: bool,
}

/// Runtime structure similar to StorageNodeRuntime.
struct IndexerRuntime {
    indexer_task_handle: Option<tokio::task::JoinHandle<Result<()>>>,
    cancellation_token: tokio_util::sync::CancellationToken,
    runtime: Runtime,
}

impl IndexerRuntime {
    fn start(args: IndexerArgs) -> Result<Self> {
        // Initialize tracing
        if std::env::var("RUST_LOG").is_err() {
            let log_level = if args.verbose { "debug" } else { "info" };
            std::env::set_var("RUST_LOG", log_level);
        }
        tracing_subscriber::fmt::init();

        info!("🐙 Starting Walrus Indexer (Octopus Index)");
        
        // Load configuration
        let mut config = if let Some(config_path) = &args.config {
            info!("Loading configuration from: {:?}", config_path);
            walrus_utils::load_from_yaml(config_path)?
        } else {
            info!("No config file provided, using defaults");
            IndexerConfig::default()
        };
        
        // Apply command-line overrides
        if let Some(db_path) = args.db_path {
            config.db_path = db_path;
        }
        if let Some(bind_address) = args.bind_address {
            if let Some(ref mut rest_config) = config.rest_api_config {
                rest_config.bind_address = bind_address;
            } else {
                config.rest_api_config = Some(RestApiConfig {
                    bind_address,
                    metrics_address: args.metrics_address.unwrap_or(default::metrics_address()),
                });
            }
        }
        if let Some(metrics_address) = args.metrics_address {
            if let Some(ref mut rest_config) = config.rest_api_config {
                rest_config.metrics_address = metrics_address;
            }
        }
        
        info!("Database path: {}", config.db_path);
        if let Some(ref rest_config) = config.rest_api_config {
            info!("API bind address: {}", rest_config.bind_address);
            info!("Metrics address: {}", rest_config.metrics_address);
        }

        // Create the tokio runtime
        let runtime = Runtime::new()?;

        // Create cancellation token for shutdown coordination
        let cancellation_token = tokio_util::sync::CancellationToken::new();
        let cancel_token_clone = cancellation_token.clone();
        
        // Initialize and start the indexer
        let indexer_task_handle = runtime.block_on(async {
            // Check if event processing is enabled
            let event_processing_enabled = config.event_processor_config.is_some()
                && config.sui.is_some();

            // Initialize the indexer with all components
            info!("Initializing indexer components");
            let indexer = WalrusIndexer::new(config.clone()).await?;

            if let Some(ref rest_config) = config.rest_api_config {
                info!("🚀 REST API will start on {}", rest_config.bind_address);
            }
            
            if event_processing_enabled {
                info!("✅ Event processing enabled - will listen for Sui events");
            } else {
                info!("⚠️  Event processing disabled - REST API only mode");
                if config.sui.is_none() {
                    info!("    To enable event processing, add 'sui' section to config file");
                }
                if config.event_processor_config.is_none() {
                    info!(
                        "    To enable event processing, add 'event_processor_config' section to config file"
                    );
                }
            }
            
            info!("📚 Available endpoints:");
            info!("  GET  /health - Health check");
            info!("  GET  /get_blob - Get blob by primary key");
            info!("  GET  /get_blob_by_object_id - Get blob by object ID");
            info!("  GET  /list_bucket - List all blobs in bucket");
            info!("  GET  /get_bucket_stats - Get bucket statistics");
            info!("  GET  /buckets - List all buckets");
            info!("  POST /bucket - Create new bucket");

            // Spawn the indexer's run() method in a separate task
            let handle = tokio::spawn(async move {
                info!("Starting indexer run loop");
                indexer.run(cancel_token_clone).await
            });
            
            Ok::<_, anyhow::Error>(Some(handle))
        })?;

        Ok(Self {
            indexer_task_handle,
            cancellation_token,
            runtime,
        })
    }

    fn join(&mut self) -> Result<()> {
        // Wait for the indexer task to complete
        if let Some(handle) = self.indexer_task_handle.take() {
            self.runtime.block_on(async {
                match handle.await {
                    Ok(Ok(())) => info!("Indexer task completed successfully"),
                    Ok(Err(e)) => warn!("Indexer task error: {}", e),
                    Err(e) => warn!("Failed to join indexer task: {}", e),
                }
            });
        }

        Ok(())
    }
}

fn indexer() -> Result<()> {
    // Parse command line arguments
    let args = IndexerArgs::parse();

    // Create and run the indexer runtime
    let mut runtime = IndexerRuntime::start(args)?;

    // Setup shutdown signal handler
    runtime.runtime.block_on(async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install CTRL+C signal handler");
        info!("Received shutdown signal");
        runtime.cancellation_token.cancel();
    });

    // Join all tasks
    runtime.join()?;

    info!("Indexer shutdown complete");
    Ok(())
}

/// The CLI entrypoint.
pub fn main() -> ExitCode {
    if let Err(err) = indexer() {
        eprintln!("Error: {:#}", err);
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}