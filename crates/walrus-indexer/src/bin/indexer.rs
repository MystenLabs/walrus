// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Walrus Indexer Binary
//!
//! This binary runs the Walrus indexer service (Octopus Index) that processes Sui events
//! and maintains the bucket-based blob indexing system.

use std::{net::SocketAddr, process::ExitCode, sync::Arc};

use anyhow::Result;
use clap::Parser;
use tracing::info;
use walrus_indexer::{
    IndexerConfig,
    WalrusIndexer,
    routes::{IndexerState, create_indexer_router},
};

#[derive(Parser, Debug)]
#[command(name = "walrus-indexer")]
#[command(about = "Walrus Indexer Service (Octopus Index)", version)]
pub struct IndexerArgs {
    /// Path to the database directory
    #[arg(long, default_value = "./indexer-db")]
    db_path: String,

    /// Sui RPC URL for event processing
    #[arg(long, default_value = "https://fullnode.devnet.sui.io:443")]
    sui_rpc_url: String,

    /// API server bind address
    #[arg(long, default_value = "0.0.0.0:8080")]
    bind_address: SocketAddr,

    /// Socket address on which the Prometheus server should export its metrics
    #[arg(short = 'a', long, default_value = "0.0.0.0:9184")]
    metrics_address: SocketAddr,

    /// Enable detailed logging
    #[arg(long)]
    verbose: bool,
}

/// Walrus Indexer Daemon
///
/// Exposes HTTP endpoints for querying the Octopus Index.
pub struct IndexerDaemon {
    indexer: WalrusIndexer,
    bind_address: SocketAddr,
    metrics_address: SocketAddr,
    db_path: String,
}

impl IndexerDaemon {
    /// Creates a new indexer daemon.
    pub async fn new(args: IndexerArgs) -> Result<Self> {
        // Create indexer configuration
        let config = IndexerConfig {
            db_path: args.db_path.clone(),
            sui_rpc_url: args.sui_rpc_url,
            use_buckets: true, // Always use buckets for Octopus Index
            api_port: args.bind_address.port(),
            event_processor_config: None, // TODO: Add CLI args for event processor configuration
        };

        // Initialize the indexer
        let indexer = WalrusIndexer::new(config).await?;

        Ok(Self {
            indexer,
            bind_address: args.bind_address,
            metrics_address: args.metrics_address,
            db_path: args.db_path,
        })
    }

    /// Runs the indexer daemon.
    pub async fn run(self) -> Result<()> {
        info!("🐙 Starting Walrus Indexer (Octopus Index)");
        info!("Database path: {}", self.db_path);
        info!("API bind address: {}", self.bind_address);
        info!("Metrics address: {}", self.metrics_address);

        // Create an Arc of the indexer for sharing
        let indexer = Arc::new(self.indexer);

        // Start the event processor in the background
        // This spawns a background task that continuously processes Sui events
        indexer.clone().start_event_processor();

        // Create indexer state for the API (using another clone of the Arc)
        let indexer_state =
            IndexerState::new(Arc::try_unwrap(indexer).unwrap_or_else(|arc| (*arc).clone()));

        // Create the API router
        let app = create_indexer_router(indexer_state);

        info!(
            "🚀 Starting Octopus Index API server on {}",
            self.bind_address
        );
        info!("📚 Available endpoints:");
        info!("  GET  /v1/health - Health check");
        info!("  GET  /v1/blobs/{{bucket_id}}/{{primary_key}} - Get blob by primary key");
        info!("  GET  /v1/bucket/{{bucket_id}} - List all blobs in bucket");
        info!("  GET  /v1/bucket/{{bucket_id}}/{{prefix}} - List blobs with prefix");
        info!("  GET  /v1/bucket/{{bucket_id}}/stats - Get bucket statistics");

        let listener = tokio::net::TcpListener::bind(self.bind_address).await?;
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = tokio::signal::ctrl_c().await;
            })
            .await?;

        Ok(())
    }
}

fn indexer() -> Result<()> {
    // Initialize tracing
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    tracing_subscriber::fmt::init();

    // Parse command line arguments
    let args = IndexerArgs::parse();

    // Create and run the daemon
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let daemon = IndexerDaemon::new(args).await?;
        daemon.run().await
    })
}

/// The CLI entrypoint.
pub fn main() -> ExitCode {
    if let Err(err) = indexer() {
        eprintln!("Error: {:#}", err);
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
