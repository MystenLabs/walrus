// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Walrus Indexer Binary
//!
//! This binary runs the Walrus indexer service that processes Sui events
//! and maintains the bucket-based blob indexing system.

use std::{path::PathBuf, process::ExitCode};

use anyhow::Result;
use clap::Parser;
use tokio::runtime::Runtime;
use walrus_indexer::{IndexerConfig, WalrusIndexer};
use walrus_service::utils::MetricsAndLoggingRuntime;

#[derive(Parser, Debug)]
#[command(name = "walrus-indexer")]
#[command(about = "Walrus Indexer Service", version)]
pub struct IndexerArgs {
    /// Path to the configuration file (YAML format).
    #[arg(short = 'c', long)]
    config: Option<std::path::PathBuf>,

    /// Path to the database directory (overrides config file).
    #[arg(long)]
    db_path: Option<PathBuf>,

    /// Enable detailed logging.
    #[arg(long)]
    verbose: bool,
}

/// Runtime structure similar to StorageNodeRuntime.
struct IndexerRuntime {
    indexer_task_handle: Option<tokio::task::JoinHandle<Result<()>>>,
    cancellation_token: tokio_util::sync::CancellationToken,
    runtime: Runtime,
    // Keep metrics runtime alive for the lifetime of the program
    #[allow(dead_code)]
    metrics_runtime: MetricsAndLoggingRuntime,
}

impl IndexerRuntime {
    fn start(args: IndexerArgs) -> Result<Self> {
        // Initialize tracing (already handled by MetricsAndLoggingRuntime)
        if std::env::var("RUST_LOG").is_err() {
            let log_level = if args.verbose { "debug" } else { "info" };
            std::env::set_var("RUST_LOG", log_level);
        }

        tracing::info!("🚀 Starting Walrus Indexer");

        // Load configuration
        let mut config = if let Some(config_path) = &args.config {
            tracing::info!("Loading configuration from: {:?}", config_path);
            walrus_utils::load_from_yaml(config_path)?
        } else {
            tracing::info!("No config file provided, using defaults");
            IndexerConfig::default()
        };

        // Apply command-line overrides
        if let Some(db_path) = args.db_path {
            config.db_path = db_path;
        }

        tracing::info!("Database path: {}", config.db_path.display());

        // Initialize metrics and logging runtime with the configured address
        let metrics_runtime = MetricsAndLoggingRuntime::start(config.metrics_address)?;

        // Create the tokio runtime
        let runtime = Runtime::new()?;

        // Create cancellation token for shutdown coordination
        let cancellation_token = tokio_util::sync::CancellationToken::new();
        let cancel_token_clone = cancellation_token.clone();

        // Get the registry from metrics runtime
        let registry = &metrics_runtime.registry;
        let registry_clone = registry.clone();

        // Initialize and start the indexer
        let indexer_task_handle = runtime.block_on(async {
            // Check if event processing is enabled
            let event_processing_enabled = config.event_processor_config.is_some();

            // Initialize the indexer with all components
            tracing::info!("Initializing indexer components");
            let indexer = WalrusIndexer::new(config.clone()).await?;

            if event_processing_enabled {
                tracing::info!("✅ Event processing enabled - will listen for Sui events");
            } else {
                tracing::info!("⚠️  Event processing disabled - operating as key-value store");
            }

            // Spawn the indexer's run() method in a separate task
            let handle = tokio::spawn(async move {
                tracing::info!("Starting indexer run loop");
                indexer.run(&registry_clone, cancel_token_clone).await
            });

            Ok::<_, anyhow::Error>(Some(handle))
        })?;

        Ok(Self {
            indexer_task_handle,
            cancellation_token,
            runtime,
            metrics_runtime,
        })
    }

    fn join(&mut self) -> Result<()> {
        // Wait for the indexer task to complete
        if let Some(handle) = self.indexer_task_handle.take() {
            self.runtime.block_on(async {
                match handle.await {
                    Ok(Ok(())) => tracing::info!("Indexer task completed successfully"),
                    Ok(Err(e)) => tracing::warn!("Indexer task error: {}", e),
                    Err(e) => tracing::warn!("Failed to join indexer task: {}", e),
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
        tracing::info!("Received shutdown signal");
        runtime.cancellation_token.cancel();
    });

    // Join all tasks
    runtime.join()?;

    tracing::info!("Indexer shutdown complete");
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
