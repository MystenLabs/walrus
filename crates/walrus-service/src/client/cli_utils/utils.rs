// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Utilities for running the Walrus client binary.

use std::env;

use anyhow::{anyhow, Result};
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt, EnvFilter, Layer};

use super::{
    args::{Commands, RpcArg},
    App,
};
use crate::{client::cli_utils::runner::ClientCommandRunner, utils::MetricsAndLoggingRuntime};

/// Initializes the logger and tracing subscriber.
pub fn init_tracing_subscriber() -> Result<()> {
    // Use INFO level by default.
    let directive = format!(
        "info,{}",
        env::var(EnvFilter::DEFAULT_ENV).unwrap_or_default()
    );
    let layer = tracing_subscriber::fmt::layer().with_writer(std::io::stderr);

    // Control output format based on `LOG_FORMAT` env variable.
    let format = env::var("LOG_FORMAT").ok();
    let layer = if let Some(format) = &format {
        match format.to_lowercase().as_str() {
            "default" => layer.boxed(),
            "compact" => layer.compact().boxed(),
            "pretty" => layer.pretty().boxed(),
            "json" => layer.json().boxed(),
            s => Err(anyhow!("LOG_FORMAT '{}' is not supported", s))?,
        }
    } else {
        layer.boxed()
    };

    tracing_subscriber::registry()
        .with(layer.with_filter(EnvFilter::new(directive.clone())))
        .init();
    tracing::debug!(%directive, ?format, "initialized tracing subscriber");

    Ok(())
}

/// Runs the binary commands in "cli" mode (i.e., without running a server).
pub async fn run_cli_app(app: App) -> Result<()> {
    let runner = ClientCommandRunner::new(&app.config, &app.wallet, app.gas_budget, app.json);
    match app.command {
        Commands::Read {
            blob_id,
            out,
            rpc_arg: RpcArg { rpc_url },
        } => runner.read(blob_id, out, rpc_url).await,

        Commands::Store {
            file,
            epochs,
            dry_run,
            force,
        } => runner.store(file, epochs, dry_run, force).await,

        Commands::BlobStatus {
            file_or_blob_id,
            timeout,
            rpc_arg: RpcArg { rpc_url },
        } => runner.blob_status(file_or_blob_id, timeout, rpc_url).await,

        Commands::Info {
            rpc_arg: RpcArg { rpc_url },
            dev,
        } => runner.info(rpc_url, dev).await,

        Commands::Json { .. } => {
            unreachable!("we unpack JSON commands until we obtain a different command")
        }

        Commands::BlobId {
            file,
            n_shards,
            rpc_arg: RpcArg { rpc_url },
        } => runner.blob_id(file, n_shards, rpc_url).await,

        Commands::ConvertBlobId { blob_id_decimal } => runner.convert_blob_id(blob_id_decimal),

        Commands::ListBlobs { include_expired } => runner.list_blobs(include_expired).await,

        // We panic as this is an implementation mistake, not a user error.
        _ => panic!("wrong command for CLI mode; use `run_daemon_app` for daemon mode"),
    }
}

/// Runs the binary commands in "daemon" mode (i.e., running a server).
pub async fn run_daemon_app(app: App, metrics_runtime: MetricsAndLoggingRuntime) -> Result<()> {
    let runner = ClientCommandRunner::new(&app.config, &app.wallet, app.gas_budget, app.json);
    match app.command {
        Commands::Publisher { args } => runner.publisher(&metrics_runtime.registry, args).await,

        Commands::Aggregator {
            rpc_arg: RpcArg { rpc_url },
            daemon_args,
        } => {
            runner
                .aggregator(&metrics_runtime.registry, rpc_url, daemon_args)
                .await
        }

        Commands::Daemon { args } => runner.daemon(&metrics_runtime.registry, args).await,

        // We panic as this is an implementation mistake, not a user error.
        _ => panic!("wrong command for daemon mode; use `run_cli_app` for CLI mode"),
    }
}
