//! Walrus Fan-out Proxy entry point.
use std::{env, fs::canonicalize, net::SocketAddr};

use anyhow::Result;
use axum::{Router, routing::get};
use clap::{Parser, Subcommand};
use tokio::net::TcpListener;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt}; //prelude::*;

// This is an important refactoring.
#[derive(Parser)]
#[command(author, version, about)]
struct Args {
    /// Subcommand to run.
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Runt the Walrus Fan-out Proxy.
    Proxy,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Run from the git root directory.
    let args = Args::parse();
    tracing_subscriber::registry()
        // We don't need timestamps in the logs.
        .with(
            tracing_subscriber::fmt::layer()
                .with_file(true)
                .with_line_number(true)
                .without_time(),
        )
        // Allow usage of RUST_LOG environment variable to set the log level.
        .with(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();

    match args.command {
        Command::Proxy => {
            // Run the proxy.
            Ok(run_proxy().await?)
        }
    }
}

async fn run_proxy() -> anyhow::Result<()> {
    // pass incoming GET requests on "/hello-world" to "hello_world" handler.
    let app = Router::new().route("/hello-world", get(hello_world));

    // write address like this to not make typos
    let addr = SocketAddr::from(([0, 0, 0, 0], 3030));
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(?addr, "Listening...");
    axum::serve(listener, app.into_make_service()).await?;

    Ok(())
}

async fn hello_world() -> &'static str {
    "Hello, world!"
}
