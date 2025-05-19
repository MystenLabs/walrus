//! Walrus Fan-out Proxy entry point.
use std::{env, fs::canonicalize, net::SocketAddr};

use anyhow::Result;
use axum::{
    Router,
    body::Bytes,
    extract::Query,
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::post,
};
use axum_server::bind;
use clap::{Parser, Subcommand};
use serde::Deserialize;
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
    let args = Args::parse();
    tracing_subscriber::registry()
        // We don't need timestamps in the logs.
        .with(
            tracing_subscriber::fmt::layer()
                .with_file(true)
                .with_line_number(true),
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
            let app = Router::new().route("/upload", post(upload_blob));
            let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
            Ok(bind(addr).serve(app.into_make_service()).await?)
        }
    }
}

#[derive(Deserialize)]
struct Params {
    tx: String,
    blob_id: String,
}

async fn upload_blob(Query(params): Query<Params>, body: Bytes) -> impl IntoResponse {
    // Validate "tx" length and hex-ness
    if params.tx.len() != 32 || !params.tx.chars().all(|c| c.is_ascii_hexdigit()) {
        return (StatusCode::BAD_REQUEST, "Invalid tx parameter").into_response();
    }
    if !params.blob_id.chars().all(|c| c.is_ascii_hexdigit()) {
        return (StatusCode::BAD_REQUEST, "Invalid blob_id parameter").into_response();
    }

    let size: usize = body.as_ref().len();

    let response = serde_json::json!({
        "tx": params.tx,
        "blob_id": params.blob_id,
        "blob_size": size,
    });

    (StatusCode::OK, Json(response)).into_response()
}
