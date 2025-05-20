//! Walrus Fan-out Proxy entry point.
use std::{env, net::SocketAddr, num::NonZeroU16};

use anyhow::Result;
use axum::{
    Router,
    body::Bytes,
    extract::Query,
    http::StatusCode,
    response::{IntoResponse, Json, Response},
    routing::post,
};
use axum_server::bind;
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt as _, util::SubscriberInitExt};
use walrus_core::{
    BlobId,
    BlobIdParseError,
    EncodingType,
    encoding::{DataTooLargeError, EncodingConfig, EncodingConfigTrait as _, SliverPair},
    metadata::BlobMetadataWithId,
};

// TODO: Pull this from the active committee on chain.
const N_SHARDS: NonZeroU16 = NonZeroU16::new(1000).expect("N_SHARDS must be non-zero");

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

fn init_logging() {
    // Use INFO level by default.
    let directive = format!(
        "info,{}",
        env::var(EnvFilter::DEFAULT_ENV).unwrap_or_default()
    );
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::stderr)
                .boxed()
                .with_filter(EnvFilter::new(directive.clone())),
        )
        .init();
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    init_logging();

    match args.command {
        Command::Proxy => {
            let app = Router::new().route("/v1/blob-fan-out", post(fan_out_blob_slivers));
            let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
            tracing::info!(?addr, "Serving fan-out proxy");
            Ok(bind(addr).serve(app.into_make_service()).await?)
        }
    }
}

#[derive(Debug, Deserialize)]
struct Params {
    tx: String,
    blob_id: String,
}

#[derive(Serialize)]
struct ResponseType {
    blob_size: usize,
    blob_id: String,
    symbol_size: u16,
    n_shards: u16,
}

use thiserror::Error;

/// Fan-out Proxy Errors
#[derive(Debug, Error)]
pub enum Error {
    /// Invalid input error.
    #[error("Bad input: {0}")]
    BadRequest(String),

    /// Blob is too large error.
    #[error(transparent)]
    DataTooLargeError(#[from] DataTooLargeError),

    /// Invalid BlobId error.
    #[error(transparent)]
    BlobIdParseError(#[from] BlobIdParseError),

    /// Internal server error.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        match self {
            Error::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg).into_response(),
            Error::DataTooLargeError(error) => {
                (StatusCode::BAD_REQUEST, error.to_string()).into_response()
            }
            Error::BlobIdParseError(error) => {
                (StatusCode::BAD_REQUEST, error.to_string()).into_response()
            }
            Error::Other(error) => {
                (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response()
            }
        }
    }
}

async fn fan_out_blob_slivers(
    Query(params): Query<Params>,
    body: Bytes,
) -> Result<impl IntoResponse, Error> {
    tracing::info!(?params, "fan_out_blob_slivers");
    // Validate "tx" length and hex-ness
    if params.tx.len() != 32 || !params.tx.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(Error::BadRequest(format!(
            "Invalid tx parameter [tx={}]",
            params.tx
        )));
    }
    let blob_id: BlobId = params.blob_id.parse()?;
    let blob = body.as_ref();
    let size: usize = blob.len();

    let encoding_type: EncodingType = EncodingType::RS2;
    let encoding_config = EncodingConfig::new(N_SHARDS);
    let encode_start_timer = Instant::now();
    let (sliver_pairs, metadata): (Vec<SliverPair>, BlobMetadataWithId<true>) = encoding_config
        .get_for_type(encoding_type)
        .encode_with_metadata(blob)?;
    let duration = encode_start_timer.elapsed();
    if *metadata.blob_id() != blob_id {
        return Err(Error::BadRequest(format!(
            "Blob ID mismatch [expected={}, actual={}]",
            blob_id,
            metadata.blob_id()
        )));
    }

    let pair = sliver_pairs
        .first()
        .expect("the encoding produces sliver pairs");
    let symbol_size = pair.primary.symbols.symbol_size().get();

    tracing::info!(
        symbol_size,
        primary_sliver_size = pair.primary.symbols.len() * usize::from(symbol_size),
        secondary_sliver_size = pair.secondary.symbols.len() * usize::from(symbol_size),
        ?duration,
        "encoded sliver pairs and metadata"
    );
    let response = ResponseType {
        blob_id: blob_id.to_string(),
        blob_size: size,
        symbol_size,
        n_shards: metadata.n_shards().into(),
    };

    Ok((StatusCode::OK, Json(response)).into_response())
}
