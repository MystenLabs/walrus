//! Walrus Fan-out Proxy entry point.
use std::{env, net::SocketAddr, num::NonZeroU16, path::PathBuf};

use anyhow::Result;
use axum::{
    Router,
    body::Bytes,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::post,
};
use axum_server::bind;
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::time::Instant;
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt as _, util::SubscriberInitExt};
use walrus_core::{
    BlobId,
    BlobIdParseError,
    EncodingType,
    encoding::{DataTooLargeError, EncodingConfig, EncodingConfigTrait as _, SliverPair},
    metadata::BlobMetadataWithId,
};
use walrus_sdk::client::Client;
use walrus_sui::client::{SuiContractClient, contract_config::ContractConfig};

use crate::error::FanOutError;

mod error;

// TODO: Pull this from the active committee on chain.
const N_SHARDS: NonZeroU16 = NonZeroU16::new(1000).expect("N_SHARDS must be non-zero");

#[derive(Parser)]
#[command(author, version, about)]
struct Args {
    /// Subcommand to run.
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the Walrus Fan-out Proxy.
    Proxy {
        /// The file path of the Fan-out Proxy operator's wallet. This is the account that will be
        /// used to check for fan-out transactions that may have been received.
        wallet_path: PathBuf,
        /// Object ID of the Walrus system object.
        system_object: ObjectID,
        /// Object ID of the Walrus staking object.
        staking_object: ObjectID,
    },
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

#[derive(Clone)]
pub(crate) struct Controller {
    client: Client<SuiContractClient>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    init_logging();
    match args.command {
        Command::Proxy {
            wallet_path,
            system_object,
            staking_object,
        } => {
            let client = get_client(wallet_path, system_object, staking_object).await?;
            let app = Router::new()
                .with_state(Arc::new(Controller { client }))
                .route("/v1/blob-fan-out", post(fan_out_blob_slivers));
            let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
            tracing::info!(?addr, "Serving fan-out proxy");
            Ok(bind(addr).serve(app.into_make_service()).await?)
        }
    }
}

async fn get_client(
    wallet_path: PathBuf,
    system_object: ObjectID,
    staking_object: ObjectID,
) -> Result<Client<SuiContractClient>> {
    let wallet =
        load_wallet_context_from_path(wallet_path, None).context("unable to load wallet")?;
    let contract_config = ContractConfig::new(system_object_id, staking_object_id);

    #[allow(deprecated)]
    let rpc_urls = &[wallet.get_rpc_url()?];

    SuiContractClient::new(wallet, rpc_urls, &contract_config, Default::default(), None).await
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

async fn fan_out_blob_slivers(
    State(controller): State<Arc<Controller>>,
    Query(params): Query<Params>,
    body: Bytes,
) -> Result<impl IntoResponse, FanOutError> {
    tracing::info!(?params, "fan_out_blob_slivers");
    // Validate "tx" length and hex-ness
    if params.tx.len() != 32 || !params.tx.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(FanOutError::BadRequest(format!(
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
        return Err(FanOutError::BadRequest(format!(
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
