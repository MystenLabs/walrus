//! Walrus Fan-out Proxy entry point.
use std::{
    env,
    net::SocketAddr,
    num::NonZeroU16,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use axum::{
    Router,
    body::Bytes,
    debug_handler,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::post,
};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt as _, util::SubscriberInitExt};
use walrus_core::{
    BlobId,
    EncodingType,
    encoding::{EncodingConfig, EncodingConfigTrait as _, SliverPair},
    messages::{BlobPersistenceType, ProtocolMessageCertificate},
    metadata::BlobMetadataWithId,
};
use walrus_sdk::{
    SuiReadClient,
    client::Client,
    config::{ClientConfig, combine_rpc_urls},
    sui::{client::retry_client::RetriableSuiClient, config::WalletConfig, wallet::Wallet},
};

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

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the Walrus Fan-out Proxy.
    Proxy {
        /// The configuration context to use for the client, if omitted the default_context is used.
        #[arg(long, global = true)]
        context: Option<String>,
        /// The file path to the Walrus read client configuration.
        #[arg(long, global = true)]
        config_path: PathBuf,
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

#[allow(dead_code)]
pub(crate) struct Controller {
    pub(crate) client: Client<SuiReadClient>,
}

impl Controller {
    fn new(client: Client<SuiReadClient>) -> Self {
        Self { client }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    init_logging();
    match args.command {
        Command::Proxy {
            context,
            config_path,
        } => {
            let c = context.as_deref();
            let client = get_client(c, config_path.as_path()).await?;
            let app = Router::new()
                .route("/v1/blob-fan-out", post(fan_out_blob_slivers))
                .with_state(Arc::new(Controller::new(client)));

            let addr: SocketAddr = "0.0.0.0:3000".parse().context("invalid address")?;
            let listener = tokio::net::TcpListener::bind(&addr).await?;
            tracing::info!(?addr, "Serving fan-out proxy");
            Ok(axum::serve(listener, app).await?)
        }
    }
}

async fn get_client(context: Option<&str>, config_path: &Path) -> Result<Client<SuiReadClient>> {
    let config: ClientConfig = walrus_sdk::config::load_configuration(Some(config_path), context)?;
    // TODO: allow configuration of wallet.
    let wallet: Wallet = WalletConfig::load_wallet(None, None)?;

    #[allow(deprecated)]
    let rpc_url = wallet.get_rpc_url()?;

    tracing::info!(
        ?wallet,
        rpc_url = rpc_url.as_str(),
        ?config,
        "loaded wallet and client config"
    );

    let retriable_sui_client = RetriableSuiClient::new_for_rpc_urls(
        &combine_rpc_urls(rpc_url, &config.rpc_urls),
        config.backoff_config().clone(),
        None,
    )
    .await?;

    let sui_read_client = config.new_read_client(retriable_sui_client).await?;

    let refresh_handle = config
        .refresh_config
        .build_refresher_and_run(sui_read_client.clone())
        .await?;
    Ok(Client::new_read_client(config, refresh_handle, sui_read_client).await?)
}

#[derive(Debug, Deserialize)]
struct Params {
    tx: String,
    blob_id: String,
}

#[derive(Serialize)]
struct ResponseType<T> {
    blob_size: usize,
    blob_id: String,
    symbol_size: u16,
    n_shards: u16,
    protocol_message_certificate: ProtocolMessageCertificate<T>,
}

#[debug_handler]
async fn fan_out_blob_slivers(
    State(controller): State<Arc<Controller>>,
    Query(params): Query<Params>,
    body: Bytes,
) -> Result<impl IntoResponse, FanOutError> {
    tracing::info!(?params, "fan_out_blob_slivers");

    // TODO: add this parameter to the API?
    let blob_persistence_type: BlobPersistenceType = BlobPersistenceType::Permanent;

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

    let protocol_message_certificate = controller
        .client
        .send_blob_data_and_get_certificate(&metadata, &sliver_pairs, &blob_persistence_type, None)
        .await?;

    // ASSUME None of the slivers have been uploaded yet.
    let response = ResponseType {
        blob_id: blob_id.to_string(),
        blob_size: size,
        symbol_size,
        n_shards: metadata.n_shards().into(),
        protocol_message_certificate,
    };

    Ok((StatusCode::OK, Json(response)).into_response())
}
