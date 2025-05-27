// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

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
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::post,
};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use tip::{TipChecker, TipConfig, TipKind};
use tokio::time::Instant;
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt as _, util::SubscriberInitExt};
use walrus_core::{
    BlobId,
    EncodingType,
    encoding::{EncodingConfig, EncodingConfigTrait as _, SliverPair},
    messages::{BlobPersistenceType, ConfirmationCertificate},
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
mod tip;

const DEFAULT_SERVER_ADDRESS: &'static str = "0.0.0.0:57391";

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
        /// The address to listen on. Defaults to 0.0.0.0:57391.
        #[arg(long, global = true)]
        server_address: Option<SocketAddr>,
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
    pub(crate) encoding_config: Arc<EncodingConfig>,
    pub(crate) checker: TipChecker,
}

impl Controller {
    fn new(
        client: Client<SuiReadClient>,
        encoding_config: Arc<EncodingConfig>,
        checker: TipChecker,
    ) -> Self {
        Self {
            client,
            encoding_config,
            checker,
        }
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
            server_address,
        } => {
            // Create a client we can use to communicate with the Sui network, which is used to
            // coordinate the Walrus network.
            let client = get_client(context.as_deref(), config_path.as_path()).await?;

            let n_shards = client.get_committees().await?.n_shards();
            let encoding_config = Arc::new(EncodingConfig::new(n_shards));
            let checker = TipChecker::new(
                TipConfig::NoTip,                         // TODO: configurable.
                client.sui_client().sui_client().clone(), // TODO: lol this naming?
                n_shards,
            );

            // Build our HTTP application to handle the blob fan-out operations.
            let app = Router::new()
                .route("/v1/blob-fan-out", post(fan_out_blob_slivers))
                .with_state(Arc::new(Controller::new(client, encoding_config, checker)));

            let addr: SocketAddr = if let Some(socket_addr) = server_address {
                socket_addr
            } else {
                DEFAULT_SERVER_ADDRESS.parse().context("invalid address")?
            };

            let listener = tokio::net::TcpListener::bind(&addr).await?;
            tracing::info!(?addr, n_shards, "Serving fan-out proxy");
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

    tracing::debug!(
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
    blob_id: String,
}

#[derive(Serialize)]
struct ResponseType {
    blob_id: String,
    confirmation_certificate: ConfirmationCertificate,
}

async fn fan_out_blob_slivers(
    State(controller): State<Arc<Controller>>,
    Query(params): Query<Params>,
    body: Bytes,
) -> Result<impl IntoResponse, FanOutError> {
    tracing::info!(?params, "fan_out_blob_slivers");

    // TODO: add this parameter to the API?
    // Not necessary.
    let blob_persistence_type: BlobPersistenceType = BlobPersistenceType::Permanent;

    let blob_id: BlobId = params.blob_id.parse()?;
    let blob = body.as_ref();
    let encoding_type: EncodingType = EncodingType::RS2;

    let encode_start_timer = Instant::now();
    let (sliver_pairs, metadata): (Vec<SliverPair>, BlobMetadataWithId<true>) = controller
        .encoding_config
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

    // Attempt to upload the slivers.
    let confirmation_certificate: ConfirmationCertificate = controller
        .client
        .send_blob_data_and_get_certificate(&metadata, &sliver_pairs, &blob_persistence_type, None)
        .await?;

    // Reply with the confirmation certificate.
    let response = ResponseType {
        blob_id: blob_id.to_string(),
        confirmation_certificate,
    };

    Ok((StatusCode::OK, Json(response)).into_response())
}
