// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Walrus Fan-out Proxy entry point.

use std::{
    env,
    net::SocketAddr,
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
use fastcrypto::encoding::Base64;
use params::Params;
use serde::Serialize;
use tip::{TipChecker, TipConfig};
use tokio::time::Instant;
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt as _, util::SubscriberInitExt};
// use utoipa::IntoParams;
use walrus_core::{
    BlobId,
    encoding::{EncodingConfig, EncodingConfigTrait as _, SliverPair},
    messages::{BlobPersistenceType, ConfirmationCertificate},
    metadata::BlobMetadataWithId,
};
use walrus_sdk::{
    SuiReadClient,
    client::Client,
    config::ClientConfig,
    core_utils::load_from_yaml,
    sui::client::retry_client::RetriableSuiClient,
};

use crate::error::FanOutError;

mod error;
mod params;
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
        walrus_config: PathBuf,
        /// The address to listen on. Defaults to 0.0.0.0:57391.
        #[arg(long, global = true)]
        server_address: Option<SocketAddr>,
        #[arg(long, global = true)]
        tip_config: PathBuf,
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
            walrus_config,
            server_address,
            tip_config,
        } => {
            // Create a client we can use to communicate with the Sui network, which is used to
            // coordinate the Walrus network.
            let client = get_client(context.as_deref(), walrus_config.as_path()).await?;

            let n_shards = client.get_committees().await?.n_shards();
            let encoding_config = Arc::new(EncodingConfig::new(n_shards));
            let tip_config: TipConfig = load_from_yaml(tip_config)?;
            let checker = TipChecker::new(
                tip_config,
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

async fn get_client(context: Option<&str>, walrus_config: &Path) -> Result<Client<SuiReadClient>> {
    let config: ClientConfig =
        walrus_sdk::config::load_configuration(Some(walrus_config), context)?;
    tracing::debug!(?config, "loaded client config");

    let retriable_sui_client = RetriableSuiClient::new_for_rpc_urls(
        &config.rpc_urls,
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

/// The response of the fanout proxy, containing the blob ID and the corresponding certificate.
#[derive(Serialize)]
struct ResponseType {
    blob_id: BlobId,
    confirmation_certificate: ConfirmationCertificate,
}

async fn fan_out_blob_slivers(
    State(controller): State<Arc<Controller>>,
    Query(params): Query<Params>,
    body: Bytes,
) -> Result<impl IntoResponse, FanOutError> {
    tracing::info!(?params, "fan_out_blob_slivers");

    let Params {
        blob_id,
        tx_bytes,
        signature,
    } = params;

    let registration = controller
        .checker
        .execute_and_check_transaction(
            // Convert to the non-URL encoded version of the bytes, which is required by the
            // API endpoint.
            Base64::from_bytes(tx_bytes.bytes()),
            vec![Base64::from_bytes(signature.bytes())],
            blob_id,
        )
        .await?;

    let encode_start_timer = Instant::now();
    // TODO: encoding should probably be done on a separate thread pool.
    let (sliver_pairs, metadata): (Vec<SliverPair>, BlobMetadataWithId<true>) = controller
        .encoding_config
        .get_for_type(registration.encoding_type)
        .encode_with_metadata(body.as_ref())?;
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
        primary_sliver_size = pair.primary.symbols.data().len(),
        secondary_sliver_size = pair.secondary.symbols.data().len(),
        ?duration,
        "encoded sliver pairs and metadata"
    );

    // Attempt to upload the slivers.
    let blob_persistence = if registration.deletable {
        BlobPersistenceType::Deletable {
            object_id: registration.object_id.into(),
        }
    } else {
        BlobPersistenceType::Permanent
    };
    let confirmation_certificate: ConfirmationCertificate = controller
        .client
        .send_blob_data_and_get_certificate(&metadata, &sliver_pairs, &blob_persistence, None)
        .await?;

    // Reply with the confirmation certificate.
    let response = ResponseType {
        blob_id: blob_id,
        confirmation_certificate,
    };

    Ok((StatusCode::OK, Json(response)).into_response())
}

#[cfg(test)]
mod tests {

    use std::str::FromStr;

    use axum::{extract::Query, http::Uri};
    use walrus_core::BlobId;

    use crate::{Params, params::B64UrlEncodedBytes};

    #[test]
    fn test_parse_fanout_query() {
        let blob_id_str = "efshm0WcBczCA_GVtB0itHbbSXLT5VMeQDl0A1b2_0Y";
        let blob_id = BlobId::from_str(blob_id_str).expect("valid blob id");
        let tx_bytes = B64UrlEncodedBytes::new(vec![13; 50]);
        let signature = B64UrlEncodedBytes::new(vec![42; 20]);

        let uri_str = format!(
            "http://localhost/v1/blob-fan-out?blob_id={}&tx_bytes={}&signature={}",
            blob_id_str,
            tx_bytes.to_string(),
            signature.to_string(),
        );
        dbg!(&uri_str);

        let uri: Uri = uri_str.parse().expect("valid uri");
        let result = Query::<Params>::try_from_uri(&uri).expect("parsing the uri works");
        assert_eq!(blob_id, result.blob_id);
        assert_eq!(tx_bytes, result.tx_bytes);
        assert_eq!(signature, result.signature);
    }
}
