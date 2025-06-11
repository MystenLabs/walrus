// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! The proxy's main controller logic.

use std::{
    net::SocketAddr,
    num::NonZeroU16,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use axum::{
    Router,
    body::Bytes,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, put},
};
use serde::{Deserialize, Serialize};
use sui_sdk::rpc_types::SuiTransactionBlockResponseOptions;
use sui_types::digests::TransactionDigest;
use tokio::time::Instant;
use tower_http::cors::{Any, CorsLayer};
use tracing::Level;
use utoipa::OpenApi;
use utoipa_redoc::{Redoc, Servable};
use walrus_sdk::{
    SuiReadClient,
    client::Client,
    config::ClientConfig,
    core::{
        BlobId,
        encoding::EncodingConfigTrait as _,
        messages::{BlobPersistenceType, ConfirmationCertificate},
    },
    core_utils::{load_from_yaml, metrics::Registry},
    sui::client::{SuiClientMetricSet, retry_client::RetriableSuiClient},
};

use crate::{
    error::FanOutError,
    metrics::FanOutProxyMetricSet,
    params::{AuthPackage, Params},
    tip::{TipConfig, check_response_tip, check_tx_freshness},
    utils::check_tx_authentication,
};

const DEFAULT_SERVER_ADDRESS: &str = "0.0.0.0:57391";
pub(crate) const BLOB_FAN_OUT_ROUTE: &str = "/v1/blob-fan-out";
pub(crate) const TIP_CONFIG_ROUTE: &str = "/v1/tip-config";
pub(crate) const API_DOCS: &str = "/v1/api";

/// The maximum time gap between the time the tip transaction is executed (i.e., the tip is paid),
/// and the request to store is made to the fan-out proxy.
// TODO: Make this configurable.
pub(crate) const FRESHNESS_THRESHOLD: Duration = Duration::from_secs(60 * 60); // 1 Hour.

/// The maximum amount of time in the future we can tolerate a transaction timestamp to be.
/// This is to account for clock skew between the fan out proxy and the full nodes.
// TODO: Make this configurable.
pub(crate) const MAX_FUTURE_THRESHOLD: Duration = Duration::from_secs(30);

/// The controller for the fanout proxy.
///
/// It is shared by all fan-out route handlers, and is responsible for checking incoming
/// requests and pushing slivers and metadata to storage nodes.
pub(crate) struct Controller {
    pub(crate) client: Client<SuiReadClient>,
    pub(crate) tip_config: TipConfig,
    pub(crate) n_shards: NonZeroU16,
    pub(crate) metric_set: FanOutProxyMetricSet,
}

impl Controller {
    /// Creates a new controller.
    pub(crate) fn new(
        client: Client<SuiReadClient>,
        n_shards: NonZeroU16,
        tip_config: TipConfig,
        metric_set: FanOutProxyMetricSet,
    ) -> Self {
        Self {
            client,
            tip_config,
            n_shards,
            metric_set,
        }
    }

    /// Checks the request and fans out the data to the storage nodes.
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    pub(crate) async fn fan_out(
        &self,
        body: Bytes,
        params: Params,
    ) -> Result<ResponseType, FanOutError> {
        // Check authentication pre-conditions for fan-out.
        self.validate_auth_package(params.tx_id, &params.auth_package, body.as_ref())
            .await?;

        let encode_start_timer = Instant::now();
        // PERF: encoding should probably be done on a separate thread pool.
        let (sliver_pairs, metadata) = self
            .client
            .encoding_config()
            // TODO: Encoding type configuration.
            .get_for_type(walrus_sdk::core::EncodingType::RS2)
            .encode_with_metadata(body.as_ref())?;
        let duration = encode_start_timer.elapsed();

        tracing::debug!(
            computed_blob_id=%metadata.blob_id(),
            expected_blob_id=%params.blob_id,
            "blob id computed"
        );

        if *metadata.blob_id() != params.blob_id {
            return Err(FanOutError::BlobIdMismatch);
        }

        let pair = sliver_pairs
            .first()
            .expect("the encoding produces sliver pairs");
        let symbol_size = pair.primary.symbols.symbol_size().get();

        tracing::debug!(
            symbol_size,
            primary_sliver_size = pair.primary.symbols.data().len(),
            secondary_sliver_size = pair.secondary.symbols.data().len(),
            ?duration,
            "encoded sliver pairs and metadata"
        );

        // Attempt to upload the slivers.
        // TODO: Blob persistence configuration
        let blob_persistence = if let Some(object_id) = params.deletable_blob_object {
            BlobPersistenceType::Deletable {
                object_id: object_id.into(),
            }
        } else {
            BlobPersistenceType::Permanent
        };
        let confirmation_certificate: ConfirmationCertificate = self
            .client
            .send_blob_data_and_get_certificate(&metadata, &sliver_pairs, &blob_persistence, None)
            .await?;

        self.metric_set.blobs_uploaded.inc();

        // Reply with the confirmation certificate.
        Ok(ResponseType {
            blob_id: params.blob_id,
            confirmation_certificate,
        })
    }

    async fn validate_auth_package(
        &self,
        tx_id: TransactionDigest,
        auth_package: &AuthPackage,
        blob: &[u8],
    ) -> Result<(), FanOutError> {
        // Get transaction inputs from tx_id.
        let tx = self
            .client
            .sui_client()
            .sui_client()
            .get_transaction_with_options(
                tx_id,
                SuiTransactionBlockResponseOptions::new()
                    .with_raw_input()
                    .with_balance_changes(),
            )
            .await
            .map_err(Box::new)?;

        check_tx_freshness(&tx, FRESHNESS_THRESHOLD, MAX_FUTURE_THRESHOLD)?;
        check_response_tip(
            &self.tip_config,
            &tx,
            blob.len()
                .try_into()
                .expect("we are running on a 64bit machine"),
            self.n_shards,
            // TODO: fix encoding type
            walrus_sdk::core::EncodingType::RS2,
        )?;
        check_tx_authentication(blob, tx, auth_package)?;

        // This request looks OK.
        Ok(())
    }
}

/// The response of the fanout proxy, containing the blob ID and the corresponding certificate.
#[derive(Serialize, Debug, Deserialize)]
pub(crate) struct ResponseType {
    pub blob_id: BlobId,
    pub confirmation_certificate: ConfirmationCertificate,
}

/// Runs the proxy.
pub(crate) async fn run_proxy(
    context: Option<String>,
    walrus_config: PathBuf,
    server_address: Option<SocketAddr>,
    tip_config: PathBuf,
    registry: Registry,
) -> Result<()> {
    let metric_set = FanOutProxyMetricSet::new(&registry);

    // Create a client we can use to communicate with the Sui network, which is used to
    // coordinate the Walrus network.
    let client = get_client(context.as_deref(), walrus_config.as_path(), &registry).await?;

    let n_shards = client.get_committees().await?.n_shards();
    let tip_config: TipConfig = load_from_yaml(tip_config)?;
    tracing::debug!(?tip_config, "loaded tip config");

    // Build our HTTP application to handle the blob fan-out operations.
    let app = Router::new()
        .merge(Redoc::with_url(API_DOCS, FanOutApiDoc::openapi()))
        .route(TIP_CONFIG_ROUTE, get(send_tip_config))
        .route(BLOB_FAN_OUT_ROUTE, put(fan_out_blob_slivers))
        .with_state(Arc::new(Controller::new(
            client, n_shards, tip_config, metric_set,
        )))
        .layer(cors_layer());

    let addr: SocketAddr = if let Some(socket_addr) = server_address {
        socket_addr
    } else {
        DEFAULT_SERVER_ADDRESS.parse().context("invalid address")?
    };

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!(?addr, n_shards, "Serving fan-out proxy");
    Ok(axum::serve(listener, app).await?)
}

/// Returns a `CorsLayer` for the controller endpoints.
pub(crate) fn cors_layer() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .max_age(Duration::from_secs(86400))
        .allow_headers(Any)
}

#[derive(OpenApi)]
#[openapi(
    info(title = "Walrus Fan-out Proxy"),
    paths(fan_out_blob_slivers),
    components(schemas(BlobId,))
)]
pub(super) struct FanOutApiDoc;

/// Returns the tip configuration for the current fanout proxy.
///
/// Allows clients to refresh their configuration of the proxy's address and tip amounts.
#[utoipa::path(
    get,
    path = TIP_CONFIG_ROUTE,
    params(),
    responses(
        (
            status = 200,
            description = "The tip configuration was retrieved successfully",
            body = TipConfig
        ),
    ),
)]
#[tracing::instrument(level = Level::ERROR, skip_all)]
pub(crate) async fn send_tip_config(
    State(controller): State<Arc<Controller>>,
) -> impl IntoResponse {
    tracing::debug!("returning tip config");
    (StatusCode::OK, Json(&controller.tip_config)).into_response()
}

/// Upload a Blob to the Walrus Network
///
/// Note that the Blob must have previously been registered.
///
/// This endpoint checks that any required Tip has been supplied, then fulfills a request to store
/// slivers.
#[utoipa::path(
    put,
    path = BLOB_FAN_OUT_ROUTE,
    request_body = &[u8],
    params(Params),
    responses(
        (status = 200, description = "The blob was fanned-out to the Walrus Network successfully"),
        // FanOutError, // TODO: add the FanOutError IntoResponses implementation
    ),
)]
#[tracing::instrument(level = Level::ERROR, skip_all, fields(%params.blob_id))]
pub(crate) async fn fan_out_blob_slivers(
    State(controller): State<Arc<Controller>>,
    Query(params): Query<Params>,
    body: Bytes,
) -> Result<impl IntoResponse, FanOutError> {
    tracing::debug!("starting to process a fan-out request");
    let response = controller.fan_out(body, params).await?;
    Ok((StatusCode::OK, Json(response)).into_response())
}

/// Returns a Walrus read client from the context and Walrus configuration.
pub(crate) async fn get_client(
    context: Option<&str>,
    walrus_config: &Path,
    registry: &Registry,
) -> Result<Client<SuiReadClient>> {
    let config: ClientConfig =
        walrus_sdk::config::load_configuration(Some(walrus_config), context)?;
    tracing::debug!(?config, "loaded client config");

    let retriable_sui_client = RetriableSuiClient::new_for_rpc_urls(
        &config.rpc_urls,
        config.backoff_config().clone(),
        None,
    )
    .await?
    .with_metrics(Some(Arc::new(SuiClientMetricSet::new(registry))));

    let sui_read_client = config.new_read_client(retriable_sui_client).await?;

    let refresh_handle = config
        .refresh_config
        .build_refresher_and_run(sui_read_client.clone())
        .await?;
    Ok(Client::new_read_client(config, refresh_handle, sui_read_client).await?)
}
