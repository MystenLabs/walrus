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
    extract::{DefaultBodyLimit, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, post},
};
#[cfg(any(test, feature = "test-client"))]
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
#[cfg(any(test, feature = "test-client"))]
use reqwest::Url;
use serde::{Deserialize, Serialize};
use sui_sdk::rpc_types::SuiTransactionBlockResponseOptions;
use tokio::time::Instant;
use tower_http::cors::{Any, CorsLayer};
use tracing::Level;
use utoipa::{OpenApi, ToSchema};
use utoipa_redoc::{Redoc, Servable};
use walrus_sdk::{
    SuiReadClient,
    client::Client,
    config::ClientConfig,
    core::{
        BlobId,
        EncodingType,
        encoding::EncodingConfigTrait as _,
        messages::{BlobPersistenceType, ConfirmationCertificate},
    },
    core_utils::{load_from_yaml, metrics::Registry},
    sui::{
        ObjectIdSchema,
        client::{SuiClientMetricSet, retry_client::RetriableSuiClient},
    },
};

use crate::{
    error::FanOutError,
    metrics::FanOutProxyMetricSet,
    params::{DigestSchema, PaidTipParams, Params, TransactionDigestSchema},
    tip::{TipConfig, TipKind, check_response_tip, check_tx_freshness},
    utils::check_tx_auth_package,
};

const DEFAULT_SERVER_ADDRESS: &str = "0.0.0.0:57391";
pub(crate) const BLOB_FAN_OUT_ROUTE: &str = "/v1/blob-fan-out";
pub(crate) const TIP_CONFIG_ROUTE: &str = "/v1/tip-config";
pub(crate) const API_DOCS: &str = "/v1/api";

/// The configuration for the fanout proxy.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct FanOutConfig {
    /// The configuration for tipping.
    tip_config: TipConfig,
    /// The maximum time gap between the time the tip transaction is executed (i.e., the tip is
    /// paid), and the request to store is made to the fan-out proxy.
    tx_freshness_threshold: Duration,
    /// The maximum amount of time in the future we can tolerate a transaction timestamp to be.
    ///
    /// This is to account for clock skew between the fan out proxy and the full nodes.
    tx_max_future_threshold: Duration,
}

/// The controller for the fanout proxy.
///
/// It is shared by all fan-out route handlers, and is responsible for checking incoming
/// requests and pushing slivers and metadata to storage nodes.
pub(crate) struct Controller {
    pub(crate) client: Client<SuiReadClient>,
    pub(crate) fan_out_config: FanOutConfig,
    pub(crate) n_shards: NonZeroU16,
    pub(crate) metric_set: FanOutProxyMetricSet,
}

impl Controller {
    /// Creates a new controller.
    pub(crate) fn new(
        client: Client<SuiReadClient>,
        n_shards: NonZeroU16,
        fan_out_config: FanOutConfig,
        metric_set: FanOutProxyMetricSet,
    ) -> Self {
        Self {
            client,
            fan_out_config,
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
        if self.fan_out_config.tip_config.requires_payment() {
            // Check authentication pre-conditions for fan-out, if the proxy requires a tip.
            let paid_params = params.to_paid_params()?;
            self.validate_auth_package(&paid_params, body.as_ref())
                .await?;
        }

        let encode_start_timer = Instant::now();
        // PERF: encoding should probably be done on a separate thread pool.
        let (sliver_pairs, metadata) = self
            .client
            .encoding_config()
            .get_for_type(
                params
                    .encoding_type
                    .unwrap_or(walrus_sdk::core::DEFAULT_ENCODING),
            )
            .encode_with_metadata(body.as_ref())?;
        let duration = encode_start_timer.elapsed();

        tracing::debug!(
            computed_blob_id=%metadata.blob_id(),
            expected_blob_id=%params.blob_id,
            "blob id computed"
        );

        if *metadata.blob_id() != params.blob_id {
            self.metric_set.blob_id_mismatch.inc();
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
        params: &PaidTipParams,
        blob: &[u8],
    ) -> Result<(), FanOutError> {
        // Get transaction inputs from tx_id.
        let tx = self
            .client
            .sui_client()
            .sui_client()
            .get_transaction_with_options(
                params.tx_id,
                SuiTransactionBlockResponseOptions::new()
                    .with_raw_input()
                    .with_balance_changes(),
            )
            .await
            .map_err(|err| {
                self.metric_set.get_transaction_error.inc();
                Box::new(err)
            })?;

        check_tx_freshness(
            &tx,
            self.fan_out_config.tx_freshness_threshold,
            self.fan_out_config.tx_max_future_threshold,
        )
        .inspect_err(|_| self.metric_set.freshness_check_error.inc())?;
        check_response_tip(
            &self.fan_out_config.tip_config,
            &tx,
            blob.len().try_into().expect("using 32 or 64 bit arch"),
            self.n_shards,
            params
                .encoding_type
                .unwrap_or(walrus_sdk::core::DEFAULT_ENCODING),
        )
        .inspect_err(|_| self.metric_set.tip_check_error.inc())?;
        check_tx_auth_package(blob, &params.nonce, tx).inspect_err(|_| {
            self.metric_set.auth_package_check_error.inc();
        })?;

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
    fan_out_config: PathBuf,
    registry: Registry,
) -> Result<()> {
    let metric_set = FanOutProxyMetricSet::new(&registry);

    // Create a client we can use to communicate with the Sui network, which is used to
    // coordinate the Walrus network.
    let client = get_client(context.as_deref(), walrus_config.as_path(), &registry).await?;

    let n_shards = client.get_committees().await?.n_shards();
    let fan_out_config: FanOutConfig = load_from_yaml(fan_out_config)?;
    tracing::debug!(?fan_out_config, "loaded tip config");

    // Build our HTTP application to handle the blob fan-out operations.
    let app = Router::new()
        .merge(Redoc::with_url(API_DOCS, FanOutApiDoc::openapi()))
        .route(TIP_CONFIG_ROUTE, get(send_tip_config))
        .route(BLOB_FAN_OUT_ROUTE, post(fan_out_blob_slivers))
        .layer(DefaultBodyLimit::max(1024 * 1024 * 1024))
        .with_state(Arc::new(Controller::new(
            client,
            n_shards,
            fan_out_config,
            metric_set,
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
    paths(fan_out_blob_slivers, send_tip_config),
    components(schemas(
        BlobId,
        EncodingType,
        ObjectIdSchema,
        TransactionDigestSchema,
        DigestSchema,
        TipKind,
        TipConfig,
    ))
)]
pub(super) struct FanOutApiDoc;

/// Returns the tip configuration for the current fanout proxy.
///
/// Allows clients to refresh their configuration of the proxy's address and tip amounts.
#[utoipa::path(
    get,
    path = TIP_CONFIG_ROUTE,
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
    (StatusCode::OK, Json(&controller.fan_out_config.tip_config)).into_response()
}

// NOTE: Copied from walrus service
#[derive(Debug, ToSchema)]
#[schema(value_type = String, format = Binary)]
pub(crate) struct Binary(());

/// Upload a Blob to the Walrus Network
///
/// Note that the Blob must have previously been registered.
///
/// This endpoint checks that any required Tip has been supplied, then fulfills a request to store
/// slivers.
#[utoipa::path(
    post,
    path = BLOB_FAN_OUT_ROUTE,
    request_body(
        content = Binary,
        content_type = "application/octet-stream",
        description = "Binary data of the unencoded blob to be stored."
        ),
    params(Params),
    responses(
        (status = 200, description = "The blob was fanned-out to the Walrus Network successfully"),
        // FanOutError, // TODO: add the FanOutError IntoResponses implementation
    ),
)]
#[tracing::instrument(level = Level::ERROR, skip_all, fields(blob_id=%params.blob_id))]
pub(crate) async fn fan_out_blob_slivers(
    State(controller): State<Arc<Controller>>,
    Query(params): Query<Params>,
    body: Bytes,
) -> Result<impl IntoResponse, FanOutError> {
    let start = Instant::now();
    let blob_id = params.blob_id;
    tracing::info!(?params, "starting to process a fan-out request");
    let response = controller
        .fan_out(body, params)
        .await
        .inspect_err(|error| tracing::debug!(?error, "responding to request with error"))?;
    tracing::info!(
        duration = ?start.elapsed(),
        ?blob_id,
        "finished to process a fan-out request",
    );

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

#[cfg(any(test, feature = "test-client"))]
pub(crate) fn fan_out_blob_url(server_url: &Url, params: &Params) -> Result<Url> {
    let mut url = server_url.join(BLOB_FAN_OUT_ROUTE)?;
    let mut query_pairs = url.query_pairs_mut();

    query_pairs.append_pair("blob_id", &params.blob_id.to_string());

    if let Some(object_id) = params.deletable_blob_object {
        query_pairs.append_pair("deletable_blob_object", &object_id.to_string());
    };

    if let Some(tx_id) = params.tx_id {
        query_pairs.append_pair("tx_id", &tx_id.to_string());
    }

    if let Some(nonce) = params.nonce {
        query_pairs.append_pair("nonce", &URL_SAFE_NO_PAD.encode(nonce));
    }
    drop(query_pairs);

    // TODO: add encoding type serialization.

    Ok(url)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use sui_types::base_types::SuiAddress;

    use super::FanOutConfig;
    use crate::tip::{TipConfig, TipKind};

    const EXAMPLE_CONFIG_PATH: &str = "walrus_upload_relay_config_example.yaml";

    #[test]
    fn keep_example_config_in_sync() {
        let config = FanOutConfig {
            tip_config: TipConfig::SendTip {
                address: SuiAddress::from_bytes([42; 32]).expect("valid bytes"),
                kind: TipKind::Const(42),
            },
            tx_freshness_threshold: Duration::from_secs(60 * 60 * 10), // 10 hours.
            tx_max_future_threshold: Duration::from_secs(30),
        };

        walrus_test_utils::overwrite_file_and_fail_if_not_equal(
            EXAMPLE_CONFIG_PATH,
            serde_yaml::to_string(&config).expect("serialization succeeds"),
        )
        .expect("overwrite failed");
    }
}
