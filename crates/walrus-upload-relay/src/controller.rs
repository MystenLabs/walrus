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

use anyhow::Result;
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_with::{DurationSeconds, serde_as};
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
    error::WalrusUploadRelayError,
    metrics::WalrusUploadRelayMetricSet,
    params::{DigestSchema, PaidTipParams, Params, TransactionDigestSchema},
    shared::{API_DOCS, BLOB_UPLOAD_RELAY_ROUTE, ResponseType, TIP_CONFIG_ROUTE},
    tip::{
        check::{check_response_tip, check_tx_freshness},
        config::{TipConfig, TipKind},
    },
    utils::check_tx_auth_package,
};

pub(crate) const DEFAULT_SERVER_ADDRESS: &str = "0.0.0.0:57391";

/// The configuration for the Walrus Upload Relay.
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct WalrusUploadRelayConfig {
    /// The configuration for tipping.
    tip_config: TipConfig,
    /// The maximum time gap (in seconds) between the time the tip transaction is executed (i.e.,
    /// the tip is paid), and the request to store is made to the Walrus upload relay.
    #[serde(rename = "tx_freshness_threshold_secs")]
    #[serde_as(as = "DurationSeconds")]
    tx_freshness_threshold: Duration,
    /// The maximum amount of time in the future (in seconds) we can tolerate a transaction
    /// timestamp to be.
    ///
    /// This is to account for clock skew between the Walrus upload relay and the full nodes.
    #[serde(rename = "tx_max_future_threshold_secs")]
    #[serde_as(as = "DurationSeconds")]
    tx_max_future_threshold: Duration,
}

/// The controller for the Walrus Upload Relay.
///
/// It is shared by all Walrus Upload Relay route handlers, and is responsible for checking incoming
/// requests and pushing slivers and metadata to storage nodes.
pub(crate) struct Controller {
    pub(crate) client: Client<SuiReadClient>,
    pub(crate) relay_config: WalrusUploadRelayConfig,
    pub(crate) n_shards: NonZeroU16,
    pub(crate) metric_set: WalrusUploadRelayMetricSet,
}

impl Controller {
    /// Creates a new controller.
    pub(crate) fn new(
        client: Client<SuiReadClient>,
        n_shards: NonZeroU16,
        relay_config: WalrusUploadRelayConfig,
        metric_set: WalrusUploadRelayMetricSet,
    ) -> Self {
        Self {
            client,
            relay_config,
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
    ) -> Result<ResponseType, WalrusUploadRelayError> {
        if self.relay_config.tip_config.requires_payment() {
            // Check authentication pre-conditions for upload relay, if the configuration requires a
            // tip.
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
            return Err(WalrusUploadRelayError::BlobIdMismatch);
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
    ) -> Result<(), WalrusUploadRelayError> {
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
            self.relay_config.tx_freshness_threshold,
            self.relay_config.tx_max_future_threshold,
        )
        .inspect_err(|_| self.metric_set.freshness_check_error.inc())?;
        check_response_tip(
            &self.relay_config.tip_config,
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

/// Runs the upload relay.
pub(crate) async fn run_upload_relay(
    context: Option<String>,
    walrus_config: PathBuf,
    server_address: SocketAddr,
    relay_config_path: PathBuf,
    registry: Registry,
) -> Result<()> {
    let metric_set = WalrusUploadRelayMetricSet::new(&registry);

    // Create a client we can use to communicate with the Sui network, which is used to
    // coordinate the Walrus network.
    let client = get_client(context.as_deref(), walrus_config.as_path(), &registry).await?;

    let n_shards = client.get_committees().await?.n_shards();
    let relay_config: WalrusUploadRelayConfig = load_from_yaml(relay_config_path)?;
    tracing::debug!(?relay_config, "loaded relay config");

    // Build our HTTP application to handle the blob fan-out operations.
    let app = Router::new()
        .merge(Redoc::with_url(
            API_DOCS,
            WalrusUploadRelayApiDoc::openapi(),
        ))
        .route(TIP_CONFIG_ROUTE, get(send_tip_config))
        .route(BLOB_UPLOAD_RELAY_ROUTE, post(blob_upload_relay_handler))
        .layer(DefaultBodyLimit::max(1024 * 1024 * 1024))
        .with_state(Arc::new(Controller::new(
            client,
            n_shards,
            relay_config,
            metric_set,
        )))
        .layer(cors_layer());

    let listener = tokio::net::TcpListener::bind(&server_address).await?;
    tracing::info!(?server_address, n_shards, "Serving Walrus Upload Relay...");
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
    info(title = "Walrus Upload Relay"),
    paths(blob_upload_relay_handler, send_tip_config),
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
pub(super) struct WalrusUploadRelayApiDoc;

/// Returns the tip configuration for the current Walrus Upload Relay.
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
    (StatusCode::OK, Json(&controller.relay_config.tip_config)).into_response()
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
    path = BLOB_UPLOAD_RELAY_ROUTE,
    request_body(
        content = Binary,
        content_type = "application/octet-stream",
        description = "Binary data of the unencoded blob to be stored."
        ),
    params(Params),
    responses(
        (status = 200, description = "The blob was relayed to the Walrus Network successfully"),
        // TODO(WAL-913): extend these docs with the the WalrusUploadRelayError variants.
    ),
)]
#[tracing::instrument(level = Level::ERROR, skip_all, fields(blob_id=%params.blob_id))]
pub(crate) async fn blob_upload_relay_handler(
    State(controller): State<Arc<Controller>>,
    Query(params): Query<Params>,
    body: Bytes,
) -> Result<impl IntoResponse, WalrusUploadRelayError> {
    let start = Instant::now();
    let blob_id = params.blob_id;
    tracing::info!(?params, "starting to process a relay request");
    let response = controller
        .fan_out(body, params)
        .await
        .inspect_err(|error| tracing::debug!(?error, "responding to request with error"))?;
    tracing::info!(
        duration = ?start.elapsed(),
        ?blob_id,
        "finished processing a blob upload relay request",
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use sui_types::base_types::SuiAddress;
    use utoipa::OpenApi;
    use utoipa_redoc::Redoc;

    use super::{WalrusUploadRelayApiDoc, WalrusUploadRelayConfig};
    use crate::tip::config::{TipConfig, TipKind};

    const EXAMPLE_CONFIG_PATH: &str = "walrus_upload_relay_config_example.yaml";

    #[test]
    fn keep_example_config_in_sync() {
        let config = WalrusUploadRelayConfig {
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

    /// Serializes the upload relay's openAPI spec when this test is run.
    ///
    /// This test ensures that the files `upload_relay_openapi.yaml` and
    /// `upload_relay_openapi.html` are kept in sync with changes to the spec.
    #[test]
    fn upload_relay_check_and_update_openapi_spec() {
        let label = "upload_relay";
        let spec_path = format!("{label}_openapi.yaml");
        let html_path = format!("{label}_openapi.html");

        let mut spec = WalrusUploadRelayApiDoc::openapi();
        spec.info.version = "<VERSION>".to_string();

        std::fs::write(html_path, Redoc::new(spec.clone()).to_html())
            .expect("should be able to write to disk");

        if let Err(error) = walrus_test_utils::overwrite_file_and_fail_if_not_equal(
            spec_path,
            spec.to_yaml().expect("should be a ble to encode to yaml"),
        ) {
            panic!("the OpenAPI spec was updated by the test: {error:?}");
        };
    }
}
