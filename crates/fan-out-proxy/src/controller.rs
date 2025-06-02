// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! The poxy's main controller logic.

use std::{
    net::SocketAddr,
    ops::Deref,
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
    routing::{get, post},
};
use fastcrypto::{encoding::Base64, hash::Digest};
use serde::{Deserialize, Serialize};
use sui_sdk::rpc_types::{
    SuiTransactionBlockData,
    SuiTransactionBlockDataAPI,
    SuiTransactionBlockResponseOptions,
};
use sui_types::{
    digests::TransactionDigest,
    transaction::{
        CallArg,
        InputObjectKind,
        SenderSignedData,
        TransactionData,
        TransactionDataAPI as _,
        TransactionDataV1,
        TransactionKind,
    },
};
use tokio::time::Instant;
use tower_http::cors::{Any, CorsLayer};
use tracing::Level;
use utoipa::OpenApi;
use utoipa_redoc::{Redoc, Servable};
use walrus_sdk::{
    ObjectID,
    SuiReadClient,
    client::Client,
    config::ClientConfig,
    core::{
        BlobId,
        EncodingType,
        encoding::EncodingConfigTrait as _,
        ensure,
        merkle::DIGEST_LEN,
        messages::{BlobPersistenceType, ConfirmationCertificate},
    },
    core_utils::{load_from_yaml, metrics::Registry},
    sui::client::{BlobPersistence, retry_client::RetriableSuiClient},
};

use crate::{
    TipConfig,
    client::AuthPackage,
    error::FanOutError,
    metrics::FanOutProxyMetricSet,
    params::{B64UrlEncodedBytes, Params},
    tip::TipChecker,
    utils::compute_blob_digest_sha256,
};

const DEFAULT_SERVER_ADDRESS: &str = "0.0.0.0:57391";
pub(crate) const BLOB_FAN_OUT_ROUTE: &str = "/v1/blob-fan-out";
pub(crate) const TIP_CONFIG_ROUTE: &str = "/v1/tip-config";
pub(crate) const API_DOCS: &str = "/v1/api";

/// The controller for the fanout proxy.
///
/// It is responsible for checking the incoming requests and pushing the slivers and metadata to the
/// storage nodes.
pub(crate) struct Controller {
    pub(crate) client: Client<SuiReadClient>,
    pub(crate) checker: TipChecker,
    pub(crate) metric_set: FanOutProxyMetricSet,
}

impl Controller {
    /// Creates a new controller.
    pub(crate) fn new(
        client: Client<SuiReadClient>,
        checker: TipChecker,
        metric_set: FanOutProxyMetricSet,
    ) -> Self {
        Self {
            client,
            checker,
            metric_set,
        }
    }

    /// Checks the request and fans out the data to the storage nodes.
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    pub(crate) async fn fan_out(
        &self,
        body: Bytes,
        blob_id: BlobId,
        tx_digest: TransactionDigest,
        auth_package: AuthPackage,
        object_id: ObjectID,
    ) -> Result<ResponseType, FanOutError> {
        // Check authentication pre-conditions for fan-out.
        validate_auth_package(
            self.client.sui_client().sui_client().clone(),
            tx_digest,
            &auth_package,
            body.as_ref(),
        )
        .await?;
        // TODO: get the blob object from the ptb above and pull object_id, encoding_type and
        // blob_persistence from it.

        let encode_start_timer = Instant::now();

        // PERF: encoding should probably be done on a separate thread pool.
        let (sliver_pairs, metadata) = self
            .client
            .encoding_config()
            .get_for_type(auth_package.encoding_type)
            .encode_with_metadata(body.as_ref())?;
        let duration = encode_start_timer.elapsed();

        tracing::debug!(
            computed_blob_id=%metadata.blob_id(),
            expected_blob_id=%blob_id,
            "blob id computed"
        );

        if *metadata.blob_id() != blob_id {
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
        let blob_persistence = match auth_package.blob_persistence {
            BlobPersistence::Deletable => BlobPersistenceType::Deletable {
                object_id: object_id.into(),
            },
            BlobPersistence::Permanent => BlobPersistenceType::Permanent,
        };
        let confirmation_certificate: ConfirmationCertificate = self
            .client
            .send_blob_data_and_get_certificate(&metadata, &sliver_pairs, &blob_persistence, None)
            .await?;

        self.metric_set.blobs_uploaded.inc();

        // Reply with the confirmation certificate.
        Ok(ResponseType {
            blob_id,
            blob_object: object_id,
            confirmation_certificate,
        })
    }
}

async fn validate_auth_package(
    sui_client: RetriableSuiClient,
    tx_digest: TransactionDigest,
    auth_package: &AuthPackage,
    blob: &[u8],
) -> Result<(), FanOutError> {
    // Get transaction inputs from tx_id.
    let tx = sui_client
        .get_transaction_with_options(
            tx_digest,
            SuiTransactionBlockResponseOptions::new()
                .with_raw_input()
                .with_balance_changes(),
        )
        .await?;

    // Check the tx details against the auth package.
    let orig_tx: SenderSignedData =
        bcs::from_bytes(&tx.raw_transaction).context("invalid raw transaction data")?;
    let TransactionData::V1(TransactionDataV1 {
        kind: TransactionKind::ProgrammableTransaction(ptb),
        ..
    }) = orig_tx.transaction_data()
    else {
        return Err(FanOutError::Other(anyhow::anyhow!(
            "invalid transaction data"
        )));
    };
    let Some(CallArg::Pure(auth_package_hash)) = ptb.inputs.get(0) else {
        return Err(FanOutError::Other(anyhow::anyhow!(
            "invalid transaction input construction"
        )));
    };
    let tx_auth_package_digest = Digest::<DIGEST_LEN>::new(
        auth_package_hash
            .as_slice()
            .try_into()
            .map_err(|_| FanOutError::InvalidPtbAuthPackageHash)?,
    );
    ensure!(
        tx_auth_package_digest == auth_package.to_digest()?,
        FanOutError::AuthPackageMismatch
    );
    ensure!(
        compute_blob_digest_sha256(blob).as_ref() == auth_package.blob_digest,
        FanOutError::BlobDigestMismatch
    );
    // This request looks OK.
    Ok(())
}

/// The response of the fanout proxy, containing the blob ID and the corresponding certificate.
#[derive(Serialize, Debug, Deserialize)]
pub(crate) struct ResponseType {
    pub blob_id: BlobId,
    pub blob_object: ObjectID,
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
    let client = get_client(context.as_deref(), walrus_config.as_path()).await?;

    let n_shards = client.get_committees().await?.n_shards();
    let tip_config: TipConfig = load_from_yaml(tip_config)?;
    tracing::debug!(?tip_config, "loaded tip config");
    let checker = TipChecker::new(
        tip_config,
        client.sui_client().sui_client().clone(), // TODO: lol this naming?
        n_shards,
    );

    // Build our HTTP application to handle the blob fan-out operations.
    let app = Router::new()
        .merge(Redoc::with_url(API_DOCS, FanOutApiDoc::openapi()))
        .route(TIP_CONFIG_ROUTE, get(send_tip_config))
        .route(BLOB_FAN_OUT_ROUTE, post(fan_out_blob_slivers))
        .with_state(Arc::new(Controller::new(client, checker, metric_set)))
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
    (StatusCode::OK, Json(controller.checker.config())).into_response()
}

/// Upload a Blob to the Walrus Network
///
/// Note that the Blob must have previously been registered.
///
/// This endpoint checks that any required Tip has been supplied, then fulfills a request to store
/// slivers.
#[utoipa::path(
    get,
    path = BLOB_FAN_OUT_ROUTE,
    request_body = &[u8],
    params(Params),
    responses(
        (status = 200, description = "The blob was fanned-out to the Walrus Network successfully"),
        // FanOutError, // TODO: add the FanOutError IntoResponses implementation
    ),
)]
#[tracing::instrument(level = Level::ERROR, skip_all, fields(%blob_id))]
pub(crate) async fn fan_out_blob_slivers(
    State(controller): State<Arc<Controller>>,
    Query(Params {
        blob_id,
        tx_id,
        auth_package,
        object_id,
    }): Query<Params>,
    body: Bytes,
) -> Result<impl IntoResponse, FanOutError> {
    tracing::debug!("starting to process a fan-out request");
    let response = controller
        .fan_out(body, blob_id, tx_id, auth_package, object_id)
        .await?;
    Ok((StatusCode::OK, Json(response)).into_response())
}

/// Returns a Walrus read client from the context and Walrus configuration.
pub(crate) async fn get_client(
    context: Option<&str>,
    walrus_config: &Path,
) -> Result<Client<SuiReadClient>> {
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

#[cfg(test)]
mod tests {

    use std::str::FromStr;

    use axum::{extract::Query, http::Uri};
    use walrus_core::BlobId;

    use crate::params::{B64UrlEncodedBytes, Params};

    #[test]
    fn test_parse_fanout_query() {
        let blob_id_str = "efshm0WcBczCA_GVtB0itHbbSXLT5VMeQDl0A1b2_0Y";
        let blob_id = BlobId::from_str(blob_id_str).expect("valid blob id");
        let tx_bytes = B64UrlEncodedBytes::new(vec![13; 50]);
        let signature = B64UrlEncodedBytes::new(vec![42; 20]);

        let uri_str = format!(
            "http://localhost/v1/blob-fan-out?blob_id={}&tx_bytes={}&signature={}",
            blob_id_str, tx_bytes, signature,
        );
        dbg!(&uri_str);

        let uri: Uri = uri_str.parse().expect("valid uri");
        let result = Query::<Params>::try_from_uri(&uri).expect("parsing the uri works");
        assert_eq!(blob_id, result.blob_id);
        assert_eq!(tx_bytes, result.tx_bytes);
        assert_eq!(signature, result.signature);
    }
}
