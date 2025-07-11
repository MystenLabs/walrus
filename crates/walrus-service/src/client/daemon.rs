// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! A client daemon who serves a set of simple HTTP endpoints to store, encode, or read blobs.

use std::{collections::HashSet, fmt::Debug, net::SocketAddr, sync::Arc};

use axum::{
    BoxError,
    Router,
    body::HttpBody,
    error_handling::HandleErrorLayer,
    extract::{DefaultBodyLimit, Query, Request, State},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, put},
};
use axum_extra::{
    TypedHeader,
    headers::{Authorization, authorization::Bearer},
};
use openapi::{AggregatorApiDoc, DaemonApiDoc, PublisherApiDoc};
use reqwest::StatusCode;
pub use routes::PublisherQuery;
use routes::{
    BLOB_GET_ENDPOINT,
    BLOB_OBJECT_GET_ENDPOINT,
    BLOB_PUT_ENDPOINT,
    QUILT_PATCH_BY_ID_GET_ENDPOINT,
    QUILT_PATCH_BY_IDENTIFIER_GET_ENDPOINT,
    QUILT_PUT_ENDPOINT,
    STATUS_ENDPOINT,
    daemon_cors_layer,
};
use sui_types::base_types::ObjectID;
use tower::{
    ServiceBuilder,
    buffer::BufferLayer,
    limit::ConcurrencyLimitLayer,
    load_shed::{LoadShedLayer, error::Overloaded},
};
use tower_http::trace::TraceLayer;
use utoipa::OpenApi;
use utoipa_redoc::{Redoc, Servable};
use walrus_core::{
    BlobId,
    DEFAULT_ENCODING,
    EncodingType,
    EpochCount,
    QuiltPatchId,
    encoding::{
        Primary,
        quilt_encoding::{QuiltStoreBlob, QuiltVersion},
    },
};
use walrus_sdk::{
    client::{
        Client,
        quilt_client::QuiltClientConfig,
        responses::{BlobStoreResult, QuiltStoreResult},
    },
    error::{ClientError, ClientResult},
    store_optimizations::StoreOptimizations,
};
use walrus_sui::{
    client::{BlobPersistence, PostStoreAction, ReadClient, SuiContractClient},
    types::move_structs::BlobWithAttribute,
};
use walrus_utils::metrics::Registry;

use crate::{
    client::{
        cli::{AggregatorArgs, PublisherArgs},
        config::AuthConfig,
        daemon::auth::verify_jwt_claim,
    },
    common::telemetry::{MakeHttpSpan, MetricsMiddlewareState, metrics_middleware},
};

pub mod auth;
pub(crate) mod cache;
pub(crate) use cache::{CacheConfig, CacheHandle};
mod openapi;
mod routes;

pub trait WalrusReadClient {
    /// Reads a blob from Walrus.
    fn read_blob(
        &self,
        blob_id: &BlobId,
    ) -> impl std::future::Future<Output = ClientResult<Vec<u8>>> + Send;

    /// Returns the blob object and its associated attributes given the object ID of either
    /// a blob object or a shared blob.
    fn get_blob_by_object_id(
        &self,
        blob_object_id: &ObjectID,
    ) -> impl std::future::Future<Output = ClientResult<BlobWithAttribute>> + Send;

    /// Retrieves blobs from quilt by their patch IDs.
    /// Default implementation returns an error indicating quilt is not supported.
    fn get_blobs_by_quilt_patch_ids(
        &self,
        _quilt_patch_ids: &[QuiltPatchId],
    ) -> impl std::future::Future<Output = ClientResult<Vec<QuiltStoreBlob<'static>>>> + Send {
        async {
            use walrus_sdk::error::ClientErrorKind;
            Err(ClientError::from(ClientErrorKind::Other(
                "quilt functionality not supported by this client".into(),
            )))
        }
    }

    /// Retrieves a blob from quilt by quilt ID and identifier.
    /// Default implementation returns an error indicating quilt is not supported.
    fn get_blob_by_quilt_id_and_identifier(
        &self,
        _quilt_id: &BlobId,
        _identifier: &str,
    ) -> impl std::future::Future<Output = ClientResult<QuiltStoreBlob<'static>>> + Send {
        async {
            use walrus_sdk::error::ClientErrorKind;
            Err(ClientError::from(ClientErrorKind::Other(
                "quilt functionality not supported by this client".into(),
            )))
        }
    }
}

/// Trait representing a client that can write blobs to Walrus.
pub trait WalrusWriteClient: WalrusReadClient {
    /// Writes a blob to Walrus.
    fn write_blob(
        &self,
        blob: &[u8],
        encoding_type: Option<EncodingType>,
        epochs_ahead: EpochCount,
        store_optimizations: StoreOptimizations,
        persistence: BlobPersistence,
        post_store: PostStoreAction,
    ) -> impl std::future::Future<Output = ClientResult<BlobStoreResult>> + Send;

    /// Constructs a quilt from blobs.
    fn construct_quilt<V: QuiltVersion>(
        &self,
        blobs: &[QuiltStoreBlob<'_>],
        encoding_type: Option<EncodingType>,
    ) -> impl std::future::Future<Output = ClientResult<V::Quilt>> + Send;

    /// Writes a quilt to Walrus.
    fn write_quilt<V: QuiltVersion>(
        &self,
        quilt: V::Quilt,
        encoding_type: Option<EncodingType>,
        epochs_ahead: EpochCount,
        store_optimizations: StoreOptimizations,
        persistence: BlobPersistence,
        post_store: PostStoreAction,
    ) -> impl std::future::Future<Output = ClientResult<QuiltStoreResult>> + Send;

    /// Returns the default [`PostStoreAction`] for this client.
    fn default_post_store_action(&self) -> PostStoreAction;
}

impl<T: ReadClient> WalrusReadClient for Client<T> {
    async fn read_blob(&self, blob_id: &BlobId) -> ClientResult<Vec<u8>> {
        self.read_blob_retry_committees::<Primary>(blob_id).await
    }

    async fn get_blob_by_object_id(
        &self,
        blob_object_id: &ObjectID,
    ) -> ClientResult<BlobWithAttribute> {
        self.get_blob_by_object_id(blob_object_id).await
    }

    async fn get_blobs_by_quilt_patch_ids(
        &self,
        quilt_patch_ids: &[QuiltPatchId],
    ) -> ClientResult<Vec<QuiltStoreBlob<'static>>> {
        self.quilt_client(QuiltClientConfig::default())
            .get_blobs_by_ids(quilt_patch_ids)
            .await
    }

    async fn get_blob_by_quilt_id_and_identifier(
        &self,
        quilt_id: &BlobId,
        identifier: &str,
    ) -> ClientResult<QuiltStoreBlob<'static>> {
        let blobs = self
            .quilt_client(QuiltClientConfig::default())
            .get_blobs_by_identifiers(quilt_id, &[identifier])
            .await?;

        blobs.into_iter().next().ok_or_else(|| {
            use walrus_sdk::error::ClientErrorKind;
            ClientError::from(ClientErrorKind::Other(
                format!("blob with identifier '{identifier}' not found in quilt").into(),
            ))
        })
    }
}

impl WalrusWriteClient for Client<SuiContractClient> {
    async fn write_blob(
        &self,
        blob: &[u8],
        encoding_type: Option<EncodingType>,
        epochs_ahead: EpochCount,
        store_optimizations: StoreOptimizations,
        persistence: BlobPersistence,
        post_store: PostStoreAction,
    ) -> ClientResult<BlobStoreResult> {
        let encoding_type = encoding_type.unwrap_or(DEFAULT_ENCODING);

        let result = self
            .reserve_and_store_blobs_retry_committees(
                &[blob],
                encoding_type,
                epochs_ahead,
                store_optimizations,
                persistence,
                post_store,
                None,
            )
            .await?;

        Ok(result
            .into_iter()
            .next()
            .expect("there is only one blob, as store was called with one blob"))
    }

    async fn construct_quilt<V: QuiltVersion>(
        &self,
        blobs: &[QuiltStoreBlob<'_>],
        encoding_type: Option<EncodingType>,
    ) -> ClientResult<V::Quilt> {
        let encoding_type = encoding_type.unwrap_or(DEFAULT_ENCODING);

        // TODO(WAL-927): Make QuiltConfig part of ClientConfig.
        self.quilt_client(QuiltClientConfig::default())
            .construct_quilt::<V>(blobs, encoding_type)
            .await
    }

    async fn write_quilt<V: QuiltVersion>(
        &self,
        quilt: V::Quilt,
        encoding_type: Option<EncodingType>,
        epochs_ahead: EpochCount,
        store_optimizations: StoreOptimizations,
        persistence: BlobPersistence,
        post_store: PostStoreAction,
    ) -> ClientResult<QuiltStoreResult> {
        let encoding_type = encoding_type.unwrap_or(DEFAULT_ENCODING);

        self.quilt_client(QuiltClientConfig::default())
            .reserve_and_store_quilt::<V>(
                &quilt,
                encoding_type,
                epochs_ahead,
                store_optimizations,
                persistence,
                post_store,
            )
            .await
    }

    fn default_post_store_action(&self) -> PostStoreAction {
        PostStoreAction::Keep
    }
}

/// Configuration for the response headers of the aggregator.
#[derive(Debug, Clone, Default)]
pub struct AggregatorResponseHeaderConfig {
    /// The headers that are allowed to be returned in the response.
    pub allowed_headers: HashSet<String>,
    /// If true, the tags of the quilt patch will be returned in the response headers.
    pub allow_quilt_patch_tags_in_response: bool,
}

/// The client daemon.
///
/// Exposes different HTTP endpoints depending on which function `ClientDaemon::new_*` it is
/// constructed with.
#[derive(Debug, Clone)]
pub struct ClientDaemon<T> {
    client: Arc<T>,
    network_address: SocketAddr,
    metrics: MetricsMiddlewareState,
    router: Router<Arc<T>>,
    response_header_config: Arc<AggregatorResponseHeaderConfig>,
}

impl<T: WalrusReadClient + Send + Sync + 'static> ClientDaemon<T> {
    /// Constructs a new [`ClientDaemon`] with aggregator functionality.
    pub fn new_aggregator(
        client: T,
        network_address: SocketAddr,
        registry: &Registry,
        allowed_headers: Vec<String>,
        allow_quilt_patch_tags_in_response: bool,
    ) -> Self {
        Self::new::<AggregatorApiDoc>(client, network_address, registry).with_aggregator(
            AggregatorResponseHeaderConfig {
                allowed_headers: allowed_headers.into_iter().collect(),
                allow_quilt_patch_tags_in_response,
            },
        )
    }

    /// Creates a new [`ClientDaemon`], which serves requests at the provided `network_address` and
    /// interacts with Walrus through the `client`.
    ///
    /// The exposed APIs can be defined by calling a subset of the functions `with_*`. The daemon is
    /// started through [`Self::run()`].
    fn new<A: OpenApi>(client: T, network_address: SocketAddr, registry: &Registry) -> Self {
        ClientDaemon {
            client: Arc::new(client),
            network_address,
            metrics: MetricsMiddlewareState::new(registry),
            router: Router::new()
                .merge(Redoc::with_url(routes::API_DOCS, A::openapi()))
                .route(STATUS_ENDPOINT, get(routes::status)),
            response_header_config: Arc::new(AggregatorResponseHeaderConfig::default()),
        }
    }

    /// Specifies that the daemon should expose the aggregator interface (read blobs).
    fn with_aggregator(mut self, response_header_config: AggregatorResponseHeaderConfig) -> Self {
        self.response_header_config = Arc::new(response_header_config);
        tracing::info!(
            "Aggregator response header config: {:?}",
            self.response_header_config
        );
        self.router = self
            .router
            .route(BLOB_GET_ENDPOINT, get(routes::get_blob))
            .route(
                BLOB_OBJECT_GET_ENDPOINT,
                get(routes::get_blob_by_object_id)
                    .with_state((self.client.clone(), self.response_header_config.clone())),
            )
            .route(
                QUILT_PATCH_BY_ID_GET_ENDPOINT,
                get(routes::get_blob_by_quilt_patch_id)
                    .with_state((self.client.clone(), self.response_header_config.clone())),
            )
            .route(
                QUILT_PATCH_BY_IDENTIFIER_GET_ENDPOINT,
                get(routes::get_blob_by_quilt_id_and_identifier)
                    .with_state((self.client.clone(), self.response_header_config.clone())),
            );
        self
    }

    /// Runs the daemon.
    pub async fn run(self) -> Result<(), std::io::Error> {
        let listener = tokio::net::TcpListener::bind(self.network_address).await?;
        tracing::info!(address = %self.network_address, "the client daemon is starting");

        let request_layers = ServiceBuilder::new()
            .layer(middleware::from_fn_with_state(
                self.metrics.clone(),
                metrics_middleware,
            ))
            .layer(
                TraceLayer::new_for_http()
                    .make_span_with(MakeHttpSpan::new())
                    .on_response(MakeHttpSpan::new()),
            )
            .layer(daemon_cors_layer());

        axum::serve(
            listener,
            self.router.with_state(self.client).layer(request_layers),
        )
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
    }
}

impl<T: WalrusWriteClient + Send + Sync + 'static> ClientDaemon<T> {
    /// Constructs a new [`ClientDaemon`] with publisher functionality.
    pub fn new_publisher(
        client: T,
        auth_config: Option<AuthConfig>,
        args: &PublisherArgs,
        registry: &Registry,
    ) -> Self {
        Self::new::<PublisherApiDoc>(client, args.daemon_args.bind_address, registry)
            .with_publisher(
                auth_config,
                args.max_body_size(),
                args.max_request_buffer_size,
                args.max_concurrent_requests,
                args.max_quilt_body_size(),
            )
    }

    /// Constructs a new [`ClientDaemon`] with combined aggregator and publisher functionality.
    pub fn new_daemon(
        client: T,
        auth_config: Option<AuthConfig>,
        registry: &Registry,
        publisher_args: &PublisherArgs,
        aggregator_args: &AggregatorArgs,
    ) -> Self {
        Self::new::<DaemonApiDoc>(client, publisher_args.daemon_args.bind_address, registry)
            .with_aggregator(AggregatorResponseHeaderConfig {
                allowed_headers: aggregator_args
                    .allowed_headers
                    .clone()
                    .into_iter()
                    .collect(),
                allow_quilt_patch_tags_in_response: aggregator_args
                    .allow_quilt_patch_tags_in_response,
            })
            .with_publisher(
                auth_config,
                publisher_args.max_body_size_kib,
                publisher_args.max_request_buffer_size,
                publisher_args.max_concurrent_requests,
                publisher_args.max_quilt_body_size(),
            )
    }

    /// Specifies that the daemon should expose the publisher interface (store blobs).
    fn with_publisher(
        mut self,
        auth_config: Option<AuthConfig>,
        max_body_limit: usize,
        max_request_buffer_size: usize,
        max_concurrent_requests: usize,
        max_quilt_body_limit: usize,
    ) -> Self {
        tracing::debug!(
            %max_body_limit,
            %max_request_buffer_size,
            %max_concurrent_requests,
            "configuring the publisher endpoint",
        );

        let base_layers = ServiceBuilder::new()
            .layer(HandleErrorLayer::new(handle_publisher_error))
            .layer(LoadShedLayer::new())
            .layer(BufferLayer::new(max_request_buffer_size))
            .layer(ConcurrencyLimitLayer::new(max_concurrent_requests))
            .layer(DefaultBodyLimit::max(max_body_limit));

        if let Some(auth_config) = auth_config {
            // Create and run the cache to track the used JWT tokens.
            let replay_suppression_cache = auth_config.replay_suppression_config.build_and_run();

            let auth_layers = ServiceBuilder::new()
                .layer(axum::middleware::from_fn_with_state(
                    (Arc::new(auth_config), Arc::new(replay_suppression_cache)),
                    auth_layer,
                ))
                .layer(base_layers.clone());

            self.router = self
                .router
                .route(
                    BLOB_PUT_ENDPOINT,
                    put(routes::put_blob).route_layer(auth_layers.clone()),
                )
                .route(
                    QUILT_PUT_ENDPOINT,
                    put(routes::put_quilt)
                        .route_layer(DefaultBodyLimit::max(max_quilt_body_limit))
                        .route_layer(auth_layers),
                );
        } else {
            self.router = self
                .router
                .route(
                    BLOB_PUT_ENDPOINT,
                    put(routes::put_blob).route_layer(base_layers.clone()),
                )
                .route(
                    QUILT_PUT_ENDPOINT,
                    put(routes::put_quilt)
                        .route_layer(DefaultBodyLimit::max(max_quilt_body_limit))
                        .route_layer(base_layers),
                );
        }
        self
    }
}

pub(crate) async fn auth_layer(
    State((auth_config, token_cache)): State<(Arc<AuthConfig>, Arc<CacheHandle<String>>)>,
    query: Query<PublisherQuery>,
    TypedHeader(bearer_header): TypedHeader<Authorization<Bearer>>,
    request: Request,
    next: Next,
) -> Response {
    // Get a hint on the body size if possible.
    // Note: Try to get a body hint to reject a oversize payload as fast as possible.
    // It is fine to use this imprecise hint, because we will check again the size when storing to
    // Walrus.
    tracing::debug!(query = ?query.0, "authenticating a request to store a blob");

    if let Err(resp) = verify_jwt_claim(
        query,
        bearer_header,
        &auth_config,
        token_cache.as_ref(),
        request.body().size_hint(),
    )
    .await
    {
        resp
    } else {
        next.run(request).await
    }
}

async fn handle_publisher_error(error: BoxError) -> Response {
    if error.is::<Overloaded>() {
        (
            StatusCode::TOO_MANY_REQUESTS,
            "the publisher is receiving too many requests; please try again later",
        )
            .into_response()
    } else {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "something went wrong while storing the blob",
        )
            .into_response()
    }
}
