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
    QUILT_BLOBS_ENDPOINT,
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
    encoding::{Primary, QuiltBlobOwned},
};
use walrus_sdk::{
    client::{Client, responses::BlobStoreResult},
    error::{ClientError, ClientResult},
    store_when::StoreWhen,
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

    /// Retrieves blobs from quilt by their IDs.
    /// Default implementation returns an error indicating quilt is not supported.
    fn get_blobs_by_quilt_ids(
        &self,
        _quilt_blob_ids: &[QuiltPatchId],
    ) -> impl std::future::Future<Output = ClientResult<Vec<QuiltBlobOwned>>> + Send {
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
        store_when: StoreWhen,
        persistence: BlobPersistence,
        post_store: PostStoreAction,
    ) -> impl std::future::Future<Output = ClientResult<BlobStoreResult>> + Send;

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

    async fn get_blobs_by_quilt_ids(
        &self,
        quilt_blob_ids: &[QuiltPatchId],
    ) -> ClientResult<Vec<QuiltBlobOwned>> {
        self.quilt_client().get_blobs_by_ids(quilt_blob_ids).await
    }
}

impl WalrusWriteClient for Client<SuiContractClient> {
    async fn write_blob(
        &self,
        blob: &[u8],
        encoding_type: Option<EncodingType>,
        epochs_ahead: EpochCount,
        store_when: StoreWhen,
        persistence: BlobPersistence,
        post_store: PostStoreAction,
    ) -> ClientResult<BlobStoreResult> {
        let encoding_type = encoding_type.unwrap_or(DEFAULT_ENCODING);

        let result = self
            .reserve_and_store_blobs_retry_committees(
                &[blob],
                encoding_type,
                epochs_ahead,
                store_when,
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

    fn default_post_store_action(&self) -> PostStoreAction {
        PostStoreAction::Keep
    }
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
    allowed_headers: Arc<HashSet<String>>,
}

impl<T: WalrusReadClient + Send + Sync + 'static> ClientDaemon<T> {
    /// Constructs a new [`ClientDaemon`] with aggregator functionality.
    pub fn new_aggregator(
        client: T,
        network_address: SocketAddr,
        registry: &Registry,
        allowed_headers: Vec<String>,
    ) -> Self {
        Self::new::<AggregatorApiDoc>(client, network_address, registry)
            .with_aggregator(allowed_headers)
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
            allowed_headers: Arc::new(HashSet::new()),
        }
    }

    /// Specifies that the daemon should expose the aggregator interface (read blobs).
    fn with_aggregator(mut self, allowed_headers: Vec<String>) -> Self {
        self.with_allowed_headers(allowed_headers);
        tracing::info!("Aggregator allowed headers: {:?}", self.allowed_headers);
        tracing::info!("Aggregator with quilt: {:?}", QUILT_BLOBS_ENDPOINT);
        self.router = self
            .router
            .route(BLOB_GET_ENDPOINT, get(routes::get_blob))
            .route(
                BLOB_OBJECT_GET_ENDPOINT,
                get(routes::get_blob_by_object_id)
                    .with_state((self.client.clone(), self.allowed_headers.clone())),
            )
            .route(
                QUILT_BLOBS_ENDPOINT,
                get(routes::get_blob_in_quilt)
                    .with_state((self.client.clone(), self.allowed_headers.clone())),
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
        network_address: SocketAddr,
        max_body_limit: usize,
        registry: &Registry,
        max_request_buffer_size: usize,
        max_concurrent_requests: usize,
    ) -> Self {
        Self::new::<PublisherApiDoc>(client, network_address, registry).with_publisher(
            auth_config,
            max_body_limit,
            max_request_buffer_size,
            max_concurrent_requests,
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
            .with_aggregator(aggregator_args.allowed_headers.clone())
            .with_publisher(
                auth_config,
                publisher_args.max_body_size_kib,
                publisher_args.max_request_buffer_size,
                publisher_args.max_concurrent_requests,
            )
    }

    /// Specifies that the daemon should expose the publisher interface (store blobs).
    fn with_publisher(
        mut self,
        auth_config: Option<AuthConfig>,
        max_body_limit: usize,
        max_request_buffer_size: usize,
        max_concurrent_requests: usize,
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
            self.router = self.router.route(
                BLOB_PUT_ENDPOINT,
                put(routes::put_blob).route_layer(
                    ServiceBuilder::new()
                        .layer(axum::middleware::from_fn_with_state(
                            (Arc::new(auth_config), Arc::new(replay_suppression_cache)),
                            auth_layer,
                        ))
                        .layer(base_layers),
                ),
            );
        } else {
            self.router = self.router.route(
                BLOB_PUT_ENDPOINT,
                put(routes::put_blob).route_layer(base_layers),
            );
        }
        self
    }
}

impl<T> ClientDaemon<T> {
    fn with_allowed_headers(&mut self, allowed_headers: Vec<String>) {
        self.allowed_headers = Arc::new(allowed_headers.into_iter().collect());
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
