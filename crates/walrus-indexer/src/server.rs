// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! REST API server for the Walrus Indexer.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::anyhow;
use axum::{
    Router,
    routing::{get, post},
};
use axum_server::Handle;
use futures::FutureExt;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tower::ServiceBuilder;
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};
use tracing::Instrument as _;
use walrus_utils::metrics::Registry;

use crate::{RestApiConfig, WalrusIndexer, routes};

/// Extended configuration for the indexer REST API with additional server-specific settings.
#[derive(Debug, Clone)]
pub struct IndexerRestApiServerConfig {
    /// The socket address on which the server should listen.
    pub bind_address: SocketAddr,
    
    /// Socket address on which the Prometheus server should export its metrics.
    pub metrics_address: SocketAddr,
    
    /// Duration for which to wait for connections to close, when shutting down the server.
    ///
    /// Zero waits indefinitely and None immediately closes the connections.
    pub graceful_shutdown_period: Option<Duration>,
}

impl From<&RestApiConfig> for IndexerRestApiServerConfig {
    fn from(config: &RestApiConfig) -> Self {
        Self {
            bind_address: config.bind_address,
            metrics_address: config.metrics_address,
            graceful_shutdown_period: Some(Duration::from_secs(30)),
        }
    }
}

/// State shared across all REST API routes.
#[derive(Clone)]
pub struct IndexerRestApiState {
    pub indexer: Arc<WalrusIndexer>,
    pub config: Arc<IndexerRestApiServerConfig>,
}

impl IndexerRestApiState {
    fn new(indexer: Arc<WalrusIndexer>, config: Arc<IndexerRestApiServerConfig>) -> Self {
        Self { indexer, config }
    }
}

/// REST API server for the Walrus Indexer.
pub struct IndexerRestApiServer {
    state: IndexerRestApiState,
    cancel_token: CancellationToken,
    handle: Mutex<Option<Handle>>,
}

impl IndexerRestApiServer {
    /// Creates a new REST API server for the indexer.
    pub fn new(
        indexer: Arc<WalrusIndexer>,
        cancel_token: CancellationToken,
        config: IndexerRestApiServerConfig,
        _registry: &Registry,
    ) -> Self {
        Self {
            state: IndexerRestApiState::new(indexer, Arc::new(config)),
            cancel_token,
            handle: Default::default(),
        }
    }

    /// Runs the server, may only be called once for a given instance.
    pub async fn run(&self) -> Result<(), anyhow::Error> {
        {
            let handle = self.handle.lock().await;
            assert!(handle.is_none(), "run can only be called once");
        }

        let request_layers = ServiceBuilder::new()
            .layer(
                TraceLayer::new_for_http()
                    .on_failure(())
                    .on_response(()),
            )
            .layer(Self::cors_layer());

        let app = self
            .define_routes()
            .with_state(self.state.clone())
            .layer(request_layers)
            .into_make_service_with_connect_info::<SocketAddr>();

        let handle = self.init_handle().await;

        // For now, we only support HTTP (no TLS)
        let server = axum_server::bind(self.config().bind_address).handle(handle.clone());

        tokio::spawn(
            Self::handle_shutdown_signal(
                handle,
                self.cancel_token.clone(),
                self.config().graceful_shutdown_period,
            )
            .in_current_span(),
        );

        server
            .serve(app)
            .inspect(|_| tracing::info!("indexer server run has completed"))
            .await
            .map_err(|error| anyhow!(error))
    }

    fn define_routes(&self) -> Router<IndexerRestApiState> {
        Router::new()
            .route("/health", get(routes::health))
            .route("/get_blob", get(routes::get_blob_handler))
            .route("/get_blob_by_object_id", get(routes::get_blob_by_object_id_handler))
            .route("/list_bucket", get(routes::list_bucket_handler))
            .route("/get_bucket_stats", get(routes::get_bucket_stats_handler))
            .route("/buckets", get(routes::list_all_buckets_handler))
            .route("/bucket", post(routes::create_bucket_handler))
    }

    fn cors_layer() -> CorsLayer {
        CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any)
    }

    async fn init_handle(&self) -> Handle {
        let handle = Handle::new();
        *self.handle.lock().await = Some(handle.clone());
        handle
    }

    async fn handle_shutdown_signal(
        handle: Handle,
        cancel_token: CancellationToken,
        shutdown_duration: Option<Duration>,
    ) {
        cancel_token.cancelled().await;

        match shutdown_duration {
            Some(Duration::ZERO) => {
                tracing::info!("immediately shutting down server");
                handle.shutdown();
            }
            Some(duration) => {
                tracing::info!(?duration, "gracefully shutting down server");
                handle.graceful_shutdown(Some(duration));
            }
            None => {
                tracing::info!("waiting indefinitely for server to shutdown");
                handle.graceful_shutdown(None);
            }
        }
    }

    fn config(&self) -> &IndexerRestApiServerConfig {
        &self.state.config
    }
}