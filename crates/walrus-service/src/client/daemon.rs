// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A client daemon who serves a set of simple HTTP endpoints to store, encode, or read blobs.

use std::{net::SocketAddr, sync::Arc};

use axum::{
    error_handling::HandleErrorLayer,
    extract::DefaultBodyLimit,
    middleware,
    response::{IntoResponse, Response},
    routing::{get, put},
    BoxError,
    Router,
};
use openapi::{AggregatorApiDoc, DaemonApiDoc, PublisherApiDoc};
use prometheus::{HistogramVec, Registry};
use reqwest::StatusCode;
use routes::{BLOB_GET_ENDPOINT, BLOB_PUT_ENDPOINT, STATUS_ENDPOINT};
use tower::{
    buffer::BufferLayer,
    limit::ConcurrencyLimitLayer,
    load_shed::{error::Overloaded, LoadShedLayer},
    ServiceBuilder,
};
use tower_http::trace::TraceLayer;
use utoipa::OpenApi;
use utoipa_redoc::{Redoc, Servable};
use walrus_core::{encoding::Primary, BlobId, EpochCount};
use walrus_sui::client::{BlobPersistence, ReadClient, SuiContractClient};

use super::{responses::BlobStoreResult, Client, ClientResult, StoreWhen};
use crate::common::telemetry::{metrics_middleware, register_http_metrics, MakeHttpSpan};

mod openapi;
mod routes;

pub trait WalrusReadClient {
    fn read_blob(
        &self,
        blob_id: &BlobId,
    ) -> impl std::future::Future<Output = ClientResult<Vec<u8>>> + Send;
    fn set_metric_registry(&mut self, registry: &Registry);
}

pub trait WalrusWriteClient: WalrusReadClient {
    fn write_blob(
        &self,
        blob: &[u8],
        epochs_ahead: EpochCount,
        store_when: StoreWhen,
        persistence: BlobPersistence,
    ) -> impl std::future::Future<Output = ClientResult<BlobStoreResult>> + Send;
}

impl<T: ReadClient> WalrusReadClient for Client<T> {
    async fn read_blob(&self, blob_id: &BlobId) -> ClientResult<Vec<u8>> {
        self.read_blob_retry_epoch::<Primary>(blob_id).await
    }

    fn set_metric_registry(&mut self, registry: &Registry) {
        self.set_metric_registry(registry);
    }
}

impl WalrusWriteClient for Client<SuiContractClient> {
    async fn write_blob(
        &self,
        blob: &[u8],
        epochs_ahead: EpochCount,
        store_when: StoreWhen,
        persistence: BlobPersistence,
    ) -> ClientResult<BlobStoreResult> {
        self.reserve_and_store_blob_retry_epoch(blob, epochs_ahead, store_when, persistence)
            .await
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
    metrics: HistogramVec,
    router: Router<Arc<T>>,
}

impl<T: WalrusReadClient + Send + Sync + 'static> ClientDaemon<T> {
    /// Constructs a new [`ClientDaemon`] with aggregator functionality.
    pub fn new_aggregator(client: T, network_address: SocketAddr, registry: &Registry) -> Self {
        Self::new::<AggregatorApiDoc>(client, network_address, registry).with_aggregator()
    }

    /// Creates a new [`ClientDaemon`], which serves requests at the provided `network_address` and
    /// interacts with Walrus through the `client`.
    ///
    /// The exposed APIs can be defined by calling a subset of the functions `with_*`. The daemon is
    /// started through [`Self::run()`].
    fn new<A: OpenApi>(mut client: T, network_address: SocketAddr, registry: &Registry) -> Self {
        client.set_metric_registry(registry);
        ClientDaemon {
            client: Arc::new(client),
            network_address,
            metrics: register_http_metrics(registry),
            router: Router::new()
                .merge(Redoc::with_url(routes::API_DOCS, A::openapi()))
                .route(STATUS_ENDPOINT, get(routes::status)),
        }
    }

    /// Specifies that the daemon should expose the aggregator interface (read blobs).
    fn with_aggregator(mut self) -> Self {
        self.router = self.router.route(BLOB_GET_ENDPOINT, get(routes::get_blob));
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
            );

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
        network_address: SocketAddr,
        max_body_limit: usize,
        registry: &Registry,
        max_request_buffer_size: usize,
        max_concurrent_requests: usize,
    ) -> Self {
        Self::new::<PublisherApiDoc>(client, network_address, registry).with_publisher(
            max_body_limit,
            max_request_buffer_size,
            max_concurrent_requests,
        )
    }

    /// Constructs a new [`ClientDaemon`] with combined aggregator and publisher functionality.
    pub fn new_daemon(
        client: T,
        network_address: SocketAddr,
        max_body_limit: usize,
        registry: &Registry,
        max_request_buffer_size: usize,
        max_concurrent_requests: usize,
    ) -> Self {
        Self::new::<DaemonApiDoc>(client, network_address, registry)
            .with_aggregator()
            .with_publisher(
                max_body_limit,
                max_request_buffer_size,
                max_concurrent_requests,
            )
    }

    /// Specifies that the daemon should expose the publisher interface (store blobs).
    fn with_publisher(
        mut self,
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
        let publisher_layers = ServiceBuilder::new()
            .layer(DefaultBodyLimit::max(max_body_limit))
            .layer(HandleErrorLayer::new(handle_publisher_error))
            .layer(LoadShedLayer::new())
            .layer(BufferLayer::new(max_request_buffer_size))
            .layer(ConcurrencyLimitLayer::new(max_concurrent_requests));

        self.router = self.router.route(
            BLOB_PUT_ENDPOINT,
            put(routes::put_blob)
                .route_layer(publisher_layers)
                .options(routes::store_blob_options),
        );
        self
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
