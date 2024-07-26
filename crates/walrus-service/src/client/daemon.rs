// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A client daemon who serves a set of simple HTTP endpoints to store, encode, or read blobs.

use std::{net::SocketAddr, sync::Arc};

use axum::{
    extract::DefaultBodyLimit,
    routing::{get, put},
    Router,
};
use openapi::{AggregatorApiDoc, DaemonApiDoc, PublisherApiDoc};
use routes::{BLOB_GET_ENDPOINT, BLOB_PUT_ENDPOINT};
use tower_http::trace::TraceLayer;
use utoipa::OpenApi;
use utoipa_redoc::{Redoc, Servable};
use walrus_sui::client::ContractClient;

use super::Client;

mod openapi;
mod routes;

/// The client daemon.
///
/// Exposes different HTTP endpoints depending on which function `ClientDaemon::new_*` it is
/// constructed with.
#[derive(Debug, Clone)]
pub struct ClientDaemon<T> {
    client: Arc<Client<T>>,
    network_address: SocketAddr,
    router: Router<Arc<Client<T>>>,
}

impl<T: Send + Sync + 'static> ClientDaemon<T> {
    /// Constructs a new [`ClientDaemon`] with aggregator functionality.
    pub fn new_aggregator(client: Client<T>, network_address: SocketAddr) -> Self {
        Self::new::<AggregatorApiDoc>(client, network_address).with_aggregator()
    }

    /// Creates a new [`ClientDaemon`], which serves requests at the provided `network_address` and
    /// interacts with Walrus through the `client`.
    ///
    /// The exposed APIs can be defined by calling a subset of the functions `with_*`. The daemon is
    /// started through [`Self::run()`].
    fn new<A: OpenApi>(client: Client<T>, network_address: SocketAddr) -> Self {
        ClientDaemon {
            client: Arc::new(client),
            network_address,
            router: Router::new().merge(Redoc::with_url(routes::API_DOCS, A::openapi())),
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
        axum::serve(
            listener,
            self.router
                .with_state(self.client)
                .layer(TraceLayer::new_for_http()),
        )
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
    }
}

impl<T: ContractClient + 'static> ClientDaemon<T> {
    /// Constructs a new [`ClientDaemon`] with publisher functionality.
    pub fn new_publisher(
        client: Client<T>,
        network_address: SocketAddr,
        max_body_limit: usize,
    ) -> Self {
        Self::new::<PublisherApiDoc>(client, network_address).with_publisher(max_body_limit)
    }

    /// Constructs a new [`ClientDaemon`] with combined aggregator and publisher functionality.
    pub fn new_daemon(
        client: Client<T>,
        network_address: SocketAddr,
        max_body_limit: usize,
    ) -> Self {
        Self::new::<DaemonApiDoc>(client, network_address)
            .with_aggregator()
            .with_publisher(max_body_limit)
    }

    /// Specifies that the daemon should expose the publisher interface (store blobs).
    fn with_publisher(mut self, max_body_limit: usize) -> Self {
        self.router = self.router.route(
            BLOB_PUT_ENDPOINT,
            put(routes::put_blob)
                .route_layer(DefaultBodyLimit::max(max_body_limit))
                .options(routes::store_blob_options),
        );
        self
    }
}
