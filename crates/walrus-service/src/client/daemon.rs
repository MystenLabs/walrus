// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A client daemon who serves a set of simple HTTP endpoints to store, encode, or read blobs.

use std::{net::SocketAddr, sync::Arc};

use axum::{
    extract::DefaultBodyLimit,
    routing::{get, put},
    Router,
};
use routes::{BLOB_GET_ENDPOINT, BLOB_PUT_ENDPOINT};
use tower_http::trace::TraceLayer;
use walrus_sui::client::ContractClient;

use crate::client::Client;

mod openapi;
mod routes;

/// The client daemon.
///
/// Exposes different HTTP endpoints depending on which functions `with_*` were applied after
/// constructing it with [`ClientDaemon::new`].
#[derive(Debug, Clone)]
pub struct ClientDaemon<T> {
    client: Arc<Client<T>>,
    network_address: SocketAddr,
    router: Router<Arc<Client<T>>>,
}

impl<T: Send + Sync + 'static> ClientDaemon<T> {
    /// Creates a new [`ClientDaemon`], which serves requests at the provided `network_address` and
    /// interacts with Walrus through the `client`.
    ///
    /// The exposed APIs can be defined by calling a subset of the functions `with_*`. The daemon is
    /// started through [`Self::run()`].
    pub fn new(client: Client<T>, network_address: SocketAddr) -> Self {
        ClientDaemon {
            client: Arc::new(client),
            network_address,
            router: Router::new(),
        }
    }

    /// Specifies that the daemon should expose the aggregator interface (read blobs).
    pub fn with_aggregator(mut self) -> Self {
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
    /// Specifies that the daemon should expose the publisher interface (store blobs).
    pub fn with_publisher(mut self, max_body_limit: usize) -> Self {
        self.router = self.router.route(
            BLOB_PUT_ENDPOINT,
            put(routes::put_blob)
                .route_layer(DefaultBodyLimit::max(max_body_limit))
                .options(routes::store_blob_options),
        );
        self
    }
}
