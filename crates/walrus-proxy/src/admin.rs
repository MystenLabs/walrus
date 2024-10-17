// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    consumer::Label,
    middleware::{
        expect_content_length, expect_mysten_proxy_header, expect_valid_recoverable_pubkey,
    },
};
use axum::{extract::DefaultBodyLimit, middleware, routing::post, Extension, Router};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use sui_proxy::histogram_relay::HistogramRelay;
use sui_proxy::var;
use tokio::signal;
use tower::ServiceBuilder;
use tower_http::{
    timeout::TimeoutLayer,
    trace::{DefaultOnFailure, DefaultOnResponse, TraceLayer},
    LatencyUnit,
};
use tracing::{info, Level};

use crate::config::RemoteWriteConfig;
use crate::handlers;
use crate::providers::WalrusNodeProvider;

/// Reqwest client holds the global client for remote_push api calls
/// it also holds the username and password.  The client has an underlying
/// connection pool.  See reqwest documentation for details
#[derive(Debug, Clone)]
pub struct ReqwestClient {
    /// client pool builder for connections to mimir
    pub client: reqwest::Client,
    /// settings for remote write connection
    pub settings: RemoteWriteConfig,
}

/// make a reqwest client to connect to mimir
pub fn make_reqwest_client(settings: RemoteWriteConfig, user_agent: &str) -> ReqwestClient {
    ReqwestClient {
        client: reqwest::Client::builder()
            .user_agent(user_agent)
            .pool_max_idle_per_host(settings.pool_max_idle_per_host)
            .timeout(Duration::from_secs(var!("MIMIR_CLIENT_TIMEOUT", 30)))
            .build()
            .expect("cannot create reqwest client"),
        settings,
    }
}

/// build our axum app
pub fn app(
    labels: Vec<Label>,
    client: ReqwestClient,
    relay: HistogramRelay,
    allower: Option<WalrusNodeProvider>,
) -> Router {
    // build our application with a route and our sender mpsc
    let mut router = Router::new()
        .route("/publish/metrics", post(handlers::publish_metrics))
        .route_layer(DefaultBodyLimit::max(var!(
            "MAX_BODY_SIZE",
            1024 * 1024 * 5
        )))
        .route_layer(middleware::from_fn(expect_mysten_proxy_header))
        .route_layer(middleware::from_fn(expect_content_length));
    if let Some(allower) = allower {
        router = router
            .route_layer(middleware::from_fn(expect_valid_recoverable_pubkey))
            .layer(Extension(Arc::new(allower)));
    }
    router
        // Enforce on all routes.
        // If the request does not complete within the specified timeout it will be aborted
        // and a 408 Request Timeout response will be sent.
        .layer(TimeoutLayer::new(Duration::from_secs(var!(
            "NODE_CLIENT_TIMEOUT",
            20
        ))))
        .layer(Extension(relay))
        .layer(Extension(labels))
        .layer(Extension(client))
        .layer(
            ServiceBuilder::new().layer(
                TraceLayer::new_for_http()
                    .on_response(
                        DefaultOnResponse::new()
                            .level(Level::INFO)
                            .latency_unit(LatencyUnit::Seconds),
                    )
                    .on_failure(
                        DefaultOnFailure::new()
                            .level(Level::ERROR)
                            .latency_unit(LatencyUnit::Seconds),
                    ),
            ),
        )
}

/// Server creates our http/https server
pub async fn server(listener: std::net::TcpListener, app: Router) -> std::io::Result<()> {
    // setup our graceful shutdown
    let handle = axum_server::Handle::new();
    // Spawn a task to gracefully shutdown server.
    tokio::spawn(shutdown_signal(handle.clone()));

    axum_server::Server::from_tcp(listener)
        .handle(handle)
        .serve(app.into_make_service_with_connect_info::<SocketAddr>())
        .await
}

/// Configure our graceful shutdown scenarios
pub async fn shutdown_signal(h: axum_server::Handle) {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    let grace = 30;
    info!(
        "signal received, starting graceful shutdown, grace period {} seconds, if needed",
        &grace
    );
    h.graceful_shutdown(Some(Duration::from_secs(grace)))
}
