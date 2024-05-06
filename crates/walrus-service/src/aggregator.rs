// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The aggregator (cache) to serve blobs from an HTTP server.

use std::{net::SocketAddr, sync::Arc};

use axum::{
    extract::{Path, State},
    http::{HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use reqwest::header::ACCESS_CONTROL_ALLOW_ORIGIN;
use tower_http::trace::TraceLayer;
use walrus_core::encoding::Primary;

use crate::{
    client::{string_prefix, Client},
    server::BlobIdString,
};

/// The path to get the blob at the given blob Id.
pub const BLOB_ENDPOINT: &str = "/v1/:blobId";

/// The aggregator server
///
/// Exposes and HTTP endpoint to request blobs by blob ID.
#[derive(Debug)]
pub struct AggregatorServer<T> {
    storage_client: Arc<Client<T>>,
}

impl<T: Send + Sync + 'static> AggregatorServer<T> {
    /// Creates a new aggregator.
    pub fn new(storage_client: Arc<Client<T>>) -> Self {
        Self { storage_client }
    }

    /// Runs the aggregator.
    pub async fn run(&self, network_address: &SocketAddr) -> Result<(), std::io::Error> {
        let app = Router::new()
            .route(BLOB_ENDPOINT, get(Self::retrieve_blob))
            .with_state(self.storage_client.clone())
            .layer(TraceLayer::new_for_http());

        let listener = tokio::net::TcpListener::bind(network_address).await?;
        tracing::info!(?network_address, "the aggregator is starting");
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = tokio::signal::ctrl_c().await;
            })
            .await
    }

    async fn retrieve_blob(
        State(client): State<Arc<Client<T>>>,
        Path(BlobIdString(blob_id)): Path<BlobIdString>,
    ) -> Response {
        match client.read_blob::<Primary>(&blob_id).await {
            Ok(blob) => {
                tracing::debug!(
                    blob_id_prefix=?string_prefix(&blob_id),
                    "successfully retrieved blob"
                );
                let mut response = (StatusCode::OK, blob).into_response();
                // Allow requests from any origin, s.t. content can be loaded in browsers.
                response
                    .headers_mut()
                    .insert(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
                response
            }
            Err(e) => {
                tracing::error!(
                    error=?e,
                    blob_id_prefix=?string_prefix(&blob_id),
                    "error retrieving blob"
                );
                (StatusCode::NOT_FOUND, "Not found").into_response()
            }
        }
    }
}
