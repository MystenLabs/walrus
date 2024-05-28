// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A client daemon who serves a set of simple HTTP endpoints to store, encode, or read blobs.

use std::{net::SocketAddr, sync::Arc};

use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, put},
    Router,
};
use reqwest::header::ACCESS_CONTROL_ALLOW_ORIGIN;
use serde::Deserialize;
use tower_http::trace::TraceLayer;
use tracing::Level;
use walrus_core::encoding::Primary;
use walrus_sui::{client::ContractClient, types::Blob};

use crate::{
    client::{Client, ClientErrorKind},
    server::BlobIdString,
};

/// The path to get the blob with the given blob ID.
pub const BLOB_GET_ENDPOINT: &str = "/v1/:blobId";

/// The path to store a blob.
pub const BLOB_PUT_ENDPOINT: &str = "/v1/store";

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
        self.router = self.router.route(BLOB_GET_ENDPOINT, get(retrieve_blob));
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
            put(store_blob).route_layer(DefaultBodyLimit::max(max_body_limit)),
        );
        self
    }
}

#[tracing::instrument(level = Level::ERROR, skip_all, fields(%blob_id))]
async fn retrieve_blob<T: Send + Sync>(
    State(client): State<Arc<Client<T>>>,
    Path(BlobIdString(blob_id)): Path<BlobIdString>,
) -> Response {
    tracing::debug!("starting to read blob");
    match client.read_blob::<Primary>(&blob_id).await {
        Ok(blob) => {
            tracing::debug!("successfully retrieved blob");
            let mut response = (StatusCode::OK, blob).into_response();
            // Allow requests from any origin, s.t. content can be loaded in browsers.
            response
                .headers_mut()
                .insert(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
            response
        }
        Err(error) => match error.kind() {
            ClientErrorKind::BlobIdDoesNotExist => {
                tracing::info!(?blob_id, "the requested blob ID does not exist");
                StatusCode::NOT_FOUND.into_response()
            }
            _ => {
                tracing::error!(%error, "error retrieving blob");
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        },
    }
}

#[tracing::instrument(level = Level::ERROR, skip_all, fields(%epochs))]
async fn store_blob<T: ContractClient>(
    State(client): State<Arc<Client<T>>>,
    Query(PublisherQuery { epochs }): Query<PublisherQuery>,
    blob: Bytes,
) -> Response {
    tracing::debug!("starting to store received blob");
    match client.reserve_and_store_blob(&blob[..], epochs).await {
        Ok(Blob { blob_id, .. }) => {
            tracing::debug!(
                %blob_id, "successfully stored blob"
            );
            (StatusCode::OK, blob_id.to_string()).into_response()
        }
        Err(error) => {
            tracing::error!(%error, "error storing blob");
            match error.kind() {
                ClientErrorKind::NotEnoughConfirmations(_, _) => {
                    StatusCode::GATEWAY_TIMEOUT.into_response()
                }
                _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct PublisherQuery {
    #[serde(default = "default_epochs")]
    epochs: u64,
}

fn default_epochs() -> u64 {
    1
}
