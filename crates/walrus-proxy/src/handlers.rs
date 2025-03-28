// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
use axum::{extract::Extension, http::StatusCode};
use once_cell::sync::Lazy;
use prometheus::{CounterVec, HistogramOpts, HistogramVec, Opts};

use crate::{
    admin::ReqwestClient,
    consumer::{convert_to_remote_write, populate_labels, Label, NodeMetric},
    histogram_relay::HistogramRelay,
    middleware::LenDelimProtobuf,
    providers::NodeInfo,
    register_metric,
};

/// HANDLER_HITS is a counter vector for tracking number of HTTP requests handled by each handler.
///
/// # Panics
///
/// This will panic if the `CounterVec` cannot be created or registered.
/// This typically indicates a programming error or a conflicting metric name.
static HANDLER_HITS: Lazy<CounterVec> = Lazy::new(|| {
    register_metric!(CounterVec::new(
        Opts::new("http_handler_hits", "Number of HTTP requests made."),
        &["handler", "remote"]
    )
    .expect("Failed to create and register http_handler_hits CounterVec"))
});

/// HTTP_HANDLER_DURATION is a histogram vector for measuring HTTP request latencies.
///
/// # Panics
///
/// This will panic if the `HistogramVec` cannot be created or registered.
/// This typically indicates a programming error or a conflicting metric name.
static HTTP_HANDLER_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_metric!(HistogramVec::new(
        HistogramOpts::new(
            "http_handler_duration_seconds",
            "The HTTP request latencies in seconds.",
        )
        .buckets(vec![
            1.0, 1.25, 1.5, 1.75, 2.0, 2.25, 2.5, 2.75, 3.0, 3.25, 3.5, 3.75, 4.0, 4.25, 4.5, 4.75,
            5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0
        ]),
        &["handler", "remote"]
    )
    .expect("Failed to create and register http_handler_duration_seconds HistogramVec"))
});

/// Publish handler which receives metrics from nodes.  Nodes will call us at
/// this endpoint and we relay them to the upstream tsdb. Clients will receive
/// a response after successfully relaying the metrics upstream
pub async fn publish_metrics(
    Extension(labels): Extension<Vec<Label>>,
    Extension(remote_write_client): Extension<ReqwestClient>,
    Extension(NodeInfo {
        name,
        network_address,
        ..
    }): Extension<NodeInfo>,
    Extension(relay): Extension<HistogramRelay>,
    LenDelimProtobuf(data): LenDelimProtobuf,
) -> (StatusCode, &'static str) {
    walrus_utils::with_label!(HANDLER_HITS, "publish_metrics", &name).inc();
    let timer =
        walrus_utils::with_label!(HTTP_HANDLER_DURATION, "publish_metrics", &name).start_timer();
    let data = populate_labels(name, labels, data);
    relay.submit(data.clone());
    let response = convert_to_remote_write(
        remote_write_client.clone(),
        NodeMetric {
            data,
            network_address,
        },
    )
    .await;
    timer.observe_duration();
    response
}
