// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    net::TcpListener,
    sync::{Arc, RwLock},
};

use axum::{extract::Extension, http::StatusCode, routing::get, Router};
use once_cell::sync::Lazy;
use prometheus::{IntCounter, Opts, Registry, TextEncoder};
use tower::ServiceBuilder;
use tower_http::{
    trace::{DefaultOnResponse, TraceLayer},
    LatencyUnit,
};
use tracing::Level;

// Walrus proxy prom registry, to avoid collisions with prometheus in some
// shared counters from other crates, namely sui-proxy
pub(crate) static WALRUS_PROXY_PROM_REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);

/// Function to access the registry
pub fn walrus_proxy_prom_registry() -> &'static Registry {
    &WALRUS_PROXY_PROM_REGISTRY
}
/// macro to register metrics into the walrus proxy (local) default registry
#[macro_export]
macro_rules! register_metric {
    ($metric:expr) => {{
        let m = $metric;
        $crate::metrics::walrus_proxy_prom_registry()
            .register(Box::new(m.clone()))
            .unwrap();
        m
    }};
}

const METRICS_ROUTE: &str = "/metrics";
const POD_HEALTH_ROUTE: &str = "/pod_health";
const POD_LIVENESS_ROUTE: &str = "/liveness";

type HealthCheckMetrics = Arc<RwLock<HealthCheck>>;

/// Do not access struct members without using HealthCheckMetrics to arc+mutex
#[derive(Debug)]
struct HealthCheck {
    // eg; consumer_operations_submitted{...}
    consumer_operations_submitted: f64,
}

/// HealthCheck contains fields we believe are interesting that say whether this
/// pod should be considered health.  do not use w/o using an arc+mutex
impl HealthCheck {
    fn new() -> Self {
        Self {
            consumer_operations_submitted: 0.0,
        }
    }
}

/// a simple uptime metric
fn uptime_metric(registry: &Registry) {
    // Define the uptime counter
    let opts = Opts::new("uptime_seconds", "Uptime in seconds");
    let uptime_counter = IntCounter::with_opts(opts).unwrap();

    // Register the counter with the registry
    registry.register(Box::new(uptime_counter.clone())).unwrap();

    // Spawn a background task to increment the uptime counter every second
    tokio::spawn(async move {
        loop {
            uptime_counter.inc();
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
    });
}

/// Creates a new http server that has as a sole purpose to expose
/// and endpoint that prometheus agent can use to poll for the metrics.
pub fn start_prometheus_server(listener: TcpListener) {
    let registry = walrus_proxy_prom_registry();

    uptime_metric(registry);

    let pod_health_data = Arc::new(RwLock::new(HealthCheck::new()));

    let app = Router::new()
        .route(METRICS_ROUTE, get(metrics))
        .route(POD_HEALTH_ROUTE, get(pod_health))
        .route(POD_LIVENESS_ROUTE, get(liveness))
        .layer(Extension(registry))
        .layer(Extension(pod_health_data.clone()))
        .layer(
            ServiceBuilder::new().layer(
                TraceLayer::new_for_http().on_response(
                    DefaultOnResponse::new()
                        .level(Level::INFO)
                        .latency_unit(LatencyUnit::Seconds),
                ),
            ),
        );

    tokio::spawn(async move {
        listener.set_nonblocking(true).unwrap();
        let listener = tokio::net::TcpListener::from_std(listener).unwrap();
        axum::serve(listener, app).await.unwrap();
    });
}

async fn metrics(
    Extension(registry_service): Extension<&Registry>,
    Extension(pod_health): Extension<HealthCheckMetrics>,
) -> (StatusCode, String) {
    let mut metric_families = registry_service.gather();
    metric_families.extend(prometheus::gather());

    if let Some(consumer_operations_submitted) = metric_families
        .iter()
        .filter_map(|v| {
            if v.get_name() == "consumer_operations_submitted" {
                // Expecting one metric, so return the first one, as it is the only one
                v.get_metric().first().map(|m| m.get_counter().get_value())
            } else {
                None
            }
        })
        .next()
    {
        pod_health
            .write()
            .expect("unable to write to pod health metrics")
            .consumer_operations_submitted = consumer_operations_submitted;
    };
    match TextEncoder.encode_to_string(&metric_families) {
        Ok(metrics) => (StatusCode::OK, metrics),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("unable to encode metrics: {error}"),
        ),
    }
}

/// liveness is called by k8s to know if this service is correctly processing
/// data. What this means is that if we do not receive any data, we can still
/// technically be alive and healthy but for one reason or another we're not
/// doing real work.  Properly configured liveness checks in k8s should be aware
/// of this behavior so as to give enoug time to the pod to receive data and
/// become healthy.  a typical system will converge in a few seconds under
/// steady state.
async fn liveness(Extension(pod_health): Extension<HealthCheckMetrics>) -> (StatusCode, String) {
    let consumer_operations_submitted = pod_health
        .read()
        .expect("unable to read pod health metrics")
        .consumer_operations_submitted;

    if consumer_operations_submitted > 0.0 {
        (StatusCode::OK, consumer_operations_submitted.to_string())
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            consumer_operations_submitted.to_string(),
        )
    }
}

/// pod_health is called by k8s to know if this service is alive and able to respond
async fn pod_health() -> StatusCode {
    StatusCode::OK
}
