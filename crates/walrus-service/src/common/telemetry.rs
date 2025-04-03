// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Telemetry utilities for instrumenting walrus services.

// Not all functions here are used in every feature.
#![allow(dead_code)]

use std::{
    net::{IpAddr, SocketAddr},
    str::FromStr as _,
    sync::{
        atomic::{AtomicU64 as StdAtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use axum::{
    body::{Body, Bytes, HttpBody},
    extract::{ConnectInfo, MatchedPath, State},
    http::{
        self,
        header::{self, AsHeaderName},
        uri::Scheme,
        Request,
        Version,
    },
    middleware,
};
use opentelemetry::propagation::Extractor;
use prometheus::{
    core::{AtomicU64, Collector, GenericGauge},
    Histogram,
    HistogramVec,
    IntGauge,
    IntGaugeVec,
    Opts,
    Registry,
};
use reqwest::Method;
use tokio::time::Instant;
use tokio_metrics::TaskMonitor;
use tower_http::trace::{MakeSpan, OnResponse};
use tracing::{field, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use walrus_core::Epoch;
use walrus_utils::{
    http::{http_body::Frame, BodyVisitor, VisitBody},
    metrics::TaskMonitorFamily,
};

use super::active_committees::ActiveCommittees;

/// Route string used in metrics for invalid routes.
pub(crate) const UNMATCHED_ROUTE: &str = "invalid-route";

const HTTP_RESPONSE_PART_HEADERS: &str = "headers";
const HTTP_RESPONSE_PART_PAYLOAD: &str = "payload";

walrus_utils::metrics::define_metric_set! {
    #[namespace = "http_server"]
    /// Metrics reported by the HTTP server.
    ///
    /// Fields are those suggested by [OTel].
    ///
    /// [OTel]: https://opentelemetry.io/docs/specs/semconv/http/http-metrics/#http-server
    pub(crate) struct HttpServerMetrics {
        #[help = "Number of active HTTP server requests"]
        active_requests: IntGaugeVec["http_request_method", "url_scheme", "http_route"],

        #[help = "Time (in seconds) spent processing requests and serving the response."]
        request_duration_seconds: HistogramVec {
            labels: [
                "http_request_method",
                "url_scheme",
                "http_route",
                "network_protocol_version",
                "error_type",
                "http_response_status_code",
                // If "headers", time from request in to response headers being provided. If
                // "payload", time from response headers to the response body being consumed to be
                // written to the wire.
                "http_response_part"
            ],
            buckets: vec![
                0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0
            ],
        },

        #[help = "The size in bytes of the (compressed) request body."]
        request_body_size_bytes: HistogramVec{
            labels: [
                "http_request_method",
                "url_scheme",
                "http_route",
                "network_protocol_version",
                "error_type",
                "http_response_status_code",
            ],
            buckets: walrus_utils::metrics::default_buckets_for_bytes()
        },

        #[help = "The size in bytes of the (compressed) response body."]
        response_body_size_bytes: HistogramVec{
            labels: [
                "http_request_method",
                "url_scheme",
                "http_route",
                "network_protocol_version",
                "error_type",
                "http_response_status_code",
            ],
            buckets: walrus_utils::metrics::default_buckets_for_bytes()
        }
    }
}

/// Struct to generate new [`tracing::Span`]s for HTTP requests.
#[derive(Debug, Clone, Default)]
pub(crate) struct MakeHttpSpan;

impl MakeHttpSpan {
    /// Creates a new `MakeHttpSpan` instance.
    pub(crate) fn new() -> MakeHttpSpan {
        Self
    }

    fn make_span<B>(&mut self, request: &Request<B>) -> Span {
        let route = self.get_route(request);
        let span = tracing::info_span!(
            parent: &Span::current(),
            "rest_api",
            // Overrides the exported span name to "{http.request.method} {http.route}"
            "otel.name" = format!("{} {}", request.method(), route),
            "otel.kind" = "SERVER",
            "http.request.method" = %request.method(),
            "http.route" = route,
            "url.full" = %request.uri(),
            "url.path" = request.uri().path(),
            "url.scheme" = "http",  // TODO(jsmith): Identify HTTPS once enabled (#609)
            // Dynamically added to the span:
            "server.port" = field::Empty,
            "server.address" = field::Empty,
            "url.query" = field::Empty,
            "client.address" = field::Empty,
            "network.peer.address" = field::Empty,
            "network.peer.port" = field::Empty,
            "user_agent.original" = field::Empty,
            "network.protocol.version" = field::Empty,
            // Populated later with details of the response:
            "error.type" = field::Empty,
            "http.response.status_code" = field::Empty,
            "otel.status_code" = field::Empty,
        );

        self.propagate_context(request, &span);
        let peer_ip = self.record_remote_address(request, &span);
        self.record_client_address(request, &span, peer_ip);
        self.record_server_address(request, &span);
        self.record_url_query(request, &span);
        self.record_user_agent(request, &span);
        self.record_network_protocol_version(request, &span);

        span
    }

    fn get_route<B>(&self, request: &Request<B>) -> String {
        if let Some(path) = request.extensions().get::<MatchedPath>() {
            path.as_str().into()
        } else {
            UNMATCHED_ROUTE.into()
        }
    }

    /// Record the address of the entity on the other end of the TCP connection.
    fn record_remote_address<B>(&self, request: &Request<B>, span: &Span) -> Option<IpAddr> {
        if let Some(peer_address) = request.extensions().get::<ConnectInfo<SocketAddr>>() {
            span.record("network.peer.address", field::display(peer_address.ip()));
            span.record("network.peer.port", peer_address.port());

            Some(peer_address.ip())
        } else {
            None
        }
    }

    /// Record the client address, which may be different from the peer address.
    fn record_client_address<B>(
        &self,
        _request: &Request<B>,
        span: &Span,
        peer_ip: Option<IpAddr>,
    ) {
        // Check the standard Forwarded header for the client's address, and fallback to the
        // de-facto but non-standardized X-Forwarded-For header, before accepting that the peer is
        // also the client.
        // TODO(jsmith): Parse the forwarded headers to report the client behind proxies (#609)
        if let Some(addr) = peer_ip {
            span.record("client.address", field::display(addr));
        }
    }

    /// Record the server's address as the host identified in the header, or forwarded by a proxy.
    fn record_server_address<B>(&self, request: &Request<B>, span: &Span) {
        // Set the server address based on
        // https://opentelemetry.io/docs/specs/semconv/http/http-spans/
        //
        // Should be the first of:
        // - The original host which may be passed by the reverse proxy in the Forwarded#host,
        //   X-Forwarded-Host, or a similar header.
        // - The :authority pseudo-header in case of HTTP/2 or HTTP/3
        // - The Host header.
        // TODO(jsmith): Handle forwarded hosts and HTTP/2 authority (#609)
        if let Some(host) = get_header_as_str(request, header::HOST) {
            // Attempt to parse as a socket address, otherwise assume it's just a domain name
            // or sole IP address.
            if let Ok(server_address) = SocketAddr::from_str(host) {
                span.record("server.address", field::display(server_address.ip()));
                span.record("server.port", server_address.port());
            } else {
                span.record("server.address", host);
            }
        }
    }

    fn record_url_query<B>(&self, request: &Request<B>, span: &Span) {
        if let Some(query) = request.uri().query() {
            span.record("url.query", query);
        }
    }

    fn record_user_agent<B>(&self, request: &Request<B>, span: &Span) {
        if let Some(user_agent) = get_header_as_str(request, header::USER_AGENT) {
            span.record("user_agent.original", user_agent);
        }
    }

    fn record_network_protocol_version<B>(&self, request: &Request<B>, span: &Span) {
        let version = match request.version() {
            Version::HTTP_09 => "0.9",
            Version::HTTP_10 => "1.0",
            Version::HTTP_11 => "1.1",
            Version::HTTP_2 => "2.0",
            Version::HTTP_3 => "3.0",
            _ => return,
        };
        span.record("network.protocol.version", version);
    }

    fn propagate_context<B>(&self, request: &Request<B>, span: &Span) {
        let parent_context = opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.extract(&HeaderExtractor(request.headers()))
        });
        span.set_parent(parent_context);
    }
}

struct HeaderExtractor<'a>(pub &'a http::HeaderMap);

impl Extractor for HeaderExtractor<'_> {
    /// Get a value for a key from the HeaderMap.  If the value is not valid ASCII, returns None.
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    /// Collect all the keys from the HeaderMap.
    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .map(|value| value.as_str())
            .collect::<Vec<_>>()
    }
}

impl<B> MakeSpan<B> for MakeHttpSpan {
    fn make_span(&mut self, request: &Request<B>) -> Span {
        self.make_span::<B>(request)
    }
}

/// Marks the wrapped error as an internal error to add corresponding log entries.
#[derive(Debug, Clone)]
pub(crate) struct InternalError(pub(crate) Arc<dyn std::error::Error + Sync + Send + 'static>);

impl<B> OnResponse<B> for MakeHttpSpan {
    fn on_response(self, response: &http::Response<B>, _: Duration, span: &Span) {
        let status_code = response.status();
        // At the server, only 500 errors result in span errors
        // https://opentelemetry.io/docs/specs/semconv/http/http-spans/#status
        let is_error = status_code.is_server_error();

        span.record("otel.status_code", if is_error { "ERROR" } else { "OK" });
        span.record("http.response.status_code", status_code.as_str());

        if is_error {
            span.record("error.type", status_code.as_str());

            // If an `InternalError` extension is attached to the response then log it and add a
            // status message to the trace.
            if let Some(InternalError(error)) = response.extensions().get::<InternalError>() {
                span.in_scope(|| {
                    tracing::error!(?error);
                });
                span.record("otel.status_message", field::display(error));
            }
        }
    }
}

#[inline]
fn get_header_as_str<B, K: AsHeaderName>(request: &Request<B>, key: K) -> Option<&str> {
    request
        .headers()
        .get(key)
        .and_then(|value| value.to_str().ok())
}

fn http_version(request: &axum::extract::Request) -> &'static str {
    match request.version() {
        Version::HTTP_09 => "0.9",
        Version::HTTP_10 => "1.0",
        Version::HTTP_11 => "1.1",
        Version::HTTP_2 => "2",
        Version::HTTP_3 => "3",
        _ => "",
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MetricsMiddlewareState {
    inner: Arc<MetricsMiddlewareStateInner>,
}

impl MetricsMiddlewareState {
    pub fn new(registry: &Registry) -> Self {
        Self {
            inner: Arc::new(MetricsMiddlewareStateInner {
                metrics: HttpServerMetrics::new(registry),
                task_monitors: TaskMonitorFamily::new(registry.clone()),
            }),
        }
    }

    /// Returns the task monitor for the route and request, where `http_route` should be a
    /// known route or `UNMATCHED_ROUTE`.
    fn task_monitor(&self, method: Method, http_route: &str) -> TaskMonitor {
        self.inner
            .task_monitors
            .get_or_insert_with_task_name(&(method.clone(), http_route.to_owned()), || {
                format!("{} {}", method, http_route)
            })
    }

    fn metrics(&self) -> &HttpServerMetrics {
        &self.inner.metrics
    }
}

#[derive(Debug)]
struct MetricsMiddlewareStateInner {
    metrics: HttpServerMetrics,
    task_monitors: TaskMonitorFamily<(Method, String)>,
}

/// Middleware that records the elapsed time, HTTP method, and status of requests.
pub(crate) async fn metrics_middleware(
    State(state): State<MetricsMiddlewareState>,
    request: axum::extract::Request,
    next: middleware::Next,
) -> axum::response::Response {
    let metrics = state.metrics();
    // Manually record the time in seconds, since we do not yet know the status code which is
    // required to get the concrete histogram.
    let start = Instant::now();

    let http_request_method = request.method().clone();
    let http_route: String = if let Some(path) = request.extensions().get::<MatchedPath>() {
        path.as_str().into()
    } else {
        // We do not want to return the requested URI, as this would lead to a new histogram
        // for each rest to an invalid URI. Use a
        UNMATCHED_ROUTE.into()
    };
    let url_scheme = request.uri().scheme().cloned();
    let network_protocol_version = http_version(&request);

    let active_requests = walrus_utils::with_label!(
        metrics.active_requests,
        http_request_method.as_str(),
        url_scheme.as_ref().map(Scheme::as_str).unwrap_or_default(),
        http_route
    );
    active_requests.inc();

    // Observe the body size of the request, as we cannot always rely on `Content-Length`.
    let body_size_total = Arc::new(StdAtomicU64::default());
    let request = request.map(|body| {
        let body_size_total = body_size_total.clone();
        Body::new(VisitBody::new(
            body,
            move |maybe_result: Option<Result<&Frame<Bytes>, &axum::Error>>| {
                if let Some(data) = maybe_result.and_then(|r| r.ok()?.data_ref()) {
                    let data_len = u64::try_from(data.len()).expect("chunk length is within u64");
                    body_size_total.fetch_add(data_len, Ordering::Relaxed);
                }
            },
        ))
    });

    let monitor = state.task_monitor(http_request_method.clone(), &http_route);
    let response = monitor.instrument(next.run(request)).await;

    let response_available_at = Instant::now();
    let http_response_status_code = response.status();
    let error_type = if http_response_status_code.is_client_error()
        || http_response_status_code.is_server_error()
    {
        http_response_status_code.as_str()
    } else {
        ""
    };

    metrics
        .request_body_size_bytes
        .with_label_values(&[
            http_request_method.as_str(),
            url_scheme.as_ref().map(Scheme::as_str).unwrap_or_default(),
            &http_route,
            network_protocol_version,
            error_type,
            http_response_status_code.as_str(),
        ])
        .observe(body_size_total.load(Ordering::Relaxed) as f64);

    let request_duration = metrics.request_duration_seconds.with_label_values(&[
        http_request_method.as_str(),
        url_scheme.as_ref().map(Scheme::as_str).unwrap_or_default(),
        &http_route,
        network_protocol_version,
        error_type,
        http_response_status_code.as_str(),
        HTTP_RESPONSE_PART_HEADERS,
    ]);
    request_duration.observe(response_available_at.duration_since(start).as_secs_f64());

    let exact_response_body_size = response.body().size_hint().exact();
    let request_duration = metrics.request_duration_seconds.with_label_values(&[
        http_request_method.as_str(),
        url_scheme.as_ref().map(Scheme::as_str).unwrap_or_default(),
        &http_route,
        network_protocol_version,
        error_type,
        http_response_status_code.as_str(),
        HTTP_RESPONSE_PART_PAYLOAD,
    ]);
    let response_body_size = metrics.response_body_size_bytes.with_label_values(&[
        http_request_method.as_str(),
        url_scheme.as_ref().map(Scheme::as_str).unwrap_or_default(),
        &http_route,
        network_protocol_version,
        error_type,
        http_response_status_code.as_str(),
    ]);

    response.map(move |body| {
        Body::new(VisitBody::new(
            body,
            ResponseBodyVisitor {
                active_requests,
                request_duration,
                response_body_size,
                response_available_at,
                exact_response_body_size,
                observed_response_body_size: 0,
            },
        ))
    })
}

struct ResponseBodyVisitor {
    active_requests: IntGauge,
    request_duration: Histogram,
    response_body_size: Histogram,
    response_available_at: Instant,
    exact_response_body_size: Option<u64>,
    observed_response_body_size: u64,
}

impl<E> BodyVisitor<Bytes, E> for ResponseBodyVisitor {
    fn frame_polled(&mut self, maybe_result: Option<Result<&Frame<Bytes>, &E>>) {
        if let Some(data) = maybe_result.and_then(|r| r.ok()?.data_ref()) {
            let data_len = u64::try_from(data.len()).expect("chunk length is within u64");
            self.observed_response_body_size += data_len;
        }
    }

    fn body_dropped(&mut self, _is_end_stream: bool) {
        self.active_requests.dec();
        self.request_duration
            .observe(self.response_available_at.elapsed().as_secs_f64());
        self.response_body_size.observe(
            self.exact_response_body_size
                .unwrap_or(self.observed_response_body_size) as f64,
        );
    }
}

/// Metric `current_epoch` that records the currently observed walrus epoch.
///
/// Set the epoch with [`Self::set`].
#[derive(Debug, Clone)]
pub(crate) struct CurrentEpochMetric(GenericGauge<AtomicU64>);

impl CurrentEpochMetric {
    pub fn new() -> Self {
        Self(walrus_utils::metrics::create_metric!(
            GenericGauge<AtomicU64>,
            Opts::new("current_epoch", "The current Walrus epoch").namespace("walrus"),
            []
        ))
    }

    /// Sets the currently observed epoch.
    pub fn set(&self, epoch: Epoch) {
        self.0.set(epoch.into())
    }
}

impl Default for CurrentEpochMetric {
    fn default() -> Self {
        Self::new()
    }
}

impl From<CurrentEpochMetric> for Box<dyn Collector> {
    fn from(value: CurrentEpochMetric) -> Self {
        Box::new(value.0)
    }
}

/// Metric `current_epoch_state` that tracks the current epoch state.
///
/// Use the `set_*` methods to change the observed state.
#[derive(Debug, Clone)]
pub(crate) struct CurrentEpochStateMetric(IntGaugeVec);

impl CurrentEpochStateMetric {
    const CHANGE_SYNC: &str = "epoch_change_sync";
    const CHANGE_DONE: &str = "epoch_change_done";
    const NEXT_PARAMS_SELECTED: &str = "next_params_selected";

    /// Returns a new, unregistered instance of the metric.
    pub fn new() -> Self {
        let opts = Opts::new(
            "current_epoch_state",
            "The state of the current walrus epoch",
        )
        .namespace("walrus");
        let metric = walrus_utils::metrics::create_metric!(IntGaugeVec, opts, ["state"]);
        Self(metric)
    }

    /// Record the current state based on the set of active committees.
    pub fn set_from_committees(&self, committees: &ActiveCommittees) {
        if committees.is_change_in_progress() {
            self.set_change_sync_state();
        } else if committees.next_committee().is_none() {
            // Change not in progress and next committee has not yet been decided.
            self.set_change_done_state();
        } else {
            // Next committee has been decided.
            self.set_next_params_selected_state();
        }
    }

    /// Record the current state as being `EpochState::EpochChangeSync`.
    pub fn set_change_sync_state(&self) {
        self.clear_state();
        walrus_utils::with_label!(self.0, Self::CHANGE_SYNC).set(true.into());
    }

    /// Record the current state as being `EpochState::EpochChangeDone`.
    pub fn set_change_done_state(&self) {
        self.clear_state();
        walrus_utils::with_label!(self.0, Self::CHANGE_DONE).set(true.into());
    }

    /// Record the current state as being `EpochState::NextParamsSelected`.
    pub fn set_next_params_selected_state(&self) {
        self.clear_state();
        walrus_utils::with_label!(self.0, Self::NEXT_PARAMS_SELECTED).set(true.into());
    }

    fn clear_state(&self) {
        walrus_utils::with_label!(self.0, Self::CHANGE_SYNC).set(false.into());
        walrus_utils::with_label!(self.0, Self::CHANGE_DONE).set(false.into());
        walrus_utils::with_label!(self.0, Self::NEXT_PARAMS_SELECTED).set(false.into());
    }
}

impl Default for CurrentEpochStateMetric {
    fn default() -> Self {
        Self::new()
    }
}

impl From<CurrentEpochStateMetric> for Box<dyn Collector> {
    fn from(value: CurrentEpochStateMetric) -> Self {
        Box::new(value.0)
    }
}
