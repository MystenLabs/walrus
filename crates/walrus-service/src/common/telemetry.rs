// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Telemetry utilities for instrumenting walrus services.

// Not all functions here are used in every feature.
#![allow(dead_code)]

use std::{
    net::{IpAddr, SocketAddr},
    str::FromStr as _,
    sync::Arc,
    time::Duration,
};

use axum::{
    body::Body,
    extract::{ConnectInfo, MatchedPath, State},
    http::{
        self,
        header::{self, AsHeaderName},
        Request,
    },
    middleware,
};
use futures::StreamExt;
use opentelemetry::propagation::Extractor;
use prometheus::{
    core::{AtomicU64, Collector, GenericGauge},
    register_histogram_vec_with_registry,
    HistogramVec,
    IntGaugeVec,
    Opts,
    Registry,
};
use tokio::time::Instant;
use tower_http::trace::{MakeSpan, OnResponse};
use tracing::{field, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use walrus_core::Epoch;

use super::active_committees::ActiveCommittees;

/// Route string used in metrics for invalid routes.
pub(crate) const UNMATCHED_ROUTE: &str = "invalid-route";

/// HTTP metrics for tracking request/response statistics
#[derive(Debug, Clone)]
pub(crate) struct HttpMetrics {
    pub(crate) duration: HistogramVec,
    pub(crate) request_size: HistogramVec,
    pub(crate) response_size: HistogramVec,
}

impl HttpMetrics {
    pub(crate) fn new(
        duration: HistogramVec,
        request_size: HistogramVec,
        response_size: HistogramVec,
    ) -> Self {
        Self {
            duration,
            request_size,
            response_size,
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
            http::version::Version::HTTP_09 => "0.9",
            http::version::Version::HTTP_10 => "1.0",
            http::version::Version::HTTP_11 => "1.1",
            http::version::Version::HTTP_2 => "2.0",
            http::version::Version::HTTP_3 => "3.0",
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

/// Middleware that records the elapsed time, HTTP method, and status of requests.
pub(crate) async fn metrics_middleware(
    State(metrics): State<HttpMetrics>,
    request: axum::extract::Request,
    next: middleware::Next,
) -> axum::response::Response {
    // Manually record the time in seconds, since we do not yet know the status code which is
    // required to get the concrete histogram.
    let start = Instant::now();
    let method = request.method().clone();
    let route: String = if let Some(path) = request.extensions().get::<MatchedPath>() {
        path.as_str().into()
    } else {
        // We do not want to return the requested URI, as this would lead to a new histogram
        // for each rest to an invalid URI. Use a
        UNMATCHED_ROUTE.into()
    };
    // Track request size by reading the body
    let method_clone = method.clone();
    let route_clone = route.clone();
    let (parts, body) = request.into_parts();
    let counted_body = Body::from_stream(body.into_data_stream().map(move |chunk| {
        chunk.inspect(|c| {
            walrus_utils::with_label!(metrics.request_size, method_clone.as_str(), &route_clone)
                .observe(c.len() as f64);
        })
    }));
    let request = Request::from_parts(parts, counted_body);

    let response = next.run(request).await;

    // Record response metrics
    let method_clone = method.clone();
    let route_clone = route.clone();
    let status = response.status();
    let (parts, body) = response.into_parts();
    let counted_body = Body::from_stream(body.into_data_stream().map(move |chunk| {
        chunk.inspect(|c| {
            walrus_utils::with_label!(
                metrics.response_size,
                method_clone.as_str(),
                &route_clone,
                status.as_str()
            )
            .observe(c.len() as f64);
        })
    }));
    let response = http::Response::from_parts(parts, counted_body);

    walrus_utils::with_label!(metrics.duration, method.as_str(), &route, status.as_str())
        .observe(start.elapsed().as_secs_f64());

    response
}

/// Registers the HTTP request method, route, status, and durations metrics.
pub(crate) fn register_http_metrics(registry: &Registry) -> HttpMetrics {
    let duration_opts = prometheus::Opts::new(
        "request_duration_seconds",
        "Time (in seconds) spent serving HTTP requests.",
    )
    .namespace("http");

    let request_size_opts = prometheus::Opts::new(
        "request_size_bytes_total",
        "Total size of HTTP requests in bytes.",
    )
    .namespace("http");

    let response_size_opts = prometheus::Opts::new(
        "response_size_bytes_total",
        "Total size of HTTP responses in bytes.",
    )
    .namespace("http");

    let duration_histogram = register_histogram_vec_with_registry!(
        duration_opts.into(),
        &["method", "route", "status_code"],
        registry
    )
    .expect("metric registration must not fail");

    let request_size_histogram = register_histogram_vec_with_registry!(
        request_size_opts.into(),
        &["method", "route"],
        registry
    )
    .expect("metric registration must not fail");

    let response_size_histogram = register_histogram_vec_with_registry!(
        response_size_opts.into(),
        &["method", "route", "status_code"],
        registry
    )
    .expect("metric registration must not fail");

    HttpMetrics::new(
        duration_histogram,
        request_size_histogram,
        response_size_histogram,
    )
}

/// Defines a set of prometheus metrics.
///
/// # Example
///
/// ```
/// define_metric_set! {
///     /// Docstring applied to the containing struct.
///     struct MyMetricSet {
///         // Gauges, counters, and histograms can be defined with an empty `[]`.
///         #[help = "Help text and docstring for this metric"]
///         my_int_counter: IntCounter[],
///         #[help = "Help text for the my_histogram field"]
///         my_histogram: Histogram[],
///
///         // Vec-type metrics have their label names specified in the brackets.
///         #[help = "Help text for the int_counter_vec field"]
///         int_counter_vec: IntCounterVec["label1", "label2"],
///         #[help = "Help text for the my_histogram_vec field"]
///         my_histogram_vec: HistogramVec["label1", "label2"],
///
///         // `Histogram` and `HistogramVec` can additionally have their buckets specified.
///         #[help = "Help text for the my_histogram_with_buckets field"]
///         my_histogram_with_buckets: Histogram{buckets: vec![0.25, 1.0, 10.0]},
///         #[help = "Help text for the my_histogram_vec_with_buckets field"]
///         my_histogram_vec_with_buckets: HistogramVec{
///             labels: ["field1", "field2"], buckets: vec![1.0, 2.0]
///         },
///
///         // New-type metrics can be used to define metrics, and are any types that implement both
///         // `Default` and `Into<Box<dyn Collector>>`.
///         typed_metric: CurrentEpochMetric,
///     }
/// }
/// ```
macro_rules! define_metric_set {
    (
        $(#[$outer:meta])*
        struct $name:ident {
            $($new_type_field:ident: $new_type_field_type:ident,)*
            $(
                #[help = $help_str:literal]
                $field_name:ident: $field_type:ident $field_def:tt
            ),* $(,)?
        }
    ) => {
        $(#[$outer])*
        #[derive(Debug, Clone)]
        pub(crate) struct $name {
            $(
                #[doc = $help_str]
                pub $field_name: $field_type,
            )*
            $(
                pub $new_type_field: $new_type_field_type,
            )*
        }

        impl $name {
            pub fn new(registry: &Registry) -> Self {
                Self { $(
                    $field_name: {
                        let opts = Opts::new(stringify!($field_name), $help_str)
                            .namespace("walrus");
                        let metric = $crate::common::telemetry::create_metric!(
                            $field_type,
                            opts,
                            $field_def
                        );
                        registry
                            .register(Box::new(metric.clone()))
                            .expect("metrics defined at compile time must be valid");
                        metric
                    },
                )* $(
                    $new_type_field: {
                        let metric = $new_type_field_type::default();
                        registry
                            .register(metric.clone().into())
                            .expect("metrics defined at compile time must be valid");
                        metric
                    }
                ),* }
            }
        }
    };
}

pub(crate) use define_metric_set;

macro_rules! create_metric {
    ($field_type:ty, $opts:expr, []) => {{
        <$field_type>::with_opts($opts.into())
            .expect("this must be called with valid metrics type and options")
    }};
    ($field_type:ty, $opts:expr, [$($label_names:tt)+]) => {{
        <$field_type>::new($opts.into(), &[$($label_names)+])
            .expect("this must be called with valid metrics type and options")
    }};
    (Histogram, $opts:expr, {buckets: $buckets:expr}) => {{
        let mut opts: prometheus::HistogramOpts = $opts.into();
        opts.buckets = $buckets.into();

        prometheus::Histogram::with_opts(opts)
            .expect("this must be called with valid metrics type and options")
    }};
    (HistogramVec, $opts:expr, {labels: [$($label_names:tt)+], buckets: $buckets:expr $(,)?}) => {{
        let mut opts: prometheus::HistogramOpts = $opts.into();
        opts.buckets = $buckets.into();

        prometheus::HistogramVec::new(opts, &[$($label_names)+])
            .expect("this must be called with valid metrics type and options")
    }};
}

pub(crate) use create_metric;

/// Metric `current_epoch` that records the currently observed walrus epoch.
///
/// Set the epoch with [`Self::set`].
#[derive(Debug, Clone)]
pub(crate) struct CurrentEpochMetric(GenericGauge<AtomicU64>);

impl CurrentEpochMetric {
    pub fn new() -> Self {
        Self(create_metric!(
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
        let metric = create_metric!(IntGaugeVec, opts, ["state"]);
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
