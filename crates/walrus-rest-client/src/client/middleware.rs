// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    borrow::Cow,
    future::Future,
    pin::Pin,
    task::{self, Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use futures::future::FusedFuture;
use http_body::Body as _;
use opentelemetry::propagation::Injector;
use prometheus::{HistogramVec, IntGauge, IntGaugeVec};
use reqwest::{
    Error,
    Method,
    Request,
    Response,
    StatusCode,
    Url,
    Version,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use tokio::time::Instant;
use tower::Service;
use tracing::{Instrument as _, Span, field, instrument::Instrumented};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;
use walrus_utils::{
    http::{BodyVisitor, VisitBody, http_body},
    metrics::{self as metric_utils, OwnedGaugeGuard},
};

use self::helpers::ErrorType;

pub(crate) type RequestWithUrlTemplate = (Request, UrlTemplate);
pub(crate) struct UrlTemplate(pub &'static str); // Helps with incorrect lifetime inferences.

const HTTP_RESPONSE_PART_HEADERS: &str = "headers";
const HTTP_RESPONSE_PART_PAYLOAD: &str = "payload";

metric_utils::define_metric_set! {
    #[namespace = "http_client"]
    pub(crate) struct HttpClientMetrics {
        #[help = "Time (in seconds) sending the request and waiting for the response"]
        request_duration_seconds: HistogramVec {
            // If "headers", time from request in to response headers being provided. If
            // "payload", time from response headers to the response body being consumed to be
            // written to the wire.
            labels: metric_utils::concat_labels(
                &HttpLabels::LABEL_NAMES, &["http_response_part"]
            ),
            buckets: vec![
                0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0
            ],
        },

        #[help = "The size in bytes of the (compressed) request body."]
        request_body_size_bytes: HistogramVec {
            labels: HttpLabels::LABEL_NAMES,
            buckets: metric_utils::default_buckets_for_bytes(),
        },

        #[help = "The size in bytes of the (compressed) response body."]
        response_body_size_bytes: HistogramVec {
            labels: HttpLabels::LABEL_NAMES,
            buckets: metric_utils::default_buckets_for_bytes(),
        },

        #[help = "Number of active HTTP client requests"]
        active_requests: IntGaugeVec[
            "http_request_method",
            "server_address",
            "server_port",
            "url_scheme",
            "url_template",
        ],

    }
}

impl HttpClientMetrics {
    fn observe_request_duration(
        &self,
        duration: Duration,
        labels: &HttpLabels,
        http_response_part: &str,
    ) {
        let request_duration = &self.request_duration_seconds;

        let mut labels = labels.to_extended_array::<{ HttpLabels::LENGTH + 1 }>();
        labels[HttpLabels::LENGTH] = http_response_part;

        let histogram = request_duration
            .get_metric_with_label_values(&labels)
            .expect("label count is the same as definition");

        histogram.observe(duration.as_secs_f64());
    }

    fn observe_request_body_size(&self, body_size: u64, labels: &HttpLabels) {
        self.request_body_size_bytes
            .get_metric_with_label_values(&labels.to_array())
            .expect("label count is the same as definition")
            .observe(body_size as f64);
    }

    fn observe_response_body_size(&self, body_size: usize, labels: &HttpLabels) {
        self.request_body_size_bytes
            .get_metric_with_label_values(&labels.to_array())
            .expect("label count is the same as definition")
            .observe(body_size as f64);
    }

    fn active_requests(&self, labels: &HttpLabels) -> IntGauge {
        self.active_requests
            .get_metric_with_label_values(&[
                labels.http_request_method.as_str(),
                labels.server_address.as_str(),
                labels.server_port.as_str(),
                labels.url_scheme.as_ref(),
                labels.url_template,
            ])
            .expect("label count is the same as definition")
    }
}

/// Common label values for [`HttpClientMetrics`].
struct HttpLabels {
    http_request_method: Method,
    server_address: String,
    server_port: String,
    url_scheme: Cow<'static, str>,
    url_template: &'static str,
    network_protocol_version: Cow<'static, str>,
    error_type: Option<ErrorType>,
    http_response_status_code: Option<StatusCode>,
}

impl HttpLabels {
    const LENGTH: usize = 8;
    const LABEL_NAMES: [&str; Self::LENGTH] = [
        "http_request_method",
        "server_address",
        "server_port",
        "url_scheme",
        "url_template",
        "network_protocol_version",
        "error_type",
        "http_response_status_code",
    ];

    /// Returns a new [`HttpLabels`] with labels values populated from [`Request`].
    fn new(request: &Request, url_template: &'static str) -> Self {
        let url = request.url();

        Self {
            http_request_method: request.method().clone(),
            url_template,
            server_address: url.host_str().map(ToOwned::to_owned).unwrap_or_default(),
            server_port: url
                .port_or_known_default()
                .map(|p| p.to_string())
                .unwrap_or_default(),
            url_scheme: helpers::static_scheme(url),
            network_protocol_version: helpers::static_version(request.version()),
            error_type: None,
            http_response_status_code: None,
        }
    }

    /// Adds label values for the the provided output.
    ///
    /// A value of `None` indicates that the request was aborted, and the added
    /// label values reflect that.
    fn add_response_labels(&mut self, maybe_output: Option<Result<&Response, &reqwest::Error>>) {
        self.http_response_status_code = maybe_output.and_then(|output| {
            output.map_or_else(Error::status, |response| Some(response.status()))
        });
        self.error_type =
            match maybe_output.map(|result| result.map(Response::error_for_status_ref)) {
                Some(Ok(Ok(_))) => None,
                Some(Ok(Err(ref err))) | Some(Err(&ref err)) => {
                    if err.is_connect() {
                        Some(ErrorType::Connect)
                    } else if err.is_timeout() {
                        Some(ErrorType::Timeout)
                    } else if err.is_status() {
                        Some(ErrorType::StatusCode(
                            self.http_response_status_code
                                .expect("status code is present since the error was from a status"),
                        ))
                    } else {
                        Some(ErrorType::Other("NodeError".into()))
                    }
                }
                None => Some(ErrorType::Aborted),
            };
        if let Some(response) = maybe_output.and_then(|output| output.ok()) {
            self.network_protocol_version = helpers::static_version(response.version());
        }
    }

    /// Returns the labels values as an array of `&str` which can be used to get metrics.
    fn to_array(&self) -> [&str; Self::LENGTH] {
        self.to_extended_array()
    }

    /// Similar to [`Self::to_array`], but the length of the returned array can be specified to be
    /// longer than the minimum required to store the labels, allowing other label values to be
    /// post-pended.
    fn to_extended_array<const N: usize>(&self) -> [&str; N] {
        assert!(N >= Self::LENGTH, "`N` must be at least `Labels::LENGTH`");
        let mut array = [""; N];

        array[0] = self.http_request_method.as_str();
        array[1] = &self.server_address;
        array[2] = &self.server_port;
        array[3] = self.url_scheme.as_ref();
        array[4] = self.url_template;
        array[5] = self.network_protocol_version.as_ref();
        array[6] = self.error_type_as_str();
        array[7] = self
            .http_response_status_code
            .as_ref()
            .map(StatusCode::as_str)
            .unwrap_or_default();
        array
    }

    fn is_aborted(&self) -> bool {
        matches!(self.error_type, Some(ErrorType::Aborted))
    }

    fn error_type_as_str(&self) -> &str {
        self.error_type
            .as_ref()
            .map(ErrorType::as_str)
            .unwrap_or_default()
    }
}

/// Middleware that adds HTTP metrics and span information to requests.
///
/// Specifically, the middleware
///
/// - instruments the returned future and response to track the metrics present
///   in [`HttpClientMetrics`],
/// - creates an HTTP span around the request identifying the remote server, status code, etc; and
/// - propagates the trace ID of the created span to the server, so that their traces can be
///   stitched together.
///
#[derive(Debug, Clone)]
pub(crate) struct HttpMiddleware<S> {
    inner: S,
    metrics: HttpClientMetrics,
}

impl<S> HttpMiddleware<S> {
    pub(crate) fn new(inner: S, metrics: HttpClientMetrics) -> Self {
        Self { inner, metrics }
    }

    pub(crate) fn into_inner(self) -> S {
        self.inner
    }

    fn propagate_trace(request: &mut Request, http_span: &Span) {
        // We use the global propagator as in the examples, since this allows a client using the
        // library to completely disable propagation for contextual information.
        opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.inject_context(
                &http_span.context(),
                &mut HeaderInjector(request.headers_mut()),
            );
        });
    }
}

impl<S> Service<RequestWithUrlTemplate> for HttpMiddleware<S>
where
    S: Service<Request, Error = Error, Response = Response>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Instrumented<HttpMiddlewareFuture<S::Future>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, (mut request, url_template): RequestWithUrlTemplate) -> Self::Future {
        let monitor = RequestMonitor::new(&request, url_template.0, self.metrics.clone());
        let http_span = monitor.http_span().expect("span was just created").clone();

        Self::propagate_trace(&mut request, &http_span);

        HttpMiddlewareFuture::new(self.inner.call(request), monitor).instrument(http_span)
    }
}

/// Future returned by the [`HttpMiddleware`] service.
#[pin_project::pin_project]
pub(crate) struct HttpMiddlewareFuture<Fut> {
    #[pin]
    inner: Fut,
    monitor: Option<RequestMonitor>,
}

impl<Fut> FusedFuture for HttpMiddlewareFuture<Fut>
where
    Fut: Future<Output = Result<Response, Error>>,
{
    fn is_terminated(&self) -> bool {
        self.monitor.is_none()
    }
}

impl<Fut> HttpMiddlewareFuture<Fut> {
    fn new(inner: Fut, monitor: RequestMonitor) -> Self {
        Self {
            inner,
            monitor: Some(monitor),
        }
    }
}

impl<Fut> Future for HttpMiddlewareFuture<Fut>
where
    Fut: Future<Output = Result<Response, Error>>,
{
    type Output = Fut::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.is_terminated() {
            return Poll::Pending;
        }

        let this = self.project();
        let output = task::ready!(this.inner.poll(cx));

        let monitor = this
            .monitor
            .take()
            .expect("future had not yet terminated")
            .into_response_monitor(output.as_ref());

        let output = output.map(|response| -> Response {
            let response: http::Response<_> = response.into();
            let response = response.map(|body| reqwest::Body::wrap(VisitBody::new(body, monitor)));
            response.into()
        });

        Poll::Ready(output)
    }
}

/// Common internal state of the [`RequestMonitor`] and [`ResponseMonitor`].
struct MonitorInner {
    /// The span in which the request is being fetched.
    ///
    /// Follows the [OTel guidelines](https://opentelemetry.io/docs/specs/semconv/http/http-spans/)
    http_span: Span,
    /// Metrics being recorded for the request/response cycle.
    metrics: HttpClientMetrics,
    /// The start of the request/response.
    start: Instant,
    /// The labels associated with the request to be added to metrics.
    labels: HttpLabels,
    /// Decrements the in flight counter on drop
    _active_request_guard: OwnedGaugeGuard,
}

/// Monitors metrics about the request up until the point the response is returned.
///
/// Afterwards, it can be converted into a [`ResponseMonitor`] for monitoring metrics about the
/// response payload and overall duration.
struct RequestMonitor {
    /// INV: None iff all tracing/metrics have have been recorded for the request.
    inner: Option<MonitorInner>,
    /// Observed size of the request.
    request_body_size: u64,
}

impl RequestMonitor {
    pub fn new(request: &Request, url_template: &'static str, metrics: HttpClientMetrics) -> Self {
        let labels = HttpLabels::new(request, url_template);
        let request_body_size = request
            .body()
            .and_then(|body| {
                let exact_size = body.size_hint().exact();
                if exact_size.is_none() {
                    tracing::warn!("reading the length from the body not yet implemented");
                }
                exact_size
            })
            .unwrap_or(0);
        let active_request_guard = OwnedGaugeGuard::acquire(metrics.active_requests(&labels));

        Self {
            inner: Some(MonitorInner {
                http_span: Self::create_span(&labels, request.url().as_str()),
                metrics,
                start: Instant::now(),
                labels,
                _active_request_guard: active_request_guard,
            }),
            request_body_size,
        }
    }

    pub fn http_span(&self) -> Option<&Span> {
        self.inner.as_ref().map(|inner| &inner.http_span)
    }

    /// Record metrics and tracing fields for the request on receiving a response,
    /// returning a monitor for the response portion of the request.
    pub fn into_response_monitor(mut self, output: Result<&Response, &Error>) -> ResponseMonitor {
        self.response_received_or_aborted(Some(output))
            .expect("monitor is returned when output is `Some`")
    }

    fn response_received_or_aborted(
        &mut self,
        maybe_output: Option<Result<&Response, &Error>>,
    ) -> Option<ResponseMonitor> {
        self.inner_mut().labels.add_response_labels(maybe_output);
        let response_available_at = Instant::now();

        self.populate_fields_from_response(maybe_output.and_then(|res| res.err()));
        self.observe_request_duration(response_available_at);
        self.inner()
            .metrics
            .observe_request_body_size(self.request_body_size, &self.inner().labels);

        maybe_output.is_some().then(|| {
            ResponseMonitor::new(MonitorInner {
                start: response_available_at,
                ..self.inner.take().expect("is only made `None` here")
            })
        })
    }

    fn is_complete(&self) -> bool {
        self.inner.is_none()
    }

    fn inner(&self) -> &MonitorInner {
        self.inner.as_ref().expect("never called after complete")
    }

    fn inner_mut(&mut self) -> &mut MonitorInner {
        self.inner.as_mut().expect("never called after complete")
    }

    fn create_span(labels: &HttpLabels, full_url: &str) -> Span {
        tracing::info_span!(
            parent: &Span::current(),
            "http_request",
            otel.name = format!("{} {}", labels.http_request_method.as_str(), labels.url_template),
            otel.kind = "CLIENT",
            otel.status_code = field::Empty,
            otel.status_message = field::Empty,
            http.request.method = labels.http_request_method.as_str(),
            http.response.status_code = field::Empty,
            server.address = &labels.server_address,
            server.port = &labels.server_port,
            url.full = full_url,
            "error.type" = field::Empty,
        )
    }

    fn populate_fields_from_response(&self, maybe_err: Option<&reqwest::Error>) {
        let http_span = &self.inner().http_span;
        let labels = &self.inner().labels;

        if self.inner().labels.is_aborted() {
            // If called after the request is sent but before receiving a response, then the
            // monitor has been dropped and this is an abort.
            http_span.record("otel.status_code", "OK");
            http_span.record("error.type", labels.error_type_as_str());
        } else if let Some(error) = maybe_err {
            http_span.record("otel.status_code", "ERROR");
            http_span.record("otel.status_message", error.to_string());
            http_span.record("error.type", labels.error_type_as_str());
        } else {
            let status_code = labels
                .http_response_status_code
                .expect("status code must be set since there is no error");

            http_span.record("http.response.status_code", status_code.as_str());

            // We don't handle anything that is not a 2xx, so they're all failures to us.
            if !status_code.is_success() {
                http_span.record("otel.status_code", "ERROR");
                // For HTTP errors, otel recommends not setting status_message.
                http_span.record("error.type", status_code.as_str());
            }
        }
    }

    /// Observe the duration of the request up-until the headers of the response was received.
    fn observe_request_duration(&self, end: Instant) {
        let inner = self.inner();

        inner.metrics.observe_request_duration(
            end.duration_since(inner.start),
            &inner.labels,
            HTTP_RESPONSE_PART_HEADERS,
        );
    }
}

impl Drop for RequestMonitor {
    fn drop(&mut self) {
        if !self.is_complete() {
            self.response_received_or_aborted(None);
        }
    }
}

/// Monitors metrics about the response-portion of request, starting from the point at which the
/// response is received up until the point the body is dropped.
struct ResponseMonitor {
    inner: MonitorInner,
    response_body_size: usize,
}

impl ResponseMonitor {
    fn new(inner: MonitorInner) -> Self {
        Self {
            inner,
            response_body_size: 0,
        }
    }

    fn response_complete(&mut self, response_body_size: usize) {
        let MonitorInner {
            metrics,
            start,
            labels,
            ..
        } = &self.inner;

        // The duration of the request from the body headers being received to the body being
        // dropped.
        metrics.observe_request_duration(start.elapsed(), labels, HTTP_RESPONSE_PART_PAYLOAD);
        metrics.observe_response_body_size(response_body_size, labels);
    }
}

impl<E> BodyVisitor<Bytes, E> for ResponseMonitor {
    fn frame_polled(&mut self, maybe_result: Option<Result<&http_body::Frame<Bytes>, &E>>) {
        if let Some(data) = maybe_result.and_then(|result| result.ok()?.data_ref()) {
            self.response_body_size += data.len();
        }
    }

    fn body_dropped(&mut self, _is_end_stream: bool) {
        self.response_complete(self.response_body_size);
    }
}

// opentelemetry_http currently uses too low a version of the http crate,
// so we reimplement the injector here.
struct HeaderInjector<'a>(pub &'a mut HeaderMap);

impl Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let Ok(name) = HeaderName::from_bytes(key.as_bytes()) {
            if let Ok(val) = HeaderValue::from_str(&value) {
                self.0.insert(name, val);
            }
        }
    }
}

mod helpers {
    use super::*;

    /// Enum of client HTTP error types that may be attached to metrics/traces.
    #[derive(Debug, Clone)]
    pub(super) enum ErrorType {
        Connect,
        Timeout,
        Aborted,
        StatusCode(StatusCode),
        Other(Cow<'static, str>),
    }

    impl ErrorType {
        /// Returns a string representation of this error type.
        pub(super) fn as_str(&self) -> &str {
            match self {
                ErrorType::Connect => "connect",
                ErrorType::Timeout => "timeout",
                ErrorType::Aborted => "aborted",
                ErrorType::StatusCode(code) => code.as_str(),
                ErrorType::Other(cow) => cow.as_ref(),
            }
        }
    }

    /// Helper to use a `&'static str` for the scheme in the common case.
    pub(super) fn static_scheme(url: &Url) -> Cow<'static, str> {
        match url.scheme() {
            "http" => Cow::Borrowed("http"),
            "https" => Cow::Borrowed("https"),
            other => Cow::Owned(other.to_owned()),
        }
    }

    pub(super) fn static_version(version: Version) -> Cow<'static, str> {
        match version {
            Version::HTTP_09 => Cow::Borrowed("0.9"),
            Version::HTTP_10 => Cow::Borrowed("1.0"),
            Version::HTTP_11 => Cow::Borrowed("1.1"),
            Version::HTTP_2 => Cow::Borrowed("2.0"),
            Version::HTTP_3 => Cow::Borrowed("3.0"),
            unrecognized => Cow::Owned(format!("{unrecognized:?}")),
        }
    }
}
