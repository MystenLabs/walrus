// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Telemetry utilities for instrumenting walrus services.
use std::{
    net::{IpAddr, SocketAddr},
    str::FromStr as _,
    sync::Arc,
    time::Duration,
};

use axum::{
    extract::{ConnectInfo, MatchedPath},
    http::{
        self,
        header::{self, AsHeaderName},
        Request,
    },
};
use opentelemetry::propagation::Extractor;
use tower_http::trace::{MakeSpan, OnResponse};
use tracing::{field, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::server::UNMATCHED_ROUTE;

#[derive(Debug, Clone, Default)]
pub(crate) struct MakeHttpSpan;

impl MakeHttpSpan {
    pub fn new() -> MakeHttpSpan {
        Self
    }

    fn make_span<B>(&mut self, request: &Request<B>) -> Span {
        let route = self.get_route(request);
        let span = tracing::info_span!(
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
        // Set the server address based on https://opentelemetry.io/docs/specs/semconv/http/http-spans/
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

impl<'a> Extractor for HeaderExtractor<'a> {
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

#[derive(Debug, Clone)]
pub(crate) struct InternalError(pub Arc<dyn std::error::Error + Sync + Send + 'static>);

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
            if let Some(InternalError(err)) = response.extensions().get::<InternalError>() {
                span.in_scope(|| {
                    tracing::error!(error = ?err);
                });
                span.record("otel.status_message", field::display(err));
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
