// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
use std::{
    collections::VecDeque,
    net::TcpListener,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Result, bail};
use axum::{Router, extract::Extension, http::StatusCode, routing::get};
use once_cell::sync::Lazy;
use prometheus::{
    CounterVec,
    HistogramOpts,
    HistogramVec,
    Opts,
    proto::{Metric, MetricFamily},
};
use tower::ServiceBuilder;
use tower_http::{
    LatencyUnit,
    trace::{DefaultOnResponse, TraceLayer},
};
use tracing::{Level, info};

use crate::{register_metric, var};

const METRICS_ROUTE: &str = "/metrics";

static RELAY_PRESSURE: Lazy<CounterVec> = Lazy::new(|| {
    register_metric!(
        CounterVec::new(
            Opts::new(
                "relay_pressure",
                "HistogramRelay's number of metric families submitted, exported, \
overflowed to/from the queue.",
            ),
            &["histogram_relay"]
        )
        .expect("metric creation cannot fail with valid parameters")
    )
});
static RELAY_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_metric!(
        HistogramVec::new(
            HistogramOpts::new(
                "relay_duration_seconds",
                "HistogramRelay's submit/export fn latencies in seconds.",
            )
            .buckets(vec![
                0.0008, 0.0016, 0.0032, 0.0064, 0.0128, 0.0256, 0.0512, 0.1024, 0.2048, 0.4096,
                0.8192, 1.0, 1.25, 1.5, 1.75, 2.0, 4.0, 8.0, 10.0, 12.5, 15.0
            ]),
            &["histogram_relay"]
        )
        .expect("metric creation cannot fail with valid parameters")
    )
});

/// Creates a new http server that has as a sole purpose to expose
/// and endpoint that prometheus agent can use to poll for the metrics.
/// A RegistryService is returned that can be used to get access in prometheus
/// Registries.
pub fn start_prometheus_server(listener: TcpListener) -> HistogramRelay {
    let relay = HistogramRelay::new();
    let app = Router::new()
        .route(METRICS_ROUTE, get(metrics))
        .layer(Extension(relay.clone()))
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
        listener
            .set_nonblocking(true)
            .expect("listener is valid and supports non-blocking mode");
        let listener = tokio::net::TcpListener::from_std(listener)
            .expect("valid std listener can always be converted to tokio listener");
        axum::serve(listener, app)
            .await
            .expect("axum server should run without error");
    });
    relay
}

async fn metrics(Extension(relay): Extension<HistogramRelay>) -> (StatusCode, String) {
    let Ok(expformat) = relay.export() else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "unable to pop metrics from HistogramRelay".into(),
        );
    };
    (StatusCode::OK, expformat)
}

struct Wrapper(i64, Vec<MetricFamily>);

/// HistogramRelay manages the histograms we receive from nodes.  it exports
/// them to a local agent for scraping we do this because histograms pose a
/// challenge in the remote write protobuf we use.  our prometheus crate
/// does not support native histograms but the remote write protobuf expects
/// them, so we have to export them this way
#[allow(missing_debug_implementations)]
#[derive(Clone)]
pub struct HistogramRelay(Arc<Mutex<VecDeque<Wrapper>>>);

impl Default for HistogramRelay {
    fn default() -> Self {
        HistogramRelay(Arc::new(Mutex::new(VecDeque::new())))
    }
}
impl HistogramRelay {
    /// create a new HistogramRelay
    pub fn new() -> Self {
        Self::default()
    }
    /// submit will take metric family submissions and store them for scraping
    /// in doing so, it will also wrap each entry in a timestamp which will be
    /// use for pruning old entries on each submission call. this may not be
    /// ideal long term.
    pub fn submit(&self, data: Vec<MetricFamily>) {
        walrus_utils::with_label!(RELAY_PRESSURE, "submit").inc();
        let timer = walrus_utils::with_label!(RELAY_DURATION, "submit").start_timer();
        //  represents a collection timestamp
        let timestamp_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after UNIX_EPOCH")
            .as_secs()
            .try_into()
            .expect("timestamp should fit in i64");
        let mut queue = self
            .0
            .lock()
            .expect("couldn't get mut lock on HistogramRelay");
        queue.retain(|v| {
            // 5 mins is the max time in the queue allowed
            if (timestamp_secs - v.0) < var!("MAX_QUEUE_TIME_SECS", 300) {
                return true;
            }
            walrus_utils::with_label!(RELAY_PRESSURE, "overflow").inc();
            false
        }); // drain anything 5 mins or older

        // filter out our histograms from normal metrics
        let data: Vec<MetricFamily> = extract_histograms(data).collect();
        walrus_utils::with_label!(RELAY_PRESSURE, "submitted").inc_by(data.len() as f64);
        queue.push_back(Wrapper(timestamp_secs, data));
        timer.observe_duration();
    }
    /// export drains our histogram registry from the nodes and exports it to a
    /// string format that we can send to mimir
    pub fn export(&self) -> Result<String> {
        walrus_utils::with_label!(RELAY_PRESSURE, "export").inc();
        let timer = walrus_utils::with_label!(RELAY_DURATION, "export").start_timer();
        // totally drain all metrics whenever we get a scrape request from the metrics
        // handler
        let mut queue = self
            .0
            .lock()
            .expect("couldn't get mut lock on HistogramRelay");

        let data: Vec<Wrapper> = queue.drain(..).collect();
        let mut histograms = vec![];
        for mf in data {
            histograms.extend(mf.1);
        }
        info!(
            "histogram queue drained {} items; remaining count {}",
            histograms.len(),
            queue.len()
        );

        let encoder = prometheus::TextEncoder::new();
        let string = match encoder.encode_to_string(&histograms) {
            Ok(s) => s,
            Err(error) => bail!("{error}"),
        };
        walrus_utils::with_label!(RELAY_PRESSURE, "exported").inc_by(histograms.len() as f64);
        timer.observe_duration();
        Ok(string)
    }
}

/// extract_histograms just grabs the histograms from our metrics because we
/// don't want to export non-histogram metric types here
fn extract_histograms(data: Vec<MetricFamily>) -> impl Iterator<Item = MetricFamily> {
    data.into_iter().filter_map(|mf| {
        let metrics = mf.get_metric().iter().filter_map(|m| {
            if !m.has_histogram() {
                return None;
            }
            let mut v = Metric::default();
            v.set_label(protobuf::RepeatedField::from_slice(m.get_label()));
            v.set_histogram(m.get_histogram().to_owned());
            v.set_timestamp_ms(m.get_timestamp_ms());
            Some(v)
        });

        let only_histograms = protobuf::RepeatedField::from_iter(metrics);
        if only_histograms.is_empty() {
            return None;
        }

        let mut v = MetricFamily::default();
        v.set_name(mf.get_name().to_owned());
        v.set_help(mf.get_help().to_owned());
        v.set_field_type(mf.get_field_type());
        v.set_metric(only_histograms);
        Some(v)
    })
}
