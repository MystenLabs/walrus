// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Expose metrics for the load generator.

use std::time::Duration;

use prometheus::{
    register_counter_vec_with_registry,
    register_histogram_vec_with_registry,
    register_int_counter_with_registry,
    CounterVec,
    HistogramVec,
    IntCounter,
    Registry,
};

const LATENCY_SEC_BUCKETS: &[f64] = &[
    1., 1.5, 2., 2.5, 3., 4., 5., 6., 7., 8., 9., 10., 20., 40., 80., 160.,
];

/// The workload types for the client.
pub const WRITE_WORKLOAD: &str = "write";
pub const READ_WORKLOAD: &str = "read";

#[derive(Debug)]
pub struct ClientMetrics {
    pub benchmark_duration: IntCounter,
    pub submitted: CounterVec,
    pub latency_s: HistogramVec,
    pub latency_squared_s: CounterVec,
    pub errors: CounterVec,
    pub gas_refill: IntCounter,
    pub wal_refill: IntCounter,
}

impl ClientMetrics {
    pub fn new(registry: &Registry) -> Self {
        Self {
            benchmark_duration: register_int_counter_with_registry!(
                "benchmark_duration",
                "Duration of the benchmark",
                registry,
            )
            .expect("this is a valid metrics registration"),
            submitted: register_counter_vec_with_registry!(
                "submitted",
                "Number of submitted transactions",
                &["workload"],
                registry,
            )
            .expect("this is a valid metrics registration"),
            latency_s: register_histogram_vec_with_registry!(
                "latency_s",
                "Total time in seconds to to achieve finality",
                &["workload"],
                LATENCY_SEC_BUCKETS.to_vec(),
                registry,
            )
            .expect("this is a valid metrics registration"),
            latency_squared_s: register_counter_vec_with_registry!(
                "latency_squared_s",
                "Square of total time in seconds to achieve finality",
                &["workload"],
                registry,
            )
            .expect("this is a valid metrics registration"),
            errors: register_counter_vec_with_registry!(
                "errors",
                "Reports various errors",
                &["type"],
                registry,
            )
            .expect("this is a valid metrics registration"),
            gas_refill: register_int_counter_with_registry!(
                "gas_refill",
                "Number of gas refills",
                registry,
            )
            .expect("this is a valid metrics registration"),
            wal_refill: register_int_counter_with_registry!(
                "wal_refill",
                "Number of wal refills",
                registry,
            )
            .expect("this is a valid metrics registration"),
        }
    }

    pub fn observe_benchmark_duration(&self, uptime: Duration) {
        let previous = self.benchmark_duration.get();
        self.benchmark_duration.inc_by(uptime.as_secs() - previous);
    }

    pub fn observe_submitted(&self, workload: &str) {
        self.submitted.with_label_values(&[workload]).inc();
    }

    pub fn observe_latency(&self, workload: &str, latency: Duration) {
        self.latency_s
            .with_label_values(&[workload])
            .observe(latency.as_secs_f64());
        self.latency_squared_s
            .with_label_values(&[workload])
            .inc_by(latency.as_secs_f64().powi(2));
    }

    pub fn observe_error(&self, error: &str) {
        self.errors.with_label_values(&[error]).inc();
    }

    pub fn observe_gas_refill(&self) {
        self.gas_refill.inc();
    }

    pub fn observe_wal_refill(&self) {
        self.wal_refill.inc();
    }
}
