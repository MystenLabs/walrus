// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Metrics for the client and daemon.

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

// Workload types for the client.
/// The name of the write workload.
pub const WRITE_WORKLOAD: &str = "write";
/// The name of the read workload.
pub const READ_WORKLOAD: &str = "read";

/// Container for the client metrics.
#[derive(Debug)]
pub struct ClientMetrics {
    /// Duration of the execution.
    pub execution_duration: IntCounter,
    /// Number of transactions submitted by the client.
    pub submitted: CounterVec,
    /// Total time in seconds used by the workloads.
    pub latency_s: HistogramVec,
    /// Square of the total time in seconds used by the workloads.
    pub latency_squared_s: CounterVec,
    /// Errors encountered by the client.
    pub errors: CounterVec,
    /// Number of gas refills performed by the client.
    pub gas_refill: IntCounter,
    /// Number of WAL refills performed by the client.
    pub wal_refill: IntCounter,
}

impl ClientMetrics {
    /// Creates a new instance of the client metrics from an existing registry.
    pub fn new(registry: &Registry) -> Self {
        Self {
            execution_duration: register_int_counter_with_registry!(
                "execution_duration",
                "Duration of the execution",
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
                "Total time in seconds used by the workloads",
                &["workload"],
                LATENCY_SEC_BUCKETS.to_vec(),
                registry,
            )
            .expect("this is a valid metrics registration"),
            latency_squared_s: register_counter_vec_with_registry!(
                "latency_squared_s",
                "Square of the total time in seconds used by the workloads",
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

    /// Increments the execution duration by the given uptime.
    pub fn observe_execution_duration(&self, uptime: Duration) {
        let previous = self.execution_duration.get();
        self.execution_duration.inc_by(uptime.as_secs() - previous);
    }

    /// Increments the number of submitted transactions for the given workload.
    pub fn observe_submitted(&self, workload: &str) {
        walrus_utils::with_label!(self.submitted, workload).inc();
    }

    /// Logs the latency for the given workload.
    pub fn observe_latency(&self, workload: &str, latency: Duration) {
        walrus_utils::with_label!(self.latency_s, workload).observe(latency.as_secs_f64());
        walrus_utils::with_label!(self.latency_squared_s, workload)
            .inc_by(latency.as_secs_f64().powi(2));
    }

    /// Increments the error counter for the given error type.
    pub fn observe_error(&self, error: &str) {
        walrus_utils::with_label!(self.errors, error).inc();
    }

    /// Increments the gas refill counter.
    pub fn observe_gas_refill(&self) {
        self.gas_refill.inc();
    }

    /// Increments the WAL refill counter.
    pub fn observe_wal_refill(&self) {
        self.wal_refill.inc();
    }
}
