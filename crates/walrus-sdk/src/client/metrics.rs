// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Metrics for the client and daemon.

use std::time::Duration;

use prometheus::{
    CounterVec,
    Gauge,
    GaugeVec,
    Histogram,
    HistogramVec,
    IntCounter,
    IntGauge,
    register_counter_vec_with_registry,
    register_gauge_vec_with_registry,
    register_gauge_with_registry,
    register_histogram_vec_with_registry,
    register_histogram_with_registry,
    register_int_counter_with_registry,
    register_int_gauge_with_registry,
};
use walrus_utils::metrics::Registry;

const LATENCY_SEC_BUCKETS: &[f64] = &[
    1., 1.5, 2., 2.5, 3., 4., 5., 6., 7., 8., 9., 10., 20., 40., 80., 160.,
];

const LATENCY_SEC_SMALL_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.03, 0.05, 0.07, 1., 1.3, 1.5, 1.7, 2., 2.3, 2.5, 2.7, 3.,
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
    /// Time to encode a blob.
    pub encoding_latency_s: Histogram,
    /// Time to check the status of a blob.
    pub checking_blob_status_latency_s: Histogram,
    /// Time to store a blob.
    pub store_operation_latency_s: Histogram,
    /// Time to get certificates.
    pub get_certificates_latency_s: Histogram,
    /// Time to upload a certificate to Sui.
    pub upload_certificate_latency_s: Histogram,

    // Task Scheduling and Wait Time Metrics
    /// Time from task submission to execution start
    pub task_queue_wait_time_s: HistogramVec,
    /// Current number of tasks waiting in queue
    pub tasks_in_queue: GaugeVec,
    /// Current number of tasks actively executing
    pub tasks_executing: GaugeVec,
    /// Time from task creation to completion (includes wait + execution)
    pub task_total_latency_s: HistogramVec,
    /// Actual execution time (excludes wait time)
    pub task_execution_time_s: HistogramVec,

    // Semaphore and Resource Wait Metrics
    /// Time waiting for semaphore permit
    pub semaphore_acquire_wait_s: HistogramVec,
    /// Number of tasks waiting for semaphore
    pub semaphore_waiters: GaugeVec,
    /// Queue depth when task arrives
    pub queue_depth_on_arrival: Histogram,

    // Upload Progress Metrics
    /// Real-time count of nodes that have successfully received slivers
    pub nodes_with_slivers_gauge: Gauge,
    /// Latency distribution for each node to receive all its slivers
    pub per_node_complete_latency_s: HistogramVec,
    /// Total slivers successfully sent
    pub slivers_sent_total: IntCounter,
    /// Total slivers expected to be sent
    pub slivers_expected_total: IntCounter,

    // Concurrency Metrics
    /// Current number of concurrent sliver uploads
    pub concurrent_sliver_uploads: IntGauge,
    /// Number of nodes being uploaded to concurrently
    pub concurrent_node_connections: IntGauge,

    // Retry Metrics
    /// Retry attempts per error type and phase
    pub retry_attempts_total: CounterVec,
    /// Current nodes in retry state
    pub nodes_in_retry_state: IntGauge,
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
            encoding_latency_s: register_histogram_with_registry!(
                "encoding_latency_s",
                "Time to encode a blob",
                LATENCY_SEC_SMALL_BUCKETS.to_vec(),
                registry,
            )
            .expect("this is a valid metrics registration"),
            checking_blob_status_latency_s: register_histogram_with_registry!(
                "checking_blob_status_latency_s",
                "Time to check the status of a blob",
                LATENCY_SEC_SMALL_BUCKETS.to_vec(),
                registry,
            )
            .expect("this is a valid metrics registration"),
            store_operation_latency_s: register_histogram_with_registry!(
                "store_operation_latency_s",
                "Time to store a blob",
                LATENCY_SEC_BUCKETS.to_vec(),
                registry,
            )
            .expect("this is a valid metrics registration"),
            get_certificates_latency_s: register_histogram_with_registry!(
                "get_certificates_latency_s",
                "Time to get certificates",
                LATENCY_SEC_BUCKETS.to_vec(),
                registry,
            )
            .expect("this is a valid metrics registration"),
            upload_certificate_latency_s: register_histogram_with_registry!(
                "upload_certificate_latency_s",
                "Time to upload a certificate to Sui",
                LATENCY_SEC_BUCKETS.to_vec(),
                registry,
            )
            .expect("this is a valid metrics registration"),

            // Task Scheduling and Wait Time Metrics
            task_queue_wait_time_s: register_histogram_vec_with_registry!(
                "task_queue_wait_time_s",
                "Time from task submission to execution start",
                &["task_type", "node_id"],
                LATENCY_SEC_SMALL_BUCKETS.to_vec(),
                registry,
            )
            .expect("this is a valid metrics registration"),
            tasks_in_queue: register_gauge_vec_with_registry!(
                "tasks_in_queue",
                "Current number of tasks waiting in queue",
                &["task_type"],
                registry,
            )
            .expect("this is a valid metrics registration"),
            tasks_executing: register_gauge_vec_with_registry!(
                "tasks_executing",
                "Current number of tasks actively executing",
                &["task_type"],
                registry,
            )
            .expect("this is a valid metrics registration"),
            task_total_latency_s: register_histogram_vec_with_registry!(
                "task_total_latency_s",
                "Time from task creation to completion (includes wait + execution)",
                &["task_type", "node_id"],
                LATENCY_SEC_BUCKETS.to_vec(),
                registry,
            )
            .expect("this is a valid metrics registration"),
            task_execution_time_s: register_histogram_vec_with_registry!(
                "task_execution_time_s",
                "Actual execution time (excludes wait time)",
                &["task_type", "node_id"],
                LATENCY_SEC_BUCKETS.to_vec(),
                registry,
            )
            .expect("this is a valid metrics registration"),

            // Semaphore and Resource Wait Metrics
            semaphore_acquire_wait_s: register_histogram_vec_with_registry!(
                "semaphore_acquire_wait_s",
                "Time waiting for semaphore permit",
                &["resource_type"],
                LATENCY_SEC_SMALL_BUCKETS.to_vec(),
                registry,
            )
            .expect("this is a valid metrics registration"),
            semaphore_waiters: register_gauge_vec_with_registry!(
                "semaphore_waiters",
                "Number of tasks waiting for semaphore",
                &["resource_type"],
                registry,
            )
            .expect("this is a valid metrics registration"),
            queue_depth_on_arrival: register_histogram_with_registry!(
                "queue_depth_on_arrival",
                "Queue depth when task arrives",
                vec![0.0, 1.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0, 1000.0],
                registry,
            )
            .expect("this is a valid metrics registration"),

            // Upload Progress Metrics
            nodes_with_slivers_gauge: register_gauge_with_registry!(
                "nodes_with_slivers",
                "Real-time count of nodes that have successfully received slivers",
                registry,
            )
            .expect("this is a valid metrics registration"),
            per_node_complete_latency_s: register_histogram_vec_with_registry!(
                "per_node_complete_latency_s",
                "Latency distribution for each node to receive all its slivers",
                &["node_id", "shard_count"],
                LATENCY_SEC_BUCKETS.to_vec(),
                registry,
            )
            .expect("this is a valid metrics registration"),
            slivers_sent_total: register_int_counter_with_registry!(
                "slivers_sent_total",
                "Total slivers successfully sent",
                registry,
            )
            .expect("this is a valid metrics registration"),
            slivers_expected_total: register_int_counter_with_registry!(
                "slivers_expected_total",
                "Total slivers expected to be sent",
                registry,
            )
            .expect("this is a valid metrics registration"),

            // Concurrency Metrics
            concurrent_sliver_uploads: register_int_gauge_with_registry!(
                "concurrent_sliver_uploads",
                "Current number of concurrent sliver uploads",
                registry,
            )
            .expect("this is a valid metrics registration"),
            concurrent_node_connections: register_int_gauge_with_registry!(
                "concurrent_node_connections",
                "Number of nodes being uploaded to concurrently",
                registry,
            )
            .expect("this is a valid metrics registration"),

            // Retry Metrics
            retry_attempts_total: register_counter_vec_with_registry!(
                "retry_attempts_total",
                "Retry attempts per error type and phase",
                &["node_id", "error_type", "phase"],
                registry,
            )
            .expect("this is a valid metrics registration"),
            nodes_in_retry_state: register_int_gauge_with_registry!(
                "nodes_in_retry_state",
                "Current nodes in retry state",
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

    /// Logs the latency for encoding a blob.
    pub fn observe_encoding_latency(&self, latency: Duration) {
        self.encoding_latency_s.observe(latency.as_secs_f64());
    }

    /// Logs the latency for checking the status of a blob.
    pub fn observe_checking_blob_status(&self, latency: Duration) {
        self.checking_blob_status_latency_s
            .observe(latency.as_secs_f64());
    }

    /// Logs the latency for storing a blob.
    pub fn observe_store_operation(&self, latency: Duration) {
        self.store_operation_latency_s
            .observe(latency.as_secs_f64());
    }

    /// Logs the latency for uploading a certificate to Sui.
    pub fn observe_upload_certificate(&self, latency: Duration) {
        self.upload_certificate_latency_s
            .observe(latency.as_secs_f64());
    }

    /// Logs the latency for getting certificates.
    pub fn observe_get_certificates(&self, latency: Duration) {
        self.get_certificates_latency_s
            .observe(latency.as_secs_f64());
    }
}
