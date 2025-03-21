// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use prometheus::{HistogramVec, IntCounterVec, Opts};

fn default_buckets_for_slow_operations() -> Vec<f64> {
    vec![
        0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ]
}

walrus_utils::metrics::define_metric_set! {
    #[namespace = "walrus"]
    /// Metrics for the Sui client operations.
    pub struct SuiClientMetrics {
        #[help = "Total number of Sui RPC calls made"]
        rpc_calls_total: IntCounterVec["method", "status"],

        #[help = "Duration of Sui RPC calls in seconds"]
        rpc_call_duration_seconds: HistogramVec{
            labels: ["method", "status"],
            buckets: default_buckets_for_slow_operations()
        },
    }
}

impl SuiClientMetrics {
    /// Record a Sui RPC call with the given method and status, and duration.
    pub fn record_rpc_call(&self, method: &str, status: &str, duration: std::time::Duration) {
        self.rpc_calls_total
            .with_label_values(&[method, status])
            .inc();

        self.rpc_call_duration_seconds
            .with_label_values(&[method, status])
            .observe(duration.as_secs_f64());
    }
}
