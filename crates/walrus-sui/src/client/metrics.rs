// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use prometheus::{HistogramVec, IntCounterVec, Opts};

fn default_buckets_for_slow_operations() -> Vec<f64> {
    prometheus::exponential_buckets(0.001, 2.0, 14).expect("count, start, and factor are valid")
}

walrus_utils::metrics::define_metric_set! {
    #[namespace = "walrus"]
    /// Metrics for the Sui client operations.
    pub struct SuiClientMetrics {
        #[help = "Total number of Sui RPC calls made"]
        sui_rpc_calls_count: IntCounterVec["method", "status"],

        #[help = "Duration of Sui RPC calls in seconds"]
        sui_rpc_call_duration_seconds: HistogramVec{
            labels: ["method", "status"],
            buckets: default_buckets_for_slow_operations()
        },
    }
}

impl SuiClientMetrics {
    /// Record a Sui RPC call with the given method and status, and duration.
    pub fn record_rpc_call(&self, method: &str, status: &str, duration: std::time::Duration) {
        self.sui_rpc_calls_count
            .with_label_values(&[method, status])
            .inc();

        self.sui_rpc_call_duration_seconds
            .with_label_values(&[method, status])
            .observe(duration.as_secs_f64());
    }
}
