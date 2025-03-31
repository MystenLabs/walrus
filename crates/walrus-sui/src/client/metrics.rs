// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use prometheus::{HistogramVec, IntCounterVec};

fn default_buckets_for_slow_operations() -> Vec<f64> {
    prometheus::exponential_buckets(0.001, 2.0, 14).expect("count, start, and factor are valid")
}

walrus_utils::metrics::define_metric_set! {
    #[namespace = "walrus"]
    /// Metrics for the Sui client operations.
    pub struct SuiClientMetricSet {
        #[help = "Total number of Sui RPC calls made"]
        sui_rpc_calls_count: IntCounterVec["method", "status"],

        #[help = "Duration of Sui RPC calls in seconds"]
        sui_rpc_call_duration_seconds: HistogramVec{
            labels: ["method", "status"],
            buckets: default_buckets_for_slow_operations()
        },

        #[help = "Number of retries for RPC call"]
        sui_rpc_retry_count: IntCounterVec["method", "status"],
    }
}

impl SuiClientMetricSet {
    /// Record a Sui RPC call with the given method and status, and duration.
    pub fn record_rpc_call(&self, method: &str, status: &str, duration: std::time::Duration) {
        self.sui_rpc_calls_count
            .with_label_values(&[method, status])
            .inc();

        self.sui_rpc_call_duration_seconds
            .with_label_values(&[method, status])
            .observe(duration.as_secs_f64());
    }

    /// Record the number of retries for an RPC call with the given method and error string.
    ///
    /// Only sui rpc calls that have retried is recorded, the status is the final result.
    pub fn record_rpc_retry_count(&self, method: &str, count: u64, error_str: &str) {
        self.sui_rpc_retry_count
            .with_label_values(&[method, error_str])
            .inc_by(count);
    }
}
