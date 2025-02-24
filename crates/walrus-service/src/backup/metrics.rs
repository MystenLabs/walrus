// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use prometheus::{Gauge, GaugeVec, Histogram, IntCounter, IntCounterVec, Opts, Registry};

use crate::common::telemetry;

telemetry::define_metric_set! {
    /// Metrics exported by the backup fetcher node.
    struct BackupFetcherMetricSet {
        #[help = "The total count of blobs fetched from Walrus"]
        blobs_fetched: IntCounter[],

        #[help = "The total count of failed blob fetches"]
        blob_fetch_errors: IntCounterVec["client_error"],

        #[help = "The total count of blob bytes fetched successfully"]
        blob_bytes_fetched: IntCounter[],

        #[help = "The current count of failed blob fetches in a row"]
        consecutive_blob_fetch_errors: Gauge[],

        #[help = "The time it takes to fetch a blob from the network"]
        blob_fetch_duration: Histogram {
            buckets: buckets_for_blob_durations(),
        },

        #[help = "The total count of blobs uploaded"]
        blobs_uploaded: IntCounter[],

        #[help = "The time it takes to upload a blob"]
        blob_upload_duration: Histogram {
            buckets: buckets_for_blob_durations(),
        },

        #[help = "The total count of blob bytes uploaded successfully"]
        blob_bytes_uploaded: IntCounter[],

        #[help = "The number of retries due to serializability failures"]
        db_serializability_retries: IntCounterVec["context"],

        #[help = "The count of database reconnects"]
        db_reconnects: IntCounter[],

        #[help = "Idle"]
        idle_state: Gauge[],
    }
}

fn buckets_for_blob_durations() -> Vec<f64> {
    prometheus::exponential_buckets(0.02, 2.7, 12).unwrap()
}

telemetry::define_metric_set! {
    /// Metrics exported by the backup orchestrator node.
    struct BackupOrchestratorMetricSet {
        #[help = "The count of all Sui stream events seen"]
        sui_events_seen: IntCounter[],

        #[help = "The count of Walrus contract events seen"]
        events_recorded: IntCounter[],

        #[help = "The count of database reconnects"]
        db_reconnects: IntCounter[],

        #[help = "The number of retries due to serializability failures"]
        db_serializability_retries: IntCounterVec["context"],
    }
}
telemetry::define_metric_set! {
    /// Metrics exported by the backup orchestrator node.
    struct BackupDbMetricSet {
        #[help = "The states of the blobs in the db"]
        blob_states: GaugeVec["state"],
    }
}
