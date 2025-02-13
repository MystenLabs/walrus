// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use prometheus::{Histogram, IntCounter, IntCounterVec, Opts, Registry};

use crate::common::telemetry;

telemetry::define_metric_set! {
    /// Metrics exported by the backup fetcher node.
    struct BackupFetcherMetricSet {
        #[help = "The total count of blobs fetched from Walrus"]
        blobs_fetched: IntCounter[],

        #[help = "The total count of failed blob fetches"]
        blob_fetch_errors: IntCounterVec["client_error"],

        #[help = "The total count of blobs uploaded"]
        blobs_uploaded: IntCounter[],

        #[help = "The time it takes to upload a blob"]
        blob_upload_duration: Histogram[]
    }
}
telemetry::define_metric_set! {
    /// Metrics exported by the backup orchestrator node.
    struct BackupOrchestratorMetricSet {
        #[help = "The count of all Sui stream events seen"]
        sui_events_seen: IntCounter[],
        #[help = "The count of Walrus contract events seen"]
        events_recorded: IntCounter[],
    }
}
