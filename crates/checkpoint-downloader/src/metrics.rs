// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Metrics for the checkpoint downloader.

use prometheus::IntGauge;

walrus_utils::metrics::define_metric_set! {
    #[namespace = "checkpoint_downloader"]
    /// Metrics for the event processor.
    pub struct AdaptiveDownloaderMetrics {
        #[help = "Number of workers active in the worker pool"]
        num_workers: IntGauge[],
        #[help = "Current checkpoint lag between local store and full node"]
        checkpoint_lag: IntGauge[],
        #[help = "Number of inflight checkpoint downloads"]
        num_inflight_downloading: IntGauge[],
        #[help = "Number of checkpoints pending processing"]
        num_pending_processing_checkpoints: IntGauge[],
    }
}
