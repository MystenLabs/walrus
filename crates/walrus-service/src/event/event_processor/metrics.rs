// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Metrics module for the event processor.

use prometheus::{IntCounter, IntCounterVec, IntGauge};

walrus_utils::metrics::define_metric_set! {
    #[namespace = "walrus"]
    /// Metrics for the event processor.
    pub struct EventProcessorMetrics {
        #[help = "Latest downloaded full checkpoint"]
        event_processor_latest_downloaded_checkpoint: IntGauge[],
        #[help = "The number of checkpoints downloaded. Useful for computing the download rate"]
        event_processor_total_downloaded_checkpoints: IntCounter[],
        #[help = "The number of event blobs fetched with their source"]
        event_processor_event_blob_fetched: IntCounterVec["blob_source"],
    }
}
