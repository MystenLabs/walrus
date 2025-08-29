// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Metrics module for the indexer event processor.

use prometheus::{IntCounter, IntGauge};

walrus_utils::metrics::define_metric_set! {
    #[namespace = "walrus_indexer"]
    /// Metrics for the indexer event processor.
    pub struct IndexerEventProcessorMetrics {
        #[help = "Latest downloaded full checkpoint"]
        indexer_event_processor_latest_downloaded_checkpoint: IntGauge[],
        #[help = "The number of checkpoints downloaded. Useful for computing the download rate"]
        indexer_event_processor_total_downloaded_checkpoints: IntCounter[],
        #[help = "The number of index operations processed"]
        indexer_event_processor_index_operations_processed: IntCounter[],
        #[help = "The number of index events extracted from checkpoints"]
        indexer_event_processor_index_events_extracted: IntCounter[],
    }
}
