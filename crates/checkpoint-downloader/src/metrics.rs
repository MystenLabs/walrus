// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Metrics for the checkpoint downloader.

use prometheus::{register_int_gauge_with_registry, IntGauge};
use walrus_utils::metrics::Registry;

#[derive(Clone, Debug)]
pub(crate) struct AdaptiveDownloaderMetrics {
    /// The number of checkpoint downloader workers.
    pub num_workers: IntGauge,
    /// The current checkpoint lag between the local store and the full node.
    pub checkpoint_lag: IntGauge,
}

impl AdaptiveDownloaderMetrics {
    pub fn new(registry: &Registry) -> Self {
        Self {
            num_workers: register_int_gauge_with_registry!(
                "checkpoint_downloader_num_workers",
                "Number of workers active in the worker pool",
                registry,
            )
            .expect("this is a valid metrics registration"),
            checkpoint_lag: register_int_gauge_with_registry!(
                "checkpoint_downloader_checkpoint_lag",
                "Current checkpoint lag between local store and full node",
                registry,
            )
            .expect("this is a valid metrics registration"),
        }
    }
}
