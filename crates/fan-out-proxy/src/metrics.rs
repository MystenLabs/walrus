// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use prometheus::IntCounter;
use walrus_sdk::core_utils::metrics::define_metric_set;

define_metric_set! {
    #[namespace = "fan_out_proxy"]
    /// Metrics exported by the fan-out-proxy.
    pub(crate) struct FanOutProxyMetricSet {
        #[help = "The total count of blobs uploaded"]
        blobs_uploaded: IntCounter[],
    }
}
