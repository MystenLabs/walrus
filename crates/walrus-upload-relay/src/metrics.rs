// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use prometheus::IntCounter;
use walrus_sdk::core_utils::metrics::define_metric_set;

define_metric_set! {
    #[namespace = "fan_out_proxy"]
    /// Metrics exported by the walrus-upload-relay.
    pub(crate) struct FanOutProxyMetricSet {
        #[help = "The total count of blobs uploaded"]
        blobs_uploaded: IntCounter[],
        #[help = "The count of blob id mismatches"]
        blob_id_mismatch: IntCounter[],
        #[help = "The count of errors looking up tip transactions"]
        get_transaction_error: IntCounter[],
        #[help = "The count of errors related to freshness checks of transactions"]
        freshness_check_error: IntCounter[],
        #[help = "The count of errors related to tip checks"]
        tip_check_error: IntCounter[],
        #[help = "The count of errors related to auth package checks"]
        auth_package_check_error: IntCounter[],
    }
}
