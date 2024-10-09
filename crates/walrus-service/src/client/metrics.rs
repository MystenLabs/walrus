// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use prometheus::Registry;

use crate::common::telemetry::{self, CurrentEpochMetric, CurrentEpochStateMetric};

telemetry::define_metric_set! {
    ClientMetricSet;
    @TypedMetrics: [
        (current_epoch, CurrentEpochMetric),
        (current_epoch_state, CurrentEpochStateMetric),
    ],
}
