// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Timings at the Walrus/Strata boundary. Strata exports its own disk and GC metrics.

use prometheus::{Histogram, HistogramTimer, HistogramVec, IntCounter, IntGauge, IntGaugeVec};
use walrus_utils::metrics::OwnedGaugeGuard;

walrus_utils::metrics::define_metric_set! {
    #[namespace = "walrus_strata"]
    pub(crate) struct LifecycleMetrics {
        #[help = "Epoch reconciliation attempt duration, including scanning, locks and durability."]
        reconciliation_duration_seconds: Histogram {
            buckets: vec![
                0.01, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0,
                3600.0,
            ],
        },
        #[help = "Reconciliation attempts currently running, including time waiting for locks."]
        reconciliation_active: IntGauge [],
        #[help = "Epoch reconciliation passes completed durably."]
        reconciliation_completed_total: IntCounter [],
        #[help = "Latest durably requested reconciliation epoch."]
        reconciliation_requested_epoch: IntGauge [],
        #[help = "Latest durably completed reconciliation epoch."]
        reconciliation_completed_epoch: IntGauge [],
        #[help = "Reconciliation phase time, including scheduling and locks."]
        reconciliation_phase_duration_seconds: HistogramVec {
            labels: ["phase"],
            buckets: vec![
                0.001, 0.01, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0,
                1800.0, 3600.0,
            ],
        },
        #[help = "Candidates in the latest reconciliation snapshot, not remaining work."]
        reconciliation_candidates: IntGaugeVec ["kind"],
        #[help = "Strata API time including queue wait, excluding blob locks and durability."]
        operation_duration_seconds: HistogramVec {
            labels: ["operation"],
            buckets: vec![
                0.00001, 0.0001, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
                5.0, 10.0, 30.0, 60.0,
            ],
        },
        #[help = "Walrus put/put-many time including admission, submission and durability."]
        put_duration_seconds: Histogram {
            buckets: vec![
                0.001, 0.01, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0,
            ],
        },
        #[help = "Put preparation time and time queued for a blocking worker, by phase."]
        put_phase_duration_seconds: HistogramVec {
            labels: ["phase"],
            buckets: vec![
                0.00001, 0.0001, 0.001, 0.01, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0,
            ],
        },
        #[help = "Wait for LSN durability, including the periodic sync interval."]
        durability_wait_duration_seconds: Histogram {
            buckets: vec![
                0.0001, 0.001, 0.01, 0.1, 0.25, 0.5, 0.75, 1.0, 1.5, 2.5, 5.0, 10.0, 30.0,
                60.0,
            ],
        },
        #[help = "Explicit adapter sync time, including commit or checkpoint publication."]
        sync_duration_seconds: HistogramVec {
            labels: ["store", "reason"],
            buckets: vec![
                0.0001, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
                30.0, 60.0,
            ],
        },
        #[help = "Lock acquisition time, including cancelled waits, excluding hold time."]
        lock_wait_duration_seconds: HistogramVec {
            labels: ["lock", "mode"],
            buckets: vec![
                0.00001, 0.0001, 0.001, 0.01, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
                120.0, 300.0,
            ],
        },
        #[help = "Tasks currently waiting to acquire a lock, excluding holders."]
        lock_waiters: IntGaugeVec ["lock", "mode"],
        #[help = "Admission halted after an incomplete write; reopening is required."]
        admission_halts_total: IntCounter [],
    }
}

impl LifecycleMetrics {
    pub(crate) fn put_phase(&self, phase: &str) -> HistogramTimer {
        self.put_phase_duration_seconds
            .with_label_values(&[phase])
            .start_timer()
    }
    // Both guards drop on cancellation as well as on successful acquisition.
    pub(super) fn lock_wait(&self, lock: &str, mode: &str) -> (HistogramTimer, OwnedGaugeGuard) {
        (
            self.lock_wait_duration_seconds
                .with_label_values(&[lock, mode])
                .start_timer(),
            OwnedGaugeGuard::acquire(self.lock_waiters.with_label_values(&[lock, mode])),
        )
    }
}
