// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::time::Instant;

use once_cell::sync::OnceCell;
use prometheus::{IntGaugeVec, Registry, register_int_gauge_vec_with_registry};
use tap::TapFallible;

#[derive(Debug)]
pub struct MonitoredScopeMetrics {
    pub scope_iterations: IntGaugeVec,
    pub scope_duration_ns: IntGaugeVec,
    pub scope_entrance: IntGaugeVec,
}

impl MonitoredScopeMetrics {
    fn new(registry: &Registry) -> Self {
        Self {
            scope_entrance: register_int_gauge_vec_with_registry!(
                "walrus_monitored_scope_entrance",
                "Number of entrance in the scope.",
                &["scope_name"],
                registry,
            )
            .expect("metric registration cannot fail with valid parameters"),
            scope_iterations: register_int_gauge_vec_with_registry!(
                "walrus_monitored_scope_iterations",
                "Total number of times where the monitored scope runs",
                &["scope_name"],
                registry,
            )
            .expect("metric registration cannot fail with valid parameters"),
            scope_duration_ns: register_int_gauge_vec_with_registry!(
                "walrus_monitored_scope_duration_ns",
                "Total duration in nanosecs where the monitored scope is running",
                &["scope_name"],
                registry,
            )
            .expect("metric registration cannot fail with valid parameters"),
        }
    }
}

static MONITORED_SCOPE_METRICS: OnceCell<MonitoredScopeMetrics> = OnceCell::new();

pub fn init_monitored_scope_metrics(registry: &Registry) {
    let _ = MONITORED_SCOPE_METRICS
        .set(MonitoredScopeMetrics::new(registry))
        // this happens many times during tests
        .tap_err(|_| tracing::warn!("init_monitored_scope_metrics registry overwritten"));
}

pub fn get_monitored_scope_metrics() -> Option<&'static MonitoredScopeMetrics> {
    MONITORED_SCOPE_METRICS.get()
}

#[derive(Debug, Clone)]
#[must_use]
pub struct MonitoredScopeGuard {
    metrics: &'static MonitoredScopeMetrics,
    name: &'static str,
    timer: Instant,
}

impl Drop for MonitoredScopeGuard {
    fn drop(&mut self) {
        self.metrics
            .scope_duration_ns
            .with_label_values(&[self.name])
            .add(
                self.timer
                    .elapsed()
                    .as_nanos()
                    .try_into()
                    .unwrap_or(i64::MAX),
            );
        self.metrics
            .scope_entrance
            .with_label_values(&[self.name])
            .dec();
    }
}

/// This function creates a named scoped object, that keeps track of
/// - the total iterations where the scope is called in the `monitored_scope_iterations` metric.
/// - and the total duration of the scope in the `monitored_scope_duration_ns` metric.
///
/// The monitored scope should be single threaded, e.g. the scoped object encompass the lifetime of
/// a select loop or guarded by mutex.
/// Then the rate of `monitored_scope_duration_ns`, converted to the unit of sec / sec, would be
/// how full the single threaded scope is running.
pub fn monitored_scope(name: &'static str) -> Option<MonitoredScopeGuard> {
    let metrics = get_monitored_scope_metrics();
    if let Some(m) = metrics {
        m.scope_iterations.with_label_values(&[name]).inc();
        m.scope_entrance.with_label_values(&[name]).inc();
        Some(MonitoredScopeGuard {
            metrics: m,
            name,
            timer: Instant::now(),
        })
    } else {
        None
    }
}
