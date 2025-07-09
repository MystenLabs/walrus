// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    any::Any,
    collections::HashMap,
    sync::{Arc, Mutex},
};

use prometheus::{IntGauge, core::Collector, proto::MetricFamily};

#[cfg(all(feature = "tokio-metrics", feature = "metrics"))]
mod tokio;

#[cfg(feature = "metrics")]
pub mod monitored_scope;

#[cfg(all(feature = "tokio-metrics", feature = "metrics"))]
pub use tokio::{TaskMonitorCollector, TaskMonitorFamily};

/// Errors returned during registration of a collector with [`Registry::register`].
#[derive(Debug, thiserror::Error)]
pub enum RegistrationError {
    /// Error returned when collectors with the same name have different types (e.g., counter and
    /// gauge with the same fully-qualified name).
    #[error("a collector with the same ID was already registered but with a different type")]
    InconsistentType,

    /// Distinct collectors (such as a histogram) should not have an overlapping set of metrics
    /// (i.e., metrics with the same fully-qualified name).
    ///
    /// This could also be raised if a collector was registered directly on the inner
    /// [`prometheus::Registry`]. To avoid this, ensure that all registrations go through a clone
    /// of a single registry.
    #[error("at least one metric in the collector has already been registered, ensure no overlaps")]
    MetricsOverlap,

    /// Errors raised by the inner [`prometheus::Registry::register`].
    ///
    /// This will never be [`prometheus::Error::AlreadyReg`].
    #[error(transparent)]
    Prometheus(prometheus::Error),
}

/// A thin wrapper around [`prometheus::Registry`] that returns the existing metric on attempts
/// to register the same metric twice.
///
// NB(jsmith): Can be removed if rust-prometheus issues #495 or #248 are ever resolved to provide an
// alternative solution:
// github.com/tikv/rust-prometheus/issues/495, github.com/tikv/rust-prometheus/issues/248
#[derive(Debug, Clone, Default)]
pub struct Registry {
    inner: prometheus::Registry,
    collectors_by_id: Arc<Mutex<HashMap<u64, Box<dyn Any + Send>>>>,
}

impl Registry {
    /// Returns a new instance of the registry wrapping the provided [`prometheus::Registry`]
    pub fn new(inner: prometheus::Registry) -> Self {
        Self {
            inner,
            collectors_by_id: Default::default(),
        }
    }

    /// Gets a previously registered collector that matches the provided collector,
    /// or registers that collector and returns it.
    #[must_use = "use the returned value as the given collector may have already been registered"]
    pub fn get_or_register<T>(&self, collector: T) -> Result<T, RegistrationError>
    where
        T: Collector + Send + Clone + 'static,
    {
        let result = self.inner.register(Box::new(collector.clone()));

        match result {
            Err(prometheus::Error::AlreadyReg) => (), // This case is handled below
            Ok(()) => {
                let collector_id = Self::collector_id(&collector);
                let prior_collector = self
                    .collectors_by_id
                    .lock()
                    .expect("critical section shouldnt panic")
                    .insert(collector_id, Box::new(collector.clone()));
                assert!(
                    prior_collector.is_none(),
                    "collectors should not be unregistered"
                );

                return Ok(collector);
            }
            Err(other) => return Err(RegistrationError::Prometheus(other)),
        }

        // The registry returned AlreadyReg, which it does if ANY of the contained metrics in the
        // collector is already registered. Therefore, we check if a collector with the same ID has
        // already been registered. If so, then since the ID of the collector considers the IDs of
        // the inner metrics, the conflict is likely caused by this exact collector being previously
        // registered, and not two collectors with different IDs but overlapping metrics being
        // registered.
        let collector_id = Self::collector_id(&collector);
        let collectors_by_id = self
            .collectors_by_id
            .lock()
            .expect("critical section shouldnt panic");

        let any_collector = collectors_by_id
            .get(&collector_id)
            .ok_or(RegistrationError::MetricsOverlap)?;

        any_collector
            .downcast_ref::<T>()
            .cloned()
            .ok_or(RegistrationError::InconsistentType)
    }

    /// Registers a metric on the underlying registry.
    pub fn register(&self, collector: Box<dyn Collector>) -> Result<(), prometheus::Error> {
        self.inner.register(collector)
    }

    /// Returns an ID for the collector.
    fn collector_id<T: Collector>(collector: &T) -> u64 {
        // This is the approach used by `prometheus::Registry::register` for constructing the ID of
        // a collector (as of 2025-04-07).
        collector
            .desc()
            .into_iter()
            .fold(0u64, |collector_id, desc| {
                collector_id.wrapping_add(desc.id)
            })
    }

    /// Calls `prometheus::Registry::gather()`.
    pub fn gather(&self) -> Vec<MetricFamily> {
        self.inner.gather()
    }
}

/// Defines a set of prometheus metrics.
///
/// # Example
///
/// ```ignore
/// walrus_utils::define_metric_set! {
///     #[namespace = "walrus"]
///     /// Docstring applied to the containing struct.
///     struct MyMetricSet {
///         // Gauges, counters, and histograms can be defined with an empty `[]`.
///         #[help = "Help text and docstring for this metric"]
///         my_int_counter: IntCounter[],
///         #[help = "Help text for the my_histogram field"]
///         my_histogram: Histogram[],
///
///         // Vec-type metrics have their label names specified in the brackets.
///         #[help = "Help text for the int_counter_vec field"]
///         int_counter_vec: IntCounterVec["label1", "label2"],
///         #[help = "Help text for the my_histogram_vec field"]
///         my_histogram_vec: HistogramVec["label1", "label2"],
///
///         // `Histogram` and `HistogramVec` can additionally have their buckets specified.
///         #[help = "Help text for the my_histogram_with_buckets field"]
///         my_histogram_with_buckets: Histogram{buckets: vec![0.25, 1.0, 10.0]},
///         #[help = "Help text for the my_histogram_vec_with_buckets field"]
///         my_histogram_vec_with_buckets: HistogramVec{
///             labels: ["field1", "field2"], buckets: vec![1.0, 2.0]
///         },
///
///         // New-type metrics can be used to define metrics, and are any types that implement both
///         // `Default` and `Into<Box<dyn Collector>>`.
///         typed_metric: CurrentEpochMetric,
///     }
/// }
/// ```
#[macro_export]
macro_rules! define_metric_set {
    (
        #[namespace = $namespace:literal]
        $(#[$outer:meta])*
        $vis:vis struct $name:ident {
            $($new_type_field:ident: $new_type_field_type:ident,)*
            $(
                #[help = $help_str:literal]
                $field_name:ident: $field_type:ident $field_def:tt
            ),* $(,)?
        }
    ) => {
        $(#[$outer])*
        #[derive(Debug, Clone)]
        $vis struct $name {
            $(
                #[doc = $help_str]
                pub $field_name: $field_type,
            )*
            $(
                pub $new_type_field: $new_type_field_type,
            )*
        }

        impl $name {
            /// The namespace in which the metrics reside.
            pub const NAMESPACE: &'static str = $namespace;

            /// Creates a new instance of the metric set.
            pub fn new(registry: &$crate::metrics::Registry) -> Self {
                Self::new_inner(registry, Default::default())
            }

            /// Creates a new instance of the metric set.
            ///
            /// The instance is created with a const-label of `custom_id` set to `id`.
            ///
            /// See [`Self::new_with_const_labels`] for more information.
            ///
            /// # Panics
            ///
            /// Panics if the metrics are not unique on the registry, or have different sets of
            /// labels or constant labels.
            pub fn new_with_id(registry: &$crate::metrics::Registry, id: String) -> Self {
                Self::new_with_const_labels(registry, [("custom_id".to_owned(), id)])
            }

            /// Creates a new instance of the metric set with the specified constant labels.
            ///
            /// Constant labels allow multiple instances of metrics to be registered on a
            /// given prometheus registry. However, all instances of each metric must have the same
            /// set of constant labels, and the values of the constant labels must be differ for
            /// each instance.
            ///
            /// Therefore, when using this method, it is the callers responsibility to ensure that
            /// for each metric defined in the set, its set of labels and constant labels are
            /// consistent with the same metric being defined elsewhere and being registered to the
            /// registry.
            ///
            /// # Panics
            ///
            /// Panics if the metrics are not unique on the registry, or have different sets of
            /// labels or constant labels.
            pub fn new_with_const_labels<I>(
                registry: &$crate::metrics::Registry,
                const_labels: I
            ) -> Self
                where I: ::std::iter::IntoIterator<Item = (String, String)>,
            {
                Self::new_inner(registry, const_labels.into_iter().collect())
            }

            fn new_inner(
                registry: &$crate::metrics::Registry,
                const_labels: ::std::collections::HashMap<String, String>
            ) -> Self {
                Self { $(
                    $field_name: {
                        let opts = ::prometheus::Opts::new(stringify!($field_name), $help_str)
                            .const_labels(const_labels.clone())
                            .namespace($namespace);
                        let metric = $crate::create_metric!($field_type, opts, $field_def);
                        registry.get_or_register(metric)
                            .expect("metrics defined at compile time must be valid")
                    },
                )* $(
                    $new_type_field: {
                        // TODO(jsmith): See if it makes sense to cache this.
                        let metric = $new_type_field_type::default();
                        registry
                            .register(metric.clone().into())
                            .expect("metrics defined at compile time must be valid");
                        metric
                    }
                ),* }
            }
        }
    };
}

pub use define_metric_set;

#[macro_export]
macro_rules! create_metric {
    ($field_type:ty, $opts:expr, []) => {{
        <$field_type>::with_opts($opts.into())
            .expect("this must be called with valid metrics type and options")
    }};
    (Histogram, $opts:expr, {buckets: $buckets:expr $(,)?}) => {{
        let mut opts: ::prometheus::HistogramOpts = $opts.into();
        opts.buckets = $buckets.into();

        ::prometheus::Histogram::with_opts(opts)
            .expect("this must be called with valid metrics type and options")
    }};
    (HistogramVec, $opts:expr, {labels: $label_names:expr, buckets: $buckets:expr $(,)?}) => {{
        let mut opts: ::prometheus::HistogramOpts = $opts.into();
        opts.buckets = $buckets.into();

        ::prometheus::HistogramVec::new(opts, &$label_names)
            .expect("this must be called with valid metrics type and options")
    }};
    ($field_type:ty, $opts:expr, $label_names:expr) => {{
        <$field_type>::new($opts.into(), &$label_names)
            .expect("this must be called with valid metrics type and options")
    }};
}

pub use create_metric;

#[macro_export]
macro_rules! with_label {
    ($metric:expr, $($label:expr),+$(,)?) => {
        $metric.with_label_values(&[$($label.as_ref()),+])
    };
}

/// Returns 21 buckets from <= 128 bytes to approx. <= 134 MB.
///
/// As prometheus includes a bucket to +Inf, values over 134 MB are still counted.
pub fn default_buckets_for_bytes() -> Vec<f64> {
    prometheus::exponential_buckets(128.0, 2.0, 21).expect("count, start, and factor are valid")
}

/// Concatenates to the two label lists into a vector.
pub fn concat_labels<'a>(first: &[&'a str], second: &[&'a str]) -> Vec<&'a str> {
    let mut output = Vec::with_capacity(first.len() + second.len());
    output.extend_from_slice(first);
    output.extend_from_slice(second);
    output
}

/// Increments gauge when acquired, decrements when guard drops.
#[derive(Debug, Clone)]
#[must_use]
pub struct OwnedGaugeGuard(IntGauge);

impl OwnedGaugeGuard {
    /// Increment and take ownership of the gauge.
    pub fn acquire(gauge: IntGauge) -> Self {
        gauge.inc();
        Self(gauge)
    }
}

impl Drop for OwnedGaugeGuard {
    fn drop(&mut self) {
        self.0.dec();
    }
}

#[cfg(test)]
mod tests {
    use prometheus::{Counter, Gauge};

    use super::*;

    #[test]
    fn allows_repeated_registrations() {
        let registry = Registry::default();
        let name = "my_counter";
        let help = "a simple counter";

        let _ = registry
            .get_or_register(Counter::new(name, help).unwrap())
            .expect("must register successfully");

        let _ = registry
            .get_or_register(Counter::new(name, help).unwrap())
            .expect("must repeatedly register successfully");
    }

    #[test]
    fn fails_for_collectors_with_different_types_but_same_name() {
        let registry = Registry::default();
        let name = "my_counter";
        let help = "a simple counter";

        let _ = registry
            .get_or_register(Counter::new(name, help).unwrap())
            .expect("must register successfully");

        let error = registry
            .get_or_register(Gauge::new(name, help).unwrap())
            .expect_err("must fail for different type but same name");
        assert!(matches!(error, RegistrationError::InconsistentType));
    }
}
