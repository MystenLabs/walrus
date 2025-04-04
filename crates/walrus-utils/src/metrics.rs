// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use prometheus::IntGauge;

#[cfg(all(feature = "tokio-metrics", feature = "metrics"))]
mod tokio;

#[cfg(all(feature = "tokio-metrics", feature = "metrics"))]
pub use tokio::{TaskMonitorCollector, TaskMonitorFamily};

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

            /// Create a unique ID for this instance of the defined metric set, which will be used
            /// to separate multiple instantiations of the metric, so that they can be registered
            /// together.
            fn metric_set_instance_id() -> String {
                static TYPE_LOCAL_ID: std::sync::atomic::AtomicUsize =
                    std::sync::atomic::AtomicUsize::new(0);
                format!(
                    "{:?}::{}",
                    std::any::TypeId::of::<Self>(),
                    TYPE_LOCAL_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                )
            }

            /// Creates a new instance of the metric set.
            ///
            /// The instance is created with a const-label of `metric_set_instance_id` set to a
            /// unique value derived from the struct's type and an instance counter. This allows
            /// multiple instances of this metric set to be registered on the same registry.
            ///
            /// See [`Self::new_with_const_labels`] for more information.
            ///
            /// # Panics
            ///
            /// Panics if the metrics are not unique on the registry, or have different sets of
            /// labels or constant labels.
            pub fn new(registry: &::prometheus::Registry) -> Self {
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
            pub fn new_with_id(registry: &::prometheus::Registry, id: String) -> Self {
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
                registry: &::prometheus::Registry,
                const_labels: I
            ) -> Self
                where I: ::std::iter::IntoIterator<Item = (String, String)>,
            {
                Self::new_inner(registry, const_labels.into_iter().collect())
            }

            fn new_inner(
                registry: &::prometheus::Registry,
                mut const_labels: ::std::collections::HashMap<String, String>
            ) -> Self {
                const_labels.entry("metric_set_instance_id".to_owned())
                    .or_insert_with(|| Self::metric_set_instance_id());

                Self { $(
                    $field_name: {
                        let opts = ::prometheus::Opts::new(stringify!($field_name), $help_str)
                            .const_labels(const_labels.clone())
                            .namespace($namespace);
                        let metric = $crate::create_metric!($field_type, opts, $field_def);
                        registry
                            .register(Box::new(metric.clone()))
                            .expect("metrics defined at compile time must be valid");
                        metric
                    },
                )* $(
                    $new_type_field: {
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

/// Increments gauge when acquired, decrements when guard drops
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
