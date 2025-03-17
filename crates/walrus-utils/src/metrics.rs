// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Defines a set of prometheus metrics.
///
/// # Example
///
/// ```
/// define_metric_set! {
///     /// Docstring applied to the containing struct.
///     #[namespace = "walrus"]
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
            pub fn new(registry: &prometheus::Registry) -> Self {
                Self { $(
                    $field_name: {
                        let opts = Opts::new(stringify!($field_name), $help_str)
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
    ($field_type:ty, $opts:expr, [$($label_names:tt)+]) => {{
        <$field_type>::new($opts.into(), &[$($label_names)+])
            .expect("this must be called with valid metrics type and options")
    }};
    (Histogram, $opts:expr, {buckets: $buckets:expr $(,)?}) => {{
        let mut opts: prometheus::HistogramOpts = $opts.into();
        opts.buckets = $buckets.into();

        prometheus::Histogram::with_opts(opts)
            .expect("this must be called with valid metrics type and options")
    }};
    (HistogramVec, $opts:expr, {labels: [$($label_names:tt)+], buckets: $buckets:expr $(,)?}) => {{
        let mut opts: prometheus::HistogramOpts = $opts.into();
        opts.buckets = $buckets.into();

        prometheus::HistogramVec::new(opts, &[$($label_names)+])
            .expect("this must be called with valid metrics type and options")
    }};
}

pub use create_metric;

#[macro_export]
macro_rules! with_label {
    ($metric:expr, $label:expr) => {
        $metric.with_label_values(&[$label.as_ref()])
    };
    ($metric:expr, $label1:expr, $label2:expr) => {
        $metric.with_label_values(&[$label1.as_ref(), $label2.as_ref()])
    };
    ($metric:expr, $label1:expr, $label2:expr, $label3:expr) => {
        $metric.with_label_values(&[$label1.as_ref(), $label2.as_ref(), $label3.as_ref()])
    };
}
