// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use prometheus::{
    core::{AtomicU64, GenericGauge},
    IntCounter,
    IntCounterVec,
    Opts,
    Registry,
};

macro_rules! increment {
    ($metrics:expr, $metric:ident) => {
        $metrics.$metric.inc()
    };
    ($metrics:expr, $metric:ident, label = $label:expr) => {
        $metrics
            .$metric
            .with_label_values(&[&$label.as_ref()])
            .inc()
    };
}
pub(super) use increment;

macro_rules! add {
    ($metrics:expr, $metric:ident, $amount:expr) => {
        $metrics.$metric.add($amount)
    };
}
pub(super) use add;

macro_rules! register_node_metric {
    ($metric_type:ty, $registry:ident, $opts:expr) => {{
        let metric = <$metric_type>::with_opts($opts).unwrap();
        $registry
            .register(Box::new(metric.clone()))
            .map(|()| metric)
            .expect("metrics defined at compile time must be valid")
    }};
    ($metric_type:ty, $registry:ident, $opts:expr, $label_names:expr) => {{
        let metric = <$metric_type>::new($opts, $label_names).unwrap();
        $registry
            .register(Box::new(metric.clone()))
            .map(|()| metric)
            .expect("metrics defined at compile time must be valid")
    }};
}

macro_rules! define_node_metric_set {
    (
        $(
            $metric_type:path: [
                $(( $metric:ident, $descr:literal $(, $labels:expr )? )),+ $(,)?
            ]
        ),+ $(,)?
    ) => {
        #[derive(Debug)]
        pub(super) struct NodeMetricSet {
            $($( pub $metric: $metric_type ),*),*
        }

        impl NodeMetricSet {
            pub fn new(registry: &Registry) -> Self {
                Self { $($(
                    $metric: register_node_metric!(
                        $metric_type,
                        registry,
                        Opts::new(stringify!($metric), $descr).namespace("walrus").subsystem("node")
                        $(, $labels)?
                    )
                ),*),*}
            }
        }
    };
}

define_node_metric_set! {
    IntCounter: [
        (metadata_stored_total, "The total number of metadata stored"),
        (metadata_retrieved_total, "The total number of metadata instances returned"),
        (storage_confirmations_issued_total, "The total number of storage confirmations issued")
    ],
    IntCounterVec: [
        (blob_events_total, "The total number of blob events observed", &["blob_event_type"]),
        (slivers_stored_total, "The total number of slivers stored", &["sliver_type"]),
        (slivers_retrieved_total, "Total number of sliver instances returned", &["sliver_type"])
    ],
    GenericGauge<AtomicU64>: [
        (events_sequentially_processed, "The number of Walurs events sequentially processed"),
    ]
}
