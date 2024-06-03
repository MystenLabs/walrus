// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use prometheus::{IntCounter, IntCounterVec, Registry};

macro_rules! increment {
    ($this:ident, $metric:ident) => {
        $this.metrics.get(NodeMetric::$metric).inc()
    };
    ($this:ident, $metric:ident, label = $label:expr) => {
        $this
            .metrics
            .get_with_label_values(NodeMetric::$metric, &[&$label.to_string()])
            .inc()
    };
}

pub(super) use increment;

macro_rules! define_node_metrics {
    ($($metric:ident: ($str_repr:literal, $descr:literal, $variable_labels:expr)),+ $(,)?) => {

        #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Clone)]
        #[allow(clippy::enum_variant_names)]
        pub(super) enum NodeMetric {
            $($metric),+
        }

        impl NodeMetric {
            const METRICS: &'static [Self] = &[$(Self::$metric),+];

            fn as_str(&self) -> &'static str {
                match self {
                    $(Self::$metric => $str_repr),+
                }
            }

            fn variable_labels(&self) -> &'static [&'static str] {
                match self {
                    $(Self::$metric => $variable_labels),+
                }
            }

            fn opts(&self) -> prometheus::Opts {
                match self {
                    $(Self::$metric => {
                        prometheus::Opts::new(self.as_str(), $descr)
                            .namespace("walrus")
                            .subsystem("node")
                    }),+
                }
            }
        }
    };
}

define_node_metrics! {
    BlobEventsTotal: (
        "blob_events_total", "The total number of blob events observed", &["blob_event_type"]
    ),
    MetadataStoredTotal: ("metadata_stored_total", "The total number of metadata stored", &[]),
    MetadataRetrievedTotal: (
        "metadata_retrieved_total", "The total number of metadata instances returned", &[]
    ),
    SliversStoredTotal: (
        "slivers_stored_total", "The total number of slivers stored", &["sliver_type"]
    ),
    SliversRetrievedTotal: (
        "slivers_retrieved_total", "Total number of sliver instances returned", &["sliver_type"]
    ),
    StorageConfirmationsIssuedTotal: (
        "storage_confirmations_issued_total",
        "The total number of storage confirmations issued",
        &[]
    ),
}

#[derive(Debug, Clone)]
enum PrometheusMetric {
    IntCounter(IntCounter),
    IntCounterVec(IntCounterVec),
}

#[derive(Debug, Default)]
pub(super) struct NodeMetricSet {
    metrics: HashMap<NodeMetric, PrometheusMetric>,
}

impl NodeMetricSet {
    pub fn new(registry: &Registry) -> Self {
        let mut this = Self::default();

        NodeMetric::METRICS
            .iter()
            .for_each(|&metric| this.register_metric(registry, metric));

        this
    }

    #[must_use]
    pub fn get_with_label_values(&self, metric: NodeMetric, label_values: &[&str]) -> IntCounter {
        let PrometheusMetric::IntCounterVec(ref metric) = self.metrics[&metric] else {
            panic!("attempt to retrieve a non-labelled metric using a label");
        };

        metric.with_label_values(label_values)
    }

    #[must_use]
    pub fn get(&self, metric: NodeMetric) -> &IntCounter {
        let PrometheusMetric::IntCounter(ref metric) = self.metrics[&metric] else {
            panic!("attempt to retrieve a non-int-counter metric as an int counter");
        };
        metric
    }

    fn register_metric(&mut self, registry: &Registry, metric: NodeMetric) {
        let handle = if metric.variable_labels().is_empty() {
            PrometheusMetric::IntCounter(
                prometheus::register_int_counter_with_registry!(metric.opts(), registry)
                    .expect("metrics defined at compile time must be valid"),
            )
        } else {
            PrometheusMetric::IntCounterVec(
                prometheus::register_int_counter_vec_with_registry!(
                    metric.opts(),
                    metric.variable_labels(),
                    registry
                )
                .expect("metrics defined at compile time must be valid"),
            )
        };

        self.metrics.insert(metric, handle);
    }
}
