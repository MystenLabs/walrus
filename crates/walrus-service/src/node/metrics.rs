// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use prometheus::{
    core::{AtomicU64, GenericGaugeVec},
    HistogramVec,
    IntCounter,
    IntCounterVec,
    IntGaugeVec,
    Opts,
    Registry,
};

pub(crate) const STATUS_FAILURE: &str = "failure";
pub(crate) const STATUS_SUCCESS: &str = "success";
pub(crate) const STATUS_ABORTED: &str = "aborted";
pub(crate) const STATUS_CANCELLED: &str = "cancelled";
pub(crate) const STATUS_SKIPPED: &str = "skip";
pub(crate) const STATUS_INCONSISTENT: &str = "inconsistent";
pub(crate) const STATUS_QUEUED: &str = "queued";
pub(crate) const STATUS_PENDING: &str = "pending";
pub(crate) const STATUS_PERSISTED: &str = "persisted";
pub(crate) const STATUS_IN_PROGRESS: &str = "in-progress";

macro_rules! with_label {
    ($metric:expr, $label:expr) => {
        $metric.with_label_values(&[$label.as_ref()])
    };
}

pub(super) use with_label;

macro_rules! create_metric {
    ($metric_type:ty, $registry:ident, $opts:expr) => {{
        <$metric_type>::with_opts($opts).unwrap()
    }};
    ($metric_type:ty, $registry:ident, $opts:expr, $label_names:expr) => {{
        <$metric_type>::new($opts.into(), $label_names).unwrap()
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
        pub(crate) struct NodeMetricSet {
            $($( pub $metric: $metric_type ),*),*
        }

        impl NodeMetricSet {
            pub fn new(registry: &Registry) -> Self {
                Self { $($(
                    $metric: {
                        let metric = create_metric!(
                            $metric_type,
                            registry,
                            Opts::new(stringify!($metric), $descr).namespace("walrus")
                            $(, $labels)?
                        );

                        registry
                            .register(Box::new(metric.clone()))
                            .expect("metrics defined at compile time must be valid");

                        metric
                    }
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
        (slivers_stored_total, "The total number of slivers stored", &["sliver_type"]),
        (slivers_retrieved_total, "Total number of sliver instances returned", &["sliver_type"])
    ],
    GenericGaugeVec<AtomicU64>: [
        (event_cursor_progress, "The number of Walrus events processed", &["state"]),
    ],
    IntGaugeVec: [
        (recover_blob_backlog, "The number of blob recoveries currently pending", &["state"]),
    ],
    HistogramVec: [
        (
            event_process_duration_seconds,
            "Time (in seconds) spent processing events",
            &["event_type"]
        ),
        (
            recover_blob_duration_seconds,
            "Time (in seconds) spent recovering blobs",
            &["status"]
        ),
        (
            recover_blob_part_duration_seconds,
            "Time (in seconds) spent recovering metadata or slivers of blobs",
            &["part", "status"]
        )
    ]
}
