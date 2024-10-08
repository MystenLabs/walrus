// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use prometheus::{
    core::{AtomicU64, GenericGauge, GenericGaugeVec},
    HistogramVec,
    IntCounter,
    IntCounterVec,
    IntGaugeVec,
    Opts,
    Registry,
};
use walrus_event::EventStreamElement;
use walrus_sui::types::{BlobCertified, BlobEvent, ContractEvent, EpochChangeEvent};

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

pub(crate) const EPOCH_STATE_CHANGE_SYNC: &str = "epoch_change_sync";
pub(crate) const EPOCH_STATE_CHANGE_DONE: &str = "epoch_change_done";
pub(crate) const EPOCH_STATE_NEXT_PARAMS_SELECTED: &str = "next_params_selected";

macro_rules! with_label {
    ($metric:expr, $label:expr) => {
        $metric.with_label_values(&[$label.as_ref()])
    };
}

pub(super) use with_label;

macro_rules! create_metric {
    ($metric_type:ty, $registry:ident, $opts:expr) => {{
        <$metric_type>::with_opts($opts)
            .expect("this must be called with valid metrics type and options")
    }};
    ($metric_type:ty, $registry:ident, $opts:expr, $label_names:expr) => {{
        <$metric_type>::new($opts.into(), $label_names)
            .expect("this must be called with valid metrics type, options, and labels")
    }};
}

macro_rules! define_metric_set {
    (
        $name:ident;
        $(
            $metric_type:path: [
                $(( $metric:ident, $descr:literal $(, $labels:expr )? )),+ $(,)?
            ]
        ),+ $(,)?
    ) => {
        #[derive(Debug)]
        pub(crate) struct $name {
            $($( pub $metric: $metric_type ),*),*
        }

        impl $name {
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

define_metric_set! {
    NodeMetricSet;
    IntCounter: [
        (metadata_stored_total, "The total number of metadata stored"),
        (metadata_retrieved_total, "The total number of metadata instances returned"),
        (storage_confirmations_issued_total, "The total number of storage confirmations issued")
    ],
    IntCounterVec: [
        (slivers_stored_total, "The total number of slivers stored", &["sliver_type"]),
        (slivers_retrieved_total, "Total number of sliver instances returned", &["sliver_type"]),
    ],
    GenericGaugeVec<AtomicU64>: [
        (event_cursor_progress, "The number of Walrus events processed", &["state"]),
    ],
    IntGaugeVec: [
        (recover_blob_backlog, "The number of blob recoveries currently pending", &["state"]),
    ],
    HistogramVec: [
        (
            event_process_duration_seconds, "Time (in seconds) spent processing events",
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

define_metric_set! {
    CommitteeServiceMetricSet;
    GenericGauge<AtomicU64>: [
        (current_epoch, "The current Walrus epoch"),
    ],
    IntGaugeVec: [
        (current_epoch_state, "The state of the current walrus epoch", &["state"]),
    ]
}

pub(crate) trait TelemetryLabel {
    fn label(&self) -> &'static str;
}

impl TelemetryLabel for BlobEvent {
    fn label(&self) -> &'static str {
        match self {
            BlobEvent::Registered(_) => "registered",
            BlobEvent::Certified(event) => event.label(),
            BlobEvent::Deleted(_) => "deleted",
            BlobEvent::InvalidBlobID(_) => "invalid-blob",
        }
    }
}

impl TelemetryLabel for EpochChangeEvent {
    fn label(&self) -> &'static str {
        match self {
            EpochChangeEvent::EpochParametersSelected(_) => "epoch-parameters-selected",
            EpochChangeEvent::EpochChangeStart(_) => "epoch-change-start",
            EpochChangeEvent::EpochChangeDone(_) => "epoch-change-done",
            EpochChangeEvent::ShardsReceived(_) => "shards-received",
            EpochChangeEvent::ShardRecoveryStart(_) => "shard-recovery-start",
        }
    }
}

impl TelemetryLabel for ContractEvent {
    fn label(&self) -> &'static str {
        match self {
            ContractEvent::BlobEvent(event) => event.label(),
            ContractEvent::EpochChangeEvent(event) => event.label(),
        }
    }
}

impl TelemetryLabel for EventStreamElement {
    fn label(&self) -> &'static str {
        match self {
            EventStreamElement::ContractEvent(event) => event.label(),
            EventStreamElement::CheckpointBoundary => "end-of-checkpoint",
        }
    }
}

impl TelemetryLabel for BlobCertified {
    fn label(&self) -> &'static str {
        "certified"
    }
}
