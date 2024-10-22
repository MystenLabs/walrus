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
pub(crate) use telemetry::with_label;
use walrus_event::EventStreamElement;
use walrus_sui::types::{BlobCertified, BlobEvent, ContractEvent, EpochChangeEvent};

use crate::common::telemetry::{self, CurrentEpochMetric, CurrentEpochStateMetric};

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

telemetry::define_metric_set! {
    NodeMetricSet;
    IntCounter: [
        (metadata_stored_total, "The total number of metadata stored"),
        (metadata_retrieved_total, "The total number of metadata instances returned"),
        (storage_confirmations_issued_total, "The total number of storage confirmations issued")
    ],
    IntCounterVec: [
        (slivers_stored_total, "The total number of slivers stored", &["sliver_type"]),
        (slivers_retrieved_total, "Total number of sliver instances returned", &["sliver_type"]),
        (shard_sync_total, "The number of shard sync per status", &["status"]),
        (sync_shard_sync_sliver_total,
            "Total number of slivers synced during shard sync", &["shard"]),
        (sync_shard_recover_sliver_total,
            "Total number of slivers started recovery during shard sync", &["shard"]),
        (sync_shard_recover_sliver_success_total,
            "Total number of slivers successfully recovered during shard sync", &["shard"]),
        (sync_shard_recover_sliver_error_total,
            "Total number of slivers failed to recover during shard sync", &["shard"]),
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

telemetry::define_metric_set! {
    CommitteeServiceMetricSet;
    @TypedMetrics: [
        (current_epoch, CurrentEpochMetric),
        (current_epoch_state, CurrentEpochStateMetric),
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
