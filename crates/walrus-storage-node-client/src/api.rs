// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! API types.

use std::cmp::{Ordering, Reverse};

use serde::{Deserialize, Serialize};
use sui_types::event::EventID;
use tokio::time::Duration;
use utoipa::openapi::Ref;
use walrus_core::{Epoch, PublicKey, ShardIndex};

use self::errors::Status;

pub mod errors;

/// Error message returned by the service.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ServiceResponse<T> {
    /// The request was successful.
    Success {
        /// The success code.
        code: u16,
        /// The data returned by the service.
        data: T,
    },

    /// The error message returned by the service.
    #[serde(untagged)]
    Error(Status),
}

/// Contains the certification status of a blob.
///
/// If the a permanent blob exists, it also contains its end epoch and the ID of the Sui event
/// from which the latest status (registered or certified) resulted.
#[derive(
    Debug, Deserialize, Serialize, PartialEq, Eq, Clone, Copy, Default, Hash, utoipa::ToSchema,
)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum BlobStatus {
    /// The blob does not exist (anymore) within Walrus.
    #[default]
    Nonexistent,
    /// The blob ID has been marked as invalid.
    Invalid {
        /// The ID of the Sui event in which the blob was marked as invalid.
        #[schema(schema_with = event_id_schema)]
        event: EventID,
    },
    /// The blob exists within Walrus in a permanent state.
    Permanent {
        /// The latest epoch at which the blob expires (non-inclusive).
        #[schema(value_type = u64)]
        end_epoch: Epoch,
        /// Whether the blob is certified (true) or only registered (false).
        is_certified: bool,
        /// The ID of the Sui event that caused the status with the given `end_epoch`.
        #[schema(schema_with = event_id_schema)]
        status_event: EventID,
        /// Counts of deletable `Blob` objects.
        #[schema(inline)]
        deletable_counts: DeletableCounts,
        /// If the blob is certified, contains the epoch where it was initially certified.
        initial_certified_epoch: Option<Epoch>,
    },
    /// The blob exists within Walrus; but there is no related permanent object, so it may be
    /// deleted at any time.
    Deletable {
        /// If the blob is certified, contains the epoch where it was initially certified.
        // INV: certified_epoch.is_some() == count_deletable_certified > 0
        initial_certified_epoch: Option<Epoch>,
        /// Counts of deletable `Blob` objects.
        #[schema(inline)]
        deletable_counts: DeletableCounts,
    },
}

fn event_id_schema() -> Ref {
    Ref::new("#/components/schemas/EventID")
}

impl BlobStatus {
    /// Returns the certification epoch for the blob, or `None` if the blob is not certified.
    pub fn initial_certified_epoch(&self) -> Option<Epoch> {
        match self {
            BlobStatus::Permanent {
                initial_certified_epoch,
                ..
            }
            | BlobStatus::Deletable {
                initial_certified_epoch,
                ..
            } => *initial_certified_epoch,
            _ => None,
        }
    }

    /// Returns true iff the blob is registered within Walrus.
    pub fn is_registered(&self) -> bool {
        matches!(self, Self::Deletable { .. } | Self::Permanent { .. })
    }
}

/// Contains counts of all and certified deletable `Blob` objects.
#[derive(
    Debug, Deserialize, Serialize, PartialEq, Eq, Clone, Copy, Default, Hash, utoipa::ToSchema,
)]
pub struct DeletableCounts {
    /// Total number of active deletable `Blob` objects for the given blob ID.
    pub count_deletable_total: u32,
    /// Number of certified deletable `Blob` objects for the given blob ID.
    pub count_deletable_certified: u32,
}

impl PartialOrd for DeletableCounts {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DeletableCounts {
    fn cmp(&self, other: &Self) -> Ordering {
        // Tuples are compared using lexicographic ordering.
        (self.count_deletable_certified, self.count_deletable_total)
            .cmp(&(other.count_deletable_certified, other.count_deletable_total))
    }
}

impl PartialOrd for BlobStatus {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BlobStatus {
    fn cmp(&self, other: &Self) -> Ordering {
        use BlobStatus::*;

        match (self, other) {
            (s, o) if s == o => Ordering::Equal,
            // Nonexistent is the "smallest" status.
            (Nonexistent, _) => Ordering::Less,
            (_, Nonexistent) => Ordering::Greater,
            // Invalid is the "largest" status.
            (Invalid { .. }, Invalid { .. }) => Ordering::Equal,
            (Invalid { .. }, _) => Ordering::Greater,
            (_, Invalid { .. }) => Ordering::Less,
            // Permanent is "larger" than Deletable.
            (Permanent { .. }, Deletable { .. }) => Ordering::Greater,
            (Deletable { .. }, Permanent { .. }) => Ordering::Less,
            // For Deletable, first compare certified blobs, then all, then finally compare the
            // initial certification epoch, preferring smaller values.
            (
                Deletable {
                    initial_certified_epoch,
                    deletable_counts,
                },
                Deletable {
                    initial_certified_epoch: initial_certified_epoch_other,
                    deletable_counts: deletable_counts_other,
                },
            ) => (deletable_counts, Reverse(initial_certified_epoch)).cmp(&(
                deletable_counts_other,
                Reverse(initial_certified_epoch_other),
            )),
            // For Permanent, compare status, end epochs, count of deletable blobs, and initial
            // certification epoch (preferring smaller values), in this order.
            (
                Permanent {
                    end_epoch,
                    is_certified,
                    deletable_counts,
                    initial_certified_epoch,
                    ..
                },
                Permanent {
                    end_epoch: end_epoch_other,
                    is_certified: is_certified_other,
                    deletable_counts: deletable_counts_other,
                    initial_certified_epoch: initial_certified_epoch_other,
                    ..
                },
            ) => {
                // Tuples are compared using lexicographic ordering.
                (
                    is_certified,
                    end_epoch,
                    deletable_counts,
                    Reverse(initial_certified_epoch),
                )
                    .cmp(&(
                        is_certified_other,
                        end_epoch_other,
                        deletable_counts_other,
                        Reverse(initial_certified_epoch_other),
                    ))
            }
        }
    }
}

/// Contains the storage status of a sliver or metadata.
#[derive(
    Debug, Deserialize, Serialize, PartialEq, Eq, Clone, Copy, Default, Hash, utoipa::ToSchema,
)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum StoredOnNodeStatus {
    /// The sliver or metadata does not exist on the storage node.
    #[default]
    Nonexistent,
    /// The sliver or metadata has been accepted and is buffered in memory, waiting for
    /// registration to complete before it is persisted.
    Buffered,
    /// The sliver or metadata is stored on the storage node.
    Stored,
}

/// Represents information about the health of the storage node service.
#[derive(Debug, Deserialize, Serialize, Clone, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServiceHealthInfo {
    /// The uptime of the service.
    #[schema(value_type = Object)]
    pub uptime: Duration,
    /// The epoch of the storage node.
    #[schema(value_type = u64)]
    pub epoch: Epoch,
    /// The public key of the storage node.
    #[schema(value_type = [u8], format = "Base58")]
    pub public_key: PublicKey,
    /// The status of the storage node.
    pub node_status: String,
    /// The event progress of the storage node.
    #[schema(inline)]
    pub event_progress: EventProgress,
    /// The overall status of the shards.
    pub shard_summary: ShardStatusSummary,
    /// The status of the shards for which the node is responsible.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard_detail: Option<ShardStatusDetail>,
    /// The latest checkpoint sequence number downloaded by the node.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_checkpoint_sequence_number: Option<u64>,
}

/// The status of the shards for which the node is responsible.
#[derive(Debug, Default, Clone, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct OwnedShardStatus {
    /// The number of owned shards in an unknown state.
    pub unknown: usize,
    /// The number of owned shards that are up-to-date for the epoch.
    pub ready: usize,
    /// The number of owned shards that are being transferred to the node.
    pub in_transfer: usize,
    /// The number of owned shards that are being recovered.
    pub in_recovery: usize,
}

/// Summary of the shard statuses.
///
/// Summarises the number of nodes for which this node is responsible, as well as those that are
/// being transferred to another storage node.
#[derive(Debug, Default, Clone, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ShardStatusSummary {
    /// The number of shards, for which this node is responsible.
    ///
    /// Their statuses are summarized in `owned_shard_status`.
    pub owned: usize,
    /// The statuses of the shards for which this node is responsible.
    #[schema(inline)]
    pub owned_shard_status: OwnedShardStatus,
    /// The number of shards, no longer owned by the node, that are read only,
    /// i.e., only serving reads from this node.
    pub read_only: usize,
}

/// Detail statuses of individual shards.
///
/// Provides the status of each shard for which the node is responsible. Additionally, will provide
/// the status of shards which the node is not responsible for in the current epoch, but
/// nonetheless currently stores. These will not appear in the [`ShardStatusSummary`].
#[derive(Debug, Default, Clone, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ShardStatusDetail {
    /// Statuses of the shards for which the node is responsible in this epoch.
    pub owned: Vec<ShardHealthInfo>,
    /// Statuses of other shards the node currently stores.
    pub other: Vec<ShardHealthInfo>,
}

/// A shard with its status.
#[derive(Debug, Clone, Deserialize, Serialize, utoipa::ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ShardHealthInfo {
    /// The identifier of the shard in the walrus system.
    #[schema(value_type = u16)]
    pub shard: ShardIndex,
    /// The status of the shard, None if unavailable.
    pub status: ShardStatus,
}

/// The current state of a shard on the storage node.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum ShardStatus {
    /// The status of the shard is indeterminate.
    Unknown,
    /// The shard is ready and up-to-date.
    Ready,
    /// The shard is being transferred to this storage node.
    InTransfer,
    /// The shard is being recovered.
    InRecovery,
    /// The shard is currently not accepting any more writes.
    ReadOnly,
}

/// Represents the progress of the events.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, utoipa::ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EventProgress {
    /// The number of events that have been persisted.
    pub persisted: u64,
    /// The number of events that are pending in memory.
    pub pending: u64,
    /// The highest event index that has been finished.
    pub highest_finished_event_index: Option<u64>,
}
