// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! API types.

use std::cmp::{Ordering, Reverse};

use serde::{Deserialize, Serialize};
use sui_types::event::EventID;
use tokio::time::Duration;
use walrus_core::{Epoch, PublicKey};

use crate::error::ServiceError;

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
    Error {
        /// The error code.
        code: u16,
        /// The error message.
        message: String,
        /// Optionally contains a more detailed server side reason for the error.
        #[serde(flatten)]
        reason: Option<ServiceError>,
    },
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
        status_event: EventID,
        /// Counts of deletable `Blob` objects.
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
        deletable_counts: DeletableCounts,
    },
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
    /// The sliver or metadata is stored on the storage node.
    Stored,
}

/// Represents information about the health of the storage node service.
#[derive(Debug, Deserialize, Serialize, utoipa::ToSchema)]
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
}
