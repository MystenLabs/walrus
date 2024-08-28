// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! API types.

use std::cmp::Ordering;

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

/// The certification status of the blob as determined by on-chain events.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Copy, Hash, utoipa::ToSchema)]
#[repr(u8)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BlobCertificationStatus {
    /// The blob has been registered.
    Registered,
    /// The blob has been certified.
    Certified,
    /// The blob has been shown to be invalid.
    Invalid,
}

impl std::fmt::Display for BlobCertificationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Registered => write!(f, "registered"),
            Self::Certified => write!(f, "certified"),
            Self::Invalid => write!(f, "invalid"),
        }
    }
}

impl PartialOrd for BlobCertificationStatus {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BlobCertificationStatus {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (x, y) if x == y => Ordering::Equal,
            (Self::Registered, _) => Ordering::Less,
            (Self::Certified, Self::Registered) => Ordering::Greater,
            (Self::Certified, _) => Ordering::Less,
            (Self::Invalid, _) => Ordering::Greater,
        }
    }
}

/// Contains the certification status of a blob.
///
/// If the blob exists, it also contains its end epoch and the ID of the Sui event
/// from which the status resulted.
#[derive(
    Debug, Deserialize, Serialize, PartialEq, Eq, Clone, Copy, Default, Hash, utoipa::ToSchema,
)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum BlobStatus {
    /// The blob does not exist (anymore) within Walrus.
    #[default]
    Nonexistent,
    /// The blob exists within Walrus.
    Existent {
        /// The epoch at which the blob expires (non-inclusive).
        #[schema(value_type = u64)]
        end_epoch: Epoch,
        /// The certification status of the blob.
        #[schema(inline)]
        status: BlobCertificationStatus,
        /// The ID of the Sui event in which the status was changed to the current status.
        status_event: EventID,
    },
}

impl PartialOrd for BlobStatus {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BlobStatus {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            // Nonexistent is the "smallest" status.
            (Self::Nonexistent, Self::Nonexistent) => Ordering::Equal,
            (Self::Nonexistent, _) => Ordering::Less,
            (_, Self::Nonexistent) => Ordering::Greater,
            // Compare existent statuses and end epochs if the status is equal.
            (
                Self::Existent {
                    status, end_epoch, ..
                },
                Self::Existent {
                    status: status_other,
                    end_epoch: end_epoch_other,
                    ..
                },
            ) => {
                if status == status_other {
                    end_epoch.cmp(end_epoch_other)
                } else {
                    status.cmp(status_other)
                }
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
