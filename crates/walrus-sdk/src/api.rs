// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! API types.
use serde::{Deserialize, Serialize};
use sui_types::event::EventID;
use walrus_core::Epoch;

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
    },
}

/// The certification status of the blob as determined by on-chain events.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Copy, utoipa::ToSchema)]
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

/// Contains the certification status of a blob.
///
/// If the blob exists, it also contains its end epoch and the ID of the Sui event
/// from which the status resulted.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone, Copy, Default, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
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
