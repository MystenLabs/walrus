// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! API types.
use serde::{Deserialize, Serialize};
use sui_types::event::EventID;
use walrus_core::Epoch;

/// Error message returned by the service.
#[derive(Debug, Serialize, Deserialize)]
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
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Copy)]
#[repr(u8)]
pub enum BlobCertificationStatus {
    /// The blob has been registered.
    Registered,
    /// The blob has been certified.
    Certified,
    /// The blob has been shown to be invalid.
    Invalid,
}

/// Contains the certification status of a blob as well as its end epoch
/// and the id of the sui event from which the status resulted.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone, Copy)]
pub struct BlobStatus {
    /// The epoch at which the blob expires (non-inclusive).
    pub end_epoch: Epoch,
    /// The certification status of the blob.
    pub status: BlobCertificationStatus,
    /// The ID of the sui event in which the status was changed to the current status.
    pub status_event: EventID,
}
