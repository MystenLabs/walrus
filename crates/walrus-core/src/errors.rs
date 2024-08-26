// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Errors that may be encountered while interacting with the Walrus service.

use alloc::string::String;

use serde::{Deserialize, Serialize};

use crate::Epoch;

/// Error returned when the epoch in a request is invalid.
#[derive(Debug, thiserror::Error, Serialize, Deserialize, Clone)]
pub enum InvalidEpoch {
    /// The requester's epoch is too old.
    #[error("Requester epoch too old. Server epoch: {0}")]
    TooOld(Epoch),
    /// The requester's epoch is too new.
    #[error("Requester epoch too new. Server epoch: {0}")]
    TooNew(Epoch),
}

/// Error returned by the Walrus service. The errors in this enum are meant to be
/// deserialized from the service's JSON response.
#[derive(Debug, thiserror::Error, Serialize, Deserialize)]
pub enum WalrusServiceError {
    /// Sync shard request failed because the requester's epoch is invalid.
    #[error(transparent)]
    SyncShardInvalidEpoch(#[from] InvalidEpoch),
    /// TODO: currently, all other errors go to here. Move other useful errors that
    /// can be handled by the client to their own variants.
    ///
    /// The service encountered an internal error.
    #[error("Internal error {0}")]
    Internal(String),
}
