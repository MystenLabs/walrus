// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! API types.
use serde::{Deserialize, Serialize};

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
