// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Errors that can occur during quilt operations.
#[derive(Debug, thiserror::Error)]
pub enum QuiltError {
    /// The number of shards provided was invalid (zero).
    #[error("invalid number of shards provided")]
    InvalidShardCount,
} 