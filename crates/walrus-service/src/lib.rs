// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Service functionality for Walrus.

/// Client for the Walrus service.
pub mod client;
/// Configuration module.
pub mod config;
/// Mapping from sliver pairs to shards.
pub mod mapping;
/// Server for the Walrus service.
pub mod server;

mod node;
pub use node::StorageNode;

mod storage;

#[cfg(test)]
mod test_utils;
