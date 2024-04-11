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
/// Facilities to deploy a demo testbed.
pub mod testbed;

mod node;
pub use node::{StorageNode, StorageNodeBuilder};

mod storage;
pub use storage::Storage;

pub mod system_events;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;
