// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// walrus specific provider implementation for nodes in committee
mod walrus;

// Re-export for easier access
pub use crate::providers::walrus::{
    provider::WalrusNodeProvider,
    query::{NodeInfo, get_walrus_nodes},
};
