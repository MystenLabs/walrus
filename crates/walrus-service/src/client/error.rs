// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The errors for the storage client and the communication with storage nodes.

use walrus_core::{SliverPairIndex, SliverType};
use walrus_sdk::error::NodeError;

/// Storing the metadata and the set of sliver pairs onto the storage node, and retrieving the
/// storage confirmation, failed.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// One ore more slivers could not be stored on the node
    #[error("one ore more slivers could not be stored on the node")]
    SliverStore(Vec<SliverStoreError>),
    /// The sliver could not be stored on the node.
    #[error(transparent)]
    Metadata(NodeError),
    /// A valid storage confirmation could not retrieved from the node.
    #[error(transparent)]
    Confirmation(NodeError),
}

/// The sliver could not be stored on the node.
#[derive(Debug, thiserror::Error)]
#[error("the sliver could not be stored on the node")]
pub struct SliverStoreError {
    pub pair_idx: SliverPairIndex,
    pub sliver_type: SliverType,
    pub error: NodeError,
}
