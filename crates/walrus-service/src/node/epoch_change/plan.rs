// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Shared vocabulary for describing epoch-change shard work.

use walrus_core::ShardIndex;

/// How created shards are brought up to date. A created shard is filled either by shard sync
/// or by node recovery, never both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShardFill {
    /// Sync each shard's contents from its previous owner.
    ShardSync,
    /// Force the shards to `Active` status; node recovery fills their missing blobs per blob
    /// (full-recovery path, where no previous-owner assignment is known to sync from).
    ForceActive,
}

/// The shards newly assigned to the node in this epoch change: their storage is created, and
/// their contents are then brought up to date by the described fill method.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NewShards {
    /// The shards to create storage for.
    pub shards: Vec<ShardIndex>,
    /// How the created shards are brought up to date.
    pub fill: ShardFill,
}
