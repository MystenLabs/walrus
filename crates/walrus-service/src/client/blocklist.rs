// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashSet, path::PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use walrus_core::BlobId;

/// Internal blocklist struct to deserialize from YAML.
#[serde_as]
#[derive(Debug, Deserialize, Clone, Default)]
struct BlocklistInner(#[serde_as(as = "Vec<DisplayFromStr>")] pub Vec<BlobId>);

/// A blocklist of blob IDs.
///
/// Supports checking if a blob ID is blocked and inserting/removing blob IDs.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Blocklist {
    blocked_blobs: HashSet<BlobId>,
}

impl Blocklist {
    /// Reads a blocklist of blob IDs in YAML format from the provided path.
    ///
    /// If no path is provided, the returned blocklist is empty.
    ///
    /// Returns an error if the file is not found or parsing fails.
    pub fn new(path: &Option<PathBuf>) -> Result<Self> {
        let Some(path) = path else {
            return Ok(Self::default());
        };

        let blocklist: BlocklistInner =
            serde_yaml::from_str(&std::fs::read_to_string(path).context(format!(
                "Unable to read blocklist file at {}",
                path.display()
            ))?)
            .context(format!("Parsing blocklist at {} failed", path.display()))?;

        Ok(Self {
            blocked_blobs: blocklist.0.into_iter().collect(),
        })
    }

    /// Checks if a blob ID is blocked.
    #[inline]
    pub fn is_blocked(&self, blob_id: &BlobId) -> bool {
        self.blocked_blobs.contains(blob_id)
    }

    /// Adds a blob ID to the blocklist.
    ///
    /// Returns whether the ID was newly inserted.
    #[inline]
    pub fn insert(&mut self, blob_id: BlobId) -> bool {
        self.blocked_blobs.insert(blob_id)
    }

    /// Removes a blob ID from the blocklist.
    ///
    /// Returns whether the ID was previously blocked.
    #[inline]
    pub fn remove(&mut self, blob_id: &BlobId) -> bool {
        self.blocked_blobs.remove(blob_id)
    }
}
