// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
//! A module defining blob slicing strategies.
use std::str::FromStr;

use anyhow::Context;
use serde::{Deserialize, Serialize};

/// A blob slicing strategy. Blob slicing can be used to split blobs into smaller chunks for various
/// reasons.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SliceSize {
    /// No blob slicing.
    Disabled,
    /// Slice blobs into max_blob_size chunks. (Based on the current network committee
    /// configuration.)
    Auto,
    /// Slice blobs into chunks of the given size (in bytes).
    Specific(u64),
}

impl Default for SliceSize {
    fn default() -> Self {
        Self::Disabled
    }
}

impl FromStr for SliceSize {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "auto" => Ok(SliceSize::Auto),
            "disabled" => Ok(SliceSize::Disabled),
            _ => {
                // Try to parse as a number
                let size = s.parse::<u64>().context(
                    "slice size must be either 'auto', 'disabled', or a positive integer",
                )?;
                Ok(SliceSize::Specific(size))
            }
        }
    }
}

impl std::fmt::Display for SliceSize {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SliceSize::Disabled => write!(f, "disabled"),
            SliceSize::Auto => write!(f, "auto"),
            SliceSize::Specific(size) => write!(f, "{}", size),
        }
    }
}
