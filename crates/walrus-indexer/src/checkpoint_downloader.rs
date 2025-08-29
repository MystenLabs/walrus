// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Checkpoint downloader integration for the Walrus indexer.
//!
//! This module re-exports the checkpoint-downloader crate for use in the indexer.

// Re-export the checkpoint-downloader crate types
pub use checkpoint_downloader::{
    AdaptiveDownloaderConfig,
    CheckpointEntry,
    ParallelCheckpointDownloader,
    ParallelDownloaderConfig,
};
