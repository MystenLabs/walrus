// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Checkpoint downloader for parallel downloading of checkpoints.
//!
//! This crate provides functionality for downloading checkpoints in parallel with adaptive worker
//! scaling.

mod config;
mod downloader;
mod metrics;
mod types;

pub use config::{AdaptiveDownloaderConfig, ChannelConfig, ParallelDownloaderConfig};
pub use downloader::ParallelCheckpointDownloader;
pub use types::CheckpointEntry;
