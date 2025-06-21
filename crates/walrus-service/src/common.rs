// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Service functionality for Walrus shared by client and storage node.

pub(crate) mod api;
pub mod config;
pub mod telemetry;
pub mod utils;

#[cfg(feature = "client")]
pub mod event_blob_downloader;
