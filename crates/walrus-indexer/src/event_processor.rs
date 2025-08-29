// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Event processor for the Walrus indexer.
//!
//! This module processes Sui events from checkpoints and extracts
//! index-related events for the indexer to consume.

pub mod checkpoint;
pub mod client;
pub mod config;
pub mod db;
pub mod metrics;
pub mod processor;
