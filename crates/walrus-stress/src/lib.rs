// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The stress test client for the Walrus service.

use serde::{Deserialize, Serialize};
use walrus_service::config::LoadConfig;

/// The parameters for the stress test.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StressParameters {
    /// The gas budget for transactions.
    pub gas_budget: u64,
    /// The percentage of write requests. If this is 0, only read requests are made;
    /// if this is 100, only write requests are made; otherwise, a mix of read and write
    /// requests are made.
    pub load_type: u64,
    /// The size of the blob to read and write (in bytes).
    pub blob_size: usize,
    /// The address to expose the metrics.
    pub metrics_port: u16,
}

impl Default for StressParameters {
    fn default() -> Self {
        Self {
            gas_budget: 500_000_000,
            load_type: 100,
            blob_size: 1024,
            metrics_port: 9584,
        }
    }
}

impl LoadConfig for StressParameters {}
