// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The stress test client for the Walrus service.

// use serde::{Deserialize, Serialize};
// use walrus_service::config::LoadConfig;

// /// The parameters for the stress test.
// #[derive(Serialize, Deserialize, Debug, Clone)]
// pub struct StressParameters {
//     /// The gas budget for transactions.
//     #[serde(default = "default::default_gas_budget")]
//     pub gas_budget: u64,
//     /// The percentage of write requests. If this is 0, only read requests are made;
//     /// if this is 100, only write requests are made; otherwise, a mix of read and write
//     /// requests are made.
//     #[serde(default = "default::default_load_type")]
//     pub load_type: u64,
//     /// The size of the blob to read and write (in bytes).
//     #[serde(default = "default::default_blob_size")]
//     pub blob_size: usize,
//     /// The address to expose the metrics.
//     #[serde(default = "default::default_metrics_port")]
//     pub metrics_port: u16,
//     /// skip pre-generation of transactions.
//     #[serde(default = "default::default_skip_pre_generation")]
//     pub skip_pre_generation: bool,
// }

// mod default {
//     pub fn default_gas_budget() -> u64 {
//         500_000_000
//     }

//     pub fn default_load_type() -> u64 {
//         100
//     }

//     pub fn default_blob_size() -> usize {
//         1024
//     }

//     pub fn default_metrics_port() -> u16 {
//         9584
//     }

//     pub fn default_skip_pre_generation() -> bool {
//         false
//     }
// }

// impl Default for StressParameters {
//     fn default() -> Self {
//         Self {
//             gas_budget: default::default_gas_budget(),
//             load_type: default::default_load_type(),
//             blob_size: default::default_blob_size(),
//             metrics_port: default::default_metrics_port(),
//             skip_pre_generation: default::default_skip_pre_generation(),
//         }
//     }
// }

// impl LoadConfig for StressParameters {}
