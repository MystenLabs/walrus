// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Preset upload modes that tune client communication for throughput/latency.

use serde::{Deserialize, Serialize};

use crate::config::{
    communication_config::ClientCommunicationConfig,
    reqwest_config::RequestRateConfig,
};

/// Upload preset modes for tuning client concurrency and network usage.
///
/// These presets only adjust in-memory ClientCommunicationConfig and are intended
/// to be applied per-run by the CLI (or other callers). They do not mutate the
/// on-disk configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum UploadMode {
    /// Targets ~10–100 Mbps links with low memory footprint and modest per-node pressure.
    /// Sets bytes-in-flight near 100 Mb (~12.5 MB) and a conservative per-node cap.
    Conservative,
    /// Targets ~1 Gbps links with balanced throughput and stability.
    /// Sets bytes-in-flight near 1 Gb (~125 MB) and moderate per-node cap.
    #[default]
    Balanced,
    /// Targets ~10 Gbps environments where higher memory and per-node parallelism are acceptable.
    /// Sets bytes-in-flight near 10 Gb (~1.25 GB) and higher per-node cap.
    Aggressive,
}

impl UploadMode {
    /// Applies this preset to a given communication config and returns the updated config.
    pub fn apply_to(self, mut c: ClientCommunicationConfig) -> ClientCommunicationConfig {
        let default_bytes = crate::config::communication_config::default::max_data_in_flight();
        let default_node_conns = RequestRateConfig::default().max_node_connections;

        match self {
            UploadMode::Conservative => {
                // Safe defaults with low memory use.
                // max_data_in_flight ≈ 50 MB for sub‑gigabit links.
                // Effective write concurrency per blob is ~floor(max_data_in_flight / sliver_size).
                // Global cap (max_concurrent_writes) is set high so bytes‑in‑flight limit is the
                // primary governor.
                if c.max_concurrent_writes.is_none() {
                    c.max_concurrent_writes = Some(1000);
                }
                if c.max_data_in_flight == default_bytes {
                    c.max_data_in_flight = 50_000_000; // 50 MB
                }
                if c.request_rate_config.max_node_connections == default_node_conns {
                    c.request_rate_config.max_node_connections = 100;
                }
            }
            UploadMode::Balanced => {
                // Target ~1 Gbps links while avoiding large memory spikes.
                // max_data_in_flight ≈ 500 MB with headroom for RTT/jitter.
                // Global cap (max_concurrent_writes) is set high so bytes‑in‑flight limit is the
                // primary governor.
                if c.max_concurrent_writes.is_none() {
                    c.max_concurrent_writes = Some(1000);
                }
                if c.max_data_in_flight == default_bytes {
                    c.max_data_in_flight = 500_000_000; // 500 MB
                }
                if c.request_rate_config.max_node_connections == default_node_conns {
                    c.request_rate_config.max_node_connections = 100;
                }
            }
            UploadMode::Aggressive => {
                // Target very fast links (several Gbps up to ~10 Gbps) with ample parallelism.
                // max_data_in_flight ≈ 1.25 GB; consider 2–3× for high RTT/jitter
                // if memory allows.
                // Global cap (max_concurrent_writes) is set high so bytes‑in‑flight limit is the
                // primary governor.
                if c.max_concurrent_writes.is_none() {
                    c.max_concurrent_writes = Some(1280);
                }
                if c.max_data_in_flight == default_bytes {
                    c.max_data_in_flight = 1_250_000_000; // 1250 MB (1.25 GB)
                }
                if c.request_rate_config.max_node_connections == default_node_conns {
                    c.request_rate_config.max_node_connections = 100;
                }
            }
        }

        c
    }

    /// Applies this preset forcibly, overriding existing values regardless of defaults.
    pub fn apply_force(self, mut c: ClientCommunicationConfig) -> ClientCommunicationConfig {
        match self {
            UploadMode::Conservative => {
                c.max_concurrent_writes = Some(1000);
                c.max_data_in_flight = 12_500_000; // 12.5 MB
                c.request_rate_config.max_node_connections = 100;
            }
            UploadMode::Balanced => {
                c.max_concurrent_writes = Some(1000);
                c.max_data_in_flight = 125_000_000; // 125 MB
                c.request_rate_config.max_node_connections = 100;
            }
            UploadMode::Aggressive => {
                c.max_concurrent_writes = Some(1000);
                c.max_data_in_flight = 1_250_000_000; // 1250 MB (1.25 GB)
                c.request_rate_config.max_node_connections = 100;
            }
        }
        c
    }
}
