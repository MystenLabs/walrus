// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Preset upload modes that tune client communication for throughput/latency.

use serde::{Deserialize, Serialize};

use crate::config::communication_config::{
    ClientCommunicationConfig,
    UploadMode as CommunicationUploadMode,
};

/// Upload preset modes for tuning client concurrency and network usage.
///
/// These presets only adjust in-memory ClientCommunicationConfig and are intended
/// to be applied per-run by the CLI (or other callers). They do not mutate the
/// on-disk configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
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

struct CommunicationParameters {
    max_concurrent_writes: usize,
    max_data_in_flight: usize,
}

impl UploadMode {
    fn to_communication_parameters(self) -> CommunicationParameters {
        match self {
            UploadMode::Conservative => CommunicationParameters {
                max_concurrent_writes: 500,
                max_data_in_flight: 12_500_000, // 12.5 MB
            },
            UploadMode::Balanced => CommunicationParameters {
                max_concurrent_writes: 1000,
                max_data_in_flight: 512_000_000, // 512 MB
            },
            UploadMode::Aggressive => CommunicationParameters {
                max_concurrent_writes: 2000,
                max_data_in_flight: 1_250_000_000, // 1.25 GB
            },
        }
    }

    /// Applies this preset to a given communication config and returns the updated config.
    /// If `force` is true, overrides values unconditionally; otherwise only fills defaults.
    pub fn apply_to(self, mut config: ClientCommunicationConfig) -> ClientCommunicationConfig {
        let default_bytes = crate::config::communication_config::default::max_data_in_flight();
        let CommunicationParameters {
            max_concurrent_writes,
            max_data_in_flight,
        } = self.to_communication_parameters();

        if config.max_concurrent_writes.is_none() {
            config.max_concurrent_writes = Some(max_concurrent_writes);
        }

        if config.max_data_in_flight == default_bytes {
            config.max_data_in_flight = max_data_in_flight;
        }

        config
    }
}

impl From<CommunicationUploadMode> for UploadMode {
    fn from(value: CommunicationUploadMode) -> Self {
        match value {
            CommunicationUploadMode::Conservative => UploadMode::Conservative,
            CommunicationUploadMode::Balanced => UploadMode::Balanced,
            CommunicationUploadMode::Aggressive => UploadMode::Aggressive,
        }
    }
}
