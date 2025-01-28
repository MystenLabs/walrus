// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Configuration for the blob backup service.

use std::{net::SocketAddr, path::PathBuf};

use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use crate::{
    common::{config::SuiReaderConfig, utils},
    node::events::EventProcessorConfig,
};

pub(super) const WORKER_COUNT: usize = 4;

/// Configuration of a Walrus backup node.
#[serde_as]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BackupNodeConfig {
    /// Directory in which to persist the database
    #[serde(deserialize_with = "utils::resolve_home_dir")]
    pub backup_storage_path: PathBuf,
    /// Socket address on which the Prometheus server should export its metrics.
    #[serde(default = "defaults::metrics_address")]
    pub metrics_address: SocketAddr,
    /// Sui config for the node
    pub sui: SuiReaderConfig,
    /// Configuration for the event processor.
    #[serde(default, skip_serializing_if = "defaults::is_default")]
    pub event_processor_config: EventProcessorConfig,
    /// Database URL.
    ///
    /// URL of the PostgreSQL database used to manage blob backup state and event stream progress.
    #[serde(default, skip_serializing_if = "defaults::is_default")]
    pub database_url: String,
    /// Size of the message queue.
    ///
    /// This is the maximum number of messages that can be stored in the queue before incoming work
    /// items will be pushed into the future.
    #[serde(default = "defaults::message_queue_size")]
    pub message_queue_size: u32,
    /// Maximum number of fetch attempts allowed per blob.
    ///
    /// This is the maximum number of times that a backup_delegator will enqueue a particular blob
    /// for fetching. (This is a safety measure to prevent resource starvation for the other blobs'
    /// backup lifecycles.)
    #[serde(default = "defaults::max_fetch_attempts_per_blob")]
    pub max_fetch_attempts_per_blob: u32,
}

impl BackupNodeConfig {
    /// Creates a new `BackupNodeConfig` with default values for all optional fields.
    pub fn new_with_defaults(
        backup_storage_path: PathBuf,
        sui: SuiReaderConfig,
        database_url: String,
    ) -> Self {
        Self {
            backup_storage_path,
            metrics_address: defaults::metrics_address(),
            sui,
            event_processor_config: Default::default(),
            database_url,
            message_queue_size: defaults::message_queue_size(),
            max_fetch_attempts_per_blob: defaults::max_fetch_attempts_per_blob(),
        }
    }
}

/// Backup-related default values.
pub mod defaults {
    use std::net::{Ipv4Addr, SocketAddr};

    use super::WORKER_COUNT;

    /// Default backup node metrics port.
    pub const METRICS_PORT: u16 = 10184;

    /// Returns the default metrics address.
    pub fn metrics_address() -> SocketAddr {
        (Ipv4Addr::LOCALHOST, METRICS_PORT).into()
    }

    /// Returns true iff the value is the default and we don't run in test mode.
    pub fn is_default<T: PartialEq + Default>(t: &T) -> bool {
        // The `cfg!(test)` check is there to allow serializing the full configuration, specifically
        // to generate the example configuration files.
        !cfg!(test) && t == &T::default()
    }
    /// Default backup work item queue size.
    pub fn message_queue_size() -> u32 {
        WORKER_COUNT.try_into().expect("WORKER_COUNT overflow")
    }
    /// Default backup max_fetch_attempts_per_blob.
    pub fn max_fetch_attempts_per_blob() -> u32 {
        25
    }
}
