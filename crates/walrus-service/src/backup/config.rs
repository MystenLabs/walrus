// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Configuration for the blob backup service.

use std::{net::SocketAddr, path::PathBuf, time::Duration};

use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DurationMilliSeconds, DurationSeconds};

use crate::{
    common::{config::SuiReaderConfig, utils},
    node::events::EventProcessorConfig,
};

/// The subdirectory in which to store the backup blobs when running without remote storage.
pub const BACKUP_BLOB_ARCHIVE_SUBDIR: &str = "archive";

/// Configuration of a Walrus backup node.
#[serde_as]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BackupConfig {
    /// Directory in which to persist the event processor database and downloaded blob archives if
    /// no backup_bucket is specified.
    #[serde(deserialize_with = "utils::resolve_home_dir")]
    pub backup_storage_path: PathBuf,
    /// Google Cloud Storage bucket to which the backup blobs will be uploaded.
    ///
    /// If this is `None`, the backup blobs will not be uploaded to GCS, they will be placed in the
    /// `backup_storage_path` under the `archive` subdir.
    pub backup_bucket: Option<String>,
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
    /// Maximum number of retries allowed per blob.
    ///
    /// This is the maximum number of times that a backup_delegator will enqueue a particular blob
    /// for fetching. (This is a safety measure to prevent resource starvation for the other blobs'
    /// backup lifecycles.)
    #[serde(default = "defaults::max_retries_per_blob")]
    pub max_retries_per_blob: u32,
    /// How long to delay between retries when a backup fails in minutes.
    #[serde_as(as = "DurationSeconds<u64>")]
    #[serde(
        rename = "retry_fetch_after_interval",
        default = "defaults::retry_fetch_after_interval"
    )]
    pub retry_fetch_after_interval: Duration,
    /// How long to sleep between fetcher job polling. Note that workers will not sleep after
    /// successful fetches in order to ensure a rapid recovery if there is a queue building up.
    #[serde_as(as = "DurationMilliSeconds<u64>")]
    #[serde(
        rename = "idle_fetcher_sleep_time_milliseconds",
        default = "defaults::idle_fetcher_sleep_time"
    )]
    pub idle_fetcher_sleep_time: Duration,
}

impl BackupConfig {
    /// Creates a new `BackupConfig` with default values for all optional fields.
    pub fn new_with_defaults(
        backup_storage_path: PathBuf,
        sui: SuiReaderConfig,
        database_url: String,
    ) -> Self {
        Self {
            backup_storage_path,
            backup_bucket: None,
            metrics_address: defaults::metrics_address(),
            sui,
            event_processor_config: Default::default(),
            database_url,
            max_retries_per_blob: defaults::max_retries_per_blob(),
            retry_fetch_after_interval: defaults::retry_fetch_after_interval(),
            idle_fetcher_sleep_time: defaults::idle_fetcher_sleep_time(),
        }
    }
}

/// Backup-related default values.
pub mod defaults {
    use std::{
        net::{Ipv4Addr, SocketAddr},
        time::Duration,
    };

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

    /// Default backup max_retries_per_blob.
    pub fn max_retries_per_blob() -> u32 {
        25
    }

    /// The default interval between retries for any specific blob.
    ///
    /// Explanation of the 45 minute interval:
    ///    15 GB at 15 MBps = ~16.7 minutes. Double that to add time to send to GCS.
    ///    Add some extra buffer time to make it 45 minutes.
    ///
    /// This is a rough estimate and can be adjusted as needed. A minor goal here is to avoid the
    /// need for another layer of synchronization between fetcher workers and the delegator.
    pub fn retry_fetch_after_interval() -> Duration {
        Duration::from_secs(45 * 60)
    }

    /// The default interval between the fetcher polling the database when there is no work to do.
    /// Note that there are likely to be many fetchers running simultaneously.
    pub fn idle_fetcher_sleep_time() -> Duration {
        Duration::from_secs(1)
    }
}
