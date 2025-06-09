// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Configuration for the blob backup service.

use std::{net::SocketAddr, path::PathBuf, time::Duration};

use serde::{Deserialize, Serialize};
use serde_with::{DurationMilliSeconds, DurationSeconds, serde_as};

use crate::{common::config::SuiReaderConfig, node::events::EventProcessorConfig};

/// The subdirectory in which to store the backup blobs when running without remote storage.
pub const BACKUP_BLOB_ARCHIVE_SUBDIR: &str = "archive";

#[serde_as]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BackupDbConfig {
    /// Database URL.
    ///
    /// URL of the PostgreSQL database used to manage blob backup state and event stream progress.
    #[serde(default = "defaults::database_url_from_env_var")]
    pub database_url: String,
    /// How long to sleep between PostgreSQL serializable transaction retries.
    #[serde_as(as = "DurationMilliSeconds<u64>")]
    #[serde(
        rename = "db_serializability_retry_time_milliseconds",
        default = "defaults::db_serializability_retry_time"
    )]
    pub db_serializability_retry_time: Duration,
    /// How long to sleep between PostgreSQL reconnection attempts.
    #[serde_as(as = "DurationMilliSeconds<u64>")]
    #[serde(
        rename = "db_reconnect_wait_time_milliseconds",
        default = "defaults::db_reconnect_wait_time"
    )]
    pub db_reconnect_wait_time: Duration,
}

/// Configuration of a Walrus backup node used for serialization and deserialization.
#[serde_as]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BackupConfig {
    /// Directory in which to persist the event processor database and downloaded blob archives if
    /// no backup_bucket is specified.
    #[serde(deserialize_with = "walrus_utils::config::resolve_home_dir")]
    pub backup_storage_path: PathBuf,
    /// Database configuration. Split apart to ease passing these values to db-specific routines.
    #[serde(flatten)]
    pub db_config: BackupDbConfig,
    /// Google Cloud Storage bucket to which the backup blobs will be uploaded.
    ///
    /// If this is `None`, the backup blobs will not be uploaded to GCS, they will be placed in the
    /// `backup_storage_path` under the `archive` subdir.
    pub backup_bucket: Option<String>,
    /// Time allowed to spend deleting a blob before timing out.
    #[serde_as(as = "DurationSeconds<u64>")]
    #[serde(default = "defaults::blob_delete_timeout")]
    pub blob_delete_timeout: Duration,
    /// Time allowed to spend uploading a blob before timing out.
    #[serde_as(as = "DurationSeconds<u64>")]
    #[serde(default = "defaults::blob_upload_timeout")]
    pub blob_upload_timeout: Duration,
    /// Socket address on which the Prometheus server should export its metrics.
    #[serde(default = "defaults::metrics_address")]
    pub metrics_address: SocketAddr,
    /// Sui config for the node
    pub sui: SuiReaderConfig,
    /// Configuration for the event processor.
    #[serde(default, skip_serializing_if = "defaults::is_default")]
    pub event_processor_config: EventProcessorConfig,
    /// Maximum number of retries allowed per blob.
    ///
    /// This is the maximum number of times that a backup_delegator will enqueue a particular blob
    /// for fetching. (This is a safety measure to prevent resource starvation for the other blobs'
    /// backup lifecycles.)
    #[serde(default = "defaults::max_retries_per_blob")]
    pub max_retries_per_blob: u32,
    /// The number of waiting blobs to fetch for any given single worker at a time. This is a
    /// simplistic way of avoiding database read/write contention.
    #[serde(default = "defaults::blob_job_chunk_size")]
    pub blob_job_chunk_size: u32,
    /// How long to delay between retries when a backup fails in minutes. Note that this will need
    /// to be a function of the blob_job_chunk_size in order to prevent fetcher races.
    #[serde_as(as = "DurationSeconds<u64>")]
    #[serde(
        rename = "retry_fetch_after_interval",
        default = "defaults::retry_fetch_after_interval"
    )]
    pub retry_fetch_after_interval: Duration,
    /// How long to sleep after realizing the fetcher has no work to do.
    #[serde_as(as = "DurationMilliSeconds<u64>")]
    #[serde(
        rename = "idle_fetcher_sleep_time_milliseconds",
        default = "defaults::idle_fetcher_sleep_time"
    )]
    pub idle_fetcher_sleep_time: Duration,
    /// How long to sleep after realizing the garbage collector has no work to do.
    #[serde_as(as = "DurationSeconds<u64>")]
    #[serde(
        rename = "idle_garbage_collector_sleep_time_seconds",
        default = "defaults::idle_garbage_collector_sleep_time"
    )]
    pub idle_garbage_collector_sleep_time: Duration,
}

impl BackupConfig {
    /// Creates a new `BackupConfigSpec` with default values for all optional fields.
    pub fn new_with_defaults(
        backup_storage_path: PathBuf,
        sui: SuiReaderConfig,
        database_url: String,
    ) -> Self {
        Self {
            backup_storage_path,
            backup_bucket: None,
            blob_upload_timeout: defaults::blob_upload_timeout(),
            blob_delete_timeout: defaults::blob_delete_timeout(),
            metrics_address: defaults::metrics_address(),
            sui,
            event_processor_config: Default::default(),
            db_config: BackupDbConfig {
                database_url,
                db_serializability_retry_time: defaults::db_serializability_retry_time(),
                db_reconnect_wait_time: defaults::db_reconnect_wait_time(),
            },
            max_retries_per_blob: defaults::max_retries_per_blob(),
            blob_job_chunk_size: defaults::blob_job_chunk_size(),
            retry_fetch_after_interval: defaults::retry_fetch_after_interval(),
            idle_fetcher_sleep_time: defaults::idle_fetcher_sleep_time(),
            idle_garbage_collector_sleep_time: defaults::idle_garbage_collector_sleep_time(),
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
        3
    }
    /// The default interval between retries for any specific blob.
    ///
    /// Explanation of the 45 minute interval:
    ///    15 GB at 15 MBps = ~16.7 minutes. Double that to add time to send to GCS.
    ///    Add some extra buffer time to make it 45 minutes.
    ///
    /// This is a rough estimate and can be adjusted as needed. A minor goal here is to avoid the
    /// need for another layer of synchronization between fetcher workers and the delegator.
    ///
    /// The reason we multiply by the chunk size is to ensure that the fetchers don't race to fetch
    /// blobs that are already in another fetcher's in-memory queue.
    pub fn retry_fetch_after_interval() -> Duration {
        Duration::from_secs(45 * 60 * u64::from(blob_job_chunk_size()))
    }

    /// The default interval between the fetcher polling the database when there is no work to do.
    /// Note that there are likely to be many fetchers running simultaneously.
    pub fn idle_fetcher_sleep_time() -> Duration {
        Duration::from_secs(1)
    }
    /// The default interval between the garbage collector polling the database when there is no
    /// work to do.
    pub fn idle_garbage_collector_sleep_time() -> Duration {
        Duration::from_secs(10)
    }

    /// Returns the database URL from the `DATABASE_URL` environment variable. Fails hard if it
    /// can't find it to ensure there is always a database_url.
    pub fn database_url_from_env_var() -> String {
        std::env::var("DATABASE_URL").expect("missing DATABASE_URL env var")
    }
    /// Default wait time between PostgreSQL serializability error retries.
    pub fn db_serializability_retry_time() -> Duration {
        Duration::from_millis(100)
    }
    /// Default wait time between PostgreSQL reconnect attempts.
    pub fn db_reconnect_wait_time() -> Duration {
        Duration::from_millis(1000)
    }
    /// Default time to allow blob uploads to take before timing out.
    pub fn blob_upload_timeout() -> Duration {
        Duration::from_secs(60)
    }
    /// Default time to allow blob deletions to take before timing out.
    pub fn blob_delete_timeout() -> Duration {
        Duration::from_secs(60)
    }
    pub fn blob_job_chunk_size() -> u32 {
        10
    }
}
