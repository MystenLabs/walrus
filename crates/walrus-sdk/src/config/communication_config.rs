// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Communication configuration options.

use std::{
    num::{NonZeroU16, NonZeroUsize},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_with::{DefaultOnNull, DurationMilliSeconds, serde_as};
use walrus_core::{
    EncodingType,
    encoding::{EncodingConfig, EncodingFactory as _, Primary},
};
use walrus_utils::backoff::ExponentialBackoffConfig;

use crate::config::{
    reqwest_config::{RequestRateConfig, ReqwestConfig},
    sliver_write_extra_time::SliverWriteExtraTime,
};

/// Below this threshold, the `NodeCommunication` client will not check if the sliver is present on
/// the node, but directly try to store it.
///
/// The threshold is chosen in a somewhat arbitrary way, but with the guiding principle that the
/// direct sliver store should only take 1 RTT, therefore having similar latency to the sliver
/// status check. To ensure this is the case, we take compute the threshold as follows: take the TCP
/// payload size (1440 B); multiply it for an initial congestion window of 4 packets (although in
/// modern systems this is usually 10, there may be other data being sent in this window); and
/// conservatively subtract 200 B to account for HTTP headers and other overheads.
pub const DEFAULT_SLIVER_STATUS_CHECK_THRESHOLD: usize = 5_560;
fn default_sliver_status_check_threshold() -> usize {
    DEFAULT_SLIVER_STATUS_CHECK_THRESHOLD
}

/// Upload mode for controlling concurrency and aggressiveness of uploads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UploadMode {
    /// Conservative mode: lower concurrency, slower but more reliable.
    Conservative,
    /// Balanced mode: moderate concurrency (default).
    Balanced,
    /// Aggressive mode: higher concurrency, faster but more resource-intensive.
    Aggressive,
}

impl Default for UploadMode {
    fn default() -> Self {
        Self::Balanced
    }
}

/// Default number of sliver uploads that should be observed before evaluating throughput.
pub const DEFAULT_AUTO_TUNE_WINDOW_SAMPLE_TARGET: usize = 20;
/// Default timeout while waiting for a throughput sample window to complete.
pub const DEFAULT_AUTO_TUNE_WINDOW_TIMEOUT: Duration = Duration::from_secs(10);
/// Default multiplicative factor applied when exploring higher permit counts.
pub const DEFAULT_AUTO_TUNE_INCREASE_FACTOR: f64 = 2.0;
/// Default lock factor applied to the best-performing permit count.
pub const DEFAULT_AUTO_TUNE_LOCK_FACTOR: f64 = 1.5;
/// Default minimum number of permits that the auto-tuner evaluates.
pub const DEFAULT_AUTO_TUNE_MIN_PERMITS: usize = 50;
/// Default maximum number of permits that the auto-tuner evaluates.
pub const DEFAULT_AUTO_TUNE_MAX_PERMITS: usize = 2_000;
/// Default weight applied to secondary slivers relative to primaries when computing throughput.
pub const DEFAULT_AUTO_TUNE_SECONDARY_WEIGHT: f64 = 0.5;
/// Default minimum blob size (in bytes) required to enable auto-tune.
pub const DEFAULT_AUTO_TUNE_MIN_BLOB_SIZE_BYTES: u64 = 50 * 1024 * 1024; // 50MB

/// Configuration for runtime auto-tuning of the data-in-flight limit.
#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(default)]
pub struct DataInFlightAutoTuneConfig {
    /// Enables the additive-increase / multiplicative-decrease controller.
    pub enabled: bool,
    /// Number of completed sliver uploads required to close a measurement window.
    pub window_sample_target: usize,
    /// Maximum time spent gathering throughput data for a single measurement window.
    #[serde(rename = "window_timeout_millis")]
    #[serde_as(as = "DurationMilliSeconds")]
    pub window_timeout: Duration,
    /// Multiplicative factor used while searching for higher throughput.
    pub increase_factor: f64,
    /// Factor applied to the best performing permit count when locking in the result.
    pub lock_factor: f64,
    /// Minimum number of permits the auto-tuner will consider.
    pub min_permits: usize,
    /// Maximum number of permits the auto-tuner will consider.
    pub max_permits: usize,
    /// Weight applied to secondary slivers relative to primaries when measuring throughput.
    pub secondary_weight: f64,
    /// Minimum blob size (in bytes) required to enable auto-tune.
    pub min_blob_size_bytes: u64,
}

impl Default for DataInFlightAutoTuneConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            window_sample_target: DEFAULT_AUTO_TUNE_WINDOW_SAMPLE_TARGET,
            window_timeout: DEFAULT_AUTO_TUNE_WINDOW_TIMEOUT,
            increase_factor: DEFAULT_AUTO_TUNE_INCREASE_FACTOR,
            lock_factor: DEFAULT_AUTO_TUNE_LOCK_FACTOR,
            min_permits: DEFAULT_AUTO_TUNE_MIN_PERMITS,
            max_permits: DEFAULT_AUTO_TUNE_MAX_PERMITS,
            secondary_weight: DEFAULT_AUTO_TUNE_SECONDARY_WEIGHT,
            min_blob_size_bytes: DEFAULT_AUTO_TUNE_MIN_BLOB_SIZE_BYTES,
        }
    }
}

impl DataInFlightAutoTuneConfig {
    /// Returns a sanitized sample target guaranteeing at least one observation per window.
    pub fn sample_target(&self) -> usize {
        self.window_sample_target.max(1)
    }

    /// Returns the window timeout, defaulting to the configured default if zero.
    pub fn timeout(&self) -> Duration {
        if self.window_timeout.is_zero() {
            DEFAULT_AUTO_TUNE_WINDOW_TIMEOUT
        } else {
            self.window_timeout
        }
    }

    /// Returns the multiplicative increase factor, ensuring it is not less than 1.0.
    pub fn increase_factor(&self) -> f64 {
        self.increase_factor.max(1.0)
    }

    /// Returns the lock factor, clamped to the `(0, 1]` interval.
    pub fn lock_factor(&self) -> f64 {
        self.lock_factor.clamp(f64::EPSILON, 1.0)
    }

    /// Returns sanitized permit bounds with `min <= max` and both at least one.
    pub fn permit_bounds(&self) -> (usize, usize) {
        let min = self.min_permits.max(1);
        let max = self.max_permits.max(min);
        (min, max)
    }

    /// Returns the configured secondary sliver weight, clamped to the `[0, 1]` interval.
    pub fn secondary_sliver_weight(&self) -> f64 {
        self.secondary_weight.clamp(0.0, 1.0)
    }
}

/// Configuration for the communication parameters of the client
#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(default)]
pub struct ClientCommunicationConfig {
    /// The maximum number of open connections the client can have at any one time for writes.
    ///
    /// If `None`, the value is set by the client to optimize the write speed while avoiding running
    /// out of memory.
    pub max_concurrent_writes: Option<usize>,
    /// The maximum number of slivers the client requests in parallel. If `None`, the value is set
    /// by the client to `n - 2f`, depending on the number of shards `n`.
    pub max_concurrent_sliver_reads: Option<usize>,
    /// The maximum number of nodes the client contacts to get the blob metadata in parallel.
    pub max_concurrent_metadata_reads: usize,
    /// The maximum number of nodes the client contacts to get a blob status in parallel.
    pub max_concurrent_status_reads: Option<usize>,
    /// The maximum amount of data (in bytes) associated with concurrent requests.
    // The `serde_as` attribute is added for backwards compatibility.
    #[serde_as(deserialize_as = "DefaultOnNull")]
    pub max_data_in_flight: usize,
    /// The configuration for the `reqwest` client.
    pub reqwest_config: ReqwestConfig,
    /// The configuration specific to each node connection.
    pub request_rate_config: RequestRateConfig,
    /// Disable the use of system proxies for communication.
    pub disable_proxy: bool,
    /// Disable the use of operating system certificates for authenticating the communication.
    pub disable_native_certs: bool,
    /// The extra time allowed for sliver writes.
    pub sliver_write_extra_time: SliverWriteExtraTime,
    /// Limit under which the client skips the sliver status check before uploads.
    /// Defaults to 5_560 bytes.
    #[serde(default = "default_sliver_status_check_threshold")]
    pub sliver_status_check_threshold: usize,
    /// Enable uploading slivers via a detached child process that continues tail writes.
    pub child_process_uploads_enabled: bool,
    /// Auto-tuning options for write concurrency derived from the data-in-flight limit.
    pub data_in_flight_auto_tune: DataInFlightAutoTuneConfig,
    /// The delay for which the client waits before storing data to ensure that storage nodes have
    /// seen the registration event.
    #[serde(rename = "registration_delay_millis")]
    #[serde_as(as = "DurationMilliSeconds")]
    pub registration_delay: Duration,
    /// The maximum total blob size allowed to store if multiple blobs are uploaded.
    pub max_total_blob_size: usize,
    /// The configuration for the backoff after committee change is detected.
    pub committee_change_backoff: ExponentialBackoffConfig,
    /// The request timeout for the SuiClient communicating with Sui network.
    /// If not set, the default timeout in SuiClient will be used.
    #[serde(rename = "sui_client_request_timeout_millis")]
    #[serde_as(as = "Option<DurationMilliSeconds>")]
    pub sui_client_request_timeout: Option<Duration>,
}

impl Default for ClientCommunicationConfig {
    fn default() -> Self {
        Self {
            disable_native_certs: false,
            max_concurrent_writes: Default::default(),
            max_concurrent_sliver_reads: Default::default(),
            max_concurrent_metadata_reads:
                super::communication_config::default::max_concurrent_metadata_reads(),
            max_concurrent_status_reads: Default::default(),
            max_data_in_flight: default::max_data_in_flight(),
            reqwest_config: Default::default(),
            request_rate_config: Default::default(),
            disable_proxy: Default::default(),
            sliver_write_extra_time: Default::default(),
            child_process_uploads_enabled: false,
            data_in_flight_auto_tune: Default::default(),
            registration_delay: Duration::from_millis(200),
            max_total_blob_size: 1024 * 1024 * 1024, // 1GiB
            sliver_status_check_threshold: DEFAULT_SLIVER_STATUS_CHECK_THRESHOLD,
            committee_change_backoff: ExponentialBackoffConfig::new(
                Duration::from_secs(1),
                Duration::from_secs(5),
                Some(5),
            ),
            sui_client_request_timeout: None,
        }
    }
}

impl ClientCommunicationConfig {
    /// Provides a config with lower number of retries to speed up integration testing.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn default_for_test() -> Self {
        use walrus_utils::backoff::ExponentialBackoffConfig;

        #[cfg(msim)]
        let max_retries = Some(3);
        #[cfg(not(msim))]
        let max_retries = Some(1);
        ClientCommunicationConfig {
            disable_proxy: true,
            disable_native_certs: true,
            request_rate_config: RequestRateConfig {
                max_node_connections: 10,
                backoff_config: ExponentialBackoffConfig {
                    max_retries,
                    min_backoff: Duration::from_secs(2),
                    max_backoff: Duration::from_secs(10),
                },
            },
            ..Default::default()
        }
    }

    /// Provides a config with lower number of retries and a custom timeout to speed up integration
    /// testing.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn default_for_test_with_reqwest_timeout(timeout: Duration) -> Self {
        let mut config = Self::default_for_test();
        config.reqwest_config.total_timeout = timeout;
        config
    }
}

/// Communication limits in the client.
#[derive(Debug, Clone, PartialEq)]
pub struct CommunicationLimits {
    /// The maximum number of concurrent writes to the storage nodes.
    pub max_concurrent_writes: usize,
    /// The maximum number of concurrent sliver reads from the storage nodes.
    pub max_concurrent_sliver_reads: usize,
    /// The maximum number of concurrent metadata reads from the storage nodes.
    pub max_concurrent_metadata_reads: usize,
    /// The maximum number of concurrent status reads from the storage nodes.
    pub max_concurrent_status_reads: usize,
    /// The maximum amount of data in flight (in bytes) for the client.
    pub max_data_in_flight: usize,
    /// Configuration for auto-tuning concurrency during writes.
    pub auto_tune: DataInFlightAutoTuneConfig,
}

impl CommunicationLimits {
    /// Creates a new instance of [`CommunicationLimits`] based on the provided configuration and
    /// number of shards.
    pub fn new(communication_config: &ClientCommunicationConfig, n_shards: NonZeroU16) -> Self {
        let max_concurrent_writes = communication_config
            .max_concurrent_writes
            .unwrap_or(default::max_concurrent_writes(n_shards));
        let max_concurrent_sliver_reads = communication_config
            .max_concurrent_sliver_reads
            .unwrap_or(default::max_concurrent_sliver_reads(n_shards));
        let max_concurrent_metadata_reads = communication_config.max_concurrent_metadata_reads;
        let max_concurrent_status_reads = communication_config
            .max_concurrent_status_reads
            .unwrap_or(default::max_concurrent_status_reads(n_shards));
        Self {
            max_concurrent_writes,
            max_concurrent_sliver_reads,
            max_concurrent_metadata_reads,
            max_concurrent_status_reads,
            max_data_in_flight: communication_config.max_data_in_flight,
            auto_tune: communication_config.data_in_flight_auto_tune.clone(),
        }
    }

    fn max_connections_for_request_and_blob_size(
        &self,
        request_size: NonZeroUsize,
        max_connections: usize,
    ) -> usize {
        (self.max_data_in_flight / request_size.get())
            .min(max_connections)
            .max(1)
    }

    /// Compute the size of a sliver for the given blob size and encoding type.
    fn sliver_size_for_blob(
        &self,
        blob_size: u64,
        encoding_config: &EncodingConfig,
        encoding_type: EncodingType,
    ) -> NonZeroUsize {
        encoding_config
            .get_for_type(encoding_type)
            .sliver_size_for_blob::<Primary>(blob_size)
            .expect("blob must not be too large to be encoded")
            .try_into()
            .expect("we assume at least a 32-bit architecture")
    }

    /// This computes the maximum number of concurrent sliver writes based on the unencoded blob
    /// size.
    ///
    /// This applies two limits:
    /// 1. The result is at most [`self.max_concurrent_writes`][Self::max_concurrent_writes].
    /// 2. The result multiplied with the primary sliver size does not exceed
    ///    `self.max_data_in_flight`.
    ///
    /// # Panics
    ///
    /// Panics if the provided blob size is too large to be encoded, see
    /// CommunicationLimits::sliver_size_for_blob.
    pub fn max_concurrent_sliver_writes_for_blob_size(
        &self,
        blob_size: u64,
        encoding_config: &EncodingConfig,
        encoding_type: EncodingType,
    ) -> usize {
        let sliver_size = self.sliver_size_for_blob(blob_size, encoding_config, encoding_type);
        let effective_max_data_in_flight = self.max_data_in_flight.max(sliver_size.get());
        let allowed_by_bytes = effective_max_data_in_flight / sliver_size.get();
        let limited_by_connections = allowed_by_bytes >= self.max_concurrent_writes;
        let concurrency = allowed_by_bytes.min(self.max_concurrent_writes).max(1);

        tracing::debug!(
            blob_size,
            sliver_size = sliver_size.get(),
            max_data_in_flight = self.max_data_in_flight,
            effective_max_data_in_flight,
            max_concurrent_writes = self.max_concurrent_writes,
            allowed_by_bytes,
            limited_by = if limited_by_connections {
                "max_concurrent_writes"
            } else {
                "max_data_in_flight"
            },
            concurrency,
            "computed sliver write concurrency limit"
        );

        concurrency
    }

    /// This computes the maximum number of concurrent sliver writes based on the unencoded blob
    /// size.
    ///
    /// This applies two limits:
    /// 1. The result is at most
    ///    [`self.max_concurrent_sliver_reads`][Self::max_concurrent_sliver_reads].
    /// 2. The result multiplied with the primary sliver size does not exceed
    ///    `self.max_data_in_flight`.
    ///
    /// # Panics
    ///
    /// Panics if the provided blob size is too large to be encoded, see
    /// CommunicationLimits::sliver_size_for_blob.
    pub fn max_concurrent_sliver_reads_for_blob_size(
        &self,
        blob_size: u64,
        encoding_config: &EncodingConfig,
        encoding_type: EncodingType,
    ) -> usize {
        self.max_connections_for_request_and_blob_size(
            self.sliver_size_for_blob(blob_size, encoding_config, encoding_type),
            self.max_concurrent_sliver_reads,
        )
    }
}

pub(crate) mod default {
    use std::num::NonZeroU16;

    use walrus_core::bft;

    pub fn max_concurrent_writes(n_shards: NonZeroU16) -> usize {
        // No limit as we anyway want to store as many slivers as possible.
        n_shards.get().into()
    }

    pub fn max_concurrent_sliver_reads(n_shards: NonZeroU16) -> usize {
        // Read up to `n-2f` slivers concurrently to avoid wasted work on the storage nodes.
        (n_shards.get() - 2 * bft::max_n_faulty(n_shards)).into()
    }

    pub fn max_concurrent_status_reads(n_shards: NonZeroU16) -> usize {
        // No limit as we need 2f+1 responses and requests are small.
        n_shards.get().into()
    }

    pub fn max_concurrent_metadata_reads() -> usize {
        3
    }

    // This corresponds to 100Mb, i.e., 1 second on a 100 Mbps connection.
    pub fn max_data_in_flight() -> usize {
        12_500_000
    }
}
