// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

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

/// Upload mode for controlling concurrency and aggressiveness of uploads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UploadMode {
    /// Conservative mode: lower concurrency, slower but more reliable
    Conservative,
    /// Balanced mode: moderate concurrency (default)
    Balanced,
    /// Aggressive mode: higher concurrency, faster but more resource-intensive
    Aggressive,
}

impl Default for UploadMode {
    fn default() -> Self {
        Self::Balanced
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
    /// Enable uploading slivers via a detached child process that continues tail writes.
    pub child_uploads_enabled: bool,
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
            child_uploads_enabled: false,
            registration_delay: Duration::from_millis(200),
            max_total_blob_size: 1024 * 1024 * 1024, // 1GiB
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
        self.max_connections_for_request_and_blob_size(
            self.sliver_size_for_blob(blob_size, encoding_config, encoding_type),
            self.max_concurrent_writes,
        )
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
