// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use crate::k6_tests::common::{byte_size::ByteSize, k6::K6Environment};

type BlobLatencyThresholdsInner = [(ByteSize, u64, u64); 4];
type BlobThroughputThresholdsInner = [(K6Environment, ByteSize, usize, usize); 8];

/// These thresholds are taken from one set of nightly experiments (NightlyWithLatency), and it's
/// expected that all tests that have enforced thresholds are lower than this.
static BLOB_UPLOAD_DOWNLOAD_LATENCY_THRESHOLDS: BlobLatencyThresholdsInner = [
    (ByteSize::kibi(1), 12, 2),
    (ByteSize::mebi(10), 22, 3),
    (ByteSize::mebi(100), 150, 35),
    (ByteSize::mebi(500), 500, 110),
];

static BLOB_UPLOAD_DOWNLOAD_TPUT_THRESHOLDS: BlobThroughputThresholdsInner = {
    use K6Environment::*;
    [
        // The following must be sorted, within each env group.
        // Environment, key, up_throughput, down_throughput
        // ---- NightlyBaseline ----
        (NightlyBaseline, ByteSize::kibi(1), 75, 330),
        (NightlyBaseline, ByteSize::mebi(100), 37, 105),
        // ---- NightlyWithLatency ----
        (NightlyWithLatency, ByteSize::kibi(1), 50, 280),
        (NightlyWithLatency, ByteSize::mebi(100), 10, 80),
        // ---- TestnetFromCi ----
        (TestnetFromCi, ByteSize::kibi(1), 50, 280),
        (TestnetFromCi, ByteSize::mebi(100), 10, 80),
        // ---- Localhost ----
        (Localhost, ByteSize::kibi(1), 10, 10),
        (Localhost, ByteSize::mebi(100), 10, 10),
    ]
};

/// Stores the default thresholds for the provided environment.
///
/// Each of the calls to retrieve a value requires a blob size. The value returned corresponds to
/// the provided blob size, or the next higher blob size for which a value is known. If the
/// provided key is greater than all of the blob sizes stored, the largest is returned.
pub(crate) struct BlobThresholds {
    is_upload: bool,
    env: K6Environment,
    latency_thresholds: &'static BlobLatencyThresholdsInner,
    throughput_thresholds: &'static BlobThroughputThresholdsInner,
}

impl BlobThresholds {
    fn new(env: K6Environment, is_upload: bool) -> Self {
        assert!(
            BLOB_UPLOAD_DOWNLOAD_TPUT_THRESHOLDS
                .iter()
                .find(|(item_env, ..)| *item_env == env)
                .is_some(),
            "the requested env ({}) must have thresholds set",
            env
        );
        Self {
            env,
            is_upload,
            latency_thresholds: &BLOB_UPLOAD_DOWNLOAD_LATENCY_THRESHOLDS,
            throughput_thresholds: &BLOB_UPLOAD_DOWNLOAD_TPUT_THRESHOLDS,
        }
    }

    pub fn upload(env: K6Environment) -> Self {
        Self::new(env, /*is_upload=*/ true)
    }

    pub fn download(env: K6Environment) -> Self {
        Self::new(env, /*is_upload=*/ false)
    }

    pub fn enforce_thresholds(&self) -> bool {
        matches!(
            self.env,
            K6Environment::NightlyWithLatency | K6Environment::NightlyBaseline
        )
    }

    pub fn latency_threshold(&self, key: ByteSize) -> Duration {
        Duration::from_secs(self.get(key, self.latency_thresholds.iter().copied()))
    }

    pub fn throughput_start_rpm(&self, key: ByteSize) -> usize {
        self.get(
            key,
            self.throughput_thresholds
                .iter()
                .filter_map(|(item_env, size, upload, download)| {
                    if *item_env == self.env {
                        Some((*size, *upload, *download))
                    } else {
                        None
                    }
                }),
        )
    }

    fn get<I, T>(&self, key: ByteSize, iter: I) -> T
    where
        I: Iterator<Item = (ByteSize, T, T)> + Clone,
    {
        let (_, upload_value, download_value) = iter
            .clone()
            .find(|(item_key, ..)| *item_key >= key)
            .or_else(|| iter.last())
            .expect("invariant guarantees at least one element exists matching the env");

        if self.is_upload {
            upload_value
        } else {
            download_value
        }
    }
}
