// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{cmp, time::Duration};

use self::{byte_size::ByteSize, k6::K6Command, thresholds::BlobThresholds};
use crate::k6_tests::TestResult;

pub(crate) mod byte_size;
pub(crate) mod k6;
pub(crate) mod thresholds;

pub(crate) fn blob_request_latency(
    mut command: K6Command,
    payload_size: ByteSize,
    files_to_store: usize,
    max_concurrency: usize,
    thresholds: BlobThresholds,
) -> TestResult {
    let duration_threshold_millis = thresholds.latency_threshold(payload_size).as_secs() * 1000;

    if thresholds.enforce_thresholds() {
        command.env(
            "WALRUS_K6_HTTP_DURATION_THRESHOLD",
            duration_threshold_millis,
        );
    }

    command
        .env("WALRUS_K6_PAYLOAD_SIZE", payload_size)
        .env("WALRUS_K6_BLOB_COUNT", files_to_store)
        .env("WALRUS_K6_MAX_CONCURRENCY", max_concurrency)
        .tag("payload_size", payload_size)
        .tag("payload_size_bytes", payload_size.byte_value())
        .status()
}

pub(crate) fn blob_request_throughput(
    mut command: K6Command,
    payload_size: ByteSize,
    thresholds: BlobThresholds,
) -> TestResult {
    let start_rate = thresholds.throughput_start_rpm(payload_size);
    let base_duration = thresholds.latency_threshold(payload_size);
    let increment = throughput_increment(start_rate);
    let stage_duration_secs = throughput_stage_duration_secs(base_duration);

    command
        .env("WALRUS_K6_PAYLOAD_SIZE", payload_size)
        .env("WALRUS_K6_START_RATE_PER_MINUTE", start_rate.to_string())
        .env("WALRUS_K6_RATE_INCREMENT", increment.to_string())
        .env(
            "WALRUS_K6_HELD_STAGE_DURATION",
            format!("{}s", stage_duration_secs),
        )
        .env(
            "WALRUS_K6_REQUEST_SLO_MILLIS",
            format!("{}", base_duration.as_secs() * 1000),
        )
        .tag("payload_size", payload_size)
        .tag("payload_size_bytes", payload_size.byte_value())
        .allow_threshold_failures()
        .status()
}

pub(crate) fn throughput_increment(start_rate: usize) -> usize {
    cmp::max((start_rate as f64 * 0.05).round() as usize, 1)
}

pub(crate) fn throughput_stage_duration_secs(base_request_duration: Duration) -> u64 {
    let stage_duration_factor = if base_request_duration <= Duration::from_secs(30) {
        2 // Enough for 2 rounds of requests.
    } else {
        1 // Enough for 1 round of requests.
    };
    base_request_duration.as_secs() * stage_duration_factor
}
