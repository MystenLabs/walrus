// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::k6_tests::{self, ByteSize, TestResult};

mod blob {
    use super::*;
    use crate::k6_tests::{K6Environment, WALRUS_K6_ENVIRONMENT};

    walrus_test_utils::param_test! {
        blob_download_latency -> TestResult: [
            payload_1ki: (ByteSize::kibi(1), 20, 3),
            payload_100ki: (ByteSize::kibi(100), 20, 3),
            payload_1mi: (ByteSize::mebi(1), 20, 3),
            payload_10mi: (ByteSize::mebi(10), 20, 3),
            payload_100mi: (ByteSize::mebi(100), 5, 1),
            payload_500mi: (ByteSize::mebi(500), 3, 1),
            payload_1gi: (ByteSize::gibi(1), 3, 1),
            payload_2gi: (ByteSize::gibi(2), 3, 1),
        ]
    }
    fn blob_download_latency(
        payload_size: ByteSize,
        files_to_read: usize,
        max_concurrency: usize,
    ) -> TestResult {
        k6_tests::run(
            "aggregator/aggregator_v1_get_blob_latency.ts",
            &format!("download:latency:{payload_size}"),
        )
        .env("WALRUS_K6_PAYLOAD_SIZE", payload_size)
        .env("WALRUS_K6_BLOBS_TO_READ", files_to_read)
        .env("WALRUS_K6_MAX_CONCURRENCY", max_concurrency)
        .tag("payload_size", payload_size)
        .tag("payload_size_bytes", payload_size.byte_value())
        .status()
    }

    #[test]
    fn blob_download_request_throughput() -> TestResult {
        let (start, increment, duration) = match *WALRUS_K6_ENVIRONMENT {
            K6Environment::NightlyBaseline => (280, 10, "10s"),
            K6Environment::Localhost => (180, 10, "15s"),
            K6Environment::TestnetFromCi => (180, 10, "15s"),
        };

        blob_download_throughput("requests", ByteSize::kibi(1), start, increment, duration)
    }

    #[test]
    fn blob_download_data_throughput() -> TestResult {
        let (start, increment, duration) = match *WALRUS_K6_ENVIRONMENT {
            K6Environment::NightlyBaseline => (80, 5, "15s"),
            K6Environment::Localhost => (40, 5, "90s"),
            K6Environment::TestnetFromCi => (40, 5, "90s"),
        };

        blob_download_throughput("data", ByteSize::mebi(100), start, increment, duration)
    }

    fn blob_download_throughput(
        throughput_type: &str,
        payload_size: ByteSize,
        start_rate: usize,
        increment: usize,
        stage_duration: &str,
    ) -> TestResult {
        k6_tests::run(
            "aggregator/aggregator_v1_get_blob_throughput.ts",
            &format!("download:throughput:{throughput_type}"),
        )
        .env("WALRUS_K6_PAYLOAD_SIZE", payload_size)
        .env("WALRUS_K6_START_RATE_PER_MINUTE", start_rate.to_string())
        .env("WALRUS_K6_RATE_INCREMENT", increment.to_string())
        .env("WALRUS_K6_HELD_STAGE_DURATION", stage_duration)
        .tag("payload_size", payload_size)
        .tag("payload_size_bytes", payload_size.byte_value())
        .allow_failed_thresholds()
        .status()
    }
}

mod quilt {
    use super::*;

    walrus_test_utils::param_test! {
        patch_download_latency_uniform_by_patch_id -> TestResult: [
            total_file_size_1mi: (ByteSize::mebi(1), 20, 3),
            total_file_size_10mi: (ByteSize::mebi(10), 20, 3),
            total_file_size_100mi: (ByteSize::mebi(100), 20, 3),
            total_file_size_500mi: (ByteSize::mebi(500), 20, 3),
            total_file_size_1gi: (ByteSize::gibi(1), 20, 3),
        ]
    }
    fn patch_download_latency_uniform_by_patch_id(
        total_file_size: ByteSize,
        patches_to_read: usize,
        max_concurrency: usize,
    ) -> TestResult {
        let key = format!("quilt:{total_file_size}:uniform:patches");

        k6_tests::run(
            "aggregator/aggregator_v1_get_quilt_patch_latency.ts",
            &format!("quilt-patch-id-download:latency:{total_file_size}:uniform_patches"),
        )
        .env("WALRUS_K6_PATCHES_TO_READ", patches_to_read)
        .env("WALRUS_K6_PATCH_ID_LIST_KEY", key)
        .env("WALRUS_K6_MAX_CONCURRENCY", max_concurrency)
        .tag("total_file_size", total_file_size)
        .status()
    }

    walrus_test_utils::param_test! {
        patch_download_latency_uniform_by_quilt_id -> TestResult: [
            total_file_size_1mi: (ByteSize::mebi(1), 20, 3),
            total_file_size_10mi: (ByteSize::mebi(10), 20, 3),
            total_file_size_100mi: (ByteSize::mebi(100), 20, 3),
            total_file_size_500mi: (ByteSize::mebi(500), 20, 3),
            total_file_size_1gi: (ByteSize::gibi(1), 20, 3),
        ]
    }
    fn patch_download_latency_uniform_by_quilt_id(
        total_file_size: ByteSize,
        patches_to_read: usize,
        max_concurrency: usize,
    ) -> TestResult {
        let key = format!("quilt:{total_file_size}:uniform:file_ids");

        k6_tests::run(
            "aggregator/aggregator_v1_get_quilt_patch_latency.ts",
            &format!("quilt-file-id-download:latency:{total_file_size}:uniform_patches"),
        )
        .env("WALRUS_K6_PATCHES_TO_READ", patches_to_read)
        .env("WALRUS_K6_PATCH_ID_LIST_KEY", key)
        .env("WALRUS_K6_MAX_CONCURRENCY", max_concurrency)
        .tag("total_file_size", total_file_size)
        .status()
    }
}
