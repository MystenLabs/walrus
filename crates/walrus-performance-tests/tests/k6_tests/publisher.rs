// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{ByteSize, TestResult};
use crate::k6_tests;

mod blob {
    use super::*;
    use crate::k6_tests::{K6Environment, WALRUS_K6_ENVIRONMENT};

    walrus_test_utils::param_test! {
        blob_upload_latency -> TestResult: [
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
    fn blob_upload_latency(
        payload_size: ByteSize,
        files_to_store: usize,
        max_concurrency: usize,
    ) -> TestResult {
        k6_tests::run(
            "publisher/publisher_v1_put_blob_latency.ts",
            &format!("upload:latency:{payload_size}"),
        )
        .env("WALRUS_K6_PAYLOAD_SIZE", payload_size)
        .env("WALRUS_K6_BLOBS_TO_STORE", files_to_store)
        .env("WALRUS_K6_MAX_CONCURRENCY", max_concurrency)
        .tag("payload_size", payload_size)
        .tag("payload_size_bytes", payload_size.byte_value())
        .status()
    }

    #[test]
    fn blob_upload_request_throughput() -> TestResult {
        let (start, increment, duration) = match *WALRUS_K6_ENVIRONMENT {
            K6Environment::NightlyBaseline => (60, 5, "15s"),
            // Untuned below this line...
            K6Environment::Localhost => (40, 5, "30s"),
            K6Environment::TestnetFromCi => (40, 5, "30s"),
        };

        blob_upload_throughput("requests", ByteSize::kibi(1), start, increment, duration)
    }

    #[test]
    fn blob_upload_data_throughput() -> TestResult {
        let (start, increment, duration) = match *WALRUS_K6_ENVIRONMENT {
            K6Environment::NightlyBaseline => (18, 1, "90s"),
            // Untuned below this line...
            K6Environment::Localhost => (40, 1, "25s"),
            K6Environment::TestnetFromCi => (8, 1, "90s"),
        };

        blob_upload_throughput("data", ByteSize::mebi(100), start, increment, duration)
    }

    fn blob_upload_throughput(
        throughput_type: &str,
        payload_size: ByteSize,
        start_rate: usize,
        increment: usize,
        stage_duration: &str,
    ) -> TestResult {
        k6_tests::run(
            "publisher/publisher_v1_put_blob_throughput.ts",
            &format!("upload:throughput:{throughput_type}"),
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
        quilt_upload_latency_uniform_file_sizes -> TestResult: [
            total_file_size_1mi: (ByteSize::mebi(1), 20, 3),
            total_file_size_10mi: (ByteSize::mebi(10), 20, 3),
            total_file_size_100mi: (ByteSize::mebi(100), 5, 1),
            total_file_size_500mi: (ByteSize::mebi(500), 3, 1),
            total_file_size_1gi: (ByteSize::gibi(1), 3, 1),
        ]
    }
    fn quilt_upload_latency_uniform_file_sizes(
        total_file_size: ByteSize,
        quilts_to_store: usize,
        max_concurrency: usize,
    ) -> TestResult {
        quilt_upload_latency("uniform", total_file_size, quilts_to_store, max_concurrency)
    }

    walrus_test_utils::param_test! {
        quilt_upload_latency_random_file_sizes -> TestResult: [
            total_file_size_1mi: (ByteSize::mebi(1), 20, 3),
            total_file_size_10mi: (ByteSize::mebi(10), 20, 3),
            total_file_size_100mi: (ByteSize::mebi(100), 5, 1),
            total_file_size_500mi: (ByteSize::mebi(500), 3, 1),
            total_file_size_1gi: (ByteSize::gibi(1), 3, 1),
        ]
    }
    fn quilt_upload_latency_random_file_sizes(
        total_file_size: ByteSize,
        quilts_to_store: usize,
        max_concurrency: usize,
    ) -> TestResult {
        quilt_upload_latency("random", total_file_size, quilts_to_store, max_concurrency)
    }

    fn quilt_upload_latency(
        file_size_assignment: &str,
        total_file_size: ByteSize,
        quilts_to_store: usize,
        max_concurrency: usize,
    ) -> TestResult {
        k6_tests::run(
            "publisher/publisher_v1_put_quilt_latency.ts",
            &format!("quilt-{file_size_assignment}-upload:latency:{total_file_size}"),
        )
        .env("WALRUS_K6_TOTAL_FILE_SIZE", total_file_size)
        .env("WALRUS_K6_QUILTS_TO_STORE", quilts_to_store)
        .env("WALRUS_K6_QUILT_FILE_COUNT", -1)
        .env("WALRUS_K6_QUILT_FILE_SIZE_ASSIGNMENT", file_size_assignment)
        .env("WALRUS_K6_MAX_QUILT_FILE_SIZE", total_file_size / 10)
        .env("WALRUS_K6_MAX_CONCURRENCY", max_concurrency)
        .tag("total_quilt_file_size", total_file_size)
        .status()
    }
}
