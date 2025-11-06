// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::k6_tests::{
    self,
    SAMPLE_SIZE_FAST,
    SAMPLE_SIZE_SLOW,
    SAMPLE_SIZE_VERY_SLOW,
    TestResult,
    WALRUS_K6_ENVIRONMENT,
    common,
    common::{byte_size::ByteSize, thresholds::BlobThresholds},
};

mod blob {
    use super::*;

    walrus_test_utils::param_test! {
        blob_upload_latency -> TestResult: [
            payload_1ki: (ByteSize::kibi(1), SAMPLE_SIZE_FAST, 3),
            payload_100ki: (ByteSize::kibi(100), SAMPLE_SIZE_FAST, 3),
            payload_1mi: (ByteSize::mebi(1), SAMPLE_SIZE_FAST, 3),
            payload_10mi: (ByteSize::mebi(10), SAMPLE_SIZE_FAST, 3),
            payload_100mi: (ByteSize::mebi(100), SAMPLE_SIZE_SLOW, 1),
            payload_500mi: (ByteSize::mebi(500), SAMPLE_SIZE_VERY_SLOW, 1),
            payload_1gi: (ByteSize::gibi(1), SAMPLE_SIZE_VERY_SLOW, 1),
            payload_2gi: (ByteSize::gibi(2), SAMPLE_SIZE_VERY_SLOW, 1),
        ]
    }
    fn blob_upload_latency(
        payload_size: ByteSize,
        samples: usize,
        max_concurrency: usize,
    ) -> TestResult {
        let thresholds = BlobThresholds::upload(*WALRUS_K6_ENVIRONMENT);
        let command = k6_tests::run(
            "publisher/publisher_v1_put_blob_latency.ts",
            &format!("upload:latency:{payload_size}"),
        );

        common::blob_request_latency(command, payload_size, samples, max_concurrency, thresholds)
    }

    walrus_test_utils::param_test! {
        blob_upload_throughput -> TestResult: [
            requests: ("requests", ByteSize::kibi(1)),
            data: ("data", ByteSize::mebi(100)),
        ]
    }
    fn blob_upload_throughput(throughput_type: &str, payload_size: ByteSize) -> TestResult {
        let thresholds = BlobThresholds::upload(*WALRUS_K6_ENVIRONMENT);
        let command = k6_tests::run(
            "publisher/publisher_v1_put_blob_throughput.ts",
            &format!("upload:throughput:{throughput_type}"),
        );

        common::blob_request_throughput(command, payload_size, thresholds)
    }
}

mod quilt {
    use super::*;

    walrus_test_utils::param_test! {
        quilt_upload_latency_uniform_file_sizes -> TestResult: [
            total_file_size_1mi: (ByteSize::mebi(1), SAMPLE_SIZE_FAST, 3),
            total_file_size_10mi: (ByteSize::mebi(10), SAMPLE_SIZE_FAST, 3),
            total_file_size_100mi: (ByteSize::mebi(100), SAMPLE_SIZE_SLOW, 1),
            total_file_size_500mi: (ByteSize::mebi(500), SAMPLE_SIZE_VERY_SLOW, 1),
            total_file_size_1gi: (ByteSize::gibi(1), SAMPLE_SIZE_VERY_SLOW, 1),
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
            total_file_size_1mi: (ByteSize::mebi(1), SAMPLE_SIZE_FAST, 3),
            total_file_size_10mi: (ByteSize::mebi(10), SAMPLE_SIZE_FAST, 3),
            total_file_size_100mi: (ByteSize::mebi(100), SAMPLE_SIZE_SLOW, 1),
            total_file_size_500mi: (ByteSize::mebi(500), SAMPLE_SIZE_VERY_SLOW, 1),
            total_file_size_1gi: (ByteSize::gibi(1), SAMPLE_SIZE_VERY_SLOW, 1),
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
