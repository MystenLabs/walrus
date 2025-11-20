// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::k6_tests::{
    self, SAMPLE_SIZE_FAST, SAMPLE_SIZE_SLOW, TestResult, WALRUS_K6_ENVIRONMENT,
    common::{self, byte_size::ByteSize, thresholds::BlobThresholds},
};

mod blob {
    use super::*;

    walrus_test_utils::param_test! {
        blob_download_latency -> TestResult: [
            payload_1ki: (ByteSize::kibi(1), SAMPLE_SIZE_FAST, 3),
            payload_100ki: (ByteSize::kibi(100), SAMPLE_SIZE_FAST, 3),
            payload_1mi: (ByteSize::mebi(1), SAMPLE_SIZE_FAST, 3),
            payload_10mi: (ByteSize::mebi(10), SAMPLE_SIZE_FAST, 3),
            // ^=== Fast / Slow ===v
            payload_100mi: (ByteSize::mebi(100), SAMPLE_SIZE_SLOW, 1),
            payload_500mi: (ByteSize::mebi(500), SAMPLE_SIZE_SLOW, 1),
            payload_1gi: (ByteSize::gibi(1), SAMPLE_SIZE_SLOW, 1),
            payload_2gi: (ByteSize::gibi(2), SAMPLE_SIZE_SLOW, 1),
        ]
    }
    fn blob_download_latency(
        payload_size: ByteSize,
        samples: usize,
        max_concurrency: usize,
    ) -> TestResult {
        let command = k6_tests::run(
            "aggregator/aggregator_v1_get_blob_latency.ts",
            &format!("download:latency:{payload_size}"),
        );

        common::blob_request_latency(command, payload_size, samples, max_concurrency)
    }

    walrus_test_utils::param_test! {
        blob_download_throughput -> TestResult: [
            requests: ("requests", ByteSize::kibi(1)),
            data: ("data", ByteSize::mebi(100)),
        ]
    }
    fn blob_download_throughput(throughput_type: &str, payload_size: ByteSize) -> TestResult {
        let thresholds = BlobThresholds::download(*WALRUS_K6_ENVIRONMENT);
        let command = k6_tests::run(
            "aggregator/aggregator_v1_get_blob_throughput.ts",
            &format!("download:throughput:{throughput_type}"),
        );

        common::blob_request_throughput(command, payload_size, thresholds)
    }
}

mod quilt {
    use super::*;

    walrus_test_utils::param_test! {
        patch_download_latency_uniform_by_patch_id -> TestResult: [
            total_file_size_1mi: (ByteSize::mebi(1), SAMPLE_SIZE_FAST, 3),
            total_file_size_10mi: (ByteSize::mebi(10), SAMPLE_SIZE_FAST, 3),
            total_file_size_100mi: (ByteSize::mebi(100), SAMPLE_SIZE_FAST, 3),
            total_file_size_500mi: (ByteSize::mebi(500), SAMPLE_SIZE_FAST, 3),
            total_file_size_1gi: (ByteSize::gibi(1), SAMPLE_SIZE_FAST, 3),
        ]
    }
    fn patch_download_latency_uniform_by_patch_id(
        total_file_size: ByteSize,
        patches_to_read: usize,
        max_concurrency: usize,
    ) -> TestResult {
        patch_download_latency(
            &format!("quilt-patch-id-download:latency:{total_file_size}:uniform_patches"),
            format!("quilt:{total_file_size}:uniform:patches"),
            total_file_size,
            patches_to_read,
            max_concurrency,
        )
    }

    walrus_test_utils::param_test! {
        patch_download_latency_uniform_by_quilt_id -> TestResult: [
            total_file_size_1mi: (ByteSize::mebi(1), SAMPLE_SIZE_FAST, 3),
            total_file_size_10mi: (ByteSize::mebi(10), SAMPLE_SIZE_FAST, 3),
            total_file_size_100mi: (ByteSize::mebi(100), SAMPLE_SIZE_FAST, 3),
            total_file_size_500mi: (ByteSize::mebi(500), SAMPLE_SIZE_FAST, 3),
            total_file_size_1gi: (ByteSize::gibi(1), SAMPLE_SIZE_FAST, 3),
        ]
    }
    fn patch_download_latency_uniform_by_quilt_id(
        total_file_size: ByteSize,
        patches_to_read: usize,
        max_concurrency: usize,
    ) -> TestResult {
        patch_download_latency(
            &format!("quilt-file-id-download:latency:{total_file_size}:uniform_patches"),
            format!("quilt:{total_file_size}:uniform:file_ids"),
            total_file_size,
            patches_to_read,
            max_concurrency,
        )
    }

    fn patch_download_latency(
        test_id: &str,
        patch_id_list_key: String,
        total_file_size: ByteSize,
        patches_to_read: usize,
        max_concurrency: usize,
    ) -> TestResult {
        k6_tests::run(
            "aggregator/aggregator_v1_get_quilt_patch_latency.ts",
            test_id,
        )
        .env("WALRUS_K6_PATCHES_TO_READ", patches_to_read)
        .env("WALRUS_K6_PATCH_ID_LIST_KEY", patch_id_list_key)
        .env("WALRUS_K6_MAX_CONCURRENCY", max_concurrency)
        .tag("total_file_size", total_file_size)
        .status()
    }
}
