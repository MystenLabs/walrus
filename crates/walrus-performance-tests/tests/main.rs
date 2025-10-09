// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Walrus performance and load tests.

use std::{
    env,
    path::{Path, PathBuf},
};

mod common;
use common::k6::K6Command;

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

const DEFAULT_K6_SCRIPTS_DIRECTORY: &str = "../../scripts/k6/src/tests";
const DEFAULT_REPORT_FILENAME: &str = "report";

fn k6_test_command<P>(script: P, plan_suffix: &str) -> K6Command
where
    P: AsRef<Path>,
{
    let script_path = env::var("WALRUS_K6_SCRIPTS_DIRECTORY")
        .map(|s| PathBuf::from(s))
        .unwrap_or(Path::new(DEFAULT_K6_SCRIPTS_DIRECTORY).to_path_buf())
        .join(script);

    let maybe_plan = env::var("WALRUS_K6_STACK")
        .map(|stack| format!("{stack}:{plan_suffix}"))
        .ok();
    let maybe_plan = maybe_plan.as_deref();

    let test_run_id = maybe_plan
        .zip(env::var("WALRUS_K6_TEST_RUN_ID_SUFFIX").ok())
        .map(|(plan, suffix)| format!("{plan}:{suffix}"));

    let report_path = env::var("WALRUS_K6_REPORT_DIRECTORY").map(|directory| {
        let filename_stem = maybe_plan
            .map(|plan| plan.replace(':', "-"))
            .unwrap_or(DEFAULT_REPORT_FILENAME.to_owned());
        format!("{directory}/{filename_stem}.html")
    });

    let mut command = K6Command::new(script_path);
    command
        .maybe_env("WALRUS_K6_PLAN", maybe_plan)
        .maybe_env("WALRUS_K6_TEST_ID", maybe_plan)
        .maybe_env("WALRUS_K6_TEST_RUN_ID", test_run_id)
        .web_dashboard_export(report_path.ok());
    command
}

mod publisher {
    use super::*;

    walrus_test_utils::param_test! {
        blob_upload_latency -> TestResult: [
            payload_1ki: ("1Ki"),
            payload_100ki: ("100Ki"),
            payload_1mi: ("1Mi"),
            payload_10mi: ("10Mi"),
            payload_100mi: ("100Mi"),
        ]
    }
    fn blob_upload_latency(payload_size: &str) -> TestResult {
        k6_test_command(
            "publisher/publisher_v1_put_blobs.ts",
            &format!("upload:latency:{payload_size}"),
        )
        .status()
    }

    walrus_test_utils::param_test! {
        blob_upload_throughput -> TestResult: [
            requests: ("requests"),
            data: ("data"),
        ]
    }
    fn blob_upload_throughput(kind: &str) -> TestResult {
        k6_test_command(
            "publisher/publisher_v1_put_blobs_breakpoint.ts",
            &format!("upload:throughput:{kind}"),
        )
        .status()
    }
}

mod aggregator {
    use super::*;

    walrus_test_utils::param_test! {
        blob_download_latency -> TestResult: [
            payload_1ki: ("1Ki"),
            payload_100ki: ("100Ki"),
            payload_1mi: ("1Mi"),
            payload_10mi: ("10Mi"),
            payload_100mi: ("100Mi"),
        ]
    }
    fn blob_download_latency(payload_size: &str) -> TestResult {
        k6_test_command(
            "aggregator/aggregator_v1_get_blob_latency.ts",
            &format!("download:latency:{payload_size}"),
        )
        .status()
    }

    walrus_test_utils::param_test! {
        blob_download_throughput -> TestResult: [
            requests: ("requests"),
            data: ("data"),
        ]
    }
    fn blob_download_throughput(kind: &str) -> TestResult {
        k6_test_command(
            "aggregator/aggregator_v1_get_blobs_breakpoint.ts",
            &format!("download:throughput:{kind}"),
        )
        .status()
    }
}
