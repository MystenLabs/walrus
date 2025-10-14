// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Walrus performance and load tests.
mod k6_tests;

//type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;
//
//const DEFAULT_K6_SCRIPTS_DIRECTORY: &str = "../../scripts/k6/src/tests";
//
//// struct K6TestCommand {
////     command: K6Command,
//// }
////
//// impl K6TestCommand {
////     fn new<P: AsRef<Path>>(script: P, test_id: &str) -> Self {
////         let script_path = env::var("WALRUS_K6_SCRIPTS_DIRECTORY")
////             .map(|s| PathBuf::from(s))
////             .unwrap_or(Path::new(DEFAULT_K6_SCRIPTS_DIRECTORY).to_path_buf())
////             .join(script);
////
////         let maybe_stack = env::var("WALRUS_K6_STACK").ok();
////         let maybe_stack = maybe_stack.as_deref();
////
////         let test_id: Cow<'_, str> = maybe_stack
////             .map(|stack| format!("{stack}:{test_id}").into())
////             .unwrap_or(test_id.into());
////
////         let test_run_id = env::var("WALRUS_K6_TEST_RUN_ID_SUFFIX")
////             .map(|run_id_suffix| format!("{test_id}:{run_id_suffix}"))
////             .ok();
////
////         let report_path = env::var("WALRUS_K6_REPORT_DIRECTORY").map(|directory| {
////             let filename_stem = test_id.replace(':', "-");
////             format!("{directory}/{filename_stem}.html")
////         });
////
////         let mut command = K6Command::new(script_path);
////         command
////             .maybe_env("WALRUS_K6_PLAN", maybe_stack)
////             .maybe_env("WALRUS_K6_TEST_ID", maybe_stack)
////             .maybe_env("WALRUS_K6_TEST_RUN_ID", test_run_id)
////             .web_dashboard_export(report_path.ok());
////         command
////     }
//// }
//
//fn script_path<P: AsRef<Path>>(script: P) -> PathBuf {
//    env::var("WALRUS_K6_SCRIPTS_DIRECTORY")
//        .map(|s| PathBuf::from(s))
//        .unwrap_or(Path::new(DEFAULT_K6_SCRIPTS_DIRECTORY).to_path_buf())
//        .join(script)
//}
//
//fn k6_test_command<P>(script: P, test_id: &str) -> K6Command
//where
//    P: AsRef<Path>,
//{
//    let script_path = env::var("WALRUS_K6_SCRIPTS_DIRECTORY")
//        .map(|s| PathBuf::from(s))
//        .unwrap_or(Path::new(DEFAULT_K6_SCRIPTS_DIRECTORY).to_path_buf())
//        .join(script);
//
//    let maybe_stack = env::var("WALRUS_K6_STACK").ok();
//    let maybe_stack = maybe_stack.as_deref();
//
//    let test_id: Cow<'_, str> = maybe_stack
//        .map(|stack| format!("{stack}:{test_id}").into())
//        .unwrap_or(test_id.into());
//
//    let test_run_id = env::var("WALRUS_K6_TEST_RUN_ID_SUFFIX")
//        .map(|run_id_suffix| format!("{test_id}:{run_id_suffix}"))
//        .ok();
//
//    let report_path = env::var("WALRUS_K6_REPORT_DIRECTORY").map(|directory| {
//        let filename_stem = test_id.replace(':', "-");
//        format!("{directory}/{filename_stem}.html")
//    });
//
//    let mut command = K6Command::new(script_path);
//    command
//        .maybe_env("WALRUS_K6_PLAN", maybe_stack)
//        .maybe_env("WALRUS_K6_TEST_ID", maybe_stack)
//        .maybe_env("WALRUS_K6_TEST_RUN_ID", test_run_id)
//        .web_dashboard_export(report_path.ok());
//    command
//}
//
//mod publisher {
//    use super::*;
//
//    walrus_test_utils::param_test! {
//        blob_upload_latency -> TestResult: [
//            payload_1ki: ("1Ki", 20, 3),
//            payload_100ki: ("100Ki", 20, 3),
//            payload_1mi: ("1Mi", 20, 3),
//            payload_10mi: ("10Mi", 20, 3),
//            payload_100mi: ("100Mi", 5, 1),
//            payload_500mi: ("500Mi", 3, 1),
//            payload_1gi: ("1Gi", 3, 1),
//            payload_2gi: ("2Gi", 3, 1),
//        ]
//    }
//    fn blob_upload_latency(
//        payload_size: &str,
//        files_to_store: usize,
//        max_concurrency: usize,
//    ) -> TestResult {
//        K6Command::new(script_path("publisher/publisher_v1_put_blob_latency.ts"))
//            .env("WALRUS_K6_PAYLOAD_SIZE", payload_size)
//            .env("WALRUS_K6_BLOBS_TO_STORE", files_to_store.to_string())
//            .env("WALRUS_K6_MAX_CONCURRENCY", max_concurrency.to_string())
//            .status()
//    }
//
//    walrus_test_utils::param_test! {
//        blob_upload_throughput -> TestResult: [
//            requests: ("requests"),
//            data: ("data"),
//        ]
//    }
//    fn blob_upload_throughput(kind: &str) -> TestResult {
//        k6_test_command(
//            "publisher/publisher_v1_put_blobs_breakpoint.ts",
//            &format!("upload:throughput:{kind}"),
//        )
//        .allow_failed_thresholds()
//        .status()
//    }
//}
//
//mod aggregator {
//    use super::*;
//
//    walrus_test_utils::param_test! {
//        blob_download_latency -> TestResult: [
//            payload_1ki: ("1Ki"),
//            payload_100ki: ("100Ki"),
//            payload_1mi: ("1Mi"),
//            payload_10mi: ("10Mi"),
//            payload_100mi: ("100Mi"),
//        ]
//    }
//    fn blob_download_latency(payload_size: &str) -> TestResult {
//        k6_test_command(
//            "aggregator/aggregator_v1_get_blob_latency.ts",
//            &format!("download:latency:{payload_size}"),
//        )
//        .env("WALRUS_K6_PAYLOAD_SIZE", payload_size)
//        .status()
//    }
//
//    walrus_test_utils::param_test! {
//        blob_download_throughput -> TestResult: [
//            requests: ("requests"),
//            data: ("data"),
//        ]
//    }
//    fn blob_download_throughput(kind: &str) -> TestResult {
//        k6_test_command(
//            "aggregator/aggregator_v1_get_blobs_breakpoint.ts",
//            &format!("download:throughput:{kind}"),
//        )
//        .allow_failed_thresholds()
//        .status()
//    }
//}
