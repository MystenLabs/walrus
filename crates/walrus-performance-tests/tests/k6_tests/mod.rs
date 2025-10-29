// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    env,
    path::{Path, PathBuf},
    sync::LazyLock,
};

use crate::k6_tests::common::k6::{K6Command, K6Environment};

mod aggregator;
mod common;
mod publisher;

/// Alias for test results that return a standard error.
type TestResult = Result<(), Box<dyn std::error::Error>>;

/// Number of samples to collector for cases where the time required for each is fast.
const SAMPLE_SIZE_FAST: usize = 10;
/// Number of samples to collector for cases where the time required for each is slow.
const SAMPLE_SIZE_SLOW: usize = 3;

/// Default directory in which the k6 test scripts can be found.
const DEFAULT_K6_SCRIPTS_DIRECTORY: &str = "../../scripts/k6/src/tests";

/// The environment in which the test is running, defaults to 'localhost'.
static WALRUS_K6_ENVIRONMENT: LazyLock<K6Environment> = LazyLock::new(|| {
    env::var("WALRUS_K6_ENVIRONMENT")
        .map(|s| {
            s.parse()
                .expect("WALRUS_K6_ENVIRONMENT must be a known environment")
        })
        .unwrap_or_default()
});

/// The directory in which the k6 scripts are located.
static WALRUS_K6_SCRIPTS_DIRECTORY: LazyLock<PathBuf> = LazyLock::new(|| {
    PathBuf::from(
        env::var("WALRUS_K6_SCRIPTS_DIRECTORY").unwrap_or(DEFAULT_K6_SCRIPTS_DIRECTORY.to_owned()),
    )
});

/// Optional suffix appended to `testid`s to create a `test_run_id` metric label value.
///
/// Specifying this in CI allows identifying test results from different workflow runs.
static WALRUS_K6_TEST_RUN_ID_SUFFIX: LazyLock<Option<String>> =
    LazyLock::new(|| env::var("WALRUS_K6_TEST_RUN_ID_SUFFIX").ok());

/// A directory that, if specified, causes test reports to be generated and written into it.
static WALRUS_K6_REPORT_DIRECTORY: LazyLock<Option<String>> =
    LazyLock::new(|| env::var("WALRUS_K6_REPORT_DIRECTORY").ok());

/// If specified, defines the argument to the k6 `--out` flag, for how to export metrics.
static WALRUS_K6_OUT: LazyLock<Option<String>> = LazyLock::new(|| env::var("WALRUS_K6_OUT").ok());

/// If set to 'true', call k6 with the `--quiet` flag, to avoid real-time progress out.
static WALRUS_K6_QUIET: LazyLock<bool> =
    LazyLock::new(|| env::var("WALRUS_K6_QUIET").is_ok_and(|s| s.parse().unwrap_or(false)));

/// If set to 'true', call k6 with the `--no-color` flag.
static WALRUS_K6_NO_COLOR: LazyLock<bool> =
    LazyLock::new(|| env::var("WALRUS_K6_NO_COLOR").is_ok_and(|s| s.parse().unwrap_or(false)));

/// Create a new [`K6`] instance for running a command and sets common options.
fn run<P>(script: P, testid_suffix: &str) -> K6Command
where
    P: AsRef<Path>,
{
    let testid = format!("{0}:{testid_suffix}", *WALRUS_K6_ENVIRONMENT);
    let script = script_path(script);

    let mut k6_run = K6Command::new(script);
    k6_run
        .env("WALRUS_K6_ENVIRONMENT", *WALRUS_K6_ENVIRONMENT)
        .tag("testid", &testid)
        .maybe_tag("test_run_id", get_test_run_id(&testid))
        .web_dashboard_export(report_path(&testid));
    k6_run
}

/// If the environment variable `WALRUS_K6_TEST_RUN_ID_SUFFIX` is set, append it to the provided
/// testid and return the resulting test_run_id.
fn get_test_run_id(testid: &str) -> Option<String> {
    WALRUS_K6_TEST_RUN_ID_SUFFIX
        .as_deref()
        .map(|suffix| format!("{testid}:{suffix}"))
}

/// Returns the path [`WALRUS_K6_SCRIPTS_DIRECTORY`] + `/<script>`.
fn script_path<P: AsRef<Path>>(script: P) -> PathBuf {
    WALRUS_K6_SCRIPTS_DIRECTORY.join(script)
}

/// If [`WALRUS_K6_REPORT_DIRECTORY`] returns `WALRUS_K6_REPORT_DIRECTORY/<testid>.html`,
/// with any colons in testid replaced with hyphens.
fn report_path(testid: &str) -> Option<String> {
    WALRUS_K6_REPORT_DIRECTORY.as_deref().map(|directory| {
        let filename_stem = testid.replace(':', "-");
        format!("{directory}/{filename_stem}.html")
    })
}
