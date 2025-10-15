// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    env,
    fmt::{self, Display},
    ops::Div,
    path::{Path, PathBuf},
};

use crate::k6_tests::k6::K6;

mod aggregator;
mod k6;
mod publisher;

/// Alias for test results that return a standard error.
type TestResult = Result<(), Box<dyn std::error::Error>>;

/// Default directory in which the k6 test scripts can be found.
const DEFAULT_K6_SCRIPTS_DIRECTORY: &str = "../../scripts/k6/src/tests";
/// Default environment in which the k6 tests are being run.
const DEFAULT_K6_ENVIRONMENT: &str = "localhost";

/// Create a new [`K6`] instance for running a command and sets common options.
fn run<P>(script: P, testid_suffix: &str) -> K6
where
    P: AsRef<Path>,
{
    let environment = get_environment();
    let testid = format!("{environment}:{testid_suffix}");
    let script = script_path(script);

    let mut k6_run = K6::new(script);
    k6_run
        .env("WALRUS_K6_ENVIRONMENT", environment)
        .tag("testid", &testid)
        .maybe_tag("test_run_id", get_test_run_id(&testid))
        .web_dashboard_export(report_path(&testid));
    k6_run
}

/// Returns the environment specified by the `WALRUS_K6_ENVIRONMENT` env variable,
/// or DEFAULT_K6_ENVIRONMENT if unset.
fn get_environment() -> String {
    env::var("WALRUS_K6_ENVIRONMENT").unwrap_or(DEFAULT_K6_ENVIRONMENT.to_owned())
}

// TODO(jsmith): Need to override the environment as my default is likely different than that of the
// script.

fn get_test_run_id(testid: &str) -> Option<String> {
    env::var("WALRUS_K6_TEST_RUN_ID_SUFFIX")
        .map(|suffix| format!("{testid}:{suffix}"))
        .ok()
}

fn script_path<P: AsRef<Path>>(script: P) -> PathBuf {
    env::var("WALRUS_K6_SCRIPTS_DIRECTORY")
        .map(|s| PathBuf::from(s))
        .unwrap_or(Path::new(DEFAULT_K6_SCRIPTS_DIRECTORY).to_path_buf())
        .join(script)
}

fn report_path(testid: &str) -> Option<String> {
    env::var("WALRUS_K6_REPORT_DIRECTORY")
        .map(|directory| {
            let filename_stem = testid.replace(':', "-");
            format!("{directory}/{filename_stem}.html")
        })
        .ok()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ByteSize(usize);

impl ByteSize {
    pub fn byte_value(self) -> usize {
        self.0
    }

    pub fn kibi(value: usize) -> Self {
        Self(value << 10)
    }

    pub fn mebi(value: usize) -> Self {
        Self(value << 20)
    }

    pub fn gibi(value: usize) -> Self {
        Self(value << 30)
    }
}

impl Display for ByteSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (shift, suffix) = if self.0.trailing_zeros() >= 30 {
            (30, "Gi")
        } else if self.0.trailing_zeros() >= 20 {
            (20, "Mi")
        } else if self.0.trailing_zeros() >= 10 {
            (10, "Ki")
        } else {
            (0, "")
        };
        write!(f, "{}{}", self.0 >> shift, suffix)
    }
}

impl Div<usize> for ByteSize {
    type Output = Self;

    fn div(self, rhs: usize) -> Self::Output {
        Self(self.0 / rhs)
    }
}
