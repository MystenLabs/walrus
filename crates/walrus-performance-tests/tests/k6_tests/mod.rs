// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    env,
    fmt::{self, Display},
    ops::Div,
    path::{Path, PathBuf},
    str::FromStr,
    sync::LazyLock,
};

use crate::k6_tests::k6::K6;

mod aggregator;
mod k6;
mod publisher;

/// Alias for test results that return a standard error.
type TestResult = Result<(), Box<dyn std::error::Error>>;

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
    LazyLock::new(|| env::var("WALRUS_K6_QUIET").map_or(false, |s| s.parse().unwrap_or(false)));

/// If set to 'true', call k6 with the `--no-color` flag.
static WALRUS_K6_NO_COLOR: LazyLock<bool> =
    LazyLock::new(|| env::var("WALRUS_K6_NO_COLOR").map_or(false, |s| s.parse().unwrap_or(false)));

/// Create a new [`K6`] instance for running a command and sets common options.
fn run<P>(script: P, testid_suffix: &str) -> K6
where
    P: AsRef<Path>,
{
    let testid = format!("{0}:{testid_suffix}", *WALRUS_K6_ENVIRONMENT);
    let script = script_path(script);

    let mut k6_run = K6::new(script);
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

/// Environment in which the tests are running.
///
/// Used to tag metrics as well as determine some test defaults.
// An enum is used to ensure that whenever a new environment is added, the places where defaults
// need to be set are highlighted.
#[derive(Debug, Default, PartialEq, Eq, Clone, Copy)]
enum K6Environment {
    #[default]
    Localhost,
    NightlyBaseline,
    TestnetFromCi,
}

impl FromStr for K6Environment {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "localhost" => Ok(Self::Localhost),
            "ci-testnet-performance" => Ok(Self::TestnetFromCi),
            "performance-main-baseline" => Ok(Self::NightlyBaseline),
            _ => Err(format!("unrecognised environment: {s}")),
        }
    }
}

impl Display for K6Environment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            K6Environment::Localhost => f.write_str("localhost"),
            K6Environment::NightlyBaseline => f.write_str("performance-main-baseline"),
            K6Environment::TestnetFromCi => f.write_str("ci-testnet-performance"),
        }
    }
}

/// A wrapper around byte counts which allows printing the value as `<x>Ki`, `<x>Mi`, or `<x>Gi`.
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
