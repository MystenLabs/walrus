// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    env,
    fmt::{self, Display},
    ops::Div,
    path::{Path, PathBuf},
};

mod k6;
mod publisher;

const DEFAULT_K6_SCRIPTS_DIRECTORY: &str = "../../scripts/k6/src/tests";

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

fn get_environment() -> String {
    env::var("WALRUS_K6_ENVIRONMENT").unwrap_or("localhost".to_owned())
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ByteSize(usize);

impl ByteSize {
    pub fn byte_value(self) -> usize {
        self.0
    }

    pub fn kibibyte(value: usize) -> Self {
        Self(value << 10)
    }

    pub fn mebibyte(value: usize) -> Self {
        Self(value << 20)
    }

    pub fn gibibyte(value: usize) -> Self {
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
