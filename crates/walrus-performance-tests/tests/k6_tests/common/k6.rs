// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use core::fmt;
use std::{
    borrow::Cow,
    error::Error,
    ffi::OsStr,
    fmt::{Display, Write},
    path::PathBuf,
    process::Command,
    str::FromStr,
};

use crate::k6_tests::{WALRUS_K6_NO_COLOR, WALRUS_K6_OUT, WALRUS_K6_QUIET};

const K6_THRESHOLD_FAILED_EXIT_CODE: i32 = 99;

/// Environment in which the tests are running.
///
/// Used to tag metrics as well as determine some test defaults.
// An enum is used to ensure that whenever a new environment is added, the places where defaults
// need to be set are highlighted.
#[derive(Debug, Default, PartialEq, Eq, Clone, Copy, Hash)]
pub(crate) enum K6Environment {
    #[default]
    Localhost,
    NightlyBaseline,
    NightlyWithLatency,
    TestnetFromCi,
}

impl FromStr for K6Environment {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "localhost" => Ok(Self::Localhost),
            "ci-testnet-performance" => Ok(Self::TestnetFromCi),
            "performance-main-baseline" => Ok(Self::NightlyBaseline),
            "performance-main-latency" => Ok(Self::NightlyWithLatency),
            _ => Err(format!("unrecognised environment: {s}")),
        }
    }
}

impl Display for K6Environment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            K6Environment::Localhost => f.write_str("localhost"),
            K6Environment::NightlyBaseline => f.write_str("performance-main-baseline"),
            K6Environment::NightlyWithLatency => f.write_str("performance-main-latency"),
            K6Environment::TestnetFromCi => f.write_str("ci-testnet-performance"),
        }
    }
}

/// Executes a load test using the Grafana k6 load testing utility.
#[derive(Debug)]
pub struct K6Command {
    is_threshold_failure_ok: bool,
    args: Vec<Cow<'static, str>>,
    script: PathBuf,
}

impl Display for K6Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("k6")?;
        for arg in self.args.iter() {
            f.write_char(' ')?;
            f.write_str(arg)?;
        }
        f.write_char(' ')?;
        f.write_fmt(format_args!("{}", self.script.display()))?;
        Ok(())
    }
}

impl K6Command {
    pub fn new<P>(script: P) -> Self
    where
        P: Into<PathBuf>,
    {
        let mut args = vec!["run".into()];

        if *WALRUS_K6_QUIET {
            args.push("--quiet".into());
        }
        if let Some(target) = WALRUS_K6_OUT.as_deref() {
            args.extend(["--out".into(), target.into()]);
        }
        if *WALRUS_K6_NO_COLOR {
            args.push("--no-color".into());
        }

        Self {
            args,
            script: script.into(),
            is_threshold_failure_ok: false,
        }
    }

    pub fn tag<V>(&mut self, key: &str, value: V) -> &mut Self
    where
        V: Display,
    {
        self.args
            .extend(["--tag".into(), format!("{key}={0}", value).into()]);
        self
    }

    pub fn maybe_tag<V>(&mut self, key: &str, value: Option<V>) -> &mut Self
    where
        V: Display,
    {
        let Some(value) = value else {
            return self;
        };
        self.tag(key, value)
    }

    pub fn allow_threshold_failures(&mut self) -> &mut Self {
        self.is_threshold_failure_ok = true;
        self
    }

    pub fn env<S>(&mut self, key: &str, value: S) -> &mut Self
    where
        S: Display,
    {
        self.args
            .extend(["--env".into(), format!("{key}={0}", value).into()]);
        self
    }

    pub fn web_dashboard_export<S>(&mut self, path: Option<S>) -> &mut Self
    where
        S: Display,
    {
        if let Some(path) = path {
            self.env("K6_WEB_DASHBOARD", "true");
            self.env("K6_WEB_DASHBOARD_EXPORT", path);
        }
        self
    }

    pub fn status(&mut self) -> Result<(), Box<dyn Error>> {
        println!("{}", self);

        let mut args: Vec<_> = self
            .args
            .iter()
            .map(|s| OsStr::new((*s).as_ref()))
            .collect();
        args.push(OsStr::new(&self.script));

        let status = Command::new("k6").args(args).status()?;

        if status.success()
            || self.is_threshold_failure_ok && status.code() == Some(K6_THRESHOLD_FAILED_EXIT_CODE)
        {
            Ok(())
        } else {
            Err(status.to_string().into())
        }
    }
}
