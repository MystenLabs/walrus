// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    borrow::Cow,
    env,
    error::Error,
    ffi::OsStr,
    fmt::{Display, Write},
    path::PathBuf,
    process::Command,
};

#[derive(Debug)]
pub struct K6Command {
    args: Vec<Cow<'static, str>>,
    script: PathBuf,
}

impl Display for K6Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("k6")?;
        for arg in self.args.iter() {
            f.write_char(' ')?;
            f.write_str(&arg)?;
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
        let mut command = Self {
            args: vec!["run".into()],
            script: script.into(),
        };

        if env::var("WALRUS_K6_QUIET")
            .map(|value| value == "true")
            .unwrap_or(false)
        {
            command.quiet();
        }
        if let Some(out) = env::var("WALRUS_K6_OUT").ok() {
            command.out(out);
        }
        command
    }

    pub fn maybe_env<S>(&mut self, key: &str, value: Option<S>) -> &mut Self
    where
        S: AsRef<str>,
    {
        if let Some(value) = value {
            self.env(key, value)
        } else {
            self
        }
    }

    pub fn env<S>(&mut self, key: &str, value: S) -> &mut Self
    where
        S: AsRef<str>,
    {
        self.args
            .extend(["--env".into(), format!("{key}={0}", value.as_ref()).into()]);
        self
    }

    pub fn quiet(&mut self) -> &mut Self {
        self.args.push("--quiet".into());
        self
    }

    pub fn out<S>(&mut self, target: S) -> &mut Self
    where
        S: Into<Cow<'static, str>>,
    {
        self.args.extend(["--out".into(), target.into()]);
        self
    }

    pub fn web_dashboard_export<S>(&mut self, path: Option<S>) -> &mut Self
    where
        S: AsRef<str>,
    {
        if let Some(path) = path {
            self.env("K6_WEB_DASHBOARD", "true");
            self.env("K6_WEB_DASHBOARD_EXPORT", path);
        }
        self
    }

    pub fn status(&mut self) -> Result<(), Box<dyn Error>> {
        println!("k6 command: {}", self);

        let mut args: Vec<_> = self
            .args
            .iter()
            .map(|s| OsStr::new((*s).as_ref()))
            .collect();
        args.push(OsStr::new(&self.script));

        let status = Command::new("k6").args(args).status()?;

        if status.success() {
            Ok(())
        } else {
            Err(status.to_string().into())
        }
    }
}
