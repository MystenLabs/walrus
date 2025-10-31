// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(missing_docs)]

use std::{env, path::Path};

use anyhow::Context;
use serde::de::DeserializeOwned;

#[cfg(feature = "backoff")]
pub mod backoff;

#[cfg(feature = "config")]
pub mod config;

#[cfg(feature = "metrics")]
pub mod metrics;

#[cfg(feature = "http")]
pub mod http;

#[cfg(feature = "log")]
pub mod tracing_sampled;

#[cfg(feature = "log")]
pub use tracing_sampled::*;

#[cfg(feature = "test-utils")]
pub mod tests {
    use std::sync::OnceLock;

    use tokio::sync::Mutex;

    // Prevent tests running simultaneously to avoid interferences or race conditions.
    pub fn global_test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(Mutex::default)
    }
}

#[inline]
pub fn is_internal_run() -> bool {
    env::var("INTERNAL_RUN")
        .map(|v| v == "true")
        .unwrap_or(false)
}

/// Load the config from a YAML file located at the provided path.
pub fn load_from_yaml<P: AsRef<Path>, T: DeserializeOwned>(path: P) -> anyhow::Result<T> {
    let path = path.as_ref();
    tracing::debug!(path = %path.display(), "[load_from_yaml] reading from file");

    let reader = std::fs::File::open(path).with_context(|| {
        format!(
            "[load_from_yaml] unable to load config from {}",
            path.display()
        )
    })?;

    Ok(serde_yaml::from_reader(reader)?)
}

/// Reads raw data from the filesystem or returns a helpful error message.
pub fn read_data_from_file(path: impl AsRef<Path>) -> anyhow::Result<Vec<u8>> {
    std::fs::read(&path).context(format!(
        "unable to read data from '{}'",
        path.as_ref().display()
    ))
}

/// A macro to print a crumb of information to the console. This is useful for debugging.
#[macro_export]
macro_rules! crumb {
    ($($arg:tt)+) => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let name = type_name_of(f);
        eprintln!(
            "{}:{}: crumb: [{:?}]: {}: {}",
            file!(),
            line!(),
            std::thread::current().id(),
            name.strip_suffix("::f").expect("type name should end with ::f"),
            format!($($arg)+),
        );
    }};
    ($expr:literal) => {{
        $crate::crumb!("{}", $expr);
    }};
    () => {{
        $crate::crumb!("you left a crumb here.");
    }};
}

/// Hidden reexports for the bin_version macro
pub mod _hidden {
    pub use const_str::concat;
    pub use git_version::git_version;
}

/// Define constants that hold the git revision and package versions.
///
/// Defines two global `const`s:
///   `GIT_REVISION`: The git revision as specified by the `GIT_REVISION` env
/// variable provided at   compile time, or the current git revision as
/// discovered by running `git describe`.
///
///   `VERSION`: The value of the `CARGO_PKG_VERSION` environment variable
/// concatenated with the   value of `GIT_REVISION`.
///
/// Note: This macro must only be used from a binary, if used inside a library
/// this will fail to compile.
#[macro_export]
macro_rules! bin_version {
    () => {
        $crate::git_revision!();

        const VERSION: &str =
            $crate::_hidden::concat!(env!("CARGO_PKG_VERSION"), "-", GIT_REVISION);
    };
}

/// Defines constant that holds the git revision at build time.
///
///   `GIT_REVISION`: The git revision as specified by the `GIT_REVISION` env
/// variable provided at   compile time, or the current git revision as
/// discovered by running `git describe`.
///
/// Note: This macro must only be used from a binary, if used inside a library
/// this will fail to compile.
#[macro_export]
macro_rules! git_revision {
    () => {
        const _ASSERT_IS_BINARY: () = {
            env!(
                "CARGO_BIN_NAME",
                "`bin_version!()` must be used from a binary"
            );
        };

        /// The Git revision obtained through `git describe` at compile time.
        const GIT_REVISION: &str = {
            if let Some(revision) = option_env!("GIT_REVISION") {
                revision
            } else {
                let version = $crate::_hidden::git_version!(
                    args = ["--always", "--abbrev=12", "--dirty", "--exclude", "*"],
                    fallback = ""
                );
                if version.is_empty() {
                    panic!("unable to query git revision");
                }
                version
            }
        };
    };
}
