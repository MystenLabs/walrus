// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{fs, path::Path};

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

/// Reads a blob from the filesystem or returns a helpful error message.
pub fn read_blob_from_file(path: impl AsRef<Path>) -> anyhow::Result<Vec<u8>> {
    fs::read(&path).context(format!(
        "unable to read blob from '{}'",
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
            name.strip_suffix("::f").unwrap(),
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
