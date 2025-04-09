// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::path::Path;

use anyhow::Context;
use serde::de::DeserializeOwned;

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
