// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{
    Deserialize,
    Deserializer,
    de::{DeserializeOwned, Error},
};

/// Can be used to deserialize optional paths such that the `~` is resolved to the user's home
/// directory.
pub fn resolve_home_dir<'de, D>(deserializer: D) -> Result<PathBuf, D::Error>
where
    D: Deserializer<'de>,
{
    let path: PathBuf = Deserialize::deserialize(deserializer)?;
    path_with_resolved_home_dir(path).map_err(D::Error::custom)
}

/// Can be used to deserialize optional paths such that the `~` is resolved to the user's home
/// directory.
pub fn resolve_home_dir_vec<'de, D>(deserializer: D) -> Result<Vec<PathBuf>, D::Error>
where
    D: Deserializer<'de>,
{
    let paths: Vec<PathBuf> = Deserialize::deserialize(deserializer)?;
    paths
        .into_iter()
        .map(|p| path_with_resolved_home_dir(p).map_err(D::Error::custom))
        .collect()
}

/// Can be used to deserialize optional paths such that the `~` is resolved to the user's home
/// directory.
pub fn resolve_home_dir_option<'de, D>(deserializer: D) -> Result<Option<PathBuf>, D::Error>
where
    D: Deserializer<'de>,
{
    let path: Option<PathBuf> = Deserialize::deserialize(deserializer)?;
    let Some(path) = path else { return Ok(None) };
    Ok(Some(
        path_with_resolved_home_dir(path).map_err(D::Error::custom)?,
    ))
}

fn path_with_resolved_home_dir(path: PathBuf) -> Result<PathBuf> {
    if path.starts_with("~/") {
        let home = home::home_dir().context("unable to resolve home directory")?;
        Ok(home.join(
            path.strip_prefix("~")
                .expect("we just checked for this prefix"),
        ))
    } else {
        Ok(path)
    }
}

/// Returns the path if it is `Some` or any of the default paths if they exist (attempt in order).
pub fn path_or_defaults_if_exist(
    path: Option<impl AsRef<Path>>,
    defaults: &[PathBuf],
) -> Option<PathBuf> {
    let mut path = path.map(|p| p.as_ref().to_path_buf());
    tracing::debug!(?path, ?defaults, "looking for configuration file");
    for default in defaults {
        if path.is_some() {
            break;
        }
        path = default.exists().then_some(default.clone());
    }
    path.map(|p| p.to_path_buf())
}

/// Trait for configuration types that can be loaded from a file and validated.
pub trait Config: DeserializeOwned {
    /// Validates the configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid.
    fn validate(&self) -> anyhow::Result<()> {
        Ok(())
    }

    /// Loads the configuration from a file and validates it.
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration cannot be read or parsed, or if the config is invalid.
    fn load_and_validate(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let config: Self = super::load_from_yaml(path)?;
        config.validate()?;
        Ok(config)
    }
}
