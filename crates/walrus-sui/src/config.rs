// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Wallet configuration utilities.
use std::{
    fmt::Debug,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use sui_keys::keystore::AccountKeystore;
use sui_sdk::wallet_context::WalletContext;
use sui_types::base_types::SuiAddress;
use walrus_utils::config::{path_or_defaults_if_exist, resolve_home_dir};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(untagged)]
/// The wallet configuration is a mechanism for specifying the sui wallet config, with a potential
/// override for the active environment.
pub enum WalletConfig {
    /// The path to the wallet configuration file. The wallet context will use the active
    /// environment from the given configuration file.
    Path(#[serde(default, deserialize_with = "resolve_home_dir")] PathBuf),
    /// The path to the wallet configuration file with some optional overrides `active_env`, and
    /// `active_address`. The wallet context will use the specified `active_env` or `active_address`
    /// if it is non-null. Loading of the `WalletContext` will fail if an invalid `active_...`
    /// parameter is used. Specifying a null `active_..` parameter will use its prespecified value
    /// from the related configuration file.
    PathWithOverride {
        /// The path to the wallet configuration file.
        #[serde(default, deserialize_with = "resolve_home_dir")]
        path: PathBuf,
        /// The optional `active_env` to use to override whatever `active_env` is listed in the
        /// configuration file.
        #[serde(default)]
        active_env: Option<String>,
        /// The optional `active_address` to use to override whatever `active_address` is listed in
        /// the configuration file.
        #[serde(default)]
        active_address: Option<SuiAddress>,
    },
}

/// Helper function to load the wallet context from the given optional wallet path.
pub fn load_wallet_context_from_path(
    wallet_path: Option<impl AsRef<Path>>,
) -> Result<WalletContext> {
    WalletConfig::load_wallet_context(wallet_path.map(WalletConfig::from_path).as_ref())
}

impl WalletConfig {
    /// Construct a simple pass-through wallet configuration from a path.
    pub fn from_path(path: impl AsRef<Path>) -> Self {
        WalletConfig::Path(path.as_ref().to_path_buf())
    }

    /// Retrieves the path to the wallet configuration file.
    pub fn path(&self) -> &Path {
        match self {
            WalletConfig::Path(path) => path,
            WalletConfig::PathWithOverride { path, .. } => path,
        }
    }

    /// Retrieves the `active_env`, if it was specified.
    pub fn active_env(&self) -> Option<&str> {
        match self {
            WalletConfig::Path(_) => None,
            WalletConfig::PathWithOverride { active_env, .. } => active_env.as_deref(),
        }
    }

    /// Retrieves the `active_address`, if it was specified.
    pub fn active_address(&self) -> Option<SuiAddress> {
        match self {
            WalletConfig::Path(_) => None,
            WalletConfig::PathWithOverride { active_address, .. } => *active_address,
        }
    }

    /// Loads the wallet context from the given optional wallet config (optional path and optional
    /// Sui env).
    ///
    /// If no path is provided, tries to load the configuration first from the local folder, and
    /// then from the standard Sui configuration directory.
    // NB: When making changes to the logic, make sure to update the argument docs in
    // `crates/walrus-service/bin/client.rs`.
    pub fn load_wallet_context(wallet_config: Option<&WalletConfig>) -> Result<WalletContext> {
        let mut default_paths = vec!["./sui_config.yaml".into()];
        if let Some(home_dir) = home::home_dir() {
            default_paths.push(home_dir.join(".sui").join("sui_config").join("client.yaml"))
        }

        let path = path_or_defaults_if_exist(wallet_config.map(|c| c.path()), &default_paths)
            .ok_or(anyhow!("could not find a valid wallet config file"))?;
        tracing::info!("using Sui wallet configuration from '{}'", path.display());
        let mut wallet_context: WalletContext = WalletContext::new(&path, None, None)?;
        if let Some(active_env) = wallet_config.and_then(|wallet_config| wallet_config.active_env())
        {
            if !wallet_context
                .config
                .envs
                .iter()
                .any(|env| env.alias == active_env)
            {
                return Err(anyhow!(
                    "Env '{}' not found in wallet config file '{}'.",
                    active_env,
                    path.display()
                ));
            }
            wallet_context.config.active_env = Some(active_env.to_string());
        }
        if let Some(active_address) =
            wallet_config.and_then(|wallet_config| wallet_config.active_address())
        {
            if !wallet_context
                .config
                .keystore
                .addresses()
                .iter()
                .any(|address| *address == active_address)
            {
                return Err(anyhow!(
                    "Address '{}' not found in wallet config file '{}'.",
                    active_address,
                    path.display()
                ));
            }

            wallet_context.config.active_address = Some(active_address);
        }
        Ok(wallet_context)
    }
}
