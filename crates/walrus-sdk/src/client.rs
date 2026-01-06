// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! The Walrus Rust SDK Client. This module provides the primary client
//! structure for interacting with the Walrus SDK, including wallet management,
//! configuration loading, and various operations related to blob management.
#![allow(dead_code)]
use std::path::Path;

use anyhow::Result;
use sui_types::base_types::ObjectID;
use walrus_sui::{
    client::{SuiContractClient, SuiReadClient, retry_client::RetriableSuiClient},
    config::WalletConfig,
    types::move_structs::BlobAttribute,
    wallet::Wallet,
};

use crate::{
    blocklist::Blocklist,
    config::{ClientConfig, load_configuration},
    error::{ClientErrorKind, ClientResult},
    node_client::WalrusNodeClient,
};

/// The primary client structure for interacting with the Walrus SDK.
#[allow(missing_debug_implementations)]
pub struct WalrusClient {
    /// The Sui wallet for the client.
    wallet: Result<Wallet>,
    /// The config for the client.
    config: Result<ClientConfig>,
}

fn ok_or_err<T>(res: &Result<T>) -> &'static str {
    match res {
        Ok(_) => "ok",
        Err(_) => "err",
    }
}

impl std::fmt::Debug for WalrusClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WalrusClient(wallet={}, config={})",
            ok_or_err(&self.wallet),
            ok_or_err(&self.config)
        )
    }
}

impl WalrusClient {
    /// Creates a new client runner, loading the configuration and wallet context.
    pub fn new(
        config_path: &Option<impl AsRef<Path>>,
        context: Option<&str>,
        wallet_override: &Option<impl AsRef<Path>>,
    ) -> Self {
        let config = load_configuration(config_path.as_ref(), context);
        let wallet_config = wallet_override
            .as_ref()
            .map(WalletConfig::from_path)
            .or(config
                .as_ref()
                .ok()
                .and_then(|config: &ClientConfig| config.wallet_config.clone()));
        let wallet = WalletConfig::load_wallet(
            wallet_config.as_ref(),
            config
                .as_ref()
                .ok()
                .and_then(|config| config.communication_config.sui_client_request_timeout),
        );

        Self { wallet, config }
    }

    /// Returns the wallet, or an error if it is not available.
    #[deprecated]
    pub fn required_wallet(&self, context: &str) -> ClientResult<&Wallet> {
        match &self.wallet {
            Ok(wallet) => Ok(wallet),
            Err(e) => Err(ClientErrorKind::InitializationError(format!(
                "Client operation '{context}' requires a valid wallet; the wallet failed to \
                    load with: {e:?}"
            ))
            .into()),
        }
    }

    /// Returns the configuration, or an error if it is not available.
    #[deprecated]
    pub fn required_config(&self, context: &str) -> ClientResult<&ClientConfig> {
        match &self.config {
            Ok(config) => Ok(config),
            Err(e) => {
                tracing::error!(
                    "Client operation '{context}' requires a valid configuration; it failed to \
                    load with: {e:?}"
                );
                Err(ClientErrorKind::InvalidConfig.into())
            }
        }
    }

    /// Returns both the wallet and configuration, or an error if either is not available.
    #[deprecated]
    pub fn required_both(&self, context: &str) -> ClientResult<(&Wallet, &ClientConfig)> {
        Ok((
            self.required_wallet(context)?,
            self.required_config(context)?,
        ))
    }

    /// Funds a shared blob object with the specified amount.
    pub async fn fund_shared_blob(
        &self,
        shared_blob_obj_id: ObjectID,
        amount: u64,
        gas_budget: Option<u64>,
    ) -> Result<()> {
        #[allow(deprecated)]
        let (wallet, config) = self.required_both("fund_shared_blob")?;
        let sui_client = config
            .new_contract_client(wallet.clone(), gas_budget)
            .await?;
        Ok(sui_client
            .fund_shared_blob(shared_blob_obj_id, amount)
            .await?)
    }

    /// Creates a new Sui contract client using the current wallet and configuration.
    pub async fn new_sui_write_client(
        &self,
        context: &str,
        gas_budget: Option<u64>,
    ) -> Result<SuiContractClient> {
        #[allow(deprecated)]
        let (wallet, config) = self.required_both(context)?;
        let sui_client = config
            .new_contract_client(wallet.clone(), gas_budget)
            .await?;
        Ok(sui_client)
    }

    /// Creates a long-lived object able to read and write directly to/from Walrus storage nodes.
    /// See [`WalrusNodeClient<SuiContractClient>`].
    #[tracing::instrument(skip_all)]
    pub async fn walrus_node_write_client(
        &self,
        context: &str,
        gas_budget: Option<u64>,
    ) -> ClientResult<WalrusNodeClient<SuiContractClient>> {
        #[allow(deprecated)]
        let (wallet, config) = self.required_both(context)?;
        let sui_client = config
            .new_contract_client(wallet.clone(), gas_budget)
            .await?;

        WalrusNodeClient::new_contract_client_with_refresher(config.clone(), sui_client).await
    }

    /// Creates a long-lived object able to read from Walrus storage nodes. See
    /// [`WalrusNodeClient<SuiReadClient>`].
    #[tracing::instrument(skip_all)]
    pub async fn walrus_node_read_client(
        &self,
        context: &str,
        rpc_url: Option<String>,
    ) -> ClientResult<WalrusNodeClient<SuiReadClient>> {
        #[allow(deprecated)]
        let config = self.required_config(context)?.clone();
        WalrusNodeClient::new_read_client_with_refresher(
            config,
            self.new_sui_read_client(context, rpc_url).await?,
        )
        .await
    }

    /// Creates a new Sui read client using the current wallet and configuration.
    pub async fn new_sui_read_client(
        &self,
        context: &str,
        rpc_url: Option<String>,
    ) -> ClientResult<SuiReadClient> {
        #[allow(deprecated)]
        let config = self.required_config(context)?;
        let sui_read_client =
            get_sui_read_client_from_rpc_node_or_wallet(config, rpc_url, &self.wallet).await?;
        Ok(sui_read_client)
    }

    /// Extends the lifetime of a blob object by the specified number of epochs.
    pub async fn extend_blob(
        &self,
        blob_obj_id: ObjectID,
        shared: bool,
        epochs_extended: u32,
        gas_budget: Option<u64>,
    ) -> Result<()> {
        let contract_client: SuiContractClient =
            self.new_sui_write_client("extend_blob", gas_budget).await?;
        if shared {
            contract_client
                .extend_shared_blob(blob_obj_id, epochs_extended)
                .await?;
        } else {
            contract_client
                .extend_blob(blob_obj_id, epochs_extended)
                .await?;
        }
        Ok(())
    }

    /// Sets blob attributes for the specified blob object.
    pub async fn set_blob_attributes(
        &self,
        blob_obj_id: ObjectID,
        key_value_pairs: Vec<(String, String)>,
        gas_budget: Option<u64>,
    ) -> Result<()> {
        let mut sui_contract_client = self
            .new_sui_write_client("set_blob_attribute", gas_budget)
            .await?;
        let attribute = BlobAttribute::from(key_value_pairs);
        sui_contract_client
            .insert_or_update_blob_attribute_pairs(blob_obj_id, attribute.into_iter(), true)
            .await?;
        Ok(())
    }
}

/// Creates a [`SuiReadClient`] from the provided RPC URL or wallet.
///
/// The RPC URL is set based on the `rpc_url` parameter (if `Some`), the `rpc_url` field in the
/// `config` (if `Some`), or the `wallet` (if `Ok`). An error is returned if it cannot be set
/// successfully.
// NB: When making changes to the logic, make sure to update the docstring of `get_read_client` and
// the argument docs in `crates/walrus-service/client/cli/args.rs`.
pub async fn get_sui_read_client_from_rpc_node_or_wallet(
    config: &ClientConfig,
    rpc_url: Option<String>,
    wallet: &Result<Wallet>,
) -> ClientResult<SuiReadClient> {
    tracing::debug!(
        ?rpc_url,
        ?config.rpc_urls,
        "attempting to create a read client from explicitly set RPC URL, RPC URLs in client \
        config, or wallet config"
    );
    let backoff_config = config.backoff_config().clone();
    let rpc_urls = match (rpc_url, &config.rpc_urls, wallet) {
        (Some(url), _, _) => {
            tracing::info!("using explicitly set RPC URL: {url}");
            vec![url]
        }
        (_, urls, _) if !urls.is_empty() => {
            tracing::info!(
                "using RPC URLs set in client configuration: {}",
                urls.join(", ")
            );
            urls.clone()
        }
        (_, _, Ok(wallet)) => {
            let url = wallet.get_rpc_url().to_string();
            tracing::info!("using RPC URL set in wallet configuration: {url}");
            vec![url]
        }
        (_, _, Err(e)) => {
            return Err(ClientErrorKind::InitializationError(format!(
                "Sui RPC url is not specified as a CLI argument or in the client configuration, \
                and no valid Sui wallet was provided ({e})"
            ))
            .into());
        }
    };

    let sui_client = RetriableSuiClient::new_for_rpc_urls(
        &rpc_urls,
        backoff_config,
        config.communication_config.sui_client_request_timeout,
    )?;

    Ok(config.new_read_client(sui_client).await?)
}

/// Creates a [`WalrusNodeClient`] based on the provided [`ClientConfig`] with read-only access to
/// Sui.
///
/// The RPC URL is set based on the `rpc_url` parameter (if `Some`), the `rpc_url` field in the
/// `config` (if `Some`), or the `wallet` (if `Ok`). An error is returned if it cannot be set
/// successfully.
pub async fn get_read_client(
    config: ClientConfig,
    rpc_url: Option<String>,
    wallet: Result<Wallet>,
    blocklist_path: &Option<impl AsRef<Path>>,
    max_blob_size: Option<u64>,
) -> Result<WalrusNodeClient<SuiReadClient>> {
    let sui_read_client =
        get_sui_read_client_from_rpc_node_or_wallet(&config, rpc_url, &wallet).await?;

    let refresh_handle = config
        .build_refresher_and_run(sui_read_client.clone())
        .await?;
    let client = WalrusNodeClient::new_read_client_with_max_blob_size(
        config,
        refresh_handle,
        sui_read_client,
        max_blob_size,
    )?;

    if blocklist_path.is_some() {
        Ok(client.with_blocklist(Blocklist::new(blocklist_path)?))
    } else {
        Ok(client)
    }
}
