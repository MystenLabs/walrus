#![allow(dead_code)]
use std::path::Path;

use anyhow::{Context, Result};
use sui_types::base_types::ObjectID;
use walrus_sui::{
    client::{SuiContractClient, SuiReadClient, retry_client::RetriableSuiClient},
    config::WalletConfig,
    types::move_structs::BlobAttribute,
    wallet::Wallet,
};

use crate::config::{ClientConfig, load_configuration};

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

    fn required_wallet(&self, context: &str) -> Result<&Wallet> {
        match &self.wallet {
            Ok(wallet) => Ok(wallet),
            Err(e) => Err(anyhow::anyhow!(
                "'{context}' requires a valid wallet; it failed to load with: {e:?}"
            )),
        }
    }

    fn required_config(&self, context: &str) -> Result<&ClientConfig> {
        match &self.config {
            Ok(config) => Ok(config),
            Err(e) => Err(anyhow::anyhow!(
                "'{context}' requires a valid configuration; it failed to load with: {e:?}"
            )),
        }
    }

    fn required_both(&self, context: &str) -> Result<(&Wallet, &ClientConfig)> {
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
        let (wallet, config) = self.required_both(context)?;
        let sui_client = config
            .new_contract_client(wallet.clone(), gas_budget)
            .await?;
        Ok(sui_client)
    }

    /// Creates a new Sui read client using the current wallet and configuration.
    pub async fn new_sui_read_client(&self, context: &str) -> Result<SuiReadClient> {
        let config = self.required_config(context)?;
        let sui_read_client =
            get_sui_read_client_from_rpc_node_or_wallet(config, None, &self.wallet).await?;
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
) -> Result<SuiReadClient> {
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
            anyhow::bail!(
                "Sui RPC url is not specified as a CLI argument or in the client configuration, \
                and no valid Sui wallet was provided ({e})"
            );
        }
    };

    let sui_client = RetriableSuiClient::new_for_rpc_urls(
        &rpc_urls,
        backoff_config,
        config.communication_config.sui_client_request_timeout,
    )
    .context(format!(
        "cannot connect to Sui RPC nodes at {}",
        rpc_urls.join(", ")
    ))?;

    Ok(config.new_read_client(sui_client).await?)
}
