#![allow(dead_code)]
use std::path::Path;

use anyhow::Result;
use sui_types::base_types::ObjectID;
use walrus_sui::{config::WalletConfig, wallet::Wallet};

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

    /// Funds a shared blob object with the specified amount.
    pub async fn fund_shared_blob(
        &self,
        shared_blob_obj_id: ObjectID,
        amount: u64,
        gas_budget: Option<u64>,
    ) -> Result<()> {
        let config = self.required_config("fund_shared_blob")?;
        let sui_client = config
            .new_contract_client(
                self.required_wallet("fund_shared_blob")?.clone(),
                gas_budget,
            )
            .await?;
        Ok(sui_client
            .fund_shared_blob(shared_blob_obj_id, amount)
            .await?)
    }
}
