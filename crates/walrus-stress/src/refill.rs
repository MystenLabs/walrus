// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Utilities to refill gas for the stress clients.

use std::{collections::BTreeSet, path::PathBuf, sync::Arc, time::Duration};

use anyhow::Result;
use futures::{
    future::{self, try_join_all},
    StreamExt,
};
use sui_sdk::{
    rpc_types::SuiTransactionBlockResponse,
    types::{
        base_types::SuiAddress,
        programmable_transaction_builder::ProgrammableTransactionBuilder,
    },
    wallet_context::WalletContext,
    SuiClient,
};
use tokio::{sync::Mutex, task::JoinHandle, time::MissedTickBehavior};
use walrus_sui::utils::{send_faucet_request, sign_and_send_ptb, SuiNetwork};

use crate::metrics::ClientMetrics;

// If a node has less than `MIN_NUM_COINS` without at least `MIN_COIN_VALUE`,
// we need to request additional coins from the faucet.
const MIN_COIN_VALUE: u64 = 500_000_000;
// We need at least a payment coin and a gas coin.
const MIN_NUM_COINS: usize = 2;
/// The amount in MIST that is transferred from the wallet refill account to the stress clients at
/// each request.
const WALLET_REFILL_AMOUNT: u64 = 1_000_000_000;

/// Trait to request gas for a client.
pub(crate) trait GasRefill: Send + Sync {
    /// Sends a request to get gas for the given `address`.
    fn send_gas_request(
        &self,
        address: SuiAddress,
    ) -> impl std::future::Future<Output = Result<()>> + std::marker::Send;
}

/// The `GasRefill` implementation for the standard faucet.
pub(crate) struct FaucetGasRefill {
    pub(crate) network: SuiNetwork,
}

impl FaucetGasRefill {
    pub(crate) fn new(network: SuiNetwork) -> Self {
        Self { network }
    }
}

impl GasRefill for FaucetGasRefill {
    async fn send_gas_request(&self, address: SuiAddress) -> Result<()> {
        send_faucet_request(address, &self.network).await
    }
}

/// The `GasRefill` implementation a Sui wallet.
///
/// The wallet sends gas to the specified address.
pub(crate) struct WalletGasRefill {
    /// The wallet containing the funds.
    wallet: Mutex<WalletContext>,
    /// The amount of gas to send at each request.
    refill_size: u64,
    /// The active address of the wallet.
    // NOTE: This is added only to avoid having to borrow the wallet mutably when checking the
    // sender. (See the TODO in the `WalletContext::active_address` method.)
    sender: SuiAddress,
}

impl WalletGasRefill {
    /// The gas budget for each transaction.
    ///
    /// Should be sufficient to sent gas.
    const GAS_BUDGET: u64 = 100_000_000;

    pub(crate) fn new(mut wallet: WalletContext, refill_size: u64) -> Result<Self> {
        let sender = wallet.active_address()?;
        let wallet = Mutex::new(wallet);
        Ok(Self {
            wallet,
            refill_size,
            sender,
        })
    }

    async fn send_gas(&self, address: SuiAddress) -> Result<()> {
        let mut pt_builder = ProgrammableTransactionBuilder::new();
        pt_builder.pay_sui(vec![address], vec![self.refill_size])?;
        self.sign_and_send_ptb(pt_builder).await?;
        Ok(())
    }

    pub(crate) async fn sign_and_send_ptb(
        &self,
        programmable_transaction: ProgrammableTransactionBuilder,
    ) -> Result<SuiTransactionBlockResponse> {
        let wallet = self.wallet.lock().await;
        let gas_coin = wallet
            .gas_for_owner_budget(self.sender, Self::GAS_BUDGET, BTreeSet::new())
            .await?
            .1
            .object_ref();

        sign_and_send_ptb(
            self.sender,
            &wallet,
            programmable_transaction.finish(),
            vec![gas_coin],
            Self::GAS_BUDGET,
        )
        .await
    }
}

impl GasRefill for WalletGasRefill {
    async fn send_gas_request(&self, address: SuiAddress) -> Result<()> {
        self.send_gas(address).await
    }
}

pub(crate) enum FaucetOrWallet {
    Faucet(FaucetGasRefill),
    Wallet(WalletGasRefill),
}

impl FaucetOrWallet {
    pub(crate) fn new(sui_network: SuiNetwork, wallet_path: Option<PathBuf>) -> Result<Self> {
        if let Some(wallet_path) = wallet_path {
            tracing::info!(
                "Creating gas refill station from wallet: {:?}",
                &wallet_path
            );
            let wallet = walrus_service::utils::load_wallet_context(&Some(wallet_path))?;
            Ok(Self::new_wallet(wallet)?)
        } else {
            tracing::info!("Created gas refill station from faucet: {:?}", &sui_network);
            Ok(Self::new_faucet(sui_network))
        }
    }

    pub(crate) fn new_faucet(network: SuiNetwork) -> Self {
        Self::Faucet(FaucetGasRefill::new(network))
    }

    pub(crate) fn new_wallet(wallet: WalletContext) -> Result<Self> {
        Ok(Self::Wallet(WalletGasRefill::new(
            wallet,
            WALLET_REFILL_AMOUNT,
        )?))
    }
}

impl GasRefill for FaucetOrWallet {
    async fn send_gas_request(&self, address: SuiAddress) -> Result<()> {
        match self {
            Self::Faucet(faucet) => faucet.send_gas_request(address).await,
            Self::Wallet(wallet) => wallet.send_gas_request(address).await,
        }
    }
}

/// Refills the gas for the clients.
pub(crate) struct Refiller<G> {
    pub(crate) gas_refill: Arc<G>,
}

impl<G> Clone for Refiller<G> {
    fn clone(&self) -> Self {
        Self {
            gas_refill: self.gas_refill.clone(),
        }
    }
}

impl<G: GasRefill + 'static> Refiller<G> {
    pub(crate) fn new(gas_refill: G) -> Self {
        Self {
            gas_refill: Arc::new(gas_refill),
        }
    }

    pub(crate) fn refill_gas(
        &self,
        addresses: Vec<SuiAddress>,
        period: Duration,
        metrics: Arc<ClientMetrics>,
        sui_client: SuiClient,
    ) -> JoinHandle<anyhow::Result<()>> {
        let mut interval = tokio::time::interval(period);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let refiller = self.gas_refill.clone();

        tokio::spawn(async move {
            loop {
                interval.tick().await;
                let sui_client = &sui_client;
                let metrics = &metrics;
                let _ = try_join_all(addresses.iter().cloned().map(|address| {
                    let inner = refiller.clone();
                    async move {
                        if sui_client
                            .coin_read_api()
                            .get_coins_stream(address, None)
                            .filter(|coin| future::ready(coin.balance >= MIN_COIN_VALUE))
                            .take(MIN_NUM_COINS)
                            .collect::<Vec<_>>()
                            .await
                            .len()
                            < MIN_NUM_COINS
                        {
                            let result = inner.send_gas_request(address).await;
                            tracing::debug!("Clients gas coins refilled");
                            metrics.observe_gas_refill();
                            result
                        } else {
                            Ok(())
                        }
                    }
                }))
                .await
                .inspect_err(|e| tracing::error!("error while refilling gas: {e}"));
            }
        })
    }
}

impl<G: GasRefill> GasRefill for Refiller<G> {
    async fn send_gas_request(&self, address: SuiAddress) -> Result<()> {
        self.gas_refill.send_gas_request(address).await
    }
}
