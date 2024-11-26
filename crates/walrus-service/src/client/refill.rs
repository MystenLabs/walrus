// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Utilities to refill gas for the stress clients.

use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::Result;
use futures::{
    future::{self, try_join_all},
    StreamExt,
};
use sui_sdk::{
    types::{
        base_types::{ObjectID, SuiAddress},
        programmable_transaction_builder::ProgrammableTransactionBuilder,
    },
    SuiClient,
};
use tokio::{task::JoinHandle, time::MissedTickBehavior};
use walrus_sui::{
    client::SuiContractClient,
    utils::{send_faucet_request, SuiNetwork},
};

use super::metrics::ClientMetrics;
use crate::utils;

// If a node has less than `MIN_NUM_COINS` without at least `MIN_COIN_VALUE`,
// we need to request additional coins from the faucet.
const MIN_COIN_VALUE: u64 = 500_000_000;
// The minimum number of coins needed in the wallet.
const MIN_NUM_COINS: usize = 1;
/// The amount in MIST that is transferred from the wallet refill account to the stress clients at
/// each request.
const WALLET_MIST_AMOUNT: u64 = 1_000_000_000;
/// The amount in FROST that is transferred from the wallet refill account to the stress clients at
/// each request.
const WALLET_FROST_AMOUNT: u64 = 1_000_000_000;

/// Trait to request gas and Wal coins for a client.
pub trait CoinRefill: Send + Sync {
    /// Sends a request to get gas for the given `address`.
    fn send_gas_request(
        &self,
        address: SuiAddress,
    ) -> impl std::future::Future<Output = Result<()>> + std::marker::Send;

    /// Sends a request to get WAL for the given `address`.
    fn send_wal_request(
        &self,
        address: SuiAddress,
    ) -> impl std::future::Future<Output = Result<()>> + std::marker::Send;
}

/// The `CoinRefill` implementation that uses the Sui network.
///
/// The faucet is used to refill gas, and a contract is used to exchange sui for WAL.
#[derive(Debug)]
pub struct NetworkCoinRefill {
    pub network: SuiNetwork,
}

impl NetworkCoinRefill {
    pub fn new(network: SuiNetwork) -> Self {
        Self { network }
    }
}

impl CoinRefill for NetworkCoinRefill {
    async fn send_gas_request(&self, address: SuiAddress) -> Result<()> {
        send_faucet_request(address, &self.network).await
    }

    // TODO: The WAL refill in not implemented.
    async fn send_wal_request(&self, _address: SuiAddress) -> Result<()> {
        unimplemented!("WAL refill is not implemented for the network coin refill (#1015)")
    }
}

/// The `CoinRefill` implementation for a Sui wallet.
///
/// The wallet sends gas and WAL to the specified address.
#[derive(Debug)]
pub struct WalletCoinRefill {
    /// The wallet containing the funds.
    sui_client: SuiContractClient,
    /// The amount of MIST to send at each request.
    gas_refill_size: u64,
    /// The amount of FROST to send at each request.
    wal_refill_size: u64,
    /// The gas budget.
    gas_budget: u64,
}

impl WalletCoinRefill {
    /// The gas budget for each transaction.
    ///
    /// Should be sufficient to execute a coin transfer transaction.

    pub fn new(
        sui_client: SuiContractClient,
        gas_refill_size: u64,
        wal_refill_size: u64,
        gas_budget: u64,
    ) -> Result<Self> {
        Ok(Self {
            sui_client,
            gas_refill_size,
            wal_refill_size,
            gas_budget,
        })
    }

    async fn send_gas(&self, address: SuiAddress) -> Result<()> {
        tracing::debug!(%address, "sending gas to sub-wallet");
        let mut pt_builder = ProgrammableTransactionBuilder::new();

        // Lock the wallet here to ensure there are no race conditions with object references.
        let wallet = self.sui_client.wallet().await;

        pt_builder.pay_sui(vec![address], vec![self.gas_refill_size])?;
        self.sui_client
            .sign_and_send_ptb(
                &wallet,
                pt_builder.finish(),
                Some(self.gas_budget + self.gas_refill_size),
            )
            .await?;
        Ok(())
    }

    async fn send_wal(&self, address: SuiAddress) -> Result<()> {
        tracing::debug!(%address, "sending WAL to sub-wallet");
        let mut pt_builder = self.sui_client.transaction_builder();

        // Lock the wallet here to ensure there are no race conditions with object references.
        let wallet = self.sui_client.wallet().await;
        pt_builder.pay_wal(address, self.wal_refill_size).await?;
        let (ptb, _) = pt_builder.finish().await?;
        self.sui_client
            .sign_and_send_ptb(&wallet, ptb, None)
            .await?;
        Ok(())
    }
}

impl CoinRefill for WalletCoinRefill {
    async fn send_gas_request(&self, address: SuiAddress) -> Result<()> {
        self.send_gas(address).await
    }

    async fn send_wal_request(&self, address: SuiAddress) -> Result<()> {
        self.send_wal(address).await
    }
}

/// A `CoinRefill` implementation that can use either the network or a wallet.
#[derive(Debug)]
pub enum NetworkOrWallet {
    /// A refiller that uses a faucet.
    Network(NetworkCoinRefill),
    /// A refiller that uses a wallet.
    Wallet(WalletCoinRefill),
}

impl NetworkOrWallet {
    /// Creates a new refiller from either a wallet or a faucet.
    pub async fn new(
        system_object: ObjectID,
        staking_object: ObjectID,
        sui_network: SuiNetwork,
        wallet_path: Option<PathBuf>,
        gas_budget: u64,
    ) -> Result<Self> {
        if let Some(wallet_path) = wallet_path {
            tracing::info!(
                "creating gas refill station from wallet at '{}'",
                wallet_path.display()
            );
            let wallet = utils::load_wallet_context(&Some(wallet_path))?;
            let sui_client =
                SuiContractClient::new(wallet, system_object, staking_object, gas_budget).await?;
            Ok(Self::new_wallet(sui_client, gas_budget)?)
        } else {
            tracing::info!(?sui_network, "created gas refill station from faucet");
            Ok(Self::new_faucet(sui_network))
        }
    }

    /// Creates a new refiller that uses a faucet.
    fn new_faucet(network: SuiNetwork) -> Self {
        Self::Network(NetworkCoinRefill::new(network))
    }

    /// Creates a new refiller that uses a wallet.
    pub fn new_wallet(sui_client: SuiContractClient, gas_budget: u64) -> Result<Self> {
        Ok(Self::Wallet(WalletCoinRefill::new(
            sui_client,
            WALLET_MIST_AMOUNT,
            WALLET_FROST_AMOUNT,
            gas_budget,
        )?))
    }
}

impl CoinRefill for NetworkOrWallet {
    async fn send_gas_request(&self, address: SuiAddress) -> Result<()> {
        match self {
            Self::Network(faucet) => faucet.send_gas_request(address).await,
            Self::Wallet(wallet) => wallet.send_gas_request(address).await,
        }
    }

    async fn send_wal_request(&self, address: SuiAddress) -> Result<()> {
        match self {
            Self::Network(faucet) => faucet.send_wal_request(address).await,
            Self::Wallet(wallet) => wallet.send_wal_request(address).await,
        }
    }
}

/// Refills gas and WAL for the clients.
#[derive(Debug)]
pub struct Refiller<G> {
    /// The inner implementation of the refiller.
    pub refill_inner: Arc<G>,
    /// The package id of the system.
    pub system_pkg_id: ObjectID,
}

impl<G> Clone for Refiller<G> {
    fn clone(&self) -> Self {
        Self {
            refill_inner: self.refill_inner.clone(),
            system_pkg_id: self.system_pkg_id,
        }
    }
}

impl<G: CoinRefill + 'static> Refiller<G> {
    /// Creates a new refiller.
    pub fn new(gas_refill: G, system_pkg_id: ObjectID) -> Self {
        Self {
            refill_inner: Arc::new(gas_refill),
            system_pkg_id,
        }
    }

    /// Refills gas and WAL for the clients.
    pub fn refill_gas_and_wal(
        &self,
        addresses: Vec<SuiAddress>,
        period: Duration,
        metrics: Arc<ClientMetrics>,
        sui_client: SuiClient,
    ) -> RefillHandles {
        let _gas_refill_handle = self.refill_gas(
            addresses.clone(),
            period,
            metrics.clone(),
            sui_client.clone(),
        );
        let _wal_refill_handle =
            self.refill_wal(addresses, period, metrics.clone(), sui_client.clone());

        RefillHandles {
            _gas_refill_handle,
            _wal_refill_handle,
        }
    }

    fn refill_gas(
        &self,
        addresses: Vec<SuiAddress>,
        period: Duration,
        metrics: Arc<ClientMetrics>,
        sui_client: SuiClient,
    ) -> JoinHandle<anyhow::Result<()>> {
        self.periodic_refill(
            addresses,
            period,
            sui_client,
            None, // Use SUI
            MIN_COIN_VALUE,
            MIN_NUM_COINS,
            move |refiller, address| {
                let metrics = metrics.clone();
                async move {
                    refiller.send_gas_request(address).await?;
                    tracing::info!("clients gas coins refilled");
                    metrics.observe_gas_refill();
                    Ok(())
                }
            },
        )
    }

    fn refill_wal(
        &self,
        addresses: Vec<SuiAddress>,
        period: Duration,
        metrics: Arc<ClientMetrics>,
        sui_client: SuiClient,
    ) -> JoinHandle<anyhow::Result<()>> {
        self.periodic_refill(
            addresses,
            period,
            sui_client,
            Some(self.coin_type()),
            MIN_COIN_VALUE,
            MIN_NUM_COINS,
            move |refiller, address| {
                let metrics = metrics.clone();
                async move {
                    refiller.send_wal_request(address).await?;
                    tracing::info!("clients WAL coins refilled");
                    metrics.observe_wal_refill();
                    Ok(())
                }
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn periodic_refill<F, Fut>(
        &self,
        addresses: Vec<SuiAddress>,
        period: Duration,
        sui_client: SuiClient,
        coin_type: Option<String>,
        min_coin_value: u64,
        min_num_coins: usize,
        inner_action: F,
    ) -> JoinHandle<anyhow::Result<()>>
    where
        F: Fn(Arc<G>, SuiAddress) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = anyhow::Result<()>> + Send,
    {
        let mut interval = tokio::time::interval(period);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let refiller = self.refill_inner.clone();
        tokio::spawn(async move {
            loop {
                interval.tick().await;
                let sui_client = &sui_client;
                let _ = try_join_all(addresses.iter().cloned().map(|address| {
                    let coin_type_inner = coin_type.clone();
                    let inner_fut = inner_action(refiller.clone(), address);
                    async move {
                        if sui_client
                            .coin_read_api()
                            .get_coins_stream(address, coin_type_inner)
                            .filter(|coin| future::ready(coin.balance >= min_coin_value))
                            .take(min_num_coins)
                            .collect::<Vec<_>>()
                            .await
                            .len()
                            < min_num_coins
                        {
                            inner_fut.await
                        } else {
                            Ok(())
                        }
                    }
                }))
                .await
                .inspect_err(|error| {
                    tracing::error!(
                        ?error,
                        "error during periodic refill of coin type {:?}",
                        coin_type
                    )
                });
            }
        })
    }

    /// The WAL coin type.
    pub fn coin_type(&self) -> String {
        format!("{}::wal::WAL", self.system_pkg_id)
    }
}

impl<G: CoinRefill> CoinRefill for Refiller<G> {
    async fn send_gas_request(&self, address: SuiAddress) -> Result<()> {
        self.refill_inner.send_gas_request(address).await
    }

    async fn send_wal_request(&self, address: SuiAddress) -> Result<()> {
        self.refill_inner.send_wal_request(address).await
    }
}

/// Helper struct to hold the handles for the refiller tasks.
#[derive(Debug)]
pub struct RefillHandles {
    /// The handle for the gas refill task.
    pub _gas_refill_handle: JoinHandle<anyhow::Result<()>>,
    /// The handle for the WAL refill task.
    pub _wal_refill_handle: JoinHandle<anyhow::Result<()>>,
}
