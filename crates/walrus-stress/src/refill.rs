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
        base_types::{ObjectID, ObjectRef, SuiAddress},
        programmable_transaction_builder::ProgrammableTransactionBuilder,
    },
    SuiClient,
};
use tokio::{task::JoinHandle, time::MissedTickBehavior};
use walrus_sui::{
    client::SuiContractClient,
    utils::{send_faucet_request, SuiNetwork},
};

use crate::metrics::ClientMetrics;

// If a node has less than `MIN_NUM_COINS` without at least `MIN_COIN_VALUE`,
// we need to request additional coins from the faucet.
const MIN_COIN_VALUE: u64 = 500_000_000;
// We need at least a payment coin and a gas coin.
const MIN_NUM_COINS: usize = 2;
/// The amount in MIST that is transferred from the wallet refill account to the stress clients at
/// each request.
const WALLET_MIST_AMOUNT: u64 = 1_000_000_000;
/// The amount in FROST that is transferred from the wallet refill account to the stress clients at
/// each request.
const WALLET_FROST_AMOUNT: u64 = 5_000_000_000;

/// Trait to request gas and Wal coins for a client.
pub(crate) trait CoinRefill: Send + Sync {
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
// TODO: The WAL refill in not implemented.
pub(crate) struct NetworkCoinRefill {
    pub(crate) network: SuiNetwork,
}

impl NetworkCoinRefill {
    pub(crate) fn new(network: SuiNetwork) -> Self {
        Self { network }
    }
}

impl CoinRefill for NetworkCoinRefill {
    async fn send_gas_request(&self, address: SuiAddress) -> Result<()> {
        send_faucet_request(address, &self.network).await
    }

    async fn send_wal_request(&self, _address: SuiAddress) -> Result<()> {
        unimplemented!("WAL refill is not implemented for the network coin refill (#1015)")
    }
}

/// The `CoinRefill` implementation for a Sui wallet.
///
/// The wallet sends gas and WAL to the specified address.
pub(crate) struct WalletCoinRefill {
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

    pub(crate) fn new(
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
        tracing::debug!("Sending gas to address: {:?}", &address);
        let mut pt_builder = ProgrammableTransactionBuilder::new();
        pt_builder.pay_sui(vec![address], vec![self.gas_refill_size])?;
        self.sui_client
            .sign_and_send_ptb(
                pt_builder.finish(),
                Some(self.gas_budget + self.gas_refill_size),
            )
            .await?;
        Ok(())
    }

    async fn send_wal(&self, address: SuiAddress) -> Result<()> {
        tracing::debug!("Sending wal to address: {:?}", &address);
        let mut pt_builder = ProgrammableTransactionBuilder::new();
        let wal_coin: ObjectRef = self
            .sui_client
            .get_wal_coin(self.wal_refill_size)
            .await?
            .object_ref();
        pt_builder.pay(vec![wal_coin], vec![address], vec![self.wal_refill_size])?;
        self.sui_client
            .sign_and_send_ptb(pt_builder.finish(), None)
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

pub(crate) enum NetworkOrWallet {
    Network(NetworkCoinRefill),
    Wallet(WalletCoinRefill),
}

impl NetworkOrWallet {
    pub(crate) async fn new(
        system_object: ObjectID,
        staking_object: ObjectID,
        sui_network: SuiNetwork,
        wallet_path: Option<PathBuf>,
        gas_budget: u64,
    ) -> Result<Self> {
        if let Some(wallet_path) = wallet_path {
            tracing::info!(
                "Creating gas refill station from wallet: {:?}",
                &wallet_path
            );
            let wallet = walrus_service::utils::load_wallet_context(&Some(wallet_path))?;
            let sui_client =
                SuiContractClient::new(wallet, system_object, staking_object, gas_budget).await?;
            Ok(Self::new_wallet(sui_client, gas_budget)?)
        } else {
            tracing::info!("Created gas refill station from faucet: {:?}", &sui_network);
            Ok(Self::new_faucet(sui_network))
        }
    }

    pub(crate) fn new_faucet(network: SuiNetwork) -> Self {
        Self::Network(NetworkCoinRefill::new(network))
    }

    pub(crate) fn new_wallet(sui_client: SuiContractClient, gas_budget: u64) -> Result<Self> {
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
pub(crate) struct Refiller<G> {
    pub(crate) refill_inner: Arc<G>,
    pub(crate) system_pkg_id: ObjectID,
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
    pub(crate) fn new(gas_refill: G, system_pkg_id: ObjectID) -> Self {
        Self {
            refill_inner: Arc::new(gas_refill),
            system_pkg_id,
        }
    }

    pub(crate) fn refill_gas_and_wal(
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

    pub(crate) fn refill_gas(
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
                    tracing::debug!("Clients gas coins refilled");
                    metrics.observe_gas_refill();
                    Ok(())
                }
            },
        )
    }

    pub(crate) fn refill_wal(
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
                    tracing::debug!("Clients wal coins refilled");
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
                .inspect_err(|e| {
                    tracing::error!(
                        "error during periodic refill of coin type {:?}: {e}",
                        coin_type
                    )
                });
            }
        })
    }

    pub(crate) fn coin_type(&self) -> String {
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

#[derive(Debug)]
pub(crate) struct RefillHandles {
    pub(crate) _gas_refill_handle: JoinHandle<anyhow::Result<()>>,
    pub(crate) _wal_refill_handle: JoinHandle<anyhow::Result<()>>,
}
