// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Utilities to refill gas for the stress clients.

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use futures::{StreamExt, stream};
use sui_sdk::types::base_types::SuiAddress;
use tokio::{task::JoinHandle, time::MissedTickBehavior};
use tracing::Instrument;
use walrus_sdk::node_client::metrics::ClientMetrics;
use walrus_sui::{
    client::{SuiContractClient, retry_client::RetriableSuiClient},
    coin::Coin,
};

// Keep RPC fan-out bounded, and amortize the funding wallet's transaction lock across recipients.
const BALANCE_CHECK_CONCURRENCY: usize = 32;
const REFILL_BATCH_SIZE: usize = 100;

/// Refills gas and WAL for the clients.
#[derive(Debug, Clone)]
pub struct Refiller {
    /// The inner implementation of the refiller.
    contract_client: Arc<SuiContractClient>,
    /// The amount of MIST to send at each request.
    gas_refill_size: u64,
    /// The amount of FROST to send at each request.
    wal_refill_size: u64,
    /// The minimum balance the wallet should have before refilling.
    min_balance: u64,
}

impl Refiller {
    /// Creates a new [`Refiller`] from a [`SuiContractClient`].
    pub fn new(
        contract_client: SuiContractClient,
        gas_refill_size: u64,
        wal_refill_size: u64,
        min_balance: u64,
    ) -> Self {
        Self {
            contract_client: Arc::new(contract_client),
            gas_refill_size,
            wal_refill_size,
            min_balance,
        }
    }

    /// Refills gas and WAL for the clients.
    pub fn refill_gas_and_wal(
        &self,
        addresses: Vec<SuiAddress>,
        period: Duration,
        metrics: Arc<ClientMetrics>,
        sui_client: RetriableSuiClient,
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
        sui_client: RetriableSuiClient,
    ) -> JoinHandle<anyhow::Result<()>> {
        let amount = self.gas_refill_size;
        self.periodic_refill(
            addresses,
            period,
            sui_client,
            Coin::SUI,
            self.min_balance,
            move |refiller, addresses| {
                let metrics = metrics.clone();
                async move {
                    refiller.send_sui_batch(amount, &addresses).await?;
                    tracing::info!(clients = addresses.len(), "clients gas coins refilled");
                    for _ in addresses {
                        metrics.observe_gas_refill();
                    }
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
        sui_client: RetriableSuiClient,
    ) -> JoinHandle<anyhow::Result<()>> {
        let amount = self.wal_refill_size;
        self.periodic_refill(
            addresses,
            period,
            sui_client,
            self.wal_coin_type(),
            self.min_balance,
            move |refiller, addresses| {
                let metrics = metrics.clone();
                async move {
                    refiller.send_wal_batch(amount, &addresses).await?;
                    tracing::info!(clients = addresses.len(), "clients WAL coins refilled");
                    for _ in addresses {
                        metrics.observe_wal_refill();
                    }
                    Ok(())
                }
            },
        )
    }

    fn periodic_refill<F, Fut>(
        &self,
        addresses: Vec<SuiAddress>,
        period: Duration,
        sui_client: RetriableSuiClient,
        coin_type: &str,
        min_coin_value: u64,
        inner_action: F,
    ) -> JoinHandle<anyhow::Result<()>>
    where
        F: Fn(Arc<SuiContractClient>, Vec<SuiAddress>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = anyhow::Result<()>> + Send,
    {
        let mut interval = tokio::time::interval(period);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let refiller = self.contract_client.clone();
        let coin_type = coin_type.to_owned();
        tokio::spawn(async move {
            loop {
                let span =
                    tracing::debug_span!("periodic_refill", coin_type, min_coin_value, ?period);
                async {
                    interval.tick().await;
                    let recipients: Vec<_> = stream::iter(addresses.iter().copied())
                        .map(|address| {
                            let sui_client = &sui_client;
                            let coin_type = &coin_type;
                            async move {
                                should_refill(sui_client, address, coin_type, min_coin_value)
                                    .await
                                    .then_some(address)
                            }
                        })
                        .buffer_unordered(BALANCE_CHECK_CONCURRENCY)
                        .filter_map(std::future::ready)
                        .collect()
                        .await;

                    // A failed batch must not cancel refills for the remaining wallets. The next
                    // pass checks balances again, including after an uncertain transaction result.
                    for batch in recipients.chunks(REFILL_BATCH_SIZE) {
                        if let Err(error) = inner_action(refiller.clone(), batch.to_vec()).await {
                            tracing::error!(
                                ?error,
                                clients = batch.len(),
                                "failed to refill wallets"
                            );
                        }
                    }
                }
                .instrument(span)
                .await;
            }
        })
    }

    /// The WAL coin type.
    pub fn wal_coin_type(&self) -> &str {
        self.contract_client.read_client().wal_coin_type()
    }

    /// Sends SUI to the specified address.
    pub async fn send_gas_request(&self, address: SuiAddress) -> Result<()> {
        Ok(self
            .contract_client
            .send_sui(self.gas_refill_size, address)
            .await?)
    }

    /// Sends WAL to the specified address.
    pub async fn send_wal_request(&self, address: SuiAddress) -> Result<()> {
        Ok(self
            .contract_client
            .send_wal(self.wal_refill_size, address)
            .await?)
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

/// Checks if the wallet should be refilled.
///
/// The wallet should be refilled if it has less than `MIN_BALANCE` in balance. _However_, if the
/// RPC returns an error, we assume that the wallet has enough coins and we do not try to refill
/// it, therefore returning `false`.
pub async fn should_refill(
    sui_client: &RetriableSuiClient,
    address: SuiAddress,
    coin_type: &str,
    min_balance: u64,
) -> bool {
    sui_client
        .get_total_balance(address, coin_type)
        .await
        .map(|balance| balance < min_balance)
        .inspect_err(|error| {
            tracing::debug!(
                ?error,
                %address,
                "failed checking the balance of the address"
            );
        })
        // If the returned value is an error, we assume that the RPC failed, the
        // address has enough coins, and we do not try to refill.
        .unwrap_or(false)
}
