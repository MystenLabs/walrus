// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A client mulitplexer, that allows to submit requests using multiple clients in the background.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use prometheus::Registry;
use sui_sdk::{
    sui_client_config::SuiEnv,
    types::base_types::SuiAddress,
    wallet_context::WalletContext,
};
use walrus_core::{BlobId, EpochCount};
use walrus_sui::{
    client::{get_system_package_id, BlobPersistence, SuiContractClient, SuiReadClient},
    test_utils::temp_dir_wallet,
};
use walrus_test_utils::WithTempDir;

use super::{
    daemon::{WalrusReadClient, WalrusWriteClient},
    metrics::ClientMetrics,
    refill::{CoinRefill, NetworkOrWallet, RefillHandles, Refiller},
    responses::BlobStoreResult,
    Client,
    ClientResult,
    StoreWhen,
};
use crate::client::Config;

pub struct ClientMultiplexer {
    client_pool: WriteClientPool,
    read_client: Client<SuiReadClient>,
    _refill_handles: RefillHandles,
}

impl ClientMultiplexer {
    pub async fn new(
        n_clients: usize,
        wallet: WalletContext,
        config: &Config,
        gas_budget: u64,
        refill_interval: Duration,
        prometheus_registry: &Registry,
    ) -> anyhow::Result<Self> {
        let sui_env = wallet.config.get_active_env()?.clone();
        let contract_client = config.new_contract_client(wallet, gas_budget).await?;

        let sui_client = contract_client.sui_client().clone();
        let sui_read_client = contract_client.read_client.clone();
        let read_client = Client::new_read_client(config.clone(), sui_read_client).await?;

        let system_pkg_id = get_system_package_id(&sui_client, config.system_object).await?;
        let refiller = Refiller::new(
            NetworkOrWallet::new_wallet(contract_client, gas_budget)?,
            system_pkg_id,
        );

        let client_pool =
            WriteClientPool::new(n_clients, config, sui_env, gas_budget, &refiller).await?;

        let metrics = Arc::new(ClientMetrics::new(prometheus_registry));
        let refill_handles = refiller.refill_gas_and_wal(
            client_pool.addresses(),
            refill_interval,
            metrics,
            sui_client,
        );

        Ok(Self {
            client_pool,
            read_client,
            _refill_handles: refill_handles,
        })
    }

    /// Submits a write request to the client pool.
    #[tracing::instrument(err, skip_all)]
    pub async fn submit_write(
        &self,
        blob: &[u8],
        epochs_ahead: EpochCount,
        store_when: StoreWhen,
        persistence: BlobPersistence,
    ) -> ClientResult<BlobStoreResult> {
        let client = self.client_pool.next_client().await;
        tracing::debug!("submitting write request to client in pool");

        let result = client
            .reserve_and_store_blob_retry_epoch(blob, epochs_ahead, store_when, persistence)
            .await?;

        Ok(result)
    }
}

impl WalrusReadClient for ClientMultiplexer {
    async fn read_blob(&self, blob_id: &BlobId) -> ClientResult<Vec<u8>> {
        WalrusReadClient::read_blob(&self.read_client, blob_id).await
    }

    fn set_metric_registry(&mut self, registry: &Registry) {
        self.read_client.set_metric_registry(registry);
    }
}

impl WalrusWriteClient for ClientMultiplexer {
    async fn write_blob(
        &self,
        blob: &[u8],
        epochs_ahead: EpochCount,
        store_when: StoreWhen,
        persistence: BlobPersistence,
    ) -> ClientResult<BlobStoreResult> {
        self.submit_write(blob, epochs_ahead, store_when, persistence)
            .await
    }
}

/// A pool of temporary write clients that are rotaated.
pub struct WriteClientPool {
    pool: Vec<Arc<TempWriteClient>>,
    cur_idx: AtomicUsize,
}

impl WriteClientPool {
    /// Creates a new client pool with `n_client`, based on the given `config` and `sui_env`.
    pub async fn new<G: CoinRefill + 'static>(
        n_clients: usize,
        config: &Config,
        sui_env: SuiEnv,
        gas_budget: u64,
        refiller: &Refiller<G>,
    ) -> anyhow::Result<Self> {
        let mut pool = Vec::with_capacity(n_clients);

        tracing::info!(%n_clients, "creating write client pool");
        for _ in 0..n_clients {
            let client = Arc::new(
                TempWriteClient::from_sui_env(config, sui_env.clone(), gas_budget, refiller)
                    .await?,
            );
            pool.push(client);
        }

        Ok(Self {
            pool,
            cur_idx: AtomicUsize::new(0),
        })
    }

    /// Returns the addresses of the clients in the pool.
    pub fn addresses(&self) -> Vec<SuiAddress> {
        self.pool
            .iter()
            .map(|client| client.as_ref().address())
            .collect()
    }

    /// Returns the next client in the pool.
    pub async fn next_client(&self) -> Arc<TempWriteClient> {
        let cur_idx = self.cur_idx.fetch_add(1, Ordering::Relaxed) % self.pool.len();

        let client = self
            .pool
            .get(cur_idx)
            .expect("the index is computed modulo the length and clients cannot be removed")
            .clone();

        client
    }
}

/// A temporary write client for client multiplexing.
#[derive(Debug)]
pub(crate) struct TempWriteClient(WithTempDir<Client<SuiContractClient>>);

impl TempWriteClient {
    /// Creates a new temporary write client.
    pub async fn from_sui_env<G: CoinRefill + 'static>(
        config: &Config,
        sui_env: SuiEnv,
        gas_budget: u64,
        refiller: &Refiller<G>,
    ) -> anyhow::Result<Self> {
        let client = new_client(config, sui_env, gas_budget, refiller).await?;
        Ok(Self(client))
    }

    /// Returns the active address of the client.
    pub fn address(&self) -> SuiAddress {
        self.0.as_ref().sui_client().address()
    }

    /// Stores the blob to Walrus, retrying if it fails because of epoch change.
    pub async fn reserve_and_store_blob_retry_epoch(
        &self,
        blob: &[u8],
        epochs_ahead: EpochCount,
        store_when: StoreWhen,
        persistence: BlobPersistence,
    ) -> ClientResult<BlobStoreResult> {
        self.0
            .as_ref()
            .reserve_and_store_blob_retry_epoch(blob, epochs_ahead, store_when, persistence)
            .await
    }
}

async fn new_client<G: CoinRefill + 'static>(
    config: &Config,
    sui_env: SuiEnv,
    gas_budget: u64,
    refiller: &Refiller<G>,
) -> anyhow::Result<WithTempDir<Client<SuiContractClient>>> {
    // Create the client with a separate wallet
    let wallet = wallet_for_testing_from_refill(sui_env, refiller).await?;
    let sui_read_client = config
        .new_read_client(wallet.as_ref().get_client().await?)
        .await?;
    let sui_contract_client = wallet.and_then(|wallet| {
        SuiContractClient::new_with_read_client(wallet, gas_budget, sui_read_client)
    })?;

    let client = sui_contract_client
        .and_then_async(|contract_client| {
            Client::new_contract_client(config.clone(), contract_client)
        })
        .await?;
    Ok(client)
}

async fn wallet_for_testing_from_refill<G: CoinRefill + 'static>(
    sui_env: SuiEnv,
    refiller: &Refiller<G>,
) -> anyhow::Result<WithTempDir<WalletContext>> {
    let mut wallet = temp_dir_wallet(sui_env)?;
    let address = wallet.as_mut().active_address()?;
    refiller.send_gas_request(address).await?;
    refiller.send_wal_request(address).await?;
    Ok(wallet)
}
