// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A client mulitplexer, that allows to submit requests using multiple clients in the background.

use std::{
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use prometheus::Registry;
use sui_sdk::{
    sui_client_config::SuiEnv,
    types::base_types::SuiAddress,
    wallet_context::WalletContext,
};
use walrus_core::{BlobId, EpochCount};
use walrus_sui::{
    client::{BlobPersistence, SuiContractClient, SuiReadClient},
    utils::create_wallet,
};

use super::{
    cli::PublisherArgs,
    daemon::{WalrusReadClient, WalrusWriteClient},
    metrics::ClientMetrics,
    refill::{RefillHandles, Refiller},
    responses::BlobStoreResult,
    Client,
    ClientResult,
    StoreWhen,
};
use crate::client::{refill::should_refill, Config};

pub struct ClientMultiplexer {
    client_pool: WriteClientPool,
    read_client: Client<SuiReadClient>,
    _refill_handles: RefillHandles,
}

impl ClientMultiplexer {
    pub async fn new(
        wallet: WalletContext,
        config: &Config,
        gas_budget: u64,
        prometheus_registry: &Registry,
        args: &PublisherArgs,
    ) -> anyhow::Result<Self> {
        let sui_env = wallet.config.get_active_env()?.clone();
        let contract_client = config.new_contract_client(wallet, gas_budget).await?;

        let sui_client = contract_client.sui_client().clone();
        let sui_read_client = contract_client.read_client.clone();
        let read_client = Client::new_read_client(config.clone(), sui_read_client).await?;

        let refiller = Refiller::new(
            contract_client,
            args.gas_refill_amount,
            args.wal_refill_amount,
            args.sub_wallets_min_balance,
        );

        let client_pool = WriteClientPool::new(
            args.n_clients,
            config,
            sui_env,
            gas_budget,
            &args.sub_wallets_dir,
            &refiller,
            args.sub_wallets_min_balance,
        )
        .await?;

        let metrics = Arc::new(ClientMetrics::new(prometheus_registry));
        let refill_handles = refiller.refill_gas_and_wal(
            client_pool.addresses(),
            args.refill_interval,
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
    pool: Vec<Arc<Client<SuiContractClient>>>,
    cur_idx: AtomicUsize,
}

impl WriteClientPool {
    /// Creates a new client pool with `n_client`, based on the given `config` and `sui_env`.
    pub async fn new(
        n_clients: usize,
        config: &Config,
        sui_env: SuiEnv,
        gas_budget: u64,
        sub_wallets_dir: &Path,
        refiller: &Refiller,
        min_balance: u64,
    ) -> anyhow::Result<Self> {
        tracing::info!(%n_clients, "creating write client pool");
        let pool = SubClientLoader::new(
            config,
            sub_wallets_dir,
            sui_env,
            gas_budget,
            refiller,
            min_balance,
        )
        .create_or_load_sub_clients(n_clients)
        .await?;

        Ok(Self {
            pool,
            cur_idx: AtomicUsize::new(0),
        })
    }

    /// Returns the addresses of the clients in the pool.
    pub fn addresses(&self) -> Vec<SuiAddress> {
        self.pool
            .iter()
            .map(|client| client.sui_client().address())
            .collect()
    }

    /// Returns the next client in the pool.
    pub async fn next_client(&self) -> Arc<Client<SuiContractClient>> {
        let cur_idx = self.cur_idx.fetch_add(1, Ordering::Relaxed) % self.pool.len();

        let client = self
            .pool
            .get(cur_idx)
            .expect("the index is computed modulo the length and clients cannot be removed")
            .clone();

        client
    }
}

/// Helper struct to build or load sub clients for the client multiplexer.
struct SubClientLoader<'a> {
    config: &'a Config,
    sub_wallets_dir: &'a Path,
    sui_env: SuiEnv,
    gas_budget: u64,
    refiller: &'a Refiller,
    /// The minimum balance the sub-wallets should have, below which they are refilled at startup.
    min_balance: u64,
}

impl<'a> SubClientLoader<'a> {
    fn new(
        config: &'a Config,
        sub_wallets_dir: &'a Path,
        sui_env: SuiEnv,
        gas_budget: u64,
        refiller: &'a Refiller,
        min_balance: u64,
    ) -> Self {
        Self {
            config,
            sub_wallets_dir,
            sui_env,
            gas_budget,
            refiller,
            min_balance,
        }
    }

    async fn create_or_load_sub_clients(
        &self,
        n_clients: usize,
    ) -> anyhow::Result<Vec<Arc<Client<SuiContractClient>>>> {
        let mut clients = Vec::with_capacity(n_clients);
        for idx in 0..n_clients {
            let client = self.create_or_load_sub_client(idx).await?;
            clients.push(Arc::new(client));
        }

        Ok(clients)
    }

    /// Crates or loads a Walrus write client for the multiplexer, with the specified index.
    async fn create_or_load_sub_client(
        &self,
        sub_wallet_idx: usize,
    ) -> anyhow::Result<Client<SuiContractClient>> {
        let mut wallet = self.create_or_load_sub_wallet(sub_wallet_idx)?;
        self.top_up_if_necessary(&mut wallet, self.min_balance)
            .await?;

        let sui_client = self
            .config
            .new_contract_client(wallet, self.gas_budget)
            .await?;
        // Merge existing coins to avoid fragmentation.
        sui_client.merge_coins().await?;

        Ok(Client::new_contract_client(self.config.clone(), sui_client).await?)
    }

    /// Creates or loads a new wallet to use with the multiplexer.
    ///
    /// The function looks for a wallet configuration file in the given `sub_wallets_dir`, with the
    /// name `sui_client_<sub_wallet_idx>.yaml`. If the file exists, it loads the wallet from the
    /// file. Otherwise, it creates a new wallet and saves it to the file.
    ///
    /// The corresponding keystore files are named `sui_<sub_wallet_idx>.keystore`.
    fn create_or_load_sub_wallet(&self, sub_wallet_idx: usize) -> anyhow::Result<WalletContext> {
        let wallet_config_path = self
            .sub_wallets_dir
            .join(format!("sui_client_{}.yaml", sub_wallet_idx));
        let keystore_filename = format!("sui_{}.keystore", sub_wallet_idx);

        if wallet_config_path.exists() {
            tracing::debug!(?wallet_config_path, "loading sub-wallet from file");
            WalletContext::new(&wallet_config_path, None, None)
        } else {
            tracing::debug!(?wallet_config_path, "creating new sub-wallet");
            create_wallet(
                &wallet_config_path,
                self.sui_env.clone(),
                Some(&keystore_filename),
            )
        }
    }

    /// Ensures the wallet has at least 1 coin of at least`min_balance` SUI and WAL.
    async fn top_up_if_necessary(
        &self,
        wallet: &'a mut WalletContext,
        min_balance: u64,
    ) -> anyhow::Result<()> {
        let wal_coin_type = self.refiller.wal_coin_type();
        let address = wallet.active_address()?;
        tracing::debug!(%address, "refilling sub-wallet with SUI and WAL");
        let sui_client = wallet.get_client().await?;

        if should_refill(&sui_client, address, None, min_balance).await {
            self.refiller.send_gas_request(address).await?;
        } else {
            tracing::debug!(%address, "sub-wallet has enough SUI, skipping refill");
        }

        if should_refill(&sui_client, address, Some(wal_coin_type), min_balance).await {
            self.refiller.send_wal_request(address).await?;
        } else {
            tracing::debug!(%address, "sub-wallet has enough WAL, skipping refill");
        }

        Ok(())
    }
}
