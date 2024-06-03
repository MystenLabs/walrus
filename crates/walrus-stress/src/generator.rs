// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Generate transactions for the stress test.

use std::{sync::Arc, time::Duration};

use rand::{rngs::StdRng, Rng, SeedableRng};
use tokio::{
    sync::{
        mpsc::{channel, Receiver, Sender},
        Notify,
    },
    task::JoinHandle,
    time::sleep,
};
use walrus_core::{encoding::SliverPair, metadata::VerifiedBlobMetadataWithId};
use walrus_service::{
    cli_utils::load_wallet_context,
    client::{Client, Config},
};
use walrus_sui::{client::SuiContractClient, utils::SuiNetwork};

use crate::StressParameters;

/// Create a random blob of a given size.
pub fn create_random_blob(rng: &mut StdRng, blob_size: usize) -> Vec<u8> {
    (0..blob_size).map(|_| rng.gen::<u8>()).collect()
}

/// Create a simple Walrus client. This client does not interacts with the chain but simply reads
/// and writes blobs. The walrus nodes are expected to be running without checking whether the
/// metadata associated with the blobs are stored on the chain.
pub async fn create_walrus_client(
    config: Config,
    stress_parameters: &StressParameters,
) -> anyhow::Result<Client<SuiContractClient>> {
    let wallet_path = config.wallet_config.clone();
    let wallet = load_wallet_context(&wallet_path)?;
    let sui_client = SuiContractClient::new(
        wallet,
        config.system_pkg,
        config.system_object,
        stress_parameters.gas_budget,
    )
    .await?;
    let client = Client::new(config, sui_client).await?;
    Ok(client)
}

/// Reserve a blob on the chain.
pub async fn reserve_blob(
    config: Config,
    stress_parameters: &StressParameters,
    _sui_network: SuiNetwork,
    rng: &mut StdRng,
) -> anyhow::Result<(
    Client<SuiContractClient>,
    Vec<SliverPair>,
    VerifiedBlobMetadataWithId,
)> {
    let client = create_walrus_client(config, stress_parameters).await?;

    // Gather extra coins from the faucet.
    // let wallet_context = client.sui_client().wallet();
    // let client_address = wallet_context.active_address()?;
    // let sui_client = wallet_context.get_client().await?;
    // {
    //     let mut faucet_requests = Vec::with_capacity(FAUCET_REQUESTS);
    //     for _ in 0..FAUCET_REQUESTS {
    //         let request = request_sui_from_faucet(client_address, sui_network, &sui_client);
    //         faucet_requests.push(request);
    //     }
    //     try_join_all(faucet_requests).await?;
    // }

    // Encode a blob.
    let blob_size = stress_parameters.blob_size;
    let blob = create_random_blob(rng, blob_size);
    let (pairs, metadata) = client
        .encoding_config()
        .get_blob_encoder(&blob)?
        .encode_with_metadata();

    // Pay for the blob registration.
    let epochs_ahead = 1;
    let _blob_sui_object = client
        .reserve_blob(&metadata, blob.len(), epochs_ahead)
        .await?;

    // TODO: Detect when the blob is stored by querying the storage nodes.
    sleep(Duration::from_secs(1)).await;

    Ok((client, pairs, metadata))
}

/// Make dumb (but valid) write transactions.
#[derive(Debug)]
pub struct WriteTransactionGenerator {
    notify: Arc<Notify>,
    _tx_transaction: Sender<(
        Client<SuiContractClient>,
        Vec<SliverPair>,
        VerifiedBlobMetadataWithId,
    )>,
    rx_transaction: Receiver<(
        Client<SuiContractClient>,
        Vec<SliverPair>,
        VerifiedBlobMetadataWithId,
    )>,
    _handler: JoinHandle<anyhow::Result<()>>,
}

impl WriteTransactionGenerator {
    /// Start the write transaction generator.
    pub async fn start(
        config: Config,
        stress_parameters: StressParameters,
        sui_network: SuiNetwork,
        pre_generation: usize,
    ) -> anyhow::Result<Self> {
        let (tx_transaction, rx_transaction) = channel(pre_generation * 10 + 1);
        let notify = Arc::new(Notify::new());
        let cloned_notify = notify.clone();

        let mut rng = StdRng::from_entropy();

        // Generate new transactions in the background.
        let sender = tx_transaction.clone();
        let handler = tokio::spawn(async move {
            let mut i = 0;
            loop {
                if i % pre_generation == 0 {
                    tracing::debug!("Generated {i} tx");
                }

                // Generate a new transaction.
                let (client, pairs, metadata) =
                    reserve_blob(config.clone(), &stress_parameters, sui_network, &mut rng).await?;

                i += 1;
                if i == pre_generation {
                    cloned_notify.notify_one();
                }

                // This call blocks when the channel is full.
                sender
                    .send((client, pairs, metadata))
                    .await
                    .expect("Failed to send tx");
                tokio::task::yield_now().await;
            }
        });

        Ok(Self {
            notify,
            _tx_transaction: tx_transaction,
            rx_transaction,
            _handler: handler,
        })
    }

    /// Get a new transaction.
    pub async fn make_tx(
        &mut self,
    ) -> (
        Client<SuiContractClient>,
        Vec<SliverPair>,
        VerifiedBlobMetadataWithId,
    ) {
        self.rx_transaction
            .recv()
            .await
            .expect("Failed to get new write tx")
    }

    /// Wait for the generator to finish pre-generating transactions.
    pub async fn initialize(&self) {
        self.notify.notified().await;
    }
}

// Make dumb (but valid) read transactions.
#[derive(Debug)]
pub struct ReadTransactionGenerator {
    notify: Arc<Notify>,
    _tx_transaction: Sender<Client<SuiContractClient>>,
    rx_transaction: Receiver<Client<SuiContractClient>>,
    _handler: JoinHandle<anyhow::Result<()>>,
}

impl ReadTransactionGenerator {
    /// Start the write transaction generator.
    pub async fn start(
        config: Config,
        stress_parameters: StressParameters,
        pre_generation: usize,
    ) -> anyhow::Result<Self> {
        let (tx_transaction, rx_transaction) = channel(pre_generation * 10 + 1);
        let notify = Arc::new(Notify::new());
        let cloned_notify = notify.clone();

        // Generate new transactions in the background.
        let sender = tx_transaction.clone();
        let handler = tokio::spawn(async move {
            let mut i = 0;
            loop {
                if i % pre_generation == 0 {
                    tracing::debug!("Generated {i} tx");
                }

                // Generate a new transaction.
                let client = create_walrus_client(config.clone(), &stress_parameters).await?;

                i += 1;
                if i == pre_generation {
                    cloned_notify.notify_one();
                }

                // This call blocks when the channel is full.
                sender.send(client).await.expect("Failed to send tx");
                tokio::task::yield_now().await;
            }
        });

        Ok(Self {
            notify,
            _tx_transaction: tx_transaction,
            rx_transaction,
            _handler: handler,
        })
    }

    /// Get a new transaction.
    pub async fn make_tx(&mut self) -> Client<SuiContractClient> {
        self.rx_transaction
            .recv()
            .await
            .expect("Failed to get new write tx")
    }

    /// Wait for the generator to finish pre-generating transactions.
    pub async fn initialize(&self) {
        self.notify.notified().await;
    }
}
