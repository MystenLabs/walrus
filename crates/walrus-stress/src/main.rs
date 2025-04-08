// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Load generators for stress testing the Walrus nodes.

use std::{
    collections::{HashMap, HashSet},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::{NonZeroU64, NonZeroUsize},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use clap::{Parser, Subcommand};
use generator::blob::WriteBlobConfig;
use rand::{seq::SliceRandom, RngCore};
use sui_types::base_types::ObjectID;
use walrus_service::{
    client::{metrics::ClientMetrics, Config, Refiller},
    utils::load_from_yaml,
};
use walrus_sui::{
    client::{CoinType, ReadClient, SuiContractClient, MIN_STAKING_THRESHOLD},
    config::WalletConfig,
    types::StorageNode,
    utils::SuiNetwork,
};

use crate::generator::LoadGenerator;

mod generator;

/// The amount of gas or MIST to refill each time.
const COIN_REFILL_AMOUNT: u64 = 500_000_000;
/// The minimum balance to keep in the wallet.
const MIN_BALANCE: u64 = 1_000_000_000;

#[derive(Parser)]
#[clap(rename_all = "kebab-case")]
#[clap(name = env!("CARGO_BIN_NAME"))]
#[derive(Debug)]
struct Args {
    /// The path to the client configuration file containing the system object address.
    #[clap(long, default_value = "./working_dir/client_config.yaml")]
    config_path: PathBuf,

    /// Sui network for which the config is generated.
    #[clap(long, default_value = "testnet")]
    sui_network: SuiNetwork,

    /// The port on which metrics are exposed.
    #[clap(long, default_value = "9584")]
    metrics_port: u16,

    /// The path to the Sui Wallet to be used for funding the gas.
    ///
    /// If specified, the funds to run the stress client will be taken from this wallet. Otherwise,
    /// the stress client will try to use the faucet.
    #[clap(long)]
    wallet_path: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug, Clone)]
#[clap(rename_all = "kebab-case")]
enum Commands {
    /// Register nodes based on parameters exported by the `walrus-node setup` command, send the
    /// storage-node capability to the respective node's wallet, and optionally stake with them.
    Stress(StressArgs),
    /// Deploy the Walrus system contract on the Sui network.
    Staking,
}

#[derive(Parser, Debug, Clone)]
#[clap(rename_all = "kebab-case")]
#[command(author, version, about = "Walrus stress load generator", long_about = None)]
struct StressArgs {
    /// The target write load to submit to the system (writes/minute).
    /// The actual load may be limited by the number of clients.
    /// If the write load is 0, a single write will be performed to enable reads.
    #[clap(long, default_value_t = 60)]
    write_load: u64,
    /// The minimum duration of epoch (inclusive) to store the blob for.
    #[clap(long, default_value_t = 1)]
    min_epochs_to_store: u32,
    /// The maximum duration of epoch (inclusive) to store the blob for.
    #[clap(long, default_value_t = 10)]
    max_epochs_to_store: u32,
    /// The target read load to submit to the system (reads/minute).
    /// The actual load may be limited by the number of clients.
    #[clap(long, default_value_t = 60)]
    read_load: u64,
    /// The number of clients to use for the load generation for reads and writes.
    #[clap(long, default_value = "10")]
    n_clients: NonZeroUsize,
    /// The binary logarithm of the minimum blob size to use for the load generation.
    ///
    /// Blobs sizes are uniformly distributed across the powers of two between
    /// this and the maximum blob size.
    #[clap(long, default_value = "10")]
    min_size_log2: u8,
    /// The binary logarithm of the maximum blob size to use for the load generation.
    #[clap(long, default_value = "20")]
    max_size_log2: u8,
    /// The period in milliseconds to check if gas needs to be refilled.
    ///
    /// This is useful for continuous load testing where the gas budget need to be refilled
    /// periodically.
    #[clap(long, default_value = "1000")]
    gas_refill_period_millis: NonZeroU64,
    /// The fraction of writes that write inconsistent blobs.
    #[clap(long, default_value_t = 0.0)]
    inconsistent_blob_rate: f64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let _ = tracing_subscriber::fmt::try_init();
    let mut config: Config =
        load_from_yaml(args.config_path).context("Failed to load client config")?;

    // Start the metrics server.
    let metrics_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), args.metrics_port);
    let registry_service = mysten_metrics::start_prometheus_server(metrics_address);
    let prometheus_registry = registry_service.default_registry();
    let metrics = Arc::new(ClientMetrics::new(&walrus_utils::metrics::Registry::new(
        prometheus_registry,
    )));
    tracing::info!("starting metrics server on {metrics_address}");

    if let Some(wallet_path) = &args.wallet_path {
        tracing::info!(
            "overriding wallet configuration from '{}'",
            wallet_path.display()
        );
        config.wallet_config = Some(WalletConfig::from_path(wallet_path));
    }

    match args.command {
        Commands::Stress(stress_args) => {
            run_stress(config, metrics, args.sui_network, stress_args).await
        }
        Commands::Staking => run_staking(config, metrics).await,
    }
}

async fn run_stress(
    config: Config,
    metrics: Arc<ClientMetrics>,
    sui_network: SuiNetwork,
    args: StressArgs,
) -> anyhow::Result<()> {
    let n_clients = args.n_clients.get();

    // Start the write transaction generator.
    let gas_refill_period = Duration::from_millis(args.gas_refill_period_millis.get());
    let wallet = WalletConfig::load_wallet_context(config.wallet_config.as_ref())
        .context("Failed to load wallet context")?;
    let contract_client = config.new_contract_client(wallet, None).await?;

    let wal_balance = contract_client.balance(CoinType::Wal).await?;
    let sui_balance = contract_client.balance(CoinType::Sui).await?;
    tracing::info!(wal_balance, sui_balance, "initial balances");

    let refiller = Refiller::new(
        contract_client,
        COIN_REFILL_AMOUNT,
        COIN_REFILL_AMOUNT,
        MIN_BALANCE,
    );
    let blob_config = WriteBlobConfig::new(
        args.min_size_log2,
        args.max_size_log2,
        args.min_epochs_to_store,
        args.max_epochs_to_store,
    );
    let mut load_generator = LoadGenerator::new(
        n_clients,
        blob_config,
        config,
        sui_network,
        gas_refill_period,
        metrics,
        refiller,
    )
    .await?;

    load_generator
        .start(args.write_load, args.read_load, args.inconsistent_blob_rate)
        .await?;
    Ok(())
}

#[derive(Debug)]
enum StakingModeAtEpoch {
    Stake(u32),
    RequestWithdrawal(u32),
    Withdraw(u32),
}

async fn run_staking(config: Config, _metrics: Arc<ClientMetrics>) -> anyhow::Result<()> {
    tracing::info!("Starting the staking stress runner.");
    // Start the re-staking machine.
    let restaking_period = Duration::from_secs(15);
    let wallet = WalletConfig::load_wallet_context(config.wallet_config.as_ref())
        .context("Failed to load wallet context")?;
    let contract_client: SuiContractClient = config.new_contract_client(wallet, None).await?;

    // The Staked Wal at any given time.
    let mut wal_staked: HashSet<ObjectID> = Default::default();

    let mut mode = StakingModeAtEpoch::Stake(rand::thread_rng().next_u32() % 2);
    loop {
        tokio::time::sleep(restaking_period).await;
        let mut committee = contract_client.read_client().current_committee().await?;
        let current_epoch = committee.epoch;
        let wal_balance = contract_client.balance(CoinType::Wal).await?;
        let sui_balance = contract_client.balance(CoinType::Sui).await?;
        tracing::info!(current_epoch, ?mode, wal_balance, sui_balance, "woke up");

        match mode {
            StakingModeAtEpoch::Stake(epoch) => {
                assert!(wal_staked.is_empty());
                if epoch <= current_epoch {
                    // Get the current committee. (Consider also staking on the next committee?)
                    let mut nodes: Vec<StorageNode> = committee.members().to_vec();

                    // Shuffle the nodes to determine which ones get preferential staking
                    // treatment.
                    nodes.shuffle(&mut rand::thread_rng());

                    // Eliminate 1/3 of the nodes to increase shard movement.
                    nodes.truncate(std::cmp::max(1, nodes.len() * 2 / 3));

                    // Allocate half the WAL to various nodes. This is a linear walk over all of the
                    // WAL which we're going to stake, just to simplify the algorithm. Each "stake"
                    // below is one unit of MIN_STAKING_THRESHOLD.
                    let available_stakes = (wal_balance / MIN_STAKING_THRESHOLD) / 2;
                    let mut node_allocations = HashMap::<ObjectID, u64>::new();
                    for i in 0..available_stakes {
                        // Loop through the nodes in shuffled order until we've allocated all our
                        // stake.
                        node_allocations
                            .entry(nodes[i as usize % nodes.len()].node_id)
                            .and_modify(|x| *x += MIN_STAKING_THRESHOLD)
                            .or_insert(MIN_STAKING_THRESHOLD);
                    }
                    tracing::info!(current_epoch, ?node_allocations, "staking with allocations");
                    let node_ids_with_amounts: Vec<(ObjectID, u64)> =
                        node_allocations.into_iter().collect();
                    let staked_wals = contract_client
                        .stake_with_pools(&node_ids_with_amounts)
                        .await?;
                    // Save the list of staked WALs so we can withdraw them later.
                    wal_staked.extend(staked_wals.into_iter().map(|x| x.id));

                    // Re-read the current epoch to avoid a race condition.
                    committee = contract_client.read_client().current_committee().await?;
                    // Some time after we've staked, we should schedule a withdrawal.
                    mode = StakingModeAtEpoch::RequestWithdrawal(
                        committee.epoch + 1 + (rand::thread_rng().next_u32() % 3),
                    );
                }
            }
            StakingModeAtEpoch::RequestWithdrawal(staked_at_epoch) => {
                assert!(!wal_staked.is_empty());
                if staked_at_epoch < current_epoch {
                    // Request Withdrawal for any Wal we had staked.
                    for &staked_wal_id in wal_staked.iter() {
                        // Request to withdraw our WAL.
                        contract_client
                            .request_withdraw_stake(staked_wal_id)
                            .await?;
                    }
                    tracing::info!(
                        current_epoch,
                        ?wal_staked,
                        "requested withdrawal of staking allocations"
                    );

                    // Re-read the current epoch to avoid a race condition.
                    committee = contract_client.read_client().current_committee().await?;
                    // After we've scheduled a withdrawal, let's do a real withdrawal.
                    mode = StakingModeAtEpoch::Withdraw(committee.epoch + 1)
                }
            }
            StakingModeAtEpoch::Withdraw(epoch) => {
                if epoch <= current_epoch {
                    tracing::info!(
                        current_epoch,
                        ?wal_staked,
                        "withdrawing staking allocations"
                    );

                    // Empty the current map so we can start our staking simulation anew.
                    let mut wal_staked_temp = Default::default();
                    std::mem::swap(&mut wal_staked_temp, &mut wal_staked);

                    // Unstake any Wal we had staked.
                    for staked_wal_id in wal_staked_temp {
                        // Actually withdraw our WAL.
                        contract_client.withdraw_stake(staked_wal_id).await?;
                    }

                    // Re-read the current epoch to avoid a race condition.
                    committee = contract_client.read_client().current_committee().await?;
                    // After we've withdrawn, let's schedule more staking.
                    mode = StakingModeAtEpoch::Stake(
                        committee.epoch + 1 + (rand::thread_rng().next_u32() % 2),
                    );
                }
            }
        }
    }
}
