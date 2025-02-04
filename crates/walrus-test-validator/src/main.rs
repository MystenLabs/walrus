// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Validate Walrus test results

use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use walrus_service::{
    client::Config,
    utils::{load_from_yaml, load_wallet_context},
};
use walrus_sui::{client::ReadClient, utils::SuiNetwork};

mod health_checker;

#[derive(Parser, Debug, Clone)]
#[clap(rename_all = "kebab-case")]
#[command(author, version, about = "Walrus load generator", long_about = None)]
struct Args {
    /// The path to the client configuration file containing the system object address.
    #[clap(long, default_value = "./working_dir/client_config.yaml")]
    config_path: PathBuf,
    /// Sui network for which the config is generated.
    #[clap(long, default_value = "testnet")]
    sui_network: SuiNetwork,
    /// The path to the Sui Wallet to be used for funding the gas.
    ///
    /// If specified, the funds to run the stress client will be taken from this wallet. Otherwise,
    /// the stress client will try to use the faucet.
    #[clap(long)]
    wallet_path: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let _ = tracing_subscriber::fmt::try_init();

    let config: Config =
        load_from_yaml(args.config_path).context("Failed to load client config")?;

    let wallet = load_wallet_context(&args.wallet_path)?;
    let contract_client = config.new_contract_client(wallet, None).await?;
    let committee = contract_client.current_committee().await?;
    println!("Committee: {:?}", committee);

    let mut health_checker =
        health_checker::HealthChecker::new(30, PathBuf::from("health_check.log"));
    for member in committee.members() {
        health_checker.add_node(member.clone()).await?;
    }
    health_checker.run().await?;

    Ok(())
}
