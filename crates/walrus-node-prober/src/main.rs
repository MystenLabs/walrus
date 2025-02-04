// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Validate Walrus test results

use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use walrus_service::{client::Config, utils::load_from_yaml};
use walrus_sui::client::ReadClient;

mod health_checker;

#[derive(Parser, Debug, Clone)]
#[clap(rename_all = "kebab-case")]
#[command(author, version, about = "Walrus load generator", long_about = None)]
struct Args {
    /// The path to the client configuration file containing the system object address.
    #[clap(long, default_value = "./working_dir/client_config.yaml")]
    config_path: PathBuf,
    /// The path to the log directory.
    #[clap(long, default_value = "./working_dir/logs")]
    log_dir: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let _ = tracing_subscriber::fmt::try_init();

    let config: Config =
        load_from_yaml(args.config_path).context("Failed to load client config")?;

    let contract_client = config
        .new_contract_client_with_wallet_in_config(None)
        .await?;
    let committee = contract_client.current_committee().await?;

    let mut health_checker = health_checker::HealthChecker::new(30, args.log_dir);
    for member in committee.members() {
        health_checker.add_node(member.clone()).await?;
    }
    health_checker.run().await?;

    Ok(())
}
