// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! CLI tool to generate Walrus configurations and deploy testbeds.

use std::{
    fs,
    net::IpAddr,
    num::NonZeroU16,
    path::{Path, PathBuf},
};

use anyhow::Context;
use clap::{Parser, Subcommand};
use tokio::sync::oneshot;
use walrus_service::{
    node::config::defaults::{METRICS_PORT, REST_API_PORT},
    testbed,
    utils::VERSION,
};
use walrus_sui::utils::SuiNetwork;

#[derive(Parser)]
#[clap(rename_all = "kebab-case")]
#[clap(name = env!("CARGO_BIN_NAME"))]
#[clap(version = VERSION)]
#[derive(Debug)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug, Clone)]
#[clap(rename_all = "kebab-case")]
enum Commands {
    /// Deploy the Walrus system contract on the Sui network.
    DeploySystemContract(DeploySystemContractArgs),

    /// Generate the configuration files to run a testbed of storage nodes.
    GenerateDryRunConfigs(GenerateDryRunConfigsArgs),
}

#[derive(Debug, Clone, clap::Args)]
struct DeploySystemContractArgs {
    /// The directory where the storage nodes will be deployed.
    #[clap(long, default_value = "./working_dir")]
    working_dir: PathBuf,
    /// Sui network for which the config is generated.
    #[clap(long, default_value = "testnet")]
    sui_network: SuiNetwork,
    /// The directory in which the contracts are located.
    #[clap(long, default_value = "./contracts/blob_store")]
    contract_path: PathBuf,
    /// Gas budget for sui transactions to publish the contracts and set up the system.
    #[arg(long, default_value_t = 500_000_000)]
    gas_budget: u64,
    /// The total number of shards. The shards are distributed evenly among the storage nodes.
    // Todo: accept non-even shard distributions #377
    #[arg(long, default_value_t = 1000)]
    n_shards: u16,
    /// The list of hostnames or public ip addresses of the storage nodes.
    #[clap(long, value_name = "ADDR", value_delimiter = ' ', num_args(4..))]
    host_addresses: Vec<String>,
    /// The port on which the REST API of the storage nodes will listen.
    #[clap(long, default_value_t = REST_API_PORT)]
    rest_api_port: u16,
    /// The path to the configuration file of the Walrus testbed.
    /// [default: <WORKING_DIR>/testbed_config.yaml]
    #[clap(long)]
    testbed_config_path: Option<PathBuf>,
    // Note: The storage unit is set in `crates/walrus-sui/utils.rs`. Change the unit in
    // the doc comment here if it changes.
    /// The price to set per unit of storage (1 KiB) and epoch.
    #[arg(long, default_value_t = 50)]
    price_per_unit: u64,
    /// The storage capacity in bytes to deploy the system with.
    #[arg(long, default_value_t = 1_000_000_000_000)]
    storage_capacity: u64,
    /// If set, generates the protocol key pairs of the nodes deterministically.
    #[arg(long, action)]
    deterministic_keys: bool,
}

#[derive(Debug, Clone, clap::Args)]
struct GenerateDryRunConfigsArgs {
    /// The directory where the storage nodes will be deployed.
    #[clap(long, default_value = "./working_dir")]
    working_dir: PathBuf,
    /// The path to the configuration file of the Walrus testbed.
    /// [default: <WORKING_DIR>/testbed_config.yaml]
    #[clap(long)]
    testbed_config_path: Option<PathBuf>,
    /// The list of listening ip addresses of the storage nodes.
    /// If not set, defaults to the addresses or (resolved) host names set in the testbed config.
    #[clap(long, value_name = "ADDR", value_delimiter = ' ', num_args(..))]
    listening_ips: Option<Vec<IpAddr>>,
    /// The port on which the metrics server of the storage nodes will listen.
    #[clap(long, default_value_t = METRICS_PORT)]
    metrics_port: u16,
    /// Path of the directory in which the config files will be stored on deployed nodes.
    ///
    /// If specified, the working directory in the paths contained in the node, client,
    /// and wallet configs will be replaced with this directory.
    #[clap(long)]
    set_config_dir: Option<PathBuf>,
    /// Path of the node database.
    ///
    /// If specified the database path of all nodes will be set to this path, otherwise it
    /// will be located in the config directory and have the same name as the node it belongs to.
    #[clap(long)]
    set_db_path: Option<PathBuf>,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    match args.command {
        Commands::DeploySystemContract(args) => commands::deploy_system_contract(args)?,

        Commands::GenerateDryRunConfigs(args) => commands::generate_dry_run_configs(args)?,
    }
    Ok(())
}

mod commands {
    use walrus_service::{
        testbed::{
            create_client_config,
            create_storage_node_configs,
            deploy_walrus_contract,
            even_shards_allocation,
            DeployTestbedContractParameters,
            TestbedConfig,
        },
        utils::LoadConfig as _,
    };

    use super::*;

    #[tokio::main]
    pub(super) async fn deploy_system_contract(
        DeploySystemContractArgs {
            working_dir,
            sui_network,
            contract_path,
            gas_budget,
            n_shards,
            host_addresses,
            rest_api_port,
            testbed_config_path,
            price_per_unit,
            storage_capacity,
            deterministic_keys,
        }: DeploySystemContractArgs,
    ) -> anyhow::Result<()> {
        tracing_subscriber::fmt::init();

        fs::create_dir_all(&working_dir)
            .with_context(|| format!("Failed to create directory '{}'", working_dir.display()))?;
        // Turn the working directory into an absolute path.
        let working_dir = working_dir
            .canonicalize()
            .context("canonicalizing the working directory path failed")?;

        // Deploy the system contract.
        let number_of_shards = NonZeroU16::new(n_shards).context("number of shards must be > 0")?;
        let committee_size = NonZeroU16::new(host_addresses.len() as u16).unwrap();
        let shards_information = even_shards_allocation(number_of_shards, committee_size);

        let testbed_config = deploy_walrus_contract(DeployTestbedContractParameters {
            working_dir: &working_dir,
            sui_network,
            contract_path,
            gas_budget,
            shards_information,
            host_addresses,
            rest_api_port,
            storage_capacity,
            price_per_unit,
            deterministic_keys,
        })
        .await
        .context("Failed to deploy system contract")?;

        // Write the Testbed config to file.
        let serialized_testbed_config =
            serde_yaml::to_string(&testbed_config).context("Failed to serialize Testbed config")?;
        let testbed_config_path = get_testbed_config_path(testbed_config_path, &working_dir);
        fs::write(testbed_config_path, serialized_testbed_config)
            .context("Failed to write Testbed config")?;
        Ok(())
    }

    #[tokio::main]
    pub(super) async fn generate_dry_run_configs(
        GenerateDryRunConfigsArgs {
            working_dir,
            testbed_config_path,
            metrics_port,
            set_config_dir,
            listening_ips,
            set_db_path,
        }: GenerateDryRunConfigsArgs,
    ) -> anyhow::Result<()> {
        tracing_subscriber::fmt::init();

        fs::create_dir_all(&working_dir)
            .with_context(|| format!("Failed to create directory '{}'", working_dir.display()))?;
        // Turn the working directory into an absolute path.
        let working_dir = working_dir
            .canonicalize()
            .context("canonicalizing the working directory path failed")?;

        let testbed_config_path = get_testbed_config_path(testbed_config_path, &working_dir);
        let testbed_config = TestbedConfig::load(testbed_config_path)?;

        let client_config = create_client_config(
            testbed_config.system_object,
            working_dir.as_path(),
            testbed_config.sui_network,
            set_config_dir.as_deref(),
        )
        .await?;
        let serialized_client_config =
            serde_yaml::to_string(&client_config).context("Failed to serialize client configs")?;
        let client_config_path = working_dir.join("client_config.yaml");
        fs::write(client_config_path, serialized_client_config)
            .context("Failed to write client configs")?;

        let committee_size =
            NonZeroU16::new(testbed_config.nodes.len() as u16).expect("committee size must be > 0");
        let storage_node_configs = create_storage_node_configs(
            working_dir.as_path(),
            testbed_config,
            listening_ips,
            metrics_port,
            set_config_dir.as_deref(),
            set_db_path.as_deref(),
        )
        .await?;
        for (i, storage_node_config) in storage_node_configs.into_iter().enumerate() {
            let serialized_storage_node_config = serde_yaml::to_string(&storage_node_config)
                .context("Failed to serialize storage node configs")?;
            let node_config_name = format!(
                "{}.yaml",
                testbed::node_config_name_prefix(i as u16, committee_size)
            );
            let node_config_path = working_dir.join(node_config_name);
            fs::write(node_config_path, serialized_storage_node_config)
                .context("Failed to write storage node configs")?;
        }

        Ok(())
    }
}

/// Wait for SIGINT and SIGTERM (unix only).
#[tokio::main(flavor = "current_thread")]
#[tracing::instrument(skip_all)]
async fn wait_until_terminated(mut exit_listener: oneshot::Receiver<()>) {
    #[cfg(not(unix))]
    async fn wait_for_other_signals() {
        // Disables this branch in the select statement.
        std::future::pending().await
    }

    #[cfg(unix)]
    async fn wait_for_other_signals() {
        use tokio::signal::unix;

        unix::signal(unix::SignalKind::terminate())
            .expect("unable to register for SIGTERM signals")
            .recv()
            .await;
        tracing::info!("received SIGTERM")
    }

    tokio::select! {
        biased;
        _ = wait_for_other_signals() => (),
        _ = tokio::signal::ctrl_c() => tracing::info!("received SIGINT"),
        exit_or_dropped = &mut exit_listener => match exit_or_dropped {
            Err(_) => tracing::info!("exit notification sender was dropped"),
            Ok(_) => tracing::info!("exit notification received"),
        }
    }
}

fn get_testbed_config_path(
    maybe_testbed_config_path: Option<PathBuf>,
    working_dir: &Path,
) -> PathBuf {
    maybe_testbed_config_path.unwrap_or_else(|| working_dir.join("testbed_config.yaml"))
}
