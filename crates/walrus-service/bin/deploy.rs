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
use humantime::Duration;
use walrus_core::EpochCount;
use walrus_service::{
    node::config::{
        self,
        defaults::{METRICS_PORT, REST_API_PORT},
    },
    testbed,
    utils::version,
};
use walrus_sui::utils::SuiNetwork;

const VERSION: &str = version!();
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
    /// Register nodes based on parameters exported by the `walrus-node setup` command, send the
    /// storage-node capability to the respective node's wallet, and optionally stake with them.
    RegisterNodes(RegisterNodesArgs),
    /// Deploy the Walrus system contract on the Sui network.
    DeploySystemContract(DeploySystemContractArgs),
    /// Generate the configuration files to run a testbed of storage nodes.
    GenerateDryRunConfigs(GenerateDryRunConfigsArgs),
}

#[derive(Debug, Clone, clap::Args)]
#[clap(rename_all = "kebab-case")]
struct RegisterNodesArgs {
    /// The path to the client config.
    #[clap(long)]
    client_config: PathBuf,
    /// The files containing the registration parameters exported by the `walrus-node setup`
    /// command.
    #[clap(long, alias("files"), num_args(1..))]
    param_files: Vec<PathBuf>,
    /// The (optional) amount of WAL to stake with the newly registered nodes.
    ///
    /// The stake amount is staked with all nodes.
    // For simplicity and to prevent mistakes, we only allow a single stake amount here. Different
    // stake amounts are supported with the `walrus stake` command.
    #[clap(long)]
    stake_amount: Option<u64>,
    /// Gas budget for Sui transactions to register the nodes.
    #[arg(long)]
    gas_budget: Option<u64>,
}

#[derive(Debug, Clone, clap::Args)]
struct DeploySystemContractArgs {
    /// The directory where the storage nodes will be deployed.
    #[clap(long, default_value = "./working_dir")]
    working_dir: PathBuf,
    /// Sui network for which the config is generated.
    ///
    /// Available options are `devnet`, `testnet`, and `localnet`, or a custom Sui network. To
    /// specify a custom Sui network, pass a string of the format `<RPC_URL>;<FAUCET_URL>`.
    #[clap(long, default_value = "testnet")]
    sui_network: SuiNetwork,
    /// The directory in which the contracts are located.
    #[clap(long, default_value = "./contracts")]
    contract_dir: PathBuf,
    /// Gas budget for sui transactions to publish the contracts and set up the system.
    ///
    /// If not specified, the gas budget is estimated automatically.
    #[arg(long)]
    gas_budget: Option<u64>,
    /// The total number of shards. The shards are distributed evenly among the storage nodes.
    #[arg(long, default_value = "1000")]
    n_shards: NonZeroU16,
    /// The epoch duration.
    #[arg(long, default_value = "1h")]
    epoch_duration: Duration,
    /// The duration of epoch 0.
    #[arg(long, default_value = "0s")]
    epoch_zero_duration: Duration,
    /// The list of host names or public IP addresses of the storage nodes.
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
    /// The price in FROST to set per unit of storage (1 MiB) per epoch.
    #[arg(long, default_value_t = config::defaults::storage_price())]
    storage_price: u64,
    /// The price in FROST to set for writing one unit of storage (1 MiB).
    #[arg(long, default_value_t = config::defaults::write_price())]
    write_price: u64,
    /// The storage capacity in bytes to deploy the system with.
    #[arg(long, default_value_t = 1_000_000_000_000)]
    storage_capacity: u64,
    /// If set, generates the protocol key pairs of the nodes deterministically.
    #[arg(long, action)]
    deterministic_keys: bool,
    /// The maximum number of epochs ahead for which storage can be obtained.
    #[arg(long, default_value_t = 104)]
    max_epochs_ahead: EpochCount,
    /// The path to the admin wallet. If not provided, a new wallet is created.
    #[clap(long)]
    admin_wallet_path: Option<PathBuf>,
    /// If not set, contracts are copied to `working_dir` and published from there to keep the
    /// `Move.toml` unchanged. Use this flag to publish from the original directory and update
    /// the `Move.toml` to point to the new contracts.
    #[arg(long, action)]
    do_not_copy_contracts: bool,
    /// If set, creates a WAL exchange.
    #[arg(long, action)]
    with_wal_exchange: bool,
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
    /// The list of listening IP addresses of the storage nodes.
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
    /// Cooldown duration for the faucet.
    ///
    /// Setting this makes sure that we wait at least this duration after a faucet request, before
    /// sending another request.
    #[clap(long)]
    faucet_cooldown: Option<Duration>,
    /// Use the legacy event processor instead of the standard checkpoint-based event processor.
    #[clap(long, action)]
    use_legacy_event_provider: bool,
    /// Disable the event blob writer.
    /// This will disable the event blob writer and the event blob writer service.
    #[clap(long, action)]
    disable_event_blob_writer: bool,
    /// Configure the Postgres database URL for the Backup service.
    #[clap(long)]
    backup_database_url: Option<String>,
    /// The path to the admin wallet. If not provided, the default wallet path in the
    /// working directory is used.
    #[clap(long)]
    admin_wallet_path: Option<PathBuf>,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    match args.command {
        Commands::RegisterNodes(args) => commands::register_nodes(args)?,
        Commands::DeploySystemContract(args) => commands::deploy_system_contract(args)?,
        Commands::GenerateDryRunConfigs(args) => commands::generate_dry_run_configs(args)?,
    }
    Ok(())
}

mod commands {
    use config::NodeRegistrationParamsForThirdPartyRegistration;
    use itertools::Itertools as _;
    use testbed::ADMIN_CONFIG_PREFIX;
    use walrus_service::{
        client::cli::HumanReadableFrost,
        testbed::{
            create_backup_config,
            create_client_config,
            create_storage_node_configs,
            deploy_walrus_contract,
            DeployTestbedContractParameters,
            TestbedConfig,
        },
        utils::{self, load_from_yaml},
    };
    use walrus_sui::utils::load_wallet;
    use walrus_utils::backoff::ExponentialBackoffConfig;

    use super::*;

    #[tokio::main]
    pub(super) async fn register_nodes(
        RegisterNodesArgs {
            client_config,
            param_files,
            stake_amount,
            gas_budget,
        }: RegisterNodesArgs,
    ) -> anyhow::Result<()> {
        let config: walrus_service::client::Config = load_from_yaml(client_config)?;
        let contract_client = config
            .new_contract_client_with_wallet_in_config(gas_budget)
            .await?;
        let count = param_files.len();

        let unpacked_registration_params = param_files
            .iter()
            .map(|param_file| {
                let params: NodeRegistrationParamsForThirdPartyRegistration =
                    load_from_yaml(param_file)?;
                Ok((
                    params.node_registration_params,
                    params.proof_of_possession,
                    params.wallet_address,
                ))
            })
            .collect::<anyhow::Result<Vec<_>>>()
            .context("failed to load all registration parameters")?;

        let node_caps = contract_client
            .register_candidates(unpacked_registration_params)
            .await?;
        let node_ids = node_caps.iter().map(|cap| cap.node_id).collect::<Vec<_>>();
        println!(
            "Successfully registered {count} storage nodes:\n{}",
            node_ids.iter().map(|id| id.to_string()).join(", ")
        );

        let Some(stake_amount) = stake_amount else {
            return Ok(());
        };

        contract_client
            .stake_with_pools(
                &node_ids
                    .iter()
                    .map(|id| (*id, stake_amount))
                    .collect::<Vec<_>>(),
            )
            .await
            .context("staking with newly registered nodes failed")?;

        println!(
            "Successfully staked {} with each newly registered node (total: {}).",
            HumanReadableFrost::from(stake_amount),
            HumanReadableFrost::from(
                stake_amount * u64::try_from(count).expect("definitely fits into a u64")
            )
        );
        Ok(())
    }

    #[tokio::main]
    pub(super) async fn deploy_system_contract(
        DeploySystemContractArgs {
            working_dir,
            sui_network,
            contract_dir,
            gas_budget,
            n_shards,
            epoch_duration,
            epoch_zero_duration,
            host_addresses,
            rest_api_port,
            testbed_config_path,
            storage_price,
            write_price,
            storage_capacity,
            deterministic_keys,
            max_epochs_ahead,
            admin_wallet_path,
            do_not_copy_contracts,
            with_wal_exchange,
        }: DeploySystemContractArgs,
    ) -> anyhow::Result<()> {
        utils::init_tracing_subscriber()?;

        fs::create_dir_all(&working_dir)
            .with_context(|| format!("Failed to create directory '{}'", working_dir.display()))?;
        // Turn the working directory into an absolute path.
        let working_dir = working_dir
            .canonicalize()
            .context("canonicalizing the working directory path failed")?;

        // Deploy the system contract.
        let testbed_config = deploy_walrus_contract(DeployTestbedContractParameters {
            working_dir: &working_dir,
            sui_network,
            contract_dir,
            gas_budget,
            host_addresses,
            rest_api_port,
            storage_capacity,
            storage_price,
            write_price,
            deterministic_keys,
            n_shards,
            epoch_duration: *epoch_duration,
            epoch_zero_duration: *epoch_zero_duration,
            max_epochs_ahead,
            admin_wallet_path,
            do_not_copy_contracts,
            with_wal_exchange,
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
            faucet_cooldown,
            use_legacy_event_provider,
            disable_event_blob_writer,
            backup_database_url,
            admin_wallet_path,
        }: GenerateDryRunConfigsArgs,
    ) -> anyhow::Result<()> {
        utils::init_tracing_subscriber()?;

        fs::create_dir_all(&working_dir)
            .with_context(|| format!("Failed to create directory '{}'", working_dir.display()))?;
        // Turn the working directory into an absolute path.
        let working_dir = working_dir
            .canonicalize()
            .context("canonicalizing the working directory path failed")?;

        let testbed_config_path = get_testbed_config_path(testbed_config_path, &working_dir);
        let testbed_config: TestbedConfig = load_from_yaml(testbed_config_path)?;

        if let Some(cooldown) = faucet_cooldown {
            tracing::info!("sleeping for {cooldown} to let faucet cool down");
            tokio::time::sleep(cooldown.into()).await;
        }

        let admin_wallet_path = admin_wallet_path.or(Some(
            working_dir.join(format!("{ADMIN_CONFIG_PREFIX}.yaml")),
        ));
        let admin_wallet = load_wallet(admin_wallet_path).context("unable to load admin wallet")?;
        let mut admin_contract_client = testbed_config
            .system_ctx
            .new_contract_client(admin_wallet, ExponentialBackoffConfig::default(), None)
            .await?;

        let client_config = create_client_config(
            &testbed_config.system_ctx,
            working_dir.as_path(),
            testbed_config.sui_network.clone(),
            set_config_dir.as_deref(),
            &mut admin_contract_client,
            testbed_config.exchange_object.into_iter().collect(),
        )
        .await?;
        let serialized_client_config =
            serde_yaml::to_string(&client_config).context("Failed to serialize client configs")?;
        let client_config_path = working_dir.join("client_config.yaml");
        fs::write(client_config_path, serialized_client_config)
            .context("Failed to write client configs")?;

        if let Some(database_url) = backup_database_url {
            let backup_config = create_backup_config(
                &testbed_config.system_ctx,
                working_dir.as_path(),
                database_url.as_str(),
                testbed_config.sui_network.env().rpc,
            )
            .await?;
            let serialized_backup_config = serde_yaml::to_string(&backup_config)
                .context("Failed to serialize backup config")?;
            let backup_config_path = working_dir.join("backup_config.yaml");
            fs::write(backup_config_path, serialized_backup_config)
                .context("Failed to write backup config")?;
        }

        let committee_size =
            NonZeroU16::new(testbed_config.nodes.len() as u16).expect("committee size must be > 0");
        let storage_node_configs = create_storage_node_configs(
            working_dir.as_path(),
            testbed_config,
            listening_ips,
            metrics_port,
            set_config_dir.as_deref(),
            set_db_path.as_deref(),
            faucet_cooldown.map(|duration| duration.into()),
            &mut admin_contract_client,
            use_legacy_event_provider,
            disable_event_blob_writer,
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

fn get_testbed_config_path(
    maybe_testbed_config_path: Option<PathBuf>,
    working_dir: &Path,
) -> PathBuf {
    maybe_testbed_config_path.unwrap_or_else(|| working_dir.join("testbed_config.yaml"))
}
