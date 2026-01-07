// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! CLI tool to generate Walrus configurations and deploy testbeds.

use std::{
    fs,
    net::IpAddr,
    num::NonZeroU16,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use anyhow::Context;
use clap::{Parser, Subcommand};
use sui_types::base_types::{ObjectID, SuiAddress};
use walrus_core::EpochCount;
use walrus_service::{
    node::config::{
        self,
        defaults::{METRICS_PORT, REST_API_PORT},
    },
    testbed,
};
use walrus_sui::{
    client::{UpgradeType, rpc_config::RpcFallbackConfigArgs},
    utils::SuiNetwork,
};

// Define the `GIT_REVISION` and `VERSION` consts
walrus_utils::bin_version!();

#[derive(Parser, Debug)]
#[command(
    name = env!("CARGO_BIN_NAME"),
    version = VERSION,
    rename_all = "kebab-case"
)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug, Clone)]
#[command(rename_all = "kebab-case")]
enum Commands {
    /// Register nodes based on parameters exported by the `walrus-node setup` command, send the
    /// storage-node capability to the respective node's wallet, and optionally stake with them.
    RegisterNodes(RegisterNodesArgs),
    /// Deploy the Walrus system contract on the Sui network.
    DeploySystemContract(DeploySystemContractArgs),
    /// Generate the configuration files to run a testbed of storage nodes.
    GenerateDryRunConfigs(GenerateDryRunConfigsArgs),
    /// Upgrades the system contract with an Emergency Upgrade.
    EmergencyUpgrade(UpgradeArgs),
    /// Upgrades the system contract with a quorum-based upgrade that has been voted for by
    /// a quorum of storage nodes by shard weight.
    Upgrade(UpgradeArgs),
    /// Migrates the system and staking objects to a new package version.
    Migrate(MigrateArgs),
}

#[derive(Debug, Clone, clap::Args)]
#[command(rename_all = "kebab-case")]
struct RegisterNodesArgs {
    /// The path to the client config.
    #[arg(long)]
    client_config: PathBuf,
    /// The files containing the registration parameters exported by the `walrus-node setup`
    /// command.
    #[arg(long, alias("files"), num_args(1..))]
    param_files: Vec<PathBuf>,
    /// The (optional) amount of WAL to stake with the newly registered nodes.
    ///
    /// The stake amount is staked with all nodes.
    // For simplicity and to prevent mistakes, we only allow a single stake amount here. Different
    // stake amounts are supported with the `walrus stake` command.
    #[arg(long)]
    stake_amount: Option<u64>,
    /// Gas budget for Sui transactions to register the nodes.
    #[arg(long)]
    gas_budget: Option<u64>,
}

#[derive(Debug, Clone, clap::Args)]
struct DeploySystemContractArgs {
    /// The directory where the storage nodes will be deployed.
    #[arg(long, default_value = "./working_dir")]
    working_dir: PathBuf,
    /// Sui network for which the config is generated.
    ///
    /// Available options are `devnet`, `testnet`, `mainnet`, and `localnet`, or a custom Sui
    /// network. To specify a custom Sui network, pass a string of the format
    /// `<RPC_URL>(;<FAUCET_URL>)?`.
    #[arg(long, default_value = "testnet")]
    sui_network: SuiNetwork,
    /// The directory in which the contracts are located.
    #[arg(long, default_value = "./contracts")]
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
    #[arg(long, default_value = "1h", value_parser = humantime::parse_duration)]
    epoch_duration: Duration,
    /// Configuration for epoch 0.
    #[command(flatten)]
    epoch_zero_config: EpochZeroConfig,
    /// The list of host names or public IP addresses of the storage nodes.
    #[arg(long, value_name = "ADDR", value_delimiter = ' ', num_args(1..))]
    host_addresses: Vec<String>,
    /// The port on which the REST API of the storage nodes will listen.
    #[arg(long, default_value_t = REST_API_PORT)]
    rest_api_port: u16,
    /// The path to the configuration file of the Walrus testbed.
    /// [default: <WORKING_DIR>/testbed_config.yaml]
    #[arg(long)]
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
    #[arg(long)]
    deterministic_keys: bool,
    /// The maximum number of epochs ahead for which storage can be obtained.
    #[arg(long, default_value_t = 53)]
    max_epochs_ahead: EpochCount,
    /// The path to the admin wallet. If not provided, a new wallet is created.
    #[arg(long)]
    admin_wallet_path: Option<PathBuf>,
    /// If not set, contracts are copied to `working_dir` and published from there to keep the
    /// `Move.toml` unchanged. Use this flag to publish from the original directory and update
    /// the `Move.toml` to point to the new contracts.
    #[arg(long)]
    do_not_copy_contracts: bool,
    /// If set, creates a WAL exchange.
    #[arg(long)]
    with_wal_exchange: bool,
    /// If set, the deployment reuses the token deployed at the address specified in the `Move.lock`
    /// file of the WAL contract. Otherwise, a new WAL token is created.
    #[arg(long)]
    use_existing_wal_token: bool,
}

/// Configuration for epoch 0, either as a duration or as an absolute end time.
#[derive(Debug, Clone, clap::Args, PartialEq, Eq)]
#[group(multiple = false)]
pub struct EpochZeroConfig {
    /// The minimum duration of epoch 0. If neither this nor `--epoch-zero-end` is set, the duration
    /// of epoch 0 will be 0.
    #[arg(long, value_parser = humantime::parse_duration)]
    epoch_zero_duration: Option<Duration>,
    /// The earliest time when epoch 0 can end, in RFC3339 format (e.g., "2024-03-20T15:00:00Z")
    /// or a more relaxed format (e.g., "2024-03-20 15:00:00").
    #[arg(long, value_parser = humantime::parse_rfc3339_weak)]
    epoch_zero_end: Option<SystemTime>,
}

#[derive(Debug, Clone, clap::Args)]
struct GenerateDryRunConfigsArgs {
    /// The directory where the storage nodes will be deployed.
    #[arg(long, default_value = "./working_dir")]
    working_dir: PathBuf,
    /// The path to the configuration file of the Walrus testbed.
    /// [default: <WORKING_DIR>/testbed_config.yaml]
    #[arg(long)]
    testbed_config_path: Option<PathBuf>,
    /// The list of listening IP addresses of the storage nodes.
    /// If not set, defaults to the addresses or (resolved) host names set in the testbed config.
    #[arg(long, value_name = "ADDR", value_delimiter = ' ', num_args(..))]
    listening_ips: Option<Vec<IpAddr>>,
    /// The port on which the metrics server of the storage nodes will listen.
    #[arg(long, default_value_t = METRICS_PORT)]
    metrics_port: u16,
    /// Path of the directory in which the config files will be stored on deployed nodes.
    ///
    /// If specified, the working directory in the paths contained in the node, client,
    /// and wallet configs will be replaced with this directory.
    #[arg(long)]
    set_config_dir: Option<PathBuf>,
    /// Path of the node database.
    ///
    /// If specified the database path of all nodes will be set to this path, otherwise it
    /// will be located in the config directory and have the same name as the node it belongs to.
    #[arg(long)]
    set_db_path: Option<PathBuf>,
    /// Cooldown duration for the faucet.
    ///
    /// Setting this makes sure that we wait at least this duration after a faucet request, before
    /// sending another request.
    #[arg(long, value_parser = humantime::parse_duration)]
    faucet_cooldown: Option<Duration>,
    /// Disable the event blob writer.
    /// This will disable the event blob writer and the event blob writer service.
    #[arg(long)]
    disable_event_blob_writer: bool,
    /// Configure the Postgres database URL for the Backup service.
    #[arg(long)]
    backup_database_url: Option<String>,
    /// The path to the admin wallet. If not provided, the default wallet path in the
    /// working directory is used.
    #[arg(long)]
    admin_wallet_path: Option<PathBuf>,
    /// The amount of SUI (in MIST) to send to all created wallets.
    #[arg(long, default_value_t = 1_000_000_000, requires = "admin_wallet_path")]
    sui_amount: u64,
    /// The config for rpc fallback.
    #[command(flatten)]
    rpc_fallback_config_args: Option<RpcFallbackConfigArgs>,
    /// Any extra client wallets to generate. Each will get 1 Million WAL and some Sui.
    #[arg(long)]
    extra_client_wallets: Option<String>,
    /// The request timeout for the client config.
    #[arg(long, value_parser = humantime::parse_duration)]
    sui_client_request_timeout: Option<Duration>,
}

#[derive(Debug, Clone, clap::Args)]
struct UpgradeArgs {
    /// The path to the wallet used to perform the upgrade. If not provided, the default
    /// wallet path is used.
    #[arg(long)]
    wallet_path: Option<PathBuf>,
    /// The path to the contract directory.
    #[arg(long)]
    contract_dir: PathBuf,
    /// The staking object ID.
    #[arg(long)]
    staking_object_id: ObjectID,
    /// The system object ID.
    #[arg(long)]
    system_object_id: ObjectID,
    /// The upgrade manager object ID.
    #[arg(long)]
    upgrade_manager_object_id: ObjectID,
    /// Create and output a serialized unsigned transaction. Useful for an emergency upgrade
    /// authorized by a multisig address.
    #[arg(long)]
    serialize_unsigned: bool,
    /// The sender address to use to create the serialized unsigned transaction. If not specified,
    /// the active address from the wallet is used.
    #[arg(long, requires = "serialize_unsigned")]
    sender: Option<SuiAddress>,
    /// Also set the migration epoch on the staking object after upgrading the contract.
    #[arg(long, conflicts_with = "serialize_unsigned")]
    set_migration_epoch: bool,
}

#[derive(Debug, Clone, clap::Args)]
struct MigrateArgs {
    /// The path to the wallet used to perform the upgrade. If not provided, the default
    /// wallet path is used.
    #[arg(long)]
    wallet_path: Option<PathBuf>,
    /// The staking object ID.
    #[arg(long)]
    staking_object_id: ObjectID,
    /// The system object ID.
    #[arg(long)]
    system_object_id: ObjectID,
    /// The package ID of the upgraded package.
    #[arg(long)]
    new_package_id: ObjectID,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    match args.command {
        Commands::RegisterNodes(args) => commands::register_nodes(args)?,
        Commands::DeploySystemContract(args) => commands::deploy_system_contract(args)?,
        Commands::GenerateDryRunConfigs(args) => commands::generate_dry_run_configs(args)?,
        Commands::EmergencyUpgrade(args) => commands::upgrade(args, UpgradeType::Emergency)?,
        Commands::Upgrade(args) => commands::upgrade(args, UpgradeType::Quorum)?,
        Commands::Migrate(args) => commands::migrate(args)?,
    }
    Ok(())
}

mod commands {
    use anyhow::anyhow;
    use config::NodeRegistrationParamsForThirdPartyRegistration;
    use fastcrypto::encoding::Encoding;
    use itertools::Itertools as _;
    use testbed::ADMIN_CONFIG_PREFIX;
    use walrus_service::{
        client::cli::{HumanReadableFrost, success},
        testbed::{
            DeployTestbedContractParameters,
            TestbedConfig,
            create_backup_config,
            create_client_config,
            create_storage_node_configs,
            deploy_walrus_contract,
        },
        utils,
    };
    use walrus_sui::{
        client::{
            SuiContractClient,
            UpgradeType,
            contract_config::ContractConfig,
            transaction_builder::WalrusPtbBuilder,
        },
        config::load_wallet_context_from_path,
        system_setup::compile_package,
    };
    use walrus_utils::{backoff::ExponentialBackoffConfig, load_from_yaml};

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
        let config: walrus_service::client::ClientConfig = load_from_yaml(client_config)?;
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
            "{} Registered {} storage nodes:\n{}",
            success(),
            count,
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
            "{} Staked {} with each newly registered node (total: {}).",
            success(),
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
            epoch_zero_config,
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
            use_existing_wal_token,
        }: DeploySystemContractArgs,
    ) -> anyhow::Result<()> {
        utils::init_tracing_subscriber()?;

        fs::create_dir_all(&working_dir)
            .with_context(|| format!("Failed to create directory '{}'", working_dir.display()))?;
        // Turn the working directory into an absolute path.
        let working_dir = working_dir
            .canonicalize()
            .context("canonicalizing the working directory path failed")?;

        let epoch_zero_duration = match epoch_zero_config {
            EpochZeroConfig {
                epoch_zero_duration: Some(duration),
                ..
            } => duration,
            EpochZeroConfig {
                epoch_zero_end: Some(end),
                ..
            } => end.duration_since(SystemTime::now())?,
            _ => Duration::from_secs(0),
        };

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
            epoch_duration,
            epoch_zero_duration,
            max_epochs_ahead,
            admin_wallet_path,
            do_not_copy_contracts,
            with_wal_exchange,
            use_existing_wal_token,
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
            disable_event_blob_writer,
            backup_database_url,
            rpc_fallback_config_args,
            admin_wallet_path,
            sui_amount,
            extra_client_wallets,
            sui_client_request_timeout,
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
            tracing::info!(
                "sleeping for {} to let faucet cool down",
                humantime::Duration::from(cooldown)
            );
            tokio::time::sleep(cooldown).await;
        }

        let admin_wallet_path = admin_wallet_path.or(Some(
            working_dir.join(format!("{ADMIN_CONFIG_PREFIX}.yaml")),
        ));
        let admin_wallet =
            load_wallet_context_from_path(admin_wallet_path, sui_client_request_timeout)
                .context("unable to load admin wallet")?;

        let rpc_urls = &[admin_wallet.get_rpc_url().to_string()];

        let mut admin_contract_client = testbed_config
            .system_ctx
            .new_contract_client(
                admin_wallet,
                rpc_urls,
                ExponentialBackoffConfig::default(),
                None,
            )
            .await?;

        create_client_wallets(
            &testbed_config,
            working_dir.as_path(),
            set_config_dir.as_deref(),
            &mut admin_contract_client,
            sui_amount,
            extra_client_wallets,
            sui_client_request_timeout,
        )
        .await?;

        if let Some(database_url) = backup_database_url {
            let backup_config = create_backup_config(
                &testbed_config.system_ctx,
                working_dir.as_path(),
                database_url.as_str(),
                vec![testbed_config.sui_network.env().rpc],
                rpc_fallback_config_args
                    .as_ref()
                    .and_then(|args| args.to_config()),
            )?;
            let serialized_backup_config = serde_yaml::to_string(&backup_config)
                .context("Failed to serialize backup config")?;
            let backup_config_path = working_dir.join("backup_config.yaml");
            fs::write(backup_config_path, serialized_backup_config)
                .context("Failed to write backup config")?;
        }

        let committee_size =
            NonZeroU16::new(testbed_config.nodes.len().try_into().map_err(|_| {
                anyhow!(
                    "committee size is too large: {} > {}",
                    testbed_config.nodes.len(),
                    u16::MAX
                )
            })?)
            .ok_or_else(|| anyhow!("committee size must be > 0"))?;
        let storage_node_configs = create_storage_node_configs(
            working_dir.as_path(),
            testbed_config,
            listening_ips,
            metrics_port,
            set_config_dir.as_deref(),
            set_db_path.as_deref(),
            faucet_cooldown,
            rpc_fallback_config_args
                .as_ref()
                .and_then(|args| args.to_config()),
            &mut admin_contract_client,
            disable_event_blob_writer,
            sui_amount,
            sui_client_request_timeout,
        )
        .await?;
        assert!(
            storage_node_configs.len() == usize::from(committee_size.get()),
            "number of storage nodes ({}) does not match committee size ({})",
            storage_node_configs.len(),
            committee_size.get()
        );

        for (i, storage_node_config) in storage_node_configs.into_iter().enumerate() {
            let serialized_storage_node_config = serde_yaml::to_string(&storage_node_config)
                .context("Failed to serialize storage node configs")?;
            let node_config_name = format!(
                "{}.yaml",
                testbed::node_config_name_prefix(
                    u16::try_from(i)
                        .expect("we checked above that the number of configs is at most 2^16"),
                    committee_size
                )
            );
            let node_config_path = working_dir.join(node_config_name);
            fs::write(node_config_path, serialized_storage_node_config)
                .context("Failed to write storage node configs")?;
        }

        Ok(())
    }

    #[tokio::main]
    pub(super) async fn upgrade(
        UpgradeArgs {
            wallet_path,
            contract_dir,
            staking_object_id,
            system_object_id,
            upgrade_manager_object_id,
            sender,
            serialize_unsigned,
            set_migration_epoch,
        }: UpgradeArgs,
        upgrade_type: UpgradeType,
    ) -> anyhow::Result<()> {
        utils::init_tracing_subscriber()?;

        let wallet = load_wallet_context_from_path(wallet_path.clone(), None)
            .context("unable to load wallet")?;
        let contract_config = ContractConfig::new(system_object_id, staking_object_id);

        let rpc_urls = &[wallet.get_rpc_url().to_string()];

        let contract_client =
            SuiContractClient::new(wallet, rpc_urls, &contract_config, Default::default(), None)
                .await?;
        if serialize_unsigned {
            // Compile package
            let chain_id = contract_client
                .retriable_sui_client()
                .get_chain_identifier()
                .await
                .ok();
            let wallet = load_wallet_context_from_path(wallet_path, None)
                .context("unable to load wallet")?;
            let (compiled_package, _build_config, _root_package) =
                compile_package(contract_dir, Default::default(), chain_id, &wallet).await?;

            let sender = sender.unwrap_or(contract_client.address());
            let mut pt_builder = WalrusPtbBuilder::new(contract_client.read_client, sender);

            pt_builder
                .custom_walrus_upgrade(upgrade_manager_object_id, compiled_package, upgrade_type)
                .await?;

            let tx_data = pt_builder.build_transaction_data(None).await?;
            println!(
                "{}",
                fastcrypto::encoding::Base64::encode(bcs::to_bytes(&tx_data)?)
            );
        } else {
            let new_package_id = contract_client
                .upgrade(upgrade_manager_object_id, contract_dir, upgrade_type)
                .await?;
            println!(
                "{} Upgraded the system contract:\npackage_id: {}",
                success(),
                new_package_id
            );
            if set_migration_epoch {
                contract_client.set_migration_epoch(new_package_id).await?;
                println!(
                    "{} Set the migration epoch on the staking object for package_id: {}",
                    success(),
                    new_package_id
                );
            }
        }
        Ok(())
    }

    #[tokio::main]
    pub(super) async fn migrate(
        MigrateArgs {
            wallet_path,
            staking_object_id,
            system_object_id,
            new_package_id,
        }: MigrateArgs,
    ) -> anyhow::Result<()> {
        utils::init_tracing_subscriber()?;

        let wallet =
            load_wallet_context_from_path(wallet_path, None).context("unable to load wallet")?;
        let contract_config = ContractConfig::new(system_object_id, staking_object_id);

        let rpc_urls = &[wallet.get_rpc_url().to_string()];

        let contract_client =
            SuiContractClient::new(wallet, rpc_urls, &contract_config, Default::default(), None)
                .await?;

        contract_client.migrate_contracts(new_package_id).await?;
        println!(
            "{} Migrated the shared objects to package_id: {}",
            success(),
            new_package_id
        );
        Ok(())
    }

    /// Helper to create client wallets as requested.
    async fn create_client_wallets(
        testbed_config: &TestbedConfig,
        working_dir: &Path,
        set_config_dir: Option<&Path>,
        admin_contract_client: &mut SuiContractClient,
        sui_amount: u64,
        extra_client_wallets: Option<String>,
        sui_client_request_timeout: Option<Duration>,
    ) -> anyhow::Result<()> {
        // The "sui_client" wallet is always created, and we add on any extra client wallets as
        // requested.
        // TODO: This is a bit of technical debt. We should probably not have implicit wallets
        // created. It would be best if all callsites passed their desired client wallets in. See
        // WAL-737.
        let mut client_wallets = vec![(
            String::from("sui_client"),
            String::from("client_config.yaml"),
        )];
        if let Some(extra_client_wallets) = extra_client_wallets {
            client_wallets.extend(
                extra_client_wallets
                    .split(',')
                    .map(|x| (String::from(x), format!("client_config_{x}.yaml"))),
            );
        }
        for (sui_wallet_name, walrus_config_filename) in client_wallets {
            tracing::debug!(
                sui_wallet_name,
                walrus_config_filename,
                "creating client wallet configuration"
            );
            let client_config = create_client_config(
                &testbed_config.system_ctx,
                working_dir,
                testbed_config.sui_network.clone(),
                set_config_dir,
                admin_contract_client,
                testbed_config.exchange_object.into_iter().collect(),
                sui_amount,
                &sui_wallet_name,
                sui_client_request_timeout,
            )
            .await?;
            let serialized_client_config = serde_yaml::to_string(&client_config)
                .context("Failed to serialize client configs")?;
            let client_config_path = working_dir.join(walrus_config_filename);
            fs::write(client_config_path, serialized_client_config)
                .context("Failed to write client configs")?;
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
