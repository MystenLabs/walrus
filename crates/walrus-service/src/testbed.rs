// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Facilities to deploy a Walrus testbed.

use std::{
    collections::HashSet,
    fs,
    io::Write as _,
    net::{IpAddr, SocketAddr, ToSocketAddrs},
    num::NonZeroU16,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{anyhow, ensure, Context};
use futures::future::join_all;
use rand::{rngs::StdRng, SeedableRng};
use serde::{Deserialize, Serialize};
use serde_with::base64::Base64;
use sui_sdk::wallet_context::WalletContext;
use sui_types::base_types::ObjectID;
use walrus_core::{
    keys::{NetworkKeyPair, ProtocolKeyPair},
    EpochCount,
    ShardIndex,
};
use walrus_sui::{
    client::SuiContractClient,
    system_setup::InitSystemParams,
    test_utils::{
        system_setup::{
            create_and_init_system,
            end_epoch_zero,
            mint_wal_to_addresses,
            register_committee_and_stake,
            SystemContext,
        },
        DEFAULT_GAS_BUDGET,
    },
    types::{move_structs::VotingParams, NetworkAddress, NodeMetadata, NodeRegistrationParams},
    utils::{create_wallet, request_sui_from_faucet, SuiNetwork},
};
use walrus_utils::backoff::ExponentialBackoffConfig;

use crate::{
    client::{self, ClientCommunicationConfig},
    common::utils::LoadConfig,
    node::config::{
        defaults::{self, REST_API_PORT},
        StorageNodeConfig,
        SuiConfig,
    },
};

/// The config file name for the admin wallet.
pub const ADMIN_CONFIG_PREFIX: &str = "sui_admin";

/// Node-specific testbed configuration.
#[serde_with::serde_as]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TestbedNodeConfig {
    /// Name of the storage node.
    pub name: String,
    /// The REST API address of the node.
    pub network_address: NetworkAddress,
    /// The key of the node.
    #[serde_as(as = "Base64")]
    pub keypair: ProtocolKeyPair,
    /// The network key of the node.
    #[serde_as(as = "Base64")]
    pub network_keypair: NetworkKeyPair,
    /// The commission rate of the storage node.
    pub commission_rate: u16,
    /// The vote for the storage price per unit.
    pub storage_price: u64,
    /// The vote for the write price per unit.
    pub write_price: u64,
    /// The capacity of the node that determines the vote for the capacity
    /// after shards are assigned.
    pub node_capacity: u64,
}

impl From<TestbedNodeConfig> for NodeRegistrationParams {
    fn from(config: TestbedNodeConfig) -> Self {
        NodeRegistrationParams {
            name: config.name,
            network_address: config.network_address,
            public_key: config.keypair.public().clone(),
            network_public_key: config.network_keypair.public().clone(),
            commission_rate: config.commission_rate,
            storage_price: config.storage_price,
            write_price: config.write_price,
            node_capacity: config.node_capacity,
            metadata: NodeMetadata::default(),
        }
    }
}

/// Configuration for a Walrus testbed.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TestbedConfig {
    /// Sui network for which the config is generated.
    #[serde(default = "defaults::network")]
    pub sui_network: SuiNetwork,
    /// The list of ip addresses of the storage nodes.
    pub nodes: Vec<TestbedNodeConfig>,
    /// The objects used in the system contract.
    pub system_ctx: SystemContext,
    /// The object ID of the shared WAL exchange.
    pub exchange_object: ObjectID,
}

impl LoadConfig for TestbedConfig {}

/// Prefix for the node configuration file name.
pub fn node_config_name_prefix(node_index: u16, committee_size: NonZeroU16) -> String {
    let width = if committee_size.get() == 1 {
        1
    } else {
        usize::try_from((committee_size.get() - 1).ilog10())
            .expect("this is smaller than `u16::MAX`")
            + 1
    };
    format!("dryrun-node-{node_index:00$}", width)
}

/// Generates deterministic keypairs for the benchmark purposes.
pub fn deterministic_keypairs(n: usize) -> Vec<(ProtocolKeyPair, NetworkKeyPair)> {
    let mut rng = StdRng::seed_from_u64(0);
    // Generate key pairs sequentially to ensure backwards compatibility of the protocol keys.
    let protocol_keys: Vec<_> = (0..n)
        .map(|_| ProtocolKeyPair::generate_with_rng(&mut rng))
        .collect();
    let network_keys = (0..n).map(|_| NetworkKeyPair::generate_with_rng(&mut rng));

    protocol_keys.into_iter().zip(network_keys).collect()
}

/// Generates a list of random keypairs.
pub fn random_keypairs(n: usize) -> Vec<(ProtocolKeyPair, NetworkKeyPair)> {
    (0..n)
        .map(|_| (ProtocolKeyPair::generate(), NetworkKeyPair::generate()))
        .collect()
}

/// Formats the metrics address for a node. If the node index is provided, the port is adjusted
/// to ensure uniqueness across nodes.
pub fn metrics_socket_address(ip: IpAddr, port: u16, node_index: Option<u16>) -> SocketAddr {
    let port = port + node_index.unwrap_or(0);
    SocketAddr::new(ip, port)
}

/// Formats the REST API address for a node. If both the node index and the committee size is
/// provided, the port is adjusted to ensure uniqueness across nodes.
pub fn rest_api_socket_address(
    ip: IpAddr,
    port: u16,
    node_index: Option<u16>,
    committee_size: Option<u16>,
) -> SocketAddr {
    SocketAddr::new(ip, rest_api_port(port, node_index, committee_size))
}

/// Creates the REST API address for a node. If both the node index and the committee size is
/// provided, the port is adjusted to ensure uniqueness across nodes.
pub fn public_rest_api_address(
    host: String,
    port: u16,
    node_index: Option<u16>,
    committee_size: Option<u16>,
) -> NetworkAddress {
    NetworkAddress(format!(
        "{}:{}",
        host,
        rest_api_port(port, node_index, committee_size)
    ))
}

fn rest_api_port(port: u16, node_index: Option<u16>, committee_size: Option<u16>) -> u16 {
    if let (Some(node_index), Some(committee_size)) = (node_index, committee_size) {
        port + committee_size + node_index
    } else {
        port
    }
}

/// Generates deterministic and even shard allocation for the benchmark purposes.
pub fn even_shards_allocation(
    n_shards: NonZeroU16,
    committee_size: NonZeroU16,
) -> Vec<Vec<ShardIndex>> {
    let shards_per_node = n_shards.get() / committee_size.get();
    let remainder_shards = n_shards.get() % committee_size.get();
    let mut start = 0;
    let mut shards_information = Vec::new();
    for i in 0..committee_size.get() {
        let end = if i < remainder_shards {
            start + shards_per_node + 1
        } else {
            start + shards_per_node
        };
        let shard_ids = (start..end).map(ShardIndex).collect();
        start = end;
        shards_information.push(shard_ids);
    }
    shards_information
}

/// Parameters to deploy the system contract.
#[derive(Debug)]
pub struct DeployTestbedContractParameters<'a> {
    /// The path to store configs in.
    pub working_dir: &'a Path,
    /// The sui network to deploy the contract on.
    pub sui_network: SuiNetwork,
    /// The path of the contract.
    pub contract_dir: PathBuf,
    /// The gas budget to use for deployment.
    pub gas_budget: u64,
    /// The hostnames or public ip addresses of the nodes.
    pub host_addresses: Vec<String>,
    /// The rest api port of the nodes.
    pub rest_api_port: u16,
    /// The storage capacity of the deployed system.
    pub storage_capacity: u64,
    /// The price to charge per unit of storage.
    pub storage_price: u64,
    /// The price to charge for writes per unit.
    pub write_price: u64,
    /// Flag to generate keys deterministically.
    pub deterministic_keys: bool,
    /// The total number of shards.
    pub n_shards: NonZeroU16,
    /// The epoch duration of the genesis epoch.
    pub epoch_zero_duration: Duration,
    /// The epoch duration.
    pub epoch_duration: Duration,
    /// The maximum number of epochs ahead for which storage can be obtained.
    pub max_epochs_ahead: EpochCount,
    /// If set, the contracts are not copied to `working_dir` and instead published from the
    /// original directory.
    pub do_not_copy_contracts: bool,
}

/// Create and deploy a Walrus contract.
pub async fn deploy_walrus_contract(
    DeployTestbedContractParameters {
        working_dir,
        sui_network,
        contract_dir,
        gas_budget,
        host_addresses: hosts,
        rest_api_port,
        storage_capacity,
        storage_price,
        write_price,
        deterministic_keys,
        n_shards,
        epoch_zero_duration,
        epoch_duration,
        max_epochs_ahead,
        do_not_copy_contracts,
    }: DeployTestbedContractParameters<'_>,
) -> anyhow::Result<TestbedConfig> {
    const WAL_MINT_AMOUNT: u64 = 100_000_000 * 1_000_000_000;
    const WAL_AMOUNT_EXCHANGE: u64 = 10_000_000 * 1_000_000_000;

    // Check whether the testbed collocates the storage nodes on the same machine
    // (that is, local testbed).
    let hosts_set = hosts.iter().collect::<HashSet<_>>();
    let collocated = hosts_set.len() != hosts.len();

    // Build one Sui storage node config for each storage node.
    let committee_size = hosts.len() as u16;
    let keypairs = if deterministic_keys {
        deterministic_keypairs(committee_size as usize)
    } else {
        random_keypairs(committee_size as usize)
    };

    let mut node_configs = Vec::new();

    for (i, ((keypair, network_keypair), host)) in
        keypairs.into_iter().zip(hosts.iter().cloned()).enumerate()
    {
        let node_index = i as u16;
        let name = node_config_name_prefix(node_index, NonZeroU16::new(committee_size).unwrap());
        let network_address = if collocated {
            public_rest_api_address(host, rest_api_port, Some(node_index), Some(committee_size))
        } else {
            public_rest_api_address(host, rest_api_port, None, None)
        };

        node_configs.push(TestbedNodeConfig {
            name,
            network_address: network_address.clone(),
            keypair,
            network_keypair,
            commission_rate: 0,
            storage_price,
            write_price,
            node_capacity: storage_capacity / (hosts.len() as u64),
        });
    }

    // Create the working directory if it does not exist
    fs::create_dir_all(working_dir).expect("Failed to create working directory");

    // Create wallet for publishing contracts on sui and setting up system object
    let mut admin_wallet = create_wallet(
        &working_dir.join(format!("{ADMIN_CONFIG_PREFIX}.yaml")),
        sui_network.env(),
        Some(&format!("{ADMIN_CONFIG_PREFIX}.keystore")),
    )?;
    let admin_address = admin_wallet.active_address()?;

    // Get coins from faucet for the wallets.
    let sui_client = admin_wallet.get_client().await?;
    request_sui_from_faucet(admin_address, &sui_network, &sui_client).await?;

    let deploy_directory = if do_not_copy_contracts {
        None
    } else {
        Some(working_dir.join("contracts"))
    };

    let system_ctx = create_and_init_system(
        contract_dir,
        &mut admin_wallet,
        InitSystemParams {
            n_shards,
            epoch_zero_duration,
            epoch_duration,
            max_epochs_ahead,
        },
        gas_budget,
        deploy_directory,
    )
    .await?;

    // Mint WAL to the admin wallet.
    mint_wal_to_addresses(
        &mut admin_wallet,
        system_ctx.wal_pkg_id,
        system_ctx.treasury_cap,
        &[admin_address],
        WAL_MINT_AMOUNT,
    )
    .await?;

    let contract_config = system_ctx.contract_config();

    // Create WAL exchange.
    let contract_client = SuiContractClient::new(
        admin_wallet,
        &contract_config,
        ExponentialBackoffConfig::default(),
        gas_budget,
    )
    .await?;

    // TODO(WAL-520): create multiple exchange objects
    let exchange_object = contract_client
        .create_and_fund_exchange(system_ctx.wal_exchange_pkg_id, WAL_AMOUNT_EXCHANGE)
        .await?;

    println!(
        "Walrus contract created:\n\
            package_id: {}\n\
            system_object: {}\n\
            staking_object: {}\n\
            exchange_object: {}",
        system_ctx.walrus_pkg_id,
        system_ctx.system_object,
        system_ctx.staking_object,
        exchange_object
    );
    Ok(TestbedConfig {
        sui_network,
        nodes: node_configs,
        system_ctx,
        exchange_object,
    })
}

/// Create client configurations for the testbed.
pub async fn create_client_config(
    system_ctx: &SystemContext,
    working_dir: &Path,
    sui_network: SuiNetwork,
    set_config_dir: Option<&Path>,
    admin_wallet: &mut WalletContext,
    exchange_object: ObjectID,
) -> anyhow::Result<client::Config> {
    // Create the working directory if it does not exist
    fs::create_dir_all(working_dir).expect("Failed to create working directory");

    // Create wallet for the client
    let client_wallet_path = working_dir.join("sui_client.yaml");
    let mut client_wallet = create_wallet(
        &client_wallet_path,
        sui_network.env(),
        Some("sui_client.keystore"),
    )?;

    let client_address = client_wallet.active_address()?;

    // Get coins from faucet for the wallets.
    let sui_client = client_wallet.get_client().await?;
    request_sui_from_faucet(client_wallet.active_address()?, &sui_network, &sui_client).await?;

    let wallet_path = if let Some(final_directory) = set_config_dir {
        replace_keystore_path(&client_wallet_path, final_directory)
            .context("replacing the keystore path failed")?;
        final_directory.join(
            client_wallet_path
                .file_name()
                .expect("file name should exist"),
        )
    } else {
        client_wallet_path
    };

    // Mint WAL to the client address.
    mint_wal_to_addresses(
        admin_wallet,
        system_ctx.wal_pkg_id,
        system_ctx.treasury_cap,
        &[client_address],
        1_000_000 * 1_000_000_000, // 1 million WAL
    )
    .await?;

    let contract_config = system_ctx.contract_config();

    // Create the client config.
    let client_config = client::Config {
        contract_config,
        exchange_objects: vec![exchange_object],
        wallet_config: Some(wallet_path),
        communication_config: ClientCommunicationConfig::default(),
    };

    Ok(client_config)
}

/// Create storage node configurations for the testbed.
#[tracing::instrument(err, skip_all)]
#[allow(clippy::too_many_arguments)]
pub async fn create_storage_node_configs(
    working_dir: &Path,
    testbed_config: TestbedConfig,
    listening_ips: Option<Vec<IpAddr>>,
    metrics_port: u16,
    set_config_dir: Option<&Path>,
    set_db_path: Option<&Path>,
    faucet_cooldown: Option<Duration>,
    admin_wallet: &mut WalletContext,
    use_legacy_event_provider: bool,
    disable_event_blob_writer: bool,
) -> anyhow::Result<Vec<StorageNodeConfig>> {
    tracing::debug!(
        ?working_dir,
        ?listening_ips,
        metrics_port,
        ?set_config_dir,
        ?set_db_path,
        ?faucet_cooldown,
        use_legacy_event_provider,
        disable_event_blob_writer,
        "starting to create storage-node configs"
    );
    let nodes = testbed_config.nodes;
    // Check whether the testbed collocates the storage nodes on the same machine
    // (that is, local testbed).
    let host_set = nodes
        .iter()
        .map(|node| node.network_address.get_host())
        .collect::<HashSet<_>>();
    let collocated = host_set.len() != nodes.len();

    // Get the listening addresses by resolving the host address if not set.
    let rest_api_addrs = if let Some(listening_ips) = listening_ips {
        ensure!(
            listening_ips.len() == nodes.len(),
            "mismatch between number of listening addresses and nodes"
        );
        listening_ips
            .into_iter()
            .zip(nodes.iter())
            .map(|(addr, node)| {
                node.network_address
                    .try_get_port()
                    .map(|port| SocketAddr::new(addr, port.unwrap_or(REST_API_PORT)))
            })
            .collect::<Result<Vec<_>, _>>()?
    } else {
        nodes
            .iter()
            .map(|node| {
                (
                    node.network_address.get_host(),
                    node.network_address
                        .try_get_port()?
                        .unwrap_or(REST_API_PORT),
                )
                    .to_socket_addrs()?
                    .next()
                    .ok_or_else(|| anyhow!("could not get socket addr from node address"))
            })
            .collect::<Result<Vec<_>, _>>()?
    };

    // Build one Sui storage node config for each storage node.
    let committee_size = nodes.len() as u16;
    let wallets = create_storage_node_wallets(
        working_dir,
        NonZeroU16::new(committee_size).expect("committee size must be > 0"),
        testbed_config.sui_network,
        faucet_cooldown,
    )
    .await?;

    let (node_params, protocol_keypairs): (Vec<_>, Vec<_>) = nodes
        .clone()
        .into_iter()
        .map(|node_config| {
            let keypair = node_config.keypair.clone();
            (NodeRegistrationParams::from(node_config), keypair)
        })
        .unzip();

    let rpc = wallets[0].config.get_active_env()?.rpc.clone();
    let mut storage_node_configs = Vec::new();
    for (i, (node, rest_api_address)) in nodes.into_iter().zip(rest_api_addrs).enumerate() {
        let node_index = i as u16;
        let name = node_config_name_prefix(node_index, NonZeroU16::new(committee_size).unwrap());

        let metrics_address = if collocated {
            metrics_socket_address(rest_api_address.ip(), metrics_port, Some(node_index))
        } else {
            metrics_socket_address(rest_api_address.ip(), metrics_port, None)
        };

        let wallet_path = if let Some(final_directory) = set_config_dir {
            let wallet_path = wallets[i].config.path();
            replace_keystore_path(wallet_path, final_directory)
                .context("replacing the keystore path failed")?;
            final_directory.join(wallet_path.file_name().expect("file name should exist"))
        } else {
            wallets[i].config.path().to_path_buf()
        };

        let contract_config = testbed_config.system_ctx.contract_config();

        let sui = Some(SuiConfig {
            rpc: rpc.clone(),
            contract_config,
            event_polling_interval: defaults::polling_interval(),
            wallet_config: wallet_path,
            backoff_config: ExponentialBackoffConfig::default(),
            gas_budget: defaults::gas_budget(),
        });

        let storage_path = set_db_path
            .map(|path| path.to_path_buf())
            .or(set_config_dir.map(|path| path.join(&name)))
            .unwrap_or_else(|| working_dir.join(&name));

        storage_node_configs.push(StorageNodeConfig {
            name: node.name.clone(),
            storage_path,
            blocklist_path: None,
            protocol_key_pair: node.keypair.into(),
            network_key_pair: node.network_keypair.into(),
            public_host: node.network_address.get_host().to_owned(),
            public_port: node.network_address.try_get_port()?.context(format!(
                "network address without port: {}",
                node.network_address
            ))?,
            metrics_address,
            rest_api_address,
            sui,
            db_config: Default::default(),
            rest_graceful_shutdown_period_secs: None,
            blob_recovery: Default::default(),
            tls: Default::default(),
            shard_sync_config: Default::default(),
            event_processor_config: Default::default(),
            use_legacy_event_provider,
            disable_event_blob_writer,
            commission_rate: node.commission_rate,
            voting_params: VotingParams {
                storage_price: node.storage_price,
                write_price: node.write_price,
                node_capacity: node.node_capacity,
            },
            metrics_push: None,
            metadata: Default::default(),
        });
    }

    let contract_clients = join_all(wallets.into_iter().map(|wallet| async {
        testbed_config
            .system_ctx
            .new_contract_client(
                wallet,
                ExponentialBackoffConfig::default(),
                DEFAULT_GAS_BUDGET,
            )
            .await
            .expect("should not fail")
    }))
    .await;
    assert_eq!(node_params.len(), contract_clients.len());

    let amounts_to_stake = vec![1_000 * 1_000_000_000; node_params.len()];

    register_committee_and_stake(
        admin_wallet,
        &testbed_config.system_ctx,
        &node_params,
        &protocol_keypairs,
        &contract_clients.iter().collect::<Vec<_>>(),
        1_000_000_000_000,
        &amounts_to_stake,
    )
    .await?;

    end_epoch_zero(
        contract_clients
            .first()
            .expect("there should be at least one storage node"),
    )
    .await?;

    Ok(storage_node_configs)
}

#[tracing::instrument(err)]
fn replace_keystore_path(wallet_path: &Path, new_directory: &Path) -> anyhow::Result<()> {
    let reader = std::fs::File::open(wallet_path)?;
    let mut wallet_contents: serde_yaml::Mapping = serde_yaml::from_reader(reader)?;
    let keystore_path = wallet_contents
        .get_mut("keystore")
        .expect("keystore to exist in wallet config")
        .get_mut("File")
        .ok_or_else(|| anyhow!("keystore path is not set"))?;
    *keystore_path = new_directory
        .join(
            Path::new(
                keystore_path
                    .as_str()
                    .ok_or_else(|| anyhow!("path could not be converted to str"))?,
            )
            .file_name()
            .expect("file name to be set"),
        )
        .to_str()
        .ok_or_else(|| anyhow!("path could not be converted to str"))?
        .into();
    let serialized_config = serde_yaml::to_string(&wallet_contents)?;
    fs::write(wallet_path, serialized_config)?;
    Ok(())
}

async fn create_storage_node_wallets(
    working_dir: &Path,
    n_nodes: NonZeroU16,
    sui_network: SuiNetwork,
    faucet_cooldown: Option<Duration>,
) -> anyhow::Result<Vec<WalletContext>> {
    // Create wallets for the storage nodes
    let mut storage_node_wallets = (0..n_nodes.get())
        .map(|index| {
            let name = node_config_name_prefix(index, n_nodes);
            let wallet_path = working_dir.join(format!("{}-sui.yaml", name));
            create_wallet(
                &wallet_path,
                sui_network.env(),
                Some(&format!("{}.keystore", name)),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

    print_wallet_addresses(&mut storage_node_wallets)?;

    let sui_client = storage_node_wallets[0].get_client().await?;
    // Get coins from faucet for the wallets.
    for wallet in storage_node_wallets.iter_mut() {
        if let Some(cooldown) = faucet_cooldown {
            tracing::info!(
                "sleeping for {} to let faucet cool down",
                humantime::Duration::from(cooldown)
            );
            tokio::time::sleep(cooldown).await;
        }
        request_sui_from_faucet(wallet.active_address()?, &sui_network, &sui_client).await?;
    }
    Ok(storage_node_wallets)
}

fn print_wallet_addresses(wallets: &mut [WalletContext]) -> anyhow::Result<()> {
    println!("Wallet addresses:");
    for wallet in wallets.iter_mut() {
        println!("{}", wallet.active_address()?);
    }
    // Try to flush output
    let _ = std::io::stdout().flush();
    Ok(())
}
