// Copyright (c) Walrus Foundation
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

use anyhow::{Context, anyhow, ensure};
use futures::{StreamExt, future::join_all, stream::FuturesOrdered};
use rand::{SeedableRng, rngs::StdRng};
use serde::{Deserialize, Serialize};
use serde_with::base64::Base64;
use sui_types::base_types::ObjectID;
use walrus_core::{
    EpochCount,
    ShardIndex,
    keys::{NetworkKeyPair, ProtocolKeyPair},
};
use walrus_sdk::config::ClientCommunicationConfig;
use walrus_sui::{
    client::{
        SuiContractClient,
        retry_client::{RetriableSuiClient, retriable_sui_client::LazySuiClientBuilder},
        rpc_config::RpcFallbackConfig,
    },
    config::{WalletConfig, load_wallet_context_from_path},
    system_setup::InitSystemParams,
    test_utils::system_setup::{
        SystemContext,
        create_and_init_system,
        end_epoch_zero,
        register_committee_and_stake,
    },
    types::{
        NetworkAddress,
        NodeRegistrationParams,
        move_structs::{NodeMetadata, VotingParams},
    },
    utils::{SuiNetwork, create_wallet, get_sui_from_wallet_or_faucet, request_sui_from_faucet},
    wallet::Wallet,
};
use walrus_utils::backoff::ExponentialBackoffConfig;

use crate::{
    backup::BackupConfig,
    client::{self},
    common::config::{SuiConfig, SuiReaderConfig},
    node::{
        config::{
            LiveUploadDeferralConfig,
            PathOrInPlace,
            StorageNodeConfig,
            defaults::{self, REST_API_PORT},
        },
        consistency_check::StorageNodeConsistencyCheckConfig,
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
    /// The path to the protocol key pair.
    pub protocol_key_pair_path: Option<PathBuf>,
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
    pub exchange_object: Option<ObjectID>,
}

/// Prefix for the node configuration file name.
pub fn node_config_name_prefix(node_index: u16, committee_size: NonZeroU16) -> String {
    let width = if committee_size.get() == 1 {
        1
    } else {
        usize::try_from((committee_size.get() - 1).ilog10())
            .expect("this is smaller than `u16::MAX`")
            + 1
    };
    format!("dryrun-node-{node_index:0width$}")
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

/// Creates the REST API address for a node. If both the node index and the committee size is
/// provided, the port is adjusted to ensure uniqueness across nodes.
pub fn public_rest_api_address(
    host: String,
    port: u16,
    node_index: Option<u16>,
    committee_size: Option<NonZeroU16>,
) -> NetworkAddress {
    NetworkAddress(format!(
        "{}:{}",
        host,
        rest_api_port(port, node_index, committee_size)
    ))
}

fn rest_api_port(port: u16, node_index: Option<u16>, committee_size: Option<NonZeroU16>) -> u16 {
    if let (Some(node_index), Some(committee_size)) = (node_index, committee_size) {
        port + committee_size.get() + node_index
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
    /// The gas budget to use for deployment. If not provided, the gas budget is estimated.
    pub gas_budget: Option<u64>,
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
    /// The path to the admin wallet. If not provided, a new wallet is created and SUI will be
    /// requested from the faucet.
    pub admin_wallet_path: Option<PathBuf>,
    /// Flag to create a WAL exchange.
    pub with_wal_exchange: bool,
    /// Flag to use an existing WAL token deployment at the address specified in `Move.lock`.
    pub use_existing_wal_token: bool,
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
        admin_wallet_path,
        do_not_copy_contracts,
        with_wal_exchange,
        use_existing_wal_token,
    }: DeployTestbedContractParameters<'_>,
) -> anyhow::Result<TestbedConfig> {
    const WAL_AMOUNT_EXCHANGE: u64 = 10_000_000 * 1_000_000_000;
    // Check whether the testbed collocates the storage nodes on the same machine
    // (that is, local testbed).
    let hosts_set = hosts.iter().collect::<HashSet<_>>();
    let collocated = hosts_set.len() != hosts.len();

    tracing::debug!("storage nodes collocated on same machine: {}", collocated);
    tracing::debug!("deploying contract to Sui network '{}'", sui_network);

    // Build one Sui storage node config for each storage node.
    let committee_size = hosts.len();
    let keypairs = if deterministic_keys {
        deterministic_keypairs(committee_size)
    } else {
        random_keypairs(committee_size)
    };
    let committee_size = committee_size_from_usize(committee_size)?;

    tracing::debug!(
        "finished generating keypairs for {} storage nodes",
        committee_size
    );

    let mut node_configs = Vec::new();
    let mid = keypairs.len() / 2;

    for (i, ((keypair, network_keypair), host)) in
        keypairs.into_iter().zip(hosts.iter().cloned()).enumerate()
    {
        let node_index = i
            .try_into()
            .expect("we checked above that the number of keypairs is at most 2^16");
        let name = node_config_name_prefix(node_index, committee_size);
        let network_address = if collocated {
            public_rest_api_address(host, rest_api_port, Some(node_index), Some(committee_size))
        } else {
            public_rest_api_address(host, rest_api_port, None, None)
        };

        tracing::debug!(
            "Generating configuration for storage node {}/{}: name={}, network_address={}",
            i + 1,
            committee_size,
            name,
            network_address
        );

        // The first half of the nodes will have a protocol key pair path, the second half will not.
        let protocol_key_pair_path = if i < mid {
            Some(working_dir.join(format!("node-{node_index}.key")))
        } else {
            None
        };

        node_configs.push(TestbedNodeConfig {
            name,
            network_address: network_address.clone(),
            keypair,
            protocol_key_pair_path,
            network_keypair,
            commission_rate: 0,
            storage_price,
            write_price,
            node_capacity: storage_capacity / u64::from(committee_size.get()),
        });
    }

    tracing::debug!(
        "Finished generating configurations for {} storage nodes",
        committee_size
    );

    // Create the working directory if it does not exist
    fs::create_dir_all(working_dir).context("failed to create working directory")?;

    tracing::debug!("creating working directory at {}", working_dir.display());

    // Load or create wallet for publishing contracts on sui and setting up system object
    let admin_wallet = if let Some(admin_wallet_path) = admin_wallet_path {
        tracing::debug!(
            "loading existing admin wallet from '{}'",
            admin_wallet_path.display()
        );
        load_wallet_context_from_path(Some(&admin_wallet_path), None)?
    } else {
        tracing::debug!("creating new admin wallet in working directory");
        let mut admin_wallet = create_wallet(
            &working_dir.join(format!("{ADMIN_CONFIG_PREFIX}.yaml")),
            sui_network.env(),
            Some(&format!("{ADMIN_CONFIG_PREFIX}.keystore")),
            None,
        )
        .await?;

        // Print the wallet address.
        println!("Admin wallet address:");
        println!("{}", admin_wallet.active_address()?);
        // Try to flush output
        let _ = std::io::stdout().flush();

        let rpc_url = admin_wallet.get_rpc_url()?;

        // Get coins from faucet for the wallet.
        let sui_client = RetriableSuiClient::new(
            vec![LazySuiClientBuilder::new(rpc_url, None)],
            Default::default(),
        )?;
        request_sui_from_faucet(admin_wallet.active_address()?, &sui_network, &sui_client).await?;
        admin_wallet
    };

    let deploy_directory = if do_not_copy_contracts {
        None
    } else {
        Some(working_dir.join("contracts"))
    };

    let (system_ctx, contract_client) = create_and_init_system(
        admin_wallet,
        InitSystemParams {
            n_shards,
            epoch_zero_duration,
            epoch_duration,
            max_epochs_ahead,
            contract_dir,
            deploy_directory,
            use_existing_wal_token,
            with_wal_exchange,
            with_credits: false,
            with_walrus_subsidies: true,
        },
        gas_budget,
    )
    .await?;

    tracing::debug!(
        "Successfully created and initialized system context with {} shards",
        n_shards
    );

    tracing::debug!("retrieved contract configuration from system context");

    let exchange_object = if let Some(wal_exchange_pkg_id) = system_ctx.wal_exchange_pkg_id {
        // Create WAL exchange.
        // TODO(WAL-520): create multiple exchange objects
        Some(
            contract_client
                .create_and_fund_exchange(wal_exchange_pkg_id, WAL_AMOUNT_EXCHANGE)
                .await?,
        )
    } else {
        None
    };

    tracing::debug!(
        "Successfully created WAL exchange object: {}",
        exchange_object
            .map(|id| id.to_string())
            .unwrap_or_else(|| "None".to_string())
    );

    println!(
        "Walrus contract created:\n\
            package_id: {}\n\
            system_object: {}\n\
            staking_object: {}\n\
            upgrade_manager_object: {}\n\
            exchange_object: {}",
        system_ctx.walrus_pkg_id,
        system_ctx.system_object,
        system_ctx.staking_object,
        system_ctx.upgrade_manager_object,
        exchange_object
            .map(|id| id.to_string())
            .unwrap_or_else(|| "None".to_string()),
    );

    Ok(TestbedConfig {
        sui_network,
        nodes: node_configs,
        system_ctx,
        exchange_object,
    })
}

/// Create client configurations for the testbed and fund the client wallet with SUI and WAL.
#[allow(clippy::too_many_arguments)]
pub async fn create_client_config(
    system_ctx: &SystemContext,
    working_dir: &Path,
    sui_network: SuiNetwork,
    set_config_dir: Option<&Path>,
    admin_contract_client: &mut SuiContractClient,
    exchange_objects: Vec<ObjectID>,
    sui_amount: u64,
    wallet_name: &str,
    sui_client_request_timeout: Option<Duration>,
) -> anyhow::Result<client::ClientConfig> {
    // Create the working directory if it does not exist
    fs::create_dir_all(working_dir).context("failed to create working directory")?;

    // Create wallet for the client
    let sui_client_wallet_path = working_dir.join(format!("{wallet_name}.yaml"));
    let mut sui_client_wallet_context = create_wallet(
        &sui_client_wallet_path,
        sui_network.env(),
        Some(&format!("{wallet_name}.keystore")),
        sui_client_request_timeout,
    )
    .await?;

    let client_address = sui_client_wallet_context.active_address()?;

    // Get Sui coins from faucet or the admin wallet.
    get_sui_from_wallet_or_faucet(
        client_address,
        admin_contract_client.wallet_mut(),
        &sui_network,
        sui_amount,
    )
    .await?;
    // Fund the client wallet with WAL.
    admin_contract_client
        .send_wal(
            1_000_000 * 1_000_000_000, // 1 million WAL
            client_address,
        )
        .await?;

    let wallet_path = if let Some(final_directory) = set_config_dir {
        replace_keystore_path(&sui_client_wallet_path, final_directory)
            .context("replacing the keystore path failed")?;
        final_directory.join(
            sui_client_wallet_path
                .file_name()
                .expect("file name should exist"),
        )
    } else {
        sui_client_wallet_path
    };

    let contract_config = system_ctx.contract_config();

    // Create the client config.
    let client_config = client::ClientConfig {
        contract_config,
        exchange_objects,
        wallet_config: Some(WalletConfig::from_path(wallet_path)),
        rpc_urls: vec![],
        communication_config: ClientCommunicationConfig {
            sui_client_request_timeout,
            ..Default::default()
        },
        refresh_config: Default::default(),
        quilt_client_config: Default::default(),
        byte_range_read_client_config: Default::default(),
    };

    Ok(client_config)
}

/// Create the config for the walrus-backup node associated with a network.
pub fn create_backup_config(
    system_ctx: &SystemContext,
    working_dir: &Path,
    database_url: &str,
    mut rpc_urls: Vec<String>,
    rpc_fallback_config: Option<RpcFallbackConfig>,
) -> anyhow::Result<BackupConfig> {
    if rpc_urls.is_empty() {
        return Err(anyhow!("No RPC URLs provided"));
    }
    Ok(BackupConfig::new_with_defaults(
        working_dir.join("backup"),
        SuiReaderConfig {
            rpc: rpc_urls.remove(0),
            additional_rpc_endpoints: rpc_urls,
            contract_config: system_ctx.contract_config(),
            backoff_config: ExponentialBackoffConfig::default(),
            event_polling_interval: defaults::polling_interval(),
            rpc_fallback_config,
            request_timeout: None,
        },
        database_url.to_string(),
    ))
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
    rpc_fallback_config: Option<RpcFallbackConfig>,
    admin_contract_client: &mut SuiContractClient,
    disable_event_blob_writer: bool,
    sui_amount: u64,
    sui_client_request_timeout: Option<Duration>,
) -> anyhow::Result<Vec<StorageNodeConfig>> {
    tracing::debug!(
        ?working_dir,
        ?listening_ips,
        metrics_port,
        ?set_config_dir,
        ?set_db_path,
        ?faucet_cooldown,
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
    let committee_size = committee_size_from_usize(nodes.len())?;
    let wallets = create_storage_node_wallets(
        working_dir,
        committee_size,
        testbed_config.sui_network,
        faucet_cooldown,
        admin_contract_client.wallet_mut(),
        sui_amount,
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

    let rpc = wallets[0].get_active_env()?.rpc.clone();
    let mut storage_node_configs = Vec::new();
    for (i, (node, rest_api_address)) in nodes.into_iter().zip(rest_api_addrs).enumerate() {
        let node_index = i
            .try_into()
            .expect("we checked above that the number of nodes is at most 2^16");
        let name = node_config_name_prefix(node_index, committee_size);

        let metrics_address = if collocated {
            metrics_socket_address(rest_api_address.ip(), metrics_port, Some(node_index))
        } else {
            metrics_socket_address(rest_api_address.ip(), metrics_port, None)
        };

        let wallet_path = if let Some(final_directory) = set_config_dir {
            let wallet_path = wallets[i].get_config_path();
            replace_keystore_path(wallet_path, final_directory)
                .context("replacing the keystore path failed")?;
            final_directory.join(wallet_path.file_name().expect("file name should exist"))
        } else {
            wallets[i].get_config_path().to_path_buf()
        };

        let contract_config = testbed_config.system_ctx.contract_config();

        let sui = Some(SuiConfig {
            rpc: rpc.clone(),
            contract_config,
            event_polling_interval: defaults::polling_interval(),
            wallet_config: WalletConfig::from_path(wallet_path),
            backoff_config: ExponentialBackoffConfig::default(),
            gas_budget: None,
            rpc_fallback_config: rpc_fallback_config.clone(),
            additional_rpc_endpoints: vec![],
            request_timeout: sui_client_request_timeout,
        });

        let storage_path = set_db_path
            .map(|path| path.to_path_buf())
            .or(set_config_dir.map(|path| path.join(&name)))
            .unwrap_or_else(|| working_dir.join(&name));

        let protocol_key_pair = if let Some(path) = &node.protocol_key_pair_path {
            fs::write(path, node.keypair.to_base64().as_bytes())
                .context("Failed to write protocol key pair")?;
            if let Some(set_config_dir) = set_config_dir {
                PathOrInPlace::from_path(
                    set_config_dir.join(path.file_name().expect("file name should exist")),
                )
            } else {
                PathOrInPlace::from_path(path)
            }
        } else {
            node.keypair.into()
        };
        storage_node_configs.push(StorageNodeConfig {
            name: node.name.clone(),
            storage_path,
            blocklist_path: None,
            protocol_key_pair,
            next_protocol_key_pair: None,
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
            rest_server: Default::default(),
            rest_graceful_shutdown_period_secs: None,
            blob_recovery: Default::default(),
            tls: Default::default(),
            shard_sync_config: Default::default(),
            event_processor_config: Default::default(),
            pending_sliver_cache: Default::default(),
            pending_metadata_cache: Default::default(),
            disable_event_blob_writer,
            commission_rate: node.commission_rate,
            voting_params: VotingParams {
                storage_price: node.storage_price,
                write_price: node.write_price,
                node_capacity: node.node_capacity,
            },
            metrics_push: None,
            metadata: Default::default(),
            config_synchronizer: Default::default(),
            storage_node_cap: None,
            num_uncertified_blob_threshold: Some(10),
            balance_check: Default::default(),
            thread_pool: Default::default(),
            consistency_check: StorageNodeConsistencyCheckConfig::default_for_test(),
            checkpoint_config: Default::default(),
            live_upload_deferral: LiveUploadDeferralConfig::default_for_test(),
            admin_socket_path: Some(working_dir.join(format!("admin-{node_index}.sock"))),
            node_recovery_config: Default::default(),
            blob_event_processor_config: Default::default(),
            garbage_collection: Default::default(),
        });
    }

    let contract_clients = join_all(wallets.into_iter().map(|wallet| async {
        let rpc_urls = &[wallet
            .get_rpc_url()
            .expect("wallet environment should contain an rpc url")];

        testbed_config
            .system_ctx
            .new_contract_client(wallet, rpc_urls, ExponentialBackoffConfig::default(), None)
            .await
            .expect("should not fail")
    }))
    .await;
    assert_eq!(node_params.len(), contract_clients.len());

    let amounts_to_stake = vec![1_000 * 1_000_000_000; node_params.len()];

    let storage_node_caps = register_committee_and_stake(
        admin_contract_client,
        &node_params,
        &protocol_keypairs,
        &contract_clients.iter().collect::<Vec<_>>(),
        &amounts_to_stake,
        Some(10),
    )
    .await?;

    for (config, node_cap) in storage_node_configs
        .iter_mut()
        .zip(storage_node_caps.iter())
    {
        config.storage_node_cap = Some(node_cap.id);
    }

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

fn committee_size_from_usize(n: usize) -> Result<NonZeroU16, anyhow::Error> {
    NonZeroU16::new(
        n.try_into()
            .map_err(|_| anyhow!("committee size is too large: {} > {}", n, u16::MAX))?,
    )
    .ok_or_else(|| anyhow!("committee size must be > 0"))
}

async fn create_storage_node_wallets(
    working_dir: &Path,
    n_nodes: NonZeroU16,
    sui_network: SuiNetwork,
    faucet_cooldown: Option<Duration>,
    admin_wallet: &mut Wallet,
    sui_amount: u64,
) -> anyhow::Result<Vec<Wallet>> {
    // Create wallets for the storage nodes.
    // Use FuturesOrdered to ensure that the wallets are returned in the same order as the
    // node index.
    let mut futures = FuturesOrdered::new();
    for index in 0..n_nodes.get() {
        let name = node_config_name_prefix(index, n_nodes);
        let wallet_path = working_dir.join(format!("{name}-sui.yaml"));
        let env = sui_network.env();
        let keystore = format!("{name}.keystore");
        futures.push_back(async move {
            let wallet = create_wallet(&wallet_path, env, Some(&keystore), None).await?;
            Ok::<Wallet, anyhow::Error>(wallet)
        });
    }

    let mut storage_node_wallets = Vec::with_capacity(n_nodes.get() as usize);
    while let Some(result) = futures.next().await {
        let wallet = result?;
        storage_node_wallets.push(wallet);
    }

    print_wallet_addresses(&mut storage_node_wallets)?;

    // Get coins from faucet for the wallets.
    for wallet in storage_node_wallets.iter_mut() {
        if let Some(cooldown) = faucet_cooldown {
            tracing::info!(
                "sleeping for {} to let faucet cool down",
                humantime::Duration::from(cooldown)
            );
            tokio::time::sleep(cooldown).await;
        }
        get_sui_from_wallet_or_faucet(
            wallet.active_address()?,
            admin_wallet,
            &sui_network,
            sui_amount,
        )
        .await?;
    }
    Ok(storage_node_wallets)
}

fn print_wallet_addresses(wallets: &mut [Wallet]) -> anyhow::Result<()> {
    println!("Wallet addresses:");
    for wallet in wallets.iter_mut() {
        println!("{}", wallet.active_address()?);
    }
    // Try to flush output
    let _ = std::io::stdout().flush();
    Ok(())
}
