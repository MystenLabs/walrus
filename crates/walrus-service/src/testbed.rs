// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Facilities to deploy a demo testbed.

use std::{
    collections::HashSet,
    fs,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZeroU16,
    path::{Path, PathBuf},
};

use fastcrypto::traits::KeyPair;
use futures::future::try_join_all;
use rand::{rngs::StdRng, SeedableRng};
use sui_sdk::wallet_context::WalletContext;
use sui_types::base_types::ObjectID;
use walrus_core::{keys::ProtocolKeyPair, ShardIndex};
use walrus_sui::{
    system_setup::{create_system_object, publish_package, SystemParameters},
    types::{Committee, StorageNode as SuiStorageNode},
    utils::{create_wallet, request_sui_from_faucet, SuiNetwork},
};

use crate::{
    client::{self, ClientCommunicationConfig},
    config::{defaults, PathOrInPlace, StorageNodeConfig, SuiConfig, TestbedConfig},
};

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
pub fn benchmark_keypairs(n: usize) -> Vec<ProtocolKeyPair> {
    let mut rng = StdRng::seed_from_u64(0);
    (0..n)
        .map(|_| ProtocolKeyPair::generate_with_rng(&mut rng))
        .collect()
}

/// Formats the metrics address for a node. If the node index is provided, the port is adjusted
/// to ensure uniqueness across nodes.
pub fn format_metrics_address(ip: Ipv4Addr, port: u16, node_index: Option<u16>) -> SocketAddr {
    let port = port + node_index.unwrap_or(0);
    SocketAddr::new(IpAddr::V4(ip), port)
}

/// Formats the REST API address for a node. If both the node index and the committee size is
/// provided, the port is adjusted to ensure uniqueness across nodes.
pub fn format_rest_api_address(
    ip: Ipv4Addr,
    mut port: u16,
    node_index: Option<u16>,
    committee_size: Option<u16>,
) -> SocketAddr {
    if let (Some(node_index), Some(committee_size)) = (node_index, committee_size) {
        port += committee_size + node_index
    }
    SocketAddr::new(IpAddr::V4(ip), port)
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

#[allow(clippy::too_many_arguments)] // Todo: Refactor configs #377
/// Create and deploy a Walrus contract.
pub async fn deploy_walrus_contract(
    working_dir: &Path,
    sui_network: SuiNetwork,
    contract_path: PathBuf,
    gas_budget: u64,
    shards_information: Vec<Vec<ShardIndex>>,
    ips: Vec<Ipv4Addr>,
    rest_api_port: u16,
) -> anyhow::Result<TestbedConfig> {
    assert!(
        shards_information.len() == ips.len(),
        "Mismatch in the number of shards and IPs"
    );

    // Check whether the testbed collocates the storage nodes on the same machine
    // (that is, local testbed).
    let ip_set = ips.iter().collect::<HashSet<_>>();
    let collocated = ip_set.len() != ips.len();

    // Build one Sui storage node config for each storage node.
    let committee_size = ips.len() as u16;
    let mut keypairs = benchmark_keypairs(committee_size as usize);
    let mut sui_storage_nodes = Vec::new();

    for (i, ((keypair, shard_ids), ip)) in keypairs
        .iter_mut()
        .zip(shards_information.into_iter())
        .zip(ips.iter().cloned())
        .enumerate()
    {
        let node_index = i as u16;
        let name = node_config_name_prefix(node_index, NonZeroU16::new(committee_size).unwrap());
        let public_key = keypair.as_ref().public().clone();
        let rest_api_address = if collocated {
            format_rest_api_address(ip, rest_api_port, Some(node_index), Some(committee_size))
        } else {
            format_rest_api_address(ip, rest_api_port, None, None)
        };

        sui_storage_nodes.push(SuiStorageNode {
            name,
            network_address: rest_api_address.into(),
            public_key,
            shard_ids: shard_ids.clone(),
        });
    }

    // Create the working directory if it does not exist
    fs::create_dir_all(working_dir).expect("Failed to create working directory");

    // Create wallet for publishing contracts on sui and setting up system object
    let mut admin_wallet = create_wallet(
        &working_dir.join("sui_admin.yaml"),
        sui_network.env(),
        Some("sui_admin.keystore"),
    )?;

    // Get coins from faucet for the wallets.
    let sui_client = admin_wallet.get_client().await?;
    let mut faucet_requests = Vec::with_capacity(4);
    for _ in 0..2 {
        faucet_requests.push(request_sui_from_faucet(
            admin_wallet.active_address()?,
            sui_network,
            &sui_client,
        ));
    }
    try_join_all(faucet_requests).await?;

    // Publish package and set up system object
    let (pkg_id, committee_cap) =
        publish_package(&mut admin_wallet, contract_path, gas_budget).await?;
    let committee = Committee::new(sui_storage_nodes, 0)?;
    let system_params = SystemParameters::new_with_sui(committee, 1_000_000_000_000, 10);
    let system_object = create_system_object(
        &mut admin_wallet,
        pkg_id,
        committee_cap,
        &system_params,
        gas_budget,
    )
    .await?;

    Ok(TestbedConfig {
        sui_network,
        ips,
        rest_api_port,
        pkg_id,
        system_object,
    })
}

/// Create client configurations for the testbed.
pub async fn create_client_config(
    pkg_id: ObjectID,
    system_object: ObjectID,
    working_dir: &Path,
    sui_network: SuiNetwork,
) -> anyhow::Result<client::Config> {
    // Create the working directory if it does not exist
    fs::create_dir_all(working_dir).expect("Failed to create working directory");

    // Create wallet for the client
    let client_wallet_path = working_dir.join("sui_client.yaml");
    let mut client_wallet = create_wallet(
        &working_dir.join("sui_client.yaml"),
        sui_network.env(),
        Some("sui_client.keystore"),
    )?;

    // Get coins from faucet for the wallets.
    let sui_client = client_wallet.get_client().await?;
    let mut faucet_requests = Vec::with_capacity(4);
    for _ in 0..2 {
        faucet_requests.push(request_sui_from_faucet(
            client_wallet.active_address()?,
            sui_network,
            &sui_client,
        ))
    }

    try_join_all(faucet_requests).await?;

    // Create the client config.
    let client_config = client::Config {
        system_pkg: pkg_id,
        system_object,
        wallet_config: Some(std::fs::canonicalize(client_wallet_path)?),
        communication_config: ClientCommunicationConfig::default(),
    };

    Ok(client_config)
}

/// Create storage node configurations for the testbed.
pub async fn create_storage_node_configs(
    working_dir: &Path,
    testbed_config: TestbedConfig,
    metrics_port: u16,
) -> anyhow::Result<Vec<StorageNodeConfig>> {
    let ips = testbed_config.ips;
    let rest_api_port = testbed_config.rest_api_port;

    // Check whether the testbed collocates the storage nodes on the same machine
    // (that is, local testbed).
    let ip_set = ips.iter().collect::<HashSet<_>>();
    let collocated = ip_set.len() != ips.len();

    // Build one Sui storage node config for each storage node.
    let committee_size = ips.len() as u16;
    let keypairs = benchmark_keypairs(committee_size as usize);
    let wallets = create_storage_node_wallets(
        working_dir,
        NonZeroU16::new(committee_size).expect("committee size must be > 0"),
        testbed_config.sui_network,
    )
    .await?;
    let rpc = wallets[0].config.get_active_env()?.rpc.clone();
    let mut storage_node_configs = Vec::new();
    for (i, (keypair, ip)) in keypairs.into_iter().zip(ips.into_iter()).enumerate() {
        let node_index = i as u16;
        let name = node_config_name_prefix(node_index, NonZeroU16::new(committee_size).unwrap());

        let (rest_api_address, metrics_address) = if collocated {
            (
                format_rest_api_address(ip, rest_api_port, Some(node_index), Some(committee_size)),
                format_metrics_address(ip, metrics_port, Some(node_index)),
            )
        } else {
            (
                format_rest_api_address(ip, rest_api_port, None, None),
                format_metrics_address(ip, metrics_port, None),
            )
        };

        let sui = Some(SuiConfig {
            rpc: rpc.clone(),
            pkg_id: testbed_config.pkg_id,
            system_object: testbed_config.system_object,
            event_polling_interval: defaults::polling_interval(),
            wallet_config: wallets[i].config.path().canonicalize()?,
            gas_budget: defaults::gas_budget(),
        });

        storage_node_configs.push(StorageNodeConfig {
            storage_path: working_dir.join(&name),
            protocol_key_pair: PathOrInPlace::InPlace(keypair),
            metrics_address,
            rest_api_address,
            sui,
        });
    }
    Ok(storage_node_configs)
}

async fn create_storage_node_wallets(
    working_dir: &Path,
    n_nodes: NonZeroU16,
    sui_network: SuiNetwork,
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

    let sui_client = storage_node_wallets[0].get_client().await?;
    // Get coins from faucet for the wallets.
    let mut faucet_requests = Vec::with_capacity(storage_node_wallets.len());
    for wallet in storage_node_wallets.iter_mut() {
        for _ in 0..2 {
            faucet_requests.push(request_sui_from_faucet(
                wallet.active_address()?,
                sui_network,
                &sui_client,
            ))
        }
    }
    try_join_all(faucet_requests).await?;
    Ok(storage_node_wallets)
}
