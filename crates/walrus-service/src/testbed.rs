// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Facilities to deploy a demo testbed.

use std::{
    fs,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZeroU16,
    path::{Path, PathBuf},
    time::Duration,
};

use fastcrypto::traits::KeyPair;
use futures::future::try_join_all;
use rand::{rngs::StdRng, SeedableRng};
use sui_types::base_types::ObjectID;
use walrus_core::{keys::ProtocolKeyPair, ShardIndex};
use walrus_sui::{
    system_setup::{create_system_object, publish_package, SystemParameters},
    types::{Committee, StorageNode as SuiStorageNode},
    utils::{create_wallet, request_sui_from_faucet, SuiNetwork},
};

use crate::{
    client::{self, ClientCommunicationConfig},
    config::{self, PathOrInPlace, StorageNodeConfig, SuiConfig},
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

/// Deterministically generate storage node configurations without a `SuiConfig`.
///
/// The storage node configurations are created for a total of `committee_size` nodes and
/// `n_shards`.
pub fn create_local_storage_node_configs(
    working_dir: &Path,
    committee_size: NonZeroU16,
    n_shards: NonZeroU16,
) -> (Vec<StorageNodeConfig>, Vec<SuiStorageNode>) {
    let mut rng = StdRng::seed_from_u64(0);
    let mut storage_node_configs = Vec::new();

    // Generate all storage node configs from a seed.
    let shards_per_node = n_shards.get() / committee_size.get();
    let remainder_shards = n_shards.get() % committee_size.get();
    let mut start = 0;
    let mut sui_storage_node_configs = Vec::with_capacity(committee_size.get().into());
    for i in 0..committee_size.get() {
        let name = node_config_name_prefix(i, committee_size);

        let protocol_key_pair = ProtocolKeyPair::generate_with_rng(&mut rng);
        let public_key = protocol_key_pair.as_ref().public().clone();

        let mut metrics_address = config::defaults::metrics_address();
        metrics_address.set_port(metrics_address.port() + i);

        let mut rest_api_address = config::defaults::rest_api_address();
        rest_api_address.set_port(metrics_address.port() + committee_size.get() + i);

        storage_node_configs.push(StorageNodeConfig {
            storage_path: working_dir.join(&name),
            protocol_key_pair: PathOrInPlace::InPlace(protocol_key_pair),
            metrics_address,
            rest_api_address,
            sui: None,
        });

        let end = if i < remainder_shards {
            start + shards_per_node + 1
        } else {
            start + shards_per_node
        };
        let shard_ids = (start..end).map(ShardIndex).collect();
        start = end;

        sui_storage_node_configs.push(SuiStorageNode {
            name,
            network_address: rest_api_address.into(),
            public_key,
            shard_ids,
        });
    }
    (storage_node_configs, sui_storage_node_configs)
}

/// Configuration for the testbed.
pub async fn testbed_configs(
    working_dir: &Path,
    committee_size: NonZeroU16,
    n_shards: NonZeroU16,
    sui_network: SuiNetwork,
    contract_path: PathBuf,
    gas_budget: u64,
) -> anyhow::Result<(Vec<StorageNodeConfig>, client::Config)> {
    let (mut storage_node_configs, sui_storage_nodes) =
        create_local_storage_node_configs(working_dir, committee_size, n_shards);

    // Create wallet for publishing contracts on sui and setting up system object
    let mut admin_wallet = create_wallet(
        &working_dir.join("sui_admin.yaml"),
        &sui_network,
        Some("sui_admin.keystore"),
    )?;

    // Create wallet for the client
    let client_wallet_path = working_dir.join("sui_client.yaml");
    let mut client_wallet = create_wallet(
        &client_wallet_path,
        &sui_network,
        Some("sui_client.keystore"),
    )?;

    let sui_client = admin_wallet.get_client().await?;
    // Get coins from faucet for the wallets.
    let mut faucet_requests = Vec::with_capacity(4);
    for wallet in [&mut admin_wallet, &mut client_wallet] {
        for _ in 0..2 {
            faucet_requests.push(request_sui_from_faucet(
                wallet.active_address()?,
                sui_network,
                &sui_client,
            ))
        }
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

    // Update the storage node configs with the sui config
    let sui_config = Some(SuiConfig {
        rpc: admin_wallet.config.get_active_env()?.rpc.clone(),
        pkg_id,
        system_object,
        event_polling_interval: config::defaults::polling_interval(),
    });
    storage_node_configs
        .iter_mut()
        .for_each(|conf| conf.sui.clone_from(&sui_config));

    // Create the client config.
    let client_config = client::Config {
        system_pkg: pkg_id,
        system_object,
        wallet_config: Some(std::fs::canonicalize(client_wallet_path)?),
        communication_config: ClientCommunicationConfig::default(),
    };

    Ok((storage_node_configs, client_config))
}

/// Generate deterministic keypairs for the benchmark purposes.
pub fn benchmark_keypairs(n: usize) -> Vec<ProtocolKeyPair> {
    let mut rng = StdRng::seed_from_u64(0);
    (0..n)
        .map(|_| ProtocolKeyPair::generate_with_rng(&mut rng))
        .collect()
}

/// Create and deploy a Walrus contract.
pub async fn deploy_walrus_contract(
    working_dir: &Path,
    sui_network: SuiNetwork,
    contract_path: PathBuf,
    gas_budget: u64,
    shards_information: Vec<Vec<ShardIndex>>,
    ips: Vec<Ipv4Addr>,
    event_polling_interval: Duration,
) -> anyhow::Result<SuiConfig> {
    assert!(
        shards_information.len() == ips.len(),
        "Mismatch in the number of shards and IPs"
    );
    let committee_size = ips.len();
    let mut keypairs = benchmark_keypairs(committee_size);
    let mut sui_storage_nodes = Vec::new();

    for (i, ((keypair, shard_ids), ip)) in keypairs
        .iter_mut()
        .zip(shards_information.into_iter())
        .zip(ips.into_iter())
        .enumerate()
    {
        let name =
            node_config_name_prefix(i as u16, NonZeroU16::new(committee_size as u16).unwrap());
        let public_key = keypair.as_ref().public().clone();
        let rest_api_address = get_rest_api_address(ip, i as u16, committee_size as u16);

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
        &sui_network,
        Some("sui_admin.keystore"),
    )?;
    let rpc = admin_wallet.config.get_active_env()?.rpc.clone();

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

    let sui_config = SuiConfig {
        rpc,
        pkg_id,
        system_object,
        event_polling_interval,
    };
    Ok(sui_config)
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
        &sui_network,
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
pub fn create_storage_node_configs(
    working_dir: &Path,
    sui_config: SuiConfig,
    ips: Vec<Ipv4Addr>,
) -> Vec<StorageNodeConfig> {
    let committee_size = ips.len();
    let keypairs = benchmark_keypairs(committee_size);
    let mut storage_node_configs = Vec::new();
    for (i, (keypair, ip)) in keypairs.into_iter().zip(ips.into_iter()).enumerate() {
        let name =
            node_config_name_prefix(i as u16, NonZeroU16::new(committee_size as u16).unwrap());

        let metrics_address = get_metrics_address(ip, i as u16);
        let rest_api_address = get_rest_api_address(ip, i as u16, committee_size as u16);

        storage_node_configs.push(StorageNodeConfig {
            storage_path: working_dir.join(&name),
            protocol_key_pair: PathOrInPlace::InPlace(keypair),
            metrics_address,
            rest_api_address,
            sui: Some(sui_config.clone()),
        });
    }
    storage_node_configs
}

/// Get the metrics address for a node.
pub fn get_metrics_address(ip: Ipv4Addr, node_index: u16) -> SocketAddr {
    let mut metrics_address = config::defaults::metrics_address();
    metrics_address.set_ip(IpAddr::V4(ip));
    metrics_address.set_port(metrics_address.port() + node_index);
    metrics_address
}

/// Get the REST API address for a node.
pub fn get_rest_api_address(ip: Ipv4Addr, node_index: u16, committee_size: u16) -> SocketAddr {
    let mut rest_api_address = config::defaults::rest_api_address();
    rest_api_address.set_ip(IpAddr::V4(ip));
    rest_api_address.set_port(rest_api_address.port() + committee_size + node_index);
    rest_api_address
}

/// Generate deterministic and even shard allocation for the benchmark purposes.
pub fn even_shards_allocation(
    n_shards: NonZeroU16,
    committee_size: NonZeroU16,
) -> Vec<Vec<ShardIndex>> {
    let shards_per_node = n_shards.get() / committee_size.get();
    let remainder_shards = n_shards.get() % committee_size.get();
    let mut start = 0;
    let mut shards_information = Vec::with_capacity(committee_size.get() as usize);
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
