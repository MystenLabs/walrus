// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    num::NonZeroU16,
    path::{Path, PathBuf},
    time::Duration,
};

use fastcrypto::traits::KeyPair;
use futures::future::try_join_all;
use rand::{rngs::StdRng, SeedableRng};
use walrus_core::{ProtocolKeyPair, ShardIndex};
use walrus_sui::{
    system_setup::{create_system_object, publish_package, SystemParameters},
    types::{Committee, StorageNode as SuiStorageNode},
    utils::{create_wallet, request_sui_from_faucet, SuiNetwork},
};

use crate::{
    client,
    config::{self, PathOrInPlace, StorageNodeConfig, SuiConfig},
};

/// Prefix for the node configuration file name.
pub fn node_config_name_prefix(node_index: u16, committee_size: NonZeroU16) -> String {
    format!(
        "dryrun-node-{node_index:00$}",
        (committee_size.get() - 1).ilog10() as usize + 1
    )
}

/// Deterministically generate storage node configurations without a `SuiConfig` for a
/// total of `committee_size` nodes and `n_shards`.
pub fn create_storage_node_configs(
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
        create_storage_node_configs(working_dir, committee_size, n_shards);

    // Create wallet for publishing contracts on sui and setting up system object
    let mut admin_wallet = create_wallet(
        &working_dir.join("sui_admin.yaml"),
        &sui_network,
        Some("sui_admin.keystore"),
    )?;

    // Create wallet for the client
    let mut client_wallet = create_wallet(
        &working_dir.join("sui_client.yaml"),
        &sui_network,
        Some("sui_client.keystore"),
    )?;

    let sui_client = admin_wallet.get_client().await?;
    // Get coins from faucet for the wallets.
    let faucet_requests = [
        request_sui_from_faucet(admin_wallet.active_address()?, sui_network, &sui_client),
        request_sui_from_faucet(client_wallet.active_address()?, sui_network, &sui_client),
    ];
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
        .for_each(|conf| conf.sui = sui_config.clone());

    // Create the client config.
    let client_config = client::Config {
        concurrent_requests: committee_size.get().into(),
        connection_timeout: Duration::from_secs(10),
        system_pkg: pkg_id,
        system_object,
    };

    Ok((storage_node_configs, client_config))
}
