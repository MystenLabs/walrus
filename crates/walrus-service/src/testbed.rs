// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{num::NonZeroU16, path::Path, time::Duration};

use fastcrypto::traits::KeyPair;
use rand::{rngs::StdRng, SeedableRng};
use walrus_core::{ProtocolKeyPair, ShardIndex};
use walrus_sui::types::StorageNode as SuiStorageNode;

use crate::{
    client,
    config::{self, PathOrInPlace, StorageNodeConfig},
};

/// Prefix for the node configuration file name.
pub fn node_config_name_prefix(node_index: u16, committee_size: NonZeroU16) -> String {
    format!(
        "dryrun-node-{node_index:00$}",
        (committee_size.get() - 1).ilog10() as usize + 1
    )
}

/// Configuration for the testbed.
pub fn testbed_configs(
    working_dir: &Path,
    committee_size: NonZeroU16,
    n_shards: NonZeroU16,
    source_symbols_primary: NonZeroU16,
    source_symbols_secondary: NonZeroU16,
) -> (Vec<StorageNodeConfig>, client::Config) {
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

    // Print the client config.
    let client_config = client::Config {
        committee: walrus_sui::types::Committee {
            members: sui_storage_node_configs,
            epoch: 0,
            total_weight: n_shards.get(),
        },
        source_symbols_primary,
        source_symbols_secondary,
        concurrent_requests: committee_size.get().into(),
        connection_timeout: Duration::from_secs(10),
    };

    (storage_node_configs, client_config)
}
