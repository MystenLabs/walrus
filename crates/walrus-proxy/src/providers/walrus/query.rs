// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashSet,
    fmt::{self, Display},
    hash::Hash,
    path::PathBuf,
    vec,
};

use anyhow::{Context, Error};
use serde::{Deserialize, Serialize};
use sui_types::base_types::ObjectID;
use walrus_sui::{
    client::{CommitteesAndState, ReadClient, contract_config::ContractConfig},
    types::Committee,
};
use walrus_utils::backoff::ExponentialBackoffConfig;

use crate::NetworkPublicKey;

/// NodeInfo represents a node we discovered that is a member of the staking
/// committee
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    /// name of the node, can be anything
    pub name: String,
    /// the dns or ip address of the node with port number
    pub network_address: String,
    /// the pubkey stored on chain
    pub network_public_key: NetworkPublicKey,
}

impl Display for NodeInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "NodeInfo{{name: {}, network_address: {}, network_public_key: {}}}",
            self.name, self.network_address, self.network_public_key
        )
    }
}

/// Merges the Committee types into a hashset of NodeInfo and then returns a vec of it.
/// We use previous, current and next epoch members and return a unique vec of them.
pub fn merge_committee_nodes_across_epochs(committees_state: &CommitteesAndState) -> Vec<NodeInfo> {
    let committees: Vec<&Committee> = vec![
        Some(&committees_state.current),
        committees_state.previous.as_ref(),
        committees_state.next.as_ref(),
    ]
    .into_iter()
    .flatten()
    .collect();

    committees
        .into_iter()
        .flat_map(|committee| committee.members())
        .map(|storage_node| NodeInfo {
            name: storage_node.name.clone(),
            network_address: storage_node.network_address.to_string(),
            network_public_key: storage_node.network_public_key.clone(),
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect()
}

/// Loads the allowlist from a file and returns a vec of NodeInfo.
pub fn load_allowlist_from_file(allowlist_path: &Option<PathBuf>) -> Result<Vec<NodeInfo>, Error> {
    if let Some(path) = allowlist_path {
        tracing::info!("loading allowlist from file: {:?}", path);
        // Read file in yaml format which stores a list of name, network_address, network_public_key
        let nodes: Vec<NodeInfo> = serde_yaml::from_reader(
            std::fs::File::open(path).context(format!("cannot open {path:?}"))?,
        )?;
        for node in nodes.iter() {
            tracing::info!(
                "loaded node info from file: {:} {:} {:}",
                node.name,
                node.network_address,
                node.network_public_key
            );
        }
        Ok(nodes)
    } else {
        Ok(Vec::new())
    }
}

/// Returns the [`NodeInfo`] for all nodes that are part of the last, current,
/// or next Walrus committee.
pub async fn get_walrus_nodes(
    rpc_address: &str,
    system_object_id: &str,
    staking_object_id: &str,
    allowlist_path: &Option<PathBuf>,
) -> Result<Vec<NodeInfo>, Error> {
    let contract_config = ContractConfig::new_with_subsidies(
        ObjectID::from_hex_literal(system_object_id)?,
        ObjectID::from_hex_literal(staking_object_id)?,
        None,
    );
    let backoff_config = ExponentialBackoffConfig::default();
    let c: walrus_sui::client::SuiReadClient = walrus_sui::client::SuiReadClient::new_for_rpc_urls(
        &[rpc_address],
        &contract_config,
        backoff_config,
    )
    .await
    .map_err(|e| {
        tracing::error!("unable to create walrus-sui client");
        dbg!(e)
    })?;
    let cas = c.get_committees_and_state().await.map_err(|e| {
        tracing::error!("unable to get committees and state data via rpc");
        dbg!(e)
    })?;

    // Load allowlist from file first so that if there are nodes on chain using the same public key,
    // the on chain information will take precedence.
    let mut nodes = load_allowlist_from_file(allowlist_path)?;
    nodes.extend(merge_committee_nodes_across_epochs(&cas));
    Ok(nodes)
}
