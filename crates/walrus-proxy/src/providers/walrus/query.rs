// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashSet, hash::Hash, vec};

use anyhow::Error;
use fastcrypto::traits::ToFromBytes;
use sui_types::base_types::ObjectID;
use tracing::error;
use walrus_sui::{
    client::{contract_config::ContractConfig, CommitteesAndState, ReadClient},
    types::Committee,
};
use walrus_utils::backoff::ExponentialBackoffConfig;

/// NodeInfo represents a node we discovered that is a member of the staking
/// committee
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct NodeInfo {
    /// name of the node, can be anything
    pub name: String,
    /// the dns or ip address of the node with port number
    pub network_address: String,
    /// the pubkey stored on chain
    pub network_public_key: Vec<u8>,
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
            network_public_key: storage_node.network_public_key.as_bytes().to_vec(),
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect()
}

/// Returns the [`NodeInfo`] for all nodes that are part of the last, current,
/// or next Walrus committee.
pub async fn get_walrus_nodes(
    rpc_address: &str,
    system_object_id: &str,
    staking_object_id: &str,
) -> Result<Vec<NodeInfo>, Error> {
    let contract_config = ContractConfig::new_with_subsidies(
        ObjectID::from_hex_literal(system_object_id)?,
        ObjectID::from_hex_literal(staking_object_id)?,
        None,
    );
    let backoff_config = ExponentialBackoffConfig::default();
    let c: walrus_sui::client::SuiReadClient = walrus_sui::client::SuiReadClient::new_for_rpc(
        rpc_address,
        &contract_config,
        backoff_config,
    )
    .await
    .map_err(|e| {
        error!("unable to create walrus-sui client");
        dbg!(e)
    })?;
    let cas = c.get_committees_and_state().await.map_err(|e| {
        error!("unable to get committees and state data via rpc");
        dbg!(e)
    })?;
    let nodes = merge_committee_nodes_across_epochs(&cas);
    Ok(nodes)
}
