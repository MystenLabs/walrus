// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Utilities to publish the walrus contracts and deploy a system object for testing.

use std::{iter, path::PathBuf, str::FromStr};

use anyhow::{anyhow, Result};
use sui_sdk::{
    rpc_types::{SuiExecutionStatus, SuiObjectDataOptions, SuiTransactionBlockEffectsAPI},
    types::base_types::ObjectID,
    wallet_context::WalletContext,
};
use sui_types::{
    base_types::SuiAddress,
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    transaction::TransactionData,
    Identifier,
    TypeTag,
    SUI_FRAMEWORK_PACKAGE_ID,
};

use super::DEFAULT_GAS_BUDGET;
use crate::{
    client::{ContractClient, ReadClient, SuiContractClient},
    system_setup::{create_system_and_staking_objects, publish_coin_and_system_package},
    types::{NodeRegistrationParams, StorageNodeCap},
};

/// Provides the default contract path for testing for the package with name `package`.
pub fn contract_path_for_testing(package: &str) -> anyhow::Result<PathBuf> {
    Ok(PathBuf::from_str(env!("CARGO_MANIFEST_DIR"))?
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("contracts")
        .join(package))
}

/// Helper struct to pass around all needed object IDs when setting up the system.
#[derive(Debug, Clone)]
pub struct SystemContext {
    /// The package ID.
    pub package_id: ObjectID,
    /// The ID of the system Object.
    pub system_obj_id: ObjectID,
    /// The ID of the staking Object.
    pub staking_obj_id: ObjectID,
    /// The ID of the WAL treasury Cap.
    pub treasury_cap: ObjectID,
}

/// Publishes the contracts and initializes the system.
///
/// Returns the package id and the object IDs of the system object and the staking object.
pub async fn create_and_init_system(
    admin_wallet: &mut WalletContext,
    n_shards: u16,
    epoch_zero_duration_ms: u64,
) -> Result<SystemContext> {
    let (package_id, cap_id, treasury_cap) = publish_coin_and_system_package(
        admin_wallet,
        contract_path_for_testing("walrus")?,
        DEFAULT_GAS_BUDGET,
    )
    .await?;

    let (system_obj_id, staking_obj_id) = create_system_and_staking_objects(
        admin_wallet,
        package_id,
        cap_id,
        n_shards,
        epoch_zero_duration_ms,
        DEFAULT_GAS_BUDGET,
    )
    .await?;
    Ok(SystemContext {
        package_id,
        system_obj_id,
        staking_obj_id,
        treasury_cap,
    })
}

/// Registers the nodes based on the provided parameters, distributes WAL to each contract client,
/// and stakes an equal amount with each storage node. Each of the contract clients will hold
/// the `StorageNodeCap` for the node with the same index.
pub async fn register_committee_and_stake(
    admin_wallet: &mut WalletContext,
    system_context: &SystemContext,
    node_params: &[NodeRegistrationParams],
    contract_clients: &[&SuiContractClient],
    amounts_to_stake: &[u64],
) -> Result<Vec<StorageNodeCap>> {
    let receiver_addrs: Vec<_> = contract_clients
        .iter()
        .map(|client| client.address())
        .collect();

    mint_wal_to_addresses(
        admin_wallet,
        system_context.package_id,
        system_context.treasury_cap,
        &receiver_addrs,
        *amounts_to_stake
            .iter()
            .max()
            .ok_or_else(|| anyhow!("no staking amounts provided"))?,
    )
    .await?;

    // Initialize client
    let mut storage_node_cap = Vec::new();

    for ((storage_node_params, contract_client), amount_to_stake) in node_params
        .iter()
        .zip(contract_clients)
        .zip(amounts_to_stake)
    {
        let node_cap = contract_client
            .register_candidate(storage_node_params)
            .await?;

        storage_node_cap.push(node_cap.clone());

        // stake with storage nodes
        let _staked_wal = contract_client
            .stake_with_pool(*amount_to_stake, node_cap.node_id)
            .await?;
    }
    Ok(storage_node_cap)
}

/// Calls `voting_end`, immediately followed by `initiate_epoch_change`
pub async fn end_epoch_zero(contract_client: &SuiContractClient) -> Result<()> {
    // call vote end
    contract_client.voting_end().await?;

    tracing::info!(
        "Epoch state after voting end: {:?}",
        contract_client.read_client().current_committee().await?
    );

    // call epoch change
    contract_client.initiate_epoch_change().await?;

    tracing::info!(
        "Epoch state after initiating epoch change: {:?}",
        contract_client.read_client().current_committee().await?
    );

    // TODO(#784): call epoch change done from each node
    Ok(())
}

async fn mint_wal_to_addresses(
    admin_wallet: &mut WalletContext,
    pkg_id: ObjectID,
    treasury_cap: ObjectID,
    receiver_addrs: &[SuiAddress],
    value: u64,
) -> Result<()> {
    // Mint Wal to stake with storage nodes
    let sender = admin_wallet.active_address()?;
    let mut pt_builder = ProgrammableTransactionBuilder::new();
    let treasury_cap_arg = pt_builder.input(
        admin_wallet
            .get_client()
            .await?
            .read_api()
            .get_object_with_options(treasury_cap, SuiObjectDataOptions::new())
            .await?
            .into_object()?
            .object_ref()
            .into(),
    )?;

    let amount_arg = pt_builder.pure(value)?;
    for addr in receiver_addrs.iter().chain(iter::once(&sender)) {
        let result = pt_builder.programmable_move_call(
            SUI_FRAMEWORK_PACKAGE_ID,
            Identifier::new("coin").expect("should be able to convert to Identifier"),
            Identifier::new("mint").expect("should be able to convert to Identifier"),
            vec![TypeTag::from_str(&format!("{pkg_id}::wal::WAL"))?],
            vec![treasury_cap_arg, amount_arg],
        );
        pt_builder.transfer_arg(*addr, result);
    }

    let gas_price = admin_wallet.get_reference_gas_price().await?;
    let gas_coin = admin_wallet
        .gas_for_owner_budget(sender, DEFAULT_GAS_BUDGET, Default::default())
        .await?
        .1
        .object_ref();
    let transaction = TransactionData::new_programmable(
        sender,
        vec![gas_coin],
        pt_builder.finish(),
        DEFAULT_GAS_BUDGET,
        gas_price,
    );

    let transaction = admin_wallet.sign_transaction(&transaction);

    let tx_response = admin_wallet
        .execute_transaction_may_fail(transaction)
        .await?;

    match tx_response
        .effects
        .as_ref()
        .ok_or_else(|| anyhow!("No transaction effects in response"))?
        .status()
    {
        SuiExecutionStatus::Success => Ok(()),
        SuiExecutionStatus::Failure { error } => {
            Err(anyhow!("Error when executing mint transaction: {}", error))
        }
    }
}
