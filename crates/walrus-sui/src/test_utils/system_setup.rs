// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Utilities to publish the walrus contracts and deploy a system object for testing.

use std::{iter, path::PathBuf, str::FromStr};

use anyhow::{anyhow, Result};
use fastcrypto::{bls12381::min_pk::BLS12381PublicKey, traits::ToFromBytes};
use rand::{rngs::StdRng, SeedableRng as _};
use serde::{Deserialize, Serialize};
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
use walrus_core::keys::NetworkKeyPair;

use super::DEFAULT_GAS_BUDGET;
use crate::{
    client::{ContractClient, ReadClient, SuiContractClient},
    system_setup::{create_system_and_staking_objects, publish_coin_and_system_package},
    types::{NetworkAddress, NodeRegistrationParams},
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

/// Publishes the package with a default system object.
///
/// The system object has the default e2e test setup (compatible with the current tests), and
/// returns the IDs of the system and staking objects. The default test setup currently uses a
/// single storage node with sk = 117.
pub async fn publish_with_default_system(
    admin_wallet: &mut WalletContext,
    node_wallet: WalletContext,
) -> Result<(ObjectID, ObjectID)> {
    // Default system config, compatible with current tests

    // TODO(#814): make epoch duration in test configurable. Currently hardcoded to 1 hour.
    let system_context = create_and_init_system(admin_wallet, 100, 0, 3600000).await?;

    // Set up node params.
    // Pk corresponding to secret key scalar(117)
    let network_key_pair = NetworkKeyPair::generate_with_rng(&mut StdRng::seed_from_u64(0));
    let pubkey_bytes = [
        149, 234, 204, 58, 220, 9, 200, 39, 89, 63, 88, 30, 142, 45, 224, 104, 191, 76, 245, 208,
        192, 235, 41, 229, 55, 47, 13, 35, 54, 71, 136, 238, 15, 155, 235, 17, 44, 138, 126, 156,
        47, 12, 114, 4, 51, 112, 92, 240,
    ];

    let storage_node_params = NodeRegistrationParams {
        name: "Test0".to_owned(),
        network_address: NetworkAddress::from_str("127.0.0.1:8080")?,
        public_key: BLS12381PublicKey::from_bytes(&pubkey_bytes)?,
        network_public_key: network_key_pair.public().clone(),
        commission_rate: 0,
        storage_price: 5,
        write_price: 1,
        node_capacity: 1_000_000_000_000,
    };

    // Initialize client
    let contract_client = SuiContractClient::new(
        node_wallet,
        system_context.system_obj_id,
        system_context.staking_obj_id,
        DEFAULT_GAS_BUDGET,
    )
    .await?;

    register_committee_and_stake(
        admin_wallet,
        &system_context,
        &[storage_node_params],
        &[&contract_client],
        &[1_000_000_000],
    )
    .await?;

    // call vote end
    end_epoch_zero(&contract_client).await?;

    Ok((system_context.system_obj_id, system_context.staking_obj_id))
}

/// Helper struct to pass around all needed object IDs when setting up the system.
#[derive(Debug, Clone, Deserialize, Serialize)]
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
    epoch_duration_ms: u64,
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
        epoch_duration_ms,
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
) -> Result<()> {
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

    for ((storage_node_params, contract_client), amount_to_stake) in node_params
        .iter()
        .zip(contract_clients)
        .zip(amounts_to_stake)
    {
        let node_cap = contract_client
            .register_candidate(storage_node_params)
            .await?;

        // stake with storage nodes
        let _staked_wal = contract_client
            .stake_with_pool(*amount_to_stake, node_cap.node_id)
            .await?;
    }
    Ok(())
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

/// Mints WAL to the provided addresses.
pub async fn mint_wal_to_addresses(
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
