// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Utilities to publish the walrus contracts and deploy a system object for testing.

use std::{path::PathBuf, str::FromStr};

use anyhow::{anyhow, bail, Result};
use fastcrypto::{bls12381::min_pk::BLS12381PublicKey, traits::ToFromBytes};
use rand::{rngs::StdRng, SeedableRng as _};
use sui_sdk::{
    rpc_types::{SuiExecutionStatus, SuiObjectDataOptions, SuiTransactionBlockEffectsAPI},
    types::base_types::ObjectID,
    wallet_context::WalletContext,
};
use sui_types::{
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    transaction::TransactionData,
    Identifier,
    TypeTag,
    SUI_FRAMEWORK_PACKAGE_ID,
};
use walrus_core::keys::NetworkKeyPair;

use super::DEFAULT_GAS_BUDGET;
use crate::{
    client::{ContractClient, SuiContractClient},
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
    mut node_wallet: WalletContext,
) -> Result<(ObjectID, ObjectID)> {
    // Default system config, compatible with current tests

    // TODO: move to publish_and_initialize_system function, only temp here
    let (pkg_id, cap_id, treasury_cap) = publish_coin_and_system_package(
        admin_wallet,
        contract_path_for_testing("walrus")?,
        DEFAULT_GAS_BUDGET,
    )
    .await?;

    let (system_obj_id, staking_obj_id) =
        create_system_and_staking_objects(admin_wallet, pkg_id, cap_id, 100, 0, DEFAULT_GAS_BUDGET)
            .await?;

    // mint Wal to stake with storage nodes
    let sender = admin_wallet.active_address()?;
    let receiver = node_wallet.active_address()?;
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

    // TODO: clean up
    let amount_arg = pt_builder.pure(1_000_000_000u64)?;
    let result = pt_builder.programmable_move_call(
        SUI_FRAMEWORK_PACKAGE_ID,
        Identifier::new("coin").expect("should be able to convert to Identifier"),
        Identifier::new("mint").expect("should be able to convert to Identifier"),
        vec![TypeTag::from_str(&format!("{pkg_id}::wal::WAL"))?],
        vec![treasury_cap_arg, amount_arg],
    );
    pt_builder.transfer_arg(receiver, result);

    let amount_arg = pt_builder.pure(1_000_000_000u64)?;
    let result = pt_builder.programmable_move_call(
        SUI_FRAMEWORK_PACKAGE_ID,
        Identifier::new("coin").expect("should be able to convert to Identifier"),
        Identifier::new("mint").expect("should be able to convert to Identifier"),
        vec![TypeTag::from_str(&format!("{pkg_id}::wal::WAL"))?],
        vec![treasury_cap_arg, amount_arg],
    );
    pt_builder.transfer_arg(sender, result);

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
        SuiExecutionStatus::Success => {}
        SuiExecutionStatus::Failure { error } => {
            bail!("Error when executing mint transaction: {}", error)
        }
    }

    // Initialize client

    let contract_client = SuiContractClient::new(
        node_wallet,
        system_obj_id,
        staking_obj_id,
        DEFAULT_GAS_BUDGET,
    )
    .await?;

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

    let node_cap = contract_client
        .register_candidate(storage_node_params)
        .await?;

    // stake with storage nodes
    let _staked_wal = contract_client
        .stake_with_pool(100_000, node_cap.node_id)
        .await?;

    // call vote end
    contract_client.voting_end().await?;

    // call epoch change
    contract_client.initiate_epoch_change().await?;

    // TODO: call epoch change done from each node

    Ok((system_obj_id, staking_obj_id))
}
