// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Utilities to publish the walrus contracts and deploy a system object for testing.

use std::{collections::BTreeSet, path::PathBuf, str::FromStr};

use anyhow::{anyhow, bail, Result};
use sui_move_build::BuildConfig;
use sui_sdk::{
    rpc_types::{SuiExecutionStatus, SuiTransactionBlockEffectsAPI, SuiTransactionBlockResponse},
    types::{
        base_types::ObjectID,
        programmable_transaction_builder::ProgrammableTransactionBuilder,
        transaction::TransactionData,
        Identifier,
        TypeTag,
    },
    wallet_context::WalletContext,
};
use sui_types::{
    transaction::ObjectArg,
    SUI_CLOCK_OBJECT_ID,
    SUI_CLOCK_OBJECT_SHARED_VERSION,
    SUI_FRAMEWORK_ADDRESS,
};
use tracing::instrument;
use walrus_core::ensure;

use crate::{
    contracts::{self, StructTag},
    utils::get_created_sui_object_ids_by_type,
};

const INIT_MODULE: &str = "init";

const INIT_CAP_TAG: StructTag<'_> = StructTag {
    name: "InitCap",
    module: INIT_MODULE,
};

const TREASURY_CAP_TAG: StructTag<'_> = StructTag {
    name: "TreasuryCap",
    module: "coin",
};

fn get_pkg_id_from_tx_response(tx_response: &SuiTransactionBlockResponse) -> Result<ObjectID> {
    tx_response
        .effects
        .as_ref()
        .ok_or_else(|| anyhow!("could not read transaction effects"))?
        .created()
        .iter()
        .find(|obj| obj.owner.is_immutable())
        .map(|obj| obj.object_id())
        .ok_or_else(|| anyhow!("no immutable object was created"))
}

#[instrument(err, skip(wallet, gas_budget))]
pub(crate) async fn publish_package(
    wallet: &mut WalletContext,
    package_path: PathBuf,
    gas_budget: u64,
) -> Result<SuiTransactionBlockResponse> {
    let sender = wallet.active_address()?;
    let sui = wallet.get_client().await?;

    let build_config = if cfg!(any(test, feature = "test-utils")) {
        BuildConfig::new_for_testing()
    } else {
        BuildConfig::default()
    };

    let compiled_package = build_config
        .build(&package_path)
        .expect("Building package failed");
    let compiled_modules = compiled_package.get_package_bytes(true);

    let dep_ids: Vec<ObjectID> = compiled_package
        .dependency_ids
        .published
        .values()
        .cloned()
        .collect();

    // Build a publish transaction
    let publish_tx = sui
        .transaction_builder()
        .publish(sender, compiled_modules, dep_ids, None, gas_budget)
        .await?;

    // Get a signed transaction
    let transaction = wallet.sign_transaction(&publish_tx);

    // Submit the transaction
    let transaction_response = wallet.execute_transaction_may_fail(transaction).await?;

    ensure!(
        transaction_response.status_ok() == Some(true),
        "Error during transaction execution: {:?}",
        transaction_response.errors
    );
    Ok(transaction_response)
}

/// Publishes the `wal` and the `walrus` packages.
///
/// Returns the IDs of the walrus package and the `InitCap` as well as the `TreasuryCap`
/// of the `WAL` coin.
#[instrument(err, skip(wallet, gas_budget))]
pub async fn publish_coin_and_system_package(
    wallet: &mut WalletContext,
    walrus_contract_path: PathBuf,
    gas_budget: u64,
) -> Result<(ObjectID, ObjectID, ObjectID)> {
    // Publish `walrus` package with unpublished dependencies
    let transaction_response = publish_package(wallet, walrus_contract_path, gas_budget).await?;

    let walrus_pkg_id = get_pkg_id_from_tx_response(&transaction_response)?;

    let [init_cap_id] = get_created_sui_object_ids_by_type(
        &transaction_response,
        &INIT_CAP_TAG.to_move_struct_tag(walrus_pkg_id, &[])?,
    )?[..] else {
        bail!("unexpected number of InitCap objects created");
    };

    let wal_type_tag = TypeTag::from_str(&format!("{walrus_pkg_id}::wal::WAL"))?;

    let treasury_cap_struct_tag =
        TREASURY_CAP_TAG.to_move_struct_tag(SUI_FRAMEWORK_ADDRESS.into(), &[wal_type_tag])?;

    let [treasury_cap_id] =
        get_created_sui_object_ids_by_type(&transaction_response, &treasury_cap_struct_tag)?[..]
    else {
        bail!("unexpected number of TreasuryCap objects created");
    };

    Ok((walrus_pkg_id, init_cap_id, treasury_cap_id))
}

/// Initialize the system and staking objects on chain.
pub async fn create_system_and_staking_objects(
    wallet: &mut WalletContext,
    contract_pkg_id: ObjectID,
    init_cap: ObjectID,
    n_shards: u16,
    epoch_zero_duration_ms: u64,
    gas_budget: u64,
) -> Result<(ObjectID, ObjectID)> {
    let mut pt_builder = ProgrammableTransactionBuilder::new();

    // prepare the arguments
    let init_cap_ref = wallet.get_object_ref(init_cap).await?;

    let init_cap_arg = pt_builder.input(init_cap_ref.into())?;
    let epoch_zero_duration_arg = pt_builder.pure(epoch_zero_duration_ms)?;
    let n_shards_arg = pt_builder.pure(n_shards)?;
    let clock_arg = pt_builder.obj(ObjectArg::SharedObject {
        id: SUI_CLOCK_OBJECT_ID,
        initial_shared_version: SUI_CLOCK_OBJECT_SHARED_VERSION,
        mutable: false,
    })?;

    // Create the system and staking objects
    pt_builder.programmable_move_call(
        contract_pkg_id,
        Identifier::from_str(contracts::init::initialize_walrus.module)?,
        Identifier::from_str(contracts::init::initialize_walrus.name)?,
        vec![],
        vec![
            init_cap_arg,
            epoch_zero_duration_arg,
            n_shards_arg,
            clock_arg,
        ],
    );

    // finalize transaction
    let ptb = pt_builder.finish();
    let address = wallet.active_address()?;
    let gas_price = wallet.get_reference_gas_price().await?;
    let gas = wallet
        .gas_for_owner_budget(address, gas_budget, BTreeSet::new())
        .await?;
    let transaction = TransactionData::new_programmable(
        address,
        vec![gas.1.object_ref()],
        ptb,
        gas_budget,
        gas_price,
    );

    // sign and send transaction
    let transaction = wallet.sign_transaction(&transaction);
    let response = wallet.execute_transaction_may_fail(transaction).await?;

    if let SuiExecutionStatus::Failure { error } = response
        .effects
        .as_ref()
        .ok_or_else(|| anyhow!("No transaction effects in response"))?
        .status()
    {
        bail!("Error during execution: {}", error);
    }

    let [staking_object_id] = get_created_sui_object_ids_by_type(
        &response,
        &contracts::staking::Staking.to_move_struct_tag(contract_pkg_id, &[])?,
    )?[..] else {
        bail!("unexpected number of staking objects created");
    };

    let [system_object_id] = get_created_sui_object_ids_by_type(
        &response,
        &contracts::system::System.to_move_struct_tag(contract_pkg_id, &[])?,
    )?[..] else {
        bail!("unexpected number of system objects created");
    };
    Ok((system_object_id, staking_object_id))
}
