// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Utilities to publish the walrus contracts and deploy a system object for testing.

use std::{collections::BTreeSet, path::PathBuf, str::FromStr};

use anyhow::{anyhow, bail, Result};
use fastcrypto::traits::ToFromBytes;
use sui_move_build::{BuildConfig, PackageDependencies};
use sui_sdk::{
    rpc_types::{SuiExecutionStatus, SuiTransactionBlockEffectsAPI},
    types::{
        base_types::ObjectID,
        programmable_transaction_builder::ProgrammableTransactionBuilder,
        transaction::{Command, TransactionData},
        Identifier,
        TypeTag,
    },
    wallet_context::WalletContext,
    SUI_COIN_TYPE,
};
use walrus_core::ensure;

use crate::{
    contracts::{self, StructTag},
    types::Committee,
    utils::get_created_sui_object_ids_by_type,
};

const E2E_MOVE_MODULE: &str = "e2e_test";

const COMMITTEE_CAP_HOLDER_TAG: StructTag<'_> = StructTag {
    name: "CommitteeCapHolder",
    module: E2E_MOVE_MODULE,
};

/// Parameters for test system deployment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemParameters {
    /// The committee to use for creating the system object.
    pub committee: Committee,
    /// The storage capacity of the system.
    pub capacity: u64,
    /// The price per unit of storage and time to use in the system.
    pub price: u64,
    /// The coin type of the system.
    pub coin_type: TypeTag,
}

impl SystemParameters {
    /// Constructor for [`SystemParameters`] with SUI as coin type.
    pub fn new_with_sui(committee: Committee, capacity: u64, price: u64) -> Self {
        Self {
            committee,
            capacity,
            price,
            coin_type: TypeTag::from_str(SUI_COIN_TYPE).expect("conversion should always succeed"),
        }
    }
}

fn compile_package(package_path: PathBuf) -> (PackageDependencies, Vec<Vec<u8>>) {
    let build_config = BuildConfig::new_for_testing();
    let compiled_package = build_config
        .build(&package_path)
        .expect("Building package failed");
    let compiled_modules = compiled_package.get_package_bytes(false);
    (compiled_package.dependency_ids, compiled_modules)
}

/// Publishes the `blob_store` package.
///
/// Returns the IDs of the created package and the `CommitteeCapHolder`.
pub async fn publish_package(
    wallet: &mut WalletContext,
    contract_path: PathBuf,
    gas_budget: u64,
) -> Result<(ObjectID, ObjectID)> {
    let sender = wallet.active_address()?;
    let sui = wallet.get_client().await?;

    let (dependencies, compiled_modules) = compile_package(contract_path);

    let dep_ids: Vec<ObjectID> = dependencies.published.values().cloned().collect();

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

    // get package id and CommitteeCapHolder id
    let pkg_id = transaction_response
        .effects
        .as_ref()
        .ok_or_else(|| anyhow!("could not read transaction effects"))?
        .created()
        .iter()
        .find(|obj| obj.owner.is_immutable())
        .map(|obj| obj.object_id())
        .ok_or_else(|| anyhow!("no immutable object was created"))?;

    let [committee_cap_holder_id] = get_created_sui_object_ids_by_type(
        &transaction_response,
        &COMMITTEE_CAP_HOLDER_TAG.to_move_struct_tag(pkg_id, &[])?,
    )?[..] else {
        bail!("unexpected number of CommitteeCapHolder objects created");
    };

    Ok((pkg_id, committee_cap_holder_id))
}

/// Create a new system object on chain
pub async fn create_system_object(
    wallet: &mut WalletContext,
    contract_pkg_id: ObjectID,
    committee_cap_holder_id: ObjectID,
    system_params: &SystemParameters,
    gas_budget: u64,
) -> Result<ObjectID> {
    let mut pt_builder = ProgrammableTransactionBuilder::new();

    // prepare the arguments to create the `StorageNodeInfo`s
    let storage_node_args = system_params
        .committee
        .members()
        .iter()
        .map(|node| {
            Ok(vec![
                pt_builder.pure(node.name.to_owned())?,
                pt_builder.pure(node.network_address.to_string())?,
                pt_builder.pure(node.public_key.as_bytes())?,
                pt_builder.pure(node.shard_ids.iter().map(|id| id.0).collect::<Vec<_>>())?,
            ])
        })
        .collect::<Result<Vec<_>>>()?;

    // prepare the other arguments
    let cap_holder_ref = wallet.get_object_ref(committee_cap_holder_id).await?;
    let cap_holder_arg = pt_builder.input(cap_holder_ref.into())?;
    let epoch_arg = pt_builder.pure(system_params.committee.epoch)?;
    let capacity_arg = pt_builder.pure(system_params.capacity)?;
    let price_arg = pt_builder.pure(system_params.price)?;

    // Call the move function to create each `StorageNodeInfo`
    // Command 0
    let storage_node_res = storage_node_args
        .iter()
        .map(|arguments| {
            Ok(pt_builder.programmable_move_call(
                contract_pkg_id,
                Identifier::from_str(contracts::storage_node::create_storage_node_info.module)?,
                Identifier::from_str(contracts::storage_node::create_storage_node_info.name)?,
                vec![],
                arguments.to_owned(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    // Create a vector from the `StorageNodeInfo`s
    // Command 1
    let storage_node_vec = pt_builder.command(Command::MakeMoveVec(
        Some(
            contracts::storage_node::StorageNodeInfo
                .to_move_struct_tag(contract_pkg_id, &[])?
                .into(),
        ),
        storage_node_res,
    ));

    // Create the committee
    // Command 2
    let committee_res = pt_builder.programmable_move_call(
        contract_pkg_id,
        Identifier::from_str(E2E_MOVE_MODULE)?,
        Identifier::from_str("make_committee")?,
        vec![],
        vec![cap_holder_arg, epoch_arg, storage_node_vec],
    );

    // Create the system object
    // Command 3
    pt_builder.programmable_move_call(
        contract_pkg_id,
        Identifier::from_str(contracts::system::share_new.module)?,
        Identifier::from_str(contracts::system::share_new.name)?,
        vec![system_params.coin_type.clone()],
        vec![committee_res, capacity_arg, price_arg],
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

    let [system_object_id] = get_created_sui_object_ids_by_type(
        &response,
        &contracts::system::System
            .to_move_struct_tag(contract_pkg_id, &[system_params.coin_type.to_owned()])?,
    )?[..] else {
        bail!("unexpected number of System objects created");
    };
    Ok(system_object_id)
}
