// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Utilities to publish the walrus contracts and deploy a system object for testing.

use std::{
    collections::BTreeSet,
    num::NonZeroU16,
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};
use sui::client_commands::{Opts, OptsWithGas, SuiClientCommands};
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
use walkdir::WalkDir;
use walrus_core::{ensure, EpochCount};

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

const UPGRADE_CAP_TAG: StructTag<'_> = StructTag {
    name: "UpgradeCap",
    module: "package",
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

#[tracing::instrument(err, skip(wallet))]
pub(crate) async fn publish_package(
    wallet: &mut WalletContext,
    package_path: PathBuf,
) -> Result<SuiTransactionBlockResponse> {
    let sui_client_command = SuiClientCommands::Publish {
        package_path,
        build_config: Default::default(),
        opts: OptsWithGas {
            gas: None,
            rest: Opts {
                gas_budget: None,
                dry_run: false,
                dev_inspect: false,
                serialize_unsigned_transaction: false,
                serialize_signed_transaction: false,
            },
        },
        skip_dependency_verification: true,
        with_unpublished_dependencies: true,
    };
    let result = sui_client_command.execute(wallet).await?;
    let transaction_response = result
        .tx_block_response()
        .ok_or_else(|| anyhow!("no transaction response"))?
        .to_owned();

    ensure!(
        transaction_response.status_ok() == Some(true),
        "Error during transaction execution: {:?}",
        transaction_response.errors
    );
    Ok(transaction_response)
}

pub(crate) struct PublishSystemPackageResult {
    pub walrus_pkg_id: ObjectID,
    pub wal_pkg_id: ObjectID,
    pub wal_exchange_pkg_id: ObjectID,
    pub init_cap_id: ObjectID,
    pub upgrade_cap_id: ObjectID,
    pub treasury_cap_id: ObjectID,
}

/// Copy files from the `source` directory to the `destination` directory recursively.
#[tracing::instrument(err, skip(source, destination))]
fn copy_recursively(source: impl AsRef<Path>, destination: impl AsRef<Path>) -> Result<()> {
    std::fs::create_dir_all(destination.as_ref())?;
    for entry in WalkDir::new(source.as_ref()) {
        let entry = entry?;
        let filetype = entry.file_type();
        let dest_path = entry.path().strip_prefix(source.as_ref())?;
        if filetype.is_dir() {
            std::fs::create_dir_all(destination.as_ref().join(dest_path))?;
        } else {
            std::fs::copy(entry.path(), destination.as_ref().join(dest_path))?;
        }
    }
    Ok(())
}

/// Publishes the `wal`, `wal_exchange`, and `walrus` packages.
///
/// Returns the IDs of the walrus package and the `InitCap` as well as the `TreasuryCap`
/// of the `WAL` coin.
///
/// If `deploy_directory` is provided, the contracts will be copied to this directory and published
/// from there to keep the `Move.toml` in the original directory unchanged.
#[tracing::instrument(err, skip(wallet))]
pub async fn publish_coin_and_system_package(
    wallet: &mut WalletContext,
    walrus_contract_directory: PathBuf,
    deploy_directory: Option<PathBuf>,
) -> Result<PublishSystemPackageResult> {
    let walrus_contract_directory = if let Some(deploy_directory) = deploy_directory {
        copy_recursively(&walrus_contract_directory, &deploy_directory)?;
        deploy_directory
    } else {
        walrus_contract_directory
    };

    // Publish `wal` package.
    let transaction_response =
        publish_package(wallet, walrus_contract_directory.join("wal")).await?;
    let wal_pkg_id = get_pkg_id_from_tx_response(&transaction_response)?;
    let wal_type_tag = TypeTag::from_str(&format!("{wal_pkg_id}::wal::WAL"))?;

    let treasury_cap_struct_tag = TREASURY_CAP_TAG
        .to_move_struct_tag_with_package(SUI_FRAMEWORK_ADDRESS.into(), &[wal_type_tag])?;

    let [treasury_cap_id] =
        get_created_sui_object_ids_by_type(&transaction_response, &treasury_cap_struct_tag)?[..]
    else {
        bail!("unexpected number of TreasuryCap objects created");
    };

    // Publish `wal_exchange` package.
    let transaction_response =
        publish_package(wallet, walrus_contract_directory.join("wal_exchange")).await?;
    let wal_exchange_pkg_id = get_pkg_id_from_tx_response(&transaction_response)?;

    // Publish `walrus` package.
    let transaction_response =
        publish_package(wallet, walrus_contract_directory.join("walrus")).await?;
    let walrus_pkg_id = get_pkg_id_from_tx_response(&transaction_response)?;

    let [init_cap_id] = get_created_sui_object_ids_by_type(
        &transaction_response,
        &INIT_CAP_TAG.to_move_struct_tag_with_package(walrus_pkg_id, &[])?,
    )?[..] else {
        bail!("unexpected number of InitCap objects created");
    };

    let [upgrade_cap_id] = get_created_sui_object_ids_by_type(
        &transaction_response,
        &UPGRADE_CAP_TAG.to_move_struct_tag_with_package(SUI_FRAMEWORK_ADDRESS.into(), &[])?,
    )?[..] else {
        bail!("unexpected number of UpgradeCap objects created");
    };

    Ok(PublishSystemPackageResult {
        walrus_pkg_id,
        wal_pkg_id,
        wal_exchange_pkg_id,
        init_cap_id,
        upgrade_cap_id,
        treasury_cap_id,
    })
}

/// Parameters used to call the `init_walrus` function in the Walrus contracts.
#[derive(Debug, Clone, Copy)]
pub struct InitSystemParams {
    /// Number of shards in the system.
    pub n_shards: NonZeroU16,
    /// Duration of the initial epoch in milliseconds.
    pub epoch_zero_duration: Duration,
    /// Duration of an epoch in milliseconds.
    pub epoch_duration: Duration,
    /// The maximum number of epochs ahead for which storage can be obtained.
    pub max_epochs_ahead: EpochCount,
}

/// Initialize the system and staking objects on chain.
pub async fn create_system_and_staking_objects(
    wallet: &mut WalletContext,
    contract_pkg_id: ObjectID,
    init_cap: ObjectID,
    upgrade_cap: ObjectID,
    system_params: InitSystemParams,
    gas_budget: u64,
) -> Result<(ObjectID, ObjectID)> {
    let mut pt_builder = ProgrammableTransactionBuilder::new();

    let epoch_duration_millis: u64 = system_params
        .epoch_duration
        .as_millis()
        .try_into()
        .context("epoch duration is too long")?;
    let epoch_zero_duration_millis: u64 = system_params
        .epoch_zero_duration
        .as_millis()
        .try_into()
        .context("genesis epoch duration is too long")?;

    // prepare the arguments
    let init_cap_ref = wallet.get_object_ref(init_cap).await?;
    let init_cap_arg = pt_builder.input(init_cap_ref.into())?;

    let upgrade_cap_ref = wallet.get_object_ref(upgrade_cap).await?;
    let upgrade_cap_arg = pt_builder.input(upgrade_cap_ref.into())?;

    let epoch_zero_duration_arg = pt_builder.pure(epoch_zero_duration_millis)?;
    let epoch_duration_arg = pt_builder.pure(epoch_duration_millis)?;
    let n_shards_arg = pt_builder.pure(system_params.n_shards.get())?;
    let max_epochs_ahead_arg = pt_builder.pure(system_params.max_epochs_ahead)?;
    let clock_arg = pt_builder.obj(ObjectArg::SharedObject {
        id: SUI_CLOCK_OBJECT_ID,
        initial_shared_version: SUI_CLOCK_OBJECT_SHARED_VERSION,
        mutable: false,
    })?;

    // Create the system and staking objects
    let result = pt_builder.programmable_move_call(
        contract_pkg_id,
        Identifier::from_str(contracts::init::initialize_walrus.module)?,
        Identifier::from_str(contracts::init::initialize_walrus.name)?,
        vec![],
        vec![
            init_cap_arg,
            upgrade_cap_arg,
            epoch_zero_duration_arg,
            epoch_duration_arg,
            n_shards_arg,
            max_epochs_ahead_arg,
            clock_arg,
        ],
    );

    pt_builder.transfer_arg(wallet.active_address()?, result);

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
        &contracts::staking::Staking.to_move_struct_tag_with_package(contract_pkg_id, &[])?,
    )?[..] else {
        bail!("unexpected number of staking objects created");
    };

    let [system_object_id] = get_created_sui_object_ids_by_type(
        &response,
        &contracts::system::System.to_move_struct_tag_with_package(contract_pkg_id, &[])?,
    )?[..] else {
        bail!("unexpected number of system objects created");
    };
    Ok((system_object_id, staking_object_id))
}
