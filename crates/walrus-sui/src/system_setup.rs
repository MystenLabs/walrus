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
use move_core_types::account_address::AccountAddress;
use move_package::BuildConfig as MoveBuildConfig;
use sui_move_build::{
    build_from_resolution_graph,
    check_invalid_dependencies,
    check_unpublished_dependencies,
    gather_published_ids,
    BuildConfig,
    CompiledPackage,
    PackageDependencies,
};
use sui_sdk::{
    apis::ReadApi,
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
    utils::{estimate_gas_budget, get_created_sui_object_ids_by_type, resolve_lock_file_path},
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

pub(crate) async fn publish_package_with_default_build_config(
    wallet: &mut WalletContext,
    package_path: PathBuf,
) -> Result<SuiTransactionBlockResponse> {
    publish_package(wallet, package_path, Default::default(), None).await
}

#[tracing::instrument(err, skip(wallet))]
pub(crate) async fn publish_package(
    wallet: &mut WalletContext,
    package_path: PathBuf,
    build_config: MoveBuildConfig,
    gas_budget: Option<u64>,
) -> Result<SuiTransactionBlockResponse> {
    let sender = wallet.active_address()?;
    let client = wallet.get_client().await?;
    let chain_id = client.read_api().get_chain_identifier().await.ok();

    let package_path = package_path.canonicalize()?;

    let build_config = resolve_lock_file_path(build_config, Some(&package_path))?;

    // Set the package ID to zero.
    let previous_id = if let Some(ref chain_id) = chain_id {
        sui_package_management::set_package_id(
            &package_path,
            build_config.install_dir.clone(),
            chain_id,
            AccountAddress::ZERO,
        )?
    } else {
        None
    };

    let (dependencies, compiled_package) =
        compile_package(client.read_api(), build_config.clone(), &package_path).await?;

    let compiled_modules = compiled_package.get_package_bytes(false);
    // Restore original ID.
    if let (Some(chain_id), Some(previous_id)) = (chain_id, previous_id) {
        let _ = sui_package_management::set_package_id(
            &package_path,
            build_config.install_dir.clone(),
            &chain_id,
            previous_id,
        )?;
    }

    // Publish the package
    let transaction_kind = client
        .transaction_builder()
        .publish_tx_kind(
            sender,
            compiled_modules,
            dependencies.published.into_values().collect(),
        )
        .await?;

    let gas_budget = if let Some(gas_budget) = gas_budget {
        gas_budget
    } else {
        estimate_gas_budget(&client, sender, transaction_kind.clone()).await?
    };

    let gas_coins = client
        .coin_read_api()
        .select_coins(sender, None, gas_budget as u128, vec![])
        .await?
        .into_iter()
        .map(|coin| coin.coin_object_id)
        .collect::<Vec<_>>();

    let transaction = client
        .transaction_builder()
        .tx_data(
            sender,
            transaction_kind,
            gas_budget,
            wallet.get_reference_gas_price().await?,
            gas_coins,
            None,
        )
        .await?;

    let signed_transaction = wallet.sign_transaction(&transaction);
    let response = wallet
        .execute_transaction_may_fail(signed_transaction)
        .await?;

    // Update the lock file
    sui_package_management::update_lock_file(
        wallet,
        sui_package_management::LockCommand::Publish,
        build_config.install_dir,
        build_config.lock_file,
        &response,
    )
    .await
    .context("failed to update Move.lock")?;

    Ok(response)
}

// Based on `compile_package` from the sui CLI codebase, simplified for our needs.
pub(crate) async fn compile_package(
    read_api: &ReadApi,
    build_config: MoveBuildConfig,
    package_path: &Path,
) -> Result<(PackageDependencies, CompiledPackage), anyhow::Error> {
    let config = resolve_lock_file_path(build_config, Some(package_path))?;
    let run_bytecode_verifier = true;
    let print_diags_to_stderr = false;
    let chain_id = read_api.get_chain_identifier().await.ok();
    let config = BuildConfig {
        config,
        run_bytecode_verifier,
        print_diags_to_stderr,
        chain_id: chain_id.clone(),
    };
    let resolution_graph = config.resolution_graph(package_path, chain_id.clone())?;
    let (_, dependencies) = gather_published_ids(&resolution_graph, chain_id.clone());

    // Check that the dependencies have a valid published address.
    check_invalid_dependencies(&dependencies.invalid)?;
    // Check that all dependencies are published.
    check_unpublished_dependencies(&dependencies.unpublished)?;

    let compiled_package = build_from_resolution_graph(
        resolution_graph,
        run_bytecode_verifier,
        print_diags_to_stderr,
        chain_id,
    )?;

    ensure!(
        compiled_package.published_root_module().is_none(),
        "package was already published, modules must all have 0x0 as their addresses."
    );

    Ok((dependencies, compiled_package))
}

pub(crate) struct PublishSystemPackageResult {
    pub walrus_pkg_id: ObjectID,
    pub wal_pkg_id: ObjectID,
    pub wal_exchange_pkg_id: Option<ObjectID>,
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
    with_wal_exchange: bool,
) -> Result<PublishSystemPackageResult> {
    let walrus_contract_directory = if let Some(deploy_directory) = deploy_directory {
        copy_recursively(&walrus_contract_directory, &deploy_directory)?;
        deploy_directory
    } else {
        walrus_contract_directory
    };

    // Publish `wal` package.
    let transaction_response =
        publish_package_with_default_build_config(wallet, walrus_contract_directory.join("wal"))
            .await?;
    let wal_pkg_id = get_pkg_id_from_tx_response(&transaction_response)?;
    let wal_type_tag = TypeTag::from_str(&format!("{wal_pkg_id}::wal::WAL"))?;

    let treasury_cap_struct_tag = TREASURY_CAP_TAG
        .to_move_struct_tag_with_package(SUI_FRAMEWORK_ADDRESS.into(), &[wal_type_tag])?;

    let [treasury_cap_id] =
        get_created_sui_object_ids_by_type(&transaction_response, &treasury_cap_struct_tag)?[..]
    else {
        bail!("unexpected number of TreasuryCap objects created");
    };

    let wal_exchange_pkg_id = if with_wal_exchange {
        // Publish `wal_exchange` package.
        let transaction_response = publish_package_with_default_build_config(
            wallet,
            walrus_contract_directory.join("wal_exchange"),
        )
        .await?;
        Some(get_pkg_id_from_tx_response(&transaction_response)?)
    } else {
        None
    };

    // Publish `walrus` package.
    let transaction_response =
        publish_package_with_default_build_config(wallet, walrus_contract_directory.join("walrus"))
            .await?;
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
