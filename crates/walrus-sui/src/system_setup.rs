// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Utilities to publish the walrus contracts and deploy a system object for testing.

use std::{
    num::NonZeroU16,
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use move_core_types::account_address::AccountAddress;
use move_package::BuildConfig as MoveBuildConfig;
use sui_move_build::{
    BuildConfig,
    CompiledPackage,
    build_from_resolution_graph,
    check_invalid_dependencies,
    check_unpublished_dependencies,
    gather_published_ids,
};
use sui_sdk::{
    rpc_types::{SuiExecutionStatus, SuiTransactionBlockEffectsAPI, SuiTransactionBlockResponse},
    types::{
        Identifier,
        base_types::ObjectID,
        programmable_transaction_builder::ProgrammableTransactionBuilder,
        transaction::TransactionData,
    },
};
use sui_types::{
    SUI_CLOCK_OBJECT_ID,
    SUI_CLOCK_OBJECT_SHARED_VERSION,
    SUI_FRAMEWORK_ADDRESS,
    transaction::{ObjectArg, TransactionKind},
};
use walkdir::WalkDir;
use walrus_core::{EpochCount, ensure};

use crate::{
    client::retry_client::{RetriableSuiClient, retriable_sui_client::LazySuiClientBuilder},
    contracts::{self, StructTag},
    utils::{get_created_sui_object_ids_by_type, resolve_lock_file_path},
    wallet::Wallet,
};

const INIT_MODULE: &str = "init";

const INIT_CAP_TAG: StructTag<'_> = StructTag {
    name: "InitCap",
    module: INIT_MODULE,
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
    wallet: &mut Wallet,
    package_path: PathBuf,
    gas_budget: Option<u64>,
) -> Result<SuiTransactionBlockResponse> {
    publish_package(wallet, package_path, Default::default(), gas_budget).await
}

/// Compiles a package and returns the compiled package, and build config.
pub async fn compile_package(
    package_path: PathBuf,
    build_config: MoveBuildConfig,
    chain_id: Option<String>,
) -> Result<(CompiledPackage, MoveBuildConfig)> {
    tokio::task::spawn_blocking(|| {
        compile_package_inner_blocking(package_path, build_config, chain_id)
    })
    .await?
}

/// Synchronous method to compile the package. Should only be called from an async context
/// using `tokio::task::spawn_blocking` or similar methods.
fn compile_package_inner_blocking(
    package_path: PathBuf,
    build_config: MoveBuildConfig,
    chain_id: Option<String>,
) -> Result<(CompiledPackage, MoveBuildConfig)> {
    let build_config = resolve_lock_file_path(build_config, &package_path)?;

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

    let run_bytecode_verifier = true;
    let print_diags_to_stderr = false;
    let config = BuildConfig {
        config: build_config.clone(),
        run_bytecode_verifier,
        print_diags_to_stderr,
        chain_id: chain_id.clone(),
    };
    let resolution_graph = config.resolution_graph(&package_path, chain_id.clone())?;
    let (_, dependencies) = gather_published_ids(&resolution_graph, chain_id.clone());

    // Check that the dependencies have a valid published address.
    check_invalid_dependencies(&dependencies.invalid)?;
    // Check that all dependencies are published.
    check_unpublished_dependencies(&dependencies.unpublished)?;

    let compiled_package = build_from_resolution_graph(
        resolution_graph,
        run_bytecode_verifier,
        print_diags_to_stderr,
        chain_id.clone(),
    )?;

    ensure!(
        compiled_package.published_root_module().is_none(),
        "package was already published, modules must all have 0x0 as their addresses."
    );

    // Restore original ID.
    if let (Some(chain_id), Some(previous_id)) = (chain_id, previous_id) {
        let _ = sui_package_management::set_package_id(
            &package_path,
            build_config.install_dir.clone(),
            &chain_id,
            previous_id,
        )?;
    }

    Ok((compiled_package, build_config))
}

#[tracing::instrument(err, skip(wallet, build_config))]
pub(crate) async fn publish_package(
    wallet: &mut Wallet,
    package_path: PathBuf,
    build_config: MoveBuildConfig,
    gas_budget: Option<u64>,
) -> Result<SuiTransactionBlockResponse> {
    let sender = wallet.active_address()?;
    let retry_client = RetriableSuiClient::new(
        vec![LazySuiClientBuilder::new(wallet.get_rpc_url()?, None)],
        Default::default(),
    )
    .await?;

    let chain_id = retry_client.get_chain_identifier().await.ok();

    let (compiled_package, build_config) =
        compile_package(package_path, build_config, chain_id).await?;

    let compiled_modules = compiled_package.get_package_bytes(false);

    // Publish the package
    // TODO: WAL-778 support `publish_tx_kind` with failover mechanics.
    #[allow(deprecated)]
    let transaction_kind = retry_client
        .get_current_client()
        .await
        .transaction_builder()
        .publish_tx_kind(
            sender,
            compiled_modules,
            compiled_package
                .dependency_ids
                .published
                .into_values()
                .collect(),
        )
        .await?;

    let gas_budget = if let Some(gas_budget) = gas_budget {
        gas_budget
    } else {
        let gas_price = retry_client.get_reference_gas_price().await?;
        retry_client
            .estimate_gas_budget(sender, transaction_kind.clone(), gas_price)
            .await?
    };

    let gas_coins = retry_client
        .select_coins(sender, None, u128::from(gas_budget), vec![])
        .await?
        .into_iter()
        .map(|coin| coin.object_ref())
        .collect::<Vec<_>>();

    // TODO: WAL-778 support `gas_price` with failover mechanics.
    #[allow(deprecated)]
    let gas_price = wallet.get_reference_gas_price().await?;

    let tx_data = TransactionData::new_with_gas_coins_allow_sponsor(
        transaction_kind,
        sender,
        gas_coins,
        gas_budget,
        gas_price,
        sender,
    );

    #[allow(deprecated)]
    let response = wallet
        .execute_transaction_may_fail(wallet.sign_transaction(&tx_data))
        .await?;

    // Update the lock file with the new package ID.
    wallet
        .update_lock_file(
            sui_package_management::LockCommand::Publish,
            build_config.install_dir,
            build_config.lock_file,
            &response,
        )
        .await
        .context("failed to update Move.lock")?;

    Ok(response)
}

pub(crate) struct PublishSystemPackageResult {
    pub walrus_pkg_id: ObjectID,
    pub wal_exchange_pkg_id: Option<ObjectID>,
    pub credits_pkg_id: Option<ObjectID>,
    pub walrus_subsidies_pkg_id: Option<ObjectID>,
    pub init_cap_id: ObjectID,
    pub upgrade_cap_id: ObjectID,
}

/// Copy files from the `source` directory to the `destination` directory recursively.
#[tracing::instrument(err, skip(source, destination))]
pub async fn copy_recursively(
    source: impl AsRef<Path>,
    destination: impl AsRef<Path>,
) -> Result<()> {
    let source = source.as_ref().to_owned();
    let destination = destination.as_ref().to_owned();
    tokio::task::spawn_blocking(|| copy_recursively_inner_blocking(source, destination)).await?
}

/// Synchronous method to copy directories recursively. Should only be called from an async context
/// using `tokio::task::spawn_blocking` or similar methods.
fn copy_recursively_inner_blocking(
    source: impl AsRef<Path>,
    destination: impl AsRef<Path>,
) -> Result<()> {
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

/// Publishes the `wal`, `wal_exchange`, `subsidies`, and `walrus` packages.
///
/// Returns the IDs of the packages, the `InitCap`, and the `UpgradeCap`.
///
/// If `deploy_directory` is provided, the contracts will be copied to this directory and published
/// from there to keep the `Move.toml` in the original directory unchanged.
///
/// If `use_existing_wal_token` is set, skips the deployment of the `wal` package. This requires
/// the package address to be set in the `wal/Move.lock` file for the current network.
#[tracing::instrument(err, skip(wallet))]
pub async fn publish_coin_and_system_package(
    wallet: &mut Wallet,
    InitSystemParams {
        contract_dir,
        deploy_directory,
        with_wal_exchange,
        use_existing_wal_token,
        with_credits,
        with_walrus_subsidies,
        ..
    }: InitSystemParams,
    gas_budget: Option<u64>,
) -> Result<PublishSystemPackageResult> {
    let walrus_contract_directory = if let Some(deploy_directory) = deploy_directory {
        copy_recursively(&contract_dir, &deploy_directory).await?;
        deploy_directory
    } else {
        contract_dir
    };

    if !use_existing_wal_token {
        // Publish `wal` package.
        publish_package_with_default_build_config(
            wallet,
            walrus_contract_directory.join("wal"),
            gas_budget,
        )
        .await?;
    }

    let wal_exchange_pkg_id = if with_wal_exchange {
        // Publish `wal_exchange` package.
        let transaction_response = publish_package_with_default_build_config(
            wallet,
            walrus_contract_directory.join("wal_exchange"),
            gas_budget,
        )
        .await?;
        Some(get_pkg_id_from_tx_response(&transaction_response)?)
    } else {
        None
    };

    // Publish `walrus` package.
    let transaction_response = publish_package_with_default_build_config(
        wallet,
        walrus_contract_directory.join("walrus"),
        gas_budget,
    )
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

    let credits_pkg_id = if with_credits {
        // Publish `subsidies` package.
        let transaction_response = publish_package_with_default_build_config(
            wallet,
            walrus_contract_directory.join("subsidies"),
            gas_budget,
        )
        .await?;
        Some(get_pkg_id_from_tx_response(&transaction_response)?)
    } else {
        None
    };

    let walrus_subsidies_pkg_id = if with_walrus_subsidies {
        // Publish `walrus_subsidies` package.
        let transaction_response = publish_package_with_default_build_config(
            wallet,
            walrus_contract_directory.join("walrus_subsidies"),
            gas_budget,
        )
        .await?;
        Some(get_pkg_id_from_tx_response(&transaction_response)?)
    } else {
        None
    };

    Ok(PublishSystemPackageResult {
        walrus_pkg_id,
        wal_exchange_pkg_id,
        credits_pkg_id,
        init_cap_id,
        upgrade_cap_id,
        walrus_subsidies_pkg_id,
    })
}

/// Parameters used to call the `init_walrus` function in the Walrus contracts.
#[derive(Debug, Clone)]
pub struct InitSystemParams {
    /// Number of shards in the system.
    pub n_shards: NonZeroU16,
    /// Duration of the initial epoch in milliseconds.
    pub epoch_zero_duration: Duration,
    /// Duration of an epoch in milliseconds.
    pub epoch_duration: Duration,
    /// The maximum number of epochs ahead for which storage can be obtained.
    pub max_epochs_ahead: EpochCount,
    /// The directory containing the contract source code.
    pub contract_dir: PathBuf,
    /// The directory to deploy the contracts to.
    pub deploy_directory: Option<PathBuf>,
    /// Whether to publish the `wal_exchange` package.
    pub with_wal_exchange: bool,
    /// Whether to use an existing WAL token.
    pub use_existing_wal_token: bool,
    /// Whether to publish the `subsidies` package for client-side credits.
    pub with_credits: bool,
    /// Whether to publish the `walrus_subsidies` package for system subsidies.
    pub with_walrus_subsidies: bool,
}

/// Initialize the system and staking objects on chain.
///
/// Returns the IDs of the system, staking, and upgrade manager objects.
pub async fn create_system_and_staking_objects(
    wallet: &mut Wallet,
    contract_pkg_id: ObjectID,
    init_cap: ObjectID,
    upgrade_cap: ObjectID,
    system_params: InitSystemParams,
    gas_budget: Option<u64>,
) -> Result<(ObjectID, ObjectID, ObjectID)> {
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
    #[allow(deprecated)]
    let init_cap_ref = wallet.get_object_ref(init_cap).await?;
    let init_cap_arg = pt_builder.input(init_cap_ref.into())?;

    #[allow(deprecated)]
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

    let retry_client = RetriableSuiClient::new(
        vec![LazySuiClientBuilder::new(wallet.get_rpc_url()?, None)],
        Default::default(),
    )
    .await?;
    let gas_price = retry_client.get_reference_gas_price().await?;

    let gas_budget = if let Some(gas_budget) = gas_budget {
        gas_budget
    } else {
        retry_client
            .estimate_gas_budget(
                address,
                TransactionKind::ProgrammableTransaction(ptb.clone()),
                gas_price,
            )
            .await?
    };

    let gas_coins = retry_client
        .select_coins(address, None, u128::from(gas_budget), vec![])
        .await?
        .into_iter()
        .map(|coin| coin.object_ref())
        .collect::<Vec<_>>();

    let transaction =
        TransactionData::new_programmable(address, gas_coins, ptb, gas_budget, gas_price);

    // sign and send transaction
    let signed_transaction = wallet.sign_transaction(&transaction);
    #[allow(deprecated)]
    let response = wallet
        .execute_transaction_may_fail(signed_transaction)
        .await?;

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

    let [upgrade_manager_object_id] = get_created_sui_object_ids_by_type(
        &response,
        &contracts::upgrade::UpgradeManager
            .to_move_struct_tag_with_package(contract_pkg_id, &[])?,
    )?[..] else {
        bail!("unexpected number of upgrade manager objects created");
    };

    Ok((
        system_object_id,
        staking_object_id,
        upgrade_manager_object_id,
    ))
}
