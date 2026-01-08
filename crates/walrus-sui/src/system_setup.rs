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
use move_package_alt::{
    RootPackage,
    schema::{OriginalID, Publication, PublishAddresses, PublishedID},
};
use move_package_alt_compilation::build_config::BuildConfig as MoveBuildConfig;
use sui_move_build::{CompiledPackage, PackageDependencies};
use sui_package_alt::{BuildParams, SuiFlavor};
use sui_package_management::LockCommand;
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
    transaction::{ObjectArg, SharedObjectMutability, TransactionKind},
};
use walkdir::WalkDir;
use walrus_core::{EpochCount, ensure};

#[cfg(any(test, feature = "test-utils"))]
use crate::test_utils::system_setup;
use crate::{
    client::retry_client::{
        RetriableSuiClient,
        retriable_sui_client::{GasBudgetAndPrice, LazySuiClientBuilder},
    },
    contracts,
    utils::get_created_sui_object_ids_by_type,
    wallet::Wallet,
};

#[cfg(any(test, feature = "test-utils"))]
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

#[cfg(any(test, feature = "test-utils"))]
pub(crate) async fn publish_package_with_default_build_config(
    wallet: &mut Wallet,
    package_path: PathBuf,
    gas_budget: Option<u64>,
) -> Result<SuiTransactionBlockResponse> {
    publish_package(wallet, package_path, Default::default(), gas_budget).await
}

/// Compiles a package and returns the compiled package, and build config.
/// `env` is the environment to use for the package management system, and should be derived
/// from the wallet that performs the publish/upgrade.
pub async fn compile_package(
    package_path: PathBuf,
    build_config: MoveBuildConfig,
    chain_id: Option<String>,
    wallet: &Wallet,
) -> Result<(CompiledPackage, MoveBuildConfig, RootPackage<SuiFlavor>)> {
    let env = wallet
        .find_package_environment(&package_path, &build_config)
        .await?;

    let build_config_clone = build_config.clone();
    let package_path_clone = package_path.clone();

    let root_pkg: RootPackage<SuiFlavor> = build_config_clone
        .package_loader(&package_path_clone, &env)
        .load()
        .await?;

    tokio::task::spawn_blocking(|| {
        sui_macros::nondeterministic!(compile_package_inner_blocking(
            package_path,
            build_config,
            chain_id,
            root_pkg
        ))
    })
    .await?
}

/// Synchronous method to compile the package. Should only be called from an async context
/// using `tokio::task::spawn_blocking` or similar methods.
fn compile_package_inner_blocking(
    package_path: PathBuf,
    build_config: MoveBuildConfig,
    chain_id: Option<String>,
    root_pkg: RootPackage<SuiFlavor>,
) -> Result<(CompiledPackage, MoveBuildConfig, RootPackage<SuiFlavor>)> {
    let mut stdout = std::io::stdout();
    let package = move_package_alt_compilation::compile_from_root_package::<
        std::io::Stdout,
        SuiFlavor,
    >(&root_pkg, &build_config, &mut stdout)
    .expect("Compilation should succeed");

    let package_dependencies = PackageDependencies::new(&root_pkg)?;
    tracing::info!(
        "package path {:?}, chain_id {:?}, package_dependencies {:?}",
        package_path,
        chain_id,
        package_dependencies
    );

    // Check that all dependencies are published.
    if !package_dependencies.unpublished.is_empty() {
        bail!(
            "Walrus packages must not have unpublished dependencies. Unpublished dependencies: {}
        ",
            package_dependencies
                .unpublished
                .into_iter()
                .map(|n| n.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    let published_at = root_pkg
        .publication()
        .map(|p| ObjectID::from_address(p.addresses.published_at.0));

    let compiled_package = CompiledPackage {
        package,
        published_at,
        dependency_ids: package_dependencies,
    };

    ensure!(
        compiled_package.published_root_module().is_none(),
        "package was already published, modules must all have 0x0 as their addresses."
    );

    Ok((compiled_package, build_config, root_pkg))
}

// TODO(WAL-1127): this is a complete copy of the function from
// https://github.com/MystenLabs/sui/blob/a5ee2b9b736e712dfc917c5226ae1d835c59abd9/
// crates/sui/src/client_commands.rs#L3548
// Make that function in sui publicly accessible and use it here.
/// Return the update publication data, without writing it to lockfile
pub fn update_publication(
    chain_id: &str,
    command: LockCommand,
    response: &SuiTransactionBlockResponse,
    _build_config: &MoveBuildConfig,
    publication: Option<&mut Publication<SuiFlavor>>,
) -> Result<Publication<SuiFlavor>, anyhow::Error> {
    // Get the published package ID and version from the response
    let (published_id, version, _) = response.get_new_package_obj().ok_or_else(|| {
        anyhow!(
            "Expected a valid published package response but didn't see \
        one when attempting to update the `Move.lock`."
        )
    })?;

    match command {
        LockCommand::Publish => {
            let (upgrade_cap, _, _) = response
                .get_new_package_upgrade_cap()
                .ok_or_else(|| anyhow!("Expected a valid published package with a upgrade cap"))?;
            Ok(Publication::<SuiFlavor> {
                chain_id: chain_id.to_string(),
                metadata: sui_package_alt::PublishedMetadata {
                    toolchain_version: Some(env!("CARGO_PKG_VERSION").into()),
                    build_config: Some(sui_package_alt::BuildParams::default()),
                    upgrade_capability: Some(upgrade_cap),
                },
                addresses: PublishAddresses {
                    published_at: PublishedID(*published_id),
                    original_id: OriginalID(*published_id),
                },
                version: version.value(),
            })
        }
        LockCommand::Upgrade => {
            let publication =
                publication.expect("for upgrade there should already exist publication info");
            publication.addresses.published_at = PublishedID(*published_id);
            publication.version = version.value();
            // TODO: fix build config data
            publication.metadata.build_config = Some(BuildParams::default());
            publication.metadata.toolchain_version = Some(env!("CARGO_PKG_VERSION").into());
            // TODO: fix this, we should return a mut publication instead of creating a new one in
            // the Publish case
            Ok(publication.clone())
        }
    }
}

// This function is used to publish walrus packages in a local environment such as integration
// test or local testbed. This cannot be used in production environments.
#[cfg(any(test, feature = "test-utils"))]
#[tracing::instrument(err, skip(wallet, build_config))]
pub(crate) async fn publish_package(
    wallet: &mut Wallet,
    package_path: PathBuf,
    build_config: MoveBuildConfig,
    gas_budget: Option<u64>,
) -> Result<SuiTransactionBlockResponse> {
    let sender = wallet.active_address();
    let retry_client = RetriableSuiClient::new(
        vec![LazySuiClientBuilder::new(wallet.get_rpc_url(), None)],
        Default::default(),
    )?;

    let chain_id = retry_client.get_chain_identifier().await?;

    // TODO(WAL-1126): this is a temporary workaround and should be removed once sui publish works
    // with ephemeral publishing.
    system_setup::add_localnet_env_to_contract_toml(package_path.clone(), chain_id.clone())?;

    if cfg!(msim) {
        // TODO(WAL-1125): before the new sui package management system introduced in 1.63 can
        // support external dependencies, in simtest, we have to update all the implicit
        // dependencies to sui using a local copy of the sui repository.
        // The local copy should be pointed to by the SUI_REPO environment variable, and it should
        // match the sui version used by the walrus. The pulling logic is implemented in the
        // cargo-simtest script.
        //
        // Note that this must be done after the localnet environment is added to the Move.toml
        // file.
        system_setup::update_contract_sui_dependency_to_local_copy(package_path.clone())?;
    }

    let (compiled_package, final_build_config, mut root_package) =
        compile_package(package_path, build_config, Some(chain_id.clone()), wallet).await?;

    let compiled_modules = compiled_package.get_package_bytes(false);

    // Publish the package
    // TODO: WAL-778 support `publish_tx_kind` with failover mechanics.
    #[allow(deprecated)]
    let transaction_kind = retry_client
        .get_current_client()
        .await
        .sui_client
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

    let GasBudgetAndPrice {
        gas_budget,
        gas_price,
    } = retry_client
        .gas_budget_and_price(gas_budget, sender, transaction_kind.clone())
        .await?;

    let gas_coins = retry_client
        .select_coins(sender, None, u128::from(gas_budget), vec![])
        .await?
        .into_iter()
        .map(|coin| coin.object_ref())
        .collect::<Vec<_>>();

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
        .execute_transaction_may_fail(wallet.sign_transaction(&tx_data).await)
        .await?;

    // Write published data.
    let publish_data = update_publication(
        chain_id.as_str(),
        LockCommand::Publish,
        &response,
        &final_build_config,
        None,
    )?;

    root_package.write_publish_data(publish_data)?;

    Ok(response)
}

#[cfg(any(test, feature = "test-utils"))]
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
#[cfg(any(test, feature = "test-utils"))]
#[tracing::instrument(err, skip(wallet))]
pub(crate) async fn publish_coin_and_system_package(
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
    use sui_types::SUI_FRAMEWORK_ADDRESS;

    use crate::contracts::StructTag;

    const INIT_MODULE: &str = "init";
    const INIT_CAP_TAG: StructTag<'_> = StructTag {
        name: "InitCap",
        module: INIT_MODULE,
    };
    const UPGRADE_CAP_TAG: StructTag<'_> = StructTag {
        name: "UpgradeCap",
        module: "package",
    };

    let walrus_contract_directory = if let Some(deploy_directory) = deploy_directory {
        // Clear the deploy directory before copying to avoid stale files
        if deploy_directory.exists() {
            // TODO(WAL-1126): remove this once sui publish works with ephemeral publishing.
            // If the contract directory already exists and has been used for publishing, all the
            // published info will be stored in the contract directory, which will make all the
            // contracts appear as published. The root cause is that sui publish does not support
            // ephemeral publishing yet.
            //
            // To make this work, we should clear the published info from the contract directory.
            // This should not be needed if we can use ephemeral publishing.
            tracing::warn!(
                "clearing deploy directory {:?} before copying to it",
                deploy_directory
            );
            // Clear all contents inside the directory without removing the directory itself
            for entry in std::fs::read_dir(&deploy_directory)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() {
                    std::fs::remove_dir_all(path)?;
                } else {
                    std::fs::remove_file(path)?;
                }
            }
        }
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
        mutability: SharedObjectMutability::Immutable,
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

    pt_builder.transfer_arg(wallet.active_address(), result);

    // finalize transaction
    let ptb = pt_builder.finish();
    let address = wallet.active_address();

    let retry_client = RetriableSuiClient::new(
        vec![LazySuiClientBuilder::new(wallet.get_rpc_url(), None)],
        Default::default(),
    )?;

    let GasBudgetAndPrice {
        gas_budget,
        gas_price,
    } = retry_client
        .gas_budget_and_price(
            gas_budget,
            address,
            TransactionKind::ProgrammableTransaction(ptb.clone()),
        )
        .await?;

    let gas_coins = retry_client
        .select_coins(address, None, u128::from(gas_budget), vec![])
        .await?
        .into_iter()
        .map(|coin| coin.object_ref())
        .collect::<Vec<_>>();

    let transaction =
        TransactionData::new_programmable(address, gas_coins, ptb, gas_budget, gas_price);

    // sign and send transaction
    let signed_transaction = wallet.sign_transaction(&transaction).await;
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
