// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use anyhow::{anyhow, ensure, Result};
use sui_move_build::{
    build_from_resolution_graph,
    gather_published_ids,
    BuildConfig,
    PackageDependencies,
};
use sui_sdk::{
    rpc_types::SuiTransactionBlockEffectsAPI,
    types::base_types::ObjectID,
    wallet_context::WalletContext,
};

pub fn compile_package(package_path: PathBuf) -> (PackageDependencies, Vec<Vec<u8>>) {
    let mut build_config = BuildConfig::default();
    build_config.config.install_dir = Some(package_path.clone());
    build_config.config.lock_file = Some(package_path.join("Move.lock"));
    let resolution_graph = build_config
        .resolution_graph(&package_path)
        .expect("Resolution failed");
    let (_, dependencies) = gather_published_ids(&resolution_graph);
    let compiled_package =
        build_from_resolution_graph(package_path, resolution_graph, false, false)
            .expect("Compiling package failed");
    let compiled_modules = compiled_package.get_package_bytes(false);
    (dependencies, compiled_modules)
}

pub fn contract_path(contract: &str) -> anyhow::Result<PathBuf> {
    Ok(std::env::current_dir()?
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("contracts")
        .join(contract))
}

// Publish the package with the e2e test setup and return the IDs of the package
// and the system object
pub async fn publish_package(
    wallet: &mut WalletContext,
    contract: &str,
) -> Result<(ObjectID, ObjectID)> {
    let sender = wallet.active_address()?;
    let sui = wallet.get_client().await?;

    let (dependencies, compiled_modules) = compile_package(contract_path(contract)?);

    let dep_ids: Vec<ObjectID> = dependencies.published.values().cloned().collect();

    // Build a publish transaction
    let publish_tx = sui
        .transaction_builder()
        .publish(sender, compiled_modules, dep_ids, None, 10000000000)
        .await?;

    // Get a signed transaction
    let transaction = wallet.sign_transaction(&publish_tx);

    // Submit the transaction
    let transaction_response = wallet.execute_transaction_may_fail(transaction).await?;

    ensure!(
        transaction_response.status_ok() == Some(true),
        "Status not ok"
    );

    match transaction_response.effects {
        None => Err(anyhow!("could not read transaction effects")),
        Some(effects) => Ok((
            effects
                .created()
                .iter()
                .find(|obj| obj.owner.is_immutable())
                .map(|obj| obj.object_id())
                .ok_or_else(|| anyhow!("no immutable object was created"))?,
            effects
                .created()
                .iter()
                .find(|obj| obj.owner.is_shared())
                .map(|obj| obj.object_id())
                .ok_or_else(|| anyhow!("no shared object was created"))?,
        )),
    }
}
