// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Utilities to publish the walrus contracts and deploy a system object for testing.

use std::{collections::HashMap, num::NonZeroU16, path::PathBuf, str::FromStr, time::Duration};

use anyhow::Result;
use futures::{stream::FuturesUnordered, TryStreamExt as _};
use serde::{Deserialize, Serialize};
use sui_sdk::{types::base_types::ObjectID, wallet_context::WalletContext};
use walrus_core::{keys::ProtocolKeyPair, EpochCount};
use walrus_utils::backoff::ExponentialBackoffConfig;

use crate::{
    client::{
        contract_config::ContractConfig,
        ReadClient,
        SuiClientError,
        SuiClientResult,
        SuiContractClient,
    },
    system_setup::{self, InitSystemParams, PublishSystemPackageResult},
    types::{NodeRegistrationParams, StorageNodeCap},
};

const DEFAULT_MAX_EPOCHS_AHEAD: EpochCount = 104;

/// Provides the default contract directory for testing.
pub fn contract_dir_for_testing() -> anyhow::Result<PathBuf> {
    Ok(PathBuf::from_str(env!("CARGO_MANIFEST_DIR"))?
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("contracts"))
}

/// Helper struct to pass around all needed object IDs when setting up the system.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SystemContext {
    /// The package ID of the walrus contracts.
    pub walrus_pkg_id: ObjectID,
    /// The ID of the system Object.
    pub system_object: ObjectID,
    /// The ID of the staking Object.
    pub staking_object: ObjectID,
    /// The ID of the upgrade manager object.
    pub upgrade_manager_object: ObjectID,
    /// The ID of the WAL exchange package.
    pub wal_exchange_pkg_id: Option<ObjectID>,
    /// The ID of the subsidies Object.
    pub subsidies_object: Option<ObjectID>,
    /// The ID of the subsidies package.
    pub subsidies_pkg_id: Option<ObjectID>,
}

impl SystemContext {
    /// Creates a [`SuiContractClient`] based on the configuration.
    pub async fn new_contract_client(
        &self,
        wallet: WalletContext,
        backoff_config: ExponentialBackoffConfig,
        gas_budget: Option<u64>,
    ) -> Result<SuiContractClient, SuiClientError> {
        let contract_config = self.contract_config();
        SuiContractClient::new(wallet, &contract_config, backoff_config, gas_budget).await
    }

    /// Returns the contract config for the system.
    pub fn contract_config(&self) -> ContractConfig {
        ContractConfig::new_with_subsidies(
            self.system_object,
            self.staking_object,
            self.subsidies_object,
        )
    }
}

/// Publishes the test contracts and initializes the system allowing to specify the
/// deploy directory. If not specified a fresh temp directory is used.
///
/// Returns the package id and the object IDs of the system object and the staking object.
pub async fn create_and_init_system_for_test(
    admin_wallet: &mut WalletContext,
    n_shards: NonZeroU16,
    epoch_zero_duration: Duration,
    epoch_duration: Duration,
    max_epochs_ahead: Option<EpochCount>,
    with_subsidies: bool,
    deploy_directory: Option<PathBuf>,
) -> Result<SystemContext> {
    let temp_dir; // make sure the temp_dir is in scope until the end of the function
    let deploy_directory = if deploy_directory.is_none() {
        temp_dir = tempfile::tempdir()?;
        Some(temp_dir.path().to_path_buf())
    } else {
        deploy_directory
    };
    create_and_init_system(
        admin_wallet,
        InitSystemParams {
            n_shards,
            epoch_zero_duration,
            epoch_duration,
            max_epochs_ahead: max_epochs_ahead.unwrap_or(DEFAULT_MAX_EPOCHS_AHEAD),
            contract_dir: contract_dir_for_testing()?,
            deploy_directory,
            with_wal_exchange: true,
            use_existing_wal_token: false,
            with_subsidies,
        },
        None,
    )
    .await
}

/// Publishes the contracts and initializes the system with the provided parameters.
///
/// Returns a `SystemContext` containing the package ID and object IDs for the system, staking,
/// upgrade manager, and optionally subsidies and WAL exchange objects.
///
/// If `deploy_directory` is provided, the contracts will be copied to this directory and published
/// from there to keep the `Move.toml` in the original directory unchanged.
pub async fn create_and_init_system(
    admin_wallet: &mut WalletContext,
    init_system_params: InitSystemParams,
    gas_budget: Option<u64>,
) -> Result<SystemContext> {
    let init_system_params_cloned = init_system_params.clone();
    let PublishSystemPackageResult {
        walrus_pkg_id,
        init_cap_id,
        upgrade_cap_id,
        wal_exchange_pkg_id,
        subsidies_pkg_id,
    } = system_setup::publish_coin_and_system_package(
        admin_wallet,
        init_system_params_cloned.contract_dir,
        init_system_params_cloned.deploy_directory,
        init_system_params_cloned.with_wal_exchange,
        init_system_params_cloned.use_existing_wal_token,
        init_system_params_cloned.with_subsidies,
        gas_budget,
    )
    .await?;

    let (system_object, staking_object, upgrade_manager_object) =
        system_setup::create_system_and_staking_objects(
            admin_wallet,
            walrus_pkg_id,
            init_cap_id,
            upgrade_cap_id,
            init_system_params,
            gas_budget,
        )
        .await?;

    Ok(SystemContext {
        walrus_pkg_id,
        system_object,
        staking_object,
        upgrade_manager_object,
        subsidies_object: None,
        wal_exchange_pkg_id,
        subsidies_pkg_id,
    })
}

/// Registers the nodes based on the provided parameters, distributes WAL to each contract client,
/// and stakes an equal amount with each storage node. Each of the contract clients will hold
/// the `StorageNodeCap` for the node with the same index.
#[tracing::instrument(
    err,
    skip(
        admin_contract_client,
        node_params,
        node_bls_keys,
        node_contract_clients,
        amounts_to_stake
    )
)]
pub async fn register_committee_and_stake(
    admin_contract_client: &SuiContractClient,
    node_params: &[NodeRegistrationParams],
    node_bls_keys: &[ProtocolKeyPair],
    node_contract_clients: &[&SuiContractClient],
    amounts_to_stake: &[u64],
) -> Result<Vec<StorageNodeCap>> {
    let current_epoch = admin_contract_client.current_epoch().await?;

    // Note that it is important to return the node capabilities in the same order as the nodes
    // were registered, so that the node capabilities match the nodes in the node config.
    let node_capabilities = (0..node_params.len())
        .map(|i| async move {
            let storage_node_params = &node_params[i];
            let contract_client = node_contract_clients[i];
            let bls_sk = &node_bls_keys[i];
            let proof_of_possession =
                crate::utils::generate_proof_of_possession(bls_sk, contract_client, current_epoch);

            #[cfg(msim)]
            {
                use rand::Rng;
                // In simtest, have a small probability of storage node owning multiple capability
                // objects
                // for the same node.
                if rand::thread_rng().gen_bool(0.1) {
                    let _ = contract_client
                        .register_candidate(storage_node_params, proof_of_possession.clone())
                        .await?;
                }
            }

            SuiClientResult::<_>::Ok((
                i,
                contract_client
                    .register_candidate(storage_node_params, proof_of_possession)
                    .await?,
            ))
        })
        .collect::<FuturesUnordered<_>>()
        .try_collect::<HashMap<_, _>>()
        .await?;

    let node_capabilities: Vec<_> = (0..node_params.len())
        .map(|i| {
            node_capabilities
                .get(&i)
                .expect("all indices are inserted above")
        })
        .cloned()
        .collect();

    // Use admin wallet to stake with storage nodes
    let node_ids_with_stake_amounts: Vec<_> = node_capabilities
        .iter()
        .zip(amounts_to_stake.iter())
        .filter(|(_, amount)| **amount > 0)
        .map(|(cap, amount)| (cap.node_id, *amount))
        .collect();
    let _staked_wal = admin_contract_client
        .stake_with_pools(&node_ids_with_stake_amounts)
        .await?;

    Ok(node_capabilities)
}

/// Calls `voting_end`, immediately followed by `initiate_epoch_change`.
pub async fn end_epoch_zero(contract_client: &SuiContractClient) -> Result<()> {
    // call vote end
    contract_client.voting_end().await?;

    let voting_end_epoch_state = contract_client.current_committee().await?;
    tracing::info!("epoch state after voting end: {:?}", voting_end_epoch_state);

    // call epoch change
    contract_client.initiate_epoch_change().await?;

    let initiate_epoch_state = contract_client.current_committee().await?;
    tracing::info!(
        "epoch state after initiating epoch change: {:?}",
        initiate_epoch_state
    );

    Ok(())
}
