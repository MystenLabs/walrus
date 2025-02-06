// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Utilities to publish the walrus contracts and deploy a system object for testing.

use std::{num::NonZeroU16, path::PathBuf, str::FromStr, time::Duration};

use anyhow::Result;
use rand::{rngs::StdRng, SeedableRng as _};
use serde::{Deserialize, Serialize};
use sui_sdk::{types::base_types::ObjectID, wallet_context::WalletContext};
use walrus_core::{
    keys::{NetworkKeyPair, ProtocolKeyPair},
    EpochCount,
};
use walrus_utils::backoff::ExponentialBackoffConfig;

use super::default_protocol_keypair;
use crate::{
    client::{contract_config::ContractConfig, ReadClient, SuiClientError, SuiContractClient},
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

/// Publishes the package with a default system object.
///
/// The system object has the default e2e test setup (compatible with the current tests), and
/// returns the IDs of the system and staking objects. The default test setup currently uses a
/// single storage node with sk = 117.
pub async fn publish_with_default_system(
    mut admin_wallet: WalletContext,
    node_wallet: WalletContext,
) -> Result<(SystemContext, SuiContractClient)> {
    // Default system config, compatible with current tests

    // TODO(#814): make epoch duration in test configurable. Currently hardcoded to 1 hour.
    let system_context = create_and_init_system_for_test(
        &mut admin_wallet,
        NonZeroU16::new(100).expect("100 is not 0"),
        Duration::from_secs(0),
        Duration::from_secs(3600),
        None,
    )
    .await?;

    // Set up node params.
    // Pk corresponding to secret key scalar(117)
    let network_key_pair = NetworkKeyPair::generate_with_rng(&mut StdRng::seed_from_u64(0));
    let protocol_keypair = default_protocol_keypair();

    let storage_node_params =
        NodeRegistrationParams::new_for_test(protocol_keypair.public(), network_key_pair.public());

    // Create admin contract client
    let admin_contract_client = system_context
        .new_contract_client(admin_wallet, ExponentialBackoffConfig::default(), None)
        .await?;

    // Initialize node contract client
    let contract_client = system_context
        .new_contract_client(node_wallet, ExponentialBackoffConfig::default(), None)
        .await?;

    register_committee_and_stake(
        &admin_contract_client,
        &[storage_node_params],
        &[protocol_keypair],
        &[&contract_client],
        &[1_000_000_000],
    )
    .await?;

    // call vote end
    end_epoch_zero(&contract_client).await?;

    Ok((system_context, admin_contract_client))
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
    /// The ID of the WAL package.
    pub wal_pkg_id: ObjectID,
    /// The ID of the WAL exchange package.
    pub wal_exchange_pkg_id: Option<ObjectID>,
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
        ContractConfig::new(self.system_object, self.staking_object)
    }
}

/// Publishes the test contracts and initializes the system.
///
/// Returns the package id and the object IDs of the system object and the staking object.
pub async fn create_and_init_system_for_test(
    admin_wallet: &mut WalletContext,
    n_shards: NonZeroU16,
    epoch_zero_duration: Duration,
    epoch_duration: Duration,
    max_epochs_ahead: Option<EpochCount>,
) -> Result<SystemContext> {
    let temp_dir = tempfile::tempdir()?;
    let deploy_directory = Some(temp_dir.path().to_path_buf());
    create_and_init_system(
        contract_dir_for_testing()?,
        admin_wallet,
        InitSystemParams {
            n_shards,
            epoch_zero_duration,
            epoch_duration,
            max_epochs_ahead: max_epochs_ahead.unwrap_or(DEFAULT_MAX_EPOCHS_AHEAD),
        },
        None,
        deploy_directory,
        true,
    )
    .await
}

/// Publishes the contracts specified in `contract_path` and initializes the system.
///
/// Returns the package ID and the object IDs of the system object, the staking object, and the WAL
/// treasury cap.
///
/// If `deploy_directory` is provided, the contracts will be copied to this directory and published
/// from there to keep the `Move.toml` in the original directory unchanged.
pub async fn create_and_init_system(
    contract_dir: PathBuf,
    admin_wallet: &mut WalletContext,
    init_system_params: InitSystemParams,
    gas_budget: Option<u64>,
    deploy_directory: Option<PathBuf>,
    with_wal_exchange: bool,
) -> Result<SystemContext> {
    let PublishSystemPackageResult {
        walrus_pkg_id,
        init_cap_id,
        upgrade_cap_id,
        wal_exchange_pkg_id,
        wal_pkg_id,
    } = system_setup::publish_coin_and_system_package(
        admin_wallet,
        contract_dir,
        deploy_directory,
        with_wal_exchange,
        gas_budget,
    )
    .await?;

    let (system_object, staking_object) = system_setup::create_system_and_staking_objects(
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
        wal_pkg_id,
        wal_exchange_pkg_id,
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

    // Initialize client.
    // Note that it is important to return the node capabilities in the same order as the nodes
    // were registered, so that the node capabilities match the nodes in the node config.
    let mut node_capabilities = Vec::new();
    for ((storage_node_params, bls_sk), contract_client) in node_params
        .iter()
        .zip(node_bls_keys)
        .zip(node_contract_clients)
    {
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

        let node_cap = contract_client
            .register_candidate(storage_node_params, proof_of_possession)
            .await?;
        node_capabilities.push(node_cap);
    }
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
