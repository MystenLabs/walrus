// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Utilities to publish the walrus contracts and deploy a system object for testing.

use std::{iter, num::NonZeroU16, path::PathBuf, str::FromStr, time::Duration};

use anyhow::{anyhow, Result};
use rand::{rngs::StdRng, SeedableRng as _};
use serde::{Deserialize, Serialize};
use sui_sdk::{
    rpc_types::{SuiExecutionStatus, SuiObjectDataOptions, SuiTransactionBlockEffectsAPI},
    types::base_types::ObjectID,
    wallet_context::WalletContext,
};
use sui_types::{
    base_types::SuiAddress,
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    transaction::TransactionData,
    Identifier,
    TypeTag,
    SUI_FRAMEWORK_PACKAGE_ID,
};
use walrus_core::{
    keys::{NetworkKeyPair, ProtocolKeyPair},
    EpochCount,
};
use walrus_utils::backoff::ExponentialBackoffConfig;

use super::{default_protocol_keypair, DEFAULT_GAS_BUDGET};
use crate::{
    client::{contract_config::ContractConfig, ReadClient, SuiClientError, SuiContractClient},
    system_setup::{self, InitSystemParams, PublishSystemPackageResult},
    types::{NodeRegistrationParams, StorageNodeCap},
};

const DEFAULT_MAX_EPOCHS_AHEAD: EpochCount = 104;

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
    node_wallet: WalletContext,
) -> Result<SystemContext> {
    // Default system config, compatible with current tests

    // TODO(#814): make epoch duration in test configurable. Currently hardcoded to 1 hour.
    let system_context = create_and_init_system_for_test(
        admin_wallet,
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

    // Initialize client
    let contract_client = system_context
        .new_contract_client(
            node_wallet,
            ExponentialBackoffConfig::default(),
            DEFAULT_GAS_BUDGET,
        )
        .await?;

    register_committee_and_stake(
        admin_wallet,
        &system_context,
        &[storage_node_params],
        &[protocol_keypair],
        &[&contract_client],
        1_000_000_000_000,
        &[1_000_000_000],
    )
    .await?;

    // call vote end
    end_epoch_zero(&contract_client).await?;

    Ok(system_context)
}

/// Helper struct to pass around all needed object IDs when setting up the system.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SystemContext {
    /// The package ID.
    pub package_id: ObjectID,
    /// The ID of the system Object.
    pub system_object: ObjectID,
    /// The ID of the staking Object.
    pub staking_object: ObjectID,
    /// The ID of the WAL treasury Cap.
    pub treasury_cap: ObjectID,
}

impl SystemContext {
    /// Creates a [`SuiContractClient`] based on the configuration.
    pub async fn new_contract_client(
        &self,
        wallet: WalletContext,
        backoff_config: ExponentialBackoffConfig,
        gas_budget: u64,
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
    create_and_init_system(
        contract_path_for_testing("walrus")?,
        admin_wallet,
        InitSystemParams {
            n_shards,
            epoch_zero_duration,
            epoch_duration,
            max_epochs_ahead: max_epochs_ahead.unwrap_or(DEFAULT_MAX_EPOCHS_AHEAD),
        },
        DEFAULT_GAS_BUDGET,
        true,
    )
    .await
}

/// Publishes the contracts specified in `contract_path` and initializes the system.
///
/// Returns the package ID and the object IDs of the system object, the staking object, and the WAL
/// treasury cap.
pub async fn create_and_init_system(
    contract_path: PathBuf,
    admin_wallet: &mut WalletContext,
    init_system_params: InitSystemParams,
    gas_budget: u64,
    for_test: bool,
) -> Result<SystemContext> {
    let PublishSystemPackageResult {
        walrus_pkg_id,
        init_cap_id,
        upgrade_cap_id,
        treasury_cap_id,
    } = system_setup::publish_coin_and_system_package(
        admin_wallet,
        contract_path,
        gas_budget,
        for_test,
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
        package_id: walrus_pkg_id,
        system_object,
        staking_object,
        treasury_cap: treasury_cap_id,
    })
}

/// Registers the nodes based on the provided parameters, distributes WAL to each contract client,
/// and stakes an equal amount with each storage node. Each of the contract clients will hold
/// the `StorageNodeCap` for the node with the same index.
pub async fn register_committee_and_stake(
    admin_wallet: &mut WalletContext,
    system_context: &SystemContext,
    node_params: &[NodeRegistrationParams],
    node_bls_keys: &[ProtocolKeyPair],
    contract_clients: &[&SuiContractClient],
    wal_to_mint: u64,
    amounts_to_stake: &[u64],
) -> Result<Vec<StorageNodeCap>> {
    let receiver_addrs: Vec<_> = contract_clients
        .iter()
        .map(|client| client.address())
        .collect();

    mint_wal_to_addresses(
        admin_wallet,
        system_context.package_id,
        system_context.treasury_cap,
        &receiver_addrs,
        wal_to_mint.max(
            *amounts_to_stake
                .iter()
                .max()
                .expect("stake amount must be set"),
        ),
    )
    .await?;
    let current_epoch = contract_clients[0].current_epoch().await?;

    // Initialize client
    let mut node_capabilities = Vec::new();
    for (((storage_node_params, bls_sk), contract_client), amount_to_stake) in node_params
        .iter()
        .zip(node_bls_keys)
        .zip(contract_clients)
        .zip(amounts_to_stake)
    {
        let proof_of_possession =
            crate::utils::generate_proof_of_possession(bls_sk, contract_client, current_epoch);
        let node_cap = contract_client
            .register_candidate(storage_node_params, proof_of_possession)
            .await?;
        // stake with storage nodes
        if *amount_to_stake > 0 {
            let _staked_wal = contract_client
                .stake_with_pools(&[(node_cap.node_id, *amount_to_stake)])
                .await?;
        }
        node_capabilities.push(node_cap);
    }
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

/// Mints WAL to the provided addresses.
pub async fn mint_wal_to_addresses(
    admin_wallet: &mut WalletContext,
    pkg_id: ObjectID,
    treasury_cap: ObjectID,
    receiver_addrs: &[SuiAddress],
    value: u64,
) -> Result<()> {
    // Mint WAL to stake with storage nodes.
    let sender = admin_wallet.active_address()?;
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

    let amount_arg = pt_builder.pure(value)?;
    for addr in receiver_addrs.iter().chain(iter::once(&sender)) {
        let result = pt_builder.programmable_move_call(
            SUI_FRAMEWORK_PACKAGE_ID,
            Identifier::new("coin").expect("should be able to convert to Identifier"),
            Identifier::new("mint").expect("should be able to convert to Identifier"),
            vec![TypeTag::from_str(&format!("{pkg_id}::wal::WAL"))?],
            vec![treasury_cap_arg, amount_arg],
        );
        pt_builder.transfer_arg(*addr, result);
    }

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
        SuiExecutionStatus::Success => Ok(()),
        SuiExecutionStatus::Failure { error } => {
            Err(anyhow!("Error when executing mint transaction: {}", error))
        }
    }
}
