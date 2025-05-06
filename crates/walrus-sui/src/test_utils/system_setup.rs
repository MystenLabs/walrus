// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Utilities to publish the walrus contracts and deploy a system object for testing.

use std::{
    collections::HashMap,
    num::NonZeroU16,
    path::PathBuf,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use anyhow::Result;
use futures::{TryStreamExt as _, stream::FuturesUnordered};
use serde::{Deserialize, Serialize};
use sui_sdk::types::base_types::ObjectID;
use walrus_core::{
    EpochCount,
    keys::{NetworkKeyPair, ProtocolKeyPair},
};
use walrus_test_utils::WithTempDir;
use walrus_utils::backoff::ExponentialBackoffConfig;

use super::{TestClusterHandle, TestNodeKeys, new_wallet_on_sui_test_cluster};
use crate::{
    client::{
        ReadClient,
        SuiClientError,
        SuiClientResult,
        SuiContractClient,
        contract_config::ContractConfig,
    },
    system_setup::{self, InitSystemParams, PublishSystemPackageResult},
    types::{NodeRegistrationParams, StorageNodeCap},
    wallet::Wallet,
};

const DEFAULT_MAX_EPOCHS_AHEAD: EpochCount = 104;
const ONE_WAL: u64 = 1_000_000_000;
const MEGA_WAL: u64 = 1_000_000 * ONE_WAL;

/// Provides the path of the latest development contracts directory.
pub fn development_contract_dir() -> anyhow::Result<PathBuf> {
    Ok(PathBuf::from_str(env!("CARGO_MANIFEST_DIR"))?
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("contracts"))
}

/// Provides the path of the testnet contracts directory.
pub fn testnet_contract_dir() -> anyhow::Result<PathBuf> {
    Ok(PathBuf::from_str(env!("CARGO_MANIFEST_DIR"))?
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("testnet-contracts"))
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
    pub async fn new_contract_client<S: AsRef<str>>(
        &self,
        wallet: Wallet,
        rpc_urls: &[S],
        backoff_config: ExponentialBackoffConfig,
        gas_budget: Option<u64>,
    ) -> Result<SuiContractClient, SuiClientError> {
        let contract_config = self.contract_config();
        SuiContractClient::new(
            wallet,
            rpc_urls,
            &contract_config,
            backoff_config,
            gas_budget,
        )
        .await
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
#[allow(clippy::too_many_arguments)]
pub async fn create_and_init_system_for_test(
    admin_wallet: &mut Wallet,
    n_shards: NonZeroU16,
    epoch_zero_duration: Duration,
    epoch_duration: Duration,
    max_epochs_ahead: Option<EpochCount>,
    with_subsidies: bool,
    deploy_directory: Option<PathBuf>,
    contract_dir: Option<PathBuf>,
) -> Result<SystemContext> {
    let temp_dir; // make sure the temp_dir is in scope until the end of the function
    let deploy_directory = if deploy_directory.is_none() {
        temp_dir = tempfile::tempdir()?;
        Some(temp_dir.path().to_path_buf())
    } else {
        deploy_directory
    };
    let contract_dir = if let Some(contract_dir) = contract_dir {
        contract_dir
    } else {
        development_contract_dir()?
    };
    create_and_init_system(
        admin_wallet,
        InitSystemParams {
            n_shards,
            epoch_zero_duration,
            epoch_duration,
            max_epochs_ahead: max_epochs_ahead.unwrap_or(DEFAULT_MAX_EPOCHS_AHEAD),
            contract_dir,
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
    admin_wallet: &mut Wallet,
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
    batch_size: Option<usize>,
) -> Result<Vec<StorageNodeCap>> {
    let current_epoch = admin_contract_client.current_epoch().await?;

    // Note that it is important to return the node capabilities in the same order as the nodes
    // were registered, so that the node capabilities match the nodes in the node config.
    let batch_size = batch_size.unwrap_or(node_params.len());
    let mut node_capabilities = HashMap::new();

    // Chunk the node params into batches of size `batch_size` and register the nodes in parallel.
    // This is done to avoid overwhelming the staking object with too many requests in parallel
    // causing shared object congestion to kick in.
    for chunk in (0..node_params.len())
        .collect::<Vec<_>>()
        .chunks(batch_size)
    {
        let chunk_futures = chunk
            .iter()
            .map(|&i| async move {
                let storage_node_params = &node_params[i];
                let contract_client = node_contract_clients[i];
                let bls_sk = &node_bls_keys[i];
                let proof_of_possession = crate::utils::generate_proof_of_possession(
                    bls_sk,
                    contract_client,
                    current_epoch,
                );

                #[cfg(msim)]
                {
                    use rand::Rng;
                    // In simtest, have a small probability of storage node owning multiple
                    // capability objects for the same node.
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
            .collect::<FuturesUnordered<_>>();

        let chunk_results: HashMap<_, _> = chunk_futures.try_collect().await?;
        node_capabilities.extend(chunk_results);
    }

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

/// Initializes the walrus contract and a wallet for testing and contract benchmarking.
///
/// Returns the cluster, system context, and wallet as well as the [`TestNodeKeys`] for
/// the nodes in the committee to allow signing of certificates. All nodes in the committee
/// have equal stake. Note that depending on `n_nodes` this does not necessarily imply that
/// they have equal weight in the committee.
pub async fn initialize_contract_and_wallet_for_testing(
    epoch_duration: Duration,
    with_subsidies: bool,
    n_nodes: usize,
) -> anyhow::Result<(
    Arc<tokio::sync::Mutex<TestClusterHandle>>,
    WithTempDir<SuiContractClient>,
    SystemContext,
    TestNodeKeys,
)> {
    #[cfg(not(msim))]
    let sui_cluster = super::using_tokio::global_sui_test_cluster();
    #[cfg(msim)]
    let sui_cluster = super::using_msim::global_sui_test_cluster().await;

    // Get a wallet on the global sui test cluster
    let admin_wallet = new_wallet_on_sui_test_cluster(sui_cluster.clone()).await?;

    let bls_keys: Vec<_> = (0..n_nodes).map(|_| ProtocolKeyPair::generate()).collect();

    let result = admin_wallet
        .and_then_async(
            async |admin_wallet| -> anyhow::Result<(SystemContext, SuiContractClient)> {
                #[allow(deprecated)]
                let rpc_urls = &[admin_wallet.get_rpc_url()?];

                publish_with_default_system_with_epoch_duration(
                    admin_wallet,
                    rpc_urls,
                    &bls_keys,
                    epoch_duration,
                    with_subsidies,
                )
                .await
            },
        )
        .await?;
    let system_context = result.inner.0.clone();
    let admin_contract_client = result.map(|(_, client)| client);
    let committee = admin_contract_client
        .as_ref()
        .read_client()
        .current_committee()
        .await?;
    let test_node_keys = TestNodeKeys::new(bls_keys, &committee)?;
    Ok((
        sui_cluster,
        admin_contract_client,
        system_context,
        test_node_keys,
    ))
}

/// Publishes the package for testing `sui-walrus`. Enrolls nodes with keys
/// `bls_keys` and equal stake.
///
/// Returns the system context and the contract client with the admin wallet that
/// also holds the node caps for all nodes.
async fn publish_with_default_system_with_epoch_duration(
    mut admin_wallet: Wallet,
    rpc_urls: &[String],
    bls_keys: &[ProtocolKeyPair],
    epoch_duration: Duration,
    with_subsidies: bool,
) -> anyhow::Result<(SystemContext, SuiContractClient)> {
    let system_context = create_and_init_system_for_test(
        &mut admin_wallet,
        NonZeroU16::new(1000).expect("1000 is not 0"),
        Duration::from_secs(0),
        epoch_duration,
        None,
        with_subsidies,
        None,
        None,
    )
    .await?;

    tracing::info!(?system_context, "created system");

    // Set up node params.
    // Pk corresponding to secret key scalar(117)
    let storage_node_params: Vec<_> = bls_keys
        .iter()
        .map(|protocol_keypair| {
            let network_key_pair = NetworkKeyPair::generate();
            NodeRegistrationParams::new_for_test(
                protocol_keypair.public(),
                network_key_pair.public(),
            )
        })
        .collect();

    // Create admin contract client
    let contract_client = system_context
        .new_contract_client(admin_wallet, rpc_urls, Default::default(), None)
        .await?;

    // We only care about gas cost, so the actual subsidy rate can be zero to make it cheap to fund.
    let subsidy_rate = 0;

    if let Some(subsidies_pkg_id) = system_context.subsidies_pkg_id {
        let (subsidies_object_id, _admin_cap_id) = contract_client
            .create_and_fund_subsidies(subsidies_pkg_id, subsidy_rate, subsidy_rate, MEGA_WAL)
            .await?;

        contract_client
            .read_client()
            .set_subsidies_object(subsidies_object_id)
            .await?;
    }

    // use the same contract client for all nodes
    let node_contract_clients: Vec<_> = bls_keys.iter().map(|_| &contract_client).collect();
    let amounts_to_stake: Vec<_> = bls_keys.iter().map(|_| ONE_WAL).collect();

    register_committee_and_stake(
        &contract_client,
        &storage_node_params,
        bls_keys,
        &node_contract_clients,
        &amounts_to_stake,
        Some(1), // we're using the same contract client, no point waiting for the lock
    )
    .await?;

    // call vote end
    end_epoch_zero(&contract_client).await?;

    Ok((system_context, contract_client))
}
