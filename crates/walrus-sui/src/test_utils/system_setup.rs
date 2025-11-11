// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Utilities to publish the walrus contracts and deploy a system object for testing.

use std::{
    collections::HashMap,
    num::NonZeroU16,
    path::{Path, PathBuf},
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
        contract_config::{ContractConfig, DEFAULT_CACHE_TTL},
    },
    system_setup::{self, InitSystemParams, PublishSystemPackageResult},
    types::{NodeRegistrationParams, StorageNodeCap},
    wallet::Wallet,
};

/// The default value for the maximum number of epochs for which blobs can be stored.
pub const DEFAULT_MAX_EPOCHS_AHEAD: EpochCount = 53;

const ONE_WAL: u64 = 1_000_000_000;
const MEGA_WAL: u64 = 1_000_000 * ONE_WAL;

/// Default buyer subsidy rate (5%) in credits object.
const DEFAULT_BUYER_CREDITS_RATE: u16 = 500;
/// Default system subsidy rate (6%) in credits object.
const DEFAULT_SYSTEM_CREDITS_RATE: u16 = 600;
/// Default initial credits funds amount
const DEFAULT_CREDITS_FUNDS: u64 = MEGA_WAL;

/// Default system subsidy rate (50%) in walrus subsidies object.
const DEFAULT_SYSTEM_SUBSIDY_RATE: u32 = 5_000;
/// Default base subsidy in walrus subsidies object.
const DEFAULT_BASE_SUBSIDY: u64 = ONE_WAL;
/// Default subsidy per shard in walrus subsidies object.
const DEFAULT_SUBSIDY_PER_SHARD: u64 = ONE_WAL / 10;
/// Default initial subsidies funds amount
const DEFAULT_SUBSIDIES_FUNDS: u64 = MEGA_WAL;

/// Provides the path of the latest development contracts directory.
pub fn development_contract_dir() -> anyhow::Result<PathBuf> {
    Ok(project_root_dir()?.join("contracts"))
}

/// Provides the path of the testnet contracts directory.
pub fn testnet_contract_dir() -> anyhow::Result<PathBuf> {
    Ok(project_root_dir()?.join("testnet-contracts"))
}

fn project_root_dir() -> anyhow::Result<PathBuf> {
    Ok(PathBuf::from_str(env!("CARGO_MANIFEST_DIR"))?
        .parent()
        .and_then(Path::parent)
        .expect("the directory structure guarantees this exists")
        .to_owned())
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
    /// The ID of the credits Object.
    pub credits_object: Option<ObjectID>,
    /// The ID of the credits package.
    pub credits_pkg_id: Option<ObjectID>,
    /// The ID of the walrus subsidies object.
    pub walrus_subsidies_object: Option<ObjectID>,
    /// The ID of the walrus subsidies package.
    pub walrus_subsidies_pkg_id: Option<ObjectID>,
    /// The number of shards in the system.
    pub n_shards: NonZeroU16,
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
        ContractConfig {
            system_object: self.system_object,
            staking_object: self.staking_object,
            credits_object: self.credits_object,
            walrus_subsidies_object: self.walrus_subsidies_object,
            n_shards: Some(self.n_shards),
            cache_ttl: DEFAULT_CACHE_TTL,
        }
    }
}

/// Publishes the test contracts and initializes the system allowing to specify the
/// deploy directory. If not specified a fresh temp directory is used.
///
/// Returns the package id and the object IDs of the system object and the staking object.
#[allow(clippy::too_many_arguments)]
pub async fn create_and_init_system_for_test(
    admin_wallet: Wallet,
    n_shards: NonZeroU16,
    epoch_zero_duration: Duration,
    epoch_duration: Duration,
    max_epochs_ahead: Option<EpochCount>,
    with_credits: bool,
    deploy_directory: Option<PathBuf>,
    contract_dir: Option<PathBuf>,
) -> Result<(SystemContext, SuiContractClient)> {
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
            with_credits,
            with_walrus_subsidies: true,
        },
        None,
    )
    .await
}

/// Publishes the contracts and initializes the system with the provided parameters.
///
/// Returns a `SystemContext` containing the package ID and object IDs for the system, staking,
/// upgrade manager, and optionally credits and walrus subsidies objects, as well as an initialized
/// walrus client with the admin wallet.
///
/// If `deploy_directory` is provided, the contracts will be copied to this directory and published
/// from there to keep the `Move.toml` in the original directory unchanged.
pub async fn create_and_init_system(
    mut admin_wallet: Wallet,
    init_system_params: InitSystemParams,
    gas_budget: Option<u64>,
) -> Result<(SystemContext, SuiContractClient)> {
    let init_system_params_cloned = init_system_params.clone();
    let n_shards = init_system_params.n_shards;
    let PublishSystemPackageResult {
        walrus_pkg_id,
        init_cap_id,
        upgrade_cap_id,
        wal_exchange_pkg_id,
        credits_pkg_id,
        walrus_subsidies_pkg_id,
    } = system_setup::publish_coin_and_system_package(
        &mut admin_wallet,
        init_system_params_cloned,
        gas_budget,
    )
    .await?;

    let (system_object, staking_object, upgrade_manager_object) =
        system_setup::create_system_and_staking_objects(
            &mut admin_wallet,
            walrus_pkg_id,
            init_cap_id,
            upgrade_cap_id,
            init_system_params,
            gas_budget,
        )
        .await?;

    let contract_config = ContractConfig::new(system_object, staking_object);

    let rpc_urls = &[admin_wallet.get_rpc_url()?];
    let admin_contract_client = SuiContractClient::new(
        admin_wallet,
        rpc_urls,
        &contract_config,
        ExponentialBackoffConfig::default(),
        gas_budget,
    )
    .await?;

    // Create credits object if enabled.
    let credits_object = if let Some(pkg_id) = credits_pkg_id {
        let credits_object_id = admin_contract_client
            .create_and_fund_credits(
                pkg_id,
                DEFAULT_BUYER_CREDITS_RATE,
                DEFAULT_SYSTEM_CREDITS_RATE,
                DEFAULT_CREDITS_FUNDS,
            )
            .await?
            .object_id;

        admin_contract_client
            .read_client()
            .set_credits_object(Some(credits_object_id))
            .await?;
        Some(credits_object_id)
    } else {
        None
    };

    // Create walrus subsidies object if enabled.
    let walrus_subsidies_object = if let Some(pkg_id) = walrus_subsidies_pkg_id {
        let walrus_subsidies_object_id = admin_contract_client
            .create_walrus_subsidies(
                pkg_id,
                DEFAULT_SYSTEM_SUBSIDY_RATE,
                DEFAULT_BASE_SUBSIDY,
                DEFAULT_SUBSIDY_PER_SHARD,
            )
            .await?
            .object_id;
        admin_contract_client
            .read_client()
            .set_walrus_subsidies_object(Some(walrus_subsidies_object_id))
            .await?;
        admin_contract_client
            .fund_walrus_subsidies(DEFAULT_SUBSIDIES_FUNDS)
            .await?;
        Some(walrus_subsidies_object_id)
    } else {
        None
    };

    Ok((
        SystemContext {
            walrus_pkg_id,
            system_object,
            staking_object,
            upgrade_manager_object,
            credits_object,
            wal_exchange_pkg_id,
            credits_pkg_id,
            walrus_subsidies_pkg_id,
            walrus_subsidies_object,
            n_shards,
        },
        admin_contract_client,
    ))
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
    with_credits: bool,
    subsidy_rate: u16,
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
                publish_with_default_system_with_epoch_duration(
                    admin_wallet,
                    &bls_keys,
                    epoch_duration,
                    with_credits,
                    subsidy_rate,
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
    admin_wallet: Wallet,
    bls_keys: &[ProtocolKeyPair],
    epoch_duration: Duration,
    with_credits: bool,
    subsidy_rate: u16,
) -> anyhow::Result<(SystemContext, SuiContractClient)> {
    let (system_context, contract_client) = create_and_init_system_for_test(
        admin_wallet,
        NonZeroU16::new(1000).expect("1000 is not 0"),
        Duration::from_secs(0),
        epoch_duration,
        None,
        with_credits,
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

    if let Some(credits_pkg_id) = system_context.credits_pkg_id {
        let credits_object_id = contract_client
            .create_and_fund_credits(credits_pkg_id, subsidy_rate, subsidy_rate, MEGA_WAL)
            .await?
            .object_id;

        contract_client
            .read_client()
            .set_credits_object(Some(credits_object_id))
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
