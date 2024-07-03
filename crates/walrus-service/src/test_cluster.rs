// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A module for creating a test Walrus cluster in process.

use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};

use tokio::sync::Mutex;
use walrus_sui::{
    client::{SuiContractClient, SuiReadClient},
    system_setup::{create_system_object, publish_package, SystemParameters},
    test_utils,
    test_utils::{
        new_wallet_on_sui_test_cluster,
        system_setup::contract_path_for_testing,
        TestClusterHandle,
    },
    types::Committee,
};
use walrus_test_utils::WithTempDir;

use crate::{
    client::{Client, ClientCommunicationConfig, Config},
    committee::SuiCommitteeServiceFactory,
    contract_service::SuiSystemContractService,
    system_events::SuiSystemEventProvider,
    test_utils::TestCluster,
};

/// Performs the default setup for the test cluster.
pub async fn default_setup() -> anyhow::Result<(
    Arc<TestClusterHandle>,
    TestCluster,
    WithTempDir<Client<SuiContractClient>>,
)> {
    #[cfg(not(msim))]
    let sui_cluster = test_utils::using_tokio::global_sui_test_cluster();
    #[cfg(msim)]
    let sui_cluster = test_utils::using_msim::global_sui_test_cluster().await;

    // Get a wallet on the global sui test cluster
    let mut wallet = new_wallet_on_sui_test_cluster(sui_cluster.clone()).await?;

    let cluster_builder = TestCluster::builder();

    // Get the default committee from the test cluster builder
    let members = cluster_builder
        .storage_node_test_configs()
        .iter()
        .enumerate()
        .map(|(i, info)| info.to_storage_node_info(&format!("node-{i}")))
        .collect();

    // Publish package and set up system object
    let gas_budget = 500_000_000;
    let (system_pkg, committee_cap) = publish_package(
        &mut wallet.inner,
        contract_path_for_testing("blob_store")?,
        gas_budget,
    )
    .await?;
    let committee = Committee::new(members, 0)?;
    let system_params = SystemParameters::new_with_sui(committee, 1_000_000_000_000, 10);
    let system_object = create_system_object(
        &mut wallet.inner,
        system_pkg,
        committee_cap,
        &system_params,
        gas_budget,
    )
    .await?;

    // Build the walrus cluster
    let sui_read_client =
        SuiReadClient::new(wallet.as_ref().get_client().await?, system_object).await?;

    // Create a contract service for the storage nodes using a wallet in a temp dir
    // The sui test cluster handler can be dropped since we already have one
    let sui_contract_service = new_wallet_on_sui_test_cluster(sui_cluster.clone())
        .await?
        .and_then(|wallet| {
            SuiContractClient::new_with_read_client(wallet, gas_budget, sui_read_client.clone())
        })?
        .map(SuiSystemContractService::new);

    // Set up the cluster
    let cluster_builder = cluster_builder
        .with_committee_service_factories(SuiCommitteeServiceFactory::new(
            sui_read_client.clone(),
            Default::default(),
        ))
        .with_system_event_providers(SuiSystemEventProvider::new(
            sui_read_client.clone(),
            Duration::from_millis(100),
        ))
        .with_system_contract_services(Arc::new(sui_contract_service));

    let cluster = {
        // Lock to avoid race conditions.
        let _lock = global_test_lock().lock().await;
        cluster_builder.build().await?
    };

    // Create the client with a separate wallet
    let sui_contract_client = new_wallet_on_sui_test_cluster(sui_cluster.clone())
        .await?
        .and_then(|wallet| {
            SuiContractClient::new_with_read_client(wallet, gas_budget, sui_read_client)
        })?;
    let config = Config {
        system_object,
        wallet_config: None,
        communication_config: ClientCommunicationConfig::default_for_test(),
    };

    let client = sui_contract_client
        .and_then_async(|contract_client| Client::new(config, contract_client))
        .await?;
    Ok((sui_cluster, cluster, client))
}

// Prevent tests running simultaneously to avoid interferences or race conditions.
fn global_test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(Mutex::default)
}
