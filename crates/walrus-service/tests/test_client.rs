// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{sync::OnceLock, time::Duration};

use anyhow::anyhow;
use test_cluster::TestClusterBuilder as SuiTestClusterBuilder;
use tokio::sync::Mutex;
use walrus_core::encoding::Primary;
use walrus_service::{
    client::{Client, ClientCommunicationConfig, Config},
    committee::SuiCommitteeServiceFactory,
    system_events::SuiSystemEventProvider,
    test_utils::TestCluster,
};
use walrus_sui::{
    client::{SuiContractClient, SuiReadClient},
    system_setup::{create_system_object, publish_package, SystemParameters},
    test_utils::system_setup::contract_path_for_testing,
    types::Committee,
};
use walrus_test_utils::async_param_test;

async_param_test! {
    test_store_and_read_blob_with_crash_failures : [
        #[ignore = "ignore E2E tests by default"] #[tokio::test] no_failures: (&[], Ok(())),
        #[ignore = "ignore E2E tests by default"] #[tokio::test] shard_failure: (&[0], Ok(())),
        #[ignore = "ignore E2E tests by default"] #[tokio::test] f_shard_failure: (&[4], Ok(())),
        #[ignore = "ignore E2E tests by default"] #[tokio::test] f_plus_one_shard_failure:
            (&[0, 4], Err(anyhow!("not enough confirmations for the blob id were retrieved"))),
        #[ignore = "ignore E2E tests by default"] #[tokio::test] all_shard_failure:
            (
                &[0, 1, 2, 3, 4],
                Err(anyhow!("not enough confirmations for the blob id were retrieved"))
            ),
    ]
}
async fn test_store_and_read_blob_with_crash_failures(
    failed_nodes: &[usize],
    expected: anyhow::Result<()>,
) {
    let result = run_store_and_read_with_crash_failures(failed_nodes).await;
    match (result, expected) {
        (Ok(()), Ok(())) => (),
        (Err(actual_err), Err(expected_err)) => {
            assert_eq!(
                format!("{}", actual_err.root_cause()),
                format!("{}", expected_err.root_cause())
            )
        }
        (act, exp) => panic!(
            "test result mismatch; expected=({:?}); actual=({:?});",
            exp, act
        ),
    }
}

async fn run_store_and_read_with_crash_failures(failed_nodes: &[usize]) -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let cluster_builder = TestCluster::builder();

    // Get the default committee from the test cluster builder
    let members = cluster_builder
        .storage_node_test_configs()
        .iter()
        .enumerate()
        .map(|(i, info)| info.to_storage_node_info(&format!("node-{i}")))
        .collect();

    // Set up the sui test cluster
    let sui_test_cluster = SuiTestClusterBuilder::new().build().await;
    let mut wallet = sui_test_cluster.wallet;

    // Publish package and set up system object
    let gas_budget = 500_000_000;
    let (system_pkg, committee_cap) = publish_package(
        &mut wallet,
        contract_path_for_testing("blob_store")?,
        gas_budget,
    )
    .await?;
    let committee = Committee::new(members, 0)?;
    let system_params = SystemParameters::new_with_sui(committee, 1_000_000_000_000, 10);
    let system_object = create_system_object(
        &mut wallet,
        system_pkg,
        committee_cap,
        &system_params,
        gas_budget,
    )
    .await?;

    // Build the walrus cluster
    let sui_read_client =
        SuiReadClient::new(wallet.get_client().await?, system_pkg, system_object).await?;
    let cluster_builder = cluster_builder
        .with_committee_service_factories(SuiCommitteeServiceFactory::new(sui_read_client.clone()))
        .with_system_event_providers(SuiSystemEventProvider::new(
            sui_read_client,
            Duration::from_millis(100),
        ));

    let mut cluster = {
        // Lock to avoid race conditions.
        let _lock = global_test_lock().lock().await;
        cluster_builder.build().await?
    };

    let sui_contract_client =
        SuiContractClient::new(wallet, system_pkg, system_object, gas_budget).await?;
    let config = Config {
        system_pkg,
        system_object,
        wallet_config: None,
        communication_config: ClientCommunicationConfig::default(),
    };

    let client = Client::new(config, sui_contract_client).await?;

    // Stop the nodes in the failure set.
    failed_nodes
        .iter()
        .for_each(|&idx| cluster.cancel_node(idx));

    // Store a blob and get confirmations from each node.
    let blob = walrus_test_utils::random_data(31415);
    let blob_confirmation = client.reserve_and_store_blob(&blob, 1).await?;

    // Read the blob.
    let read_blob = client
        .read_blob::<Primary>(&blob_confirmation.blob_id)
        .await?;

    assert_eq!(read_blob, blob);

    Ok(())
}

// Prevent tests running simultaneously to avoid interferences or race conditions.
fn global_test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(Mutex::default)
}
