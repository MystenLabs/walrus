// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{sync::OnceLock, time::Duration};

use tokio::sync::Mutex;
use walrus_core::encoding::Primary;
use walrus_service::{
    client::{
        Client,
        ClientCommunicationConfig,
        ClientError,
        ClientErrorKind::{self, NoMetadataReceived, NotEnoughConfirmations, NotEnoughSlivers},
        Config,
    },
    committee::SuiCommitteeServiceFactory,
    system_events::SuiSystemEventProvider,
    test_utils::TestCluster,
};
use walrus_sui::{
    client::{SuiContractClient, SuiReadClient},
    system_setup::{create_system_object, publish_package, SystemParameters},
    test_utils::{sui_test_cluster, system_setup::contract_path_for_testing},
    types::Committee,
};
use walrus_test_utils::async_param_test;

async_param_test! {
    test_store_and_read_blob_with_crash_failures : [
        #[ignore = "ignore E2E tests by default"] #[tokio::test] no_failures: (&[], &[], None),
        #[ignore = "ignore E2E tests by default"] #[tokio::test] one_failure: (&[0], &[], None),
        #[ignore = "ignore E2E tests by default"] #[tokio::test] f_failures: (&[4], &[], None),
        #[ignore = "ignore E2E tests by default"] #[tokio::test] f_plus_one_failures:
            (&[0, 4], &[], Some(NotEnoughConfirmations(8, 9))),
        #[ignore = "ignore E2E tests by default"] #[tokio::test] all_shard_failures:
            (&[0, 1, 2, 3, 4], &[], Some(NotEnoughConfirmations(0, 9))),
        #[ignore = "ignore E2E tests by default"] #[tokio::test] f_plus_one_read_failures:
            (&[], &[0, 4], None),
        #[ignore = "ignore E2E tests by default"] #[tokio::test] two_f_plus_one_read_failures:
            (&[], &[1, 2, 4], Some(NotEnoughSlivers)),
        #[ignore = "ignore E2E tests by default"] #[tokio::test] all_read_failures:
            (&[], &[0, 1, 2, 3, 4], Some(NoMetadataReceived)),
        #[ignore = "ignore E2E tests by default"] #[tokio::test] read_and_write_overlap_failures:
            (&[4], &[2, 3], Some(NotEnoughSlivers)),
    ]
}
async fn test_store_and_read_blob_with_crash_failures(
    failed_shards_write: &[usize],
    failed_shards_read: &[usize],
    expected: Option<ClientErrorKind>,
) {
    let result =
        run_store_and_read_with_crash_failures(failed_shards_write, failed_shards_read).await;

    match (result, expected) {
        (Ok(()), None) => (),
        (Err(actual_err), Some(expected_err)) => match actual_err.downcast::<ClientError>() {
            Ok(client_err) => {
                if !error_kind_matches(client_err.kind(), &expected_err) {
                    panic!(
                        "client error mismatch; expected=({:?}); actual=({:?});",
                        expected_err, client_err
                    )
                }
            }
            Err(_) => panic!("unexpected error"),
        },
        (act, exp) => panic!(
            "test result mismatch; expected=({:?}); actual=({:?});",
            exp, act
        ),
    }
}

async fn run_store_and_read_with_crash_failures(
    failed_shards_write: &[usize],
    failed_shards_read: &[usize],
) -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    // Set up the sui test cluster
    let sui_test_cluster = sui_test_cluster().await;
    let mut wallet = sui_test_cluster.wallet;

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

    // Endsure that the servers in the cluster have sufficient time to get ready.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let sui_contract_client =
        SuiContractClient::new(wallet, system_pkg, system_object, gas_budget).await?;
    let config = Config {
        system_pkg,
        system_object,
        wallet_config: None,
        communication_config: ClientCommunicationConfig::default(),
    };

    let client = Client::new(config, sui_contract_client).await?;

    // Stop the nodes in the write failure set.
    failed_shards_write
        .iter()
        .for_each(|&idx| cluster.cancel_node(idx));

    // Store a blob and get confirmations from each node.
    let blob = walrus_test_utils::random_data(31415);
    let blob_confirmation = client.reserve_and_store_blob(&blob, 1).await?;

    // Stop the nodes in the read failure set.
    failed_shards_read
        .iter()
        .for_each(|&idx| cluster.cancel_node(idx));

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

fn error_kind_matches(actual: &ClientErrorKind, expected: &ClientErrorKind) -> bool {
    match (actual, expected) {
        (ClientErrorKind::CertificationFailed(_), ClientErrorKind::CertificationFailed(_)) => true,
        (
            ClientErrorKind::NotEnoughConfirmations(act_a, act_b),
            ClientErrorKind::NotEnoughConfirmations(exp_a, exp_b),
        ) => act_a == exp_a && act_b == exp_b,
        (ClientErrorKind::NotEnoughSlivers, ClientErrorKind::NotEnoughSlivers) => true,
        (ClientErrorKind::BlobIdDoesNotExist, ClientErrorKind::BlobIdDoesNotExist) => true,
        (ClientErrorKind::NoMetadataReceived, ClientErrorKind::NoMetadataReceived) => true,
        (ClientErrorKind::Other(_), ClientErrorKind::Other(_)) => true,
        (_, _) => false,
    }
}
