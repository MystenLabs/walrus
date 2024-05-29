// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    num::NonZeroU16,
    sync::{Arc, OnceLock},
    time::Duration,
};

use test_cluster::TestCluster as SuiTestCluster;
use tokio::sync::Mutex;
use tokio_stream::StreamExt;
use walrus_core::{
    encoding::Primary,
    merkle::Node,
    metadata::VerifiedBlobMetadataWithId,
    BlobId,
    SliverPairIndex,
};
use walrus_service::{
    client::{
        Client,
        ClientCommunicationConfig,
        ClientError,
        ClientErrorKind::{self, NoMetadataReceived, NotEnoughConfirmations, NotEnoughSlivers},
        Config,
    },
    committee::SuiCommitteeServiceFactory,
    contract_service::SuiSystemContractService,
    system_events::SuiSystemEventProvider,
    test_utils::TestCluster,
};
use walrus_sui::{
    client::{ContractClient, ReadClient, SuiContractClient, SuiReadClient},
    system_setup::{create_system_object, publish_package, SystemParameters},
    test_utils::{sui_test_cluster, system_setup::contract_path_for_testing, wallet_for_testing},
    types::{BlobEvent, Committee},
};
use walrus_test_utils::{async_param_test, WithTempDir};

async_param_test! {
    test_store_and_read_blob_with_crash_failures : [
        #[ignore = "ignore E2E tests by default"] #[tokio::test] no_failures: (&[], &[], &[]),
        #[ignore = "ignore E2E tests by default"] #[tokio::test] one_failure: (&[0], &[], &[]),
        #[ignore = "ignore E2E tests by default"] #[tokio::test] f_failures: (&[4], &[], &[]),
        #[ignore = "ignore E2E tests by default"] #[tokio::test] f_plus_one_failures:
            (&[0, 4], &[], &[NotEnoughConfirmations(8, 9)]),
        #[ignore = "ignore E2E tests by default"] #[tokio::test] all_shard_failures:
            (&[0, 1, 2, 3, 4], &[], &[NotEnoughConfirmations(0, 9)]),
        #[ignore = "ignore E2E tests by default"] #[tokio::test] f_plus_one_read_failures:
            (&[], &[0, 4], &[]),
        #[ignore = "ignore E2E tests by default"] #[tokio::test] two_f_plus_one_read_failures:
            (&[], &[1, 2, 4], &[NoMetadataReceived, NotEnoughSlivers]),
        #[ignore = "ignore E2E tests by default"] #[tokio::test] read_and_write_overlap_failures:
            (&[4], &[2, 3], &[NoMetadataReceived, NotEnoughSlivers]),
    ]
}
async fn test_store_and_read_blob_with_crash_failures(
    failed_shards_write: &[usize],
    failed_shards_read: &[usize],
    expected_errors: &[ClientErrorKind],
) {
    let _ = tracing_subscriber::fmt::try_init();
    let result =
        run_store_and_read_with_crash_failures(failed_shards_write, failed_shards_read).await;

    match (result, expected_errors) {
        (Ok(()), []) => (),
        (Err(actual_err), expected_errs) => match actual_err.downcast::<ClientError>() {
            Ok(client_err) => {
                if !expected_errs
                    .iter()
                    .any(|expected_err| error_kind_matches(client_err.kind(), expected_err))
                {
                    panic!(
                        "client error mismatch; expected=({:?}); actual=({:?});",
                        expected_errs, client_err
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

    let (_sui_cluster, mut cluster, mut client) = default_setup().await?;

    // Stop the nodes in the write failure set.
    failed_shards_write
        .iter()
        .for_each(|&idx| cluster.cancel_node(idx));

    // Store a blob and get confirmations from each node.
    let blob = walrus_test_utils::random_data(31415);
    let blob_confirmation = client.as_ref().reserve_and_store_blob(&blob, 1).await?;

    // Stop the nodes in the read failure set.
    failed_shards_read
        .iter()
        .for_each(|&idx| cluster.cancel_node(idx));

    // We need to reset the reqwest client to ensure that the client cannot communicate with nodes
    // that are being shut down.
    client.as_mut().reset_reqwest_client()?;

    // Read the blob.
    let read_blob = client
        .as_ref()
        .read_blob::<Primary>(&blob_confirmation.blob_id)
        .await?;

    assert_eq!(read_blob, blob);

    Ok(())
}

async_param_test! {
    test_inconsistency -> anyhow::Result<()> : [
        #[ignore = "ignore E2E tests by default"] #[tokio::test] no_failures: (&[]),
        #[ignore = "ignore E2E tests by default"] #[tokio::test] one_failure: (&[0]),
        #[ignore = "ignore E2E tests by default"] #[tokio::test] f_failures: (&[4]),
    ]
}
/// Stores a blob that is inconsistent in shard 1
async fn test_inconsistency(failed_shards: &[usize]) -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let (_sui_cluster, mut cluster, client) = default_setup().await?;

    // Store a blob and get confirmations from each node.
    let blob = walrus_test_utils::random_data(31415);

    // Encode the blob with false metadata for one shard.
    let (pairs, metadata) = client
        .as_ref()
        .encoding_config()
        .get_blob_encoder(&blob)?
        .encode_with_metadata();
    let mut metadata = metadata.metadata().to_owned();
    metadata.hashes[1].primary_hash = Node::Digest([0; 32]);
    let blob_id = BlobId::from_sliver_pair_metadata(&metadata);
    let metadata = VerifiedBlobMetadataWithId::new_verified_unchecked(blob_id, metadata);

    tracing::debug!(
        "Shard index for inconsistent sliver: {}",
        SliverPairIndex::new(1).to_shard_index(NonZeroU16::new(13).unwrap(), &blob_id)
    );
    // Register blob.
    let blob_sui_object = client
        .as_ref()
        .reserve_blob(&metadata, blob.len(), 1)
        .await?;

    // Wait to ensure that the storage nodes received the registration event.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Certify blob.
    let certificate = client
        .as_ref()
        .store_metadata_and_pairs(&metadata, &pairs)
        .await?;

    // Stop the nodes in the failure set.
    failed_shards
        .iter()
        .for_each(|&idx| cluster.cancel_node(idx));

    client
        .as_ref()
        .sui_client()
        .certify_blob(&blob_sui_object, &certificate)
        .await?;

    // Wait to receive an inconsistent blob event.
    let events = client
        .as_ref()
        .sui_client()
        .read_client()
        .blob_events(Duration::from_millis(50), None)
        .await?;
    let mut events = std::pin::pin!(events);
    tokio::time::timeout(Duration::from_secs(30), async {
        while let Some(event) = events.next().await {
            tracing::debug!("Event: {:?}", event);
            if let BlobEvent::InvalidBlobID(_) = event {
                return;
            }
        }
        panic!("should be infinite stream")
    })
    .await
    .map_err(anyhow::Error::new)
}

async fn default_setup() -> anyhow::Result<(
    SuiTestCluster,
    TestCluster,
    WithTempDir<Client<SuiContractClient>>,
)> {
    // Set up the sui test cluster
    let mut sui_test_cluster = sui_test_cluster().await;
    let wallet = &mut sui_test_cluster.wallet;

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
    let (system_pkg, committee_cap) =
        publish_package(wallet, contract_path_for_testing("blob_store")?, gas_budget).await?;
    let committee = Committee::new(members, 0)?;
    let system_params = SystemParameters::new_with_sui(committee, 1_000_000_000_000, 10);
    let system_object = create_system_object(
        wallet,
        system_pkg,
        committee_cap,
        &system_params,
        gas_budget,
    )
    .await?;

    // Build the walrus cluster
    let sui_read_client =
        SuiReadClient::new(wallet.get_client().await?, system_pkg, system_object).await?;

    // Create a contract service for the storage nodes using a wallet in a temp dir
    let sui_contract_service = wallet_for_testing(wallet)
        .await?
        .and_then(|wallet| {
            SuiContractClient::new_with_read_client(wallet, gas_budget, sui_read_client.clone())
        })?
        .map(SuiSystemContractService::new);

    // Set up the cluster
    let cluster_builder = cluster_builder
        .with_committee_service_factories(SuiCommitteeServiceFactory::new(sui_read_client.clone()))
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

    // Ensure that the servers in the cluster have sufficient time to get ready.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Create the client with a separate wallet
    let sui_contract_client = wallet_for_testing(wallet).await?.and_then(|wallet| {
        SuiContractClient::new_with_read_client(wallet, gas_budget, sui_read_client)
    })?;
    let config = Config {
        system_pkg,
        system_object,
        wallet_config: None,
        communication_config: ClientCommunicationConfig::default_for_test(),
    };

    let client = sui_contract_client
        .and_then_async(|contract_client| Client::new(config, contract_client))
        .await?;
    Ok((sui_test_cluster, cluster, client))
}

// Prevent tests running simultaneously to avoid interferences or race conditions.
fn global_test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(Mutex::default)
}

fn error_kind_matches(actual: &ClientErrorKind, expected: &ClientErrorKind) -> bool {
    match (actual, expected) {
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
