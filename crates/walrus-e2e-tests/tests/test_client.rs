// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{num::NonZeroU16, time::Duration};

use tokio_stream::StreamExt;
use walrus_core::{
    encoding::Primary,
    merkle::Node,
    metadata::VerifiedBlobMetadataWithId,
    BlobId,
    EpochCount,
    SliverPairIndex,
};
use walrus_proc_macros::walrus_simtest;
use walrus_sdk::api::BlobStatus;
use walrus_service::{
    client::{
        responses::BlobStoreResult,
        ClientError,
        ClientErrorKind::{
            self,
            NoMetadataReceived,
            NoValidStatusReceived,
            NotEnoughConfirmations,
            NotEnoughSlivers,
        },
        StoreWhen,
    },
    test_utils::{test_cluster, StorageNodeHandle},
};
use walrus_sui::{
    client::{BlobPersistence, ContractClient, ReadClient},
    types::{BlobEvent, ContractEvent},
};
use walrus_test_utils::{async_param_test, Result as TestResult};

async_param_test! {
    #[ignore = "ignore E2E tests by default"]
    #[walrus_simtest]
    test_store_and_read_blob_without_failures : [
        empty: (0),
        one_byte: (1),
    ]
}
async fn test_store_and_read_blob_without_failures(blob_size: usize) {
    assert!(matches!(
        run_store_and_read_with_crash_failures(&[], &[], blob_size).await,
        Ok(()),
    ))
}

async_param_test! {
    #[ignore = "ignore E2E tests by default"]
    #[walrus_simtest]
    test_store_and_read_blob_with_crash_failures : [
        no_failures: (&[], &[], &[]),
        one_failure: (&[0], &[], &[]),
        f_failures: (&[4], &[], &[]),
        f_plus_one_failures: (&[0, 4], &[], &[NotEnoughConfirmations(8, 9)]),
        all_shard_failures: (&[0, 1, 2, 3, 4], &[], &[NoValidStatusReceived]),
        f_plus_one_read_failures: (&[], &[0, 4], &[]),
        two_f_plus_one_read_failures: (
            &[], &[1, 2, 4], &[NoMetadataReceived, NotEnoughSlivers]),
        read_and_write_overlap_failures: (
            &[4], &[2, 3], &[NoMetadataReceived, NotEnoughSlivers]),
    ]
}
async fn test_store_and_read_blob_with_crash_failures(
    failed_shards_write: &[usize],
    failed_shards_read: &[usize],
    expected_errors: &[ClientErrorKind],
) {
    let _ = tracing_subscriber::fmt::try_init();
    let result =
        run_store_and_read_with_crash_failures(failed_shards_write, failed_shards_read, 31415)
            .await;

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
                        expected_errs,
                        client_err.kind()
                    )
                }
            }
            Err(err) => panic!("unexpected error {err}"),
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
    data_length: usize,
) -> TestResult {
    let _ = tracing_subscriber::fmt::try_init();

    let (_sui_cluster_handle, mut cluster, client) = test_cluster::default_setup().await?;

    // Stop the nodes in the write failure set.
    failed_shards_write
        .iter()
        .for_each(|&idx| cluster.cancel_node(idx));

    // Store a blob and get confirmations from each node.
    let blob = walrus_test_utils::random_data(data_length);
    let BlobStoreResult::NewlyCreated {
        blob_object: blob_confirmation,
        ..
    } = client
        .as_ref()
        .reserve_and_store_blob(&blob, 1, StoreWhen::Always, BlobPersistence::Permanent)
        .await?
    else {
        panic!("expect newly stored blob")
    };

    // Stop the nodes in the read failure set.
    failed_shards_read
        .iter()
        .for_each(|&idx| cluster.cancel_node(idx));

    // Read the blob.
    let read_blob = client
        .as_ref()
        .read_blob::<Primary>(&blob_confirmation.blob_id)
        .await?;

    assert_eq!(read_blob, blob);

    Ok(())
}

async_param_test! {
    #[ignore = "ignore E2E tests by default"]
    #[walrus_simtest]
    test_inconsistency -> TestResult : [
        no_failures: (&[]),
        one_failure: (&[0]),
        f_failures: (&[4]),
    ]
}
/// Stores a blob that is inconsistent in shard 1
async fn test_inconsistency(failed_shards: &[usize]) -> TestResult {
    let _ = tracing_subscriber::fmt::try_init();

    let (_sui_cluster_handle, mut cluster, mut client) = test_cluster::default_setup().await?;

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
    let (blob_sui_object, _) = client
        .as_ref()
        .resource_manager()
        .await
        .get_existing_registration(
            &metadata,
            1,
            BlobPersistence::Permanent,
            StoreWhen::NotStored,
        )
        .await?;

    // Certify blob.
    let certificate = client
        .as_ref()
        .send_blob_data_and_get_certificate(&metadata, &pairs)
        .await?;

    // Stop the nodes in the failure set.
    failed_shards
        .iter()
        .for_each(|&idx| cluster.cancel_node(idx));

    client
        .as_mut()
        .sui_client()
        .certify_blob(blob_sui_object, &certificate)
        .await?;

    // Wait to receive an inconsistent blob event.
    let events = client
        .as_mut()
        .sui_client()
        .event_stream(Duration::from_millis(50), None)
        .await?;
    let mut events = std::pin::pin!(events);
    tokio::time::timeout(Duration::from_secs(30), async {
        while let Some(event) = events.next().await {
            tracing::debug!("Event: {:?}", event);
            if let ContractEvent::BlobEvent(BlobEvent::InvalidBlobID(_)) = event {
                return;
            }
        }
        panic!("should be infinite stream")
    })
    .await?;

    Ok(())
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
        (ClientErrorKind::NoValidStatusReceived, ClientErrorKind::NoValidStatusReceived) => true,
        (ClientErrorKind::Other(_), ClientErrorKind::Other(_)) => true,
        (_, _) => false,
    }
}

async_param_test! {
    #[ignore = "ignore E2E tests by default"]
    #[walrus_simtest]
    test_store_with_existing_blob_resource -> TestResult : [
        reuse_resource: (1, 1, true),
        reuse_resource_two: (2, 1, true),
        no_reuse_resource: (1, 2, false),
    ]
}
/// Tests that the client reuses existing (uncertified) blob registrations to store blobs.
///
/// The `epochs_ahead_registered` are the epochs ahead of the already-existing blob object.
/// The `epochs_ahead_required` are the epochs ahead that are requested to the client when
/// registering anew.
/// `should_match` is a boolean that indicates if the blob object used in the final upload
/// should be the same as the first one registered.
async fn test_store_with_existing_blob_resource(
    epochs_ahead_registered: EpochCount,
    epochs_ahead_required: EpochCount,
    should_match: bool,
) -> TestResult {
    let _ = tracing_subscriber::fmt::try_init();

    let (_sui_cluster_handle, _cluster, client) = test_cluster::default_setup().await?;

    let blob = walrus_test_utils::random_data(31415);
    let (_, metadata) = client
        .as_ref()
        .encoding_config()
        .get_blob_encoder(&blob)?
        .encode_with_metadata();
    let metadata = metadata.metadata().to_owned();
    let blob_id = BlobId::from_sliver_pair_metadata(&metadata);
    let metadata = VerifiedBlobMetadataWithId::new_verified_unchecked(blob_id, metadata);

    // Register a new blob.
    let (original_blob_object, _) = client
        .as_ref()
        .resource_manager()
        .await
        .get_existing_registration(
            &metadata,
            epochs_ahead_registered,
            BlobPersistence::Permanent,
            StoreWhen::NotStored,
        )
        .await?;

    // Now ask the client to store again.
    let blob_store = client
        .inner
        .reserve_and_store_blob(
            &blob,
            epochs_ahead_required,
            StoreWhen::NotStored,
            BlobPersistence::Permanent,
        )
        .await?;

    if let BlobStoreResult::NewlyCreated { blob_object, .. } = blob_store {
        // Check if the storage object used is the same.
        assert!(should_match == (blob_object.id == original_blob_object.id));
    } else {
        panic!("the client should be able to store the blob")
    };

    Ok(())
}

async_param_test! {
    #[ignore = "ignore E2E tests by default"]
    #[walrus_simtest]
    test_store_with_existing_storage_resource -> TestResult : [
        reuse_storage: (1, 1, true),
        reuse_storage_two: (2, 1, true),
        no_reuse_storage: (1, 2, false),
    ]
}
/// Tests that the client reuses existing storage resources to store blobs.
///
/// The `epochs_ahead_registered` are the epochs ahead of the already-existing storage resource.
/// The `epochs_ahead_required` are the epochs ahead that are requested to the client when
/// registering anew.
/// `should_match` is a boolean that indicates if the storage object used in the final upload
/// should be the same as the first one registered.
async fn test_store_with_existing_storage_resource(
    epochs_ahead_registered: EpochCount,
    epochs_ahead_required: EpochCount,
    should_match: bool,
) -> TestResult {
    let _ = tracing_subscriber::fmt::try_init();

    let (_sui_cluster_handle, _cluster, client) = test_cluster::default_setup().await?;

    let blob = walrus_test_utils::random_data(31415);
    let (_, metadata) = client
        .as_ref()
        .encoding_config()
        .get_blob_encoder(&blob)?
        .encode_with_metadata();
    let metadata = metadata.metadata().to_owned();
    let blob_id = BlobId::from_sliver_pair_metadata(&metadata);
    let metadata = VerifiedBlobMetadataWithId::new_verified_unchecked(blob_id, metadata);

    // Register a new storage resource.
    let encoded_size = metadata
        .metadata()
        .encoded_size()
        .expect("we just encoded this blob");

    let original_storage_resource = client
        .as_ref()
        .sui_client()
        .reserve_space(encoded_size, epochs_ahead_registered)
        .await?;

    // Now ask the client to store again.

    let blob_store = client
        .inner
        .reserve_and_store_blob(
            &blob,
            epochs_ahead_required,
            StoreWhen::NotStored,
            BlobPersistence::Permanent,
        )
        .await?;

    if let BlobStoreResult::NewlyCreated { blob_object, .. } = blob_store {
        // Check if the storage object used is the same.
        assert!(should_match == (blob_object.storage.id == original_storage_resource.id));
    } else {
        panic!("the client should be able to store the blob")
    };

    Ok(())
}

async_param_test! {
    #[ignore = "ignore E2E tests by default"]
    #[walrus_simtest]
    test_delete_blob -> TestResult : [
        no_delete: (0),
        one_delete: (1),
        multi_delete: (2),
    ]
}
/// Tests blob object deletion.
async fn test_delete_blob(blobs_to_create: u32) -> TestResult {
    let _ = tracing_subscriber::fmt::try_init();
    let (_sui_cluster_handle, _cluster, client) = test_cluster::default_setup().await?;
    let blob = walrus_test_utils::random_data(314);

    // Store the blob multiple times, using separate end times to obtain multiple blob objects
    // with the same blob ID.
    for idx in 1..blobs_to_create + 1 {
        client
            .as_ref()
            .reserve_and_store_blob(&blob, idx, StoreWhen::Always, BlobPersistence::Deletable)
            .await?;
    }

    // Add a blob that is not deletable.
    let result = client
        .as_ref()
        .reserve_and_store_blob(&blob, 1, StoreWhen::Always, BlobPersistence::Permanent)
        .await?;
    let blob_id = result.blob_id();

    // Check that we have the correct number of blobs
    let blobs = client.as_ref().sui_client().owned_blobs(false).await?;
    assert_eq!(blobs.len(), blobs_to_create as usize + 1);

    // Delete the blobs
    let deleted = client.as_ref().delete_owned_blob(blob_id).await?;
    assert_eq!(deleted, blobs_to_create as usize);

    // Only one blob should remain: The non-deletable one.
    let blobs = client.as_ref().sui_client().owned_blobs(false).await?;
    assert_eq!(blobs.len(), 1);

    // TODO(mlegner): Check correct handling on nodes.

    Ok(())
}

#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_storage_nodes_delete_data_for_deleted_blobs() -> TestResult {
    let _ = tracing_subscriber::fmt::try_init();
    let (_sui_cluster_handle, _cluster, client) = test_cluster::default_setup().await?;
    let client = client.as_ref();
    let blob = walrus_test_utils::random_data(314);

    let store_result = client
        .reserve_and_store_blob(&blob, 1, StoreWhen::Always, BlobPersistence::Deletable)
        .await?;
    let blob_id = store_result.blob_id();
    assert!(matches!(store_result, BlobStoreResult::NewlyCreated { .. }));

    assert_eq!(client.read_blob::<Primary>(blob_id).await?, blob);

    client.delete_owned_blob(blob_id).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let status_result = client
        .get_verified_blob_status(
            blob_id,
            client.sui_client().read_client(),
            Duration::from_secs(1),
        )
        .await?;
    assert!(matches!(status_result, BlobStatus::Nonexistent));

    let read_result = client.read_blob::<Primary>(blob_id).await;
    assert!(matches!(
        read_result.unwrap_err().kind(),
        ClientErrorKind::BlobIdDoesNotExist,
    ));

    Ok(())
}

/// Tests that storing the same blob multiple times with possibly different end epochs,
/// persistence, and force-store conditions always works.
#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_multiple_stores_same_blob() -> TestResult {
    let _ = tracing_subscriber::fmt::try_init();
    let (_sui_cluster_handle, _cluster, client) = test_cluster::default_setup().await?;
    let client = client.as_ref();
    let blob = walrus_test_utils::random_data(314);

    // NOTE: not in a param_test, because we want to test these store operations in sequence.
    // If the last `bool` parameter is `true`, the store operation should return a
    // `BlobStoreResult::AlreadyCertified`. Otherwise, it should return a
    // `BlobStoreResult::NewlyCreated`.
    let configurations = vec![
        (1, StoreWhen::NotStored, BlobPersistence::Deletable, false),
        (1, StoreWhen::Always, BlobPersistence::Deletable, false),
        (2, StoreWhen::NotStored, BlobPersistence::Deletable, false),
        (3, StoreWhen::Always, BlobPersistence::Deletable, false),
        (1, StoreWhen::NotStored, BlobPersistence::Permanent, false),
        (1, StoreWhen::NotStored, BlobPersistence::Permanent, true),
        (1, StoreWhen::Always, BlobPersistence::Permanent, false),
        (4, StoreWhen::NotStored, BlobPersistence::Permanent, false),
        (2, StoreWhen::NotStored, BlobPersistence::Permanent, true),
        (2, StoreWhen::Always, BlobPersistence::Permanent, false),
        (1, StoreWhen::NotStored, BlobPersistence::Deletable, true),
        (5, StoreWhen::NotStored, BlobPersistence::Deletable, false),
    ];

    for (epochs, store_when, persistence, is_already_certified) in configurations {
        let result = client
            .reserve_and_store_blob(&blob, epochs, store_when, persistence)
            .await?;

        match result {
            BlobStoreResult::NewlyCreated { .. } => {
                assert!(!is_already_certified, "the blob should be newly stored");
            }
            BlobStoreResult::AlreadyCertified { .. } => {
                assert!(is_already_certified, "the blob should be already stored");
            }
            _ => panic!("we either store the blob, or find it's already created"),
        }
    }

    // At the end of all the operations above, count the number of blob objects owned by the
    // client.
    let blobs = client.sui_client().owned_blobs(false).await?;
    assert_eq!(blobs.len(), 9);

    Ok(())
}

// Tests moving a shard to a storage node and then move it back.
#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_repeated_shard_move() -> TestResult {
    let _ = tracing_subscriber::fmt::try_init();
    let (_sui_cluster_handle, walrus_cluster, client) =
        test_cluster::default_setup_with_epoch_duration_generic::<StorageNodeHandle>(
            Duration::from_secs(20),
            &[1, 1],
        )
        .await?;

    client
        .as_ref()
        .stake_with_node_pool(
            walrus_cluster.nodes[1]
                .storage_capability
                .as_ref()
                .unwrap()
                .node_id,
            1_000_000_000,
        )
        .await?;

    walrus_cluster.wait_for_nodes_to_reach_epoch(4).await;
    assert_eq!(walrus_cluster.nodes[0].storage_node.shards().len(), 0);
    assert_eq!(walrus_cluster.nodes[1].storage_node.shards().len(), 2);

    client
        .as_ref()
        .stake_with_node_pool(
            walrus_cluster.nodes[0]
                .storage_capability
                .as_ref()
                .unwrap()
                .node_id,
            500_000_000_000,
        )
        .await?;

    walrus_cluster.wait_for_nodes_to_reach_epoch(7).await;
    assert_eq!(walrus_cluster.nodes[0].storage_node.shards().len(), 2);
    assert_eq!(walrus_cluster.nodes[1].storage_node.shards().len(), 0);

    Ok(())
}
