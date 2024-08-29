// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{num::NonZeroU16, time::Duration};

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
        responses::BlobStoreResult,
        ClientError,
        ClientErrorKind::{self, NoMetadataReceived, NotEnoughConfirmations, NotEnoughSlivers},
    },
    test_utils::test_cluster,
};
use walrus_sui::{
    client::{ContractClient, ReadClient},
    types::BlobEvent,
};
use walrus_test_utils::async_param_test;

async_param_test! {
    test_store_and_read_blob_without_failures : [
        #[ignore = "ignore E2E tests by default"] #[tokio::test] empty: (0),
        #[ignore = "ignore E2E tests by default"] #[tokio::test] one_byte: (1),
    ]
}
async fn test_store_and_read_blob_without_failures(blob_size: usize) {
    assert!(matches!(
        run_store_and_read_with_crash_failures(&[], &[], blob_size).await,
        Ok(()),
    ))
}

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
                        expected_errs, client_err
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
) -> anyhow::Result<()> {
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
        .reserve_and_store_blob(&blob, 1, true)
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
    test_inconsistency -> anyhow::Result<()> : [
        #[ignore = "ignore E2E tests by default"] #[tokio::test] no_failures: (&[]),
        #[ignore = "ignore E2E tests by default"] #[tokio::test] one_failure: (&[0]),
        #[ignore = "ignore E2E tests by default"] #[tokio::test] f_failures: (&[4]),
    ]
}
/// Stores a blob that is inconsistent in shard 1
async fn test_inconsistency(failed_shards: &[usize]) -> anyhow::Result<()> {
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
    let blob_sui_object = client
        .as_ref()
        .reserve_and_register_blob(&metadata, 1)
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
        .as_mut()
        .sui_client()
        .certify_blob(blob_sui_object, &certificate)
        .await?;

    // Wait to receive an inconsistent blob event.
    let events = client
        .as_mut()
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

async_param_test! {
    test_store_with_existing_blob_resource -> anyhow::Result<()> : [
        #[ignore = "ignore E2E tests by default"] #[tokio::test] reuse_resource: (1, 1, true),
        #[ignore = "ignore E2E tests by default"] #[tokio::test] reuse_resource_two: (2, 1, true),
        #[ignore = "ignore E2E tests by default"] #[tokio::test] no_reuse_resource: (1, 2, false),
    ]
}
/// Tests that the client reuses existing (uncertified) blob registrations to store blobs.
///
/// The `epochs_ahead_registered` are the epochs ahead of the already-existing blob object.
/// The `epochs_ahead_required` are the epochs ahead that are requested to the client when
/// registering anew.
/// `should_match` is a boolean that indicates if the storage object used in the final upload should
/// be the same as the first one registered.
async fn test_store_with_existing_blob_resource(
    epochs_ahead_registered: u64,
    epochs_ahead_required: u64,
    should_match: bool,
) -> anyhow::Result<()> {
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
    let original_blob_object = client
        .as_ref()
        .reserve_and_register_blob(&metadata, epochs_ahead_registered)
        .await?;

    // Wait to ensure that the storage nodes received the registration event.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Now ask the client to store again.
    let blob_store = client
        .inner
        .reserve_and_store_blob(&blob, epochs_ahead_required, false)
        .await?;

    if let BlobStoreResult::NewlyCreated { blob_object, .. } = blob_store {
        // Check if the storage object used is the same.
        assert!(should_match == (blob_object.id == original_blob_object.id));
    } else {
        panic!("the client should be able to store the blob")
    };

    Ok(())
}
