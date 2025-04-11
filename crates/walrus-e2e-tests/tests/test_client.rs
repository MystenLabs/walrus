// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Contains end-to-end tests for the Walrus client interacting with a Walrus test cluster.

#[cfg(msim)]
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};
use std::{
    collections::{HashMap, HashSet},
    num::NonZeroU16,
    path::{Path, PathBuf},
    time::Duration,
};

use indicatif::MultiProgress;
use rand::random;
#[cfg(msim)]
use sui_macros::{clear_fail_point, register_fail_point_if};
use sui_types::base_types::{SuiAddress, SUI_ADDRESS_LENGTH};
use tempfile::TempDir;
use tokio_stream::StreamExt;
use walrus_core::{
    encoding::{EncodingConfigTrait as _, Primary},
    merkle::Node,
    messages::BlobPersistenceType,
    metadata::VerifiedBlobMetadataWithId,
    BlobId,
    EncodingType,
    EpochCount,
    ShardIndex,
    SliverPairIndex,
    DEFAULT_ENCODING,
};
use walrus_proc_macros::walrus_simtest;
use walrus_rest_client::api::BlobStatus;
use walrus_sdk::{
    client::{responses::BlobStoreResult, Blocklist, Client, WalrusStoreBlob, WalrusStoreBlobApi},
    config::ClientCommunicationConfig,
    error::{
        ClientError,
        ClientErrorKind::{
            self,
            NoMetadataReceived,
            NoValidStatusReceived,
            NotEnoughConfirmations,
            NotEnoughSlivers,
        },
    },
    store_when::StoreWhen,
};
use walrus_service::test_utils::{
    test_cluster::{self, FROST_PER_NODE_WEIGHT},
    StorageNodeHandle,
    StorageNodeHandleTrait,
    TestNodesConfig,
    DEFAULT_SUBSIDY_FUNDS,
};
use walrus_sui::{
    client::{
        BlobPersistence,
        ExpirySelectionPolicy,
        PostStoreAction,
        ReadClient,
        SuiClientError,
        SuiContractClient,
        UpgradeType,
    },
    types::{
        move_errors::{MoveExecutionError, RawMoveError},
        move_structs::{BlobAttribute, SharedBlob, Subsidies},
        Blob,
        BlobEvent,
        ContractEvent,
    },
};
use walrus_test_utils::{assert_unordered_eq, async_param_test, Result as TestResult, WithTempDir};

async_param_test! {
    #[ignore = "ignore E2E tests by default"]
    #[walrus_simtest]
    test_store_and_read_blob_without_failures : [
        empty: (0),
        one_byte: (1),
        large_byte: (30000),
    ]
}
async fn test_store_and_read_blob_without_failures(blob_size: usize) {
    telemetry_subscribers::init_for_testing();
    assert!(matches!(
        run_store_and_read_with_crash_failures(&[], &[], blob_size).await,
        Ok(()),
    ))
}

/// Basic read and write test for the client.
///
/// It generates random blobs and stores them.
/// It then reads the blobs back and verifies that the data is correct.
pub async fn basic_store_and_read<F>(
    client: &WithTempDir<Client<SuiContractClient>>,
    num_blobs: usize,
    data_length: usize,
    pre_read_hook: F,
) -> TestResult
where
    F: FnOnce() -> TestResult,
{
    // Generate random blobs.
    let blob_data = walrus_test_utils::random_data_list(data_length, num_blobs);
    let mut path_to_data: HashMap<PathBuf, Vec<u8>> = HashMap::new();
    let mut blobs_with_paths: Vec<(PathBuf, Vec<u8>)> = vec![];
    let mut path_to_blob_id: HashMap<PathBuf, BlobId> = HashMap::new();

    // Create paths for each blob.
    for (i, data) in blob_data.iter().enumerate() {
        let path = PathBuf::from(format!("blob_{}", i));
        path_to_data.insert(path.clone(), data.to_vec());
        blobs_with_paths.push((path, data.to_vec()));
    }

    let store_result = client
        .as_ref()
        .reserve_and_store_blobs_retry_committees_with_path(
            &blobs_with_paths,
            DEFAULT_ENCODING,
            1,
            StoreWhen::Always,
            BlobPersistence::Permanent,
            PostStoreAction::Keep,
        )
        .await?;

    for result in store_result {
        match result.blob_store_result {
            BlobStoreResult::NewlyCreated { blob_object, .. } => {
                assert_eq!(
                    blob_object.encoding_type, DEFAULT_ENCODING,
                    "Stored blob has incorrect encoding type"
                );
                path_to_blob_id.insert(result.path, blob_object.blob_id);
            }
            _ => panic!(
                "Expected NewlyCreated result, got: {:?}",
                result.blob_store_result
            ),
        };
    }

    // Call the pre-read hook before reading data.
    pre_read_hook()?;

    // Read back and verify all blobs.
    for (path, blob_id) in path_to_blob_id {
        let read_data = client.as_ref().read_blob::<Primary>(&blob_id).await?;

        assert_eq!(
            read_data,
            path_to_data[&path],
            "Data mismatch for blob at path {}",
            path.display()
        );
    }

    Ok(())
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
    telemetry_subscribers::init_for_testing();
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

    // Create closure that will stop nodes for read failures.
    let pre_read_hook = {
        let cluster = &mut cluster;
        let failed_nodes = failed_shards_read.to_vec();
        move || {
            failed_nodes
                .iter()
                .for_each(|&idx| cluster.cancel_node(idx));
            Ok(())
        }
    };

    // Use basic_store_and_read with our pre_read_hook.
    basic_store_and_read(&client, 4, data_length, pre_read_hook).await
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
/// Stores a blob that is inconsistent in 1 shard
async fn test_inconsistency(failed_nodes: &[usize]) -> TestResult {
    telemetry_subscribers::init_for_testing();

    let (_sui_cluster_handle, mut cluster, mut client) = test_cluster::default_setup().await?;

    // Store a blob and get confirmations from each node.
    let blob = walrus_test_utils::random_data(31415);

    // Find the shards of the failed nodes.
    let failed_node_names: Vec<String> =
        failed_nodes.iter().map(|i| format!("node-{}", i)).collect();
    let committees = client
        .as_ref()
        .get_committees()
        .await
        .expect("committees should be available");
    let shards_of_failed_nodes = client
        .as_ref()
        .shards_of(&failed_node_names, &committees)
        .await;

    // Encode the blob with false metadata for one shard.
    let (pairs, metadata) = client
        .as_ref()
        .encoding_config()
        .get_for_type(DEFAULT_ENCODING)
        .encode_with_metadata(&blob)?;
    let mut metadata = metadata.metadata().to_owned();
    let mut i = 0;
    // Change a shard that is not in the failure set. Since the mapping of slivers to shards
    // depends on the blob id, we need to search for an invalid hash for which the modified shard
    // is not in the failure set.
    loop {
        metadata.mut_inner().hashes[1].primary_hash = Node::Digest([i; 32]);
        let blob_id = BlobId::from_sliver_pair_metadata(&metadata);
        if !shards_of_failed_nodes.contains(
            &SliverPairIndex::new(1).to_shard_index(NonZeroU16::new(13).unwrap(), &blob_id),
        ) {
            break;
        }
        i += 1;
    }
    let blob_id = BlobId::from_sliver_pair_metadata(&metadata);
    let metadata = VerifiedBlobMetadataWithId::new_verified_unchecked(blob_id, metadata);

    tracing::debug!(
        "shard index for inconsistent sliver: {}",
        SliverPairIndex::new(1).to_shard_index(NonZeroU16::new(13).unwrap(), &blob_id)
    );
    // Register blob.
    let (blob_sui_object, _) = client
        .as_ref()
        .resource_manager(&committees)
        .await
        .get_existing_or_register(
            &[&metadata],
            1,
            BlobPersistence::Permanent,
            StoreWhen::NotStored,
        )
        .await?
        .into_iter()
        .next()
        .expect("should register exactly one blob");

    // Certify blob.
    let certificate = client
        .as_ref()
        .send_blob_data_and_get_certificate(
            &metadata,
            &pairs,
            &BlobPersistenceType::Permanent,
            &MultiProgress::new(),
        )
        .await?;

    // Stop the nodes in the failure set.
    failed_nodes
        .iter()
        .for_each(|&idx| cluster.cancel_node(idx));

    client
        .as_mut()
        .sui_client()
        .certify_blobs(&[(&blob_sui_object, certificate)], PostStoreAction::Keep)
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
            tracing::debug!("event: {event:?}");
            if let ContractEvent::BlobEvent(BlobEvent::InvalidBlobID(_)) = event {
                return;
            }
        }
        panic!("should be infinite stream")
    })
    .await?;

    // Cancel the nodes and wait to prevent event handles being dropped.
    cluster.nodes.iter().for_each(|node| node.cancel());
    tokio::time::sleep(Duration::from_secs(1)).await;

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
        reuse_and_extend: (1, 2, true),
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
    telemetry_subscribers::init_for_testing();

    let (_sui_cluster_handle, _cluster, client) = test_cluster::default_setup().await?;

    let blob_data = walrus_test_utils::random_data_list(31415, 4);
    let blobs: Vec<&[u8]> = blob_data.iter().map(AsRef::as_ref).collect();
    let encoding_type = DEFAULT_ENCODING;
    let metatdatum = blobs
        .iter()
        .map(|blob| {
            let (_, metadata) = client
                .as_ref()
                .encoding_config()
                .get_for_type(encoding_type)
                .encode_with_metadata(blob)
                .expect("blob encoding should not fail");
            let metadata = metadata.metadata().to_owned();
            let blob_id = BlobId::from_sliver_pair_metadata(&metadata);
            VerifiedBlobMetadataWithId::new_verified_unchecked(blob_id, metadata)
        })
        .collect::<Vec<_>>();

    // Register a list of new blobs.
    let committees = client
        .as_ref()
        .get_committees()
        .await
        .expect("committees should be available");

    let original_blob_objects = client
        .as_ref()
        .resource_manager(&committees)
        .await
        .get_existing_or_register(
            &metatdatum.iter().collect::<Vec<_>>(),
            epochs_ahead_registered,
            BlobPersistence::Permanent,
            StoreWhen::NotStored,
        )
        .await?
        .into_iter()
        .map(|(blob, _)| (blob.blob_id, blob))
        .collect::<HashMap<_, _>>();

    // Now ask the client to store again.
    let blob_stores = client
        .inner
        .reserve_and_store_blobs(
            &blobs,
            encoding_type,
            epochs_ahead_required,
            StoreWhen::NotStored,
            BlobPersistence::Permanent,
            PostStoreAction::Keep,
        )
        .await?
        .into_iter()
        .map(|blob_store_result| match blob_store_result {
            BlobStoreResult::NewlyCreated { blob_object, .. } => (blob_object.blob_id, blob_object),
            _ => panic!("the client should be able to store the blob"),
        })
        .collect::<HashMap<_, _>>();

    for (blob_id, blob_object) in blob_stores {
        let original_blob_object = original_blob_objects
            .get(&blob_id)
            .expect("should have original blob object");
        assert!(should_match == (blob_object.id == original_blob_object.id));
    }
    Ok(())
}

/// Registers a blob and returns the blob ID.
async fn register_blob(
    client: &WithTempDir<Client<SuiContractClient>>,
    blob: &[u8],
    encoding_type: EncodingType,
    epochs_ahead: EpochCount,
) -> TestResult<BlobId> {
    // Encode blob and get metadata
    let (_, metadata) = client
        .as_ref()
        .encoding_config()
        .get_for_type(encoding_type)
        .encode_with_metadata(blob)
        .expect("blob encoding should not fail");
    let metadata = metadata.metadata().to_owned();
    let blob_id = BlobId::from_sliver_pair_metadata(&metadata);
    let metadata = VerifiedBlobMetadataWithId::new_verified_unchecked(blob_id, metadata);

    let committees = client
        .as_ref()
        .get_committees()
        .await
        .expect("committees should be available");
    // Register the blob
    let blob_id = client
        .as_ref()
        .resource_manager(&committees)
        .await
        .get_existing_or_register(
            &[&metadata],
            epochs_ahead,
            BlobPersistence::Permanent,
            StoreWhen::NotStored,
        )
        .await?
        .into_iter()
        .map(|(blob, _)| blob.blob_id)
        .next()
        .expect("should have registered blob");

    Ok(blob_id)
}

/// Store a blob and return the blob id.
async fn store_blob(
    client: &WithTempDir<Client<SuiContractClient>>,
    blob: &[u8],
    encoding_type: EncodingType,
    epochs_ahead: EpochCount,
) -> TestResult<BlobId> {
    let result = client
        .inner
        .reserve_and_store_blobs(
            &[blob],
            encoding_type,
            epochs_ahead,
            StoreWhen::NotStored,
            BlobPersistence::Permanent,
            PostStoreAction::Keep,
        )
        .await?;

    Ok(result
        .into_iter()
        .next()
        .expect("should have one blob store result")
        .blob_id()
        .expect("blob id should be present"))
}

/// Tests that the client can store and read duplicate blobs.
#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
pub async fn test_store_and_read_duplicate_blobs() -> TestResult {
    telemetry_subscribers::init_for_testing();

    let (_sui_cluster_handle, _cluster, client) = test_cluster::default_setup().await?;
    let client = client.as_ref();

    // Generate random blobs.
    let mut blob_data = walrus_test_utils::random_data_list(31415, 3);
    blob_data.push(blob_data[0].clone());
    blob_data.push(blob_data[1].clone());
    blob_data.push(blob_data[0].clone());
    let mut blobs_with_paths: Vec<(PathBuf, Vec<u8>)> = vec![];

    // Create paths for each blob.
    for (i, data) in blob_data.iter().enumerate() {
        let path = PathBuf::from(format!("blob_{}", i));
        blobs_with_paths.push((path, data.to_vec()));
    }

    let store_result_with_path = client
        .reserve_and_store_blobs_retry_committees_with_path(
            &blobs_with_paths,
            DEFAULT_ENCODING,
            1,
            StoreWhen::Always,
            BlobPersistence::Permanent,
            PostStoreAction::Keep,
        )
        .await?;

    let read_result =
        futures::future::join_all(store_result_with_path.iter().map(|result| async {
            let blob = client
                .read_blob::<Primary>(
                    &result
                        .blob_store_result
                        .blob_id()
                        .expect("blob id should be present"),
                )
                .await
                .expect("should be able to read blob");
            (result.blob_store_result.blob_id(), blob)
        }))
        .await;

    assert_eq!(store_result_with_path.len(), blob_data.len());
    store_result_with_path
        .iter()
        .zip(blobs_with_paths.iter())
        .zip(read_result.iter())
        .for_each(|((result, (path, data)), (blob_id, blob))| {
            assert_eq!(&result.path, path);
            assert_eq!(blob, data);
            assert_eq!(blob_id, &result.blob_store_result.blob_id());
        });

    Ok(())
}

/// Tests that blobs can be extended when possible.
#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_store_with_existing_blobs() -> TestResult {
    telemetry_subscribers::init_for_testing();

    let (_sui_cluster_handle, _cluster, client) = test_cluster::default_setup().await?;

    let blob_data = walrus_test_utils::random_data_list(31415, 5);
    let blobs: Vec<&[u8]> = blob_data.iter().map(AsRef::as_ref).collect();

    // Initial setup, with blobs in different states, the names indicate the later outcome
    // of a following store operation.
    let encoding_type = DEFAULT_ENCODING;
    let reuse_blob = register_blob(&client, blobs[0], encoding_type, 40).await?;
    let certify_and_extend_blob = register_blob(&client, blobs[1], encoding_type, 10).await?;
    let already_certified_blob = store_blob(&client, blobs[2], encoding_type, 50).await?;
    let extended_blob = store_blob(&client, blobs[3], encoding_type, 20).await?;

    let epoch = client.as_ref().sui_client().current_epoch().await?;
    let epochs_ahead = 30;
    let store_results: Vec<BlobStoreResult> = client
        .inner
        .reserve_and_store_blobs(
            &blobs,
            encoding_type,
            epochs_ahead,
            StoreWhen::NotStored,
            BlobPersistence::Permanent,
            PostStoreAction::Keep,
        )
        .await?;
    for result in store_results {
        if result.blob_id() == Some(reuse_blob) {
            assert!(matches!(
                &result,
                BlobStoreResult::NewlyCreated{blob_object:_, resource_operation, ..
                } if resource_operation.is_reuse_registration()));
            assert!(
                result
                    .end_epoch()
                    .is_some_and(|end| end >= epoch + epochs_ahead),
                "end_epoch should exist and be at least epoch + epochs_ahead {}, {} {}",
                epoch,
                epochs_ahead,
                result.end_epoch().unwrap_or(0)
            );
        } else if result.blob_id() == Some(certify_and_extend_blob) {
            assert!(matches!(
                &result,
                BlobStoreResult::NewlyCreated {
                    resource_operation,
                    ..
                } if resource_operation.is_certify_and_extend()
            ));
            assert!(
                result
                    .end_epoch()
                    .is_some_and(|end| end == epoch + epochs_ahead),
                "end_epoch should exist and be equal to epoch + epochs_ahead"
            );
        } else if result.blob_id() == Some(already_certified_blob) {
            assert!(matches!(&result, BlobStoreResult::AlreadyCertified { .. }));
            assert!(
                result
                    .end_epoch()
                    .is_some_and(|end| end >= epoch + epochs_ahead),
                "end_epoch should exist and be at least epoch + epochs_ahead"
            );
        } else if result.blob_id() == Some(extended_blob) {
            assert!(matches!(
                &result,
                BlobStoreResult::NewlyCreated {
                    resource_operation,
                ..
            } if resource_operation.is_extend()
            ));
            assert!(
                result
                    .end_epoch()
                    .is_some_and(|end| end == epoch + epochs_ahead),
                "end_epoch should exist and be equal to epoch + epochs_ahead"
            );
        } else {
            assert!(matches!(
                &result,
                BlobStoreResult::NewlyCreated {
                    resource_operation,
                    ..
                } if resource_operation.is_registration()
            ));
            assert!(
                result
                    .end_epoch()
                    .is_some_and(|end| end == epoch + epochs_ahead),
                "end_epoch should exist and be equal to epoch + epochs_ahead"
            );
        }
    }

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
    telemetry_subscribers::init_for_testing();

    let (_sui_cluster_handle, _cluster, client) = test_cluster::default_setup().await?;

    let blob_data = walrus_test_utils::random_data_list(31415, 4);
    let unencoded_blobs = blob_data
        .iter()
        .enumerate()
        .map(|(i, data)| WalrusStoreBlob::new_unencoded(data, format!("test-{:02}", i)))
        .collect();
    let encoding_type = DEFAULT_ENCODING;
    let encoded_blobs = client
        .as_ref()
        .encode_blobs(unencoded_blobs, encoding_type)?;
    let encoded_sizes = encoded_blobs
        .iter()
        .map(|blob| blob.encoded_size().expect("encoded size should be present"))
        .collect::<Vec<_>>();

    // Reserve space for the blobs. Collect all original storage resource objects ids.
    let original_storage_resources =
        futures::future::join_all(encoded_sizes.iter().map(|encoded_size| async {
            let resource = client
                .as_ref()
                .sui_client()
                .reserve_space(*encoded_size, epochs_ahead_registered)
                .await
                .expect("reserve space should not fail");
            resource.id
        }))
        .await
        .into_iter()
        .collect::<HashSet<_>>();

    let blobs = encoded_blobs
        .iter()
        .map(|blob| blob.get_blob())
        .collect::<Vec<_>>();
    // Now ask the client to store again.
    // Collect all object ids of the newly created blob object.
    let blob_store = client
        .inner
        .reserve_and_store_blobs(
            &blobs,
            encoding_type,
            epochs_ahead_required,
            StoreWhen::NotStored,
            BlobPersistence::Permanent,
            PostStoreAction::Keep,
        )
        .await?
        .into_iter()
        .map(|blob_store_result| match blob_store_result {
            BlobStoreResult::NewlyCreated { blob_object, .. } => blob_object.storage.id,
            _ => panic!("the client should be able to store the blob"),
        })
        .collect::<HashSet<_>>();

    // Check the object ids are the same.
    assert!(should_match == (original_storage_resources == blob_store));
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
    telemetry_subscribers::init_for_testing();
    let (_sui_cluster_handle, _cluster, client) = test_cluster::default_setup().await?;
    let blob = walrus_test_utils::random_data(314);
    let blobs = vec![blob.as_slice()];
    // Store the blob multiple times, using separate end times to obtain multiple blob objects
    // with the same blob ID.
    let encoding_type = DEFAULT_ENCODING;
    for idx in 1..blobs_to_create + 1 {
        client
            .as_ref()
            .reserve_and_store_blobs(
                &blobs,
                encoding_type,
                idx,
                StoreWhen::Always,
                BlobPersistence::Deletable,
                PostStoreAction::Keep,
            )
            .await?;
    }

    // Add a blob that is not deletable.
    let result = client
        .as_ref()
        .reserve_and_store_blobs(
            &blobs,
            encoding_type,
            1,
            StoreWhen::Always,
            BlobPersistence::Permanent,
            PostStoreAction::Keep,
        )
        .await?;
    let blob_id = result.first().unwrap().blob_id();

    // Check that we have the correct number of blobs
    let blobs = client
        .as_ref()
        .sui_client()
        .owned_blobs(None, ExpirySelectionPolicy::Valid)
        .await?;
    assert_eq!(blobs.len(), blobs_to_create as usize + 1);

    // Delete the blobs
    let deleted = client
        .as_ref()
        .delete_owned_blob(&blob_id.expect("blob id should be present"))
        .await?;
    assert_eq!(deleted, blobs_to_create as usize);

    // Only one blob should remain: The non-deletable one.
    let blobs = client
        .as_ref()
        .sui_client()
        .owned_blobs(None, ExpirySelectionPolicy::Valid)
        .await?;
    assert_eq!(blobs.len(), 1);

    Ok(())
}

#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_storage_nodes_delete_data_for_deleted_blobs() -> TestResult {
    telemetry_subscribers::init_for_testing();
    let (_sui_cluster_handle, _cluster, client) = test_cluster::default_setup().await?;
    let client = client.as_ref();
    let blob = walrus_test_utils::random_data(314);
    let blobs = vec![blob.as_slice()];

    let results = client
        .reserve_and_store_blobs(
            &blobs,
            DEFAULT_ENCODING,
            1,
            StoreWhen::Always,
            BlobPersistence::Deletable,
            PostStoreAction::Keep,
        )
        .await?;
    let store_result = results.first().expect("should have one blob store result");
    let blob_id = store_result.blob_id();
    assert!(matches!(store_result, BlobStoreResult::NewlyCreated { .. }));

    assert_eq!(
        client
            .read_blob::<Primary>(&blob_id.expect("blob id should be present"))
            .await?,
        blob
    );

    client
        .delete_owned_blob(&blob_id.expect("blob id should be present"))
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let status_result = client
        .get_verified_blob_status(
            &blob_id.expect("blob id should be present"),
            client.sui_client().read_client(),
            Duration::from_secs(1),
        )
        .await?;
    assert!(matches!(status_result, BlobStatus::Nonexistent));

    let read_result = client
        .read_blob::<Primary>(&blob_id.expect("blob id should be present"))
        .await;
    assert!(matches!(
        read_result.unwrap_err().kind(),
        ClientErrorKind::BlobIdDoesNotExist,
    ));

    Ok(())
}

#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_blocklist() -> TestResult {
    telemetry_subscribers::init_for_testing();
    let blocklist_dir = tempfile::tempdir().expect("temporary directory creation must succeed");
    let (_sui_cluster_handle, _cluster, client) =
        test_cluster::default_setup_with_num_checkpoints_generic::<StorageNodeHandle>(
            Duration::from_secs(60 * 60),
            TestNodesConfig {
                node_weights: vec![1, 2, 3, 3, 4],
                use_legacy_event_processor: true,
                disable_event_blob_writer: false,
                blocklist_dir: Some(blocklist_dir.path().to_path_buf()),
                enable_node_config_synchronizer: false,
            },
            None,
            ClientCommunicationConfig::default_for_test(),
            false,
            None,
        )
        .await?;
    let client = client.as_ref();
    let blob = walrus_test_utils::random_data(314);

    let store_results = client
        .reserve_and_store_blobs(
            &[&blob],
            DEFAULT_ENCODING,
            1,
            StoreWhen::Always,
            BlobPersistence::Deletable,
            PostStoreAction::Keep,
        )
        .await?;
    let store_result = store_results[0].clone();
    let blob_id = store_result.blob_id();
    assert!(matches!(store_result, BlobStoreResult::NewlyCreated { .. }));

    assert_eq!(
        client
            .read_blob::<Primary>(&blob_id.expect("blob id should be present"))
            .await?,
        blob
    );

    let mut blocklists = vec![];

    for (i, _) in _cluster.nodes.iter().enumerate() {
        let blocklist_file = blocklist_dir.path().join(format!("blocklist-{i}.yaml"));
        let blocklist =
            Blocklist::new(&Some(blocklist_file)).expect("blocklist creation must succeed");
        blocklists.push(blocklist);
    }

    tracing::info!("Adding blob to blocklist");

    for blocklist in blocklists.iter_mut() {
        blocklist.insert(blob_id.expect("blob id should be present"))?;
    }

    // Read the blob using the client until it fails with forbidden
    let mut blob_read_result = client
        .read_blob::<Primary>(&blob_id.expect("blob id should be present"))
        .await;
    while let Ok(_blob) = blob_read_result {
        blob_read_result = client
            .read_blob::<Primary>(&blob_id.expect("blob id should be present"))
            .await;
        // sleep for a bit to allow the nodes to sync
        tokio::time::sleep(Duration::from_secs(30)).await;
    }

    let error = blob_read_result.expect_err("result must be an error");

    assert!(
        matches!(error.kind(), ClientErrorKind::BlobIdBlocked(_)),
        "unexpected error {:?}",
        error
    );

    // Remove the blob from the blocklist
    for blocklist in blocklists.iter_mut() {
        blocklist.remove(&blob_id.expect("blob id should be present"))?;
    }

    tracing::info!("Removing blob from blocklist");

    // Read the blob again until it succeeds
    let mut blob_read_result = client
        .read_blob::<Primary>(&blob_id.expect("blob id should be present"))
        .await;
    while blob_read_result.is_err() {
        blob_read_result = client
            .read_blob::<Primary>(&blob_id.expect("blob id should be present"))
            .await;
        // sleep for a bit to allow the nodes to sync
        tokio::time::sleep(Duration::from_secs(30)).await;
    }

    assert_eq!(blob_read_result?, blob);

    Ok(())
}

#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_blob_operations_with_subsidies() -> TestResult {
    telemetry_subscribers::init_for_testing();
    let (_sui_cluster_handle, _cluster, client) =
        test_cluster::default_setup_with_subsidies().await?;
    let client = client.as_ref();

    // Store a blob with subsidies
    let blob_data = walrus_test_utils::random_data(314);
    let blobs = vec![blob_data.as_slice()];
    let store_result = client
        .reserve_and_store_blobs(
            &blobs,
            DEFAULT_ENCODING,
            1,
            StoreWhen::Always,
            BlobPersistence::Permanent,
            PostStoreAction::Keep,
        )
        .await?;

    let blob_object = match &store_result[0] {
        BlobStoreResult::NewlyCreated { blob_object, .. } => blob_object.clone(),
        _ => panic!("Expected newly created blob"),
    };

    let initial_storage = blob_object.storage.clone();

    // Extend blob storage with subsidies
    client.sui_client().extend_blob(blob_object.id, 5).await?;

    // Verify blob storage was extended with subsidies
    let extended_blob: Blob = client
        .sui_client()
        .sui_client()
        .get_sui_object(blob_object.id)
        .await?;

    // Verify the blob was extended
    assert!(extended_blob.storage.end_epoch > initial_storage.end_epoch);

    // Verify subsidies were applied by checking remaining funds
    let subsidies: Subsidies = client
        .sui_client()
        .read_client()
        .sui_client()
        .get_sui_object(
            client
                .sui_client()
                .read_client()
                .get_subsidies_object_id()
                .unwrap(),
        )
        .await?;
    assert!(
        subsidies.subsidy_pool < DEFAULT_SUBSIDY_FUNDS,
        "Subsidies should have been used"
    );

    Ok(())
}

/// Tests that storing the same blob multiple times with possibly different end epochs,
/// persistence, and force-store conditions always works.
#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_multiple_stores_same_blob() -> TestResult {
    telemetry_subscribers::init_for_testing();
    let (_sui_cluster_handle, _cluster, client) = test_cluster::default_setup().await?;
    let client = client.as_ref();
    let blob = walrus_test_utils::random_data(314);
    let blobs = vec![blob.as_slice()];
    let encoding_type = DEFAULT_ENCODING;

    // NOTE: not in a param_test, because we want to test these store operations in sequence.
    // If the last `bool` parameter is `true`, the store operation should return a
    // `BlobStoreResult::AlreadyCertified`. Otherwise, it should return a
    // `BlobStoreResult::NewlyCreated`.
    let configurations = vec![
        (1, StoreWhen::NotStored, BlobPersistence::Deletable, false),
        (1, StoreWhen::Always, BlobPersistence::Deletable, false),
        (2, StoreWhen::NotStored, BlobPersistence::Deletable, true), // Extend lifetime
        (3, StoreWhen::Always, BlobPersistence::Deletable, false),
        (1, StoreWhen::NotStored, BlobPersistence::Permanent, false),
        (1, StoreWhen::NotStored, BlobPersistence::Permanent, true),
        (1, StoreWhen::Always, BlobPersistence::Permanent, false),
        (4, StoreWhen::NotStored, BlobPersistence::Permanent, true), // Extend lifetime
        (2, StoreWhen::NotStored, BlobPersistence::Permanent, true),
        (2, StoreWhen::Always, BlobPersistence::Permanent, false),
        (1, StoreWhen::NotStored, BlobPersistence::Deletable, true),
        (5, StoreWhen::NotStored, BlobPersistence::Deletable, true), // Extend lifetime
    ];

    for (epochs, store_when, persistence, is_already_certified) in configurations {
        tracing::debug!(
            "testing: epochs={:?}, store_when={:?}, persistence={:?}, is_already_certified={:?}",
            epochs,
            store_when,
            persistence,
            is_already_certified
        );
        let results = client
            .reserve_and_store_blobs(
                &blobs,
                encoding_type,
                epochs,
                store_when,
                persistence,
                PostStoreAction::Keep,
            )
            .await?;
        let store_result = results.first().expect("should have one blob store result");

        match store_result {
            BlobStoreResult::NewlyCreated {
                blob_object: _,
                resource_operation,
                ..
            } => {
                assert_eq!(
                    resource_operation.is_extend(),
                    is_already_certified,
                    "the blob should be newly stored"
                );
            }
            BlobStoreResult::AlreadyCertified { .. } => {
                assert!(is_already_certified, "the blob should be already stored");
            }
            other => panic!(
                "we either store the blob, or find it's already created:\n{:?}",
                other
            ),
        }
    }

    // At the end of all the operations above, count the number of blob objects owned by the
    // client.
    let blobs = client
        .sui_client()
        .owned_blobs(None, ExpirySelectionPolicy::Valid)
        .await?;
    assert_eq!(blobs.len(), 6);

    Ok(())
}

// Tests moving a shard to a storage node and then move it back.
#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_repeated_shard_move() -> TestResult {
    telemetry_subscribers::init_for_testing();
    let (_sui_cluster_handle, walrus_cluster, client) =
        test_cluster::default_setup_with_num_checkpoints_generic::<StorageNodeHandle>(
            Duration::from_secs(20),
            TestNodesConfig {
                node_weights: vec![1, 1],
                use_legacy_event_processor: true,
                disable_event_blob_writer: false,
                blocklist_dir: None,
                enable_node_config_synchronizer: false,
            },
            None,
            ClientCommunicationConfig::default_for_test(),
            false,
            None,
        )
        .await?;

    client
        .as_ref()
        .stake_with_node_pool(
            walrus_cluster.nodes[1]
                .storage_node_capability
                .as_ref()
                .unwrap()
                .node_id,
            1_000 * FROST_PER_NODE_WEIGHT,
        )
        .await?;

    walrus_cluster.wait_for_nodes_to_reach_epoch(4).await;
    assert_eq!(
        walrus_cluster.nodes[0]
            .storage_node()
            .existing_shards()
            .await
            .len(),
        0
    );
    assert_eq!(
        walrus_cluster.nodes[1]
            .storage_node()
            .existing_shards()
            .await
            .len(),
        2
    );

    client
        .as_ref()
        .stake_with_node_pool(
            walrus_cluster.nodes[0]
                .storage_node_capability
                .as_ref()
                .unwrap()
                .node_id,
            500_000 * FROST_PER_NODE_WEIGHT,
        )
        .await?;

    walrus_cluster.wait_for_nodes_to_reach_epoch(7).await;
    assert_eq!(
        walrus_cluster.nodes[0]
            .storage_node()
            .existing_shards()
            .await
            .len(),
        2
    );
    assert_eq!(
        walrus_cluster.nodes[1]
            .storage_node()
            .existing_shards()
            .await
            .len(),
        0
    );

    Ok(())
}

#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_burn_blobs() -> TestResult {
    const N_BLOBS: usize = 3;
    const N_TO_DELETE: usize = 2;
    telemetry_subscribers::init_for_testing();

    let (_sui_cluster_handle, _cluster, client) = test_cluster::default_setup().await?;

    let mut blob_object_ids = vec![];
    for idx in 0..N_BLOBS {
        let blob = walrus_test_utils::random_data(314 + idx);
        let result = client
            .as_ref()
            .reserve_and_store_blobs(
                &[blob.as_slice()],
                DEFAULT_ENCODING,
                1,
                StoreWhen::Always,
                BlobPersistence::Permanent,
                PostStoreAction::Keep,
            )
            .await?;
        blob_object_ids.push({
            let BlobStoreResult::NewlyCreated { blob_object, .. } = result
                .into_iter()
                .next()
                .expect("expect one blob store result")
            else {
                panic!("expect newly stored blob")
            };
            blob_object.id
        });
    }

    let blobs = client
        .as_ref()
        .sui_client()
        .owned_blobs(None, ExpirySelectionPolicy::Valid)
        .await?;
    assert_eq!(blobs.len(), N_BLOBS);

    client
        .as_ref()
        .sui_client()
        .burn_blobs(&blob_object_ids[..N_TO_DELETE])
        .await?;

    let blobs = client
        .as_ref()
        .sui_client()
        .owned_blobs(None, ExpirySelectionPolicy::Valid)
        .await?;
    assert_eq!(blobs.len(), N_BLOBS - N_TO_DELETE);

    Ok(())
}

#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_extend_owned_blobs() -> TestResult {
    let _ = tracing_subscriber::fmt::try_init();
    let (_sui_cluster_handle, _cluster, client) = test_cluster::default_setup().await?;

    let current_epoch = client.as_ref().sui_client().current_epoch().await?;
    let blob = walrus_test_utils::random_data(314);
    let encoding_type = DEFAULT_ENCODING;
    let result = client
        .as_ref()
        .reserve_and_store_blobs(
            &[blob.as_slice()],
            encoding_type,
            1,
            StoreWhen::Always,
            BlobPersistence::Permanent,
            PostStoreAction::Keep,
        )
        .await?;
    let BlobStoreResult::NewlyCreated { blob_object, .. } = result[0].clone() else {
        panic!("expect newly stored blob")
    };
    let (end_epoch, blob_object_id) = (blob_object.storage.end_epoch, blob_object.id);
    assert_eq!(end_epoch, current_epoch + 1);

    // Extend it by 5 epochs.
    client
        .as_ref()
        .sui_client()
        .extend_blob(blob_object_id, 5)
        .await?;

    let extended_blob_object: Blob = client
        .as_ref()
        .sui_client()
        .sui_client()
        .get_sui_object(blob_object_id)
        .await?;
    assert_eq!(extended_blob_object.storage.end_epoch, end_epoch + 5);

    // Store it again with a longer lifetime, should extend it correctly.
    let result = client
        .as_ref()
        .reserve_and_store_blobs(
            &[blob.as_slice()],
            encoding_type,
            20,
            StoreWhen::NotStored,
            BlobPersistence::Permanent,
            PostStoreAction::Keep,
        )
        .await?;
    let BlobStoreResult::NewlyCreated {
        blob_object: second_store_blob_object,
        resource_operation,
        ..
    } = result[0].clone()
    else {
        panic!("unexpected result")
    };
    assert_eq!(second_store_blob_object.id, blob_object_id);
    assert_eq!(
        second_store_blob_object.storage.end_epoch,
        current_epoch + 20
    );
    assert!(resource_operation.is_extend());

    Ok(())
}

#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_share_blobs() -> TestResult {
    telemetry_subscribers::init_for_testing();

    let (_sui_cluster_handle, _cluster, client) = test_cluster::default_setup().await?;

    let blob = walrus_test_utils::random_data(314);
    let result = client
        .as_ref()
        .reserve_and_store_blobs(
            &[blob.as_slice()],
            DEFAULT_ENCODING,
            1,
            StoreWhen::Always,
            BlobPersistence::Permanent,
            PostStoreAction::Keep,
        )
        .await?;
    let (end_epoch, blob_object_id) = {
        let BlobStoreResult::NewlyCreated { blob_object, .. } = result
            .into_iter()
            .next()
            .expect("expect one blob store result")
        else {
            panic!("expect newly stored blob")
        };
        (blob_object.storage.end_epoch, blob_object.id)
    };

    // Share the blob without funding.
    let shared_blob_object_id = client
        .as_ref()
        .sui_client()
        .share_and_maybe_fund_blob(blob_object_id, None)
        .await?;
    let shared_blob: SharedBlob = client
        .as_ref()
        .sui_client()
        .sui_client()
        .get_sui_object(shared_blob_object_id)
        .await?;
    assert_eq!(shared_blob.funds, 0);

    // Fund the shared blob.
    client
        .as_ref()
        .sui_client()
        .fund_shared_blob(shared_blob_object_id, 1000000000)
        .await?;
    let shared_blob: SharedBlob = client
        .as_ref()
        .sui_client()
        .sui_client()
        .get_sui_object(shared_blob_object_id)
        .await?;
    assert_eq!(shared_blob.funds, 1000000000);

    // Extend the shared blob.
    client
        .as_ref()
        .sui_client()
        .extend_shared_blob(shared_blob_object_id, 100)
        .await?;
    let shared_blob: SharedBlob = client
        .as_ref()
        .sui_client()
        .sui_client()
        .get_sui_object(shared_blob_object_id)
        .await?;
    assert_eq!(shared_blob.blob.storage.end_epoch, end_epoch + 100);
    assert_eq!(shared_blob.funds, 999999500);

    // Read the blob object with attributes from the shared blob object id
    let _blob_with_attribute = client
        .as_ref()
        .get_blob_by_object_id(&shared_blob_object_id)
        .await?;
    Ok(())
}

const TARGET_ADDRESS: [u8; SUI_ADDRESS_LENGTH] = [42; SUI_ADDRESS_LENGTH];
async_param_test! {
    #[ignore = "ignore E2E tests by default"]
    #[walrus_simtest]
    test_post_store_action -> TestResult : [
        keep: (PostStoreAction::Keep, 4, 0),
        transfer: (
            PostStoreAction::TransferTo(
                SuiAddress::from_bytes(TARGET_ADDRESS).expect("valid address"),
            ),
            0,
            4
        ),
        burn: (PostStoreAction::Burn, 0, 0),
        share: (PostStoreAction::Share, 0, 0)
    ]
}
async fn test_post_store_action(
    post_store: PostStoreAction,
    n_owned_blobs: usize,
    n_target_blobs: usize,
) -> TestResult {
    telemetry_subscribers::init_for_testing();
    let (_sui_cluster_handle, _cluster, client) = test_cluster::default_setup().await?;
    let target_address: SuiAddress = SuiAddress::from_bytes(TARGET_ADDRESS).expect("valid address");

    let blob_data = walrus_test_utils::random_data_list(314, 4);
    let blobs: Vec<&[u8]> = blob_data.iter().map(AsRef::as_ref).collect();
    let results = client
        .as_ref()
        .reserve_and_store_blobs_retry_committees(
            &blobs,
            DEFAULT_ENCODING,
            1,
            StoreWhen::Always,
            BlobPersistence::Permanent,
            post_store,
            None,
        )
        .await?;

    let owned_blobs = client
        .as_ref()
        .sui_client()
        .owned_blobs(None, ExpirySelectionPolicy::Valid)
        .await?;
    assert_eq!(owned_blobs.len(), n_owned_blobs);
    let target_address_blobs = client
        .as_ref()
        .sui_client()
        .owned_blobs(Some(target_address), ExpirySelectionPolicy::Valid)
        .await?;
    assert_eq!(target_address_blobs.len(), n_target_blobs);
    for result in &results {
        println!("test_post_store_action result: {:?}", result);
    }

    if post_store == PostStoreAction::Share {
        for result in results {
            match result {
                BlobStoreResult::NewlyCreated {
                    shared_blob_object,
                    blob_object,
                    ..
                } => {
                    let shared_blob: SharedBlob = client
                        .as_ref()
                        .sui_client()
                        .sui_client()
                        .get_sui_object(shared_blob_object.unwrap())
                        .await?;
                    assert_eq!(shared_blob.funds, 0);
                    assert_eq!(shared_blob.blob.id, blob_object.id);
                }
                _ => panic!("expect newly created blob"),
            }
        }
    } else {
        for result in results {
            match result {
                BlobStoreResult::NewlyCreated {
                    shared_blob_object, ..
                } => {
                    assert!(shared_blob_object.is_none());
                }
                _ => panic!("expect newly created blob"),
            }
        }
    }
    Ok(())
}

// Tests upgrading the walrus contracts.
#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_quorum_contract_upgrade() -> TestResult {
    telemetry_subscribers::init_for_testing();
    let contract_dir = TempDir::new()?;
    let (_sui_cluster_handle, walrus_cluster, client, system_ctx) =
        test_cluster::default_setup_with_deploy_directory_generic::<StorageNodeHandle>(
            Duration::from_secs(60 * 60), // The voting & upgrade has to complete within one epoch
            TestNodesConfig {
                node_weights: vec![1, 2, 3, 3, 4],
                use_legacy_event_processor: true,
                disable_event_blob_writer: false,
                blocklist_dir: None,
                enable_node_config_synchronizer: false,
            },
            None,
            ClientCommunicationConfig::default_for_test(),
            false,
            Some(contract_dir.path().to_path_buf()),
            true,
            None,
        )
        .await?;

    // TODO(WAL-654): once mainnet upgrades follow testnet, upgrade from testnet-contracts instead
    // Change the version in the contracts
    let walrus_package_path = contract_dir.path().join("walrus");
    let staking_path = walrus_package_path.join("sources/staking.move");
    let system_path = walrus_package_path.join("sources/system.move");
    replace_version(&staking_path)?;
    replace_version(&system_path)?;

    // Vote for the upgrade
    // We can vote on behalf of all nodes from the client wallet since the client
    // wallet address was authorized for all nodes in the setup.
    for node in walrus_cluster.nodes.iter() {
        let node_id = node
            .storage_node_capability
            .as_ref()
            .expect("capability should be set")
            .node_id;
        client
            .as_ref()
            .sui_client()
            .vote_for_upgrade(
                system_ctx.upgrade_manager_object,
                node_id,
                walrus_package_path.clone(),
            )
            .await?;
    }

    // Commit the upgrade
    let new_package_id = client
        .as_ref()
        .sui_client()
        .upgrade(
            system_ctx.upgrade_manager_object,
            walrus_package_path,
            UpgradeType::Quorum,
        )
        .await?;

    tracing::info!("after upgrade");

    // Migrate the objects
    client
        .as_ref()
        .sui_client()
        .migrate_contracts(new_package_id)
        .await?;

    // Check the version
    assert_eq!(
        client
            .as_ref()
            .sui_client()
            .read_client()
            .system_object_version()
            .await?,
        2
    );

    // Store a blob after the upgrade to check if everything works after the upgrade.
    let blob_data = walrus_test_utils::random_data_list(314, 1);
    let blobs: Vec<&[u8]> = blob_data.iter().map(AsRef::as_ref).collect();
    let _results = client
        .as_ref()
        .reserve_and_store_blobs_retry_committees(
            &blobs,
            DEFAULT_ENCODING,
            1,
            StoreWhen::Always,
            BlobPersistence::Permanent,
            PostStoreAction::Keep,
            None,
        )
        .await?;

    Ok(())
}

fn replace_version(contract_file: &Path) -> anyhow::Result<()> {
    let contents = std::fs::read_to_string(contract_file)?;
    let contents = contents.replace("const VERSION: u64 = 1;", "const VERSION: u64 = 2;");
    std::fs::write(contract_file, contents)?;
    Ok(())
}

/// A toolkit for blob attribute tests.
struct BlobAttributeTestContext<'a> {
    pub client: &'a mut WithTempDir<Client<SuiContractClient>>,
    pub blob: Blob,
    pub key_value_pairs: HashMap<String, String>,
    pub expected_pairs: Option<HashMap<String, String>>,
}

impl<'a> BlobAttributeTestContext<'a> {
    /// Verify the blob attribute are the same the expected pairs.
    async fn verify_blob_attribute(&self) -> TestResult {
        let client = self.client.as_ref().sui_client();
        let blob = self.blob.clone();

        let res = client.get_blob_by_object_id(&blob.id).await?;
        if res.attribute.is_none() {
            assert!(self.expected_pairs.is_none());
        }
        if let Some(expected_pairs) = &self.expected_pairs {
            assert_eq!(
                res.attribute
                    .as_ref()
                    .expect("attribute should exist")
                    .len(),
                expected_pairs.len()
            );
            for (key, value) in expected_pairs.iter() {
                assert_eq!(
                    res.attribute
                        .as_ref()
                        .expect("attribute should exist")
                        .get(key)
                        .expect("key should exist"),
                    value
                );
            }
        }
        Ok(())
    }

    /// Add the blob attribute.
    pub async fn add_attribute_and_verify(
        &mut self,
        attribute: BlobAttribute,
        force: bool,
    ) -> TestResult {
        let client = self.client.as_mut().sui_client_mut();
        client
            .add_blob_attribute(self.blob.id, attribute.clone(), force)
            .await?;
        if self.expected_pairs.is_none() {
            self.expected_pairs = Some(HashMap::new());
        }
        let expected_pairs = self
            .expected_pairs
            .as_mut()
            .expect("expected_pairs should be Some at this point");
        for (key, value) in attribute.iter() {
            expected_pairs.insert(key.clone(), value.clone());
        }
        self.verify_blob_attribute().await?;
        Ok(())
    }

    /// Remove the blob attribute and verify the result.
    pub async fn remove_attribute_and_verify(&mut self) -> TestResult {
        let client = self.client.as_mut().sui_client_mut();
        client.remove_blob_attribute(self.blob.id).await?;
        self.expected_pairs = None;
        self.verify_blob_attribute().await?;
        Ok(())
    }

    /// Insert or update the blob attribute and verify the result.
    ///
    /// When force is true, a new attribute dynamic field will be created if it
    /// does not exist.
    pub async fn insert_or_update_attribute_pairs_and_verify(
        &mut self,
        kvs: &HashMap<String, String>,
        force: bool,
    ) -> TestResult {
        let client = self.client.as_mut().sui_client_mut();
        client
            .insert_or_update_blob_attribute_pairs(
                self.blob.id,
                kvs.iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect::<Vec<(String, String)>>(),
                force,
            )
            .await?;
        if let Some(expected_pairs) = &mut self.expected_pairs {
            expected_pairs.extend(kvs.iter().map(|(k, v)| (k.to_string(), v.to_string())));
        } else {
            panic!("expected_pairs should be Some at this point");
        }
        self.verify_blob_attribute().await?;
        Ok(())
    }

    /// Remove the blob attribute pairs and verify the result.
    pub async fn remove_blob_attribute_pairs_and_verify(&mut self, keys: &[&String]) -> TestResult {
        let client = self.client.as_mut().sui_client_mut();
        client
            .remove_blob_attribute_pairs(self.blob.id, keys)
            .await?;
        if let Some(expected_pairs) = &mut self.expected_pairs {
            for key in keys.iter() {
                expected_pairs.remove(*key);
            }
        } else {
            panic!("expected_pairs should be Some at this point");
        }
        self.verify_blob_attribute().await?;
        Ok(())
    }

    /// Create a new test context with multiple copies of the same blob.
    pub async fn new(client: &'a mut WithTempDir<Client<SuiContractClient>>) -> TestResult<Self> {
        let blobs_to_create = 2;
        let blob = walrus_test_utils::random_data(314);
        let blobs = vec![blob.as_slice()];

        // Store multiple copies of the same blob with different end times.
        for idx in 1..blobs_to_create + 1 {
            client
                .as_mut()
                .reserve_and_store_blobs(
                    &blobs,
                    DEFAULT_ENCODING,
                    idx,
                    StoreWhen::Always,
                    BlobPersistence::Permanent,
                    PostStoreAction::Keep,
                )
                .await
                .expect("reserve_and_store_blobs should succeed.");
        }

        // Verify initial blob count and no metadata.
        let blobs = client
            .as_mut()
            .sui_client()
            .owned_blobs(None, ExpirySelectionPolicy::Valid)
            .await
            .expect("owned_blobs should succeed.");
        assert_eq!(blobs.len(), blobs_to_create as usize);

        for blob in blobs.iter() {
            let res = client
                .as_ref()
                .sui_client()
                .get_blob_by_object_id(&blob.id)
                .await
                .expect("get_blob_by_object_id should succeed.");
            assert!(res.attribute.is_none());
        }

        Ok(Self {
            client,
            blob: blobs[0].clone(),
            key_value_pairs: HashMap::from([
                ("name".to_string(), "test_blob".to_string()),
                (
                    "description".to_string(),
                    "A test blob for metadata".to_string(),
                ),
                ("version".to_string(), "1.0.0".to_string()),
                ("author".to_string(), "walrus_test".to_string()),
                ("timestamp".to_string(), "2024-01-01".to_string()),
                ("addr".to_string(), "Mars".to_string()),
            ]),
            expected_pairs: Some(HashMap::new()),
        })
    }
}

#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_blob_attribute_add_and_remove() -> TestResult {
    telemetry_subscribers::init_for_testing();

    let (_sui_cluster_handle, _cluster, mut client) = test_cluster::default_setup().await?;
    let mut test_context = BlobAttributeTestContext::new(&mut client).await?;

    let mut attribute = BlobAttribute::default();
    for (key, value) in test_context.key_value_pairs.iter() {
        if random::<bool>() {
            attribute.insert(key.clone(), value.clone());
        }
    }
    test_context
        .add_attribute_and_verify(attribute.clone(), false)
        .await?;

    // Test duplicate metadata error (should fail when force=false).
    let duplicate_result = test_context
        .add_attribute_and_verify(attribute.clone(), false)
        .await;
    assert!(matches!(
        duplicate_result
            .unwrap_err()
            .downcast::<SuiClientError>()
            .unwrap()
            .as_ref(),
        SuiClientError::AttributeAlreadyExists
    ));

    // Test force adding duplicate metadata (should succeed).
    let mut updated_attribute = BlobAttribute::default();
    updated_attribute.insert("new_key".to_string(), "new_value".to_string());
    test_context
        .add_attribute_and_verify(updated_attribute, true)
        .await?;

    // Test removing metadata from the blob.
    test_context.remove_attribute_and_verify().await?;

    // Removing metadata from a blob that does not have any should fail.
    let result = test_context.remove_attribute_and_verify().await;
    assert!(matches!(
        result
            .unwrap_err()
            .downcast::<SuiClientError>()
            .unwrap()
            .as_ref(),
        SuiClientError::AttributeDoesNotExist
    ));

    Ok(())
}

#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_blob_attribute_fields_operations() -> TestResult {
    telemetry_subscribers::init_for_testing();

    let (_sui_cluster_handle, _cluster, mut client) = test_cluster::default_setup().await?;
    let mut test_context = BlobAttributeTestContext::new(&mut client).await?;

    // Test adding a pair without attribute should fail.
    let result = test_context
        .insert_or_update_attribute_pairs_and_verify(
            &HashMap::from([("key".to_string(), "value".to_string())]),
            false,
        )
        .await;
    assert!(matches!(
        result
            .unwrap_err()
            .downcast::<SuiClientError>()
            .unwrap()
            .as_ref(),
        SuiClientError::AttributeDoesNotExist
    ));

    // Test removing a pair without an existing attribute should fail.
    let result = test_context
        .remove_blob_attribute_pairs_and_verify(&[&"key".to_string()])
        .await;
    assert!(matches!(
        result
            .unwrap_err()
            .downcast::<SuiClientError>()
            .unwrap()
            .as_ref(),
        SuiClientError::AttributeDoesNotExist
    ));

    test_context
        .insert_or_update_attribute_pairs_and_verify(
            &HashMap::from([("key".to_string(), "value".to_string())]),
            true,
        )
        .await?;

    let kvs = test_context.key_value_pairs.clone();
    // Test adding individual pairs.
    test_context
        .insert_or_update_attribute_pairs_and_verify(&kvs, false)
        .await?;

    // Test removing random pairs.
    for key in kvs.keys() {
        if random::<bool>() {
            continue;
        }
        test_context
            .remove_blob_attribute_pairs_and_verify(&[key])
            .await?;
    }

    // Test updating existing pairs.
    let key = "test_key".to_string();
    let initial_value = "initial_value".to_string();
    let updated_value = "updated_value".to_string();

    test_context
        .insert_or_update_attribute_pairs_and_verify(
            &HashMap::from([(key.clone(), initial_value.clone())]),
            false,
        )
        .await?;
    test_context
        .insert_or_update_attribute_pairs_and_verify(
            &HashMap::from([(key.clone(), updated_value.clone())]),
            false,
        )
        .await?;

    // Test removing non-existent pairs.
    let non_existing_key = "non_existing_key".to_string();
    let result = test_context
        .remove_blob_attribute_pairs_and_verify(&[&non_existing_key])
        .await;
    assert!(matches!(
        result.unwrap_err().downcast::<SuiClientError>().unwrap().as_ref(),
        SuiClientError::TransactionExecutionError(
            MoveExecutionError::OtherMoveModule(RawMoveError {
                function,
                module,
                error_code,
                ..
            })
        ) if function.as_str() == "get_idx" && module.as_str() == "vec_map" && *error_code == 1
    ));

    Ok(())
}

#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_shard_move_out_and_back_in_immediately() -> TestResult {
    telemetry_subscribers::init_for_testing();
    let (_sui_cluster_handle, walrus_cluster, client) =
        test_cluster::default_setup_with_num_checkpoints_generic::<StorageNodeHandle>(
            Duration::from_secs(20),
            TestNodesConfig {
                node_weights: vec![1, 1],
                use_legacy_event_processor: true,
                disable_event_blob_writer: false,
                blocklist_dir: None,
                enable_node_config_synchronizer: false,
            },
            None,
            ClientCommunicationConfig::default_for_test(),
            false,
            None,
        )
        .await?;

    walrus_cluster.wait_for_nodes_to_reach_epoch(2).await;

    // In epoch 2, move all the shards to node 1.
    client
        .as_ref()
        .stake_with_node_pool(
            walrus_cluster.nodes[1]
                .storage_node_capability
                .as_ref()
                .unwrap()
                .node_id,
            FROST_PER_NODE_WEIGHT * 5,
        )
        .await?;

    walrus_cluster.wait_for_nodes_to_reach_epoch(3).await;

    // In epoch 3, move all the shards to node 0.
    client
        .as_ref()
        .stake_with_node_pool(
            walrus_cluster.nodes[0]
                .storage_node_capability
                .as_ref()
                .unwrap()
                .node_id,
            FROST_PER_NODE_WEIGHT * 30,
        )
        .await?;

    walrus_cluster.wait_for_nodes_to_reach_epoch(4).await;
    // Wait for a little bit to make sure the shard sync task starts and shard status is updated.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // In epoch 4, all the shards are locked in node 1, and live in node 0.
    assert_eq!(
        walrus_cluster.nodes[1]
            .storage_node()
            .existing_shards()
            .await
            .len(),
        2
    );
    assert_eq!(
        walrus_cluster.nodes[0]
            .storage_node()
            .existing_shards()
            .await
            .len(),
        2
    );
    assert_unordered_eq!(
        walrus_cluster.nodes[1]
            .storage_node()
            .existing_shards_live()
            .await,
        vec![]
    );
    assert_unordered_eq!(
        walrus_cluster.nodes[0]
            .storage_node()
            .existing_shards_live()
            .await,
        vec![ShardIndex(0), ShardIndex(1)]
    );

    walrus_cluster.wait_for_nodes_to_reach_epoch(6).await;

    // In epoch 6, shards should be removed from node 1.
    assert_eq!(
        walrus_cluster.nodes[1]
            .storage_node()
            .existing_shards()
            .await
            .len(),
        0
    );
    assert_eq!(
        walrus_cluster.nodes[0]
            .storage_node()
            .existing_shards()
            .await
            .len(),
        2
    );

    Ok(())
}

#[ignore = "ignore E2E tests by default"]
#[cfg(msim)]
#[walrus_simtest]
async fn test_ptb_retriable_error() -> TestResult {
    // Set up test environment with cluster and client
    let (_sui_cluster_handle, cluster, client) = test_cluster::default_setup().await?;

    // Create an atomic counter to track number of failure attempts
    let failure_counter = Arc::new(AtomicU32::new(0));
    let failure_counter_clone = failure_counter.clone();

    // Register a fail point that will fail the first 2 attempts and succeed on the 3rd
    register_fail_point_if("ptb_executor_stake_pool_retriable_error", move || {
        let attempt_number = failure_counter_clone.fetch_add(1, Ordering::SeqCst);
        attempt_number < 2 // Return true (fail) for first 2 attempts
    });

    // Attempt to stake with the node pool - this should retry on failure
    let result = client
        .inner
        .stake_with_node_pool(
            cluster.nodes[0]
                .storage_node_capability
                .as_ref()
                .unwrap()
                .node_id,
            1_234_567_000, // Stake amount
        )
        .await;

    // Verify the operation was attempted 3 times (2 failures + 1 success)
    assert_eq!(failure_counter.load(Ordering::SeqCst), 3);
    assert!(result.is_ok());

    // Clean up the fail point
    clear_fail_point("ptb_executor_stake_pool_retriable_error");
    Ok(())
}
