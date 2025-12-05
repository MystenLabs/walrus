// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// Allowing `unwrap`s in tests.
#![allow(clippy::unwrap_used)]
// This avoids the following error when running simtests:
// error[E0275]: overflow evaluating the requirement `{coroutine ...}: std::marker::Send`
#![recursion_limit = "256"]

//! Contains end-to-end tests for the Walrus client interacting with a Walrus test cluster.

#[cfg(msim)]
use std::sync::atomic::{AtomicU32, Ordering};
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    num::NonZeroU16,
    path::PathBuf,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use rand::{Rng, random, seq::SliceRandom, thread_rng};
use reqwest::Url;
#[cfg(msim)]
use sui_macros::{clear_fail_point, register_fail_point_if};
use sui_types::base_types::{SUI_ADDRESS_LENGTH, SuiAddress};
use tokio::sync::Mutex;
use tokio_stream::StreamExt;
use walrus_core::{
    BlobId,
    DEFAULT_ENCODING,
    EncodingType,
    EpochCount,
    QuiltPatchId,
    ShardIndex,
    SliverPairIndex,
    encoding::{
        BLOB_TYPE_ATTRIBUTE_KEY,
        EncodingFactory as _,
        Primary,
        QUILT_TYPE_VALUE,
        encoded_blob_length_for_n_shards,
        quilt_encoding::{QuiltApi, QuiltStoreBlob, QuiltVersionV1},
    },
    merkle::Node,
    messages::BlobPersistenceType,
    metadata::{QuiltMetadata, VerifiedBlobMetadataWithId},
};
use walrus_proc_macros::walrus_simtest;
use walrus_sdk::{
    client::{
        Blocklist,
        StoreArgs,
        StoreBlobsApi as _,
        WalrusNodeClient,
        client_types::WalrusStoreBlob,
        quilt_client::QuiltClientConfig,
        responses::{BlobStoreResult, QuiltStoreResult},
        upload_relay_client::UploadRelayClient,
    },
    config::ClientConfig,
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
    store_optimizations::StoreOptimizations,
    upload_relay::tip_config::{TipConfig, TipKind},
    uploader::TailHandling,
};
use walrus_service::test_utils::{
    StorageNodeHandleTrait,
    TestNodesConfig,
    UnusedSocketAddress,
    test_cluster::{self, FROST_PER_NODE_WEIGHT},
};
use walrus_storage_node_client::api::BlobStatus;
use walrus_sui::{
    client::{
        BlobPersistence,
        ExpirySelectionPolicy,
        PostStoreAction,
        ReadClient,
        SuiClientError,
        SuiContractClient,
        retry_client::{RetriableSuiClient, retriable_sui_client::LazySuiClientBuilder},
    },
    config::WalletConfig,
    test_utils::{self, fund_addresses, wallet_for_testing},
    types::{
        Blob,
        BlobEvent,
        ContractEvent,
        move_errors::{MoveExecutionError, RawMoveError},
        move_structs::{BlobAttribute, BlobWithAttribute, Credits, SharedBlob},
    },
};
use walrus_test_utils::{Result as TestResult, WithTempDir, assert_unordered_eq, async_param_test};
use walrus_upload_relay::{
    UploadRelayHandle,
    controller::{WalrusUploadRelayConfig, get_client_with_config},
};
use walrus_utils::{backoff::ExponentialBackoffConfig, metrics::Registry};

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
    walrus_test_utils::init_tracing();
    assert!(matches!(
        run_store_and_read_with_crash_failures(&[], &[], blob_size, None).await,
        Ok(()),
    ))
}

/// Basic read and write test for the client.
///
/// It generates random blobs and stores them.
/// It then reads the blobs back and verifies that the data is correct.
async fn basic_store_and_read<F>(
    client: &WithTempDir<WalrusNodeClient<SuiContractClient>>,
    num_blobs: usize,
    data_length: usize,
    upload_relay_client: Option<UploadRelayClient>,
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
        let path = PathBuf::from(format!("blob_{i}"));
        path_to_data.insert(path.clone(), data.to_vec());
        blobs_with_paths.push((path, data.to_vec()));
    }

    let tail_handle_collector = Arc::new(Mutex::new(vec![]));
    let store_args = {
        let store_args = StoreArgs::default_with_epochs(1).no_store_optimizations();
        if let Some(upload_relay_client) = upload_relay_client {
            store_args.with_upload_relay_client(upload_relay_client)
        } else if rand::thread_rng().gen_bool(0.5) {
            store_args
                .with_tail_handling(TailHandling::Detached)
                .with_tail_handle_collector(tail_handle_collector.clone())
        } else {
            store_args
        }
    };

    let store_result = client
        .as_ref()
        .reserve_and_store_blobs_retry_committees_with_path(blobs_with_paths, &store_args)
        .await?;

    // Wait for the tail uploads to complete.
    // This is going to ensure that we wait for tail uploads to complete before reading the blobs.
    // This is similar to blocking on tail uploads but happens after forming the certificate.
    let mut handles = tail_handle_collector.lock().await;
    while let Some(handle) = handles.pop() {
        handle.await.unwrap();
    }

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
    walrus_test_utils::init_tracing();
    let result = run_store_and_read_with_crash_failures(
        failed_shards_write,
        failed_shards_read,
        31415,
        None,
    )
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
        (act, exp) => panic!("test result mismatch; expected=({exp:?}); actual=({act:?});"),
    }
}

async fn run_store_and_read_with_crash_failures(
    failed_shards_write: &[usize],
    failed_shards_read: &[usize],
    data_length: usize,
    upload_relay_client: Option<UploadRelayClient>,
) -> TestResult {
    walrus_test_utils::init_tracing();

    let (_sui_cluster_handle, mut cluster, client, _) =
        test_cluster::E2eTestSetupBuilder::new().build().await?;
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
    basic_store_and_read(&client, 4, data_length, upload_relay_client, pre_read_hook).await
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
/// Stores a blob that is inconsistent in 1 shard.
async fn test_inconsistency(failed_nodes: &[usize]) -> TestResult {
    walrus_test_utils::init_tracing();

    let (_sui_cluster_handle, mut cluster, mut client, _) =
        test_cluster::E2eTestSetupBuilder::new().build().await?;

    // Store a blob and get confirmations from each node.
    let blob = walrus_test_utils::random_data(31415);

    // Find the shards of the failed nodes.
    let failed_node_names: Vec<String> = failed_nodes.iter().map(|i| format!("node-{i}")).collect();
    let committees = client
        .as_ref()
        .get_committees()
        .await
        .expect("committees should be available");
    let shards_of_failed_nodes = client.as_ref().shards_of(&failed_node_names, &committees);

    // Encode the blob with false metadata for one shard.
    let (pairs, metadata) = client
        .as_ref()
        .encoding_config()
        .get_for_type(DEFAULT_ENCODING)
        .encode_with_metadata(blob)?;
    let mut metadata = metadata.metadata().to_owned();
    let mut i = 0;
    // Change a shard that is not in the failure set. Since the mapping of slivers to shards
    // depends on the blob ID, we need to search for an invalid hash for which the modified shard
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
        .get_existing_or_register(
            &[&metadata],
            1,
            BlobPersistence::Permanent,
            StoreOptimizations::none(),
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
            Arc::new(pairs),
            &BlobPersistenceType::Permanent,
            None,
            TailHandling::Blocking,
            None,
            None,
        )
        .await?;

    // Stop the nodes in the failure set.
    failed_nodes
        .iter()
        .for_each(|&idx| cluster.cancel_node(idx));

    let blob_with_attr = BlobWithAttribute {
        blob: blob_sui_object,
        attribute: None,
    };
    client
        .as_mut()
        .sui_client()
        .certify_blobs(&[(&blob_with_attr, certificate)], PostStoreAction::Keep)
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
        reuse_resource_permanent_1: (
            1, BlobPersistence::Permanent, 1, BlobPersistence::Permanent, true
        ),
        reuse_resource_permanent_2: (
            2, BlobPersistence::Permanent, 1, BlobPersistence::Permanent, true
        ),
        reuse_and_extend_permanent: (
            1, BlobPersistence::Permanent, 2, BlobPersistence::Permanent, true
        ),
        reuse_resource_deletable_1: (
            1, BlobPersistence::Deletable, 1, BlobPersistence::Deletable, true
        ),
        reuse_resource_deletable_2: (
            2, BlobPersistence::Deletable, 1, BlobPersistence::Deletable, true
        ),
        reuse_and_extend_deletable: (
            1, BlobPersistence::Deletable, 2, BlobPersistence::Deletable, true
        ),
        persistence_mismatch_1: (
            1, BlobPersistence::Permanent, 1, BlobPersistence::Deletable, false
        ),
        persistence_mismatch_2: (
            1, BlobPersistence::Deletable, 1, BlobPersistence::Permanent, false
        ),
    ]
}
/// Tests that the client reuses existing (uncertified) blob registrations to store blobs.
///
/// The `epochs_ahead_registered` are the epochs ahead of the already-existing blob object.
/// The `persistence_registered` is the persistence of the already-existing blob object.
/// The `epochs_ahead_required` are the epochs ahead that are requested to the client when
/// registering anew.
/// The `persistence_required` is the persistence that is requested to the client when
/// registering anew.
/// `should_match` is a boolean that indicates if the blob object used in the final upload
/// should be the same as the first one registered.
async fn test_store_with_existing_blob_resource(
    epochs_ahead_registered: EpochCount,
    persistence_registered: BlobPersistence,
    epochs_ahead_required: EpochCount,
    persistence_required: BlobPersistence,
    should_match: bool,
) -> TestResult {
    walrus_test_utils::init_tracing();

    let (_sui_cluster_handle, _cluster, client, _) =
        test_cluster::E2eTestSetupBuilder::new().build().await?;

    let blobs = walrus_test_utils::random_data_list(31415, 4);
    let encoding_type = DEFAULT_ENCODING;
    let metadata = blobs
        .iter()
        .map(|blob| {
            let (_, metadata) = client
                .as_ref()
                .encoding_config()
                .get_for_type(encoding_type)
                .encode_with_metadata(blob.clone())
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
        .get_existing_or_register(
            &metadata.iter().collect::<Vec<_>>(),
            epochs_ahead_registered,
            persistence_registered,
            StoreOptimizations::all(),
        )
        .await?
        .into_iter()
        .map(|(blob, _)| (blob.blob_id, blob))
        .collect::<HashMap<_, _>>();

    // Now ask the client to store again.
    let store_args = StoreArgs::default_with_epochs(epochs_ahead_required)
        .with_encoding_type(encoding_type)
        .with_persistence(persistence_required);
    let blob_stores = client
        .inner
        .reserve_and_store_blobs(blobs, &store_args)
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
    client: &WithTempDir<WalrusNodeClient<SuiContractClient>>,
    blob: &[u8],
    encoding_type: EncodingType,
    epochs_ahead: EpochCount,
    persistence: BlobPersistence,
) -> TestResult<BlobId> {
    // Encode blob and get metadata
    let metadata = client
        .as_ref()
        .encoding_config()
        .get_for_type(encoding_type)
        .compute_metadata(blob)
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
        .get_existing_or_register(
            &[&metadata],
            epochs_ahead,
            persistence,
            StoreOptimizations::all(),
        )
        .await?
        .into_iter()
        .map(|(blob, _)| blob.blob_id)
        .next()
        .expect("should have registered blob");

    Ok(blob_id)
}

/// Store a blob and return the blob ID.
async fn store_blob(
    client: &WithTempDir<WalrusNodeClient<SuiContractClient>>,
    blob: Vec<u8>,
    encoding_type: EncodingType,
    epochs_ahead: EpochCount,
    persistence: BlobPersistence,
) -> TestResult<BlobId> {
    let store_args = StoreArgs::default_with_epochs(epochs_ahead)
        .with_encoding_type(encoding_type)
        .with_persistence(persistence);
    let result = client
        .inner
        .reserve_and_store_blobs(vec![blob.to_vec()], &store_args)
        .await?;

    Ok(result
        .into_iter()
        .next()
        .expect("should have one blob store result")
        .blob_id()
        .expect("blob ID should be present"))
}

/// Tests that the client can store and read duplicate blobs.
#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
pub async fn test_store_and_read_duplicate_blobs() -> TestResult {
    walrus_test_utils::init_tracing();

    let (_sui_cluster_handle, _cluster, client, _) =
        test_cluster::E2eTestSetupBuilder::new().build().await?;
    let client = client.as_ref();

    // Generate random blobs.
    let mut blob_data = walrus_test_utils::random_data_list(31415, 3);
    blob_data.push(blob_data[0].clone());
    blob_data.push(blob_data[1].clone());
    blob_data.push(blob_data[0].clone());
    let blobs_with_paths: Vec<_> = blob_data
        .into_iter()
        .enumerate()
        .map(|(i, data)| {
            let path = PathBuf::from(format!("blob_{i}"));
            (path, data)
        })
        .collect();

    let store_args = StoreArgs::default_with_epochs(1).no_store_optimizations();
    let store_result_with_path = client
        .reserve_and_store_blobs_retry_committees_with_path(blobs_with_paths.clone(), &store_args)
        .await?;

    let read_result =
        futures::future::join_all(store_result_with_path.iter().map(|result| async {
            let blob = client
                .read_blob::<Primary>(
                    &result
                        .blob_store_result
                        .blob_id()
                        .expect("blob ID should be present"),
                )
                .await
                .expect("should be able to read blob");
            (result.blob_store_result.blob_id(), blob)
        }))
        .await;

    assert_eq!(store_result_with_path.len(), blobs_with_paths.len());
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

async_param_test! {
    #[ignore = "ignore E2E tests by default"]
    #[walrus_simtest]
    test_store_with_existing_blobs -> TestResult : [
        deletable: (BlobPersistence::Deletable),
        permanent: (BlobPersistence::Permanent),
    ]
}
/// Tests that blobs can be extended when possible.
async fn test_store_with_existing_blobs(persistence: BlobPersistence) -> TestResult {
    walrus_test_utils::init_tracing();

    let (_sui_cluster_handle, _cluster, client, _) =
        test_cluster::E2eTestSetupBuilder::new().build().await?;

    let blobs = walrus_test_utils::random_data_list(31415, 5);

    // Initial setup, with blobs in different states, the names indicate the later outcome
    // of a following store operation.
    let encoding_type = DEFAULT_ENCODING;
    let reuse_blob = register_blob(&client, &blobs[0], encoding_type, 40, persistence).await?;
    let certify_and_extend_blob =
        register_blob(&client, &blobs[1], encoding_type, 10, persistence).await?;
    let already_certified_blob =
        store_blob(&client, blobs[2].clone(), encoding_type, 50, persistence).await?;
    let extended_blob =
        store_blob(&client, blobs[3].clone(), encoding_type, 20, persistence).await?;

    let epoch = client.as_ref().sui_client().current_epoch().await?;
    let epochs_ahead = 30;
    let store_args = StoreArgs::default_with_epochs(epochs_ahead)
        .with_encoding_type(encoding_type)
        .with_persistence(persistence);
    let store_results = client
        .inner
        .reserve_and_store_blobs(blobs, &store_args)
        .await?;
    for (blob_index, result) in store_results.into_iter().enumerate() {
        let Some(end_epoch) = result.end_epoch() else {
            panic!("end_epoch should be present for blob {blob_index}");
        };
        assert!(
            end_epoch >= epoch + epochs_ahead,
            "blob {blob_index}: end_epoch ({end_epoch}) should be at least epoch ({epoch}) + \
            epochs_ahead ({epochs_ahead})"
        );

        if result.blob_id() == Some(reuse_blob) {
            assert!(matches!(
                &result,
                BlobStoreResult::NewlyCreated{blob_object:_, resource_operation, ..
                } if resource_operation.is_reuse_registration()));
        } else if result.blob_id() == Some(certify_and_extend_blob) {
            assert!(matches!(
                &result,
                BlobStoreResult::NewlyCreated {
                    resource_operation,
                    ..
                } if resource_operation.is_certify_and_extend()
            ));
        } else if result.blob_id() == Some(already_certified_blob) {
            assert!(matches!(&result, BlobStoreResult::AlreadyCertified { .. }));
        } else if result.blob_id() == Some(extended_blob) {
            assert!(matches!(
                &result,
                BlobStoreResult::NewlyCreated {
                    resource_operation,
                ..
            } if resource_operation.is_extend()
            ));
        } else {
            assert!(matches!(
                &result,
                BlobStoreResult::NewlyCreated {
                    resource_operation,
                    ..
                } if resource_operation.is_registration()
            ));
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
    walrus_test_utils::init_tracing();

    let (_sui_cluster_handle, _cluster, client, _) =
        test_cluster::E2eTestSetupBuilder::new().build().await?;

    let blobs = walrus_test_utils::random_data_list(31415, 4);
    let encoding_type = DEFAULT_ENCODING;
    let unencoded_blobs = blobs
        .iter()
        .cloned()
        .enumerate()
        .map(|(i, data)| {
            WalrusStoreBlob::new_unencoded(
                data,
                format!("test-{i:02}"),
                BlobAttribute::default(),
                client
                    .as_ref()
                    .encoding_config()
                    .get_for_type(encoding_type),
            )
        })
        .collect();
    let encoded_blobs = walrus_sdk::client::encode_blobs(unencoded_blobs, None, None)?;
    let encoded_sizes = encoded_blobs
        .iter()
        .map(|blob| {
            blob.common
                .encoded_size()
                .expect("encoded size should be present")
        })
        .collect::<Vec<_>>();

    // Reserve space for the blobs. Collect all original storage resource objects IDs.
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

    // Now ask the client to store again.
    // Collect all object IDs of the newly created blob objects.
    let store_args =
        StoreArgs::default_with_epochs(epochs_ahead_required).with_encoding_type(encoding_type);
    let blob_store = client
        .inner
        .reserve_and_store_blobs(blobs, &store_args)
        .await?
        .into_iter()
        .map(|blob_store_result| match blob_store_result {
            BlobStoreResult::NewlyCreated { blob_object, .. } => blob_object.storage.id,
            _ => panic!("the client should be able to store the blob"),
        })
        .collect::<HashSet<_>>();

    // Check that the object IDs are the same.
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
    walrus_test_utils::init_tracing();
    let (_sui_cluster_handle, _cluster, client, _) =
        test_cluster::E2eTestSetupBuilder::new().build().await?;
    let blobs = walrus_test_utils::random_data_list(314, 1);

    // Store the blob multiple times, using separate end times to obtain multiple blob objects
    // with the same blob ID.
    let encoding_type = DEFAULT_ENCODING;
    for idx in 1..blobs_to_create + 1 {
        let store_args = StoreArgs::default_with_epochs(idx)
            .with_encoding_type(encoding_type)
            .no_store_optimizations()
            .deletable();
        client
            .as_ref()
            .reserve_and_store_blobs(blobs.clone(), &store_args)
            .await?;
    }

    // Add a blob that is not deletable.
    let store_args = StoreArgs::default_with_epochs(1)
        .with_encoding_type(encoding_type)
        .no_store_optimizations()
        .permanent();
    let result = client
        .as_ref()
        .reserve_and_store_blobs(blobs, &store_args)
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
        .delete_owned_blob(&blob_id.expect("blob ID should be present"))
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
async fn test_storage_nodes_do_not_serve_data_for_deleted_blobs() -> TestResult {
    walrus_test_utils::init_tracing();
    let (_sui_cluster_handle, _cluster, client, _) =
        test_cluster::E2eTestSetupBuilder::new().build().await?;
    let client = client.as_ref();
    let blob = walrus_test_utils::random_data(314);
    let blobs = vec![blob.clone()];

    let store_args = StoreArgs::default_with_epochs(1)
        .no_store_optimizations()
        .deletable();
    let results = client.reserve_and_store_blobs(blobs, &store_args).await?;
    let store_result = results.first().expect("should have one blob store result");
    let blob_id = store_result
        .blob_id()
        .expect("blob ID should be present after store");
    assert!(matches!(store_result, BlobStoreResult::NewlyCreated { .. }));

    assert_eq!(client.read_blob::<Primary>(&blob_id).await?, blob);

    let count_deleted = client.delete_owned_blob(&blob_id).await?;
    assert_eq!(count_deleted, 1, "should have deleted one blob");
    tokio::time::sleep(Duration::from_millis(50)).await;

    check_that_blob_is_not_available(client, blob_id).await
}

/// Tests that nodes correctly update the blob info for expired deletable blobs and no longer serve
/// data for them.
#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_storage_nodes_do_not_serve_data_for_expired_deletable_blobs() -> TestResult {
    walrus_test_utils::init_tracing();
    let epoch_duration = Duration::from_secs(15);
    let (_sui_cluster_handle, cluster, client, _) = test_cluster::E2eTestSetupBuilder::new()
        .with_epoch_duration(epoch_duration)
        .build()
        .await?;
    let client = client.as_ref();
    let blobs = walrus_test_utils::random_data_list(314, 1);
    let blob = blobs[0].clone();

    // Store the blob for 42 epochs.
    let [
        BlobStoreResult::NewlyCreated {
            blob_object:
                Blob {
                    blob_id,
                    id: object_id,
                    ..
                },
            ..
        },
    ] = client
        .reserve_and_store_blobs(
            blobs.clone(),
            &StoreArgs::default_with_epochs(42)
                .no_store_optimizations()
                .deletable(),
        )
        .await?[..]
    else {
        panic!("should have exactly one newly created blob");
    };

    // Store the blob again for a single epoch.
    let [
        BlobStoreResult::NewlyCreated {
            ref blob_object, ..
        },
    ] = client
        .reserve_and_store_blobs(
            blobs,
            &StoreArgs::default_with_epochs(1)
                .no_store_optimizations()
                .deletable(),
        )
        .await?[..]
    else {
        panic!("should have exactly one newly created blob");
    };
    assert_eq!(blob_object.blob_id, blob_id);
    let early_end_epoch = blob_object.storage.end_epoch;

    // Make sure we can read the blob.
    assert_eq!(client.read_blob::<Primary>(&blob_id).await?, blob);

    // Delete the blob with the later end epoch.
    client.delete_owned_blob_by_object(object_id).await?;

    // Wait until the end epoch of the early blob is reached, at which point it should no longer be
    // available.
    tokio::time::timeout(
        epoch_duration * 3,
        cluster.wait_for_nodes_to_reach_epoch(early_end_epoch),
    )
    .await
    .expect("timed out waiting for nodes to reach end epoch of first blob");

    // Wait for garbage collection to complete for this epoch on all nodes.
    walrus_service::test_utils::wait_for_garbage_collection_to_complete(
        &cluster.nodes,
        early_end_epoch,
        epoch_duration * 3,
    )
    .await?;

    check_that_blob_is_not_available(client, blob_id).await
}

async fn check_that_blob_is_not_available(
    client: &WalrusNodeClient<SuiContractClient>,
    blob_id: BlobId,
) -> TestResult {
    let status_result = client
        .get_verified_blob_status(
            &blob_id,
            client.sui_client().read_client(),
            Duration::from_secs(1),
        )
        .await?;
    assert!(
        matches!(status_result, BlobStatus::Nonexistent),
        "status_result: {status_result:?}"
    );

    let read_result = client.read_blob::<Primary>(&blob_id).await;
    assert!(matches!(
        read_result.unwrap_err().kind(),
        ClientErrorKind::BlobIdDoesNotExist,
    ));

    Ok(())
}

fn group_identifiers_randomly<'a>(identifiers: &'a mut [&str]) -> Vec<Vec<&'a str>> {
    identifiers.shuffle(&mut thread_rng());

    let mut groups = Vec::new();
    let mut current_pos = 0;

    while current_pos < identifiers.len() {
        let end_index = thread_rng().gen_range(current_pos..=identifiers.len());
        let group = identifiers[current_pos..end_index].to_vec();
        groups.push(group);
        current_pos = end_index;
    }

    groups
}

async_param_test! {
    #[ignore = "ignore E2E tests by default"]
    #[walrus_simtest]
    test_store_quilt -> TestResult : [
        one_blob: (1),
        two_blobs: (2),
        seven_blobs: (10),
    ]
}
/// Tests that a quilt can be stored.
async fn test_store_quilt(blobs_to_create: u32) -> TestResult {
    walrus_test_utils::init_tracing();

    let test_nodes_config = TestNodesConfig {
        node_weights: vec![7, 7, 7, 7, 7],
        ..Default::default()
    };
    let test_cluster_builder =
        test_cluster::E2eTestSetupBuilder::new().with_test_nodes_config(test_nodes_config);
    let (_sui_cluster_handle, _cluster, client, _) = test_cluster_builder.build().await?;
    let client = client.as_ref();
    let blobs = walrus_test_utils::random_data_list(314, blobs_to_create as usize);
    let encoding_type = DEFAULT_ENCODING;
    let quilt_store_blobs = blobs
        .iter()
        .enumerate()
        .map(|(i, blob)| {
            let mut blob = QuiltStoreBlob::new(blob, format!("test-blob-{}", i + 1))
                .expect("Should create blob");
            if i == 0 {
                blob = blob.with_tags(vec![("tag1".to_string(), "value1".to_string())]);
            }
            blob
        })
        .collect::<Vec<_>>();

    // Store the quilt.
    let quilt_client = client
        .quilt_client()
        .with_config(QuiltClientConfig::new(6, Duration::from_mins(1)));
    let quilt = quilt_client
        .construct_quilt::<QuiltVersionV1>(&quilt_store_blobs, encoding_type)
        .await?;
    let quilt_index = quilt.quilt_index()?.clone();
    let store_args = StoreArgs::default_with_epochs(2)
        .with_encoding_type(encoding_type)
        .no_store_optimizations();
    let store_operation_result = quilt_client
        .reserve_and_store_quilt::<QuiltVersionV1>(quilt, &store_args)
        .await?;

    let QuiltStoreResult {
        blob_store_result,
        stored_quilt_blobs,
    } = store_operation_result;
    let blob_object = match blob_store_result {
        BlobStoreResult::NewlyCreated { blob_object, .. } => blob_object,
        _ => panic!("Expected NewlyCreated, got {blob_store_result:?}"),
    };

    let attribute = client.get_blob_by_object_id(&blob_object.id).await?;
    assert_eq!(
        attribute.attribute,
        Some(BlobAttribute::from([(
            BLOB_TYPE_ATTRIBUTE_KEY,
            QUILT_TYPE_VALUE,
        )]))
    );

    // Read the blobs in the quilt.
    let id_blob_map = quilt_store_blobs
        .iter()
        .map(|b| (b.identifier(), b))
        .collect::<HashMap<_, _>>();

    let blob_id = blob_object.blob_id;
    let quilt_metadata = quilt_client.get_quilt_metadata(&blob_id).await?;
    let QuiltMetadata::V1(metadata_v1) = quilt_metadata;
    assert_eq!(metadata_v1.index, quilt_index);

    let mut identifiers = stored_quilt_blobs
        .iter()
        .map(|b| b.identifier.as_str())
        .collect::<Vec<_>>();
    let groups = group_identifiers_randomly(&mut identifiers);

    tracing::info!(groups = ?groups, "test retrieving quilts by groups");

    for group in groups {
        let retrieved_quilt_blobs: Vec<QuiltStoreBlob> = quilt_client
            .get_blobs_by_identifiers(&blob_id, &group)
            .await?;

        assert_eq!(
            retrieved_quilt_blobs.len(),
            group.len(),
            "Mismatch in number of blobs retrieved from quilt"
        );

        for retrieved_quilt_blob in &retrieved_quilt_blobs {
            let original_blob = id_blob_map
                .get(retrieved_quilt_blob.identifier())
                .expect("identifier should be present");
            assert_eq!(&retrieved_quilt_blob, original_blob);
        }
    }

    // Test retrieving blobs by patch IDs
    let quilt_patch_ids: Vec<QuiltPatchId> = stored_quilt_blobs
        .iter()
        .map(|stored_blob| {
            QuiltPatchId::from_str(&stored_blob.quilt_patch_id)
                .expect("should be able to parse quilt patch ID")
        })
        .collect();

    let retrieved_blobs_by_ids = quilt_client.get_blobs_by_ids(&quilt_patch_ids).await?;

    assert_eq!(
        retrieved_blobs_by_ids.len(),
        quilt_patch_ids.len(),
        "Number of retrieved blobs should match number of patch IDs"
    );

    // Verify that retrieved blobs match the original blobs
    for retrieved_blob in &retrieved_blobs_by_ids {
        let original_blob = id_blob_map
            .get(retrieved_blob.identifier())
            .expect("identifier should be present");
        assert_eq!(retrieved_blob, *original_blob);
    }

    // Test retrieving the blobs by tag.
    let retrieved_blobs_by_tag = quilt_client
        .get_blobs_by_tag(&blob_id, "tag1", "value1")
        .await?;
    assert_eq!(retrieved_blobs_by_tag.len(), 1);
    assert_eq!(retrieved_blobs_by_tag[0].identifier(), "test-blob-1");

    Ok(())
}

#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_blocklist() -> TestResult {
    walrus_test_utils::init_tracing();
    let blocklist_dir = tempfile::tempdir().expect("temporary directory creation must succeed");
    let (_sui_cluster_handle, _cluster, client, _) = test_cluster::E2eTestSetupBuilder::new()
        .with_test_nodes_config(TestNodesConfig {
            blocklist_dir: Some(blocklist_dir.path().to_path_buf()),
            ..Default::default()
        })
        .build()
        .await?;

    let client = client.as_ref();
    let blob = walrus_test_utils::random_data(314);
    let blobs = vec![blob.clone()];

    let store_args = StoreArgs::default_with_epochs(1)
        .no_store_optimizations()
        .deletable();
    let store_results = client.reserve_and_store_blobs(blobs, &store_args).await?;
    let store_result = store_results[0].clone();
    let blob_id = store_result.blob_id();
    assert!(matches!(store_result, BlobStoreResult::NewlyCreated { .. }));

    assert_eq!(
        client
            .read_blob::<Primary>(&blob_id.expect("blob ID should be present"))
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

    tracing::info!("adding blob to blocklist");

    for blocklist in blocklists.iter_mut() {
        blocklist.insert(blob_id.expect("blob ID should be present"))?;
    }

    // Read the blob using the client until it fails with forbidden
    let mut blob_read_result = client
        .read_blob::<Primary>(&blob_id.expect("blob ID should be present"))
        .await;
    while let Ok(_blob) = blob_read_result {
        blob_read_result = client
            .read_blob::<Primary>(&blob_id.expect("blob ID should be present"))
            .await;
        // sleep for a bit to allow the nodes to sync
        tokio::time::sleep(Duration::from_secs(30)).await;
    }

    let error = blob_read_result.expect_err("result must be an error");

    assert!(
        matches!(error.kind(), ClientErrorKind::BlobIdBlocked(_)),
        "unexpected error {error:?}"
    );

    // Remove the blob from the blocklist
    for blocklist in blocklists.iter_mut() {
        blocklist.remove(&blob_id.expect("blob ID should be present"))?;
    }

    tracing::info!("removing blob from blocklist");

    // Read the blob again until it succeeds
    let mut blob_read_result = client
        .read_blob::<Primary>(&blob_id.expect("blob ID should be present"))
        .await;
    while blob_read_result.is_err() {
        blob_read_result = client
            .read_blob::<Primary>(&blob_id.expect("blob ID should be present"))
            .await;
        // sleep for a bit to allow the nodes to sync
        tokio::time::sleep(Duration::from_secs(30)).await;
    }

    assert_eq!(blob_read_result?, blob);

    Ok(())
}

#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_blob_operations_with_credits() -> TestResult {
    walrus_test_utils::init_tracing();
    let (_sui_cluster_handle, _cluster, client, _) = test_cluster::E2eTestSetupBuilder::new()
        .with_credits()
        .build()
        .await?;
    let client = client.as_ref();

    // Get initial funds in credits object.
    let credits_object_id = client
        .sui_client()
        .read_client()
        .credits_object_id()
        .expect("credits object ID should be set");
    let initial_credits_funds = client
        .sui_client()
        .read_client()
        .retriable_sui_client()
        .get_sui_object::<Credits>(credits_object_id)
        .await?
        .subsidy_pool;

    // Store a blob with credits
    let blobs = walrus_test_utils::random_data_list(314, 1);
    let store_args = StoreArgs::default_with_epochs(1).no_store_optimizations();
    let store_result = client.reserve_and_store_blobs(blobs, &store_args).await?;

    let blob_object = match &store_result[0] {
        BlobStoreResult::NewlyCreated { blob_object, .. } => blob_object.clone(),
        _ => panic!("Expected newly created blob"),
    };

    let initial_storage = blob_object.storage.clone();

    // Extend blob storage with credits
    client.sui_client().extend_blob(blob_object.id, 5).await?;

    // Verify blob storage was extended with credits
    let extended_blob: Blob = client
        .sui_client()
        .retriable_sui_client()
        .get_sui_object(blob_object.id)
        .await?;

    // Verify the blob was extended
    assert!(extended_blob.storage.end_epoch > initial_storage.end_epoch);

    // Verify subsidies were applied by checking remaining funds
    let credits_funds = client
        .sui_client()
        .read_client()
        .retriable_sui_client()
        .get_sui_object::<Credits>(credits_object_id)
        .await?
        .subsidy_pool;
    assert!(
        credits_funds < initial_credits_funds,
        "Credits should have been used"
    );

    Ok(())
}

#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_walrus_subsidies_get_called_by_node() -> TestResult {
    walrus_test_utils::init_tracing();

    let (_sui_cluster_handle, cluster, client, _) = test_cluster::E2eTestSetupBuilder::new()
        .with_epoch_duration(Duration::from_secs(20))
        .build()
        .await?;

    let initial_subsidies_funds = client
        .as_ref()
        .sui_client()
        .read_client()
        .get_walrus_subsidies_object(true)
        .await?
        .subsidy_pool_funds()
        .expect("should return some, subsidies were requested with inner");
    assert!(initial_subsidies_funds > 0);

    let epoch = client.as_ref().sui_client().current_epoch().await?;
    // Use basic_store_and_read with our pre_read_hook.
    basic_store_and_read(&client, 4, 314, None, || Ok(())).await?;

    // Wait for the cluster to reach two epochs ahead of the current epoch. This is to ensure that
    // the subsidies are processed at least once between checking the initial and final funds, since
    // there is a full epoch between the two checks.
    cluster.wait_for_nodes_to_reach_epoch(epoch + 2).await;

    let final_subsidies_funds = client
        .as_ref()
        .sui_client()
        .read_client()
        .get_walrus_subsidies_object(true)
        .await?
        .subsidy_pool_funds()
        .expect("should return some, subsidies were requested with inner");

    assert!(final_subsidies_funds < initial_subsidies_funds);

    Ok(())
}

/// Tests that storing the same blob multiple times with possibly different end epochs,
/// persistence, and force-store conditions always works.
#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_multiple_stores_same_blob() -> TestResult {
    walrus_test_utils::init_tracing();
    let (_sui_cluster_handle, _cluster, client, _) =
        test_cluster::E2eTestSetupBuilder::new().build().await?;
    let client = client.as_ref();
    let blobs = walrus_test_utils::random_data_list(314, 1);
    let encoding_type = DEFAULT_ENCODING;

    // NOTE: not in a param_test, because we want to test these store operations in sequence.
    // If the last `bool` parameter is `true`, the store operation should return a
    // `BlobStoreResult::AlreadyCertified`. Otherwise, it should return a
    // `BlobStoreResult::NewlyCreated`.
    let configurations = vec![
        (
            1,
            StoreOptimizations::all(),
            BlobPersistence::Deletable,
            false,
        ),
        (
            1,
            StoreOptimizations::none(),
            BlobPersistence::Deletable,
            false,
        ),
        (
            2,
            StoreOptimizations::all(),
            BlobPersistence::Deletable,
            true,
        ), // Extend lifetime
        (
            3,
            StoreOptimizations::none(),
            BlobPersistence::Deletable,
            false,
        ),
        (
            1,
            StoreOptimizations::all(),
            BlobPersistence::Permanent,
            false,
        ),
        (
            1,
            StoreOptimizations::all(),
            BlobPersistence::Permanent,
            true,
        ),
        (
            1,
            StoreOptimizations::none(),
            BlobPersistence::Permanent,
            false,
        ),
        (
            4,
            StoreOptimizations::all(),
            BlobPersistence::Permanent,
            true,
        ), // Extend lifetime
        (
            2,
            StoreOptimizations::all(),
            BlobPersistence::Permanent,
            true,
        ),
        (
            2,
            StoreOptimizations::none(),
            BlobPersistence::Permanent,
            false,
        ),
        (
            1,
            StoreOptimizations::all(),
            BlobPersistence::Deletable,
            true,
        ),
        (
            5,
            StoreOptimizations::all(),
            BlobPersistence::Deletable,
            true,
        ), // Extend lifetime
    ];

    for (i, (epochs, store_optimizations, persistence, is_already_certified)) in
        configurations.into_iter().enumerate()
    {
        tracing::info!(
            "testing step {i}: epochs={epochs}, store_optimizations={store_optimizations:?}, \
            persistence={persistence:?}, is_already_certified={is_already_certified}",
        );
        let store_args = StoreArgs::new(
            encoding_type,
            epochs,
            store_optimizations,
            persistence,
            PostStoreAction::Keep,
        );
        let results = client
            .reserve_and_store_blobs(blobs.clone(), &store_args)
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
            other => panic!("we either store the blob, or find it's already created:\n{other:?}"),
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
    walrus_test_utils::init_tracing();
    let (_sui_cluster_handle, walrus_cluster, client, _) = test_cluster::E2eTestSetupBuilder::new()
        .with_epoch_duration(Duration::from_secs(20))
        .with_test_nodes_config(TestNodesConfig {
            node_weights: vec![1, 1],
            ..Default::default()
        })
        .build()
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
    walrus_test_utils::init_tracing();

    let (_sui_cluster_handle, _cluster, client, _) =
        test_cluster::E2eTestSetupBuilder::new().build().await?;

    let mut blob_object_ids = vec![];
    for idx in 0..N_BLOBS {
        let blobs = walrus_test_utils::random_data_list(314 + idx, 1);
        let store_args = StoreArgs::default_with_epochs(1).no_store_optimizations();
        let result = client
            .as_ref()
            .reserve_and_store_blobs(blobs, &store_args)
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
    walrus_test_utils::init_tracing();
    let (_sui_cluster_handle, _cluster, client, _) =
        test_cluster::E2eTestSetupBuilder::new().build().await?;

    let current_epoch = client.as_ref().sui_client().current_epoch().await?;
    let blobs = walrus_test_utils::random_data_list(314, 1);

    let store_args = StoreArgs::default_with_epochs(1).no_store_optimizations();

    let result = client
        .as_ref()
        .reserve_and_store_blobs(blobs.clone(), &store_args)
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
        .retriable_sui_client()
        .get_sui_object(blob_object_id)
        .await?;
    assert_eq!(extended_blob_object.storage.end_epoch, end_epoch + 5);

    // Store it again with a longer lifetime, should extend it correctly.
    let store_args = StoreArgs::default_with_epochs(20);
    let result = client
        .as_ref()
        .reserve_and_store_blobs(blobs, &store_args)
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
    const EXTEND_EPOCHS: EpochCount = 10;
    const INITIAL_FUNDS: u64 = 1000000000;
    walrus_test_utils::init_tracing();

    let (_sui_cluster_handle, _cluster, client, _) =
        test_cluster::E2eTestSetupBuilder::new().build().await?;

    let blobs = walrus_test_utils::random_data_list(314, 1);
    let store_args = StoreArgs::default_with_epochs(1).no_store_optimizations();
    let result = client
        .as_ref()
        .reserve_and_store_blobs(blobs, &store_args)
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
        .retriable_sui_client()
        .get_sui_object(shared_blob_object_id)
        .await?;
    assert_eq!(shared_blob.funds, 0);

    // Fund the shared blob.
    client
        .as_ref()
        .sui_client()
        .fund_shared_blob(shared_blob_object_id, INITIAL_FUNDS)
        .await?;
    let shared_blob: SharedBlob = client
        .as_ref()
        .sui_client()
        .retriable_sui_client()
        .get_sui_object(shared_blob_object_id)
        .await?;
    assert_eq!(shared_blob.funds, INITIAL_FUNDS);

    // Extend the shared blob.
    client
        .as_ref()
        .sui_client()
        .extend_shared_blob(shared_blob_object_id, EXTEND_EPOCHS)
        .await?;
    let shared_blob: SharedBlob = client
        .as_ref()
        .sui_client()
        .retriable_sui_client()
        .get_sui_object(shared_blob_object_id)
        .await?;
    assert_eq!(
        shared_blob.blob.storage.end_epoch,
        end_epoch + EXTEND_EPOCHS
    );
    assert_eq!(shared_blob.funds, INITIAL_FUNDS - 50);

    // Read the blob object with attributes from the shared blob object ID.
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
    walrus_test_utils::init_tracing();
    let (_sui_cluster_handle, _cluster, client, _) =
        test_cluster::E2eTestSetupBuilder::new().build().await?;
    let target_address: SuiAddress = SuiAddress::from_bytes(TARGET_ADDRESS).expect("valid address");

    let blobs = walrus_test_utils::random_data_list(314, 4);
    let store_args = StoreArgs::default_with_epochs(1)
        .no_store_optimizations()
        .with_post_store(post_store);
    let results = client
        .as_ref()
        .reserve_and_store_blobs_retry_committees(blobs, vec![], &store_args)
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
        println!("test_post_store_action result: {result:?}");
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
                        .retriable_sui_client()
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

/// Tests storing a blob with random initial blob attributes.
#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_store_blob_with_random_attributes() -> TestResult {
    walrus_test_utils::init_tracing();

    let (_sui_cluster_handle, _cluster, client, _) =
        test_cluster::E2eTestSetupBuilder::new().build().await?;

    let num_attributes = thread_rng().gen_range(0..=10);
    let mut attribute = BlobAttribute::default();
    for i in 0..num_attributes {
        let key = format!("test_key_{i}");
        let value = hex::encode(walrus_test_utils::random_data(64));
        attribute.insert(key, value);
    }

    let blobs = walrus_test_utils::random_data_list(1024, 1);
    let attributes = vec![attribute.clone()];

    // Store the blob with attributes.
    let store_args = StoreArgs::default_with_epochs(2).no_store_optimizations();
    let results = client
        .as_ref()
        .reserve_and_store_blobs_retry_committees(blobs, attributes, &store_args)
        .await?;

    assert_eq!(results.len(), 1);
    let store_result = &results[0];

    let blob_object = match store_result {
        BlobStoreResult::NewlyCreated { blob_object, .. } => blob_object,
        _ => panic!("Expected newly created blob"),
    };

    // Verify the blob was stored with attributes.
    let blob_with_attribute = client
        .as_ref()
        .get_blob_by_object_id(&blob_object.id)
        .await?;

    // Check that the attributes were stored correctly.
    if num_attributes > 0 {
        let stored_attribute = blob_with_attribute.attribute.unwrap();
        assert_eq!(stored_attribute, attribute);
    } else {
        assert!(blob_with_attribute.attribute.is_none());
    }

    tracing::info!(
        "Successfully stored blob with {} random attributes",
        num_attributes
    );

    Ok(())
}

/// A toolkit for blob attribute tests.
struct BlobAttributeTestContext<'a> {
    pub client: &'a mut WithTempDir<WalrusNodeClient<SuiContractClient>>,
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
    pub async fn new(
        client: &'a mut WithTempDir<WalrusNodeClient<SuiContractClient>>,
    ) -> TestResult<Self> {
        let blobs_to_create = 2;
        let blobs = walrus_test_utils::random_data_list(314, 1);

        // Store multiple copies of the same blob with different end times.
        for idx in 1..blobs_to_create + 1 {
            let store_args = StoreArgs::default_with_epochs(idx).no_store_optimizations();
            client
                .as_mut()
                .reserve_and_store_blobs(blobs.clone(), &store_args)
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
    walrus_test_utils::init_tracing();

    let (_sui_cluster_handle, _cluster, mut client, _) =
        test_cluster::E2eTestSetupBuilder::new().build().await?;
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
    walrus_test_utils::init_tracing();

    let (_sui_cluster_handle, _cluster, mut client, _) =
        test_cluster::E2eTestSetupBuilder::new().build().await?;
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
    walrus_test_utils::init_tracing();
    let (_sui_cluster_handle, walrus_cluster, client, _) = test_cluster::E2eTestSetupBuilder::new()
        .with_epoch_duration(Duration::from_secs(20))
        .with_test_nodes_config(TestNodesConfig {
            node_weights: vec![1, 1],
            ..Default::default()
        })
        .build()
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
    let (_sui_cluster_handle, cluster, client, _) =
        test_cluster::E2eTestSetupBuilder::new().build().await?;

    // Create an atomic counter to track number of failure attempts
    let failure_counter = std::sync::Arc::new(AtomicU32::new(0));
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

/// Tests the select_coins function on the retriable sui client works as expected when dealing
/// with a large number of coins.
#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
pub async fn test_select_coins_max_objects() -> TestResult {
    walrus_test_utils::init_tracing();
    let (sui_cluster_handle, _, _, _) = test_cluster::E2eTestSetupBuilder::new().build().await?;

    // Create a new wallet on the cluster.
    let mut cluster_wallet = walrus_sui::config::load_wallet_context_from_path(
        Some(
            sui_cluster_handle
                .lock()
                .await
                .wallet_path()
                .await
                .as_path(),
        ),
        None,
    )?;
    let env = cluster_wallet.get_active_env()?.to_owned();
    let mut wallet = test_utils::temp_dir_wallet(None, env).await?;

    let sui = |sui: u64| sui * 1_000_000_000;

    // Add 4 coins with 1 SUI each to the wallet.
    let address = wallet.as_mut().active_address()?;
    walrus_sui::test_utils::fund_addresses(&mut cluster_wallet, vec![address; 4], Some(sui(1)))
        .await?;

    let rpc_urls = &[wallet.as_ref().get_rpc_url().unwrap()];

    // Create a new client with the funded wallet.
    let retry_client = RetriableSuiClient::new(
        rpc_urls
            .iter()
            .map(|rpc_url| LazySuiClientBuilder::new(rpc_url, None))
            .collect(),
        ExponentialBackoffConfig::default(),
    )?;

    let balance = retry_client.get_balance(address, None).await?;
    assert_eq!(balance.total_balance, u128::from(sui(4)));

    // The maximum number of coins that can be selected to reach the amount.
    let max_num_coins = 2;

    let result = retry_client
        .select_coins_with_limit(address, None, sui(1).into(), vec![], max_num_coins)
        .await;
    assert!(result.is_ok(), "1 SUI can be constructed with <= 2 coins");

    let result = retry_client
        .select_coins_with_limit(address, None, sui(3).into(), vec![], max_num_coins)
        .await;
    if let Err(error) = result {
        assert!(
            matches!(error, SuiClientError::InsufficientFundsWithMaxCoins(_)),
            "expected InsufficientFundsWithMaxCoins error"
        );
    } else {
        panic!("3 SUI cannot be achieved with 2 coins, but the wallet has 4 SUI");
    }

    let result = retry_client
        .select_coins_with_limit(address, None, sui(5).into(), vec![], max_num_coins)
        .await;
    if let Err(error) = result {
        assert!(
            matches!(error, SuiClientError::SuiSdkError(_)),
            "expected SuiSdkError error"
        );
    } else {
        panic!("the wallet does not have 5 SUI");
    }

    Ok(())
}

#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_store_with_upload_relay_no_tip() {
    walrus_test_utils::init_tracing();

    // Start the Sui and Walrus clusters.
    let (sui_cluster_handle, _cluster, cluster_client, _) =
        test_cluster::E2eTestSetupBuilder::new()
            .build()
            .await
            .expect("setup should succeed");

    // Get the cluster wallet so we can fund the client wallet.
    let cluster_wallet_path = sui_cluster_handle.lock().await.wallet_path().await;
    let mut cluster_wallet = walrus_sui::config::load_wallet_context_from_path(
        Some(cluster_wallet_path.as_path()),
        None,
    )
    .expect("loading cluster wallet should succeed");

    let mut relay_wallet = wallet_for_testing(&mut cluster_wallet, false)
        .await
        .expect("wallet creation should succeed");
    fund_addresses(
        &mut cluster_wallet,
        vec![
            relay_wallet
                .inner
                .active_address()
                .expect("relay wallet active address should exist"),
        ],
        Some(10_000_000_000),
    )
    .await
    .expect("funding wallet should succeed");

    // Create the Walrus config for the upload relay.
    let cluster_config = cluster_client.inner.config();
    let walrus_client_config = ClientConfig {
        wallet_config: Some(WalletConfig::from_path(cluster_wallet_path)),
        rpc_urls: vec![
            sui_cluster_handle
                .lock()
                .await
                .cluster()
                .rpc_url()
                .to_string(),
        ],
        ..cluster_config.clone()
    };
    let server_address = UnusedSocketAddress::AllInterfaces {
        force_time_wait: false,
    }
    .try_into()
    .expect("get unused socket address");

    let registry = Registry::default();

    let upload_relay_sui_client = get_client_with_config(walrus_client_config, &registry)
        .await
        .expect("create upload relay sui client");
    let upload_relay_handle: UploadRelayHandle = walrus_upload_relay::start_upload_relay(
        upload_relay_sui_client,
        WalrusUploadRelayConfig {
            tip_config: TipConfig::NoTip,
            tx_freshness_threshold: Duration::from_mins(5),
            tx_max_future_threshold: Duration::from_secs(10),
        },
        server_address,
        registry,
    )
    .expect("start upload relay should succeed");

    upload_relay_handle
        .wait_for_tcp_bind()
        .await
        .expect("wait for TCP bind");

    let n_shards = cluster_client.inner.encoding_config().n_shards();
    let upload_relay_url = get_upload_relay_url(&server_address);
    let upload_relay_client = UploadRelayClient::new(
        relay_wallet
            .inner
            .active_address()
            .expect("client wallet active address should exist"),
        n_shards,
        upload_relay_url,
        None,
        Default::default(),
    )
    .await
    .expect("upload relay client creation should succeed");
    match basic_store_and_read(&cluster_client, 1, 40000, Some(upload_relay_client), || {
        Ok(())
    })
    .await
    {
        Ok(_) => {}
        Err(error) => {
            panic!("store and read with upload relay should succeed: {error}")
        }
    };
    upload_relay_handle
        .shutdown()
        .await
        .expect("shutdown upload relay");
}

#[cfg(msim)]
fn get_upload_relay_url(server_address: &SocketAddr) -> Url {
    format!(
        "http://1.1.1.1:{server_port}",
        server_port = server_address.port()
    )
    .parse()
    .expect("valid URL")
}

#[cfg(not(msim))]
fn get_upload_relay_url(server_address: &SocketAddr) -> Url {
    format!(
        "http://127.0.0.1:{server_port}",
        server_port = server_address.port()
    )
    .parse()
    .expect("valid URL")
}

#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_store_with_upload_relay_with_tip() {
    walrus_test_utils::init_tracing();
    walrus_test_utils::init_tracing();

    // Start the Sui and Walrus clusters.
    let (sui_cluster_handle, _cluster, cluster_client, _system_context) =
        test_cluster::E2eTestSetupBuilder::new()
            .build()
            .await
            .expect("setup should succeed");

    // Get the cluster wallet so we can fund the client wallet.
    let cluster_wallet_path = sui_cluster_handle.lock().await.wallet_path().await;
    let mut cluster_wallet = walrus_sui::config::load_wallet_context_from_path(
        Some(cluster_wallet_path.as_path()),
        None,
    )
    .expect("loading cluster wallet should succeed");

    let mut relay_wallet = wallet_for_testing(&mut cluster_wallet, false)
        .await
        .expect("wallet creation should succeed");

    let relay_address = relay_wallet
        .inner
        .active_address()
        .expect("relay wallet active address should exist");

    // Create the Walrus config for the upload relay.
    let walrus_read_client_config = ClientConfig {
        wallet_config: None,
        rpc_urls: vec![
            sui_cluster_handle
                .lock()
                .await
                .cluster()
                .rpc_url()
                .to_string(),
        ],
        ..cluster_client.inner.config().clone()
    };

    let server_address = UnusedSocketAddress::AllInterfaces {
        force_time_wait: false,
    }
    .try_into()
    .expect("get unused socket address");

    const TIP_BASE: u64 = 1000;
    const TIP_MULTIPLIER: u64 = 100;

    let registry = Registry::default();

    let upload_relay_sui_client = get_client_with_config(walrus_read_client_config, &registry)
        .await
        .expect("create upload relay sui client");
    let upload_relay_handle = walrus_upload_relay::start_upload_relay(
        upload_relay_sui_client,
        WalrusUploadRelayConfig {
            tip_config: TipConfig::SendTip {
                address: relay_address,
                kind: TipKind::Linear {
                    base: TIP_BASE,
                    encoded_size_mul_per_kib: TIP_MULTIPLIER,
                },
            },
            tx_freshness_threshold: Duration::from_mins(5),
            tx_max_future_threshold: Duration::from_secs(10),
        },
        server_address,
        registry,
    )
    .expect("start upload relay should succeed");

    upload_relay_handle
        .wait_for_tcp_bind()
        .await
        .expect("wait for TCP bind");

    assert_ne!(
        cluster_wallet
            .active_address()
            .expect("cluster_wallet should have an address"),
        cluster_client.inner.sui_client().address()
    );
    let n_shards = cluster_client.inner.encoding_config().n_shards();
    let upload_relay_url = get_upload_relay_url(&server_address);
    let upload_relay_client = UploadRelayClient::new(
        cluster_client.inner.sui_client().address(),
        n_shards,
        upload_relay_url,
        None,
        Default::default(),
    )
    .await
    .expect("upload relay client creation should succeed");

    // Create a retry client to check balances
    let retry_client = {
        let rpc_url = sui_cluster_handle
            .lock()
            .await
            .cluster()
            .rpc_url()
            .to_string();
        RetriableSuiClient::new(
            vec![LazySuiClientBuilder::new(&rpc_url, None)],
            ExponentialBackoffConfig::default(),
        )
        .expect("create retry client")
    };

    // Get initial balance of relay wallet to verify tip payment
    let initial_relay_balance = retry_client
        .get_balance(relay_address, None)
        .await
        .expect("get balance")
        .total_balance;

    const BLOB_SIZE: usize = 40000;
    match basic_store_and_read(
        &cluster_client,
        1,
        BLOB_SIZE,
        Some(upload_relay_client),
        || Ok(()),
    )
    .await
    {
        Ok(_) => {}
        Err(error) => {
            panic!("store and read with upload relay should succeed: {error}")
        }
    };

    // Verify that the relay wallet received a tip
    let final_relay_balance = retry_client
        .get_balance(relay_address, None)
        .await
        .expect("get balance")
        .total_balance;

    tracing::info!(
        "Relay address balance - Initial: {initial_relay_balance}, Final: {final_relay_balance}",
    );

    assert!(
        final_relay_balance > initial_relay_balance,
        "Relay wallet should have received a tip. Initial: {initial_relay_balance}, \
        Final: {final_relay_balance}",
    );

    // Calculate expected tip based on the linear formula.
    let encoded_blob_size =
        encoded_blob_length_for_n_shards(n_shards, BLOB_SIZE as u64, EncodingType::RS2)
            .expect("encoded blob size should be valid");

    let expected_tip_lower_bound =
        u128::from(TIP_BASE + encoded_blob_size.div_ceil(1024) * TIP_MULTIPLIER);
    let actual_tip = final_relay_balance - initial_relay_balance;

    tracing::info!(
        "Tip paid: {} (expected at least: {})",
        actual_tip,
        expected_tip_lower_bound
    );

    assert_eq!(
        actual_tip, expected_tip_lower_bound,
        "Tip should be the calculated minimum. Actual: {actual_tip}, \
        Expected minimum: {expected_tip_lower_bound}",
    );

    upload_relay_handle
        .shutdown()
        .await
        .expect("shutdown upload relay");
}

/// Tests client byte range read functionality.
#[ignore = "ignore E2E tests by default"]
#[walrus_simtest]
async fn test_byte_range_read_client() -> TestResult {
    walrus_test_utils::init_tracing();

    let test_nodes_config = TestNodesConfig {
        node_weights: vec![7, 7, 7, 7, 7, 7, 7, 7, 7, 7],
        ..Default::default()
    };
    let test_cluster_builder =
        test_cluster::E2eTestSetupBuilder::new().with_test_nodes_config(test_nodes_config);
    let (_sui_cluster_handle, _cluster, client, _) = test_cluster_builder.build().await?;
    let client = client.as_ref();

    // Generate a non zero blob size.
    let blob_size: u64 = thread_rng().gen_range(1..=1000000);
    tracing::info!("blob size: {blob_size}");
    let blobs = walrus_test_utils::random_data_list(
        usize::try_from(blob_size).expect("blob size should be valid"),
        1,
    );

    // Store the blob on walrus.
    let blob_read_result = client
        .reserve_and_store_blobs(
            blobs.clone(),
            &StoreArgs::default_with_epochs(5).with_encoding_type(DEFAULT_ENCODING),
        )
        .await?;

    let blob_id = blob_read_result[0]
        .blob_id()
        .expect("blob ID should be present");

    // Test reading byte range from the blob. We do 20 randomly generated byte range reads and
    // verify that the data is correct.
    for _ in 0..100 {
        // Generate a random byte range.
        let start_byte = random::<u64>() % blob_size;
        let byte_length = random::<u64>() % (blob_size - start_byte) + 1;
        tracing::info!(
            "reading byte range {start_byte}-{end_byte} of blob {blob_id}",
            start_byte = start_byte,
            end_byte = start_byte + byte_length,
            blob_id = blob_id
        );
        let byte_range_data = client
            .byte_range_read_client()
            .read_byte_range(&blob_id, start_byte, byte_length)
            .await?;
        assert_eq!(
            byte_range_data.data,
            blobs[0][usize::try_from(start_byte).expect("start byte should be valid")
                ..usize::try_from(start_byte + byte_length).expect("end byte should be valid")]
                .to_vec()
        );
        assert_eq!(byte_range_data.unencoded_blob_size, blobs[0].len() as u64);
    }

    Ok(())
}
