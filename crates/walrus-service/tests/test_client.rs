// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{num::NonZeroU16, sync::OnceLock, time::Duration};

use anyhow::anyhow;
use sui_types::{base_types::ObjectID, digests::TransactionDigest, event::EventID};
use tokio::sync::Mutex;
use walrus_core::{
    encoding::{EncodingConfig, Primary},
    BlobId,
    EncodingType,
};
use walrus_service::{
    client::{Client, Config},
    test_utils::TestCluster,
};
use walrus_sui::{
    test_utils::{MockContractClient, MockSuiReadClient},
    types::{BlobCertified, BlobRegistered},
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

    let blob = walrus_test_utils::random_data(31415);

    // The default `TestCluster` has 13 shards.
    let encoding_config = EncodingConfig::new(NonZeroU16::new(13).unwrap());

    let config = Config {
        concurrent_requests: 10,
        connection_timeout: Duration::from_secs(10),
        system_pkg: ObjectID::random(),
        system_object: ObjectID::random(),
        wallet_config: None,
    };

    let blob_id = encoding_config
        .get_blob_encoder(&blob)?
        .compute_metadata()
        .blob_id()
        .to_owned();

    let cluster_builder = TestCluster::builder().with_system_event_providers(vec![
        blob_registered_event(blob_id).into(),
        blob_certified_event(blob_id).into(),
    ]);
    let mut cluster = {
        // Lock to avoid race conditions.
        let _lock = global_test_lock().lock().await;
        cluster_builder.build().await?
    };

    let sui_contract_client = MockContractClient::new(
        0,
        MockSuiReadClient::new_with_blob_ids([blob_id], Some(cluster.committee()?)),
    );
    let client = Client::new(config, sui_contract_client).await?;

    // Stop the nodes in the failure set.
    failed_nodes
        .iter()
        .for_each(|&idx| cluster.cancel_node(idx));

    // Store a blob and get confirmations from each node.
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

fn blob_registered_event(blob_id: BlobId) -> BlobRegistered {
    BlobRegistered {
        epoch: 0,
        blob_id,
        size: 10000,
        erasure_code_type: EncodingType::RedStuff,
        end_epoch: 42,
        event_id: EventID {
            tx_digest: TransactionDigest::random(),
            event_seq: 0,
        },
    }
}

fn blob_certified_event(blob_id: BlobId) -> BlobCertified {
    BlobCertified {
        epoch: 0,
        blob_id,
        end_epoch: 42,
        event_id: EventID {
            tx_digest: TransactionDigest::random(),
            event_seq: 0,
        },
    }
}
