// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{num::NonZeroU16, time::Duration};

use anyhow::Context;
use sui_types::{base_types::ObjectID, digests::TransactionDigest, event::EventID};
use walrus_core::{
    encoding::{EncodingConfig, Primary},
    BlobId,
    EncodingType,
    ShardIndex,
};
use walrus_service::{
    client::{Client, Config},
    test_utils::TestCluster,
};
use walrus_sui::{
    test_utils::{MockContractClient, MockSuiReadClient},
    types::{BlobCertified, BlobRegistered, Committee, StorageNode as SuiStorageNode},
};
use walrus_test_utils::Result as TestResult;

#[tokio::test]
#[ignore = "ignore E2E tests by default"]
async fn test_store_and_read_blob() -> TestResult {
    let _ = tracing_subscriber::fmt::try_init();

    let encoding_config = EncodingConfig::new(NonZeroU16::new(10).unwrap());

    let config = Config {
        concurrent_requests: 10,
        connection_timeout: Duration::from_secs(10),
        system_pkg: ObjectID::random(),
        system_object: ObjectID::random(),
        wallet_config: None,
    };

    let blob = walrus_test_utils::random_data(31415);
    let blob_id = encoding_config
        .get_blob_encoder(&blob)?
        .compute_metadata()
        .blob_id()
        .to_owned();

    let assignment: Vec<&[u16]> = vec![&[0, 1], &[2, 3], &[4, 5, 6], &[7, 8, 9]];
    let cluster = TestCluster::builder()
        .with_shard_assignment(&assignment)
        .with_system_event_providers(vec![
            blob_registered_event(blob_id).into(),
            blob_certified_event(blob_id).into(),
        ])
        .build()
        .await?;

    let members = cluster
        .nodes
        .iter()
        .zip(assignment)
        .enumerate()
        .map(|(i, (node, shard_ids))| SuiStorageNode {
            name: format!("node-{i}"),
            network_address: node.rest_api_address.into(),
            public_key: node.public_key.clone(),
            shard_ids: shard_ids.iter().map(ShardIndex::from).collect(),
        })
        .collect();
    let committee = Committee::new(members, 0)?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let sui_contract_client = MockContractClient::new(
        0,
        MockSuiReadClient::new_with_blob_ids([blob_id], Some(committee)),
    );
    let client = Client::new(config, sui_contract_client).await?;

    // Store a blob and get confirmations from each node.
    let blob_confirmation = client
        .reserve_and_store_blob(&blob, 1)
        .await
        .context("unable to reserve and store blob")?;

    // Read the blob.
    let read_blob = client
        .read_blob::<Primary>(&blob_confirmation.blob_id)
        .await?;

    assert_eq!(read_blob, blob);

    Ok(())
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
