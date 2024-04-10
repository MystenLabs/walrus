// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use sui_types::{digests::TransactionDigest, event::EventID};
use walrus_core::{
    encoding::{EncodingConfig, Primary, Secondary},
    BlobId,
    EncodingType,
    ShardIndex,
};
use walrus_service::{
    client::{Client, Config},
    test_utils::TestCluster,
};
use walrus_sui::types::{BlobCertified, BlobRegistered, Committee, StorageNode as SuiStorageNode};

#[tokio::test]
#[ignore = "ignore E2E tests by default"]
async fn test_store_and_read_blob() {
    let encoding_config = EncodingConfig::new(2, 4, 10);

    let mut config = Config {
        committee: Committee {
            members: vec![],
            epoch: 0,
            total_weight: encoding_config.n_shards().into(),
        },
        source_symbols_primary: encoding_config.n_source_symbols::<Primary>(),
        source_symbols_secondary: encoding_config.n_source_symbols::<Secondary>(),
        concurrent_requests: 10,
        connection_timeout: Duration::from_secs(10),
    };

    let blob = walrus_test_utils::random_data(31415);
    let blob_id = encoding_config
        .get_blob_encoder(&blob)
        .unwrap()
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
        .build(encoding_config)
        .await
        .expect("cluster construction must succeed");

    config.committee.members = cluster
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
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = Client::new(config).unwrap();

    // Store a blob and get confirmations from each node.
    let (metadata, confirmation) = client.store_blob(&blob).await.unwrap();
    assert!(confirmation.len() == 4);

    // Read the blob.
    let read_blob = client
        .read_blob::<Primary>(metadata.blob_id())
        .await
        .unwrap();
    assert_eq!(read_blob, blob);
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
