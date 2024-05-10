// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::num::{NonZeroU16, NonZeroU64};

use anyhow::bail;
use fastcrypto::traits::ToFromBytes;
use tokio_stream::StreamExt;
use walrus_core::{encoding::EncodingConfig, merkle::Node, BlobId, EncodingType, ShardIndex};
use walrus_sui::{
    client::{ContractClient, ReadClient, SuiContractClient},
    test_utils::{
        get_default_blob_certificate,
        get_default_invalid_certificate,
        sui_test_cluster,
        system_setup::publish_with_default_system,
    },
    types::{BlobEvent, EpochStatus},
};

#[tokio::test]
#[ignore = "ignore integration tests by default"]
async fn test_register_certify_blob() -> anyhow::Result<()> {
    _ = tracing_subscriber::fmt::try_init();
    let test_cluster = sui_test_cluster().await;
    let mut wallet = test_cluster.wallet;
    let (package_id, system_object) = publish_with_default_system(&mut wallet).await?;
    let walrus_client =
        SuiContractClient::new(wallet, package_id, system_object, 10000000000).await?;

    // used to calculate the encoded size of the blob
    let encoding_config = EncodingConfig::new(NonZeroU16::new(100).unwrap());

    // Get event streams for the events
    let polling_duration = std::time::Duration::from_millis(50);
    let mut events = walrus_client
        .read_client
        .blob_events(polling_duration, None)
        .await?;

    let size = 10_000;
    let resource_size = encoding_config.encoded_blob_length(size).unwrap();
    let storage_resource = walrus_client.reserve_space(resource_size, 3).await?;
    assert_eq!(storage_resource.start_epoch, 0);
    assert_eq!(storage_resource.end_epoch, 3);
    assert_eq!(storage_resource.storage_size, resource_size);
    #[rustfmt::skip]
    let root_hash = [
        1, 2, 3, 4, 5, 6, 7, 8,
        1, 2, 3, 4, 5, 6, 7, 8,
        1, 2, 3, 4, 5, 6, 7, 8,
        1, 2, 3, 4, 5, 6, 7, 8,
    ];

    let blob_id = BlobId::from_metadata(
        Node::from(root_hash),
        EncodingType::RedStuff,
        NonZeroU64::new(size).unwrap(),
    );
    let blob_obj = walrus_client
        .register_blob(
            &storage_resource,
            blob_id,
            root_hash,
            size,
            EncodingType::RedStuff,
        )
        .await?;
    assert_eq!(blob_obj.blob_id, blob_id);
    assert_eq!(blob_obj.size, size);
    assert_eq!(blob_obj.certified, None);
    assert_eq!(blob_obj.storage, storage_resource);
    assert_eq!(blob_obj.stored_epoch, 0);

    // Make sure that we got the expected event
    let BlobEvent::Registered(blob_registered) = events.next().await.unwrap() else {
        bail!("unexpected event type");
    };
    assert_eq!(blob_registered.blob_id, blob_id);
    assert_eq!(blob_registered.epoch, blob_obj.stored_epoch);
    assert_eq!(
        blob_registered.erasure_code_type,
        blob_obj.erasure_code_type
    );
    assert_eq!(blob_registered.end_epoch, storage_resource.end_epoch);
    assert_eq!(blob_registered.size, blob_obj.size);

    let certificate = get_default_blob_certificate(blob_id, 0);

    // Values printed here should match move test `test_blob_certify_single_function`
    // Note: we keep these commented in case we ever need to recompute them to update tests.
    //
    // println!("certificate signature: {:?}", certificate.signature.as_bytes());
    // let bytes : Vec<u8> = certificate.confirmation.clone().into();
    // println!("certificate message: {:?}", bytes);

    let blob_obj = walrus_client.certify_blob(&blob_obj, &certificate).await?;
    assert_eq!(blob_obj.certified, Some(0));

    // Make sure that we got the expected event
    let BlobEvent::Certified(blob_certified) = events.next().await.unwrap() else {
        bail!("unexpected event type");
    };
    assert_eq!(blob_certified.blob_id, blob_id);
    assert_eq!(Some(blob_registered.epoch), blob_obj.certified);
    assert_eq!(blob_certified.end_epoch, storage_resource.end_epoch);

    // Drop event stream
    drop(events);
    // Get new event stream with cursors
    let mut events = walrus_client
        .read_client
        .blob_events(polling_duration, Some(blob_certified.event_id))
        .await?;

    // Now register and certify a blob with a different blob id again to check that
    // we receive the event
    let storage_resource = walrus_client.reserve_space(resource_size, 3).await?;
    #[rustfmt::skip]
    let root_hash = [
        1, 2, 3, 4, 5, 6, 7, 0,
        1, 2, 3, 4, 5, 6, 7, 0,
        1, 2, 3, 4, 5, 6, 7, 0,
        1, 2, 3, 4, 5, 6, 7, 0,
    ];
    let blob_id = BlobId::from_metadata(
        Node::from(root_hash),
        EncodingType::RedStuff,
        NonZeroU64::new(size).unwrap(),
    );

    let blob_obj = walrus_client
        .register_blob(
            &storage_resource,
            blob_id,
            root_hash,
            size,
            EncodingType::RedStuff,
        )
        .await?;

    // Make sure that we got the expected event
    let BlobEvent::Registered(blob_registered) = events.next().await.unwrap() else {
        bail!("unexpected event type");
    };
    assert_eq!(blob_registered.blob_id, blob_id);

    let _blob_obj = walrus_client
        .certify_blob(&blob_obj, &get_default_blob_certificate(blob_id, 0))
        .await?;

    // Make sure that we got the expected event
    let BlobEvent::Certified(blob_certified) = events.next().await.unwrap() else {
        bail!("unexpected event type");
    };
    assert_eq!(blob_certified.blob_id, blob_id);

    Ok(())
}

#[tokio::test]
#[ignore = "ignore integration tests by default"]
async fn test_invalidate_blob() -> anyhow::Result<()> {
    _ = tracing_subscriber::fmt::try_init();
    let test_cluster = sui_test_cluster().await;
    let mut wallet = test_cluster.wallet;
    let (package_id, system_object) = publish_with_default_system(&mut wallet).await?;
    let walrus_client =
        SuiContractClient::new(wallet, package_id, system_object, 10000000000).await?;

    // Get event streams for the events
    let polling_duration = std::time::Duration::from_millis(50);
    let mut events = walrus_client
        .read_client
        .blob_events(polling_duration, None)
        .await?;

    #[rustfmt::skip]
    let blob_id = BlobId([
        1, 2, 3, 4, 5, 6, 7, 8,
        1, 2, 3, 4, 5, 6, 7, 8,
        1, 2, 3, 4, 5, 6, 7, 8,
        1, 2, 3, 4, 5, 6, 7, 8,
    ]);

    let certificate = get_default_invalid_certificate(blob_id, 0);

    walrus_client.invalidate_blob_id(&certificate).await?;

    // Make sure that we got the expected event
    let BlobEvent::InvalidBlobID(invalid_blob_id) = events.next().await.unwrap() else {
        bail!("unexpected event type");
    };
    assert_eq!(invalid_blob_id.blob_id, blob_id);
    assert_eq!(invalid_blob_id.epoch, 0);
    Ok(())
}

#[tokio::test]
#[ignore = "ignore integration tests by default"]
async fn test_get_system() -> anyhow::Result<()> {
    let test_cluster = sui_test_cluster().await;
    let mut wallet = test_cluster.wallet;
    let (package_id, system_object) = publish_with_default_system(&mut wallet).await?;
    let walrus_client =
        SuiContractClient::new(wallet, package_id, system_object, 10000000000).await?;
    let system = walrus_client.read_client.get_system_object().await?;
    assert_eq!(system.epoch_status, EpochStatus::Done);
    assert_eq!(system.price_per_unit_size, 10);
    assert_eq!(system.total_capacity_size, 1000000000);
    assert_eq!(system.used_capacity_size, 0);
    let committee = walrus_client.read_client.current_committee().await?;
    assert_eq!(system.current_committee, committee);
    assert_eq!(committee.epoch, 0);
    assert_eq!(committee.n_shards().get(), 10);
    assert_eq!(committee.members().len(), 1);
    let storage_node = &committee.members()[0];
    assert_eq!(storage_node.name, "Test0");
    assert_eq!(storage_node.network_address.to_string(), "127.0.0.1:8080");
    assert_eq!(
        storage_node.shard_ids,
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
            .into_iter()
            .map(ShardIndex)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        storage_node.public_key.as_bytes(),
        [
            149, 234, 204, 58, 220, 9, 200, 39, 89, 63, 88, 30, 142, 45, 224, 104, 191, 76, 245,
            208, 192, 235, 41, 229, 55, 47, 13, 35, 54, 71, 136, 238, 15, 155, 235, 17, 44, 138,
            126, 156, 47, 12, 114, 4, 51, 112, 92, 240
        ]
    );
    Ok(())
}
