// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{num::NonZeroU16, str::FromStr, sync::Arc};

use anyhow::bail;
use fastcrypto::{bls12381::min_pk::BLS12381PublicKey, traits::ToFromBytes};
use rand::{rngs::StdRng, SeedableRng};
use tokio_stream::StreamExt;
use walrus_core::{
    encoding::EncodingConfig,
    keys::NetworkKeyPair,
    merkle::Node,
    BlobId,
    EncodingType,
    ShardIndex,
};
use walrus_sui::{
    client::{ContractClient, ReadClient, SuiContractClient},
    test_utils::{
        self,
        get_default_blob_certificate,
        get_default_invalid_certificate,
        new_wallet_on_sui_test_cluster,
        system_setup::{create_and_init_system, end_epoch_zero, register_committee_and_stake},
        TestClusterHandle,
    },
    types::{ContractEvent, NetworkAddress, NodeRegistrationParams, StorageNodeCap},
};
use walrus_test_utils::WithTempDir;

const GAS_BUDGET: u64 = 1_000_000_000;

async fn initialize_contract_and_wallet() -> anyhow::Result<(
    Arc<TestClusterHandle>,
    WithTempDir<SuiContractClient>,
    WithTempDir<SuiContractClient>,
    StorageNodeCap,
)> {
    #[cfg(not(msim))]
    let sui_cluster = test_utils::using_tokio::global_sui_test_cluster();
    #[cfg(msim)]
    let sui_cluster = test_utils::using_msim::global_sui_test_cluster().await;

    // Get a wallet on the global sui test cluster
    let mut admin_wallet = new_wallet_on_sui_test_cluster(sui_cluster.clone()).await?;
    let node_wallet = new_wallet_on_sui_test_cluster(sui_cluster.clone()).await?;
    let system_context = create_and_init_system(&mut admin_wallet.inner, 100, 0).await?;

    // Set up node params.
    // Pk corresponding to secret key scalar(117)
    let network_key_pair = NetworkKeyPair::generate_with_rng(&mut StdRng::seed_from_u64(0));
    let pubkey_bytes = [
        149, 234, 204, 58, 220, 9, 200, 39, 89, 63, 88, 30, 142, 45, 224, 104, 191, 76, 245, 208,
        192, 235, 41, 229, 55, 47, 13, 35, 54, 71, 136, 238, 15, 155, 235, 17, 44, 138, 126, 156,
        47, 12, 114, 4, 51, 112, 92, 240,
    ];

    let storage_node_params = NodeRegistrationParams {
        name: "Test0".to_owned(),
        network_address: NetworkAddress::from_str("127.0.0.1:8080")?,
        public_key: BLS12381PublicKey::from_bytes(&pubkey_bytes)?,
        network_public_key: network_key_pair.public().clone(),
        commission_rate: 0,
        storage_price: 5,
        write_price: 1,
        node_capacity: 1_000_000_000_000,
    };

    let node_contract_client = node_wallet
        .and_then_async(|wallet| {
            SuiContractClient::new(
                wallet,
                system_context.system_obj_id,
                system_context.staking_obj_id,
                GAS_BUDGET,
            )
        })
        .await?;

    let storage_node_cap = register_committee_and_stake(
        &mut admin_wallet.inner,
        &system_context,
        &[storage_node_params],
        &[&node_contract_client.inner],
        &[1_000_000_000],
    )
    .await?
    .first()
    .expect("Should get storage node cap")
    .clone();

    // call vote end
    end_epoch_zero(&node_contract_client.inner).await?;

    Ok((
        sui_cluster,
        admin_wallet
            .and_then_async(|wallet| {
                SuiContractClient::new(
                    wallet,
                    system_context.system_obj_id,
                    system_context.staking_obj_id,
                    GAS_BUDGET,
                )
            })
            .await?,
        node_contract_client,
        storage_node_cap,
    ))
}

#[tokio::test]
#[ignore = "ignore E2E tests by default"]
async fn test_initialize_contract() -> anyhow::Result<()> {
    _ = tracing_subscriber::fmt::try_init();
    initialize_contract_and_wallet().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "ignore integration tests by default"]
async fn test_register_certify_blob() -> anyhow::Result<()> {
    _ = tracing_subscriber::fmt::try_init();

    let (_sui_cluster_handle, walrus_client, ..) = initialize_contract_and_wallet().await?;

    // used to calculate the encoded size of the blob
    let encoding_config = EncodingConfig::new(NonZeroU16::new(100).unwrap());

    // Get event streams for the events
    let polling_duration = std::time::Duration::from_millis(50);
    let mut events = walrus_client
        .as_ref()
        .read_client
        .event_stream(polling_duration, None)
        .await?;

    let size = 10_000;
    let resource_size = encoding_config.encoded_blob_length(size).unwrap();
    let storage_resource = walrus_client
        .as_ref()
        .reserve_space(resource_size, 3)
        .await?;
    assert_eq!(storage_resource.start_epoch, 1);
    assert_eq!(storage_resource.end_epoch, 4);
    assert_eq!(storage_resource.storage_size, resource_size);

    #[rustfmt::skip]
    let root_hash = [
        1, 2, 3, 4, 5, 6, 7, 8,
        1, 2, 3, 4, 5, 6, 7, 8,
        1, 2, 3, 4, 5, 6, 7, 8,
        1, 2, 3, 4, 5, 6, 7, 8,
    ];

    let blob_id = BlobId::from_metadata(Node::from(root_hash), EncodingType::RedStuff, size);
    let blob_obj = walrus_client
        .as_ref()
        .register_blob(
            &storage_resource,
            blob_id,
            root_hash,
            size,
            EncodingType::RedStuff,
            false,
        )
        .await?;
    assert_eq!(blob_obj.blob_id, blob_id);
    assert_eq!(blob_obj.size, size);
    assert_eq!(blob_obj.certified_epoch, None);
    assert_eq!(blob_obj.storage, storage_resource);
    assert_eq!(blob_obj.registered_epoch, 1);
    assert!(!blob_obj.deletable);

    // Make sure that we got the expected event
    let ContractEvent::EpochParametersSelected(_) = events.next().await.unwrap() else {
        bail!("unexpected event type. expecting EpochParametersSelected");
    };
    let ContractEvent::EpochChangeStart(_) = events.next().await.unwrap() else {
        bail!("unexpected event type. expecting EpochChangeStart");
    };
    let ContractEvent::BlobRegistered(blob_registered) = events.next().await.unwrap() else {
        bail!("unexpected event type. expecting BlobRegistered");
    };
    assert_eq!(blob_registered.blob_id, blob_id);
    assert_eq!(blob_registered.epoch, blob_obj.registered_epoch);
    assert_eq!(blob_registered.encoding_type, blob_obj.encoding_type);
    assert_eq!(blob_registered.end_epoch, storage_resource.end_epoch);
    assert_eq!(blob_registered.size, blob_obj.size);

    let certificate = get_default_blob_certificate(blob_id, 1);

    let blob_obj = walrus_client
        .as_ref()
        .certify_blob(blob_obj, &certificate)
        .await?;
    assert_eq!(blob_obj.certified_epoch, Some(1));

    // Make sure that we got the expected event
    let ContractEvent::BlobCertified(blob_certified) = events.next().await.unwrap() else {
        bail!("unexpected event type. expecting BlobCertified");
    };
    assert_eq!(blob_certified.blob_id, blob_id);
    assert_eq!(Some(blob_registered.epoch), blob_obj.certified_epoch);
    assert_eq!(blob_certified.end_epoch, storage_resource.end_epoch);

    // Drop event stream
    drop(events);
    // Get new event stream with cursors
    let mut events = walrus_client
        .as_ref()
        .read_client
        .event_stream(polling_duration, Some(blob_certified.event_id))
        .await?;

    // Now register and certify a blob with a different blob id again to check that
    // we receive the event
    let storage_resource = walrus_client
        .as_ref()
        .reserve_space(resource_size, 3)
        .await?;
    #[rustfmt::skip]
    let root_hash = [
        1, 2, 3, 4, 5, 6, 7, 0,
        1, 2, 3, 4, 5, 6, 7, 0,
        1, 2, 3, 4, 5, 6, 7, 0,
        1, 2, 3, 4, 5, 6, 7, 0,
    ];
    let blob_id = BlobId::from_metadata(Node::from(root_hash), EncodingType::RedStuff, size);

    let blob_obj = walrus_client
        .as_ref()
        .register_blob(
            &storage_resource,
            blob_id,
            root_hash,
            size,
            EncodingType::RedStuff,
            false,
        )
        .await?;

    // Make sure that we got the expected event
    let ContractEvent::BlobRegistered(blob_registered) = events.next().await.unwrap() else {
        bail!("unexpected event type. expecting BlobRegistered");
    };
    assert_eq!(blob_registered.blob_id, blob_id);

    let _blob_obj = walrus_client
        .as_ref()
        .certify_blob(blob_obj, &get_default_blob_certificate(blob_id, 1))
        .await?;

    // Make sure that we got the expected event
    let ContractEvent::BlobCertified(blob_certified) = events.next().await.unwrap() else {
        bail!("unexpected event type. expecting BlobCertified");
    };
    assert_eq!(blob_certified.blob_id, blob_id);

    Ok(())
}

#[tokio::test]
#[ignore = "ignore integration tests by default"]
async fn test_invalidate_blob() -> anyhow::Result<()> {
    _ = tracing_subscriber::fmt::try_init();

    let (_sui_cluster_handle, walrus_client, ..) = initialize_contract_and_wallet().await?;

    // Get event streams for the events
    let polling_duration = std::time::Duration::from_millis(50);
    let mut events = walrus_client
        .as_ref()
        .read_client
        .event_stream(polling_duration, None)
        .await?;

    #[rustfmt::skip]
    let blob_id = BlobId([
        1, 2, 3, 4, 5, 6, 7, 8,
        1, 2, 3, 4, 5, 6, 7, 8,
        1, 2, 3, 4, 5, 6, 7, 8,
        1, 2, 3, 4, 5, 6, 7, 8,
    ]);

    let certificate = get_default_invalid_certificate(blob_id, 1);

    walrus_client
        .as_ref()
        .invalidate_blob_id(&certificate)
        .await?;

    // Make sure that we got the expected event
    let ContractEvent::EpochParametersSelected(_) = events.next().await.unwrap() else {
        bail!("unexpected event type. expecting EpochParametersSelected");
    };
    let ContractEvent::EpochChangeStart(_) = events.next().await.unwrap() else {
        bail!("unexpected event type. expecting EpochChangeStart");
    };
    let ContractEvent::InvalidBlobID(invalid_blob_id) = events.next().await.unwrap() else {
        bail!("unexpected event type. expecting InvalidBlobID");
    };
    assert_eq!(invalid_blob_id.blob_id, blob_id);
    assert_eq!(invalid_blob_id.epoch, 1);
    Ok(())
}

#[tokio::test]
#[ignore = "ignore integration tests by default"]
async fn test_get_committee() -> anyhow::Result<()> {
    _ = tracing_subscriber::fmt::try_init();
    let (_sui_cluster_handle, walrus_client, ..) = initialize_contract_and_wallet().await?;
    let committee = walrus_client
        .as_ref()
        .read_client
        .current_committee()
        .await?;
    assert_eq!(committee.epoch, 1);
    assert_eq!(committee.n_shards().get(), 100);
    assert_eq!(committee.members().len(), 1);
    let storage_node = &committee.members()[0];
    assert_eq!(storage_node.name, "Test0");
    assert_eq!(storage_node.network_address.to_string(), "127.0.0.1:8080");
    assert_eq!(
        storage_node.shard_ids,
        (0..100).map(ShardIndex).collect::<Vec<_>>()
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

#[tokio::test]
#[ignore = "ignore integration tests by default"]
async fn test_first_epoch_change() -> anyhow::Result<()> {
    _ = tracing_subscriber::fmt::try_init();

    let (_sui_cluster_handle, walrus_client, node_client, node_cap) =
        initialize_contract_and_wallet().await?;

    // Get event streams for the events
    let polling_duration = std::time::Duration::from_millis(50);
    let mut events = walrus_client
        .as_ref()
        .read_client
        .event_stream(polling_duration, None)
        .await?;

    // Make sure that we got the expected event
    let ContractEvent::EpochParametersSelected(_) = events.next().await.unwrap() else {
        bail!("unexpected event type. expecting EpochParametersSelected");
    };
    let ContractEvent::EpochChangeStart(_) = events.next().await.unwrap() else {
        bail!("unexpected event type. expecting EpochChangeStart");
    };

    node_client
        .as_ref()
        .epoch_sync_done(node_cap.node_id)
        .await?;

    let ContractEvent::EpochChangeDone(_) = events.next().await.unwrap() else {
        bail!("unexpected event type. expecting EpochChangeDone");
    };

    // Checks that we can get correct epoch 0 committee.
    let (_current_committee, previous_committee) = walrus_client
        .as_ref()
        .read_client
        .current_and_previous_committee()
        .await?;
    assert_eq!(previous_committee.epoch, 0);
    assert_eq!(previous_committee.members().len(), 0);

    Ok(())
}
