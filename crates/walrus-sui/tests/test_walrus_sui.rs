// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Contains integration tests for the Sui bindings.

use std::{num::NonZeroU16, sync::Arc};

use anyhow::bail;
use fastcrypto::traits::ToFromBytes;
use tokio_stream::StreamExt;
use walrus_core::{
    encoding::EncodingConfig,
    keys::{NetworkKeyPair, ProtocolKeyPair},
    merkle::Node,
    BlobId,
    EncodingType,
    ShardIndex,
};
use walrus_sui::{
    client::{
        BlobObjectMetadata,
        BlobPersistence,
        CoinType,
        PostStoreAction,
        ReadClient,
        SuiClientError,
        SuiContractClient,
    },
    test_utils::{
        self,
        get_default_blob_certificate,
        get_default_invalid_certificate,
        new_contract_client_on_sui_test_cluster,
        new_wallet_on_sui_test_cluster,
        system_setup::publish_with_default_system,
        TestClusterHandle,
    },
    types::{BlobEvent, ContractEvent, EpochChangeEvent, NodeRegistrationParams},
    utils,
};
use walrus_test_utils::WithTempDir;

const GAS_BUDGET: u64 = 1_000_000_000;

async fn initialize_contract_and_wallet(
) -> anyhow::Result<(Arc<TestClusterHandle>, WithTempDir<SuiContractClient>)> {
    #[cfg(not(msim))]
    let sui_cluster = test_utils::using_tokio::global_sui_test_cluster();
    #[cfg(msim)]
    let sui_cluster = test_utils::using_msim::global_sui_test_cluster().await;

    // Get a wallet on the global sui test cluster
    let mut admin_wallet = new_wallet_on_sui_test_cluster(sui_cluster.clone()).await?;
    let node_wallet = new_wallet_on_sui_test_cluster(sui_cluster.clone()).await?;

    // TODO(#793): make this nicer, s.t. we don't throw away the wallet with the storage node cap.
    // Fix once the testbed setup is ready.
    let (system_object, staking_object) = node_wallet
        .and_then_async(|wallet| publish_with_default_system(&mut admin_wallet.inner, wallet))
        .await?
        .inner;
    Ok((
        sui_cluster,
        admin_wallet
            .and_then_async(|wallet| {
                SuiContractClient::new(wallet, system_object, staking_object, None, GAS_BUDGET)
            })
            .await?,
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

    let (_sui_cluster_handle, walrus_client) = initialize_contract_and_wallet().await?;

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
    let blob_metadata = BlobObjectMetadata {
        blob_id,
        root_hash: Node::from(root_hash),
        unencoded_size: size,
        encoded_size: resource_size,
        encoding_type: EncodingType::RedStuff,
    };

    let blob_obj = walrus_client
        .as_ref()
        .register_blob(&storage_resource, blob_metadata, BlobPersistence::Permanent)
        .await?;
    assert_eq!(blob_obj.blob_id, blob_id);
    assert_eq!(blob_obj.size, size);
    assert_eq!(blob_obj.certified_epoch, None);
    assert_eq!(blob_obj.storage, storage_resource);
    assert_eq!(blob_obj.registered_epoch, 1);
    assert!(!blob_obj.deletable);

    // Make sure that we got the expected event
    let ContractEvent::EpochChangeEvent(EpochChangeEvent::EpochParametersSelected(_)) =
        events.next().await.unwrap()
    else {
        bail!("unexpected event type. expecting EpochParametersSelected");
    };
    let ContractEvent::EpochChangeEvent(EpochChangeEvent::EpochChangeStart(_)) =
        events.next().await.unwrap()
    else {
        bail!("unexpected event type. expecting EpochChangeStart");
    };
    let ContractEvent::BlobEvent(BlobEvent::Registered(blob_registered)) =
        events.next().await.unwrap()
    else {
        bail!("unexpected event type. expecting BlobRegistered");
    };
    assert_eq!(blob_registered.blob_id, blob_id);
    assert_eq!(blob_registered.epoch, blob_obj.registered_epoch);
    assert_eq!(blob_registered.encoding_type, blob_obj.encoding_type);
    assert_eq!(blob_registered.end_epoch, storage_resource.end_epoch);
    assert_eq!(blob_registered.size, blob_obj.size);

    let certificate = get_default_blob_certificate(blob_id, 1);

    walrus_client
        .as_ref()
        .certify_blob(blob_obj, &certificate, PostStoreAction::Keep)
        .await?;

    // Make sure that we got the expected event
    let ContractEvent::BlobEvent(BlobEvent::Certified(blob_certified)) =
        events.next().await.unwrap()
    else {
        bail!("unexpected event type. expecting BlobCertified");
    };
    assert_eq!(blob_certified.blob_id, blob_id);
    assert_eq!(Some(blob_registered.epoch), Some(1));
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
    let blob_metadata = BlobObjectMetadata {
        blob_id,
        root_hash: Node::from(root_hash),
        unencoded_size: size,
        encoded_size: resource_size,
        encoding_type: EncodingType::RedStuff,
    };

    let blob_obj = walrus_client
        .as_ref()
        .register_blob(&storage_resource, blob_metadata, BlobPersistence::Permanent)
        .await?;

    // Make sure that we got the expected event
    let ContractEvent::BlobEvent(BlobEvent::Registered(blob_registered)) =
        events.next().await.unwrap()
    else {
        bail!("unexpected event type. expecting BlobRegistered");
    };
    assert_eq!(blob_registered.blob_id, blob_id);

    walrus_client
        .as_ref()
        .certify_blob(
            blob_obj,
            &get_default_blob_certificate(blob_id, 1),
            PostStoreAction::Keep,
        )
        .await?;

    // Make sure that we got the expected event
    let ContractEvent::BlobEvent(BlobEvent::Certified(blob_certified)) =
        events.next().await.unwrap()
    else {
        bail!("unexpected event type. expecting BlobCertified");
    };
    assert_eq!(blob_certified.blob_id, blob_id);

    Ok(())
}

#[tokio::test]
#[ignore = "ignore integration tests by default"]
async fn test_invalidate_blob() -> anyhow::Result<()> {
    _ = tracing_subscriber::fmt::try_init();

    let (_sui_cluster_handle, walrus_client) = initialize_contract_and_wallet().await?;

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
    let ContractEvent::EpochChangeEvent(EpochChangeEvent::EpochParametersSelected(_)) =
        events.next().await.unwrap()
    else {
        bail!("unexpected event type. expecting EpochParametersSelected");
    };
    let ContractEvent::EpochChangeEvent(EpochChangeEvent::EpochChangeStart(_)) =
        events.next().await.unwrap()
    else {
        bail!("unexpected event type. expecting EpochChangeStart");
    };
    let ContractEvent::BlobEvent(BlobEvent::InvalidBlobID(invalid_blob_id)) =
        events.next().await.unwrap()
    else {
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
    let (_sui_cluster_handle, walrus_client) = initialize_contract_and_wallet().await?;
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
async fn test_register_candidate() -> anyhow::Result<()> {
    _ = tracing_subscriber::fmt::try_init();
    let (_sui_cluster_handle, walrus_client) = initialize_contract_and_wallet().await?;
    let protocol_key_pair = ProtocolKeyPair::generate();
    let network_key_pair = NetworkKeyPair::generate();

    let registration_params =
        NodeRegistrationParams::new_for_test(protocol_key_pair.public(), network_key_pair.public());

    let proof_of_possession = utils::generate_proof_of_possession(
        &protocol_key_pair,
        &walrus_client.inner,
        &registration_params,
        walrus_client.inner.current_epoch().await?,
    );

    let _cap = walrus_client
        .inner
        .register_candidate(&registration_params, proof_of_possession.clone())
        .await?;

    // Second registration should fail since there is already a capability object exist in the
    // address.
    let second_registration_result = walrus_client
        .inner
        .register_candidate(&registration_params, proof_of_possession)
        .await;

    assert!(matches!(
        second_registration_result,
        Err(SuiClientError::CapabilityObjectAlreadyExists(_))
    ));

    Ok(())
}

#[tokio::test]
#[ignore = "ignore integration tests by default"]
async fn test_exchange_sui_for_wal() -> anyhow::Result<()> {
    _ = tracing_subscriber::fmt::try_init();
    let (_sui_cluster_handle, walrus_client) = initialize_contract_and_wallet().await?;

    let exchange_id = walrus_client
        .as_ref()
        .create_and_fund_exchange(1_000_000)
        .await?;

    let exchange_val = 100_000;
    let pre_balance = walrus_client.as_ref().balance(CoinType::Wal).await?;
    walrus_client
        .as_ref()
        .exchange_sui_for_wal(exchange_id, exchange_val)
        .await?;

    let post_balance = walrus_client.as_ref().balance(CoinType::Wal).await?;
    assert_eq!(post_balance, pre_balance + exchange_val);

    Ok(())
}

#[tokio::test]
#[ignore = "ignore integration tests by default"]
async fn test_automatic_wal_coin_squashing() -> anyhow::Result<()> {
    _ = tracing_subscriber::fmt::try_init();
    let (sui_cluster_handle, client_1) = initialize_contract_and_wallet().await?;

    let original_balance = client_1.as_ref().balance(CoinType::Wal).await?;

    let client_2 =
        new_contract_client_on_sui_test_cluster(sui_cluster_handle.clone(), client_1.as_ref())
            .await?;

    let client_1_address = client_1.as_ref().address();
    let client_2_address = client_2.as_ref().address();

    let amount = 100_000;

    // Fund the wallet with two separate WAL coins.
    let wallet = client_1.as_ref().wallet().await;
    let mut tx_builder = client_1.as_ref().transaction_builder();
    tx_builder.pay_wal(client_2_address, amount).await?;
    tx_builder.pay_wal(client_2_address, amount).await?;
    client_1
        .as_ref()
        .sign_and_send_ptb(&wallet, tx_builder.finish().await?.0, None)
        .await?;
    drop(wallet);

    // Get the number of coins owned by the first wallet to check later that we received exactly
    // one coin.
    let n_coins = client_1
        .as_ref()
        .sui_client()
        .coin_read_api()
        .get_balance(
            client_1_address,
            Some(client_2.as_ref().read_client().wal_coin_type()),
        )
        .await?
        .coin_object_count;

    // Check that we have the correct balance.
    assert_eq!(client_2.as_ref().balance(CoinType::Wal).await?, 2 * amount);

    // Check that we need to send back two coins to cover the full amount.
    assert_eq!(
        client_2
            .as_ref()
            .read_client()
            .get_coins_with_total_balance(
                client_2.as_ref().address(),
                CoinType::Wal,
                2 * amount,
                vec![]
            )
            .await?
            .len(),
        2
    );

    // Now send the full amount back, which should trigger the squashing.
    let wallet = client_2.as_ref().wallet().await;
    let mut tx_builder = client_2.as_ref().transaction_builder();
    tx_builder.pay_wal(client_1_address, amount * 2).await?;
    client_2
        .as_ref()
        .sign_and_send_ptb(&wallet, tx_builder.finish().await?.0, None)
        .await?;

    // Check that the second wallet has no WAL coins left.
    assert_eq!(client_2.as_ref().balance(CoinType::Wal).await?, 0);

    // Check that the first wallet has the correct balance.
    assert_eq!(
        client_1.as_ref().balance(CoinType::Wal).await?,
        original_balance
    );

    // Check that we have the correct number of coins.
    assert_eq!(
        client_1
            .as_ref()
            .sui_client()
            .coin_read_api()
            .get_balance(
                client_1_address,
                Some(client_2.as_ref().read_client().wal_coin_type())
            )
            .await?
            .coin_object_count,
        n_coins + 1
    );
    Ok(())
}
