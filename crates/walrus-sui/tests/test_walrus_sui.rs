// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// Allowing `unwrap`s in tests.
#![allow(clippy::unwrap_used)]

//! Contains integration tests for the Sui bindings.

use std::{num::NonZeroU16, sync::Arc, time::Duration};

use anyhow::bail;
use sui_types::base_types::SuiAddress;
use tokio_stream::StreamExt;
use walrus_core::{
    self,
    BlobId,
    EncodingType,
    ShardIndex,
    encoding::{EncodingConfig, EncodingFactory as _},
    keys::{NetworkKeyPair, ProtocolKeyPair},
    merkle::Node,
};
use walrus_sui::{
    client::{
        BlobObjectMetadata,
        BlobPersistence,
        CoinType,
        PostStoreAction,
        ReadClient,
        SuiContractClient,
    },
    test_utils::{
        TestClusterHandle,
        TestNodeKeys,
        new_contract_client_on_sui_test_cluster,
        system_setup::{SystemContext, initialize_contract_and_wallet_for_testing},
    },
    types::{
        BlobEvent,
        ContractEvent,
        EpochChangeEvent,
        NodeRegistrationParams,
        move_structs::BlobWithAttribute,
    },
    utils,
};
use walrus_test_utils::{WithTempDir, async_param_test};

async fn initialize_contract_and_wallet_with_single_node() -> anyhow::Result<(
    Arc<tokio::sync::Mutex<TestClusterHandle>>,
    WithTempDir<SuiContractClient>,
    SystemContext,
    TestNodeKeys,
)> {
    initialize_contract_and_wallet_for_testing(Duration::from_secs(3600), false, 0, 1).await
}

async fn initialize_contract_and_wallet_with_credits_with_single_node() -> anyhow::Result<(
    Arc<tokio::sync::Mutex<TestClusterHandle>>,
    WithTempDir<SuiContractClient>,
    SystemContext,
    TestNodeKeys,
)> {
    initialize_contract_and_wallet_for_testing(Duration::from_secs(3600), true, 10_000, 1).await
}

#[tokio::test]
#[ignore = "ignore E2E tests by default"]
async fn test_initialize_contract() -> anyhow::Result<()> {
    _ = tracing_subscriber::fmt::try_init();
    initialize_contract_and_wallet_with_single_node().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "ignore integration tests by default"]
async fn test_register_certify_blob_100_percent_buyer_credits() -> anyhow::Result<()> {
    _ = tracing_subscriber::fmt::try_init();
    let encoding_type = EncodingType::RS2;

    let (_sui_cluster_handle, walrus_client, _, _) =
        initialize_contract_and_wallet_with_credits_with_single_node().await?;

    // used to calculate the encoded size of the blob
    let encoding_config = EncodingConfig::new(NonZeroU16::new(1000).unwrap());

    let size = 10_000;
    // Send all WAL coins from admin wallet to a random wallet to ensure that we have a zero coin
    let admin_balance = walrus_client.as_ref().balance(CoinType::Wal).await?;
    if admin_balance > 0 {
        let random_address = SuiAddress::random_for_testing_only();

        // Send all WAL balance to random wallet
        walrus_client
            .as_ref()
            .send_wal(admin_balance, random_address)
            .await?;

        // Verify admin wallet now has zero balance
        let new_admin_balance = walrus_client.as_ref().balance(CoinType::Wal).await?;
        assert_eq!(new_admin_balance, 0);
    }

    // Call the reserve_space method with zero coin
    let resource_size = encoding_config
        .get_for_type(encoding_type)
        .encoded_blob_length(size)
        .unwrap();
    let storage_resource = walrus_client
        .as_ref()
        .reserve_space(resource_size, 3)
        .await?;
    assert_eq!(storage_resource.start_epoch, 1);
    assert_eq!(storage_resource.end_epoch, 4);
    assert_eq!(storage_resource.storage_size, resource_size);

    Ok(())
}

#[tokio::test]
#[ignore = "ignore integration tests by default"]
async fn test_register_certify_blob() -> anyhow::Result<()> {
    _ = tracing_subscriber::fmt::try_init();
    let encoding_type = EncodingType::RS2;

    let (_sui_cluster_handle, walrus_client, _, test_node_keys) =
        initialize_contract_and_wallet_with_single_node().await?;

    // used to calculate the encoded size of the blob
    let encoding_config = EncodingConfig::new(NonZeroU16::new(1000).unwrap());

    // Get event streams for the events
    let polling_duration = std::time::Duration::from_millis(50);
    let mut events = walrus_client
        .as_ref()
        .read_client
        .event_stream(polling_duration, None)
        .await?;

    let size = 10_000;
    let resource_size = encoding_config
        .get_for_type(encoding_type)
        .encoded_blob_length(size)
        .unwrap();
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

    let blob_id = BlobId::from_metadata(Node::from(root_hash), encoding_type, size);
    let blob_metadata = BlobObjectMetadata {
        blob_id,
        root_hash: Node::from(root_hash),
        unencoded_size: size,
        encoded_size: resource_size,
        encoding_type,
    };

    let blob_obj = walrus_client
        .as_ref()
        .register_blobs(
            vec![(blob_metadata, storage_resource.clone())],
            BlobPersistence::Permanent,
        )
        .await?
        .into_iter()
        .next()
        .expect("expected one blob object");
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

    let certificate = test_node_keys.blob_certificate_for_signers(&[0], blob_id, 1)?;

    let blob_with_attr = BlobWithAttribute {
        blob: blob_obj,
        attribute: None,
    };
    walrus_client
        .as_ref()
        .certify_blobs(&[(&blob_with_attr, certificate)], PostStoreAction::Keep)
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
    let blob_id = BlobId::from_metadata(Node::from(root_hash), encoding_type, size);
    let blob_metadata = BlobObjectMetadata {
        blob_id,
        root_hash: Node::from(root_hash),
        unencoded_size: size,
        encoded_size: resource_size,
        encoding_type,
    };

    let blob_obj = walrus_client
        .as_ref()
        .register_blobs(
            vec![(blob_metadata, storage_resource)],
            BlobPersistence::Permanent,
        )
        .await?
        .into_iter()
        .next()
        .expect("expected one blob object");

    // Make sure that we got the expected event
    let ContractEvent::BlobEvent(BlobEvent::Registered(blob_registered)) =
        events.next().await.unwrap()
    else {
        bail!("unexpected event type. expecting BlobRegistered");
    };
    assert_eq!(blob_registered.blob_id, blob_id);

    let blob_with_attr = BlobWithAttribute {
        blob: blob_obj,
        attribute: None,
    };
    walrus_client
        .as_ref()
        .certify_blobs(
            &[(
                &blob_with_attr,
                test_node_keys.blob_certificate_for_signers(&[0], blob_id, 1)?,
            )],
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

    let (_sui_cluster_handle, walrus_client, _, test_node_keys) =
        initialize_contract_and_wallet_with_single_node().await?;

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

    let certificate = test_node_keys.invalid_blob_certificate_for_signers(&[0], blob_id, 1)?;

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
    let (_sui_cluster_handle, walrus_client, _, test_node_keys) =
        initialize_contract_and_wallet_with_single_node().await?;
    let committee = walrus_client
        .as_ref()
        .read_client
        .current_committee()
        .await?;
    assert_eq!(committee.epoch, 1);
    assert_eq!(committee.n_shards().get(), 1000);
    assert_eq!(committee.members().len(), 1);
    let storage_node = &committee.members()[0];
    assert_eq!(storage_node.name, "Test0");
    assert_eq!(storage_node.network_address.to_string(), "127.0.0.1:8080");
    assert_eq!(
        storage_node.shard_ids,
        (0..1000).map(ShardIndex).collect::<Vec<_>>()
    );
    assert_eq!(&storage_node.public_key, test_node_keys.keys()[0].public());
    Ok(())
}

#[tokio::test]
#[ignore = "ignore integration tests by default"]
async fn test_set_authorized() -> anyhow::Result<()> {
    use sui_types::base_types::ObjectID;
    use walrus_sui::types::move_structs::Authorized;

    _ = tracing_subscriber::fmt::try_init();
    let (_sui_cluster_handle, walrus_client, _, _) =
        initialize_contract_and_wallet_with_single_node().await?;
    let protocol_key_pair = ProtocolKeyPair::generate();
    let network_key_pair = NetworkKeyPair::generate();

    let registration_params =
        NodeRegistrationParams::new_for_test(protocol_key_pair.public(), network_key_pair.public());

    let proof_of_possession = utils::generate_proof_of_possession(
        &protocol_key_pair,
        &walrus_client.inner,
        walrus_client.inner.current_epoch().await?,
    );

    let cap = walrus_client
        .inner
        .register_candidate(&registration_params, proof_of_possession.clone())
        .await?;

    // Set the commission receiver to a new address
    let new_address = SuiAddress::random_for_testing_only();
    walrus_client
        .as_ref()
        .set_commission_receiver(cap.node_id, Authorized::Address(new_address))
        .await?;

    let commission_receiver = walrus_client
        .as_ref()
        .read_client()
        .get_staking_pool(cap.node_id)
        .await?
        .commission_receiver;
    assert_eq!(commission_receiver, Authorized::Address(new_address));

    // Set the governance authorized to the storage node cap
    walrus_client
        .as_ref()
        .set_governance_authorized(cap.node_id, Authorized::Object(cap.id))
        .await?;

    let governance_authorized = walrus_client
        .as_ref()
        .read_client()
        .get_staking_pool(cap.node_id)
        .await?
        .governance_authorized;
    assert_eq!(governance_authorized, Authorized::Object(cap.id));

    // Set the governance authorized to another object using the storage node cap to authenticate
    let object_id = ObjectID::random();
    walrus_client
        .as_ref()
        .set_governance_authorized(cap.node_id, Authorized::Object(object_id))
        .await?;

    let governance_authorized = walrus_client
        .as_ref()
        .read_client()
        .get_staking_pool(cap.node_id)
        .await?
        .governance_authorized;
    assert_eq!(governance_authorized, Authorized::Object(object_id));

    Ok(())
}

#[tokio::test]
#[ignore = "ignore integration tests by default"]
async fn test_exchange_sui_for_wal() -> anyhow::Result<()> {
    _ = tracing_subscriber::fmt::try_init();
    let (_sui_cluster_handle, walrus_client, system_context, _) =
        initialize_contract_and_wallet_with_single_node().await?;
    let exchange_id = walrus_client
        .as_ref()
        .create_and_fund_exchange(
            system_context
                .wal_exchange_pkg_id
                .expect("wal_exchange should exist for tests"),
            1_000_000,
        )
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
async fn test_collect_commission() -> anyhow::Result<()> {
    _ = tracing_subscriber::fmt::try_init();

    // Set zero duration, s.t. we can change the epoch whenever we need to.
    let (_sui_cluster_handle, walrus_client, _, _) =
        initialize_contract_and_wallet_for_testing(Duration::ZERO, false, 0, 1).await?;

    let cap = walrus_client
        .as_ref()
        .read_client()
        .get_address_capability_object(walrus_client.as_ref().address())
        .await?
        .expect("cap should exist");

    // Signal that epoch zero is done.
    walrus_client.as_ref().epoch_sync_done(1, cap.id).await?;

    let resource_size = 1_000_000_000_000;
    let _storage_resource = walrus_client
        .as_ref()
        .reserve_space(resource_size, 1)
        .await?;

    // Change epoch to allow collecting commission.
    walrus_client.as_ref().voting_end().await?;
    walrus_client.as_ref().initiate_epoch_change().await?;

    // Get the value of the commission from the staking pool.
    let expected_commission = walrus_client
        .as_ref()
        .read_client()
        .get_staking_pool(cap.node_id)
        .await?
        .commission;

    // Collect the commission.
    let commission = walrus_client
        .as_ref()
        .collect_commission(cap.node_id)
        .await?;

    assert_eq!(expected_commission, commission);

    Ok(())
}

async_param_test! {
    #[ignore = "ignore integration tests by default"]
    test_automatic_wal_coin_squashing -> anyhow::Result<()> : [
        #[tokio::test] send_one: (1, 1),
        #[tokio::test] send_two_as_one: (2, 1),
        #[tokio::test] send_three_as_two: (3, 2),
        #[tokio::test] send_four_as_two: (4, 2),
        #[tokio::test] send_four_as_three: (4, 3),
    ]
}
async fn test_automatic_wal_coin_squashing(
    n_source_coins: u64,
    n_target_coins: u64,
) -> anyhow::Result<()> {
    _ = tracing_subscriber::fmt::try_init();
    // Use a source_amount that is cleanly divisible by `n_target_coins` to make sure that we send
    // the full amount back in `n_target_coins` coins payments.
    let source_amount = 10_000 * n_target_coins;
    let target_amount = n_source_coins * source_amount / n_target_coins;

    let (sui_cluster_handle, client_1, _, _) =
        initialize_contract_and_wallet_with_single_node().await?;

    let original_balance = client_1.as_ref().balance(CoinType::Wal).await?;

    let client_2 =
        new_contract_client_on_sui_test_cluster(sui_cluster_handle.clone(), client_1.as_ref())
            .await?;

    let client_1_address = client_1.as_ref().address();
    let client_2_address = client_2.as_ref().address();

    // Fund the wallet with two separate WAL coins.
    client_1
        .as_ref()
        .multiple_pay_wal(client_2_address, source_amount, n_source_coins)
        .await?;

    // Get the number of coins owned by the first wallet to check later that we received exactly
    // `n_target_coins` coins.
    let n_coins = client_1
        .as_ref()
        .retriable_sui_client()
        .get_balance(
            client_1_address,
            Some(client_2.as_ref().read_client().wal_coin_type().to_owned()),
        )
        .await?
        .coin_object_count;

    // Check that we have the correct balance.
    assert_eq!(
        client_2.as_ref().balance(CoinType::Wal).await?,
        n_source_coins * source_amount
    );

    // Check that we need to use `n_source_coins` coins to cover the full amount.
    assert_eq!(
        client_2
            .as_ref()
            .read_client()
            .get_coins_with_total_balance(
                client_2.as_ref().address(),
                CoinType::Wal,
                n_target_coins * target_amount,
                vec![]
            )
            .await?
            .len(),
        usize::try_from(n_source_coins).unwrap()
    );

    // Now send the full amount back in `n_target_coins` coins payments, which should trigger the
    // squashing.
    client_2
        .as_ref()
        .multiple_pay_wal(client_1_address, target_amount, n_target_coins)
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
            .retriable_sui_client()
            .get_balance(
                client_1_address,
                Some(client_2.as_ref().read_client().wal_coin_type().to_owned()),
            )
            .await?
            .coin_object_count,
        n_coins + usize::try_from(n_target_coins).unwrap()
    );
    Ok(())
}
