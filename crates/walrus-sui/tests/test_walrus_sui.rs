// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// Allowing `unwrap`s in tests.
#![allow(clippy::unwrap_used)]

//! Contains integration tests for the Sui bindings.

use std::{num::NonZeroU16, str::FromStr as _, sync::Arc, time::Duration};

use sui_types::{
    Identifier,
    SUI_FRAMEWORK_PACKAGE_ID,
    TypeTag,
    base_types::SuiAddress,
    gas_coin::GAS,
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    transaction::{Argument, Command, FundsWithdrawalArg},
};
use walrus_core::{
    self,
    BlobId,
    EncodingType,
    ShardIndex,
    encoding::{EncodingConfig, EncodingFactory as _},
    keys::{NetworkKeyPair, ProtocolKeyPair},
    merkle::Node,
    messages::BlobPersistenceType,
};
use walrus_sui::{
    client::{
        BlobObjectMetadata,
        BlobPersistence,
        PostStoreAction,
        ReadClient,
        SuiClientError,
        SuiContractClient,
        dual_client::DEFAULT_CHECKPOINT_WAIT_TIMEOUT,
        transaction_builder::build_transaction_data_with_address_balance_gas,
    },
    coin::{Coin, CoinType},
    test_utils::{
        TestClusterHandle,
        TestNodeKeys,
        system_setup::{SystemContext, initialize_contract_and_wallet_for_testing},
    },
    types::{
        NodeRegistrationParams,
        move_structs::{BlobWithAttribute, PooledBlob},
    },
    utils,
};
use walrus_test_utils::WithTempDir;

async fn initialize_contract_and_wallet_with_single_node() -> anyhow::Result<(
    Arc<tokio::sync::Mutex<TestClusterHandle>>,
    WithTempDir<SuiContractClient>,
    SystemContext,
    TestNodeKeys,
)> {
    initialize_contract_and_wallet_for_testing(Duration::from_hours(1), false, 0, 1).await
}

async fn initialize_contract_and_wallet_with_credits_with_single_node() -> anyhow::Result<(
    Arc<tokio::sync::Mutex<TestClusterHandle>>,
    WithTempDir<SuiContractClient>,
    SystemContext,
    TestNodeKeys,
)> {
    initialize_contract_and_wallet_for_testing(Duration::from_hours(1), true, 10_000, 1).await
}

#[tokio::test]
#[ignore = "ignore E2E tests by default"]
async fn test_initialize_contract() -> anyhow::Result<()> {
    walrus_test_utils::init_tracing();
    initialize_contract_and_wallet_with_single_node().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "ignore integration tests by default"]
async fn test_register_certify_blob_100_percent_buyer_credits() -> anyhow::Result<()> {
    walrus_test_utils::init_tracing();
    let encoding_type = EncodingType::RS2;

    let (_sui_cluster_handle, walrus_client, _, _) =
        initialize_contract_and_wallet_with_credits_with_single_node().await?;

    // used to calculate the encoded size of the blob
    let encoding_config = EncodingConfig::new(NonZeroU16::new(1000).unwrap());

    let size = 10_000;
    // Send all WAL coins from admin wallet to a random wallet to ensure that we have a zero coin
    let admin_balance = walrus_client
        .as_ref()
        .address_balance(CoinType::Wal)
        .await?;
    if admin_balance > 0 {
        let random_address = SuiAddress::random_for_testing_only();

        // Send all WAL balance to random wallet
        walrus_client
            .as_ref()
            .send_wal(admin_balance, random_address)
            .await?;

        // Verify admin wallet now has zero balance
        let new_admin_balance = walrus_client
            .as_ref()
            .address_balance(CoinType::Wal)
            .await?;
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
    walrus_test_utils::init_tracing();
    let encoding_type = EncodingType::RS2;

    let (_sui_cluster_handle, walrus_client, _, test_node_keys) =
        initialize_contract_and_wallet_with_single_node().await?;

    // used to calculate the encoded size of the blob
    let encoding_config = EncodingConfig::new(NonZeroU16::new(1000).unwrap());

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
    assert_eq!(blob_obj.encoding_type, encoding_type);
    assert_eq!(blob_obj.storage.end_epoch, storage_resource.end_epoch);

    let certificate = test_node_keys.blob_certificate_for_signers(&[0], blob_id, 1)?;

    let blob_with_attr = BlobWithAttribute {
        blob: blob_obj,
        attribute: None,
    };
    walrus_client
        .as_ref()
        .certify_blobs(&[(&blob_with_attr, certificate)], PostStoreAction::Keep)
        .await?;

    // Now register and certify a blob with a different blob id to confirm the second tx
    // succeeds end-to-end.
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
    assert_eq!(blob_obj.blob_id, blob_id);

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

    Ok(())
}

#[tokio::test]
#[ignore = "ignore integration tests by default"]
async fn test_storage_pool_pooled_blob_lifecycle() -> anyhow::Result<()> {
    walrus_test_utils::init_tracing();
    let encoding_type = EncodingType::RS2;

    let (_sui_cluster_handle, walrus_client, _system_context, test_node_keys) =
        initialize_contract_and_wallet_with_single_node().await?;

    let encoding_config = EncodingConfig::new(NonZeroU16::new(1000).unwrap());
    let size = 10_000;
    let encoded_size = encoding_config
        .get_for_type(encoding_type)
        .encoded_blob_length(size)
        .unwrap();

    let storage_pool_object_id = walrus_client
        .as_ref()
        .create_storage_pool(encoded_size, 3)
        .await?;

    let storage_pool_created = walrus_client
        .as_ref()
        .storage_pool_status(storage_pool_object_id)
        .await?;
    assert_eq!(
        storage_pool_created.storage_pool_object_id,
        storage_pool_object_id
    );
    assert_eq!(
        storage_pool_created.reserved_encoded_capacity_bytes,
        encoded_size
    );

    let first_root_hash = Node::from([7; 32]);
    let first_blob_id = BlobId::from_metadata(first_root_hash.clone(), encoding_type, size);
    let first_blob_metadata = BlobObjectMetadata {
        blob_id: first_blob_id,
        root_hash: first_root_hash,
        unencoded_size: size,
        encoded_size,
        encoding_type,
    };

    let first_pooled_blob = walrus_client
        .as_ref()
        .register_pooled_blobs(
            storage_pool_object_id,
            vec![first_blob_metadata],
            BlobPersistence::Deletable,
        )
        .await?
        .into_iter()
        .next()
        .expect("expected one pooled blob object");
    assert_eq!(first_pooled_blob.blob_id, first_blob_id);
    assert_eq!(first_pooled_blob.unencoded_size, size);
    assert_eq!(first_pooled_blob.storage_pool_id, storage_pool_object_id);
    assert!(first_pooled_blob.deletable);
    assert!(!first_pooled_blob.is_certified());
    assert_eq!(
        first_pooled_blob.blob_persistence_type(),
        BlobPersistenceType::Deletable {
            object_id: first_pooled_blob.id.into(),
        }
    );

    let certificate = test_node_keys.blob_certificate_for_signers_with_persistence(
        &[0],
        first_blob_id,
        first_pooled_blob.registered_epoch,
        BlobPersistenceType::Deletable {
            object_id: first_pooled_blob.id.into(),
        },
    )?;
    walrus_client
        .as_ref()
        .certify_pooled_blobs(storage_pool_object_id, &[(&first_pooled_blob, certificate)])
        .await?;

    let first_pooled_blob_after_certify: PooledBlob = walrus_client
        .as_ref()
        .retriable_sui_client()
        .get_sui_object(first_pooled_blob.id)
        .await?;
    assert_eq!(first_pooled_blob_after_certify.blob_id, first_blob_id);
    assert_eq!(
        first_pooled_blob_after_certify.storage_pool_id,
        storage_pool_object_id
    );
    assert!(first_pooled_blob_after_certify.deletable);
    assert!(first_pooled_blob_after_certify.is_certified());

    walrus_client
        .as_ref()
        .extend_storage_pool(storage_pool_object_id, 2)
        .await?;

    let storage_pool_extended = walrus_client
        .as_ref()
        .storage_pool_status(storage_pool_object_id)
        .await?;
    assert_eq!(
        storage_pool_extended.storage_pool_object_id,
        storage_pool_object_id
    );
    assert!(storage_pool_extended.end_epoch > storage_pool_created.end_epoch);

    walrus_client
        .as_ref()
        .increase_storage_pool_capacity(storage_pool_object_id, encoded_size)
        .await?;

    let second_root_hash = Node::from([9; 32]);
    let second_blob_id = BlobId::from_metadata(second_root_hash.clone(), encoding_type, size);
    let second_blob_metadata = BlobObjectMetadata {
        blob_id: second_blob_id,
        root_hash: second_root_hash,
        unencoded_size: size,
        encoded_size,
        encoding_type,
    };

    let second_pooled_blob = walrus_client
        .as_ref()
        .register_pooled_blobs(
            storage_pool_object_id,
            vec![second_blob_metadata],
            BlobPersistence::Permanent,
        )
        .await?
        .into_iter()
        .next()
        .expect("expected one pooled blob object after increasing capacity");
    assert_eq!(second_pooled_blob.blob_id, second_blob_id);
    assert_eq!(second_pooled_blob.storage_pool_id, storage_pool_object_id);
    assert!(!second_pooled_blob.deletable);

    let storage_pool_after_register = walrus_client
        .as_ref()
        .storage_pool_status(storage_pool_object_id)
        .await?;
    let blob_count_after_register = storage_pool_after_register.blob_count;

    walrus_client
        .as_ref()
        .delete_pooled_blob(storage_pool_object_id, first_blob_id)
        .await?;

    let storage_pool_after_delete = walrus_client
        .as_ref()
        .storage_pool_status(storage_pool_object_id)
        .await?;
    assert_eq!(
        storage_pool_after_delete.blob_count,
        blob_count_after_register - 1
    );

    // The second blob should still be in the pool.
    let second_pooled_blob_after_delete: PooledBlob = walrus_client
        .as_ref()
        .retriable_sui_client()
        .get_sui_object(second_pooled_blob.id)
        .await?;
    assert_eq!(second_pooled_blob_after_delete.blob_id, second_blob_id);

    Ok(())
}

#[tokio::test]
#[ignore = "ignore integration tests by default"]
async fn test_invalidate_blob() -> anyhow::Result<()> {
    walrus_test_utils::init_tracing();

    let (_sui_cluster_handle, walrus_client, _, test_node_keys) =
        initialize_contract_and_wallet_with_single_node().await?;

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

    assert_eq!(walrus_client.as_ref().read_client.current_epoch().await?, 1);
    Ok(())
}

#[tokio::test]
#[ignore = "ignore integration tests by default"]
async fn test_get_committee() -> anyhow::Result<()> {
    walrus_test_utils::init_tracing();
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

    walrus_test_utils::init_tracing();
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

    tracing::info!(
        ?registration_params,
        ?proof_of_possession,
        "registering candidate"
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
    walrus_test_utils::init_tracing();
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
    let pre_balance = walrus_client
        .as_ref()
        .address_balance(CoinType::Wal)
        .await?;
    walrus_client
        .as_ref()
        .exchange_sui_for_wal(exchange_id, exchange_val)
        .await?;

    let post_balance = walrus_client
        .as_ref()
        .address_balance(CoinType::Wal)
        .await?;
    assert_eq!(post_balance, pre_balance + exchange_val);

    Ok(())
}

#[tokio::test]
#[ignore = "ignore integration tests by default"]
async fn test_collect_commission() -> anyhow::Result<()> {
    walrus_test_utils::init_tracing();

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
    walrus_client.as_ref().epoch_sync_done(2, cap.id).await?;

    // Collecting commission before voting_end should fail because it is blocked.
    let err = walrus_client
        .as_ref()
        .collect_commission(cap.node_id)
        .await
        .expect_err("commission should be blocked before voting_end");
    assert!(
        err.to_string()
            .contains("no balance change for sender in transaction response"),
        "unexpected error: {err}",
    );

    // After voting_end, the blocked commission is cleared and can be collected.
    walrus_client.as_ref().voting_end().await?;

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

/// Returns a contract client whose wallet holds no funds.
async fn new_unfunded_contract_client(
    existing_client: &mut SuiContractClient,
) -> anyhow::Result<WithTempDir<SuiContractClient>> {
    let temp_dir = tempfile::tempdir()?;
    let wallet = utils::create_wallet(
        &temp_dir.path().join("wallet_config.yaml"),
        existing_client.wallet_mut().get_active_env().to_owned(),
        None,
        None,
    )
    .await?;
    let rpc_urls = &[wallet.get_rpc_url().to_string()];
    let client = SuiContractClient::new(
        wallet,
        rpc_urls,
        &existing_client.read_client().contract_config(),
        Default::default(),
        None,
        DEFAULT_CHECKPOINT_WAIT_TIMEOUT,
    )
    .await?;
    Ok(WithTempDir {
        inner: client,
        temp_dir,
    })
}

/// Sends coin objects with the given `amounts` of `coin_type` from the address balance of `sender`
/// to `recipient`.
async fn send_coin_objects(
    sender: &SuiContractClient,
    recipient: SuiAddress,
    coin_type: &str,
    amounts: &[u64],
) -> anyhow::Result<()> {
    let coin_type = TypeTag::from_str(coin_type)?;
    let total: u64 = amounts.iter().sum();
    let mut ptb = ProgrammableTransactionBuilder::new();
    let withdrawal = ptb.funds_withdrawal(FundsWithdrawalArg::balance_from_sender(
        total,
        coin_type.clone(),
    ))?;
    let coin = ptb.programmable_move_call(
        SUI_FRAMEWORK_PACKAGE_ID,
        Identifier::new("coin")?,
        Identifier::new("redeem_funds")?,
        vec![coin_type.clone()],
        vec![withdrawal],
    );
    let amount_args = amounts
        .iter()
        .map(|amount| ptb.pure(*amount))
        .collect::<Result<Vec<_>, _>>()?;
    let Argument::Result(split_result) = ptb.command(Command::SplitCoins(coin, amount_args)) else {
        unreachable!("commands return results");
    };
    let split_coins = (0..amounts.len())
        .map(|index| Argument::NestedResult(split_result, u16::try_from(index).unwrap()))
        .collect();
    ptb.transfer_args(recipient, split_coins);
    ptb.programmable_move_call(
        SUI_FRAMEWORK_PACKAGE_ID,
        Identifier::new("coin")?,
        Identifier::new("destroy_zero")?,
        vec![coin_type.clone()],
        vec![coin],
    );
    let sui_withdrawn = if coin_type == GAS::type_tag() {
        total
    } else {
        0
    };
    let transaction = build_transaction_data_with_address_balance_gas(
        ptb.finish(),
        sender.retriable_sui_client(),
        sender.address(),
        None,
        sui_withdrawn,
    )
    .await?;
    sender
        .sign_and_send_transaction(transaction, "send_coin_objects")
        .await?;
    Ok(())
}

/// Returns the number of coin objects of `coin_type` owned by `address`.
async fn coin_object_count(
    client: &SuiContractClient,
    address: SuiAddress,
    coin_type: &str,
) -> anyhow::Result<usize> {
    Ok(client
        .retriable_sui_client()
        .select_all_coins(address, coin_type)
        .await?
        .len())
}

#[tokio::test]
#[ignore = "ignore integration tests by default"]
async fn test_pay_from_address_balance() -> anyhow::Result<()> {
    const STORAGE_SIZE: u64 = 100 * 1024 * 1024;
    walrus_test_utils::init_tracing();
    let (_sui_cluster_handle, mut walrus_client, system_context, _) =
        initialize_contract_and_wallet_with_single_node().await?;
    let exchange_id = walrus_client
        .as_ref()
        .create_and_fund_exchange(
            system_context
                .wal_exchange_pkg_id
                .expect("wal_exchange should exist for tests"),
            1_000_000_000,
        )
        .await?;

    let client = new_unfunded_contract_client(walrus_client.as_mut()).await?;
    let address = client.as_ref().address();
    let wal_coin_type = client.as_ref().read_client().wal_coin_type().to_owned();

    // Fund the address balances; no coin objects are created.
    let sui_funding = 10_000_000_000;
    let wal_funding = 1_000_000_000;
    walrus_client
        .as_ref()
        .send_sui(sui_funding, address)
        .await?;
    walrus_client
        .as_ref()
        .send_wal(wal_funding, address)
        .await?;
    assert_eq!(
        client.as_ref().address_balance(CoinType::Sui).await?,
        sui_funding
    );
    assert_eq!(
        client.as_ref().address_balance(CoinType::Wal).await?,
        wal_funding
    );

    // Pay for storage with WAL and gas from the address balances.
    client.as_ref().reserve_space(STORAGE_SIZE, 2).await?;
    let wal_after_reserve = client.as_ref().address_balance(CoinType::Wal).await?;
    assert!(wal_after_reserve < wal_funding);
    assert!(client.as_ref().address_balance(CoinType::Sui).await? < sui_funding);

    // Send SUI and WAL, as when refilling sub-wallets, to the recipient's address balances.
    let recipient = SuiAddress::random_for_testing_only();
    client.as_ref().send_sui(1_000_000, recipient).await?;
    client.as_ref().send_wal(1_000, recipient).await?;
    let sui_client = client.as_ref().retriable_sui_client();
    assert_eq!(
        sui_client.get_address_balance(recipient, Coin::SUI).await?,
        1_000_000
    );
    assert_eq!(
        sui_client
            .get_address_balance(recipient, &wal_coin_type)
            .await?,
        1_000
    );

    // Exchange SUI for WAL; the WAL arrives in the address balance.
    let exchange_amount = 500;
    client
        .as_ref()
        .exchange_sui_for_wal(exchange_id, exchange_amount)
        .await?;
    assert_eq!(
        client.as_ref().address_balance(CoinType::Wal).await?,
        wal_after_reserve - 1_000 + exchange_amount
    );

    // Many payments in one PTB share a single withdrawal; the protocol allows at most 10.
    client.as_ref().multiple_pay_wal(recipient, 10, 20).await?;
    assert_eq!(
        sui_client
            .get_address_balance(recipient, &wal_coin_type)
            .await?,
        1_200
    );

    // Zero amounts do not need a withdrawal, which Sui rejects for an amount of zero.
    client.as_ref().send_sui(0, recipient).await?;
    client.as_ref().send_wal(0, recipient).await?;

    // The client never created coin objects for itself or the recipient.
    for owner in [address, recipient] {
        assert_eq!(
            coin_object_count(client.as_ref(), owner, Coin::SUI).await?,
            0
        );
        assert_eq!(
            coin_object_count(client.as_ref(), owner, &wal_coin_type).await?,
            0
        );
    }

    Ok(())
}

#[tokio::test]
#[ignore = "ignore integration tests by default"]
async fn test_deposit_coins_to_address_balance() -> anyhow::Result<()> {
    walrus_test_utils::init_tracing();
    let (_sui_cluster_handle, mut walrus_client, _, _) =
        initialize_contract_and_wallet_with_single_node().await?;
    let client = new_unfunded_contract_client(walrus_client.as_mut()).await?;
    let address = client.as_ref().address();
    let wal_coin_type = client.as_ref().read_client().wal_coin_type().to_owned();

    // The wallet only holds coin objects, so it cannot pay for gas from its address balance.
    let sui_coins = [2_000_000_000, 1_000_000_000];
    let wal_coins = [700, 300, 5];
    send_coin_objects(walrus_client.as_ref(), address, Coin::SUI, &sui_coins).await?;
    send_coin_objects(walrus_client.as_ref(), address, &wal_coin_type, &wal_coins).await?;
    assert!(matches!(
        client
            .as_ref()
            .send_wal(1, SuiAddress::random_for_testing_only())
            .await,
        Err(SuiClientError::NoCompatibleWalCoins)
    ));

    // The first transaction pays gas with the SUI coins, the second one moves the rest.
    let digests = client.as_ref().deposit_coins_to_address_balance().await?;
    assert_eq!(digests.len(), 2);
    assert_eq!(
        coin_object_count(client.as_ref(), address, Coin::SUI).await?,
        0
    );
    assert_eq!(
        coin_object_count(client.as_ref(), address, &wal_coin_type).await?,
        0
    );
    assert_eq!(
        client.as_ref().address_balance(CoinType::Wal).await?,
        wal_coins.iter().sum::<u64>()
    );
    // Only gas was paid; deleting the coins even yields storage rebates.
    let sui_balance = client.as_ref().address_balance(CoinType::Sui).await?;
    assert!(sui_balance > sui_coins.iter().sum::<u64>() - 100_000_000);

    // Nothing is left to move.
    assert!(
        client
            .as_ref()
            .deposit_coins_to_address_balance()
            .await?
            .is_empty()
    );

    // The deposited funds can be spent.
    client
        .as_ref()
        .send_wal(1_000, SuiAddress::random_for_testing_only())
        .await?;
    Ok(())
}
