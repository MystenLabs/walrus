// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use sui_sdk::{
    rpc_types::{EventFilter, SuiTransactionBlockResponseOptions},
    types::{base_types::ObjectID, quorum_driver_types::ExecuteTransactionRequestType},
    SuiClientBuilder,
};
use test_cluster::TestClusterBuilder;
use walrus_e2e_tests::{compile_package, contract_path};

#[tokio::test]
async fn test_publish_blob_storage_package_and_check_events() -> anyhow::Result<()> {
    let mut test_cluster = TestClusterBuilder::new().build().await;
    let context = &mut test_cluster.wallet;
    let sender = context.active_address().unwrap();

    let (dependencies, compiled_modules) = compile_package(contract_path("blob_store")?);

    let sui = SuiClientBuilder::default()
        .build(&test_cluster.fullnode_handle.rpc_url)
        .await
        .expect("Failed to get client.");

    let dep_ids: Vec<ObjectID> = dependencies.published.values().cloned().collect();

    // Build a publish transaction
    let publish_tx = sui
        .transaction_builder()
        .publish(sender, compiled_modules, dep_ids, None, 10000000000)
        .await?;

    // Get a signed transaction
    let transaction = test_cluster.wallet.sign_transaction(&publish_tx);

    // Submit the transaction
    let transaction_response = sui
        .quorum_driver_api()
        .execute_transaction_block(
            transaction,
            SuiTransactionBlockResponseOptions::full_content(),
            Some(ExecuteTransactionRequestType::WaitForLocalExecution),
        )
        .await?;

    assert!(
        transaction_response.status_ok() == Some(true),
        "Status not ok"
    );

    // Read the system creation event

    let events = sui
        .event_api()
        .query_events(EventFilter::Sender(sender), None, None, false)
        .await?;

    let epoch_change_sync = events
        .data
        .iter()
        .find(|e| e.type_.name.to_string() == "EpochChangeSync")
        .expect("No EpochChangeSync event found");

    let epoch_change_done = events
        .data
        .iter()
        .find(|e| e.type_.name.to_string() == "EpochChangeDone")
        .expect("No EpochChangeDone event found");

    assert_eq!(
        epoch_change_sync.type_.address,
        epoch_change_done.type_.address
    );

    Ok(())
}
