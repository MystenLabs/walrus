// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use sui_sdk::rpc_types::EventFilter;
use test_cluster::TestClusterBuilder;
use walrus_sui::test_utils::system_setup::publish_with_default_system;

#[tokio::test]
#[ignore = "ignore E2E tests by default"]
async fn test_publish_blob_storage_package_and_check_events() -> anyhow::Result<()> {
    let mut test_cluster = TestClusterBuilder::new().build().await;
    let wallet = &mut test_cluster.wallet;
    let sender = wallet.active_address()?;
    publish_with_default_system(wallet, "blob_store").await?;

    // Read the system creation event

    let events = wallet
        .get_client()
        .await?
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
