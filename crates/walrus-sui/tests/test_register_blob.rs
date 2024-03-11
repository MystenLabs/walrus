// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use test_cluster::TestClusterBuilder;
use walrus_e2e_tests::publish_package;
use walrus_move_client::client::WalrusSuiClient;

#[tokio::test]
async fn test_obtain_storage() -> anyhow::Result<()> {
    let test_cluster = TestClusterBuilder::new().build().await;
    let mut wallet = test_cluster.wallet;
    let (package_id, system_object) = publish_package(&mut wallet, "blob_store").await?;
    let walrus_client =
        WalrusSuiClient::new(wallet, package_id, system_object, 10000000000).await?;
    let storage_resource = walrus_client.reserve_space(10, 3).await?;
    assert_eq!(storage_resource.start_epoch, 0);
    assert_eq!(storage_resource.end_epoch, 3);
    assert_eq!(storage_resource.storage_size, 10);
    Ok(())
}
