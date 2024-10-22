// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! End-to-end tests for event blobs.
use std::time::Duration;

use anyhow::Context;
use walrus_core::{
    encoding::{Primary, Secondary},
    BlobId,
};
use walrus_service::{
    client::ClientCommunicationConfig,
    test_utils::{test_cluster, StorageNodeHandle},
};
use walrus_sui::client::ReadClient;

#[tokio::test]
#[ignore = "ignore E2E tests by default"]
async fn test_event_blobs() -> anyhow::Result<()> {
    let (_sui_cluster, _cluster, client) =
        test_cluster::default_setup_with_num_checkpoints_generic::<StorageNodeHandle>(
            Duration::from_secs(60 * 60),
            &[1, 1],
            false,
            Some(10),
            ClientCommunicationConfig::default_for_test(),
        )
        .await?;

    let event_blob_id = loop {
        if let Some(blob) = client
            .inner
            .sui_client()
            .read_client
            .last_certified_event_blob()
            .await
            .unwrap()
        {
            break blob.blob_id;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    };

    tokio::time::sleep(Duration::from_secs(10)).await;

    // Read the blob using primary slivers.
    let read_blob_primary = client
        .as_ref()
        .read_blob::<Primary>(&event_blob_id)
        .await
        .context("should be able to read blob we just stored")?;

    // Read using secondary slivers and check the result.
    let read_blob_secondary = client
        .as_ref()
        .read_blob::<Secondary>(&event_blob_id)
        .await
        .context("should be able to read blob we just stored")?;

    assert_eq!(read_blob_primary, read_blob_secondary);

    let mut prev_event_blob = event_blob_id;
    while prev_event_blob != BlobId::ZERO {
        let read_blob_primary = client
            .as_ref()
            .read_blob::<Primary>(&prev_event_blob)
            .await
            .context("should be able to read blob we just stored")?;
        let event_blob =
            walrus_service::node::events::event_blob::EventBlob::new(&read_blob_primary)?;
        prev_event_blob = event_blob.prev_blob_id();
        for i in event_blob {
            tracing::debug!("element: {:?}", i);
        }
    }
    Ok(())
}
