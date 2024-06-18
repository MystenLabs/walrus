// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use sui_macros::sim_test;
use sui_protocol_config::ProtocolConfig;
use walrus_core::encoding::{Primary, Secondary};
use walrus_service::{client::BlobStoreResult, test_cluster};

#[sim_test(check_determinism)]
async fn test_walrus_basic_determinism() {
    let _guard = ProtocolConfig::apply_overrides_for_testing(|_, mut config| {
        // TODO: this test fails due to some non-determinism caused by submitting messages to
        // consensus. It does not appear to be caused by this feature itself, so I'm disabling this
        // until I have time to debug further.
        config.set_enable_jwk_consensus_updates_for_testing(false);
        config.set_random_beacon_for_testing(false);
        config
    });

    tracing::info!("ZZZZ start default setup");
    let (sui_cluster, _walrus_cluster, mut client) = test_cluster::default_setup().await.unwrap();
    tracing::info!("ZZZZ finish default setup");

    let blob = walrus_test_utils::random_data(31415);
    let BlobStoreResult::NewlyCreated(blob_confirmation) = client
        .as_ref()
        .reserve_and_store_blob(&blob, 1, true)
        .await
        .unwrap()
    else {
        panic!("expect newly stored blob")
    };

    // We need to reset the reqwest client to ensure that the client cannot communicate with nodes
    // that are being shut down.
    client
        .as_mut()
        .reset_reqwest_client()
        .expect("Reset reqwest client failed");

    // Read the blob.
    let read_blob = client
        .as_ref()
        .read_blob::<Primary>(&blob_confirmation.blob_id)
        .await
        .expect("Read blob failed");

    assert_eq!(read_blob, blob);

    tracing::info!(
        "ZZZZZ hold sui cluster {:?}",
        sui_cluster._cluster.get_address_0()
    );

    let read_blob = client
        .as_ref()
        .read_blob::<Secondary>(&blob_confirmation.blob_id)
        .await
        .expect("Read blob failed");

    assert_eq!(read_blob, blob);
}
