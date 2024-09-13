// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use sui_macros::sim_test;
use sui_protocol_config::ProtocolConfig;
use walrus_core::encoding::{Primary, Secondary};
use walrus_service::{client::responses::BlobStoreResult, test_utils::test_cluster};

// Tests that we can create a Walrus cluster with a Sui cluster and running basic
// operations deterministically.
#[sim_test(check_determinism)]
#[ignore = "ignore simtests by default"]
async fn simtest_walrus_basic_determinism() {
    let _guard = ProtocolConfig::apply_overrides_for_testing(|_, mut config| {
        // TODO: remove once Sui simtest can work with these features.
        config.set_enable_jwk_consensus_updates_for_testing(false);
        config.set_random_beacon_for_testing(false);
        config
    });

    let (_sui_cluster, _walrus_cluster, client) = test_cluster::default_setup().await.unwrap();

    // Write a random blob.
    let blob = walrus_test_utils::random_data(31415);
    let BlobStoreResult::NewlyCreated {
        blob_object: blob_confirmation,
        ..
    } = client
        .as_ref()
        .reserve_and_store_blob(&blob, 1, true, false)
        .await
        .unwrap()
    else {
        panic!("expect newly stored blob")
    };

    // Read the blob using primary slivers.
    let read_blob = client
        .as_ref()
        .read_blob::<Primary>(&blob_confirmation.blob_id)
        .await
        .expect("should be able to read blob we just stored");

    // Check that blob is what we wrote.
    assert_eq!(read_blob, blob);

    // Read using secondary slivers and check the result.
    let read_blob = client
        .as_ref()
        .read_blob::<Secondary>(&blob_confirmation.blob_id)
        .await
        .expect("should be able to read blob we just stored");
    assert_eq!(read_blob, blob);
}
