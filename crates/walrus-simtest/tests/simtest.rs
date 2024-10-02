// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use sui_protocol_config::ProtocolConfig;
use walrus_core::encoding::{Primary, Secondary};
use walrus_proc_macros::walrus_simtest;
use walrus_service::{
    client::{responses::BlobStoreResult, StoreWhen},
    test_utils::test_cluster,
};
use walrus_sui::client::BlobPersistence;

// Tests that we can create a Walrus cluster with a Sui cluster and running basic
// operations deterministically.
#[walrus_simtest(check_determinism)]
#[ignore = "ignore simtests by default"]
async fn walrus_basic_determinism() {
    let _guard = ProtocolConfig::apply_overrides_for_testing(|_, mut config| {
        // TODO: remove once Sui simtest can work with these features.
        config.set_enable_jwk_consensus_updates_for_testing(false);
        config.set_random_beacon_for_testing(false);
        config
    });

    let (_sui_cluster, _cluster, client) = test_cluster::default_setup().await.unwrap();

    // Write a random blob.
    let blob = walrus_test_utils::random_data(31415);
    let BlobStoreResult::NewlyCreated {
        blob_object: blob_confirmation,
        ..
    } = client
        .as_ref()
        .reserve_and_store_blob(&blob, 1, StoreWhen::Always, BlobPersistence::Permanent)
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
