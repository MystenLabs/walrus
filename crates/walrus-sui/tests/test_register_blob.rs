// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use fastcrypto::{
    bls12381::min_pk::{BLS12381AggregateSignature, BLS12381PrivateKey},
    traits::{Signer, ToFromBytes},
};
use test_cluster::TestClusterBuilder;
use walrus_core::{
    messages::{Confirmation, ConfirmationCertificate},
    BlobId,
    EncodingType,
};
use walrus_e2e_tests::publish_package;
use walrus_sui::client::WalrusSuiClient;

#[tokio::test]
async fn test_register_blob() -> anyhow::Result<()> {
    let test_cluster = TestClusterBuilder::new().build().await;
    let mut wallet = test_cluster.wallet;
    let (package_id, system_object) = publish_package(&mut wallet, "blob_store").await?;
    let walrus_client =
        WalrusSuiClient::new(wallet, package_id, system_object, 10000000000).await?;
    let size = 10000;
    let storage_resource = walrus_client.reserve_space(size, 3).await?;
    assert_eq!(storage_resource.start_epoch, 0);
    assert_eq!(storage_resource.end_epoch, 3);
    assert_eq!(storage_resource.storage_size, size);
    #[rustfmt::skip]
    let blob_id = BlobId([
        1, 2, 3, 4, 5, 6, 7, 8,
        1, 2, 3, 4, 5, 6, 7, 8,
        1, 2, 3, 4, 5, 6, 7, 8,
        1, 2, 3, 4, 5, 6, 7, 8,
    ]);
    let blob_obj = walrus_client
        .register_blob(&storage_resource, blob_id, size, EncodingType::RedStuff)
        .await?;
    assert_eq!(blob_obj.blob_id, blob_id);
    assert_eq!(blob_obj.encoded_size, size);
    assert_eq!(blob_obj.certified, None);
    assert_eq!(blob_obj.storage, storage_resource);
    assert_eq!(blob_obj.stored_epoch, 0);
    // Use the same private key that is registered in the committee in
    // `contracts/blob_store/e2etest.move`
    let mut sk = [0; 32];
    sk[31] = 117;
    let sk = BLS12381PrivateKey::from_bytes(&sk)?;
    let confirmation = bcs::to_bytes(&Confirmation::new(0, blob_id)).unwrap();
    let signature = BLS12381AggregateSignature::from(sk.sign(&confirmation));
    let cert = ConfirmationCertificate {
        confirmation,
        signature,
        signers: vec![0],
    };
    let blob_obj = walrus_client.certify_blob(&blob_obj, &cert).await?;
    assert_eq!(blob_obj.certified, Some(0));
    Ok(())
}
