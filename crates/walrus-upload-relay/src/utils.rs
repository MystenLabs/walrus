// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Utilities for the Walrus Upload Relay.

use fastcrypto::hash::{Digest, HashFunction as _, Sha256};
use sui_sdk::rpc_types::SuiTransactionBlockResponse;
use sui_types::transaction::{
    CallArg,
    SenderSignedData,
    TransactionData,
    TransactionDataV1,
    TransactionKind,
};
use walrus_sdk::core::ensure;

use crate::{
    error::WalrusUploadRelayError,
    params::{DIGEST_LEN, HashedAuthPackage},
};

/// Compute a SHA256 hash of a blob.
pub fn compute_digest_sha256(blob: &[u8]) -> Digest<32> {
    let mut blob_hash = Sha256::new();
    blob_hash.update(blob);
    blob_hash.finalize()
}

/// Checks the blob hash in the transaction matches the hash of the blob that was sent to the
/// Walrus Upload Relay.
pub(crate) fn check_tx_auth_package(
    blob: &[u8],
    rcvd_nonce: &[u8; DIGEST_LEN],
    tx: SuiTransactionBlockResponse,
) -> Result<(), WalrusUploadRelayError> {
    let tx_auth_package = extract_hashed_auth_package(tx)?;
    auth_package_checks(tx_auth_package, blob, rcvd_nonce)
}

/// Performs the authentication checks on the hashed auth package in the transaction.
///
/// NOTE: Ordering is important. First we discard attempts by unauthorized senders using the
/// nonce, then we check the length, making sure that the blob-hashing work we will have to do is
/// fixed (and has been paid for, through the tip check), and finally we check that the hash of
/// the blob is matching.
pub(crate) fn auth_package_checks(
    tx_auth_package: HashedAuthPackage,
    blob: &[u8],
    rcvd_nonce: &[u8; DIGEST_LEN],
) -> Result<(), WalrusUploadRelayError> {
    // 1. Check that the nonce in the received package is the preimage of the one in the tx.
    let rcvd_nonce_digest = compute_digest_sha256(rcvd_nonce).digest;
    ensure!(
        rcvd_nonce_digest == tx_auth_package.nonce_digest,
        WalrusUploadRelayError::InvalidNonceHash
    );

    // 2. Check that the received blob has the expected length.
    ensure!(
        u64::try_from(blob.len()).expect("using 32 or 64 bit arch")
            == tx_auth_package.unencoded_length,
        WalrusUploadRelayError::BlobLengthMismatch
    );

    // 3. Check that
    ensure!(
        compute_digest_sha256(blob).digest == tx_auth_package.blob_digest,
        WalrusUploadRelayError::BlobDigestMismatch
    );
    Ok(())
}

/// Extracts the hashed auth package from the transaction.
pub(crate) fn extract_hashed_auth_package(
    tx: SuiTransactionBlockResponse,
) -> Result<HashedAuthPackage, WalrusUploadRelayError> {
    let orig_tx: SenderSignedData = bcs::from_bytes(&tx.raw_transaction).map_err(|_| {
        WalrusUploadRelayError::other("error deserializing the transaction from bytes")
    })?;

    let TransactionData::V1(TransactionDataV1 {
        kind: TransactionKind::ProgrammableTransaction(ptb),
        ..
    }) = orig_tx.transaction_data()
    else {
        return Err(WalrusUploadRelayError::InvalidTipTransaction);
    };
    let Some(CallArg::Pure(tx_auth_package_bytes)) = ptb.inputs.first() else {
        return Err(WalrusUploadRelayError::MissingAuthPackage);
    };
    bcs::from_bytes(tx_auth_package_bytes).map_err(|_| WalrusUploadRelayError::InvalidAuthPackage)
}

#[cfg(test)]
mod tests {
    use walrus_test_utils::param_test;

    use super::*;

    struct TestPackage {
        blob: Vec<u8>,
        nonce: [u8; DIGEST_LEN],
        auth_package: HashedAuthPackage,
    }

    impl TestPackage {
        fn for_test() -> Self {
            let blob = b"hello";
            let nonce = [23; DIGEST_LEN];
            Self {
                blob: blob.to_vec(),
                nonce,
                auth_package: HashedAuthPackage {
                    blob_digest: compute_digest_sha256(blob.as_slice()).digest,
                    nonce_digest: compute_digest_sha256(&nonce).digest,
                    unencoded_length: blob.len() as u64,
                },
            }
        }
    }

    param_test! {
        test_auth_package_checks: [
            correct: (TestPackage::for_test(), None),
            wrong_nonce: ({
                let mut test_package = TestPackage::for_test();
                test_package.nonce = [24; DIGEST_LEN];
                test_package
            }, Some(WalrusUploadRelayError::InvalidNonceHash)),
            wrong_blob: ({
                let mut test_package = TestPackage::for_test();
                test_package.blob = b"world".to_vec();
                test_package
            }, Some(WalrusUploadRelayError::BlobDigestMismatch)),
            wrong_blob_length: ({
                let mut test_package = TestPackage::for_test();
                test_package.auth_package.unencoded_length = 10;
                test_package
            }, Some(WalrusUploadRelayError::BlobLengthMismatch)),
        ]
    }
    fn test_auth_package_checks(
        test_package: TestPackage,
        expected_error: Option<WalrusUploadRelayError>,
    ) {
        let result = auth_package_checks(
            test_package.auth_package,
            &test_package.blob,
            &test_package.nonce,
        );

        match (result, expected_error) {
            (Ok(_), None) => {}
            (Err(err), Some(expected_err)) => match (&err, &expected_err) {
                (
                    WalrusUploadRelayError::InvalidNonceHash,
                    WalrusUploadRelayError::InvalidNonceHash,
                ) => {}
                (
                    WalrusUploadRelayError::BlobDigestMismatch,
                    WalrusUploadRelayError::BlobDigestMismatch,
                ) => {}
                (
                    WalrusUploadRelayError::BlobLengthMismatch,
                    WalrusUploadRelayError::BlobLengthMismatch,
                ) => {}
                _ => panic!("expected error {expected_err:?} but got {err:?}"),
            },
            (Err(_), None) => panic!("expected an error but got Ok"),
            (Ok(_), Some(_)) => panic!("expected an error but got Ok"),
        }
    }
}
