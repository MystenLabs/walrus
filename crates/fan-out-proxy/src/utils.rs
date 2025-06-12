// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Utilities for the fanout proxy.

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
    error::FanOutError,
    params::{DIGEST_LEN, HashedAuthPackage},
};

/// Defines a constant containing the version consisting of the package version and git revision.
///
/// We are using a macro as placing this logic into a library can result in unnecessary builds.
// TODO: This is a duplicate from the same macro in walrus-service. Let's move this macro to a
// shared crate.
#[macro_export]
macro_rules! version {
    () => {{
        /// The Git revision obtained through `git describe` at compile time.
        const GIT_REVISION: &str = {
            if let Some(revision) = option_env!("GIT_REVISION") {
                revision
            } else {
                let version = git_version::git_version!(
                    args = ["--always", "--abbrev=12", "--dirty", "--exclude", "*"],
                    fallback = ""
                );
                if version.is_empty() {
                    panic!("unable to query git revision");
                }
                version
            }
        };

        // The version consisting of the package version and Git revision.
        walrus_sdk::core::concat_const_str!(env!("CARGO_PKG_VERSION"), "-", GIT_REVISION)
    }};
}

pub use version;

/// Compute a SHA256 hash of a blob.
pub fn compute_digest_sha256(blob: &[u8]) -> Digest<32> {
    let mut blob_hash = Sha256::new();
    blob_hash.update(blob);
    blob_hash.finalize()
}

/// Checks the blob hash in the transaction matches the hash of the blob that was sent to the
/// fanout.
pub(crate) fn check_tx_auth_package(
    blob: &[u8],
    rcvd_nonce: &[u8; DIGEST_LEN],
    tx: SuiTransactionBlockResponse,
) -> Result<(), FanOutError> {
    let orig_tx: SenderSignedData = bcs::from_bytes(&tx.raw_transaction)
        .map_err(|_| FanOutError::other("error deserializing the transaction from bytes"))?;

    let TransactionData::V1(TransactionDataV1 {
        kind: TransactionKind::ProgrammableTransaction(ptb),
        ..
    }) = orig_tx.transaction_data()
    else {
        return Err(FanOutError::other("invalid transaction data"));
    };
    let Some(CallArg::Pure(tx_auth_package_bytes)) = ptb.inputs.first() else {
        return Err(FanOutError::other("invalid transaction input construction"));
    };
    let tx_auth_package: HashedAuthPackage = bcs::from_bytes(tx_auth_package_bytes)
        .map_err(|_| FanOutError::other("cannot decode the bytes of input 0"))?;

    // Authentication checks.
    // NOTE: Ordering is important. First we discard attempts by unauthorized senders using the
    // nonce, then we check the length, making sure that the blob-hashing work we will have to do is
    // fixed (and has been paid for, through the tip check), and finally we check that the hash of
    // the blob is matching.

    // 1. Check that the nonce in the received package is the preimage of the one in the tx.
    let rcvd_nonce_digest = compute_digest_sha256(rcvd_nonce).digest;
    ensure!(
        rcvd_nonce_digest == tx_auth_package.nonce_digest,
        FanOutError::InvalidNonceHash
    );

    // 2. Check that the received blob has the expected length.
    ensure!(
        u64::try_from(blob.len()).expect("using 32 or 64 bit arch")
            == tx_auth_package.unencoded_length,
        FanOutError::BlobLengthMismatch
    );

    // 3. Check that
    ensure!(
        compute_digest_sha256(blob).digest == tx_auth_package.blob_digest,
        FanOutError::BlobDigestMismatch
    );
    Ok(())
}
