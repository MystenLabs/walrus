// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Utilities for the fanout proxy.

use fastcrypto::hash::{Digest, HashFunction as _, Sha256};
use sui_sdk::rpc_types::SuiTransactionBlockResponse;
use walrus_sdk::{
    core::BlobId,
    sui::types::{BlobEvent, BlobRegistered},
};

use crate::error::FanOutError;

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
pub fn compute_blob_digest_sha256(blob: &[u8]) -> Digest<32> {
    let mut blob_hash = Sha256::new();
    blob_hash.update(blob);
    blob_hash.finalize()
}

/// Returns the blob registration for the given blob ID.
///
/// If the expected registration is found, the function will return the registration
/// information. Otherwise, it will return an error.
pub(crate) fn blob_registration_from_response(
    response: SuiTransactionBlockResponse,
    expected_blob_id: BlobId,
) -> Result<BlobRegistered, FanOutError> {
    let registrations = blob_registrations_from_response(response);

    registrations
        .into_iter()
        .find(|reg| reg.blob_id == expected_blob_id)
        .ok_or(FanOutError::BlobIdNotRegistered(expected_blob_id))
}

/// Returns all the blob events contained in the response.
pub(crate) fn blob_registrations_from_response(
    response: SuiTransactionBlockResponse,
) -> Vec<BlobRegistered> {
    let Some(events) = response.events else {
        return vec![];
    };

    events
        .data
        .into_iter()
        .filter_map(|sui_event| {
            sui_event.try_into().ok().and_then(|blob_event| {
                if let BlobEvent::Registered(registration) = blob_event {
                    Some(registration)
                } else {
                    None
                }
            })
        })
        .collect::<Vec<_>>()
}
