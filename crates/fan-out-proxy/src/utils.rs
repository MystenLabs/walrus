// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Utilities for the fanout proxy.

use fastcrypto::hash::{Digest, HashFunction as _, Sha256};

/// Defines a constant containing the version consisting of the package version and git revision.
///
/// We are using a macro as placing this logic into a library can result in unnecessary builds.
// TODO: Duplicate from the same macro in walrus-service. Either import service or place in a shared
// crate.
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
    blob_hash.update(&blob);
    let blob_digest = blob_hash.finalize();
    blob_digest
}
