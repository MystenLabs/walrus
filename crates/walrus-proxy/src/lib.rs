// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! # Walrus Proxy
//!
//! This crate provides a proxy layer for the Walrus system.
//! It includes modules for managing administration, configuration, metrics,
//! and providers.
//!
//! ## Modules
//!
//! - `admin`: Handles administrative actions for the proxy.
//! - `config`: Config params for this service.
//! - `consumer`: Manages the proxy code that consumes metrics from calling
//!   nodes.
//! - `handlers`: Axum handler that clients hit.
//! - `histogram_relay`: Export histograms to an agent for scraping. Remote
//!   write restrictions.
//! - `metrics`: Exposes metrics for the system to monitor the server itself
//! - `middleware`: Middleware we use to validate incoming client connections.
//! - `providers`: Implements the provider functionality for the proxy.

use fastcrypto::secp256r1::Secp256r1PublicKey;

/// Handles administrative actions for the proxy.
pub mod admin;

/// Config params for this service.
pub mod config;

/// Manages the proxy code that consumes metrics from calling nodes.
pub mod consumer;

/// Axum handler that clients hit.
pub mod handlers;

/// Export histograms to an agent for scraping. Remote write restrictions.
pub mod histogram_relay;

/// Exposes metrics for the system to monitor the server itself.
pub mod metrics;

/// Middleware we use to validate incoming client connections.
pub mod middleware;

/// Implements the provider functionality for the proxy.
pub mod providers;

/// Implements the prometheus to remote write conversion.
pub mod prom_to_mimir;

#[allow(clippy::all)]
// Implements remote write details, generated from protobufs.
pub mod remote_write;

/// The public key type for the storage node. This should be the same type as the network public
/// key defined in the walrus-core crate. Defining it here to avoid depending on the walrus-core
/// crate.
pub type NetworkPublicKey = Secp256r1PublicKey;

/// The Allower trait provides an interface for callers to decide if a generic
/// key type should be allowed
pub trait Allower<KeyType>: std::fmt::Debug + Send + Sync {
    /// allowed is called in middleware to determine if a client should be
    /// allowed. Providers implement this interface
    fn allowed(&self, key: &KeyType) -> bool;
}

/// Hidden reexports for the bin_version macro
pub mod _hidden {
    pub use const_str::concat;
    pub use git_version::git_version;
}

/// Define constants that hold the git revision and package versions.
///
/// Defines two global `const`s:
///   `GIT_REVISION`: The git revision as specified by the `GIT_REVISION` env
/// variable provided at   compile time, or the current git revision as
/// discovered by running `git describe`.
///
///   `VERSION`: The value of the `CARGO_PKG_VERSION` environment variable
/// concatenated with the   value of `GIT_REVISION`.
///
/// Note: This macro must only be used from a binary, if used inside a library
/// this will fail to compile.
#[macro_export]
macro_rules! bin_version {
    () => {
        $crate::git_revision!();

        const VERSION: &str =
            $crate::_hidden::concat!(env!("CARGO_PKG_VERSION"), "-", GIT_REVISION);
    };
}

/// Defines constant that holds the git revision at build time.
///
///   `GIT_REVISION`: The git revision as specified by the `GIT_REVISION` env
/// variable provided at   compile time, or the current git revision as
/// discovered by running `git describe`.
///
/// Note: This macro must only be used from a binary, if used inside a library
/// this will fail to compile.
#[macro_export]
macro_rules! git_revision {
    () => {
        const _ASSERT_IS_BINARY: () = {
            env!(
                "CARGO_BIN_NAME",
                "`bin_version!()` must be used from a binary"
            );
        };

        const GIT_REVISION: &str = {
            if let Some(revision) = option_env!("GIT_REVISION") {
                revision
            } else {
                let version = $crate::_hidden::git_version!(
                    args = ["--always", "--abbrev=12", "--dirty", "--exclude", "*"],
                    fallback = ""
                );

                if version.is_empty() {
                    panic!("unable to query git revision");
                }
                version
            }
        };
    };
}

/// var extracts environment variables at runtime with a default fallback value
/// if a default is not provided, the value is simply an empty string if not
/// found This function will return the provided default if env::var cannot find
/// the key or if the key is somehow malformed.
#[macro_export]
macro_rules! var {
    ($key:expr) => {
        match std::env::var($key) {
            Ok(val) => val,
            Err(_) => "".into(),
        }
    };
    ($key:expr, $default:expr) => {
        match std::env::var($key) {
            Ok(val) => val.parse::<_>().unwrap(),
            Err(_) => $default,
        }
    };
}

#[cfg(test)]
mod tests {

    use anyhow::{Error, Result};
    use fastcrypto::{
        secp256r1,
        traits::{
            EncodeDecodeBase64,
            KeyPair,
            RecoverableSignature,
            RecoverableSigner,
            ToFromBytes,
        },
    };
    use rand::thread_rng;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_secp156r1_is_recoverable() -> Result<(), Error> {
        let kp = secp256r1::Secp256r1KeyPair::generate(&mut thread_rng());
        // create our uuid for a message to sign
        let uid = Uuid::now_v7();
        let signature: secp256r1::recoverable::Secp256r1RecoverableSignature =
            kp.sign_recoverable(uid.as_bytes());
        // this would be sent in the header: Authorization: Secp256r1-recoverable
        // signature: base64_value message: base64_uuidv7
        let signature_b64 = signature.encode_base64();
        // now pretend we decoded that header and verify the pub key matches
        let recovered_signature =
            secp256r1::recoverable::Secp256r1RecoverableSignature::decode_base64(&signature_b64)
                .map_err(|e| anyhow::anyhow!(e))?;
        let recovered_pub_key = recovered_signature.recover(uid.as_bytes())?;
        // Verify the signature using the public key
        assert_eq!(kp.public().as_bytes(), recovered_pub_key.as_bytes());
        Ok(())
    }
}
