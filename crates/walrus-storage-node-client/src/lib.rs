// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Code for interacting with the Walrus system.

use std::fmt::Write;

use fastcrypto::traits::ToFromBytes as _;
use walrus_core::NetworkPublicKey;

pub use self::{
    client::{
        RecoverySymbolsFilter,
        StorageNodeClient,
        StorageNodeClientBuilder,
        SymbolIdFilter,
        UploadIntent,
    },
    error::{ClientBuildError, NodeError},
};

pub mod api;
pub mod client;
pub mod error;

mod node_response;
mod tls;

/// Returns a string `<first-4-bytes-as-hex>.network.walrus.alt` corresponding to the public key.
pub fn server_name_from_public_key(public_key: &NetworkPublicKey) -> String {
    const N_PUBLIC_KEY_BYTES_IN_SUBJECT: usize = 4;

    let public_key_bytes = public_key.as_bytes();

    let mut server_name = String::new();
    for byte in public_key_bytes.iter().take(N_PUBLIC_KEY_BYTES_IN_SUBJECT) {
        write!(&mut server_name, "{byte:x}").expect("write on a string always succeeds");
    }
    server_name.push_str(".network.walrus.alt");
    server_name
}
