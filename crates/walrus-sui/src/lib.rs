// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Bindings to call the Walrus contracts from Rust.

#[macro_use]
pub mod utils;
pub mod client;
pub mod config;
pub mod contracts;
pub mod system_setup;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

pub mod types;

/// Schema for the [`sui_types::event::EventID`] type.
#[allow(unused)]
#[derive(Debug, utoipa::ToSchema)]
#[schema(
    as = EventID,
    rename_all = "camelCase",
    examples(json!({
        "txDigest": "EhtoQF9UpPyg5PsPUs69LdkcRrjQ3R4cTsHnwxZVTNrC",
        "eventSeq": 0
    }))
)]
pub struct EventIdSchema {
    #[schema(format = Byte)]
    tx_digest: Vec<u8>,
    #[schema(value_type = String)]
    event_seq: u64,
}

/// Schema for the `ObjectID` type.
#[derive(Debug, utoipa::ToSchema)]
#[schema(
    as = ObjectID,
    value_type = String,
    title = "Sui object ID",
    description = "Sui object ID as a hexadecimal string",
    examples("0x56ae1c86e17db174ea002f8340e28880bc8a8587c56e8604a4fa6b1170b23a60"),
)]
pub struct ObjectIdSchema(());

/// Schema for Sui addresses.
#[derive(Debug, utoipa::ToSchema)]
#[schema(
    as = SuiAddress,
    value_type = String,
    title = "Sui address",
    description = "Sui address encoded as a hexadecimal string",
    examples("0x02a212de6a9dfa3a69e22387acfbafbb1a9e591bd9d636e7895dcfc8de0"),
)]
pub struct SuiAddressSchema(());
